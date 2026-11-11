//! Operator-supplied migrations, independent of the upstream version sequence.
//! SQL is trusted deployment configuration, never request input.
use std::{borrow::Cow, collections::BTreeSet};

use serde::Deserialize;
use sqlx::{
    Connection, PgConnection,
    migrate::{Migrate, Migration, MigrationType, Migrator},
};

use crate::SessionStoreError;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Bundle {
    pub namespace: String,
    pub migrations: Vec<Entry>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Entry {
    pub version: i64,
    pub description: String,
    pub sql: String,
    pub legacy_version: Option<i64>,
}

fn invalid(message: impl Into<String>) -> SessionStoreError {
    SessionStoreError::ExtensionMigration(message.into())
}

impl Entry {
    fn migration(&self) -> Migration {
        Migration::new(
            self.version,
            Cow::Owned(self.description.clone()),
            MigrationType::Simple,
            Cow::Owned(self.sql.clone()),
            false,
        )
    }
}

impl Bundle {
    pub fn from_env() -> Result<Option<Self>, SessionStoreError> {
        match std::env::var("SESSION_EXTENSION_MIGRATIONS") {
            Ok(value) => Self::parse(&value).map(Some),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(_) => Err(invalid("SESSION_EXTENSION_MIGRATIONS is not UTF-8")),
        }
    }

    pub fn parse(value: &str) -> Result<Self, SessionStoreError> {
        let bundle: Self = serde_json::from_str(value)
            .map_err(|_| invalid("invalid extension migration bundle"))?;
        if bundle.namespace.is_empty()
            || !bundle
                .namespace
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err(invalid("invalid extension migration namespace"));
        }
        let mut versions = BTreeSet::new();
        let mut legacy = BTreeSet::new();
        let mut previous = 0;
        for entry in &bundle.migrations {
            if entry.version <= previous
                || !versions.insert(entry.version)
                || entry.description.is_empty()
                || entry.sql.trim().is_empty()
            {
                return Err(invalid(
                    "extension migrations must have positive increasing versions and nonempty descriptions and SQL",
                ));
            }
            if let Some(version) = entry.legacy_version
                && (version <= 0 || !legacy.insert(version))
            {
                return Err(invalid("duplicate or invalid legacy migration version"));
            }
            previous = entry.version;
        }
        Ok(bundle)
    }
}

/// Hold SQLx's own advisory lock across adoption, upstream migrations and
/// extension application. Concurrent new API replicas cannot interleave them.
/// An old binary must be stopped during the one-time legacy adoption rollout.
pub(crate) async fn run(
    conn: &mut PgConnection,
    mut core: Migrator,
    bundle: Option<&Bundle>,
) -> Result<(), SessionStoreError> {
    conn.lock().await?;
    core.set_locking(false);
    let result = run_locked(conn, core, bundle).await;
    let unlocked = conn.unlock().await;
    result?;
    unlocked?;
    Ok(())
}

async fn run_locked(
    conn: &mut PgConnection,
    core: Migrator,
    bundle: Option<&Bundle>,
) -> Result<(), SessionStoreError> {
    let mut tx = conn.begin().await?;
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS schema_migrations_extensions (
        namespace text NOT NULL, version bigint NOT NULL, description text NOT NULL,
        checksum bytea NOT NULL, installed_on timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (namespace, version))",
    )
    .execute(&mut *tx)
    .await?;
    let applied: Vec<(String, i64, String, Vec<u8>)> = sqlx::query_as(
        "SELECT namespace, version, description, checksum FROM schema_migrations_extensions ORDER BY namespace, version"
    ).fetch_all(&mut *tx).await?;
    for (namespace, version, description, checksum) in &applied {
        let entry = bundle
            .filter(|b| b.namespace == *namespace)
            .and_then(|b| b.migrations.iter().find(|e| e.version == *version));
        if !entry.is_some_and(|e| {
            e.description == *description && e.migration().checksum.as_ref() == checksum
        }) {
            return Err(invalid(format!(
                "missing or changed extension migration {namespace}/{version}"
            )));
        }
    }
    let tracked: bool = sqlx::query_scalar("SELECT to_regclass('_sqlx_migrations') IS NOT NULL")
        .fetch_one(&mut *tx)
        .await?;
    if let Some(bundle) = bundle {
        // Validate every legacy record before committing any ledger transfer.
        for entry in &bundle.migrations {
            let Some(old) = entry.legacy_version.filter(|_| tracked) else {
                continue;
            };
            let row: Option<(String, Vec<u8>, bool)> = sqlx::query_as(
                "SELECT description, checksum, success FROM _sqlx_migrations WHERE version=$1",
            )
            .bind(old)
            .fetch_optional(&mut *tx)
            .await?;
            let Some((description, checksum, success)) = row else {
                continue;
            };
            let migration = entry.migration();
            if success
                && description == entry.description
                && checksum == migration.checksum.as_ref()
            {
                // Never remove a migration still embedded in this binary.
                if core
                    .iter()
                    .any(|m| m.version == old && m.checksum.as_ref() == checksum)
                {
                    return Err(invalid(format!(
                        "legacy migration {old} is still in the upstream migration set"
                    )));
                }
                sqlx::query("INSERT INTO schema_migrations_extensions(namespace,version,description,checksum,installed_on)
                    SELECT $1,$2,description,checksum,installed_on FROM _sqlx_migrations WHERE version=$3
                    ON CONFLICT (namespace,version) DO NOTHING")
                    .bind(&bundle.namespace).bind(entry.version).bind(old).execute(&mut *tx).await?;
                sqlx::query("DELETE FROM _sqlx_migrations WHERE version=$1")
                    .bind(old)
                    .execute(&mut *tx)
                    .await?;
            } else if !success
                || !core
                    .iter()
                    .any(|m| m.version == old && m.checksum.as_ref() == checksum)
            {
                return Err(invalid(format!(
                    "unexpected legacy migration {old}; refusing adoption"
                )));
            }
        }
    }
    tx.commit().await?;
    core.run(&mut *conn).await?;
    if let Some(bundle) = bundle {
        for entry in &bundle.migrations {
            let mut tx = conn.begin().await?;
            let installed: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM schema_migrations_extensions WHERE namespace=$1 AND version=$2)")
                .bind(&bundle.namespace).bind(entry.version).fetch_one(&mut *tx).await?;
            if !installed {
                sqlx::raw_sql(&entry.sql).execute(&mut *tx).await?;
                sqlx::query("INSERT INTO schema_migrations_extensions(namespace,version,description,checksum) VALUES($1,$2,$3,$4)")
                    .bind(&bundle.namespace).bind(entry.version).bind(&entry.description).bind(entry.migration().checksum.as_ref())
                    .execute(&mut *tx).await?;
            }
            tx.commit().await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bundle() -> Bundle {
        Bundle::parse(r#"{"namespace":"test","migrations":[{"version":202611110001,"description":"extension table","sql":"CREATE TABLE extension_data (id int); INSERT INTO extension_data VALUES (42);","legacy_version":61}]}"#).unwrap()
    }

    fn core(reuse_version: bool) -> Migrator {
        let version = if reuse_version { 61 } else { 1 };
        Migrator {
            migrations: Cow::Owned(vec![Migration::new(
                version,
                Cow::Borrowed("upstream table"),
                MigrationType::Simple,
                Cow::Borrowed("CREATE TABLE upstream_data (id int);"),
                false,
            )]),
            ..Migrator::DEFAULT
        }
    }

    async fn database() -> Option<(PgConnection, String)> {
        let url = std::env::var("SESSION_EXTENSION_TEST_DATABASE_URL")
            .or_else(|_| std::env::var("SESSION_SQLX_TEST_DATABASE_URL"))
            .or_else(|_| std::env::var("SESSION_RUNTIME_TEST_DATABASE_URL"))
            .ok()?;
        let mut conn = PgConnection::connect(&url).await.unwrap();
        let schema = format!("extension_test_{}", uuid::Uuid::new_v4().simple());
        sqlx::raw_sql(&format!(
            "CREATE SCHEMA {schema}; SET search_path TO {schema}"
        ))
        .execute(&mut conn)
        .await
        .unwrap();
        Some((conn, schema))
    }

    async fn cleanup(mut conn: PgConnection, schema: String) {
        sqlx::raw_sql(&format!("DROP SCHEMA {schema} CASCADE"))
            .execute(&mut conn)
            .await
            .unwrap();
    }

    async fn legacy(conn: &mut PgConnection, bundle: &Bundle) {
        conn.ensure_migrations_table().await.unwrap();
        let e = &bundle.migrations[0];
        sqlx::raw_sql(&e.sql).execute(&mut *conn).await.unwrap();
        sqlx::query("INSERT INTO _sqlx_migrations(version,description,checksum,success,execution_time,installed_on) VALUES(61,$1,$2,true,0,'2026-01-01Z')")
            .bind(&e.description).bind(e.migration().checksum.as_ref()).execute(conn).await.unwrap();
    }

    #[test]
    fn rejects_duplicate_versions_and_invalid_bundles() {
        for input in [
            r#"{"namespace":"","migrations":[]}"#,
            r#"{"namespace":"ok","migrations":[],"typo":true}"#,
            r#"{"namespace":"ok","migrations":[{"version":0,"description":"x","sql":"select 1"}]}"#,
        ] {
            assert!(Bundle::parse(input).is_err());
        }
        let mut value = serde_json::to_value(serde_json::from_str::<serde_json::Value>(r#"{"namespace":"ok","migrations":[{"version":1,"description":"x","sql":"select 1"}]}"#).unwrap()).unwrap();
        let entry = value["migrations"][0].clone();
        value["migrations"].as_array_mut().unwrap().push(entry);
        assert!(Bundle::parse(&value.to_string()).is_err());
    }

    #[tokio::test]
    async fn fresh_install_and_rerun_keep_ledgers_independent() {
        let Some((mut conn, schema)) = database().await else {
            return;
        };
        let bundle = bundle();
        run(&mut conn, core(true), Some(&bundle)).await.unwrap();
        run(&mut conn, core(true), Some(&bundle)).await.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM extension_data")
                .fetch_one(&mut conn)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT version FROM _sqlx_migrations")
                .fetch_one(&mut conn)
                .await
                .unwrap(),
            61
        );
        cleanup(conn, schema).await;
    }

    #[tokio::test]
    async fn adoption_preserves_data_and_time_then_upstream_reuses_version() {
        let Some((mut conn, schema)) = database().await else {
            return;
        };
        let bundle = bundle();
        legacy(&mut conn, &bundle).await;
        run(&mut conn, core(true), Some(&bundle)).await.unwrap();
        // The extension SQL is deliberately non-idempotent: replay would fail.
        run(&mut conn, core(true), Some(&bundle)).await.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i32>("SELECT id FROM extension_data")
                .fetch_one(&mut conn)
                .await
                .unwrap(),
            42
        );
        assert!(
            sqlx::query_scalar::<_, bool>(
                "SELECT installed_on='2026-01-01Z'::timestamptz FROM schema_migrations_extensions"
            )
            .fetch_one(&mut conn)
            .await
            .unwrap()
        );
        cleanup(conn, schema).await;
    }

    #[tokio::test]
    async fn changed_or_missing_extensions_fail_before_upstream_ddl() {
        let Some((mut conn, schema)) = database().await else {
            return;
        };
        let mut bundle = bundle();
        run(&mut conn, core(false), Some(&bundle)).await.unwrap();
        assert!(run(&mut conn, core(true), None).await.is_err());
        bundle.migrations[0].sql.push_str(" SELECT 1;");
        assert!(run(&mut conn, core(true), Some(&bundle)).await.is_err());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT max(version) FROM _sqlx_migrations")
                .fetch_one(&mut conn)
                .await
                .unwrap(),
            1
        );
        cleanup(conn, schema).await;
    }

    #[tokio::test]
    async fn corrupt_legacy_checksum_is_not_adopted() {
        let Some((mut conn, schema)) = database().await else {
            return;
        };
        let bundle = bundle();
        legacy(&mut conn, &bundle).await;
        sqlx::query("UPDATE _sqlx_migrations SET checksum='bad'::bytea")
            .execute(&mut conn)
            .await
            .unwrap();
        assert!(run(&mut conn, core(true), Some(&bundle)).await.is_err());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM _sqlx_migrations WHERE version=61")
                .fetch_one(&mut conn)
                .await
                .unwrap(),
            1
        );
        cleanup(conn, schema).await;
    }

    #[tokio::test]
    async fn failed_extension_rolls_back_ddl_and_can_retry() {
        let Some((mut conn, schema)) = database().await else {
            return;
        };
        let mut bundle = bundle();
        bundle.migrations[0].sql.push_str(" SELECT 1/0;");
        assert!(run(&mut conn, core(false), Some(&bundle)).await.is_err());
        assert!(
            sqlx::query_scalar::<_, bool>("SELECT to_regclass('extension_data') IS NULL")
                .fetch_one(&mut conn)
                .await
                .unwrap()
        );
        run(&mut conn, core(false), Some(&self::bundle()))
            .await
            .unwrap();
        cleanup(conn, schema).await;
    }
    // Optional operator bundle check against the complete upstream schema.
    // Uses a new database: never applies migrations to the supplied admin DB.
    #[tokio::test]
    async fn operator_bundle_fresh_install_and_legacy_upgrade() {
        let Ok(path) = std::env::var("SESSION_EXTENSION_TEST_BUNDLE") else {
            return;
        };
        let bundle = Bundle::parse(&std::fs::read_to_string(path).unwrap()).unwrap();
        let url = std::env::var("SESSION_EXTENSION_TEST_DATABASE_URL").unwrap();
        let mut admin = PgConnection::connect(&url).await.unwrap();
        let name = format!("extension_bundle_{}", uuid::Uuid::new_v4().simple());
        sqlx::raw_sql(&format!("CREATE DATABASE {name}"))
            .execute(&mut admin)
            .await
            .unwrap();
        let options: sqlx::postgres::PgConnectOptions = url.parse().unwrap();
        let mut conn = PgConnection::connect_with(&options.database(&name))
            .await
            .unwrap();
        let migrations = crate::migration_list(crate::TextSearchBackend::Postgres);
        let make_core = || Migrator {
            migrations: Cow::Owned(migrations.clone()),
            ..Migrator::DEFAULT
        };
        run(&mut conn, make_core(), Some(&bundle)).await.unwrap();
        // Simulate the previously deployed binary's ledger, without changing DDL.
        for entry in &bundle.migrations {
            if let Some(old) = entry.legacy_version {
                sqlx::query("INSERT INTO _sqlx_migrations(version,description,checksum,success,execution_time,installed_on) SELECT $1,description,checksum,true,0,installed_on FROM schema_migrations_extensions WHERE namespace=$2 AND version=$3")
                    .bind(old).bind(&bundle.namespace).bind(entry.version).execute(&mut conn).await.unwrap();
            }
        }
        sqlx::query("DELETE FROM schema_migrations_extensions")
            .execute(&mut conn)
            .await
            .unwrap();
        run(&mut conn, make_core(), Some(&bundle)).await.unwrap();
        run(&mut conn, make_core(), Some(&bundle)).await.unwrap();
        conn.close().await.unwrap();
        sqlx::raw_sql(&format!("DROP DATABASE {name}"))
            .execute(&mut admin)
            .await
            .unwrap();
    }
    #[tokio::test]
    async fn concurrent_replicas_apply_extensions_once() {
        let Some((mut first, schema)) = database().await else {
            return;
        };
        let url = std::env::var("SESSION_EXTENSION_TEST_DATABASE_URL")
            .or_else(|_| std::env::var("SESSION_SQLX_TEST_DATABASE_URL"))
            .or_else(|_| std::env::var("SESSION_RUNTIME_TEST_DATABASE_URL"))
            .unwrap();
        let mut second = PgConnection::connect(&url).await.unwrap();
        sqlx::raw_sql(&format!("SET search_path TO {schema}"))
            .execute(&mut second)
            .await
            .unwrap();
        let mut bundle = bundle();
        bundle.migrations[0].sql.push_str(" SELECT pg_sleep(0.05);");
        let (a, b) = tokio::join!(
            run(&mut first, core(true), Some(&bundle)),
            run(&mut second, core(true), Some(&bundle)),
        );
        a.unwrap();
        b.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM extension_data")
                .fetch_one(&mut first)
                .await
                .unwrap(),
            1
        );
        second.close().await.unwrap();
        cleanup(first, schema).await;
    }

    #[tokio::test]
    async fn failed_upstream_after_adoption_resumes_without_replaying_extension() {
        let Some((mut conn, schema)) = database().await else {
            return;
        };
        let bundle = bundle();
        legacy(&mut conn, &bundle).await;
        let broken = Migrator {
            migrations: Cow::Owned(vec![Migration::new(
                1,
                Cow::Borrowed("broken"),
                MigrationType::Simple,
                Cow::Borrowed("SELECT 1/0;"),
                false,
            )]),
            ..Migrator::DEFAULT
        };
        assert!(run(&mut conn, broken, Some(&bundle)).await.is_err());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM schema_migrations_extensions")
                .fetch_one(&mut conn)
                .await
                .unwrap(),
            1
        );
        run(&mut conn, core(true), Some(&bundle)).await.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i32>("SELECT id FROM extension_data")
                .fetch_one(&mut conn)
                .await
                .unwrap(),
            42
        );
        cleanup(conn, schema).await;
    }
}
