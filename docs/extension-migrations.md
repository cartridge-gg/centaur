# Deployment extension migrations

`SESSION_EXTENSION_MIGRATIONS` accepts a JSON bundle of trusted operator SQL:

```json
{
  "namespace": "example",
  "migrations": [
    {
      "version": 202611110001,
      "description": "example table",
      "sql": "CREATE TABLE example_items (id bigint PRIMARY KEY);"
    }
  ]
}
```

Versions must be positive, unique and increasing. SQL is executed on the session
store connection after upstream migrations, before API startup completes.
`schema_migrations_extensions` independently records namespace, version,
description, SHA-384 checksum and installation time. Upstream continues to use
`_sqlx_migrations` unchanged. Once installed, extension entries must remain in the
bundle with identical descriptions and SQL bytes; omission or modification
fails startup before upstream DDL. Omitting the entire bundle also fails if the
database already has extensions. This interface currently accepts one namespace
per database.

SQLx's PostgreSQL advisory lock covers validation, adoption, upstream migrations
and extension application. Each new extension's SQL and ledger entry commit in
one transaction. Do not include transaction control or operations that cannot run
inside a transaction. The SQL is privileged deployment configuration and must
never come from an untrusted request.

An entry may specify `legacy_version` to adopt a former upstream-ledger entry.
Adoption requires its successful status, exact description and exact SHA-384
checksum of the supplied SQL, and refuses to remove a migration still embedded
in the new binary. All ledger transfers are transactional, preserve installation
times and do not execute SQL. A future upstream migration at the same version
is distinguished by its embedded checksum and left alone. Unknown legacy records
fail closed. Keep adoption metadata for older installations and retries.

Stop every old API binary and take a database backup before a transition that
removes embedded migrations. The advisory lock serializes migration runs but
cannot stop an old binary from trying to restore its migrations later. Do not
roll back only the image after adoption; use a compatible binary or coordinated
database recovery. Adoption and completed upstream migrations remain committed
if a later migration fails; startup retries pending work safely.

For database regression tests, set `SESSION_SQLX_TEST_DATABASE_URL` or
`SESSION_EXTENSION_TEST_DATABASE_URL` to an isolated PostgreSQL instance and run
`cargo test -p centaur-session-sqlx`. To test a deployment's actual JSON bundle
against the full upstream schema, also set `SESSION_EXTENSION_TEST_BUNDLE` to
its file path and `SESSION_EXTENSION_TEST_DATABASE_URL` to a PostgreSQL admin
connection with permission to create databases. That test creates and drops a
unique database; it does not migrate the supplied admin database.
