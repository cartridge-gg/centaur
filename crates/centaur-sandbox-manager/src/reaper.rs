//! Background garbage collection for leaked sandboxes.
//!
//! Sessions pause idle sandboxes (replicas to zero), but paused sandboxes and
//! sandboxes whose sessions never go idle still need a restart-surviving
//! backstop. The reaper sweeps the backend's observed sandboxes and stops any
//! that exceed the configured max lifetime, releasing the sandbox, its proxy
//! resources, and its node pod slots. That includes sandboxes whose agent
//! exited (`Stopped`): sessions replace rather than reuse them, so nothing
//! else ever releases their backend resources. Each sweep also deletes iron-proxy
//! resources whose sandbox no longer has a live Sandbox, the orphan class no
//! observed-sandbox path can reach. A live sandbox whose session was active
//! within the activity grace is kept past the max lifetime.

use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, SystemTime},
};

use centaur_sandbox_core::{ObservedSandbox, SandboxResult, SandboxStatus};
use centaur_session_sqlx::{PgSessionStore, SessionStoreError};
use tokio::time::{MissedTickBehavior, interval};
use tracing::{info, warn};

use crate::SandboxManager;

#[derive(Clone, Copy, Debug)]
pub struct SandboxReaperConfig {
    /// How often to sweep.
    pub interval: Duration,
    /// Minimum age of an iron-proxy resource whose Sandbox no longer exists
    /// before the orphan sweep may delete it.
    pub orphan_sweep_grace: Duration,
    /// Stop any sandbox older than this regardless of status. `None` disables
    /// the max-lifetime sweep.
    pub max_lifetime: Option<Duration>,
    /// How far past `max_lifetime` a session's retention (`keep-until` /
    /// `keep-hard-deadline` on the observed sandbox) may defer the stop.
    /// `None` ignores retention entirely.
    pub keepalive_max: Option<Duration>,
    /// Keep a sandbox past `max_lifetime` while its session was active within
    /// this window (a turn ended, started, or is still queued or running).
    /// `None` stops on age alone.
    pub activity_grace: Option<Duration>,
}

impl SandboxReaperConfig {
    /// The orphan sweep runs whenever a sweep interval is configured, so the
    /// reaper is enabled even when the max-lifetime sweep is disabled.
    pub fn is_enabled(&self) -> bool {
        self.interval > Duration::ZERO || self.max_lifetime.is_some()
    }
}

pub struct SandboxReaper {
    manager: Arc<SandboxManager>,
    config: SandboxReaperConfig,
    activity_store: Option<PgSessionStore>,
}

impl SandboxReaper {
    pub fn new(manager: Arc<SandboxManager>, config: SandboxReaperConfig) -> Self {
        Self {
            manager,
            config,
            activity_store: None,
        }
    }

    /// Read session activity from `store` so `activity_grace` can keep an
    /// in-use sandbox past the max lifetime.
    pub fn with_activity_store(mut self, store: PgSessionStore) -> Self {
        self.activity_store = Some(store);
        self
    }

    pub fn spawn(self) {
        tokio::spawn(async move {
            // Orphans left by a dead control plane are reached sooner when
            // the first sweep runs at startup rather than after the interval.
            if let Err(error) = self.reap_once().await {
                warn!(%error, "initial sandbox reaper sweep failed");
            }
            let mut tick = interval(self.config.interval);
            tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                if let Err(error) = self.reap_once().await {
                    warn!(%error, "sandbox reaper sweep failed");
                }
            }
        });
    }

    /// Sweep once and return how many sandboxes were stopped. A failed stop is
    /// logged and skipped so one wedged sandbox cannot stall the sweep.
    pub async fn reap_once(&self) -> SandboxResult<usize> {
        let now = SystemTime::now();
        let observed = self.manager.list_observed().await?;
        let activity = match self.last_activity(&observed, now).await {
            Ok(activity) => activity,
            Err(error) => {
                // Without the activity, a stop could cut off a session in use.
                // Leave the max-lifetime stops to the next sweep.
                warn!(%error, "sandbox reaper could not read session activity");
                self.reap_orphans().await?;
                return Ok(0);
            }
        };
        let mut reaped = 0;
        for observed in observed {
            let last_active = activity.get(observed.id.as_str()).copied();
            let Some(reason) = reap_reason(&observed, now, last_active, &self.config) else {
                if recently_active(now, last_active, &self.config) {
                    info!(
                        sandbox_id = %observed.id.as_str(),
                        last_active = ?last_active,
                        "kept sandbox past max lifetime: session recently active"
                    );
                }
                continue;
            };
            match self.manager.stop(&observed.id).await {
                Ok(()) => {
                    reaped += 1;
                    info!(
                        sandbox_id = %observed.id.as_str(),
                        reason,
                        "reaped expired sandbox"
                    );
                }
                Err(error) => {
                    warn!(
                        sandbox_id = %observed.id.as_str(),
                        reason,
                        %error,
                        "failed to reap expired sandbox"
                    );
                }
            }
        }
        self.reap_orphans().await?;
        Ok(reaped)
    }

    async fn reap_orphans(&self) -> SandboxResult<()> {
        let orphaned = self
            .manager
            .reap_orphan_iron_proxy_resources(self.config.orphan_sweep_grace)
            .await?;
        if !orphaned.is_empty() {
            info!(?orphaned, "reaped orphaned iron-proxy resources");
        }
        Ok(())
    }

    /// Session activity of the sandboxes past their max lifetime. Empty when
    /// no activity grace or store is configured, or no sandbox is due.
    async fn last_activity(
        &self,
        observed: &[ObservedSandbox],
        now: SystemTime,
    ) -> Result<BTreeMap<String, SystemTime>, SessionStoreError> {
        let (Some(store), Some(_)) = (&self.activity_store, self.config.activity_grace) else {
            return Ok(BTreeMap::new());
        };
        let due: Vec<String> = observed
            .iter()
            .filter(|observed| past_max_lifetime(observed, now, &self.config))
            .map(|observed| observed.id.as_str().to_owned())
            .collect();
        if due.is_empty() {
            return Ok(BTreeMap::new());
        }
        Ok(store
            .sandbox_last_activity(&due)
            .await?
            .into_iter()
            .map(|(sandbox_id, last_active)| (sandbox_id, last_active.into()))
            .collect())
    }
}

fn past_max_lifetime(
    observed: &ObservedSandbox,
    now: SystemTime,
    config: &SandboxReaperConfig,
) -> bool {
    if observed.status == SandboxStatus::Gone {
        return false;
    }
    let (Some(max_lifetime), Some(created_at)) = (config.max_lifetime, observed.created_at) else {
        return false;
    };
    now.duration_since(created_at)
        .is_ok_and(|age| age >= max_lifetime)
}

/// Whether the session's last activity falls inside the activity grace. An
/// activity time ahead of `now` (database clock skew) counts as active.
fn recently_active(
    now: SystemTime,
    last_active: Option<SystemTime>,
    config: &SandboxReaperConfig,
) -> bool {
    let (Some(grace), Some(last_active)) = (config.activity_grace, last_active) else {
        return false;
    };
    !now.duration_since(last_active)
        .is_ok_and(|quiet| quiet >= grace)
}

fn reap_reason(
    observed: &ObservedSandbox,
    now: SystemTime,
    last_active: Option<SystemTime>,
    config: &SandboxReaperConfig,
) -> Option<&'static str> {
    if !past_max_lifetime(observed, now, config) {
        return None;
    }
    // Past the ordinary lifetime, but the session is still in use: a stop now
    // would replace the sandbox in the middle of a conversation. The sandbox
    // goes once the session has been quiet for the whole grace. A stopped
    // sandbox is replaced on the next turn anyway, so activity cannot keep it.
    if observed.status != SandboxStatus::Stopped && recently_active(now, last_active, config) {
        return None;
    }
    let (Some(max_lifetime), Some(created_at)) = (config.max_lifetime, observed.created_at) else {
        return None;
    };
    // Past the ordinary lifetime. A session may have asked to keep this
    // sandbox (it owns work that is still open, e.g. a pull request); honour
    // that until the earliest of its keep-until, its hard deadline, and the
    // reaper's own cap past the max lifetime, so a stale or forged annotation
    // can never keep a sandbox forever.
    if let (Some(keep_until), Some(keepalive_max)) = (observed.keep_until, config.keepalive_max) {
        let hard_deadline = observed.keep_hard_deadline.unwrap_or(keep_until);
        let ceiling = created_at + max_lifetime + keepalive_max;
        let deadline = keep_until.min(hard_deadline).min(ceiling);
        if now < deadline {
            return None;
        }
        if now >= hard_deadline || now >= ceiling {
            return Some("retention_expired");
        }
    }
    Some("max_lifetime")
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, sync::Mutex, time::UNIX_EPOCH};

    use async_trait::async_trait;
    use centaur_sandbox_core::{
        SandboxBackend, SandboxError, SandboxHandle, SandboxId, SandboxIo, SandboxSpec,
        SandboxStatus,
    };

    use super::*;

    fn config(max_lifetime: Option<Duration>) -> SandboxReaperConfig {
        SandboxReaperConfig {
            interval: Duration::from_secs(60),
            orphan_sweep_grace: Duration::from_secs(600),
            max_lifetime,
            keepalive_max: Some(Duration::from_secs(7 * 86_400)),
            activity_grace: None,
        }
    }

    fn retained(
        status: centaur_sandbox_core::SandboxStatus,
        now: SystemTime,
        age_secs: u64,
        keep_until_in: i64,
        hard_in: Option<i64>,
    ) -> ObservedSandbox {
        let at = |secs: i64| {
            if secs >= 0 {
                now + Duration::from_secs(secs as u64)
            } else {
                now - Duration::from_secs(secs.unsigned_abs())
            }
        };
        observed(status)
            .with_created_at(Some(now - Duration::from_secs(age_secs)))
            .with_keep_until(Some(at(keep_until_in)))
            .with_keep_hard_deadline(hard_in.map(at))
    }

    #[test]
    fn retention_defers_the_max_lifetime_stop_while_the_lease_is_open() {
        let now = SystemTime::now();
        let day = 86_400;
        let config = config(Some(Duration::from_secs(day)));
        // Two days old, kept until tomorrow, hard deadline in five days.
        let sandbox = retained(
            centaur_sandbox_core::SandboxStatus::Suspended,
            now,
            2 * day,
            day as i64,
            Some(5 * day as i64),
        );
        assert_eq!(reap_reason(&sandbox, now, None, &config), None);
        // Running sandboxes are retained the same way.
        let sandbox = retained(
            centaur_sandbox_core::SandboxStatus::Running,
            now,
            2 * day,
            day as i64,
            Some(5 * day as i64),
        );
        assert_eq!(reap_reason(&sandbox, now, None, &config), None);
    }

    #[test]
    fn retention_ends_when_the_lease_the_hard_deadline_or_the_cap_passes() {
        let now = SystemTime::now();
        let day = 86_400;
        let config = config(Some(Duration::from_secs(day)));
        // Lease ended an hour ago: ordinary max-lifetime stop.
        let sandbox = retained(
            centaur_sandbox_core::SandboxStatus::Suspended,
            now,
            2 * day,
            -3_600,
            Some(5 * day as i64),
        );
        assert_eq!(
            reap_reason(&sandbox, now, None, &config),
            Some("max_lifetime")
        );
        // Lease still claims tomorrow but the hard deadline passed.
        let sandbox = retained(
            centaur_sandbox_core::SandboxStatus::Suspended,
            now,
            2 * day,
            day as i64,
            Some(-60),
        );
        assert_eq!(
            reap_reason(&sandbox, now, None, &config),
            Some("retention_expired")
        );
        // Ten days old with a lease and a hard deadline far away: the reaper's
        // cap (max lifetime + keepalive_max = 8 days) wins.
        let sandbox = retained(
            centaur_sandbox_core::SandboxStatus::Suspended,
            now,
            10 * day,
            day as i64,
            Some(30 * day as i64),
        );
        assert_eq!(
            reap_reason(&sandbox, now, None, &config),
            Some("retention_expired")
        );
        // No hard deadline recorded: keep-until alone bounds it.
        let sandbox = retained(
            centaur_sandbox_core::SandboxStatus::Suspended,
            now,
            2 * day,
            3_600,
            None,
        );
        assert_eq!(reap_reason(&sandbox, now, None, &config), None);
    }

    #[test]
    fn retention_is_ignored_when_the_reaper_has_no_keepalive_cap() {
        let now = SystemTime::now();
        let day = 86_400;
        let mut config = config(Some(Duration::from_secs(day)));
        config.keepalive_max = None;
        let sandbox = retained(
            centaur_sandbox_core::SandboxStatus::Suspended,
            now,
            2 * day,
            day as i64,
            Some(5 * day as i64),
        );
        assert_eq!(
            reap_reason(&sandbox, now, None, &config),
            Some("max_lifetime")
        );
    }

    #[test]
    fn recent_activity_keeps_a_sandbox_past_max_lifetime() {
        let now = SystemTime::now();
        let hour = 3_600;
        let mut config = config(Some(Duration::from_secs(24 * hour)));
        config.activity_grace = Some(Duration::from_secs(3 * hour));
        let sandbox = observed(centaur_sandbox_core::SandboxStatus::Running)
            .with_created_at(Some(now - Duration::from_secs(30 * hour)));
        let active = |secs_ago: u64| Some(now - Duration::from_secs(secs_ago));

        // A turn an hour ago keeps it.
        assert_eq!(reap_reason(&sandbox, now, active(hour), &config), None);
        // Quiet for the whole grace: the ordinary stop.
        assert_eq!(
            reap_reason(&sandbox, now, active(3 * hour), &config),
            Some("max_lifetime")
        );
        // No session activity known: the ordinary stop.
        assert_eq!(
            reap_reason(&sandbox, now, None, &config),
            Some("max_lifetime")
        );
        // Activity stamped ahead of the reaper's clock counts as active.
        assert_eq!(
            reap_reason(&sandbox, now, Some(now + Duration::from_secs(5)), &config),
            None
        );
        // Paused sandboxes are kept the same way.
        let paused = observed(centaur_sandbox_core::SandboxStatus::Suspended)
            .with_created_at(Some(now - Duration::from_secs(30 * hour)));
        assert_eq!(reap_reason(&paused, now, active(hour), &config), None);
    }

    #[test]
    fn activity_is_ignored_without_a_grace() {
        let now = SystemTime::now();
        let sandbox = observed(centaur_sandbox_core::SandboxStatus::Running)
            .with_created_at(Some(now - Duration::from_secs(100_000)));
        let config = config(Some(Duration::from_secs(86_400)));

        assert_eq!(
            reap_reason(&sandbox, now, Some(now), &config),
            Some("max_lifetime")
        );
    }

    #[test]
    fn activity_keeps_a_sandbox_whose_lease_ran_out() {
        let now = SystemTime::now();
        let day = 86_400;
        let mut config = config(Some(Duration::from_secs(day)));
        config.activity_grace = Some(Duration::from_secs(3 * 3_600));
        // Ten days old: past the keepalive cap, but in use.
        let sandbox = retained(
            centaur_sandbox_core::SandboxStatus::Running,
            now,
            10 * day,
            -3_600,
            Some(-60),
        );
        assert_eq!(reap_reason(&sandbox, now, Some(now), &config), None);
        assert_eq!(
            reap_reason(&sandbox, now, None, &config),
            Some("retention_expired")
        );
    }

    fn observed(status: centaur_sandbox_core::SandboxStatus) -> ObservedSandbox {
        ObservedSandbox::new("sandbox-1", "fake", status)
    }

    #[test]
    fn reaps_running_sandbox_past_max_lifetime() {
        let now = SystemTime::now();
        let sandbox = observed(centaur_sandbox_core::SandboxStatus::Running)
            .with_created_at(Some(now - Duration::from_secs(100_000)));

        let reason = reap_reason(
            &sandbox,
            now,
            None,
            &config(Some(Duration::from_secs(86_400))),
        );

        assert_eq!(reason, Some("max_lifetime"));
    }

    #[test]
    fn reaps_suspended_sandbox_past_max_lifetime() {
        let now = SystemTime::now();
        let sandbox = observed(centaur_sandbox_core::SandboxStatus::Suspended)
            .with_created_at(Some(now - Duration::from_secs(100_000)))
            .with_suspended_since(Some(now - Duration::from_secs(60)));

        let reason = reap_reason(
            &sandbox,
            now,
            None,
            &config(Some(Duration::from_secs(86_400))),
        );

        assert_eq!(reason, Some("max_lifetime"));
    }

    #[test]
    fn keeps_running_sandbox_within_max_lifetime() {
        let now = SystemTime::now();
        let sandbox = observed(centaur_sandbox_core::SandboxStatus::Running)
            .with_created_at(Some(now - Duration::from_secs(60)));

        let reason = reap_reason(
            &sandbox,
            now,
            None,
            &config(Some(Duration::from_secs(86_400))),
        );

        assert_eq!(reason, None);
    }

    #[test]
    fn reaps_stopped_sandbox_past_max_lifetime() {
        let now = SystemTime::now();
        let sandbox = observed(centaur_sandbox_core::SandboxStatus::Stopped)
            .with_created_at(Some(now - Duration::from_secs(100_000)));

        let config = SandboxReaperConfig {
            activity_grace: Some(Duration::from_secs(3_600)),
            ..config(Some(Duration::from_secs(86_400)))
        };

        // Recent session activity does not keep a sandbox whose agent exited.
        let reason = reap_reason(&sandbox, now, Some(now), &config);

        assert_eq!(reason, Some("max_lifetime"));
    }

    #[test]
    fn ignores_gone_sandboxes() {
        let now = SystemTime::now();
        let sandbox = observed(centaur_sandbox_core::SandboxStatus::Gone)
            .with_created_at(Some(now - Duration::from_secs(100_000)));

        let reason = reap_reason(
            &sandbox,
            now,
            None,
            &config(Some(Duration::from_secs(86_400))),
        );

        assert_eq!(reason, None);
    }

    #[test]
    fn disabled_max_lifetime_reaps_nothing_by_age() {
        let now = SystemTime::now();
        let sandbox = observed(centaur_sandbox_core::SandboxStatus::Suspended)
            .with_created_at(Some(now - Duration::from_secs(100_000)))
            .with_suspended_since(Some(now - Duration::from_secs(100_000)));
        let config = config(None);

        assert_eq!(reap_reason(&sandbox, now, None, &config), None);
    }

    #[test]
    fn orphan_sweep_keeps_the_reaper_enabled_without_max_lifetime() {
        let config = config(None);
        assert!(config.is_enabled());
    }

    #[tokio::test]
    async fn sweep_keeps_old_sandboxes_whose_session_is_active() {
        let Some(store) = test_store().await else {
            return;
        };
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        let active = format!("reaper-active-{suffix}");
        let quiet = format!("reaper-quiet-{suffix}");
        let unowned = format!("reaper-unowned-{suffix}");
        for (sandbox_id, quiet_for) in [(&active, "1 hour"), (&quiet, "4 hours")] {
            sqlx::query(
                "insert into sessions \
                 (thread_key, harness_type, status, sandbox_id, sandbox_last_active_at) \
                 values ('test:' || $1, 'codex', 'idle', $1, now() - $2::interval)",
            )
            .bind(sandbox_id)
            .bind(quiet_for)
            .execute(store.pool())
            .await
            .expect("insert session row");
        }
        let hour = 3_600;
        let created_at = SystemTime::now() - Duration::from_secs(30 * hour);
        let backend = Arc::new(AgedBackend::new(
            [active.as_str(), quiet.as_str(), unowned.as_str()],
            created_at,
        ));
        let mut config = config(Some(Duration::from_secs(24 * hour)));
        config.activity_grace = Some(Duration::from_secs(3 * hour));
        let reaper = SandboxReaper::new(Arc::new(SandboxManager::new(backend.clone())), config)
            .with_activity_store(store);

        let reaped = reaper.reap_once().await.expect("sweep");

        // Quiet for 4 h and no session at all: stopped. Active an hour ago: kept.
        assert_eq!(reaped, 2);
        assert_eq!(backend.stopped(), BTreeSet::from([quiet, unowned]));
    }

    async fn test_store() -> Option<PgSessionStore> {
        let Ok(url) = std::env::var("SESSION_RUNTIME_TEST_DATABASE_URL") else {
            eprintln!("skipping: SESSION_RUNTIME_TEST_DATABASE_URL not set");
            return None;
        };
        let store = PgSessionStore::connect(&url)
            .await
            .expect("connect test db");
        store
            .run_migrations(centaur_session_sqlx::TextSearchBackend::Postgres)
            .await
            .expect("run migrations");
        Some(store)
    }

    /// Running sandboxes that all share one creation time.
    struct AgedBackend {
        created_at: SystemTime,
        running: Mutex<BTreeSet<String>>,
        stopped: Mutex<BTreeSet<String>>,
    }

    impl AgedBackend {
        fn new<'a>(ids: impl IntoIterator<Item = &'a str>, created_at: SystemTime) -> Self {
            Self {
                created_at,
                running: Mutex::new(ids.into_iter().map(str::to_owned).collect()),
                stopped: Mutex::default(),
            }
        }

        fn stopped(&self) -> BTreeSet<String> {
            self.stopped.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl SandboxBackend for AgedBackend {
        fn name(&self) -> &'static str {
            "aged"
        }

        async fn create(&self, _spec: SandboxSpec) -> SandboxResult<SandboxHandle> {
            unreachable!("the reaper never creates sandboxes")
        }

        async fn open_io(&self, _id: &SandboxId) -> SandboxResult<SandboxIo> {
            unreachable!("the reaper never opens I/O")
        }

        async fn status(&self, id: &SandboxId) -> SandboxResult<SandboxStatus> {
            if self.running.lock().unwrap().contains(id.as_str()) {
                Ok(SandboxStatus::Running)
            } else {
                Err(SandboxError::NotFound(id.as_str().to_owned()))
            }
        }

        async fn observe(&self, id: &SandboxId) -> SandboxResult<ObservedSandbox> {
            Ok(
                ObservedSandbox::new(id.clone(), self.name(), self.status(id).await?)
                    .with_created_at(Some(self.created_at)),
            )
        }

        async fn list_observed(&self) -> SandboxResult<Vec<ObservedSandbox>> {
            Ok(self
                .running
                .lock()
                .unwrap()
                .iter()
                .map(|id| {
                    ObservedSandbox::new(id.as_str(), self.name(), SandboxStatus::Running)
                        .with_created_at(Some(self.created_at))
                })
                .collect())
        }

        async fn stop(&self, id: &SandboxId) -> SandboxResult<()> {
            self.running.lock().unwrap().remove(id.as_str());
            self.stopped.lock().unwrap().insert(id.as_str().to_owned());
            Ok(())
        }

        async fn pause(&self, _id: &SandboxId) -> SandboxResult<()> {
            unreachable!("the reaper never pauses sandboxes")
        }

        async fn resume(&self, _id: &SandboxId) -> SandboxResult<()> {
            unreachable!("the reaper never resumes sandboxes")
        }
    }
}
