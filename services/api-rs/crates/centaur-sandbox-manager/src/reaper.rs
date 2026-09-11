//! Background garbage collection for leaked sandboxes.
//!
//! Sessions pause idle sandboxes (replicas to zero), but paused sandboxes and
//! sandboxes whose sessions never go idle still need a restart-surviving
//! backstop. The reaper sweeps the backend's observed sandboxes and stops any
//! that exceed the configured max lifetime, releasing the sandbox, its proxy
//! resources, and its node pod slots.

use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use centaur_sandbox_core::ObservedSandbox;
use centaur_sandbox_core::SandboxResult;
use tokio::time::{MissedTickBehavior, interval};
use tracing::{info, warn};

use crate::SandboxManager;

#[derive(Clone, Copy, Debug)]
pub struct SandboxReaperConfig {
    /// How often to sweep.
    pub interval: Duration,
    /// Stop any sandbox older than this regardless of status. `None` disables
    /// the max-lifetime sweep.
    pub max_lifetime: Option<Duration>,
    /// How far past `max_lifetime` a session's retention (`keep-until` /
    /// `keep-hard-deadline` on the observed sandbox) may defer the stop.
    /// `None` ignores retention entirely.
    pub keepalive_max: Option<Duration>,
}

impl SandboxReaperConfig {
    pub fn is_enabled(&self) -> bool {
        self.max_lifetime.is_some()
    }
}

pub struct SandboxReaper {
    manager: Arc<SandboxManager>,
    config: SandboxReaperConfig,
}

impl SandboxReaper {
    pub fn new(manager: Arc<SandboxManager>, config: SandboxReaperConfig) -> Self {
        Self { manager, config }
    }

    pub fn spawn(self) {
        tokio::spawn(async move {
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
        let mut reaped = 0;
        for observed in self.manager.list_observed().await? {
            let Some(reason) = reap_reason(&observed, now, &self.config) else {
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
        Ok(reaped)
    }
}

fn reap_reason(
    observed: &ObservedSandbox,
    now: SystemTime,
    config: &SandboxReaperConfig,
) -> Option<&'static str> {
    if observed.status.is_terminal() {
        return None;
    }
    let (Some(max_lifetime), Some(created_at)) = (config.max_lifetime, observed.created_at) else {
        return None;
    };
    if !now
        .duration_since(created_at)
        .is_ok_and(|age| age >= max_lifetime)
    {
        return None;
    }
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
    use super::*;

    fn config(max_lifetime: Option<Duration>) -> SandboxReaperConfig {
        SandboxReaperConfig {
            interval: Duration::from_secs(60),
            max_lifetime,
            keepalive_max: Some(Duration::from_secs(7 * 86_400)),
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
        assert_eq!(reap_reason(&sandbox, now, &config), None);
        // Running sandboxes are retained the same way.
        let sandbox = retained(
            centaur_sandbox_core::SandboxStatus::Running,
            now,
            2 * day,
            day as i64,
            Some(5 * day as i64),
        );
        assert_eq!(reap_reason(&sandbox, now, &config), None);
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
        assert_eq!(reap_reason(&sandbox, now, &config), Some("max_lifetime"));
        // Lease still claims tomorrow but the hard deadline passed.
        let sandbox = retained(
            centaur_sandbox_core::SandboxStatus::Suspended,
            now,
            2 * day,
            day as i64,
            Some(-60),
        );
        assert_eq!(
            reap_reason(&sandbox, now, &config),
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
            reap_reason(&sandbox, now, &config),
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
        assert_eq!(reap_reason(&sandbox, now, &config), None);
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
        assert_eq!(reap_reason(&sandbox, now, &config), Some("max_lifetime"));
    }

    fn observed(status: centaur_sandbox_core::SandboxStatus) -> ObservedSandbox {
        ObservedSandbox::new("sandbox-1", "fake", status)
    }

    #[test]
    fn reaps_running_sandbox_past_max_lifetime() {
        let now = SystemTime::now();
        let sandbox = observed(centaur_sandbox_core::SandboxStatus::Running)
            .with_created_at(Some(now - Duration::from_secs(100_000)));

        let reason = reap_reason(&sandbox, now, &config(Some(Duration::from_secs(86_400))));

        assert_eq!(reason, Some("max_lifetime"));
    }

    #[test]
    fn reaps_suspended_sandbox_past_max_lifetime() {
        let now = SystemTime::now();
        let sandbox = observed(centaur_sandbox_core::SandboxStatus::Suspended)
            .with_created_at(Some(now - Duration::from_secs(100_000)))
            .with_suspended_since(Some(now - Duration::from_secs(60)));

        let reason = reap_reason(&sandbox, now, &config(Some(Duration::from_secs(86_400))));

        assert_eq!(reason, Some("max_lifetime"));
    }

    #[test]
    fn keeps_running_sandbox_within_max_lifetime() {
        let now = SystemTime::now();
        let sandbox = observed(centaur_sandbox_core::SandboxStatus::Running)
            .with_created_at(Some(now - Duration::from_secs(60)));

        let reason = reap_reason(&sandbox, now, &config(Some(Duration::from_secs(86_400))));

        assert_eq!(reason, None);
    }

    #[test]
    fn ignores_terminal_sandboxes() {
        let now = SystemTime::now();
        let sandbox = observed(centaur_sandbox_core::SandboxStatus::Gone)
            .with_created_at(Some(now - Duration::from_secs(100_000)));

        let reason = reap_reason(&sandbox, now, &config(Some(Duration::from_secs(86_400))));

        assert_eq!(reason, None);
    }

    #[test]
    fn disabled_config_reaps_nothing() {
        let now = SystemTime::now();
        let sandbox = observed(centaur_sandbox_core::SandboxStatus::Suspended)
            .with_created_at(Some(now - Duration::from_secs(100_000)))
            .with_suspended_since(Some(now - Duration::from_secs(100_000)));
        let config = config(None);

        assert!(!config.is_enabled());
        assert_eq!(reap_reason(&sandbox, now, &config), None);
    }
}
