//! Background garbage collection for leaked sandboxes.
//!
//! Sessions pause idle sandboxes (replicas to zero), but paused sandboxes and
//! sandboxes whose sessions never go idle still need a restart-surviving
//! backstop. The reaper sweeps the backend's observed sandboxes and stops any
//! that exceed the configured max lifetime, or that have sat suspended longer
//! than the configured max idle, releasing the sandbox, its proxy resources,
//! and its node pod slots.

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
    /// Stop a suspended sandbox once it has been suspended this long. Resuming
    /// clears the suspension timestamp, so activity extends the deadline and
    /// this acts as an inactivity TTL. `None` disables the max-idle sweep.
    pub max_idle: Option<Duration>,
}

impl SandboxReaperConfig {
    pub fn is_enabled(&self) -> bool {
        self.max_lifetime.is_some() || self.max_idle.is_some()
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
    if let (Some(max_lifetime), Some(created_at)) = (config.max_lifetime, observed.created_at)
        && now
            .duration_since(created_at)
            .is_ok_and(|age| age >= max_lifetime)
    {
        return Some("max_lifetime");
    }
    if let (Some(max_idle), Some(suspended_since)) = (config.max_idle, observed.suspended_since)
        && observed.status == centaur_sandbox_core::SandboxStatus::Suspended
        && now
            .duration_since(suspended_since)
            .is_ok_and(|idle| idle >= max_idle)
    {
        return Some("max_idle");
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(max_lifetime: Option<Duration>) -> SandboxReaperConfig {
        SandboxReaperConfig {
            interval: Duration::from_secs(60),
            max_lifetime,
            max_idle: None,
        }
    }

    fn idle_config(max_idle: Option<Duration>) -> SandboxReaperConfig {
        SandboxReaperConfig {
            interval: Duration::from_secs(60),
            max_lifetime: None,
            max_idle,
        }
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

    #[test]
    fn reaps_sandbox_suspended_past_max_idle() {
        let now = SystemTime::now();
        let sandbox = observed(centaur_sandbox_core::SandboxStatus::Suspended)
            .with_created_at(Some(now - Duration::from_secs(200_000)))
            .with_suspended_since(Some(now - Duration::from_secs(20_000)));
        let config = idle_config(Some(Duration::from_secs(10_000)));

        assert!(config.is_enabled());
        assert_eq!(reap_reason(&sandbox, now, &config), Some("max_idle"));
    }

    #[test]
    fn keeps_sandbox_suspended_within_max_idle() {
        let now = SystemTime::now();
        let sandbox = observed(centaur_sandbox_core::SandboxStatus::Suspended)
            .with_created_at(Some(now - Duration::from_secs(200_000)))
            .with_suspended_since(Some(now - Duration::from_secs(60)));

        let reason = reap_reason(&sandbox, now, &idle_config(Some(Duration::from_secs(10_000))));

        assert_eq!(reason, None);
    }

    #[test]
    fn max_idle_ignores_running_sandboxes() {
        // A running sandbox is active (or inside its idle-pause window); only a
        // suspension timestamp old enough to exceed max_idle may reap it, and a
        // resume clears that timestamp.
        let now = SystemTime::now();
        let sandbox = observed(centaur_sandbox_core::SandboxStatus::Running)
            .with_created_at(Some(now - Duration::from_secs(200_000)));

        let reason = reap_reason(&sandbox, now, &idle_config(Some(Duration::from_secs(10_000))));

        assert_eq!(reason, None);
    }

    #[test]
    fn max_lifetime_still_reaps_active_sandboxes_as_backstop() {
        let now = SystemTime::now();
        let sandbox = observed(centaur_sandbox_core::SandboxStatus::Running)
            .with_created_at(Some(now - Duration::from_secs(100_000)));
        let config = SandboxReaperConfig {
            interval: Duration::from_secs(60),
            max_lifetime: Some(Duration::from_secs(86_400)),
            max_idle: Some(Duration::from_secs(10_000)),
        };

        assert_eq!(reap_reason(&sandbox, now, &config), Some("max_lifetime"));
    }
}
