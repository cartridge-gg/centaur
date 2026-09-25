//! Asks each model provider whether it has capacity, so that
//! `provider_health` is right for every client: sessions, sandbox processes
//! that call a provider themselves, and new sessions.
//!
//! A health URL answers `GET` with `{"exhausted": bool, "reset_at": <Unix
//! seconds, optional>}`. An exhausted provider is marked until `reset_at`
//! (15 minutes when it is unknown, refreshed on each check); a provider with
//! capacity again is cleared at once, before its recorded reset. Errors
//! change nothing.

use std::collections::HashSet;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use centaur_session_core::HarnessType;
use centaur_session_sqlx::PgSessionStore;
use centaur_telemetry::{init_provider_health_probe, record_provider_health_probe};
use serde::Deserialize;
use tokio::time::{MissedTickBehavior, interval};
use tracing::{debug, info, warn};

use crate::PROVIDER_EXHAUSTED_DEFAULT_COOLDOWN;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// The `result` label of each check.
const HEALTHY: &str = "healthy";
const EXHAUSTED: &str = "exhausted";
const ERROR: &str = "error";

#[derive(Clone, Debug)]
pub struct ProviderHealthProbeConfig {
    pub interval: Duration,
    /// The health URL of each harness's provider.
    pub endpoints: Vec<(HarnessType, String)>,
    /// Sent as `Authorization: Bearer <token>`.
    pub bearer_token: Option<String>,
}

/// One answer of a health URL.
#[derive(Debug, Deserialize, PartialEq, Eq)]
pub(crate) struct HealthReport {
    pub exhausted: bool,
    #[serde(default)]
    pub reset_at: Option<i64>,
}

/// Creates the check metric of each checked harness at 0 for every result, so
/// that `increase()` sees the first exhausted or failed check after a start.
fn init_metrics(endpoints: &[(HarnessType, String)]) {
    for (harness, _) in endpoints {
        for result in [HEALTHY, EXHAUSTED, ERROR] {
            init_provider_health_probe(harness.as_ref(), result);
        }
    }
}

pub(crate) struct ProviderHealthProbe {
    store: PgSessionStore,
    client: reqwest::Client,
    config: ProviderHealthProbeConfig,
    /// Harnesses whose last check failed: a failure is logged once, when it
    /// starts, and counted in the metric every time.
    failing: Mutex<HashSet<HarnessType>>,
}

impl ProviderHealthProbe {
    pub(crate) fn new(store: PgSessionStore, config: ProviderHealthProbeConfig) -> Self {
        init_metrics(&config.endpoints);
        Self {
            store,
            client: reqwest::Client::new(),
            config,
            failing: Mutex::new(HashSet::new()),
        }
    }

    pub(crate) fn spawn(self) {
        tokio::spawn(async move {
            let mut tick = interval(self.config.interval);
            tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                self.check_all().await;
            }
        });
    }

    pub(crate) async fn check_all(&self) {
        for (harness, url) in &self.config.endpoints {
            let result = match self.check(url).await {
                Ok(report) => self.apply(harness, &report).await,
                Err(error) => Err(error),
            };
            let label = match &result {
                Ok(true) => EXHAUSTED,
                Ok(false) => HEALTHY,
                Err(_) => ERROR,
            };
            record_provider_health_probe(harness.as_ref(), label);
            let mut failing = self
                .failing
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            match result {
                Err(error) if failing.insert(harness.clone()) => warn!(
                    component = crate::COMPONENT_SESSION_RUNTIME,
                    event = "provider_health_probe_failed",
                    harness = %harness,
                    %error,
                    "failed to check model provider health; later failures are only counted"
                ),
                Err(error) => {
                    debug!(harness = %harness, %error, "provider health check failed again")
                }
                Ok(_) if failing.remove(harness) => info!(
                    component = crate::COMPONENT_SESSION_RUNTIME,
                    event = "provider_health_probe_recovered",
                    harness = %harness,
                    "model provider health checks work again"
                ),
                Ok(_) => {}
            }
        }
    }

    async fn check(&self, url: &str) -> Result<HealthReport, String> {
        let mut request = self.client.get(url).timeout(REQUEST_TIMEOUT);
        if let Some(token) = &self.config.bearer_token {
            request = request.bearer_auth(token);
        }
        let response = request.send().await.map_err(|error| error.to_string())?;
        let status = response.status();
        if !status.is_success() {
            return Err(format!("health URL answered {status}"));
        }
        response
            .json::<HealthReport>()
            .await
            .map_err(|error| format!("invalid health report: {error}"))
    }

    /// Records the report. Returns whether the provider is exhausted.
    async fn apply(&self, harness: &HarnessType, report: &HealthReport) -> Result<bool, String> {
        if report.exhausted {
            let until = report_until(report.reset_at, SystemTime::now());
            self.store
                .mark_provider_exhausted(harness, until, "provider health check")
                .await
                .map_err(|error| error.to_string())?;
            return Ok(true);
        }
        let cleared = self
            .store
            .clear_provider_exhausted(harness, "provider health check: capacity again")
            .await
            .map_err(|error| error.to_string())?;
        if cleared {
            info!(
                component = crate::COMPONENT_SESSION_RUNTIME,
                event = "provider_health_recovered",
                harness = %harness,
                "model provider has capacity again"
            );
        }
        Ok(false)
    }
}

/// Until when an exhausted provider is marked: its reset time if that is in
/// the future, else the default cooldown.
fn report_until(reset_at: Option<i64>, now: SystemTime) -> SystemTime {
    reset_at
        .and_then(|reset| u64::try_from(reset).ok())
        .map(|reset| UNIX_EPOCH + Duration::from_secs(reset))
        .filter(|reset| *reset > now)
        .unwrap_or(now + PROVIDER_EXHAUSTED_DEFAULT_COOLDOWN)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_need_the_exhausted_flag() {
        let report: HealthReport =
            serde_json::from_str(r#"{"exhausted": true, "reset_at": 1790220376, "accounts": 3}"#)
                .unwrap();
        assert_eq!(
            report,
            HealthReport {
                exhausted: true,
                reset_at: Some(1_790_220_376)
            }
        );
        let report: HealthReport =
            serde_json::from_str(r#"{"exhausted": false, "reset_at": null}"#).unwrap();
        assert!(!report.exhausted);
        assert!(serde_json::from_str::<HealthReport>(r#"{"reset_at": 1}"#).is_err());
    }

    #[test]
    fn an_unknown_or_past_reset_uses_the_cooldown() {
        let now = UNIX_EPOCH + Duration::from_secs(1_790_000_000);
        assert_eq!(
            report_until(Some(1_790_000_600), now),
            UNIX_EPOCH + Duration::from_secs(1_790_000_600)
        );
        for reset in [None, Some(1_789_999_000), Some(-5)] {
            assert_eq!(
                report_until(reset, now),
                now + PROVIDER_EXHAUSTED_DEFAULT_COOLDOWN
            );
        }
    }

    #[test]
    fn check_metrics_start_at_zero_for_each_checked_harness() {
        centaur_telemetry::prometheus_handle().unwrap();
        init_metrics(&[(
            HarnessType::ClaudeCode,
            "http://pool.test/claude".to_owned(),
        )]);
        let metrics = centaur_telemetry::render_metrics().unwrap();
        for result in [HEALTHY, EXHAUSTED, ERROR] {
            let series = format!(
                r#"centaur_provider_health_probes_total{{harness="claudecode",result="{result}"}} "#
            );
            assert!(metrics.contains(&series), "{series}");
        }
    }
}
