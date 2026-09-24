//! Detects that a model provider has no capacity left for this session: a
//! subscription usage limit, or a proxy whose whole account pool is
//! exhausted. Such a failure does not heal by retrying the same provider, so
//! Centaur can continue the session on another harness.
//!
//! Every `error` notification and every failed `turn/completed` gets
//! `params.centaur.providerExhausted` when it matches. Clients that do not
//! know the field ignore it.

use std::sync::LazyLock;

use regex::Regex;
use serde::Serialize;
use serde_json::{Map, Value, json};

/// Comma-separated texts that mark an exhausted provider pool in an upstream
/// error, for example the error code of a subscription proxy.
pub const MARKERS_ENV: &str = "CENTAUR_PROVIDER_EXHAUSTED_MARKERS";

/// Longest `detail` kept in an annotation.
const MAX_DETAIL: usize = 500;

static CLASSIFIER: LazyLock<Classifier> = LazyLock::new(Classifier::from_env);

static RESET_AT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"reset_?at["']?\s*[=:]\s*(\d{9,12})"#).expect("valid regex"));

/// Claude Code's own usage-limit message ends with `|<epoch seconds>`.
static CLAUDE_RESET: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)usage limit reached\|(\d{9,12})").expect("valid regex"));

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Signal {
    /// The harness reported its native usage limit.
    UsageLimit,
    /// The upstream error contains a configured pool-exhaustion marker.
    PoolMarker,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderExhausted {
    /// When the provider accepts requests again, in Unix seconds, if known.
    pub reset_at: Option<i64>,
    pub signal: Signal,
    pub detail: String,
}

#[derive(Debug, Clone, Default)]
pub struct Classifier {
    markers: Vec<String>,
}

impl Classifier {
    pub fn new(markers: impl IntoIterator<Item = String>) -> Self {
        Self {
            markers: markers
                .into_iter()
                .map(|m| m.trim().to_string())
                .filter(|m| !m.is_empty())
                .collect(),
        }
    }

    pub fn from_env() -> Self {
        let raw = std::env::var(MARKERS_ENV).unwrap_or_default();
        Self::new(raw.split(',').map(str::to_string))
    }

    /// Classifies an app-server `error` notification or a failed
    /// `turn/completed` notification.
    pub fn classify_notification(&self, value: &Value) -> Option<ProviderExhausted> {
        let error = match value.get("method").and_then(Value::as_str)? {
            "error" => value.pointer("/params/error")?,
            "turn/completed"
                if value.pointer("/params/turn/status").and_then(Value::as_str)
                    == Some("failed") =>
            {
                value.pointer("/params/turn/error")?
            }
            _ => return None,
        };
        let texts: Vec<&str> = ["message", "additionalDetails"]
            .iter()
            .filter_map(|key| error.get(*key).and_then(Value::as_str))
            .collect();
        self.classify(&texts, error.get("codexErrorInfo"))
    }

    fn classify(
        &self,
        texts: &[&str],
        codex_error_info: Option<&Value>,
    ) -> Option<ProviderExhausted> {
        let native_codex_limit = codex_error_info.is_some_and(|info| {
            info.as_str() == Some("usageLimitExceeded") || info.get("usageLimitExceeded").is_some()
        });
        let native_claude_limit = texts
            .iter()
            .any(|t| t.to_ascii_lowercase().contains("usage limit reached"));
        let marker = texts
            .iter()
            .any(|t| self.markers.iter().any(|m| t.contains(m.as_str())));

        let signal = if marker {
            Signal::PoolMarker
        } else if native_codex_limit || native_claude_limit {
            Signal::UsageLimit
        } else {
            return None;
        };
        let reset_at = texts.iter().find_map(|t| {
            RESET_AT
                .captures(t)
                .or_else(|| CLAUDE_RESET.captures(t))
                .and_then(|c| c[1].parse().ok())
        });
        let detail = texts
            .iter()
            .max_by_key(|t| t.len())
            .map(|t| t.chars().take(MAX_DETAIL).collect())
            .unwrap_or_default();
        Some(ProviderExhausted {
            reset_at,
            signal,
            detail,
        })
    }
}

/// The notification with `params.centaur.providerExhausted` added, when it
/// reports an exhausted provider. `None` leaves the line unchanged.
pub(crate) fn annotate(value: &Value) -> Option<Value> {
    annotate_with(&CLASSIFIER, value)
}

pub(crate) fn annotate_with(classifier: &Classifier, value: &Value) -> Option<Value> {
    let exhausted = classifier.classify_notification(value)?;
    let mut value = value.clone();
    let params = value.get_mut("params")?.as_object_mut()?;
    let centaur = params
        .entry("centaur")
        .or_insert_with(|| Value::Object(Map::new()));
    if let Some(centaur) = centaur.as_object_mut() {
        centaur.insert("providerExhausted".to_string(), json!(exhausted));
    }
    Some(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn classifier() -> Classifier {
        Classifier::new(["pool_exhausted".to_string()])
    }

    fn lines(name: &str) -> Vec<Value> {
        let path = format!(
            "{}/tests/fixtures/failover/{name}",
            env!("CARGO_MANIFEST_DIR")
        );
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    /// Recorded from codex app-server 0.154 against a stub proxy that answers
    /// 503 with a pool-exhaustion marker.
    #[test]
    fn codex_retry_and_final_errors_carry_the_marker() {
        let recorded = lines("codex-app-server.jsonl");
        let classifier = classifier();
        let [retrying, terminal, completed] = recorded.as_slice() else {
            panic!("3 recorded lines");
        };

        // The retry notice says only "Reconnecting...", but its
        // additionalDetails has the upstream error: early detection works.
        let early = classifier.classify_notification(retrying).unwrap();
        assert_eq!(early.signal, Signal::PoolMarker);
        assert_eq!(early.reset_at, Some(1_790_220_376));

        for line in [terminal, completed] {
            let exhausted = classifier.classify_notification(line).unwrap();
            assert_eq!(exhausted.reset_at, Some(1_790_220_376));
            assert!(exhausted.detail.contains("pool_exhausted"));
        }
    }

    #[test]
    fn annotation_keeps_the_line_and_adds_the_field() {
        let recorded = lines("codex-app-server.jsonl");
        let annotated = annotate_with(&classifier(), &recorded[1]).unwrap();
        assert_eq!(annotated["params"]["error"], recorded[1]["params"]["error"]);
        assert_eq!(annotated["params"]["willRetry"], false);
        assert_eq!(
            annotated["params"]["centaur"]["providerExhausted"]["resetAt"],
            1_790_220_376
        );
        assert_eq!(
            annotated["params"]["centaur"]["providerExhausted"]["signal"],
            "poolMarker"
        );
    }

    /// Claude Code 2.1.281 reports the same 503 as a `result` with this text.
    /// harness-server turns it into an `error` notification with the text as
    /// the message.
    #[test]
    fn claude_api_error_text_carries_the_marker() {
        let recorded = lines("claude-stream.jsonl");
        let result = recorded.iter().find(|l| l["type"] == "result").unwrap();
        assert_eq!(result["api_error_status"], 503);
        let error_line = json!({
            "method": "error",
            "params": {"error": {"message": result["result"], "codexErrorInfo": null, "additionalDetails": null},
                       "willRetry": false, "threadId": "t", "turnId": "u"}
        });
        let exhausted = classifier().classify_notification(&error_line).unwrap();
        assert_eq!(exhausted.reset_at, Some(1_790_220_353));
    }

    #[test]
    fn native_usage_limits_need_no_marker() {
        let codex = json!({"method": "error", "params": {"error": {"message": "You've hit your usage limit.", "codexErrorInfo": "usageLimitExceeded"}, "willRetry": false}});
        let exhausted = Classifier::default().classify_notification(&codex).unwrap();
        assert_eq!(exhausted.signal, Signal::UsageLimit);
        assert_eq!(exhausted.reset_at, None);

        let claude = json!({"method": "error", "params": {"error": {"message": "Claude AI usage limit reached|1790000000"}}});
        let exhausted = Classifier::default()
            .classify_notification(&claude)
            .unwrap();
        assert_eq!(exhausted.reset_at, Some(1_790_000_000));
    }

    #[test]
    fn other_failures_are_not_exhaustion() {
        let classifier = classifier();
        for line in [
            json!({"method": "error", "params": {"error": {"message": "unexpected status 503: overloaded", "codexErrorInfo": "serverOverloaded"}}}),
            json!({"method": "error", "params": {"error": {"message": "Engine not found", "codexErrorInfo": null}}}),
            json!({"method": "turn/completed", "params": {"turn": {"status": "completed", "error": null}}}),
            json!({"method": "item/completed", "params": {"item": {"text": "pool_exhausted"}}}),
        ] {
            assert_eq!(classifier.classify_notification(&line), None, "{line}");
        }
        // Without a configured marker, a proxy's text means nothing.
        let recorded = lines("codex-app-server.jsonl");
        assert_eq!(
            Classifier::default().classify_notification(&recorded[1]),
            None
        );
    }
}
