//! Provider failover: a session continues on another harness when the model
//! provider of its harness has no capacity left.
//!
//! The switch happens in the sandbox (harness-server `switch.rs`): the
//! harness server converts the native session and resumes it on the other
//! harness. api-rs decides per execution whether a switch is allowed, tells
//! the sandbox with a `centaur` directive on each user line, and records the
//! switch when the sandbox reports it with a `centaur/providerFailover` line.

use std::collections::HashMap;

use centaur_session_core::{HarnessType, ThreadKey};
use serde_json::{Map, Value, json};

/// The line that a sandbox prints after it moved the session.
pub(crate) const FAILOVER_NOTICE_METHOD: &str = "centaur/providerFailover";
/// The session event for a switch.
pub(crate) const PROVIDER_FAILOVER_EVENT: &str = "session.provider_failover";
/// `sessions.metadata` key of the last switch.
pub(crate) const PROVIDER_FAILOVER_METADATA_KEY: &str = "provider_failover";
/// Execution metadata key: `false` turns failover off for the execution.
pub(crate) const EXECUTION_FAILOVER_KEY: &str = "provider_failover";

#[derive(Clone, Debug, Default)]
pub struct ProviderFailoverConfig {
    /// Sandboxes can switch harness (`CENTAUR_HARNESS_SWITCHING`), and
    /// executions get the directive.
    pub enabled: bool,
    /// The model of a session after it moves to the harness. Without an
    /// entry, the harness uses its default model.
    pub models: HashMap<HarnessType, String>,
    /// Thread key prefixes of sessions that can fail over. Empty: all.
    pub thread_prefixes: Vec<String>,
}

impl ProviderFailoverConfig {
    pub(crate) fn allows(&self, thread_key: &ThreadKey) -> bool {
        self.enabled
            && (self.thread_prefixes.is_empty()
                || self
                    .thread_prefixes
                    .iter()
                    .any(|prefix| thread_key.as_str().starts_with(prefix.as_str())))
    }

    pub(crate) fn model_for(&self, harness: &HarnessType) -> Option<&str> {
        self.models.get(harness).map(String::as_str)
    }
}

/// The harness that a session on `harness` fails over to.
pub(crate) fn failover_target(harness: &HarnessType) -> Option<HarnessType> {
    match harness {
        HarnessType::Codex => Some(HarnessType::ClaudeCode),
        HarnessType::ClaudeCode => Some(HarnessType::Codex),
        HarnessType::Amp | HarnessType::Nanocodex | HarnessType::Hermes => None,
    }
}

/// False when the requester turned failover off for the execution.
pub(crate) fn execution_allows_failover(metadata: Option<&Value>) -> bool {
    metadata
        .and_then(|metadata| metadata.get(EXECUTION_FAILOVER_KEY))
        .and_then(Value::as_bool)
        != Some(false)
}

/// What the sandbox may do in this execution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FailoverDirective {
    /// The harness for the turn. When the sandbox runs another one, it moves
    /// the session before the turn starts.
    pub harness: HarnessType,
    /// The session may move to the other harness if the provider fails.
    pub enabled: bool,
    /// The model after such a move.
    pub model: Option<String>,
    /// The user lines were written for another harness: their model,
    /// provider and reasoning effort do not apply.
    pub retarget_model: Option<Option<String>>,
}

impl FailoverDirective {
    fn to_value(&self) -> Value {
        let mut failover = json!({"enabled": self.enabled});
        if let Some(model) = &self.model {
            failover["model"] = json!(model);
        }
        json!({"harness": self.harness.as_ref(), "failover": failover})
    }

    /// The user lines with the directive. Other lines are unchanged.
    pub(crate) fn apply(&self, lines: Vec<String>) -> Vec<String> {
        let directive = self.to_value();
        lines
            .into_iter()
            .map(|line| {
                let Ok(Value::Object(mut map)) = serde_json::from_str::<Value>(&line) else {
                    return line;
                };
                if map.get("type").and_then(Value::as_str) != Some("user") {
                    return line;
                }
                if let Some(model) = &self.retarget_model {
                    retarget(&mut map, model.as_deref());
                }
                map.insert("centaur".to_owned(), directive.clone());
                Value::Object(map).to_string()
            })
            .collect()
    }
}

fn retarget(line: &mut Map<String, Value>, model: Option<&str>) {
    for key in ["model", "provider", "reasoning"] {
        line.remove(key);
    }
    if let Some(model) = model {
        line.insert("model".to_owned(), json!(model));
    }
}

/// A `centaur/providerFailover` line from a sandbox.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct FailoverNotice {
    pub from: HarnessType,
    pub to: HarnessType,
    pub mode: String,
    pub params: Value,
}

impl FailoverNotice {
    pub(crate) fn parse(value: &Value) -> Option<Self> {
        if value.get("method").and_then(Value::as_str) != Some(FAILOVER_NOTICE_METHOD) {
            return None;
        }
        let params = value.get("params")?;
        let harness = |key: &str| params.get(key)?.as_str()?.parse::<HarnessType>().ok();
        Some(Self {
            from: harness("from")?,
            to: harness("to")?,
            mode: params
                .get("mode")
                .and_then(Value::as_str)
                .unwrap_or("reactive")
                .to_owned(),
            params: params.clone(),
        })
    }

    /// The exhaustion that caused a reactive switch.
    pub(crate) fn provider_exhausted(&self) -> Option<&Value> {
        self.params
            .get("providerExhausted")
            .filter(|value| value.is_object())
    }
}

/// `sessions.metadata.provider_failover`: the last switch of the session.
pub(crate) fn failover_record(
    from: &HarnessType,
    to: &HarnessType,
    mode: &str,
    execution_id: Option<&str>,
    reset_at: Option<i64>,
) -> Value {
    json!({
        "from": from.as_ref(),
        "to": to.as_ref(),
        "mode": mode,
        "at": jiff::Timestamp::now().to_string(),
        "execution_id": execution_id,
        "reset_at": reset_at,
    })
}

/// True when the session left `requested` in a failover and still runs on
/// `existing`: a request for `requested` is from a client that did not see
/// the switch.
pub(crate) fn kept_after_failover(
    record: Option<&Value>,
    requested: &HarnessType,
    existing: &str,
) -> bool {
    let Some(record) = record else {
        return false;
    };
    record.get("from").and_then(Value::as_str) == Some(requested.as_ref())
        && record.get("to").and_then(Value::as_str) == Some(existing)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn thread(key: &str) -> ThreadKey {
        ThreadKey::parse(key).unwrap()
    }

    #[test]
    fn prefixes_limit_the_threads() {
        let mut config = ProviderFailoverConfig {
            enabled: true,
            ..Default::default()
        };
        assert!(config.allows(&thread("chat:C1:1.0")));
        config.thread_prefixes = vec!["chat:".to_owned()];
        assert!(config.allows(&thread("chat:C1:1.0")));
        assert!(!config.allows(&thread("github:org/repo:1")));
        config.enabled = false;
        assert!(!config.allows(&thread("chat:C1:1.0")));
    }

    #[test]
    fn only_codex_and_claude_code_fail_over() {
        assert_eq!(
            failover_target(&HarnessType::Codex),
            Some(HarnessType::ClaudeCode)
        );
        assert_eq!(
            failover_target(&HarnessType::ClaudeCode),
            Some(HarnessType::Codex)
        );
        assert_eq!(failover_target(&HarnessType::Hermes), None);
    }

    #[test]
    fn an_execution_can_turn_failover_off() {
        assert!(execution_allows_failover(None));
        assert!(execution_allows_failover(Some(&json!({"source": "slack"}))));
        assert!(!execution_allows_failover(Some(
            &json!({"provider_failover": false})
        )));
    }

    #[test]
    fn the_directive_goes_on_user_lines_only() {
        let directive = FailoverDirective {
            harness: HarnessType::ClaudeCode,
            enabled: true,
            model: Some("gpt-5.5".to_owned()),
            retarget_model: None,
        };
        let lines = directive.apply(vec![
            json!({"type": "user", "text": "hi", "model": "claude-opus-5-5"}).to_string(),
            json!({"type": "attachment.chunk", "attachmentId": "a"}).to_string(),
            "not json".to_owned(),
        ]);
        let user: Value = serde_json::from_str(&lines[0]).unwrap();
        assert_eq!(
            user["centaur"],
            json!({"harness": "claudecode", "failover": {"enabled": true, "model": "gpt-5.5"}})
        );
        assert_eq!(user["model"], "claude-opus-5-5");
        assert!(!lines[1].contains("centaur"));
        assert_eq!(lines[2], "not json");
    }

    #[test]
    fn retargeted_lines_lose_the_settings_of_the_other_harness() {
        let directive = FailoverDirective {
            harness: HarnessType::ClaudeCode,
            enabled: false,
            model: None,
            retarget_model: Some(Some("claude-opus-5-5".to_owned())),
        };
        let lines = directive.apply(vec![
            json!({"type": "user", "text": "hi", "model": "gpt-5.5", "provider": "p", "reasoning": "high"})
                .to_string(),
        ]);
        let user: Value = serde_json::from_str(&lines[0]).unwrap();
        assert_eq!(user["model"], "claude-opus-5-5");
        assert!(user.get("provider").is_none() && user.get("reasoning").is_none());
        assert_eq!(user["centaur"]["failover"], json!({"enabled": false}));
    }

    #[test]
    fn notices_name_known_harnesses() {
        let notice = FailoverNotice::parse(&json!({"method": "centaur/providerFailover",
            "params": {"from": "codex", "to": "claudecode", "mode": "reactive",
                       "providerExhausted": {"resetAt": 1_790_220_376}}}))
        .unwrap();
        assert_eq!(notice.from, HarnessType::Codex);
        assert_eq!(notice.to, HarnessType::ClaudeCode);
        assert_eq!(
            notice.provider_exhausted().unwrap()["resetAt"],
            1_790_220_376
        );

        let unknown = json!({"method": "centaur/providerFailover", "params": {"from": "codex", "to": "gemini"}});
        assert_eq!(FailoverNotice::parse(&unknown), None);
        assert_eq!(
            FailoverNotice::parse(&json!({"method": "error", "params": {}})),
            None
        );
    }

    #[test]
    fn only_the_harness_left_in_a_failover_is_kept() {
        let record = failover_record(
            &HarnessType::Codex,
            &HarnessType::ClaudeCode,
            "reactive",
            None,
            None,
        );
        assert!(kept_after_failover(
            Some(&record),
            &HarnessType::Codex,
            "claudecode"
        ));
        assert!(!kept_after_failover(
            Some(&record),
            &HarnessType::Amp,
            "claudecode"
        ));
        assert!(!kept_after_failover(
            Some(&record),
            &HarnessType::Codex,
            "hermes"
        ));
        assert!(!kept_after_failover(
            None,
            &HarnessType::Codex,
            "claudecode"
        ));
    }
}
