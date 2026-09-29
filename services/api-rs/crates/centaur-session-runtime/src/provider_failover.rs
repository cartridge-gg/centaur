//! Provider failover: a session continues on another harness when the model
//! provider of its harness has no capacity left.
//!
//! The switch happens in the sandbox (harness-server `switch.rs`): the
//! harness server converts the native session and resumes it on the other
//! harness. api-rs decides per execution whether a switch is allowed, tells
//! the sandbox with a `centaur` directive on each user line, and records the
//! switch when the sandbox reports it with a `centaur/providerFailover` line.

use std::collections::{HashMap, HashSet};

use centaur_session_core::{HarnessType, ThreadKey};
use centaur_telemetry::{init_session_failure, init_session_provider_failover};
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

/// Every `mode` of a switch: a turn hit an exhausted provider (`reactive`),
/// the provider was known to be exhausted before the turn (`proactive`), the
/// user asked for the harness (`requested`), or the sandbox could not use the
/// converted session and went back (`revert`).
pub(crate) const FAILOVER_MODES: [&str; 4] = ["reactive", "proactive", "requested", "revert"];

/// The harnesses whose sessions can fail over.
pub(crate) const FAILOVER_HARNESSES: [HarnessType; 3] = [
    HarnessType::Codex,
    HarnessType::ClaudeCode,
    HarnessType::Hermes,
];

/// Creates the metrics of switches and of turns that failed on an exhausted
/// provider at 0. These events are rare, and `increase()` cannot see the first
/// increment of a series that starts at 1; each process start begins new
/// series. A switch goes to a failover candidate, or back from it
/// (`requested`, `revert`), so each pair has both directions.
pub(crate) fn init_metrics(failover_enabled: bool) {
    for harness in FAILOVER_HARNESSES {
        init_session_failure(harness.as_ref(), crate::PROVIDER_EXHAUSTED_FAILURE_CLASS);
        if !failover_enabled {
            continue;
        }
        for candidate in failover_candidates(&harness) {
            for mode in FAILOVER_MODES {
                init_session_provider_failover(harness.as_ref(), candidate.as_ref(), mode);
                init_session_provider_failover(candidate.as_ref(), harness.as_ref(), mode);
            }
        }
    }
}

/// The harnesses that a session on `harness` can fail over to, in order of
/// preference. Codex and Claude Code fail over to each other. Hermes fails
/// over to Codex, or to Claude Code when the Codex provider is exhausted too.
pub(crate) fn failover_candidates(harness: &HarnessType) -> &'static [HarnessType] {
    match harness {
        HarnessType::Codex => &[HarnessType::ClaudeCode],
        HarnessType::ClaudeCode => &[HarnessType::Codex],
        HarnessType::Hermes => &[HarnessType::Codex, HarnessType::ClaudeCode],
        HarnessType::Amp | HarnessType::Nanocodex => &[],
    }
}

/// The first of `candidates` whose provider is not known to be exhausted.
/// `exhausted` has the names of the exhausted harnesses.
pub(crate) fn first_available<'a>(
    candidates: &'a [HarnessType],
    exhausted: &HashSet<String>,
) -> Option<&'a HarnessType> {
    candidates
        .iter()
        .find(|candidate| !exhausted.contains(candidate.as_ref()))
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
    /// The session may move to `target` if the provider fails.
    pub enabled: bool,
    /// The failover target of `harness`.
    pub target: Option<HarnessType>,
    /// The model after such a move.
    pub model: Option<String>,
    /// The user lines were written for another harness: their model,
    /// provider and reasoning effort do not apply.
    pub retarget_model: Option<Option<String>>,
    /// The user asked for `harness`: the sandbox reports the move with the
    /// mode `requested`.
    pub requested: bool,
}

impl FailoverDirective {
    fn to_value(&self) -> Value {
        let mut failover = json!({"enabled": self.enabled});
        if let Some(target) = &self.target {
            failover["harness"] = json!(target.as_ref());
        }
        if let Some(model) = &self.model {
            failover["model"] = json!(model);
        }
        let mut directive = json!({"harness": self.harness.as_ref(), "failover": failover});
        if self.requested {
            directive["mode"] = json!("requested");
        }
        directive
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

/// `sessions.metadata.provider_failover`: the last switch of the session,
/// and the sandbox it happened in. The state volume of that sandbox has the
/// session, and the sessions that each switch converted it from.
pub(crate) fn failover_record(
    from: &HarnessType,
    to: &HarnessType,
    mode: &str,
    execution_id: Option<&str>,
    reset_at: Option<i64>,
    sandbox_id: Option<&str>,
) -> Value {
    json!({
        "from": from.as_ref(),
        "to": to.as_ref(),
        "mode": mode,
        "at": jiff::Timestamp::now().to_string(),
        "execution_id": execution_id,
        "reset_at": reset_at,
        "sandbox_id": sandbox_id,
    })
}

/// True when the sandbox of the last switch still serves the session. That
/// sandbox can switch harness, and its state volume has the session.
pub(crate) fn switched_in(record: Option<&Value>, sandbox_id: Option<&str>) -> bool {
    let recorded = record
        .and_then(|record| record.get("sandbox_id"))
        .and_then(Value::as_str);
    sandbox_id.is_some() && recorded == sandbox_id
}

/// True while a switch that the user asked for has not reached the sandbox.
pub(crate) fn switch_pending(record: Option<&Value>) -> bool {
    record
        .and_then(|record| record.get("pending"))
        .and_then(Value::as_bool)
        == Some(true)
}

/// The harnesses that the session left in the switches of the sandbox of
/// `record`, oldest first. A record from before this list names only `from`.
pub(crate) fn left_harnesses(record: Option<&Value>) -> Vec<String> {
    let Some(record) = record else {
        return Vec::new();
    };
    match record.get("left").and_then(Value::as_array) {
        Some(left) => left
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect(),
        None => record
            .get("from")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .into_iter()
            .collect(),
    }
}

/// `record`, a switch from [`failover_record`], with `left`: the harnesses
/// that the session left in the switches of this sandbox, oldest first, with
/// the `from` of this switch last. The list of `previous` goes on while the
/// sandbox stays, so a session that left Hermes and then Codex has both. The
/// harness that the session runs on is never in it.
pub(crate) fn with_left(mut record: Value, previous: Option<&Value>) -> Value {
    let sandbox_id = record.get("sandbox_id").and_then(Value::as_str);
    let mut left = if switched_in(previous, sandbox_id) {
        left_harnesses(previous)
    } else {
        Vec::new()
    };
    let from = record
        .get("from")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let to = record.get("to").and_then(Value::as_str).unwrap_or_default();
    left.retain(|harness| harness != from && harness != to);
    if !from.is_empty() {
        left.push(from.to_owned());
    }
    record["left"] = json!(left);
    record
}

/// True when the session left `requested` in a failover and still runs on
/// `existing`: a request for `requested` is from a client that did not see
/// the switch, or, with `harness_explicit`, a request to go back. The session
/// can have left more than one harness in its sandbox.
pub(crate) fn kept_after_failover(
    record: Option<&Value>,
    requested: &HarnessType,
    existing: &str,
) -> bool {
    record.is_some_and(|record| record.get("to").and_then(Value::as_str) == Some(existing))
        && left_harnesses(record)
            .iter()
            .any(|harness| harness == requested.as_ref())
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
    fn codex_claude_code_and_hermes_fail_over() {
        use HarnessType::{Amp, ClaudeCode, Codex, Hermes, Nanocodex};
        for (harness, candidates) in [
            (Codex, &[ClaudeCode][..]),
            (ClaudeCode, &[Codex][..]),
            (Hermes, &[Codex, ClaudeCode][..]),
            (Amp, &[][..]),
            (Nanocodex, &[][..]),
        ] {
            assert_eq!(failover_candidates(&harness), candidates, "{harness}");
        }
        for harness in FAILOVER_HARNESSES {
            assert!(!failover_candidates(&harness).is_empty(), "{harness}");
        }
    }

    #[test]
    fn the_first_candidate_that_is_not_exhausted_is_the_target() {
        let hermes = failover_candidates(&HarnessType::Hermes);
        let exhausted = |names: &[&str]| -> HashSet<String> {
            names.iter().map(|name| (*name).to_owned()).collect()
        };
        assert_eq!(
            first_available(hermes, &exhausted(&[])),
            Some(&HarnessType::Codex)
        );
        assert_eq!(
            first_available(hermes, &exhausted(&["hermes"])),
            Some(&HarnessType::Codex)
        );
        assert_eq!(
            first_available(hermes, &exhausted(&["codex"])),
            Some(&HarnessType::ClaudeCode)
        );
        assert_eq!(
            first_available(hermes, &exhausted(&["codex", "claudecode"])),
            None
        );
        assert_eq!(first_available(&[], &exhausted(&[])), None);
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
            target: Some(HarnessType::Codex),
            model: Some("gpt-5.5".to_owned()),
            retarget_model: None,
            requested: false,
        };
        let lines = directive.apply(vec![
            json!({"type": "user", "text": "hi", "model": "claude-opus-5-5"}).to_string(),
            json!({"type": "attachment.chunk", "attachmentId": "a"}).to_string(),
            "not json".to_owned(),
        ]);
        let user: Value = serde_json::from_str(&lines[0]).unwrap();
        assert_eq!(
            user["centaur"],
            json!({"harness": "claudecode",
                   "failover": {"enabled": true, "harness": "codex", "model": "gpt-5.5"}})
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
            target: None,
            model: None,
            retarget_model: Some(Some("claude-opus-5-5".to_owned())),
            requested: true,
        };
        let lines = directive.apply(vec![
            json!({"type": "user", "text": "hi", "model": "gpt-5.5", "provider": "p", "reasoning": "high"})
                .to_string(),
        ]);
        let user: Value = serde_json::from_str(&lines[0]).unwrap();
        assert_eq!(user["model"], "claude-opus-5-5");
        assert!(user.get("provider").is_none() && user.get("reasoning").is_none());
        assert_eq!(user["centaur"]["failover"], json!({"enabled": false}));
        assert_eq!(user["centaur"]["mode"], "requested");
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

        let back = FailoverNotice::parse(&json!({"method": "centaur/providerFailover",
            "params": {"from": "codex", "to": "hermes", "mode": "requested"}}))
        .unwrap();
        assert_eq!(back.to, HarnessType::Hermes);

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
            Some("sbx-1"),
        );
        assert!(switched_in(Some(&record), Some("sbx-1")));
        assert!(!switched_in(Some(&record), Some("sbx-2")));
        assert!(!switched_in(Some(&record), None));
        assert!(!switched_in(None, Some("sbx-1")));
        assert!(!switch_pending(Some(&record)));
        assert!(switch_pending(Some(&json!({"pending": true}))));
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

    #[test]
    fn a_session_remembers_every_harness_that_it_left_in_its_sandbox() {
        let switch =
            |from: HarnessType, to: HarnessType, sandbox: &str, previous: Option<&Value>| {
                with_left(
                    failover_record(&from, &to, "reactive", None, None, Some(sandbox)),
                    previous,
                )
            };
        let first = switch(HarnessType::Hermes, HarnessType::Codex, "sbx-1", None);
        assert_eq!(left_harnesses(Some(&first)), ["hermes"]);
        let second = switch(
            HarnessType::Codex,
            HarnessType::ClaudeCode,
            "sbx-1",
            Some(&first),
        );
        assert_eq!(left_harnesses(Some(&second)), ["hermes", "codex"]);
        for requested in [HarnessType::Hermes, HarnessType::Codex] {
            assert!(
                kept_after_failover(Some(&second), &requested, "claudecode"),
                "{requested}"
            );
        }
        assert!(!kept_after_failover(
            Some(&second),
            &HarnessType::Hermes,
            "codex"
        ));

        // Back on Hermes: the list has the other two, and never the harness
        // that the session runs on.
        let back = switch(
            HarnessType::ClaudeCode,
            HarnessType::Hermes,
            "sbx-1",
            Some(&second),
        );
        assert_eq!(left_harnesses(Some(&back)), ["codex", "claudecode"]);

        // A switch in another sandbox starts a new list.
        let elsewhere = switch(
            HarnessType::Codex,
            HarnessType::ClaudeCode,
            "sbx-2",
            Some(&second),
        );
        assert_eq!(left_harnesses(Some(&elsewhere)), ["codex"]);

        // A record from before the list names only `from`.
        let old = json!({"from": "codex", "to": "claudecode", "sandbox_id": "sbx-1"});
        assert_eq!(left_harnesses(Some(&old)), ["codex"]);
        assert!(kept_after_failover(
            Some(&old),
            &HarnessType::Codex,
            "claudecode"
        ));
        assert!(!kept_after_failover(
            Some(&old),
            &HarnessType::Hermes,
            "claudecode"
        ));
    }

    #[test]
    fn switch_and_exhausted_failure_metrics_start_at_zero() {
        centaur_telemetry::prometheus_handle().unwrap();
        init_metrics(true);
        let metrics = centaur_telemetry::render_metrics().unwrap();
        for (from, to) in [
            ("codex", "claudecode"),
            ("claudecode", "codex"),
            ("hermes", "codex"),
            ("codex", "hermes"),
            ("hermes", "claudecode"),
            ("claudecode", "hermes"),
        ] {
            for mode in FAILOVER_MODES {
                let series = format!(
                    r#"centaur_session_provider_failovers_total{{from="{from}",to="{to}",mode="{mode}"}} "#
                );
                assert!(metrics.contains(&series), "{series}");
            }
        }
        for harness in ["codex", "claudecode", "hermes"] {
            let series = format!(
                r#"centaur_session_failures_total{{failure_class="provider_exhausted",harness="{harness}"}} "#
            );
            assert!(metrics.contains(&series), "{series}");
        }
    }
}
