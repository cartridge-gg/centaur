//! `harness-server transcript convert`: converts a Codex or Claude Code
//! session so that the other harness can resume it with the full history.

use std::path::PathBuf;

use chrono::Local;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::{RolloutItem, RolloutLine};
use serde::Serialize;
use serde_json::Value;
use session_transfer::convert::{ConvertOptions, TrailingPrompt, convert};
use session_transfer::discover::{Homes, SessionRef, resolve_session};
use session_transfer::prepare::PrepareOptions;
use session_transfer::{Converted, Target, Tool};
use uuid::Uuid;

use crate::{HarnessServerError, Result};

/// Name in the import preamble and message ids of converted sessions.
pub const BRAND: &str = "centaur";

/// What to convert, and where the result goes.
#[derive(Debug, Clone)]
pub struct ConvertRequest {
    pub from: Tool,
    pub to: Tool,
    pub session: SessionRef,
    /// Project directory of the new session. Defaults to the source cwd.
    pub cwd: Option<String>,
    pub homes: Homes,
    pub last_messages: Option<usize>,
    pub max_tool_output: Option<usize>,
    pub keep_thinking: bool,
    pub redact: bool,
    /// `session_meta.model_provider` of a Codex rollout.
    pub codex_model_provider: Option<String>,
    /// Leave out an unanswered prompt at the end, because the caller sends
    /// it again as the next turn.
    pub drop_trailing_prompt: bool,
    pub dry_run: bool,
}

/// The JSON that the subcommand prints.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConvertReport {
    pub tool: &'static str,
    pub id: Uuid,
    pub path: PathBuf,
    pub resume_command: String,
    pub bytes: usize,
    pub dry_run: bool,
    pub source: SourceReport,
}

#[derive(Debug, Serialize)]
pub struct SourceReport {
    pub tool: &'static str,
    pub id: String,
    pub messages: usize,
}

/// Converts the session and, unless it is a dry run, writes it.
pub fn convert_session(request: &ConvertRequest) -> Result<ConvertReport> {
    let session = resolve_session(
        request.from,
        &request.session,
        request.homes.get(request.from),
    )?;
    let cwd = request
        .cwd
        .clone()
        .or_else(|| session.cwd.clone())
        .ok_or(session_transfer::Error::MissingCwd)?;
    let target = Target {
        home: request.homes.get(request.to).to_path_buf(),
        cwd,
        id: Uuid::new_v4(),
        now: Local::now().fixed_offset(),
    };
    let options = ConvertOptions {
        prepare: PrepareOptions {
            max_tool_output: request.max_tool_output,
            last_messages: request.last_messages,
            keep_thinking: request.keep_thinking,
            redact: request.redact,
            brand: BRAND,
        },
        trailing_prompt: if request.drop_trailing_prompt {
            TrailingPrompt::Drop
        } else {
            TrailingPrompt::Keep
        },
        codex_model_provider: request
            .codex_model_provider
            .clone()
            .unwrap_or_else(|| ConvertOptions::default().codex_model_provider),
    };
    let converted = convert(&session, request.to, &target, &options)?;
    check_converted(&converted)?;
    if !request.dry_run {
        converted.write()?;
    }
    Ok(ConvertReport {
        tool: converted.tool.id(),
        id: converted.id,
        path: converted.path,
        resume_command: converted.resume_command,
        bytes: converted.contents.len(),
        dry_run: request.dry_run,
        source: SourceReport {
            tool: session.source.id(),
            id: session.id,
            messages: session.messages.len(),
        },
    })
}

/// Checks that the target harness can read a converted session, before
/// anything uses it. A harness does not always report a file that it cannot
/// read: Codex resumes a rollout whose lines it cannot parse as a thread with
/// no turns, and Claude Code leaves out the lines that it cannot parse.
///
/// - Codex: every line is a rollout line of the Codex protocol that this
///   crate is built with. The first line is the `session_meta` of the new
///   id, and at least one message follows.
/// - Claude Code: every conversation line has the new session id, and the
///   lines form one chain from the first to the last. `claude --resume`
///   loads that chain.
///
/// # Errors
///
/// [`HarnessServerError::UnreadableConversion`] with the first problem.
pub fn check_converted(converted: &Converted) -> Result<()> {
    let checked = match converted.tool {
        Tool::Codex => check_codex_rollout(&converted.contents, converted.id),
        Tool::Claude => check_claude_transcript(&converted.contents, converted.id),
    };
    checked.map_err(|reason| HarnessServerError::UnreadableConversion {
        tool: converted.tool.label(),
        reason,
    })
}

fn check_codex_rollout(contents: &str, id: Uuid) -> std::result::Result<(), String> {
    let mut messages = 0;
    for (index, line) in contents.lines().enumerate() {
        let number = index + 1;
        let line: RolloutLine = serde_json::from_str(line)
            .map_err(|error| format!("line {number} is not a rollout line: {error}"))?;
        match line.item {
            RolloutItem::SessionMeta(meta) if index == 0 => {
                if meta.meta.id.to_string() != id.to_string() {
                    return Err(format!(
                        "session_meta has the id {}, not {id}",
                        meta.meta.id
                    ));
                }
            }
            _ if index == 0 => return Err("the first line is not session_meta".to_owned()),
            RolloutItem::ResponseItem(ResponseItem::Message { .. }) => messages += 1,
            _ => {}
        }
    }
    if messages == 0 {
        return Err("the rollout has no messages".to_owned());
    }
    Ok(())
}

fn check_claude_transcript(contents: &str, id: Uuid) -> std::result::Result<(), String> {
    let id = id.to_string();
    let mut previous: Option<String> = None;
    for (index, line) in contents.lines().enumerate() {
        let number = index + 1;
        let record: Value = serde_json::from_str(line)
            .map_err(|error| format!("line {number} is not JSON: {error}"))?;
        let kind = record
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("line {number} has no type"))?;
        if record
            .get("sessionId")
            .and_then(Value::as_str)
            .is_some_and(|session| session != id)
        {
            return Err(format!("line {number} belongs to another session"));
        }
        if !matches!(kind, "user" | "assistant") {
            continue;
        }
        if record.get("sessionId").and_then(Value::as_str) != Some(id.as_str()) {
            return Err(format!("line {number} has no session id"));
        }
        if record.pointer("/message/role").and_then(Value::as_str) != Some(kind)
            || !record
                .pointer("/message/content")
                .is_some_and(|content| content.is_string() || content.is_array())
        {
            return Err(format!("line {number} has no {kind} message"));
        }
        if record.get("parentUuid").and_then(Value::as_str) != previous.as_deref() {
            return Err(format!(
                "line {number} does not continue the line before it"
            ));
        }
        let uuid = record
            .get("uuid")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("line {number} has no uuid"))?;
        previous = Some(uuid.to_owned());
    }
    if previous.is_none() {
        return Err("the session has no messages".to_owned());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use serde_json::json;

    use super::*;

    fn converted(from: Tool, fixture: &str) -> Converted {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../session-transfer/tests/fixtures")
            .join(fixture);
        let session = session_transfer::discover::read_session(from, &path).unwrap();
        let to = match from {
            Tool::Codex => Tool::Claude,
            Tool::Claude => Tool::Codex,
        };
        let target = Target {
            home: std::env::temp_dir().join("never-written"),
            cwd: "/work/app".to_owned(),
            id: Uuid::new_v4(),
            now: Local::now().fixed_offset(),
        };
        convert(&session, to, &target, &ConvertOptions::default()).unwrap()
    }

    fn codex_rollout() -> Converted {
        converted(
            Tool::Claude,
            "claude/19fa6065-e665-49b6-9565-cf655fa8bc17.jsonl",
        )
    }

    fn claude_transcript() -> Converted {
        converted(Tool::Codex, "codex/recorded-codex-0.154.jsonl")
    }

    /// `converted` with line `index` changed by `change`.
    fn with_line(mut converted: Converted, index: usize, change: impl Fn(&mut Value)) -> Converted {
        let mut lines: Vec<String> = converted.contents.lines().map(str::to_owned).collect();
        let mut value: Value = serde_json::from_str(&lines[index]).unwrap();
        change(&mut value);
        lines[index] = value.to_string();
        converted.contents = lines.iter().map(|line| format!("{line}\n")).collect();
        converted
    }

    fn reason(converted: &Converted) -> String {
        match check_converted(converted) {
            Err(HarnessServerError::UnreadableConversion { reason, .. }) => reason,
            other => panic!("expected an unreadable conversion, got {other:?}"),
        }
    }

    #[test]
    fn every_fixture_converts_to_a_session_that_passes_the_check() {
        for (from, fixture) in [
            (
                Tool::Claude,
                "claude/19fa6065-e665-49b6-9565-cf655fa8bc17.jsonl",
            ),
            (
                Tool::Claude,
                "claude/C1A0DE00-0000-4000-8000-00000000000A.jsonl",
            ),
            (
                Tool::Claude,
                "claude/sessport/11111111-2222-4333-8444-555555555555.jsonl",
            ),
            (Tool::Claude, "claude/synthetic-broken-chain.jsonl"),
            (Tool::Codex, "codex/recorded-codex-0.154.jsonl"),
            (
                Tool::Codex,
                "codex/sessport/rollout-2025-06-01T10-00-00-5973b6c0-94b8-487b-a530-2aeb6098ae0e.jsonl",
            ),
            (
                Tool::Codex,
                "codex/sessport/rollout-2026-09-11T16-00-00-0199aaaa-bbbb-7ccc-8ddd-eeeeeeeeeeee.jsonl",
            ),
            (Tool::Codex, "codex/synthetic-edge-cases.jsonl"),
        ] {
            if let Err(error) = check_converted(&converted(from, fixture)) {
                panic!("{fixture}: {error}");
            }
        }
    }

    #[test]
    fn a_rollout_line_that_codex_cannot_parse_fails_the_check() {
        let broken = with_line(codex_rollout(), 2, |line| {
            line["type"] = json!("not_a_rollout_item");
        });
        assert!(reason(&broken).starts_with("line 3 is not a rollout line"));

        let other_id = with_line(codex_rollout(), 0, |line| {
            line["payload"]["id"] = json!(Uuid::new_v4().to_string());
        });
        assert!(reason(&other_id).starts_with("session_meta has the id"));

        let mut meta_only = codex_rollout();
        meta_only.contents = format!("{}\n", meta_only.contents.lines().next().unwrap());
        assert_eq!(reason(&meta_only), "the rollout has no messages");
    }

    #[test]
    fn a_broken_claude_conversation_fails_the_check() {
        let converted = claude_transcript();
        let first_message = converted
            .contents
            .lines()
            .position(|line| line.contains(r#""type":"user""#))
            .unwrap();
        let chain = with_line(claude_transcript(), first_message + 2, |line| {
            line["parentUuid"] = json!(Uuid::new_v4().to_string());
        });
        assert_eq!(
            reason(&chain),
            format!(
                "line {} does not continue the line before it",
                first_message + 3
            )
        );

        let session = with_line(claude_transcript(), first_message, |line| {
            line["sessionId"] = json!(Uuid::new_v4().to_string());
        });
        assert_eq!(
            reason(&session),
            format!("line {} belongs to another session", first_message + 1)
        );

        let message = with_line(claude_transcript(), first_message, |line| {
            line["message"] = json!({"text": "hi"});
        });
        assert_eq!(
            reason(&message),
            format!("line {} has no user message", first_message + 1)
        );

        let mut empty = claude_transcript();
        empty.contents = String::new();
        assert_eq!(reason(&empty), "the session has no messages");
    }
}
