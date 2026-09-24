//! Writes Codex CLI rollout files (`~/.codex/sessions/YYYY/MM/DD/rollout-*.jsonl`).
//!
//! The rollout has what `codex resume` and the app-server `thread/resume`
//! need: a `session_meta` line, then each message as a `response_item` (the
//! model history) and an `event_msg` (the history that the TUI shows).

use chrono::{SecondsFormat, Utc};
use serde::Serialize;

use crate::model::{Role, Session, Tool};
use crate::output::{Converted, Target, shell_quote};
use crate::prepare::{PrepareOptions, prepare_messages};

/// Value of `session_meta.cli_version`, which marks an imported rollout.
pub const IMPORTED_CLI_VERSION: &str = "0.0.0";

/// Renders `session` as a Codex rollout. `model_provider` goes into
/// `session_meta`; Codex uses it only when a resume names no provider.
#[must_use]
pub fn render_session(
    session: &Session,
    target: &Target,
    options: &PrepareOptions,
    model_provider: &str,
) -> Converted {
    let prepared = prepare_messages(session, Tool::Codex, options);
    let timestamp = target
        .now
        .with_timezone(&Utc)
        .to_rfc3339_opts(SecondsFormat::Millis, true);
    let id = target.id.to_string();

    let mut lines = vec![to_json(&Line {
        timestamp: &timestamp,
        record: Record::SessionMeta(SessionMeta {
            session_id: &id,
            id: &id,
            timestamp: &timestamp,
            cwd: &target.cwd,
            originator: options.brand,
            cli_version: IMPORTED_CLI_VERSION,
            source: "cli",
            model_provider,
            base_instructions: None,
            git: session
                .git_branch
                .as_deref()
                .filter(|branch| !branch.is_empty())
                .map(|branch| Git { branch }),
        }),
    })];
    for message in &prepared {
        let (content, event) = match message.role {
            Role::User => (
                Content::InputText {
                    text: &message.text,
                },
                Event::UserMessage {
                    message: &message.text,
                    kind: "plain",
                },
            ),
            Role::Assistant => (
                Content::OutputText {
                    text: &message.text,
                },
                Event::AgentMessage {
                    message: &message.text,
                },
            ),
        };
        lines.push(to_json(&Line {
            timestamp: &timestamp,
            record: Record::ResponseItem(ResponseItem::Message {
                role: match message.role {
                    Role::User => "user",
                    Role::Assistant => "assistant",
                },
                content: [content],
            }),
        }));
        lines.push(to_json(&Line {
            timestamp: &timestamp,
            record: Record::EventMsg(event),
        }));
    }

    // Codex names rollout files in local time.
    let day = target.now.format("%Y/%m/%d").to_string();
    let stamp = target.now.format("%Y-%m-%dT%H-%M-%S");
    let mut contents = lines.join("\n");
    contents.push('\n');
    Converted {
        tool: Tool::Codex,
        id: target.id,
        path: target
            .home
            .join("sessions")
            .join(day)
            .join(format!("rollout-{stamp}-{id}.jsonl")),
        resume_command: format!("cd {} && codex resume {id}", shell_quote(&target.cwd)),
        contents,
    }
}

fn to_json(line: &impl Serialize) -> String {
    serde_json::to_string(line).expect("rollout lines always serialize")
}

/// One rollout line. The field order is the key order in the file.
#[derive(Serialize)]
struct Line<'a> {
    timestamp: &'a str,
    #[serde(flatten)]
    record: Record<'a>,
}

#[derive(Serialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
enum Record<'a> {
    SessionMeta(SessionMeta<'a>),
    ResponseItem(ResponseItem<'a>),
    EventMsg(Event<'a>),
}

#[derive(Serialize)]
struct SessionMeta<'a> {
    session_id: &'a str,
    id: &'a str,
    timestamp: &'a str,
    cwd: &'a str,
    originator: &'a str,
    cli_version: &'a str,
    source: &'a str,
    model_provider: &'a str,
    base_instructions: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    git: Option<Git<'a>>,
}

#[derive(Serialize)]
struct Git<'a> {
    branch: &'a str,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponseItem<'a> {
    Message {
        role: &'a str,
        content: [Content<'a>; 1],
    },
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Content<'a> {
    InputText { text: &'a str },
    OutputText { text: &'a str },
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Event<'a> {
    UserMessage { message: &'a str, kind: &'a str },
    AgentMessage { message: &'a str },
}
