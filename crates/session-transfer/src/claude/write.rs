//! Writes Claude Code session files (`~/.claude/projects/<encoded-cwd>/<id>.jsonl`).

use std::iter;

use chrono::{SecondsFormat, TimeDelta, Utc};
use serde::Serialize;
use uuid::Uuid;

use crate::js;
use crate::model::{Role, Session, Tool};
use crate::output::{Converted, Target, shell_quote};
use crate::prepare::{PrepareOptions, prepare_messages};

/// Value of the `version` field on each line.
pub const CLAUDE_VERSION: &str = "2.1.0";

/// Longest project directory name that Claude Code keeps as is.
const MAX_PROJECT_DIR_LEN: usize = 200;

/// Renders `session` as a Claude Code session. `next_uuid` gives the uuid of
/// each line.
#[must_use]
pub fn render_session(
    session: &Session,
    target: &Target,
    options: &PrepareOptions,
    mut next_uuid: impl FnMut() -> Uuid,
) -> Converted {
    let prepared = prepare_messages(session, Tool::Claude, options);
    let git_branch = session.git_branch.as_deref().unwrap_or("");
    // Strictly increasing timestamps, one second apart, so that Claude Code
    // keeps the order.
    let start = target.now.with_timezone(&Utc) - TimeDelta::seconds(prepared.len() as i64);
    let times = iter::successors(Some(start), |t| Some(*t + TimeDelta::seconds(1)));

    let mut contents = String::new();
    let mut parent_uuid = None;
    for ((index, message), time) in prepared.iter().enumerate().zip(times) {
        let uuid = next_uuid();
        let entry = match message.role {
            Role::User => Entry::User {
                message: UserMessage {
                    content: &message.text,
                },
            },
            Role::Assistant => Entry::Assistant {
                message: AssistantMessage {
                    id: format!("msg_{}_{}", options.brand, uuid.simple()),
                    kind: "message",
                    role: "assistant",
                    model: message
                        .model
                        .as_deref()
                        .or(session.model.as_deref())
                        .unwrap_or("imported"),
                    content: [TextBlock {
                        text: &message.text,
                    }],
                    stop_reason: "end_turn",
                    stop_sequence: None,
                    usage: Usage::default(),
                },
                request_id: format!("req_{}_{index}", options.brand),
            },
        };
        push_line(
            &mut contents,
            &Turn {
                parent_uuid,
                is_sidechain: false,
                user_type: "external",
                cwd: &target.cwd,
                session_id: target.id,
                version: CLAUDE_VERSION,
                git_branch,
                entry,
                uuid,
                timestamp: time.to_rfc3339_opts(SecondsFormat::Millis, true),
            },
        );
        parent_uuid = Some(uuid);
    }

    let title = session
        .title
        .as_deref()
        .or(session.preview.as_deref())
        .unwrap_or(&session.id);
    push_line(
        &mut contents,
        &AiTitle {
            ai_title: js::one_line(&format!("[{}] {title}", session.source.id()), 100),
            session_id: target.id,
        },
    );

    Converted {
        tool: Tool::Claude,
        id: target.id,
        path: target
            .home
            .join("projects")
            .join(encode_project_dir(&target.cwd))
            .join(format!("{}.jsonl", target.id)),
        resume_command: format!(
            "cd {} && claude --resume {}",
            shell_quote(&target.cwd),
            target.id
        ),
        contents,
    }
}

fn push_line(contents: &mut String, line: &impl Serialize) {
    contents.push_str(&serde_json::to_string(line).expect("session lines always serialize"));
    contents.push('\n');
}

/// One conversation line. The field order is the key order in the file.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Turn<'a> {
    parent_uuid: Option<Uuid>,
    is_sidechain: bool,
    user_type: &'static str,
    cwd: &'a str,
    session_id: Uuid,
    version: &'static str,
    git_branch: &'a str,
    #[serde(flatten)]
    entry: Entry<'a>,
    uuid: Uuid,
    timestamp: String,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum Entry<'a> {
    User {
        message: UserMessage<'a>,
    },
    Assistant {
        message: AssistantMessage<'a>,
        #[serde(rename = "requestId")]
        request_id: String,
    },
}

#[derive(Serialize)]
#[serde(tag = "role", rename = "user")]
struct UserMessage<'a> {
    content: &'a str,
}

#[derive(Serialize)]
struct AssistantMessage<'a> {
    id: String,
    #[serde(rename = "type")]
    kind: &'static str,
    role: &'static str,
    model: &'a str,
    content: [TextBlock<'a>; 1],
    stop_reason: &'static str,
    stop_sequence: Option<&'static str>,
    usage: Usage,
}

#[derive(Serialize)]
#[serde(tag = "type", rename = "text")]
struct TextBlock<'a> {
    text: &'a str,
}

#[derive(Serialize, Default)]
struct Usage {
    input_tokens: u32,
    output_tokens: u32,
}

/// The session title that `claude --resume` shows.
#[derive(Serialize)]
#[serde(tag = "type", rename = "ai-title", rename_all = "camelCase")]
struct AiTitle {
    ai_title: String,
    session_id: Uuid,
}

/// Claude Code names each project directory after its cwd: every character
/// that is not an ASCII letter or digit becomes `-` (one per UTF-16 code unit).
///
/// A name longer than 200 characters is cut to 200 characters, and `-` plus
/// a hash of the cwd is added. This rule comes from observing Claude Code
/// 2.1.280. sessport does not apply it, so for a long cwd sessport writes to a
/// directory that `claude --resume` does not read.
#[must_use]
pub fn encode_project_dir(cwd: &str) -> String {
    let encoded: String = cwd
        .chars()
        .flat_map(|c| {
            let (ch, count) = if c.is_ascii_alphanumeric() {
                (c, 1)
            } else {
                ('-', c.len_utf16())
            };
            iter::repeat_n(ch, count)
        })
        .collect();
    if encoded.len() <= MAX_PROJECT_DIR_LEN {
        return encoded;
    }
    // `encoded` is ASCII, so byte and character positions are the same.
    let hash = u64::from(java_string_hash(cwd).unsigned_abs());
    format!("{}-{}", &encoded[..MAX_PROJECT_DIR_LEN], base36(hash))
}

/// Java's `String.hashCode` over UTF-16 code units: `h = 31 * h + unit`, as i32.
fn java_string_hash(s: &str) -> i32 {
    s.encode_utf16().fold(0i32, |h, unit| {
        h.wrapping_mul(31).wrapping_add(i32::from(unit))
    })
}

fn base36(n: u64) -> String {
    let digits: Vec<char> = iter::successors(Some(n), |&n| (n >= 36).then_some(n / 36))
        .map(|n| char::from_digit((n % 36) as u32, 36).expect("n % 36 is a base-36 digit"))
        .collect();
    digits.into_iter().rev().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_dir_encoding() {
        assert_eq!(
            encode_project_dir("/home/agent/state/workspace/a.b"),
            "-home-agent-state-workspace-a-b"
        );
        assert_eq!(encode_project_dir("/tmp/ü😀"), "-tmp----");
    }

    /// Vectors from a JavaScript implementation of the rule, which matched
    /// the directory names that Claude Code 2.1.280 created for long paths.
    #[test]
    fn long_project_dirs_get_a_hash_suffix() {
        let path = |len: usize| {
            let base = "/home/agent/state/workspace/";
            format!("{base}{}", "x".repeat(len - base.len()))
        };
        // Both signs of the Java hash use its absolute value.
        for (len, suffix) in [
            (200, None),
            (201, Some("v57fd7")),
            (202, Some("sy6jdv")),
            (304, Some("3pontp")),
        ] {
            let cwd = path(len);
            let full = cwd.replace(|c: char| !c.is_ascii_alphanumeric(), "-");
            let expected = match suffix {
                None => full,
                Some(hash) => format!("{}-{hash}", &full[..200]),
            };
            assert_eq!(encode_project_dir(&cwd), expected, "cwd length {len}");
        }
    }

    #[test]
    fn base36_digits() {
        assert_eq!(base36(0), "0");
        assert_eq!(base36(35), "z");
        assert_eq!(base36(36), "10");
        assert_eq!(base36(215_271_542), "3k60l2");
    }
}
