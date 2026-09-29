//! Reads Hermes Agent sessions from its database
//! (`$HERMES_HOME/state.db`, tables `sessions` and `messages`).
//!
//! The reader takes the history that Hermes itself loads for a session:
//!
//! - A compaction with `compression.in_place: false` ends the session and
//!   continues it in a child session. The reader follows that chain to its
//!   newest session, as `session.resume` does (see [`compression_tip`]).
//! - Only rows with `active = 1` count. An in-place compaction keeps the old
//!   rows with `active = 0` and inserts the new history as new rows.
//! - A delegated subagent has its own child session. Its rows are not part of
//!   the parent's history, although both share one `id` sequence.
//!
//! So the transcript has the compaction summary, not the messages that the
//! summary replaced. The Codex reader keeps those messages, because a rollout
//! file still has them once, in order. Hermes keeps copies of them instead,
//! and the copies would repeat messages.
//!
//! Messages use the chat-completions format: `user`, `assistant` with
//! `tool_calls`, and `tool` results. Fields are read leniently: a value of an
//! unexpected type counts as missing.

use std::collections::{BTreeSet, HashSet};
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde_json::{Map, Value};

use crate::error::{Error, Result};
use crate::js;
use crate::model::{Message, Part, Role, Session, Tool, ToolInput, push_part};

/// Hermes stores list and object content as this prefix, then JSON.
const JSON_CONTENT_PREFIX: &str = "\u{0}json:";

/// Hermes writes these user rows itself. They stay in the transcript, but
/// they are not the first prompt of a session.
const SYNTHETIC_USER_PREFIXES: &[&str] = &[
    "[CONTEXT COMPACTION",
    "[CONTEXT SUMMARY]",
    "[PRIOR CONTEXT",
    "[ASYNC DELEGATION",
    "[System note:",
    "[System:",
    "[IMPORTANT:",
];

/// Longest compaction chain that the reader follows, as in Hermes.
const MAX_CHAIN: usize = 100;

/// The database file in the Hermes home directory.
pub const DATABASE_FILE: &str = "state.db";

/// Opens the database at `db` read-only.
///
/// # Errors
///
/// [`Error::OpenDatabase`] if the file cannot be opened.
pub fn open(db: &Path) -> Result<Connection> {
    Connection::open_with_flags(db, OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(|source| {
        Error::OpenDatabase {
            path: db.to_path_buf(),
            source,
        }
    })
}

/// Opens the database at `db` read-only and reads session `id`.
///
/// # Errors
///
/// [`Error::OpenDatabase`] if the file cannot be opened, [`Error::Database`]
/// if a query fails, and [`Error::NotFound`] if the session does not exist.
pub fn read_session(db: &Path, id: &str) -> Result<Session> {
    read_session_from(&open(db)?, id)
}

/// A conversation in a Hermes database: a session that is not the child of
/// another session, seen at the newest session of its compaction chain.
#[derive(Debug, Clone, PartialEq)]
pub struct Conversation {
    /// The newest session of the compaction chain.
    pub id: String,
    /// The first session of the compaction chain.
    pub root_id: String,
    pub cwd: Option<String>,
    pub title: Option<String>,
    /// The first prompt of the newest session, on one line.
    pub preview: Option<String>,
    /// The last activity, in Unix seconds.
    pub last_active: f64,
}

/// The conversations that have at least one message, newest first.
///
/// # Errors
///
/// [`Error::Database`] if a query fails.
pub fn conversations(conn: &Connection) -> Result<Vec<Conversation>> {
    let roots: Vec<String> = conn
        .prepare(
            "SELECT s.id FROM sessions s
             WHERE s.parent_session_id IS NULL
               AND EXISTS (SELECT 1 FROM messages m WHERE m.session_id = s.id)",
        )?
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    let mut details = conn.prepare(&format!(
        "SELECT s.cwd, s.title, {} FROM sessions s WHERE s.id = ?1",
        last_active_sql("s")
    ))?;
    let mut prompts = conn.prepare(
        "SELECT content FROM messages
         WHERE session_id = ?1 AND active = 1 AND role = 'user' ORDER BY id LIMIT 50",
    )?;
    let mut found = Vec::with_capacity(roots.len());
    for root_id in roots {
        let id = compression_tip(conn, &root_id)?;
        let (cwd, title, last_active) = details.query_row([&id], |row| {
            Ok((
                row.get::<_, Option<String>>(0).ok().flatten(),
                row.get::<_, Option<String>>(1).ok().flatten(),
                row.get::<_, Option<f64>>(2).ok().flatten().unwrap_or(0.0),
            ))
        })?;
        let contents: Vec<Option<String>> = prompts
            .query_map([&id], |row| Ok(row.get(0).ok().flatten()))?
            .collect::<rusqlite::Result<_>>()?;
        let parts: Vec<Part> = contents
            .iter()
            .flat_map(|content| content_parts(content.as_deref()))
            .collect();
        found.push(Conversation {
            preview: first_prompt(parts.iter().filter_map(Part::as_text)),
            id,
            root_id,
            cwd,
            title: title.filter(|title| !title.is_empty()),
            last_active,
        });
    }
    found.sort_by(|a, b| b.last_active.total_cmp(&a.last_active));
    Ok(found)
}

/// The session that `query` names: the session with that id, or else the
/// only compaction chain with a session whose id starts with `query`.
///
/// # Errors
///
/// [`Error::NotFound`] if no session matches, [`Error::Ambiguous`] if
/// sessions of more than one chain match, and [`Error::Database`] if a query
/// fails.
pub fn find_session(conn: &Connection, query: &str) -> Result<String> {
    let exact: Option<String> = conn
        .query_row("SELECT id FROM sessions WHERE id = ?1", [query], |row| {
            row.get(0)
        })
        .optional()?;
    if let Some(id) = exact {
        return Ok(id);
    }
    // Not LIKE: `_` in Hermes ids is a LIKE wildcard.
    let hits: Vec<String> = conn
        .prepare("SELECT id FROM sessions WHERE substr(id, 1, length(?1)) = ?1")?
        .query_map([query], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    let tips = hits
        .iter()
        .map(|id| compression_tip(conn, id))
        .collect::<Result<BTreeSet<String>>>()?;
    let mut tips = tips.into_iter();
    match (tips.next(), tips.len()) {
        (None, _) => Err(not_found(conn, query)),
        (Some(tip), 0) => Ok(tip),
        (Some(first), _) => Err(Error::Ambiguous {
            tool: Tool::Hermes,
            prefix: query.to_string(),
            ids: std::iter::once(first).chain(tips).collect(),
        }),
    }
}

/// The error for a session that the database does not have.
fn not_found(conn: &Connection, reference: &str) -> Error {
    Error::NotFound {
        tool: Tool::Hermes,
        reference: reference.to_string(),
        dir: conn.path().map(PathBuf::from).unwrap_or_default(),
    }
}

/// SQL for the last activity of the session `alias`, as in Hermes: the newer
/// of `last_activity_at` and the newest message, or else `started_at`.
fn last_active_sql(alias: &str) -> String {
    format!(
        "COALESCE(
           (SELECT MAX(activity.v) FROM (
              SELECT {alias}.last_activity_at AS v
              UNION ALL
              SELECT (SELECT MAX(m.timestamp) FROM messages m WHERE m.session_id = {alias}.id)
           ) activity),
           {alias}.started_at)"
    )
}

/// Reads session `id` from an open Hermes database. If a compaction continued
/// the session in a child session, the result is the newest session of that
/// chain, and [`Session::id`] is its id.
///
/// # Errors
///
/// [`Error::Database`] if a query fails, and [`Error::NotFound`] if the
/// session does not exist.
pub fn read_session_from(conn: &Connection, id: &str) -> Result<Session> {
    let tip = compression_tip(conn, id)?;
    let header = conn
        .query_row(
            "SELECT title, cwd, model, git_branch FROM sessions WHERE id = ?1",
            [&tip],
            |row| {
                Ok(Header {
                    title: row.get(0).ok().flatten(),
                    cwd: row.get(1).ok().flatten(),
                    model: row.get(2).ok().flatten(),
                    git_branch: row.get(3).ok().flatten(),
                })
            },
        )
        .optional()?
        .ok_or_else(|| not_found(conn, id))?;

    let mut statement = conn.prepare(
        "SELECT role, content, tool_call_id, tool_calls, reasoning, reasoning_content
         FROM messages WHERE session_id = ?1 AND active = 1 ORDER BY id",
    )?;
    let rows = statement.query_map([&tip], |row| {
        Ok(Row {
            role: row.get(0).ok().flatten(),
            content: row.get(1).ok().flatten(),
            tool_call_id: row.get(2).ok().flatten(),
            tool_calls: row.get(3).ok().flatten(),
            reasoning: row.get(4).ok().flatten(),
            reasoning_content: row.get(5).ok().flatten(),
        })
    })?;
    let mut messages = Vec::new();
    for row in rows {
        for (role, part) in row?.into_parts() {
            push_part(&mut messages, role, part, header.model.as_deref());
        }
    }

    Ok(Session {
        source: Tool::Hermes,
        id: tip,
        title: header.title.filter(|title| !title.is_empty()),
        // state.db does not record the Hermes version.
        source_version: None,
        preview: preview(&messages),
        cwd: header.cwd,
        model: header.model,
        git_branch: header.git_branch,
        messages,
    })
}

/// The newest session of the compaction chain that starts at `id`, or `id`
/// itself. A port of `SessionDB.get_compression_tip` in Hermes 0.20.0: follow
/// the children of a session that ended with `compression`, but not branches,
/// delegated subagents or tool sessions. Prefer a child that continues the
/// chain, then a live child, then the one that was active last.
///
/// # Errors
///
/// [`Error::Database`] if a query fails.
pub fn compression_tip(conn: &Connection, id: &str) -> Result<String> {
    let mut statement = conn.prepare(&format!(
        "SELECT child.id
         FROM sessions parent
         JOIN sessions child ON child.parent_session_id = parent.id
         WHERE parent.id = ?1
           AND parent.end_reason = 'compression'
           AND (NOT json_valid(child.model_config)
                OR (json_extract(child.model_config, '$._branched_from') IS NULL
                    AND json_extract(child.model_config, '$._delegate_from') IS NULL))
           AND COALESCE(child.source, '') != 'tool'
         ORDER BY
           CASE
             WHEN child.end_reason = 'compression' THEN 0
             WHEN child.ended_at IS NULL THEN 1
             ELSE 2
           END,
           {} DESC,
           child.started_at DESC,
           child.id DESC
         LIMIT 1",
        last_active_sql("child")
    ))?;
    let mut current = id.to_string();
    let mut seen = HashSet::from([current.clone()]);
    for _ in 0..MAX_CHAIN {
        let child: Option<String> = statement
            .query_row([&current], |row| row.get(0))
            .optional()?;
        match child {
            Some(child) if !child.is_empty() && seen.insert(child.clone()) => current = child,
            _ => break,
        }
    }
    Ok(current)
}

struct Header {
    title: Option<String>,
    cwd: Option<String>,
    model: Option<String>,
    git_branch: Option<String>,
}

/// One row of `messages`.
struct Row {
    role: Option<String>,
    content: Option<String>,
    tool_call_id: Option<String>,
    tool_calls: Option<String>,
    reasoning: Option<String>,
    reasoning_content: Option<String>,
}

impl Row {
    /// The parts that this row adds to the transcript, and who says them.
    fn into_parts(self) -> Vec<(Role, Part)> {
        match self.role.as_deref() {
            Some("user") => content_parts(self.content.as_deref())
                .into_iter()
                .map(|part| (Role::User, part))
                .collect(),
            Some("assistant") => {
                let thinking = self
                    .reasoning
                    .filter(|text| !js::trim(text).is_empty())
                    .or(self.reasoning_content)
                    .filter(|text| !js::trim(text).is_empty())
                    .map(Part::Thinking);
                thinking
                    .into_iter()
                    .chain(content_parts(self.content.as_deref()))
                    .chain(tool_calls(self.tool_calls.as_deref()))
                    .map(|part| (Role::Assistant, part))
                    .collect()
            }
            Some("tool") => {
                let output = content_text(self.content.as_deref());
                let is_error = is_error_output(&output);
                vec![(
                    Role::User,
                    Part::ToolResult {
                        call_id: self.tool_call_id.unwrap_or_default(),
                        output,
                        is_error,
                    },
                )]
            }
            // Hermes keeps the system prompt in `system_prompts`, not here.
            _ => Vec::new(),
        }
    }
}

/// Decodes a `messages.content` value: plain text, or the JSON prefix and a
/// list of parts (or 1 part object).
fn decode_content(raw: &str) -> Value {
    match raw.strip_prefix(JSON_CONTENT_PREFIX) {
        Some(json) => serde_json::from_str(json).unwrap_or_else(|_| Value::String(raw.to_string())),
        None => Value::String(raw.to_string()),
    }
}

/// The text and image parts of a message.
fn content_parts(raw: Option<&str>) -> Vec<Part> {
    let Some(raw) = raw else {
        return Vec::new();
    };
    let value = decode_content(raw);
    let items = match &value {
        Value::Array(items) => items.iter().collect(),
        Value::String(_) | Value::Object(_) => vec![&value],
        _ => Vec::new(),
    };
    items
        .into_iter()
        .filter_map(|item| match item {
            Value::String(text) => Some(Part::Text(text.clone())),
            Value::Object(fields) => content_item(fields),
            _ => None,
        })
        .filter(|part| part.as_text().is_none_or(|text| !text.is_empty()))
        .collect()
}

/// One part of list content: `{"type": "text", "text": …}`, or an image.
fn content_item(fields: &Map<String, Value>) -> Option<Part> {
    match fields.get("type").and_then(Value::as_str) {
        Some("image_url" | "input_image" | "image") => Some(Part::Image {
            note: "image".to_string(),
        }),
        _ => fields
            .get("text")
            .and_then(Value::as_str)
            .map(|text| Part::Text(text.to_string())),
    }
}

/// The content of a tool result as text. An image becomes `[image]`.
fn content_text(raw: Option<&str>) -> String {
    let texts: Vec<String> = content_parts(raw)
        .into_iter()
        .map(|part| match part {
            Part::Text(text) => text,
            _ => "[image]".to_string(),
        })
        .collect();
    texts.join("\n")
}

/// True for a tool result that is a JSON object with a non-empty `error`
/// string, as Hermes tools report a failure. A command that exits with a
/// non-zero code is not an error of the tool call.
fn is_error_output(output: &str) -> bool {
    serde_json::from_str::<Value>(output).is_ok_and(|value| {
        value
            .get("error")
            .and_then(Value::as_str)
            .is_some_and(|error| !error.is_empty())
    })
}

/// The tool calls of an assistant row: a JSON array of
/// `{"id", "call_id", "type": "function", "function": {"name", "arguments"}}`.
fn tool_calls(raw: Option<&str>) -> Vec<Part> {
    let Some(Ok(Value::Array(calls))) = raw.map(serde_json::from_str::<Value>) else {
        return Vec::new();
    };
    calls
        .iter()
        .filter_map(Value::as_object)
        .map(|call| {
            let text = |field: &str| {
                call.get(field)
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned)
            };
            let function = call.get("function").and_then(Value::as_object);
            Part::ToolCall {
                id: text("id").or_else(|| text("call_id")).unwrap_or_default(),
                name: function
                    .and_then(|f| f.get("name"))
                    .and_then(Value::as_str)
                    .unwrap_or("tool")
                    .to_string(),
                input: function
                    .and_then(|f| f.get("arguments"))
                    .map_or(ToolInput::Missing, arguments),
            }
        })
        .collect()
}

/// Function-call `arguments` are a JSON document in a string. A string that is
/// not JSON stays text.
fn arguments(value: &Value) -> ToolInput {
    match value {
        Value::String(raw) => match serde_json::from_str(raw) {
            Ok(Value::String(text)) => ToolInput::Text(text),
            Ok(json) => ToolInput::Json(json),
            Err(_) => ToolInput::Text(raw.clone()),
        },
        json => ToolInput::Json(json.clone()),
    }
}

/// The first prompt that a person wrote, on one line.
fn preview(messages: &[Message]) -> Option<String> {
    first_prompt(
        messages
            .iter()
            .filter(|m| m.role == Role::User)
            .flat_map(|m| m.parts.iter().filter_map(Part::as_text)),
    )
}

/// The first of the user `texts` that a person wrote, on one line.
fn first_prompt<'a>(texts: impl IntoIterator<Item = &'a str>) -> Option<String> {
    texts
        .into_iter()
        .find(|text| {
            let text = js::trim_start(text);
            !text.is_empty() && !SYNTHETIC_USER_PREFIXES.iter().any(|p| text.starts_with(p))
        })
        .map(|text| js::one_line(text, 80))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_content_becomes_text_and_image_parts() {
        let raw = format!(
            "{JSON_CONTENT_PREFIX}{}",
            r#"[{"type":"text","text":"look"},{"type":"image_url","image_url":{"url":"data:image/png;base64,AA"}},{"type":"text","text":""}]"#
        );
        assert_eq!(
            content_parts(Some(&raw)),
            vec![
                Part::Text("look".to_string()),
                Part::Image {
                    note: "image".to_string()
                }
            ]
        );
        assert_eq!(content_text(Some(&raw)), "look\n[image]");
    }

    #[test]
    fn plain_content_is_one_text_part() {
        assert_eq!(
            content_parts(Some("hi")),
            vec![Part::Text("hi".to_string())]
        );
        assert!(content_parts(Some("")).is_empty());
        assert!(content_parts(None).is_empty());
        // Broken JSON after the prefix stays text, as Hermes returns it.
        let broken = format!("{JSON_CONTENT_PREFIX}[");
        assert_eq!(
            content_parts(Some(&broken)),
            vec![Part::Text(broken.clone())]
        );
    }

    #[test]
    fn only_an_error_string_marks_a_failed_tool() {
        assert!(is_error_output(
            r#"{"error": "media file not found", "success": false}"#
        ));
        assert!(!is_error_output(
            r#"{"output": "", "exit_code": 1, "error": null}"#
        ));
        assert!(!is_error_output(r#"{"error": ""}"#));
        assert!(!is_error_output("[terminal] ran `ls` -> exit 1"));
    }

    #[test]
    fn tool_calls_parse_leniently() {
        let raw = r#"[
            {"id": "call_1", "call_id": "call_1", "type": "function",
             "function": {"name": "terminal", "arguments": "{\"command\": \"ls\"}"}},
            {"call_id": "call_2", "function": {"name": "patch", "arguments": "not json"}},
            {"function": {"arguments": {"a": 1}}},
            7
        ]"#;
        assert_eq!(
            tool_calls(Some(raw)),
            vec![
                Part::ToolCall {
                    id: "call_1".to_string(),
                    name: "terminal".to_string(),
                    input: ToolInput::Json(serde_json::json!({"command": "ls"})),
                },
                Part::ToolCall {
                    id: "call_2".to_string(),
                    name: "patch".to_string(),
                    input: ToolInput::Text("not json".to_string()),
                },
                Part::ToolCall {
                    id: String::new(),
                    name: "tool".to_string(),
                    input: ToolInput::Json(serde_json::json!({"a": 1})),
                },
            ]
        );
        assert!(tool_calls(Some("{")).is_empty());
    }
}
