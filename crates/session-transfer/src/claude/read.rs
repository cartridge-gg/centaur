//! Reads Claude Code session files (`~/.claude/projects/<encoded-cwd>/<id>.jsonl`).
//!
//! Each line is one record. Conversation records (`user`, `assistant`) link
//! to their parent through `parentUuid`. A rewind or an edited message leaves
//! a dead branch in the file, so the reader follows the chain back from the
//! newest message. Claude Code writes one line per content block, and lines
//! that share `message.id` belong to one assistant message.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::sync::LazyLock;

use regex::Regex;
use serde::Deserialize;
use serde::de::IgnoredAny;
use serde_json::Value;

use crate::error::{Error, Result};
use crate::js;
use crate::lenient::{Lenient, lenient, tool_input};
use crate::model::{Message, Part, Role, Session, Tool, ToolInput};
use crate::synthetic::is_synthetic_user_text;

static UUID_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
        .expect("valid regex")
});

/// Reads and parses one session file.
///
/// # Errors
///
/// [`Error::Read`] if the file cannot be read. Malformed lines are skipped,
/// not reported.
pub fn read_session(path: &Path) -> Result<Session> {
    let text = fs::read_to_string(path).map_err(|source| Error::Read {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(parse_records(&crate::codex::parse_jsonl(&text), path))
}

/// Builds a session from the records of a session file. The file name gives
/// the session id when it is a UUID.
pub(crate) fn parse_records(lines: &[Value], path: &Path) -> Session {
    let records: Vec<Record> = lines
        .iter()
        .filter(|line| line.is_object())
        .map(|line| Record::deserialize(line).unwrap_or_default())
        .collect();

    let mut session = SessionBuilder::default();
    for record in active_chain(&records) {
        session.apply(record);
    }
    session.finish(title(&records), path)
}

/// The conversation that the newest message belongs to, oldest first.
fn active_chain(records: &[Record]) -> Vec<&Record> {
    // A repeated uuid resolves to its last record, as in a JavaScript Map.
    let by_uuid: HashMap<&str, &Record> = records
        .iter()
        .filter_map(|r| Some((r.uuid.as_deref()?, r)))
        .collect();
    let conversational: Vec<&Record> = records
        .iter()
        .filter(|r| r.is_conversational() && r.is_sidechain != Some(true))
        .collect();
    let Some(&leaf) = conversational.last() else {
        return Vec::new();
    };

    let mut chain = Vec::new();
    let mut seen = HashSet::new();
    let mut current = Some(leaf);
    while let Some(record) = current {
        let Some(id) = record.uuid.as_deref().filter(|id| !id.is_empty()) else {
            break;
        };
        if !seen.insert(id) {
            break;
        }
        chain.push(record);
        current = record
            .parent_uuid
            .as_deref()
            .filter(|parent| !parent.is_empty())
            .and_then(|parent| by_uuid.get(parent).copied());
    }
    chain.reverse();
    chain.retain(|r| r.is_conversational());

    // A broken chain (for example after a compaction) can be much shorter
    // than the file. Then the file order is the better guess.
    if chain.len() * 2 < conversational.len() {
        conversational
    } else {
        chain
    }
}

/// The session title: a custom title wins, then the last AI title, then a
/// summary record.
fn title(records: &[Record]) -> Option<String> {
    let mut custom_title = None;
    let mut title: Option<String> = None;
    for record in records {
        match record.kind.as_deref() {
            Some("custom-title") => custom_title = record.custom_title.clone().or(custom_title),
            Some("ai-title") => title = record.ai_title.clone().or(title),
            Some("summary") if title.as_deref().is_none_or(str::is_empty) => {
                title = record.summary.clone();
            }
            _ => {}
        }
    }
    custom_title.or(title)
}

/// Collects the session header fields and the transcript, record by record.
#[derive(Default)]
struct SessionBuilder {
    id: Option<String>,
    cwd: Option<String>,
    git_branch: Option<String>,
    version: Option<String>,
    model: Option<String>,
    messages: Vec<Message>,
    last_assistant_id: Option<String>,
}

impl SessionBuilder {
    fn apply(&mut self, record: &Record) {
        // The first cwd and session id win; the last branch and version win.
        self.cwd = self.cwd.take().or_else(|| record.cwd.clone());
        self.id = self.id.take().or_else(|| record.session_id.clone());
        self.git_branch = record.git_branch.clone().or(self.git_branch.take());
        self.version = record.version.clone().or(self.version.take());

        let message = record.message.as_ref();
        if record.kind.as_deref() == Some("assistant") {
            let id = message.and_then(|m| m.id.clone());
            let model = message.and_then(|m| m.model.clone());
            if let Some(model) = model
                .as_deref()
                .filter(|m| !m.is_empty() && *m != "<synthetic>")
            {
                self.model = Some(model.to_string());
            }
            // Claude Code saves a failed API request as an assistant message
            // with the error text. It is not an answer of the model.
            let parts = if record.is_api_error_message == Some(true) {
                Vec::new()
            } else {
                message.map(assistant_parts).unwrap_or_default()
            };
            let continues =
                id.as_ref().is_some_and(|id| !id.is_empty()) && id == self.last_assistant_id;
            match self.messages.last_mut() {
                Some(last) if last.role == Role::Assistant && continues => last.parts.extend(parts),
                _ if !parts.is_empty() => self.messages.push(Message {
                    role: Role::Assistant,
                    model,
                    parts,
                }),
                _ => {}
            }
            self.last_assistant_id = id;
        } else {
            self.last_assistant_id = None;
            let parts = if record.is_meta == Some(true) {
                Vec::new()
            } else {
                message.map(user_parts).unwrap_or_default()
            };
            if !parts.is_empty() {
                self.messages.push(Message {
                    role: Role::User,
                    model: None,
                    parts,
                });
            }
        }
    }

    fn finish(self, title: Option<String>, path: &Path) -> Session {
        let preview = self
            .messages
            .iter()
            .filter(|m| m.role == Role::User)
            .map(|m| m.parts.iter().filter_map(Part::as_text).collect::<Vec<_>>())
            .find(|texts| !texts.is_empty())
            .map(|texts| js::one_line(&texts.join(" "), 80));

        let file_name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let stem = file_name.strip_suffix(".jsonl").unwrap_or(&file_name);
        // `claude --resume` finds a session by its file name.
        let id = if UUID_RE.is_match(stem) {
            stem.to_string()
        } else {
            self.id.unwrap_or(file_name)
        };

        Session {
            source: Tool::Claude,
            id,
            title,
            source_version: self.version,
            cwd: self.cwd,
            preview,
            model: self.model,
            git_branch: self.git_branch,
            messages: self.messages,
        }
    }
}

fn user_parts(message: &MessageBody) -> Vec<Part> {
    match &message.content {
        Some(Content::Text(text)) if is_synthetic_user_text(text) => Vec::new(),
        Some(Content::Text(text)) => vec![Part::Text(text.clone())],
        Some(Content::Blocks(blocks)) => blocks
            .iter()
            .filter_map(Lenient::as_valid)
            .filter_map(|block| match block {
                Block::Text(item) => {
                    let text = item.text.clone().unwrap_or_default();
                    (!is_synthetic_user_text(&text)).then_some(Part::Text(text))
                }
                Block::Image {} => Some(Part::Image {
                    note: "image attached by user".to_string(),
                }),
                Block::ToolResult(result) => Some(Part::ToolResult {
                    call_id: result.tool_use_id.clone().unwrap_or_default(),
                    output: result
                        .content
                        .as_ref()
                        .map(ResultContent::text)
                        .unwrap_or_default(),
                    is_error: result.is_error == Some(true),
                }),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn assistant_parts(message: &MessageBody) -> Vec<Part> {
    match &message.content {
        Some(Content::Text(text)) if text.is_empty() => Vec::new(),
        Some(Content::Text(text)) => vec![Part::Text(text.clone())],
        Some(Content::Blocks(blocks)) => blocks
            .iter()
            .filter_map(Lenient::as_valid)
            .filter_map(|block| match block {
                Block::Text(item) => item.text.clone().filter(|t| !t.is_empty()).map(Part::Text),
                Block::Thinking(item) => item
                    .thinking
                    .clone()
                    .filter(|t| !t.is_empty())
                    .map(Part::Thinking),
                Block::ToolUse(call) | Block::ServerToolUse(call) => Some(Part::ToolCall {
                    id: call.id.clone().unwrap_or_default(),
                    name: call.name.clone().unwrap_or_else(|| "tool".to_string()),
                    input: call.input.clone(),
                }),
                Block::WebSearchToolResult(result) | Block::WebFetchToolResult(result) => {
                    let kind = match block {
                        Block::WebSearchToolResult(_) => "web_search_tool_result",
                        _ => "web_fetch_tool_result",
                    };
                    Some(Part::ToolResult {
                        call_id: result.tool_use_id.clone().unwrap_or_default(),
                        output: format!("[{kind}]"),
                        is_error: false,
                    })
                }
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// One line of a session file. Only the fields the conversion needs.
#[derive(Deserialize, Default)]
#[serde(default, rename_all = "camelCase")]
struct Record {
    #[serde(rename = "type", deserialize_with = "lenient")]
    kind: Option<String>,
    #[serde(deserialize_with = "lenient")]
    uuid: Option<String>,
    #[serde(deserialize_with = "lenient")]
    parent_uuid: Option<String>,
    #[serde(deserialize_with = "lenient")]
    is_sidechain: Option<bool>,
    #[serde(deserialize_with = "lenient")]
    is_meta: Option<bool>,
    #[serde(deserialize_with = "lenient")]
    is_api_error_message: Option<bool>,
    #[serde(deserialize_with = "lenient")]
    cwd: Option<String>,
    #[serde(deserialize_with = "lenient")]
    git_branch: Option<String>,
    #[serde(deserialize_with = "lenient")]
    version: Option<String>,
    #[serde(deserialize_with = "lenient")]
    session_id: Option<String>,
    #[serde(deserialize_with = "lenient")]
    message: Option<MessageBody>,
    #[serde(deserialize_with = "lenient")]
    custom_title: Option<String>,
    #[serde(deserialize_with = "lenient")]
    ai_title: Option<String>,
    #[serde(deserialize_with = "lenient")]
    summary: Option<String>,
}

impl Record {
    fn is_conversational(&self) -> bool {
        matches!(self.kind.as_deref(), Some("user" | "assistant"))
    }
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct MessageBody {
    #[serde(deserialize_with = "lenient")]
    id: Option<String>,
    #[serde(deserialize_with = "lenient")]
    model: Option<String>,
    content: Option<Content>,
}

/// Message content: a string, or a list of content blocks.
#[derive(Deserialize)]
#[serde(untagged)]
enum Content {
    Text(String),
    Blocks(Vec<Lenient<Block>>),
    Other(IgnoredAny),
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Block {
    Text(TextBlock),
    Thinking(ThinkingBlock),
    Image {},
    ToolUse(ToolUse),
    ServerToolUse(ToolUse),
    ToolResult(ToolResult),
    WebSearchToolResult(ServerToolResult),
    WebFetchToolResult(ServerToolResult),
    #[serde(other)]
    Other,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct TextBlock {
    #[serde(deserialize_with = "lenient")]
    text: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct ThinkingBlock {
    #[serde(deserialize_with = "lenient")]
    thinking: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct ToolUse {
    #[serde(deserialize_with = "lenient")]
    id: Option<String>,
    #[serde(deserialize_with = "lenient")]
    name: Option<String>,
    #[serde(deserialize_with = "tool_input")]
    input: ToolInput,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct ToolResult {
    #[serde(deserialize_with = "lenient")]
    tool_use_id: Option<String>,
    content: Option<ResultContent>,
    #[serde(deserialize_with = "lenient")]
    is_error: Option<bool>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct ServerToolResult {
    #[serde(deserialize_with = "lenient")]
    tool_use_id: Option<String>,
}

/// The content of a tool result: text, or a list of text and image items.
#[derive(Deserialize)]
#[serde(untagged)]
enum ResultContent {
    Text(String),
    Items(Vec<Lenient<ResultItem>>),
    Other(IgnoredAny),
}

impl ResultContent {
    fn text(&self) -> String {
        match self {
            Self::Text(text) => text.clone(),
            Self::Items(items) => {
                let texts: Vec<String> = items
                    .iter()
                    .filter_map(Lenient::as_valid)
                    .map(ResultItem::text)
                    .filter(|text| !text.is_empty())
                    .collect();
                texts.join("\n")
            }
            Self::Other(_) => String::new(),
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResultItem {
    Text(TextBlock),
    Image {},
    ToolReference(ToolReference),
    #[serde(other)]
    Other,
}

impl ResultItem {
    fn text(&self) -> String {
        match self {
            Self::Text(item) => item.text.clone().unwrap_or_default(),
            Self::Image {} => "[image]".to_string(),
            Self::ToolReference(reference) => format!(
                "[tool reference: {}]",
                reference.tool_name.as_deref().unwrap_or("?")
            ),
            Self::Other => String::new(),
        }
    }
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct ToolReference {
    #[serde(deserialize_with = "lenient")]
    tool_name: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parse(lines: &[Value]) -> Session {
        parse_records(
            lines,
            Path::new("11111111-2222-4333-8444-555555555555.jsonl"),
        )
    }

    fn user(uuid: &str, parent: Option<&str>, text: &str) -> Value {
        json!({"type": "user", "uuid": uuid, "parentUuid": parent, "sessionId": "s",
               "cwd": "/work/app", "message": {"role": "user", "content": text}})
    }

    fn assistant(uuid: &str, parent: &str, id: &str, text: &str) -> Value {
        json!({"type": "assistant", "uuid": uuid, "parentUuid": parent, "sessionId": "s",
               "message": {"id": id, "role": "assistant", "model": "claude-x",
                           "content": [{"type": "text", "text": text}]}})
    }

    fn texts(session: &Session) -> Vec<String> {
        session
            .messages
            .iter()
            .flat_map(|m| &m.parts)
            .filter_map(Part::as_text)
            .map(str::to_string)
            .collect()
    }

    #[test]
    fn follows_the_active_branch_after_a_rewind() {
        let session = parse(&[
            user("u1", None, "first"),
            assistant("a1", "u1", "m1", "old answer"),
            // The user rewound to u1 and asked again.
            user("u2", Some("u1"), "second try"),
            assistant("a2", "u2", "m2", "new answer"),
        ]);
        assert_eq!(texts(&session), ["first", "second try", "new answer"]);
        assert_eq!(session.id, "11111111-2222-4333-8444-555555555555");
        assert_eq!(session.model.as_deref(), Some("claude-x"));
    }

    #[test]
    fn lines_with_one_message_id_form_one_message() {
        let session = parse(&[
            user("u1", None, "hi"),
            assistant("a1", "u1", "m1", "part one"),
            assistant("a2", "a1", "m1", "part two"),
        ]);
        assert_eq!(session.messages.len(), 2);
        assert_eq!(session.messages[1].parts.len(), 2);
    }

    #[test]
    fn falls_back_to_file_order_when_the_chain_is_broken() {
        let session = parse(&[
            user("u1", None, "one"),
            assistant("a1", "u1", "m1", "two"),
            user("u2", Some("missing"), "three"),
        ]);
        // The chain from u2 has 1 of 3 messages, so the file order wins.
        assert_eq!(texts(&session), ["one", "two", "three"]);
    }

    #[test]
    fn a_failed_api_request_is_not_an_answer() {
        let mut error = assistant("a1", "u1", "m1", "API Error: 503 no capacity");
        error["isApiErrorMessage"] = json!(true);
        error["message"]["model"] = json!("<synthetic>");
        let session = parse(&[
            user("u1", None, "hi"),
            error,
            user("u2", Some("a1"), "again"),
            assistant("a2", "u2", "m2", "hello"),
        ]);
        // The chain goes through the error record, but the error text is left out.
        assert_eq!(texts(&session), ["hi", "again", "hello"]);
        assert_eq!(session.messages.len(), 3);
    }
}
