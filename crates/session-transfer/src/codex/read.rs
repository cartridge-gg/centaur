//! Reads Codex CLI rollout files (`~/.codex/sessions/YYYY/MM/DD/rollout-*.jsonl`).
//!
//! Each line is one JSON record: `{"timestamp", "type", "payload"}`. Legacy
//! (2025) rollouts put the session header and the items at the top level.
//!
//! Fields are read leniently, as sessport reads them: a field of an
//! unexpected type counts as missing, and the rest of the record still counts.

use std::fs;
use std::path::Path;
use std::sync::LazyLock;

use regex::Regex;
use serde::de::IgnoredAny;
use serde::{Deserialize, Deserializer};
use serde_json::{Map, Value};

use crate::error::{Error, Result};
use crate::js;
use crate::lenient::{Lenient, lenient, tool_input};
use crate::model::{Message, Part, Role, Session, Tool, ToolInput};
use crate::synthetic::is_synthetic_user_text;

pub static ROLLOUT_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)^rollout-([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}-[0-9]{2}-[0-9]{2})-([0-9a-f-]{36})(?:_[0-9a-f-]+)?\.jsonl(?:\.zst)?$",
    )
    .expect("valid regex")
});

/// Item types that legacy rollouts write at the top level, without a payload.
const LEGACY_ITEM_TYPES: &[&str] = &[
    "message",
    "reasoning",
    "function_call",
    "function_call_output",
    "custom_tool_call",
    "custom_tool_call_output",
    "local_shell_call",
    "web_search_call",
    "tool_search_call",
    "tool_search_output",
];

/// Reads and parses one rollout file.
///
/// # Errors
///
/// [`Error::Compressed`] for a `.jsonl.zst` file, and [`Error::Read`] if the
/// file cannot be read. Malformed lines are skipped, not reported.
pub fn read_session(path: &Path) -> Result<Session> {
    if path.extension().is_some_and(|e| e == "zst") {
        return Err(Error::Compressed(path.to_path_buf()));
    }
    let text = fs::read_to_string(path).map_err(|source| Error::Read {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(parse_records(&parse_jsonl(&text), path))
}

/// Parses JSONL and skips blank and malformed lines.
pub(crate) fn parse_jsonl(text: &str) -> Vec<Value> {
    text.split('\n')
        .map(js::trim)
        .filter(|line| !line.is_empty())
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

/// Builds a session from the records of a rollout file. The file name gives
/// the session id when no record has one.
pub(crate) fn parse_records(records: &[Value], path: &Path) -> Session {
    let mut session = SessionBuilder::default();
    for (index, record) in records.iter().enumerate() {
        if let Some(record) = Record::parse(index, record) {
            session.apply(record);
        }
    }
    session.finish(path)
}

/// True for user text that Codex injects instead of text that a person typed.
fn is_codex_context_text(text: &str) -> bool {
    // Not in sessport. Codex 0.154 adds this list of plugins as a user
    // message, and sessport shows it as the first prompt and the title.
    // Keep scripts/sessport-reference.mjs in sync with this entry.
    is_synthetic_user_text(text) || js::trim_start(text).starts_with("<recommended_plugins>")
}

/// Collects the session header fields and the transcript, record by record.
#[derive(Default)]
struct SessionBuilder {
    id: Option<String>,
    cwd: Option<String>,
    version: Option<String>,
    model: Option<String>,
    git_branch: Option<String>,
    messages: Vec<Message>,
}

impl SessionBuilder {
    fn apply(&mut self, record: Record) {
        match record {
            // The first value of each header field wins.
            Record::SessionMeta(meta) => {
                self.id = self.id.take().or(meta.id);
                self.cwd = self.cwd.take().or(meta.cwd);
                self.version = self.version.take().or(meta.cli_version);
                if let Some(git) = meta.git {
                    self.git_branch = self.git_branch.take().or(git.branch);
                }
            }
            Record::TurnContext(turn) => {
                self.model = turn.model.or(self.model.take());
                self.cwd = self.cwd.take().or(turn.cwd);
            }
            Record::Item(item) => {
                if let Some((role, part)) = item.into_part() {
                    self.push(role, part);
                }
            }
            Record::Compacted(compacted) => {
                if let Some(summary) = compacted.message.filter(|s| !s.is_empty()) {
                    let text = format!("[Earlier conversation was compacted. Summary:]\n{summary}");
                    self.push(Role::User, Part::Text(text));
                }
            }
        }
    }

    /// Adds a part to the transcript. Consecutive assistant parts share one
    /// message, and so do consecutive tool results.
    fn push(&mut self, role: Role, part: Part) {
        let is_result = |p: &Part| matches!(p, Part::ToolResult { .. });
        if let Some(last) = self.messages.last_mut().filter(|m| m.role == role)
            && (role == Role::Assistant || (is_result(&part) && last.parts.iter().all(is_result)))
        {
            last.parts.push(part);
            return;
        }
        let model = match role {
            Role::Assistant => self.model.clone(),
            Role::User => None,
        };
        self.messages.push(Message {
            role,
            model,
            parts: vec![part],
        });
    }

    fn finish(self, path: &Path) -> Session {
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
        let id = self
            .id
            .or_else(|| ROLLOUT_RE.captures(&file_name).map(|c| c[2].to_string()))
            .unwrap_or(file_name);

        Session {
            source: Tool::Codex,
            id,
            title: None,
            source_version: self.version,
            cwd: self.cwd,
            preview,
            model: self.model,
            git_branch: self.git_branch,
            messages: self.messages,
        }
    }
}

/// A rollout record that changes the session.
enum Record {
    SessionMeta(SessionMeta),
    TurnContext(TurnContext),
    Item(Item),
    Compacted(Compacted),
}

impl Record {
    fn parse(index: usize, line: &Value) -> Option<Self> {
        let fields = line.as_object()?;
        let kind = fields.get("type").and_then(Value::as_str);
        let (kind, payload) = match fields.get("payload") {
            Some(payload) => (kind?, payload),
            // Legacy layout: the first line is the session header, and items have no payload.
            None if index == 0 && kind.is_none_or(str::is_empty) && has_id(fields) => {
                ("session_meta", line)
            }
            None if kind.is_some_and(|k| LEGACY_ITEM_TYPES.contains(&k)) => ("response_item", line),
            None => return None,
        };
        // A non-object payload would deserialize as a struct from a sequence.
        if !payload.is_object() {
            return None;
        }
        match kind {
            "session_meta" => SessionMeta::deserialize(payload)
                .ok()
                .map(Self::SessionMeta),
            "turn_context" => TurnContext::deserialize(payload)
                .ok()
                .map(Self::TurnContext),
            "response_item" => Item::deserialize(payload).ok().map(Self::Item),
            "compacted" => Compacted::deserialize(payload).ok().map(Self::Compacted),
            _ => None,
        }
    }
}

fn has_id(fields: &Map<String, Value>) -> bool {
    fields
        .get("id")
        .and_then(Value::as_str)
        .is_some_and(|id| !id.is_empty())
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct SessionMeta {
    #[serde(deserialize_with = "lenient")]
    id: Option<String>,
    #[serde(deserialize_with = "lenient")]
    cwd: Option<String>,
    #[serde(deserialize_with = "lenient")]
    cli_version: Option<String>,
    #[serde(deserialize_with = "lenient")]
    git: Option<Git>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct Git {
    #[serde(deserialize_with = "lenient")]
    branch: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct TurnContext {
    #[serde(deserialize_with = "lenient")]
    model: Option<String>,
    #[serde(deserialize_with = "lenient")]
    cwd: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct Compacted {
    /// The summary. Newer Codex versions encrypt it and leave this empty.
    #[serde(deserialize_with = "lenient")]
    message: Option<String>,
}

/// An item of the model history (`response_item`).
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Item {
    Message(MessageItem),
    Reasoning(Reasoning),
    FunctionCall(FunctionCall),
    CustomToolCall(CustomToolCall),
    LocalShellCall(ActionCall),
    WebSearchCall(ActionCall),
    FunctionCallOutput(CallOutput),
    CustomToolCallOutput(CallOutput),
    #[serde(other)]
    Other,
}

impl Item {
    /// The part that this item adds to the transcript, and who says it.
    fn into_part(self) -> Option<(Role, Part)> {
        let assistant = |part| Some((Role::Assistant, part));
        match self {
            Self::Message(message) => {
                let text = message.content.map(Content::into_text).unwrap_or_default();
                match message.role? {
                    MessageRole::User if !is_codex_context_text(&text) => {
                        Some((Role::User, Part::Text(text)))
                    }
                    MessageRole::Assistant if !text.is_empty() => assistant(Part::Text(text)),
                    _ => None,
                }
            }
            Self::Reasoning(reasoning) => {
                let summary = reasoning.text();
                if js::trim(&summary).is_empty() {
                    return None;
                }
                assistant(Part::Thinking(summary))
            }
            Self::FunctionCall(call) => assistant(Part::ToolCall {
                id: call.call_id.unwrap_or_default(),
                name: call.name.unwrap_or_else(|| "function".to_string()),
                input: call.arguments,
            }),
            Self::CustomToolCall(call) => assistant(Part::ToolCall {
                id: call.call_id.unwrap_or_default(),
                name: call.name.unwrap_or_else(|| "tool".to_string()),
                input: call.input,
            }),
            Self::LocalShellCall(call) => assistant(Part::ToolCall {
                id: call.call_id.or(call.id).unwrap_or_default(),
                name: "local_shell".to_string(),
                input: ToolInput::Json(Value::Object(call.action.unwrap_or_default())),
            }),
            Self::WebSearchCall(call) => assistant(Part::ToolCall {
                id: call.id.unwrap_or_default(),
                name: "web_search".to_string(),
                input: ToolInput::Json(Value::Object(call.action.unwrap_or_default())),
            }),
            Self::FunctionCallOutput(result) | Self::CustomToolCallOutput(result) => Some((
                Role::User,
                Part::ToolResult {
                    call_id: result.call_id.unwrap_or_default(),
                    output: result.output.map(Output::into_text).unwrap_or_default(),
                    is_error: false,
                },
            )),
            Self::Other => None,
        }
    }
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct MessageItem {
    /// Other roles (`developer`, `system`) hold instructions, not conversation.
    #[serde(deserialize_with = "lenient")]
    role: Option<MessageRole>,
    content: Option<Content>,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum MessageRole {
    User,
    Assistant,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct Reasoning {
    #[serde(deserialize_with = "lenient")]
    summary: Option<Vec<Lenient<SummaryText>>>,
}

impl Reasoning {
    fn text(self) -> String {
        let lines: Vec<String> = self
            .summary
            .unwrap_or_default()
            .into_iter()
            .map(|s| s.into_option().and_then(|s| s.text).unwrap_or_default())
            .collect();
        lines.join("\n")
    }
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct SummaryText {
    #[serde(deserialize_with = "lenient")]
    text: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct FunctionCall {
    #[serde(deserialize_with = "lenient")]
    name: Option<String>,
    #[serde(deserialize_with = "lenient")]
    call_id: Option<String>,
    #[serde(deserialize_with = "json_arguments")]
    arguments: ToolInput,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct CustomToolCall {
    #[serde(deserialize_with = "lenient")]
    name: Option<String>,
    #[serde(deserialize_with = "lenient")]
    call_id: Option<String>,
    #[serde(deserialize_with = "tool_input")]
    input: ToolInput,
}

/// A built-in call that describes its input as an `action` object.
#[derive(Deserialize, Default)]
#[serde(default)]
struct ActionCall {
    #[serde(deserialize_with = "lenient")]
    call_id: Option<String>,
    #[serde(deserialize_with = "lenient")]
    id: Option<String>,
    #[serde(deserialize_with = "lenient")]
    action: Option<Map<String, Value>>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct CallOutput {
    #[serde(deserialize_with = "lenient")]
    call_id: Option<String>,
    output: Option<Output>,
}

/// Message content: a string, or a list of text and image items.
#[derive(Deserialize)]
#[serde(untagged)]
enum Content {
    Text(String),
    Items(Vec<Lenient<ContentItem>>),
    Other(IgnoredAny),
}

impl Content {
    fn into_text(self) -> String {
        match self {
            Self::Text(text) => text,
            Self::Items(items) => {
                let texts: Vec<String> = items
                    .into_iter()
                    .filter_map(|item| item.into_option()?.into_text())
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
enum ContentItem {
    InputText(TextItem),
    OutputText(TextItem),
    Text(TextItem),
    InputImage {},
    #[serde(other)]
    Other,
}

impl ContentItem {
    fn into_text(self) -> Option<String> {
        match self {
            Self::InputText(item) | Self::OutputText(item) | Self::Text(item) => {
                Some(item.text.unwrap_or_default())
            }
            Self::InputImage {} => Some("[image]".to_string()),
            Self::Other => None,
        }
    }
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct TextItem {
    #[serde(deserialize_with = "lenient")]
    text: Option<String>,
}

/// The output of a tool call.
#[derive(Deserialize)]
#[serde(untagged)]
enum Output {
    Text(String),
    Items(Vec<Lenient<ContentItem>>),
    Wrapped {
        #[serde(default)]
        content: Option<Content>,
    },
    Other(IgnoredAny),
}

impl Output {
    fn into_text(self) -> String {
        match self {
            // Older rollouts stored `{"output": "...", "metadata": {...}}` as a JSON string.
            Self::Text(text) => match serde_json::from_str::<Value>(&text) {
                Ok(Value::Object(mut fields)) => match fields.remove("output") {
                    Some(Value::String(inner)) => inner,
                    _ => text,
                },
                _ => text,
            },
            Self::Items(items) => Content::Items(items).into_text(),
            Self::Wrapped { content } => content.map(Content::into_text).unwrap_or_default(),
            Self::Other(_) => String::new(),
        }
    }
}

/// Function-call `arguments` are a JSON document in a string. A string that is
/// not JSON stays text.
fn json_arguments<'de, D: Deserializer<'de>>(deserializer: D) -> Result<ToolInput, D::Error> {
    Ok(match Value::deserialize(deserializer)? {
        Value::String(raw) => match serde_json::from_str(&raw) {
            Ok(Value::String(text)) => ToolInput::Text(text),
            Ok(json) => ToolInput::Json(json),
            Err(_) => ToolInput::Text(raw),
        },
        json => ToolInput::Json(json),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recommended_plugins_is_not_a_prompt() {
        // Codex 0.154 writes the plugin list as the first user message.
        let lines: Vec<Value> = [
            r#"{"type":"session_meta","payload":{"id":"t1","cwd":"/work/app","cli_version":"0.154.0"}}"#,
            r#"{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"<recommended_plugins>\n- Example (example@remote)\n</recommended_plugins>"}]}}"#,
            r#"{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"Read TASK.md and do what it says."}]}}"#,
        ]
        .iter()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
        let session = parse_records(&lines, Path::new("rollout.jsonl"));
        assert_eq!(
            session.preview.as_deref(),
            Some("Read TASK.md and do what it says.")
        );
        assert_eq!(session.source_version.as_deref(), Some("0.154.0"));
        assert!(
            session
                .messages
                .iter()
                .flat_map(|m| &m.parts)
                .filter_map(Part::as_text)
                .all(|text| !text.contains("<recommended_plugins>"))
        );
    }

    #[test]
    fn codex_context_messages_are_synthetic() {
        assert!(is_codex_context_text(
            "\n<recommended_plugins>\n- A (a@remote)"
        ));
        assert!(is_codex_context_text("<environment_context>"));
        assert!(!is_codex_context_text(
            "<turn_aborted>\nThe user interrupted the previous turn"
        ));
        assert!(!is_codex_context_text(
            "fix the <recommended_plugins> parser"
        ));
    }

    #[test]
    fn fields_of_unexpected_type_count_as_missing() {
        let line: Value = serde_json::from_str(
            r#"{"type":"response_item","payload":{"type":"function_call","name":7,"call_id":["x"],"arguments":null}}"#,
        )
        .unwrap();
        let Some(Record::Item(item)) = Record::parse(1, &line) else {
            panic!("the item must parse");
        };
        let part = item.into_part().map(|(_, part)| part);
        assert_eq!(
            part,
            Some(Part::ToolCall {
                id: String::new(),
                name: "function".to_string(),
                input: ToolInput::Json(Value::Null),
            })
        );
    }
}
