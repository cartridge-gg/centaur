//! Turns a session into alternating user and assistant text turns.
//!
//! Tool calls become tagged text on purpose. Claude Code has its own tool set,
//! and a foreign tool call replayed as a native `tool_use` block can make the
//! API reject the history. Text keeps everything the model needs to continue.

use std::borrow::Cow;
use std::iter;
use std::sync::LazyLock;

use regex::Regex;

use crate::js;
use crate::model::{Part, Role, Session, Tool, ToolInput};
use crate::redact::redact_text;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrepareOptions {
    /// Cut each tool call and tool output to this many UTF-16 code units.
    /// `None` keeps them whole.
    pub max_tool_output: Option<usize>,
    /// Keep only the last N source messages. `None` keeps all of them.
    pub last_messages: Option<usize>,
    /// Include reasoning summaries as `<previous_thinking>` text.
    pub keep_thinking: bool,
    /// Scrub likely secrets from the converted text.
    pub redact: bool,
    /// Tool name in the preamble, message ids and truncation notes.
    pub brand: &'static str,
}

impl Default for PrepareOptions {
    fn default() -> Self {
        Self {
            max_tool_output: Some(4000),
            last_messages: None,
            keep_thinking: false,
            redact: true,
            brand: crate::BRAND,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedMessage {
    pub role: Role,
    pub model: Option<String>,
    pub text: String,
}

/// Renders `session` as text turns for `target`.
pub(crate) fn prepare_messages(
    session: &Session,
    target: Tool,
    options: &PrepareOptions,
) -> Vec<PreparedMessage> {
    let skip = options
        .last_messages
        .map_or(0, |n| session.messages.len().saturating_sub(n));
    let rendered = session.messages[skip..].iter().filter_map(|message| {
        let chunks: Vec<Cow<str>> = message
            .parts
            .iter()
            .filter_map(|part| render_part(part, options))
            .collect();
        let joined = chunks.join("\n\n");
        let text = if options.redact {
            redact_text(&joined)
        } else {
            joined
        };
        let text = js::trim(&text);
        (!text.is_empty()).then(|| PreparedMessage {
            role: message.role,
            model: message.model.clone(),
            text: text.to_string(),
        })
    });

    // Merge consecutive messages with the same role so the history alternates.
    let mut merged: Vec<PreparedMessage> = Vec::new();
    for message in iter::once(preamble(session, target, options.brand)).chain(rendered) {
        match merged.last_mut() {
            Some(last) if last.role == message.role => {
                last.text.push_str("\n\n");
                last.text.push_str(&message.text);
                last.model = last.model.take().or(message.model);
            }
            _ => merged.push(message),
        }
    }
    merged
}

fn preamble(session: &Session, target: Tool, brand: &str) -> PreparedMessage {
    let cwd = match session.cwd.as_deref() {
        Some(cwd) if !cwd.is_empty() => format!(" (cwd: {cwd})"),
        _ => String::new(),
    };
    let text = format!(
        "[{brand}] This conversation was imported from {source} into {target}.\n\
         Original session: {id}{cwd}.\n\
         Tool calls and their results from the original session are shown as plain text; \
         they were already executed there.\n\
         Use your own tools from here on, and re-check files before editing because they may have changed.",
        id = session.id,
        source = session.source.label(),
        target = target.label(),
    );
    PreparedMessage {
        role: Role::User,
        model: None,
        text,
    }
}

/// The preamble that [`preamble`] writes, with any brand, tools and session.
static IMPORT_PREAMBLE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(concat!(
        r"^\[[^\]\n]+\] This conversation was imported from [^\n]+ into [^\n]+\.\n",
        r"Original session: [^\n]*\.\n",
        r"Tool calls and their results from the original session are shown as plain text; ",
        r"they were already executed there\.\n",
        r"Use your own tools from here on, and re-check files before editing ",
        r"because they may have changed\.(?:\n\n|$)",
    ))
    .expect("valid regex")
});

/// Removes the preamble of an earlier import from the start of `text`, so
/// that a session converted twice (Codex to Claude to Codex) has one preamble.
fn strip_import_preamble(text: &str) -> &str {
    IMPORT_PREAMBLE
        .find(text)
        .map_or(text, |preamble| &text[preamble.end()..])
}

/// The text of one part, or `None` when the part adds nothing.
fn render_part<'a>(part: &'a Part, options: &PrepareOptions) -> Option<Cow<'a, str>> {
    let max = options.max_tool_output;
    let rendered = match part {
        Part::Text(text) => {
            let text = strip_import_preamble(text);
            return (!text.is_empty()).then_some(Cow::Borrowed(text));
        }
        Part::Thinking(text) if options.keep_thinking && !js::trim(text).is_empty() => {
            format!("<previous_thinking>\n{text}\n</previous_thinking>")
        }
        Part::Thinking(_) => return None,
        Part::ToolCall { id, name, input } => format!(
            "<tool_call name=\"{name}\" id=\"{id}\">\n{}\n</tool_call>",
            truncate(&input_text(input), max, options.brand)
        ),
        Part::ToolResult {
            call_id,
            output,
            is_error,
        } => format!(
            "<tool_result for=\"{call_id}\"{}>\n{}\n</tool_result>",
            if *is_error { " error=\"true\"" } else { "" },
            truncate(output, max, options.brand)
        ),
        Part::Image { note } => format!("[image omitted: {note}]"),
    };
    Some(Cow::Owned(rendered))
}

fn input_text(input: &ToolInput) -> Cow<'_, str> {
    match input {
        ToolInput::Missing => Cow::Borrowed(""),
        ToolInput::Text(text) => Cow::Borrowed(text),
        ToolInput::Json(json) => Cow::Owned(js::stringify_pretty(json)),
    }
}

/// Cuts `text` to `max` UTF-16 code units and says how much was cut.
fn truncate<'a>(text: &'a str, max: Option<usize>, brand: &str) -> Cow<'a, str> {
    let len = js::utf16_len(text);
    match max {
        Some(max) if len > max => Cow::Owned(format!(
            "{}\n… [{} more characters truncated by {brand}]",
            js::utf16_prefix(text, max),
            len - max
        )),
        _ => Cow::Borrowed(text),
    }
}
