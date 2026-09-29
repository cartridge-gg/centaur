//! The tool-neutral session model that the readers build and the writers render.

use serde_json::Value;

/// A coding-agent CLI whose sessions can be read and written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Tool {
    Codex,
    Claude,
    Hermes,
}

impl Tool {
    /// Short id, as used in `[codex]` session titles.
    #[must_use]
    pub fn id(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
            Self::Hermes => "hermes",
        }
    }

    /// Display name.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Codex => "Codex CLI",
            Self::Claude => "Claude Code",
            Self::Hermes => "Hermes Agent",
        }
    }
}

impl std::fmt::Display for Tool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

impl std::str::FromStr for Tool {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "codex" => Ok(Self::Codex),
            "claude" | "claude-code" | "claudecode" => Ok(Self::Claude),
            "hermes" => Ok(Self::Hermes),
            _ => Err(crate::Error::UnknownTool(s.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    User,
    Assistant,
}

/// The input of a tool call.
#[derive(Debug, Clone, Default, PartialEq)]
pub enum ToolInput {
    /// The source record has no input.
    #[default]
    Missing,
    /// Free-form text, such as a patch or a script. It is shown as is.
    Text(String),
    /// Structured input. It is shown as pretty-printed JSON.
    Json(Value),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Part {
    Text(String),
    /// Reasoning summary text. Encrypted reasoning is never available.
    Thinking(String),
    ToolCall {
        id: String,
        name: String,
        input: ToolInput,
    },
    ToolResult {
        call_id: String,
        output: String,
        is_error: bool,
    },
    /// An image. Image bytes are never copied between tools.
    Image {
        note: String,
    },
}

impl Part {
    #[must_use]
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text(text) => Some(text),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    pub role: Role,
    /// The model that wrote an assistant message.
    pub model: Option<String>,
    pub parts: Vec<Part>,
}

/// Adds a part to a transcript. Consecutive assistant parts share one
/// message, and so do consecutive tool results. `model` goes on a new
/// assistant message.
pub(crate) fn push_part(messages: &mut Vec<Message>, role: Role, part: Part, model: Option<&str>) {
    let is_result = |p: &Part| matches!(p, Part::ToolResult { .. });
    if let Some(last) = messages.last_mut().filter(|m| m.role == role)
        && (role == Role::Assistant || (is_result(&part) && last.parts.iter().all(is_result)))
    {
        last.parts.push(part);
        return;
    }
    let model = match role {
        Role::Assistant => model.map(str::to_owned),
        Role::User => None,
    };
    messages.push(Message {
        role,
        model,
        parts: vec![part],
    });
}

#[derive(Debug, Clone, PartialEq)]
pub struct Session {
    /// The tool that wrote the source session.
    pub source: Tool,
    pub id: String,
    /// The session title, when the source tool stores one.
    pub title: Option<String>,
    /// Version of the tool that wrote the source session.
    pub source_version: Option<String>,
    pub cwd: Option<String>,
    /// First real user prompt on one line, for titles and listings.
    pub preview: Option<String>,
    /// The last model that the session used.
    pub model: Option<String>,
    pub git_branch: Option<String>,
    pub messages: Vec<Message>,
}
