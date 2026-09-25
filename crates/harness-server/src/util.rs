use std::env;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command as ProcessCommand;
use std::time::{SystemTime, UNIX_EPOCH};

use codex_app_server_protocol::UserInput;
use codex_utils_absolute_path::AbsolutePathBuf;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{HarnessServerError, Result};

pub(crate) fn command_from_override(env_key: &str) -> Option<ProcessCommand> {
    let raw = env::var(env_key).ok()?;
    if raw.trim().is_empty() {
        return None;
    }
    let mut command = ProcessCommand::new("sh");
    command.arg("-lc").arg(raw);
    Some(command)
}

pub(crate) fn user_input_to_anthropic_content(input: &[UserInput]) -> Vec<Value> {
    input
        .iter()
        .map(|item| match item {
            UserInput::Text { text, .. } => json!({"type": "text", "text": text}),
            UserInput::Image { url, .. } => json!({
                "type": "text",
                "text": format!("[image: {url}]"),
            }),
            UserInput::LocalImage { path, .. } => json!({
                "type": "text",
                "text": format!("[local image: {}]", path.display()),
            }),
            UserInput::Skill { name, path } => json!({
                "type": "text",
                "text": format!("[skill: {name} at {}]", path.display()),
            }),
            UserInput::Mention { name, path } => json!({
                "type": "text",
                "text": format!("[mention: {name} at {path}]"),
            }),
        })
        .collect()
}

/// True for an env flag set to 1, true, yes or on.
pub(crate) fn env_flag_enabled(value: Option<&str>) -> bool {
    matches!(
        value.map(str::trim).map(str::to_ascii_lowercase).as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

pub(crate) fn write_value<W: Write>(stdout: &mut W, value: &Value) -> Result<()> {
    // Every output line passes here, so this is where a provider exhaustion
    // gets its machine-readable annotation.
    let annotated = crate::failover::annotate(value);
    serde_json::to_writer(&mut *stdout, annotated.as_ref().unwrap_or(value))?;
    stdout.write_all(b"\n")?;
    stdout.flush()?;
    Ok(())
}

pub(crate) fn absolute_path(path: PathBuf) -> Result<AbsolutePathBuf> {
    let path = if path.is_absolute() {
        path
    } else {
        env::current_dir()?.join(path)
    };
    AbsolutePathBuf::from_absolute_path(&path)
        .map_err(|_| HarnessServerError::PathMustBeAbsolute { path })
}

pub(crate) fn default_codex_home() -> PathBuf {
    env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join(".codex")
}

/// The part of `current` that a client that already has `previous` still
/// needs. A final text can drop the leading whitespace of the streamed text
/// (Hermes trims its final message). When `current` does not continue
/// `previous`, the delta is empty: resending all of `current` would show the
/// text twice, and the completed item carries the full text anyway.
pub(crate) fn suffix_delta(previous: &str, current: &str) -> String {
    current
        .strip_prefix(previous)
        .or_else(|| current.strip_prefix(previous.trim_start()))
        .unwrap_or_default()
        .to_string()
}

pub(crate) fn stable_id(raw: &str, prefix: &str) -> String {
    let clean = raw.trim();
    if clean.is_empty() {
        format!("{prefix}-{}", Uuid::new_v4().simple())
    } else {
        clean.replace(['/', ' ', ':'], "_")
    }
}

pub(crate) fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

pub(crate) fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::suffix_delta;

    #[test]
    fn a_delta_is_only_the_new_part_of_the_text() {
        assert_eq!(suffix_delta("", "hello"), "hello");
        assert_eq!(suffix_delta("hel", "hello"), "lo");
        assert_eq!(suffix_delta("hello", "hello"), "");
        // Only whitespace was streamed: all of the text is new.
        assert_eq!(suffix_delta("\n\n", "hello"), "hello");
    }

    #[test]
    fn a_trimmed_or_changed_text_is_not_sent_again() {
        // Hermes streams "\n\nhello" and completes with "hello".
        assert_eq!(suffix_delta("\n\nhello", "hello"), "");
        assert_eq!(suffix_delta("\n\nhel", "hello"), "lo");
        assert_eq!(suffix_delta("hello\n", "hello"), "");
        assert_eq!(suffix_delta("hello", "goodbye"), "");
    }
}
