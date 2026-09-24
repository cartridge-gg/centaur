//! The result of a conversion, and where it goes.

use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::PathBuf;

use chrono::{DateTime, FixedOffset};
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::model::Tool;

/// Where and as what a converted session is written.
#[derive(Debug, Clone)]
pub struct Target {
    /// Config directory of the target tool, for example `~/.claude` or `~/.codex`.
    pub home: PathBuf,
    /// Project directory that the new session belongs to.
    pub cwd: String,
    /// Id of the new session.
    pub id: Uuid,
    /// Time of the conversion. Codex names rollout files in this offset.
    pub now: DateTime<FixedOffset>,
}

/// A rendered session in the native format of the target tool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Converted {
    pub tool: Tool,
    pub id: Uuid,
    pub path: PathBuf,
    /// Command that continues the session in the target tool.
    pub resume_command: String,
    /// The JSONL file contents.
    pub contents: String,
}

impl Converted {
    /// Writes the session file. It never overwrites an existing file.
    ///
    /// # Errors
    ///
    /// [`Error::AlreadyExists`] if the file exists, and [`Error::Write`] if
    /// the directory or the file cannot be written.
    pub fn write(&self) -> Result<()> {
        let write_error = |source| Error::Write {
            path: self.path.clone(),
            source,
        };
        if let Some(dir) = self.path.parent() {
            fs::create_dir_all(dir).map_err(write_error)?;
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&self.path)
            .map_err(|e| match e.kind() {
                ErrorKind::AlreadyExists => Error::AlreadyExists(self.path.clone()),
                _ => write_error(e),
            })?;
        file.write_all(self.contents.as_bytes())
            .map_err(write_error)
    }
}

/// Quotes `s` for a POSIX shell, unless it needs no quotes.
#[must_use]
pub fn shell_quote(s: &str) -> String {
    let plain = !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || "_./:@%+=-".contains(c));
    if plain {
        s.to_string()
    } else {
        format!("'{}'", s.replace('\'', r"'\''"))
    }
}

#[cfg(test)]
mod tests {
    use super::shell_quote;

    #[test]
    fn shell_quoting() {
        assert_eq!(shell_quote("/Users/dev/app"), "/Users/dev/app");
        assert_eq!(shell_quote("/tmp/it's here"), r"'/tmp/it'\''s here'");
        assert_eq!(shell_quote(""), "''");
    }
}
