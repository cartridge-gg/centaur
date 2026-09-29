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
    /// Id of the new session: [`Target::id`], or for Hermes a Hermes id.
    pub id: String,
    /// The session file, or for Hermes the database.
    pub path: PathBuf,
    /// Command that continues the session in the target tool.
    pub resume_command: String,
    /// The JSONL file contents, or for Hermes the import payload.
    pub contents: String,
}

impl Converted {
    /// Writes the session. It never overwrites an existing session. A Hermes
    /// session is imported into its database with the Python in
    /// `$HERMES_PYTHON`, or else `python3` (see [`crate::hermes::import`]).
    ///
    /// # Errors
    ///
    /// [`Error::AlreadyExists`] if the file exists, [`Error::Write`] if
    /// the directory or the file cannot be written, and
    /// [`Error::HermesImport`] if the Hermes import fails.
    pub fn write(&self) -> Result<()> {
        if self.tool == Tool::Hermes {
            let python = std::env::var_os(crate::hermes::PYTHON_ENV)
                .filter(|python| !python.is_empty())
                .unwrap_or_else(|| "python3".into());
            return crate::hermes::import(self, &python);
        }
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
