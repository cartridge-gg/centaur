//! Writes Hermes Agent sessions.
//!
//! Hermes keeps its sessions in a SQLite database, not in files, and the
//! database needs Hermes itself: Hermes creates and migrates the schema, and
//! its full-text triggers can need a tokenizer that only Hermes loads. So the
//! writer renders the payload of `SessionDB.import_sessions`, and [`import`]
//! runs that import with the Python of the Hermes install. The import keeps
//! the new session id, and creates the database if it does not exist.
//!
//! The session has alternating user and assistant text turns (see
//! [`crate::prepare`]). It has no title: Hermes titles must be unique, and
//! the session that moved away from Hermes keeps its own. It has no model
//! either, because the source model belongs to another tool: Hermes uses its
//! configured model.

use std::ffi::OsStr;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

use serde_json::{Value, json};

use super::read::DATABASE_FILE;
use crate::error::{Error, Result};
use crate::model::{Role, Session, Tool};
use crate::output::{Converted, Target, shell_quote};
use crate::prepare::{PrepareOptions, prepare_messages};

/// Environment variable with the Python of the Hermes install, as
/// harness-server uses it. Without it, the import runs `python3`.
pub const PYTHON_ENV: &str = "HERMES_PYTHON";

/// Imports the payload on stdin and prints the result as JSON.
const IMPORT_SCRIPT: &str = "\
import json, sys
from hermes_state import SessionDB
payload = json.load(sys.stdin)
db = SessionDB()
try:
    result = db.import_sessions(payload)
finally:
    db.close()
print(json.dumps(result))
";

/// Renders `session` as a Hermes import payload. The id of the new session
/// is [`session_id`].
#[must_use]
pub fn render_session(session: &Session, target: &Target, options: &PrepareOptions) -> Converted {
    let prepared = prepare_messages(session, Tool::Hermes, options);
    let id = session_id(target);
    // Strictly increasing timestamps, one second apart, that end now. Hermes
    // orders rows by insertion; the times are for listings.
    let end = target.now.timestamp_millis() as f64 / 1000.0;
    let start = end - prepared.len() as f64;
    let messages: Vec<Value> = prepared
        .iter()
        .zip(0u32..)
        .map(|(message, index)| {
            json!({
                "role": match message.role {
                    Role::User => "user",
                    Role::Assistant => "assistant",
                },
                "content": message.text,
                "timestamp": start + f64::from(index),
            })
        })
        .collect();
    let payload = json!([{
        "id": id,
        "source": options.brand,
        "started_at": start,
        "cwd": target.cwd,
        "messages": messages,
    }]);
    let mut contents = payload.to_string();
    contents.push('\n');
    Converted {
        tool: Tool::Hermes,
        path: target.home.join(DATABASE_FILE),
        resume_command: format!("cd {} && hermes --resume {id}", shell_quote(&target.cwd)),
        id,
        contents,
    }
}

/// The id of the session that [`render_session`] writes, in the format of
/// Hermes: the local time of the conversion and 6 hex digits of the target id.
#[must_use]
pub fn session_id(target: &Target) -> String {
    let suffix = target.id.simple().to_string();
    format!("{}_{}", target.now.format("%Y%m%d_%H%M%S"), &suffix[..6])
}

/// Imports a rendered Hermes session into the database at
/// `converted.path`, with the Hermes Python `python`. `HERMES_HOME` is the
/// directory of the database.
///
/// # Errors
///
/// [`Error::HermesImport`] if the import cannot run or does not import the
/// session, for example because a session with its id exists.
pub fn import(converted: &Converted, python: &OsStr) -> Result<()> {
    let failed = |reason: String| Error::HermesImport {
        id: converted.id.clone(),
        reason,
    };
    let home = converted
        .path
        .parent()
        .ok_or_else(|| failed("the database has no directory".to_string()))?;
    fs::create_dir_all(home).map_err(|source| Error::Write {
        path: home.to_path_buf(),
        source,
    })?;
    let mut child = Command::new(python)
        .arg("-c")
        .arg(IMPORT_SCRIPT)
        .env("HERMES_HOME", home)
        .env("HERMES_QUIET", "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| {
            failed(format!(
                "cannot run {}: {error}",
                Path::new(python).display()
            ))
        })?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(converted.contents.as_bytes())
            .map_err(|error| failed(format!("cannot send the session: {error}")))?;
    }
    let output = child
        .wait_with_output()
        .map_err(|error| failed(error.to_string()))?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let result: Option<Value> = stdout
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .and_then(|line| serde_json::from_str(line).ok());
    let Some(result) = result.filter(|_| output.status.success()) else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let detail = stderr.trim().lines().last().unwrap_or("no output");
        return Err(failed(format!(
            "the import failed ({}): {detail}",
            output.status
        )));
    };
    let listed = |key: &str| {
        result
            .get(key)
            .and_then(Value::as_array)
            .is_some_and(|ids| {
                ids.iter()
                    .any(|id| id.as_str() == Some(converted.id.as_str()))
            })
    };
    if result.get("ok").and_then(Value::as_bool) == Some(true) && listed("imported_ids") {
        Ok(())
    } else if listed("skipped_ids") {
        Err(failed("a session with this id exists".to_string()))
    } else {
        Err(failed(format!("Hermes returned {result}")))
    }
}
