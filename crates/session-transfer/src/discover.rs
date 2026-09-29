//! Finds Codex, Claude Code and Hermes Agent sessions by id, id prefix or
//! `latest`, and Codex and Claude Code sessions also by path.

use std::fmt;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::Connection;
use serde::Serialize;
use serde_json::Value;

use crate::codex::ROLLOUT_RE;
use crate::error::{Error, Result};
use crate::model::{Session, Tool};
use crate::{claude, codex, hermes};

/// Config directories of the tools.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Homes {
    pub codex: PathBuf,
    pub claude: PathBuf,
    pub hermes: PathBuf,
}

impl Homes {
    /// `$CODEX_HOME` or `~/.codex`, `$CLAUDE_CONFIG_DIR` or `~/.claude`, and
    /// `$HERMES_HOME` or `~/.hermes`.
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            codex: codex_home(None),
            claude: claude_home(None),
            hermes: hermes_home(None),
        }
    }

    #[must_use]
    pub fn get(&self, tool: Tool) -> &Path {
        match tool {
            Tool::Codex => &self.codex,
            Tool::Claude => &self.claude,
            Tool::Hermes => &self.hermes,
        }
    }
}

/// Codex config directory: `explicit`, then `$CODEX_HOME`, then `~/.codex`.
#[must_use]
pub fn codex_home(explicit: Option<PathBuf>) -> PathBuf {
    explicit
        .or_else(|| std::env::var_os("CODEX_HOME").map(PathBuf::from))
        .unwrap_or_else(|| home_dir().join(".codex"))
}

/// Claude Code config directory: `explicit`, then `$CLAUDE_CONFIG_DIR`, then `~/.claude`.
#[must_use]
pub fn claude_home(explicit: Option<PathBuf>) -> PathBuf {
    explicit
        .or_else(|| std::env::var_os("CLAUDE_CONFIG_DIR").map(PathBuf::from))
        .unwrap_or_else(|| home_dir().join(".claude"))
}

/// Hermes Agent home directory: `explicit`, then `$HERMES_HOME`, then `~/.hermes`.
#[must_use]
pub fn hermes_home(explicit: Option<PathBuf>) -> PathBuf {
    explicit
        .or_else(|| std::env::var_os("HERMES_HOME").map(PathBuf::from))
        .unwrap_or_else(|| home_dir().join(".hermes"))
}

fn home_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_default()
}

/// Reads one session file of `tool`. A Hermes database holds many
/// conversations; for Hermes, `path` is the database, and the result is its
/// newest conversation.
///
/// # Errors
///
/// The errors of [`codex::read_session`], [`claude::read_session`] and
/// [`hermes::read_session_from`], and [`Error::NotFound`] for a Hermes
/// database without conversations.
pub fn read_session(tool: Tool, path: &Path) -> Result<Session> {
    match tool {
        Tool::Codex => codex::read_session(path),
        Tool::Claude => claude::read_session(path),
        Tool::Hermes => {
            let conn = hermes::open(path)?;
            let newest = newest_hermes_conversation(&conn, path)?;
            hermes::read_session_from(&conn, &newest)
        }
    }
}

/// Which session to convert. The text form is `latest`, a session file path,
/// a session id, or an id prefix, with an optional `codex:`, `claude:` or
/// `hermes:` prefix. The prefix is a label only: the caller selects the tool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionRef {
    /// The session file that changed last.
    Latest,
    /// A session file path, a session id or an id prefix.
    Query(String),
}

impl FromStr for SessionRef {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let query = match s.split_once(':') {
            Some(("codex" | "claude" | "hermes", rest)) => rest,
            Some(("gemini", _)) => return Err(Error::UnknownTool("gemini".to_string())),
            _ => s,
        };
        Ok(match query {
            "latest" | "last" => Self::Latest,
            _ => Self::Query(query.to_string()),
        })
    }
}

impl fmt::Display for SessionRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Latest => f.write_str("latest"),
            Self::Query(query) => f.write_str(query),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionSummary {
    #[serde(serialize_with = "serialize_tool")]
    pub tool: Tool,
    pub id: String,
    pub path: PathBuf,
    pub cwd: Option<String>,
    pub title: Option<String>,
    pub preview: Option<String>,
    #[serde(skip)]
    pub modified: SystemTime,
}

fn serialize_tool<S: serde::Serializer>(tool: &Tool, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(tool.id())
}

/// All sessions of `tool` under `home` that have at least one message, newest first.
#[must_use]
pub fn list_sessions(tool: Tool, home: &Path) -> Vec<SessionSummary> {
    let (files, head, parse): (_, _, fn(&[Value], &Path) -> Session) = match tool {
        Tool::Codex => {
            let mut files = Vec::new();
            collect_rollouts(&home.join("sessions"), 0, &mut files);
            (files, 60, codex::parse_records)
        }
        Tool::Claude => (claude_session_files(home), 200, claude::parse_records),
        Tool::Hermes => return list_hermes_sessions(home),
    };
    let mut sessions: Vec<SessionSummary> = files
        .into_iter()
        .filter_map(|path| {
            // The first records hold the id, the cwd and the first prompt.
            let records = read_head(&path, head)?;
            let session = parse(&records, &path);
            if session.messages.is_empty() {
                return None;
            }
            let modified = fs::metadata(&path).and_then(|m| m.modified()).ok()?;
            Some(SessionSummary {
                tool,
                id: session.id,
                cwd: session.cwd,
                title: session.title,
                preview: session.preview,
                path,
                modified,
            })
        })
        .collect();
    sessions.sort_by_key(|s| std::cmp::Reverse(s.modified));
    sessions
}

/// Loads the session of `tool` that `reference` points to.
///
/// # Errors
///
/// [`Error::NotFound`] if no session matches, [`Error::Ambiguous`] if an id
/// prefix matches more than one session, and the errors of [`read_session`].
pub fn resolve_session(tool: Tool, reference: &SessionRef, home: &Path) -> Result<Session> {
    if tool == Tool::Hermes {
        return resolve_hermes_session(reference, home);
    }
    if let SessionRef::Query(query) = reference {
        let path = Path::new(query);
        if path.is_file() {
            return read_session(tool, path);
        }
        // Claude Code names each file after its session id.
        if tool == Tool::Claude
            && let Some(path) = claude_session_files(home)
                .into_iter()
                .find(|p| p.file_stem().is_some_and(|stem| stem == query.as_str()))
        {
            return read_session(tool, &path);
        }
    }
    let sessions = list_sessions(tool, home);
    let found = match reference {
        SessionRef::Latest => sessions.first(),
        SessionRef::Query(prefix) => find_by_id(tool, &sessions, prefix)?,
    };
    let summary = found.ok_or_else(|| Error::NotFound {
        tool,
        reference: reference.to_string(),
        dir: match tool {
            Tool::Codex => home.join("sessions"),
            Tool::Claude => home.join("projects"),
            Tool::Hermes => home.join(hermes::DATABASE_FILE),
        },
    })?;
    read_session(tool, &summary.path)
}

/// The conversations in the Hermes database under `home`, newest first. See
/// [`hermes::conversations`]. A missing or unreadable database has none.
fn list_hermes_sessions(home: &Path) -> Vec<SessionSummary> {
    let db = home.join(hermes::DATABASE_FILE);
    if !db.is_file() {
        return Vec::new();
    }
    let Ok(conversations) = hermes::open(&db).and_then(|conn| hermes::conversations(&conn)) else {
        return Vec::new();
    };
    conversations
        .into_iter()
        .map(|c| SessionSummary {
            tool: Tool::Hermes,
            id: c.id,
            path: db.clone(),
            cwd: c.cwd,
            title: c.title,
            preview: c.preview,
            modified: UNIX_EPOCH + Duration::try_from_secs_f64(c.last_active).unwrap_or_default(),
        })
        .collect()
}

/// Loads the Hermes session that `reference` points to: the newest
/// conversation for `latest`, else [`hermes::find_session`].
fn resolve_hermes_session(reference: &SessionRef, home: &Path) -> Result<Session> {
    let db = home.join(hermes::DATABASE_FILE);
    let conn = hermes::open(&db)?;
    let id = match reference {
        SessionRef::Latest => newest_hermes_conversation(&conn, &db)?,
        SessionRef::Query(query) => hermes::find_session(&conn, query)?,
    };
    hermes::read_session_from(&conn, &id)
}

fn newest_hermes_conversation(conn: &Connection, db: &Path) -> Result<String> {
    hermes::conversations(conn)?
        .into_iter()
        .next()
        .map(|c| c.id)
        .ok_or_else(|| Error::NotFound {
            tool: Tool::Hermes,
            reference: SessionRef::Latest.to_string(),
            dir: db.to_path_buf(),
        })
}

/// The session whose id is `prefix`, or the only one whose id starts with it.
fn find_by_id<'a>(
    tool: Tool,
    sessions: &'a [SessionSummary],
    prefix: &str,
) -> Result<Option<&'a SessionSummary>> {
    if let Some(exact) = sessions.iter().find(|s| s.id == prefix) {
        return Ok(Some(exact));
    }
    let hits: Vec<&SessionSummary> = sessions
        .iter()
        .filter(|s| s.id.starts_with(prefix))
        .collect();
    match hits.as_slice() {
        [] => Ok(None),
        [only] => Ok(Some(only)),
        _ => Err(Error::Ambiguous {
            tool,
            prefix: prefix.to_string(),
            ids: hits.iter().map(|s| s.id.clone()).collect(),
        }),
    }
}

/// Collects rollout files under `sessions/YYYY/MM/DD/`.
fn collect_rollouts(dir: &Path, depth: usize, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        if kind.is_dir() && depth < 4 {
            collect_rollouts(&entry.path(), depth + 1, out);
        } else if kind.is_file() && ROLLOUT_RE.is_match(&entry.file_name().to_string_lossy()) {
            out.push(entry.path());
        }
    }
}

/// Session files directly under `projects/<dir>/`. Subagent transcripts live
/// deeper and are not sessions of their own.
fn claude_session_files(home: &Path) -> Vec<PathBuf> {
    let Ok(projects) = fs::read_dir(home.join("projects")) else {
        return Vec::new();
    };
    projects
        .flatten()
        .filter(|entry| entry.file_type().is_ok_and(|t| t.is_dir()))
        .filter_map(|dir| fs::read_dir(dir.path()).ok())
        .flat_map(|entries| entries.flatten())
        .filter(|entry| entry.file_type().is_ok_and(|t| t.is_file()))
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|e| e == "jsonl"))
        .collect()
}

/// The first `max` JSON records of a plain JSONL file.
fn read_head(path: &Path, max: usize) -> Option<Vec<Value>> {
    if path.extension().is_some_and(|e| e == "zst") {
        return None;
    }
    let reader = BufReader::new(File::open(path).ok()?);
    let records = reader
        .lines()
        .map_while(std::result::Result::ok)
        .filter(|line| !line.trim().is_empty())
        .filter_map(|line| serde_json::from_str(&line).ok())
        .take(max)
        .collect();
    Some(records)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_refs() {
        assert_eq!("latest".parse::<SessionRef>().unwrap(), SessionRef::Latest);
        assert_eq!(
            "codex:last".parse::<SessionRef>().unwrap(),
            SessionRef::Latest
        );
        assert_eq!(
            "claude:01a0b25a".parse::<SessionRef>().unwrap(),
            SessionRef::Query("01a0b25a".to_string())
        );
        assert!(matches!(
            "gemini:latest".parse::<SessionRef>(),
            Err(Error::UnknownTool(_))
        ));
    }
}
