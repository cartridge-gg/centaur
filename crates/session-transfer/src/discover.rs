//! Finds Codex and Claude Code sessions by path, id, id prefix or `latest`.

use std::fmt;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::SystemTime;

use serde::Serialize;
use serde_json::Value;

use crate::codex::ROLLOUT_RE;
use crate::error::{Error, Result};
use crate::model::{Session, Tool};
use crate::{claude, codex};

/// Config directories of both tools.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Homes {
    pub codex: PathBuf,
    pub claude: PathBuf,
}

impl Homes {
    /// `$CODEX_HOME` or `~/.codex`, and `$CLAUDE_CONFIG_DIR` or `~/.claude`.
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            codex: codex_home(None),
            claude: claude_home(None),
        }
    }

    #[must_use]
    pub fn get(&self, tool: Tool) -> &Path {
        match tool {
            Tool::Codex => &self.codex,
            Tool::Claude => &self.claude,
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

fn home_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_default()
}

/// Reads one session file of `tool`.
///
/// # Errors
///
/// The errors of [`codex::read_session`] and [`claude::read_session`].
pub fn read_session(tool: Tool, path: &Path) -> Result<Session> {
    match tool {
        Tool::Codex => codex::read_session(path),
        Tool::Claude => claude::read_session(path),
    }
}

/// Which session to convert. The text form is `latest`, a session file path,
/// a session id, or an id prefix, with an optional `codex:` or `claude:` prefix.
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
            Some(("codex" | "claude", rest)) => rest,
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
    let (files, head) = match tool {
        Tool::Codex => {
            let mut files = Vec::new();
            collect_rollouts(&home.join("sessions"), 0, &mut files);
            (files, 60)
        }
        Tool::Claude => (claude_session_files(home), 200),
    };
    let mut sessions: Vec<SessionSummary> = files
        .into_iter()
        .filter_map(|path| {
            // The first records hold the id, the cwd and the first prompt.
            let records = read_head(&path, head)?;
            let session = match tool {
                Tool::Codex => codex::parse_records(&records, &path),
                Tool::Claude => claude::parse_records(&records, &path),
            };
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
        },
    })?;
    read_session(tool, &summary.path)
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
