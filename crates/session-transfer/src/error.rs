use std::io;
use std::path::PathBuf;

use crate::model::Tool;

/// Errors from reading and writing agent sessions.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("cannot read {path}")]
    Read {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error("cannot write {path}")]
    Write {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error("{0} already exists; not overwriting it")]
    AlreadyExists(PathBuf),

    #[error("{0} is a compressed rollout; .jsonl.zst files are not supported yet")]
    Compressed(PathBuf),

    #[error("unknown tool \"{0}\"; use codex or claude")]
    UnknownTool(String),

    #[error("no {tool} session found for \"{reference}\" in {dir}")]
    NotFound {
        tool: Tool,
        reference: String,
        dir: PathBuf,
    },

    #[error("\"{prefix}\" matches {} {tool} sessions:\n{}\nUse a longer id.", .ids.len(), first_ids(*.tool, .ids))]
    Ambiguous {
        tool: Tool,
        prefix: String,
        ids: Vec<String>,
    },

    #[error("the session already belongs to {0}; resume it there")]
    SameTool(Tool),

    #[error("the {tool} session {id} has no messages to convert")]
    EmptySession { tool: Tool, id: String },

    #[error("the session has no working directory; pass one explicitly")]
    MissingCwd,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

fn first_ids(tool: Tool, ids: &[String]) -> String {
    let lines: Vec<String> = ids
        .iter()
        .take(5)
        .map(|id| format!("  {}:{id}", tool.id()))
        .collect();
    lines.join("\n")
}
