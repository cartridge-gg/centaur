//! Claude Code session files (`~/.claude/projects/<encoded-cwd>/<id>.jsonl`).

mod read;
mod write;

pub(crate) use read::parse_records;
pub use read::read_session;
pub use write::{CLAUDE_VERSION, encode_project_dir, render_session};
