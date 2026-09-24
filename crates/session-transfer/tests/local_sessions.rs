//! Benchmark against sessport on every Codex and Claude Code session on this
//! machine, in both directions.
//!
//! This test is ignored by default, because it needs Node.js, a built sessport
//! checkout and real sessions. Nothing from those sessions is written to the
//! repository. Run it with:
//!
//!     SESSPORT_DIR=/path/to/sessport cargo test --release --test local_sessions -- --ignored --nocapture

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use session_transfer::discover::{Homes, list_sessions, read_session};
use session_transfer::prepare::PrepareOptions;
use session_transfer::{Tool, codex};

mod common;

/// Every session file of `tool`, including those with no messages.
fn session_files(tool: Tool, homes: &Homes) -> Vec<PathBuf> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else if codex::ROLLOUT_RE.is_match(&entry.file_name().to_string_lossy()) {
                out.push(path);
            }
        }
    }
    let mut files = Vec::new();
    match tool {
        Tool::Codex => walk(&homes.codex.join("sessions"), &mut files),
        // The same files that `discover` lists: top level of each project.
        Tool::Claude => files.extend(
            list_sessions(Tool::Claude, &homes.claude)
                .into_iter()
                .map(|s| s.path),
        ),
    }
    files.sort();
    files
}

fn benchmark(from: Tool) {
    let sessport =
        std::env::var("SESSPORT_DIR").expect("set SESSPORT_DIR to a built sessport checkout");
    let files = session_files(from, &Homes::from_env());
    assert!(!files.is_empty(), "no {from} sessions found");

    let out_dir = std::env::temp_dir().join(format!(
        "session-transfer-bench-{}-{}",
        from.id(),
        std::process::id()
    ));
    let status = Command::new("node")
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/sessport-render.mjs"))
        .arg(from.id())
        .arg(&out_dir)
        .args(&files)
        .env("SESSPORT_DIR", sessport)
        .status()
        .expect("node runs");
    assert!(status.success(), "sessport-render.mjs failed");

    let to = match from {
        Tool::Codex => Tool::Claude,
        Tool::Claude => Tool::Codex,
    };
    let mut differ = Vec::new();
    for (index, file) in files.iter().enumerate() {
        let expected = fs::read_to_string(out_dir.join(format!("{index}.jsonl"))).unwrap();
        let session = read_session(from, file).unwrap();
        let actual = if session.messages.is_empty() {
            String::new()
        } else {
            common::render_like_sessport(&session, to, None, &PrepareOptions::default()).contents
        };
        if actual != expected {
            differ.push(file.display().to_string());
        }
    }
    fs::remove_dir_all(&out_dir).unwrap();
    println!(
        "{} of {} local {from} sessions match sessport byte for byte",
        files.len() - differ.len(),
        files.len()
    );
    assert!(
        differ.is_empty(),
        "these sessions differ from sessport:\n{}",
        differ.join("\n")
    );
}

#[test]
#[ignore = "needs SESSPORT_DIR and local Codex sessions"]
fn every_local_codex_session_matches_sessport() {
    benchmark(Tool::Codex);
}

#[test]
#[ignore = "needs SESSPORT_DIR and local Claude Code sessions"]
fn every_local_claude_session_matches_sessport() {
    benchmark(Tool::Claude);
}
