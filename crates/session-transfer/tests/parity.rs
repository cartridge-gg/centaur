//! Byte-for-byte comparison with sessport, the reference implementation.
//!
//! `scripts/gen-golden.mjs` runs sessport on every fixture, in both
//! directions, and writes the result to
//! `tests/golden/<direction>/<fixture>/<variant>.jsonl`, with the options in
//! `<variant>.meta.json`. This test renders the same fixtures with the same
//! options, session id, clock and uuid sequence, and compares the bytes.

use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;
use session_transfer::Tool;
use session_transfer::discover::read_session;
use session_transfer::prepare::PrepareOptions;

mod common;

fn root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

fn meta_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("tests/golden exists; run scripts/gen-golden.mjs") {
        let path = entry.unwrap().path();
        if path.is_dir() {
            meta_files(&path, out);
        } else if path.to_string_lossy().ends_with(".meta.json") {
            out.push(path);
        }
    }
}

/// Describes the first difference between two JSONL texts.
fn first_difference(actual: &str, expected: &str) -> String {
    let (a, e): (Vec<&str>, Vec<&str>) = (actual.lines().collect(), expected.lines().collect());
    if a.len() != e.len() {
        return format!("line count {} != expected {}", a.len(), e.len());
    }
    for (i, (la, le)) in a.iter().zip(&e).enumerate() {
        if la != le {
            let at = la
                .bytes()
                .zip(le.bytes())
                .take_while(|(x, y)| x == y)
                .count();
            let at = (0..=at)
                .rev()
                .find(|&i| la.is_char_boundary(i) && le.is_char_boundary(i))
                .unwrap_or(0);
            let snippet = |s: &str| s[at..].chars().take(120).collect::<String>();
            return format!(
                "line {} differs at byte {at}\n      actual: …{}\n    expected: …{}",
                i + 1,
                snippet(la),
                snippet(le)
            );
        }
    }
    "trailing newline differs".to_string()
}

#[test]
fn output_matches_sessport_golden_files() {
    let mut metas = Vec::new();
    meta_files(&root().join("tests/golden"), &mut metas);
    metas.sort();
    assert!(
        !metas.is_empty(),
        "no golden files; run scripts/gen-golden.mjs"
    );

    let mut failures = Vec::new();
    for meta_path in &metas {
        let meta: Value = serde_json::from_str(&fs::read_to_string(meta_path).unwrap()).unwrap();
        let golden_path =
            PathBuf::from(meta_path.to_string_lossy().replace(".meta.json", ".jsonl"));
        let expected = fs::read_to_string(&golden_path).unwrap();
        let name = golden_path
            .strip_prefix(root())
            .unwrap()
            .display()
            .to_string();

        let (from, to) = match meta["direction"].as_str().unwrap() {
            "codex-to-claude" => (Tool::Codex, Tool::Claude),
            "claude-to-codex" => (Tool::Claude, Tool::Codex),
            other => panic!("unknown direction {other}"),
        };
        let fixture = root().join(meta["fixture"].as_str().unwrap());
        let session = read_session(from, &fixture).unwrap();
        let opts = &meta["options"];
        let options = PrepareOptions {
            max_tool_output: Some(
                opts["maxToolOutput"]
                    .as_u64()
                    .map_or(4000, |n| usize::try_from(n).unwrap()),
            )
            .filter(|&n| n > 0),
            last_messages: opts["lastMessages"]
                .as_u64()
                .map(|n| usize::try_from(n).unwrap()),
            keep_thinking: opts["keepThinking"].as_bool().unwrap(),
            redact: opts["redact"].as_bool().unwrap(),
            ..PrepareOptions::default()
        };
        let result = common::render_like_sessport(&session, to, opts["cwd"].as_str(), &options);

        if result.contents != expected {
            failures.push(format!(
                "{name}: {}",
                first_difference(&result.contents, &expected)
            ));
        }
        let want = &meta["expected"];
        if result.path.to_string_lossy() != want["path"].as_str().unwrap() {
            failures.push(format!(
                "{name}: path {} != {}",
                result.path.display(),
                want["path"]
            ));
        }
        if result.resume_command != want["resumeCommand"].as_str().unwrap() {
            failures.push(format!(
                "{name}: resume command {:?} != {}",
                result.resume_command, want["resumeCommand"]
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} golden cases differ from sessport:\n{}",
        failures.len(),
        metas.len(),
        failures.join("\n")
    );
}
