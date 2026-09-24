//! Checks the converted output against a real Claude Code session file.
//!
//! The reference is a session that Claude Code 2.1.281 wrote in print mode,
//! scrubbed with `scripts/scrub-fixture.mjs`, plus sessport's Claude fixture
//! for the `ai-title` record, which Claude Code writes only in interactive
//! sessions.
//! The converter must only use fields that Claude Code itself writes, and the
//! lines must form one linear conversation.

use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use chrono::Local;
use serde_json::Value;
use session_transfer::claude::render_session;
use session_transfer::codex;
use session_transfer::prepare::PrepareOptions;
use session_transfer::{Converted, Target};

const REFERENCES: &[&str] = &[
    "tests/fixtures/claude/19fa6065-e665-49b6-9565-cf655fa8bc17.jsonl",
    "tests/fixtures/claude/sessport/11111111-2222-4333-8444-555555555555.jsonl",
];

fn root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

fn keys(value: &Value) -> BTreeSet<String> {
    value
        .as_object()
        .map(|o| o.keys().cloned().collect())
        .unwrap_or_default()
}

/// Top-level and `message` keys that Claude Code writes, per record type.
fn reference_keys() -> HashMap<String, (BTreeSet<String>, BTreeSet<String>)> {
    let mut out: HashMap<String, (BTreeSet<String>, BTreeSet<String>)> = HashMap::new();
    for reference in REFERENCES {
        let text = fs::read_to_string(root().join(reference)).unwrap();
        // sessport's fixture ends with a half-written line on purpose.
        for record in text
            .lines()
            .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        {
            let entry = out
                .entry(record["type"].as_str().unwrap().to_string())
                .or_default();
            entry.0.extend(keys(&record));
            entry.1.extend(keys(&record["message"]));
        }
    }
    out
}

fn codex_fixtures() -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = fs::read_dir(root().join("tests/fixtures/codex"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "jsonl"))
        .collect();
    files.sort();
    files
}

fn render(fixture: &Path, home: &Path) -> Converted {
    let session = codex::read_session(fixture).unwrap();
    let target = Target {
        home: home.to_path_buf(),
        cwd: session.cwd.clone().unwrap(),
        id: uuid::Uuid::new_v4(),
        now: Local::now().fixed_offset(),
    };
    render_session(
        &session,
        &target,
        &PrepareOptions::default(),
        uuid::Uuid::new_v4,
    )
}

#[test]
fn output_uses_only_fields_that_claude_code_writes() {
    let reference = reference_keys();
    for fixture in codex_fixtures() {
        let result = render(&fixture, Path::new("/claude-home"));
        let name = fixture.file_name().unwrap().to_string_lossy();
        for line in result.contents.lines() {
            let record: Value = serde_json::from_str(line).unwrap();
            let kind = record["type"].as_str().unwrap();
            let (top, message) = reference
                .get(kind)
                .unwrap_or_else(|| panic!("{name}: Claude Code never writes type {kind}"));
            let extra: Vec<_> = keys(&record).difference(top).cloned().collect();
            assert!(
                extra.is_empty(),
                "{name}: {kind} record has fields Claude Code does not write: {extra:?}"
            );
            let extra: Vec<_> = keys(&record["message"])
                .difference(message)
                .cloned()
                .collect();
            assert!(
                extra.is_empty(),
                "{name}: {kind} message has fields Claude Code does not write: {extra:?}"
            );
        }
    }
}

#[test]
fn output_is_one_linear_alternating_conversation() {
    for fixture in codex_fixtures() {
        let result = render(&fixture, Path::new("/claude-home"));
        let name = fixture.file_name().unwrap().to_string_lossy();
        let records: Vec<Value> = result
            .contents
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        let (title, turns) = records.split_last().unwrap();

        assert_eq!(title["type"], "ai-title", "{name}");
        assert!(
            title["aiTitle"].as_str().unwrap().starts_with("[codex] "),
            "{name}"
        );
        assert_eq!(title["sessionId"], result.id.to_string(), "{name}");
        assert!(!turns.is_empty(), "{name}");

        let mut parent = Value::Null;
        let mut last_timestamp = String::new();
        for (i, turn) in turns.iter().enumerate() {
            let role = if i % 2 == 0 { "user" } else { "assistant" };
            assert_eq!(
                turn["type"],
                role,
                "{name}: line {} breaks the user/assistant order",
                i + 1
            );
            assert_eq!(turn["message"]["role"], role, "{name}");
            assert_eq!(
                turn["parentUuid"],
                parent,
                "{name}: line {} breaks the parentUuid chain",
                i + 1
            );
            assert_eq!(turn["sessionId"], result.id.to_string(), "{name}");
            let timestamp = turn["timestamp"].as_str().unwrap().to_string();
            assert!(
                timestamp > last_timestamp,
                "{name}: timestamps must increase"
            );
            last_timestamp = timestamp;
            parent = turn["uuid"].clone();
        }
    }
}

#[test]
fn write_never_overwrites_a_session() {
    let home = std::env::temp_dir().join(format!("session-transfer-write-{}", std::process::id()));
    let result = render(&codex_fixtures()[0], &home);
    result.write().unwrap();
    assert_eq!(fs::read_to_string(&result.path).unwrap(), result.contents);
    let err = result.write().unwrap_err().to_string();
    assert!(err.contains("already exists"), "{err}");
    fs::remove_dir_all(&home).unwrap();
}
