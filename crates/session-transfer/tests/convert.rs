//! The one-call API: files written by one direction are read back by the
//! other reader, and the options that harness-server uses.

use std::path::{Path, PathBuf};

use chrono::Local;
use session_transfer::convert::{ConvertOptions, TrailingPrompt, convert};
use session_transfer::discover::read_session;
use session_transfer::model::{Part, Role};
use session_transfer::{Error, Session, Target, Tool};
use uuid::Uuid;

const CODEX_FIXTURE: &str = "tests/fixtures/codex/recorded-codex-0.154.jsonl";
const CLAUDE_FIXTURE: &str = "tests/fixtures/claude/19fa6065-e665-49b6-9565-cf655fa8bc17.jsonl";

fn fixture(tool: Tool, path: &str) -> Session {
    read_session(tool, &Path::new(env!("CARGO_MANIFEST_DIR")).join(path)).unwrap()
}

struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "session-transfer-{name}-{}-{}",
            std::process::id(),
            Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn target(home: &Path, cwd: &str) -> Target {
    Target {
        home: home.to_path_buf(),
        cwd: cwd.to_string(),
        id: Uuid::new_v4(),
        now: Local::now().fixed_offset(),
    }
}

fn first_user_text(session: &Session) -> &str {
    session
        .messages
        .iter()
        .find(|m| m.role == Role::User)
        .and_then(|m| m.parts.iter().find_map(Part::as_text))
        .unwrap()
}

#[test]
fn round_trip_keeps_one_preamble_and_the_conversation() {
    let temp = TempDir::new("round-trip");
    let original = fixture(Tool::Codex, CODEX_FIXTURE);
    let options = ConvertOptions::default();

    let claude = convert(
        &original,
        Tool::Claude,
        &target(&temp.0.join("claude"), "/work/app"),
        &options,
    )
    .unwrap();
    claude.write().unwrap();
    let as_claude = read_session(Tool::Claude, &claude.path).unwrap();
    assert_eq!(as_claude.id, claude.id.to_string());
    assert_eq!(as_claude.cwd.as_deref(), Some("/work/app"));

    let codex = convert(
        &as_claude,
        Tool::Codex,
        &target(&temp.0.join("codex"), "/work/app"),
        &options,
    )
    .unwrap();
    codex.write().unwrap();
    let back = read_session(Tool::Codex, &codex.path).unwrap();
    assert_eq!(back.id, codex.id.to_string());

    // One preamble, which names the last hop, followed by the original prompt.
    let text = first_user_text(&back);
    assert_eq!(
        text.matches("This conversation was imported from").count(),
        1,
        "{text}"
    );
    assert!(
        text.contains("imported from Claude Code into Codex CLI"),
        "{text}"
    );
    assert!(text.contains("Read calc.py and test_calc.py."), "{text}");
    // The last assistant answer survives both hops.
    let last = back.messages.last().unwrap();
    assert_eq!(last.role, Role::Assistant);
    assert!(
        last.parts
            .iter()
            .filter_map(Part::as_text)
            .any(|t| t.contains("Removed the erroneous `+ 1`"))
    );
}

#[test]
fn converting_into_the_same_tool_fails() {
    let session = fixture(Tool::Claude, CLAUDE_FIXTURE);
    let err = convert(
        &session,
        Tool::Claude,
        &target(Path::new("/unused"), "/work/app"),
        &ConvertOptions::default(),
    )
    .unwrap_err();
    assert!(matches!(err, Error::SameTool(Tool::Claude)), "{err}");
}

#[test]
fn a_trailing_prompt_can_be_left_out() {
    let mut session = fixture(Tool::Claude, CLAUDE_FIXTURE);
    session.messages.push(session_transfer::model::Message {
        role: Role::User,
        model: None,
        parts: vec![Part::Text(
            "A prompt that failed before any answer".to_string(),
        )],
    });
    let home = Path::new("/unused");

    let keep = convert(
        &session,
        Tool::Codex,
        &target(home, "/work/app"),
        &ConvertOptions::default(),
    )
    .unwrap();
    assert!(
        keep.contents
            .contains("A prompt that failed before any answer")
    );

    let options = ConvertOptions {
        trailing_prompt: TrailingPrompt::Drop,
        ..ConvertOptions::default()
    };
    let dropped = convert(&session, Tool::Codex, &target(home, "/work/app"), &options).unwrap();
    assert!(
        !dropped
            .contents
            .contains("A prompt that failed before any answer")
    );
    assert!(
        dropped
            .contents
            .contains("Fixed the bug by removing the extra `+ 1`")
    );
}

#[test]
fn tool_results_at_the_end_are_kept_when_dropping_a_prompt() {
    let mut session = fixture(Tool::Claude, CLAUDE_FIXTURE);
    session.messages.push(session_transfer::model::Message {
        role: Role::User,
        model: None,
        parts: vec![Part::ToolResult {
            call_id: "toolu_last".to_string(),
            output: "last tool output".to_string(),
            is_error: false,
        }],
    });
    let options = ConvertOptions {
        trailing_prompt: TrailingPrompt::Drop,
        ..ConvertOptions::default()
    };
    let converted = convert(
        &session,
        Tool::Codex,
        &target(Path::new("/unused"), "/work/app"),
        &options,
    )
    .unwrap();
    assert!(converted.contents.contains("last tool output"));
}

#[test]
fn codex_rollouts_record_the_model_provider() {
    let session = fixture(Tool::Claude, CLAUDE_FIXTURE);
    let options = ConvertOptions {
        codex_model_provider: "pool-proxy".to_string(),
        ..ConvertOptions::default()
    };
    let converted = convert(
        &session,
        Tool::Codex,
        &target(Path::new("/codex-home"), "/work/app"),
        &options,
    )
    .unwrap();
    let meta: serde_json::Value =
        serde_json::from_str(converted.contents.lines().next().unwrap()).unwrap();
    assert_eq!(meta["payload"]["model_provider"], "pool-proxy");
    assert!(converted.path.starts_with("/codex-home/sessions"));
    assert_eq!(
        converted.resume_command,
        format!("cd /work/app && codex resume {}", converted.id)
    );
}
