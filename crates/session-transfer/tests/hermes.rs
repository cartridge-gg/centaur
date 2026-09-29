//! The Hermes Agent reader against recorded Hermes databases, and conversions
//! from Hermes into the other tools.
//!
//! Each fixture is a SQL dump of a recorded `state.db` (see
//! `scripts/scrub-hermes-fixture.py`):
//!
//! - `recorded-hermes-0.20.0-delegation.sql`: 9 turns with terminal and file
//!   tools, parallel calls, reasoning, an image, a delegated subagent (a child
//!   session), 3 in-place compactions, an interrupted command and 2 resumes.
//! - `recorded-hermes-0.20.0-rotation.sql`: `compression.in_place: false`, so
//!   the compaction ends the first session and continues in a new one.

use std::fs;
use std::path::{Path, PathBuf};

use chrono::Local;
use rusqlite::Connection;
use serde_json::Value;
use session_transfer::convert::{ConvertOptions, convert};
use session_transfer::discover::{SessionRef, list_sessions, read_session, resolve_session};
use session_transfer::hermes::{self, compression_tip};
use session_transfer::model::{Part, Role};
use session_transfer::{Error, Session, Target, Tool};
use uuid::Uuid;

const DELEGATION: &str = "recorded-hermes-0.20.0-delegation.sql";
const DELEGATION_ID: &str = "20260926_205758_e43952";
const CHILD_ID: &str = "20260926_205919_bb12ad";
const ROTATION: &str = "recorded-hermes-0.20.0-rotation.sql";
const ENDED_ID: &str = "20260927_141754_17bd65";
const TIP_ID: &str = "20260927_141856_05140e";

fn fixture_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/hermes")
        .join(name)
}

/// Loads a fixture dump into `conn`. The dump creates `messages` before
/// `sessions`, which its foreign key references, so foreign keys must be off
/// (the bundled `SQLite` of `rusqlite` turns them on by default).
fn load(conn: &Connection, name: &str) {
    conn.execute_batch("PRAGMA foreign_keys = OFF;").unwrap();
    conn.execute_batch(&fs::read_to_string(fixture_path(name)).unwrap())
        .unwrap();
}

fn fixture(name: &str) -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    load(&conn, name);
    conn
}

struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "session-transfer-{name}-{}-{}",
            std::process::id(),
            Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    /// A Hermes home whose `state.db` holds the fixture.
    fn hermes_home(name: &str, fixture: &str) -> Self {
        let temp = Self::new(name);
        let conn = Connection::open(temp.0.join(hermes::DATABASE_FILE)).unwrap();
        load(&conn, fixture);
        temp
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn texts(session: &Session, role: Role) -> Vec<&str> {
    session
        .messages
        .iter()
        .filter(|m| m.role == role)
        .flat_map(|m| m.parts.iter().filter_map(Part::as_text))
        .collect()
}

fn tool_calls(session: &Session) -> Vec<(&str, &str)> {
    session
        .messages
        .iter()
        .flat_map(|m| &m.parts)
        .filter_map(|part| match part {
            Part::ToolCall { id, name, .. } => Some((id.as_str(), name.as_str())),
            _ => None,
        })
        .collect()
}

fn tool_results(session: &Session) -> Vec<(&str, &str, bool)> {
    session
        .messages
        .iter()
        .flat_map(|m| &m.parts)
        .filter_map(|part| match part {
            Part::ToolResult {
                call_id,
                output,
                is_error,
            } => Some((call_id.as_str(), output.as_str(), *is_error)),
            _ => None,
        })
        .collect()
}

/// Every tool result answers a tool call that comes before it.
fn assert_results_follow_calls(session: &Session) {
    let mut calls = Vec::new();
    for part in session.messages.iter().flat_map(|m| &m.parts) {
        match part {
            Part::ToolCall { id, .. } => calls.push(id.as_str()),
            Part::ToolResult { call_id, .. } => {
                assert!(calls.contains(&call_id.as_str()), "no call for {call_id}");
            }
            _ => {}
        }
    }
}

#[test]
fn in_place_compaction_keeps_only_active_rows() {
    let conn = fixture(DELEGATION);
    let session = hermes::read_session_from(&conn, DELEGATION_ID).unwrap();
    assert_eq!(session.source, Tool::Hermes);
    // A delegation child is not a continuation of its parent.
    assert_eq!(session.id, DELEGATION_ID);
    assert_eq!(session.model.as_deref(), Some("gpt-5.6-sol"));
    assert_eq!(session.cwd.as_deref(), Some("/work/app"));
    assert_eq!(session.title.as_deref(), Some("Centaur thread"));

    // The active history starts with the compaction summary, which is not
    // the first prompt.
    let first = session.messages[0].parts[0].as_text().unwrap();
    assert!(
        first.starts_with("[CONTEXT COMPACTION — REFERENCE ONLY]"),
        "{first}"
    );
    assert!(
        session
            .preview
            .as_deref()
            .unwrap()
            .starts_with("Use the todo tool")
    );

    // Each answer is there once, although older compactions kept copies of it.
    let fixed = texts(&session, Role::Assistant)
        .iter()
        .filter(|t| t.starts_with("Fixed `average()`"))
        .count();
    assert_eq!(fixed, 1);

    // The parts match the active rows.
    let (calls, results): (i64, i64) = conn
        .query_row(
            "SELECT
               (SELECT SUM(json_array_length(tool_calls)) FROM messages
                WHERE session_id = ?1 AND active = 1 AND tool_calls IS NOT NULL),
               (SELECT COUNT(*) FROM messages WHERE session_id = ?1 AND active = 1 AND role = 'tool')",
            [DELEGATION_ID],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(tool_calls(&session).len(), usize::try_from(calls).unwrap());
    assert_eq!(
        tool_results(&session).len(),
        usize::try_from(results).unwrap()
    );
    assert_results_follow_calls(&session);

    // Reasoning summaries become thinking parts.
    assert!(
        session
            .messages
            .iter()
            .flat_map(|m| &m.parts)
            .any(|p| matches!(p, Part::Thinking(_)))
    );
}

#[test]
fn a_delegation_child_stays_out_of_the_parent() {
    let conn = fixture(DELEGATION);
    let parent = hermes::read_session_from(&conn, DELEGATION_ID).unwrap();
    assert!(
        !texts(&parent, Role::Assistant)
            .iter()
            .any(|t| t.starts_with("Ran `wc -l *.py` successfully"))
    );
    // The result reaches the parent through the completion turn.
    assert!(
        texts(&parent, Role::User)
            .iter()
            .any(|t| t.starts_with("[ASYNC DELEGATION BATCH COMPLETE — deleg_61c28cfa]"))
    );

    let child = hermes::read_session_from(&conn, CHILD_ID).unwrap();
    assert_eq!(child.id, CHILD_ID);
    assert!(
        child
            .preview
            .as_deref()
            .unwrap()
            .starts_with("Run `wc -l *.py`")
    );
    assert_eq!(
        tool_calls(&child)
            .iter()
            .map(|(_, name)| *name)
            .collect::<Vec<_>>(),
        ["terminal"]
    );
}

#[test]
fn parallel_calls_share_one_message_and_their_results_another() {
    let session = hermes::read_session_from(&fixture(DELEGATION), DELEGATION_ID).unwrap();
    let index = session
        .messages
        .iter()
        .position(|m| {
            m.parts
                .iter()
                .filter(|p| matches!(p, Part::ToolCall { name, .. } if name == "read_file"))
                .count()
                == 3
        })
        .expect("an answer with 3 read_file calls");
    let ids: Vec<&str> = session.messages[index]
        .parts
        .iter()
        .filter_map(|p| match p {
            Part::ToolCall { id, .. } => Some(id.as_str()),
            _ => None,
        })
        .collect();
    let result_ids: Vec<&str> = session.messages[index + 1]
        .parts
        .iter()
        .map(|p| match p {
            Part::ToolResult { call_id, .. } => call_id.as_str(),
            other => panic!("expected only tool results, got {other:?}"),
        })
        .collect();
    assert_eq!(result_ids, ids);
}

#[test]
fn tool_errors_and_interrupts_are_kept() {
    let session = hermes::read_session_from(&fixture(DELEGATION), DELEGATION_ID).unwrap();
    let results = tool_results(&session);
    let missing_image = results
        .iter()
        .find(|(_, output, _)| output.contains("media file not found"))
        .unwrap();
    assert!(missing_image.2, "a tool error string marks an error");
    let failed_test = results
        .iter()
        .find(|(_, output, _)| output.contains("-> exit 1"))
        .unwrap();
    assert!(!failed_test.2, "a failed command is not a tool error");

    let interrupted = results
        .iter()
        .find(|(_, output, _)| output.contains("[Command interrupted]"))
        .unwrap();
    let output: Value = serde_json::from_str(interrupted.1).unwrap();
    assert_eq!(output["exit_code"], 130);
    let answers = texts(&session, Role::Assistant);
    assert!(answers.contains(&"Operation interrupted."));
    assert_eq!(
        *answers.last().unwrap(),
        "No, the `sleep 90` command was interrupted before it finished."
    );
}

#[test]
fn a_rotated_session_is_read_at_the_newest_session() {
    let conn = fixture(ROTATION);
    assert_eq!(compression_tip(&conn, ENDED_ID).unwrap(), TIP_ID);
    assert_eq!(compression_tip(&conn, TIP_ID).unwrap(), TIP_ID);

    let session = hermes::read_session_from(&conn, ENDED_ID).unwrap();
    assert_eq!(session.id, TIP_ID);
    assert_eq!(session, hermes::read_session_from(&conn, TIP_ID).unwrap());
    // The new session starts with copies of the kept messages; the rows of
    // the ended session are not added again.
    let prompts = texts(&session, Role::User);
    let git_log = prompts
        .iter()
        .filter(|t| **t == "Run `git log --oneline` and tell me the last commit message.")
        .count();
    assert_eq!(git_log, 1);
    assert_eq!(
        *prompts.last().unwrap(),
        "What was the last line of big.txt? Reply in 1 sentence."
    );
    assert_results_follow_calls(&session);
}

#[test]
fn conversations_and_ids() {
    // The delegation child and the compaction continuation are not
    // conversations of their own.
    let list = hermes::conversations(&fixture(DELEGATION)).unwrap();
    assert_eq!(
        list.iter().map(|c| c.id.as_str()).collect::<Vec<_>>(),
        [DELEGATION_ID]
    );
    let list = hermes::conversations(&fixture(ROTATION)).unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(
        (list[0].id.as_str(), list[0].root_id.as_str()),
        (TIP_ID, ENDED_ID)
    );
    assert_eq!(list[0].title.as_deref(), Some("Centaur thread"));

    let conn = fixture(ROTATION);
    assert_eq!(hermes::find_session(&conn, ENDED_ID).unwrap(), ENDED_ID);
    // Both sessions of one chain match: that is 1 conversation.
    assert_eq!(hermes::find_session(&conn, "20260927_14").unwrap(), TIP_ID);
    // `_` is not a wildcard.
    assert!(matches!(
        hermes::find_session(&conn, "20260927_1417_"),
        Err(Error::NotFound {
            tool: Tool::Hermes,
            ..
        })
    ));
    // A parent and its delegation child are 2 conversations.
    let Err(Error::Ambiguous { tool, ids, .. }) =
        hermes::find_session(&fixture(DELEGATION), "20260926_205")
    else {
        panic!("the prefix must be ambiguous");
    };
    assert_eq!(tool, Tool::Hermes);
    assert_eq!(ids, [DELEGATION_ID, CHILD_ID]);
}

#[test]
fn discover_finds_hermes_sessions_in_the_home() {
    let home = TempDir::hermes_home("hermes-discover", ROTATION);
    let listed = list_sessions(Tool::Hermes, &home.0);
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, TIP_ID);
    assert_eq!(listed[0].path, home.0.join(hermes::DATABASE_FILE));

    let by_prefix = resolve_session(
        Tool::Hermes,
        &"hermes:20260927_1417".parse().unwrap(),
        &home.0,
    )
    .unwrap();
    assert_eq!(by_prefix.id, TIP_ID);
    let latest = resolve_session(Tool::Hermes, &SessionRef::Latest, &home.0).unwrap();
    assert_eq!(latest, by_prefix);
    // A database path reads its newest conversation.
    assert_eq!(
        read_session(Tool::Hermes, &home.0.join(hermes::DATABASE_FILE)).unwrap(),
        latest
    );

    let empty = TempDir::new("hermes-empty");
    assert!(list_sessions(Tool::Hermes, &empty.0).is_empty());
    assert!(matches!(
        resolve_session(Tool::Hermes, &SessionRef::Latest, &empty.0),
        Err(Error::OpenDatabase { .. })
    ));
}

#[test]
fn hermes_converts_into_codex_and_claude() {
    let session = hermes::read_session_from(&fixture(ROTATION), ENDED_ID).unwrap();
    let temp = TempDir::new("hermes-convert");
    for to in [Tool::Codex, Tool::Claude] {
        let target = Target {
            home: temp.0.join(to.id()),
            cwd: "/work/app".to_string(),
            id: Uuid::new_v4(),
            now: Local::now().fixed_offset(),
        };
        let converted = convert(&session, to, &target, &ConvertOptions::default()).unwrap();
        converted.write().unwrap();
        let back = read_session(to, &converted.path).unwrap();

        let preamble = back.messages[0].parts[0].as_text().unwrap();
        assert!(
            preamble.contains(&format!("imported from Hermes Agent into {}", to.label())),
            "{preamble}"
        );
        assert!(preamble.contains(TIP_ID), "{preamble}");
        // The tool calls became text, and the last answer survived.
        let all_user = texts(&back, Role::User).join("\n");
        assert!(
            all_user.contains("0500 This is the last line of big.txt."),
            "{to}"
        );
        assert_eq!(
            *texts(&back, Role::Assistant).last().unwrap(),
            "The last line was “0500 This is the last line of big.txt.”"
        );
    }
}

const CODEX_FIXTURE: &str = "tests/fixtures/codex/recorded-codex-0.154.jsonl";

fn codex_session() -> Session {
    read_session(
        Tool::Codex,
        &Path::new(env!("CARGO_MANIFEST_DIR")).join(CODEX_FIXTURE),
    )
    .unwrap()
}

fn hermes_target(home: &Path) -> Target {
    Target {
        home: home.to_path_buf(),
        cwd: "/work/app".to_string(),
        id: Uuid::new_v4(),
        now: Local::now().fixed_offset(),
    }
}

#[test]
fn a_session_renders_as_a_hermes_import_payload() {
    let home = Path::new("/home/user/.hermes");
    let target = hermes_target(home);
    let converted = convert(
        &codex_session(),
        Tool::Hermes,
        &target,
        &ConvertOptions::default(),
    )
    .unwrap();
    assert_eq!(converted.tool, Tool::Hermes);
    assert_eq!(converted.id, hermes::session_id(&target));
    let id = regex::Regex::new(r"^\d{8}_\d{6}_[0-9a-f]{6}$").unwrap();
    assert!(id.is_match(&converted.id), "{}", converted.id);
    assert_eq!(converted.path, home.join(hermes::DATABASE_FILE));
    assert_eq!(
        converted.resume_command,
        format!("cd /work/app && hermes --resume {}", converted.id)
    );

    let payload: Value = serde_json::from_str(&converted.contents).unwrap();
    let sessions = payload.as_array().unwrap();
    assert_eq!(sessions.len(), 1);
    let session = &sessions[0];
    assert_eq!(session["id"], converted.id);
    assert_eq!(session["source"], session_transfer::BRAND);
    assert_eq!(session["cwd"], "/work/app");
    // Hermes titles are unique, and the source model belongs to Codex.
    assert!(session.get("title").is_none() && session.get("model").is_none());

    let messages = session["messages"].as_array().unwrap();
    let preamble = messages[0]["content"].as_str().unwrap();
    assert!(
        preamble.contains("imported from Codex CLI into Hermes Agent"),
        "{preamble}"
    );
    let roles: Vec<&str> = messages
        .iter()
        .map(|m| m["role"].as_str().unwrap())
        .collect();
    assert_eq!(roles[0], "user");
    assert!(roles.windows(2).all(|pair| pair[0] != pair[1]), "{roles:?}");
    let times: Vec<f64> = messages
        .iter()
        .map(|m| m["timestamp"].as_f64().unwrap())
        .collect();
    assert!(times.windows(2).all(|pair| pair[0] < pair[1]));
    assert_eq!(session["started_at"].as_f64(), Some(times[0]));
}

/// A stand-in for the Hermes Python: it saves the payload and its home, and
/// prints `result`.
#[cfg(unix)]
fn fake_python(dir: &Path, result: &str, code: i32) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = dir.join("fake-python");
    fs::write(
        &path,
        format!(
            "#!/bin/sh\ncat > \"$HERMES_HOME/payload.json\"\nprintf '%s\\n' '{result}'\necho 'import failed here' >&2\nexit {code}\n"
        ),
    )
    .unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
    path
}

#[cfg(unix)]
#[test]
fn import_runs_the_hermes_python() {
    let temp = TempDir::new("hermes-import");
    let home = temp.0.join("hermes");
    let converted = convert(
        &codex_session(),
        Tool::Hermes,
        &hermes_target(&home),
        &ConvertOptions::default(),
    )
    .unwrap();
    let id = &converted.id;

    let imported = fake_python(
        &temp.0,
        &format!(r#"{{"ok": true, "imported_ids": ["{id}"], "skipped_ids": []}}"#),
        0,
    );
    hermes::import(&converted, imported.as_os_str()).unwrap();
    // The payload reached Hermes, with the database directory as its home.
    assert_eq!(
        fs::read_to_string(home.join("payload.json")).unwrap(),
        converted.contents
    );

    let skipped = fake_python(
        &temp.0,
        &format!(r#"{{"ok": true, "imported_ids": [], "skipped_ids": ["{id}"]}}"#),
        0,
    );
    let error = hermes::import(&converted, skipped.as_os_str()).unwrap_err();
    assert!(
        error.to_string().contains("a session with this id exists"),
        "{error}"
    );

    let broken = fake_python(&temp.0, "Traceback", 1);
    let error = hermes::import(&converted, broken.as_os_str()).unwrap_err();
    assert!(error.to_string().contains("import failed here"), "{error}");
}

/// Needs a Hermes Agent install: set `HERMES_PYTHON` to its Python, then run
/// `cargo test --test hermes -- --ignored`.
#[test]
#[ignore = "needs HERMES_PYTHON with Hermes Agent"]
fn a_session_imports_into_a_real_hermes_and_reads_back() {
    std::env::var(hermes::PYTHON_ENV).expect("set HERMES_PYTHON to the Python of Hermes Agent");
    let temp = TempDir::new("hermes-real");
    let home = temp.0.join("hermes");
    let original = codex_session();
    let converted = convert(
        &original,
        Tool::Hermes,
        &hermes_target(&home),
        &ConvertOptions::default(),
    )
    .unwrap();
    // Hermes creates the database.
    converted.write().unwrap();

    let back = hermes::read_session(&converted.path, &converted.id).unwrap();
    assert_eq!(back.id, converted.id);
    assert_eq!(back.cwd.as_deref(), Some("/work/app"));
    let payload: Value = serde_json::from_str(&converted.contents).unwrap();
    let sent: Vec<&str> = payload[0]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["content"].as_str().unwrap())
        .collect();
    let read: Vec<&str> = back
        .messages
        .iter()
        .flat_map(|m| m.parts.iter().filter_map(Part::as_text))
        .collect();
    assert_eq!(read, sent);
    assert_eq!(
        list_sessions(Tool::Hermes, &home)
            .iter()
            .map(|s| s.id.as_str())
            .collect::<Vec<_>>(),
        [converted.id.as_str()]
    );

    // The session never overwrites one with its id.
    let error = converted.write().unwrap_err();
    assert!(
        error.to_string().contains("a session with this id exists"),
        "{error}"
    );
}
