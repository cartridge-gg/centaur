//! `harness-server transcript convert`, and whether Codex accepts what it writes.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use codex_protocol::protocol::{RolloutItem, RolloutLine};
use serde_json::{Value, json};
use uuid::Uuid;

const CLAUDE_FIXTURE: &str = "19fa6065-e665-49b6-9565-cf655fa8bc17.jsonl";
const CODEX_FIXTURE: &str = "recorded-codex-0.154.jsonl";

fn fixture(tool: &str, name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../session-transfer/tests/fixtures")
        .join(tool)
        .join(name)
}

struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("harness-server-{name}-{}", Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Runs `harness-server transcript convert` and returns its JSON report.
fn convert(args: &[&str], homes: &Path) -> Value {
    let output = Command::new(env!("CARGO_BIN_EXE_harness-server"))
        .args(["transcript", "convert"])
        .args(args)
        .arg("--codex-home")
        .arg(homes.join("codex"))
        .arg("--claude-home")
        .arg(homes.join("claude"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn claude_to_codex_writes_a_rollout_that_codex_can_parse() {
    let homes = TempDir::new("to-codex");
    let source = fixture("claude", CLAUDE_FIXTURE);
    let report = convert(
        &[
            "--from",
            "claude",
            source.to_str().unwrap(),
            "--model-provider",
            "pool-proxy",
        ],
        &homes.0,
    );
    assert_eq!(report["tool"], "codex");
    assert_eq!(report["source"]["tool"], "claude");
    let id = report["id"].as_str().unwrap();
    let path = PathBuf::from(report["path"].as_str().unwrap());
    assert!(path.starts_with(homes.0.join("codex/sessions")));
    assert!(path.to_string_lossy().ends_with(&format!("-{id}.jsonl")));
    assert_eq!(
        report["resumeCommand"],
        format!("cd /work/app && codex resume {id}")
    );

    // Every line must deserialize as Codex's own rollout schema.
    let text = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<RolloutLine> = text
        .lines()
        .map(|line| {
            serde_json::from_str(line)
                .unwrap_or_else(|e| panic!("not a Codex rollout line ({e}): {line}"))
        })
        .collect();
    let RolloutItem::SessionMeta(meta) = &lines[0].item else {
        panic!("the first line must be session_meta");
    };
    assert_eq!(meta.meta.id.to_string(), id);
    assert_eq!(meta.meta.model_provider.as_deref(), Some("pool-proxy"));
    assert!(lines[1..].iter().all(|l| matches!(
        l.item,
        RolloutItem::ResponseItem(_) | RolloutItem::EventMsg(_)
    )));
    assert!(
        text.contains("[centaur] This conversation was imported from Claude Code into Codex CLI.")
    );
}

#[test]
fn codex_to_claude_writes_a_session_under_the_encoded_cwd() {
    let homes = TempDir::new("to-claude");
    let source = fixture("codex", CODEX_FIXTURE);
    let report = convert(
        &[
            "--from",
            "codex",
            source.to_str().unwrap(),
            "--cwd",
            "/home/agent/state/workspace",
        ],
        &homes.0,
    );
    let id = report["id"].as_str().unwrap();
    assert_eq!(
        PathBuf::from(report["path"].as_str().unwrap()),
        homes
            .0
            .join("claude/projects/-home-agent-state-workspace")
            .join(format!("{id}.jsonl"))
    );
    let text = std::fs::read_to_string(report["path"].as_str().unwrap()).unwrap();
    for line in text.lines() {
        let record: Value = serde_json::from_str(line).unwrap();
        assert_eq!(record["sessionId"], id);
    }
}

#[test]
fn dry_run_writes_nothing_and_errors_are_reported() {
    let homes = TempDir::new("dry-run");
    let source = fixture("claude", CLAUDE_FIXTURE);
    let report = convert(
        &["--from", "claude", source.to_str().unwrap(), "--dry-run"],
        &homes.0,
    );
    assert_eq!(report["dryRun"], true);
    assert!(!Path::new(report["path"].as_str().unwrap()).exists());

    let output = Command::new(env!("CARGO_BIN_EXE_harness-server"))
        .args(["transcript", "convert", "--from", "codex", "--to", "codex"])
        .arg(fixture("codex", CODEX_FIXTURE))
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("already belongs to Codex CLI"));
}

/// A Responses API endpoint that answers every turn with "RESUMED" and keeps
/// the request bodies.
fn mock_responses_server() -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}/v1", listener.local_addr().unwrap());
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&bodies);
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut request_line = String::new();
            reader.read_line(&mut request_line).unwrap();
            let mut length = 0;
            loop {
                let mut header = String::new();
                reader.read_line(&mut header).unwrap();
                if header == "\r\n" || header.is_empty() {
                    break;
                }
                if let Some(value) = header.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = value.trim().parse().unwrap_or(0);
                }
            }
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            if !request_line.starts_with("POST") || !request_line.contains("/responses") {
                let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\n\r\n");
                continue;
            }
            seen.lock()
                .unwrap()
                .push(String::from_utf8_lossy(&body).into_owned());
            let events = [
                json!({"type": "response.created", "response": {"id": "resp_1"}}),
                json!({"type": "response.output_item.done", "item": {"type": "message", "role": "assistant", "id": "msg_1", "content": [{"type": "output_text", "text": "RESUMED"}]}}),
                json!({"type": "response.completed", "response": {"id": "resp_1", "usage": {"input_tokens": 1, "input_tokens_details": null, "output_tokens": 1, "output_tokens_details": null, "total_tokens": 2}}}),
            ];
            let sse: String = events
                .iter()
                .map(|e| format!("event: {}\ndata: {e}\n\n", e["type"].as_str().unwrap()))
                .collect();
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\n\r\n{sse}",
                sse.len()
            );
        }
    });
    (base, bodies)
}

#[test]
#[ignore = "runs the real codex binary (CODEX_BIN or codex on PATH) against a local mock server"]
fn real_codex_app_server_resumes_a_converted_rollout() {
    let homes = TempDir::new("real-resume");
    let codex_home = homes.0.join("codex");
    let workspace = homes.0.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let (base_url, bodies) = mock_responses_server();
    std::fs::create_dir_all(&codex_home).unwrap();
    std::fs::write(
        codex_home.join("config.toml"),
        format!(
            "model = \"gpt-5-codex\"\nmodel_provider = \"mock\"\n\n[model_providers.mock]\nname = \"mock\"\nbase_url = \"{base_url}\"\nwire_api = \"responses\"\nrequest_max_retries = 0\nstream_max_retries = 0\n"
        ),
    )
    .unwrap();

    let source = fixture("claude", CLAUDE_FIXTURE);
    let report = convert(
        &[
            "--from",
            "claude",
            source.to_str().unwrap(),
            "--cwd",
            workspace.to_str().unwrap(),
            "--model-provider",
            "mock",
        ],
        &homes.0,
    );
    let thread_id = report["id"].as_str().unwrap().to_string();
    let rollout = report["path"].as_str().unwrap().to_string();

    let bin = std::env::var("CODEX_BIN").unwrap_or_else(|_| "codex".to_string());
    let mut child = Command::new(bin)
        .args(["app-server", "--listen", "stdio://"])
        .env("CODEX_HOME", &codex_home)
        .current_dir(&workspace)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    let mut send = |value: Value| {
        writeln!(stdin, "{value}").unwrap();
        stdin.flush().unwrap();
    };
    let wait_for = |pred: &dyn Fn(&Value) -> bool| -> Value {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let line = rx
                .recv_timeout(remaining)
                .expect("codex app-server answered in time");
            if let Ok(value) = serde_json::from_str::<Value>(&line)
                && pred(&value)
            {
                return value;
            }
        }
    };

    send(
        json!({"id": 1, "method": "initialize", "params": {"clientInfo": {"name": "cargo-test", "title": null, "version": "0"}, "capabilities": null}}),
    );
    wait_for(&|v| v["id"] == 1);
    // Resume by id only: `path` needs the experimental API. Codex finds the
    // rollout under $CODEX_HOME/sessions by its file name.
    assert!(Path::new(&rollout).starts_with(codex_home.join("sessions")));
    send(
        json!({"id": 2, "method": "thread/resume", "params": {"threadId": thread_id, "modelProvider": "mock", "cwd": workspace, "excludeTurns": false}}),
    );
    let resumed = wait_for(&|v| v["id"] == 2);
    assert!(
        resumed.get("error").is_none(),
        "thread/resume failed: {resumed}"
    );
    assert_eq!(resumed["result"]["thread"]["id"], thread_id.as_str());
    let turns = resumed["result"]["thread"]["turns"].as_array().unwrap();
    assert!(
        !turns.is_empty(),
        "the imported history has no turns: {resumed}"
    );

    send(
        json!({"id": 3, "method": "turn/start", "params": {"threadId": thread_id, "input": [{"type": "text", "text": "Continue.", "text_elements": []}]}}),
    );
    let completed = wait_for(&|v| v["method"] == "turn/completed");
    child.kill().unwrap();
    child.wait().unwrap();
    assert_eq!(
        completed["params"]["turn"]["status"], "completed",
        "{completed}"
    );

    // The model request carries the imported history, oldest first, then the new prompt.
    let bodies = bodies.lock().unwrap();
    let body = bodies.last().expect("codex called the mock model");
    let preamble = body
        .find("This conversation was imported from Claude Code into Codex CLI")
        .expect("preamble in the request");
    let last_answer = body
        .find("Fixed the bug by removing the extra")
        .expect("last imported answer in the request");
    let prompt = body.rfind("Continue.").expect("new prompt in the request");
    assert!(preamble < last_answer && last_answer < prompt);
}
