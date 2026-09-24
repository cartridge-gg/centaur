//! The switchable mode (`CENTAUR_HARNESS_SWITCHING=1`): a session moves
//! between Codex and Claude Code when the model provider is exhausted. Shell
//! scripts stand in for the `codex` and `claude` CLIs; the session files are
//! the recorded fixtures of session-transfer.

#![cfg(unix)]

use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use session_transfer::claude::encode_project_dir;
use uuid::Uuid;

const CODEX_FIXTURE: &str = "codex/recorded-codex-0.154.jsonl";
const CODEX_SESSION_ID: &str = "01a0d157-b90c-7883-a302-02a9d97e383a";
const CLAUDE_FIXTURE: &str = "claude/19fa6065-e665-49b6-9565-cf655fa8bc17.jsonl";
const CLAUDE_SESSION_ID: &str = "19fa6065-e665-49b6-9565-cf655fa8bc17";
/// The first prompt of both recorded sessions.
const FIRST_PROMPT: &str = "Read calc.py and test_calc.py.";
const PROMPT: &str = "Fix the failing test.";
const TIMEOUT: Duration = Duration::from_secs(30);

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../session-transfer/tests/fixtures")
        .join(name)
}

fn failover_fixture(name: &str) -> String {
    std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/failover")
            .join(name),
    )
    .unwrap()
}

fn write_script(path: &Path, body: &str) {
    std::fs::write(path, body).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

/// A fake `codex app-server`. `turn/start` prints the lines of
/// `$FAKE_CODEX_TURN_LINES` (with `THREAD_ID` replaced), or an answer.
/// `turn/interrupt` ends the turn as interrupted.
const FAKE_CODEX: &str = r#"#!/bin/sh
if [ "${1:-}" = "app-server" ] && [ "${2:-}" = "--help" ]; then
  echo '--listen stdio://'
  exit 0
fi
thread="${FAKE_CODEX_THREAD_ID:-thread-1}"
request_id() { printf '%s' "$1" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p'; }
while IFS= read -r line; do
  printf '%s\n' "$line" >> "$FAKE_LOG_DIR/codex.log"
  id=$(request_id "$line")
  case "$line" in
    *'"method":"initialize"'*)
      printf '{"id":%s,"result":{"userAgent":"fake-codex"}}\n' "$id" ;;
    *'"method":"thread/start"'*)
      printf '{"id":%s,"result":{"thread":{"id":"%s"}}}\n' "$id" "$thread" ;;
    *'"method":"thread/resume"'*)
      thread=$(printf '%s' "$line" | sed -n 's/.*"threadId":"\([^"]*\)".*/\1/p')
      printf '{"id":%s,"result":{"thread":{"id":"%s"}}}\n' "$id" "$thread" ;;
    *'"method":"turn/start"'*)
      printf '{"id":%s,"result":{"turn":{"id":"turn-1"}}}\n' "$id"
      printf '{"method":"turn/started","params":{"threadId":"%s","turn":{"id":"turn-1","items":[],"status":"inProgress","error":null}}}\n' "$thread"
      if [ -n "${FAKE_CODEX_TURN_LINES:-}" ]; then
        sed "s/THREAD_ID/$thread/g" "$FAKE_CODEX_TURN_LINES"
        continue
      fi
      printf '{"method":"item/completed","params":{"threadId":"%s","turnId":"turn-1","item":{"type":"agentMessage","id":"answer-1","text":"codex answer","phase":"final_answer"}}}\n' "$thread"
      printf '{"method":"turn/completed","params":{"threadId":"%s","turn":{"id":"turn-1","items":[],"status":"completed","error":null}}}\n' "$thread" ;;
    *'"method":"turn/interrupt"'*)
      printf '{"id":%s,"result":{}}\n' "$id"
      printf '{"method":"turn/completed","params":{"threadId":"%s","turn":{"id":"turn-1","items":[],"status":"interrupted","error":null}}}\n' "$thread" ;;
  esac
done
"#;

/// A fake `claude --print`. It logs its arguments and its input, and prints
/// the lines of `$FAKE_CLAUDE_LINES`, or an answer.
const FAKE_CLAUDE: &str = r#"#!/bin/sh
printf 'ARGS %s\n' "$*" >> "$FAKE_LOG_DIR/claude.log"
session=""
previous=""
for arg in "$@"; do
  case "$previous" in --resume|--session-id) session="$arg" ;; esac
  previous="$arg"
done
IFS= read -r input
printf 'STDIN %s\n' "$input" >> "$FAKE_LOG_DIR/claude.log"
printf '{"type":"system","subtype":"init","session_id":"%s"}\n' "$session"
if [ -n "${FAKE_CLAUDE_LINES:-}" ]; then
  cat "$FAKE_CLAUDE_LINES"
  exit 0
fi
echo '{"type":"assistant","message":{"id":"msg_1","content":[{"type":"text","text":"claude answer"}]}}'
echo '{"type":"result","subtype":"success","result":"claude answer"}'
"#;

/// A state volume with a workspace, the fake CLIs and their logs.
struct Sandbox {
    root: PathBuf,
    workspace: PathBuf,
    env: Vec<(String, String)>,
}

impl Sandbox {
    fn new(name: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "harness-server-switching-{name}-{}",
            Uuid::new_v4().simple()
        ));
        for dir in ["workspace", "codex", "claude", "state", "bin", "logs"] {
            std::fs::create_dir_all(root.join(dir)).unwrap();
        }
        // The harness resolves its cwd without symlinks.
        let root = root.canonicalize().unwrap();
        write_script(&root.join("bin/codex"), FAKE_CODEX);
        write_script(&root.join("bin/claude"), FAKE_CLAUDE);
        let path = |dir: &str| root.join(dir).to_string_lossy().into_owned();
        let env = vec![
            ("CENTAUR_HARNESS_SWITCHING", "1".to_string()),
            (
                "CENTAUR_PROVIDER_EXHAUSTED_MARKERS",
                "pool_exhausted".to_string(),
            ),
            ("CODEX_HOME", path("codex")),
            ("CLAUDE_CONFIG_DIR", path("claude")),
            ("CENTAUR_STATE_DIR", path("state")),
            ("CODEX_BIN", path("bin/codex")),
            ("CLAUDE_BIN", path("bin/claude")),
            ("FAKE_LOG_DIR", path("logs")),
            ("HOME", root.to_string_lossy().into_owned()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        Self {
            workspace: root.join("workspace"),
            root,
            env,
        }
    }

    fn set(&mut self, key: &str, value: impl Into<String>) {
        self.env.push((key.to_string(), value.into()));
    }

    fn path(&self, relative: &str) -> PathBuf {
        self.root.join(relative)
    }

    /// Writes `lines` to a file and returns its path.
    fn lines_file(&self, name: &str, lines: &[Value]) -> String {
        let path = self.path(name);
        let text: String = lines.iter().map(|l| format!("{l}\n")).collect();
        std::fs::write(&path, text).unwrap();
        path.to_string_lossy().into_owned()
    }

    fn claude_project(&self) -> PathBuf {
        self.path("claude/projects")
            .join(encode_project_dir(&self.workspace.to_string_lossy()))
    }

    /// The recorded Codex session, as the active Codex thread.
    fn seed_codex_session(&mut self) {
        let dir = self.path("codex/sessions/2026/09/23");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::copy(
            fixture(CODEX_FIXTURE),
            dir.join(format!(
                "rollout-2026-09-23T10-00-00-{CODEX_SESSION_ID}.jsonl"
            )),
        )
        .unwrap();
        self.set("FAKE_CODEX_THREAD_ID", CODEX_SESSION_ID);
    }

    /// The recorded Claude session, as the active Claude session, followed by
    /// `PROMPT` and the failed API request that Claude Code saves for it.
    fn seed_claude_session(&self) {
        let recorded = std::fs::read_to_string(fixture(CLAUDE_FIXTURE)).unwrap();
        let last_uuid = recorded
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter_map(|record| record["uuid"].as_str().map(str::to_owned))
            .next_back()
            .unwrap();
        let prompt = json!({"type": "user", "uuid": "u-failed", "parentUuid": last_uuid,
            "sessionId": CLAUDE_SESSION_ID, "cwd": "/work/app",
            "message": {"role": "user", "content": PROMPT}});
        let error = json!({"type": "assistant", "uuid": "a-failed", "parentUuid": "u-failed",
            "sessionId": CLAUDE_SESSION_ID, "isApiErrorMessage": true, "error": "server_error",
            "message": {"id": "m-failed", "role": "assistant", "model": "<synthetic>",
                        "content": [{"type": "text", "text": "API Error: 503 pool_exhausted"}]}});
        std::fs::create_dir_all(self.claude_project()).unwrap();
        std::fs::write(
            self.claude_project()
                .join(format!("{CLAUDE_SESSION_ID}.jsonl")),
            format!("{recorded}{prompt}\n{error}\n"),
        )
        .unwrap();
        std::fs::write(
            self.path("claude/centaur-session-id"),
            format!("{CLAUDE_SESSION_ID}\n"),
        )
        .unwrap();
    }

    fn spawn(&self, harness: &str) -> Server {
        let mut command = Command::new(env!("CARGO_BIN_EXE_harness-server"));
        command
            .arg(harness)
            .current_dir(&self.workspace)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for key in [
            "CENTAUR_CLAUDE_APP_BRIDGE_COMMAND",
            "CODEX_MODEL",
            "CODEX_MODEL_PROVIDER",
            "OPENROUTER_MODEL",
            "CLAUDE_MODEL",
            "CODEX_CONTINUE_THREAD_ID",
        ] {
            command.env_remove(key);
        }
        command.envs(self.env.iter().map(|(k, v)| (k, v)));
        Server::spawn(command)
    }

    fn log(&self, name: &str) -> String {
        std::fs::read_to_string(self.path("logs").join(name)).unwrap_or_default()
    }

    fn read(&self, relative: &str) -> String {
        std::fs::read_to_string(self.path(relative)).unwrap_or_default()
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

struct Server {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: Receiver<String>,
    stderr: Receiver<String>,
}

impl Server {
    fn spawn(mut command: Command) -> Self {
        let mut child = command.spawn().unwrap();
        let stdin = child.stdin.take();
        let lines = pipe_lines(child.stdout.take().unwrap());
        let stderr = pipe_lines(child.stderr.take().unwrap());
        Self {
            child,
            stdin,
            lines,
            stderr,
        }
    }

    fn send(&mut self, value: Value) {
        let stdin = self.stdin.as_mut().unwrap();
        writeln!(stdin, "{value}").unwrap();
        stdin.flush().unwrap();
    }

    /// Output lines up to and including the next `turn/completed`.
    fn read_turn(&mut self) -> Vec<Value> {
        let deadline = Instant::now() + TIMEOUT;
        let mut out = Vec::new();
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let Ok(line) = self.lines.recv_timeout(remaining) else {
                panic!(
                    "no turn/completed in time; got {out:#?}\nstderr: {}",
                    self.stderr.try_iter().collect::<Vec<_>>().join("\n")
                );
            };
            let value: Value =
                serde_json::from_str(&line).unwrap_or_else(|e| panic!("not JSON ({e}): {line}"));
            let done = value["method"] == "turn/completed";
            out.push(value);
            if done {
                return out;
            }
        }
    }

    /// Closes stdin, and returns the remaining output after a clean exit.
    fn finish(mut self) -> Vec<Value> {
        self.stdin = None;
        let deadline = Instant::now() + TIMEOUT;
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert!(
                    status.success(),
                    "{status}\nstderr: {}",
                    self.stderr.try_iter().collect::<Vec<_>>().join("\n")
                );
                break;
            }
            assert!(Instant::now() < deadline, "harness-server did not exit");
            thread::sleep(Duration::from_millis(20));
        }
        self.lines
            .try_iter()
            .map(|line| serde_json::from_str(&line).unwrap())
            .collect()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn pipe_lines(reader: impl std::io::Read + Send + 'static) -> Receiver<String> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(reader).lines().map_while(Result::ok) {
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    rx
}

fn user_line(text: &str) -> Value {
    json!({"type": "user", "message": {"content": text}, "thread_key": "chat:C1:1.0"})
}

fn notices(lines: &[Value]) -> Vec<&Value> {
    lines
        .iter()
        .filter(|l| l["method"] == "centaur/providerFailover")
        .map(|l| &l["params"])
        .collect()
}

fn agent_texts(lines: &[Value]) -> Vec<&str> {
    lines
        .iter()
        .filter(|l| {
            l["method"] == "item/completed" && l["params"]["item"]["type"] == "agentMessage"
        })
        .filter_map(|l| l["params"]["item"]["text"].as_str())
        .collect()
}

/// The recorded Codex pool-exhaustion lines, with the fake's ids.
fn codex_exhaustion(which: &[usize]) -> Vec<Value> {
    let recorded: Vec<Value> = failover_fixture("codex-app-server.jsonl")
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    which
        .iter()
        .map(|&i| {
            let mut value = recorded[i].clone();
            value["params"]["threadId"] = json!("THREAD_ID");
            if value["params"].get("turnId").is_some() {
                value["params"]["turnId"] = json!("turn-1");
            }
            if value["params"].get("turn").is_some() {
                value["params"]["turn"]["id"] = json!("turn-1");
            }
            value
        })
        .collect()
}

/// The recorded Claude Code pool-exhaustion lines for `session_id`.
fn claude_exhaustion(session_id: &str) -> Vec<Value> {
    failover_fixture("claude-stream.jsonl")
        .lines()
        .map(|line| {
            let mut value: Value = serde_json::from_str(line).unwrap();
            value["session_id"] = json!(session_id);
            value
        })
        .collect()
}

const RETRY_NOTICE: usize = 0;

#[test]
fn codex_pool_exhaustion_moves_the_turn_to_claude() {
    let mut sandbox = Sandbox::new("codex-to-claude");
    sandbox.seed_codex_session();
    // Codex retries the request and reports each retry: the first notice is
    // enough to switch. The fake then waits for the interrupt.
    let lines = sandbox.lines_file("exhausted.jsonl", &codex_exhaustion(&[RETRY_NOTICE]));
    sandbox.set("FAKE_CODEX_TURN_LINES", lines);

    let mut server = sandbox.spawn("codex");
    let mut line = user_line(PROMPT);
    line["model"] = json!("gpt-5-codex");
    server.send(line);
    let output = server.read_turn();
    let rest = server.finish();
    assert!(rest.is_empty(), "{rest:#?}");

    let [notice] = notices(&output)[..] else {
        panic!("one notice: {output:#?}");
    };
    assert_eq!(notice["from"], "codex");
    assert_eq!(notice["to"], "claudecode");
    assert_eq!(notice["mode"], "reactive");
    assert_eq!(notice["resume"], "replay");
    assert_eq!(notice["history"], "converted");
    assert_eq!(notice["sourceSessionId"], CODEX_SESSION_ID);
    assert_eq!(notice["providerExhausted"]["signal"], "poolMarker");
    assert_eq!(notice["providerExhausted"]["resetAt"], 1_790_220_376);
    let session_id = notice["sessionId"].as_str().unwrap();

    // The client sees neither the retry notice nor the interrupted turn.
    assert!(output.iter().all(|l| l["method"] != "error"), "{output:#?}");
    let completed: Vec<&Value> = output
        .iter()
        .filter(|l| l["method"] == "turn/completed")
        .collect();
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0]["params"]["turn"]["status"], "completed");
    assert_eq!(agent_texts(&output), ["claude answer"]);
    assert!(
        sandbox
            .log("codex.log")
            .contains(r#""method":"turn/interrupt""#)
    );

    // Claude resumes the converted session, with the prompt and without the
    // Codex model.
    let claude = sandbox.log("claude.log");
    assert!(
        claude.contains(&format!("--resume {session_id}")),
        "{claude}"
    );
    assert!(!claude.contains("gpt-5-codex"), "{claude}");
    assert!(claude.contains(PROMPT), "{claude}");
    let converted =
        std::fs::read_to_string(sandbox.claude_project().join(format!("{session_id}.jsonl")))
            .unwrap();
    assert!(converted.contains(FIRST_PROMPT));
    assert!(converted.contains("imported from Codex CLI into Claude Code"));
    assert_eq!(sandbox.read("state/centaur-active-harness"), "claudecode\n");
    assert_eq!(
        sandbox.read("claude/centaur-session-id"),
        format!("{session_id}\n")
    );
}

#[test]
fn claude_pool_exhaustion_moves_the_turn_to_codex() {
    let mut sandbox = Sandbox::new("claude-to-codex");
    sandbox.seed_claude_session();
    let lines = sandbox.lines_file("exhausted.jsonl", &claude_exhaustion(CLAUDE_SESSION_ID));
    sandbox.set("FAKE_CLAUDE_LINES", lines);

    let mut server = sandbox.spawn("claude-code");
    server.send(user_line(PROMPT));
    let output = server.read_turn();
    server.finish();

    let [notice] = notices(&output)[..] else {
        panic!("one notice: {output:#?}");
    };
    assert_eq!(notice["from"], "claudecode");
    assert_eq!(notice["to"], "codex");
    assert_eq!(notice["resume"], "replay");
    assert_eq!(notice["sourceSessionId"], CLAUDE_SESSION_ID);
    assert_eq!(notice["providerExhausted"]["resetAt"], 1_790_220_353);
    let thread_id = notice["sessionId"].as_str().unwrap();

    // Only the Codex turn reaches the client.
    let statuses: Vec<&Value> = output
        .iter()
        .filter(|l| l["method"] == "turn/completed")
        .map(|l| &l["params"]["turn"]["status"])
        .collect();
    assert_eq!(statuses, ["completed"]);
    assert_eq!(agent_texts(&output), ["codex answer"]);

    // Codex resumes the converted thread and gets the prompt again.
    let codex = sandbox.log("codex.log");
    let resume = codex
        .lines()
        .find(|l| l.contains(r#""method":"thread/resume""#))
        .expect("thread/resume");
    assert!(
        resume.contains(&format!(r#""threadId":"{thread_id}""#)),
        "{resume}"
    );
    let turn = codex
        .lines()
        .find(|l| l.contains(r#""method":"turn/start""#))
        .unwrap();
    assert!(turn.contains(PROMPT), "{turn}");

    // The rollout has the history, but not the failed request: the prompt
    // is sent again instead.
    let rollout = std::fs::read_dir(sandbox.path("codex/sessions"))
        .unwrap()
        .flat_map(|year| walk(&year.unwrap().path()))
        .find(|p| p.to_string_lossy().ends_with(&format!("{thread_id}.jsonl")))
        .expect("the converted rollout");
    let rollout = std::fs::read_to_string(rollout).unwrap();
    assert!(rollout.contains(FIRST_PROMPT));
    assert!(!rollout.contains(PROMPT));
    assert!(!rollout.contains("API Error"));
    assert_eq!(
        sandbox.read("codex/centaur-thread-id"),
        format!("{thread_id}\n")
    );
    assert_eq!(sandbox.read("state/centaur-active-harness"), "codex\n");
}

fn walk(dir: &Path) -> Vec<PathBuf> {
    if dir.is_file() {
        return vec![dir.to_path_buf()];
    }
    std::fs::read_dir(dir)
        .unwrap()
        .flat_map(|entry| walk(&entry.unwrap().path()))
        .collect()
}

#[test]
fn a_turn_with_failover_off_fails_as_before() {
    let mut sandbox = Sandbox::new("failover-off");
    sandbox.seed_claude_session();
    let lines = sandbox.lines_file("exhausted.jsonl", &claude_exhaustion(CLAUDE_SESSION_ID));
    sandbox.set("FAKE_CLAUDE_LINES", lines);

    let mut server = sandbox.spawn("claude-code");
    let mut line = user_line(PROMPT);
    line["centaur"] = json!({"failover": {"enabled": false}});
    server.send(line);
    let output = server.read_turn();
    server.finish();

    assert!(notices(&output).is_empty(), "{output:#?}");
    let completed = output.last().unwrap();
    assert_eq!(completed["params"]["turn"]["status"], "failed");
    assert_eq!(
        completed["params"]["centaur"]["providerExhausted"]["signal"],
        "poolMarker"
    );
    assert!(sandbox.log("codex.log").is_empty());
    // The child did not get the directive.
    assert!(!sandbox.log("claude.log").contains("centaur"));
}

#[test]
fn a_turn_that_did_work_continues_instead_of_running_again() {
    let mut sandbox = Sandbox::new("continue");
    sandbox.seed_codex_session();
    let mut lines = vec![
        json!({"method": "item/completed", "params": {"threadId": "THREAD_ID", "turnId": "turn-1",
        "item": {"type": "commandExecution", "id": "cmd-1", "command": "pytest", "status": "completed"}}}),
    ];
    lines.extend(codex_exhaustion(&[RETRY_NOTICE]));
    let lines = sandbox.lines_file("exhausted.jsonl", &lines);
    sandbox.set("FAKE_CODEX_TURN_LINES", lines);

    let mut server = sandbox.spawn("codex");
    server.send(user_line(PROMPT));
    let output = server.read_turn();
    server.finish();

    let [notice] = notices(&output)[..] else {
        panic!("one notice: {output:#?}");
    };
    assert_eq!(notice["resume"], "continue");
    // The command ran on Codex, and the client saw it.
    assert!(
        output
            .iter()
            .any(|l| l["params"]["item"]["type"] == "commandExecution")
    );
    let claude = sandbox.log("claude.log");
    let stdin = claude.lines().find(|l| l.starts_with("STDIN")).unwrap();
    assert!(stdin.contains("[Centaur]"), "{stdin}");
    assert!(!stdin.contains(PROMPT), "{stdin}");
}

#[test]
fn a_turn_fails_over_only_once() {
    let mut sandbox = Sandbox::new("once");
    sandbox.seed_codex_session();
    let lines = sandbox.lines_file("codex-exhausted.jsonl", &codex_exhaustion(&[RETRY_NOTICE]));
    sandbox.set("FAKE_CODEX_TURN_LINES", lines);
    // The Claude pool is exhausted too. The fake answers for any session id.
    let lines = sandbox.lines_file("claude-exhausted.jsonl", &claude_exhaustion("any"));
    sandbox.set("FAKE_CLAUDE_LINES", lines);

    let mut server = sandbox.spawn("codex");
    server.send(user_line(PROMPT));
    let output = server.read_turn();
    server.finish();

    assert_eq!(notices(&output).len(), 1, "{output:#?}");
    let completed = output.last().unwrap();
    assert_eq!(completed["params"]["turn"]["status"], "failed");
    assert!(completed["params"]["centaur"]["providerExhausted"].is_object());
}

#[test]
fn the_marker_and_the_directive_choose_the_harness() {
    let sandbox = Sandbox::new("marker");
    std::fs::write(sandbox.path("state/centaur-active-harness"), "claudecode\n").unwrap();

    // The session moved to Claude in an earlier process: old arguments do
    // not move it back.
    let mut server = sandbox.spawn("codex");
    server.send(user_line("hello"));
    let first = server.read_turn();
    assert!(notices(&first).is_empty(), "{first:#?}");
    assert_eq!(agent_texts(&first), ["claude answer"]);

    // The control plane asks for Codex. The Claude session has no transcript
    // (the fake writes none), so Codex starts a new thread.
    let mut line = user_line("hello again");
    line["centaur"] = json!({"harness": "codex"});
    server.send(line);
    let second = server.read_turn();
    server.finish();
    let [notice] = notices(&second)[..] else {
        panic!("one notice: {second:#?}");
    };
    assert_eq!(notice["mode"], "proactive");
    assert_eq!(notice["from"], "claudecode");
    assert_eq!(notice["to"], "codex");
    assert_eq!(notice["history"], "empty");
    assert_eq!(agent_texts(&second), ["codex answer"]);
    assert!(
        sandbox
            .log("codex.log")
            .contains(r#""method":"thread/start""#)
    );
    assert_eq!(sandbox.read("state/centaur-active-harness"), "codex\n");
}
