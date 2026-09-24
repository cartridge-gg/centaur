//! The switchable mode (`CENTAUR_HARNESS_SWITCHING=1`): a session moves
//! between Codex and Claude Code when the model provider is exhausted. Shell
//! scripts stand in for the `codex` and `claude` CLIs; the session files are
//! the recorded fixtures of session-transfer.

#![cfg(unix)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
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
/// `$FAKE_CODEX_TURN_LINES` (with `THREAD_ID` replaced) while that file
/// exists, or an answer.
/// `turn/interrupt` ends the turn as interrupted. With
/// `FAKE_CODEX_RESUME_EMPTY=1`, `thread/resume` returns a thread with no
/// turns, as Codex does for a rollout that it cannot parse.
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
      if [ "${FAKE_CODEX_RESUME_EMPTY:-}" = "1" ]; then
        printf '{"id":%s,"result":{"thread":{"id":"%s","turns":[]}}}\n' "$id" "$thread"
      else
        printf '{"id":%s,"result":{"thread":{"id":"%s","turns":[{"id":"t0","items":[]}]}}}\n' "$id" "$thread"
      fi ;;
    *'"method":"turn/start"'*)
      printf '{"id":%s,"result":{"turn":{"id":"turn-1"}}}\n' "$id"
      printf '{"method":"turn/started","params":{"threadId":"%s","turn":{"id":"turn-1","items":[],"status":"inProgress","error":null}}}\n' "$thread"
      if [ -n "${FAKE_CODEX_TURN_LINES:-}" ] && [ -f "$FAKE_CODEX_TURN_LINES" ]; then
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
/// the lines of `$FAKE_CLAUDE_LINES` while that file exists, or an answer.
/// With `FAKE_CLAUDE_RESUME_FAILS=1`, `--resume` fails as Claude Code does
/// for a transcript that it cannot find.
const FAKE_CLAUDE: &str = r#"#!/bin/sh
printf 'ARGS %s\n' "$*" >> "$FAKE_LOG_DIR/claude.log"
session=""
previous=""
resume=""
for arg in "$@"; do
  case "$previous" in --resume|--session-id) session="$arg" ;; esac
  [ "$arg" = "--resume" ] && resume=1
  previous="$arg"
done
IFS= read -r input
printf 'STDIN %s\n' "$input" >> "$FAKE_LOG_DIR/claude.log"
if [ -n "$resume" ] && [ "${FAKE_CLAUDE_RESUME_FAILS:-}" = "1" ]; then
  echo "No conversation found with session ID: $session" >&2
  printf '{"type":"result","subtype":"error_during_execution","is_error":true,"num_turns":0,"session_id":"%s","errors":["No conversation found with session ID: %s"]}\n' "$session" "$session"
  exit 1
fi
printf '{"type":"system","subtype":"init","session_id":"%s"}\n' "$session"
if [ -n "${FAKE_CLAUDE_LINES:-}" ] && [ -f "$FAKE_CLAUDE_LINES" ]; then
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
        std::fs::write(
            self.path("codex/centaur-thread-id"),
            format!("{CODEX_SESSION_ID}\n"),
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
        self.read_until(|value| value["method"] == "turn/completed")
    }

    /// Output lines up to and including the line that ends the turn: its
    /// `turn/completed`, or an `error` that is not retried.
    fn read_until_end(&mut self) -> Vec<Value> {
        self.read_until(|value| {
            value["method"] == "turn/completed"
                || (value["method"] == "error" && value["params"]["willRetry"] != true)
        })
    }

    fn read_until(&mut self, ends: impl Fn(&Value) -> bool) -> Vec<Value> {
        let deadline = Instant::now() + TIMEOUT;
        let mut out = Vec::new();
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let Ok(line) = self.lines.recv_timeout(remaining) else {
                panic!(
                    "no end of the turn in time; got {out:#?}\nstderr: {}",
                    self.stderr.try_iter().collect::<Vec<_>>().join("\n")
                );
            };
            let value: Value =
                serde_json::from_str(&line).unwrap_or_else(|e| panic!("not JSON ({e}): {line}"));
            let done = ends(&value);
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

    // The journal names both sessions; the original is unchanged.
    let journal = sandbox.read("state/centaur-failover-journal.jsonl");
    let [entry] = &journal
        .lines()
        .map(|l| serde_json::from_str::<Value>(l).unwrap())
        .collect::<Vec<_>>()[..]
    else {
        panic!("one journal line: {journal}");
    };
    assert_eq!(entry["mode"], "reactive");
    assert_eq!(entry["sourceSessionId"], CODEX_SESSION_ID);
    let original = PathBuf::from(entry["sourcePath"].as_str().unwrap());
    assert_eq!(
        std::fs::read(&original).unwrap(),
        std::fs::read(fixture(CODEX_FIXTURE)).unwrap()
    );
    assert_eq!(
        entry["path"].as_str().unwrap(),
        sandbox
            .claude_project()
            .join(format!("{session_id}.jsonl"))
            .to_str()
            .unwrap()
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

/// What the stub model endpoint answers to request number `n` (from 0).
enum Reply {
    Answer(&'static str),
    PoolExhausted,
}

/// A Responses API endpoint for the real Codex CLI. It keeps the request
/// bodies.
fn stub_responses_server(reply: fn(usize) -> Reply) -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}/v1", listener.local_addr().unwrap());
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&bodies);
    let count = AtomicUsize::new(0);
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut request_line = String::new();
            if reader.read_line(&mut request_line).is_err() {
                continue;
            }
            let mut length = 0;
            loop {
                let mut header = String::new();
                if reader.read_line(&mut header).is_err() || header == "\r\n" || header.is_empty() {
                    break;
                }
                if let Some(value) = header.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = value.trim().parse().unwrap_or(0);
                }
            }
            let mut body = vec![0; length];
            let _ = reader.read_exact(&mut body);
            if !request_line.starts_with("POST") || !request_line.contains("/responses") {
                let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\n\r\n");
                continue;
            }
            seen.lock()
                .unwrap()
                .push(String::from_utf8_lossy(&body).into_owned());
            match reply(count.fetch_add(1, Ordering::SeqCst)) {
                Reply::Answer(text) => {
                    let events = [
                        json!({"type": "response.created", "response": {"id": "resp_1"}}),
                        json!({"type": "response.output_item.done", "item": {"type": "message", "role": "assistant", "id": "msg_1", "content": [{"type": "output_text", "text": text}]}}),
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
                Reply::PoolExhausted => {
                    let body = json!({"type": "error", "error": {"type": "overloaded_error", "code": "pool_exhausted",
                        "message": "pool_exhausted: all accounts are rate limited or unavailable (reset_at=1790220376)"}})
                    .to_string();
                    let _ = write!(
                        stream,
                        "HTTP/1.1 503 Service Unavailable\r\ncontent-type: application/json\r\nx-should-retry: false\r\ncontent-length: {}\r\n\r\n{body}",
                        body.len()
                    );
                }
            }
        }
    });
    (base, bodies)
}

/// Points the real Codex CLI at the stub endpoint.
fn use_real_codex(sandbox: &mut Sandbox, base_url: &str) {
    std::fs::write(
        sandbox.path("codex/config.toml"),
        format!(
            "model = \"gpt-5-codex\"\nmodel_provider = \"stub\"\n\n[model_providers.stub]\nname = \"stub\"\nbase_url = \"{base_url}\"\nwire_api = \"responses\"\nrequest_max_retries = 0\nstream_max_retries = 3\n"
        ),
    )
    .unwrap();
    let bin = std::env::var("CODEX_BIN").unwrap_or_else(|_| "codex".to_string());
    sandbox.set("CODEX_BIN", bin);
    sandbox.set("CODEX_MODEL_PROVIDER", "stub");
}

#[test]
#[ignore = "runs the real codex binary (CODEX_BIN or codex on PATH) against a local stub"]
fn real_codex_continues_a_session_that_claude_left() {
    let mut sandbox = Sandbox::new("real-claude-to-codex");
    sandbox.seed_claude_session();
    let lines = sandbox.lines_file("exhausted.jsonl", &claude_exhaustion(CLAUDE_SESSION_ID));
    sandbox.set("FAKE_CLAUDE_LINES", lines);
    let (base_url, bodies) = stub_responses_server(|_| Reply::Answer("RESUMED ON CODEX"));
    use_real_codex(&mut sandbox, &base_url);

    let mut server = sandbox.spawn("claude-code");
    server.send(user_line(PROMPT));
    let output = server.read_turn();
    server.finish();

    let [notice] = notices(&output)[..] else {
        panic!("one notice: {output:#?}");
    };
    assert_eq!(notice["to"], "codex");
    let completed = output.last().unwrap();
    assert_eq!(
        completed["params"]["turn"]["status"], "completed",
        "{output:#?}"
    );
    assert!(
        agent_texts(&output).contains(&"RESUMED ON CODEX"),
        "{output:#?}"
    );

    // The model request has the imported history, then the prompt once.
    let bodies = bodies.lock().unwrap();
    let body = bodies.last().expect("codex called the model");
    let history = body.find(FIRST_PROMPT).expect("imported history");
    let prompt = body.rfind(PROMPT).expect("the prompt");
    assert!(history < prompt);
    assert_eq!(body.matches(PROMPT).count(), 1, "{body}");
    assert!(!body.contains("API Error"));
}

#[test]
#[ignore = "runs the real codex binary (CODEX_BIN or codex on PATH) against a local stub"]
fn real_codex_pool_exhaustion_moves_the_turn_to_claude() {
    let mut sandbox = Sandbox::new("real-codex-to-claude");
    // The first turn works; then the account pool is empty.
    let (base_url, bodies) = stub_responses_server(|n| {
        if n == 0 {
            Reply::Answer("FIRST ANSWER FROM CODEX")
        } else {
            Reply::PoolExhausted
        }
    });
    use_real_codex(&mut sandbox, &base_url);

    let mut server = sandbox.spawn("codex");
    server.send(user_line("First request."));
    let first = server.read_turn();
    assert!(notices(&first).is_empty(), "{first:#?}");
    assert!(agent_texts(&first).contains(&"FIRST ANSWER FROM CODEX"));

    server.send(user_line(PROMPT));
    let second = server.read_turn();
    server.finish();

    let [notice] = notices(&second)[..] else {
        panic!("one notice: {second:#?}");
    };
    assert_eq!(notice["from"], "codex");
    assert_eq!(notice["to"], "claudecode");
    assert_eq!(notice["resume"], "replay");
    assert_eq!(notice["history"], "converted");
    assert_eq!(notice["providerExhausted"]["signal"], "poolMarker");
    assert!(second.iter().all(|l| l["method"] != "error"), "{second:#?}");
    assert_eq!(agent_texts(&second), ["claude answer"]);
    // Codex stopped after its first retry, not after all of them.
    assert!(bodies.lock().unwrap().len() < 5);

    let session_id = notice["sessionId"].as_str().unwrap();
    let converted =
        std::fs::read_to_string(sandbox.claude_project().join(format!("{session_id}.jsonl")))
            .unwrap();
    assert!(converted.contains("First request."));
    assert!(converted.contains("FIRST ANSWER FROM CODEX"));
    // The failed prompt is sent again, not kept in the history.
    assert!(!converted.contains(PROMPT));
    let claude = sandbox.log("claude.log");
    assert!(
        claude.contains(&format!("--resume {session_id}")),
        "{claude}"
    );
    assert!(claude.contains(PROMPT), "{claude}");
}

#[test]
#[ignore = "runs the real codex binary (CODEX_BIN or codex on PATH) against a local stub"]
fn real_codex_reports_a_converted_thread_that_it_resumes_without_history() {
    let mut sandbox = Sandbox::new("real-empty-resume");
    let (base_url, bodies) = stub_responses_server(|_| Reply::Answer("NO HISTORY"));
    use_real_codex(&mut sandbox, &base_url);
    // A valid session_meta, then lines that Codex cannot parse.
    let meta = std::fs::read_to_string(fixture(CODEX_FIXTURE))
        .unwrap()
        .lines()
        .next()
        .unwrap()
        .to_owned();
    let dir = sandbox.path("codex/sessions/2026/09/24");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join(format!(
            "rollout-2026-09-24T10-00-00-{CODEX_SESSION_ID}.jsonl"
        )),
        format!("{meta}\n{{\"broken\":true}}\n{{\"broken\":true}}\n"),
    )
    .unwrap();
    std::fs::write(
        sandbox.path("codex/centaur-thread-id"),
        format!("{CODEX_SESSION_ID}\n"),
    )
    .unwrap();
    // The Codex child as a switch starts it.
    sandbox.set("CENTAUR_HARNESS_SWITCHING", "0");
    sandbox.set("CENTAUR_CODEX_THREAD_PERSIST", "1");
    sandbox.set("CENTAUR_SESSION_RESUME_REQUIRED", "1");

    let mut server = sandbox.spawn("codex");
    server.send(user_line(PROMPT));
    let output = server.read_until_end();
    server.finish();

    let last = output.last().unwrap();
    assert_eq!(last["method"], "error", "{output:#?}");
    let message = last["params"]["error"]["message"].as_str().unwrap();
    assert!(message.contains("without its history"), "{message}");
    // No new thread, and no model request without the history.
    assert!(bodies.lock().unwrap().is_empty());
    assert_eq!(
        sandbox.read("codex/centaur-thread-id"),
        format!("{CODEX_SESSION_ID}\n")
    );
}

/// The rollouts under the Codex home.
fn rollouts(sandbox: &Sandbox) -> Vec<PathBuf> {
    walk(&sandbox.path("codex/sessions"))
}

fn journal_modes(sandbox: &Sandbox) -> Vec<Value> {
    sandbox
        .read("state/centaur-failover-journal.jsonl")
        .lines()
        .map(|l| serde_json::from_str::<Value>(l).unwrap()["mode"].clone())
        .collect()
}

/// Appends a turn to a Claude transcript, as Claude Code does after a turn.
fn append_claude_turn(transcript: &Path, prompt: &str, answer: &str) {
    let text = std::fs::read_to_string(transcript).unwrap();
    let last: Value = serde_json::from_str(text.lines().last().unwrap()).unwrap();
    let session = last["sessionId"].clone();
    let user = Uuid::new_v4().to_string();
    let lines = [
        json!({"type": "user", "uuid": user, "parentUuid": last["uuid"], "sessionId": session,
            "cwd": last["cwd"], "message": {"role": "user", "content": prompt}}),
        json!({"type": "assistant", "uuid": Uuid::new_v4().to_string(), "parentUuid": user,
            "sessionId": session, "cwd": last["cwd"],
            "message": {"id": "msg-appended", "role": "assistant", "model": "claude-opus-5-5",
                        "content": [{"type": "text", "text": answer}]}}),
    ];
    let appended: String = lines.iter().map(|l| format!("{l}\n")).collect();
    std::fs::write(transcript, format!("{text}{appended}")).unwrap();
}

#[test]
fn a_requested_switch_back_converts_the_current_session() {
    let mut sandbox = Sandbox::new("requested");
    sandbox.seed_codex_session();
    let exhausted = sandbox.lines_file("exhausted.jsonl", &codex_exhaustion(&[RETRY_NOTICE]));
    sandbox.set("FAKE_CODEX_TURN_LINES", exhausted.clone());

    let mut server = sandbox.spawn("codex");
    server.send(user_line(PROMPT));
    let first = server.read_turn();
    let [notice] = notices(&first)[..] else {
        panic!("one notice: {first:#?}");
    };
    let claude_session = notice["sessionId"].as_str().unwrap().to_owned();
    // The turn on Claude Code is in its transcript, and nowhere else.
    append_claude_turn(
        &sandbox
            .claude_project()
            .join(format!("{claude_session}.jsonl")),
        "What did the test print? MARKER-ON-CLAUDE",
        "It printed 3 passed. ANSWER-ON-CLAUDE",
    );

    // The Codex pool recovers, and the user asks for Codex again.
    std::fs::remove_file(&exhausted).unwrap();
    let mut line = user_line("Back to Codex.");
    line["centaur"] = json!({"harness": "codex", "mode": "requested"});
    server.send(line);
    let second = server.read_turn();
    server.finish();

    let [notice] = notices(&second)[..] else {
        panic!("one notice: {second:#?}");
    };
    assert_eq!(notice["mode"], "requested");
    assert_eq!(notice["history"], "converted");
    assert_eq!(
        (notice["from"].as_str(), notice["to"].as_str()),
        (Some("claudecode"), Some("codex"))
    );
    assert_eq!(notice["sourceSessionId"], claude_session.as_str());
    // A new Codex thread with the whole history: the turns from before the
    // failover, and the turn on Claude Code.
    let thread_id = notice["sessionId"].as_str().unwrap();
    assert_ne!(thread_id, CODEX_SESSION_ID);
    assert_eq!(agent_texts(&second), ["codex answer"]);
    let converted = rollouts(&sandbox)
        .into_iter()
        .find(|p| p.to_string_lossy().ends_with(&format!("{thread_id}.jsonl")))
        .expect("the converted rollout");
    let converted = std::fs::read_to_string(converted).unwrap();
    for text in [FIRST_PROMPT, "MARKER-ON-CLAUDE", "ANSWER-ON-CLAUDE"] {
        assert!(converted.contains(text), "{text}");
    }
    let resume = sandbox
        .log("codex.log")
        .lines()
        .rfind(|l| l.contains(r#""method":"thread/resume""#))
        .unwrap()
        .to_owned();
    assert!(resume.contains(thread_id), "{resume}");
    assert_eq!(
        sandbox.read("codex/centaur-thread-id"),
        format!("{thread_id}\n")
    );
    // The original Codex session is still there, unchanged.
    let original = rollouts(&sandbox)
        .into_iter()
        .find(|p| {
            p.to_string_lossy()
                .ends_with(&format!("{CODEX_SESSION_ID}.jsonl"))
        })
        .unwrap();
    assert_eq!(
        std::fs::read(original).unwrap(),
        std::fs::read(fixture(CODEX_FIXTURE)).unwrap()
    );
    assert_eq!(journal_modes(&sandbox), ["reactive", "requested"]);
}

#[test]
fn claude_that_cannot_read_the_conversion_sends_the_session_back_to_codex() {
    let mut sandbox = Sandbox::new("revert-to-codex");
    sandbox.seed_codex_session();
    let exhausted = sandbox.lines_file("exhausted.jsonl", &codex_exhaustion(&[RETRY_NOTICE]));
    sandbox.set("FAKE_CODEX_TURN_LINES", exhausted.clone());
    sandbox.set("FAKE_CLAUDE_RESUME_FAILS", "1");

    let mut server = sandbox.spawn("codex");
    server.send(user_line(PROMPT));
    let first = server.read_until_end();

    let [switch, revert] = notices(&first)[..] else {
        panic!("two notices: {first:#?}");
    };
    assert_eq!(switch["mode"], "reactive");
    let claude_session = switch["sessionId"].as_str().unwrap().to_owned();
    assert_eq!(revert["mode"], "revert");
    assert_eq!(revert["stage"], "resume");
    assert_eq!(revert["history"], "original");
    assert_eq!(
        (revert["from"].as_str(), revert["to"].as_str()),
        (Some("claudecode"), Some("codex"))
    );
    assert_eq!(revert["sessionId"], CODEX_SESSION_ID);
    assert_eq!(revert["sourceSessionId"], claude_session.as_str());
    let reason = revert["reason"].as_str().unwrap();
    assert!(reason.contains("No conversation found"), "{reason}");

    // The turn fails as it would without the switch: the Codex pool is
    // exhausted. The client sees why the switch did not help.
    let last = first.last().unwrap();
    assert_eq!(last["params"]["centaur"]["failover"]["status"], "reverted");
    assert_eq!(
        last["params"]["centaur"]["providerExhausted"]["signal"],
        "poolMarker"
    );
    assert_eq!(sandbox.read("state/centaur-active-harness"), "codex\n");
    assert_eq!(
        sandbox.read("codex/centaur-thread-id"),
        format!("{CODEX_SESSION_ID}\n")
    );
    assert!(!sandbox.path("claude/centaur-session-id").exists());
    // The converted session stays on disk, for a look at what went wrong.
    assert!(
        sandbox
            .claude_project()
            .join(format!("{claude_session}.jsonl"))
            .is_file()
    );

    // The pool recovers: the next turn runs on the original Codex thread.
    std::fs::remove_file(&exhausted).unwrap();
    server.send(user_line("Try again."));
    let second = server.read_turn();
    server.finish();
    assert!(notices(&second).is_empty(), "{second:#?}");
    assert_eq!(agent_texts(&second), ["codex answer"]);
    let resume = sandbox
        .log("codex.log")
        .lines()
        .rfind(|l| l.contains(r#""method":"thread/resume""#))
        .unwrap()
        .to_owned();
    assert!(resume.contains(CODEX_SESSION_ID), "{resume}");
    assert_eq!(rollouts(&sandbox).len(), 1);
    assert_eq!(journal_modes(&sandbox), ["reactive", "revert"]);
}

#[test]
fn codex_that_resumes_without_the_history_sends_the_session_back_to_claude() {
    let mut sandbox = Sandbox::new("revert-to-claude");
    sandbox.seed_claude_session();
    let exhausted = sandbox.lines_file("exhausted.jsonl", &claude_exhaustion(CLAUDE_SESSION_ID));
    sandbox.set("FAKE_CLAUDE_LINES", exhausted.clone());
    sandbox.set("FAKE_CODEX_RESUME_EMPTY", "1");

    let mut server = sandbox.spawn("claude-code");
    server.send(user_line(PROMPT));
    let first = server.read_until_end();

    let [switch, revert] = notices(&first)[..] else {
        panic!("two notices: {first:#?}");
    };
    let thread_id = switch["sessionId"].as_str().unwrap().to_owned();
    assert_eq!(revert["mode"], "revert");
    assert_eq!(
        (revert["from"].as_str(), revert["to"].as_str()),
        (Some("codex"), Some("claudecode"))
    );
    assert_eq!(revert["sessionId"], CLAUDE_SESSION_ID);
    assert!(
        revert["reason"]
            .as_str()
            .unwrap()
            .contains(&format!("resumed session {thread_id} without its history"))
    );
    // Codex did not start a new thread without the history.
    assert!(
        !sandbox
            .log("codex.log")
            .contains(r#""method":"thread/start""#)
    );
    let last = first.last().unwrap();
    assert_eq!(last["method"], "error");
    assert_eq!(last["params"]["centaur"]["failover"]["status"], "reverted");
    assert_eq!(sandbox.read("state/centaur-active-harness"), "claudecode\n");
    assert_eq!(
        sandbox.read("claude/centaur-session-id"),
        format!("{CLAUDE_SESSION_ID}\n")
    );
    assert!(!sandbox.path("codex/centaur-thread-id").exists());

    // The pool recovers: Claude Code resumes its own session.
    std::fs::remove_file(&exhausted).unwrap();
    server.send(user_line("Try again."));
    let second = server.read_turn();
    server.finish();
    assert!(notices(&second).is_empty(), "{second:#?}");
    assert_eq!(agent_texts(&second), ["claude answer"]);
    let claude = sandbox.log("claude.log");
    let last_args = claude.lines().rfind(|l| l.starts_with("ARGS")).unwrap();
    assert!(
        last_args.contains(&format!("--resume {CLAUDE_SESSION_ID}")),
        "{last_args}"
    );
}

#[test]
fn a_session_that_cannot_move_stays_and_says_so() {
    let mut sandbox = Sandbox::new("stay");
    sandbox.seed_codex_session();
    // The Claude project directory cannot be created.
    std::fs::create_dir_all(sandbox.path("claude/projects")).unwrap();
    std::fs::write(sandbox.claude_project(), "not a directory").unwrap();

    let mut server = sandbox.spawn("codex");
    let mut line = user_line("Switch to Claude Code.");
    line["centaur"] = json!({"harness": "claudecode", "mode": "requested"});
    server.send(line);
    let output = server.read_turn();
    server.finish();

    let [notice] = notices(&output)[..] else {
        panic!("one notice: {output:#?}");
    };
    assert_eq!(notice["mode"], "revert");
    assert_eq!(notice["stage"], "convert");
    assert_eq!(
        (notice["from"].as_str(), notice["to"].as_str()),
        (Some("claudecode"), Some("codex"))
    );
    assert_eq!(notice["sessionId"], CODEX_SESSION_ID);
    assert!(notice["reason"].as_str().unwrap().contains("cannot write"));
    // The turn runs on Codex, with its own session.
    assert_eq!(agent_texts(&output), ["codex answer"]);
    assert!(sandbox.log("claude.log").is_empty());
    assert_eq!(sandbox.read("state/centaur-active-harness"), "codex\n");
    assert_eq!(journal_modes(&sandbox), ["revert"]);
}
