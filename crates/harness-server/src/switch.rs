//! Switchable blocks mode: moves a session between Codex and Claude Code when
//! the model provider of the active harness has no capacity left.
//!
//! With `CENTAUR_HARNESS_SWITCHING=1`, `harness-server codex` and
//! `harness-server claude-code` run the blocks server of the harness as a
//! child process and relay its stdin and stdout lines. The child keeps its
//! session id on the state volume (`centaur-thread-id`, `centaur-session-id`).
//!
//! When a turn fails because the provider is exhausted (the child marks the
//! line with `params.centaur.providerExhausted`), this process:
//!
//! 1. holds back the failure, so the client does not see the turn end;
//! 2. converts the native session into the format of the other harness;
//! 3. starts the other child, which resumes the converted session;
//! 4. prints a `centaur/providerFailover` notification;
//! 5. sends the turn again. If the turn already did work, it sends a request
//!    to continue instead, because the work is in the history.
//!
//! A turn fails over at most once. The control plane can add a `centaur`
//! directive to a user line:
//!
//! ```json
//! {"type": "user", "centaur": {"harness": "claudecode", "failover": {"enabled": true, "model": "..."}}}
//! ```
//!
//! If `harness` is not the active harness, the session moves before the turn
//! starts. `failover.enabled: false` turns off the switch for the turn, and
//! `failover.model` is the model for the turn after a switch.

use std::collections::VecDeque;
use std::env;
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};

use chrono::Local;
use serde::Deserialize;
use serde_json::{Value, json};
use session_transfer::convert::{ConvertOptions, TrailingPrompt, convert};
use session_transfer::discover::{Homes, SessionRef, resolve_session};
use session_transfer::prepare::PrepareOptions;
use session_transfer::{Converted, Target, Tool};
use uuid::Uuid;

use crate::claude::{CLAUDE_SESSION_PERSIST_ENV, PERSISTED_SESSION_FILE};
use crate::codex::{CODEX_THREAD_PERSIST_ENV, PERSISTED_THREAD_FILE};
use crate::server::{BlocksState, parse_blocks_line_with_state};
use crate::util::{env_flag_enabled, write_value};
use crate::{HarnessServerError, Result};

/// Sandbox env that turns on the switchable mode.
pub const SWITCHING_ENV: &str = "CENTAUR_HARNESS_SWITCHING";
/// The notification that a session moved to another harness.
pub const FAILOVER_METHOD: &str = "centaur/providerFailover";
/// Names the active harness, so that a new process continues on it.
const ACTIVE_HARNESS_FILE: &str = "centaur-active-harness";
const STATE_DIR_ENV: &str = "CENTAUR_STATE_DIR";
/// How long a turn that stops for a switch can take to end.
const TURN_STOP_GRACE: Duration = Duration::from_secs(20);
/// How long a child can take to exit after its stdin closes.
const EXIT_GRACE: Duration = Duration::from_secs(5);

pub fn switching_enabled() -> bool {
    env_flag_enabled(env::var(SWITCHING_ENV).ok().as_deref())
}

/// Runs the blocks server of `requested` (or of the harness that the session
/// moved to earlier) and switches harness when its provider is exhausted.
pub fn run_switchable_blocks_server(requested: Tool) -> Result<()> {
    Supervisor::start(requested)?.run()
}

/// The control plane's name of a harness.
fn harness_name(tool: Tool) -> &'static str {
    match tool {
        Tool::Codex => "codex",
        Tool::Claude => "claudecode",
    }
}

fn parse_harness(name: &str) -> Option<Tool> {
    match name.trim().to_ascii_lowercase().as_str() {
        "codex" => Some(Tool::Codex),
        "claudecode" | "claude-code" | "claude" => Some(Tool::Claude),
        _ => None,
    }
}

fn other(tool: Tool) -> Tool {
    match tool {
        Tool::Codex => Tool::Claude,
        Tool::Claude => Tool::Codex,
    }
}

fn subcommand(tool: Tool) -> &'static str {
    match tool {
        Tool::Codex => "codex",
        Tool::Claude => "claude-code",
    }
}

/// Where the child of `tool` keeps its session id.
fn persisted_id_file(tool: Tool, homes: &Homes) -> PathBuf {
    match tool {
        Tool::Codex => homes.codex.join(PERSISTED_THREAD_FILE),
        Tool::Claude => homes.claude.join(PERSISTED_SESSION_FILE),
    }
}

fn read_line_file(path: &Path) -> Option<String> {
    let contents = std::fs::read_to_string(path).ok()?;
    let line = contents.trim();
    (!line.is_empty() && !line.contains('\n')).then(|| line.to_owned())
}

fn write_line_file(path: &Path, line: &str) -> io::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(path, format!("{line}\n"))
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
struct FailoverDirective {
    enabled: bool,
    model: Option<String>,
}

impl Default for FailoverDirective {
    fn default() -> Self {
        Self {
            enabled: true,
            model: None,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct Directive {
    harness: Option<String>,
    failover: FailoverDirective,
}

/// Removes the `centaur` directive from a user line and returns it.
fn take_directive(line: &mut Value) -> Directive {
    let Some(raw) = line.as_object_mut().and_then(|l| l.remove("centaur")) else {
        return Directive::default();
    };
    serde_json::from_value(raw).unwrap_or_else(|error| {
        eprintln!("harness-server: ignoring an invalid centaur directive: {error}");
        Directive::default()
    })
}

fn is_steer(line: &Value) -> bool {
    line.pointer("/trace_metadata/action")
        .and_then(Value::as_str)
        == Some("steer_active_execution")
}

fn notification_method(value: &Value) -> Option<&str> {
    if value.get("id").is_some() {
        return None;
    }
    value.get("method").and_then(Value::as_str)
}

fn will_retry(value: &Value) -> bool {
    value.pointer("/params/willRetry").and_then(Value::as_bool) == Some(true)
}

/// The turn id of a `turn/*` notification.
fn turn_id_of(value: &Value) -> Option<&str> {
    value
        .pointer("/params/turn/id")
        .or_else(|| value.pointer("/params/turnId"))
        .and_then(Value::as_str)
}

/// True for the line that ends turn `turn_id`: its `turn/completed`, or an
/// `error` that is not retried. The child ends a turn on such an error, and
/// its own errors carry no real turn id.
fn ends_turn(value: &Value, turn_id: Option<&str>) -> bool {
    match notification_method(value) {
        Some("turn/completed" | "turn/failed") => {
            !matches!((turn_id_of(value), turn_id), (Some(a), Some(b)) if a != b)
        }
        Some("error") => !will_retry(value),
        _ => false,
    }
}

/// True for an `error` in the turn, or the line that ends it. A late
/// completion of an older turn is not about this turn.
fn reports_on_turn(value: &Value, turn_id: Option<&str>) -> bool {
    notification_method(value) == Some("error") || ends_turn(value, turn_id)
}

/// True for an item that shows work of the model: an answer, a tool call or
/// a file change. Such work is in the history, so the turn must not run again.
fn is_work_item(value: &Value) -> bool {
    matches!(
        notification_method(value),
        Some("item/started" | "item/completed")
    ) && !matches!(
        value.pointer("/params/item/type").and_then(Value::as_str),
        None | Some("userMessage" | "reasoning" | "contextCompaction")
    )
}

/// The user line for the other harness: the model, provider and reasoning
/// effort of the old harness do not apply to it.
fn for_other_harness(mut line: Value) -> Value {
    if let Some(line) = line.as_object_mut() {
        for key in ["model", "provider", "reasoning"] {
            line.remove(key);
        }
    }
    line
}

fn continue_text(from: Tool, to: Tool) -> String {
    format!(
        "[Centaur] The model provider of {from} had no capacity left, so this session moved \
         to {to} during the turn. The history above comes from {from}. Its tool calls already \
         ran, and their changes are in the workspace. Check the current state of the \
         workspace, for example with `git status`, then continue the task from where it stopped.",
        from = from.label(),
        to = to.label(),
    )
}

/// A user line that asks the new harness to continue the interrupted turn.
fn continue_line(original: Option<&Value>, from: Tool, to: Tool) -> Value {
    let mut line = json!({
        "type": "user",
        "message": {"content": continue_text(from, to)},
    });
    if let Some(original) = original {
        for key in ["thread_key", "traceparent", "trace_metadata"] {
            if let Some(value) = original.get(key) {
                line[key] = value.clone();
            }
        }
    }
    line
}

/// Adds `params.centaur.failover` to a line that the switch did not replace.
fn mark_failover_unavailable(line: &mut Value, error: &str) {
    let Some(params) = line.get_mut("params").and_then(Value::as_object_mut) else {
        return;
    };
    let centaur = params.entry("centaur").or_insert_with(|| json!({}));
    if let Some(centaur) = centaur.as_object_mut() {
        centaur.insert(
            "failover".to_string(),
            json!({"status": "unavailable", "error": error}),
        );
    }
}

enum Event {
    Stdin(String),
    StdinClosed,
    Child { generation: u64, line: String },
    ChildClosed { generation: u64 },
}

/// `harness-server <harness>` in blocks mode, with session persistence on.
struct HarnessChild {
    tool: Tool,
    generation: u64,
    process: Child,
    stdin: Option<ChildStdin>,
}

impl HarnessChild {
    /// `moved`: the session came from another harness, so an operator's
    /// thread to continue does not apply.
    fn spawn(tool: Tool, generation: u64, events: &Sender<Event>, moved: bool) -> Result<Self> {
        let mut command = Command::new(env::current_exe()?);
        command
            .arg(subcommand(tool))
            .env_remove(SWITCHING_ENV)
            .env(CODEX_THREAD_PERSIST_ENV, "1")
            .env(CLAUDE_SESSION_PERSIST_ENV, "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        if moved {
            command
                .env_remove("CODEX_CONTINUE_THREAD_ID")
                .env_remove("AMP_CONTINUE_THREAD_ID");
        }
        let mut process = command.spawn()?;
        let stdin = process.stdin.take();
        let stdout = process
            .stdout
            .take()
            .ok_or(HarnessServerError::HarnessStdoutUnavailable)?;
        let events = events.clone();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if events.send(Event::Child { generation, line }).is_err() {
                    return;
                }
            }
            let _ = events.send(Event::ChildClosed { generation });
        });
        Ok(Self {
            tool,
            generation,
            process,
            stdin,
        })
    }

    fn send_line(&mut self, line: &str) {
        let Some(stdin) = self.stdin.as_mut() else {
            return;
        };
        let result = stdin
            .write_all(line.as_bytes())
            .and_then(|()| stdin.write_all(b"\n"))
            .and_then(|()| stdin.flush());
        // A child that died reports it through its closed stdout.
        if let Err(error) = result {
            eprintln!(
                "harness-server: cannot write to the {} child: {error}",
                subcommand(self.tool)
            );
        }
    }

    fn send_value(&mut self, value: &Value) {
        self.send_line(&value.to_string());
    }

    fn close_stdin(&mut self) {
        self.stdin = None;
    }

    /// Closes stdin, and kills the child if it does not exit in time.
    fn stop(&mut self) {
        self.close_stdin();
        let deadline = Instant::now() + EXIT_GRACE;
        while Instant::now() < deadline {
            match self.process.try_wait() {
                Ok(Some(_)) | Err(_) => return,
                Ok(None) => thread::sleep(Duration::from_millis(20)),
            }
        }
        let _ = self.process.kill();
        let _ = self.process.wait();
    }

    fn wait(&mut self) -> Result<()> {
        let status = self.process.wait()?;
        if status.success() {
            Ok(())
        } else {
            Err(HarnessServerError::SwitchableChildExited {
                harness: subcommand(self.tool),
                status,
            })
        }
    }
}

impl Drop for HarnessChild {
    fn drop(&mut self) {
        if matches!(self.process.try_wait(), Ok(None)) {
            let _ = self.process.kill();
            let _ = self.process.wait();
        }
    }
}

/// A user line that the child has not started yet.
struct TurnInput {
    line: Value,
    failover: FailoverDirective,
    /// Sent again after a switch: the turn does not switch a second time.
    replayed: bool,
}

struct Turn {
    id: Option<String>,
    input: Option<TurnInput>,
    progressed: bool,
    may_fail_over: bool,
}

/// The converted history for the next harness.
struct History {
    source_id: Option<String>,
    /// `None` if the source session has nothing to carry over.
    converted: Option<Converted>,
}

struct Supervisor {
    events_tx: Sender<Event>,
    events: Receiver<Event>,
    /// Stdin events that arrived while a turn stopped for a switch.
    backlog: VecDeque<Event>,
    child: HarnessChild,
    stdout: io::Stdout,
    attachments: BlocksState,
    queued: VecDeque<TurnInput>,
    turn: Option<Turn>,
    stdin_closed: bool,
    cwd: PathBuf,
    homes: Homes,
    marker: PathBuf,
}

impl Supervisor {
    fn start(requested: Tool) -> Result<Self> {
        let homes = Homes::from_env();
        let marker = env::var_os(STATE_DIR_ENV)
            .filter(|dir| !dir.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| homes.codex.clone())
            .join(ACTIVE_HARNESS_FILE);
        // The session moved in an earlier process: continue where it is.
        let active = read_line_file(&marker)
            .as_deref()
            .and_then(parse_harness)
            .unwrap_or(requested);
        if active != requested {
            eprintln!(
                "harness-server: the session moved to {} earlier; starting it instead of {}",
                active.label(),
                requested.label()
            );
        }
        let (events_tx, events) = mpsc::channel();
        spawn_stdin_reader(events_tx.clone());
        let child = HarnessChild::spawn(active, 0, &events_tx, active != requested)?;
        let supervisor = Self {
            events_tx,
            events,
            backlog: VecDeque::new(),
            child,
            stdout: io::stdout(),
            attachments: BlocksState::default(),
            queued: VecDeque::new(),
            turn: None,
            stdin_closed: false,
            cwd: env::current_dir()?,
            homes,
            marker,
        };
        supervisor.write_marker();
        Ok(supervisor)
    }

    fn run(mut self) -> Result<()> {
        loop {
            let event = match self.backlog.pop_front() {
                Some(event) => event,
                None => self.events.recv().expect("the supervisor keeps a sender"),
            };
            match event {
                Event::Stdin(line) => self.on_stdin(&line)?,
                Event::StdinClosed => {
                    self.stdin_closed = true;
                    self.child.close_stdin();
                }
                Event::Child { generation, line } if generation == self.child.generation => {
                    self.on_child_line(&line)?;
                }
                Event::ChildClosed { generation } if generation == self.child.generation => {
                    return self.child.wait();
                }
                // Output of a child that was replaced.
                Event::Child { .. } | Event::ChildClosed { .. } => {}
            }
        }
    }

    fn turn_running(&self) -> bool {
        self.turn.is_some() || !self.queued.is_empty()
    }

    fn on_stdin(&mut self, line: &str) -> Result<()> {
        let Ok(mut value) = serde_json::from_str::<Value>(line.trim()) else {
            // The child reports the error.
            self.child.send_line(line);
            return Ok(());
        };
        match value.get("type").and_then(Value::as_str) {
            // Staged here, so that a turn sent again to another child still
            // finds its files.
            Some("attachment.chunk") => {
                if parse_blocks_line_with_state(line.trim(), &mut self.attachments).is_err() {
                    self.child.send_line(line);
                }
            }
            Some("user") => {
                let directive = take_directive(&mut value);
                self.attachments.inline_staged_attachments(&mut value);
                if !(is_steer(&value) && self.turn_running()) {
                    let requested = directive.harness.as_deref().and_then(|name| {
                        parse_harness(name).or_else(|| {
                            eprintln!("harness-server: cannot switch to harness `{name}`");
                            None
                        })
                    });
                    if let Some(to) = requested
                        && to != self.child.tool
                    {
                        if self.turn_running() {
                            eprintln!(
                                "harness-server: a turn runs; staying on {} for now",
                                self.child.tool.label()
                            );
                        } else {
                            self.switch_before_turn(to)?;
                        }
                    }
                    self.queued.push_back(TurnInput {
                        line: value.clone(),
                        failover: directive.failover,
                        replayed: false,
                    });
                }
                self.child.send_value(&value);
            }
            _ => self.child.send_line(line),
        }
        Ok(())
    }

    fn on_child_line(&mut self, line: &str) -> Result<()> {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            return self.emit_line(line);
        };
        if notification_method(&value) == Some("turn/started") {
            let id = turn_id_of(&value).map(str::to_owned);
            match &mut self.turn {
                // The child started the same turn again (a retried request).
                Some(turn) => turn.id = id,
                None => {
                    let input = self.queued.pop_front();
                    let may_fail_over = input
                        .as_ref()
                        .is_none_or(|input| input.failover.enabled && !input.replayed);
                    self.turn = Some(Turn {
                        id,
                        input,
                        progressed: false,
                        may_fail_over,
                    });
                }
            }
        } else if is_work_item(&value)
            && let Some(turn) = &mut self.turn
        {
            turn.progressed = true;
        }

        let exhausted = value.pointer("/params/centaur/providerExhausted");
        if let (Some(exhausted), Some(turn)) = (exhausted, &self.turn)
            && turn.may_fail_over
            && reports_on_turn(&value, turn.id.as_deref())
        {
            let exhausted = exhausted.clone();
            return self.fail_over(value, exhausted);
        }
        if self
            .turn
            .as_ref()
            .is_some_and(|turn| ends_turn(&value, turn.id.as_deref()))
        {
            self.turn = None;
        }
        self.emit_line(line)
    }

    /// Moves the running turn to the other harness. `trigger` is the line
    /// that reported the exhausted provider; it is not printed.
    fn fail_over(&mut self, mut trigger: Value, exhausted: Value) -> Result<()> {
        let mut turn = self.turn.take().expect("a running turn");
        let from = self.child.tool;
        let to = other(from);
        let ended = ends_turn(&trigger, turn.id.as_deref());
        let trailing = if turn.progressed {
            TrailingPrompt::Keep
        } else {
            TrailingPrompt::Drop
        };
        let history = match self.prepare_history(from, to, trailing) {
            Ok(history) => history,
            Err(error) => {
                eprintln!(
                    "harness-server: cannot move the session from {} to {}: {error}",
                    from.label(),
                    to.label()
                );
                mark_failover_unavailable(&mut trigger, &error.to_string());
                if !ended {
                    // The turn goes on, and fails as it would without a switch.
                    turn.may_fail_over = false;
                    self.turn = Some(turn);
                }
                return self.emit_value(&trigger);
            }
        };
        if !ended {
            self.stop_turn(turn.id.as_deref());
        }
        let resume = if turn.progressed || turn.input.is_none() {
            "continue"
        } else {
            "replay"
        };
        self.switch_child(
            to,
            &history,
            json!({
                "mode": "reactive",
                "resume": resume,
                "turnId": turn.id,
                "providerExhausted": exhausted,
            }),
        )?;

        let failover = turn
            .input
            .as_ref()
            .map(|input| input.failover.clone())
            .unwrap_or_default();
        let mut line = match turn.input {
            Some(input) if !turn.progressed => for_other_harness(input.line),
            input => continue_line(input.as_ref().map(|input| &input.line), from, to),
        };
        if let Some(model) = &failover.model {
            line["model"] = json!(model);
        }
        // Turns that the old child queued go to the new child after this one.
        let waiting: Vec<TurnInput> = self.queued.drain(..).collect();
        self.child.send_value(&line);
        self.queued.push_back(TurnInput {
            line,
            failover,
            replayed: true,
        });
        for input in waiting {
            let line = for_other_harness(input.line);
            self.child.send_value(&line);
            self.queued.push_back(TurnInput { line, ..input });
        }
        if self.stdin_closed {
            self.child.close_stdin();
        }
        Ok(())
    }

    /// The control plane asked for another harness: move before the turn.
    fn switch_before_turn(&mut self, to: Tool) -> Result<()> {
        let from = self.child.tool;
        let history = match self.prepare_history(from, to, TrailingPrompt::Keep) {
            Ok(history) => history,
            // No session yet: the new harness starts one.
            Err(session_transfer::Error::NotFound { .. }) => History {
                source_id: None,
                converted: None,
            },
            Err(error) => {
                eprintln!(
                    "harness-server: cannot move the session from {} to {}; staying on {}: {error}",
                    from.label(),
                    to.label(),
                    from.label()
                );
                return Ok(());
            }
        };
        self.switch_child(to, &history, json!({"mode": "proactive"}))
    }

    /// Converts the active session and writes it for `to`. Nothing changes
    /// for the running child.
    fn prepare_history(
        &self,
        from: Tool,
        to: Tool,
        trailing_prompt: TrailingPrompt,
    ) -> std::result::Result<History, session_transfer::Error> {
        let id_file = persisted_id_file(from, &self.homes);
        let source_id =
            read_line_file(&id_file).ok_or_else(|| session_transfer::Error::NotFound {
                tool: from,
                reference: "the active session".to_string(),
                dir: id_file.clone(),
            })?;
        let session = resolve_session(
            from,
            &SessionRef::Query(source_id.clone()),
            self.homes.get(from),
        )?;
        let target = Target {
            home: self.homes.get(to).to_path_buf(),
            cwd: self.cwd.to_string_lossy().into_owned(),
            id: Uuid::new_v4(),
            now: Local::now().fixed_offset(),
        };
        let options = ConvertOptions {
            prepare: PrepareOptions {
                brand: crate::transcript::BRAND,
                ..PrepareOptions::default()
            },
            trailing_prompt,
            codex_model_provider: crate::codex::default_model_provider(),
        };
        let target_id_file = persisted_id_file(to, &self.homes);
        let write_error = |source| session_transfer::Error::Write {
            path: target_id_file.clone(),
            source,
        };
        let converted = match convert(&session, to, &target, &options) {
            Ok(converted) => {
                converted.write()?;
                write_line_file(&target_id_file, &converted.id.to_string()).map_err(write_error)?;
                Some(converted)
            }
            // Only a prompt that failed: the new harness starts a new session.
            Err(session_transfer::Error::EmptySession { .. }) => {
                match std::fs::remove_file(&target_id_file) {
                    Err(error) if error.kind() != io::ErrorKind::NotFound => {
                        return Err(write_error(error));
                    }
                    _ => None,
                }
            }
            Err(error) => return Err(error),
        };
        Ok(History {
            source_id: Some(source_id),
            converted,
        })
    }

    /// Interrupts the running turn and waits for its end. Stdin that arrives
    /// meanwhile waits for the new child.
    fn stop_turn(&mut self, turn_id: Option<&str>) {
        self.child.send_line(r#"{"type":"interrupt"}"#);
        let deadline = Instant::now() + TURN_STOP_GRACE;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match self.events.recv_timeout(remaining) {
                Ok(Event::Child { generation, line }) if generation == self.child.generation => {
                    if serde_json::from_str::<Value>(&line).is_ok_and(|v| ends_turn(&v, turn_id)) {
                        return;
                    }
                }
                Ok(Event::ChildClosed { generation }) if generation == self.child.generation => {
                    return;
                }
                Ok(event @ (Event::Stdin(_) | Event::StdinClosed)) => self.backlog.push_back(event),
                Ok(_) => {}
                Err(_) => {
                    eprintln!("harness-server: the turn did not stop in time; stopping the child");
                    return;
                }
            }
        }
    }

    /// Replaces the child with one for `to`, which resumes the converted
    /// session, and prints the notification.
    fn switch_child(&mut self, to: Tool, history: &History, details: Value) -> Result<()> {
        let from = self.child.tool;
        self.child.stop();
        self.child = HarnessChild::spawn(to, self.child.generation + 1, &self.events_tx, true)?;
        self.write_marker();
        let mut params = json!({
            "from": harness_name(from),
            "to": harness_name(to),
            "history": if history.converted.is_some() { "converted" } else { "empty" },
            "sessionId": history.converted.as_ref().map(|c| c.id.to_string()),
            "sourceSessionId": history.source_id,
        });
        if let (Some(params), Some(details)) = (params.as_object_mut(), details.as_object()) {
            params.extend(details.clone());
        }
        eprintln!(
            "harness-server: the session moved from {} to {}",
            from.label(),
            to.label()
        );
        self.emit_value(&json!({"method": FAILOVER_METHOD, "params": params}))
    }

    fn write_marker(&self) {
        if let Err(error) = write_line_file(&self.marker, harness_name(self.child.tool)) {
            eprintln!(
                "harness-server: cannot write {}: {error}",
                self.marker.display()
            );
        }
    }

    fn emit_line(&mut self, line: &str) -> Result<()> {
        let mut stdout = self.stdout.lock();
        stdout.write_all(line.as_bytes())?;
        stdout.write_all(b"\n")?;
        stdout.flush()?;
        Ok(())
    }

    fn emit_value(&mut self, value: &Value) -> Result<()> {
        write_value(&mut self.stdout.lock(), value)
    }
}

fn spawn_stdin_reader(events: Sender<Event>) {
    thread::spawn(move || {
        for line in io::stdin().lock().lines() {
            let Ok(line) = line else { break };
            if line.trim().is_empty() {
                continue;
            }
            if events.send(Event::Stdin(line)).is_err() {
                return;
            }
        }
        let _ = events.send(Event::StdinClosed);
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn harness_names_match_the_control_plane() {
        for tool in [Tool::Codex, Tool::Claude] {
            assert_eq!(parse_harness(harness_name(tool)), Some(tool));
            assert_eq!(other(other(tool)), tool);
        }
        assert_eq!(parse_harness("claude-code"), Some(Tool::Claude));
        assert_eq!(parse_harness("amp"), None);
    }

    #[test]
    fn the_directive_is_taken_out_of_the_line() {
        let mut line = json!({"type": "user", "text": "hi", "centaur": {"harness": "claudecode", "failover": {"enabled": false, "model": "m"}}});
        let directive = take_directive(&mut line);
        assert_eq!(line, json!({"type": "user", "text": "hi"}));
        assert_eq!(directive.harness.as_deref(), Some("claudecode"));
        assert!(!directive.failover.enabled);
        assert_eq!(directive.failover.model.as_deref(), Some("m"));

        // No directive, or a broken one: failover stays on.
        let mut line = json!({"type": "user", "centaur": {"failover": "yes"}});
        assert!(take_directive(&mut line).failover.enabled);
        assert!(
            take_directive(&mut json!({"type": "user"}))
                .failover
                .enabled
        );
    }

    #[test]
    fn a_turn_ends_on_its_completion_or_a_final_error() {
        let completed = json!({"method": "turn/completed", "params": {"turn": {"id": "t1", "status": "failed"}}});
        assert!(ends_turn(&completed, Some("t1")));
        assert!(ends_turn(&completed, None));
        // A late completion of an older turn.
        assert!(!ends_turn(&completed, Some("t2")));

        let retry = json!({"method": "error", "params": {"willRetry": true, "turnId": "t1"}});
        assert!(!ends_turn(&retry, Some("t1")));
        // The child's own errors carry no real turn id.
        let error = json!({"method": "error", "params": {"willRetry": false, "turnId": "turn"}});
        assert!(ends_turn(&error, Some("t1")));
        assert!(!ends_turn(
            &json!({"id": 1, "method": "error", "params": {}}),
            None
        ));
    }

    #[test]
    fn only_model_work_counts_as_progress() {
        let item =
            |kind: &str| json!({"method": "item/completed", "params": {"item": {"type": kind}}});
        assert!(!is_work_item(&item("userMessage")));
        assert!(!is_work_item(&item("reasoning")));
        assert!(is_work_item(&item("agentMessage")));
        assert!(is_work_item(&item("commandExecution")));
        assert!(!is_work_item(
            &json!({"method": "turn/started", "params": {}})
        ));
    }

    #[test]
    fn a_line_for_the_other_harness_has_no_model_settings() {
        let line = json!({"type": "user", "text": "hi", "model": "gpt-x", "provider": "p", "reasoning": "high", "thread_key": "k"});
        assert_eq!(
            for_other_harness(line),
            json!({"type": "user", "text": "hi", "thread_key": "k"})
        );
    }

    #[test]
    fn the_continue_line_keeps_the_trace_fields() {
        let original = json!({"type": "user", "text": "hi", "model": "gpt-x", "thread_key": "k", "traceparent": "00-a-b-01", "trace_metadata": {"x": 1}});
        let line = continue_line(Some(&original), Tool::Codex, Tool::Claude);
        assert_eq!(line["thread_key"], "k");
        assert_eq!(line["traceparent"], "00-a-b-01");
        assert_eq!(line["trace_metadata"], json!({"x": 1}));
        assert!(line.get("model").is_none());
        let text = line["message"]["content"].as_str().unwrap();
        assert!(text.starts_with("[Centaur]"), "{text}");
        assert!(text.contains("from Codex CLI") && text.contains("to Claude Code"));
    }

    #[test]
    fn an_unused_failure_line_says_why() {
        let mut line = json!({"method": "turn/completed", "params": {"turn": {}, "centaur": {"providerExhausted": {"signal": "poolMarker"}}}});
        mark_failover_unavailable(&mut line, "no session");
        assert_eq!(
            line["params"]["centaur"],
            json!({"providerExhausted": {"signal": "poolMarker"}, "failover": {"status": "unavailable", "error": "no session"}})
        );
    }
}
