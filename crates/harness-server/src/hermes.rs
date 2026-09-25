//! Hermes Agent harness — drives `hermes-agent`'s `tui_gateway` JSON-RPC
//! stdio wire as a Centaur harness.
//!
//! Unlike claude/amp (spawn-per-turn stream-json CLIs), Hermes ships a
//! long-lived JSON-RPC gateway (`python -m tui_gateway.entry`) that owns the
//! agent loop, durable session store, skills, persistent memory, background
//! self-improvement reviews, and the cron scheduler. This runtime therefore
//! mirrors the codex runtime shape — one persistent child per sandbox, a
//! handshake (`gateway.ready` → `session.create`), then one `prompt.submit`
//! per turn with events pumped into the shared `CodexTurnNormalizer`:
//!
//! - `message.delta`                       → AgentTextDelta
//! - `reasoning.delta` / `thinking.delta`  → ReasoningTextDelta
//! - `tool.start`                          → AssistantMessage(ToolUse)
//! - `tool.complete`                       → ToolResults
//! - `turn.usage`                          → TokenUsage
//! - `message.complete`                    → AssistantMessage(final) + Result
//!
//! Tools that wait for a person (`clarify`, `sudo`, `secret` and the desktop
//! read tools) get an answer at once: nobody can answer them in a Centaur
//! session, and each would hold the turn until its own timeout (an hour for
//! `clarify`). See [`blocking_request_reply`].
//!
//! Because the gateway (not this process) owns the agent loop, Hermes's
//! session history, prompt cache, learning loop, and cron jobs all survive
//! across turns. `HERMES_CONTINUE_SESSION_ID` resumes the durable session
//! after a sandbox restart (the hermes counterpart of
//! `CODEX_CONTINUE_THREAD_ID`), and Centaur's `interrupt` maps to Hermes's
//! `session.interrupt` — the turn ends Interrupted while the session lives on.

use std::collections::VecDeque;
use std::env;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command as ProcessCommand, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

use codex_app_server_protocol::UserInput;
use serde_json::{Value, json};

use crate::server::{BlocksCommand, BlocksState, parse_blocks_line_with_state, write_blocks_error};
use crate::traits::{
    NormalizedContent, NormalizedEvent, NormalizedTokenUsage, NormalizedToolResult,
};
use crate::turn::{BridgeConfig, CodexTurnNormalizer};
use crate::util::write_value;
use crate::wire::notification_to_wire_value;
use crate::{HarnessServerError, Result};

/// Gateway startup + RPC response budget. Generous because a cold Hermes
/// start imports its agent stack and may run MCP discovery first.
const RPC_TIMEOUT: Duration = Duration::from_secs(180);
/// How long an interrupted turn may take to deliver its terminal
/// `message.complete` before we stop draining and move on.
const INTERRUPT_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_CRON_TICK_SECONDS: u64 = 60;
/// The answer to Hermes's `clarify` tool. A Centaur session runs in a chat
/// thread, which cannot show an interactive question, so the model asks in
/// its reply instead.
const CLARIFY_ANSWER: &str = "Nobody can answer this prompt: this session runs in a chat thread, \
which cannot show interactive questions. Do not wait for an answer, and do not call clarify \
again in this turn. End the turn with the question and its choices in your reply. The user \
answers in the thread.";

/// Entry point for `harness-server hermes`.
pub fn run_hermes_blocks_server() -> Result<()> {
    let mut stdout = io::stdout().lock();
    let mut hermes: Option<HermesChild> = None;
    let (command_tx, command_rx) = mpsc::channel();
    let (interrupt_tx, interrupt_rx) = mpsc::channel();

    spawn_cron_ticker();

    thread::spawn(move || {
        let stdin = io::stdin();
        let mut blocks_state = BlocksState::default();
        for raw in stdin.lock().lines() {
            let Ok(line) = raw else { break };
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let sent = match parse_blocks_line_with_state(trimmed, &mut blocks_state) {
                Ok(BlocksCommand::Interrupt) => interrupt_tx.send(()).is_ok(),
                Ok(command) => command_tx.send(Ok(command)).is_ok(),
                Err(error) => command_tx.send(Err(error.to_string())).is_ok(),
            };
            if !sent {
                break;
            }
        }
    });

    let mut turn = 0u64;
    while let Ok(input) = command_rx.recv() {
        let thread_id = hermes
            .as_ref()
            .map_or("hermes", HermesChild::thread_id)
            .to_owned();
        match input {
            Ok(BlocksCommand::User {
                input,
                client_user_message_id,
                model,
                provider: _,
                reasoning,
                trace_context: _,
            }) => {
                turn += 1;
                let result = ensure_child(&mut hermes, model).and_then(|child| {
                    run_hermes_turn(
                        child,
                        &mut stdout,
                        input,
                        client_user_message_id,
                        reasoning,
                        turn,
                        &interrupt_rx,
                    )
                });
                if let Err(error) = result {
                    eprintln!("Hermes blocks turn failed: {error:#}");
                    write_blocks_error(&mut stdout, &thread_id, "turn", error.to_string())?;
                    // A dead gateway cannot serve the next turn; drop it so the
                    // next message restarts Hermes and resumes the durable
                    // session via HERMES_CONTINUE_SESSION_ID.
                    if hermes.as_mut().is_some_and(|child| !child.is_alive()) {
                        hermes = None;
                    }
                }
            }
            Ok(BlocksCommand::Interrupt) => {
                eprintln!("Hermes blocks interrupt ignored: no active turn runs");
            }
            Ok(BlocksCommand::AttachmentChunk) => {}
            Err(error) => {
                eprintln!("invalid Hermes blocks input: {error}");
                write_blocks_error(&mut stdout, &thread_id, "input", error)?;
            }
        }
        // Drain interrupts that arrived between turns so a stale one cannot
        // instantly cancel the next turn.
        while interrupt_rx.try_recv().is_ok() {}
    }
    Ok(())
}

fn ensure_child(
    hermes: &mut Option<HermesChild>,
    model: Option<String>,
) -> Result<&mut HermesChild> {
    if hermes.as_mut().is_some_and(|child| !child.is_alive()) {
        *hermes = None;
    }
    if hermes.is_none() {
        *hermes = Some(HermesChild::start(model)?);
    }
    Ok(hermes.as_mut().expect("hermes started"))
}

/// Tick `hermes cron tick` on an interval so cron jobs created inside the
/// conversation fire while the sandbox lives. Hermes serializes ticks
/// cross-process with a file lock, so this is safe alongside any other Hermes
/// process on the same HERMES_HOME. `HERMES_CRON_TICK_SECONDS=0` disables it.
fn spawn_cron_ticker() {
    let interval = env::var("HERMES_CRON_TICK_SECONDS")
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_CRON_TICK_SECONDS);
    if interval == 0 {
        return;
    }
    let bin = env::var("HERMES_BIN").unwrap_or_else(|_| "hermes".to_string());
    thread::spawn(move || {
        let mut warned = false;
        loop {
            thread::sleep(Duration::from_secs(interval));
            let status = ProcessCommand::new(&bin)
                .args(["cron", "tick"])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
            if let Err(error) = status
                && !warned
            {
                eprintln!("hermes cron ticker disabled: {bin} cron tick failed: {error}");
                warned = true;
            }
        }
    });
}

struct HermesChild {
    child: Child,
    stdin: ChildStdin,
    stdout: Receiver<io::Result<String>>,
    session_id: String,
    stored_session_id: String,
    session_started: bool,
    session_file: Option<PathBuf>,
    pending: VecDeque<Value>,
    next_rpc_id: i64,
}

impl Drop for HermesChild {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl HermesChild {
    fn start(model: Option<String>) -> Result<Self> {
        // The JSON-RPC gateway is a Python module, not a `hermes` subcommand;
        // HERMES_PYTHON points at the interpreter whose env has hermes-agent.
        let python = env::var("HERMES_PYTHON").unwrap_or_else(|_| "python3".to_string());
        let mut child = ProcessCommand::new(python)
            .args(["-m", "tui_gateway.entry"])
            .env("HERMES_QUIET", "1")
            // Centaur owns approval policy at the sandbox boundary (isolated
            // sandbox, iron-proxy egress); inside it Hermes runs unattended.
            .env(
                "HERMES_APPROVAL_MODE",
                env::var("HERMES_APPROVAL_MODE").unwrap_or_else(|_| "off".to_string()),
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|source| HarnessServerError::SpawnHarness {
                cwd: env::current_dir().unwrap_or_default(),
                source,
            })?;

        let stdin = child
            .stdin
            .take()
            .ok_or(HarnessServerError::HarnessStdinUnavailable)?;
        let stdout = child
            .stdout
            .take()
            .ok_or(HarnessServerError::HarnessStdoutUnavailable)?;
        let mut stderr = child
            .stderr
            .take()
            .ok_or(HarnessServerError::HarnessStderrUnavailable)?;
        thread::spawn(move || {
            // Unlocked handle on purpose: the child lives across turns, so
            // holding the StderrLock for the copy's lifetime would block
            // every eprintln! in the server until the child exits.
            let mut parent_stderr = io::stderr();
            let _ = io::copy(&mut stderr, &mut parent_stderr);
        });
        let (stdout_tx, stdout_rx) = mpsc::channel();
        thread::spawn(move || {
            let reader = io::BufReader::new(stdout);
            for raw in reader.lines() {
                let should_stop = raw.is_err();
                if stdout_tx.send(raw).is_err() || should_stop {
                    break;
                }
            }
        });

        let mut this = Self {
            child,
            stdin,
            stdout: stdout_rx,
            session_id: String::new(),
            stored_session_id: String::new(),
            session_started: false,
            session_file: env::var_os("CENTAUR_HERMES_SESSION_FILE").map(PathBuf::from),
            pending: VecDeque::new(),
            next_rpc_id: 0,
        };
        this.wait_for_gateway_ready()?;
        this.create_or_resume_session(model)?;
        Ok(this)
    }

    fn is_alive(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    fn thread_id(&self) -> &str {
        if self.stored_session_id.is_empty() {
            "hermes"
        } else {
            &self.stored_session_id
        }
    }

    fn wait_for_gateway_ready(&mut self) -> Result<()> {
        let deadline = Instant::now() + RPC_TIMEOUT;
        loop {
            if event_type(&self.read_frame_until(deadline)?) == Some("gateway.ready".to_string()) {
                return Ok(());
            }
        }
    }

    /// Create the thread's Hermes session, or resume the durable one after a
    /// sandbox restart.
    fn create_or_resume_session(&mut self, model: Option<String>) -> Result<()> {
        let resume = resume_session_id(
            env::var("HERMES_CONTINUE_SESSION_ID").ok().as_deref(),
            self.session_file.as_deref(),
        )?;
        if let Some(resume) = resume {
            // A failed resume must not silently replace an existing conversation.
            let result = self.rpc("session.resume", json!({"session_id": resume, "cols": 200}))?;
            return self.accept_session(&result, true);
        }

        let mut params = json!({
            "cols": 200,
            "cwd": env::current_dir().unwrap_or_default().to_string_lossy(),
            "title": "Centaur thread",
            "source": "centaur",
        });
        if let Some(model) = model.filter(|value| !value.trim().is_empty()) {
            params["model"] = Value::String(model);
        }
        let result = self.rpc("session.create", params)?;
        self.accept_session(&result, false)
    }

    fn accept_session(&mut self, result: &Value, resumed: bool) -> Result<()> {
        self.session_id = result
            .get("session_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                HarnessServerError::Protocol(
                    "Hermes session response missing live session_id".to_string(),
                )
            })?
            .to_string();
        // session_id is a short-lived gateway handle. Only stored_session_id
        // (create) / session_key (resume) identifies the durable conversation.
        self.stored_session_id = result
            .get("stored_session_id")
            .or_else(|| result.get("session_key"))
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                HarnessServerError::Protocol(
                    "Hermes session response missing durable session key".into(),
                )
            })?
            .to_owned();
        self.session_started = resumed;
        self.persist_session()
    }

    fn persist_session(&self) -> Result<()> {
        if self.session_started
            && let Some(path) = self.session_file.as_deref()
        {
            persist_session_id(path, &self.stored_session_id)?;
        }
        Ok(())
    }

    fn observe_session(&mut self, frame: &Value) -> Result<()> {
        if frame.pointer("/params/session_id").and_then(Value::as_str) == Some(&self.session_id)
            && event_type(frame).as_deref() == Some("session.info")
            && let Some(key) = frame
                .pointer("/params/payload/stored_session_id")
                .and_then(Value::as_str)
            && !key.is_empty()
            && key != self.stored_session_id
        {
            self.stored_session_id = key.to_owned();
            self.persist_session()?;
        }
        Ok(())
    }

    /// Send a request frame without waiting for the response (the caller
    /// pumps the stream itself, as `run_hermes_turn` does for prompt.submit).
    fn send_request(&mut self, method: &str, params: Value) -> Result<i64> {
        self.next_rpc_id += 1;
        self.write_frame(&json!({
            "jsonrpc": "2.0",
            "id": self.next_rpc_id,
            "method": method,
            "params": params,
        }))?;
        Ok(self.next_rpc_id)
    }

    /// Preserve interleaved events, especially completion before interrupt ACK.
    fn rpc(&mut self, method: &str, params: Value) -> Result<Value> {
        self.rpc_until(method, params, Instant::now() + RPC_TIMEOUT)
    }

    fn rpc_until(&mut self, method: &str, params: Value, deadline: Instant) -> Result<Value> {
        let id = self.send_request(method, params)?;
        loop {
            let frame = self.read_wire_frame_until(deadline)?;
            self.observe_session(&frame)?;
            if frame.get("id").and_then(Value::as_i64) != Some(id) {
                self.pending.push_back(frame);
                continue;
            }
            if let Some(error) = frame.get("error") {
                return Err(HarnessServerError::Protocol(format!(
                    "hermes {method} failed: {error}"
                )));
            }
            return Ok(frame.get("result").cloned().unwrap_or(Value::Null));
        }
    }

    fn write_frame(&mut self, value: &Value) -> Result<()> {
        serde_json::to_writer(&mut self.stdin, value)?;
        self.stdin.write_all(b"\n")?;
        self.stdin.flush()?;
        Ok(())
    }

    fn read_frame_until(&mut self, deadline: Instant) -> Result<Value> {
        if let Some(frame) = self.pending.pop_front() {
            return Ok(frame);
        }
        self.read_wire_frame_until(deadline)
    }

    fn read_wire_frame_until(&mut self, deadline: Instant) -> Result<Value> {
        loop {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or_else(gateway_timeout)?;
            match self.stdout.recv_timeout(remaining) {
                Ok(line) => {
                    // Hermes keeps stdout clean of non-JSON in quiet mode;
                    // tolerate stray lines anyway.
                    if let Ok(value) = serde_json::from_str::<Value>(line?.trim()) {
                        return Ok(value);
                    }
                }
                Err(RecvTimeoutError::Timeout) => return Err(gateway_timeout()),
                Err(RecvTimeoutError::Disconnected) => {
                    return Err(HarnessServerError::HermesExited {
                        status: self.child.wait()?,
                    });
                }
            }
        }
    }
}

fn resume_session_id(explicit: Option<&str>, path: Option<&Path>) -> Result<Option<String>> {
    if let Some(value) = explicit.map(str::trim).filter(|value| !value.is_empty()) {
        return Ok(Some(value.to_owned()));
    }
    let Some(path) = path else { return Ok(None) };
    let value = match std::fs::read_to_string(path) {
        Ok(value) => value,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let value = value.trim();
    if value.is_empty() || value.lines().count() != 1 {
        return Err(HarnessServerError::Protocol(
            "Invalid persisted Hermes session key".into(),
        ));
    }
    Ok(Some(value.to_owned()))
}

fn persist_session_id(path: &Path, id: &str) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
    std::fs::write(&temporary, format!("{id}\n"))?;
    std::fs::rename(&temporary, path)
}

fn image_requests(input: &[UserInput], session_id: &str) -> Result<Vec<(&'static str, Value)>> {
    let mut requests = Vec::new();
    for item in input {
        match item {
            UserInput::LocalImage { path, .. } => requests.push(("image.attach", json!({"session_id":session_id,"path":path}))),
            UserInput::Image { url, .. } if url.starts_with("data:image/") => requests.push(("image.attach_bytes", json!({"session_id":session_id,"content_base64":url}))),
            UserInput::Image { .. } => return Err(HarnessServerError::Protocol("Hermes requires image bytes or an uploaded local image; remote URL images must be staged first".into())),
            _ => {}
        }
    }
    Ok(requests)
}

fn gateway_timeout() -> HarnessServerError {
    HarnessServerError::Protocol("timed out waiting for hermes gateway".to_string())
}

/// The `params.type` of an event frame, or None for responses/other frames.
fn event_type(frame: &Value) -> Option<String> {
    (frame.get("method").and_then(Value::as_str) == Some("event"))
        .then(|| frame.pointer("/params/type").and_then(Value::as_str))
        .flatten()
        .map(str::to_string)
}

/// Translate one Hermes gateway frame into normalized events. Pure: the text
/// item id is deterministic per turn, and a failed turn's error rides the
/// terminal `Result` event (which `CodexTurnNormalizer` latches into
/// `last_error` for `finish_turn`). Terminal frames are recognized with the
/// standard `NormalizedEvent::is_terminal()`.
fn normalize_hermes_frame(turn: &str, frame: &Value) -> Vec<NormalizedEvent> {
    let Some(kind) = event_type(frame) else {
        return Vec::new();
    };
    let empty = json!({});
    let payload = frame.pointer("/params/payload").unwrap_or(&empty);
    let text_of = |key: &str| payload.get(key).and_then(Value::as_str).unwrap_or("");

    match kind.as_str() {
        "message.delta" if !text_of("text").is_empty() => vec![NormalizedEvent::AgentTextDelta {
            item_id: format!("hermes-msg-{turn}"),
            delta: text_of("text").to_string(),
        }],
        "reasoning.delta" | "thinking.delta" if !text_of("text").is_empty() => {
            vec![NormalizedEvent::ReasoningTextDelta {
                item_id: format!("hermes-reasoning-{turn}"),
                delta: text_of("text").to_string(),
            }]
        }
        "tool.start" => vec![NormalizedEvent::AssistantMessage {
            partial: false,
            stop_reason: None,
            content: vec![NormalizedContent::ToolUse {
                raw_id: nonempty_or(text_of("tool_id"), "tool"),
                tool: nonempty_or(text_of("name"), "tool"),
                arguments: payload.get("args").cloned().unwrap_or(json!({})),
            }],
        }],
        "tool.complete" => {
            let result = payload.get("result");
            let is_error = result.is_some_and(|result| {
                result.get("success").and_then(Value::as_bool) == Some(false)
                    || result.get("error").is_some_and(|error| !error.is_null())
            });
            vec![NormalizedEvent::ToolResults(vec![NormalizedToolResult {
                tool_use_id: nonempty_or(text_of("tool_id"), "tool"),
                content: tool_result_text(payload),
                is_error,
                exit_code: payload
                    .pointer("/result/exit_code")
                    .and_then(Value::as_i64)
                    .map(|code| code as i32),
            }])]
        }
        "turn.usage" | "session.usage" => {
            let count = |key: &str| payload.get(key).and_then(Value::as_i64);
            let usage = NormalizedTokenUsage {
                model: payload
                    .get("model")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                input_tokens: count("input_tokens"),
                output_tokens: count("output_tokens"),
                cache_creation_input_tokens: None,
                cache_read_input_tokens: count("cached_tokens"),
                reasoning_output_tokens: None,
                total_tokens: count("total_tokens"),
            };
            if usage.has_counts() {
                vec![NormalizedEvent::TokenUsage { usage }]
            } else {
                Vec::new()
            }
        }
        "message.complete" => {
            let text = text_of("text");
            if text_of("status") == "error" {
                let error = [text_of("error"), text, "hermes turn failed"]
                    .into_iter()
                    .find(|candidate| !candidate.is_empty())
                    .expect("last candidate is non-empty")
                    .to_string();
                return vec![NormalizedEvent::Result { error: Some(error) }];
            }
            let mut events = Vec::new();
            if !text.is_empty() {
                // The final text repeats the streamed deltas; the normalizer's
                // suffix-delta reconciliation prevents double emission.
                events.push(NormalizedEvent::AssistantMessage {
                    partial: false,
                    stop_reason: Some("end_turn".to_string()),
                    content: vec![NormalizedContent::AgentText {
                        item_id: format!("hermes-msg-{turn}"),
                        text: text.to_string(),
                    }],
                });
            }
            events.push(NormalizedEvent::Result { error: None });
            events
        }
        _ => Vec::new(),
    }
}

/// The reply to a Hermes tool that blocks until a person answers it:
/// `(method, params)` for the gateway, or `None` for other frames.
///
/// The gateway emits `<kind>.request` with a `request_id` and waits for
/// `<kind>.respond`. Nobody can answer in a Centaur session, so `clarify` gets
/// [`CLARIFY_ANSWER`], and the others get the empty answer that their own
/// timeout would give (`sudo` and `secret` fail, the read tools return
/// nothing). Approvals are off (`HERMES_APPROVAL_MODE`).
fn blocking_request_reply(frame: &Value) -> Option<(&'static str, Value)> {
    let kind = event_type(frame)?;
    let (method, key, answer) = match kind.as_str() {
        "clarify.request" => ("clarify.respond", "answer", CLARIFY_ANSWER),
        "sudo.request" => ("sudo.respond", "password", ""),
        "secret.request" => ("secret.respond", "value", ""),
        "terminal.read.request" => ("terminal.read.respond", "text", ""),
        "preview.read.request" => ("preview.read.respond", "text", ""),
        "window.read.request" => ("window.read.respond", "text", ""),
        _ => return None,
    };
    let request_id = frame.pointer("/params/payload/request_id")?.as_str()?;
    let mut params = serde_json::Map::new();
    params.insert("request_id".to_owned(), json!(request_id));
    if let Some(session_id) = frame.pointer("/params/session_id") {
        params.insert("session_id".to_owned(), session_id.clone());
    }
    params.insert(key.to_owned(), json!(answer));
    Some((method, Value::Object(params)))
}

fn nonempty_or(value: &str, fallback: &str) -> String {
    if value.is_empty() { fallback } else { value }.to_string()
}

fn tool_result_text(payload: &Value) -> String {
    if let Some(text) = payload.get("result_text").and_then(Value::as_str) {
        return text.to_string();
    }
    match payload.get("result") {
        Some(Value::String(text)) => text.clone(),
        Some(value) if !value.is_null() => serde_json::to_string(value).unwrap_or_default(),
        _ => payload
            .get("summary")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
    }
}

fn run_hermes_turn<W: Write>(
    child: &mut HermesChild,
    stdout: &mut W,
    input: Vec<UserInput>,
    client_user_message_id: Option<String>,
    reasoning: Option<String>,
    turn: u64,
    interrupt_rx: &Receiver<()>,
) -> Result<()> {
    let turn_id = format!("turn-{}", uuid::Uuid::new_v4().simple());
    let mut config = BridgeConfig::new(child.thread_id().to_string(), turn_id.clone());
    config.cli_version = "hermes".to_string();
    config.model_provider = "hermes".to_string();
    let mut normalizer = CodexTurnNormalizer::new(config);

    for notification in normalizer.start_notifications(turn == 1)? {
        write_value(stdout, &notification_to_wire_value(&notification)?)?;
    }
    for notification in normalizer.emit_user_message(client_user_message_id, input.clone())? {
        write_value(stdout, &notification_to_wire_value(&notification)?)?;
    }

    for (method, params) in image_requests(&input, &child.session_id)? {
        if let Err(error) = child.rpc(method, params) {
            // Discard a partially attached batch rather than leaking it into
            // the next user turn. The next gateway resumes durable history.
            let _ = child.child.kill();
            let _ = child.child.wait();
            return Err(error);
        }
    }

    let mut params = json!({
        "session_id": child.session_id,
        "text": user_input_text(&input),
    });
    if let Some(reasoning) = reasoning.filter(|value| !value.trim().is_empty()) {
        params["reasoning_effort"] = Value::String(reasoning);
    }
    let prompt_id = child.send_request("prompt.submit", params)?;

    loop {
        if interrupt_rx.try_recv().is_ok() {
            let params = json!({"session_id": child.session_id});
            let deadline = Instant::now() + INTERRUPT_DRAIN_TIMEOUT;
            let _ = child.rpc_until("session.interrupt", params, deadline);
            // Hermes ends the interrupted turn with its own terminal frame;
            // drain until it arrives (bounded) so it can't leak into the
            // next turn as an instant terminal.
            let mut settled = false;
            while let Ok(frame) = child.read_frame_until(deadline) {
                child.observe_session(&frame)?;
                if frame.pointer("/params/session_id").and_then(Value::as_str)
                    == Some(&child.session_id)
                    && normalize_hermes_frame(&turn_id, &frame)
                        .iter()
                        .any(NormalizedEvent::is_terminal)
                {
                    child.session_started = true;
                    child.persist_session()?;
                    settled = true;
                    break;
                }
            }
            if !settled {
                // A delayed completion must never finish a subsequent turn.
                let _ = child.child.kill();
                let _ = child.child.wait();
            }
            if let Some(notification) = normalizer.finish_turn_interrupted()? {
                write_value(stdout, &notification_to_wire_value(&notification)?)?;
            }
            return Ok(());
        }

        let next = if let Some(frame) = child.pending.pop_front() {
            Ok(Ok(frame.to_string()))
        } else {
            child.stdout.recv_timeout(Duration::from_millis(50))
        };
        match next {
            Ok(line) => {
                let Ok(frame) = serde_json::from_str::<Value>(line?.trim()) else {
                    continue;
                };
                if frame.get("id").and_then(Value::as_i64) == Some(prompt_id) {
                    if let Some(error) = frame.get("error") {
                        // Drop queued attachments and other partial prompt state.
                        let _ = child.child.kill();
                        let _ = child.child.wait();
                        return Err(HarnessServerError::Protocol(format!(
                            "Hermes prompt.submit failed: {error}"
                        )));
                    }
                    child.session_started = true;
                    child.persist_session()?;
                }
                child.observe_session(&frame)?;
                if frame.pointer("/params/session_id").and_then(Value::as_str)
                    != Some(&child.session_id)
                {
                    continue;
                }
                if let Some((method, params)) = blocking_request_reply(&frame) {
                    eprintln!(
                        "harness-server: Hermes waits for a person ({}); answering at once",
                        event_type(&frame).unwrap_or_default()
                    );
                    // The gateway's reply to this request has an id and no
                    // session, so the loop skips it.
                    child.send_request(method, params)?;
                    continue;
                }
                let mut terminal = false;
                for event in normalize_hermes_frame(&turn_id, &frame) {
                    terminal |= event.is_terminal();
                    for notification in normalizer.process_event(&event)? {
                        write_value(stdout, &notification_to_wire_value(&notification)?)?;
                    }
                }
                if terminal {
                    child.session_started = true;
                    child.persist_session()?;
                    // A failed turn's error was latched from the Result event.
                    if let Some(notification) = normalizer.finish_turn(None)? {
                        write_value(stdout, &notification_to_wire_value(&notification)?)?;
                    }
                    return Ok(());
                }
            }
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => {
                return Err(HarnessServerError::HermesExited {
                    status: child.child.wait()?,
                });
            }
        }
    }
}

fn user_input_text(input: &[UserInput]) -> String {
    let mut parts = Vec::new();
    for item in input {
        match item {
            UserInput::Text { text, .. } => parts.push(text.clone()),
            UserInput::Image { .. } | UserInput::LocalImage { .. } => {}
            UserInput::Skill { name, path } => {
                parts.push(format!("[skill: {name} at {}]", path.display()))
            }
            UserInput::Mention { name, path } => parts.push(format!("[mention: {name} at {path}]")),
        }
    }
    parts.join("\n")
}

#[cfg(test)]
mod tests {
    use codex_app_server_protocol::UserInput;
    use serde_json::json;

    use crate::traits::{NormalizedContent, NormalizedEvent};

    use super::normalize_hermes_frame;

    fn frame(kind: &str, payload: serde_json::Value) -> serde_json::Value {
        json!({
            "jsonrpc": "2.0",
            "method": "event",
            "params": {"type": kind, "session_id": "abc", "payload": payload},
        })
    }

    fn is_terminal(events: &[NormalizedEvent]) -> bool {
        events.iter().any(NormalizedEvent::is_terminal)
    }

    #[test]
    fn message_delta_becomes_agent_text_delta() {
        let events = normalize_hermes_frame("1", &frame("message.delta", json!({"text": "hi"})));
        assert!(!is_terminal(&events));
        assert!(matches!(
            &events[..],
            [NormalizedEvent::AgentTextDelta { delta, .. }] if delta == "hi"
        ));
    }

    #[test]
    fn reasoning_delta_becomes_reasoning_text_delta() {
        let events =
            normalize_hermes_frame("1", &frame("reasoning.delta", json!({"text": "thinking"})));
        assert!(matches!(
            &events[..],
            [NormalizedEvent::ReasoningTextDelta { delta, .. }] if delta == "thinking"
        ));
    }

    #[test]
    fn tool_start_and_complete_round_trip() {
        let start_events = normalize_hermes_frame(
            "1",
            &frame(
                "tool.start",
                json!({"tool_id": "t1", "name": "terminal", "args": {"command": "ls"}}),
            ),
        );
        let [NormalizedEvent::AssistantMessage { content, .. }] = &start_events[..] else {
            panic!("expected assistant message, got {start_events:?}");
        };
        assert!(matches!(
            &content[..],
            [NormalizedContent::ToolUse { raw_id, tool, .. }]
                if raw_id == "t1" && tool == "terminal"
        ));

        let complete_events = normalize_hermes_frame(
            "1",
            &frame(
                "tool.complete",
                json!({"tool_id": "t1", "name": "terminal", "result": {"output": "ok", "exit_code": 0}}),
            ),
        );
        let [NormalizedEvent::ToolResults(results)] = &complete_events[..] else {
            panic!("expected tool results, got {complete_events:?}");
        };
        assert_eq!(results[0].tool_use_id, "t1");
        assert!(!results[0].is_error);
        assert_eq!(results[0].exit_code, Some(0));
    }

    #[test]
    fn message_complete_finishes_turn_with_canonical_text() {
        let events = normalize_hermes_frame(
            "1",
            &frame("message.complete", json!({"text": "partial then final"})),
        );
        assert!(is_terminal(&events));
        assert!(matches!(
            &events[..],
            [
                NormalizedEvent::AssistantMessage { partial: false, content, .. },
                NormalizedEvent::Result { error: None },
            ] if matches!(
                &content[..],
                [NormalizedContent::AgentText { text, .. }] if text == "partial then final"
            )
        ));
    }

    #[test]
    fn message_complete_error_becomes_failed_result() {
        let events = normalize_hermes_frame(
            "1",
            &frame(
                "message.complete",
                json!({"text": "boom", "status": "error", "error": "provider 500"}),
            ),
        );
        assert!(is_terminal(&events));
        assert!(matches!(
            &events[..],
            [NormalizedEvent::Result { error: Some(error) }] if error == "provider 500"
        ));
    }

    #[test]
    fn message_complete_error_falls_back_to_text() {
        let events = normalize_hermes_frame(
            "1",
            &frame(
                "message.complete",
                json!({"text": "boom", "status": "error"}),
            ),
        );
        assert!(matches!(
            &events[..],
            [NormalizedEvent::Result { error: Some(error) }] if error == "boom"
        ));
    }

    #[test]
    fn tool_complete_marks_failures() {
        let events = normalize_hermes_frame(
            "1",
            &frame(
                "tool.complete",
                json!({"tool_id": "t2", "result": {"success": false, "error": "denied"}}),
            ),
        );
        let [NormalizedEvent::ToolResults(results)] = &events[..] else {
            panic!("expected tool results");
        };
        assert!(results[0].is_error);
    }

    #[test]
    fn unknown_events_are_ignored() {
        let events = normalize_hermes_frame("1", &frame("session.info", json!({"model": "x"})));
        assert!(events.is_empty());
    }

    #[test]
    fn non_event_frames_are_ignored() {
        let events = normalize_hermes_frame(
            "1",
            &json!({"jsonrpc": "2.0", "id": 7, "result": {"ok": true}}),
        );
        assert!(events.is_empty());
    }

    #[test]
    fn usage_event_maps_token_counts() {
        let events = normalize_hermes_frame(
            "1",
            &frame(
                "turn.usage",
                json!({"input_tokens": 100, "output_tokens": 20, "total_tokens": 120}),
            ),
        );
        let [NormalizedEvent::TokenUsage { usage }] = &events[..] else {
            panic!("expected token usage");
        };
        assert_eq!(usage.input_tokens, Some(100));
        assert_eq!(usage.total_tokens, Some(120));
    }

    fn fake_gateway(program: &str) -> super::HermesChild {
        use std::io::BufRead;
        use std::process::{Command, Stdio};
        let mut child = Command::new("python3")
            .args(["-u", "-c", program])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in std::io::BufReader::new(stdout).lines() {
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        super::HermesChild {
            child,
            stdin,
            stdout: rx,
            session_id: "live".into(),
            stored_session_id: "durable".into(),
            session_started: true,
            session_file: None,
            pending: Default::default(),
            next_rpc_id: 0,
        }
    }

    const GATEWAY: &str = r#"
import json,sys

def send(value): print(json.dumps(value), flush=True)
def event(kind, payload, session='live'):
    send({'method':'event','params':{'type':kind,'session_id':session,'payload':payload}})
for line in sys.stdin:
    req=json.loads(line)
    method=req['method']
    if method=='prompt.submit':
        text=req['params']['text']
        if text=='reject':
            send({'id':req['id'],'error':{'message':'prompt rejected'}})
            continue
        send({'id':req['id'],'result':{'accepted':True}})
        if text=='wait': continue
        event('message.complete', {'text':'WRONG CHILD'}, 'subagent')
        event('message.complete', {'text':'PARENT DONE'})
    elif method=='session.interrupt':
        event('message.complete', {'text':'cancelled','status':'interrupted'})
        send({'id':req['id'],'result':{'status':'interrupted'}})
    elif method=='session.resume':
        if req['params']['session_id']=='missing':
            send({'id':req['id'],'error':{'message':'session missing'}})
        else:
            send({'id':req['id'],'result':{'session_id':'new-live','session_key':req['params']['session_id']}})
    elif method=='image.attach':
        send({'id':req['id'],'error':{'message':'missing image'}})
"#;

    /// A gateway whose model calls a tool that waits for a person. The turn
    /// ends when the answer arrives, and its text is the answer. Without an
    /// answer, it ends after 5 seconds with the text `NO ANSWER`.
    const BLOCKING_GATEWAY: &str = r#"
import json,sys,threading

lock=threading.Lock()
def send(value):
    with lock: print(json.dumps(value), flush=True)
def event(kind, payload, session='live'):
    send({'method':'event','params':{'type':kind,'session_id':session,'payload':payload}})
kind=None
timer=None
for line in sys.stdin:
    req=json.loads(line)
    method=req['method']
    if method=='prompt.submit':
        kind=req['params']['text']
        send({'id':req['id'],'result':{'accepted':True}})
        event(kind+'.request', {'question':'Which one?','choices':['a','b'],'request_id':'r1'})
        timer=threading.Timer(5, lambda: event('message.complete', {'text':'NO ANSWER'}))
        timer.start()
    elif method==kind+'.respond':
        timer.cancel()
        send({'id':req['id'],'result':{'status':'ok'}})
        event('message.complete', {'text':json.dumps(req['params'], sort_keys=True)})
"#;

    fn final_text(output: &[u8]) -> String {
        String::from_utf8_lossy(output)
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .filter(|value| value["method"] == "item/completed")
            .filter_map(|value| value["params"]["item"]["text"].as_str().map(str::to_owned))
            .next_back()
            .unwrap_or_default()
    }

    #[test]
    fn a_trimmed_final_text_is_not_sent_again() {
        // Hermes streams the reply with leading newlines and completes it
        // with the trimmed text.
        let mut normalizer = crate::turn::CodexTurnNormalizer::new(crate::turn::BridgeConfig::new(
            "T-local", "turn-1",
        ));
        let mut deltas = Vec::new();
        let mut completed = Vec::new();
        for frame in [
            frame("message.delta", json!({"text": "\n\nwhich pr"})),
            frame("message.delta", json!({"text": " did you mean?"})),
            frame(
                "message.complete",
                json!({"text": "which pr did you mean?"}),
            ),
        ] {
            for event in normalize_hermes_frame("1", &frame) {
                for notification in normalizer.process_event(&event).unwrap() {
                    let rpc = crate::wire::notification_to_jsonrpc(&notification).unwrap();
                    let params = rpc.params.unwrap_or_default();
                    match rpc.method.as_str() {
                        "item/agentMessage/delta" => deltas.push(params["delta"].clone()),
                        "item/completed" if params["item"]["type"] == "agentMessage" => {
                            completed.push(params["item"]["text"].clone())
                        }
                        _ => {}
                    }
                }
            }
        }
        assert_eq!(deltas, [json!("\n\nwhich pr"), json!(" did you mean?")]);
        assert_eq!(completed, [json!("which pr did you mean?")]);
    }

    #[test]
    fn a_clarify_question_is_answered_at_once() {
        let mut child = fake_gateway(BLOCKING_GATEWAY);
        let (_tx, rx) = std::sync::mpsc::channel();
        let mut output = Vec::new();
        let input = vec![UserInput::Text {
            text: "clarify".into(),
            text_elements: Vec::new(),
        }];
        super::run_hermes_turn(&mut child, &mut output, input, None, None, 1, &rx).unwrap();

        let reply: serde_json::Value = serde_json::from_str(&final_text(&output)).unwrap();
        assert_eq!(reply["request_id"], "r1");
        assert_eq!(reply["session_id"], "live");
        let answer = reply["answer"].as_str().unwrap();
        assert!(
            answer.starts_with("Nobody can answer this prompt"),
            "{answer}"
        );
        assert!(
            answer.contains("question and its choices in your reply"),
            "{answer}"
        );
    }

    #[test]
    fn prompts_for_a_password_or_secret_get_an_empty_answer() {
        for (kind, key) in [
            ("sudo", "password"),
            ("secret", "value"),
            ("terminal.read", "text"),
        ] {
            let mut child = fake_gateway(BLOCKING_GATEWAY);
            let (_tx, rx) = std::sync::mpsc::channel();
            let mut output = Vec::new();
            let input = vec![UserInput::Text {
                text: kind.into(),
                text_elements: Vec::new(),
            }];
            super::run_hermes_turn(&mut child, &mut output, input, None, None, 1, &rx).unwrap();

            let reply: serde_json::Value = serde_json::from_str(&final_text(&output)).unwrap();
            assert_eq!(reply["request_id"], "r1", "{kind}");
            assert_eq!(reply[key], "", "{kind}");
        }
    }

    #[test]
    fn other_frames_need_no_reply() {
        assert!(
            super::blocking_request_reply(&frame("message.delta", json!({"text": "hi"}))).is_none()
        );
        assert!(
            super::blocking_request_reply(&frame("approval.request", json!({"request_id": "r1"})))
                .is_none()
        );
        // A request without an id cannot be answered.
        assert!(super::blocking_request_reply(&frame("clarify.request", json!({}))).is_none());
    }

    #[test]
    fn durable_key_is_persisted_and_compaction_updates_it() {
        let path = std::env::temp_dir().join(format!("hermes-session-{}", uuid::Uuid::new_v4()));
        let mut child = fake_gateway(GATEWAY);
        child.session_file = Some(path.clone());
        child
            .accept_session(
                &json!({"session_id":"live", "stored_session_id":"durable"}),
                false,
            )
            .unwrap();
        // Creating a draft does not create a Hermes DB row yet.
        assert!(!path.exists());
        let (_tx, rx) = std::sync::mpsc::channel();
        super::run_hermes_turn(&mut child, &mut Vec::new(), vec![], None, None, 1, &rx).unwrap();
        assert_eq!(
            super::resume_session_id(None, Some(&path))
                .unwrap()
                .as_deref(),
            Some("durable")
        );
        let mut info = frame("session.info", json!({"stored_session_id":"compacted"}));
        info["params"]["session_id"] = json!("live");
        child.observe_session(&info).unwrap();
        assert_eq!(
            super::resume_session_id(None, Some(&path))
                .unwrap()
                .as_deref(),
            Some("compacted")
        );
        assert_eq!(
            super::resume_session_id(Some("override"), Some(&path))
                .unwrap()
                .as_deref(),
            Some("override")
        );
        std::fs::write(&path, "\n").unwrap();
        assert!(super::resume_session_id(None, Some(&path)).is_err());
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn resume_uses_durable_key_and_errors_are_not_new_sessions() {
        let mut child = fake_gateway(GATEWAY);
        let result = child
            .rpc("session.resume", json!({"session_id":"durable"}))
            .unwrap();
        child.accept_session(&result, true).unwrap();
        assert_eq!(child.session_id, "new-live");
        assert_eq!(child.thread_id(), "durable");
        assert!(
            child
                .rpc("session.resume", json!({"session_id":"missing"}))
                .is_err()
        );
    }

    #[test]
    fn image_bytes_are_attached_and_not_replaced_with_text() {
        let input: Vec<codex_app_server_protocol::UserInput> = serde_json::from_value(json!([
            {"type":"text", "text":"describe", "text_elements":[]},
            {"type":"image", "url":"data:image/png;base64,aGVsbG8="},
            {"type":"localImage", "path":"/tmp/image.png"}
        ]))
        .unwrap();
        let requests = super::image_requests(&input, "live").unwrap();
        assert_eq!(requests[0].0, "image.attach_bytes");
        assert_eq!(
            requests[0].1["content_base64"],
            "data:image/png;base64,aGVsbG8="
        );
        assert_eq!(requests[1].0, "image.attach");
        assert_eq!(requests[1].1["path"], "/tmp/image.png");
        assert_eq!(super::user_input_text(&input), "describe");
    }

    fn text_input(text: &str) -> Vec<codex_app_server_protocol::UserInput> {
        serde_json::from_value(json!([{"type":"text","text":text,"text_elements":[]}])).unwrap()
    }

    #[test]
    fn child_completions_do_not_finish_parent_and_prompt_rejection_fails() {
        let mut child = fake_gateway(GATEWAY);
        let (_tx, rx) = std::sync::mpsc::channel();
        let mut output = Vec::new();
        super::run_hermes_turn(
            &mut child,
            &mut output,
            text_input("hello"),
            None,
            None,
            1,
            &rx,
        )
        .unwrap();
        let output = String::from_utf8(output).unwrap();
        assert!(output.contains("PARENT DONE"));
        assert!(!output.contains("WRONG CHILD"));
        let error = super::run_hermes_turn(
            &mut child,
            &mut Vec::new(),
            text_input("reject"),
            None,
            None,
            2,
            &rx,
        )
        .unwrap_err();
        assert!(error.to_string().contains("prompt rejected"));
        assert!(!child.is_alive());
    }

    #[test]
    fn interrupt_completion_before_ack_is_drained_before_next_turn() {
        let mut child = fake_gateway(GATEWAY);
        let (tx, rx) = std::sync::mpsc::channel();
        tx.send(()).unwrap();
        let start = std::time::Instant::now();
        super::run_hermes_turn(
            &mut child,
            &mut Vec::new(),
            text_input("wait"),
            None,
            None,
            1,
            &rx,
        )
        .unwrap();
        assert!(start.elapsed() < std::time::Duration::from_secs(5));
        assert!(child.is_alive());
        let mut output = Vec::new();
        super::run_hermes_turn(
            &mut child,
            &mut output,
            text_input("hello"),
            None,
            None,
            2,
            &rx,
        )
        .unwrap();
        assert!(String::from_utf8(output).unwrap().contains("PARENT DONE"));
    }

    #[test]
    fn failed_attachment_discards_gateway_queue() {
        let mut child = fake_gateway(GATEWAY);
        let (_tx, rx) = std::sync::mpsc::channel();
        let input =
            serde_json::from_value(json!([{"type":"localImage", "path":"/tmp/missing.png"}]))
                .unwrap();
        assert!(
            super::run_hermes_turn(&mut child, &mut Vec::new(), input, None, None, 1, &rx).is_err()
        );
        assert!(!child.is_alive());
    }
}
