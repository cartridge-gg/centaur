//! MCP tools that drive a Centaur agent session.
//!
//! A session started here lives under an `mcp-session:<principal>:<id>`
//! thread key and binds to the calling MCP principal. The principal therefore
//! sees and drives only its own sessions, and the agent sandbox runs with the
//! same credentials as that principal's MCP tool calls. Turns use the normal
//! session runtime: one queued or running execution per session, and a prompt
//! sent while a turn runs steers that turn.

use std::{
    collections::HashMap,
    sync::{Arc, OnceLock, PoisonError},
    time::{Duration, Instant},
};

use centaur_iron_control::IronControlError;
use centaur_session_core::{
    ExecutionStatus, HarnessType, MessageRole, Session, SessionEvent, SessionExecution,
    SessionMessageInput, ThreadKey,
};
use centaur_session_runtime::{
    ExecuteSessionInput, HarnessConflictPolicy, SessionRuntime, SessionRuntimeError,
};
use centaur_session_sqlx::SessionStoreError;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tracing::Span;
use uuid::Uuid;

use super::{
    McpPrincipal, McpToolCallOutcome, mcp_text_result, record_mcp_tool_correlation,
    record_mcp_tool_method,
};
use crate::{ApiError, routes::AppState};

pub(super) const SESSION_SEND_TOOL: &str = "centaur_session_send";
pub(super) const SESSION_READ_TOOL: &str = "centaur_session_read";
pub(super) const SESSION_INTERRUPT_TOOL: &str = "centaur_session_interrupt";
pub(super) const SESSION_LIST_TOOL: &str = "centaur_session_list";

const THREAD_NAMESPACE: &str = "mcp-session";
const MAX_SESSION_ID_BYTES: usize = 64;
const MAX_IDEMPOTENCY_KEY_BYTES: usize = 128;
const MAX_PROMPT_BYTES: usize = 200 * 1024;
const DEFAULT_READ_WAIT_SECONDS: u64 = 25;
// Codex clients time out MCP calls at 60 seconds by default.
const MAX_READ_WAIT_SECONDS: u64 = 50;
const READ_POLL_INTERVAL: Duration = Duration::from_secs(1);
// The terminal event follows the terminal status in a separate write.
const TERMINAL_EVENT_GRACE: time::Duration = time::Duration::seconds(10);
const EVENT_PAGE_SIZE: i64 = 500;
const MAX_EVENT_PAGES: usize = 10;
const PROGRESS_BUDGET_BYTES: usize = 32 * 1024;
const MESSAGE_TEXT_CHARS: usize = 4_000;
const SHORT_TEXT_CHARS: usize = 500;
const COMMAND_OUTPUT_TAIL_CHARS: usize = 800;
const FINAL_ANSWER_CHARS: usize = 100_000;
const DEFAULT_LIST_LIMIT: i64 = 20;
const MAX_LIST_LIMIT: i64 = 50;

pub(super) fn session_tool_name(name: &str) -> bool {
    matches!(
        name,
        SESSION_SEND_TOOL | SESSION_READ_TOOL | SESSION_INTERRUPT_TOOL | SESSION_LIST_TOOL
    )
}

pub(super) fn session_tools() -> Vec<Value> {
    vec![
        json!({
            "name": SESSION_SEND_TOOL,
            "description": concat!(
                "Send a prompt to a Centaur agent session. The agent runs in its own sandbox with ",
                "your Centaur principal's tools and credentials, and keeps its conversation across ",
                "prompts. Omit session_id to start a new session. Pass a session_id to continue ",
                "that session; an unused session_id starts a new session with that id. If a turn ",
                "is already running, the prompt steers that turn instead of starting a new one. ",
                "Returns at once with session_id and execution_id. Then call ",
                "centaur_session_read to wait for the answer."
            ),
            "inputSchema": {
                "type": "object",
                "required": ["prompt"],
                "additionalProperties": false,
                "properties": {
                    "prompt": {
                        "type": "string",
                        "minLength": 1,
                        "description": "The instruction or message for the agent.",
                    },
                    "session_id": {
                        "type": "string",
                        "pattern": "^[A-Za-z0-9._-]{1,64}$",
                        "description": "Session to continue, as returned by an earlier call. A new id starts a new session.",
                    },
                    "harness": {
                        "type": "string",
                        "enum": ["codex", "claudecode", "amp", "hermes", "nanocodex"],
                        "description": "Agent harness for a new session. Defaults to the deployment harness. A session keeps its harness.",
                    },
                    "persona_id": {
                        "type": "string",
                        "description": "Centaur persona for a new session. Defaults to the deployment persona. A session keeps its persona.",
                    },
                    "model": {
                        "type": "string",
                        "description": "Optional model for a new turn. A prompt that steers a running turn keeps that turn's model. Omit to use the harness default.",
                    },
                    "idempotency_key": {
                        "type": "string",
                        "maxLength": MAX_IDEMPOTENCY_KEY_BYTES,
                        "description": "Optional caller key for this prompt. A retry with the same key returns the same turn instead of sending the prompt again.",
                    },
                },
            },
        }),
        json!({
            "name": SESSION_READ_TOOL,
            "description": concat!(
                "Read the progress and result of a turn in a Centaur agent session. Waits up to ",
                "wait_seconds for the turn to finish. Returns the turn status, a compact progress ",
                "list (agent messages, commands, tool calls, file changes, errors), and ",
                "final_answer when the turn is done. If done is false, call again with ",
                "after_event_id set to next_after_event_id."
            ),
            "inputSchema": {
                "type": "object",
                "required": ["session_id"],
                "additionalProperties": false,
                "properties": {
                    "session_id": {
                        "type": "string",
                        "description": "Session id returned by centaur_session_send.",
                    },
                    "execution_id": {
                        "type": "string",
                        "description": "Turn to read. Defaults to the latest turn of the session.",
                    },
                    "after_event_id": {
                        "type": "integer",
                        "minimum": 0,
                        "description": "Return only progress after this event id. Use next_after_event_id from the previous read.",
                    },
                    "wait_seconds": {
                        "type": "integer",
                        "minimum": 0,
                        "maximum": MAX_READ_WAIT_SECONDS,
                        "description": "How long to wait for the turn to finish. Defaults to 25. Use 0 to return at once.",
                    },
                },
            },
        }),
        json!({
            "name": SESSION_INTERRUPT_TOOL,
            "description": "Stop the running turn of a Centaur agent session. The session stays available for new prompts.",
            "inputSchema": {
                "type": "object",
                "required": ["session_id"],
                "additionalProperties": false,
                "properties": {
                    "session_id": {
                        "type": "string",
                        "description": "Session id returned by centaur_session_send.",
                    },
                    "reason": {
                        "type": "string",
                        "description": "Optional reason, recorded with the interrupt.",
                    },
                },
            },
        }),
        json!({
            "name": SESSION_LIST_TOOL,
            "description": "List the Centaur agent sessions you started through MCP, most recently updated first.",
            "inputSchema": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": MAX_LIST_LIMIT,
                        "description": "Maximum sessions to return. Defaults to 20.",
                    },
                },
            },
        }),
    ]
}

pub(super) async fn session_tool_result(
    state: &AppState,
    principal: &McpPrincipal,
    name: &str,
    arguments: Value,
) -> Result<McpToolCallOutcome, ApiError> {
    let result = match name {
        SESSION_SEND_TOOL => session_send(state, principal, arguments).await,
        SESSION_READ_TOOL => session_read(state, principal, arguments).await,
        SESSION_INTERRUPT_TOOL => session_interrupt(state, principal, arguments).await,
        SESSION_LIST_TOOL => session_list(state, principal, arguments).await,
        _ => unreachable!("session tool names are checked before dispatch"),
    };
    match result {
        Ok(content) => json_outcome(content, false),
        Err(SessionToolError::Caller(message)) => Ok(McpToolCallOutcome {
            result: mcp_text_result(message, true),
            timed_out: false,
        }),
        Err(SessionToolError::Api(error)) => Err(error),
    }
}

/// Errors the MCP client can act on become tool errors; the rest stay API
/// errors so their details are logged and never echoed.
#[derive(Debug)]
enum SessionToolError {
    Caller(String),
    Api(ApiError),
}

impl From<ApiError> for SessionToolError {
    fn from(error: ApiError) -> Self {
        match error {
            ApiError::Runtime(error) => error.into(),
            error => Self::Api(error),
        }
    }
}

impl From<SessionRuntimeError> for SessionToolError {
    fn from(error: SessionRuntimeError) -> Self {
        match error {
            SessionRuntimeError::BadRequest(message) => Self::Caller(message),
            SessionRuntimeError::ShuttingDown => {
                Self::Caller("Centaur is restarting. Retry in a few seconds.".to_owned())
            }
            error @ SessionRuntimeError::CapacityExceeded { .. } => {
                Self::Caller(format!("{error}. Retry later."))
            }
            SessionRuntimeError::Store(
                error @ (SessionStoreError::HarnessConflict { .. }
                | SessionStoreError::PrincipalConflict { .. }
                | SessionStoreError::ExecutionNotFound { .. }),
            ) => Self::Caller(error.to_string()),
            SessionRuntimeError::Store(SessionStoreError::NotFound { .. }) => {
                Self::Caller("unknown session".to_owned())
            }
            SessionRuntimeError::IronControl(
                error @ (IronControlError::PrincipalDerivation(_)
                | IronControlError::SessionPrincipalNotPreapproved { .. }),
            ) => Self::Caller(error.to_string()),
            error => Self::Api(ApiError::Runtime(error)),
        }
    }
}

fn caller_error(message: impl Into<String>) -> SessionToolError {
    SessionToolError::Caller(message.into())
}

fn parse_arguments<T: for<'de> Deserialize<'de>>(arguments: Value) -> Result<T, SessionToolError> {
    // MCP clients send `{}` or omit arguments for tools without required fields.
    let arguments = if arguments.is_null() {
        json!({})
    } else {
        arguments
    };
    serde_json::from_value(arguments)
        .map_err(|error| caller_error(format!("invalid arguments: {error}")))
}

fn json_outcome(content: Value, is_error: bool) -> Result<McpToolCallOutcome, ApiError> {
    let mut result = mcp_text_result(serde_json::to_string_pretty(&content)?, is_error);
    result["structuredContent"] = content;
    Ok(McpToolCallOutcome {
        result,
        timed_out: false,
    })
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionSendArguments {
    prompt: String,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    harness: Option<String>,
    #[serde(default)]
    persona_id: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    idempotency_key: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionReadArguments {
    session_id: String,
    #[serde(default)]
    execution_id: Option<String>,
    #[serde(default)]
    after_event_id: Option<i64>,
    #[serde(default)]
    wait_seconds: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionInterruptArguments {
    session_id: String,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionListArguments {
    #[serde(default)]
    limit: Option<i64>,
}

async fn session_send(
    state: &AppState,
    principal: &McpPrincipal,
    arguments: Value,
) -> Result<Value, SessionToolError> {
    let args = parse_arguments::<SessionSendArguments>(arguments)?;
    record_mcp_tool_method(&Span::current(), SESSION_SEND_TOOL, "send");
    let prompt = args.prompt.trim();
    if prompt.is_empty() {
        return Err(caller_error("prompt must not be blank"));
    }
    if prompt.len() > MAX_PROMPT_BYTES {
        return Err(caller_error(format!(
            "prompt must be at most {MAX_PROMPT_BYTES} bytes"
        )));
    }
    let idempotency_key = non_blank(args.idempotency_key.as_deref())
        .map(validate_idempotency_key)
        .transpose()?;
    let request = SendRequest {
        prompt,
        harness: non_blank(args.harness.as_deref())
            .map(parse_harness)
            .transpose()?,
        persona_id: non_blank(args.persona_id.as_deref()),
        model: non_blank(args.model.as_deref()),
        idempotency_key,
    };
    let session_id = match non_blank(args.session_id.as_deref()) {
        Some(session_id) => validate_session_id(session_id)?.to_owned(),
        None => new_session_id(principal, idempotency_key),
    };
    let thread_key = session_thread_key(principal, &session_id)?;
    let runtime = state.runtime()?;

    // Sends to one session run one at a time in this process, so their
    // active-turn checks and steering checks do not interleave.
    let lock = send_lock(&thread_key);
    let result = {
        let _guard = lock.lock().await;
        send_locked(&runtime, principal, &session_id, &thread_key, request).await
    };
    drop(lock);
    release_send_lock(&thread_key);
    result
}

struct SendRequest<'a> {
    prompt: &'a str,
    harness: Option<HarnessType>,
    persona_id: Option<&'a str>,
    model: Option<&'a str>,
    idempotency_key: Option<&'a str>,
}

async fn send_locked(
    runtime: &SessionRuntime,
    principal: &McpPrincipal,
    session_id: &str,
    thread_key: &ThreadKey,
    request: SendRequest<'_>,
) -> Result<Value, SessionToolError> {
    let (session, created, unavailable_persona_id) =
        match owned_session_if_exists(runtime, principal, thread_key).await? {
            Some(session) => {
                ensure_session_settings_unchanged(
                    &session,
                    request.harness.as_ref(),
                    request.persona_id,
                )?;
                (session, false, None)
            }
            None => {
                let harness = request
                    .harness
                    .clone()
                    .unwrap_or_else(|| runtime.default_session_harness());
                let outcome = runtime
                    .create_or_get_session_with_principal(
                        thread_key,
                        &harness,
                        request.persona_id,
                        Some(session_metadata(principal)),
                        HarnessConflictPolicy::Reject,
                        Some(&principal.principal_id),
                    )
                    .await?;
                (
                    outcome.session,
                    true,
                    outcome.unavailable_requested_persona_id,
                )
            }
        };
    let reply = |execution: &SessionExecution, delivery: Delivery| {
        record_mcp_tool_correlation(&Span::current(), None, Some(&execution.execution_id), None);
        let mut result = send_result(session_id, &session, execution, delivery, created);
        if let Some(persona_id) = unavailable_persona_id.as_deref() {
            result["unavailable_requested_persona_id"] = json!(persona_id);
        }
        result
    };

    if let Some(key) = request.idempotency_key
        && let Some((execution, delivery)) = replayed_send(runtime, thread_key, key).await?
    {
        return Ok(reply(&execution, delivery));
    }

    let active = runtime.active_execution(thread_key).await?;
    // Steering reaches a turn only after its harness runs. Before that, the
    // runtime waits up to 15 seconds and then drops the prompt, so refuse it
    // here while nothing is saved yet.
    if let Some(active) = active.as_ref()
        && !runtime.execution_has_output(&active.execution_id).await?
    {
        return Err(caller_error(format!(
            "turn {} is still starting its agent and cannot take a new prompt yet. Send again in a few seconds, or wait for the turn with centaur_session_read.",
            active.execution_id
        )));
    }

    let message_id = request
        .idempotency_key
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    // Appending a user message steers the active turn, if there is one.
    let message_ids = runtime
        .append_messages(
            thread_key,
            &[SessionMessageInput {
                client_message_id: Some(message_id.clone()),
                role: MessageRole::User,
                parts: vec![json!({"type": "text", "text": request.prompt})],
                metadata: requester_metadata(principal),
            }],
        )
        .await?;
    if let Some(active) = active {
        if steering_delivered(runtime, &active.execution_id, &message_ids).await? {
            return Ok(reply(&active, Delivery::Steered));
        }
        // The turn ended before the prompt reached it, so the prompt starts a
        // new turn below. A turn that is still running refused the prompt.
        if let Some(busy) = runtime.active_execution(thread_key).await? {
            return Err(undelivered_error(&busy.execution_id));
        }
    }

    let enqueued = runtime
        .enqueue_session_execution(
            thread_key,
            ExecuteSessionInput {
                idempotency_key: Some(message_id.clone()),
                metadata: Some(execute_metadata(principal, request.model)),
                input_lines: vec![turn_input_line(
                    thread_key,
                    principal,
                    &message_id,
                    request.prompt,
                    request.model,
                )?],
                idle_timeout_ms: None,
                max_duration_ms: None,
            },
        )
        .await;
    match enqueued {
        Ok(execution) => Ok(reply(&execution, Delivery::NewTurn)),
        // Another api-rs process started a turn after the checks above. The
        // append may have steered the prompt into it.
        Err(error) if is_active_turn_conflict(&error) => {
            match runtime.active_execution(thread_key).await? {
                Some(active)
                    if steering_delivered(runtime, &active.execution_id, &message_ids).await? =>
                {
                    Ok(reply(&active, Delivery::Steered))
                }
                Some(active) => Err(undelivered_error(&active.execution_id)),
                None => Err(caller_error(
                    "another turn started at the same time, so the prompt was saved but not delivered. Send it again with a new idempotency_key.",
                )),
            }
        }
        Err(error) => Err(error.into()),
    }
}

fn undelivered_error(execution_id: &str) -> SessionToolError {
    caller_error(format!(
        "the prompt was saved but could not be delivered to running turn {execution_id}. Wait for that turn with centaur_session_read, then send the prompt again with a new idempotency_key."
    ))
}

/// The one-queued-or-running-execution index rejects a second active turn.
fn is_active_turn_conflict(error: &SessionRuntimeError) -> bool {
    matches!(
        error,
        SessionRuntimeError::Store(SessionStoreError::Sqlx(sqlx::Error::Database(database)))
            if database.constraint() == Some("session_executions_one_active_idx")
    )
}

/// A send whose key already produced a turn, or already steered one, returns
/// that turn and does not deliver the prompt again.
async fn replayed_send(
    runtime: &SessionRuntime,
    thread_key: &ThreadKey,
    key: &str,
) -> Result<Option<(SessionExecution, Delivery)>, SessionToolError> {
    let latest = runtime.latest_execution(thread_key).await?;
    if let Some(execution) = latest.as_ref()
        && execution.idempotency_key.as_deref() == Some(key)
    {
        return Ok(Some((execution.clone(), Delivery::Replayed)));
    }
    if !runtime.has_client_message(thread_key, key).await? {
        return Ok(None);
    }
    // The prompt was saved without a turn of its own, so it steered a turn.
    // With no turn at all, the send failed before it started one: resend.
    let execution = match runtime.active_execution(thread_key).await? {
        Some(active) => Some(active),
        None => latest,
    };
    Ok(execution.map(|execution| (execution, Delivery::Replayed)))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Delivery {
    NewTurn,
    Steered,
    Replayed,
}

type SendLocks = std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>;

fn send_locks() -> &'static SendLocks {
    static LOCKS: OnceLock<SendLocks> = OnceLock::new();
    LOCKS.get_or_init(Default::default)
}

fn send_lock(thread_key: &ThreadKey) -> Arc<tokio::sync::Mutex<()>> {
    send_locks()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .entry(thread_key.as_str().to_owned())
        .or_default()
        .clone()
}

/// Drop the lock entry once no send holds or waits for it.
fn release_send_lock(thread_key: &ThreadKey) {
    let mut locks = send_locks().lock().unwrap_or_else(PoisonError::into_inner);
    if locks
        .get(thread_key.as_str())
        .is_some_and(|lock| Arc::strong_count(lock) == 1)
    {
        locks.remove(thread_key.as_str());
    }
}

/// `append_messages` delivers steering to the active turn before it returns.
/// The newest steering event of that turn tells whether it carried these
/// messages; a turn that ended first records no steering event at all.
async fn steering_delivered(
    runtime: &SessionRuntime,
    execution_id: &str,
    message_ids: &[String],
) -> Result<bool, SessionToolError> {
    let event = runtime
        .latest_execution_event(
            execution_id,
            &["session.steering_delivered", "session.steering_failed"],
        )
        .await?;
    Ok(event.is_some_and(|event| steering_event_carries(&event, message_ids)))
}

fn steering_event_carries(event: &SessionEvent, message_ids: &[String]) -> bool {
    event.event_type == "session.steering_delivered"
        && event
            .payload
            .get("message_ids")
            .and_then(Value::as_array)
            .is_some_and(|delivered| {
                message_ids
                    .iter()
                    .any(|id| delivered.iter().any(|value| value.as_str() == Some(id)))
            })
}

fn send_result(
    session_id: &str,
    session: &Session,
    execution: &SessionExecution,
    delivery: Delivery,
    created: bool,
) -> Value {
    let next = match delivery {
        Delivery::NewTurn => {
            "Call centaur_session_read with this session_id to wait for the answer."
        }
        Delivery::Steered => {
            "A turn was already running, so the prompt was sent into that turn. Call centaur_session_read to wait for its answer."
        }
        Delivery::Replayed => {
            "This idempotency_key was already sent, so the prompt was not sent again. Call centaur_session_read to follow the turn."
        }
    };
    json!({
        "session_id": session_id,
        "execution_id": execution.execution_id,
        "status": execution.status,
        "steered": delivery == Delivery::Steered,
        "replayed": delivery == Delivery::Replayed,
        "created": created,
        "harness": session.harness_type,
        "persona_id": session.persona_id,
        "next": next,
    })
}

async fn session_read(
    state: &AppState,
    principal: &McpPrincipal,
    arguments: Value,
) -> Result<Value, SessionToolError> {
    let args = parse_arguments::<SessionReadArguments>(arguments)?;
    record_mcp_tool_method(&Span::current(), SESSION_READ_TOOL, "read");
    let wait_seconds = args.wait_seconds.unwrap_or(DEFAULT_READ_WAIT_SECONDS);
    if wait_seconds > MAX_READ_WAIT_SECONDS {
        return Err(caller_error(format!(
            "wait_seconds must be at most {MAX_READ_WAIT_SECONDS}"
        )));
    }
    let after_event_id = args.after_event_id.unwrap_or(0);
    if after_event_id < 0 {
        return Err(caller_error("after_event_id must not be negative"));
    }
    let session_id = validate_session_id(args.session_id.trim())?;
    let thread_key = session_thread_key(principal, session_id)?;
    let runtime = state.runtime()?;
    owned_session(&runtime, principal, &thread_key).await?;

    let execution = match non_blank(args.execution_id.as_deref()) {
        Some(execution_id) => runtime.thread_execution(&thread_key, execution_id).await?,
        None => match runtime.latest_execution(&thread_key).await? {
            Some(execution) => execution,
            None => {
                return Ok(json!({
                    "session_id": session_id,
                    "status": "idle",
                    "done": true,
                    "progress": [],
                    "next": "This session has no turns yet. Call centaur_session_send to start one.",
                }));
            }
        },
    };
    record_mcp_tool_correlation(&Span::current(), None, Some(&execution.execution_id), None);
    let (execution, terminal) = wait_for_outcome(
        &runtime,
        &thread_key,
        execution,
        Duration::from_secs(wait_seconds),
    )
    .await?;
    let done = terminal.is_some();
    let page = progress_page(
        &runtime,
        &thread_key,
        &execution.execution_id,
        after_event_id,
    )
    .await?;

    let mut result = json!({
        "session_id": session_id,
        "execution_id": execution.execution_id,
        "status": execution.status,
        "done": done,
        "progress": page.items,
        "next_after_event_id": page.next_after_event_id,
        "has_more_progress": page.has_more,
    });
    if let Some(terminal) = terminal {
        apply_terminal_outcome(&mut result, &execution, terminal.as_ref());
    }
    result["next"] = Value::String(read_next_hint(done, page.has_more).to_owned());
    Ok(result)
}

fn read_next_hint(done: bool, has_more: bool) -> &'static str {
    match (done, has_more) {
        (_, true) => {
            "More progress is available. Call centaur_session_read again with after_event_id set to next_after_event_id."
        }
        (false, false) => {
            "The turn is not done yet. Call centaur_session_read again with after_event_id set to next_after_event_id."
        }
        (true, false) => {
            "The turn is done. Call centaur_session_send with this session_id to continue the conversation."
        }
    }
}

/// Wait until the execution is terminal and its terminal event, which holds
/// the answer, is written. The runtime commits the status first, so a
/// terminal status alone can precede the answer. Returns `Some(event)` when
/// the turn is done; the inner `None` means the event never arrived.
async fn wait_for_outcome(
    runtime: &SessionRuntime,
    thread_key: &ThreadKey,
    mut execution: SessionExecution,
    wait: Duration,
) -> Result<(SessionExecution, Option<Option<SessionEvent>>), SessionToolError> {
    let deadline = Instant::now() + wait;
    loop {
        if execution_is_terminal(&execution.status) {
            let terminal = runtime
                .execution_terminal_event(&execution.execution_id)
                .await?;
            if terminal.is_some() || terminal_event_overdue(&execution, OffsetDateTime::now_utc()) {
                return Ok((execution, Some(terminal)));
            }
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok((execution, None));
        }
        tokio::time::sleep(remaining.min(READ_POLL_INTERVAL)).await;
        execution = runtime
            .thread_execution(thread_key, &execution.execution_id)
            .await?;
    }
}

fn terminal_event_overdue(execution: &SessionExecution, now: OffsetDateTime) -> bool {
    let finished_at = execution.completed_at.unwrap_or(execution.updated_at);
    now - finished_at > TERMINAL_EVENT_GRACE
}

fn execution_is_terminal(status: &ExecutionStatus) -> bool {
    matches!(
        status,
        ExecutionStatus::Completed | ExecutionStatus::Failed | ExecutionStatus::Cancelled
    )
}

fn apply_terminal_outcome(
    result: &mut Value,
    execution: &SessionExecution,
    terminal: Option<&SessionEvent>,
) {
    let payload = terminal.map(|event| &event.payload);
    match execution.status {
        ExecutionStatus::Completed => {
            let answer = payload
                .and_then(|payload| payload.get("result_text"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            let (answer, truncated) = truncate_chars(answer, FINAL_ANSWER_CHARS);
            result["final_answer"] = Value::String(answer);
            if truncated {
                result["final_answer_truncated"] = Value::Bool(true);
            }
        }
        ExecutionStatus::Failed => {
            let error = payload
                .and_then(|payload| payload.get("error"))
                .and_then(Value::as_str)
                .or(execution.error.as_deref())
                .unwrap_or("the turn failed");
            result["error"] = Value::String(error.to_owned());
        }
        ExecutionStatus::Cancelled => {
            if let Some(reason) = payload
                .and_then(|payload| payload.get("reason"))
                .and_then(Value::as_str)
            {
                result["cancel_reason"] = Value::String(reason.to_owned());
            }
        }
        ExecutionStatus::Queued | ExecutionStatus::Running => {}
    }
}

struct ProgressPage {
    items: Vec<Value>,
    next_after_event_id: i64,
    has_more: bool,
}

async fn progress_page(
    runtime: &SessionRuntime,
    thread_key: &ThreadKey,
    execution_id: &str,
    after_event_id: i64,
) -> Result<ProgressPage, SessionToolError> {
    let mut items = Vec::new();
    let mut used_bytes = 0;
    let mut cursor = after_event_id;
    for _ in 0..MAX_EVENT_PAGES {
        let events = runtime
            .list_events(thread_key, cursor, Some(execution_id), EVENT_PAGE_SIZE)
            .await?;
        let full_page = events.len() as i64 == EVENT_PAGE_SIZE;
        for event in events {
            if let Some(item) = progress_item(&event) {
                let size = item.to_string().len();
                if used_bytes + size > PROGRESS_BUDGET_BYTES && !items.is_empty() {
                    return Ok(ProgressPage {
                        items,
                        next_after_event_id: cursor,
                        has_more: true,
                    });
                }
                used_bytes += size;
                items.push(item);
            }
            cursor = event.event_id;
        }
        if !full_page {
            return Ok(ProgressPage {
                items,
                next_after_event_id: cursor,
                has_more: false,
            });
        }
    }
    Ok(ProgressPage {
        items,
        next_after_event_id: cursor,
        has_more: true,
    })
}

/// Reduce one session event to a compact progress item. Streaming deltas,
/// reasoning, and bookkeeping events return `None`.
fn progress_item(event: &SessionEvent) -> Option<Value> {
    let mut item = if event.event_type == centaur_session_runtime::SESSION_OUTPUT_LINE_EVENT {
        let line = event.payload.as_str()?;
        let value = serde_json::from_str::<Value>(line).ok()?;
        output_line_progress(&value)?
    } else {
        session_event_progress(event)?
    };
    item.insert("event_id".to_owned(), json!(event.event_id));
    Some(Value::Object(item))
}

fn session_event_progress(event: &SessionEvent) -> Option<Map<String, Value>> {
    let kind = match event.event_type.as_str() {
        "session.execution_started" => "turn_started",
        "session.sandbox_ready" | "session.warm_sandbox_claimed" | "session.sandbox_resumed" => {
            "sandbox_ready"
        }
        "session.steering_delivered" => "prompt_steered",
        "session.interrupt_delivered" => "interrupt_delivered",
        "session.execution_completed" => "turn_completed",
        "session.execution_failed" => "turn_failed",
        "session.execution_cancelled" => "turn_cancelled",
        event_type if event_type.ends_with("_failed") => {
            let mut item = kind_map("warning");
            item.insert("event".to_owned(), json!(event_type));
            if let Some(error) = event.payload.get("error").and_then(Value::as_str) {
                item.insert(
                    "error".to_owned(),
                    json!(truncate_chars(error, SHORT_TEXT_CHARS).0),
                );
            }
            return Some(item);
        }
        _ => return None,
    };
    Some(kind_map(kind))
}

fn output_line_progress(value: &Value) -> Option<Map<String, Value>> {
    let method = value
        .get("method")
        .or_else(|| value.get("type"))
        .and_then(Value::as_str)?;
    match method {
        "item/completed" | "item.completed" => {
            let item = value
                .get("item")
                .or_else(|| value.get("params").and_then(|params| params.get("item")))?;
            thread_item_progress(item)
        }
        "error" => {
            let error = value
                .get("params")
                .unwrap_or(value)
                .get("error")
                .and_then(|error| error.get("message").or(Some(error)))
                .and_then(Value::as_str)
                .unwrap_or("harness error");
            let mut item = kind_map("error");
            item.insert(
                "message".to_owned(),
                json!(truncate_chars(error, SHORT_TEXT_CHARS).0),
            );
            if let Some(will_retry) = value
                .get("params")
                .and_then(|params| params.get("willRetry"))
                .and_then(Value::as_bool)
            {
                item.insert("will_retry".to_owned(), json!(will_retry));
            }
            Some(item)
        }
        // nanocodex emits whole assistant messages as their own lines.
        "assistant.message" => {
            let payload = value.get("payload")?;
            let text = payload.get("text").and_then(Value::as_str)?;
            let mut item = kind_map("message");
            if let Some(phase) = payload.get("phase").and_then(Value::as_str) {
                item.insert("phase".to_owned(), json!(phase));
            }
            item.insert(
                "text".to_owned(),
                json!(truncate_chars(text, MESSAGE_TEXT_CHARS).0),
            );
            Some(item)
        }
        _ => None,
    }
}

fn thread_item_progress(item: &Value) -> Option<Map<String, Value>> {
    let text_field = |name: &str| item.get(name).and_then(Value::as_str);
    let progress = match item.get("type").and_then(Value::as_str)? {
        "agentMessage" | "agent_message" => {
            let mut progress = kind_map("message");
            if let Some(phase) = text_field("phase") {
                progress.insert("phase".to_owned(), json!(phase));
            }
            progress.insert(
                "text".to_owned(),
                json!(truncate_chars(text_field("text").unwrap_or_default(), MESSAGE_TEXT_CHARS).0),
            );
            progress
        }
        "userMessage" => {
            // The last text block is the prompt; earlier blocks are context.
            let text = item
                .get("content")
                .and_then(Value::as_array)
                .and_then(|content| {
                    content
                        .iter()
                        .rev()
                        .find_map(|part| part.get("text").and_then(Value::as_str))
                })
                .unwrap_or_default();
            let mut progress = kind_map("user_message");
            progress.insert(
                "text".to_owned(),
                json!(truncate_chars(text, SHORT_TEXT_CHARS).0),
            );
            progress
        }
        "commandExecution" => {
            let mut progress = kind_map("command");
            progress.insert(
                "command".to_owned(),
                json!(
                    truncate_chars(text_field("command").unwrap_or_default(), SHORT_TEXT_CHARS).0
                ),
            );
            copy_field(item, &mut progress, "status", "status");
            copy_field(item, &mut progress, "exitCode", "exit_code");
            if let Some(output) = text_field("aggregatedOutput").filter(|output| !output.is_empty())
            {
                let (tail, truncated) = tail_chars(output, COMMAND_OUTPUT_TAIL_CHARS);
                progress.insert("output_tail".to_owned(), json!(tail));
                if truncated {
                    progress.insert("output_truncated".to_owned(), json!(true));
                }
            }
            progress
        }
        "fileChange" => {
            let paths = item
                .get("changes")
                .and_then(Value::as_array)
                .map(|changes| {
                    changes
                        .iter()
                        .filter_map(|change| change.get("path").and_then(Value::as_str))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let mut progress = kind_map("file_change");
            copy_field(item, &mut progress, "status", "status");
            progress.insert("paths".to_owned(), json!(paths));
            progress
        }
        "mcpToolCall" => {
            let mut progress = kind_map("tool_call");
            copy_field(item, &mut progress, "server", "server");
            copy_field(item, &mut progress, "tool", "tool");
            copy_field(item, &mut progress, "status", "status");
            if let Some(error) = item
                .get("error")
                .and_then(|error| error.get("message"))
                .and_then(Value::as_str)
            {
                progress.insert(
                    "error".to_owned(),
                    json!(truncate_chars(error, SHORT_TEXT_CHARS).0),
                );
            }
            progress
        }
        "dynamicToolCall" => {
            let mut progress = kind_map("tool_call");
            copy_field(item, &mut progress, "namespace", "server");
            copy_field(item, &mut progress, "tool", "tool");
            copy_field(item, &mut progress, "status", "status");
            copy_field(item, &mut progress, "success", "success");
            progress
        }
        "webSearch" => {
            let mut progress = kind_map("web_search");
            copy_field(item, &mut progress, "query", "query");
            progress
        }
        "plan" => {
            let mut progress = kind_map("plan");
            progress.insert(
                "text".to_owned(),
                json!(truncate_chars(text_field("text").unwrap_or_default(), MESSAGE_TEXT_CHARS).0),
            );
            progress
        }
        "imageGeneration" => {
            let mut progress = kind_map("image");
            copy_field(item, &mut progress, "status", "status");
            copy_field(item, &mut progress, "savedPath", "saved_path");
            progress
        }
        "collabAgentToolCall" => kind_map("subagent_call"),
        "contextCompaction" => kind_map("context_compaction"),
        _ => return None,
    };
    Some(progress)
}

fn kind_map(kind: &str) -> Map<String, Value> {
    Map::from_iter([("kind".to_owned(), Value::String(kind.to_owned()))])
}

fn copy_field(source: &Value, target: &mut Map<String, Value>, from: &str, to: &str) {
    if let Some(value) = source.get(from).filter(|value| !value.is_null()) {
        target.insert(to.to_owned(), value.clone());
    }
}

async fn session_interrupt(
    state: &AppState,
    principal: &McpPrincipal,
    arguments: Value,
) -> Result<Value, SessionToolError> {
    let args = parse_arguments::<SessionInterruptArguments>(arguments)?;
    record_mcp_tool_method(&Span::current(), SESSION_INTERRUPT_TOOL, "interrupt");
    let session_id = validate_session_id(args.session_id.trim())?;
    let thread_key = session_thread_key(principal, session_id)?;
    let runtime = state.runtime()?;
    owned_session(&runtime, principal, &thread_key).await?;
    let reason = non_blank(args.reason.as_deref()).unwrap_or("Interrupted from MCP");
    let outcome = runtime
        .interrupt_active_execution(&thread_key, reason)
        .await?;
    if let Some(execution_id) = outcome.execution_id.as_deref() {
        record_mcp_tool_correlation(&Span::current(), None, Some(execution_id), None);
    }
    Ok(json!({
        "session_id": session_id,
        "interrupted": outcome.interrupted,
        "execution_id": outcome.execution_id,
    }))
}

async fn session_list(
    state: &AppState,
    principal: &McpPrincipal,
    arguments: Value,
) -> Result<Value, SessionToolError> {
    let args = parse_arguments::<SessionListArguments>(arguments)?;
    record_mcp_tool_method(&Span::current(), SESSION_LIST_TOOL, "list");
    let limit = args.limit.unwrap_or(DEFAULT_LIST_LIMIT);
    if !(1..=MAX_LIST_LIMIT).contains(&limit) {
        return Err(caller_error(format!(
            "limit must be between 1 and {MAX_LIST_LIMIT}"
        )));
    }
    let prefix = session_thread_prefix(principal)?;
    let sessions = state
        .runtime()?
        .list_principal_sessions(&prefix, principal.principal_id.trim(), limit)
        .await?;
    let sessions = sessions
        .iter()
        .filter_map(|session| {
            let session_id = session.thread_key.as_str().strip_prefix(&prefix)?;
            Some(json!({
                "session_id": session_id,
                "title": session.title,
                "status": session.status,
                "harness": session.harness_type,
                "persona_id": session.persona_id,
                "created_at": rfc3339(session.created_at),
                "updated_at": rfc3339(session.updated_at),
            }))
        })
        .collect::<Vec<_>>();
    Ok(json!({ "sessions": sessions }))
}

fn rfc3339(value: OffsetDateTime) -> Option<String> {
    value.format(&Rfc3339).ok()
}

/// Load a session and require that the caller owns it. Another principal's
/// session reads as unknown, so ids do not leak across principals.
async fn owned_session(
    runtime: &SessionRuntime,
    principal: &McpPrincipal,
    thread_key: &ThreadKey,
) -> Result<Session, SessionToolError> {
    owned_session_if_exists(runtime, principal, thread_key)
        .await?
        .ok_or_else(|| caller_error("unknown session"))
}

async fn owned_session_if_exists(
    runtime: &SessionRuntime,
    principal: &McpPrincipal,
    thread_key: &ThreadKey,
) -> Result<Option<Session>, SessionToolError> {
    let session = match runtime.session(thread_key).await {
        Ok(session) => session,
        Err(SessionRuntimeError::Store(SessionStoreError::NotFound { .. })) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if session.iron_control_principal.as_deref() != Some(principal.principal_id.trim()) {
        return Err(caller_error("unknown session"));
    }
    Ok(Some(session))
}

fn ensure_session_settings_unchanged(
    session: &Session,
    harness: Option<&HarnessType>,
    persona_id: Option<&str>,
) -> Result<(), SessionToolError> {
    if let Some(harness) = harness
        && harness != &session.harness_type
    {
        return Err(caller_error(format!(
            "this session uses harness {}; start a new session to use {harness}",
            session.harness_type
        )));
    }
    if let Some(persona_id) = persona_id
        && session.persona_id.as_deref() != Some(persona_id)
    {
        return Err(caller_error(format!(
            "this session uses persona {}; start a new session to use {persona_id}",
            session.persona_id.as_deref().unwrap_or("none")
        )));
    }
    Ok(())
}

fn session_thread_prefix(principal: &McpPrincipal) -> Result<String, SessionToolError> {
    let principal_id = principal.principal_id.trim();
    // The principal is a thread-key segment, so it must not contain the
    // separator; otherwise one principal's prefix could cover another's.
    if principal_id.is_empty() || principal_id.contains(':') {
        return Err(SessionToolError::Api(ApiError::Forbidden(
            "MCP principal cannot own agent sessions".to_owned(),
        )));
    }
    Ok(format!("{THREAD_NAMESPACE}:{principal_id}:"))
}

fn session_thread_key(
    principal: &McpPrincipal,
    session_id: &str,
) -> Result<ThreadKey, SessionToolError> {
    let thread_key = format!("{}{session_id}", session_thread_prefix(principal)?);
    ThreadKey::parse(thread_key).map_err(|error| caller_error(error.to_string()))
}

fn validate_session_id(session_id: &str) -> Result<&str, SessionToolError> {
    let valid = !session_id.is_empty()
        && session_id.len() <= MAX_SESSION_ID_BYTES
        && session_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));
    if valid {
        Ok(session_id)
    } else {
        Err(caller_error(format!(
            "session_id must be 1 to {MAX_SESSION_ID_BYTES} characters of letters, digits, '.', '_', or '-'"
        )))
    }
}

fn validate_idempotency_key(key: &str) -> Result<&str, SessionToolError> {
    if key.len() > MAX_IDEMPOTENCY_KEY_BYTES || key.chars().any(char::is_control) {
        return Err(caller_error(format!(
            "idempotency_key must be at most {MAX_IDEMPOTENCY_KEY_BYTES} bytes without control characters"
        )));
    }
    Ok(key)
}

/// A new session gets a random id. With an idempotency key, the id derives
/// from the key, so a retried first send finds the session it created.
fn new_session_id(principal: &McpPrincipal, idempotency_key: Option<&str>) -> String {
    match idempotency_key {
        Some(key) => {
            let digest = Sha256::digest(format!("{}\n{key}", principal.principal_id));
            hex::encode(&digest[..16])
        }
        None => Uuid::new_v4().simple().to_string(),
    }
}

fn parse_harness(value: &str) -> Result<HarnessType, SessionToolError> {
    value
        .parse::<HarnessType>()
        .map_err(|_| caller_error(format!("unsupported harness {value:?}")))
}

fn non_blank(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn session_metadata(principal: &McpPrincipal) -> Value {
    let mut metadata = requester_metadata(principal);
    metadata["mcp_session"] = Value::Bool(true);
    metadata
}

fn requester_metadata(principal: &McpPrincipal) -> Value {
    let mut metadata = Map::from_iter([
        ("source".to_owned(), json!("mcp")),
        ("mcp_principal_id".to_owned(), json!(principal.principal_id)),
        ("mcp_token_id".to_owned(), json!(principal.token_id)),
    ]);
    if let Some(email) = principal.console_user_email.as_deref() {
        metadata.insert("console_user_email".to_owned(), json!(email));
    }
    Value::Object(metadata)
}

fn execute_metadata(principal: &McpPrincipal, model: Option<&str>) -> Value {
    let mut metadata = requester_metadata(principal);
    metadata["action"] = json!("execute");
    if let Some(model) = model {
        metadata["model"] = json!(model);
    }
    metadata
}

/// One user line in the shape harness-server parses from execute's
/// `input_lines`, matching Console threads: a requester context block, then
/// the prompt.
fn turn_input_line(
    thread_key: &ThreadKey,
    principal: &McpPrincipal,
    message_id: &str,
    prompt: &str,
    model: Option<&str>,
) -> Result<String, SessionToolError> {
    let mut line = json!({
        "type": "user",
        "thread_key": thread_key.as_str(),
        "client_user_message_id": message_id,
        "trace_metadata": {"action": "execute", "source": "mcp"},
        "message": {
            "role": "user",
            "content": [
                {"type": "text", "text": requester_context(principal)},
                {"type": "text", "text": prompt},
            ],
        },
    });
    if let Some(model) = model {
        line["model"] = json!(model);
    }
    serde_json::to_string(&line).map_err(|error| SessionToolError::Api(error.into()))
}

fn requester_context(principal: &McpPrincipal) -> String {
    let requester = match (
        principal.console_user_name.as_deref(),
        principal.console_user_email.as_deref(),
    ) {
        (Some(name), Some(email)) => format!("{name} ({email})"),
        (Some(name), None) => name.to_owned(),
        (None, Some(email)) => email.to_owned(),
        (None, None) => principal.name.clone(),
    };
    format!(
        "# Requester Context\n\n\
         The user who prompted this turn is {requester}. They drive this session from an MCP \
         client through Centaur's MCP server. There is no Slack, GitHub, or other chat thread \
         for this session: do not post your reply anywhere. Your final answer is returned to \
         the MCP client.\n\n\
         The user message follows in the next content block.\n---"
    )
}

fn truncate_chars(text: &str, max_chars: usize) -> (String, bool) {
    match text.char_indices().nth(max_chars) {
        Some((index, _)) => (format!("{}…", &text[..index]), true),
        None => (text.to_owned(), false),
    }
}

fn tail_chars(text: &str, max_chars: usize) -> (String, bool) {
    let count = text.chars().count();
    if count <= max_chars {
        return (text.to_owned(), false);
    }
    let start = text
        .char_indices()
        .nth(count - max_chars)
        .map_or(0, |(index, _)| index);
    (format!("…{}", &text[start..]), true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn principal(id: &str) -> McpPrincipal {
        McpPrincipal {
            token_id: "mcp_tok_test".to_owned(),
            principal_id: id.to_owned(),
            console_user_email: Some("ada@example.com".to_owned()),
            console_user_name: Some("Ada".to_owned()),
            name: "Ada".to_owned(),
            scopes: vec!["mcp:tools".to_owned()],
            expires_at: None,
        }
    }

    fn output_event(event_id: i64, line: Value) -> SessionEvent {
        SessionEvent {
            event_id,
            thread_key: ThreadKey::parse("mcp-session:prn_a:s1").unwrap(),
            execution_id: Some("exe_1".to_owned()),
            event_type: centaur_session_runtime::SESSION_OUTPUT_LINE_EVENT.to_owned(),
            payload: Value::String(line.to_string()),
            created_at: OffsetDateTime::UNIX_EPOCH,
        }
    }

    fn caller_message(result: Result<impl std::fmt::Debug, SessionToolError>) -> String {
        match result {
            Err(SessionToolError::Caller(message)) => message,
            other => panic!("expected a caller error, got {other:?}"),
        }
    }

    #[test]
    fn session_thread_keys_are_scoped_to_the_principal() {
        let ada = principal("prn_ada");
        let key = session_thread_key(&ada, "fix-login").unwrap();
        assert_eq!(key.as_str(), "mcp-session:prn_ada:fix-login");
        assert!(
            key.as_str()
                .starts_with(&session_thread_prefix(&ada).unwrap())
        );
        assert!(
            !key.as_str()
                .starts_with(&session_thread_prefix(&principal("prn_ad")).unwrap())
        );
    }

    #[test]
    fn session_threads_do_not_boot_the_tool_host() {
        // The runtime boots the MCP tool host for `mcp:` keys only.
        let key = session_thread_key(&principal("prn_ada"), "s1").unwrap();
        assert!(!key.as_str().starts_with("mcp:"));
    }

    #[test]
    fn principals_with_separators_cannot_own_sessions() {
        assert!(matches!(
            session_thread_prefix(&principal("prn:evil")),
            Err(SessionToolError::Api(ApiError::Forbidden(_)))
        ));
        assert!(matches!(
            session_thread_prefix(&principal("  ")),
            Err(SessionToolError::Api(ApiError::Forbidden(_)))
        ));
    }

    #[test]
    fn session_ids_reject_separators_and_long_values() {
        assert!(validate_session_id("fix-login_2.v1").is_ok());
        for bad in ["", "a:b", "a/b", "a b", "é", &"a".repeat(65)] {
            let message = caller_message(validate_session_id(bad));
            assert!(message.contains("session_id"), "{bad:?}: {message}");
        }
    }

    #[test]
    fn idempotent_new_session_ids_are_stable_per_principal() {
        let ada = principal("prn_ada");
        let first = new_session_id(&ada, Some("req-1"));
        assert_eq!(first, new_session_id(&ada, Some("req-1")));
        assert_ne!(first, new_session_id(&ada, Some("req-2")));
        assert_ne!(first, new_session_id(&principal("prn_bob"), Some("req-1")));
        assert!(validate_session_id(&first).is_ok());
        assert_ne!(new_session_id(&ada, None), new_session_id(&ada, None));
    }

    #[test]
    fn harness_names_use_the_session_core_spelling() {
        assert_eq!(parse_harness("codex").ok(), Some(HarnessType::Codex));
        assert_eq!(
            parse_harness("claudecode").ok(),
            Some(HarnessType::ClaudeCode)
        );
        assert_eq!(parse_harness("hermes").ok(), Some(HarnessType::Hermes));
        assert!(caller_message(parse_harness("claude-code")).contains("unsupported harness"));
    }

    #[test]
    fn listed_harness_enum_matches_the_parser() {
        let tools = session_tools();
        let send = tools
            .iter()
            .find(|tool| tool["name"] == SESSION_SEND_TOOL)
            .unwrap();
        for harness in send["inputSchema"]["properties"]["harness"]["enum"]
            .as_array()
            .unwrap()
        {
            assert!(
                parse_harness(harness.as_str().unwrap()).is_ok(),
                "{harness}"
            );
        }
        for tool in &tools {
            assert!(session_tool_name(tool["name"].as_str().unwrap()));
        }
        assert!(!session_tool_name("centaur_tool_call"));
    }

    #[test]
    fn turn_input_line_matches_the_console_shape() {
        let key = session_thread_key(&principal("prn_ada"), "s1").unwrap();
        let line = turn_input_line(
            &key,
            &principal("prn_ada"),
            "msg-1",
            "list open PRs",
            Some("gpt-6-sol"),
        )
        .unwrap();
        let line = serde_json::from_str::<Value>(&line).unwrap();
        assert_eq!(line["type"], "user");
        assert_eq!(line["thread_key"], "mcp-session:prn_ada:s1");
        assert_eq!(line["client_user_message_id"], "msg-1");
        assert_eq!(line["model"], "gpt-6-sol");
        let content = line["message"]["content"].as_array().unwrap();
        assert_eq!(content.len(), 2);
        assert!(
            content[0]["text"]
                .as_str()
                .unwrap()
                .contains("Ada (ada@example.com)")
        );
        assert_eq!(content[1]["text"], "list open PRs");

        let without_model =
            turn_input_line(&key, &principal("prn_ada"), "msg-2", "hi", None).unwrap();
        assert!(
            serde_json::from_str::<Value>(&without_model)
                .unwrap()
                .get("model")
                .is_none()
        );
    }

    #[test]
    fn settings_of_an_existing_session_cannot_change() {
        let session = Session {
            thread_key: ThreadKey::parse("mcp-session:prn_ada:s1").unwrap(),
            title: None,
            sandbox_id: None,
            sandbox_capabilities: None,
            harness_type: HarnessType::Codex,
            harness_thread_id: None,
            persona_id: Some("eng".to_owned()),
            status: centaur_session_core::SessionStatus::Idle,
            iron_control_principal: Some("prn_ada".to_owned()),
            proxy_labels: Default::default(),
            sandbox_last_active_at: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: OffsetDateTime::UNIX_EPOCH,
        };
        assert!(ensure_session_settings_unchanged(&session, None, None).is_ok());
        assert!(
            ensure_session_settings_unchanged(&session, Some(&HarnessType::Codex), Some("eng"))
                .is_ok()
        );
        assert!(
            caller_message(ensure_session_settings_unchanged(
                &session,
                Some(&HarnessType::Hermes),
                None
            ))
            .contains("harness codex")
        );
        assert!(
            caller_message(ensure_session_settings_unchanged(
                &session,
                None,
                Some("ops")
            ))
            .contains("persona eng")
        );
    }

    #[test]
    fn progress_keeps_completed_items_and_drops_deltas() {
        let message = output_event(
            1,
            json!({"method": "item/completed", "params": {"item": {
                "type": "agentMessage", "id": "m1", "text": "Looking at the repo.", "phase": "commentary"
            }}}),
        );
        let delta = output_event(
            2,
            json!({"method": "item/agentMessage/delta", "params": {"delta": "Look"}}),
        );
        let reasoning = output_event(
            3,
            json!({"method": "item/completed", "params": {"item": {
                "type": "reasoning", "id": "r1", "summary": [], "content": []
            }}}),
        );
        let command = output_event(
            4,
            json!({"type": "item.completed", "item": {
                "type": "commandExecution", "id": "c1", "command": "gh pr list",
                "cwd": "/work", "status": "completed", "exitCode": 0,
                "aggregatedOutput": "x".repeat(COMMAND_OUTPUT_TAIL_CHARS + 10)
            }}),
        );

        let item = progress_item(&message).unwrap();
        assert_eq!(item["kind"], "message");
        assert_eq!(item["phase"], "commentary");
        assert_eq!(item["text"], "Looking at the repo.");
        assert_eq!(item["event_id"], 1);
        assert!(progress_item(&delta).is_none());
        assert!(progress_item(&reasoning).is_none());

        let item = progress_item(&command).unwrap();
        assert_eq!(item["kind"], "command");
        assert_eq!(item["command"], "gh pr list");
        assert_eq!(item["exit_code"], 0);
        assert_eq!(item["output_truncated"], true);
        assert_eq!(
            item["output_tail"].as_str().unwrap().chars().count(),
            COMMAND_OUTPUT_TAIL_CHARS + 1
        );
    }

    #[test]
    fn progress_summarizes_tools_files_and_errors() {
        let tool = output_event(
            1,
            json!({"method": "item/completed", "params": {"item": {
                "type": "mcpToolCall", "id": "t1", "server": "centaur", "tool": "slack",
                "status": "failed", "arguments": {}, "result": null,
                "error": {"message": "denied"}
            }}}),
        );
        let files = output_event(
            2,
            json!({"method": "item/completed", "params": {"item": {
                "type": "fileChange", "id": "f1", "status": "completed",
                "changes": [{"path": "src/a.rs", "diff": "..."}, {"path": "src/b.rs", "diff": "..."}]
            }}}),
        );
        let error = output_event(
            3,
            json!({"method": "error", "params": {"error": {"message": "rate limited"}, "willRetry": true}}),
        );
        let not_json = SessionEvent {
            payload: Value::String("plain text".to_owned()),
            ..output_event(4, json!({}))
        };

        let item = progress_item(&tool).unwrap();
        assert_eq!(item["kind"], "tool_call");
        assert_eq!(item["tool"], "slack");
        assert_eq!(item["error"], "denied");
        let item = progress_item(&files).unwrap();
        assert_eq!(item["paths"], json!(["src/a.rs", "src/b.rs"]));
        let item = progress_item(&error).unwrap();
        assert_eq!(item["kind"], "error");
        assert_eq!(item["message"], "rate limited");
        assert_eq!(item["will_retry"], true);
        assert!(progress_item(&not_json).is_none());
    }

    #[test]
    fn progress_reports_lifecycle_events_and_failures() {
        let event = |event_type: &str, payload: Value| SessionEvent {
            event_type: event_type.to_owned(),
            payload,
            ..output_event(9, json!({}))
        };
        assert_eq!(
            progress_item(&event("session.execution_started", json!({}))).unwrap()["kind"],
            "turn_started"
        );
        assert_eq!(
            progress_item(&event("session.steering_delivered", json!({}))).unwrap()["kind"],
            "prompt_steered"
        );
        let warning = progress_item(&event(
            "session.sandbox_resume_failed",
            json!({"error": "volume missing"}),
        ))
        .unwrap();
        assert_eq!(warning["kind"], "warning");
        assert_eq!(warning["event"], "session.sandbox_resume_failed");
        assert_eq!(warning["error"], "volume missing");
        assert!(progress_item(&event("session.sandbox_keepalive", json!({}))).is_none());
    }

    #[test]
    fn terminal_outcome_carries_answer_error_or_reason() {
        let execution = |status| SessionExecution {
            execution_id: "exe_1".to_owned(),
            idempotency_key: None,
            thread_key: ThreadKey::parse("mcp-session:prn_a:s1").unwrap(),
            status,
            metadata: json!({}),
            error: Some("row error".to_owned()),
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: OffsetDateTime::UNIX_EPOCH,
            started_at: None,
            completed_at: None,
        };
        let terminal = |payload: Value| SessionEvent {
            event_type: "session.execution_completed".to_owned(),
            payload,
            ..output_event(9, json!({}))
        };

        let mut result = json!({});
        apply_terminal_outcome(
            &mut result,
            &execution(ExecutionStatus::Completed),
            Some(&terminal(json!({"result_text": "3 PRs are open."}))),
        );
        assert_eq!(result["final_answer"], "3 PRs are open.");
        assert!(result.get("final_answer_truncated").is_none());

        let mut result = json!({});
        apply_terminal_outcome(&mut result, &execution(ExecutionStatus::Failed), None);
        assert_eq!(result["error"], "row error");

        let mut result = json!({});
        apply_terminal_outcome(
            &mut result,
            &execution(ExecutionStatus::Cancelled),
            Some(&terminal(json!({"reason": "turn_interrupted"}))),
        );
        assert_eq!(result["cancel_reason"], "turn_interrupted");
    }

    #[test]
    fn caller_actionable_runtime_errors_become_tool_errors() {
        let caller = |error: SessionRuntimeError| {
            matches!(SessionToolError::from(error), SessionToolError::Caller(_))
        };
        assert!(caller(SessionRuntimeError::BadRequest("bad".to_owned())));
        assert!(caller(SessionRuntimeError::ShuttingDown));
        assert!(caller(SessionRuntimeError::Store(
            SessionStoreError::NotFound {
                thread_key: "mcp-session:prn_a:s1".to_owned()
            }
        )));
        assert!(caller(SessionRuntimeError::Store(
            SessionStoreError::ExecutionNotFound {
                execution_id: "exe_1".to_owned()
            }
        )));
        assert!(!caller(SessionRuntimeError::Store(
            SessionStoreError::InvalidPersistedValue("corrupt".to_owned())
        )));
    }

    #[test]
    fn steering_counts_only_deliveries_of_these_messages() {
        let event = |event_type: &str, payload: Value| SessionEvent {
            event_type: event_type.to_owned(),
            payload,
            ..output_event(9, json!({}))
        };
        let ours = vec!["msg_new".to_owned()];
        assert!(steering_event_carries(
            &event(
                "session.steering_delivered",
                json!({"message_ids": ["msg_new"]})
            ),
            &ours
        ));
        // An earlier prompt's delivery does not cover this prompt.
        assert!(!steering_event_carries(
            &event(
                "session.steering_delivered",
                json!({"message_ids": ["msg_old"]})
            ),
            &ours
        ));
        assert!(!steering_event_carries(
            &event("session.steering_failed", json!({"error": "pipe closed"})),
            &ours
        ));
    }

    #[test]
    fn terminal_status_waits_briefly_for_its_event() {
        let finished_at = OffsetDateTime::UNIX_EPOCH + time::Duration::hours(1);
        let execution = SessionExecution {
            execution_id: "exe_1".to_owned(),
            idempotency_key: None,
            thread_key: ThreadKey::parse("mcp-session:prn_a:s1").unwrap(),
            status: ExecutionStatus::Completed,
            metadata: json!({}),
            error: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: OffsetDateTime::UNIX_EPOCH,
            started_at: None,
            completed_at: Some(finished_at),
        };
        assert!(!terminal_event_overdue(
            &execution,
            finished_at + time::Duration::seconds(3)
        ));
        assert!(terminal_event_overdue(
            &execution,
            finished_at + time::Duration::seconds(11)
        ));
        // Without completed_at, the last update stands in for it.
        let execution = SessionExecution {
            completed_at: None,
            ..execution
        };
        assert!(terminal_event_overdue(&execution, finished_at));
    }

    #[tokio::test]
    async fn send_locks_serialize_one_session_and_are_released() {
        let key = ThreadKey::parse(format!("mcp-session:prn_lock:{}", Uuid::new_v4())).unwrap();
        let first = send_lock(&key);
        let second = send_lock(&key);
        assert!(Arc::ptr_eq(&first, &second));
        let guard = first.lock().await;
        assert!(second.try_lock().is_err());
        drop(guard);

        drop(first);
        release_send_lock(&key);
        assert!(send_locks().lock().unwrap().contains_key(key.as_str()));
        drop(second);
        release_send_lock(&key);
        assert!(!send_locks().lock().unwrap().contains_key(key.as_str()));
    }

    #[test]
    fn text_limits_respect_character_boundaries() {
        assert_eq!(truncate_chars("héllo", 10), ("héllo".to_owned(), false));
        assert_eq!(truncate_chars("héllo", 2), ("hé…".to_owned(), true));
        assert_eq!(tail_chars("héllo", 3), ("…llo".to_owned(), true));
        assert_eq!(tail_chars("hé", 3), ("hé".to_owned(), false));
    }

    #[test]
    fn requester_context_forbids_posting_elsewhere() {
        let context = requester_context(&principal("prn_ada"));
        assert!(context.contains("Ada (ada@example.com)"));
        assert!(context.contains("do not post your reply anywhere"));
    }
}
