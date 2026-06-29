//! Anthropic Messages API ingress backed by Centaur sessions.
//!
//! `POST /v1/messages` accepts Anthropic-compatible requests and maps them to
//! the existing [`centaur_session_runtime::SessionRuntime`]. Thread continuity
//! follows the client's Claude Code session id: the `X-Claude-Code-Session-Id`
//! header (the same id Claude Code uses for `--resume`/`--continue`) keys the
//! thread `api:claude:<session-id>`, so a resumed CLI session continues the same
//! durable thread and warm sandbox with no extra plumbing. When the header is
//! absent a fresh `api:<uuid-v4>` thread is generated and validated through
//! [`centaur_session_core::ThreadKey`]. This endpoint defaults to
//! `HarnessType::ClaudeCode` because that wrapper emits Anthropic-shaped
//! content blocks. Request `model` selects the Claude Code model and request
//! `system` is appended as an extra system prompt after Centaur's internal
//! persona/AGENTS.md; neither replaces nor exposes Centaur's internal prompt.
//! Because the app-server sandbox is created once and reused per thread, model
//! and system are first-request-wins for a reused session. Codex
//! model/system handling is out of scope for this ingress. Request `tools`
//! remains accepted only for client compatibility and is decorative until tool
//! support lands separately. v1 appends only the trailing `role:"user"` message
//! from `messages[]`; full-history replay is a follow-up. The route is
//! unauthenticated and network-gated like `/api/session/*`; authentication is a
//! follow-up.

mod translate;

use std::{
    convert::Infallible,
    sync::{Arc, Mutex},
};

use axum::{
    Json,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{
        IntoResponse, Response, Sse,
        sse::{Event, KeepAlive},
    },
};
use centaur_session_core::{
    HarnessType, MessageRole, SessionEvent, SessionMessageInput, ThreadKey, empty_object,
};
use centaur_session_runtime::{ExecuteSessionInput, HarnessConflictPolicy};
use futures_util::{StreamExt, stream};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{ApiError, error::error_chain, routes::AppState};
use translate::{AnthropicTranslator, error_event};

/// Header Claude Code sends carrying its stable session id — the same id it uses
/// for `--resume`/`--continue`. The Centaur thread is keyed on it.
const CLAUDE_SESSION_HEADER: &str = "x-claude-code-session-id";

#[derive(Clone, Debug, Deserialize)]
pub struct AnthropicMessagesRequest {
    pub model: String,
    pub messages: Vec<AnthropicInputMessage>,
    #[serde(default)]
    pub system: Option<Value>,
    #[serde(default)]
    pub stream: bool,
    pub max_tokens: u32,
    #[serde(default)]
    pub metadata: Option<Value>,
    #[serde(default)]
    pub tools: Option<Value>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct AnthropicInputMessage {
    pub role: String,
    pub content: AnthropicInputContent,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum AnthropicInputContent {
    Text(String),
    Blocks(Vec<Value>),
}

#[derive(Clone, Debug, Serialize)]
struct AnthropicMessage {
    id: String,
    #[serde(rename = "type")]
    kind: &'static str,
    role: &'static str,
    model: String,
    content: Vec<Value>,
    stop_reason: Option<String>,
    stop_sequence: Option<String>,
    usage: AnthropicUsage,
}

#[derive(Clone, Debug, Serialize)]
struct AnthropicUsage {
    input_tokens: u32,
    output_tokens: u32,
}

#[derive(Debug)]
pub(crate) struct AnthropicHttpError {
    status: StatusCode,
    error_type: &'static str,
    message: String,
}

impl AnthropicHttpError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            error_type: "invalid_request_error",
            message: message.into(),
        }
    }

    fn internal(error: impl std::error::Error) -> Self {
        tracing::error!(
            error = %error_chain(&error),
            "Anthropic Messages request failed"
        );
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            error_type: "api_error",
            message: "internal server error".to_owned(),
        }
    }
}

impl From<ApiError> for AnthropicHttpError {
    fn from(error: ApiError) -> Self {
        let response = error.into_response();
        let status = response.status();
        if status.is_server_error() {
            return Self {
                status,
                error_type: "api_error",
                message: "internal server error".to_owned(),
            };
        }
        Self {
            status,
            error_type: "invalid_request_error",
            message: status
                .canonical_reason()
                .unwrap_or("request failed")
                .to_owned(),
        }
    }
}

impl IntoResponse for AnthropicHttpError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({
                "type": "error",
                "error": {
                    "type": self.error_type,
                    "message": self.message,
                },
            })),
        )
            .into_response()
    }
}

pub(crate) async fn anthropic_messages(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<AnthropicMessagesRequest>,
) -> Result<Response, AnthropicHttpError> {
    let thread_key = thread_key_from_headers(&headers)?;
    let model = non_empty_string(&request.model);
    let system_prompt = flatten_system_prompt(request.system.as_ref());
    let _decorative = (request.max_tokens, &request.tools);
    let runtime = state.runtime()?;
    let _outcome = runtime
        .create_or_get_session(
            &thread_key,
            &HarnessType::ClaudeCode,
            None,
            request.metadata.clone(),
            HarnessConflictPolicy::Reject,
        )
        .await
        .map_err(AnthropicHttpError::internal)?;

    let user_message = trailing_user_message(&request)?;
    let parts = input_parts(user_message);
    let session_message = SessionMessageInput {
        client_message_id: None,
        role: MessageRole::User,
        parts: parts.clone(),
        metadata: empty_object(),
    };
    runtime
        .append_messages(&thread_key, &[session_message])
        .await
        .map_err(AnthropicHttpError::internal)?;

    let input_line = serde_json::to_string(&json!({
        "type": "user",
        "message": {
            "role": "user",
            "content": parts,
        },
    }))
    .map_err(AnthropicHttpError::internal)?;
    let execution = runtime
        .execute_session(
            &thread_key,
            ExecuteSessionInput {
                idempotency_key: None,
                metadata: None,
                input_lines: vec![input_line],
                idle_timeout_ms: None,
                max_duration_ms: None,
                model,
                system_prompt,
            },
        )
        .await
        .map_err(AnthropicHttpError::internal)?;
    let events = runtime
        .stream_events(&thread_key, 0, Some(&execution.execution_id))
        .await
        .map_err(AnthropicHttpError::internal)?;

    let message_id = format!("msg_{}", Uuid::new_v4());
    if request.stream {
        let thread_key_for_log = thread_key.clone();
        let translator = Arc::new(Mutex::new(AnthropicTranslator::new(
            message_id,
            request.model,
        )));
        let events = Box::pin(events);
        let stream = stream::unfold(
            (events, false, translator, thread_key_for_log),
            |(mut events, done, translator, thread_key_for_log)| async move {
                if done {
                    return None;
                }
                let result = events.as_mut().next().await?;
                let thread_key = thread_key_for_log.clone();
                let (events_out, done) = match result {
                    Ok(event) => {
                        let done = is_terminal_session_event(&event);
                        let events = translator
                            .lock()
                            .expect("Anthropic translator mutex poisoned")
                            .translate_session_event(&event)
                            .into_iter()
                            .map(|event| Ok(event.into_sse_event()))
                            .collect::<Vec<Result<Event, Infallible>>>();
                        (events, done)
                    }
                    Err(error) => {
                        tracing::error!(
                            thread_key = %thread_key,
                            error = %error_chain(&error),
                            "session event stream failed"
                        );
                        (
                            vec![Ok(
                                error_event("api_error", "event stream failed").into_sse_event()
                            )],
                            true,
                        )
                    }
                };
                Some((events_out, (events, done, translator, thread_key_for_log)))
            },
        )
        .flat_map(stream::iter);
        return Ok(Sse::new(stream)
            .keep_alive(KeepAlive::default())
            .into_response());
    }

    let message = collect_non_streaming_message(events, message_id, request.model).await?;
    Ok(Json(message).into_response())
}

/// Derive the Centaur thread key from the client's Claude Code session id so a
/// resumed CLI session (`claude --resume`/`--continue`) continues the same
/// durable thread and reuses its warm sandbox. Falls back to a fresh
/// `api:<uuid>` thread when the header is absent (e.g. a raw SDK client).
fn thread_key_from_headers(headers: &HeaderMap) -> Result<ThreadKey, AnthropicHttpError> {
    let raw = match headers.get(CLAUDE_SESSION_HEADER) {
        Some(value) => {
            let session_id = value.to_str().map_err(|_| {
                AnthropicHttpError::bad_request("X-Claude-Code-Session-Id must be valid UTF-8")
            })?;
            format!("api:claude:{session_id}")
        }
        None => format!("api:{}", Uuid::new_v4()),
    };
    ThreadKey::parse(raw).map_err(|error| AnthropicHttpError::bad_request(error.to_string()))
}

fn trailing_user_message(
    request: &AnthropicMessagesRequest,
) -> Result<&AnthropicInputMessage, AnthropicHttpError> {
    if request.messages.is_empty() {
        return Err(AnthropicHttpError::bad_request(
            "messages must not be empty",
        ));
    }
    // Drive the turn off the LAST `user` message rather than the literal trailing
    // message: some Anthropic clients (e.g. Claude Code) append a trailing
    // `system`-role message in `messages[]` carrying dynamic context, so the
    // array can end on a non-user role. centaur owns conversation history via the
    // thread, so the latest user turn is the input.
    request
        .messages
        .iter()
        .rev()
        .find(|message| message.role == "user")
        .ok_or_else(|| AnthropicHttpError::bad_request("messages must contain a user message"))
}

fn input_parts(message: &AnthropicInputMessage) -> Vec<Value> {
    match &message.content {
        AnthropicInputContent::Text(text) => vec![json!({"type": "text", "text": text})],
        AnthropicInputContent::Blocks(blocks) => blocks.clone(),
    }
}

fn flatten_system_prompt(system: Option<&Value>) -> Option<String> {
    match system? {
        Value::String(text) => non_empty_string(text),
        Value::Array(blocks) => {
            let text = blocks
                .iter()
                .filter(|block| block.get("type").and_then(Value::as_str) == Some("text"))
                .filter_map(|block| block.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("\n");
            non_empty_string(&text)
        }
        _ => None,
    }
}

fn non_empty_string(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_owned())
}

async fn collect_non_streaming_message<S>(
    events: S,
    message_id: String,
    model: String,
) -> Result<AnthropicMessage, AnthropicHttpError>
where
    S: futures_util::Stream<
            Item = Result<
                centaur_session_core::SessionEvent,
                centaur_session_runtime::SessionRuntimeError,
            >,
        >,
{
    futures_util::pin_mut!(events);
    let mut translator = AnthropicTranslator::new(message_id.clone(), model.clone());
    let mut failure = None;
    while let Some(result) = events.next().await {
        let event = result.map_err(AnthropicHttpError::internal)?;
        if event.event_type == "session.execution_failed" {
            failure = Some(
                event
                    .payload
                    .get("error")
                    .and_then(Value::as_str)
                    .unwrap_or("execution failed")
                    .to_owned(),
            );
        }
        let _ = translator.translate_session_event(&event);
        if is_terminal_session_event(&event) {
            break;
        }
    }
    if let Some(message) = failure {
        return Err(AnthropicHttpError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            error_type: "api_error",
            message,
        });
    }

    let mut content = translator.content();
    if content.is_empty()
        && let Some(result_text) = translator.terminal_result_text()
        && !result_text.trim().is_empty()
    {
        content.push(json!({"type": "text", "text": result_text}));
    }
    Ok(AnthropicMessage {
        id: message_id,
        kind: "message",
        role: "assistant",
        model,
        content,
        stop_reason: Some("end_turn".to_owned()),
        stop_sequence: None,
        usage: AnthropicUsage {
            input_tokens: 0,
            output_tokens: 0,
        },
    })
}

fn is_terminal_session_event(event: &SessionEvent) -> bool {
    matches!(
        event.event_type.as_str(),
        "session.execution_completed" | "session.execution_failed"
    )
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderMap;

    use super::*;

    #[test]
    fn keys_thread_on_claude_session_id() {
        let mut headers = HeaderMap::new();
        headers.insert(CLAUDE_SESSION_HEADER, "sess-abc-123".parse().unwrap());
        let key = thread_key_from_headers(&headers).unwrap();
        // Stable + namespaced: resuming the same session id lands on this thread.
        assert_eq!(key.as_str(), "api:claude:sess-abc-123");
    }

    #[test]
    fn generates_fresh_thread_without_session_id() {
        let key = thread_key_from_headers(&HeaderMap::new()).unwrap();
        assert!(key.as_str().starts_with("api:"));
        assert!(!key.as_str().contains("claude"));
    }
}
