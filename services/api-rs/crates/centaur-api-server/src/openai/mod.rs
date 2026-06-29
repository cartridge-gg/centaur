//! OpenAI Responses API ingress backed by Centaur sessions.
//!
//! `POST /v1/responses` accepts OpenAI Responses-compatible requests (the wire
//! API the Codex CLI speaks) and maps them to the existing
//! [`centaur_session_runtime::SessionRuntime`]. Thread continuity follows
//! Codex's session id: the `session-id` header (equal to `prompt_cache_key`,
//! stable across `codex resume`) keys the thread `api:codex:<session-id>`, so a
//! resumed CLI session continues the same durable thread and warm sandbox. When
//! it is absent a fresh `api:<uuid-v4>` thread is generated and validated through
//! [`centaur_session_core::ThreadKey`]. The endpoint defaults to
//! `HarnessType::Codex` (the Codex wrapper speaks the same wire format the
//! client expects and uses the deployment's configured Codex model backend).
//! Centaur owns the harness, model, persona and tools, so request `model`,
//! `instructions` and `tools` are accepted and the `model` is echoed back, but
//! none are threaded into the harness — the harness uses its configured model.
//! Honoring a client-selected model/instructions is a follow-up. The streamed
//! output is a single assistant `message` item carrying `output_text`.
//! v1 drives the turn off the last `user` message in `input`; reasoning items,
//! client-side tool calls, full-history replay, usage accounting and auth are
//! follow-ups. The route is unauthenticated and network-gated like
//! `/api/session/*`.

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
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{ApiError, error::error_chain, routes::AppState};
use translate::{ResponsesTranslator, usage_object};

/// Header Codex sends carrying its stable session id (equal to
/// `prompt_cache_key` and `client_metadata.session_id`, stable across
/// `codex resume`). The Centaur thread is keyed on it.
const CODEX_SESSION_HEADER: &str = "session-id";

#[derive(Clone, Debug, Deserialize)]
pub struct OpenAIResponsesRequest {
    pub model: String,
    #[serde(default)]
    pub instructions: Option<String>,
    pub input: ResponsesInput,
    #[serde(default)]
    pub stream: bool,
    #[serde(default)]
    pub tools: Option<Value>,
    #[serde(default)]
    pub metadata: Option<Value>,
    /// Codex's stable per-conversation id; a body-side fallback for the
    /// `session-id` header when deriving the thread key.
    #[serde(default)]
    pub prompt_cache_key: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum ResponsesInput {
    Text(String),
    Items(Vec<Value>),
}

#[derive(Debug)]
pub(crate) struct ResponsesHttpError {
    status: StatusCode,
    error_type: &'static str,
    message: String,
}

impl ResponsesHttpError {
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
            "OpenAI Responses request failed"
        );
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            error_type: "api_error",
            message: "internal server error".to_owned(),
        }
    }
}

impl From<ApiError> for ResponsesHttpError {
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

impl IntoResponse for ResponsesHttpError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({
                "error": {
                    "message": self.message,
                    "type": self.error_type,
                    "code": Value::Null,
                },
            })),
        )
            .into_response()
    }
}

pub(crate) async fn create_response(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<OpenAIResponsesRequest>,
) -> Result<Response, ResponsesHttpError> {
    let thread_key = thread_key_from_request(&headers, &request)?;
    // Centaur owns the harness, model and persona. The client's `model` and
    // `instructions` are echoed back but NOT threaded into the harness: the
    // default ClaudeCode harness cannot run the gpt-* models a Codex client
    // requests, and the client instructions are the Codex CLI persona, not
    // Centaur's. Honoring them via a Codex harness mapping is a follow-up.
    let _decorative = (&request.model, &request.instructions, &request.tools);
    let runtime = state.runtime()?;
    let _outcome = runtime
        .create_or_get_session(
            &thread_key,
            &HarnessType::Codex,
            None,
            request.metadata.clone(),
            HarnessConflictPolicy::Reject,
        )
        .await
        .map_err(ResponsesHttpError::internal)?;

    let parts = last_user_input_parts(&request.input)?;
    let session_message = SessionMessageInput {
        client_message_id: None,
        role: MessageRole::User,
        parts: parts.clone(),
        metadata: empty_object(),
    };
    runtime
        .append_messages(&thread_key, &[session_message])
        .await
        .map_err(ResponsesHttpError::internal)?;

    let input_line = serde_json::to_string(&json!({
        "type": "user",
        "message": {
            "role": "user",
            "content": parts,
        },
    }))
    .map_err(ResponsesHttpError::internal)?;
    let execution = runtime
        .execute_session(
            &thread_key,
            ExecuteSessionInput {
                idempotency_key: None,
                metadata: None,
                input_lines: vec![input_line],
                idle_timeout_ms: None,
                max_duration_ms: None,
                model: None,
                system_prompt: None,
            },
        )
        .await
        .map_err(ResponsesHttpError::internal)?;
    let events = runtime
        .stream_events(&thread_key, 0, Some(&execution.execution_id))
        .await
        .map_err(ResponsesHttpError::internal)?;

    let response_id = format!("resp_{}", Uuid::new_v4());
    if request.stream {
        let thread_key_for_log = thread_key.clone();
        let translator = Arc::new(Mutex::new(ResponsesTranslator::new(
            response_id,
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
                            .expect("Responses translator mutex poisoned")
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
                            vec![Ok(translate::stream_error_event("event stream failed")
                                .into_sse_event())],
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

    let response = collect_non_streaming_response(events, response_id, request.model).await?;
    Ok(Json(response).into_response())
}

/// Derive the Centaur thread key from Codex's session id so a resumed CLI
/// session (`codex resume`) continues the same durable thread and reuses its
/// warm sandbox. Reads the `session-id` header, falling back to the body's
/// `prompt_cache_key`; both are the same id and stable across resume. Falls back
/// to a fresh `api:<uuid>` thread when neither is present.
fn thread_key_from_request(
    headers: &HeaderMap,
    request: &OpenAIResponsesRequest,
) -> Result<ThreadKey, ResponsesHttpError> {
    let header_session_id = match headers.get(CODEX_SESSION_HEADER) {
        Some(value) => Some(
            value
                .to_str()
                .map_err(|_| ResponsesHttpError::bad_request("session-id must be valid UTF-8"))?
                .to_owned(),
        ),
        None => None,
    };
    let session_id = header_session_id
        .or_else(|| request.prompt_cache_key.clone())
        .filter(|id| !id.trim().is_empty());
    let raw = match session_id {
        Some(id) => format!("api:codex:{id}"),
        None => format!("api:{}", Uuid::new_v4()),
    };
    ThreadKey::parse(raw).map_err(|error| ResponsesHttpError::bad_request(error.to_string()))
}

/// Convert the last `user` item in `input` into Anthropic-shaped text parts the
/// harness consumes. Accepts either a bare string or the Responses item list.
fn last_user_input_parts(input: &ResponsesInput) -> Result<Vec<Value>, ResponsesHttpError> {
    match input {
        ResponsesInput::Text(text) => {
            let text = text.trim();
            if text.is_empty() {
                return Err(ResponsesHttpError::bad_request("input must not be empty"));
            }
            Ok(vec![json!({"type": "text", "text": text})])
        }
        ResponsesInput::Items(items) => {
            let message = items
                .iter()
                .rev()
                .find(|item| {
                    item.get("role").and_then(Value::as_str) == Some("user")
                        && item
                            .get("type")
                            .and_then(Value::as_str)
                            .unwrap_or("message")
                            == "message"
                })
                .ok_or_else(|| {
                    ResponsesHttpError::bad_request("input must contain a user message")
                })?;
            let parts = message_text_parts(message);
            if parts.is_empty() {
                return Err(ResponsesHttpError::bad_request(
                    "the user message has no text content",
                ));
            }
            Ok(parts)
        }
    }
}

fn message_text_parts(message: &Value) -> Vec<Value> {
    match message.get("content") {
        Some(Value::String(text)) if !text.trim().is_empty() => {
            vec![json!({"type": "text", "text": text})]
        }
        Some(Value::Array(blocks)) => blocks
            .iter()
            .filter(|block| {
                matches!(
                    block.get("type").and_then(Value::as_str),
                    Some("input_text") | Some("text") | Some("output_text")
                )
            })
            .filter_map(|block| block.get("text").and_then(Value::as_str))
            .filter(|text| !text.is_empty())
            .map(|text| json!({"type": "text", "text": text}))
            .collect(),
        _ => Vec::new(),
    }
}

async fn collect_non_streaming_response<S>(
    events: S,
    response_id: String,
    model: String,
) -> Result<Value, ResponsesHttpError>
where
    S: futures_util::Stream<
            Item = Result<
                centaur_session_core::SessionEvent,
                centaur_session_runtime::SessionRuntimeError,
            >,
        >,
{
    futures_util::pin_mut!(events);
    let mut translator = ResponsesTranslator::new(response_id.clone(), model.clone());
    let mut failure = None;
    while let Some(result) = events.next().await {
        let event = result.map_err(ResponsesHttpError::internal)?;
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
        return Err(ResponsesHttpError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            error_type: "api_error",
            message,
        });
    }

    let text = translator.output_text();
    let output = if text.trim().is_empty() {
        Vec::new()
    } else {
        vec![json!({
            "id": format!("msg_{response_id}"),
            "type": "message",
            "status": "completed",
            "role": "assistant",
            "content": [{"type": "output_text", "text": text, "annotations": []}],
        })]
    };
    Ok(json!({
        "id": response_id,
        "object": "response",
        "status": "completed",
        "model": model,
        "output": output,
        "parallel_tool_calls": false,
        "tool_choice": "auto",
        "tools": [],
        "usage": usage_object(),
    }))
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

    fn req(prompt_cache_key: Option<&str>) -> OpenAIResponsesRequest {
        OpenAIResponsesRequest {
            model: "gpt-test".to_owned(),
            instructions: None,
            input: ResponsesInput::Text("hi".to_owned()),
            stream: false,
            tools: None,
            metadata: None,
            prompt_cache_key: prompt_cache_key.map(str::to_owned),
        }
    }

    #[test]
    fn keys_thread_on_codex_session_header() {
        let mut headers = HeaderMap::new();
        headers.insert(CODEX_SESSION_HEADER, "sess-xyz".parse().unwrap());
        let key = thread_key_from_request(&headers, &req(None)).unwrap();
        assert_eq!(key.as_str(), "api:codex:sess-xyz");
    }

    #[test]
    fn falls_back_to_prompt_cache_key() {
        let key = thread_key_from_request(&HeaderMap::new(), &req(Some("pck-1"))).unwrap();
        assert_eq!(key.as_str(), "api:codex:pck-1");
    }

    #[test]
    fn generates_fresh_thread_without_session_id() {
        let key = thread_key_from_request(&HeaderMap::new(), &req(None)).unwrap();
        assert!(key.as_str().starts_with("api:"));
        assert!(!key.as_str().contains("codex"));
    }
}
