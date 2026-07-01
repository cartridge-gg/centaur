//! Reverse-proxy for the sandbox agent's model calls, with optional injection
//! and routing of "local" tools.
//!
//! This module and `centaur_session_runtime::local_tool_bridge` share in-process
//! bridge state. api-rs must run as a single replica (or behind sticky routing);
//! otherwise the sandbox proxy request can land on one replica while the
//! `/v1/responses` oneshot resolver lands on another, timing out as a 504.
//!
//! Step 1 (transparent pass-through): `/sandbox/model/<rest>` ->
//! `CENTAUR_SANDBOX_MODEL_UPSTREAM`/<rest> (default hydra's `backend-api/codex`).
//! Replaces the incoming `Authorization` with the real `HYDRA_API_KEY` (the
//! sandbox only carries a placeholder), forwards method/body/headers, and
//! streams the (SSE) response straight back.
//!
//! Step 2 (this module): for the OpenAI Responses wire format (the body posted
//! to `.../responses`), when `CENTAUR_SANDBOX_LOCAL_TOOLS_STUB` is set we inject
//! "local" tools into the request and route their tool-calls through a buffering
//! re-query sub-loop, so the sandbox agent (codex) never sees those tools or
//! their calls. Everything else keeps the byte-for-byte transparent pass-through.

use std::{env, time::Duration};

use axum::{
    body::{Body, Bytes},
    extract::{Request, State},
    http::{HeaderMap, Method, StatusCode, header},
    response::{IntoResponse, Response},
};
use centaur_session_runtime::{PendingLocalCall, SessionRuntime};
use centaur_session_sqlx::{PgSessionStore, WarmSandboxAuthRecord};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::{
    SandboxModelAuthMode,
    routes::{AppState, MAX_V1_BODY_BYTES},
};

/// Shared client for the sandbox model-proxy.
static MODEL_PROXY_CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();

/// Prefix that tags local tool names so they're identifiable in the model's
/// function-call output and can be routed through the local sub-loop instead of
/// being surfaced to the sandbox agent.
const LOCAL_TOOL_PREFIX: &str = "local__";

/// Prefix tagging the sandbox agent's OWN tools, so the model sees a symmetric,
/// environment-labeled menu (`sandbox__*` vs `local__*`) and can choose where to
/// act. The proxy renames codex's native tools to this on the way out and strips
/// it from the returned `function_call`s so codex still sees its real tool names.
const SANDBOX_TOOL_PREFIX: &str = "sandbox__";

/// Whether a tool name already carries an environment prefix (so renames stay
/// idempotent across sub-loop iterations and codex re-queries).
fn is_env_prefixed(name: &str) -> bool {
    name.starts_with(LOCAL_TOOL_PREFIX) || name.starts_with(SANDBOX_TOOL_PREFIX)
}

/// Upper bound on local-tool re-query iterations before we bail with an error,
/// to avoid an unbounded loop if the model keeps calling local tools.
const MAX_SUBLOOP_ITERATIONS: usize = 16;

/// How long the proxy sub-loop waits for the user's local CLI to return a tool
/// result before giving up on a forwarded local call (the CLI must execute the
/// tool and post the `function_call_output` back through `/v1/responses`).
const LOCAL_CALL_TIMEOUT: Duration = Duration::from_secs(300);

/// A function call extracted from a Responses SSE stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionCall {
    pub name: String,
    pub call_id: String,
    /// The raw arguments JSON string (Responses transmits function-call
    /// arguments as a JSON-encoded string, streamed as deltas).
    pub arguments: String,
}

/// Reverse-proxy entrypoint for `/sandbox/model/<rest>`.
///
/// Routes to the transparent streaming pass-through (step 1) unless the request
/// targets the Responses `.../responses` endpoint AND
/// `CENTAUR_SANDBOX_LOCAL_TOOLS_STUB` is set, in which case it engages the
/// local-tool buffering sub-loop (step 2).
pub async fn proxy_sandbox_model(State(state): State<AppState>, req: Request) -> Response {
    let (parts, body) = req.into_parts();
    let path_and_query = parts
        .uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");
    let rest = path_and_query
        .strip_prefix("/sandbox/model")
        .unwrap_or(path_and_query);
    // A caller may embed the session's thread_key as a leading
    // `/s/{enc}` path segment. Strip it off so the
    // upstream path is byte-for-byte what it would be without the segment.
    let (session_thread_key, forward_path) = parse_session_path(rest);
    if let Some(thread_key) = &session_thread_key {
        tracing::debug!(thread_key = %thread_key, "sandbox model proxy: session-scoped request");
    }
    let auth_thread_key = match authorize_model_proxy_request(
        &state,
        &parts.headers,
        session_thread_key.as_deref(),
        "sandbox model proxy",
    )
    .await
    {
        Ok(thread_key) => thread_key,
        Err(response) => return response,
    };
    let upstream = env::var("CENTAUR_SANDBOX_MODEL_UPSTREAM")
        .unwrap_or_else(|_| "https://hydra.64.34.84.225.sslip.io/backend-api/codex".to_string());
    let url = format!("{}{}", upstream.trim_end_matches('/'), forward_path);

    let body_bytes = match axum::body::to_bytes(body, MAX_V1_BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(err) => {
            return (
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("model proxy: body exceeds {MAX_V1_BODY_BYTES} bytes: {err}"),
            )
                .into_response();
        }
    };

    // Only the Responses wire format on `.../responses` takes a buffering
    // sub-loop path; everything else is the transparent streaming pass-through
    // from step 1 (byte-for-byte unchanged).
    let is_responses = parts
        .uri
        .path()
        .trim_end_matches('/')
        .ends_with("/responses");
    let real_enabled = env_flag("CENTAUR_SANDBOX_LOCAL_TOOLS");
    let stub_enabled = env_flag("CENTAUR_SANDBOX_LOCAL_TOOLS_STUB");

    // Real local-tool routing (step 3) takes precedence over the step-2 stub.
    // In required auth mode the session thread_key comes from the warm sandbox
    // token. In rollback mode (`CENTAUR_SANDBOX_MODEL_AUTH=off`) we preserve the
    // legacy prompt_cache_key/session-path association.
    if is_responses
        && real_enabled
        && let Ok(runtime) = state.runtime()
        // Cheap gate: only pay body-parse + index lookup + race retry when some
        // session is actually running local tools. Otherwise fall straight through
        // to the streaming transparent pass-through below.
        && (session_thread_key.is_some() || runtime.any_local_tools_active())
    {
        let thread_key = match auth_thread_key {
            Some(thread_key) => {
                warn_if_prompt_cache_key_disagrees(&runtime, &body_bytes, &thread_key);
                Some(thread_key)
            }
            None => match session_thread_key.as_deref() {
                Some(tk) => Some(tk.to_owned()),
                None => resolve_thread_key_from_body(&runtime, &body_bytes).await,
            },
        };
        // Only engage the (buffering, non-streaming) local-tool sub-loop when this
        // session actually advertised local tools. With a generic proxy base_url
        // every sandbox model call lands here; normal sessions (no local CLI tools)
        // must keep the streaming transparent pass-through instead of being buffered.
        if let Some(thread_key) = thread_key
            && has_local_tools(&runtime, &thread_key)
        {
            return proxy_with_local_bridge(
                &runtime,
                &thread_key,
                &parts.method,
                &url,
                &parts.headers,
                body_bytes,
            )
            .await;
        }
    }

    if is_responses && stub_enabled {
        proxy_with_local_tools(&parts.method, &url, &parts.headers, body_bytes).await
    } else {
        transparent_passthrough(&parts.method, &url, &parts.headers, body_bytes).await
    }
}

/// Read a boolean-ish env flag: set and non-empty (after trim) => true.
fn env_flag(name: &str) -> bool {
    env::var(name)
        .map(|v| !v.trim().is_empty())
        .unwrap_or(false)
}

/// Split an optional leading `/s/{enc_thread_key}` session segment off the
/// proxy path (the remainder after `/sandbox/model` has been stripped).
///
/// Returns `(thread_key, upstream_path)`:
/// - `/s/{enc}/responses` -> `(Some(percent-decoded enc), "/responses")`
/// - anything else (e.g. `/responses`) -> `(None, <rest unchanged>)`
///
/// The thread_key is the percent-decoded first segment; the upstream path is
/// everything after that segment, so for non-session paths the forwarded path
/// is byte-for-byte identical to the input. Pure/testable.
fn parse_session_path(rest: &str) -> (Option<String>, String) {
    let Some(after) = rest.strip_prefix("/s/") else {
        return (None, rest.to_string());
    };
    let (segment, tail) = match after.find('/') {
        Some(idx) => (&after[..idx], &after[idx..]),
        None => (after, ""),
    };
    let decoded = urlencoding::decode(segment)
        .map(|s| s.into_owned())
        .unwrap_or_else(|_| segment.to_string());
    (Some(decoded), tail.to_string())
}

/// Number of times to re-check the harness-thread-id index before giving up. The
/// sandbox codex's first model call can narrowly beat the `thread.started` event
/// that populates the index; a short bounded retry absorbs that race without
/// stalling unrelated turns (the index is already populated for those).
const THREAD_KEY_LOOKUP_RETRIES: usize = 10;
const THREAD_KEY_LOOKUP_INTERVAL: Duration = Duration::from_millis(100);

/// Extract the `prompt_cache_key` from a Responses request body. codex sets it to
/// its thread_id (== the session's `harness_thread_id`), which the runtime indexes
/// to the Centaur thread_key. Returns `None` if the body isn't JSON or carries no
/// `prompt_cache_key`.
fn extract_prompt_cache_key(body: &[u8]) -> Option<String> {
    let value: Value = serde_json::from_slice(body).ok()?;
    value
        .get("prompt_cache_key")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .map(str::to_owned)
}

/// Whether the session's bridge advertises any local tools. Gates the buffering
/// sub-loop so only sessions with a local CLI (advertising tools) pay the
/// non-streaming cost; everything else keeps the streaming transparent path.
fn has_local_tools(runtime: &SessionRuntime, thread_key: &str) -> bool {
    matches!(
        runtime.bridge_local_tools(thread_key),
        Some(Value::Array(ref tools)) if !tools.is_empty()
    )
}

/// Resolve the Centaur thread_key for a generic (non session-scoped) sandbox model
/// call by reading its `prompt_cache_key` and reverse-mapping through the runtime,
/// briefly retrying to absorb the `thread.started` race. Returns `None` when the
/// body has no `prompt_cache_key` or no session has reported that id.
async fn resolve_thread_key_from_body(runtime: &SessionRuntime, body: &[u8]) -> Option<String> {
    let cache_key = extract_prompt_cache_key(body)?;
    for attempt in 0..THREAD_KEY_LOOKUP_RETRIES {
        if let Some(thread_key) = runtime.thread_key_for_harness_thread_id(&cache_key) {
            tracing::info!(
                prompt_cache_key = %cache_key,
                thread_key = %thread_key,
                attempt,
                "sandbox model proxy: resolved session from prompt_cache_key"
            );
            return Some(thread_key);
        }
        if attempt + 1 < THREAD_KEY_LOOKUP_RETRIES {
            tokio::time::sleep(THREAD_KEY_LOOKUP_INTERVAL).await;
        }
    }
    tracing::info!(
        prompt_cache_key = %cache_key,
        "sandbox model proxy: no session indexed for prompt_cache_key; passing through"
    );
    None
}

/// Build the upstream request: forward method/headers/body, drop hop-by-hop and
/// auth headers, and inject the real `HYDRA_API_KEY` as a Bearer token.
fn build_outbound(
    method: &Method,
    url: &str,
    headers: &HeaderMap,
    body: Vec<u8>,
) -> reqwest::RequestBuilder {
    let client = MODEL_PROXY_CLIENT.get_or_init(reqwest::Client::new);
    let mut outbound = client.request(method.clone(), url).body(body);
    for (name, value) in headers.iter() {
        match name.as_str() {
            "host" | "authorization" | "content-length" => continue,
            _ => outbound = outbound.header(name.clone(), value.clone()),
        }
    }
    if let Ok(key) = env::var("HYDRA_API_KEY") {
        let key = key.trim();
        if !key.is_empty() {
            outbound = outbound.header("authorization", format!("Bearer {key}"));
        }
    }
    outbound
}

/// Loop-free passthrough for codex model calls that reach api-rs because a
/// CoreDNS rewrite points the hydra host (`hydra.64.34.84.225.sslip.io`) at the
/// centaur svc. Such requests arrive on the hydra path (`/backend-api/...`).
/// Forward them to the REAL in-cluster hydra Service (`CENTAUR_HYDRA_REAL_UPSTREAM`,
/// a name NOT affected by the CoreDNS rewrite, so no loop), re-injecting
/// `HYDRA_API_KEY`. Gated by `CENTAUR_HYDRA_PATH_PROXY` — off => 404, so the
/// route is inert until the CoreDNS rewrite is flipped to activate it.
///
/// Building block for the egress-interception pivot (see
/// docs/local-sandbox-unification.md): because codex IGNORES config.toml's
/// base_url, the only way to route its model calls through Centaur is at the
/// network layer (CoreDNS). This handler is the Centaur-side landing for that.
/// It is transparent for now; session identification + local-tool bridge
/// routing on the hydra path are the remaining work.
pub(crate) async fn proxy_hydra_ingress(State(state): State<AppState>, req: Request) -> Response {
    let enabled = env::var("CENTAUR_HYDRA_PATH_PROXY")
        .map(|v| !v.trim().is_empty())
        .unwrap_or(false);
    if !enabled {
        return (StatusCode::NOT_FOUND, "hydra path proxy disabled").into_response();
    }
    let (parts, body) = req.into_parts();
    let path_and_query = parts
        .uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");
    let (session_thread_key, _) = parse_session_path(
        path_and_query
            .strip_prefix("/backend-api")
            .unwrap_or(path_and_query),
    );
    if let Err(response) = authorize_model_proxy_request(
        &state,
        &parts.headers,
        session_thread_key.as_deref(),
        "hydra path proxy",
    )
    .await
    {
        return response;
    }
    let upstream = env::var("CENTAUR_HYDRA_REAL_UPSTREAM")
        .unwrap_or_else(|_| "http://hydra.centaur".to_string());
    let url = format!("{}{}", upstream.trim_end_matches('/'), path_and_query);

    let body_bytes = match axum::body::to_bytes(body, MAX_V1_BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(err) => {
            return (
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("hydra proxy: body exceeds {MAX_V1_BODY_BYTES} bytes: {err}"),
            )
                .into_response();
        }
    };
    let outbound = build_outbound(&parts.method, &url, &parts.headers, body_bytes.to_vec());
    let upstream_resp = match outbound.send().await {
        Ok(resp) => resp,
        Err(err) => {
            tracing::error!(error = %err, url = %url, "hydra path proxy upstream error");
            return (
                StatusCode::BAD_GATEWAY,
                format!("hydra upstream error: {err}"),
            )
                .into_response();
        }
    };
    let status = upstream_resp.status();
    let mut builder = Response::builder().status(status.as_u16());
    for (name, value) in upstream_resp.headers().iter() {
        match name.as_str() {
            "content-length" | "transfer-encoding" | "connection" => continue,
            _ => builder = builder.header(name.clone(), value.clone()),
        }
    }
    match builder.body(Body::from_stream(upstream_resp.bytes_stream())) {
        Ok(resp) => resp,
        Err(err) => {
            tracing::error!(error = %err, "hydra path proxy response build failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "hydra proxy build failed",
            )
                .into_response()
        }
    }
}

async fn authorize_model_proxy_request(
    state: &AppState,
    headers: &HeaderMap,
    path_thread_key: Option<&str>,
    label: &'static str,
) -> Result<Option<String>, Response> {
    if state.config().sandbox_model_auth == SandboxModelAuthMode::Off {
        return Ok(None);
    }
    let Some(token) = extract_bearer_token(headers) else {
        return Err((
            StatusCode::UNAUTHORIZED,
            format!("{label}: missing bearer token"),
        )
            .into_response());
    };
    let token_hash = bearer_token_hash(token);
    let store = match state.pool() {
        Ok(pool) => PgSessionStore::new(pool),
        Err(error) => {
            tracing::error!(%error, "{label}: session store unavailable for token auth");
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                format!("{label}: token auth store unavailable"),
            )
                .into_response());
        }
    };
    let record = match store.find_warm_sandbox_by_token_hash(&token_hash).await {
        Ok(record) => record,
        Err(error) => {
            tracing::error!(%error, "{label}: token lookup failed");
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                format!("{label}: token lookup failed"),
            )
                .into_response());
        }
    };
    match model_proxy_auth_decision(true, record.as_ref(), path_thread_key) {
        Ok(thread_key) => Ok(Some(thread_key)),
        Err(ModelProxyAuthError::Unauthorized) => Err((
            StatusCode::UNAUTHORIZED,
            format!("{label}: invalid bearer token"),
        )
            .into_response()),
        Err(ModelProxyAuthError::Forbidden) => Err((
            StatusCode::FORBIDDEN,
            format!("{label}: token is not claimed for this session"),
        )
            .into_response()),
    }
}

fn extract_bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .trim()
        .strip_prefix("Bearer ")
        .map(str::trim)
        .filter(|token| !token.is_empty())
}

fn bearer_token_hash(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ModelProxyAuthError {
    Unauthorized,
    Forbidden,
}

fn model_proxy_auth_decision(
    token_present: bool,
    record: Option<&WarmSandboxAuthRecord>,
    path_thread_key: Option<&str>,
) -> Result<String, ModelProxyAuthError> {
    if !token_present {
        return Err(ModelProxyAuthError::Unauthorized);
    }
    let record = record.ok_or(ModelProxyAuthError::Unauthorized)?;
    let claimed_thread_key = record
        .claimed_thread_key
        .as_deref()
        .map(str::trim)
        .filter(|thread_key| !thread_key.is_empty())
        .ok_or(ModelProxyAuthError::Forbidden)?;
    if record.status != "claimed" {
        return Err(ModelProxyAuthError::Forbidden);
    }
    if let Some(path_thread_key) = path_thread_key
        && path_thread_key != claimed_thread_key
    {
        return Err(ModelProxyAuthError::Forbidden);
    }
    Ok(claimed_thread_key.to_owned())
}

fn warn_if_prompt_cache_key_disagrees(runtime: &SessionRuntime, body: &[u8], thread_key: &str) {
    let Some(cache_key) = extract_prompt_cache_key(body) else {
        return;
    };
    if let Some(indexed_thread_key) = runtime.thread_key_for_harness_thread_id(&cache_key)
        && indexed_thread_key != thread_key
    {
        tracing::warn!(
            prompt_cache_key = %cache_key,
            token_thread_key = %thread_key,
            indexed_thread_key = %indexed_thread_key,
            "sandbox model proxy: prompt_cache_key disagrees with token thread_key"
        );
    }
}

/// Step-1 behavior: stream the upstream (SSE) response straight back to the
/// caller, unchanged.
async fn transparent_passthrough(
    method: &Method,
    url: &str,
    headers: &HeaderMap,
    body: Bytes,
) -> Response {
    let outbound = build_outbound(method, url, headers, body.to_vec());
    let upstream_resp = match outbound.send().await {
        Ok(resp) => resp,
        Err(err) => {
            tracing::error!(error = %err, url = %url, "sandbox model proxy upstream error");
            return (
                StatusCode::BAD_GATEWAY,
                format!("model proxy upstream error: {err}"),
            )
                .into_response();
        }
    };

    let status = upstream_resp.status();
    let mut builder = Response::builder().status(status.as_u16());
    for (name, value) in upstream_resp.headers().iter() {
        match name.as_str() {
            "content-length" | "transfer-encoding" | "connection" => continue,
            _ => builder = builder.header(name.clone(), value.clone()),
        }
    }
    match builder.body(Body::from_stream(upstream_resp.bytes_stream())) {
        Ok(resp) => resp,
        Err(err) => {
            tracing::error!(error = %err, "sandbox model proxy response build failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "model proxy response build failed",
            )
                .into_response()
        }
    }
}

/// Step-2 behavior: inject local tools into the Responses request, buffer the
/// upstream SSE, and re-query with local tool results until the model stops
/// calling local tools (or a sandbox call / message is produced).
async fn proxy_with_local_tools(
    method: &Method,
    url: &str,
    headers: &HeaderMap,
    body_bytes: Bytes,
) -> Response {
    let mut request_body: Value = match serde_json::from_slice(&body_bytes) {
        Ok(value) => value,
        Err(err) => {
            // Body isn't JSON we can manipulate; never break a request we don't
            // understand — fall back to the transparent pass-through.
            tracing::warn!(error = %err, "sandbox model proxy: /responses body not JSON; passing through");
            return transparent_passthrough(method, url, headers, body_bytes).await;
        }
    };

    inject_local_tools(&mut request_body);

    for _ in 0..MAX_SUBLOOP_ITERATIONS {
        let serialized = match serde_json::to_vec(&request_body) {
            Ok(bytes) => bytes,
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("model proxy: serialize request: {err}"),
                )
                    .into_response();
            }
        };

        let outbound = build_outbound(method, url, headers, serialized);
        let upstream_resp = match outbound.send().await {
            Ok(resp) => resp,
            Err(err) => {
                tracing::error!(error = %err, url = %url, "sandbox model proxy upstream error");
                return (
                    StatusCode::BAD_GATEWAY,
                    format!("model proxy upstream error: {err}"),
                )
                    .into_response();
            }
        };

        // Capture status + forwardable headers before consuming the body.
        let status = upstream_resp.status();
        let resp_headers = upstream_resp.headers().clone();
        let collected = match upstream_resp.bytes().await {
            Ok(bytes) => bytes,
            Err(err) => {
                tracing::error!(error = %err, "sandbox model proxy: collect upstream body");
                return (
                    StatusCode::BAD_GATEWAY,
                    format!("model proxy collect error: {err}"),
                )
                    .into_response();
            }
        };

        let sse = String::from_utf8_lossy(&collected);
        let calls = parse_function_calls(&sse);
        let local_calls: Vec<&FunctionCall> = calls.iter().filter(|c| is_local(&c.name)).collect();

        if local_calls.is_empty() {
            // No local tool call — either a sandbox function_call or a plain
            // message. Re-emit the collected SSE bytes verbatim (same status +
            // headers/content-type).
            //
            // PARALLEL-CALL EDGE CASE (out of scope for v1): if a single turn
            // contains BOTH a local and a sandbox function call, we take the
            // sub-loop branch below (because a local call is present) and the
            // sandbox call in that turn is effectively dropped. v1 treats "any
            // local call present" as a sub-loop turn.
            let mut builder = Response::builder().status(status.as_u16());
            for (name, value) in resp_headers.iter() {
                match name.as_str() {
                    "content-length" | "transfer-encoding" | "connection" => continue,
                    _ => builder = builder.header(name.clone(), value.clone()),
                }
            }
            return match builder.body(Body::from(collected)) {
                Ok(resp) => resp,
                Err(err) => {
                    tracing::error!(error = %err, "sandbox model proxy response build failed");
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "model proxy response build failed",
                    )
                        .into_response()
                }
            };
        }

        // Execute each local call via the stub and append its function_call +
        // function_call_output to the request input, then re-query upstream.
        for call in local_calls {
            let output = execute_local_stub(call);
            append_followup(&mut request_body, call, &output);
        }
    }

    (
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("model proxy: local tool sub-loop exceeded {MAX_SUBLOOP_ITERATIONS} iterations"),
    )
        .into_response()
}

/// Step-3 behavior: inject the *client's* tools (renamed with the `local__`
/// prefix) into the Responses request, buffer the upstream SSE, and for each
/// `local__` call forward it to the user's local CLI through the session bridge,
/// blocking on its result before re-querying upstream. Mirrors
/// [`proxy_with_local_tools`] but routes to the real CLI instead of a stub.
async fn proxy_with_local_bridge(
    runtime: &SessionRuntime,
    thread_key: &str,
    method: &Method,
    url: &str,
    headers: &HeaderMap,
    body_bytes: Bytes,
) -> Response {
    let mut request_body: Value = match serde_json::from_slice(&body_bytes) {
        Ok(value) => value,
        Err(err) => {
            tracing::warn!(error = %err, "sandbox model proxy: /responses body not JSON; passing through");
            return transparent_passthrough(method, url, headers, body_bytes).await;
        }
    };

    // Symmetric environment namespacing: tag the sandbox agent's own tools
    // `sandbox__*` and inject the client's tools as `local__*`, then tell the model
    // (via `instructions`) how to choose. The model picks an environment per call;
    // `sandbox__*` calls are handed back to codex (prefix stripped) and `local__*`
    // calls are forwarded to the user's CLI. Done once; renames are idempotent.
    append_env_guidance(&mut request_body);
    rename_native_tools_to_sandbox(&mut request_body);
    let local_tools = runtime.bridge_local_tools(thread_key);
    let injected = local_tools
        .as_ref()
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or(0);
    tracing::info!(
        thread_key = %thread_key,
        injected_local_tools = injected,
        "sandbox model proxy: local-tool bridge engaged"
    );
    inject_bridge_local_tools(&mut request_body, local_tools.as_ref());

    for _ in 0..MAX_SUBLOOP_ITERATIONS {
        // Keep history tool names consistent with the renamed `tools[]` (codex's
        // prior calls -> `sandbox__*`; proxy-appended `local__*` left as-is).
        rename_input_history_to_sandbox(&mut request_body);
        let serialized = match serde_json::to_vec(&request_body) {
            Ok(bytes) => bytes,
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("model proxy: serialize request: {err}"),
                )
                    .into_response();
            }
        };

        let outbound = build_outbound(method, url, headers, serialized);
        let upstream_resp = match outbound.send().await {
            Ok(resp) => resp,
            Err(err) => {
                tracing::error!(error = %err, url = %url, "sandbox model proxy upstream error");
                return (
                    StatusCode::BAD_GATEWAY,
                    format!("model proxy upstream error: {err}"),
                )
                    .into_response();
            }
        };

        let status = upstream_resp.status();
        let resp_headers = upstream_resp.headers().clone();
        let collected = match upstream_resp.bytes().await {
            Ok(bytes) => bytes,
            Err(err) => {
                tracing::error!(error = %err, "sandbox model proxy: collect upstream body");
                return (
                    StatusCode::BAD_GATEWAY,
                    format!("model proxy collect error: {err}"),
                )
                    .into_response();
            }
        };

        let sse = String::from_utf8_lossy(&collected);
        let calls = parse_function_calls(&sse);
        let local_calls: Vec<&FunctionCall> = calls.iter().filter(|c| is_local(&c.name)).collect();

        if local_calls.is_empty() {
            // No local tool call — a `sandbox__*` call or a plain message. Strip the
            // `sandbox__` prefix from any function_call names so codex sees its real
            // tools, then hand the turn back (codex executes in-sandbox and drives
            // its own loop; we re-rename on its next model call).
            let rewritten = strip_sandbox_prefix_in_sse(&sse);
            let mut builder = Response::builder().status(status.as_u16());
            for (name, value) in resp_headers.iter() {
                match name.as_str() {
                    "content-length" | "transfer-encoding" | "connection" => continue,
                    _ => builder = builder.header(name.clone(), value.clone()),
                }
            }
            return match builder.body(Body::from(rewritten)) {
                Ok(resp) => resp,
                Err(err) => {
                    tracing::error!(error = %err, "sandbox model proxy response build failed");
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "model proxy response build failed",
                    )
                        .into_response()
                }
            };
        }

        // Phase-1 limitation: a single turn that mixes `local__*` and `sandbox__*`
        // calls only runs the local ones (the sandbox calls are dropped this turn).
        // The env guidance instructs the model to use one environment per turn.
        if calls.len() > local_calls.len() {
            tracing::warn!(
                thread_key = %thread_key,
                total = calls.len(),
                local = local_calls.len(),
                "sandbox model proxy: mixed local+sandbox calls in one turn; running local only"
            );
        }

        // Forward each local call to the user's CLI via the bridge and block on
        // its result, then append the followup and re-query upstream.
        for call in local_calls {
            let real_name = strip_local_prefix(&call.name).to_owned();
            tracing::info!(
                thread_key = %thread_key,
                call_id = %call.call_id,
                name = %real_name,
                "sandbox model proxy: forwarding local tool call to client"
            );
            let rx = runtime.bridge_forward_call(
                thread_key,
                PendingLocalCall {
                    call_id: call.call_id.clone(),
                    name: real_name,
                    arguments: strip_local_call_location(&call.arguments),
                },
            );
            let output = match tokio::time::timeout(LOCAL_CALL_TIMEOUT, rx).await {
                Ok(Ok(output)) => {
                    tracing::info!(
                        thread_key = %thread_key,
                        call_id = %call.call_id,
                        output_len = output.len(),
                        "sandbox model proxy: local tool result received"
                    );
                    output
                }
                Ok(Err(_)) => {
                    // Ingress dropped the sender without resolving (e.g. client
                    // disconnected). Surface a tool error to the model.
                    json!({ "error": "local tool channel closed before a result was returned" })
                        .to_string()
                }
                Err(_) => {
                    tracing::error!(
                        thread_key = %thread_key,
                        call_id = %call.call_id,
                        "sandbox model proxy: local tool call timed out"
                    );
                    return (
                        StatusCode::GATEWAY_TIMEOUT,
                        format!(
                            "model proxy: local tool '{}' timed out after {}s",
                            call.name,
                            LOCAL_CALL_TIMEOUT.as_secs()
                        ),
                    )
                        .into_response();
                }
            };
            append_followup(&mut request_body, call, &output);
        }
    }

    (
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("model proxy: local tool sub-loop exceeded {MAX_SUBLOOP_ITERATIONS} iterations"),
    )
        .into_response()
}

/// Returns true if `name` is tagged as a local tool (routed through the
/// sub-loop rather than surfaced to the sandbox agent).
pub fn is_local(name: &str) -> bool {
    name.starts_with(LOCAL_TOOL_PREFIX)
}

/// Strip the `local__` routing prefix from a tool name, yielding the real client
/// tool name. Names without the prefix are returned unchanged.
pub fn strip_local_prefix(name: &str) -> &str {
    name.strip_prefix(LOCAL_TOOL_PREFIX).unwrap_or(name)
}

/// Strip sandbox-specific location args (`workdir`, `cwd`) from a forwarded local
/// tool call's arguments. The sandbox agent fills these with sandbox paths (e.g.
/// `/home/agent/workspace`) that don't exist on the user's machine, which breaks
/// local exec; dropping them lets the local CLI use its own working directory.
/// Returns the input unchanged if it isn't a JSON object or has no such keys.
pub fn strip_local_call_location(arguments: &str) -> String {
    match serde_json::from_str::<Value>(arguments) {
        Ok(Value::Object(mut map)) => {
            let removed = map.remove("workdir").is_some() | map.remove("cwd").is_some();
            if removed {
                serde_json::to_string(&Value::Object(map)).unwrap_or_else(|_| arguments.to_owned())
            } else {
                arguments.to_owned()
            }
        }
        _ => arguments.to_owned(),
    }
}

/// Inject the client's advertised tools into the Responses request's `tools`
/// array, each renamed with the `local__` prefix so the sandbox model's calls to
/// them are routable back through the bridge. Only object entries with a string
/// `name` are renamed and forwarded (e.g. `exec_command` -> `local__exec_command`);
/// entries without a name (e.g. typed built-ins) are passed through unchanged so
/// we never drop a tool we don't understand. A `None`/empty tool set is a no-op.
pub fn inject_bridge_local_tools(body: &mut Value, local_tools: Option<&Value>) {
    let Some(Value::Array(tools)) = local_tools else {
        return;
    };
    if tools.is_empty() {
        return;
    }
    let renamed: Vec<Value> = tools
        .iter()
        .cloned()
        .map(|mut tool| {
            if let Some(name) = tool.get("name").and_then(Value::as_str) {
                let prefixed = format!("{LOCAL_TOOL_PREFIX}{name}");
                tool["name"] = Value::String(prefixed);
            }
            tool
        })
        .collect();

    match body.get_mut("tools") {
        Some(Value::Array(existing)) => existing.extend(renamed),
        _ => body["tools"] = Value::Array(renamed),
    }
}

/// Rename the sandbox agent's own (unprefixed) tools to `sandbox__<name>` so the
/// model sees a symmetric, environment-tagged menu next to the injected `local__`
/// tools. Idempotent: already-prefixed names and nameless typed builtins are left
/// untouched. Call this BEFORE injecting local tools (it only renames codex's).
pub fn rename_native_tools_to_sandbox(body: &mut Value) {
    let Some(Value::Array(tools)) = body.get_mut("tools") else {
        return;
    };
    for tool in tools.iter_mut() {
        if let Some(name) = tool.get("name").and_then(Value::as_str)
            && !is_env_prefixed(name)
        {
            tool["name"] = Value::String(format!("{SANDBOX_TOOL_PREFIX}{name}"));
        }
    }
}

/// Rename unprefixed `function_call` items in `input[]` (the sandbox agent's prior
/// calls) to `sandbox__<name>`, keeping the conversation history's tool names
/// consistent with the renamed `tools[]`. Proxy-appended `local__` calls already
/// carry their prefix and are left as-is. Idempotent.
pub fn rename_input_history_to_sandbox(body: &mut Value) {
    let Some(Value::Array(input)) = body.get_mut("input") else {
        return;
    };
    for item in input.iter_mut() {
        if item.get("type").and_then(Value::as_str) == Some("function_call")
            && let Some(name) = item.get("name").and_then(Value::as_str)
            && !is_env_prefixed(name)
        {
            item["name"] = Value::String(format!("{SANDBOX_TOOL_PREFIX}{name}"));
        }
    }
}

/// Marker that makes [`append_env_guidance`] idempotent.
const ENV_GUIDANCE_MARKER: &str = "# Tool environments";

/// Environment-selection guidance appended to the request `instructions` so the
/// model knows what `sandbox__*` vs `local__*` mean and how to pick. Phase 1:
/// one environment per turn (cover "both" by sequencing across turns).
const ENV_GUIDANCE: &str = "\n\n# Tool environments\nTools are namespaced by where they run:\n- `sandbox__*` — an ephemeral cloud sandbox (its own files, the deployment's credentials and kubeconfig, cluster network).\n- `local__*` — the user's local machine (their working files, local credentials, and network).\nThe local machine is a SEPARATE computer — often a different OS (e.g. macOS) with its own filesystem, working directory, and installed tools. For `local__*` calls do NOT reuse sandbox paths or a workdir/cwd from this sandbox, and don't assume Linux-only commands exist — prefer portable commands (use macOS forms like sysctl / sw_vers / vm_stat when probing a Mac).\nChoose the environment that matches the task: cluster/deploy/CI or sandbox files → `sandbox__*`; the user's local files, processes, or machine → `local__*`. If the user names an environment, use only that one. For read-only or diagnostic requests that aren't specific to one side (machine specs, OS, tool versions, env vars), gather from BOTH and report each side.\nDo NOT call `sandbox__*` and `local__*` tools in the same step — gather one environment, then gather the other in a following step. You may take as many steps as you need: keep going until you have everything the request needs from EVERY relevant environment, then give your final answer. Never stop and defer remaining environments to a future user message — complete them yourself in this same response.";

/// Append the environment-selection guidance to the Responses request
/// `instructions` (creating it if absent). Idempotent via [`ENV_GUIDANCE_MARKER`].
pub fn append_env_guidance(body: &mut Value) {
    let current = body
        .get("instructions")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if current.contains(ENV_GUIDANCE_MARKER) {
        return;
    }
    body["instructions"] = Value::String(format!("{current}{ENV_GUIDANCE}"));
}

/// Strip the `sandbox__` routing prefix from every `function_call` item name in a
/// Responses SSE stream, so when the proxy hands a sandbox-only turn back to codex
/// it sees its real tool names. Each `data:` line is parsed as JSON and rewritten
/// only if it changed; non-JSON / unaffected lines pass through verbatim.
pub fn strip_sandbox_prefix_in_sse(sse: &str) -> String {
    let mut lines: Vec<String> = Vec::new();
    for line in sse.split('\n') {
        if let Some(rest) = line.trim_start().strip_prefix("data:") {
            let data = rest.trim();
            if !data.is_empty()
                && data != "[DONE]"
                && let Ok(mut value) = serde_json::from_str::<Value>(data)
                && strip_sandbox_names(&mut value)
            {
                lines.push(format!("data: {value}"));
                continue;
            }
        }
        lines.push(line.to_string());
    }
    lines.join("\n")
}

/// Recursively strip the `sandbox__` prefix from the `name` of every
/// `function_call` object in a JSON value. Returns whether anything changed.
fn strip_sandbox_names(value: &mut Value) -> bool {
    let mut changed = false;
    match value {
        Value::Object(map) => {
            if map.get("type").and_then(Value::as_str) == Some("function_call")
                && let Some(Value::String(name)) = map.get_mut("name")
                && let Some(stripped) = name.strip_prefix(SANDBOX_TOOL_PREFIX)
            {
                *name = stripped.to_owned();
                changed = true;
            }
            for child in map.values_mut() {
                changed |= strip_sandbox_names(child);
            }
        }
        Value::Array(items) => {
            for child in items.iter_mut() {
                changed |= strip_sandbox_names(child);
            }
        }
        _ => {}
    }
    changed
}

/// Inject the fixed stub set of local tools into the Responses request's
/// `tools` array, creating the array if it's missing or not an array.
///
/// For step 2 the local tool set is a single Responses function tool,
/// `local__echo`.
pub fn inject_local_tools(body: &mut Value) {
    let echo = json!({
        "type": "function",
        "name": "local__echo",
        "description": "Echo back the provided text. Local stub tool (step-2 placeholder).",
        "parameters": {
            "type": "object",
            "properties": {
                "text": { "type": "string" }
            },
            "required": ["text"]
        }
    });

    match body.get_mut("tools") {
        Some(Value::Array(tools)) => tools.push(echo),
        _ => {
            body["tools"] = Value::Array(vec![echo]);
        }
    }
}

/// Parse a Responses SSE stream for function-call output items.
///
/// A function call appears as `response.output_item.added` with
/// `item.type == "function_call"` (carrying `name` and `call_id`), followed by
/// `response.function_call_arguments.delta` events whose `delta`s concatenate
/// into the arguments JSON string, and a terminal `response.output_item.done`.
/// Calls are associated by `output_index`, preserving stream order.
pub fn parse_function_calls(sse: &str) -> Vec<FunctionCall> {
    use std::collections::HashMap;

    struct Pending {
        order: usize,
        name: String,
        call_id: String,
        arguments: String,
    }

    // Keyed by output_index; ordered by first-seen via `order`.
    let mut pending: HashMap<i64, Pending> = HashMap::new();
    let mut order: usize = 0;
    let mut finished: Vec<(usize, FunctionCall)> = Vec::new();

    for line in sse.lines() {
        let data = match line.trim_start().strip_prefix("data:") {
            Some(data) => data.trim(),
            None => continue,
        };
        if data.is_empty() || data == "[DONE]" {
            continue;
        }
        let event: Value = match serde_json::from_str(data) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let event_type = event
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let output_index = event
            .get("output_index")
            .and_then(Value::as_i64)
            .unwrap_or(-1);

        match event_type {
            "response.output_item.added" => {
                let item = match event.get("item") {
                    Some(item) => item,
                    None => continue,
                };
                if item.get("type").and_then(Value::as_str) != Some("function_call") {
                    continue;
                }
                let name = item.get("name").and_then(Value::as_str).unwrap_or_default();
                let call_id = item
                    .get("call_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let arguments = item
                    .get("arguments")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                pending.insert(
                    output_index,
                    Pending {
                        order,
                        name: name.to_string(),
                        call_id: call_id.to_string(),
                        arguments: arguments.to_string(),
                    },
                );
                order += 1;
            }
            "response.function_call_arguments.delta" => {
                if let Some(entry) = pending.get_mut(&output_index)
                    && let Some(delta) = event.get("delta").and_then(Value::as_str)
                {
                    entry.arguments.push_str(delta);
                }
            }
            "response.output_item.done" => {
                if let Some(mut entry) = pending.remove(&output_index) {
                    // If no deltas were observed, fall back to the final
                    // `arguments` carried on the done item.
                    if entry.arguments.is_empty()
                        && let Some(args) = event
                            .get("item")
                            .and_then(|item| item.get("arguments"))
                            .and_then(Value::as_str)
                    {
                        entry.arguments = args.to_string();
                    }
                    finished.push((
                        entry.order,
                        FunctionCall {
                            name: entry.name,
                            call_id: entry.call_id,
                            arguments: entry.arguments,
                        },
                    ));
                }
            }
            _ => {}
        }
    }

    // Include any calls that never received an explicit done event.
    for (_, entry) in pending {
        finished.push((
            entry.order,
            FunctionCall {
                name: entry.name,
                call_id: entry.call_id,
                arguments: entry.arguments,
            },
        ));
    }

    finished.sort_by_key(|(order, _)| *order);
    finished.into_iter().map(|(_, call)| call).collect()
}

/// Execute a local tool via a stub (placeholder for the real local-CLI
/// forwarding in step 3). Returns the tool output as a string suitable for a
/// `function_call_output` item.
pub fn execute_local_stub(call: &FunctionCall) -> String {
    match call.name.as_str() {
        "local__echo" => {
            let text = serde_json::from_str::<Value>(&call.arguments)
                .ok()
                .and_then(|args| args.get("text").and_then(Value::as_str).map(str::to_string))
                .unwrap_or_default();
            json!({ "echo": text }).to_string()
        }
        other => json!({ "error": format!("unknown local tool: {other}") }).to_string(),
    }
}

/// Append the model's function_call and its function_call_output to the
/// Responses request's `input` array, creating it if it's missing or not an
/// array. Arguments are kept as the raw JSON string emitted by the model.
pub fn append_followup(body: &mut Value, call: &FunctionCall, output: &str) {
    let function_call = json!({
        "type": "function_call",
        "call_id": call.call_id,
        "name": call.name,
        "arguments": call.arguments,
    });
    let function_call_output = json!({
        "type": "function_call_output",
        "call_id": call.call_id,
        "output": output,
    });

    match body.get_mut("input") {
        Some(Value::Array(input)) => {
            input.push(function_call);
            input.push(function_call_output);
        }
        _ => {
            body["input"] = Value::Array(vec![function_call, function_call_output]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_session_path_extracts_decoded_thread_key_and_upstream_path() {
        let (tk, path) = parse_session_path("/s/api%3Acodex%3Aabc-123/responses");
        assert_eq!(tk.as_deref(), Some("api:codex:abc-123"));
        assert_eq!(path, "/responses");
    }

    #[test]
    fn parse_session_path_passes_through_non_session_paths() {
        let (tk, path) = parse_session_path("/responses");
        assert_eq!(tk, None);
        assert_eq!(path, "/responses");
    }

    #[test]
    fn parse_session_path_handles_segment_without_trailing_path() {
        let (tk, path) = parse_session_path("/s/api%3Acodex%3Ax");
        assert_eq!(tk.as_deref(), Some("api:codex:x"));
        assert_eq!(path, "");
    }

    #[test]
    fn parse_session_path_preserves_query_string() {
        let (tk, path) = parse_session_path("/s/k/responses?stream=true");
        assert_eq!(tk.as_deref(), Some("k"));
        assert_eq!(path, "/responses?stream=true");
    }

    #[test]
    fn extract_prompt_cache_key_reads_string_value() {
        let body = br#"{"model":"gpt-5.5","prompt_cache_key":"codex-thread-abc","input":[]}"#;
        assert_eq!(
            extract_prompt_cache_key(body).as_deref(),
            Some("codex-thread-abc")
        );
    }

    #[test]
    fn extract_prompt_cache_key_none_when_absent_or_blank() {
        assert_eq!(extract_prompt_cache_key(br#"{"model":"gpt-5.5"}"#), None);
        assert_eq!(
            extract_prompt_cache_key(br#"{"prompt_cache_key":"  "}"#),
            None
        );
        // Non-JSON body never panics.
        assert_eq!(extract_prompt_cache_key(b"not json"), None);
    }

    #[test]
    fn model_proxy_auth_decision_rejects_missing_or_unknown_token() {
        assert_eq!(
            model_proxy_auth_decision(false, None, None),
            Err(ModelProxyAuthError::Unauthorized)
        );
        assert_eq!(
            model_proxy_auth_decision(true, None, None),
            Err(ModelProxyAuthError::Unauthorized)
        );
    }

    #[test]
    fn model_proxy_auth_decision_requires_claimed_warm_sandbox() {
        let ready = WarmSandboxAuthRecord {
            sandbox_id: "sbx".to_owned(),
            status: "ready".to_owned(),
            claimed_thread_key: Some("thread-a".to_owned()),
        };
        assert_eq!(
            model_proxy_auth_decision(true, Some(&ready), None),
            Err(ModelProxyAuthError::Forbidden)
        );

        let missing_thread = WarmSandboxAuthRecord {
            status: "claimed".to_owned(),
            claimed_thread_key: None,
            ..ready
        };
        assert_eq!(
            model_proxy_auth_decision(true, Some(&missing_thread), None),
            Err(ModelProxyAuthError::Forbidden)
        );
    }

    #[test]
    fn model_proxy_auth_decision_uses_claimed_thread_and_checks_path_thread() {
        let record = WarmSandboxAuthRecord {
            sandbox_id: "sbx".to_owned(),
            status: "claimed".to_owned(),
            claimed_thread_key: Some("thread-a".to_owned()),
        };
        assert_eq!(
            model_proxy_auth_decision(true, Some(&record), None).as_deref(),
            Ok("thread-a")
        );
        assert_eq!(
            model_proxy_auth_decision(true, Some(&record), Some("thread-a")).as_deref(),
            Ok("thread-a")
        );
        assert_eq!(
            model_proxy_auth_decision(true, Some(&record), Some("thread-b")),
            Err(ModelProxyAuthError::Forbidden)
        );
    }

    #[test]
    fn bearer_token_hash_is_sha256_hex() {
        assert_eq!(
            bearer_token_hash("token"),
            "3c469e9d6c5875d37a43f353d4f88e61fcf812c66eee3457465a40b0da4153e0"
        );
    }

    #[test]
    fn inject_local_tools_creates_array_when_absent() {
        let mut body = json!({ "model": "codex" });
        inject_local_tools(&mut body);

        let tools = body["tools"].as_array().expect("tools array created");
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["type"], "function");
        assert_eq!(tools[0]["name"], "local__echo");
        assert_eq!(tools[0]["parameters"]["required"], json!(["text"]));
        assert_eq!(
            tools[0]["parameters"]["properties"]["text"]["type"],
            "string"
        );
    }

    #[test]
    fn inject_local_tools_appends_to_existing_array() {
        let mut body = json!({
            "tools": [{ "type": "function", "name": "sandbox__shell" }]
        });
        inject_local_tools(&mut body);

        let tools = body["tools"].as_array().expect("tools array");
        assert_eq!(tools.len(), 2);
        assert_eq!(tools[0]["name"], "sandbox__shell");
        assert_eq!(tools[1]["name"], "local__echo");
    }

    #[test]
    fn parse_function_calls_extracts_name_call_id_and_concatenated_arguments() {
        let sse = concat!(
            "event: response.output_item.added\n",
            "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"id\":\"fc_1\",\"name\":\"local__echo\",\"call_id\":\"call_abc\",\"arguments\":\"\"}}\n",
            "\n",
            "event: response.function_call_arguments.delta\n",
            "data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"delta\":\"{\\\"text\\\":\"}\n",
            "\n",
            "event: response.function_call_arguments.delta\n",
            "data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"delta\":\"\\\"hi\\\"}\"}\n",
            "\n",
            "event: response.output_item.done\n",
            "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"id\":\"fc_1\",\"name\":\"local__echo\",\"call_id\":\"call_abc\",\"arguments\":\"{\\\"text\\\":\\\"hi\\\"}\"}}\n",
            "\n",
        );

        let calls = parse_function_calls(sse);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "local__echo");
        assert_eq!(calls[0].call_id, "call_abc");
        assert_eq!(calls[0].arguments, r#"{"text":"hi"}"#);
    }

    #[test]
    fn parse_function_calls_handles_multiple_calls_in_order() {
        let sse = concat!(
            "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"name\":\"local__echo\",\"call_id\":\"call_0\",\"arguments\":\"\"}}\n",
            "data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"delta\":\"{}\"}\n",
            "data: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"name\":\"sandbox__shell\",\"call_id\":\"call_1\",\"arguments\":\"\"}}\n",
            "data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":1,\"delta\":\"{}\"}\n",
            "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{}}\n",
            "data: {\"type\":\"response.output_item.done\",\"output_index\":1,\"item\":{}}\n",
        );

        let calls = parse_function_calls(sse);
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].call_id, "call_0");
        assert_eq!(calls[1].call_id, "call_1");
    }

    #[test]
    fn is_local_detects_prefix() {
        assert!(is_local("local__echo"));
        assert!(is_local("local__anything"));
        assert!(!is_local("sandbox__shell"));
        assert!(!is_local("echo"));
        assert!(!is_local(""));
    }

    #[test]
    fn execute_local_stub_echoes_text() {
        let call = FunctionCall {
            name: "local__echo".to_string(),
            call_id: "call_abc".to_string(),
            arguments: r#"{"text":"hello world"}"#.to_string(),
        };
        assert_eq!(execute_local_stub(&call), r#"{"echo":"hello world"}"#);
    }

    #[test]
    fn execute_local_stub_unknown_tool_reports_error() {
        let call = FunctionCall {
            name: "local__nope".to_string(),
            call_id: "call_x".to_string(),
            arguments: "{}".to_string(),
        };
        let out: Value = serde_json::from_str(&execute_local_stub(&call)).unwrap();
        assert!(out["error"].as_str().unwrap().contains("local__nope"));
    }

    #[test]
    fn append_followup_appends_two_input_items() {
        let mut body = json!({ "input": [{ "type": "message", "role": "user", "content": "hi" }] });
        let call = FunctionCall {
            name: "local__echo".to_string(),
            call_id: "call_abc".to_string(),
            arguments: r#"{"text":"hi"}"#.to_string(),
        };
        append_followup(&mut body, &call, r#"{"echo":"hi"}"#);

        let input = body["input"].as_array().expect("input array");
        assert_eq!(input.len(), 3);

        let fc = &input[1];
        assert_eq!(fc["type"], "function_call");
        assert_eq!(fc["call_id"], "call_abc");
        assert_eq!(fc["name"], "local__echo");
        assert_eq!(fc["arguments"], r#"{"text":"hi"}"#);

        let fco = &input[2];
        assert_eq!(fco["type"], "function_call_output");
        assert_eq!(fco["call_id"], "call_abc");
        assert_eq!(fco["output"], r#"{"echo":"hi"}"#);
    }

    #[test]
    fn strip_local_call_location_removes_sandbox_paths() {
        // workdir + cwd removed, other args preserved.
        let out = strip_local_call_location(
            r#"{"command":["hostname"],"workdir":"/home/agent/workspace","cwd":"/sbx"}"#,
        );
        let v: Value = serde_json::from_str(&out).unwrap();
        assert!(v.get("workdir").is_none());
        assert!(v.get("cwd").is_none());
        assert_eq!(v["command"], json!(["hostname"]));
        // No location keys → unchanged.
        assert_eq!(
            strip_local_call_location(r#"{"command":["ls"]}"#),
            r#"{"command":["ls"]}"#
        );
        // Non-JSON → unchanged (never panics).
        assert_eq!(strip_local_call_location("not json"), "not json");
    }

    #[test]
    fn strip_local_prefix_strips_only_the_prefix() {
        assert_eq!(strip_local_prefix("local__exec_command"), "exec_command");
        assert_eq!(strip_local_prefix("local__write_stdin"), "write_stdin");
        // Idempotent / unprefixed names pass through unchanged.
        assert_eq!(strip_local_prefix("exec_command"), "exec_command");
        assert_eq!(strip_local_prefix(""), "");
    }

    #[test]
    fn inject_bridge_local_tools_prefixes_named_tools() {
        let mut body = json!({ "model": "codex", "input": [] });
        let local = json!([
            { "type": "function", "name": "exec_command", "parameters": {} },
            { "type": "function", "name": "write_stdin", "parameters": {} },
            // No name (typed built-in) -> passed through unchanged.
            { "type": "web_search" },
        ]);
        inject_bridge_local_tools(&mut body, Some(&local));

        let tools = body["tools"].as_array().expect("tools array created");
        assert_eq!(tools.len(), 3);
        assert_eq!(tools[0]["name"], "local__exec_command");
        assert_eq!(tools[1]["name"], "local__write_stdin");
        assert_eq!(tools[2]["type"], "web_search");
        assert!(tools[2].get("name").is_none());
    }

    #[test]
    fn inject_bridge_local_tools_appends_to_existing_array() {
        let mut body = json!({ "tools": [{ "type": "function", "name": "sandbox__shell" }] });
        let local = json!([{ "type": "function", "name": "exec_command" }]);
        inject_bridge_local_tools(&mut body, Some(&local));

        let tools = body["tools"].as_array().expect("tools array");
        assert_eq!(tools.len(), 2);
        assert_eq!(tools[0]["name"], "sandbox__shell");
        assert_eq!(tools[1]["name"], "local__exec_command");
    }

    #[test]
    fn inject_bridge_local_tools_noop_when_empty_or_absent() {
        let mut body = json!({ "model": "codex" });
        inject_bridge_local_tools(&mut body, None);
        assert!(body.get("tools").is_none());

        inject_bridge_local_tools(&mut body, Some(&json!([])));
        assert!(body.get("tools").is_none());
    }

    #[test]
    fn rename_native_tools_prefixes_unprefixed_and_is_idempotent() {
        let mut body = json!({
            "tools": [
                { "type": "function", "name": "exec_command" },
                { "type": "function", "name": "local__echo" }, // already local — skip
                { "type": "web_search" },                       // nameless — skip
            ]
        });
        rename_native_tools_to_sandbox(&mut body);
        let tools = body["tools"].as_array().unwrap();
        assert_eq!(tools[0]["name"], "sandbox__exec_command");
        assert_eq!(tools[1]["name"], "local__echo");
        assert!(tools[2].get("name").is_none());
        // Idempotent: a second pass does not double-prefix.
        rename_native_tools_to_sandbox(&mut body);
        assert_eq!(body["tools"][0]["name"], "sandbox__exec_command");
    }

    #[test]
    fn rename_input_history_prefixes_only_unprefixed_function_calls() {
        let mut body = json!({
            "input": [
                { "type": "message", "role": "user", "content": "hi" },
                { "type": "function_call", "name": "exec_command", "call_id": "c1", "arguments": "{}" },
                { "type": "function_call", "name": "local__echo", "call_id": "c2", "arguments": "{}" },
            ]
        });
        rename_input_history_to_sandbox(&mut body);
        let input = body["input"].as_array().unwrap();
        assert_eq!(input[0]["type"], "message"); // untouched
        assert_eq!(input[1]["name"], "sandbox__exec_command");
        assert_eq!(input[2]["name"], "local__echo"); // local left as-is
    }

    #[test]
    fn append_env_guidance_adds_once_and_preserves_existing() {
        let mut body = json!({ "instructions": "Base system prompt." });
        append_env_guidance(&mut body);
        let instr = body["instructions"].as_str().unwrap();
        assert!(instr.starts_with("Base system prompt."));
        assert!(instr.contains("sandbox__*"));
        assert!(instr.contains("local__*"));
        // Idempotent: second call does not duplicate the block.
        append_env_guidance(&mut body);
        assert_eq!(
            body["instructions"]
                .as_str()
                .unwrap()
                .matches("# Tool environments")
                .count(),
            1
        );
        // Creates instructions when absent.
        let mut bare = json!({ "model": "codex" });
        append_env_guidance(&mut bare);
        assert!(
            bare["instructions"]
                .as_str()
                .unwrap()
                .contains("# Tool environments")
        );
    }

    #[test]
    fn strip_sandbox_prefix_in_sse_unprefixes_function_calls_only() {
        let sse = concat!(
            "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"name\":\"sandbox__exec_command\",\"call_id\":\"c1\",\"arguments\":\"\"}}\n",
            "data: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"name\":\"local__echo\",\"call_id\":\"c2\",\"arguments\":\"\"}}\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n",
            "data: [DONE]\n",
        );
        let out = strip_sandbox_prefix_in_sse(sse);
        // sandbox__ stripped, local__ preserved, message + [DONE] untouched.
        let calls = parse_function_calls(&out);
        assert_eq!(calls[0].name, "exec_command");
        assert_eq!(calls[1].name, "local__echo");
        assert!(out.contains("\"delta\":\"hello\""));
        assert!(out.contains("[DONE]"));
    }

    #[test]
    fn append_followup_creates_input_when_absent() {
        let mut body = json!({ "model": "codex" });
        let call = FunctionCall {
            name: "local__echo".to_string(),
            call_id: "call_1".to_string(),
            arguments: "{}".to_string(),
        };
        append_followup(&mut body, &call, "{}");

        let input = body["input"].as_array().expect("input array created");
        assert_eq!(input.len(), 2);
        assert_eq!(input[0]["type"], "function_call");
        assert_eq!(input[1]["type"], "function_call_output");
    }
}
