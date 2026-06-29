use codex_app_server_protocol::{JSONRPCMessage, JSONRPCNotification, ServerNotification};
use serde_json::Value;

use crate::Result;

pub fn is_known_untyped_server_notification(method: &str) -> bool {
    // `centaur/threadStarted` is a Centaur overlay notification (not part of the
    // Codex App Server V2 protocol): the codex blocks harness emits it once per
    // thread so Centaur can capture the codex thread_id (== the model request's
    // `prompt_cache_key`) and index thread_id -> thread_key for the local-tool
    // model proxy. It carries only `params.threadId`, so it is validated as an
    // untyped notification rather than a typed `ServerNotification`.
    matches!(
        method,
        "remoteControl/status/changed" | "centaur/threadStarted"
    )
}

pub fn notification_to_jsonrpc(notification: &ServerNotification) -> Result<JSONRPCNotification> {
    let value = serde_json::to_value(notification)?;
    Ok(serde_json::from_value(value)?)
}

pub fn notification_to_wire_value(notification: &ServerNotification) -> Result<Value> {
    let rpc = notification_to_jsonrpc(notification)?;
    Ok(serde_json::to_value(JSONRPCMessage::Notification(rpc))?)
}
