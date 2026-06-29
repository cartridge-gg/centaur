//! Per-session bridge that steers a sandbox agent's "local" tool calls out to
//! the user's local CLI (the Codex client speaking the OpenAI Responses wire
//! format) and back.
//!
//! The two sides that share a bridge instance for a given thread:
//!
//! - The **model proxy** ([`crate`]-external `model_proxy.rs`): when the sandbox
//!   agent's model emits a `local__`-prefixed function call, the proxy strips the
//!   prefix and calls [`LocalToolBridge::forward_call`], blocking its sub-loop on
//!   the returned [`oneshot::Receiver`] until the local CLI returns a result.
//! - The **`/v1/responses` ingress** (`openai/mod.rs`): it owns the user's CLI
//!   connection. It drains pending calls from the outbound channel
//!   ([`LocalToolBridge::take_outbound`]), surfaces each as a Responses
//!   `function_call` item, ends the turn, and when the CLI sends the matching
//!   `function_call_output` back it calls [`LocalToolBridge::resolve_result`] to
//!   unblock the proxy sub-loop, then resumes streaming the suspended execution
//!   from the recorded [`ResumeState`].
//!
//! All state is guarded by `std::sync::Mutex` (the critical sections never hold a
//! lock across an `.await`), so every method here is synchronous except the
//! `recv` on the receiver the caller owns directly.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

/// A local tool call awaiting execution on the user's CLI. The `name` is the
/// real client tool name with the `local__` routing prefix already stripped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingLocalCall {
    pub call_id: String,
    pub name: String,
    pub arguments: String,
}

/// A suspended sandbox execution plus the next event offset to resume streaming
/// from once the local CLI returns the awaited tool result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResumeState {
    pub execution_id: String,
    /// `after_event_id` to pass to `stream_events` so the resumed turn continues
    /// after the last event already streamed to the client. Matches the i64
    /// `event_id` on `SessionEvent`.
    pub next_offset: i64,
}

/// Per-session steering bridge. Created lazily, one per `thread_key`.
pub struct LocalToolBridge {
    /// The client's `request.tools`, set by the ingress so the proxy can inject
    /// them (renamed with the `local__` prefix) into the sandbox model request.
    local_tools: Mutex<Option<Value>>,
    /// Sender half handed to [`LocalToolBridge::forward_call`]; the ingress takes
    /// the receiver once via [`LocalToolBridge::take_outbound`].
    outbound_tx: mpsc::UnboundedSender<PendingLocalCall>,
    outbound_rx: Mutex<Option<mpsc::UnboundedReceiver<PendingLocalCall>>>,
    /// In-flight calls keyed by `call_id`, each holding the oneshot the proxy
    /// sub-loop is blocked on.
    results: Mutex<HashMap<String, oneshot::Sender<String>>>,
    /// The suspended execution + resume offset, set when the ingress ends a turn
    /// on a forwarded local call.
    exec: Mutex<Option<ResumeState>>,
}

impl LocalToolBridge {
    pub fn new() -> Arc<Self> {
        let (outbound_tx, outbound_rx) = mpsc::unbounded_channel();
        Arc::new(Self {
            local_tools: Mutex::new(None),
            outbound_tx,
            outbound_rx: Mutex::new(Some(outbound_rx)),
            results: Mutex::new(HashMap::new()),
            exec: Mutex::new(None),
        })
    }

    /// Record the client's advertised tools for the proxy to inject.
    pub fn set_local_tools(&self, tools: Option<Value>) {
        *lock(&self.local_tools) = tools;
    }

    /// The client's advertised tools, if any.
    pub fn local_tools(&self) -> Option<Value> {
        lock(&self.local_tools).clone()
    }

    /// Register an in-flight local call and push it to the outbound channel for
    /// the ingress to surface. Returns the receiver the proxy sub-loop awaits.
    pub fn forward_call(&self, call: PendingLocalCall) -> oneshot::Receiver<String> {
        let (tx, rx) = oneshot::channel();
        lock(&self.results).insert(call.call_id.clone(), tx);
        // Send can only fail if the receiver was dropped (ingress gone); the
        // proxy's await on `rx` will then resolve as a closed channel and it
        // surfaces an error to the model — no panic here.
        let _ = self.outbound_tx.send(call);
        rx
    }

    /// Take the outbound receiver (once). The ingress merges it with the
    /// execution event stream; on suspend it returns it via [`Self::restore_outbound`].
    pub fn take_outbound(&self) -> Option<mpsc::UnboundedReceiver<PendingLocalCall>> {
        lock(&self.outbound_rx).take()
    }

    /// Return the outbound receiver to the bridge so a subsequent resume request
    /// can keep merging further local calls from the same execution.
    pub fn restore_outbound(&self, rx: mpsc::UnboundedReceiver<PendingLocalCall>) {
        *lock(&self.outbound_rx) = Some(rx);
    }

    /// Deliver a tool result to the waiting proxy sub-loop. Returns whether a
    /// pending call with this `call_id` was matched.
    pub fn resolve_result(&self, call_id: &str, output: String) -> bool {
        match lock(&self.results).remove(call_id) {
            Some(sender) => sender.send(output).is_ok(),
            None => false,
        }
    }

    /// Whether a call with this `call_id` is currently awaiting a result.
    pub fn has_pending(&self, call_id: &str) -> bool {
        lock(&self.results).contains_key(call_id)
    }

    pub fn set_exec(&self, state: ResumeState) {
        *lock(&self.exec) = Some(state);
    }

    pub fn take_exec(&self) -> Option<ResumeState> {
        lock(&self.exec).take()
    }
}

/// Lock a `std::sync::Mutex`, recovering the guard on poison rather than
/// propagating a panic (these critical sections are short and never `.await`).
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(call_id: &str, name: &str, arguments: &str) -> PendingLocalCall {
        PendingLocalCall {
            call_id: call_id.to_owned(),
            name: name.to_owned(),
            arguments: arguments.to_owned(),
        }
    }

    #[tokio::test]
    async fn forward_call_then_resolve_round_trip() {
        let bridge = LocalToolBridge::new();
        let rx = bridge.forward_call(call("c1", "exec_command", "{}"));

        assert!(bridge.has_pending("c1"));
        assert!(bridge.resolve_result("c1", "the-output".to_owned()));
        assert!(!bridge.has_pending("c1"));

        assert_eq!(rx.await.unwrap(), "the-output");
    }

    #[test]
    fn resolve_result_unmatched_returns_false() {
        let bridge = LocalToolBridge::new();
        assert!(!bridge.resolve_result("nope", "x".to_owned()));
    }

    #[tokio::test]
    async fn forward_call_delivers_to_outbound_receiver() {
        let bridge = LocalToolBridge::new();
        let mut outbound = bridge.take_outbound().expect("receiver available once");
        // Second take yields nothing.
        assert!(bridge.take_outbound().is_none());

        let _rx = bridge.forward_call(call("c1", "exec_command", r#"{"cmd":"ls"}"#));
        let delivered = outbound.recv().await.expect("call delivered");
        assert_eq!(delivered, call("c1", "exec_command", r#"{"cmd":"ls"}"#));

        // Restored receiver can be taken again (resume path).
        bridge.restore_outbound(outbound);
        assert!(bridge.take_outbound().is_some());
    }

    #[test]
    fn exec_state_round_trip() {
        let bridge = LocalToolBridge::new();
        assert!(bridge.take_exec().is_none());
        bridge.set_exec(ResumeState {
            execution_id: "exe_1".to_owned(),
            next_offset: 42,
        });
        let resumed = bridge.take_exec().expect("exec state set");
        assert_eq!(resumed.execution_id, "exe_1");
        assert_eq!(resumed.next_offset, 42);
        // Taken once.
        assert!(bridge.take_exec().is_none());
    }

    #[test]
    fn local_tools_round_trip() {
        let bridge = LocalToolBridge::new();
        assert!(bridge.local_tools().is_none());
        bridge.set_local_tools(Some(serde_json::json!([{ "name": "exec_command" }])));
        assert_eq!(
            bridge.local_tools(),
            Some(serde_json::json!([{ "name": "exec_command" }]))
        );
    }
}
