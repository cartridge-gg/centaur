use axum::response::sse::Event;
use centaur_session_core::SessionEvent;
use centaur_session_runtime::{
    FinalAnswerTextUpdate, TerminalOutput, content_blocks, output_line_final_answer_text,
    string_at_path, terminal_output, terminal_payload_text,
};
use serde_json::{Value, json};

/// A single OpenAI Responses streaming event (`event:`/`data:` SSE frame).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResponsesStreamEvent {
    pub name: String,
    pub data: Value,
}

impl ResponsesStreamEvent {
    pub fn into_sse_event(self) -> Event {
        let data = serde_json::to_string(&self.data).unwrap_or_else(|_| "{}".to_owned());
        Event::default().event(self.name).data(data)
    }
}

/// Translates Centaur session events into OpenAI Responses streaming events.
///
/// The harness produces the same output lines consumed by the Anthropic
/// translator (claude Anthropic-shaped content blocks, and codex events in both
/// the dotted `item.agentMessage.delta` and the Rust harness-server's
/// slash-method `item/agentMessage/delta` shapes). Centaur owns the in-sandbox
/// tools, so the Responses output is a single assistant `message` item carrying
/// `output_text`; reasoning and client-side tool calls are out of scope for v1.
#[derive(Clone, Debug)]
pub struct ResponsesTranslator {
    response_id: String,
    model: String,
    item_id: String,
    sequence: u64,
    started: bool,
    message_open: bool,
    emitted_text: String,
    /// Text streamed for the current agentMessage item only, so an `item.completed`
    /// dedups against its own deltas rather than the global accumulator.
    item_text: String,
    final_answer_text: String,
    terminal_result_text: Option<String>,
    completed: bool,
}

impl ResponsesTranslator {
    pub fn new(response_id: impl Into<String>, model: impl Into<String>) -> Self {
        let response_id = response_id.into();
        let item_id = format!("msg_{response_id}");
        Self {
            response_id,
            model: model.into(),
            item_id,
            sequence: 0,
            started: false,
            message_open: false,
            emitted_text: String::new(),
            item_text: String::new(),
            final_answer_text: String::new(),
            terminal_result_text: None,
            completed: false,
        }
    }

    /// The accumulated assistant text (streamed deltas, or the terminal fallback).
    pub fn output_text(&self) -> String {
        if !self.emitted_text.is_empty() {
            return self.emitted_text.clone();
        }
        self.terminal_result_text
            .as_deref()
            .map(str::to_owned)
            .unwrap_or_default()
    }

    pub fn translate_session_event(&mut self, event: &SessionEvent) -> Vec<ResponsesStreamEvent> {
        let mut out = Vec::new();
        match event.event_type.as_str() {
            "session.execution_started" => self.ensure_started(&mut out),
            "session.output.line" => {
                self.ensure_started(&mut out);
                self.translate_output_line(&event.payload, &mut out);
            }
            "session.execution_completed" => {
                self.ensure_started(&mut out);
                if let Some(result_text) = string_at_path(&event.payload, &["result_text"]) {
                    self.terminal_result_text = Some(result_text);
                }
                self.finish(&mut out);
            }
            "session.execution_failed" => {
                self.ensure_started(&mut out);
                let message = string_at_path(&event.payload, &["error"])
                    .unwrap_or_else(|| "execution failed".to_owned());
                self.fail(&message, &mut out);
            }
            "session.stream_error" => {
                let message = string_at_path(&event.payload, &["error"])
                    .unwrap_or_else(|| "event stream failed".to_owned());
                self.fail(&message, &mut out);
            }
            _ => {}
        }
        out
    }

    fn translate_output_line(&mut self, payload: &Value, out: &mut Vec<ResponsesStreamEvent>) {
        let Some(line) = payload.as_str() else {
            return;
        };
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            return;
        };

        if !content_blocks(&value).is_empty() {
            for block in content_blocks(&value) {
                if block.get("type").and_then(Value::as_str) == Some("text") {
                    let text = terminal_payload_text(block);
                    if !text.is_empty() {
                        self.emit_text_delta(&text, out);
                    }
                }
            }
        } else {
            self.translate_codex_event(&value, out);
        }

        if let Some(update) = output_line_final_answer_text(&value) {
            match update {
                FinalAnswerTextUpdate::Append(delta) => self.final_answer_text.push_str(&delta),
                FinalAnswerTextUpdate::Replace(canonical) => self.final_answer_text = canonical,
            }
        }
        if let Some(TerminalOutput::Completed { result_text, .. }) =
            terminal_output(&value, &self.final_answer_text)
            && result_text.is_some()
        {
            self.terminal_result_text = result_text;
        }
    }

    fn translate_codex_event(&mut self, value: &Value, out: &mut Vec<ResponsesStreamEvent>) {
        // Same dual-shape handling as the Anthropic translator: read the kind
        // from `method` (slash) or `type` (dotted) and the fields from `params`
        // when present. Reasoning is intentionally ignored for v1.
        let Some(kind) = value
            .get("method")
            .and_then(Value::as_str)
            .or_else(|| value.get("type").and_then(Value::as_str))
        else {
            return;
        };
        let normalized = kind.replace('/', ".");
        let params = value.get("params").unwrap_or(value);
        match normalized.as_str() {
            "item.agentMessage.delta" => {
                if let Some(delta) = params.get("delta").and_then(Value::as_str)
                    && !delta.is_empty()
                {
                    self.separate_new_message(out);
                    self.emit_text_delta(delta, out);
                    self.item_text.push_str(delta);
                }
            }
            "item.completed" => {
                if let Some(item) = params.get("item")
                    && item.get("type").and_then(Value::as_str) == Some("agentMessage")
                    && let Some(full) = item.get("text").and_then(Value::as_str)
                    && !full.is_empty()
                {
                    self.emit_replacement_text(full, out);
                }
            }
            _ => {}
        }
    }

    fn emit_replacement_text(&mut self, full_text: &str, out: &mut Vec<ResponsesStreamEvent>) {
        // Dedup against THIS item's streamed deltas, not the global accumulator.
        // A turn can contain several agentMessages (the agent narrates between
        // tool calls); dedupping against the global text made every item.completed
        // fail the prefix check and re-emit its whole message -> duplicated output.
        let suffix = full_text
            .strip_prefix(self.item_text.as_str())
            .unwrap_or(full_text)
            .to_owned();
        if !suffix.is_empty() {
            self.separate_new_message(out);
            self.emit_text_delta(&suffix, out);
        }
        self.item_text.clear();
    }

    /// Separate a new agentMessage from prior output with a blank line, so the
    /// agent's narration and final answer don't glue together (e.g. the agent's
    /// "...previous snapshot." running straight into "Current deployments: ...").
    fn separate_new_message(&mut self, out: &mut Vec<ResponsesStreamEvent>) {
        if self.item_text.is_empty() && !self.emitted_text.is_empty() {
            self.emit_text_delta("\n\n", out);
        }
    }

    fn emit_text_delta(&mut self, text: &str, out: &mut Vec<ResponsesStreamEvent>) {
        self.ensure_message_open(out);
        self.emitted_text.push_str(text);
        out.push(self.event(
            "response.output_text.delta",
            json!({
                "type": "response.output_text.delta",
                "item_id": self.item_id,
                "output_index": 0,
                "content_index": 0,
                "delta": text,
            }),
        ));
    }

    fn ensure_started(&mut self, out: &mut Vec<ResponsesStreamEvent>) {
        if self.started {
            return;
        }
        self.started = true;
        out.push(self.event(
            "response.created",
            json!({
                "type": "response.created",
                "response": self.response_object("in_progress", &[]),
            }),
        ));
        out.push(self.event(
            "response.in_progress",
            json!({
                "type": "response.in_progress",
                "response": self.response_object("in_progress", &[]),
            }),
        ));
    }

    fn ensure_message_open(&mut self, out: &mut Vec<ResponsesStreamEvent>) {
        if self.message_open {
            return;
        }
        self.message_open = true;
        out.push(self.event(
            "response.output_item.added",
            json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {
                    "id": self.item_id,
                    "type": "message",
                    "status": "in_progress",
                    "role": "assistant",
                    "content": [],
                },
            }),
        ));
        out.push(self.event(
            "response.content_part.added",
            json!({
                "type": "response.content_part.added",
                "item_id": self.item_id,
                "output_index": 0,
                "content_index": 0,
                "part": {"type": "output_text", "text": "", "annotations": []},
            }),
        ));
    }

    fn finish(&mut self, out: &mut Vec<ResponsesStreamEvent>) {
        if self.completed {
            return;
        }
        // Fall back to the terminal result text when nothing streamed.
        if !self.message_open {
            let fallback = self
                .terminal_result_text
                .clone()
                .filter(|text| !text.trim().is_empty());
            if let Some(text) = fallback {
                self.emit_text_delta(&text, out);
            }
        }
        let text = self.emitted_text.clone();
        if self.message_open {
            out.push(self.event(
                "response.output_text.done",
                json!({
                    "type": "response.output_text.done",
                    "item_id": self.item_id,
                    "output_index": 0,
                    "content_index": 0,
                    "text": text,
                }),
            ));
            out.push(self.event(
                "response.content_part.done",
                json!({
                    "type": "response.content_part.done",
                    "item_id": self.item_id,
                    "output_index": 0,
                    "content_index": 0,
                    "part": {"type": "output_text", "text": text, "annotations": []},
                }),
            ));
            out.push(self.event(
                "response.output_item.done",
                json!({
                    "type": "response.output_item.done",
                    "output_index": 0,
                    "item": self.message_item(),
                }),
            ));
        }
        self.completed = true;
        let output = if self.message_open {
            vec![self.message_item()]
        } else {
            Vec::new()
        };
        out.push(self.event(
            "response.completed",
            json!({
                "type": "response.completed",
                "response": self.response_object("completed", &output),
            }),
        ));
    }

    fn fail(&mut self, message: &str, out: &mut Vec<ResponsesStreamEvent>) {
        if self.completed {
            return;
        }
        self.completed = true;
        let mut response = self.response_object("failed", &[]);
        if let Some(object) = response.as_object_mut() {
            object.insert(
                "error".to_owned(),
                json!({"code": "server_error", "message": message}),
            );
        }
        out.push(self.event(
            "response.failed",
            json!({"type": "response.failed", "response": response}),
        ));
    }

    fn message_item(&self) -> Value {
        json!({
            "id": self.item_id,
            "type": "message",
            "status": "completed",
            "role": "assistant",
            "content": [{
                "type": "output_text",
                "text": self.emitted_text,
                "annotations": [],
            }],
        })
    }

    fn response_object(&self, status: &str, output: &[Value]) -> Value {
        json!({
            "id": self.response_id,
            "object": "response",
            "status": status,
            "model": self.model,
            "output": output,
            "parallel_tool_calls": false,
            "tool_choice": "auto",
            "tools": [],
            "usage": usage_object(),
        })
    }

    fn event(&mut self, name: &str, mut data: Value) -> ResponsesStreamEvent {
        let sequence = self.sequence;
        self.sequence += 1;
        if let Some(object) = data.as_object_mut() {
            object.insert("sequence_number".to_owned(), json!(sequence));
        }
        ResponsesStreamEvent {
            name: name.to_owned(),
            data,
        }
    }
}

/// A transport-level error event, emitted when the session event stream itself
/// fails outside the translator's state machine.
pub fn stream_error_event(message: &str) -> ResponsesStreamEvent {
    ResponsesStreamEvent {
        name: "error".to_owned(),
        data: json!({"type": "error", "code": "server_error", "message": message}),
    }
}

/// Usage object with the fields the Codex CLI requires (notably `total_tokens`).
pub fn usage_object() -> Value {
    json!({
        "input_tokens": 0,
        "input_tokens_details": {"cached_tokens": 0},
        "output_tokens": 0,
        "output_tokens_details": {"reasoning_tokens": 0},
        "total_tokens": 0,
    })
}

#[cfg(test)]
mod tests {
    use centaur_session_core::ThreadKey;
    use time::OffsetDateTime;

    use super::*;

    fn event(event_type: &str, payload: Value) -> SessionEvent {
        SessionEvent {
            event_id: 1,
            thread_key: ThreadKey::parse("test:openai").unwrap(),
            execution_id: Some("exe_1".to_owned()),
            event_type: event_type.to_owned(),
            payload,
            created_at: OffsetDateTime::now_utc(),
        }
    }

    fn output(value: Value) -> SessionEvent {
        event("session.output.line", Value::String(value.to_string()))
    }

    fn names(events: &[ResponsesStreamEvent]) -> Vec<String> {
        events.iter().map(|event| event.name.clone()).collect()
    }

    fn translate(events: Vec<SessionEvent>) -> Vec<ResponsesStreamEvent> {
        let mut translator = ResponsesTranslator::new("resp_test", "gpt-test");
        events
            .iter()
            .flat_map(|event| translator.translate_session_event(event))
            .collect()
    }

    fn streamed_text(events: &[ResponsesStreamEvent]) -> String {
        events
            .iter()
            .filter(|event| event.name == "response.output_text.delta")
            .map(|event| event.data["delta"].as_str().unwrap_or(""))
            .collect()
    }

    #[test]
    fn slash_method_streams_output_text_and_completes() {
        let out = translate(vec![
            event("session.execution_started", json!({})),
            output(json!({"method":"item/agentMessage/delta","params":{"delta":"PO"}})),
            output(json!({"method":"item/agentMessage/delta","params":{"delta":"NG"}})),
            output(
                json!({"method":"item/completed","params":{"item":{"type":"agentMessage","text":"PONG","phase":"final_answer"}}}),
            ),
            event("session.execution_completed", json!({})),
        ]);

        assert_eq!(
            names(&out),
            vec![
                "response.created",
                "response.in_progress",
                "response.output_item.added",
                "response.content_part.added",
                "response.output_text.delta",
                "response.output_text.delta",
                "response.output_text.done",
                "response.content_part.done",
                "response.output_item.done",
                "response.completed",
            ]
        );
        assert_eq!(streamed_text(&out), "PONG");
        let completed = out
            .iter()
            .find(|event| event.name == "response.completed")
            .unwrap();
        // Codex requires total_tokens in the terminal usage.
        assert_eq!(
            completed.data["response"]["usage"]["total_tokens"],
            json!(0)
        );
        assert_eq!(completed.data["response"]["status"], json!("completed"));
        assert_eq!(
            completed.data["response"]["output"][0]["content"][0]["text"],
            json!("PONG")
        );
    }

    #[test]
    fn multi_message_turn_does_not_duplicate() {
        // The agent narrates between tool calls: a preamble agentMessage, then the
        // answer. Each item.completed must dedup against its OWN deltas; dedupping
        // against the global accumulator re-emitted the whole answer (stuttering).
        let out = translate(vec![
            event("session.execution_started", json!({})),
            output(json!({"method":"item/agentMessage/delta","params":{"delta":"Checking"}})),
            output(
                json!({"method":"item/completed","params":{"item":{"type":"agentMessage","text":"Checking"}}}),
            ),
            output(json!({"method":"item/agentMessage/delta","params":{"delta":"Answer"}})),
            output(
                json!({"method":"item/completed","params":{"item":{"type":"agentMessage","text":"Answer"}}}),
            ),
            event("session.execution_completed", json!({})),
        ]);
        // Each message appears once (no duplication) and consecutive messages are
        // separated by a blank line rather than glued ("Checking" + "Answer").
        assert_eq!(streamed_text(&out), "Checking\n\nAnswer");
    }

    #[test]
    fn dotted_codex_text() {
        let out = translate(vec![
            event("session.execution_started", json!({})),
            output(json!({"type":"item.agentMessage.delta","delta":"Hi"})),
            event("session.execution_completed", json!({})),
        ]);
        assert_eq!(streamed_text(&out), "Hi");
    }

    #[test]
    fn claude_content_blocks_text() {
        let out = translate(vec![
            event("session.execution_started", json!({})),
            output(json!({"type":"assistant","message":{"content":[{"type":"text","text":"Yo"}]}})),
            event("session.execution_completed", json!({})),
        ]);
        assert_eq!(streamed_text(&out), "Yo");
    }

    #[test]
    fn terminal_result_text_fallback_when_nothing_streamed() {
        let out = translate(vec![
            event("session.execution_started", json!({})),
            event("session.execution_completed", json!({"result_text":"DONE"})),
        ]);
        assert_eq!(streamed_text(&out), "DONE");
        let kinds = names(&out);
        assert!(
            kinds
                .iter()
                .any(|name| name == "response.output_item.added")
        );
        assert!(kinds.iter().any(|name| name == "response.completed"));
    }

    #[test]
    fn execution_failed_emits_response_failed() {
        let out = translate(vec![
            event("session.execution_started", json!({})),
            event("session.execution_failed", json!({"error":"boom"})),
        ]);
        let failed = out
            .iter()
            .find(|event| event.name == "response.failed")
            .expect("response.failed emitted");
        assert_eq!(failed.data["response"]["error"]["message"], json!("boom"));
    }
}
