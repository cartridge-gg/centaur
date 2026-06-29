use axum::response::sse::Event;
use centaur_session_core::SessionEvent;
use centaur_session_runtime::{
    FinalAnswerTextUpdate, TerminalOutput, content_blocks, output_line_final_answer_text,
    string_at_path, terminal_output, terminal_payload_text,
};
use serde_json::{Value, json};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnthropicStreamEvent {
    pub name: &'static str,
    pub data: Value,
}

impl AnthropicStreamEvent {
    pub fn into_sse_event(self) -> Event {
        let data = serde_json::to_string(&self.data).unwrap_or_else(|_| "{}".to_owned());
        Event::default().event(self.name).data(data)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OpenBlockKind {
    Text,
    Thinking,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct OpenBlock {
    kind: OpenBlockKind,
    index: usize,
}

#[derive(Clone, Debug)]
pub struct AnthropicTranslator {
    message_id: String,
    model: String,
    next_index: usize,
    open_block: Option<OpenBlock>,
    emitted_text: String,
    /// Text streamed for the current agentMessage item only, so an `item.completed`
    /// dedups against its own deltas rather than the global accumulator.
    item_text: String,
    final_answer_text: String,
    terminal_result_text: Option<String>,
    content: Vec<Value>,
    started: bool,
    stopped: bool,
}

impl AnthropicTranslator {
    pub fn new(message_id: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            message_id: message_id.into(),
            model: model.into(),
            next_index: 0,
            open_block: None,
            emitted_text: String::new(),
            item_text: String::new(),
            final_answer_text: String::new(),
            terminal_result_text: None,
            content: Vec::new(),
            started: false,
            stopped: false,
        }
    }

    pub fn content(&self) -> Vec<Value> {
        self.content.clone()
    }

    pub fn terminal_result_text(&self) -> Option<&str> {
        self.terminal_result_text.as_deref()
    }

    pub fn translate_session_event(&mut self, event: &SessionEvent) -> Vec<AnthropicStreamEvent> {
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
                self.stop_message("end_turn", &mut out);
            }
            "session.execution_failed" => {
                self.ensure_started(&mut out);
                self.close_open_block(&mut out);
                out.push(error_event(
                    "api_error",
                    string_at_path(&event.payload, &["error"])
                        .unwrap_or_else(|| "execution failed".to_owned()),
                ));
                self.message_stop(&mut out);
            }
            "session.stream_error" => {
                out.push(error_event(
                    "api_error",
                    string_at_path(&event.payload, &["error"])
                        .unwrap_or_else(|| "event stream failed".to_owned()),
                ));
            }
            _ => {}
        }
        out
    }

    fn translate_output_line(&mut self, payload: &Value, out: &mut Vec<AnthropicStreamEvent>) {
        let Some(line) = payload.as_str() else {
            return;
        };
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            return;
        };

        if !content_blocks(&value).is_empty() {
            self.translate_anthropic_content_blocks(&value, out);
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

    fn translate_anthropic_content_blocks(
        &mut self,
        value: &Value,
        out: &mut Vec<AnthropicStreamEvent>,
    ) {
        for block in content_blocks(value) {
            match block.get("type").and_then(Value::as_str) {
                Some("text") => {
                    let text = terminal_payload_text(block);
                    if !text.is_empty() {
                        self.emit_text_delta(&text, out);
                    }
                }
                Some("thinking") => {
                    let thinking = string_at_path(block, &["thinking"])
                        .or_else(|| string_at_path(block, &["text"]))
                        .unwrap_or_else(|| terminal_payload_text(block));
                    if !thinking.is_empty() {
                        self.emit_thinking_delta(&thinking, out);
                    }
                }
                Some("tool_use") => self.emit_tool_use(block, out),
                Some("tool_result") => self.emit_tool_result(block, out),
                _ if block.get("tool_use_id").and_then(Value::as_str).is_some() => {
                    self.emit_tool_result(block, out);
                }
                _ => {}
            }
        }
    }

    fn translate_codex_event(&mut self, value: &Value, out: &mut Vec<AnthropicStreamEvent>) {
        // Harness output comes in two codex-style shapes: dotted `type` with
        // fields at the top level (`item.agentMessage.delta`, the Python codex
        // wrapper) and slash `method` with a `params` envelope
        // (`item/agentMessage/delta`, the Rust harness-server). Normalize the
        // kind (slash -> dot) and read fields from `params` when present.
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
            "item.reasoning.textDelta" | "item.reasoning.summaryTextDelta" => {
                let text = params
                    .get("delta")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .unwrap_or_else(|| terminal_payload_text(params));
                if !text.is_empty() {
                    self.emit_thinking_delta(&text, out);
                }
            }
            "item.agentMessage.delta" => {
                if let Some(delta) = params.get("delta").and_then(Value::as_str)
                    && !delta.is_empty()
                {
                    if self.item_text.is_empty() && !self.emitted_text.is_empty() {
                        // New agentMessage after prior output: separate with a
                        // blank line so narration and answer don't glue together.
                        self.emit_text_delta("\n\n", out);
                    }
                    self.emit_text_delta(delta, out);
                    self.item_text.push_str(delta);
                }
            }
            "item.completed" => {
                // Final agent-message text, emitted as a Replace so it dedups
                // against the streamed deltas (suffix-only).
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

    fn emit_replacement_text(&mut self, full_text: &str, out: &mut Vec<AnthropicStreamEvent>) {
        // Dedup against THIS item's streamed deltas, not the global accumulator,
        // so a multi-message turn (the agent narrates between tool calls) doesn't
        // re-emit each completed message as duplicated output.
        let new_message = self.item_text.is_empty() && !self.emitted_text.is_empty();
        let suffix = full_text
            .strip_prefix(self.item_text.as_str())
            .map(str::to_owned);
        self.item_text.clear();
        match suffix {
            Some(suffix) if !suffix.is_empty() => {
                if new_message {
                    self.emit_text_delta("\n\n", out);
                }
                self.emit_text_delta(&suffix, out);
            }
            Some(_) => {}
            None => {
                // Divergent within one item: emit as a fresh text block.
                self.close_open_block(out);
                self.open_text_block(out);
                self.emit_text_delta(full_text, out);
            }
        }
    }

    fn emit_text_delta(&mut self, text: &str, out: &mut Vec<AnthropicStreamEvent>) {
        self.open_text_block(out);
        let index = self.open_block.as_ref().expect("text block is open").index;
        self.append_to_content(index, "text", text);
        self.emitted_text.push_str(text);
        out.push(AnthropicStreamEvent {
            name: "content_block_delta",
            data: json!({
                "type": "content_block_delta",
                "index": index,
                "delta": {"type": "text_delta", "text": text},
            }),
        });
    }

    fn emit_thinking_delta(&mut self, thinking: &str, out: &mut Vec<AnthropicStreamEvent>) {
        self.open_thinking_block(out);
        let index = self
            .open_block
            .as_ref()
            .expect("thinking block is open")
            .index;
        self.append_to_content(index, "thinking", thinking);
        out.push(AnthropicStreamEvent {
            name: "content_block_delta",
            data: json!({
                "type": "content_block_delta",
                "index": index,
                "delta": {"type": "thinking_delta", "thinking": thinking},
            }),
        });
    }

    fn emit_tool_use(&mut self, block: &Value, out: &mut Vec<AnthropicStreamEvent>) {
        self.close_open_block(out);
        let index = self.next_content_index();
        let input = block.get("input").cloned().unwrap_or_else(|| json!({}));
        let content_block = json!({
            "type": "tool_use",
            "id": string_at_path(block, &["id"]).unwrap_or_else(|| format!("toolu_{index}")),
            "name": string_at_path(block, &["name"]).unwrap_or_else(|| "unknown".to_owned()),
            "input": input,
        });
        let mut start_block = content_block.clone();
        if let Some(object) = start_block.as_object_mut() {
            object.insert("input".to_owned(), json!({}));
        }
        self.content.push(content_block);
        out.push(content_block_start(index, start_block));
        out.push(AnthropicStreamEvent {
            name: "content_block_delta",
            data: json!({
                "type": "content_block_delta",
                "index": index,
                "delta": {
                    "type": "input_json_delta",
                    "partial_json": serde_json::to_string(&input).unwrap_or_else(|_| "{}".to_owned()),
                },
            }),
        });
        out.push(content_block_stop(index));
    }

    fn emit_tool_result(&mut self, block: &Value, out: &mut Vec<AnthropicStreamEvent>) {
        self.close_open_block(out);
        let index = self.next_content_index();
        let content_block = block.clone();
        self.content.push(content_block.clone());
        out.push(content_block_start(index, content_block));
        out.push(content_block_stop(index));
    }

    fn open_text_block(&mut self, out: &mut Vec<AnthropicStreamEvent>) {
        if self.open_block.as_ref().map(|block| block.kind) == Some(OpenBlockKind::Text) {
            return;
        }
        self.close_open_block(out);
        let index = self.next_content_index();
        let block = json!({"type": "text", "text": ""});
        self.content.push(block.clone());
        self.open_block = Some(OpenBlock {
            kind: OpenBlockKind::Text,
            index,
        });
        out.push(content_block_start(index, block));
    }

    fn open_thinking_block(&mut self, out: &mut Vec<AnthropicStreamEvent>) {
        if self.open_block.as_ref().map(|block| block.kind) == Some(OpenBlockKind::Thinking) {
            return;
        }
        self.close_open_block(out);
        let index = self.next_content_index();
        let block = json!({"type": "thinking", "thinking": ""});
        self.content.push(block.clone());
        self.open_block = Some(OpenBlock {
            kind: OpenBlockKind::Thinking,
            index,
        });
        out.push(content_block_start(index, block));
    }

    pub fn close_open_block(&mut self, out: &mut Vec<AnthropicStreamEvent>) {
        if let Some(open) = self.open_block.take() {
            out.push(content_block_stop(open.index));
        }
    }

    fn ensure_started(&mut self, out: &mut Vec<AnthropicStreamEvent>) {
        if self.started {
            return;
        }
        self.started = true;
        out.push(AnthropicStreamEvent {
            name: "message_start",
            data: json!({
                "type": "message_start",
                "message": {
                    "id": self.message_id,
                    "type": "message",
                    "role": "assistant",
                    "model": self.model,
                    "content": [],
                    "stop_reason": null,
                    "stop_sequence": null,
                    "usage": {"input_tokens": 0, "output_tokens": 0},
                },
            }),
        });
        out.push(AnthropicStreamEvent {
            name: "ping",
            data: json!({"type": "ping"}),
        });
    }

    fn stop_message(&mut self, stop_reason: &str, out: &mut Vec<AnthropicStreamEvent>) {
        self.close_open_block(out);
        out.push(AnthropicStreamEvent {
            name: "message_delta",
            data: json!({
                "type": "message_delta",
                "delta": {"stop_reason": stop_reason, "stop_sequence": null},
                "usage": {"output_tokens": 0},
            }),
        });
        self.message_stop(out);
    }

    fn message_stop(&mut self, out: &mut Vec<AnthropicStreamEvent>) {
        if self.stopped {
            return;
        }
        self.stopped = true;
        out.push(AnthropicStreamEvent {
            name: "message_stop",
            data: json!({"type": "message_stop"}),
        });
    }

    fn next_content_index(&mut self) -> usize {
        let index = self.next_index;
        self.next_index += 1;
        index
    }

    fn append_to_content(&mut self, index: usize, key: &str, value: &str) {
        let Some(block) = self.content.get_mut(index).and_then(Value::as_object_mut) else {
            return;
        };
        let current = block.entry(key.to_owned()).or_insert_with(|| json!(""));
        if let Some(text) = current.as_str() {
            *current = json!(format!("{text}{value}"));
        }
    }
}

fn content_block_start(index: usize, content_block: Value) -> AnthropicStreamEvent {
    AnthropicStreamEvent {
        name: "content_block_start",
        data: json!({
            "type": "content_block_start",
            "index": index,
            "content_block": content_block,
        }),
    }
}

fn content_block_stop(index: usize) -> AnthropicStreamEvent {
    AnthropicStreamEvent {
        name: "content_block_stop",
        data: json!({"type": "content_block_stop", "index": index}),
    }
}

pub fn error_event(error_type: &'static str, message: impl Into<String>) -> AnthropicStreamEvent {
    AnthropicStreamEvent {
        name: "error",
        data: json!({
            "type": "error",
            "error": {"type": error_type, "message": message.into()},
        }),
    }
}

#[cfg(test)]
mod tests {
    use centaur_session_core::ThreadKey;
    use time::OffsetDateTime;

    use super::*;

    fn event(event_type: &str, payload: Value) -> SessionEvent {
        SessionEvent {
            event_id: 1,
            thread_key: ThreadKey::parse("test:anthropic").unwrap(),
            execution_id: Some("exe_1".to_owned()),
            event_type: event_type.to_owned(),
            payload,
            created_at: OffsetDateTime::now_utc(),
        }
    }

    fn output(value: Value) -> SessionEvent {
        event("session.output.line", Value::String(value.to_string()))
    }

    fn names(events: &[AnthropicStreamEvent]) -> Vec<&'static str> {
        events.iter().map(|event| event.name).collect()
    }

    fn translate(events: Vec<SessionEvent>) -> Vec<AnthropicStreamEvent> {
        let mut translator = AnthropicTranslator::new("msg_test", "claude-test");
        events
            .iter()
            .flat_map(|event| translator.translate_session_event(event))
            .collect()
    }

    #[test]
    fn codex_text_only_dedups_completed_replace() {
        let out = translate(vec![
            event("session.execution_started", json!({})),
            output(json!({"type":"item.agentMessage.delta","delta":"Hello"})),
            output(json!({"type":"item.agentMessage.delta","delta":" world"})),
            output(
                json!({"type":"item.completed","item":{"type":"agentMessage","text":"Hello world","phase":"final_answer"}}),
            ),
            event("session.execution_completed", json!({})),
        ]);

        assert_eq!(
            names(&out),
            vec![
                "message_start",
                "ping",
                "content_block_start",
                "content_block_delta",
                "content_block_delta",
                "content_block_stop",
                "message_delta",
                "message_stop",
            ]
        );
        let text = out
            .iter()
            .filter(|event| event.name == "content_block_delta")
            .map(|event| event.data["delta"]["text"].as_str().unwrap_or(""))
            .collect::<String>();
        assert_eq!(text, "Hello world");
    }

    #[test]
    fn harness_server_slash_method_text() {
        // The deployed Rust harness-server emits slash `method` events with a
        // `params` envelope, not the dotted `type` shape.
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
                "message_start",
                "ping",
                "content_block_start",
                "content_block_delta",
                "content_block_delta",
                "content_block_stop",
                "message_delta",
                "message_stop",
            ]
        );
        let text = out
            .iter()
            .filter(|event| event.name == "content_block_delta")
            .map(|event| event.data["delta"]["text"].as_str().unwrap_or(""))
            .collect::<String>();
        assert_eq!(text, "PONG");
    }

    #[test]
    fn codex_reasoning_and_text() {
        let out = translate(vec![
            event("session.execution_started", json!({})),
            output(json!({"type":"item.reasoning.textDelta","delta":"thinking"})),
            output(json!({"type":"item.agentMessage.delta","delta":"answer"})),
        ]);

        assert_eq!(
            names(&out),
            vec![
                "message_start",
                "ping",
                "content_block_start",
                "content_block_delta",
                "content_block_stop",
                "content_block_start",
                "content_block_delta",
            ]
        );
        assert_eq!(out[3].data["delta"]["type"], json!("thinking_delta"));
        assert_eq!(out[6].data["delta"]["type"], json!("text_delta"));
    }

    #[test]
    fn claude_text_then_tool_use() {
        let out = translate(vec![output(json!({
            "type":"assistant",
            "message":{"content":[
                {"type":"text","text":"Let me check."},
                {"type":"tool_use","id":"toolu_1","name":"search","input":{"query":"centaur"}}
            ]}
        }))]);

        assert_eq!(
            names(&out),
            vec![
                "message_start",
                "ping",
                "content_block_start",
                "content_block_delta",
                "content_block_stop",
                "content_block_start",
                "content_block_delta",
                "content_block_stop",
            ]
        );
        assert_eq!(out[6].data["delta"]["type"], json!("input_json_delta"));
        assert_eq!(
            out[6].data["delta"]["partial_json"],
            json!("{\"query\":\"centaur\"}")
        );
    }

    #[test]
    fn claude_tool_result_is_surfaced_as_own_block() {
        let out = translate(vec![output(json!({
            "type":"user",
            "message":{"content":[
                {"type":"tool_result","tool_use_id":"toolu_1","content":"done"}
            ]}
        }))]);

        assert_eq!(
            names(&out),
            vec![
                "message_start",
                "ping",
                "content_block_start",
                "content_block_stop"
            ]
        );
        assert_eq!(out[2].data["content_block"]["type"], json!("tool_result"));
    }

    #[test]
    fn execution_failed_emits_error_and_message_stop() {
        let out = translate(vec![event(
            "session.execution_failed",
            json!({"error":"boom"}),
        )]);

        assert_eq!(
            names(&out),
            vec!["message_start", "ping", "error", "message_stop"]
        );
        assert_eq!(out[2].data["error"]["message"], json!("boom"));
    }

    #[test]
    fn stream_error_emits_error_without_message_stop() {
        let out = translate(vec![event("session.stream_error", json!({"error":"lost"}))]);

        assert_eq!(names(&out), vec!["error"]);
        assert_eq!(out[0].data["error"]["message"], json!("lost"));
    }
}
