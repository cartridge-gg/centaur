//! `harness-server transcript convert`: converts a Codex or Claude Code
//! session so that the other harness can resume it with the full history.

use std::path::PathBuf;

use chrono::Local;
use serde::Serialize;
use session_transfer::convert::{ConvertOptions, TrailingPrompt, convert};
use session_transfer::discover::{Homes, SessionRef, resolve_session};
use session_transfer::prepare::PrepareOptions;
use session_transfer::{Target, Tool};
use uuid::Uuid;

use crate::Result;

/// Name in the import preamble and message ids of converted sessions.
pub const BRAND: &str = "centaur";

/// What to convert, and where the result goes.
#[derive(Debug, Clone)]
pub struct ConvertRequest {
    pub from: Tool,
    pub to: Tool,
    pub session: SessionRef,
    /// Project directory of the new session. Defaults to the source cwd.
    pub cwd: Option<String>,
    pub homes: Homes,
    pub last_messages: Option<usize>,
    pub max_tool_output: Option<usize>,
    pub keep_thinking: bool,
    pub redact: bool,
    /// `session_meta.model_provider` of a Codex rollout.
    pub codex_model_provider: Option<String>,
    /// Leave out an unanswered prompt at the end, because the caller sends
    /// it again as the next turn.
    pub drop_trailing_prompt: bool,
    pub dry_run: bool,
}

/// The JSON that the subcommand prints.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConvertReport {
    pub tool: &'static str,
    pub id: Uuid,
    pub path: PathBuf,
    pub resume_command: String,
    pub bytes: usize,
    pub dry_run: bool,
    pub source: SourceReport,
}

#[derive(Debug, Serialize)]
pub struct SourceReport {
    pub tool: &'static str,
    pub id: String,
    pub messages: usize,
}

/// Converts the session and, unless it is a dry run, writes it.
pub fn convert_session(request: &ConvertRequest) -> Result<ConvertReport> {
    let session = resolve_session(
        request.from,
        &request.session,
        request.homes.get(request.from),
    )?;
    let cwd = request
        .cwd
        .clone()
        .or_else(|| session.cwd.clone())
        .ok_or(session_transfer::Error::MissingCwd)?;
    let target = Target {
        home: request.homes.get(request.to).to_path_buf(),
        cwd,
        id: Uuid::new_v4(),
        now: Local::now().fixed_offset(),
    };
    let options = ConvertOptions {
        prepare: PrepareOptions {
            max_tool_output: request.max_tool_output,
            last_messages: request.last_messages,
            keep_thinking: request.keep_thinking,
            redact: request.redact,
            brand: BRAND,
        },
        trailing_prompt: if request.drop_trailing_prompt {
            TrailingPrompt::Drop
        } else {
            TrailingPrompt::Keep
        },
        codex_model_provider: request
            .codex_model_provider
            .clone()
            .unwrap_or_else(|| ConvertOptions::default().codex_model_provider),
    };
    let converted = convert(&session, request.to, &target, &options)?;
    if !request.dry_run {
        converted.write()?;
    }
    Ok(ConvertReport {
        tool: converted.tool.id(),
        id: converted.id,
        path: converted.path,
        resume_command: converted.resume_command,
        bytes: converted.contents.len(),
        dry_run: request.dry_run,
        source: SourceReport {
            tool: session.source.id(),
            id: session.id,
            messages: session.messages.len(),
        },
    })
}
