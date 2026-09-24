//! One-call conversion between the two tools.

use uuid::Uuid;

use crate::error::{Error, Result};
use crate::model::{Part, Role, Session, Tool};
use crate::output::{Converted, Target};
use crate::prepare::PrepareOptions;
use crate::{claude, codex};

/// What to do with a user prompt at the end of the session that has no answer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TrailingPrompt {
    /// Keep it in the history.
    #[default]
    Keep,
    /// Leave it out, because the caller sends it again as the next turn.
    Drop,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConvertOptions {
    pub prepare: PrepareOptions,
    pub trailing_prompt: TrailingPrompt,
    /// `session_meta.model_provider` of a Codex rollout.
    pub codex_model_provider: String,
}

impl Default for ConvertOptions {
    fn default() -> Self {
        Self {
            prepare: PrepareOptions::default(),
            trailing_prompt: TrailingPrompt::Keep,
            codex_model_provider: "openai".to_string(),
        }
    }
}

/// Converts `session` into the native format of `to`. Line uuids of a Claude
/// session are random.
///
/// # Errors
///
/// [`Error::SameTool`] if the session already belongs to `to`, and
/// [`Error::EmptySession`] if it has no messages to convert.
pub fn convert(
    session: &Session,
    to: Tool,
    target: &Target,
    options: &ConvertOptions,
) -> Result<Converted> {
    if session.source == to {
        return Err(Error::SameTool(to));
    }
    let mut session = session.clone();
    if options.trailing_prompt == TrailingPrompt::Drop
        && session.messages.last().is_some_and(|m| {
            m.role == Role::User
                && m.parts
                    .iter()
                    .all(|p| matches!(p, Part::Text(_) | Part::Image { .. }))
        })
    {
        session.messages.pop();
    }
    if session.messages.is_empty() {
        return Err(Error::EmptySession {
            tool: session.source,
            id: session.id,
        });
    }
    Ok(match to {
        Tool::Claude => claude::render_session(&session, target, &options.prepare, Uuid::new_v4),
        Tool::Codex => codex::render_session(
            &session,
            target,
            &options.prepare,
            &options.codex_model_provider,
        ),
    })
}
