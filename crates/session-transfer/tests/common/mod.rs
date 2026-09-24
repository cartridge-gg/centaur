//! Deterministic rendering that matches scripts/sessport-reference.mjs.

use std::path::PathBuf;

use chrono::{DateTime, FixedOffset};
use session_transfer::model::Session;
use session_transfer::prepare::PrepareOptions;
use session_transfer::{Converted, Target, Tool, claude, codex};
use uuid::Uuid;

pub const SESSION_ID: Uuid = Uuid::from_u128(0xaaaaaaaa_0000_4000_8000_000000000000);
pub const CLAUDE_HOME: &str = "/claude-home";
pub const CODEX_HOME: &str = "/codex-home";
pub const FALLBACK_CWD: &str = "/work/no-cwd";

/// 2026-09-23T12:00:00.000Z, in UTC like the golden generator.
pub fn now() -> DateTime<FixedOffset> {
    DateTime::parse_from_rfc3339("2026-09-23T12:00:00.000Z").expect("valid time")
}

/// Renders like sessport with a fixed session id and clock, and line uuids
/// 00000000-0000-4000-8000-000000000001, 00000000-0000-4000-8000-000000000002, ...
pub fn render_like_sessport(
    session: &Session,
    to: Tool,
    cwd: Option<&str>,
    options: &PrepareOptions,
) -> Converted {
    let target = Target {
        home: PathBuf::from(match to {
            Tool::Claude => CLAUDE_HOME,
            Tool::Codex => CODEX_HOME,
        }),
        cwd: cwd
            .or(session.cwd.as_deref())
            .unwrap_or(FALLBACK_CWD)
            .to_string(),
        id: SESSION_ID,
        now: now(),
    };
    let options = PrepareOptions {
        brand: "sessport",
        ..options.clone()
    };
    match to {
        Tool::Claude => {
            // The counter is written in decimal digits, as the JavaScript side writes it.
            let mut line = 0u64;
            claude::render_session(session, &target, &options, || {
                line += 1;
                Uuid::parse_str(&format!("00000000-0000-4000-8000-{line:012}")).expect("valid uuid")
            })
        }
        Tool::Codex => codex::render_session(session, &target, &options, "openai"),
    }
}
