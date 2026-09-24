//! Converts Codex CLI and Claude Code sessions into each other, so that a
//! session can continue on the other harness with its full history.
//!
//! The conversion is a Rust port of sessport
//! (<https://github.com/lanternsmith/sessport>, MIT). With the same options,
//! the output matches sessport byte for byte, except for the differences
//! listed in the README (see `tests/parity.rs`).
//!
//! Tool calls and their results become tagged text. Each harness has its own
//! tool set, and a foreign tool call replayed as a native one can make the
//! model API reject the history.
//!
//! ```no_run
//! use chrono::Local;
//! use session_transfer::convert::{ConvertOptions, TrailingPrompt, convert};
//! use session_transfer::discover::{Homes, SessionRef, resolve_session};
//! use session_transfer::{Target, Tool};
//!
//! let homes = Homes::from_env();
//! let session = resolve_session(Tool::Codex, &"latest".parse::<SessionRef>()?, &homes.codex)?;
//! let target = Target {
//!     home: homes.claude.clone(),
//!     cwd: session.cwd.clone().unwrap_or_default(),
//!     id: uuid::Uuid::new_v4(),
//!     now: Local::now().fixed_offset(),
//! };
//! let options = ConvertOptions {
//!     // The caller sends the unanswered prompt again as the next turn.
//!     trailing_prompt: TrailingPrompt::Drop,
//!     ..ConvertOptions::default()
//! };
//! let converted = convert(&session, Tool::Claude, &target, &options)?;
//! converted.write()?; // never overwrites a file
//! println!("{}", converted.resume_command);
//! # Ok::<(), session_transfer::Error>(())
//! ```

pub mod claude;
pub mod codex;
pub mod convert;
pub mod discover;
mod error;
mod js;
mod lenient;
pub mod model;
mod output;
pub mod prepare;
mod redact;
mod synthetic;

pub use error::{Error, Result};
pub use model::{Session, Tool};
pub use output::{Converted, Target, shell_quote};

/// Default name in the preamble, message ids and truncation notes.
pub const BRAND: &str = "session-transfer";
