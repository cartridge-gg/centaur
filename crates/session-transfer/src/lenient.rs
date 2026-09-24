//! Lenient deserialization for agent session formats.
//!
//! Session files come from CLIs that change often. A field of an unexpected
//! type counts as missing, and the rest of the record still counts, the way
//! sessport reads them.

use serde::de::IgnoredAny;
use serde::{Deserialize, Deserializer};
use serde_json::Value;

use crate::model::ToolInput;

/// `T`, or any other JSON value, which then counts as missing.
#[derive(Deserialize)]
#[serde(untagged)]
pub(crate) enum Lenient<T> {
    Valid(T),
    Invalid(IgnoredAny),
}

impl<T> Lenient<T> {
    pub(crate) fn as_valid(&self) -> Option<&T> {
        match self {
            Self::Valid(value) => Some(value),
            Self::Invalid(_) => None,
        }
    }

    pub(crate) fn into_option(self) -> Option<T> {
        match self {
            Self::Valid(value) => Some(value),
            Self::Invalid(_) => None,
        }
    }
}

/// Reads an optional field. A value of an unexpected type counts as missing.
pub(crate) fn lenient<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(Option::<Lenient<T>>::deserialize(deserializer)?.and_then(Lenient::into_option))
}

/// Reads a tool input that is usually free-form text, such as a patch, a
/// script or a command. Any other JSON value, `null` included, stays JSON.
pub(crate) fn tool_input<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<ToolInput, D::Error> {
    Ok(match Value::deserialize(deserializer)? {
        Value::String(text) => ToolInput::Text(text),
        json => ToolInput::Json(json),
    })
}
