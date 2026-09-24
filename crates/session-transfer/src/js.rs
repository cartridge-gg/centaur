//! JavaScript behavior that the output depends on.
//!
//! sessport is written in TypeScript, and its output depends on UTF-16 string
//! lengths, the ECMAScript whitespace set and `JSON.stringify` formatting.
//! These helpers copy that behavior so that the converted sessions match
//! sessport byte for byte.

use serde::ser::Error as _;
use serde::{Serialize, Serializer};
use serde_json::value::RawValue;
use serde_json::{Map, Value};

/// The ECMAScript `WhiteSpace` and `LineTerminator` code points. `\s`, `trim()`
/// and `trimStart()` use this set. It includes U+FEFF and excludes U+0085,
/// unlike `char::is_whitespace`.
pub fn is_whitespace(c: char) -> bool {
    matches!(
        c,
        '\t' | '\n' | '\u{0B}' | '\u{0C}' | '\r' | ' ' | '\u{A0}' | '\u{1680}' | '\u{2000}'
            ..='\u{200A}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202F}'
                | '\u{205F}'
                | '\u{3000}'
                | '\u{FEFF}'
    )
}

/// The same set as [`is_whitespace`], as the body of a regex character class.
pub const WHITESPACE_CLASS: &str = r"\t\n\x0B\x0C\r \x{A0}\x{1680}\x{2000}-\x{200A}\x{2028}\x{2029}\x{202F}\x{205F}\x{3000}\x{FEFF}";

/// `String.prototype.trim`.
pub fn trim(s: &str) -> &str {
    s.trim_matches(is_whitespace)
}

/// `String.prototype.trimStart`.
pub fn trim_start(s: &str) -> &str {
    s.trim_start_matches(is_whitespace)
}

/// `String.prototype.length`: the number of UTF-16 code units.
pub fn utf16_len(s: &str) -> usize {
    s.chars().map(char::len_utf16).sum()
}

/// `s.slice(0, units)`, measured in UTF-16 code units.
///
/// JavaScript can cut a surrogate pair in half. This function stops before
/// the pair instead, because a Rust string cannot hold half a pair.
pub fn utf16_prefix(s: &str, units: usize) -> &str {
    let mut used = 0;
    for (i, c) in s.char_indices() {
        used += c.len_utf16();
        if used > units {
            return &s[..i];
        }
    }
    s
}

/// sessport `oneLine`: collapses whitespace runs to one space, trims, and
/// cuts the result to `max` UTF-16 code units with a trailing ellipsis.
pub fn one_line(text: &str, max: usize) -> String {
    let words: Vec<&str> = text
        .split(is_whitespace)
        .filter(|w| !w.is_empty())
        .collect();
    let flat = words.join(" ");
    if utf16_len(&flat) > max {
        format!("{}…", utf16_prefix(&flat, max.saturating_sub(1)))
    } else {
        flat
    }
}

/// `JSON.stringify(value, null, 2)`.
pub fn stringify_pretty(value: &Value) -> String {
    serde_json::to_string_pretty(&JsValue(value)).expect("a JSON value always serializes")
}

/// Serializes a `Value` the way JavaScript does: numbers in JavaScript
/// notation, and array-index keys ("0", "1", ...) first in ascending order,
/// then the other keys in insertion order.
struct JsValue<'a>(&'a Value);

impl Serialize for JsValue<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            Value::Number(n) => {
                let js = number_to_string(n.as_f64().unwrap_or_default());
                RawValue::from_string(js)
                    .map_err(S::Error::custom)?
                    .serialize(serializer)
            }
            Value::Array(items) => serializer.collect_seq(items.iter().map(JsValue)),
            Value::Object(map) => serializer.collect_map(
                property_order(map)
                    .into_iter()
                    .map(|key| (key, JsValue(&map[key]))),
            ),
            other => other.serialize(serializer),
        }
    }
}

fn property_order(map: &Map<String, Value>) -> Vec<&String> {
    let mut indexed: Vec<(u32, &String)> = map
        .keys()
        .filter_map(|key| Some((array_index(key)?, key)))
        .collect();
    indexed.sort_unstable_by_key(|&(index, _)| index);
    indexed
        .into_iter()
        .map(|(_, key)| key)
        .chain(map.keys().filter(|key| array_index(key).is_none()))
        .collect()
}

/// A canonical array index: "0", or digits without a leading zero, below 2^32 - 1.
fn array_index(key: &str) -> Option<u32> {
    let canonical = key == "0" || !key.starts_with('0');
    if !canonical || !key.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    key.parse::<u32>().ok().filter(|&index| index != u32::MAX)
}

/// `Number.prototype.toString()` for the numbers that JSON can hold.
pub fn number_to_string(value: f64) -> String {
    if value == 0.0 {
        return "0".to_string(); // also -0
    }
    if !value.is_finite() {
        return "null".to_string();
    }

    // ryu gives the shortest round-trip digits, as ECMAScript requires.
    let mut buf = ryu::Buffer::new();
    let formatted = buf.format_finite(value.abs());
    let (mantissa, exp) = match formatted.split_once('e') {
        Some((m, e)) => (m, e.parse::<i32>().expect("ryu writes a valid exponent")),
        None => (formatted, 0),
    };
    let (int_part, frac_part) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let all = format!("{int_part}{frac_part}");
    let leading = all.len() - all.trim_start_matches('0').len();
    let digits = all[leading..].trim_end_matches('0');
    // The value is 0.<digits> * 10^n, with k digits.
    let len = |s: &str| i32::try_from(s.len()).expect("an f64 has at most a few hundred digits");
    let n = len(int_part) + exp - len(&all[..leading]);
    let k = len(digits);

    let body = if k <= n && n <= 21 {
        format!("{digits}{}", "0".repeat((n - k).unsigned_abs() as usize))
    } else if 0 < n && n <= 21 {
        let (int, frac) = digits.split_at(n.unsigned_abs() as usize);
        format!("{int}.{frac}")
    } else if -6 < n && n <= 0 {
        format!("0.{}{digits}", "0".repeat(n.unsigned_abs() as usize))
    } else {
        let e = n - 1;
        let sign = if e >= 0 { '+' } else { '-' };
        let (first, rest) = digits.split_at(1);
        let point = if rest.is_empty() { "" } else { "." };
        format!("{first}{point}{rest}e{sign}{}", e.unsigned_abs())
    };
    if value < 0.0 {
        format!("-{body}")
    } else {
        body
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn numbers_match_javascript() {
        let cases = [
            (0.0, "0"),
            (-0.0, "0"),
            (1.0, "1"),
            (-1.5, "-1.5"),
            (0.5, "0.5"),
            (123.456, "123.456"),
            (1000.0, "1000"),
            (1e16, "10000000000000000"),
            (1e21, "1e+21"),
            (1.5e300, "1.5e+300"),
            (0.000_001, "0.000001"),
            (1e-7, "1e-7"),
            (1.25e-7, "1.25e-7"),
            (9_007_199_254_740_993.0, "9007199254740992"),
            (0.1 + 0.2, "0.30000000000000004"),
        ];
        for (value, expected) in cases {
            assert_eq!(number_to_string(value), expected, "{value:e}");
        }
    }

    #[test]
    fn pretty_print_orders_index_keys_first() {
        let value: Value =
            serde_json::from_str(r#"{"b":1,"10":2,"a":[],"2":{},"01":3,"4294967295":4}"#).unwrap();
        assert_eq!(
            stringify_pretty(&value),
            "{\n  \"2\": {},\n  \"10\": 2,\n  \"b\": 1,\n  \"a\": [],\n  \"01\": 3,\n  \"4294967295\": 4\n}"
        );
        assert_eq!(
            stringify_pretty(&json!([1.0, [true, null], 1e21])),
            "[\n  1,\n  [\n    true,\n    null\n  ],\n  1e+21\n]"
        );
    }

    #[test]
    fn one_line_counts_utf16_units() {
        assert_eq!(one_line("\u{FEFF} a \n\t b\u{85} ", 80), "a b\u{85}");
        assert_eq!(one_line("😀😀😀", 4), "😀…");
        assert_eq!(one_line("abcdef", 4), "abc…");
    }
}
