//! Best-effort secret scrubbing, ported rule for rule from sessport.
//!
//! The rules target well-known token shapes and `KEY=value` assignments. They
//! do not try to find arbitrary high-entropy strings, because code has too
//! many of them.

use std::ops::Range;
use std::sync::LazyLock;

use regex::{Captures, Regex};

use crate::js::WHITESPACE_CLASS as WS;

/// Replaces likely secrets in `input` with `[REDACTED:<kind>]` markers.
pub fn redact_text(input: &str) -> String {
    RULES
        .iter()
        .fold(input.to_string(), |text, rule| rule.apply(&text))
}

// JavaScript `\b` without the `u` flag is an ASCII word boundary, and the `i`
// flag without `u` folds ASCII letters only. `(?-u:...)` gives both.
static RULES: LazyLock<Vec<Rule>> = LazyLock::new(|| {
    vec![
        Rule::pattern(
            r"-----BEGIN [A-Z ]*PRIVATE KEY-----(?s:.)*?-----END [A-Z ]*PRIVATE KEY-----",
            "[REDACTED:private-key]",
        ),
        Rule::pattern(r"(?-u:\b)sk-ant-[A-Za-z0-9_-]{20,}", "[REDACTED:anthropic-key]"),
        Rule::pattern(r"(?-u:\b)sk-(?:proj-|svcacct-|admin-)?[A-Za-z0-9_-]{20,}", "[REDACTED:openai-key]"),
        Rule::pattern(
            r"(?-u:\b)(?:gh[pousr]_[A-Za-z0-9]{36,}|github_pat_[A-Za-z0-9_]{22,})",
            "[REDACTED:github-token]",
        ),
        Rule::pattern(r"(?-u:\b)(?:AKIA|ASIA)[0-9A-Z]{16}(?-u:\b)", "[REDACTED:aws-access-key]"),
        Rule::pattern(r"(?-u:\b)AIza[0-9A-Za-z_-]{35}(?-u:\b)", "[REDACTED:google-api-key]"),
        Rule::pattern(r"(?-u:\b)xox[abposr]-[A-Za-z0-9-]{10,}", "[REDACTED:slack-token]"),
        Rule::pattern(r"(?-u:\b)(?:sk|rk)_(?:live|test)_[A-Za-z0-9]{16,}", "[REDACTED:stripe-key]"),
        Rule::pattern(r"(?-u:\b)npm_[A-Za-z0-9]{36}(?-u:\b)", "[REDACTED:npm-token]"),
        Rule::pattern(
            r"(?-u:\b)eyJ[A-Za-z0-9_-]{10,}\.eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}",
            "[REDACTED:jwt]",
        ),
        Rule::pattern(
            &format!(r"(?-u:\b)((?i-u:authorization):[{WS}]*(?i-u:bearer)[{WS}]+)[A-Za-z0-9._~+/=-]{{16,}}"),
            "$1[REDACTED:bearer]",
        ),
        Rule::pattern(
            &format!(r"(?-u:\b)([a-zA-Z][a-zA-Z0-9+.-]*://[^{WS}:/@]+:)[^{WS}@/]{{3,}}(@)"),
            "$1[REDACTED:password]$2",
        ),
        Rule::Assignment(
            Regex::new(&format!(
                r#"(?-u:\b)([A-Z0-9_]*(?:API_?KEY|SECRET|TOKEN|PASSWORD|PASSWD|PRIVATE_KEY|ACCESS_KEY)[A-Z0-9_]*[{WS}]*[:=][{WS}]*)(["']?)([^{WS}"'`]{{8,}})"#
            ))
            .expect("valid regex"),
        ),
    ]
});

enum Rule {
    /// A pattern and its replacement. `$1` and `$2` refer to capture groups.
    Pattern { regex: Regex, replace: &'static str },
    /// `NAME_TOKEN=value` or `SECRET: "value"`. sessport matches the value as
    /// `(["']?)(value)\2`. The `regex` crate has no back-references, so the
    /// closing quote is checked in code: group 2 is the opening quote, and
    /// group 3 is the value.
    Assignment(Regex),
}

impl Rule {
    fn pattern(regex: &str, replace: &'static str) -> Self {
        Self::Pattern {
            regex: Regex::new(regex).expect("valid regex"),
            replace,
        }
    }

    fn replacement(&self) -> &'static str {
        match self {
            Self::Pattern { replace, .. } => replace,
            Self::Assignment(_) => "$1$2[REDACTED:secret]$2",
        }
    }

    /// The first match that starts at or after `at`, as JavaScript finds it.
    fn find_at<'t>(&self, text: &'t str, at: usize) -> Option<(Range<usize>, Captures<'t>)> {
        match self {
            Self::Pattern { regex, .. } => {
                let caps = regex.captures_at(text, at)?;
                Some((caps.get(0)?.range(), caps))
            }
            Self::Assignment(regex) => {
                let mut at = at;
                loop {
                    let caps = regex.captures_at(text, at)?;
                    let start = caps.get(0)?.start();
                    let quote = caps.get(2).map_or("", |g| g.as_str());
                    let value_end = caps.get(3)?.end();
                    if text[value_end..].starts_with(quote) {
                        return Some((start..value_end + quote.len(), caps));
                    }
                    // The quotes differ, so JavaScript finds no match at this
                    // position. It tries again at the next character.
                    at = start + text[start..].chars().next().map_or(1, char::len_utf8);
                }
            }
        }
    }

    /// Replaces every match, left to right, like `String.prototype.replace`
    /// with a global regex. Text that is already redacted stays as is.
    fn apply(&self, text: &str) -> String {
        let mut out = String::with_capacity(text.len());
        let mut copied = 0;
        while let Some((range, caps)) = self.find_at(text, copied) {
            out.push_str(&text[copied..range.start]);
            let matched = &text[range.clone()];
            if matched.contains("[REDACTED:") {
                out.push_str(matched);
            } else {
                caps.expand(self.replacement(), &mut out);
            }
            copied = range.end;
        }
        out.push_str(&text[copied..]);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::redact_text;

    #[test]
    fn assignment_needs_matching_quotes() {
        assert_eq!(
            redact_text("MY_API_KEY=abcdefgh12345"),
            "MY_API_KEY=[REDACTED:secret]"
        );
        assert_eq!(
            redact_text(r#"SECRET: "quoted-secret-value""#),
            r#"SECRET: "[REDACTED:secret]""#
        );
        assert_eq!(
            redact_text(r#"PASSWORD='mismatch-value" x"#),
            r#"PASSWORD='mismatch-value" x"#
        );
        assert_eq!(
            redact_text("password=lowercase-is-kept"),
            "password=lowercase-is-kept"
        );
        assert_eq!(redact_text("TOKEN=short"), "TOKEN=short");
    }

    /// After a quote mismatch, the search continues inside the rejected text,
    /// as in JavaScript (checked against sessport).
    #[test]
    fn assignment_search_resumes_after_a_mismatch() {
        assert_eq!(
            redact_text(r#"X_TOKEN='ABC_TOKEN=abcdefgh" end"#),
            r#"X_TOKEN='ABC_TOKEN=[REDACTED:secret]" end"#
        );
    }

    #[test]
    fn keeps_existing_markers() {
        let text = "OPENAI_API_KEY=sk-proj-aaaaaaaaaaaaaaaaaaaaaaaa";
        assert_eq!(redact_text(text), "OPENAI_API_KEY=[REDACTED:openai-key]");
    }
}
