//! Fixtures and goldens are public. This test fails when one of them looks
//! like it holds personal data: a home directory, an email address, an
//! account id, or a location.
//!
//! Record new fixtures on a throwaway repo and run `scripts/scrub-fixture.mjs`.

use std::fs;
use std::path::{Path, PathBuf};

use regex::Regex;

fn files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files(&path, out);
        } else {
            out.push(path);
        }
    }
}

#[test]
fn fixtures_hold_no_personal_data() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let mut paths = Vec::new();
    files(&root.join("fixtures"), &mut paths);
    files(&root.join("golden"), &mut paths);

    let rules = [
        // Neutral homes only: /Users/dev (sessport), /home/user and /home/agent.
        (
            "home directory",
            Regex::new(r"/(?:Users|home)/([A-Za-z0-9._-]+)").unwrap(),
        ),
        (
            "email address",
            Regex::new(r"[A-Za-z0-9._%+-]+@([A-Za-z0-9-]+\.[A-Za-z0-9.-]+)").unwrap(),
        ),
        (
            "account id",
            Regex::new(r#""[A-Za-z_]*account_id"\s*:\s*"[^"]+""#).unwrap(),
        ),
        (
            "timezone",
            Regex::new(r#""timezone"\s*:\s*"([^"]+)""#).unwrap(),
        ),
    ];
    let allowed_home = ["dev", "user", "agent"];
    // Fake addresses and keys that the redaction tests need.
    let allowed_domain = ["example.com", "db.local"];

    let mut findings = Vec::new();
    for path in &paths {
        let text = fs::read_to_string(path).unwrap();
        for (kind, rule) in &rules {
            for caps in rule.captures_iter(&text) {
                let value = caps.get(1).map_or("", |m| m.as_str());
                let allowed = match *kind {
                    "home directory" => allowed_home.contains(&value),
                    "email address" => allowed_domain.contains(&value),
                    "timezone" => value == "UTC",
                    _ => false,
                };
                if !allowed {
                    findings.push(format!(
                        "{}: {kind}: {}",
                        path.strip_prefix(&root).unwrap().display(),
                        &caps[0]
                    ));
                }
            }
        }
    }
    assert!(
        findings.is_empty(),
        "possible personal data:\n{}",
        findings.join("\n")
    );
}
