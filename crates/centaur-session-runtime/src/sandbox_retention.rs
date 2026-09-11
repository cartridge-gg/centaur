//! Session sandbox retention: keep a session's sandbox (paused, not stopped)
//! past the max lifetime while it owns open work, per-lease, bounded.
//!
//! The authoritative record lives under `sessions.metadata.sandbox_retention`
//! and survives sandbox replacement; the effective deadline is projected onto
//! the sandbox as annotations (`KEEP_UNTIL_ANNOTATION`,
//! `KEEP_HARD_DEADLINE_ANNOTATION`) so the backend-only reaper can honour it.
//!
//! Rules:
//! - `started_at` is set by the first lease and never reset; `hard_deadline =
//!   started_at + max`. Renewals and later leases cannot move it: retention is
//!   a bounded extension, not a lifetime.
//! - One lease per `key` (the overlay uses `github:<repo>:<pr>`), each with a
//!   `generation` (the open event). A closed generation stays recorded so a
//!   late duplicate "open" cannot resurrect it; a new generation reopens.
//! - The sandbox is kept while any lease is active; the effective `until` is
//!   the latest active lease's `until`, clamped to the hard deadline.
//! - Completed stop operations are recorded by idempotency key so a retried
//!   close is a no-op.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use centaur_sandbox_core::{KEEP_HARD_DEADLINE_ANNOTATION, KEEP_UNTIL_ANNOTATION};
use serde::{Deserialize, Serialize};

/// The `sessions.metadata` key the record lives under. Reserved: generic
/// metadata writers must not replace it.
pub const RETENTION_METADATA_KEY: &str = "sandbox_retention";

/// How many completed operation keys a record remembers.
const COMPLETED_OPS_KEPT: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaseState {
    Active,
    Closed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub generation: String,
    /// RFC 3339.
    pub until: String,
    pub state: LeaseState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Retention {
    /// RFC 3339; the first registration.
    pub started_at: String,
    /// RFC 3339; `started_at + max`.
    pub hard_deadline: String,
    #[serde(default)]
    pub leases: BTreeMap<String, Lease>,
    /// Idempotency keys of completed stop operations, newest last.
    #[serde(default)]
    pub completed_ops: Vec<String>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RetentionError {
    #[error("until is more than the retention cap ({max_secs}s) in the future")]
    TooFar { max_secs: u64 },
    #[error("until is in the past")]
    InPast,
}

/// What a keepalive did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeepaliveOutcome {
    /// Whether the record changed (a lease opened, renewed, or closed).
    pub applied: bool,
}

/// What closing a lease did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseClose {
    /// The named lease moved to closed by this call.
    pub closed: bool,
    /// The operation's idempotency key was already recorded.
    pub already_done: bool,
    /// Active leases remaining after the close.
    pub active_remaining: usize,
}

pub fn format_time(time: SystemTime) -> String {
    jiff::Timestamp::try_from(time)
        .map(|timestamp| timestamp.to_string())
        .unwrap_or_default()
}

pub fn parse_time(raw: &str) -> Option<SystemTime> {
    raw.trim()
        .parse::<jiff::Timestamp>()
        .ok()
        .map(SystemTime::from)
}

impl Retention {
    fn new(now: SystemTime, max: Duration) -> Self {
        Self {
            started_at: format_time(now),
            hard_deadline: format_time(now + max),
            leases: BTreeMap::new(),
            completed_ops: Vec::new(),
        }
    }

    pub fn hard_deadline_time(&self) -> Option<SystemTime> {
        parse_time(&self.hard_deadline)
    }

    pub fn active_leases(&self) -> usize {
        self.leases
            .values()
            .filter(|lease| lease.state == LeaseState::Active)
            .count()
    }

    /// The instant the sandbox is kept until: the latest active lease,
    /// clamped to the hard deadline; None when no lease is active.
    pub fn effective_until(&self) -> Option<SystemTime> {
        let latest = self
            .leases
            .values()
            .filter(|lease| lease.state == LeaseState::Active)
            .filter_map(|lease| parse_time(&lease.until))
            .max()?;
        Some(match self.hard_deadline_time() {
            Some(hard) => latest.min(hard),
            None => latest,
        })
    }

    /// The annotations that project this record onto a sandbox. Without an
    /// active lease `keep-until` is removed (None) so the reaper falls back to
    /// the ordinary max lifetime.
    pub fn annotations(&self) -> BTreeMap<String, Option<String>> {
        BTreeMap::from([
            (
                KEEP_UNTIL_ANNOTATION.to_owned(),
                self.effective_until().map(format_time),
            ),
            (
                KEEP_HARD_DEADLINE_ANNOTATION.to_owned(),
                Some(self.hard_deadline.clone()),
            ),
        ])
    }

    fn record_op(&mut self, idempotency_key: &str) {
        if idempotency_key.is_empty() || self.completed_ops.iter().any(|op| op == idempotency_key) {
            return;
        }
        self.completed_ops.push(idempotency_key.to_owned());
        if self.completed_ops.len() > COMPLETED_OPS_KEPT {
            let excess = self.completed_ops.len() - COMPLETED_OPS_KEPT;
            self.completed_ops.drain(..excess);
        }
    }
}

/// Open, renew (`until = Some`) or close (`until = None`) the lease `key`
/// for `generation`. Returns the record to persist and whether it changed.
pub fn apply_keepalive(
    existing: Option<Retention>,
    key: &str,
    generation: &str,
    until: Option<SystemTime>,
    now: SystemTime,
    max: Duration,
) -> Result<(Retention, KeepaliveOutcome), RetentionError> {
    let mut retention = existing.unwrap_or_else(|| Retention::new(now, max));
    let Some(until) = until else {
        // Close without stopping.
        let applied = match retention.leases.get_mut(key) {
            Some(lease) if lease.state == LeaseState::Active && lease.generation == generation => {
                lease.state = LeaseState::Closed;
                true
            }
            _ => false,
        };
        return Ok((retention, KeepaliveOutcome { applied }));
    };
    if until <= now {
        return Err(RetentionError::InPast);
    }
    if until > now + max {
        return Err(RetentionError::TooFar {
            max_secs: max.as_secs(),
        });
    }
    let hard = retention.hard_deadline_time().unwrap_or(until);
    let until = until.min(hard);
    if let Some(lease) = retention.leases.get(key)
        && lease.generation == generation
        && lease.state == LeaseState::Closed
    {
        // A late duplicate of an open event for a generation that already
        // closed: never resurrect it.
        return Ok((retention, KeepaliveOutcome { applied: false }));
    }
    let lease = Lease {
        generation: generation.to_owned(),
        until: format_time(until),
        state: LeaseState::Active,
    };
    let applied = retention.leases.get(key) != Some(&lease);
    retention.leases.insert(key.to_owned(), lease);
    Ok((retention, KeepaliveOutcome { applied }))
}

/// Close the lease `key` for `generation` as part of the stop operation
/// `idempotency_key`. A lease that moved on to a newer generation is left
/// alone (the close belongs to the old one); an empty `generation` closes
/// whichever generation is active, for callers whose close event does not
/// know which open event it answers.
pub fn close_lease(
    existing: Option<Retention>,
    key: &str,
    generation: &str,
    idempotency_key: &str,
    now: SystemTime,
    max: Duration,
) -> (Retention, LeaseClose) {
    let mut retention = existing.unwrap_or_else(|| Retention::new(now, max));
    if !idempotency_key.is_empty()
        && retention
            .completed_ops
            .iter()
            .any(|op| op == idempotency_key)
    {
        let active_remaining = retention.active_leases();
        return (
            retention,
            LeaseClose {
                closed: false,
                already_done: true,
                active_remaining,
            },
        );
    }
    let closed = match retention.leases.get_mut(key) {
        Some(lease)
            if lease.state == LeaseState::Active
                && (generation.is_empty() || lease.generation == generation) =>
        {
            lease.state = LeaseState::Closed;
            true
        }
        _ => false,
    };
    retention.record_op(idempotency_key);
    let active_remaining = retention.active_leases();
    (
        retention,
        LeaseClose {
            closed,
            already_done: false,
            active_remaining,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAY: Duration = Duration::from_secs(86_400);
    const WEEK: Duration = Duration::from_secs(7 * 86_400);

    fn now() -> SystemTime {
        // A fixed instant keeps the RFC 3339 strings stable.
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000)
    }

    #[test]
    fn first_lease_starts_the_clock_and_sets_the_hard_deadline() {
        let t = now();
        let (r, outcome) =
            apply_keepalive(None, "github:o/r:1", "g1", Some(t + DAY), t, WEEK).unwrap();
        assert!(outcome.applied);
        assert_eq!(parse_time(&r.started_at), Some(t));
        assert_eq!(r.hard_deadline_time(), Some(t + WEEK));
        assert_eq!(r.active_leases(), 1);
        assert_eq!(r.effective_until(), Some(t + DAY));
        let annotations = r.annotations();
        assert_eq!(
            annotations[KEEP_UNTIL_ANNOTATION].as_deref(),
            Some(format_time(t + DAY).as_str())
        );
        assert_eq!(
            annotations[KEEP_HARD_DEADLINE_ANNOTATION].as_deref(),
            Some(format_time(t + WEEK).as_str())
        );
        // Re-applying the same lease is a no-op.
        let (r2, outcome) = apply_keepalive(
            Some(r.clone()),
            "github:o/r:1",
            "g1",
            Some(t + DAY),
            t,
            WEEK,
        )
        .unwrap();
        assert!(!outcome.applied);
        assert_eq!(r2, r);
    }

    #[test]
    fn renewals_never_move_the_hard_deadline_and_are_clamped_to_it() {
        let t = now();
        let (r, _) = apply_keepalive(None, "k", "g1", Some(t + DAY), t, WEEK).unwrap();
        // Six days later, ask for another full week: clamped to the original deadline.
        let later = t + 6 * DAY;
        let (r, outcome) =
            apply_keepalive(Some(r), "k", "g1", Some(later + WEEK), later, WEEK).unwrap();
        assert!(outcome.applied);
        assert_eq!(r.hard_deadline_time(), Some(t + WEEK));
        assert_eq!(r.effective_until(), Some(t + WEEK));
        // Beyond the cap from "now" is rejected, not clamped.
        assert_eq!(
            apply_keepalive(
                Some(r.clone()),
                "k",
                "g1",
                Some(later + WEEK + DAY),
                later,
                WEEK
            )
            .unwrap_err(),
            RetentionError::TooFar {
                max_secs: WEEK.as_secs()
            }
        );
        assert_eq!(
            apply_keepalive(Some(r), "k", "g1", Some(later - DAY), later, WEEK).unwrap_err(),
            RetentionError::InPast
        );
    }

    #[test]
    fn per_pr_leases_keep_the_sandbox_until_the_last_one_closes() {
        let t = now();
        let (r, _) = apply_keepalive(None, "github:o/r:1", "g1", Some(t + DAY), t, WEEK).unwrap();
        let (r, _) =
            apply_keepalive(Some(r), "github:o/r:2", "g2", Some(t + 3 * DAY), t, WEEK).unwrap();
        assert_eq!(r.active_leases(), 2);
        assert_eq!(r.effective_until(), Some(t + 3 * DAY));

        let (r, close) = close_lease(Some(r), "github:o/r:2", "g2", "close-2", t, WEEK);
        assert!(close.closed && !close.already_done);
        assert_eq!(close.active_remaining, 1);
        assert_eq!(r.effective_until(), Some(t + DAY));

        // A retried close is a recorded no-op.
        let (r, close) = close_lease(Some(r), "github:o/r:2", "g2", "close-2", t, WEEK);
        assert!(!close.closed && close.already_done);
        assert_eq!(close.active_remaining, 1);

        let (r, close) = close_lease(Some(r), "github:o/r:1", "g1", "close-1", t, WEEK);
        assert!(close.closed);
        assert_eq!(close.active_remaining, 0);
        assert_eq!(r.effective_until(), None);
        assert_eq!(r.annotations()[KEEP_UNTIL_ANNOTATION], None);
    }

    #[test]
    fn a_closed_generation_cannot_be_resurrected_but_a_new_one_reopens() {
        let t = now();
        let (r, _) = apply_keepalive(None, "k", "g1", Some(t + DAY), t, WEEK).unwrap();
        let (r, _) = close_lease(Some(r), "k", "g1", "close-1", t, WEEK);
        let (r, outcome) = apply_keepalive(Some(r), "k", "g1", Some(t + DAY), t, WEEK).unwrap();
        assert!(!outcome.applied);
        assert_eq!(r.active_leases(), 0);
        let (r, outcome) = apply_keepalive(Some(r), "k", "g2", Some(t + DAY), t, WEEK).unwrap();
        assert!(outcome.applied);
        assert_eq!(r.active_leases(), 1);
        // A close for the old generation does not touch the new lease.
        let (r, close) = close_lease(Some(r), "k", "g1", "close-1-again", t, WEEK);
        assert!(!close.closed);
        assert_eq!(close.active_remaining, 1);
        // A close with no generation takes whichever is active.
        let (r, close) = close_lease(Some(r), "k", "", "close-any", t, WEEK);
        assert!(close.closed);
        assert_eq!(close.active_remaining, 0);
        let _ = r;
    }

    #[test]
    fn closing_via_keepalive_none_and_op_history_is_bounded() {
        let t = now();
        let (r, _) = apply_keepalive(None, "k", "g1", Some(t + DAY), t, WEEK).unwrap();
        let (r, outcome) = apply_keepalive(Some(r), "k", "g1", None, t, WEEK).unwrap();
        assert!(outcome.applied && r.active_leases() == 0);
        let (r, outcome) = apply_keepalive(Some(r), "k", "g1", None, t, WEEK).unwrap();
        assert!(!outcome.applied);
        let mut r = r;
        for i in 0..(COMPLETED_OPS_KEPT + 10) {
            r.record_op(&format!("op-{i}"));
        }
        assert_eq!(r.completed_ops.len(), COMPLETED_OPS_KEPT);
        assert_eq!(r.completed_ops.last().map(String::as_str), Some("op-73"));
        let json = serde_json::to_value(&r).unwrap();
        let back: Retention = serde_json::from_value(json).unwrap();
        assert_eq!(back, r);
    }
}
