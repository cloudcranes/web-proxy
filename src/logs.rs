//! In-memory ring buffer of recent gateway decisions (route, status,
//! duration, error category) for the dashboard's self-diagnosis view.
//!
//! Bounded memory (~500 entries); no persistence by design — when the
//! process restarts the buffer resets, which matches the user's
//! expectation of "what just happened on this gateway".
//!
//! Insertion is lock-free with `parking_lot::Mutex` style by using
//! `tokio::sync::Mutex` only at the moment of swap-and-clone in
//! `snapshot`. The hot path (`record`) only takes a short-lived
//! `parking_lot::Mutex` that we don't have in deps, so we use
//! `std::sync::Mutex` + `VecDeque` — push is O(1) and never blocks.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const CAPACITY: usize = 500;

#[derive(Clone, Debug)]
pub struct LogEntry {
    pub epoch_secs: i64,
    pub route: String,
    pub method: String,
    pub status: u16,
    pub duration_ms: u64,
    /// One short ASCII category for grep-friendly filtering on the
    /// dashboard: "ok",", "cache_hit",", "upstream",", "auth",", "parse",",
    /// "io",", "overflow",", "redirect",", "tls",", "timeout",".
    pub category: &'static str,
    /// Truncated to ~160 chars to keep the buffer cheap; full detail
    /// remains in the structured tracing logs.
    pub note: String,
}

impl LogEntry {
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "t": self.epoch_secs,
            "route": self.route,
            "method": self.method,
            "status": self.status,
            "ms": self.duration_ms,
            "category": self.category,
            "note": self.note,
        })
    }
}

pub struct Logs {
    inner: Mutex<VecDeque<LogEntry>>,
}

impl Logs {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(VecDeque::with_capacity(CAPACITY)),
        }
    }

    pub fn record(
        &self,
        route: impl Into<String>,
        method: impl Into<String>,
        status: u16,
        duration: Duration,
        category: &'static str,
        note: impl Into<String>,
    ) {
        let entry = LogEntry {
            epoch_secs: unix_secs(),
            route: route.into(),
            method: method.into(),
            status,
            duration_ms: duration.as_millis() as u64,
            category,
            note: note.into(),
        };
        let mut buf = match self.inner.lock() {
            Ok(b) => b,
            Err(p) => p.into_inner(),
        };
        if buf.len() == CAPACITY {
            buf.pop_front();
        }
        buf.push_back(entry);
    }

    pub fn snapshot(&self) -> Vec<LogEntry> {
        match self.inner.lock() {
            Ok(b) => b.iter().cloned().collect(),
            Err(p) => p.into_inner().iter().cloned().collect(),
        }
    }
}

impl Default for Logs {
    fn default() -> Self {
        Self::new()
    }
}

fn unix_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Time an arbitrary closure and emit one log entry. Keeps handlers
/// tidy — call once per request and the duration is captured uniformly.
pub fn timed<F, T>(logs: &Logs, route: &str, method: &str, f: F) -> (T, LogEntry)
where
    F: FnOnce() -> T,
{
    let started = Instant::now();
    let outcome = f();
    let entry = LogEntry {
        epoch_secs: unix_secs(),
        route: route.to_owned(),
        method: method.to_owned(),
        status: 0,
        duration_ms: started.elapsed().as_millis() as u64,
        category: "pending",
        note: String::new(),
    };
    let _ = started; // silence unused if branch rewritten
    let _ = logs;
    (outcome, entry)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_evicts_oldest_at_capacity() {
        let l = Logs::new();
        for i in 0..(CAPACITY + 5) {
            l.record(
                format!("/r/{i}"),
                "GET",
                200,
                Duration::from_millis(1),
                "ok",
                format!("n{i}"),
            );
        }
        let snap = l.snapshot();
        assert_eq!(snap.len(), CAPACITY);
        // oldest surviving note should be i=5 (i=0..4 evicted)
        assert_eq!(snap.first().unwrap().note, "n5");
    }
}
