//! Ring-buffer of recent gateway metrics for the dashboard charts.
//!
//! A background task samples the interesting counters every `SAMPLE_SECS`
//! (30 s) and keeps the last `CAP` points in memory. The dashboard reads
//! them via GET /metrics/history and renders sparklines.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use serde_json::{json, Value};
use tokio::sync::{Mutex, RwLock};

use crate::cache::Stats;
use crate::sources::SourcePool;

pub const CAP: usize = 240; // 30 s × 240 = 2 h of history

#[derive(Clone, Copy, Debug)]
pub struct Sample {
    pub epoch_secs: i64,
    pub hit_pct: f64,
    pub disk_pct: f64,
    pub active_downloads: f64,
    pub throughput_bps: f64,
}

#[derive(Default)]
struct State {
    buf: VecDeque<Sample>,
}

pub struct History {
    state: Mutex<State>,
}

impl History {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State::default()),
        }
    }

    pub async fn snapshot(&self) -> Vec<Sample> {
        self.state.lock().await.buf.iter().copied().collect()
    }

    async fn push(&self, sample: Sample) {
        let mut s = self.state.lock().await;
        if s.buf.len() == CAP {
            s.buf.pop_front();
        }
        s.buf.push_back(sample);
    }
}

pub fn spawn(
    history: Arc<History>,
    stats: Arc<Stats>,
    sources: Arc<RwLock<Arc<SourcePool>>>,
    disk_bytes: Arc<dyn Fn() -> u64 + Send + Sync>,
    disk_cap: u64,
) {
    tokio::spawn(async move {
        let interval = Duration::from_secs(30);
        loop {
            tokio::time::sleep(interval).await;
            let total = stats.blob_hits.load(std::sync::atomic::Ordering::Relaxed)
                + stats.blob_misses.load(std::sync::atomic::Ordering::Relaxed);
            let hits = stats.blob_hits.load(std::sync::atomic::Ordering::Relaxed);
            let hit_pct = if total > 0 {
                100.0 * hits as f64 / total as f64
            } else {
                0.0
            };
            let used = disk_bytes();
            let disk_pct = if disk_cap > 0 {
                100.0 * (used as f64) / (disk_cap as f64)
            } else {
                0.0
            };
            let active = stats
                .active_downloads
                .try_lock()
                .map(|g| g.len() as f64)
                .unwrap_or(0.0);
            let pool = sources.read().await.clone();
            let snapshot = pool.weights_snapshot().await;
            let mut sum_bps: u64 = 0;
            for (_, weight, stats) in snapshot {
                if weight > 0.0 {
                    sum_bps += stats.throughput_bps;
                }
            }
            let sample = Sample {
                epoch_secs: chrono_unix_secs(),
                hit_pct,
                disk_pct: disk_pct.min(100.0),
                active_downloads: active,
                throughput_bps: sum_bps as f64,
            };
            history.push(sample).await;
        }
    });
}

/// Current samples from the snapshot — not the history buffer.
pub fn snapshot_now(stats: &Stats, disk_bytes: u64, disk_cap: u64) -> Sample {
    use std::sync::atomic::Ordering;
    let total = stats.blob_hits.load(Ordering::Relaxed) + stats.blob_misses.load(Ordering::Relaxed);
    let hits = stats.blob_hits.load(Ordering::Relaxed);
    let hit_pct = if total > 0 {
        100.0 * hits as f64 / total as f64
    } else {
        0.0
    };
    let disk_pct = if disk_cap > 0 {
        (100.0 * disk_bytes as f64 / disk_cap as f64).min(100.0)
    } else {
        0.0
    };
    Sample {
        epoch_secs: chrono_unix_secs(),
        hit_pct,
        disk_pct,
        active_downloads: 0.0,
        throughput_bps: 0.0,
    }
}

/// Serialise the buffer into the JSON shape the dashboard expects.
pub async fn history_json(history: &History, current: Sample) -> Result<Value> {
    let buf = history.snapshot().await;
    let series: Vec<Value> = buf
        .iter()
        .map(|s| {
            json!({
                "t": s.epoch_secs,
                "hit": s.hit_pct,
                "disk": s.disk_pct,
                "active": s.active_downloads,
                "throughput": s.throughput_bps,
            })
        })
        .collect();
    Ok(json!({
        "current": {
            "hit": current.hit_pct,
            "disk": current.disk_pct,
            "active": current.active_downloads,
            "throughput": current.throughput_bps,
        },
        "history": series,
    }))
}

// Local replacement for chrono dependency: avoid pulling a whole crate for
// Unix-epoch-seconds. Pulled straight from system time; safe to be off by
// the few hundred ms the bootstrap path took.
fn chrono_unix_secs() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pushes_then_evicts_oldest_at_cap() {
        let h = History::new();
        for i in 0..(CAP + 5) {
            h.push(Sample {
                epoch_secs: i as i64,
                hit_pct: 0.0,
                disk_pct: 0.0,
                active_downloads: 0.0,
                throughput_bps: 0.0,
            })
            .await;
        }
        let snap = h.snapshot().await;
        assert_eq!(snap.len(), CAP);
        assert_eq!(snap.first().unwrap().epoch_secs, 5);
        assert_eq!(snap.last().unwrap().epoch_secs, (CAP + 4) as i64);
    }

    #[test]
    fn snapshot_now_disk_pct_clamped() {
        let s = Stats::default();
        let snap = snapshot_now(&s, 9500, 10_000);
        assert_eq!(snap.disk_pct, 95.0);
        let snap = snapshot_now(&s, 11_000, 10_000);
        assert_eq!(snap.disk_pct, 100.0);
    }
}
