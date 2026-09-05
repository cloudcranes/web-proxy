//! Multi-source registry mirrors with rolling health scoring.
//!
//! Sources are declared in `sources.json` (or auto-generated from
//! `DOCKERHUB_*` / `GHCR_*` env vars). A background prober measures each
//! source's latency and Range compatibility on a fixed cadence, and the
//! downloader uses those scores to weight per-chunk dispatch.
//!
//! Score design (per source, refreshed each probe):
//!   score = max(0, success_rate) / (1 + p50_ms / 1000)
//! `score == 0` disables the source; higher = preferred.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use tokio::sync::RwLock;
use url::Url;

const PROBE_INTERVAL: Duration = Duration::from_secs(30);
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);
const PROBE_PATH: &str = "/v2/"; // small and supported by every registry

#[derive(Clone, Debug)]
pub struct SourceSpec {
    pub name: String,
    pub registry_url: String,
    pub token_url: String,
    pub token_service: String,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SourceStats {
    pub success: u64,
    pub failure: u64,
    pub p50_ms: u32,
    pub range_ok: bool,
    pub last_seen: Option<Instant>,
}

impl SourceStats {
    fn score(&self) -> f64 {
        let total = self.success + self.failure;
        let rate = if total == 0 {
            0.0
        } else {
            self.success as f64 / total as f64
        };
        rate / (1.0 + self.p50_ms as f64 / 1000.0)
    }
}

pub struct SourcePool {
    client: reqwest::Client,
    specs: Vec<SourceSpec>,
    stats: RwLock<Vec<SourceStats>>,
    weights: RwLock<Vec<f64>>,
    seq: AtomicU64,
}

impl SourcePool {
    pub fn new(client: reqwest::Client, specs: Vec<SourceSpec>) -> Arc<Self> {
        assert!(!specs.is_empty(), "at least one source is required");
        let n = specs.len();
        let pool = Arc::new(Self {
            client,
            specs,
            stats: RwLock::new(vec![SourceStats::default(); n]),
            weights: RwLock::new(vec![1.0; n]),
            seq: AtomicU64::new(0),
        });
        // Seed an initial probe so the first download isn't blind.
        let seed = Arc::clone(&pool);
        tokio::spawn(async move {
            Self::probe_all_arc(seed).await;
        });
        let prober = Arc::clone(&pool);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(PROBE_INTERVAL).await;
                Self::probe_all_arc(Arc::clone(&prober)).await;
            }
        });
        pool
    }

    pub fn specs(&self) -> &[SourceSpec] {
        &self.specs
    }

    pub fn name(&self, index: usize) -> &str {
        self.specs[index].name.as_str()
    }

    pub fn spec(&self, index: usize) -> &SourceSpec {
        &self.specs[index]
    }

    /// Pick the next source index for a chunk, weighted by current scores.
    /// Disabled sources (score 0) are skipped. Falls back to round-robin if
    /// nothing is healthy yet.
    pub async fn pick(&self) -> usize {
        let weights = self.weights.read().await;
        let n = weights.len();
        if n == 0 {
            return 0;
        }
        let total: f64 = weights.iter().sum();
        if total > 0.0 {
            // Cheap PRNG seeded by monotonic clock + atomic counter; no need
            // for crypto-grade randomness, just enough jitter that weighted
            // picks actually spread across sources.
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let seq = self.seq.fetch_add(1, Ordering::Relaxed);
            let mut seed = (now as u64) ^ seq.wrapping_mul(0x9E3779B97F4A7C15);
            seed ^= seed >> 30;
            seed = seed.wrapping_mul(0xBF58476D1CE4E5B9);
            seed ^= seed >> 27;
            seed = seed.wrapping_mul(0x94D049BB133111EB);
            seed ^= seed >> 31;
            let unit = (seed >> 11) as f64 / (1u64 << 53) as f64;
            let mut target = unit * total;
            for (i, w) in weights.iter().enumerate() {
                target -= *w;
                if target <= 0.0 {
                    return i;
                }
            }
        }
        // Fallback: round-robin through all sources.
        (self.seq.fetch_add(1, Ordering::Relaxed) as usize) % n
    }

    pub async fn weights_snapshot(&self) -> Vec<(String, f64, SourceStats)> {
        let stats = self.stats.read().await;
        let weights = self.weights.read().await;
        self.specs
            .iter()
            .enumerate()
            .map(|(i, spec)| {
                (
                    spec.name.clone(),
                    weights[i],
                    SourceStats {
                        success: stats[i].success,
                        failure: stats[i].failure,
                        p50_ms: stats[i].p50_ms,
                        range_ok: stats[i].range_ok,
                        last_seen: stats[i].last_seen,
                    },
                )
            })
            .collect()
    }

    /// Mark a failed chunk fetch (transient HTTP error). Triggers a re-probe
    /// so the source's score drops immediately rather than waiting for the
    /// next probe tick.
    pub async fn report_failure(&self, index: usize) {
        {
            let mut stats = self.stats.write().await;
            if let Some(slot) = stats.get_mut(index) {
                slot.failure = slot.failure.saturating_add(1);
                slot.last_seen = Some(Instant::now());
            }
        }
        self.recompute_weights().await;
    }

    pub async fn report_success(&self, index: usize) {
        let mut stats = self.stats.write().await;
        if let Some(slot) = stats.get_mut(index) {
            slot.success = slot.success.saturating_add(1);
            slot.last_seen = Some(Instant::now());
        }
    }

    async fn recompute_weights(&self) {
        let stats = self.stats.read().await;
        let mut weights = self.weights.write().await;
        for (i, w) in weights.iter_mut().enumerate() {
            *w = if i < stats.len() {
                stats[i].score()
            } else {
                0.0
            };
        }
    }

    async fn probe_all_arc(pool: Arc<Self>) {
        for i in 0..pool.specs.len() {
            let me = Arc::clone(&pool);
            tokio::spawn(async move { Self::probe_one_arc(me, i).await });
        }
        pool.recompute_weights().await;
    }

    async fn probe_one_arc(pool: Arc<Self>, index: usize) {
        let spec = pool.specs[index].clone();
        let url = match Url::parse(&spec.registry_url)
            .ok()
            .and_then(|u| u.join(PROBE_PATH).ok())
        {
            Some(u) => u,
            None => return,
        };
        let started = Instant::now();
        let result = tokio::time::timeout(
            PROBE_TIMEOUT,
            pool.client.head(url.clone()).header("Accept", "*/*").send(),
        )
        .await;
        let elapsed_ms = started.elapsed().as_millis() as u32;
        match result {
            Ok(Ok(response)) => {
                let status = response.status();
                let range_ok = response
                    .headers()
                    .get(reqwest::header::ACCEPT_RANGES)
                    .and_then(|v| v.to_str().ok())
                    .map(|v| v.eq_ignore_ascii_case("bytes"))
                    .unwrap_or(false);
                let mut stats = pool.stats.write().await;
                if let Some(slot) = stats.get_mut(index) {
                    if status.is_success() || status.as_u16() == 401 {
                        slot.success = slot.success.saturating_add(1);
                        slot.range_ok = slot.range_ok || range_ok;
                        slot.p50_ms = ewma_ms(slot.p50_ms, elapsed_ms);
                    } else {
                        slot.failure = slot.failure.saturating_add(1);
                    }
                    slot.last_seen = Some(Instant::now());
                }
            }
            _ => {
                let mut stats = pool.stats.write().await;
                if let Some(slot) = stats.get_mut(index) {
                    slot.failure = slot.failure.saturating_add(1);
                    slot.last_seen = Some(Instant::now());
                }
            }
        }
    }
}

fn ewma_ms(prev: u32, sample: u32) -> u32 {
    if prev == 0 {
        sample
    } else {
        // Light EWMA so single spikes don't dominate.
        ((prev as u64 * 7 + sample as u64) / 8) as u32
    }
}

/// Parse `sources.json`. When the file is absent (or the env var points
/// nowhere), return the auto-generated fallback that mirrors the historical
/// `DOCKERHUB_*` / `GHCR_*` env-var configuration.
pub fn load_or_default(
    path: Option<&str>,
    dockerhub: (String, String, String),
    ghcr: (String, String, String),
) -> Result<Vec<SourceSpec>> {
    if let Some(path) = path {
        match std::fs::read_to_string(path) {
            Ok(text) => return parse(&text),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => bail!("read sources.json {path}: {error}"),
        }
    }
    let (registry, token, service) = dockerhub;
    let fallback = vec![
        SourceSpec {
            name: "dockerhub".into(),
            registry_url: registry,
            token_url: token,
            token_service: service,
        },
        SourceSpec {
            name: "ghcr".into(),
            registry_url: ghcr.0,
            token_url: ghcr.1,
            token_service: ghcr.2,
        },
    ];
    Ok(fallback)
}

fn parse(text: &str) -> Result<Vec<SourceSpec>> {
    let parsed: serde_json::Value =
        serde_json::from_str(text).map_err(|e| anyhow!("sources.json: {e}"))?;
    let array = parsed
        .get("sources")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("sources.json missing 'sources' array"))?;
    let mut out = Vec::with_capacity(array.len());
    for item in array {
        let name = item
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("source missing 'name'"))?
            .to_owned();
        let registry = item
            .get("registry")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("source missing 'registry'"))?;
        let token = item
            .get("token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("source missing 'token'"))?;
        let service = item
            .get("service")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("source missing 'service'"))?
            .to_owned();
        validate_https(registry)?;
        validate_https(token)?;
        out.push(SourceSpec {
            name,
            registry_url: registry.to_owned(),
            token_url: token.to_owned(),
            token_service: service,
        });
    }
    if out.is_empty() {
        bail!("sources.json has no entries");
    }
    Ok(out)
}

fn validate_https(url: &str) -> Result<()> {
    let u = Url::parse(url)?;
    if u.scheme() != "https" {
        bail!("source url must be https: {url}");
    }
    Ok(())
}

/// Build a name→spec lookup for tests and the dashboard.
pub fn name_index(specs: &[SourceSpec]) -> HashMap<String, usize> {
    specs
        .iter()
        .enumerate()
        .map(|(i, s)| (s.name.clone(), i))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(name: &str) -> SourceSpec {
        SourceSpec {
            name: name.into(),
            registry_url: format!("https://{name}.example"),
            token_url: format!("https://{name}.example/token"),
            token_service: name.into(),
        }
    }

    #[test]
    fn parses_sources_json() {
        let text = r#"{
            "sources": [
                {"name": "dockerhub", "registry": "https://registry-1.docker.io", "token": "https://auth.docker.io/token", "service": "registry.docker.io"},
                {"name": "mirror", "registry": "https://mirror.example", "token": "https://mirror.example/token", "service": "mirror"}
            ]
        }"#;
        let parsed = parse(text).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].name, "dockerhub");
        assert_eq!(parsed[1].registry_url, "https://mirror.example");
    }

    #[test]
    fn rejects_non_https_source() {
        let text = r#"{
            "sources": [
                {"name": "bad", "registry": "http://insecure.example", "token": "https://insecure.example", "service": "x"}
            ]
        }"#;
        assert!(parse(text).is_err());
    }

    #[test]
    fn empty_sources_errors() {
        let text = r#"{"sources": []}"#;
        assert!(parse(text).is_err());
    }

    #[test]
    fn fallback_has_two_sources() {
        let fallback = load_or_default(
            Some("/nonexistent/path/sources.json"),
            (
                "https://registry-1.docker.io".into(),
                "https://auth.docker.io/token".into(),
                "registry.docker.io".into(),
            ),
            (
                "https://ghcr.io".into(),
                "https://ghcr.io/token".into(),
                "ghcr.io".into(),
            ),
        )
        .unwrap();
        assert_eq!(fallback.len(), 2);
        assert_eq!(fallback[0].name, "dockerhub");
        assert_eq!(fallback[1].name, "ghcr");
    }

    #[test]
    fn ewma_smooths_samples() {
        assert_eq!(ewma_ms(0, 1000), 1000);
        assert_eq!(ewma_ms(1000, 1000), 1000);
        assert_eq!(ewma_ms(100, 5000), 712);
    }

    #[tokio::test]
    async fn pool_picks_when_all_weights_zero() {
        let specs = vec![spec("a"), spec("b")];
        let client = reqwest::Client::new();
        let pool = SourcePool::new(client, specs);
        let i = pool.pick().await;
        assert!(i < 2);
    }

    #[tokio::test]
    async fn failure_recording_drops_score() {
        let specs = vec![spec("a"), spec("b")];
        let client = reqwest::Client::new();
        let pool = SourcePool::new(client, specs);
        pool.report_success(0).await;
        pool.report_success(1).await;
        pool.report_failure(0).await;
        let snap = pool.weights_snapshot().await;
        assert!(
            snap[0].1 < snap[1].1,
            "failed source should drop below healthy"
        );
    }

    #[tokio::test]
    async fn name_index_round_trip() {
        let specs = vec![spec("a"), spec("b")];
        let idx = name_index(&specs);
        assert_eq!(idx["a"], 0);
        assert_eq!(idx["b"], 1);
    }
}
