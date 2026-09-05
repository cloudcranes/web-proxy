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
use tokio::sync::{Mutex, RwLock};
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
    /// EWMA of observed bulk throughput in bytes/sec from real chunk fetches
    /// (0 = never measured).
    pub throughput_bps: u64,
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
    /// Anonymous bearer tokens per (source index, repository), refreshed
    /// before expiry so parallel chunks of one blob reuse one fetch.
    token_cache: Mutex<HashMap<(usize, String), (String, Instant)>>,
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
            token_cache: Mutex::new(HashMap::new()),
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
                        throughput_bps: stats[i].throughput_bps,
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

    /// Feed a real bulk-transfer measurement into the per-source throughput
    /// EWMA. Drives the downloader's adaptive chunk plan.
    pub async fn report_throughput(&self, index: usize, bytes: u64, elapsed: Duration) {
        if bytes == 0 || elapsed.is_zero() {
            return;
        }
        let sample_bps = ((bytes as u128 * 1_000_000) / elapsed.as_micros().max(1) as u128) as u64;
        let mut stats = self.stats.write().await;
        if let Some(slot) = stats.get_mut(index) {
            slot.throughput_bps = if slot.throughput_bps == 0 {
                sample_bps
            } else {
                // 30% weight on the newest chunk, so the plan tracks drift
                // within a handful of chunks.
                (slot.throughput_bps * 7 + sample_bps * 3) / 10
            };
            slot.last_seen = Some(Instant::now());
        }
    }

    /// Adaptive chunk plan for the next download: (chunk_bytes,
    /// max_concurrent_chunks), derived from measured aggregate throughput.
    /// (8 MiB, 4) until real measurements exist.
    pub async fn chunk_plan(&self) -> (u64, usize) {
        let stats = self.stats.read().await;
        let total_bps: u64 = stats.iter().map(|s| s.throughput_bps).sum();
        chunk_plan_for(total_bps)
    }

    /// Anonymous pull token for one source's blob endpoint. Empty
    /// `token_url` marks an anonymous source (no Authorization at all).
    /// Tokens are cached per (source, repository) until shortly before
    /// expiry because parallel chunks of one blob would otherwise each
    /// trigger a fetch.
    pub async fn blob_token(&self, index: usize, path: &str) -> String {
        let spec = match self.specs.get(index) {
            Some(spec) => spec,
            None => return String::new(),
        };
        if spec.token_url.is_empty() {
            return String::new();
        }
        // /v2/<repo>/blobs/<digest> → repository scope
        let repo = path
            .strip_prefix("/v2/")
            .and_then(|rest| rest.find("/blobs/").map(|i| &rest[..i]))
            .unwrap_or_default();
        if repo.is_empty() {
            return String::new();
        }
        let key = (index, repo.to_owned());
        {
            let cache = self.token_cache.lock().await;
            if let Some((token, expires)) = cache.get(&key) {
                if Instant::now() < *expires {
                    return token.clone();
                }
            }
        }
        let mut url = match Url::parse(&spec.token_url) {
            Ok(url) => url,
            Err(_) => return String::new(),
        };
        {
            let mut query = url.query_pairs_mut();
            if !spec.token_service.is_empty() {
                query.append_pair("service", &spec.token_service);
            }
            query.append_pair("scope", &format!("repository:{repo}:pull"));
        }
        let fetched = tokio::time::timeout(Duration::from_secs(15), self.client.get(url).send())
            .await
            .ok()
            .and_then(|r| r.ok());
        // Parse the JSON body manually (reqwest runs without the json
        // feature); token bodies are small and buffered by reqwest.
        let parsed = match fetched {
            Some(response) => match response.text().await {
                Ok(text) => serde_json::from_str::<serde_json::Value>(&text).ok(),
                Err(_) => None,
            },
            None => None,
        };
        let token = parsed
            .as_ref()
            .and_then(|v| {
                v.get("token")
                    .or_else(|| v.get("access_token"))
                    .and_then(|t| t.as_str())
            })
            .unwrap_or_default()
            .to_owned();
        if token.is_empty() {
            // No usable token: let the chunk 401 and the failure stats push
            // this source out of rotation.
            return String::new();
        }
        let ttl = parsed
            .as_ref()
            .and_then(|v| v.get("expires_in"))
            .and_then(|e| e.as_u64())
            .unwrap_or(300)
            .saturating_sub(30)
            .max(30);
        let mut cache = self.token_cache.lock().await;
        if cache.len() > 4096 {
            cache.retain(|_, (_, expires)| Instant::now() < *expires);
        }
        cache.insert(
            key,
            (token.clone(), Instant::now() + Duration::from_secs(ttl)),
        );
        token
    }

    pub async fn trigger_probe(self: &Arc<Self>) {
        Self::probe_all_arc(Arc::clone(self)).await;
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

/// Chunk-size bands and concurrency bounds for the adaptive downloader.
/// Chunk size only steps in coarse bands so a resumed download (which must
/// reuse the previous attempt's chunk size, recorded in the bitmap) is not
/// invalidated by tiny throughput drift.
pub const CHUNK_MIN_BYTES: u64 = 8 * 1024 * 1024;
pub const CHUNK_MAX_BYTES: u64 = 32 * 1024 * 1024;
pub const CONCURRENCY_MIN: usize = 4;
pub const CONCURRENCY_MAX: usize = 16;
/// Aim to keep roughly this much data in flight across all chunk streams.
const TARGET_INFLIGHT_SECS: u64 = 2;

pub fn chunk_plan_for(total_bps: u64) -> (u64, usize) {
    if total_bps == 0 {
        return (CHUNK_MIN_BYTES, CONCURRENCY_MIN);
    }
    let mib_per_sec = total_bps / (1024 * 1024);
    let chunk_bytes = match mib_per_sec {
        0..=49 => CHUNK_MIN_BYTES,
        50..=149 => 16 * 1024 * 1024,
        _ => CHUNK_MAX_BYTES,
    };
    let inflight = total_bps.saturating_mul(TARGET_INFLIGHT_SECS);
    let concurrency = (((inflight + chunk_bytes - 1) / chunk_bytes) as usize)
        .clamp(CONCURRENCY_MIN, CONCURRENCY_MAX);
    (chunk_bytes, concurrency)
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
            .unwrap_or_default()
            .to_owned();
        let service = item
            .get("service")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_owned();
        validate_https(registry)?;
        // Empty token marks an anonymous mirror that serves blobs without
        // auth; only auth-style sources need a reachable https token URL.
        if !token.is_empty() {
            validate_https(&token)?;
        }
        out.push(SourceSpec {
            name,
            registry_url: registry.to_owned(),
            token_url: token,
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

    #[tokio::test]
    async fn anonymous_source_skips_token_fetch() {
        let pool = SourcePool::new(
            reqwest::Client::new(),
            vec![SourceSpec {
                name: "anon".into(),
                registry_url: "https://anon.example".into(),
                token_url: String::new(),
                token_service: String::new(),
            }],
        );
        assert_eq!(
            pool.blob_token(0, "/v2/library/alpine/blobs/sha256:abc")
                .await,
            ""
        );
    }

    #[tokio::test]
    async fn token_cache_hit_avoids_refetch() {
        let pool = SourcePool::new(reqwest::Client::new(), vec![spec("tok")]);
        {
            let mut cache = pool.token_cache.lock().await;
            cache.insert(
                (0, "library/alpine".to_owned()),
                (
                    "cached-token".to_owned(),
                    Instant::now() + Duration::from_secs(300),
                ),
            );
        }
        assert_eq!(
            pool.blob_token(0, "/v2/library/alpine/blobs/sha256:abc")
                .await,
            "cached-token"
        );
    }

    #[tokio::test]
    async fn blob_token_unknown_source_is_empty() {
        let pool = SourcePool::new(reqwest::Client::new(), vec![spec("only")]);
        assert_eq!(
            pool.blob_token(9, "/v2/library/alpine/blobs/sha256:abc")
                .await,
            ""
        );
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
