//! Content-addressed blob cache and small-object manifest cache.
//!
//! OCI blobs are immutable and identified by their sha256 digest, so the
//! digest is the cache key and the filesystem is the source of truth:
//! `<root>/sha256/<2 hex>/<64 hex>`. Partial downloads land next to the
//! final path as `.<hex>.<counter>.part` and are renamed into place only
//! after the streamed sha256 sum matches.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use tokio::sync::Mutex;

const PART_MAX_AGE: Duration = Duration::from_secs(3600);
const EVICT_TARGET_NUM: u64 = 9;
const EVICT_TARGET_DEN: u64 = 10;

static PART_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Default)]
pub struct Stats {
    pub blob_hits: AtomicU64,
    pub blob_misses: AtomicU64,
    pub bytes_from_cache: AtomicU64,
    pub bytes_from_upstream: AtomicU64,
}

pub struct CachedBlob {
    pub path: PathBuf,
    pub size: u64,
}

pub struct DiskCache {
    root: PathBuf,
    max_bytes: u64,
    on_disk: AtomicU64,
    inflight: Mutex<HashMap<String, std::sync::Arc<Mutex<()>>>>,
    evict_lock: Mutex<()>,
}

impl DiskCache {
    pub fn new(root: PathBuf, max_bytes: u64) -> Self {
        Self {
            root,
            max_bytes,
            on_disk: AtomicU64::new(0),
            inflight: Mutex::new(HashMap::new()),
            evict_lock: Mutex::new(()),
        }
    }

    /// Create the directory layout and rebuild the byte counter from disk.
    pub async fn init(&self) -> std::io::Result<()> {
        tokio::fs::create_dir_all(self.root.join("sha256")).await?;
        let total = self.scan_bytes().await;
        self.on_disk.store(total, Ordering::Relaxed);
        Ok(())
    }

    async fn scan_bytes(&self) -> u64 {
        let mut total = 0;
        let mut buckets = match tokio::fs::read_dir(self.root.join("sha256")).await {
            Ok(dir) => dir,
            Err(_) => return 0,
        };
        while let Ok(Some(bucket)) = buckets.next_entry().await {
            let mut files = match tokio::fs::read_dir(bucket.path()).await {
                Ok(dir) => dir,
                Err(_) => continue,
            };
            while let Ok(Some(entry)) = files.next_entry().await {
                if let Ok(meta) = entry.metadata().await {
                    if meta.is_file() && !entry.path().extension().is_some_and(|e| e == "part") {
                        total += meta.len();
                    }
                }
            }
        }
        total
    }

    pub fn blob_path(&self, hex: &str) -> PathBuf {
        self.root.join("sha256").join(&hex[..2]).join(hex)
    }

    pub async fn lookup(&self, hex: &str) -> Option<CachedBlob> {
        let path = self.blob_path(hex);
        let meta = tokio::fs::metadata(&path).await.ok()?;
        meta.is_file().then_some(CachedBlob {
            path,
            size: meta.len(),
        })
    }

    pub fn new_part_path(&self, hex: &str) -> PathBuf {
        let n = PART_COUNTER.fetch_add(1, Ordering::Relaxed);
        self.root
            .join("sha256")
            .join(&hex[..2])
            .join(format!(".{hex}.{n}.part"))
    }

    /// One download per digest, regardless of which repo referenced it.
    pub async fn inflight_lock(&self, hex: &str) -> std::sync::Arc<Mutex<()>> {
        let mut map = self.inflight.lock().await;
        if map.len() > 4096 {
            map.retain(|_, lock| std::sync::Arc::strong_count(lock) > 1);
        }
        map.entry(hex.to_owned()).or_default().clone()
    }

    /// Record a committed blob and trigger eviction when over capacity.
    pub async fn committed(&self, size: u64) {
        let total = self.on_disk.fetch_add(size, Ordering::Relaxed) + size;
        if total > self.max_bytes {
            self.evict_old().await;
        }
    }

    pub fn bytes_on_disk(&self) -> u64 {
        self.on_disk.load(Ordering::Relaxed)
    }

    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    async fn evict_old(&self) {
        let _guard = self.evict_lock.lock().await;
        if self.bytes_on_disk() <= self.max_bytes {
            return;
        }
        let mut files: Vec<(SystemTime, u64, PathBuf)> = Vec::new();
        let mut buckets = match tokio::fs::read_dir(self.root.join("sha256")).await {
            Ok(dir) => dir,
            Err(_) => return,
        };
        while let Ok(Some(bucket)) = buckets.next_entry().await {
            let mut entries = match tokio::fs::read_dir(bucket.path()).await {
                Ok(dir) => dir,
                Err(_) => continue,
            };
            while let Ok(Some(entry)) = entries.next_entry().await {
                let path = entry.path();
                let Ok(meta) = entry.metadata().await else {
                    continue;
                };
                if !meta.is_file() {
                    continue;
                }
                let modified = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                if path.extension().is_some_and(|e| e == "part")
                    && modified.elapsed().unwrap_or_default() < PART_MAX_AGE
                {
                    continue;
                }
                files.push((modified, meta.len(), path));
            }
        }
        files.sort_by_key(|(modified, _, _)| *modified);
        let target = self.max_bytes * EVICT_TARGET_NUM / EVICT_TARGET_DEN;
        let mut total = self.bytes_on_disk();
        for (_, size, path) in files {
            if total <= target {
                break;
            }
            if tokio::fs::remove_file(&path).await.is_ok() {
                total = total.saturating_sub(size);
            }
        }
        self.on_disk.store(total, Ordering::Relaxed);
    }

    /// Clear all cached blobs and temporary part files.
    pub async fn clear(&self) -> u64 {
        let _guard = self.evict_lock.lock().await;
        let freed = self.bytes_on_disk();
        let _ = tokio::fs::remove_dir_all(self.root.join("sha256")).await;
        let _ = tokio::fs::create_dir_all(self.root.join("sha256")).await;
        self.on_disk.store(0, Ordering::Relaxed);
        freed
    }
}

#[derive(Clone)]
pub struct CachedManifest {
    pub content_type: Option<String>,
    pub docker_digest: Option<String>,
    pub body: axum::body::Bytes,
    pub stored: Instant,
}

pub struct ManifestCache {
    ttl: Duration,
    max_entries: usize,
    entries: Mutex<HashMap<(String, String), CachedManifest>>,
}

impl ManifestCache {
    pub fn new(ttl: Duration, max_entries: usize) -> Self {
        Self {
            ttl,
            max_entries,
            entries: Mutex::new(HashMap::new()),
        }
    }

    pub async fn get(&self, key: &(String, String)) -> Option<CachedManifest> {
        let map = self.entries.lock().await;
        map.get(key)
            .filter(|entry| entry.stored.elapsed() < self.ttl)
            .cloned()
    }

    pub async fn put(&self, key: (String, String), value: CachedManifest) {
        let mut map = self.entries.lock().await;
        if map.len() >= self.max_entries {
            map.retain(|_, entry| entry.stored.elapsed() < self.ttl);
            if map.len() >= self.max_entries {
                map.clear();
            }
        }
        map.insert(key, value);
    }

    pub async fn clear(&self) {
        let mut map = self.entries.lock().await;
        map.clear();
    }
}
