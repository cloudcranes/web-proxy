//! Multi-source chunked blob downloader with bitmap-backed resumable downloads.
//!
//! Given a digest, content-length, and a `SourcePool`, splits the blob into
//! fixed-size chunks and fetches each from the highest-scoring source that still
//! supports Range. Each chunk writes to its own offset in a `.part` file;
//! when all chunks land and the full sha256 matches, the part is renamed to
//! the cache key.
//!
//! Resumable downloads:
//! Alongside `<hex>.part`, a `<hex>.bitmap` sidecar starts with an 8-byte
//! little-endian header holding the chunk size this attempt used, followed by
//! 1 byte per chunk (1 = written & verified, 0 = pending). A resumed attempt
//! reuses the recorded chunk size so offsets line up; if a prior download was
//! cancelled mid-way, already-fetched chunks are reused directly from disk
//! without touching upstream networks.

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use axum::body::Bytes;
use futures_util::StreamExt;
use reqwest::Client;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};
use url::Url;

use crate::cache::{ActiveDownload, ActiveDownloadGuard, DiskCache, Stats};
use crate::sources::SourcePool;

pub const CHUNK_BYTES: u64 = 8 * 1024 * 1024;
const CHUNK_TIMEOUT: Duration = Duration::from_secs(180);
const MAX_CHUNK_RETRIES: usize = 3;

#[derive(Debug, Clone, Copy)]
pub struct Chunk {
    pub index: usize,
    pub offset: u64,
    pub length: u64,
}

pub fn split(total: u64) -> Vec<Chunk> {
    split_with(total, CHUNK_BYTES)
}

pub fn split_with(total: u64, chunk_bytes: u64) -> Vec<Chunk> {
    if total == 0 {
        return Vec::new();
    }
    let chunk_bytes = chunk_bytes.max(1);
    let mut chunks = Vec::with_capacity((total / chunk_bytes + 1) as usize);
    let mut offset = 0;
    let mut index = 0;
    while offset < total {
        let length = (total - offset).min(chunk_bytes);
        chunks.push(Chunk {
            index,
            offset,
            length,
        });
        offset += length;
        index += 1;
    }
    chunks
}

/// Existing bitmap sidecar: the chunk size it was written with plus per-chunk
/// completion flags. `None` means "no usable sidecar" (absent, truncated, or
/// zero header) and the download should start fresh.
struct ExistingBitmap {
    chunk_bytes: u64,
    done: Vec<bool>,
}

async fn load_bitmap(bitmap_path: &Path) -> Option<ExistingBitmap> {
    let bytes = tokio::fs::read(bitmap_path).await.ok()?;
    if bytes.len() < 8 {
        return None;
    }
    let chunk_bytes = u64::from_le_bytes(bytes[..8].try_into().ok()?);
    if chunk_bytes == 0 {
        return None;
    }
    let done: Vec<bool> = bytes[8..].iter().map(|&b| b == 1).collect();
    Some(ExistingBitmap { chunk_bytes, done })
}

/// Persist updated bitmap state (v2: chunk-size header + completion flags).
async fn save_bitmap(bitmap_path: &Path, chunk_bytes: u64, state: &[bool]) {
    let mut bytes = Vec::with_capacity(8 + state.len());
    bytes.extend_from_slice(&chunk_bytes.to_le_bytes());
    bytes.extend(state.iter().map(|&b| if b { 1 } else { 0 }));
    let _ = tokio::fs::write(bitmap_path, &bytes).await;
}

/// Drive the chunked download to completion, emitting pieces in offset order
/// to `tx` for the client. Returns the final sha256 hex on success.
pub async fn download(
    client: Client,
    pool: Arc<SourcePool>,
    cache: Arc<DiskCache>,
    stats: Arc<Stats>,
    registry: String,
    path: String,
    token: String,
    digest: String,
    total_size: u64,
    part_path: PathBuf,
    final_path: PathBuf,
    mut tx: Option<mpsc::Sender<Result<Bytes, std::io::Error>>>,
) -> Result<String> {
    let bitmap_path = part_path.with_extension("bitmap");

    let dir = part_path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("part path has no parent"))?;
    tokio::fs::create_dir_all(dir).await?;

    // Adaptive plan from measured source throughput. A usable sidecar pins
    // the chunk size to whatever the interrupted attempt used so offsets and
    // bitmap flags line up; concurrency is always free to adapt.
    let (planned_chunk_bytes, max_concurrent) = pool.chunk_plan().await;
    let existing = load_bitmap(&bitmap_path).await;
    let (chunk_bytes, prior_flags) = match existing {
        Some(bitmap) if bitmap.chunk_bytes > 0 => (bitmap.chunk_bytes, Some(bitmap.done)),
        _ => (planned_chunk_bytes, None),
    };

    let chunks = split_with(total_size, chunk_bytes);
    let chunks_total = chunks.len();
    let existing_bitmap = match prior_flags {
        Some(flags) if flags.len() == chunks_total => flags,
        _ => vec![false; chunks_total],
    };
    let resumed_count = existing_bitmap.iter().filter(|&&b| b).count();
    if resumed_count > 0 {
        info!(
            resumed = resumed_count,
            total = chunks_total,
            "resuming partial download from disk"
        );
    } else {
        debug!(
            chunk_mib = chunk_bytes / 1024 / 1024,
            concurrency = max_concurrent,
            "chunk plan"
        );
    }

    let file = tokio::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&part_path)
        .await
        .with_context(|| format!("open {}", part_path.display()))?;

    let shared = Arc::new(tokio::sync::Mutex::new(file));
    let total_received = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let sem = Arc::new(tokio::sync::Semaphore::new(max_concurrent));

    // Live progress for the dashboard; the guard removes the entry on any
    // exit path (success, exhausted retries, client abort).
    let download_guard = ActiveDownloadGuard::new(
        Arc::clone(&stats),
        ActiveDownload {
            path: path.clone(),
            digest: digest.clone(),
            total: total_size,
            chunks_total,
            resumed: resumed_count > 0,
            started: Instant::now(),
            chunks_done: std::sync::atomic::AtomicU64::new(resumed_count as u64),
            bytes_done: std::sync::atomic::AtomicU64::new(0),
        },
    );
    let download_progress = download_guard.handle();

    let mut tasks: tokio::task::JoinSet<anyhow::Result<(Chunk, bool)>> =
        tokio::task::JoinSet::new();
    for chunk in &chunks {
        let is_already_done = existing_bitmap[chunk.index];
        let sem = Arc::clone(&sem);
        let shared = Arc::clone(&shared);
        let total_received = Arc::clone(&total_received);
        let client = client.clone();
        let pool = Arc::clone(&pool);
        let token = token.clone();
        let path = path.clone();
        let chunk = *chunk;

        tasks.spawn(async move {
            if is_already_done {
                return Ok::<_, anyhow::Error>((chunk, true));
            }
            let _permit = sem.acquire_owned().await.expect("semaphore closed");
            let mut attempt = 0;
            loop {
                let source_index = pool.pick().await;
                let spec = pool.spec(source_index);
                let url = match build_blob_url(&spec.registry_url, &path) {
                    Ok(url) => url,
                    Err(error) => {
                        pool.report_failure(source_index).await;
                        attempt += 1;
                        if attempt > MAX_CHUNK_RETRIES {
                            return Err(error);
                        }
                        continue;
                    }
                };
                let started = Instant::now();
                match fetch_chunk(
                    &client,
                    &url,
                    &token,
                    chunk.offset,
                    chunk.length,
                    shared.as_ref(),
                    total_received.as_ref(),
                )
                .await
                {
                    Ok(bytes) => {
                        pool.report_success(source_index).await;
                        pool.report_throughput(source_index, bytes, started.elapsed())
                            .await;
                        return Ok::<_, anyhow::Error>((chunk, false));
                    }
                    Err(error) => {
                        warn!(
                            source = spec.name.as_str(),
                            offset = chunk.offset,
                            error = %error,
                            "chunk fetch failed"
                        );
                        pool.report_failure(source_index).await;
                        attempt += 1;
                        if attempt > MAX_CHUNK_RETRIES {
                            return Err(error);
                        }
                    }
                }
            }
        });
    }

    let mut ordered: Vec<Option<Bytes>> = vec![None; chunks_total];
    let mut next_offset: u64 = 0;
    let mut chunks_remaining = chunks_total;
    let mut current_bitmap = existing_bitmap;
    let mut hasher = ring::digest::Context::new(&ring::digest::SHA256);

    while chunks_remaining > 0 {
        // JoinSet removes finished tasks as they yield, so completed chunk
        // tasks are never awaited twice (re-polling a JoinHandle panics).
        let joined = tasks
            .join_next()
            .await
            .expect("chunk tasks remain while chunks_remaining > 0");
        let outcome = match joined {
            Ok(pair) => pair,
            Err(join_error) => return Err(anyhow::anyhow!("chunk task aborted: {join_error}")),
        };
        let (chunk, from_cache) = match outcome {
            Ok(pair) => pair,
            Err(error) => return Err(error.context("a chunk fetch exhausted retries")),
        };

        if !from_cache {
            current_bitmap[chunk.index] = true;
            save_bitmap(&bitmap_path, chunk_bytes, &current_bitmap).await;
        }

        // Read back bytes from disk file for hashing and ordered client streaming
        let bytes = {
            let mut file = shared.lock().await;
            file.seek(std::io::SeekFrom::Start(chunk.offset))
                .await
                .with_context(|| format!("seek {}", chunk.offset))?;
            let mut buf = vec![0u8; chunk.length as usize];
            file.read_exact(&mut buf)
                .await
                .with_context(|| format!("read {} bytes at {}", chunk.length, chunk.offset))?;
            Bytes::from(buf)
        };
        ordered[chunk.index] = Some(bytes);

        // Emit any contiguous prefix to the client; hash here too, because
        // emission walks offsets in blob order while chunk tasks complete in
        // whatever order the network delivers them.
        while next_offset < total_size {
            let next_index = (next_offset / chunk_bytes) as usize;
            if next_index >= chunks_total {
                break;
            }
            match ordered[next_index].take() {
                Some(bytes) => {
                    hasher.update(&bytes);
                    next_offset += bytes.len() as u64;
                    download_progress
                        .chunks_done
                        .fetch_add(1, Ordering::Relaxed);
                    download_progress
                        .bytes_done
                        .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    let mut client_disconnected = false;
                    if let Some(sender) = tx.as_ref() {
                        if sender.send(Ok(bytes)).await.is_err() {
                            client_disconnected = true;
                        }
                    }
                    if client_disconnected {
                        tx = None;
                    }
                }
                None => break,
            }
        }
        chunks_remaining -= 1;
    }

    // Flush + finalize
    {
        let mut file = shared.lock().await;
        file.flush().await.context("flush part file")?;
    }
    drop(shared);

    let actual = hex_encode(hasher.finish().as_ref());
    let expected = digest.strip_prefix("sha256:").unwrap_or_default();
    if actual != expected {
        let _ = tokio::fs::remove_file(&part_path).await;
        let _ = tokio::fs::remove_file(&bitmap_path).await;
        bail!(
            "blob digest mismatch: expected {expected}, got {actual} (registry={registry} path={path})"
        );
    }

    if let Some(parent) = final_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::rename(&part_path, &final_path)
        .await
        .with_context(|| format!("commit {} -> {}", part_path.display(), final_path.display()))?;

    // Cleanup sidecar bitmap once committed cleanly
    let _ = tokio::fs::remove_file(&bitmap_path).await;

    stats
        .bytes_from_upstream
        .fetch_add(total_size, Ordering::Relaxed);
    cache.committed(total_size).await;
    Ok(actual)
}

async fn fetch_chunk(
    client: &Client,
    initial_url: &Url,
    token: &str,
    offset: u64,
    length: u64,
    file: &tokio::sync::Mutex<tokio::fs::File>,
    total_received: &std::sync::atomic::AtomicU64,
) -> Result<u64> {
    let mut current_url = initial_url.clone();
    let mut use_auth = true;
    let mut redirects = 0;
    const MAX_REDIRECTS: usize = 5;

    let response = loop {
        let mut builder = client.get(current_url.clone()).header(
            reqwest::header::RANGE,
            format!("bytes={}-{}", offset, offset + length - 1),
        );
        if use_auth && !token.is_empty() {
            builder = builder.bearer_auth(token);
        }

        let resp = tokio::time::timeout(CHUNK_TIMEOUT, builder.send())
            .await
            .map_err(|_| anyhow::anyhow!("chunk {offset} timed out after {CHUNK_TIMEOUT:?}"))??;

        if resp.status().is_redirection() {
            if redirects >= MAX_REDIRECTS {
                bail!("chunk {offset} exceeded max redirects");
            }
            redirects += 1;
            let location = resp
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| anyhow::anyhow!("missing Location header in redirect"))?;
            let next_url = current_url.join(location)?;
            if next_url.host_str() != current_url.host_str() {
                // Cross-host redirect (e.g. to CDN): drop Bearer auth as S3/R2 signed URLs use query auth
                use_auth = false;
            }
            current_url = next_url;
            continue;
        }

        break resp;
    };

    let status = response.status();
    if status == reqwest::StatusCode::RANGE_NOT_SATISFIABLE {
        // A 416 means nothing was written for this chunk. Treating it as
        // success would mark the chunk done and leave a hole in the part
        // file, so fail instead and let the retry loop handle it.
        bail!("chunk {offset} HTTP 416 range not satisfiable");
    }
    if status == reqwest::StatusCode::OK {
        // A 200 to a Range request means the upstream ignored the range and
        // would stream the whole blob from byte 0 - writing that at a chunk
        // offset corrupts the assembly (observed intermittently on CDN
        // edges). Fail and retry, hoping for a range-aware endpoint.
        bail!("chunk {offset} upstream ignored Range (HTTP 200 instead of 206)");
    }
    if !status.is_success() {
        bail!("chunk {offset} HTTP {status}");
    }
    if let Some(content_range) = response
        .headers()
        .get(reqwest::header::CONTENT_RANGE)
        .and_then(|v| v.to_str().ok())
    {
        // "bytes <start>-<end>/<total>": a start that drifts from the
        // requested offset would silently corrupt the assembled blob.
        let reported_start = content_range
            .strip_prefix("bytes ")
            .and_then(|rest| rest.split('-').next())
            .and_then(|start| start.parse::<u64>().ok());
        if let Some(start) = reported_start {
            if start != offset {
                bail!("chunk {offset} upstream returned Content-Range starting at {start}");
            }
        }
    }
    let mut stream = response.bytes_stream();
    let mut cursor = offset;
    let mut local_total = 0u64;
    futures_util::pin_mut!(stream);
    while let Some(item) = stream.next().await {
        let chunk = item.map_err(|e| anyhow::anyhow!("chunk {offset} read: {e}"))?;
        {
            let mut file = file.lock().await;
            file.seek(std::io::SeekFrom::Start(cursor))
                .await
                .with_context(|| format!("seek {cursor}"))?;
            file.write_all(&chunk)
                .await
                .with_context(|| format!("write {} bytes at {cursor}", chunk.len()))?;
        }
        cursor += chunk.len() as u64;
        local_total += chunk.len() as u64;
    }
    // tokio::fs::File buffers writes; a process kill would silently drop the
    // buffered tail while the caller still marks the chunk done. Flushing
    // here makes the bitmap entry truthful for a later resumed download.
    {
        let mut file = file.lock().await;
        file.flush()
            .await
            .with_context(|| format!("flush chunk {offset}"))?;
    }
    total_received.fetch_add(local_total, Ordering::Relaxed);
    Ok(local_total)
}

fn build_blob_url(registry_url: &str, path: &str) -> Result<Url> {
    let base = Url::parse(registry_url).context("parse registry url")?;
    base.join(path.trim_start_matches('/')).context("join path")
}

fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_small_blob_is_single_chunk() {
        let chunks = split(1024);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].offset, 0);
        assert_eq!(chunks[0].length, 1024);
        assert_eq!(chunks[0].index, 0);
    }

    #[test]
    fn split_aligned_chunks() {
        let chunks = split(CHUNK_BYTES * 3);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].offset, 0);
        assert_eq!(chunks[0].index, 0);
        assert_eq!(chunks[1].offset, CHUNK_BYTES);
        assert_eq!(chunks[1].index, 1);
        assert_eq!(chunks[2].offset, CHUNK_BYTES * 2);
        assert_eq!(chunks[2].index, 2);
        assert_eq!(chunks[2].length, CHUNK_BYTES);
    }

    #[test]
    fn split_unaligned_tail_chunk() {
        let chunks = split(CHUNK_BYTES + 1234);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[1].offset, CHUNK_BYTES);
        assert_eq!(chunks[1].length, 1234);
        assert_eq!(chunks[1].index, 1);
    }

    #[test]
    fn split_empty_blob_is_no_chunks() {
        assert!(split(0).is_empty());
    }

    #[test]
    fn hex_round_trip() {
        let bytes = b"\x00\x01\xfe\xff";
        assert_eq!(hex_encode(bytes), "0001feff");
    }

    #[tokio::test]
    async fn bitmap_persistence_round_trip() {
        let tmp = std::env::temp_dir().join("test_bitmap_persist.bitmap");
        let initial = vec![true, false, true, true];
        save_bitmap(&tmp, CHUNK_BYTES, &initial).await;
        let loaded = load_bitmap(&tmp).await.expect("bitmap readable");
        assert_eq!(loaded.chunk_bytes, CHUNK_BYTES);
        assert_eq!(loaded.done, initial);
        let _ = tokio::fs::remove_file(&tmp).await;
    }

    #[tokio::test]
    async fn bitmap_rejects_truncated_and_zero_header() {
        let tmp = std::env::temp_dir().join("test_bitmap_bad_header.bitmap");
        tokio::fs::write(&tmp, [0u8; 4]).await.unwrap();
        assert!(load_bitmap(&tmp).await.is_none());
        let zero_header = {
            let mut bytes = 0u64.to_le_bytes().to_vec();
            bytes.push(1);
            bytes
        };
        tokio::fs::write(&tmp, zero_header).await.unwrap();
        assert!(load_bitmap(&tmp).await.is_none());
        let _ = tokio::fs::remove_file(&tmp).await;
    }

    #[test]
    fn split_with_uses_requested_chunk_size() {
        let chunks = split_with(CHUNK_BYTES * 4, CHUNK_BYTES * 2);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].length, CHUNK_BYTES * 2);
        assert_eq!(chunks[1].offset, CHUNK_BYTES * 2);
    }

    #[test]
    fn chunk_plan_defaults_without_measurements() {
        assert_eq!(
            crate::sources::chunk_plan_for(0),
            (
                crate::sources::CHUNK_MIN_BYTES,
                crate::sources::CONCURRENCY_MIN
            )
        );
    }

    #[test]
    fn chunk_plan_scales_chunk_band_and_concurrency() {
        // ~10 MiB/s: default chunk, min concurrency (ceil(20/8)=3 -> clamp 4).
        assert_eq!(
            crate::sources::chunk_plan_for(10 * 1024 * 1024),
            (8 * 1024 * 1024, 4)
        );
        // ~60 MiB/s: 16 MiB chunks, ceil(120/16)=8 streams.
        assert_eq!(
            crate::sources::chunk_plan_for(60 * 1024 * 1024),
            (16 * 1024 * 1024, 8)
        );
        // ~1 GiB/s: max chunk, concurrency clamped at 16.
        assert_eq!(
            crate::sources::chunk_plan_for(1024 * 1024 * 1024),
            (32 * 1024 * 1024, crate::sources::CONCURRENCY_MAX)
        );
    }
}
