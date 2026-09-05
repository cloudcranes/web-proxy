//! Multi-source chunked blob downloader.
//!
//! Given a digest, content-length, and a `SourcePool`, splits the blob into
//! fixed-size chunks and fetches each from the highest-scoring source that still
//! supports Range. Each chunk writes to its own offset in a `.part` file;
//! when all chunks land and the full sha256 matches, the part is renamed to
//! the cache key. While the client is connected, chunks are also emitted to
//! them in offset order via an `mpsc` so the visible stream stays correct.

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use axum::body::Bytes;
use futures_util::StreamExt;
use reqwest::Client;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tracing::warn;
use url::Url;

use crate::cache::{DiskCache, Stats};
use crate::sources::SourcePool;

pub const CHUNK_BYTES: u64 = 8 * 1024 * 1024;
const CHUNK_TIMEOUT: Duration = Duration::from_secs(180);
const MAX_CHUNK_RETRIES: usize = 3;
const MAX_CONCURRENT_CHUNKS: usize = 4;

#[derive(Debug, Clone, Copy)]
pub struct Chunk {
    pub offset: u64,
    pub length: u64,
}

pub fn split(total: u64) -> Vec<Chunk> {
    if total == 0 {
        return Vec::new();
    }
    let mut chunks = Vec::with_capacity((total / CHUNK_BYTES + 1) as usize);
    let mut offset = 0;
    while offset < total {
        let length = (total - offset).min(CHUNK_BYTES);
        chunks.push(Chunk { offset, length });
        offset += length;
    }
    chunks
}

/// Drive the chunked download to completion, emitting pieces in offset order
/// to `tx` for the client. Returns the final sha256 hex on success.
pub async fn download(
    client: &Client,
    pool: &SourcePool,
    cache: Arc<DiskCache>,
    stats: Arc<Stats>,
    registry: String,
    path: String,
    token: String,
    digest: String,
    total_size: u64,
    part_path: PathBuf,
    final_path: PathBuf,
    tx: Option<mpsc::Sender<Result<Bytes, std::io::Error>>>,
) -> Result<String> {
    let chunks = split(total_size);
    let dir = part_path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("part path has no parent"))?;
    tokio::fs::create_dir_all(dir).await?;
    let file = tokio::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&part_path)
        .await
        .with_context(|| format!("open {}", part_path.display()))?;

    let shared = Arc::new(tokio::sync::Mutex::new(file));
    let total_received = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let chunks_total = chunks.len();
    let sem = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_CHUNKS));

    let mut handles = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        let sem = Arc::clone(&sem);
        let shared = Arc::clone(&shared);
        let total_received = Arc::clone(&total_received);
        handles.push(tokio::spawn(async move {
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
                match fetch_chunk(
                    client,
                    &url,
                    &token,
                    chunk.offset,
                    chunk.length,
                    shared.as_ref(),
                    total_received.as_ref(),
                )
                .await
                {
                    Ok(()) => {
                        pool.report_success(source_index).await;
                        return Ok::<_, anyhow::Error>(chunk);
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
        }));
    }

    let mut ordered: Vec<Option<Bytes>> = vec![None; chunks_total];
    let mut next_offset: u64 = 0;
    let mut chunks_remaining = chunks_total;
    let mut hasher = ring::digest::Context::new(&ring::digest::SHA256);

    while chunks_remaining > 0 {
        // Reap whichever task finishes next.
        let ((result, _index), _, _) =
            futures_util::future::select_all(handles.iter_mut().map(|h| {
                Box::pin(async move {
                    let outcome = h.await.expect("join task");
                    (outcome, 0usize)
                })
            }))
            .await;
        let chunk = match result {
            Ok(chunk) => chunk,
            Err(error) => return Err(error.context("a chunk fetch exhausted retries")),
        };
        // Read the bytes we just wrote for hashing + ordered replay.
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
        hasher.update(&bytes);
        let slot_index = (chunk.offset / CHUNK_BYTES) as usize;
        ordered[slot_index as usize] = Some(bytes);

        // Emit any contiguous prefix to the client.
        while next_offset < total_size {
            let next_index = (next_offset / CHUNK_BYTES) as usize;
            if next_index >= chunks_total {
                break;
            }
            match ordered[next_index].take() {
                Some(bytes) => {
                    next_offset += bytes.len() as u64;
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

    // Flush + finalize.
    {
        let file = shared.lock().await;
        let mut file = file;
        file.flush().await.context("flush part file")?;
    }
    drop(shared);
    let actual = hex_encode(hasher.finish().as_ref());
    let expected = digest.strip_prefix("sha256:").unwrap_or_default();
    if actual != expected {
        let _ = tokio::fs::remove_file(&part_path).await;
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
    stats
        .bytes_from_upstream
        .fetch_add(total_size, Ordering::Relaxed);
    cache.committed(total_size).await;
    Ok(actual)
}

async fn fetch_chunk(
    client: &Client,
    url: &Url,
    token: &str,
    offset: u64,
    length: u64,
    file: &tokio::sync::Mutex<tokio::fs::File>,
    total_received: &std::sync::atomic::AtomicU64,
) -> Result<()> {
    let response = tokio::time::timeout(
        CHUNK_TIMEOUT,
        client
            .get(url.clone())
            .bearer_auth(token)
            .header(
                reqwest::header::RANGE,
                format!("bytes={}-{}", offset, offset + length - 1),
            )
            .send(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("chunk {offset} timed out after {CHUNK_TIMEOUT:?}"))??;
    let status = response.status();
    if status == reqwest::StatusCode::RANGE_NOT_SATISFIABLE {
        // File is smaller than expected: count as success with empty bytes.
        total_received.fetch_add(0, Ordering::Relaxed);
        return Ok(());
    }
    if !status.is_success() {
        bail!("chunk {offset} HTTP {status}");
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
    total_received.fetch_add(local_total, Ordering::Relaxed);
    Ok(())
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
    }

    #[test]
    fn split_aligned_chunks() {
        let chunks = split(CHUNK_BYTES * 3);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].offset, 0);
        assert_eq!(chunks[1].offset, CHUNK_BYTES);
        assert_eq!(chunks[2].offset, CHUNK_BYTES * 2);
        assert_eq!(chunks[2].length, CHUNK_BYTES);
    }

    #[test]
    fn split_unaligned_tail_chunk() {
        let chunks = split(CHUNK_BYTES + 1234);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[1].offset, CHUNK_BYTES);
        assert_eq!(chunks[1].length, 1234);
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
}
