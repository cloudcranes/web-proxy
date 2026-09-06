//! Pull-as-a-service on top of the local Docker daemon (KSpeeder-style).
//!
//! POST /pull asks the daemon (via its unix socket) to pull a reference that
//! points back at this gateway, so blob traffic flows through the multi-source
//! chunked downloader and the disk cache, then retags the result to the name
//! the user typed. GET /pulls exposes per-layer progress for the dashboard.
//!
//! The Docker Engine API is spoken over the unix socket with a bare hyper
//! http1 connection; only the pull (streaming JSON progress lines) and tag
//! endpoints are used. Socket IO is unix-only, but ref parsing and progress
//! accounting are pure so they are testable on any host.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

use anyhow::{bail, Context, Result};
use hyper::{Method, Request as HyperRequest, StatusCode as HyperStatusCode};
use tokio::sync::Mutex;
use tracing::warn;

const MAX_FINISHED_JOBS: usize = 20;

/// A pull request resolved against the gateway host: what to tell the daemon
/// and how to rename the result afterwards.
pub struct PullSpec {
    /// Canonical display the user asked for, e.g. "redis:alpine".
    pub image: String,
    /// Full gateway reference to pull, e.g. "host:20516/library/redis".
    pub pull_repo: String,
    pub tag: Option<String>,
    pub digest: Option<String>,
    /// Local name to retag to, e.g. "redis" (None = leave as pulled).
    pub retag_repo: Option<String>,
    pub retag_tag: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    Pulling,
    Retagging,
    Done,
    Failed,
}

impl JobStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pulling => "pulling",
            Self::Retagging => "retagging",
            Self::Done => "done",
            Self::Failed => "failed",
        }
    }
}

pub struct LayerState {
    pub status: String,
    pub current: u64,
    pub total: u64,
}

pub struct JobState {
    pub status: JobStatus,
    pub message: String,
    pub error: Option<String>,
    pub layers: HashMap<String, LayerState>,
    pub layer_order: Vec<String>,
}

pub struct PullJob {
    pub id: u64,
    pub spec: PullSpec,
    pub started: Instant,
    pub state: Mutex<JobState>,
}

pub struct PullManager {
    socket: String,
    next_id: AtomicU64,
    jobs: Mutex<Vec<Arc<PullJob>>>,
}

impl PullManager {
    pub fn new(socket: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            socket: socket.into(),
            next_id: AtomicU64::new(1),
            jobs: Mutex::new(vec![]),
        })
    }

    pub async fn start(self: &Arc<Self>, spec: PullSpec) -> Result<Arc<PullJob>> {
        {
            let jobs = self.jobs.lock().await;
            for job in jobs.iter() {
                if job.spec.image != spec.image {
                    continue;
                }
                let active = match job.state.try_lock() {
                    Ok(s) => matches!(s.status, JobStatus::Pulling | JobStatus::Retagging),
                    Err(_) => true,
                };
                if active {
                    return Ok(Arc::clone(job));
                }
            }
        }

        let job = Arc::new(PullJob {
            id: self.next_id.fetch_add(1, Ordering::Relaxed),
            spec,
            started: Instant::now(),
            state: Mutex::new(JobState {
                status: JobStatus::Pulling,
                message: "connecting to docker daemon".into(),
                error: None,
                layers: HashMap::new(),
                layer_order: vec![],
            }),
        });
        {
            let mut jobs = self.jobs.lock().await;
            jobs.push(Arc::clone(&job));
            // Trim finished jobs from the front, keeping the newest few for
            // the dashboard. Active jobs are never dropped.
            let mut finished = 0;
            jobs.retain(|j| {
                let active = match j.state.try_lock() {
                    Ok(s) => matches!(s.status, JobStatus::Pulling | JobStatus::Retagging),
                    Err(_) => true,
                };
                if active {
                    true
                } else {
                    finished += 1;
                    finished <= MAX_FINISHED_JOBS
                }
            });
        }

        let manager = Arc::clone(self);
        let task_job = Arc::clone(&job);
        tokio::spawn(async move { manager.run(task_job).await });
        Ok(job)
    }

    async fn run(&self, job: Arc<PullJob>) {
        let outcome = docker_pull(&self.socket, &job).await;
        {
            let mut s = job.state.lock().await;
            match outcome {
                Ok(message) => {
                    s.message = message;
                    s.status = JobStatus::Done;
                }
                Err(error) => {
                    s.error = Some(format!("{error:#}"));
                    s.status = JobStatus::Failed;
                }
            }
        }
        // Rename to the canonical name so the accelerated detour is invisible
        // to later `docker run redis:alpine` style usage.
        if job.spec.retag_repo.is_some() && job.state.lock().await.status == JobStatus::Done {
            {
                let mut s = job.state.lock().await;
                s.status = JobStatus::Retagging;
            }
            match docker_retag(&self.socket, &job).await {
                Ok(()) => {
                    let mut s = job.state.lock().await;
                    s.message = format!("已拉取并重命名为 {}", job.spec.image);
                    s.status = JobStatus::Done;
                }
                Err(error) => {
                    let mut s = job.state.lock().await;
                    s.error = Some(format!("retag: {error:#}"));
                    s.status = JobStatus::Failed;
                }
            }
        }
    }

    pub async fn snapshot(&self) -> Vec<serde_json::Value> {
        let jobs = self.jobs.lock().await;
        let mut out = Vec::with_capacity(jobs.len());
        for job in jobs.iter().rev() {
            let s = job.state.lock().await;
            let (mut current, mut total) = (0u64, 0u64);
            let layers: Vec<serde_json::Value> = s
                .layer_order
                .iter()
                .filter_map(|id| {
                    let l = s.layers.get(id)?;
                    current += l.current;
                    total += l.total;
                    Some(serde_json::json!({
                        "id": id,
                        "status": l.status,
                        "current": l.current,
                        "total": l.total,
                    }))
                })
                .collect();
            let percent = if total > 0 { current * 100 / total } else { 0 };
            out.push(serde_json::json!({
                "id": job.id,
                "image": job.spec.image,
                "status": s.status.as_str(),
                "message": s.message,
                "error": s.error,
                "elapsed_secs": job.started.elapsed().as_secs(),
                "percent": percent,
                "layers": layers,
            }));
        }
        out
    }
}

/// Feed one JSON progress line from the daemon's pull stream into the job
/// state. Unparseable lines (keepalives) are ignored; `error` lines abort.
async fn apply_progress_line(job: &PullJob, line: &[u8]) -> Result<()> {
    let text = std::str::from_utf8(line).unwrap_or("").trim();
    if text.is_empty() {
        return Ok(());
    }
    let Ok(value) = serde_json::from_str::<serde_json::Value>(text) else {
        return Ok(());
    };
    if let Some(error) = value.get("error").and_then(|e| e.as_str()) {
        bail!("{error}");
    }
    let Some(status) = value.get("status").and_then(|s| s.as_str()) else {
        return Ok(());
    };
    let id = value.get("id").and_then(|i| i.as_str()).map(str::to_owned);
    let (current, total) = value
        .get("progressDetail")
        .and_then(|p| Some((p.get("current")?.as_u64()?, p.get("total")?.as_u64()?)))
        .unwrap_or((0, 0));

    let mut s = job.state.lock().await;
    match id {
        Some(id) => {
            if !s.layers.contains_key(&id) {
                s.layer_order.push(id.clone());
            }
            let layer = s.layers.entry(id).or_insert_with(|| LayerState {
                status: status.to_owned(),
                current: 0,
                total: 0,
            });
            layer.status = status.to_owned();
            if total > 0 {
                layer.total = total;
            }
            if current > 0 {
                layer.current = current;
            }
        }
        None => s.message = status.to_owned(),
    }
    Ok(())
}

#[cfg(unix)]
async fn docker_pull(socket: &str, job: &PullJob) -> Result<String> {
    use http_body_util::BodyExt;
    use hyper::client::conn::http1;
    use hyper_util::rt::TokioIo;

    let stream = tokio::net::UnixStream::connect(socket)
        .await
        .with_context(|| format!("connect docker socket {socket}"))?;
    let (mut sender, conn) = http1::handshake(TokioIo::new(stream))
        .await
        .context("docker api handshake")?;
    tokio::spawn(async move {
        if let Err(error) = conn.await {
            warn!(%error, "docker api connection ended");
        }
    });

    // fromImage contains ':' and '/', which the daemon wants percent-encoded
    // in the query or it 500s.
    let mut uri = format!(
        "/images/create?fromImage={}",
        percent_encode_path(&job.spec.pull_repo)
    );
    if let Some(tag) = &job.spec.tag {
        uri.push_str(&format!("&tag={}", percent_encode_path(tag)));
    }
    if let Some(digest) = &job.spec.digest {
        uri.push_str(&format!("&digest={}", percent_encode_path(digest)));
    }

    let request = HyperRequest::builder()
        .method(Method::POST)
        .uri(&uri)
        .header("host", "docker")
        .body(http_body_util::Empty::<axum::body::Bytes>::new())
        .context("build docker api request")?;
    let response = sender
        .send_request(request)
        .await
        .context("docker api request")?;
    if !response.status().is_success() {
        bail!("docker api HTTP {}", response.status());
    }

    let mut body = response.into_body();
    let mut buf: Vec<u8> = Vec::new();
    // Per-frame idle timeout: a hung daemon must not wedge the job (and
    // thereby block re-pulls of the same image) forever. Normal pulls emit
    // progress frames every few seconds.
    const PULL_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);
    loop {
        let frame = match tokio::time::timeout(PULL_IDLE_TIMEOUT, BodyExt::frame(&mut body)).await {
            Ok(frame) => frame,
            Err(_) => bail!("docker pull stream idle for {PULL_IDLE_TIMEOUT:?}"),
        };
        let Some(frame) = frame else { break };
        let frame = frame?;
        let data = frame
            .into_data()
            .map_err(|_| anyhow::anyhow!("non-data frame"))?;
        buf.extend_from_slice(&data);
        while let Some(pos) = buf.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = buf.drain(..=pos).collect();
            apply_progress_line(job, &line).await?;
        }
    }

    let s = job.state.lock().await;
    if s.message.starts_with("Status:") {
        Ok(s.message.clone())
    } else {
        bail!(
            "docker pull stream ended without a final status (last: {})",
            s.message
        )
    }
}

#[cfg(unix)]
async fn docker_retag(socket: &str, job: &PullJob) -> Result<()> {
    use http_body_util::BodyExt;
    use hyper::client::conn::http1;
    use hyper_util::rt::TokioIo;

    let repo = job
        .spec
        .retag_repo
        .as_deref()
        .context("retag repo missing")?;
    let tag = job.spec.retag_tag.as_deref().context("retag tag missing")?;
    let pulled = match &job.spec.tag {
        Some(tag) => format!("{}:{}", job.spec.pull_repo, tag),
        None => format!(
            "{}@{}",
            job.spec.pull_repo,
            job.spec.digest.clone().unwrap_or_default()
        ),
    };
    let uri = format!(
        "/images/{}/tag?repo={}&tag={}",
        percent_encode_path(&pulled),
        percent_encode_path(repo),
        percent_encode_path(tag),
    );

    let stream = tokio::net::UnixStream::connect(socket)
        .await
        .with_context(|| format!("connect docker socket {socket}"))?;
    let (mut sender, conn) = http1::handshake(TokioIo::new(stream))
        .await
        .context("docker api handshake")?;
    tokio::spawn(async move {
        if let Err(error) = conn.await {
            warn!(%error, "docker api connection ended");
        }
    });
    let request = HyperRequest::builder()
        .method(Method::POST)
        .uri(&uri)
        .header("host", "docker")
        .body(http_body_util::Empty::<axum::body::Bytes>::new())
        .context("build docker api request")?;
    let response = sender
        .send_request(request)
        .await
        .context("docker api request")?;
    if response.status() != HyperStatusCode::CREATED && !response.status().is_success() {
        bail!("docker api tag HTTP {}", response.status());
    }
    let _ = BodyExt::collect(response.into_body()).await?;
    Ok(())
}

#[cfg(not(unix))]
async fn docker_pull(_socket: &str, _job: &PullJob) -> Result<String> {
    bail!("docker socket pulls are only available on unix hosts");
}

#[cfg(not(unix))]
async fn docker_retag(_socket: &str, _job: &PullJob) -> Result<()> {
    bail!("docker socket pulls are only available on unix hosts");
}

fn percent_encode_path(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// Resolved image reference for a pull request. `path` is normalized with the
/// docker.io `library/` prefix when needed; `canonical` keeps the name as the
/// user typed it (without tag) for the retag step.
#[derive(Debug)]
pub struct ParsedImageRef {
    pub registry: String,
    pub path: String,
    pub canonical: String,
    pub tag: Option<String>,
    pub digest: Option<String>,
}

/// Parse a user-supplied image reference. Only docker.io and ghcr.io are
/// routed by the gateway, so other registries are rejected here.
pub fn parse_image_ref(input: &str) -> Result<ParsedImageRef> {
    let input = input.trim();
    if input.is_empty()
        || input.split_whitespace().count() != 1
        || input.contains("://")
        || input.starts_with('-')
    {
        bail!("invalid image reference");
    }

    let (name, digest) = match input.split_once('@') {
        Some((name, digest)) => {
            let hex = digest.strip_prefix("sha256:").unwrap_or_default();
            if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
                bail!("invalid digest (want sha256:<64 hex>)");
            }
            (name, Some(digest.to_owned()))
        }
        None => (input, None),
    };

    let (registry, path) = match name.split_once('/') {
        Some((first, rest))
            if first.contains('.') || first.contains(':') || first == "localhost" =>
        {
            (first.to_ascii_lowercase(), rest)
        }
        _ => ("docker.io".to_owned(), name),
    };
    if registry != "docker.io" && registry != "ghcr.io" {
        bail!("unsupported registry {registry} (gateway routes docker.io and ghcr.io only)");
    }
    // Tag: a ':' after the last '/'. A ':' inside the registry host (e.g.
    // "localhost:5000/foo") yields a pseudo-tag containing '/', which is not
    // a tag.
    let (path, tag) = match path.rsplit_once(':') {
        Some((name, tag)) if !tag.contains('/') => (name, Some(tag.to_owned())),
        _ => (path, None),
    };
    if path.is_empty()
        || path
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        bail!("invalid image path");
    }
    // Docker's own charset for repository names. Anything outside it
    // (HTML metacharacters, query separators, whitespace, uppercase) is
    // rejected before the value can reach a shell, URI, or the dashboard.
    if !path
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '.' | '_' | '-' | '/'))
    {
        bail!("image names may only contain [a-z0-9._-/]");
    }
    if let Some(tag) = &tag {
        if tag.is_empty()
            || tag.len() > 128
            || tag.starts_with(['.', '-'])
            || !tag
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
        {
            bail!("invalid tag");
        }
    }

    let (pull_path, canonical) = if registry == "docker.io" {
        let pull_path = if path.contains('/') {
            path.to_owned()
        } else {
            format!("library/{path}")
        };
        (pull_path, path.to_owned())
    } else {
        (path.to_owned(), format!("{registry}/{path}"))
    };

    Ok(ParsedImageRef {
        registry,
        path: pull_path,
        canonical,
        tag,
        digest,
    })
}

/// Turn a user image reference into the concrete pull plan against the
/// gateway host from the request (so the daemon pulls through us).
pub fn plan_pull(image: &str, gateway_host: &str) -> Result<PullSpec> {
    let parsed = parse_image_ref(image)?;
    // ghcr pulls must keep the ghcr.io/ prefix in the reference so the
    // daemon's requests hit the gateway's ghcr route instead of being
    // treated as a bare docker.io mirror path.
    let pull_repo = match parsed.registry.as_str() {
        "ghcr.io" => format!("{gateway_host}/ghcr.io/{}", parsed.path),
        _ => format!("{gateway_host}/{}", parsed.path),
    };
    // A digest-only pull cannot be retagged to a tag, so leave the image
    // under the gateway reference instead of inventing a tag.
    let (retag_repo, retag_tag) = if parsed.digest.is_some() {
        (None, None)
    } else {
        (
            Some(parsed.canonical.clone()),
            Some(parsed.tag.clone().unwrap_or_else(|| "latest".to_owned())),
        )
    };
    let display = match (&parsed.tag, &parsed.digest) {
        (Some(tag), _) => format!("{}:{tag}", parsed.canonical),
        (None, Some(digest)) => format!("{}@{digest}", parsed.canonical),
        (None, None) => format!("{}:latest", parsed.canonical),
    };
    Ok(PullSpec {
        image: display,
        pull_repo,
        tag: parsed.tag,
        digest: parsed.digest,
        retag_repo,
        retag_tag,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ref_of(input: &str) -> ParsedImageRef {
        parse_image_ref(input).unwrap()
    }

    #[test]
    fn parses_plain_docker_hub_names() {
        let r = ref_of("nginx");
        assert_eq!(r.registry, "docker.io");
        assert_eq!(r.path, "library/nginx");
        assert_eq!(r.canonical, "nginx");
        assert_eq!(r.tag, None);
        assert_eq!(r.digest, None);
    }

    #[test]
    fn parses_tags_and_namespaces() {
        let r = ref_of("bitnami/redis:7-alpine");
        assert_eq!(r.path, "bitnami/redis");
        assert_eq!(r.canonical, "bitnami/redis");
        assert_eq!(r.tag.as_deref(), Some("7-alpine"));

        let r = ref_of("library/alpine");
        assert_eq!(r.path, "library/alpine");
        assert_eq!(r.canonical, "library/alpine");
    }

    #[test]
    fn parses_explicit_ghcr_refs() {
        let r = ref_of("ghcr.io/owner/team/img:v1");
        assert_eq!(r.registry, "ghcr.io");
        assert_eq!(r.path, "owner/team/img");
        assert_eq!(r.canonical, "ghcr.io/owner/team/img");
        assert_eq!(r.tag.as_deref(), Some("v1"));
    }

    #[test]
    fn registry_ports_are_rejected_not_parsed_as_tags() {
        // The ':' in the registry host must not be mistaken for a tag; and
        // non-gateway registries are rejected outright.
        let err = parse_image_ref("localhost:5000/foo").unwrap_err();
        assert!(err.to_string().contains("unsupported registry"));
    }

    #[test]
    fn parses_digest_refs() {
        let hex = "a".repeat(64);
        let r = ref_of(&format!("nginx@sha256:{hex}"));
        assert_eq!(r.digest.as_deref(), Some(format!("sha256:{hex}").as_str()));
        assert_eq!(r.tag, None);
    }

    #[test]
    fn rejects_bad_refs() {
        for bad in [
            "",
            "nginx latest",
            "https://x/y",
            "quay.io/a/b",
            "nginx@sha256:zz",
            "nginx:..",
            "NGINX/latest",
            "nginx@sha256:short",
        ] {
            assert!(parse_image_ref(bad).is_err(), "expected {bad:?} to fail");
        }
    }

    #[test]
    fn rejects_paths_outside_docker_charset() {
        // HTML metacharacters, query separators, and percent signs must be
        // rejected before they can reach URIs or the dashboard.
        for bad in [
            "a<img/src=x/onerror=alert(1)>b",
            "a&repo=x/c",
            "a%2Fb",
            "a?b=c",
            "a#b/c",
        ] {
            assert!(parse_image_ref(bad).is_err(), "expected {bad:?} to fail");
        }
        // The legit charset keeps working.
        assert!(parse_image_ref("bitnami/wordpress-php8.3_fpm").is_ok());
    }

    #[test]
    fn plans_gateway_pull_with_retag() {
        let plan = plan_pull("redis:alpine", "192.168.1.107:20516").unwrap();
        assert_eq!(plan.pull_repo, "192.168.1.107:20516/library/redis");
        assert_eq!(plan.tag.as_deref(), Some("alpine"));
        assert_eq!(plan.retag_repo.as_deref(), Some("redis"));
        assert_eq!(plan.retag_tag.as_deref(), Some("alpine"));
        assert_eq!(plan.image, "redis:alpine");
    }

    #[test]
    fn plans_ghcr_pull_through_prefix_route() {
        let plan = plan_pull("ghcr.io/owner/img", "192.168.1.107:20516").unwrap();
        // The ghcr.io prefix must survive into the pull reference, otherwise
        // the daemon's /v2/owner/img requests are routed to Docker Hub.
        assert_eq!(plan.pull_repo, "192.168.1.107:20516/ghcr.io/owner/img");
        assert_eq!(plan.retag_repo.as_deref(), Some("ghcr.io/owner/img"));
        assert_eq!(plan.retag_tag.as_deref(), Some("latest"));
    }

    #[test]
    fn plans_digest_pull_without_retag() {
        let hex = "b".repeat(64);
        let plan = plan_pull(&format!("nginx@sha256:{hex}"), "h:1").unwrap();
        assert_eq!(plan.retag_repo, None);
        assert_eq!(plan.tag, None);
    }

    #[test]
    fn encodes_path_segment_safely() {
        assert_eq!(
            percent_encode_path("h:20516/library/redis:alpine"),
            "h%3A20516%2Flibrary%2Fredis%3Aalpine"
        );
    }

    fn fresh_job() -> PullJob {
        PullJob {
            id: 1,
            spec: PullSpec {
                image: "x".into(),
                pull_repo: "y".into(),
                tag: None,
                digest: None,
                retag_repo: None,
                retag_tag: None,
            },
            started: Instant::now(),
            state: Mutex::new(JobState {
                status: JobStatus::Pulling,
                message: String::new(),
                error: None,
                layers: HashMap::new(),
                layer_order: vec![],
            }),
        }
    }

    #[tokio::test]
    async fn progress_lines_track_layers() {
        let job = fresh_job();
        apply_progress_line(&job, br#"{"status":"Pulling from library/redis"}"#)
            .await
            .unwrap();
        apply_progress_line(
            &job,
            br#"{"status":"Downloading","progressDetail":{"current":10,"total":100},"id":"abc"}"#,
        )
        .await
        .unwrap();
        apply_progress_line(&job, br#"{"status":"Pull complete","id":"abc"}"#)
            .await
            .unwrap();
        let s = job.state.lock().await;
        assert_eq!(s.layer_order, vec!["abc".to_owned()]);
        assert_eq!(s.layers["abc"].current, 10);
        assert_eq!(s.layers["abc"].total, 100);
        assert_eq!(s.layers["abc"].status, "Pull complete");
    }

    #[tokio::test]
    async fn error_line_aborts() {
        let job = fresh_job();
        let err = apply_progress_line(
            &job,
            br#"{"error":"not found","errorDetail":{"message":"not found"}}"#,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("not found"));
    }
}

/// ---------- generic Docker Engine API helpers (unix socket) ----------

/// One JSON (or empty) request against the Docker Engine API. Returns the
/// parsed body; non-2xx raises with the daemon's message.
#[cfg(unix)]
pub(crate) async fn docker_api(
    socket: &str,
    method: Method,
    path_and_query: &str,
    body: Option<serde_json::Value>,
    timeout_secs: u64,
) -> Result<serde_json::Value> {
    use http_body_util::{BodyExt, Empty, Full};
    use hyper::client::conn::http1;
    use hyper_util::rt::TokioIo;
    use serde_json::Value;

    let stream = tokio::net::UnixStream::connect(socket)
        .await
        .with_context(|| format!("connect docker socket {socket}"))?;
    let (mut sender, conn) = http1::handshake(TokioIo::new(stream))
        .await
        .context("docker api handshake")?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let mut builder = HyperRequest::builder()
        .method(method)
        .uri(path_and_query)
        .header("host", "docker");
    // Empty and full bodies are different types, so always build a
    // Full<Bytes> request (empty payload for bodyless calls).
    let request = match body {
        Some(value) => {
            builder
                .header("content-type", "application/json")
                .body(http_body_util::Full::new(axum::body::Bytes::from(
                    value.to_string(),
                )))
        }
        None => builder
            .header("content-length", "0")
            .body(http_body_util::Full::new(axum::body::Bytes::new())),
    }
    .context("build docker api request")?;
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(timeout_secs),
        sender.send_request(request),
    )
    .await
    .map_err(|_| anyhow::anyhow!("docker api timed out after {timeout_secs}s"))?
    .context("docker api request")?;
    let status = response.status();
    let bytes = BodyExt::collect(response.into_body())
        .await
        .context("read docker api response")?
        .to_bytes();
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    if !status.is_success() {
        let message = value
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("no message");
        bail!("docker api HTTP {status}: {message}");
    }
    Ok(value)
}

/// The gateway's own container id: the container hostname is a short id.
#[cfg(unix)]
pub(crate) async fn self_container_id(socket: &str) -> Result<String> {
    let id = tokio::fs::read_to_string("/etc/hostname")
        .await
        .context("read /etc/hostname")?
        .trim()
        .to_owned();
    if id.is_empty() {
        bail!("empty /etc/hostname");
    }
    Ok(id)
}

/// Issue (or renew) the public certificate for the configured domain by
/// running a one-shot acme.sh container doing DNS-01 against the stored
/// Cloudflare token. Certificates land under {cache_dir}/certs/{domain}_ecc/
/// and are copied to {cache_dir}/certs/live/ for the gateway to load.
#[cfg(unix)]
pub(crate) async fn issue_cert_job(
    socket: &str,
    self_id: &str,
    image_ref: &str,
    cache_dir: &std::path::Path,
    domain: &str,
    accel_domain: Option<&str>,
    cf_token: &str,
    cf_zone_id: &str,
    cf_account_id: &str,
    progress: &(dyn Fn(String) + Send + Sync),
) -> Result<()> {
    use http_body_util::BodyExt;

    progress(format!("拉取 acme.sh 镜像（{image_ref}）…"));
    docker_api(
        socket,
        Method::POST,
        &format!(
            "/images/create?fromImage={}&tag=latest",
            percent_encode_path(image_ref)
        ),
        None,
        900,
    )
    .await?;

    progress("定位数据卷…".into());
    let info = docker_api(
        socket,
        Method::GET,
        &format!("/containers/{self_id}/json"),
        None,
        30,
    )
    .await?;
    let mount = info
        .get("Mounts")
        .and_then(|m| m.as_array())
        .and_then(|arr| {
            arr.iter()
                .find(|m| m.get("Destination").and_then(|d| d.as_str()) == Some("/data"))
                .cloned()
        })
        .context("gateway /data volume not found")?;
    let volume_source = mount
        .get("Source")
        .and_then(|s| s.as_str())
        .context("/data mount has no source")?
        .to_owned();

    // Remove a leftover issuer container from a previous run.
    let _ = docker_api(
        socket,
        Method::DELETE,
        "/containers/web-proxy-acme?force=true&v=true",
        None,
        30,
    )
    .await;

    // SAN set: apex, wildcard, acceleration host, git host — deduped.
    let mut names = vec![domain.to_owned(), format!("*.{domain}")];
    if let Some(extra) = accel_domain {
        if !extra.ends_with(domain) && !names.contains(&extra.to_owned()) {
            names.push(extra.to_owned());
        }
    }
    let git_host = format!("git.{domain}");
    if !names.contains(&git_host) {
        names.push(git_host);
    }

    let mut cmd = vec![
        "--issue".to_owned(),
        "--dns".to_owned(),
        "dns_cf".to_owned(),
        "--server".to_owned(),
        "letsencrypt".to_owned(),
        "--keylength".to_owned(),
        "ec-256".to_owned(),
        "--cert-home".to_owned(),
        "/certs".to_owned(),
    ];
    for name in &names {
        cmd.push("-d".to_owned());
        cmd.push(name.clone());
    }

    progress("创建 acme.sh 容器（DNS-01 验证）…".into());
    let created = docker_api(
        socket,
        Method::POST,
        "/containers/create?name=web-proxy-acme",
        Some(json!({
            "Image": "neilpang/acme.sh:latest",
            "Cmd": cmd,
            "Env": [
                format!("CF_Token={cf_token}"),
                format!("CF_Account_ID={cf_account_id}"),
                format!("CF_Zone_ID={cf_zone_id}"),
            ],
            "HostConfig": {
                "Mounts": [{"Type": "volume", "Source": volume_source, "Target": "/certs"}],
                "Dns": ["223.5.5.5", "119.29.29.29"],
                "AutoRemove": false,
            }
        })),
        60,
    )
    .await?;
    let id = created
        .get("Id")
        .and_then(|v| v.as_str())
        .context("container create returned no Id")?
        .to_owned();

    progress("等待 DNS-01 验证与签发（约 1-2 分钟）…".into());
    docker_api(
        socket,
        Method::POST,
        &format!("/containers/{id}/start"),
        None,
        60,
    )
    .await?;
    let wait = tokio::time::timeout(
        std::time::Duration::from_secs(900),
        docker_api(
            socket,
            Method::POST,
            &format!("/containers/{id}/wait"),
            None,
            960,
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("签发超时（15 分钟）"))?
    .context("docker wait")?;
    let exit_code = wait
        .get("StatusCode")
        .and_then(|c| c.as_i64())
        .unwrap_or(-1);
    if exit_code != 0 {
        let logs = container_log_tail(socket, &id, 40)
            .await
            .unwrap_or_default();
        let _ = docker_api(
            socket,
            Method::DELETE,
            &format!("/containers/{id}?force=true"),
            None,
            30,
        )
        .await;
        bail!("acme.sh 退出码 {exit_code}: {logs}");
    }
    let _ = docker_api(
        socket,
        Method::DELETE,
        &format!("/containers/{id}"),
        None,
        30,
    )
    .await;

    progress("部署证书…".into());
    let cert_dir = cache_dir.join("certs").join(format!("{domain}_ecc"));
    let live_dir = cache_dir.join("certs").join("live");
    tokio::fs::create_dir_all(&live_dir).await?;
    tokio::fs::copy(
        cert_dir.join("fullchain.cer"),
        live_dir.join("fullchain.pem"),
    )
    .await
    .context("copy fullchain")?;
    tokio::fs::copy(
        cert_dir.join(format!("{domain}.key")),
        live_dir.join("privkey.pem"),
    )
    .await
    .context("copy privkey")?;
    progress(format!("证书已签发（{}）", names.join(", ")));
    Ok(())
}

/// Pull the last `tail` lines out of docker's multiplexed log stream.
#[cfg(unix)]
async fn container_log_tail(socket: &str, id: &str, tail: u32) -> Result<String> {
    use http_body_util::BodyExt;
    use hyper::client::conn::http1;
    use hyper_util::rt::TokioIo;

    let stream = tokio::net::UnixStream::connect(socket).await?;
    let (mut sender, conn) = http1::handshake(TokioIo::new(stream)).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let request = HyperRequest::builder()
        .method(Method::GET)
        .uri(&format!(
            "/containers/{id}/logs?stdout=1&stderr=1&tail={tail}"
        ))
        .header("host", "docker")
        .body(http_body_util::Empty::<axum::body::Bytes>::new())?;
    let response = sender.send_request(request).await?;
    let bytes = BodyExt::collect(response.into_body()).await?.to_bytes();
    // Demux frames: [stream(1), pad(3), len(4 BE), payload...]
    let mut out: Vec<u8> = Vec::new();
    let mut rest: &[u8] = &bytes;
    while rest.len() >= 8 {
        let len = u32::from_be_bytes([rest[4], rest[5], rest[6], rest[7]]) as usize;
        if rest.len() < 8 + len {
            break;
        }
        out.extend_from_slice(&rest[8..8 + len]);
        rest = &rest[8 + len..];
    }
    Ok(String::from_utf8_lossy(&out)
        .lines()
        .map(|l| {
            l.trim_start_matches(|c: char| !c.is_ascii_graphic() && !c.is_alphanumeric())
                .to_owned()
                + "\n"
        })
        .collect())
}

#[cfg(not(unix))]
pub(crate) async fn docker_api(
    _socket: &str,
    _method: Method,
    _path_and_query: &str,
    _body: Option<serde_json::Value>,
    _timeout_secs: u64,
) -> Result<serde_json::Value> {
    bail!("docker socket is only available on unix hosts");
}

#[cfg(not(unix))]
pub(crate) async fn self_container_id(_socket: &str) -> Result<String> {
    bail!("docker socket is only available on unix hosts");
}

#[cfg(not(unix))]
pub(crate) async fn issue_cert_job(
    _socket: &str,
    _self_id: &str,
    _image_ref: &str,
    _cache_dir: &std::path::Path,
    _domain: &str,
    _accel_domain: Option<&str>,
    _cf_token: &str,
    _cf_zone_id: &str,
    _cf_account_id: &str,
    _progress: &(dyn Fn(String) + Send + Sync),
) -> Result<()> {
    bail!("certificate issuance is only available on unix hosts");
}
