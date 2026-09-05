mod cache;
mod chunks;
mod sources;

use std::{
    collections::HashSet,
    env,
    net::IpAddr,
    path::PathBuf,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use anyhow::{bail, Context, Result};
use axum::{
    body::{Body, Bytes},
    extract::{OriginalUri, Request, State},
    http::{
        header::{
            ACCEPT, AUTHORIZATION, CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, HOST,
            IF_MODIFIED_SINCE, IF_NONE_MATCH, IF_RANGE, LOCATION, RANGE, SET_COOKIE,
            TRANSFER_ENCODING, USER_AGENT, WWW_AUTHENTICATE,
        },
        HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri,
    },
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use cache::{CachedBlob, DiskCache, ManifestCache, Stats};
use futures_util::StreamExt;
use http_body_util::BodyExt;
use reqwest::{redirect::Policy, Client};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tower::limit::ConcurrencyLimitLayer;
use tower_http::{limit::RequestBodyLimitLayer, trace::TraceLayer};
use tracing::{info, warn};
use url::Url;

const GIT_PROTOCOL: &str = "git-protocol";
const DOCKER_API_VERSION: &str = "docker-distribution-api-version";
const DOCKER_CONTENT_DIGEST: &str = "docker-content-digest";
const MAX_TOKEN_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_MANIFEST_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_DRAIN_TIMEOUT_SECS: u64 = 30;
const BLOB_CHANNEL_DEPTH: usize = 8;
const FILE_READ_BUFFER: usize = 64 * 1024;

const GIT_PROTOCOL_HEADER: HeaderName = HeaderName::from_static(GIT_PROTOCOL);
const DOCKER_CONTENT_DIGEST_HEADER: HeaderName = HeaderName::from_static(DOCKER_CONTENT_DIGEST);

const GITHUB_HOSTS: &[&str] = &[
    "github.com",
    "api.github.com",
    "raw.githubusercontent.com",
    "codeload.github.com",
    "gist.github.com",
    "gist.githubusercontent.com",
    "objects.githubusercontent.com",
    "release-assets.githubusercontent.com",
    "pkg-containers.githubusercontent.com",
];

struct RegistryConfig {
    registry_url: String,
    token_url: String,
    token_service: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Registry {
    DockerHub,
    Ghcr,
}

impl Registry {
    fn config<'a>(&self, state: &'a AppState) -> &'a RegistryConfig {
        match self {
            Self::DockerHub => &state.dockerhub,
            Self::Ghcr => &state.ghcr,
        }
    }

    fn route_prefix(&self) -> &'static str {
        match self {
            Self::DockerHub => "docker.io/",
            Self::Ghcr => "ghcr.io/",
        }
    }
}

struct AppState {
    client: Client,
    sources: Arc<sources::SourcePool>,
    dockerhub: RegistryConfig,
    ghcr: RegistryConfig,
    allowed_registry_hosts: HashSet<String>,
    public_origin: Option<String>,
    max_redirects: usize,
    cache: Arc<DiskCache>,
    manifests: Arc<ManifestCache>,
    stats: Arc<Stats>,
}

enum RegistryKind {
    Manifest,
    Blob(String),
    Other,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "web_proxy=info,tower_http=info".into()),
        )
        .init();

    let listen_addr = env_or("LISTEN_ADDR", "0.0.0.0:20516");
    let dockerhub = registry_config(
        "DOCKERHUB",
        "https://registry-1.docker.io",
        "https://auth.docker.io/token",
        "registry.docker.io",
    )?;
    let ghcr = registry_config(
        "GHCR",
        "https://ghcr.io",
        "https://ghcr.io/token",
        "ghcr.io",
    )?;
    let public_origin = env::var("PUBLIC_ORIGIN").ok().filter(|v| !v.is_empty());

    let connect_timeout = env_parse("UPSTREAM_CONNECT_TIMEOUT_SECS", 10_u64)?;
    let max_redirects = env_parse("MAX_REDIRECTS", 8_usize)?;
    let max_concurrent_requests = env_parse("MAX_CONCURRENT_REQUESTS", 128_usize)?;
    let drain_timeout = env_parse("SHUTDOWN_DRAIN_TIMEOUT_SECS", DEFAULT_DRAIN_TIMEOUT_SECS)?;
    if max_redirects == 0 || max_concurrent_requests == 0 {
        bail!("MAX_REDIRECTS and MAX_CONCURRENT_REQUESTS must be greater than zero");
    }

    let cache_dir = PathBuf::from(env_or("CACHE_DIR", "/data"));
    let cache_dir_display = cache_dir.display().to_string();
    let cache_max_gb = env_parse("CACHE_MAX_GB", 10_u64)?;
    let cache = DiskCache::new(cache_dir, cache_max_gb.saturating_mul(1024 * 1024 * 1024));
    cache
        .init()
        .await
        .with_context(|| format!("init blob cache at {cache_dir_display}"))?;
    info!(
        cache_max_gb,
        on_disk_mib = cache.bytes_on_disk() / 1024 / 1024,
        "blob cache ready"
    );

    let manifest_ttl = env_parse("MANIFEST_TTL_SECS", 60_u64)?;
    let manifest_entries = env_parse("MANIFEST_CACHE_ENTRIES", 2048_usize)?;
    let manifests = ManifestCache::new(Duration::from_secs(manifest_ttl), manifest_entries);

    let client = Client::builder()
        .connect_timeout(Duration::from_secs(connect_timeout))
        .read_timeout(Duration::from_secs(1800))
        .redirect(Policy::none())
        .build()
        .context("build HTTP client")?;

    let mut allowed_registry_hosts: HashSet<String> = HashSet::new();
    for config in [&dockerhub, &ghcr] {
        if let Some(host) = Url::parse(&config.registry_url)
            .ok()
            .and_then(|url| url.host_str().map(str::to_owned))
        {
            allowed_registry_hosts.insert(host.to_ascii_lowercase());
        }
    }
    for host in [
        "production.cloudflare.docker.com",
        "pkg-containers.githubusercontent.com",
    ] {
        allowed_registry_hosts.insert(host.to_owned());
    }
    for host in env_or("REDIRECT_HOSTS", "")
        .split(',')
        .map(str::trim)
        .filter(|host| !host.is_empty())
    {
        allowed_registry_hosts.insert(host.to_ascii_lowercase());
    }

    let source_specs = sources::load_or_default(
        env::var("SOURCES_TOML").ok().as_deref(),
        (
            dockerhub.registry_url.clone(),
            dockerhub.token_url.clone(),
            dockerhub.token_service.clone(),
        ),
        (
            ghcr.registry_url.clone(),
            ghcr.token_url.clone(),
            ghcr.token_service.clone(),
        ),
    )?;
    let sources = sources::SourcePool::new(client.clone(), source_specs);

    let state = Arc::new(AppState {
        client,
        sources,
        dockerhub,
        ghcr,
        allowed_registry_hosts,
        public_origin,
        max_redirects,
        cache: Arc::new(cache),
        manifests: Arc::new(manifests),
        stats: Arc::new(Stats::default()),
    });

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/stats", get(stats))
        .route("/sources", get(sources_view))
        .route("/dashboard", get(dashboard))
        .fallback(proxy)
        .layer(RequestBodyLimitLayer::new(64 * 1024 * 1024))
        .layer(ConcurrencyLimitLayer::new(max_concurrent_requests))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&listen_addr)
        .await
        .with_context(|| format!("bind {listen_addr}"))?;
    info!(%listen_addr, "listening");
    let server = axum::serve(listener, app).with_graceful_shutdown(shutdown_signal());
    tokio::select! {
        result = server => result.context("serve HTTP"),
        // Cap how long in-flight blob transfers may hold the process after
        // the shutdown signal; past this point connections are dropped so
        // container stops don't hang until the runtime kill timeout.
        _ = drain_deadline(drain_timeout) => {
            warn!(drain_timeout_secs = drain_timeout, "drain timeout exceeded; closing remaining connections");
            Ok(())
        }
    }
}

async fn healthz(method: Method) -> Response {
    if method != Method::GET && method != Method::HEAD {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    "ok\n".into_response()
}

async fn stats(State(state): State<Arc<AppState>>) -> Response {
    let body = serde_json::json!({
        "blob_cache_hits": state.stats.blob_hits.load(Ordering::Relaxed),
        "blob_cache_misses": state.stats.blob_misses.load(Ordering::Relaxed),
        "bytes_from_cache": state.stats.bytes_from_cache.load(Ordering::Relaxed),
        "bytes_from_upstream": state.stats.bytes_from_upstream.load(Ordering::Relaxed),
        "disk_bytes": state.cache.bytes_on_disk(),
        "disk_cap_bytes": state.cache.max_bytes(),
    });
    ([(CONTENT_TYPE, "application/json")], body.to_string()).into_response()
}

async fn sources_view(State(state): State<Arc<AppState>>) -> Response {
    let snapshot = state.sources.weights_snapshot().await;
    let last_seen = |instant: Option<std::time::Instant>| -> Option<String> {
        instant.and_then(|t| t.checked_duration_since(std::time::Instant::now()))
    };
    let body = serde_json::json!(snapshot
        .iter()
        .map(|(name, weight, stats)| {
            serde_json::json!({
                "name": name,
                "weight": weight,
                "p50_ms": stats.p50_ms,
                "success": stats.success,
                "failure": stats.failure,
                "range_ok": stats.range_ok,
                // StatsSnapshot timestamps are relative; emit the Unix epoch
                // of the start of the process as a coarse placeholder so the
                // dashboard doesn't show a misleading "now". The prober
                // refreshes every 30s anyway.
                "last_seen": stats.last_seen.map(|_| "just now"),
            })
        })
        .collect::<Vec<_>>());
    ([(CONTENT_TYPE, "application/json")], body.to_string()).into_response()
}

async fn dashboard() -> Response {
    let body = include_str!("../assets/dashboard.html");
    ([(CONTENT_TYPE, "text/html; charset=utf-8")], body).into_response()
}

async fn proxy(
    State(state): State<Arc<AppState>>,
    OriginalUri(uri): OriginalUri,
    request: Request,
) -> Response {
    let host = request
        .headers()
        .get(HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("localhost")
        .to_owned();

    match route_request(&uri) {
        Ok(Route::RegistryRoot) => {
            registry_root(&state, &host, request.method(), request.headers())
        }
        Ok(Route::Token) => proxy_token(&state, request.method(), &uri, request.headers()).await,
        Ok(Route::Registry(registry, path)) => {
            proxy_registry(&state, registry, path, &host, request).await
        }
        Ok(Route::Github(url)) => proxy_github(&state, url, request).await,
        Err(status) => status.into_response(),
    }
}

enum Route {
    RegistryRoot,
    Token,
    Registry(Registry, String),
    Github(Url),
}

fn route_request(uri: &Uri) -> std::result::Result<Route, StatusCode> {
    let path = uri.path();
    if path == "/v2/" || path == "/v2" {
        return Ok(Route::RegistryRoot);
    }
    if path == "/token" {
        return Ok(Route::Token);
    }
    if let Some(rest) = path.strip_prefix("/v2/") {
        if let Some(repo) = rest.strip_prefix("docker.io/") {
            return Ok(Route::Registry(Registry::DockerHub, format!("/v2/{repo}")));
        }
        if let Some(repo) = rest.strip_prefix("ghcr.io/") {
            return Ok(Route::Registry(Registry::Ghcr, format!("/v2/{repo}")));
        }
        // Bare registry-mirror mode: the daemon treats this host as a
        // docker.io mirror and requests /v2/<repo>/... directly.
        return Ok(Route::Registry(Registry::DockerHub, path.to_owned()));
    }

    let Some((host, upstream_path)) = split_host_path(path) else {
        return Err(StatusCode::NOT_FOUND);
    };
    if !is_allowed_github_host(host) {
        return Err(StatusCode::NOT_FOUND);
    }

    let mut url = Url::parse(&format!("https://{host}{upstream_path}"))
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    url.set_query(uri.query());
    validate_upstream_url(&url).map_err(|_| StatusCode::BAD_REQUEST)?;
    Ok(Route::Github(url))
}

fn registry_root(state: &AppState, host: &str, method: &Method, headers: &HeaderMap) -> Response {
    if method != Method::GET && method != Method::HEAD {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    let mut response = match headers.contains_key(AUTHORIZATION) {
        true => StatusCode::OK.into_response(),
        false => {
            let mut unauthorized = StatusCode::UNAUTHORIZED.into_response();
            if let Ok(value) = registry_challenge(&origin_for(state, host), None) {
                unauthorized.headers_mut().insert(WWW_AUTHENTICATE, value);
            }
            unauthorized
        }
    };
    response.headers_mut().insert(
        HeaderName::from_static(DOCKER_API_VERSION),
        HeaderValue::from_static("registry/2.0"),
    );
    response
}

async fn proxy_token(
    state: &AppState,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
) -> Response {
    if method != Method::GET && method != Method::HEAD {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }

    let pairs: Vec<(String, String)> =
        url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
    let scopes: Vec<&str> = pairs
        .iter()
        .filter(|(key, _)| key == "scope")
        .map(|(_, value)| value.as_str())
        .collect();
    if scopes.len() > 1 {
        return (StatusCode::BAD_REQUEST, "exactly one scope is required\n").into_response();
    }
    let (registry, upstream_scope) = match scopes.first() {
        Some(scope) => match rewrite_scope(scope) {
            Ok(value) => value,
            Err(status) => return status.into_response(),
        },
        None => (Registry::DockerHub, String::new()),
    };
    let config = registry.config(state);

    let mut url = match Url::parse(&config.token_url) {
        Ok(url) => url,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("service", &config.token_service);
        if !upstream_scope.is_empty() {
            query.append_pair("scope", &upstream_scope);
        }
        for (key, value) in &pairs {
            if key == "scope" || key == "service" || key == "account" || key == "offline_token" {
                continue;
            }
            query.append_pair(key, value);
        }
    }

    let mut builder = state.client.request(method.clone(), url);
    // LAN mode: forward client credentials so private-repo pulls work.
    builder = copy_request_headers(headers, builder, true);
    let upstream = match builder.send().await {
        Ok(response) => response,
        Err(error) => return upstream_error(error),
    };
    buffered_response(upstream, *method == Method::HEAD, MAX_TOKEN_RESPONSE_BYTES).await
}

async fn proxy_registry(
    state: &AppState,
    registry: Registry,
    path: String,
    host: &str,
    request: Request,
) -> Response {
    if request.method() != Method::GET && request.method() != Method::HEAD {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    match classify_registry_path(&path) {
        RegistryKind::Blob(digest) => {
            proxy_blob(state, registry, path, digest, host, request).await
        }
        RegistryKind::Manifest => proxy_manifest(state, registry, path, host, request).await,
        RegistryKind::Other => passthrough_registry(state, registry, path, host, request).await,
    }
}

async fn proxy_manifest(
    state: &AppState,
    registry: Registry,
    path: String,
    host: &str,
    request: Request,
) -> Response {
    let head = request.method() == Method::HEAD;
    let accept = request
        .headers()
        .get_all(ACCEPT)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .collect::<Vec<_>>()
        .join(", ");
    let key = (path.clone(), accept);

    if let Some(hit) = state.manifests.get(&key).await {
        return manifest_response(&hit, head);
    }

    let origin = origin_for(state, host);
    let url = match registry_url(registry.config(state), &path, request.uri().query()) {
        Ok(url) => url,
        Err(response) => return response,
    };
    let upstream = match fetch_following_redirects(
        state,
        url,
        request.method().clone(),
        request.headers(),
        None,
        true,
    )
    .await
    {
        Ok(upstream) => upstream,
        Err(response) => return response,
    };
    if upstream.status() == StatusCode::UNAUTHORIZED {
        let mut response = streaming_response(upstream, head);
        rewrite_401_challenge(&mut response, registry, &path, &origin);
        return response;
    }
    if head || !upstream.status().is_success() {
        return streaming_response(upstream, head);
    }
    let content_type = upstream
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let docker_digest = upstream
        .headers()
        .get(&DOCKER_CONTENT_DIGEST_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    if upstream
        .content_length()
        .is_some_and(|size| size > MAX_MANIFEST_RESPONSE_BYTES as u64)
    {
        return StatusCode::BAD_GATEWAY.into_response();
    }
    let body = match upstream.bytes().await {
        Ok(bytes) if bytes.len() <= MAX_MANIFEST_RESPONSE_BYTES => bytes,
        Ok(_) | Err(_) => return StatusCode::BAD_GATEWAY.into_response(),
    };
    let hit = cache::CachedManifest {
        content_type: content_type.clone(),
        docker_digest: docker_digest.clone(),
        body: body.clone(),
        stored: std::time::Instant::now(),
    };
    state.manifests.put(key, hit.clone()).await;
    manifest_response(&hit, head)
}

fn manifest_response(hit: &cache::CachedManifest, head: bool) -> Response {
    let mut builder = Response::builder().status(StatusCode::OK);
    if let Some(content_type) = &hit.content_type {
        if let Ok(value) = HeaderValue::from_str(content_type) {
            builder = builder.header(CONTENT_TYPE, value);
        }
    }
    if let Some(digest) = &hit.docker_digest {
        if let Ok(value) = HeaderValue::from_str(digest) {
            builder = builder.header(&DOCKER_CONTENT_DIGEST_HEADER, value);
        }
    }
    if head {
        builder = builder.header(CONTENT_LENGTH, hit.body.len());
    }
    let body = if head {
        Body::empty()
    } else {
        Body::from(hit.body.clone())
    };
    builder
        .body(body)
        .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response())
}

async fn proxy_blob(
    state: &AppState,
    registry: Registry,
    path: String,
    digest: String,
    host: &str,
    request: Request,
) -> Response {
    let head = request.method() == Method::HEAD;
    let Some(hex) = sha256_hex(&digest) else {
        return passthrough_registry(state, registry, path, host, request).await;
    };
    if request.headers().contains_key(RANGE) {
        // Ranged requests bypass the cache and the chunked downloader.
        return passthrough_registry(state, registry, path, host, request).await;
    }

    if let Some(hit) = state.cache.lookup(&hex).await {
        state.stats.blob_hits.fetch_add(1, Ordering::Relaxed);
        state
            .stats
            .bytes_from_cache
            .fetch_add(hit.size, Ordering::Relaxed);
        return cached_blob_response(hit, head, &digest).await;
    }

    // Single-flight: one download per digest, late arrivals re-check the cache.
    let guard = state.cache.inflight_lock(&hex).await;
    let _guard = guard.lock().await;
    if let Some(hit) = state.cache.lookup(&hex).await {
        state.stats.blob_hits.fetch_add(1, Ordering::Relaxed);
        state
            .stats
            .bytes_from_cache
            .fetch_add(hit.size, Ordering::Relaxed);
        return cached_blob_response(hit, head, &digest).await;
    }
    state.stats.blob_misses.fetch_add(1, Ordering::Relaxed);

    let origin = origin_for(state, host);
    let url = match registry_url(registry.config(state), &path, request.uri().query()) {
        Ok(url) => url,
        Err(response) => return response,
    };
    let upstream = match fetch_following_redirects(
        state,
        url,
        request.method().clone(),
        request.headers(),
        None,
        true,
    )
    .await
    {
        Ok(upstream) => upstream,
        Err(response) => return response,
    };
    if !upstream.status().is_success() {
        let mut response = streaming_response(upstream, head);
        rewrite_401_challenge(&mut response, registry, &path, &origin);
        return response;
    }
    if head {
        return streaming_response(upstream, true);
    }

    let content_length = upstream.content_length();
    let digest_header = match HeaderValue::from_str(&digest) {
        Ok(value) => value,
        Err(_) => return StatusCode::BAD_GATEWAY.into_response(),
    };

    // Token broker call: single-source, mirrors the manifest path.
    let token_broker = registry.config(state);
    let token_url = match Url::parse(&token_broker.token_url) {
        Ok(url) => url,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let token = match fetch_token_for(&state.client, &token_broker, &path).await {
        Ok(token) => token,
        Err(response) => return response,
    };

    let total_size = match content_length {
        Some(len) => len,
        None => {
            // Chunked downloads need an explicit size. Fall back to the
            // single-source streamer so the client still gets the blob.
            return single_source_blob_fallback(
                state,
                registry,
                path.clone(),
                hex.clone(),
                digest_header,
                upstream,
            )
            .await;
        }
    };

    let part_path = state.cache.new_part_path(&hex);
    let final_path = state.cache.blob_path(&hex);
    let (tx, rx) = tokio::sync::mpsc::channel::<std::result::Result<Bytes, std::io::Error>>(
        BLOB_CHANNEL_DEPTH,
    );

    let sources = Arc::clone(&state.sources);
    let cache = Arc::clone(&state.cache);
    let stats = Arc::clone(&state.stats);
    let path_for_task = path.clone();
    let registry_label = registry.route_prefix().to_owned();
    let hex_for_task = hex.clone();
    let client = state.client.clone();
    let tx_clone = tx.clone();
    tokio::spawn(async move {
        let result = chunks::download(
            &client,
            &sources,
            cache,
            stats,
            registry_label,
            path_for_task,
            token,
            digest.clone(),
            total_size,
            part_path,
            final_path,
            Some(tx_clone),
        )
        .await;
        if let Err(error) = result {
            warn!(error = %error, "chunked blob download failed");
            let _ = tx.send(Err(std::io::Error::other(error))).await;
        }
    });

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(
            CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        )
        .header(&DOCKER_CONTENT_DIGEST_HEADER, digest_header)
        .header(CONTENT_LENGTH, total_size);
    let stream = futures_util::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    });
    builder
        .body(Body::from_stream(stream))
        .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response())
}

async fn single_source_blob_fallback(
    state: &AppState,
    registry: Registry,
    path: String,
    hex: String,
    digest_header: HeaderValue,
    upstream: reqwest::Response,
) -> Response {
    let (tx, rx) = tokio::sync::mpsc::channel::<std::result::Result<Bytes, std::io::Error>>(
        BLOB_CHANNEL_DEPTH,
    );
    let cache = Arc::clone(&state.cache);
    let stats = Arc::clone(&state.stats);
    tokio::spawn(async move {
        tee_blob_to_cache(upstream, tx, cache, stats, hex).await;
    });
    let _ = registry; // unused but kept for symmetry with the chunked path.
    let _ = path;
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(
            CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        )
        .header(&DOCKER_CONTENT_DIGEST_HEADER, digest_header);
    let stream = futures_util::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    });
    builder
        .body(Body::from_stream(stream))
        .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response())
}

async fn fetch_token_for(
    client: &reqwest::Client,
    config: &RegistryConfig,
    path: &str,
) -> std::result::Result<String, Response> {
    let scope = registry_repository(path).map(|repository| format!("repository:{repository}:pull"));
    let mut url = match Url::parse(&config.token_url) {
        Ok(url) => url,
        Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR.into_response()),
    };
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("service", &config.token_service);
        if let Some(scope) = scope {
            query.append_pair("scope", &scope);
        }
    }
    let response = match client.get(url).send().await {
        Ok(r) => r,
        Err(_) => return Err(StatusCode::BAD_GATEWAY.into_response()),
    };
    if !response.status().is_success() {
        return Err(StatusCode::BAD_GATEWAY.into_response());
    }
    let body = match response.bytes().await {
        Ok(b) => b,
        Err(_) => return Err(StatusCode::BAD_GATEWAY.into_response()),
    };
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => return Err(StatusCode::BAD_GATEWAY.into_response()),
    };
    value
        .get("token")
        .and_then(|v| v.as_str())
        .map(str::to_owned)
        .ok_or_else(|| StatusCode::BAD_GATEWAY.into_response())
}

async fn tee_blob_to_cache(
    upstream: reqwest::Response,
    tx: tokio::sync::mpsc::Sender<std::result::Result<Bytes, std::io::Error>>,
    cache: Arc<DiskCache>,
    stats: Arc<Stats>,
    hex: String,
) {
    let part_path = cache.new_part_path(&hex);
    // File::create does not make parent dirs; the digest shard must exist first.
    let dir_ready = match part_path.parent() {
        Some(dir) => tokio::fs::create_dir_all(dir).await.is_ok(),
        None => false,
    };
    let opened = match dir_ready {
        true => tokio::fs::File::create(&part_path).await,
        false => Err(std::io::Error::other("cache dir unavailable")),
    };
    let mut file = match opened {
        Ok(file) => file,
        Err(error) => {
            warn!(%error, hex, "cache write unavailable; streaming without caching");
            // Cache unavailable: degrade to plain streaming.
            stream_to_channel(upstream.bytes_stream(), &tx).await;
            return;
        }
    };

    let mut hasher = ring::digest::Context::new(&ring::digest::SHA256);
    let mut total: u64 = 0;
    let mut stream = upstream.bytes_stream();
    let mut client_gone = false;
    while let Some(item) = stream.next().await {
        let chunk = match item {
            Ok(chunk) => chunk,
            Err(error) => {
                let _ = tokio::fs::remove_file(&part_path).await;
                if !client_gone {
                    let _ = tx.send(Err(std::io::Error::other(error))).await;
                }
                return;
            }
        };
        hasher.update(&chunk);
        total += chunk.len() as u64;
        if let Err(error) = file.write_all(&chunk).await {
            let _ = tokio::fs::remove_file(&part_path).await;
            if !client_gone {
                let _ = tx.send(Err(std::io::Error::other(error))).await;
            }
            return;
        }
        if !client_gone && tx.send(Ok(chunk)).await.is_err() {
            // Client disconnected: keep downloading to seed the cache.
            client_gone = true;
        }
    }
    if let Err(error) = file.flush().await {
        let _ = tokio::fs::remove_file(&part_path).await;
        if !client_gone {
            let _ = tx.send(Err(std::io::Error::other(error))).await;
        }
        return;
    }
    drop(file);

    let actual = hex_encode(hasher.finish().as_ref());
    if actual != hex {
        let _ = tokio::fs::remove_file(&part_path).await;
        if !client_gone {
            let _ = tx
                .send(Err(std::io::Error::other("upstream blob digest mismatch")))
                .await;
        }
        return;
    }
    let dest = cache.blob_path(&hex);
    if tokio::fs::rename(&part_path, &dest).await.is_err() {
        if !client_gone {
            let _ = tx
                .send(Err(std::io::Error::other("cache commit failed")))
                .await;
        }
        return;
    }
    cache.committed(total).await;
    stats
        .bytes_from_upstream
        .fetch_add(total, Ordering::Relaxed);
}

async fn stream_to_channel(
    stream: impl futures_util::Stream<Item = reqwest::Result<Bytes>>,
    tx: &tokio::sync::mpsc::Sender<std::result::Result<Bytes, std::io::Error>>,
) {
    futures_util::pin_mut!(stream);
    while let Some(item) = stream.next().await {
        let item = item.map_err(std::io::Error::other);
        if tx.send(item).await.is_err() {
            return;
        }
    }
}

async fn passthrough_registry(
    state: &AppState,
    registry: Registry,
    path: String,
    host: &str,
    request: Request,
) -> Response {
    let head = request.method() == Method::HEAD;
    let url = match registry_url(registry.config(state), &path, request.uri().query()) {
        Ok(url) => url,
        Err(response) => return response,
    };
    let upstream = match fetch_following_redirects(
        state,
        url,
        request.method().clone(),
        request.headers(),
        None,
        true,
    )
    .await
    {
        Ok(upstream) => upstream,
        Err(response) => return response,
    };
    let mut response = streaming_response(upstream, head);
    if response.status() == StatusCode::UNAUTHORIZED {
        rewrite_401_challenge(&mut response, registry, &path, &origin_for(state, host));
    }
    response
}

async fn proxy_github(state: &AppState, url: Url, request: Request) -> Response {
    if !matches!(*request.method(), Method::GET | Method::HEAD | Method::POST) {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }

    let method = request.method().clone();
    let headers = request.headers().clone();
    let body = if method == Method::POST {
        match request.into_body().collect().await {
            Ok(collected) => Some(collected.to_bytes()),
            Err(_) => return StatusCode::BAD_REQUEST.into_response(),
        }
    } else {
        None
    };

    match fetch_following_redirects(state, url, method, &headers, body, false).await {
        Ok(response) => streaming_response(response, false),
        Err(response) => response,
    }
}

async fn fetch_following_redirects(
    state: &AppState,
    mut url: Url,
    mut method: Method,
    headers: &HeaderMap,
    mut body: Option<Bytes>,
    registry: bool,
) -> std::result::Result<reqwest::Response, Response> {
    let mut current_headers = headers.clone();
    let mut redirects = 0_usize;

    loop {
        if registry {
            if validate_registry_url(&url, &state.allowed_registry_hosts).is_err() {
                return Err(StatusCode::BAD_GATEWAY.into_response());
            }
        } else if validate_upstream_url(&url).is_err() {
            return Err(StatusCode::BAD_GATEWAY.into_response());
        }

        let mut builder = state.client.request(method.clone(), url.clone());
        builder = copy_request_headers(&current_headers, builder, registry);
        if let Some(bytes) = body.clone() {
            builder = builder.body(bytes);
        }

        let upstream = builder.send().await.map_err(upstream_error)?;
        if !upstream.status().is_redirection() {
            return Ok(upstream);
        }

        if redirects >= state.max_redirects {
            return Err((StatusCode::BAD_GATEWAY, "too many upstream redirects\n").into_response());
        }
        let location = upstream
            .headers()
            .get(LOCATION)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| StatusCode::BAD_GATEWAY.into_response())?;
        let next = url
            .join(location)
            .map_err(|_| StatusCode::BAD_GATEWAY.into_response())?;
        if registry {
            validate_registry_url(&next, &state.allowed_registry_hosts)
        } else {
            validate_upstream_url(&next)
        }
        .map_err(|_| StatusCode::BAD_GATEWAY.into_response())?;

        if upstream.status() == StatusCode::SEE_OTHER
            || ((upstream.status() == StatusCode::MOVED_PERMANENTLY
                || upstream.status() == StatusCode::FOUND)
                && method == Method::POST)
        {
            method = Method::GET;
        } else if !matches!(
            upstream.status(),
            StatusCode::MOVED_PERMANENTLY
                | StatusCode::FOUND
                | StatusCode::SEE_OTHER
                | StatusCode::TEMPORARY_REDIRECT
                | StatusCode::PERMANENT_REDIRECT
        ) {
            return Err(StatusCode::BAD_GATEWAY.into_response());
        }

        if method == Method::POST
            && !matches!(
                upstream.status(),
                StatusCode::TEMPORARY_REDIRECT | StatusCode::PERMANENT_REDIRECT
            )
        {
            method = Method::GET;
            body = None;
        }

        if url.host_str() != next.host_str() {
            current_headers.remove(AUTHORIZATION);
        }

        url = next;
        redirects += 1;
    }
}

fn streaming_response(upstream: reqwest::Response, head: bool) -> Response {
    let status = upstream.status();
    let upstream_headers = upstream.headers().clone();
    let body = if head {
        Body::empty()
    } else {
        Body::from_stream(
            upstream
                .bytes_stream()
                .map(|item| item.map_err(std::io::Error::other)),
        )
    };
    let mut response = Response::new(body);
    *response.status_mut() = status;
    copy_response_headers(&upstream_headers, response.headers_mut());
    response
}

async fn buffered_response(upstream: reqwest::Response, head: bool, max_bytes: usize) -> Response {
    let status = upstream.status();
    let upstream_headers = upstream.headers().clone();
    let body = if head {
        Body::empty()
    } else {
        if upstream
            .content_length()
            .is_some_and(|size| size > max_bytes as u64)
        {
            return StatusCode::BAD_GATEWAY.into_response();
        }
        let bytes = match upstream.bytes().await {
            Ok(bytes) if bytes.len() <= max_bytes => bytes,
            Ok(_) | Err(_) => return StatusCode::BAD_GATEWAY.into_response(),
        };
        Body::from(bytes)
    };
    let mut response = Response::new(body);
    *response.status_mut() = status;
    copy_response_headers(&upstream_headers, response.headers_mut());
    response
}

async fn cached_blob_response(hit: CachedBlob, head: bool, digest: &str) -> Response {
    let builder = Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_LENGTH, hit.size)
        .header(
            &DOCKER_CONTENT_DIGEST_HEADER,
            HeaderValue::from_str(digest).unwrap_or(HeaderValue::from_static("unknown")),
        );
    if head {
        return builder
            .body(Body::empty())
            .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response());
    }
    match tokio::fs::File::open(&hit.path).await {
        Ok(file) => builder
            .body(file_stream(file))
            .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response()),
        Err(_) => StatusCode::BAD_GATEWAY.into_response(),
    }
}

fn file_stream(file: tokio::fs::File) -> Body {
    let stream = futures_util::stream::unfold(
        (file, vec![0u8; FILE_READ_BUFFER]),
        |(mut file, mut buf)| async move {
            match file.read(&mut buf).await {
                Ok(0) => None,
                Ok(n) => Some((Ok(Bytes::copy_from_slice(&buf[..n])), (file, buf))),
                Err(error) => Some((Err(error), (file, buf))),
            }
        },
    );
    Body::from_stream(stream)
}

fn copy_request_headers(
    headers: &HeaderMap,
    builder: reqwest::RequestBuilder,
    registry: bool,
) -> reqwest::RequestBuilder {
    let allowed: &[HeaderName] = if registry {
        &[
            AUTHORIZATION,
            ACCEPT,
            CONTENT_TYPE,
            RANGE,
            IF_RANGE,
            IF_NONE_MATCH,
            IF_MODIFIED_SINCE,
            USER_AGENT,
        ]
    } else {
        &[
            ACCEPT,
            CONTENT_TYPE,
            RANGE,
            IF_RANGE,
            IF_NONE_MATCH,
            IF_MODIFIED_SINCE,
            USER_AGENT,
            GIT_PROTOCOL_HEADER,
        ]
    };

    // Accumulate into one HeaderMap so multi-valued entries (e.g. multiple
    // If-None-Match tags) survive — reqwest's per-call `.header()` replaces.
    let mut forwarded = HeaderMap::with_capacity(allowed.len());
    for name in allowed {
        for value in headers.get_all(name) {
            forwarded.append(name.clone(), value.clone());
        }
    }
    builder.headers(forwarded)
}

fn copy_response_headers(source: &HeaderMap, target: &mut HeaderMap) {
    for (name, value) in source {
        if is_response_header_allowed(name) {
            target.append(name, value.clone());
        }
    }
}

fn is_response_header_allowed(name: &HeaderName) -> bool {
    if matches!(
        name,
        &CONNECTION | &TRANSFER_ENCODING | &SET_COOKIE | &WWW_AUTHENTICATE | &LOCATION
    ) {
        return false;
    }
    !matches!(
        name.as_str(),
        "proxy-authenticate"
            | "proxy-authorization"
            | "keep-alive"
            | "te"
            | "trailer"
            | "upgrade"
            | "forwarded"
    ) && !name.as_str().starts_with("x-forwarded-")
}

fn registry_url(
    config: &RegistryConfig,
    path: &str,
    query: Option<&str>,
) -> std::result::Result<Url, Response> {
    let mut url = Url::parse(&config.registry_url)
        .and_then(|base| base.join(path.trim_start_matches('/')))
        .map_err(|_| StatusCode::BAD_GATEWAY.into_response())?;
    url.set_query(query);
    Ok(url)
}

fn rewrite_401_challenge(response: &mut Response, registry: Registry, path: &str, origin: &str) {
    if response.status() != StatusCode::UNAUTHORIZED {
        return;
    }
    let scope = registry_repository(path)
        .map(|repository| format!("repository:{}{}:pull", registry.route_prefix(), repository));
    if let Ok(value) = registry_challenge(origin, scope.as_deref()) {
        response.headers_mut().insert(WWW_AUTHENTICATE, value);
    }
}

fn origin_for(state: &AppState, host: &str) -> String {
    state
        .public_origin
        .clone()
        .unwrap_or_else(|| format!("http://{host}"))
}

fn registry_challenge(origin: &str, scope: Option<&str>) -> Result<HeaderValue> {
    let scope = scope
        .map(|value| format!(",scope=\"{value}\""))
        .unwrap_or_default();
    HeaderValue::from_str(&format!(
        "Bearer realm=\"{origin}/token\",service=\"edge-registry\"{scope}"
    ))
    .context("build registry challenge")
}

fn registry_repository(path: &str) -> Option<&str> {
    let path = path.strip_prefix("/v2/")?;
    ["/manifests/", "/blobs/", "/tags/", "/referrers/"]
        .iter()
        .filter_map(|marker| path.rfind(marker))
        .max()
        .map(|index| &path[..index])
        .filter(|repository| !repository.is_empty())
}

fn classify_registry_path(path: &str) -> RegistryKind {
    let mut best: Option<(&str, usize)> = None;
    for marker in ["/manifests/", "/blobs/", "/tags/", "/referrers/"] {
        if let Some(index) = path.rfind(marker) {
            if best.is_none_or(|(_, best_index)| index > best_index) {
                best = Some((marker, index));
            }
        }
    }
    match best {
        Some(("/blobs/", index)) => RegistryKind::Blob(path[index + "/blobs/".len()..].to_owned()),
        Some(("/manifests/", _)) => RegistryKind::Manifest,
        _ => RegistryKind::Other,
    }
}

fn rewrite_scope(scope: &str) -> std::result::Result<(Registry, String), StatusCode> {
    let mut parts = scope.splitn(3, ':');
    if parts.next() != Some("repository") {
        return Err(StatusCode::BAD_REQUEST);
    }
    let repository = parts.next().ok_or(StatusCode::BAD_REQUEST)?;
    let actions = parts.next().ok_or(StatusCode::BAD_REQUEST)?;
    if repository.is_empty()
        || repository.starts_with('/')
        || repository.ends_with('/')
        || repository
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
        || !actions.split(',').all(|action| action == "pull")
    {
        return Err(StatusCode::BAD_REQUEST);
    }

    if let Some(rest) = repository.strip_prefix("docker.io/") {
        if rest.is_empty() {
            return Err(StatusCode::BAD_REQUEST);
        }
        return Ok((Registry::DockerHub, format!("repository:{rest}:{actions}")));
    }
    if let Some(rest) = repository.strip_prefix("ghcr.io/") {
        if rest.is_empty() {
            return Err(StatusCode::BAD_REQUEST);
        }
        return Ok((Registry::Ghcr, format!("repository:{rest}:{actions}")));
    }
    // Bare scope (registry-mirror mode): the daemon omits the host prefix.
    Ok((Registry::DockerHub, scope.to_owned()))
}

fn split_host_path(path: &str) -> Option<(&str, &str)> {
    let rest = path.strip_prefix('/')?;
    let slash = rest.find('/')?;
    let host = &rest[..slash];
    let upstream_path = &rest[slash..];
    (!host.is_empty()).then_some((host, upstream_path))
}

fn is_allowed_github_host(host: &str) -> bool {
    GITHUB_HOSTS.contains(&host.to_ascii_lowercase().as_str())
}

fn sha256_hex(digest: &str) -> Option<String> {
    let hex = digest.strip_prefix("sha256:")?;
    let valid = hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit());
    valid.then(|| hex.to_ascii_lowercase())
}

fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn validate_upstream_url(url: &Url) -> Result<()> {
    validate_https_url(url)?;
    let host = url.host_str().context("missing upstream host")?;
    if !is_allowed_github_host(host) {
        bail!("upstream host is not allowed");
    }
    Ok(())
}

fn validate_registry_url(url: &Url, allowed: &HashSet<String>) -> Result<()> {
    validate_https_url(url)?;
    let host = url.host_str().context("missing upstream host")?;
    if !allowed.contains(&host.to_ascii_lowercase()) {
        bail!("registry redirect host is not allowed");
    }
    Ok(())
}

fn validate_https_url(url: &Url) -> Result<()> {
    if url.scheme() != "https" || !url.username().is_empty() || url.password().is_some() {
        bail!("invalid upstream URL");
    }
    if url.port_or_known_default() != Some(443) {
        bail!("invalid upstream port");
    }
    let host = url.host_str().context("missing upstream host")?;
    if host.parse::<IpAddr>().is_ok() {
        bail!("IP upstreams are not allowed");
    }
    Ok(())
}

fn registry_config(
    prefix: &str,
    default_registry: &str,
    default_token: &str,
    default_service: &str,
) -> Result<RegistryConfig> {
    let registry_url = env_or(&format!("{prefix}_REGISTRY_URL"), default_registry);
    let token_url = env_or(&format!("{prefix}_TOKEN_URL"), default_token);
    let token_service = env_or(&format!("{prefix}_TOKEN_SERVICE"), default_service);
    for url in [&registry_url, &token_url] {
        let parsed =
            Url::parse(url).with_context(|| format!("invalid {prefix} upstream url {url}"))?;
        if parsed.scheme() != "https" {
            bail!("{prefix} upstream url must be https: {url}");
        }
    }
    Ok(RegistryConfig {
        registry_url,
        token_url,
        token_service,
    })
}

fn upstream_error(error: reqwest::Error) -> Response {
    warn!(%error, "upstream request failed");
    (StatusCode::BAD_GATEWAY, "upstream request failed\n").into_response()
}

fn env_or(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn env_parse<T>(name: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|error| anyhow::anyhow!("invalid {name}: {error}")),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error).with_context(|| format!("read {name}")),
    }
}

async fn drain_deadline(timeout_secs: u64) {
    shutdown_signal().await;
    tokio::time::sleep(Duration::from_secs(timeout_secs)).await;
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_docker_hub_and_ghcr() {
        let docker: Uri = "/v2/docker.io/library/alpine/manifests/latest"
            .parse()
            .unwrap();
        let ghcr: Uri = "/v2/ghcr.io/owner/team/image/blobs/sha256:abc"
            .parse()
            .unwrap();

        assert!(matches!(
            route_request(&docker),
            Ok(Route::Registry(Registry::DockerHub, path))
                if path == "/v2/library/alpine/manifests/latest"
        ));
        assert!(matches!(
            route_request(&ghcr),
            Ok(Route::Registry(Registry::Ghcr, path))
                if path == "/v2/owner/team/image/blobs/sha256:abc"
        ));
    }

    #[test]
    fn routes_bare_v2_as_dockerhub_mirror() {
        let bare: Uri = "/v2/library/alpine/manifests/latest".parse().unwrap();
        assert!(matches!(
            route_request(&bare),
            Ok(Route::Registry(Registry::DockerHub, path))
                if path == "/v2/library/alpine/manifests/latest"
        ));
    }

    #[test]
    fn classifies_registry_paths() {
        assert!(matches!(
            classify_registry_path("/v2/library/alpine/blobs/sha256:abc"),
            RegistryKind::Blob(digest) if digest == "sha256:abc"
        ));
        assert!(matches!(
            classify_registry_path("/v2/owner/team/image/manifests/latest"),
            RegistryKind::Manifest
        ));
        assert!(matches!(
            classify_registry_path("/v2/library/alpine/tags/list"),
            RegistryKind::Other
        ));
        assert!(matches!(
            classify_registry_path("/v2/"),
            RegistryKind::Other
        ));
    }

    #[test]
    fn extracts_registry_repositories() {
        assert_eq!(
            registry_repository("/v2/library/alpine/manifests/latest"),
            Some("library/alpine")
        );
        assert_eq!(
            registry_repository("/v2/owner/team/image/blobs/sha256:abc"),
            Some("owner/team/image")
        );
        assert_eq!(registry_repository("/v2/"), None);
    }

    #[test]
    fn rewrites_scopes() {
        assert_eq!(
            rewrite_scope("repository:docker.io/library/alpine:pull").unwrap(),
            (
                Registry::DockerHub,
                "repository:library/alpine:pull".to_owned()
            )
        );
        assert_eq!(
            rewrite_scope("repository:ghcr.io/owner/team/image:pull").unwrap(),
            (
                Registry::Ghcr,
                "repository:owner/team/image:pull".to_owned()
            )
        );
        // Bare scope: registry-mirror mode defaults to Docker Hub.
        assert_eq!(
            rewrite_scope("repository:library/alpine:pull").unwrap(),
            (
                Registry::DockerHub,
                "repository:library/alpine:pull".to_owned()
            )
        );
    }

    #[test]
    fn rejects_bad_scopes() {
        for scope in [
            "repository:docker.io/library/alpine:pull,push",
            "repository:docker.io/../admin:pull",
            "repository:/leading-slash:pull",
            "registry:docker.io/library/alpine:pull",
        ] {
            assert!(rewrite_scope(scope).is_err(), "accepted {scope}");
        }
    }

    #[test]
    fn accepts_only_exact_github_hosts() {
        for host in GITHUB_HOSTS {
            assert!(is_allowed_github_host(host));
        }
        for host in [
            "github.com.evil.example",
            "evil.example",
            "127.0.0.1",
            "169.254.169.254",
            "::1",
        ] {
            assert!(!is_allowed_github_host(host), "accepted {host}");
        }
    }

    #[test]
    fn validates_redirect_targets() {
        for url in [
            "https://github.com/owner/repo",
            "https://objects.githubusercontent.com/file",
            "https://release-assets.githubusercontent.com/file",
        ] {
            validate_upstream_url(&Url::parse(url).unwrap()).unwrap();
        }
        for url in [
            "http://github.com/owner/repo",
            "https://github.com.evil.example/file",
            "https://127.0.0.1/file",
            "https://169.254.169.254/latest/meta-data",
            "https://[::1]/file",
            "https://user@github.com/file",
            "https://github.com:8443/file",
        ] {
            assert!(
                validate_upstream_url(&Url::parse(url).unwrap()).is_err(),
                "accepted {url}"
            );
        }
    }

    #[test]
    fn validates_registry_hosts_against_allowlist() {
        let allowed: HashSet<String> = ["registry-1.docker.io", "cdn.example.com"]
            .into_iter()
            .map(str::to_owned)
            .collect();
        for url in [
            "https://registry-1.docker.io/v2/library/alpine/blobs/sha256:abc",
            "https://cdn.example.com/file",
        ] {
            assert!(
                validate_registry_url(&Url::parse(url).unwrap(), &allowed).is_ok(),
                "rejected {url}"
            );
        }
        for url in [
            "https://evil.example/file",
            "http://registry-1.docker.io/v2/x",
            "https://registry-1.docker.io.evil.example/file",
        ] {
            assert!(
                validate_registry_url(&Url::parse(url).unwrap(), &allowed).is_err(),
                "accepted {url}"
            );
        }
    }

    #[test]
    fn routes_safe_github_urls_and_rejects_spoofs() {
        let valid: Uri = "/github.com/owner/repo/releases/download/v1/file.zip?x=1"
            .parse()
            .unwrap();
        assert!(
            matches!(route_request(&valid), Ok(Route::Github(url)) if url.as_str() == "https://github.com/owner/repo/releases/download/v1/file.zip?x=1")
        );

        for path in [
            "/github.com.evil.example/owner/repo",
            "/evil.example/?next=github.com",
            "/127.0.0.1/private",
            "/169.254.169.254/latest/meta-data",
        ] {
            assert_eq!(
                route_request(&path.parse().unwrap()).err(),
                Some(StatusCode::NOT_FOUND)
            );
        }
    }

    #[test]
    fn parses_sha256_digests() {
        let hex = "a".repeat(64);
        assert_eq!(sha256_hex(&format!("sha256:{hex}")), Some(hex));
        assert_eq!(sha256_hex("sha256:abc"), None);
        assert_eq!(sha256_hex("sha512:abc"), None);
        assert_eq!(sha256_hex("sha256:"), None);
    }
}
