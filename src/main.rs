mod cache;
mod chunks;
mod dockerpull;
mod logs;
mod metrics;
mod settings;
mod sources;

use std::{
    collections::HashSet,
    env,
    net::IpAddr,
    path::{Path, PathBuf},
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, RwLock};

use anyhow::{bail, Context, Result};
use axum::{
    body::{Body, Bytes},
    extract::{OriginalUri, Request, State},
    http::{
        header::{
            ACCEPT, ACCEPT_RANGES, AUTHORIZATION, CONNECTION, CONTENT_LENGTH, CONTENT_RANGE,
            CONTENT_TYPE, HOST, IF_MODIFIED_SINCE, IF_NONE_MATCH, IF_RANGE, LOCATION, RANGE,
            SET_COOKIE, TRANSFER_ENCODING, USER_AGENT, WWW_AUTHENTICATE,
        },
        HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri,
    },
    response::{IntoResponse, Redirect, Response},
    routing::{get, post},
    Router,
};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use cache::{CachedBlob, DiskCache, ManifestCache, Stats};
use futures_util::StreamExt;
use http_body_util::BodyExt;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as HttpConnBuilder;
use reqwest::{redirect::Policy, Client};
use rustls::pki_types::{
    CertificateDer, PrivateKeyDer, PrivatePkcs1KeyDer, PrivatePkcs8KeyDer, PrivateSec1KeyDer,
};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio_rustls::TlsAcceptor;
use tower::limit::ConcurrencyLimitLayer;
use tower::ServiceExt;
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
    sources: Arc<RwLock<Arc<sources::SourcePool>>>,
    dockerhub: RegistryConfig,
    ghcr: RegistryConfig,
    allowed_registry_hosts: HashSet<String>,
    public_origin: Option<String>,
    default_scheme: &'static str,
    max_redirects: usize,
    cache: Arc<DiskCache>,
    manifests: Arc<ManifestCache>,
    stats: Arc<Stats>,
    pulls: Arc<dockerpull::PullManager>,
    /// Address the Docker daemon should use to reach this gateway for
    /// pull-as-a-service (e.g. "192.168.1.107:20516"). The request Host
    /// header can be wrong (localhost, EdgeOne domain), and the daemon's TLS
    /// verification needs a name the certificate actually covers.
    pull_via_host: Option<String>,
    accel_addr: Option<String>,
    /// Served at GET /ca.crt so LAN clients can import the self-signed CA
    /// without copying files; unset hides the endpoint.
    ca_path: Option<String>,
    settings_path: PathBuf,
    sources_path: PathBuf,
    cert_job: Mutex<Option<Value>>,
    metrics_history: Arc<metrics::History>,
    logs: Arc<logs::Logs>,
    /// Serializes concurrent /settings PATCHes so a load+apply+save pair
    /// can't interleave with another writer and drop fields (last-writer
    /// silently overwrites the other).
    settings_lock: tokio::sync::Mutex<()>,
    /// Optional bearer token gating destructive write endpoints. When set,
    /// POST/PUT/PATCH/DELETE on /pull, /pull/warm, /sources/config,
    /// /sources/probe, /cache/clear, /settings and /settings/* must carry
    /// `Authorization: Bearer <token>`. Reads stay open so the dashboard
    /// and metrics scrapers keep working. LAN deployments without
    /// internet exposure can leave this unset.
    mgmt_bearer: Option<String>,
    started: Instant,
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
    let accel_addr = env::var("ACCEL_LISTEN_ADDR")
        .ok()
        .filter(|value| !value.is_empty());
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
    let cache = DiskCache::new(
        cache_dir.clone(),
        cache_max_gb.saturating_mul(1024 * 1024 * 1024),
    );
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

    let client = {
        let mut builder = Client::builder()
            .connect_timeout(Duration::from_secs(connect_timeout))
            .read_timeout(Duration::from_secs(1800))
            // Adaptive H2 window tuning handles initial-stream/connection
            // sizing internally; the two `initial_*` setters would be ignored
            // while this is true.
            .http2_adaptive_window(true)
            .pool_max_idle_per_host(32)
            .pool_idle_timeout(Duration::from_secs(90))
            .redirect(Policy::none());
        // Load the operator-supplied CA bundle (e.g. self-signed registry on
        // the LAN) into the outgoing trust store so the gateway can pull
        // from sources whose cert isn't in webpki-roots.
        if let Some(ca_path) = env::var("CA_CERT_PATH").ok().filter(|v| !v.is_empty()) {
            match std::fs::read(&ca_path) {
                Ok(bytes) => {
                    let mut loaded = 0usize;
                    for cert in rustls_pemfile::certs(&mut bytes.as_slice()) {
                        match cert {
                            Ok(der) => {
                                builder = builder.add_root_certificate(
                                    reqwest::tls::Certificate::from_der(der.as_ref()).map_err(
                                        |e| anyhow::anyhow!("bad cert in {ca_path}: {e}"),
                                    )?,
                                );
                                loaded += 1;
                            }
                            Err(error) => warn!(%error, "skipping invalid PEM entry in {ca_path}"),
                        }
                    }
                    if loaded > 0 {
                        info!(path = %ca_path, count = loaded, "loaded CA bundle");
                    }
                }
                Err(error) => warn!(path = %ca_path, %error, "CA_CERT_PATH read failed"),
            }
        }
        builder.build().context("build HTTP client")?
    };

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

    let sources_path = sources::sources_path(&cache_dir);
    let configured_sources = if sources_path.exists() {
        Some(sources_path.to_string_lossy().into_owned())
    } else {
        env::var("SOURCES_TOML").ok().filter(|v| !v.is_empty())
    };
    let source_specs = sources::load_or_default(
        configured_sources.as_deref(),
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
    if !sources_path.exists() {
        sources::save(&sources_path, &source_specs)?;
    }
    let sources = sources::SourcePool::new(client.clone(), source_specs);

    // Certificate precedence: settings-issued (Let's Encrypt via the settings
    // UI) over env-provided over plain HTTP. Issued certs live on the data
    // volume so they survive restarts.
    let certs_live_dir = cache_dir.join("certs").join("live");
    let live_fullchain = certs_live_dir.join("fullchain.pem");
    let live_privkey = certs_live_dir.join("privkey.pem");
    let (tls_acceptor, tls_source) = if live_fullchain.exists() && live_privkey.exists() {
        match build_tls_acceptor(
            live_fullchain.to_string_lossy().as_ref(),
            live_privkey.to_string_lossy().as_ref(),
        ) {
            Ok(acceptor) => (Some(acceptor), "settings-issued"),
            Err(error) => {
                warn!(%error, "settings-issued certificate failed to load; falling back");
                (load_tls_acceptor()?, "env")
            }
        }
    } else {
        (load_tls_acceptor()?, "env")
    };
    let default_scheme: &'static str = if tls_acceptor.is_some() {
        "https"
    } else {
        "http"
    };
    info!(source = tls_source, "TLS mode resolved");

    let settings_path = settings::settings_path(&cache_dir);

    let state = Arc::new(AppState {
        client,
        sources: Arc::new(RwLock::new(sources)),
        dockerhub,
        ghcr,
        allowed_registry_hosts,
        public_origin,
        default_scheme,
        max_redirects,
        cache: Arc::new(cache),
        manifests: Arc::new(manifests),
        stats: Arc::new(Stats::default()),
        pulls: dockerpull::PullManager::new(env_or("DOCKER_SOCKET", "/var/run/docker.sock")),
        pull_via_host: env::var("PULL_VIA_HOST").ok().filter(|v| !v.is_empty()),
        accel_addr: accel_addr.clone(),
        ca_path: env::var("CA_CERT_PATH").ok().filter(|v| !v.is_empty()),
        settings_path,
        sources_path,
        cert_job: Mutex::new(None),
        metrics_history: Arc::new(metrics::History::new()),
        logs: Arc::new(logs::Logs::new()),
        settings_lock: tokio::sync::Mutex::new(()),
        mgmt_bearer: env::var("MGMT_BEARER_TOKEN").ok().filter(|v| !v.is_empty()),
        started: Instant::now(),
    });

    // Plain HTTP listener: dashboard page + read-only stats/sources/pulls/
    // downloads/history/healthz + source-config edits. No pull, no
    // settings writes, no cache clear, no docker.sock actions, no git
    // proxy — anything auth-sensitive stays on HTTPS only. Source URLs
    // are public operational data (not credentials) so the editor is
    // exposed over plain HTTP for LAN convenience; PUT still validates
    // https-only registry hosts in `parse_value`.
    let dashboard_app = Router::new()
        .route("/", get(dashboard_redirect))
        .route("/dashboard", get(dashboard))
        .route("/healthz", get(healthz))
        .route("/stats", get(stats))
        .route("/metrics/history", get(metrics_history))
        .route("/downloads", get(downloads))
        .route("/pulls", get(list_pulls))
        .route("/sources", get(sources_view))
        .route(
            "/sources/config",
            get(get_sources_config).put(save_sources_config),
        )
        .route("/logs", get(get_logs))
        .fallback(not_found_on_http)
        .with_state(Arc::clone(&state));

    let app = Router::new()
        .route("/", get(dashboard_redirect))
        .route("/healthz", get(healthz))
        .route("/stats", get(stats))
        .route("/metrics/history", get(metrics_history))
        .route("/downloads", get(downloads))
        .route("/pull", post(start_pull))
        .route("/pull/warm", post(start_warm))
        .route("/pulls", get(list_pulls))
        .route("/ca.crt", get(serve_ca))
        .route("/settings", get(get_settings).patch(save_settings))
        .route("/settings/issue-cert", post(issue_cert))
        .route("/settings/dns-records", post(create_dns_records))
        .route("/settings/restart", post(restart_gateway))
        .route("/sources", get(sources_view))
        .route(
            "/sources/config",
            get(get_sources_config).put(save_sources_config),
        )
        .route("/sources/probe", post(trigger_probe))
        .route("/logs", get(get_logs))
        .route("/cache/clear", post(clear_cache))
        .route("/dashboard", get(dashboard))
        .fallback(proxy)
        .layer(RequestBodyLimitLayer::new(64 * 1024 * 1024))
        .layer(ConcurrencyLimitLayer::new(max_concurrent_requests))
        .layer(TraceLayer::new_for_http())
        .with_state(Arc::clone(&state));

    // Daily access (dashboard/management) and acceleration (docker pulls,
    // git proxy) can live on separate ports; both serve the same router.
    let listener = tokio::net::TcpListener::bind(&listen_addr)
        .await
        .with_context(|| format!("bind {listen_addr}"))?;
    let mut listeners = vec![listener];
    if let Some(accel) = &accel_addr {
        let listener = tokio::net::TcpListener::bind(accel)
            .await
            .with_context(|| format!("bind accel {accel}"))?;
        listeners.push(listener);
        info!(%accel, "listening (acceleration port)");
    }
    // Optional plain-HTTP port for the dashboard only. Set
    // LISTEN_ADDR_HTTP=0.0.0.0:8080 to expose /dashboard without TLS.
    // Unauthenticated acceleration endpoints stay HTTPS-only.
    let http_dashboard_addr = env::var("LISTEN_ADDR_HTTP").ok().filter(|v| !v.is_empty());
    let http_listener = if let Some(addr) = &http_dashboard_addr {
        let l = tokio::net::TcpListener::bind(addr)
            .await
            .with_context(|| format!("bind http dashboard {addr}"))?;
        info!(%addr, "listening (HTTP dashboard only)");
        Some(l)
    } else {
        None
    };

    let metrics_cache = Arc::clone(&state.cache);
    let metrics_disk_cap = state.cache.max_bytes();
    metrics::spawn(
        Arc::clone(&state.metrics_history),
        Arc::clone(&state.stats),
        Arc::clone(&state.sources),
        Arc::new(move || metrics_cache.bytes_on_disk()),
        metrics_disk_cap,
    );

    if listeners.len() == 1 && tls_acceptor.is_none() {
        info!(%listen_addr, "listening (HTTP)");
        let server =
            axum::serve(listeners.remove(0), app).with_graceful_shutdown(shutdown_signal());
        tokio::select! {
            result = server => result.context("serve HTTP")?,
            // Cap how long in-flight blob transfers may hold the process after
            // the shutdown signal; past this point connections are dropped so
            // container stops don't hang until the runtime kill timeout.
            _ = drain_deadline(drain_timeout) => {
                warn!(drain_timeout_secs = drain_timeout, "drain timeout exceeded; closing remaining connections");
            }
        }
    } else {
        serve_multi(
            Arc::new(listeners),
            tls_acceptor,
            app,
            dashboard_app,
            drain_timeout,
        )
        .await?;
    }
    Ok(())
}

fn load_tls_acceptor() -> Result<Option<TlsAcceptor>> {
    let cert_path = env::var("TLS_CERT_PATH")
        .ok()
        .filter(|value| !value.is_empty());
    let key_path = env::var("TLS_KEY_PATH")
        .ok()
        .filter(|value| !value.is_empty());
    let (cert_path, key_path) = match (cert_path, key_path) {
        (None, None) => return Ok(None),
        (Some(_), None) | (None, Some(_)) => {
            bail!("TLS_CERT_PATH and TLS_KEY_PATH must be set together");
        }
        (Some(cert_path), Some(key_path)) => (cert_path, key_path),
    };
    Ok(Some(build_tls_acceptor(&cert_path, &key_path)?))
}

fn build_tls_acceptor(cert_path: &str, key_path: &str) -> Result<TlsAcceptor> {
    let cert_pem = std::fs::read_to_string(cert_path)
        .with_context(|| format!("read TLS certificate {cert_path}"))?;
    let key_pem =
        std::fs::read_to_string(key_path).with_context(|| format!("read TLS key {key_path}"))?;
    let certs: Vec<CertificateDer<'static>> = pem_der_blocks(&cert_pem, "CERTIFICATE")
        .into_iter()
        .map(CertificateDer::from)
        .collect();
    if certs.is_empty() {
        bail!("no CERTIFICATE PEM block found in {cert_path}");
    }
    let key = private_key_from_pem(&key_pem, &key_path)?;

    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .context("select TLS protocol versions")?
    .with_no_client_auth()
    .with_single_cert(certs, key)
    .context("load TLS certificate/key pair")?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn private_key_from_pem(pem: &str, key_path: &str) -> Result<PrivateKeyDer<'static>> {
    const SUPPORTED: &[(&str, fn(Vec<u8>) -> PrivateKeyDer<'static>)] = &[
        ("PRIVATE KEY", |der| {
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(der))
        }),
        ("RSA PRIVATE KEY", |der| {
            PrivateKeyDer::Pkcs1(PrivatePkcs1KeyDer::from(der))
        }),
        ("EC PRIVATE KEY", |der| {
            PrivateKeyDer::Sec1(PrivateSec1KeyDer::from(der))
        }),
    ];
    for (label, wrap) in SUPPORTED {
        if let Some(der) = pem_der_blocks(pem, label).into_iter().next() {
            return Ok(wrap(der));
        }
    }
    bail!("no supported private key PEM block (PKCS#8/RSA/EC) found in {key_path}");
}

fn pem_der_blocks(pem: &str, label: &str) -> Vec<Vec<u8>> {
    let begin = format!("-----BEGIN {label}-----");
    let end = format!("-----END {label}-----");
    let mut ders = Vec::new();
    let mut rest = pem;
    while let Some(start) = rest.find(&begin) {
        let body_start = start + begin.len();
        let Some(stop) = rest[body_start..].find(&end) else {
            // Unterminated block: skip past this BEGIN marker and keep looking.
            rest = &rest[body_start..];
            continue;
        };
        let encoded: String = rest[body_start..body_start + stop]
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();
        match BASE64_STANDARD.decode(encoded.as_bytes()) {
            Ok(der) => ders.push(der),
            // Malformed body (e.g. a nested marker swallowed an END): skip it.
            Err(_) => {
                rest = &rest[body_start..];
                continue;
            }
        }
        rest = &rest[body_start + stop + end.len()..];
    }
    ders
}

/// Serve one or more listeners. When TLS is configured, each connection is
/// dispatched by sniffing the first byte: `0x16` (TLS ClientHello) gets
/// the full router, anything else gets the dashboard-only router — so the
/// same port can host both HTTPS (full API) and HTTP (panel) without a
/// separate bind. When TLS is off, all connections go to the full router
/// over plain HTTP.
async fn serve_multi(
    listeners: Arc<Vec<tokio::net::TcpListener>>,
    acceptor: Option<TlsAcceptor>,
    full_app: Router,
    dashboard_app: Router,
    drain_timeout: u64,
) -> Result<()> {
    // Security boundary: when TLS is configured, a plain-HTTP arrival on
    // the HTTPS port is treated as a request for the dashboard surface
    // only (no /pull, no settings writes, no daemon actions). Without
    // TLS, plain HTTP is the operator's own choice and the full router
    // is reachable on every listener — same as the single-listener no-TLS
    // branch in main().
    let plain_http_full = acceptor.is_none();
    // FuturesUnordered drops a future once it yields, so every accept must
    // re-arm its listener before the next loop turn or the accept loop dies
    // after exactly one connection per port.
    let scheme = if acceptor.is_some() {
        "https+http"
    } else {
        "http"
    };
    for listener in listeners.iter() {
        match listener.local_addr() {
            Ok(addr) => info!(listen = %addr, %scheme, "listening"),
            Err(error) => warn!(%error, "local_addr unavailable"),
        }
    }
    type AcceptFuture = std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = (
                        usize,
                        std::io::Result<(tokio::net::TcpStream, std::net::SocketAddr)>,
                    ),
                > + Send,
        >,
    >;
    let mut accepts = futures_util::stream::FuturesUnordered::<AcceptFuture>::new();
    for (i, listener) in listeners.iter().enumerate() {
        let listeners = Arc::clone(&listeners);
        accepts.push(Box::pin(async move { (i, listeners[i].accept().await) }));
    }
    let mut conns: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
    loop {
        let (i, accepted) = tokio::select! {
            Some(pair) = accepts.next(), if !accepts.is_empty() => pair,
            _ = shutdown_signal() => break,
        };
        let listeners = Arc::clone(&listeners);
        accepts.push(Box::pin(async move { (i, listeners[i].accept().await) }));

        let (mut stream, _peer) = match accepted {
            Ok(pair) => pair,
            Err(error) => {
                warn!(%error, "accept failed");
                continue;
            }
        };
        let full_app = full_app.clone();
        let dashboard_app = dashboard_app.clone();
        let acceptor = acceptor.clone();
        conns.spawn(async move {
            // TLS protocol sniffing: read the first byte of each connection
            // (consumes it from the stream) to decide TLS (0x16 ClientHello)
            // vs plain HTTP, then prepend that same byte back via Rewind so
            // axum/rustls see the complete stream.
            let mut sniff_buf = [0u8; 1];
            let first = match stream.read(&mut sniff_buf).await {
                Ok(0) => return, // client closed before sending anything
                Ok(_) => sniff_buf[0],
                Err(error) => {
                    warn!(%error, "sniff failed");
                    return;
                }
            };
            match acceptor.as_ref() {
                Some(acceptor) if first == 0x16 => {
                    // Prepend the consumed byte so rustls sees a complete
                    // ClientHello (we read 1 byte to sniff, must put it back).
                    let restored = Rewind::new(first, stream);
                    let tls = match acceptor.accept(restored).await {
                        Ok(tls) => tls,
                        Err(error) => {
                            warn!(%error, "TLS handshake failed");
                            return;
                        }
                    };
                    serve_conn(TokioIo::new(tls), full_app, drain_timeout).await;
                }
                _ => {
                    let app = if plain_http_full {
                        full_app.clone()
                    } else {
                        // HTTPS port + plain HTTP, or any multi-listener plain
                        // HTTP layout: dashboard-only surface.
                        dashboard_app.clone()
                    };
                    serve_conn(TokioIo::new(Rewind::new(first, stream)), app, drain_timeout).await;
                }
            }
        });
    }
    // Mirror the HTTP path's drain: let in-flight blob transfers finish (or
    // hit the shared drain deadline) instead of killing the runtime at once.
    while let Some(joined) = tokio::select! {
        joined = conns.join_next() => joined,
        _ = drain_deadline(drain_timeout) => {
            warn!(drain_timeout_secs = drain_timeout, "drain timeout exceeded; closing remaining connections");
            conns.abort_all();
            None
        }
    } {
        if let Err(error) = joined {
            if !error.is_cancelled() {
                warn!(%error, "connection task failed");
            }
        }
    }
    Ok(())
}

/// Prepend a buffered byte to an async stream so the protocol sniffer
/// can read the first byte to decide TLS vs HTTP and still hand the
/// complete stream to axum. The first call to `poll_read` returns the
/// buffered byte(s); subsequent calls fall through to the inner stream.
struct Rewind<I> {
    buffered: std::collections::VecDeque<u8>,
    inner: I,
}

impl<I: tokio::io::AsyncRead + Unpin + Send> Rewind<I> {
    fn new(first: u8, inner: I) -> Self {
        let mut buffered = std::collections::VecDeque::new();
        buffered.push_back(first);
        Self { buffered, inner }
    }
}

impl<I: tokio::io::AsyncRead + Unpin + Send> tokio::io::AsyncRead for Rewind<I> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if let Some(b) = self.buffered.pop_front() {
            buf.put_slice(&[b]);
            return std::task::Poll::Ready(Ok(()));
        }
        let me = &mut *self;
        std::pin::Pin::new(&mut me.inner).poll_read(cx, buf)
    }
}

impl<I: tokio::io::AsyncWrite + Unpin + Send> tokio::io::AsyncWrite for Rewind<I> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let me = &mut *self;
        std::pin::Pin::new(&mut me.inner).poll_write(cx, buf)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let me = &mut *self;
        std::pin::Pin::new(&mut me.inner).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let me = &mut *self;
        std::pin::Pin::new(&mut me.inner).poll_shutdown(cx)
    }
}

async fn serve_conn<I>(io: TokioIo<I>, app: Router, drain_timeout: u64)
where
    I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    // axum's Router speaks Service<Request<Body>>, hyper feeds Request<Incoming>:
    // bridge the body types per request via service_fn + oneshot.
    let service = hyper::service::service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
        let app = app.clone();
        async move { app.oneshot(req.map(Body::new)).await }
    });
    let builder = HttpConnBuilder::new(TokioExecutor::new());
    let mut conn = std::pin::pin!(builder.serve_connection_with_upgrades(io, service));
    tokio::select! {
        result = conn.as_mut() => {
            if let Err(error) = result {
                warn!(%error, "connection error");
            }
        }
        _ = shutdown_signal() => {
            conn.as_mut().graceful_shutdown();
            tokio::select! {
                result = conn.as_mut() => {
                    if let Err(error) = result {
                        warn!(%error, "connection error during drain");
                    }
                }
                _ = drain_deadline(drain_timeout) => {
                    warn!(drain_timeout_secs = drain_timeout, "drain timeout exceeded; closing remaining connections");
                }
            }
        }
    }
}

async fn healthz(method: Method) -> Response {
    if method != Method::GET && method != Method::HEAD {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    "ok\n".into_response()
}

async fn metrics_history(State(state): State<Arc<AppState>>) -> Response {
    let stats = Arc::clone(&state.stats);
    let snap = metrics::snapshot_now(&stats, state.cache.bytes_on_disk(), state.cache.max_bytes());
    match metrics::history_json(&state.metrics_history, snap).await {
        Ok(value) => ([(CONTENT_TYPE, "application/json")], value.to_string()).into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{error:#}\n")).into_response(),
    }
}

async fn stats(State(state): State<Arc<AppState>>) -> Response {
    let sources = state.sources.read().await.clone();
    let (chunk_mib, chunk_concurrency) = sources.chunk_plan().await;
    let body = serde_json::json!({
        "blob_cache_hits": state.stats.blob_hits.load(Ordering::Relaxed),
        "blob_cache_misses": state.stats.blob_misses.load(Ordering::Relaxed),
        "bytes_from_cache": state.stats.bytes_from_cache.load(Ordering::Relaxed),
        "bytes_from_upstream": state.stats.bytes_from_upstream.load(Ordering::Relaxed),
        "disk_bytes": state.cache.bytes_on_disk(),
        "disk_cap_bytes": state.cache.max_bytes(),
        "cache_entries": state.cache.entry_count().await,
        "manifest_entries": state.manifests.len().await,
        "active_downloads": state.stats.active_downloads.lock().await.len(),
        "uptime_secs": state.started.elapsed().as_secs(),
        "version": env!("CARGO_PKG_VERSION"),
        "chunk_mib": chunk_mib / 1024 / 1024,
        "chunk_concurrency": chunk_concurrency,
        "accel_addr": state.accel_addr,
    });
    ([(CONTENT_TYPE, "application/json")], body.to_string()).into_response()
}

async fn downloads(State(state): State<Arc<AppState>>) -> Response {
    let map = state.stats.active_downloads.lock().await;
    let rows: Vec<serde_json::Value> = map.values().map(|d| d.progress_json()).collect();
    (
        [(CONTENT_TYPE, "application/json")],
        serde_json::json!(rows).to_string(),
    )
        .into_response()
}

async fn dashboard_redirect() -> Response {
    // When the plain-HTTP dashboard port is configured, prefer it so the
    // browser does not have to deal with a TLS warning for the panel.
    // LISTEN_ADDR_HTTP_HOST overrides the host portion of the URL; defaults
    // to the gateway's primary domain (or the loopback fallback).
    if let Some(addr) = env::var("LISTEN_ADDR_HTTP").ok().filter(|v| !v.is_empty()) {
        if let Some((_, port)) = addr.rsplit_once(':') {
            if let Ok(port) = port.parse::<u16>() {
                let host = env::var("LISTEN_ADDR_HTTP_HOST")
                    .ok()
                    .filter(|v| !v.is_empty())
                    .unwrap_or_else(|| {
                        env::var("DOMAIN")
                            .ok()
                            .filter(|v| !v.is_empty())
                            .unwrap_or_else(|| "127.0.0.1".to_owned())
                    });
                return Redirect::permanent(&format!("http://{}:{}/dashboard", host, port))
                    .into_response();
            }
        }
    }
    Redirect::temporary("/dashboard").into_response()
}

async fn not_found_on_http() -> Response {
    (
        StatusCode::NOT_FOUND,
        "this endpoint is not served over plain HTTP; use the HTTPS port (20516)\n",
    )
        .into_response()
}

/* ---------- settings ---------- */

async fn get_settings(State(state): State<Arc<AppState>>) -> Response {
    let settings = settings::Settings::load(&state.settings_path);
    let cert_job = state.cert_job.lock().await.clone();
    let extra = json!({
        "pull_target": settings.pull_target().unwrap_or_default(),
        "cert_job": cert_job.unwrap_or(json!({"status": "idle"})),
    });
    (
        [(CONTENT_TYPE, "application/json")],
        settings::settings_view(&settings, extra).to_string(),
    )
        .into_response()
}

async fn save_settings(State(state): State<Arc<AppState>>, request: Request) -> Response {
    if let Some(resp) = require_mgmt_auth(&state, &request) {
        return resp;
    }
    if request.method() != Method::PATCH {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    let bytes = match request.into_body().collect().await {
        Ok(c) => c.to_bytes(),
        Err(_) => return (StatusCode::BAD_REQUEST, "invalid body\n").into_response(),
    };
    let Ok(value) = serde_json::from_slice::<Value>(&bytes) else {
        return (StatusCode::BAD_REQUEST, "expected json body\n").into_response();
    };
    let patch = settings::patch_from_body(&value);
    // Serial tx: lock the load+apply+save window so two concurrent
    // PATCHes can't read the same baseline and lose each other's fields.
    let _guard = state.settings_lock.lock().await;
    match settings::Settings::load(&state.settings_path).apply(patch, &state.settings_path) {
        Ok(updated) => {
            let view = settings::settings_view(&updated, Value::Null);
            ([(CONTENT_TYPE, "application/json")], view.to_string()).into_response()
        }
        Err(error) => (StatusCode::BAD_REQUEST, format!("{error:#}\n")).into_response(),
    }
}

/// Run acme.sh in a one-shot container to issue the public certificate for
/// the configured domain family; on success the container restarts itself so
/// the new certificate is loaded (cert paths take precedence over env TLS).
async fn issue_cert(State(state): State<Arc<AppState>>, request: Request) -> Response {
    if let Some(resp) = require_mgmt_auth(&state, &request) {
        return resp;
    }
    let settings = settings::Settings::load(&state.settings_path);
    let (domain, token, zone, account) = match (
        settings.domain.clone(),
        settings.cf_token.clone(),
        settings.cf_zone_id.clone(),
        settings.cf_account_id.clone(),
    ) {
        (Some(d), Some(t), Some(z), Some(a)) if !t.is_empty() && !z.is_empty() && !a.is_empty() => {
            (d, t, z, a)
        }
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                "需要先在设置里填好 主域名 / Cloudflare Token / Zone ID / Account ID\n",
            )
                .into_response()
        }
    };
    let running = {
        let mut job = state.cert_job.lock().await;
        if job
            .as_ref()
            .is_some_and(|j| j.get("status").and_then(|s| s.as_str()) == Some("running"))
        {
            true
        } else {
            *job = Some(json!({"status": "running", "message": "准备中…"}));
            false
        }
    };
    if running {
        return (
            StatusCode::CONFLICT,
            "certificate issuance already running\n",
        )
            .into_response();
    }

    let socket = env_or("DOCKER_SOCKET", "/var/run/docker.sock");
    let self_id = dockerpull::self_container_id(&socket)
        .await
        .unwrap_or_default();
    if self_id.is_empty() {
        return (StatusCode::INTERNAL_SERVER_ERROR, "无法确定自身容器 ID\n").into_response();
    }
    let image_ref = format!(
        "{}/neilpang/acme.sh:latest",
        settings
            .pull_target()
            .unwrap_or_else(|| "127.0.0.1:4443".into())
    );
    let accel_domain = settings.accel_domain.clone();
    let job_state = Arc::clone(&state);
    tokio::spawn(async move {
        let progress_fn = |message: String| {
            if let Ok(mut inner) = job_state.cert_job.try_lock() {
                if let Some(job) = inner.as_mut() {
                    job["message"] = Value::String(message);
                }
            }
        };
        let progress: &(dyn Fn(String) + Send + Sync) = &progress_fn;
        let result = dockerpull::issue_cert_job(
            &socket,
            &self_id,
            &image_ref,
            std::path::Path::new("/data"),
            &domain,
            accel_domain.as_deref(),
            &token,
            &zone,
            &account,
            progress,
        )
        .await;
        let mut job = job_state.cert_job.lock().await;
        match result {
            Ok(()) => {
                if let Some(job) = job.as_mut() {
                    job["status"] = Value::String("done".into());
                    job["restarting"] = Value::Bool(true);
                }
                drop(job);
                // Bring the new certificate online: the process loads
                // certs at startup, so restart the gateway container.
                tokio::time::sleep(Duration::from_millis(1200)).await;
                let socket = env_or("DOCKER_SOCKET", "/var/run/docker.sock");
                if let Ok(self_id) = dockerpull::self_container_id(&socket).await {
                    let _ = dockerpull::docker_api(
                        &socket,
                        hyper::Method::POST,
                        &format!("/containers/{self_id}/restart?t=2"),
                        None,
                        60,
                    )
                    .await;
                }
            }
            Err(error) => {
                if let Some(job) = job.as_mut() {
                    job["status"] = Value::String("failed".into());
                    job["message"] = Value::String(format!("{error:#}"));
                }
            }
        }
    });
    (StatusCode::ACCEPTED, "certificate issuance started\n").into_response()
}

async fn create_dns_records(State(state): State<Arc<AppState>>, request: Request) -> Response {
    if let Some(resp) = require_mgmt_auth(&state, &request) {
        return resp;
    }
    let settings = settings::Settings::load(&state.settings_path);
    let (Some(domain), Some(lan_ip), Some(token)) = (
        settings.domain.as_deref(),
        settings.lan_ip.as_deref(),
        settings.cf_token.as_deref(),
    ) else {
        return (
            StatusCode::BAD_REQUEST,
            "需要先在设置里填好 主域名 / LAN IP / Cloudflare Token\n",
        )
            .into_response();
    };

    let api = "https://api.cloudflare.com/client/v4";
    let auth = [("Authorization", format!("Bearer {token}"))];
    let auth_value = match HeaderValue::from_str(&auth[0].1) {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                "cf_token contains characters not allowed in HTTP headers\n".to_string(),
            )
                .into_response();
        }
    };
    let zone_lookup = match state
        .client
        .get(format!("{api}/zones"))
        .query(&[("name", domain)])
        .header("Authorization", auth_value)
        .send()
        .await
    {
        Ok(r) => r,
        Err(error) => {
            return (StatusCode::BAD_GATEWAY, format!("zone lookup: {error}\n")).into_response()
        }
    };
    let zones: Value = match zone_lookup.text().await {
        Ok(text) => serde_json::from_str(&text).unwrap_or(Value::Null),
        Err(error) => return (StatusCode::BAD_GATEWAY, format!("{error}\n")).into_response(),
    };
    let Some(zone_id) = zones
        .pointer("/result/0/id")
        .and_then(|v| v.as_str())
        .map(str::to_owned)
    else {
        return (
            StatusCode::BAD_REQUEST,
            format!("zone {domain} not found in this Cloudflare account\n"),
        )
            .into_response();
    };

    let names = [
        domain.to_owned(),
        format!("docker.{domain}"),
        format!("git.{domain}"),
        format!("*.{domain}"),
    ];
    let mut created = Vec::new();
    let mut skipped = Vec::new();
    for name in &names {
        let existing = state
            .client
            .get(format!("{api}/zones/{zone_id}/dns_records"))
            .query(&[("type", "A"), ("name", name.as_str())])
            .header("Authorization", format!("Bearer {token}"))
            .send()
            .await;
        let existing_body = match existing {
            Ok(r) => r.text().await.unwrap_or_default(),
            Err(_) => String::new(),
        };
        let already = serde_json::from_str::<Value>(&existing_body)
            .ok()
            .and_then(|v| {
                v.pointer("/result/0/id")
                    .and_then(|v| v.as_str())
                    .map(str::to_owned)
            });
        if already.is_some() {
            skipped.push(name.clone());
            continue;
        }
        let created_record = state
            .client
            .post(format!("{api}/zones/{zone_id}/dns_records"))
            .header("Authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(
                json!({
                    "type": "A",
                    "name": name,
                    "content": lan_ip,
                    "ttl": 300,
                    "proxied": false
                })
                .to_string(),
            )
            .send()
            .await;
        match created_record {
            Ok(r) if r.status().is_success() => created.push(name.clone()),
            Ok(r) => {
                let status = r.status();
                let body = r.text().await.unwrap_or_default();
                return (
                    StatusCode::BAD_GATEWAY,
                    format!("create {name} failed: HTTP {status} {body}\n"),
                )
                    .into_response();
            }
            Err(error) => return (StatusCode::BAD_GATEWAY, format!("{error}\n")).into_response(),
        }
    }
    (
        [(CONTENT_TYPE, "application/json")],
        json!({"created": created, "skipped_existing": skipped}).to_string(),
    )
        .into_response()
}

async fn restart_gateway(State(_state): State<Arc<AppState>>, request: Request) -> Response {
    if let Some(resp) = require_mgmt_auth(&_state, &request) {
        return resp;
    }
    let socket = env_or("DOCKER_SOCKET", "/var/run/docker.sock");
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(800)).await;
        if let Ok(self_id) = dockerpull::self_container_id(&socket).await {
            let _ = dockerpull::docker_api(
                &socket,
                hyper::Method::POST,
                &format!("/containers/{self_id}/restart?t=2"),
                None,
                60,
            )
            .await;
        }
    });
    (
        [(CONTENT_TYPE, "application/json")],
        "{\"restarting\": true}".to_string(),
    )
        .into_response()
}

async fn serve_ca(State(state): State<Arc<AppState>>) -> Response {
    let Some(path) = &state.ca_path else {
        return StatusCode::NOT_FOUND.into_response();
    };
    match tokio::fs::read(path).await {
        Ok(bytes) => (
            [
                (
                    CONTENT_TYPE,
                    HeaderValue::from_static("application/x-x509-ca-cert"),
                ),
                (
                    HeaderName::from_static("content-disposition"),
                    HeaderValue::from_static("attachment; filename=\"web-proxy-ca.crt\""),
                ),
            ],
            bytes,
        )
            .into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn start_pull(State(state): State<Arc<AppState>>, request: Request) -> Response {
    if let Some(resp) = require_mgmt_auth(&state, &request) {
        return resp;
    }
    if request.method() != Method::POST {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    // The daemon must reach us to pull. Settings (set in the dashboard) win,
    // then PULL_VIA_HOST; a plain request Host is only accepted when it
    // names this machine (loopback or IP literal) — an attacker-chosen
    // domain would turn the daemon into a puller of external registries.
    let settings = settings::Settings::load(&state.settings_path);
    let gateway_host = match settings.pull_target() {
        Some(target) => target,
        None => match &state.pull_via_host {
            Some(host) => host.clone(),
            None => {
                let header = request
                    .headers()
                    .get(HOST)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("127.0.0.1");
                let name = header
                    .rsplit_once(':')
                    .map(|(name, _)| name.trim_start_matches('[').trim_end_matches(']'))
                    .unwrap_or(header);
                // Accept only loopback. A private / link-local IP literal
                // (e.g. `169.254.169.254`) still bypasses the Host-as-domain
                // check above and would tell the daemon to pull from an
                // internal endpoint the operator never authorized.
                let is_loopback = match name.parse::<IpAddr>() {
                    Ok(IpAddr::V4(v4)) => v4.is_loopback(),
                    Ok(IpAddr::V6(v6)) => v6.is_loopback(),
                    Err(_) => name == "localhost",
                };
                if is_loopback {
                    header.to_owned()
                } else {
                    return (
                        StatusCode::BAD_REQUEST,
                        "refusing Host header that is not a loopback address; set PULL_VIA_HOST\n",
                    )
                        .into_response();
                }
            }
        },
    };
    let bytes = match request.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_) => return (StatusCode::BAD_REQUEST, "invalid body\n").into_response(),
    };
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
        return (StatusCode::BAD_REQUEST, "expected json body\n").into_response();
    };
    let Some(image) = value.get("image").and_then(|i| i.as_str()) else {
        return (StatusCode::BAD_REQUEST, "missing \"image\" field\n").into_response();
    };
    let spec = match dockerpull::plan_pull(image, &gateway_host) {
        Ok(spec) => spec,
        Err(error) => return (StatusCode::BAD_REQUEST, format!("{error}\n")).into_response(),
    };
    match state.pulls.start(spec).await {
        Ok(job) => (
            [(CONTENT_TYPE, "application/json")],
            serde_json::json!({"id": job.id, "image": job.spec.image}).to_string(),
        )
            .into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{error}\n")).into_response(),
    }
}

/// Cache-preload an image: walk the manifest, then warm every blob into the
/// content-addressed cache without holding a client connection. Reuses the
/// chunked downloader's single-flight + resume machinery so the next real
/// pull hits a warm cache.
async fn start_warm(State(state): State<Arc<AppState>>, request: Request) -> Response {
    if let Some(resp) = require_mgmt_auth(&state, &request) {
        return resp;
    }
    if request.method() != Method::POST {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    let bytes = match request.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_) => return (StatusCode::BAD_REQUEST, "invalid body\n").into_response(),
    };
    let value = match serde_json::from_slice::<serde_json::Value>(&bytes) {
        Ok(value) => value,
        Err(_) => return (StatusCode::BAD_REQUEST, "expected json body\n").into_response(),
    };
    let Some(image) = value
        .get("image")
        .and_then(|i| i.as_str())
        .map(str::to_owned)
    else {
        return (StatusCode::BAD_REQUEST, "missing \"image\" field\n").into_response();
    };
    let parsed = match dockerpull::parse_image_ref(&image) {
        Ok(parsed) => parsed,
        Err(error) => return (StatusCode::BAD_REQUEST, format!("{error}\n")).into_response(),
    };
    let registry = match parsed.registry.as_str() {
        "docker.io" => Registry::DockerHub,
        "ghcr.io" => Registry::Ghcr,
        other => {
            return (
                StatusCode::BAD_REQUEST,
                format!("unsupported registry {other}\n"),
            )
                .into_response()
        }
    };
    let ref_ = match parsed.tag.clone().or(parsed.digest.clone()) {
        Some(ref_) => ref_,
        None => "latest".to_owned(),
    };
    let manifest_path = format!("/v2/{}/manifests/{}", parsed.path, ref_);

    // Fetch the manifest. Use the single-image type so the registry returns
    // the platform-specific manifest instead of a list; the chunked downloader
    // works one blob at a time. Server-side warm has no inbound
    // Authorization, so fetch an anonymous bearer from the registry's
    // configured token endpoint first (matches the docker pull OAuth dance).
    let accept = "application/vnd.docker.distribution.manifest.v2+json,application/vnd.oci.image.manifest.v1+json";
    let token = match fetch_anonymous_token(&state, registry.config(&state), &parsed.path).await {
        Ok(token) => token,
        Err(error) => {
            return (
                StatusCode::BAD_GATEWAY,
                format!("warm token fetch: {error}\n"),
            )
                .into_response();
        }
    };
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("accept"),
        HeaderValue::from_static(accept),
    );
    if !token.is_empty() {
        if let Ok(value) = HeaderValue::from_str(&format!("Bearer {token}")) {
            headers.insert(AUTHORIZATION, value);
        }
    }
    let url = match registry_url(registry.config(&state), &manifest_path, None) {
        Ok(url) => url,
        Err(response) => return response,
    };
    let upstream =
        match fetch_following_redirects(&state, url, Method::GET, &headers, None, true).await {
            Ok(upstream) => upstream,
            Err(response) => return response,
        };
    if !upstream.status().is_success() {
        return (
            StatusCode::BAD_GATEWAY,
            format!("manifest fetch HTTP {}\n", upstream.status()),
        )
            .into_response();
    }
    let manifest_bytes = match upstream.bytes().await {
        Ok(bytes) => bytes,
        Err(error) => {
            return (
                StatusCode::BAD_GATEWAY,
                format!("manifest body read: {error}\n"),
            )
                .into_response();
        }
    };
    let digests = match parse_image_manifest_digests(&manifest_bytes) {
        Ok(digests) => digests,
        Err(error) => {
            return (
                StatusCode::BAD_GATEWAY,
                format!("manifest parse: {error}\n"),
            )
                .into_response();
        }
    };

    let mut queued = 0;
    let mut cached = 0;
    let mut skipped_kind = 0;
    for (kind, digest) in digests {
        // Manifest-list children are platform manifests, not blobs. Pulling
        // them via /blobs/ writes manifests into the blob cache and breaks
        // any future blob GET that happens to land on the same digest (rare
        // but real). Recursing into manifests is out of scope for a warm —
        // the chunked downloader handles individual blobs only.
        if kind != "blob" {
            skipped_kind += 1;
            continue;
        }
        let Some(hex) = sha256_hex(&digest) else {
            continue;
        };
        if state.cache.lookup(&hex).await.is_some() {
            cached += 1;
            continue;
        }
        let blob_path = format!("/v2/{}/blobs/{}", parsed.path, digest);
        // Try (not await) the per-digest inflight lock: holding it for the
        // full download would make a second /pull/warm POST for the same
        // image block until the slowest layer finishes. When another warm
        // already owns the slot, count it as in-progress and move on so the
        // HTTP request returns promptly.
        let guard = state.cache.inflight_lock(&hex).await;
        let flight = match guard.try_lock_owned() {
            Ok(flight) => flight,
            Err(_) => {
                warn!(
                    image = %image,
                    digest = %digest,
                    "warm blob already in flight; skipped"
                );
                continue;
            }
        };
        if state.cache.lookup(&hex).await.is_some() {
            cached += 1;
            continue;
        }
        // chunks::download needs content-length to split the blob into chunks;
        // a 0-sized call skips the chunk loop silently and never writes the
        // cache. Resolve size with HEAD before scheduling the download.
        let size = match head_blob_size(&state, registry, &blob_path, &parsed.path).await {
            Ok(size) => size,
            Err(error) => {
                warn!(
                    image = %image,
                    digest = %digest,
                    error = %error,
                    "warm HEAD failed; blob skipped"
                );
                continue;
            }
        };
        if size == 0 {
            continue;
        }

        let part_path = state.cache.new_part_path(&hex);
        let final_path = state.cache.blob_path(&hex);
        let sources = state.sources.read().await.clone();
        let cache_arc = Arc::clone(&state.cache);
        let stats = Arc::clone(&state.stats);
        let registry_label = registry.route_prefix().to_owned();
        let blob_path_for_task = blob_path.clone();
        let digest_for_task = digest.clone();
        let client = state.client.clone();
        let kind_for_log = kind.to_owned();
        let allowed_hosts_warm = Arc::new(state.allowed_registry_hosts.clone());
        let image_for_log = image.clone();
        tokio::spawn(async move {
            let _flight = flight;
            let result = chunks::download(
                client,
                sources,
                cache_arc,
                stats,
                registry_label,
                blob_path_for_task,
                digest_for_task.clone(),
                size,
                part_path,
                final_path,
                None,
                allowed_hosts_warm,
            )
            .await;
            if let Err(error) = result {
                warn!(
                    image = %image_for_log,
                    kind = %kind_for_log,
                    digest = %digest_for_task,
                    error = %error,
                    "warm blob download failed"
                );
            }
        });
        queued += 1;
    }

    (
        StatusCode::ACCEPTED,
        [(CONTENT_TYPE, "application/json")],
        serde_json::json!({
            "status": "warming",
            "image": image,
            "queued": queued,
            "already_cached": cached,
            "skipped_kind": skipped_kind,
        })
        .to_string(),
    )
        .into_response()
}

/// Minimal OCI/Docker v2 image-manifest parser: extracts every blob digest
/// referenced by the manifest (config + layers) so the warm flow can preload
/// them into the cache.
fn parse_image_manifest_digests(body: &[u8]) -> Result<Vec<(&'static str, String)>> {
    let value: serde_json::Value = serde_json::from_slice(body).context("invalid json")?;
    let media_type = value
        .get("media_type")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let schema_version = value
        .get("schemaVersion")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    // Schema 1 manifests use per-blob SHA + JSON signatures with a different
    // digest layout that the chunked downloader cannot verify. Reject up
    // front so the warm doesn't half-process them and the proxy surfaces a
    // clear 400 instead of a misleading 502 from the sha256 check.
    if schema_version == 1 {
        bail!("schemaVersion 1 manifests are unsupported");
    }
    let mut out = Vec::new();
    if media_type.contains("manifest.list")
        || schema_version == 2 && media_type.is_empty() && value.get("manifests").is_some()
    {
        // Manifest list: each entry is itself a platform manifest by digest.
        if let Some(manifests) = value.get("manifests").and_then(|v| v.as_array()) {
            for entry in manifests {
                if let Some(digest) = entry.get("digest").and_then(|v| v.as_str()) {
                    out.push(("manifest", digest.to_owned()));
                }
            }
        }
        return Ok(out);
    }
    if let Some(config) = value.get("config") {
        if let Some(digest) = config.get("digest").and_then(|v| v.as_str()) {
            out.push(("config", digest.to_owned()));
        }
    }
    if let Some(layers) = value.get("layers").and_then(|v| v.as_array()) {
        for layer in layers {
            if let Some(digest) = layer.get("digest").and_then(|v| v.as_str()) {
                out.push(("layer", digest.to_owned()));
            }
        }
    }
    if out.is_empty() {
        bail!("manifest has no recognized digest references");
    }
    Ok(out)
}

/// HEAD a blob URL to learn its `Content-Length`. Used by the warm flow
/// to size each chunked download up front; failures here are logged and
/// the blob is skipped (no client connection is involved).
async fn head_blob_size(
    state: &AppState,
    registry: Registry,
    blob_path: &str,
    repo: &str,
) -> Result<u64> {
    let url = registry_url(registry.config(state), blob_path, None)
        .map_err(|response| anyhow::anyhow!("invalid blob url: {}", response.status()))?;
    let token = fetch_anonymous_token(state, registry.config(state), repo).await?;
    let mut headers = HeaderMap::new();
    if !token.is_empty() {
        if let Ok(value) = HeaderValue::from_str(&format!("Bearer {token}")) {
            headers.insert(AUTHORIZATION, value);
        }
    }
    let upstream = fetch_following_redirects(state, url, Method::HEAD, &headers, None, true)
        .await
        .map_err(|response| anyhow::anyhow!("HEAD not successful: {}", response.status()))?;
    upstream
        .content_length()
        .ok_or_else(|| anyhow::anyhow!("HEAD missing Content-Length"))
}

/// Fetch an anonymous bearer token for the configured registry's
/// repository scope (matches the docker pull OAuth dance). Returns an
/// empty string when the registry has no `token_url` (anonymous mirror)
/// or when the token endpoint cannot be reached — the caller then falls
/// back to an unauthenticated request, which works for fully-public blobs
/// and cleanly errors out for private ones.
async fn fetch_anonymous_token(
    state: &AppState,
    registry_config: &RegistryConfig,
    repo: &str,
) -> Result<String> {
    if registry_config.token_url.is_empty() {
        return Ok(String::new());
    }
    let mut url =
        url::Url::parse(&registry_config.token_url).context("invalid registry token url")?;
    {
        let mut query = url.query_pairs_mut();
        if !registry_config.token_service.is_empty() {
            query.append_pair("service", &registry_config.token_service);
        }
        query.append_pair("scope", &format!("repository:{repo}:pull"));
    }
    let response = state
        .client
        .get(url)
        .timeout(Duration::from_secs(15))
        .send()
        .await
        .map_err(|error| anyhow::anyhow!("token request: {error}"))?;
    let text = response
        .text()
        .await
        .map_err(|error| anyhow::anyhow!("token body: {error}"))?;
    let value: serde_json::Value =
        serde_json::from_str(&text).map_err(|error| anyhow::anyhow!("token json: {error}"))?;
    Ok(value
        .get("token")
        .or_else(|| value.get("access_token"))
        .and_then(|token| token.as_str())
        .unwrap_or_default()
        .to_owned())
}

async fn list_pulls(State(state): State<Arc<AppState>>) -> Response {
    let rows = state.pulls.snapshot().await;
    (
        [(CONTENT_TYPE, "application/json")],
        serde_json::json!(rows).to_string(),
    )
        .into_response()
}

async fn sources_view(State(state): State<Arc<AppState>>) -> Response {
    let sources = state.sources.read().await.clone();
    let snapshot = sources.weights_snapshot().await;
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
                "throughput_bps": stats.throughput_bps,
                "last_seen": stats.last_seen.is_some(),
            })
        })
        .collect::<Vec<_>>());
    ([(CONTENT_TYPE, "application/json")], body.to_string()).into_response()
}

async fn get_sources_config(State(state): State<Arc<AppState>>) -> Response {
    let sources = state.sources.read().await.clone();
    (
        [(CONTENT_TYPE, "application/json")],
        sources.specs_json().to_string(),
    )
        .into_response()
}

async fn save_sources_config(State(state): State<Arc<AppState>>, request: Request) -> Response {
    if let Some(resp) = require_mgmt_auth(&state, &request) {
        return resp;
    }
    let bytes = match request.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_) => return (StatusCode::BAD_REQUEST, "invalid body\n").into_response(),
    };
    let value = match serde_json::from_slice::<Value>(&bytes) {
        Ok(value) => value,
        Err(_) => return (StatusCode::BAD_REQUEST, "expected json body\n").into_response(),
    };
    let specs = match sources::parse_value(&value) {
        Ok(specs) => specs,
        Err(error) => return (StatusCode::BAD_REQUEST, format!("{error:#}\n")).into_response(),
    };
    let mut current = state.sources.write().await;
    if let Err(error) = sources::save(&state.sources_path, &specs) {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("{error:#}\n")).into_response();
    }
    let pool = sources::SourcePool::new(state.client.clone(), specs);
    let body = pool.specs_json();
    *current = pool;
    ([(CONTENT_TYPE, "application/json")], body.to_string()).into_response()
}

async fn trigger_probe(State(state): State<Arc<AppState>>, request: Request) -> Response {
    if let Some(resp) = require_mgmt_auth(&state, &request) {
        return resp;
    }
    let sources = state.sources.read().await.clone();
    sources.trigger_probe();
    StatusCode::ACCEPTED.into_response()
}

async fn get_logs(State(state): State<Arc<AppState>>) -> Response {
    let entries = state.logs.snapshot();
    let body = serde_json::json!({
        "count": entries.len(),
        "entries": entries.iter().rev().map(|e| e.to_json()).collect::<Vec<_>>(),
    });
    ([(CONTENT_TYPE, "application/json")], body.to_string()).into_response()
}

/// Record a single log entry for a gateway decision. Truncates `note`
/// to 160 chars so a 500-entry ring stays cheap.
fn record_log(
    logs: &logs::Logs,
    route: &str,
    method: &str,
    status: u16,
    duration: std::time::Duration,
    category: &'static str,
    note: impl Into<String>,
) {
    let note = note.into();
    let truncated: String = if note.chars().count() > 160 {
        // Take 159 chars + ellipsis = 160 total, matching the docstring on
        // `record_log` that promises "to 160 chars".
        note.chars().take(159).collect::<String>() + "…"
    } else {
        note
    };
    logs.record(route, method, status, duration, category, truncated);
}

async fn clear_cache(State(state): State<Arc<AppState>>, request: Request) -> Response {
    if let Some(resp) = require_mgmt_auth(&state, &request) {
        return resp;
    }
    // Clearing while a chunked download is mid-flight deletes its .part and
    // dooms that transfer's commit (the client retry self-heals), so surface
    // the count instead of failing the request.
    let active = state.stats.active_downloads.lock().await.len();
    let freed_bytes = state.cache.clear().await;
    state.manifests.clear().await;
    let body = serde_json::json!({
        "status": "cleared",
        "freed_bytes": freed_bytes,
        "active_downloads": active,
    });
    ([(CONTENT_TYPE, "application/json")], body.to_string()).into_response()
}

async fn dashboard() -> Response {
    let body = include_str!("../assets/dashboard.html");
    // The page renders upstream-controlled strings; the CSP is a backstop
    // for any injection the escaping misses. Inline script/style are this
    // single-file page's own.
    let csp = "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'";
    (
        [
            (CONTENT_TYPE, "text/html; charset=utf-8"),
            (HeaderName::from_static("content-security-policy"), csp),
        ],
        body,
    )
        .into_response()
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
    let raw_scopes: Vec<&str> = pairs
        .iter()
        .filter(|(key, _)| key == "scope")
        .map(|(_, value)| value.as_str())
        .collect();
    let (registry, upstream_scope) = match merged_upstream_scope(&raw_scopes) {
        Ok(value) => value,
        Err(status) => return status.into_response(),
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
    // The same path can arrive for two registries (ghcr.io prefix is
    // stripped, bare mirror mode is docker.io) — scope the cache key by
    // registry so one cannot serve the other's manifests.
    let key = (registry.route_prefix().to_owned() + &path, accept);

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
    let started = std::time::Instant::now();
    let Some(hex) = sha256_hex(&digest) else {
        let method_label = request.method().as_str().to_owned();
        let resp = passthrough_registry(state, registry, path, host, request).await;
        record_log(
            &state.logs,
            "/v2/*/blobs/*",
            &method_label,
            resp.status().as_u16(),
            started.elapsed(),
            "passthrough",
            format!("digest parse failed: {digest}"),
        );
        return resp;
    };

    // Parse Range before deciding the cache-vs-upstream split: a valid
    // single-range request against a cached blob can be served directly from
    // disk with a 206 (instead of the historical upstream passthrough that
    // bypassed the cache entirely).
    let range_header: Option<String> = request
        .headers()
        .get(RANGE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);

    if let Some(hit) = state.cache.lookup(&hex).await {
        state.stats.blob_hits.fetch_add(1, Ordering::Relaxed);
        // bytes_from_cache only counts full reads — partial reads credit
        // their actual byte length so the dashboard gauge stays honest.
        let served = match range_header
            .as_deref()
            .and_then(|v| parse_single_byte_range(v, hit.size))
        {
            Some(Ok((start, end))) => end.saturating_sub(start).saturating_add(1),
            Some(Err(())) => 0, // unsatisfiable: nothing served
            None => hit.size,
        };
        state
            .stats
            .bytes_from_cache
            .fetch_add(served, Ordering::Relaxed);
        let hit_size = hit.size;
        let is_range = range_header.is_some();
        let resp = cached_blob_response(
            state,
            registry,
            path,
            host,
            &hex,
            hit,
            head,
            &digest,
            range_header,
            request,
        )
        .await;
        record_log(
            &state.logs,
            "/v2/*/blobs/*",
            // request moved into cached_blob_response; fall-back path is
            // upstream so the method we recorded there is the same.
            "GET",
            resp.status().as_u16(),
            started.elapsed(),
            if is_range {
                "cache_hit_range"
            } else {
                "cache_hit"
            },
            format!("hex={} size={} served={}", &hex[..8], hit_size, served),
        );
        return resp;
    }

    // No cache hit: ranged requests fall through to upstream like before
    // (single-stream is acceptable; multi-range is not supported here).
    if range_header.is_some() {
        let method_label = request.method().as_str().to_owned();
        let resp = passthrough_registry(state, registry, path, host, request).await;
        record_log(
            &state.logs,
            "/v2/*/blobs/*",
            &method_label,
            resp.status().as_u16(),
            started.elapsed(),
            "upstream_range_miss",
            format!("hex={} miss+ranged", &hex[..8]),
        );
        return resp;
    }

    // Single-flight: one download per digest, late arrivals re-check the cache.
    // The guard is held inside the download task until it truly finishes
    // (including post-disconnect seeding); releasing it when the streaming
    // response returns would let late arrivals start duplicate downloads.
    let guard = state.cache.inflight_lock(&hex).await;
    let flight_guard = guard.lock_owned().await;
    if let Some(hit) = state.cache.lookup(&hex).await {
        state.stats.blob_hits.fetch_add(1, Ordering::Relaxed);
        let served = match range_header
            .as_deref()
            .and_then(|v| parse_single_byte_range(v, hit.size))
        {
            Some(Ok((start, end))) => end.saturating_sub(start).saturating_add(1),
            Some(Err(())) => 0, // unsatisfiable: nothing served
            None => hit.size,
        };
        state
            .stats
            .bytes_from_cache
            .fetch_add(served, Ordering::Relaxed);
        let resp = cached_blob_response(
            state,
            registry,
            path,
            host,
            &hex,
            hit,
            head,
            &digest,
            range_header,
            request,
        )
        .await;
        record_log(
            &state.logs,
            "/v2/*/blobs/*",
            // request moved into cached_blob_response; both code paths
            // inside it forward the original method.
            "GET",
            resp.status().as_u16(),
            started.elapsed(),
            "cache_hit_flight",
            format!("hex={} served={}", &hex[..8], served),
        );
        return resp;
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
        Err(response) => {
            record_log(
                &state.logs,
                "/v2/*/blobs/*",
                request.method().as_str(),
                response.status().as_u16(),
                started.elapsed(),
                "upstream_error",
                format!("hex={} redirect/connect", &hex[..8]),
            );
            return response;
        }
    };
    if !upstream.status().is_success() {
        let status = upstream.status().as_u16();
        let mut response = streaming_response(upstream, head);
        rewrite_401_challenge(&mut response, registry, &path, &origin);
        record_log(
            &state.logs,
            "/v2/*/blobs/*",
            request.method().as_str(),
            status,
            started.elapsed(),
            "upstream_status",
            format!("hex={} upstream={}", &hex[..8], status),
        );
        return response;
    }
    if head {
        let status = upstream.status().as_u16();
        let resp = streaming_response(upstream, true);
        record_log(
            &state.logs,
            "/v2/*/blobs/*",
            "HEAD",
            status,
            started.elapsed(),
            "upstream_head",
            format!("hex={}", &hex[..8]),
        );
        return resp;
    }

    let content_length = upstream.content_length();
    let digest_header = match HeaderValue::from_str(&digest) {
        Ok(value) => value,
        Err(_) => return StatusCode::BAD_GATEWAY.into_response(),
    };

    let total_size = match content_length {
        Some(len) => len,
        None => {
            // Chunked downloads need an explicit size. Fall back to the
            // single-source streamer so the client still gets the blob.
            let resp = single_source_blob_fallback(
                state,
                registry,
                path.clone(),
                hex.clone(),
                digest_header,
                upstream,
                flight_guard,
            )
            .await;
            record_log(
                &state.logs,
                "/v2/*/blobs/*",
                request.method().as_str(),
                resp.status().as_u16(),
                started.elapsed(),
                "fallback_single",
                format!("hex={} no Content-Length", &hex[..8]),
            );
            return resp;
        }
    };

    let part_path = state.cache.new_part_path(&hex);
    let final_path = state.cache.blob_path(&hex);
    let (tx, rx) = tokio::sync::mpsc::channel::<std::result::Result<Bytes, std::io::Error>>(
        BLOB_CHANNEL_DEPTH,
    );

    let sources = state.sources.read().await.clone();
    let cache = Arc::clone(&state.cache);
    let stats = Arc::clone(&state.stats);
    let path_for_task = path.clone();
    let registry_label = registry.route_prefix().to_owned();
    let client = state.client.clone();
    let tx_clone = tx.clone();
    let allowed_hosts = Arc::new(state.allowed_registry_hosts.clone());
    tokio::spawn(async move {
        let _flight_guard = flight_guard;
        let result = chunks::download(
            client,
            sources,
            cache,
            stats,
            registry_label,
            path_for_task,
            digest.clone(),
            total_size,
            part_path,
            final_path,
            Some(tx_clone),
            allowed_hosts,
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
    flight_guard: tokio::sync::OwnedMutexGuard<()>,
) -> Response {
    let (tx, rx) = tokio::sync::mpsc::channel::<std::result::Result<Bytes, std::io::Error>>(
        BLOB_CHANNEL_DEPTH,
    );
    let cache = Arc::clone(&state.cache);
    let stats = Arc::clone(&state.stats);
    tokio::spawn(async move {
        let _flight_guard = flight_guard;
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
    if !matches!(*request.method(), Method::GET | Method::HEAD) {
        // Read-only: block POST/PUT/DELETE so the gateway can't be used as
        // a write proxy for GitHub APIs (comment creation, gist updates,
        // webhook delivery). The fallback route is not gated by
        // MGMT_BEARER, so without this an internet-exposed HTTPS port
        // would forward writes from any caller.
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }

    let method = request.method().clone();
    let headers = request.headers().clone();
    match fetch_following_redirects(state, url, method, &headers, None, false).await {
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

async fn cached_blob_response(
    state: &AppState,
    registry: Registry,
    path: String,
    host: &str,
    hex: &str,
    hit: CachedBlob,
    head: bool,
    digest: &str,
    range: Option<String>,
    request: Request,
) -> Response {
    let parsed = range
        .as_deref()
        .and_then(|v| parse_single_byte_range(v, hit.size));
    let (status, start, len, range_header) = match parsed {
        Some(Ok((start, end))) => {
            let len = end - start + 1;
            (
                StatusCode::PARTIAL_CONTENT,
                start,
                len,
                Some(format!("bytes {start}-{end}/{}", hit.size)),
            )
        }
        Some(Err(())) => {
            // Unsatisfiable range (e.g. bytes=-0, bytes=N-M with N >= size):
            // the cached blob is immutable so a round-trip upstream cannot
            // change the answer; return 416 with the resource size.
            let builder = Response::builder().status(StatusCode::RANGE_NOT_SATISFIABLE);
            if let Ok(value) = HeaderValue::from_str(&format!("bytes */{}", hit.size)) {
                return builder
                    .header(CONTENT_RANGE, value)
                    .header(
                        &DOCKER_CONTENT_DIGEST_HEADER,
                        HeaderValue::from_str(digest)
                            .unwrap_or(HeaderValue::from_static("unknown")),
                    )
                    .body(Body::empty())
                    .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response());
            }
            return StatusCode::RANGE_NOT_SATISFIABLE.into_response();
        }
        None => (StatusCode::OK, 0, hit.size, None),
    };

    let mut builder = Response::builder()
        .status(status)
        .header(CONTENT_LENGTH, len)
        .header(ACCEPT_RANGES, HeaderValue::from_static("bytes"))
        .header(
            &DOCKER_CONTENT_DIGEST_HEADER,
            HeaderValue::from_str(digest).unwrap_or(HeaderValue::from_static("unknown")),
        );
    if let Some(value) = range_header.and_then(|r| HeaderValue::from_str(&r).ok()) {
        builder = builder.header(CONTENT_RANGE, value);
    }
    if head {
        return builder
            .body(Body::empty())
            .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response());
    }
    match tokio::fs::File::open(&hit.path).await {
        Ok(file) => builder
            .body(file_stream_range(file, start, len))
            .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            // Race: eviction removed the blob between `lookup` and `open`.
            // Re-check; if still gone, fall through to upstream so the
            // request self-heals instead of surfacing a 502.
            warn!(hex = %&hex[..8], path = ?hit.path, "cached blob vanished mid-response; falling back to upstream");
            if state.cache.lookup(hex).await.is_some() {
                // Another racing committer wrote it back; tell the client to
                // retry the range so they get a clean read.
                return (StatusCode::SERVICE_UNAVAILABLE, "cache transient, retry\n")
                    .into_response();
            }
            passthrough_registry(state, registry, path, host, request).await
        }
        Err(_) => StatusCode::BAD_GATEWAY.into_response(),
    }
}

/// Stream exactly `len` bytes from `file` starting at `offset`. Any I/O
/// error short-reads and surfaces the failure to the client.
fn file_stream_range(file: tokio::fs::File, offset: u64, len: u64) -> Body {
    let initial = (file, offset, len, vec![0u8; FILE_READ_BUFFER]);
    let stream = futures_util::stream::unfold(initial, |state| async move {
        let (mut file, cursor, remaining, mut buf) = state;
        if remaining == 0 {
            return None;
        }
        if let Err(error) = file.seek(SeekFrom::Start(cursor)).await {
            // Surface the seek failure: returning None would silently
            // truncate the 206 body and leave the client with a hole.
            return Some((Err(error), (file, cursor, 0, buf)));
        }
        let want = (remaining.min(buf.len() as u64)) as usize;
        match file.read_exact(&mut buf[..want]).await {
            Ok(_read) => {
                let chunk = Bytes::copy_from_slice(&buf[..want]);
                Some((
                    Ok(chunk),
                    (file, cursor + want as u64, remaining - want as u64, buf),
                ))
            }
            Err(error) => Some((Err(error), (file, cursor, 0, buf))),
        }
    });
    Body::from_stream(stream)
}

/// Parse a single-range HTTP `Range` header into inclusive `(start, end)`.
///
/// Supported forms: `bytes=a-b` (both ends inclusive), `bytes=a-` (open-ended,
/// end resolves to `size-1`), and `bytes=-b` (suffix range, last `b` bytes).
/// Multi-range requests, invalid units, or unsatisfiable values return `None`,
/// signalling the caller to fall back to upstream.
/// Parse a single-range HTTP `Range` header.
///
/// Returns a tri-state:
/// `None` — malformed (caller falls back to upstream or no-range behavior).
/// `Some(Err(()))` — RFC 7233 unsatisfiable range (caller returns 416).
/// `Some(Ok((start, end)))` — inclusive `start..=end` range (caller returns 206).
///
/// Supported forms: `bytes=a-b` (both ends inclusive), `bytes=a-` (open-ended,
/// end resolves to `size-1`), and `bytes=-b` (suffix range, last `b` bytes).
fn parse_single_byte_range(value: &str, size: u64) -> Option<Result<(u64, u64), ()>> {
    let rest = value.strip_prefix("bytes=")?;
    let (start_str, end_str) = rest.split_once('-')?;
    if start_str.is_empty() && end_str.is_empty() {
        return None;
    }
    if start_str.is_empty() {
        // bytes=-b: last b bytes
        let suffix: u64 = end_str.parse().ok()?;
        if size == 0 {
            return Some(Err(()));
        }
        if suffix == 0 {
            return Some(Err(()));
        }
        let n = suffix.min(size);
        Some(Ok((size - n, size - 1)))
    } else {
        let start: u64 = start_str.parse().ok()?;
        let end: u64 = if end_str.is_empty() {
            if size == 0 {
                return Some(Err(()));
            }
            size - 1
        } else {
            match end_str.parse::<u64>() {
                Ok(e) => e,
                Err(_) => return None,
            }
        };
        if start > end {
            return Some(Err(()));
        }
        if start >= size {
            return Some(Err(()));
        }
        let end = end.min(size - 1);
        Some(Ok((start, end)))
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
        &CONNECTION
            | &TRANSFER_ENCODING
            | &SET_COOKIE
            | &WWW_AUTHENTICATE
            | &LOCATION
            | &CONTENT_LENGTH
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
        .unwrap_or_else(|| format!("{}://{host}", state.default_scheme))
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

// Docker's registry-mirrors mode may send the same repository twice (once
// docker.io-prefixed, once bare). Spec-wise multiple scope params are a
// union, so rewrite each, dedup, and merge into one upstream scope.
fn merged_upstream_scope(
    raw_scopes: &[&str],
) -> std::result::Result<(Registry, String), StatusCode> {
    let mut registry = Registry::DockerHub;
    let mut merged: Vec<String> = Vec::new();
    for scope in raw_scopes {
        let (scope_registry, upstream_scope) = rewrite_scope(scope)?;
        registry = scope_registry;
        if !upstream_scope.is_empty() && !merged.contains(&upstream_scope) {
            merged.push(upstream_scope);
        }
    }
    Ok((registry, merged.join(" ")))
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

/// Gate a management endpoint behind the configured bearer token.
/// Returns `Some(401 response)` when the token is set and the request
/// doesn't carry a matching `Authorization: Bearer <token>` header;
/// returns `None` to let the handler run.
///
/// Only mutating verbs are checked so the dashboard and metrics scrapers
/// don't need the token. The check is constant-time per byte to avoid
/// timing-leak token recovery.
fn require_mgmt_auth(state: &AppState, request: &Request) -> Option<Response> {
    let expected = state.mgmt_bearer.as_deref()?;
    let method = request.method();
    if !matches!(
        method,
        &Method::POST | &Method::PUT | &Method::PATCH | &Method::DELETE
    ) {
        return None;
    }
    let supplied = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .unwrap_or("");
    // Constant-time compare on equal-length prefixes so a remote attacker
    // can't recover the token byte-by-byte from response timing.
    let matches = supplied.len() == expected.len()
        && supplied
            .as_bytes()
            .iter()
            .zip(expected.as_bytes().iter())
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0;
    if matches {
        None
    } else {
        Some(
            (
                StatusCode::UNAUTHORIZED,
                [(WWW_AUTHENTICATE, "Bearer realm=\"web-proxy\"")],
                "missing or invalid bearer token\n",
            )
                .into_response(),
        )
    }
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

    #[test]
    fn parses_pem_certificate_blocks() {
        let pem = concat!(
            "-----BEGIN CERTIFICATE-----\n",
            "YWJj\n",
            "-----END CERTIFICATE-----\n",
            "-----BEGIN CERTIFICATE-----\n",
            "ZGVm\n",
            "-----END CERTIFICATE-----\n",
        );
        let blocks = pem_der_blocks(pem, "CERTIFICATE");
        assert_eq!(blocks, vec![b"abc".to_vec(), b"def".to_vec()]);
    }

    #[test]
    fn ignores_incomplete_pem_blocks() {
        let pem = concat!(
            "-----BEGIN CERTIFICATE-----\n",
            "YWJj\n", // missing END marker
            "-----BEGIN CERTIFICATE-----\n",
            "ZGVm\n",
            "-----END CERTIFICATE-----\n",
        );
        assert_eq!(pem_der_blocks(pem, "CERTIFICATE"), vec![b"def".to_vec()]);
    }

    #[test]
    fn private_key_from_pem_detects_pkcs8() {
        let pem = "-----BEGIN PRIVATE KEY-----\nYWJjZA==\n-----END PRIVATE KEY-----\n";
        let key = private_key_from_pem(pem, "test.pem").unwrap();
        assert!(matches!(key, PrivateKeyDer::Pkcs8(_)));
    }

    #[test]
    fn private_key_from_pem_rejects_unknown() {
        let pem = "-----BEGIN SOMETHING-----\nYWJjZA==\n-----END SOMETHING-----\n";
        assert!(private_key_from_pem(pem, "test.pem").is_err());
    }

    #[test]
    fn merges_duplicate_mirror_scopes() {
        // Mirror mode: daemon sends the docker.io-prefixed scope and the bare one.
        assert_eq!(
            merged_upstream_scope(&[
                "repository:docker.io/library/alpine:pull",
                "repository:library/alpine:pull",
            ])
            .unwrap(),
            (
                Registry::DockerHub,
                "repository:library/alpine:pull".to_owned()
            )
        );
    }

    #[test]
    fn merges_distinct_scopes_into_union() {
        assert_eq!(
            merged_upstream_scope(&[
                "repository:library/alpine:pull",
                "repository:library/busybox:pull",
            ])
            .unwrap(),
            (
                Registry::DockerHub,
                "repository:library/alpine:pull repository:library/busybox:pull".to_owned()
            )
        );
    }

    #[test]
    fn merged_scopes_empty_and_invalid() {
        assert_eq!(
            merged_upstream_scope(&[]).unwrap(),
            (Registry::DockerHub, String::new())
        );
        assert!(merged_upstream_scope(&["repository:../evil:pull"]).is_err());
    }
}
