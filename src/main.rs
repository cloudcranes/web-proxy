use std::{env, net::IpAddr, sync::Arc, time::Duration};

use anyhow::{bail, Context, Result};
use axum::{
    body::Body,
    extract::{OriginalUri, Request, State},
    http::{
        header::{
            ACCEPT, AUTHORIZATION, CONNECTION, CONTENT_TYPE, IF_MODIFIED_SINCE, IF_NONE_MATCH,
            IF_RANGE, LOCATION, RANGE, SET_COOKIE, TRANSFER_ENCODING, USER_AGENT, WWW_AUTHENTICATE,
        },
        HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri,
    },
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use futures_util::StreamExt;
use http_body_util::BodyExt;
use reqwest::{redirect::Policy, Client};
use tower::limit::ConcurrencyLimitLayer;
use tower_http::{limit::RequestBodyLimitLayer, trace::TraceLayer};
use tracing::{info, warn};
use url::Url;

const ORIGIN_SECRET_HEADER: &str = "x-origin-secret";
const GIT_PROTOCOL: &str = "git-protocol";
const DOCKER_API_VERSION: &str = "docker-distribution-api-version";
const MAX_TOKEN_RESPONSE_BYTES: usize = 1024 * 1024;
const PROBE_TOKEN: &str = "edge-registry-probe";
const UPSTREAM_READ_TIMEOUT_SECS: u64 = 1800;

const GIT_PROTOCOL_HEADER: HeaderName = HeaderName::from_static(GIT_PROTOCOL);

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

#[derive(Clone)]
struct AppState {
    client: Client,
    public_base_url: String,
    origin_secret: HeaderValue,
    max_redirects: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Registry {
    DockerHub,
    Ghcr,
}

impl Registry {
    fn upstream_base(self) -> &'static str {
        match self {
            Self::DockerHub => "https://registry-1.docker.io",
            Self::Ghcr => "https://ghcr.io",
        }
    }

    fn token_url(self) -> &'static str {
        match self {
            Self::DockerHub => "https://auth.docker.io/token",
            Self::Ghcr => "https://ghcr.io/token",
        }
    }

    fn token_service(self) -> &'static str {
        match self {
            Self::DockerHub => "registry.docker.io",
            Self::Ghcr => "ghcr.io",
        }
    }

    fn route_prefix(self) -> &'static str {
        match self {
            Self::DockerHub => "docker.io/",
            Self::Ghcr => "ghcr.io/",
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "edge_accelerator=info,tower_http=info".into()),
        )
        .init();

    let listen_addr = env_or("LISTEN_ADDR", "0.0.0.0:20516");
    let public_base_url = normalized_public_base_url(&required_env("PUBLIC_BASE_URL")?)?;
    let origin_secret = required_env("ORIGIN_SECRET")?;
    if origin_secret.as_bytes().len() < 32 {
        bail!("ORIGIN_SECRET must be at least 32 bytes");
    }

    let connect_timeout = env_parse("UPSTREAM_CONNECT_TIMEOUT_SECS", 10_u64)?;
    let max_redirects = env_parse("MAX_REDIRECTS", 8_usize)?;
    let max_concurrent_requests = env_parse("MAX_CONCURRENT_REQUESTS", 128_usize)?;
    if max_redirects == 0 || max_concurrent_requests == 0 {
        bail!("MAX_REDIRECTS and MAX_CONCURRENT_REQUESTS must be greater than zero");
    }

    let client = Client::builder()
        .connect_timeout(Duration::from_secs(connect_timeout))
        .read_timeout(Duration::from_secs(UPSTREAM_READ_TIMEOUT_SECS))
        .timeout(None)
        .redirect(Policy::none())
        .build()
        .context("build HTTP client")?;

    let state = AppState {
        client,
        public_base_url,
        origin_secret: HeaderValue::from_str(&origin_secret).context("invalid ORIGIN_SECRET")?,
        max_redirects,
    };

    let app = Router::new()
        .route("/healthz", any(healthz))
        .fallback(proxy)
        .layer(RequestBodyLimitLayer::new(64 * 1024 * 1024))
        .layer(ConcurrencyLimitLayer::new(max_concurrent_requests))
        .layer(TraceLayer::new_for_http())
        .with_state(Arc::new(state));

    let listener = tokio::net::TcpListener::bind(&listen_addr)
        .await
        .with_context(|| format!("bind {listen_addr}"))?;
    info!(%listen_addr, "listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("serve HTTP")
}

async fn healthz(State(state): State<Arc<AppState>>, request: Request) -> Response {
    if !has_origin_secret(request.headers(), &state.origin_secret) {
        return StatusCode::FORBIDDEN.into_response();
    }
    if request.method() != Method::GET && request.method() != Method::HEAD {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    if request.method() == Method::HEAD {
        Response::new(Body::empty())
    } else {
        "ok\n".into_response()
    }
}

async fn proxy(
    State(state): State<Arc<AppState>>,
    OriginalUri(uri): OriginalUri,
    request: Request,
) -> Response {
    if !has_origin_secret(request.headers(), &state.origin_secret) {
        return StatusCode::FORBIDDEN.into_response();
    }

    match route_request(&uri) {
        Ok(Route::RegistryRoot) => registry_root(&state, request.method(), request.headers()),
        Ok(Route::Token) => proxy_token(&state, request.method(), &uri, request.headers()).await,
        Ok(Route::Registry(registry, path)) => {
            proxy_registry(&state, registry, path, request).await
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
    if let Some(rest) = path.strip_prefix("/v2/docker.io/") {
        return Ok(Route::Registry(Registry::DockerHub, format!("/v2/{rest}")));
    }
    if let Some(rest) = path.strip_prefix("/v2/ghcr.io/") {
        return Ok(Route::Registry(Registry::Ghcr, format!("/v2/{rest}")));
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

fn registry_root(state: &AppState, method: &Method, headers: &HeaderMap) -> Response {
    if method != Method::GET && method != Method::HEAD {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    if headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        == Some(&format!("Bearer {PROBE_TOKEN}"))
    {
        let mut response = StatusCode::OK.into_response();
        response.headers_mut().insert(
            HeaderName::from_static(DOCKER_API_VERSION),
            HeaderValue::from_static("registry/2.0"),
        );
        return response;
    }
    let mut response = StatusCode::UNAUTHORIZED.into_response();
    response.headers_mut().insert(
        HeaderName::from_static(DOCKER_API_VERSION),
        HeaderValue::from_static("registry/2.0"),
    );
    if let Ok(value) = registry_challenge(&state.public_base_url) {
        response.headers_mut().insert(WWW_AUTHENTICATE, value);
    }
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
        match url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect()
        {
            pairs => pairs,
        };
    let scopes: Vec<&str> = pairs
        .iter()
        .filter(|(key, _)| key == "scope")
        .map(|(_, value)| value.as_str())
        .collect();
    if scopes.is_empty() {
        return probe_token(method);
    }
    if scopes.len() != 1 {
        return (StatusCode::BAD_REQUEST, "exactly one scope is required\n").into_response();
    }
    let (registry, upstream_scope) = match rewrite_scope(scopes[0]) {
        Ok(value) => value,
        Err(status) => return status.into_response(),
    };

    let mut url = match Url::parse(registry.token_url()) {
        Ok(url) => url,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("service", registry.token_service());
        query.append_pair("scope", &upstream_scope);
        for (key, value) in &pairs {
            if key == "scope" || key == "service" || key == "account" || key == "offline_token" {
                continue;
            }
            query.append_pair(key, value);
        }
    }

    let mut builder = state.client.request(method.clone(), url);
    // Public-only mode: never forward client registry credentials to token issuers.
    copy_request_headers(headers, &mut builder, false);
    let upstream = match builder.send().await {
        Ok(response) => response,
        Err(error) => return upstream_error(error),
    };
    buffered_response(upstream, *method == Method::HEAD).await
}

async fn proxy_registry(
    state: &AppState,
    registry: Registry,
    path: String,
    request: Request,
) -> Response {
    if request.method() != Method::GET && request.method() != Method::HEAD {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }

    let mut url = match Url::parse(registry.upstream_base())
        .and_then(|base| base.join(path.trim_start_matches('/')))
    {
        Ok(url) => url,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    url.set_query(request.uri().query());

    match send_streaming(
        state,
        url,
        request.method().clone(),
        request.headers(),
        None,
        true,
    )
    .await
    {
        Ok(mut response) => {
            if response.status() == StatusCode::UNAUTHORIZED {
                let scope = registry_repository(&path).map(|repository| {
                    format!("repository:{}{}:pull", registry.route_prefix(), repository)
                });
                match registry_challenge(&state.public_base_url, scope.as_deref()) {
                    Ok(value) => {
                        response.headers_mut().insert(WWW_AUTHENTICATE, value);
                    }
                    Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
                }
            }
            response
        }
        Err(response) => response,
    }
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

    match send_streaming(state, url, method, &headers, body, false).await {
        Ok(response) | Err(response) => response,
    }
}

async fn send_streaming(
    state: &AppState,
    mut url: Url,
    mut method: Method,
    headers: &HeaderMap,
    mut body: Option<axum::body::Bytes>,
    registry: bool,
) -> std::result::Result<Response, Response> {
    let mut current_headers = headers.clone();
    let mut redirects = 0_usize;

    loop {
        if registry {
            if validate_registry_url(&url).is_err() {
                return Err(StatusCode::BAD_GATEWAY.into_response());
            }
        } else if validate_upstream_url(&url).is_err() {
            return Err(StatusCode::BAD_GATEWAY.into_response());
        }

        let mut builder = state.client.request(method.clone(), url.clone());
        copy_request_headers(&current_headers, &mut builder, registry);
        if let Some(bytes) = body.clone() {
            builder = builder.body(bytes);
        }

        let upstream = builder.send().await.map_err(upstream_error)?;
        if !upstream.status().is_redirection() {
            return Ok(streaming_response(upstream, method == Method::HEAD));
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
            validate_registry_url(&next)
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

async fn buffered_response(upstream: reqwest::Response, head: bool) -> Response {
    let status = upstream.status();
    let upstream_headers = upstream.headers().clone();
    let body = if head {
        Body::empty()
    } else {
        let content_length = upstream.content_length();
        if content_length.is_some_and(|size| size > MAX_TOKEN_RESPONSE_BYTES as u64) {
            return StatusCode::BAD_GATEWAY.into_response();
        }
        let bytes = match upstream.bytes().await {
            Ok(bytes) if bytes.len() <= MAX_TOKEN_RESPONSE_BYTES => bytes,
            Ok(_) | Err(_) => return StatusCode::BAD_GATEWAY.into_response(),
        };
        Body::from(bytes)
    };
    let mut response = Response::new(body);
    *response.status_mut() = status;
    copy_response_headers(&upstream_headers, response.headers_mut());
    response
}

fn copy_request_headers(
    headers: &HeaderMap,
    builder: &mut reqwest::RequestBuilder,
    registry: bool,
) {
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
    *builder = builder.headers(forwarded);
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

fn probe_token(method: &Method) -> Response {
    let mut response = if method == Method::HEAD {
        Response::new(Body::empty())
    } else {
        Response::new(Body::from(format!(
            "{{\"token\":\"{PROBE_TOKEN}\",\"access_token\":\"{PROBE_TOKEN}\",\"expires_in\":60}}"
        )))
    };
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
}

fn registry_challenge(public_base_url: &str, scope: Option<&str>) -> Result<HeaderValue> {
    let scope = scope
        .map(|value| format!(",scope=\"{value}\""))
        .unwrap_or_default();
    HeaderValue::from_str(&format!(
        "Bearer realm=\"{public_base_url}/token\",service=\"edge-registry\"{scope}"
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

    for registry in [Registry::DockerHub, Registry::Ghcr] {
        if let Some(upstream_repository) = repository.strip_prefix(registry.route_prefix()) {
            if upstream_repository.is_empty() {
                return Err(StatusCode::BAD_REQUEST);
            }
            return Ok((
                registry,
                format!("repository:{upstream_repository}:{actions}"),
            ));
        }
    }
    Err(StatusCode::BAD_REQUEST)
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

fn validate_upstream_url(url: &Url) -> Result<()> {
    validate_https_url(url)?;
    let host = url.host_str().context("missing upstream host")?;
    if !is_allowed_github_host(host) {
        bail!("upstream host is not allowed");
    }
    Ok(())
}

fn validate_registry_url(url: &Url) -> Result<()> {
    validate_https_url(url)?;
    let host = url.host_str().context("missing upstream host")?;
    if !matches!(
        host.to_ascii_lowercase().as_str(),
        "registry-1.docker.io"
            | "ghcr.io"
            | "production.cloudflare.docker.com"
            | "pkg-containers.githubusercontent.com"
    ) {
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

fn has_origin_secret(headers: &HeaderMap, expected: &HeaderValue) -> bool {
    headers
        .get(ORIGIN_SECRET_HEADER)
        .is_some_and(|value| constant_time_eq(value.as_bytes(), expected.as_bytes()))
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut diff = left.len() ^ right.len();
    for index in 0..left.len().max(right.len()) {
        diff |= usize::from(*left.get(index).unwrap_or(&0) ^ *right.get(index).unwrap_or(&0));
    }
    diff == 0
}

fn normalized_public_base_url(value: &str) -> Result<String> {
    let url = Url::parse(value).context("invalid PUBLIC_BASE_URL")?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        bail!("PUBLIC_BASE_URL must be an HTTPS origin without a path, query, or credentials");
    }
    Ok(value.trim_end_matches('/').to_owned())
}

fn upstream_error(error: reqwest::Error) -> Response {
    warn!(%error, "upstream request failed");
    (StatusCode::BAD_GATEWAY, "upstream request failed\n").into_response()
}

fn required_env(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("{name} is required"))
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
    fn rewrites_multilevel_scopes() {
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
    }

    #[test]
    fn rejects_unknown_or_writable_scopes() {
        for scope in [
            "repository:quay.io/owner/image:pull",
            "repository:docker.io/library/alpine:pull,push",
            "repository:docker.io/../admin:pull",
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
    fn compares_secrets_without_length_shortcut() {
        assert!(constant_time_eq(b"same", b"same"));
        assert!(!constant_time_eq(b"same", b"different"));
    }
}
