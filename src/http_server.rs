use crate::{
    config::AppConfig,
    flow::{FlowAnalyzer, FlowDirection},
    http_route::{route_request, RouteDecision},
    resolver::DomainResolver,
};
use bytes::Bytes;
use http::{header, HeaderMap, HeaderName, HeaderValue, Method, Response, StatusCode};
use http_body_util::{BodyExt, Full, combinators::UnsyncBoxBody};
use hyper::{
    body::Incoming,
    client::conn::{http1 as client_http1, http2 as client_http2},
    server::conn::{http1, http2 as server_http2},
    service::service_fn,
    Request,
};
use hyper_util::rt::{TokioExecutor, TokioIo};
use anyhow::Context;
use std::{convert::Infallible, error::Error, future::Future, io, pin::Pin, sync::Arc, task::{Context as TaskContext, Poll}, time::Duration};
use tokio::{io::{AsyncRead, AsyncWrite, ReadBuf}, sync::oneshot, time::Sleep};

type BoxError = Box<dyn Error + Send + Sync>;
trait AsyncIo: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> AsyncIo for T {}
type ResponseBody = UnsyncBoxBody<Bytes, BoxError>;


/// Count bytes crossing the accepted client connection, including HTTP headers and frames.
pub(crate) struct CountingIo<S> {
    inner: S,
    flow: FlowAnalyzer,
}

impl<S> CountingIo<S> {
    pub(crate) fn new(inner: S, flow: FlowAnalyzer) -> Self {
        Self { inner, flow }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for CountingIo<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buffer.filled().len();
        match Pin::new(&mut this.inner).poll_read(cx, buffer) {
            Poll::Ready(Ok(())) => {
                let read = buffer.filled().len().saturating_sub(before);
                this.flow.record(FlowDirection::Read, read);
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for CountingIo<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_write(cx, bytes) {
            Poll::Ready(Ok(written)) => {
                this.flow.record(FlowDirection::Write, written);
                Poll::Ready(Ok(written))
            }
            other => other,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// Enforce the configured timeout while the upstream response body is streamed.
/// The forwarding future has its own timeout for DNS, connect, TLS and response headers.
struct TimeoutBody<B> {
    inner: Pin<Box<B>>,
    sleep: Option<Pin<Box<Sleep>>>,
    connection: Option<UpstreamConnectionGuard>,
    terminated: bool,
}

/// Owns the upstream driver task for as long as its response body is usable.
/// Dropping the body (including when the downstream client disconnects) aborts it.
struct UpstreamConnectionGuard(tokio::task::AbortHandle);

impl Drop for UpstreamConnectionGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<B> TimeoutBody<B> {
    fn new(inner: B, duration: Option<Duration>, connection: UpstreamConnectionGuard) -> Self {
        Self {
            inner: Box::pin(inner),
            sleep: duration.map(|duration| Box::pin(tokio::time::sleep(duration))),
            connection: Some(connection),
            terminated: false,
        }
    }
}

impl<B> http_body::Body for TimeoutBody<B>
where
    B: http_body::Body<Data = Bytes, Error = BoxError>,
{
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        if this.terminated {
            return Poll::Ready(None);
        }
        if this.sleep.as_mut().is_some_and(|sleep| sleep.as_mut().poll(cx).is_ready()) {
            this.terminated = true;
            // Stop the Hyper connection driver immediately; otherwise it may
            // remain parked on a body that nobody will consume after timeout.
            drop(this.connection.take());
            return Poll::Ready(Some(Err(Box::new(io::Error::new(
                io::ErrorKind::TimedOut,
                "configured HTTP request timeout elapsed while streaming the response body",
            )))));
        }
        match this.inner.as_mut().poll_frame(cx) {
            Poll::Ready(None) => {
                this.terminated = true;
                // The response is fully consumed. Do not leave an idle upstream
                // keep-alive driver running after the last response frame.
                drop(this.connection.take());
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) => {
                this.terminated = true;
                drop(this.connection.take());
                Poll::Ready(Some(Err(error)))
            }
            other => other,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.terminated || self.inner.as_ref().get_ref().is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        if self.terminated { http_body::SizeHint::with_exact(0) } else { self.inner.as_ref().get_ref().size_hint() }
    }
}

/// Hold an incoming request body until the upstream explicitly sends 100 Continue.
struct ExpectContinueBody<B> {
    inner: Pin<Box<B>>,
    continue_rx: Option<oneshot::Receiver<()>>,
    released: bool,
}

impl<B> ExpectContinueBody<B> {
    fn new(inner: B, continue_rx: Option<oneshot::Receiver<()>>) -> Self {
        Self {
            inner: Box::pin(inner),
            released: continue_rx.is_none(),
            continue_rx,
        }
    }
}

impl<B> http_body::Body for ExpectContinueBody<B>
where
    B: http_body::Body<Data = Bytes>,
{
    type Data = Bytes;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        if !this.released {
            let Some(continue_rx) = this.continue_rx.as_mut() else {
                this.released = true;
                return this.inner.as_mut().poll_frame(cx);
            };
            match Pin::new(continue_rx).poll(cx) {
                Poll::Ready(Ok(())) => {
                    this.released = true;
                    this.continue_rx = None;
                }
                // If upstream returns a final response without 100, keep the
                // upload gated. The response future is driven independently.
                Poll::Ready(Err(_)) | Poll::Pending => return Poll::Pending,
            }
        }
        this.inner.as_mut().poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.released && self.inner.as_ref().get_ref().is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.as_ref().get_ref().size_hint()
    }
}

fn expects_continue(headers: &HeaderMap) -> bool {
    headers.get_all(header::EXPECT).iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|value| value.trim().eq_ignore_ascii_case("100-continue"))
}

fn should_gate_expect_continue<B>(headers: &HeaderMap, body: &B) -> bool
where
    B: http_body::Body,
{
    expects_continue(headers) && !body.is_end_stream()
}

/// Hyper currently does not expose HTTP/2 server-side 100-Continue emission.
/// Reject this expectation explicitly rather than leaving an HTTP/2 client
/// waiting forever while the proxy waits for its request body.
fn http2_expect_continue_unsupported(version: http::Version, headers: &HeaderMap) -> bool {
    version == http::Version::HTTP_2 && expects_continue(headers)
}

/// Serve one HTTP/1 connection over a plain TCP or decrypted TLS stream.
/// Already-consumed bytes can be restored by wrapping the stream in PrefixedIo.
pub async fn serve<S>(io: S, scheme: &'static str, config: Arc<AppConfig>, resolver: DomainResolver, domains: Vec<String>, tls_proxy: Arc<crate::http_proxy::TlsProxy>) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let io = CountingIo::new(io, resolver.flow_analyzer());
    let service = service_fn(move |request: Request<Incoming>| {
        let config = Arc::clone(&config);
        let resolver = resolver.clone();
        let domains = domains.clone();
        let tls_proxy = Arc::clone(&tls_proxy);
        async move { Ok::<_, Infallible>(handle_request(request, scheme, &config, &resolver, &domains, &tls_proxy).await) }
    });
    http1::Builder::new().keep_alive(true)
        .serve_connection(TokioIo::new(io), service)
        .with_upgrades()
        .await?;
    Ok(())
}

/// Serve decrypted TLS traffic negotiated as HTTP/2. The same request router
/// and response/body pipeline are shared with the HTTP/1 entry point.
pub async fn serve_http2<S>(io: S, scheme: &'static str, config: Arc<AppConfig>, resolver: DomainResolver, domains: Vec<String>, tls_proxy: Arc<crate::http_proxy::TlsProxy>) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let io = CountingIo::new(io, resolver.flow_analyzer());
    let service = service_fn(move |request: Request<Incoming>| {
        let config = Arc::clone(&config);
        let resolver = resolver.clone();
        let domains = domains.clone();
        let tls_proxy = Arc::clone(&tls_proxy);
        async move { Ok::<_, Infallible>(handle_request(request, scheme, &config, &resolver, &domains, &tls_proxy).await) }
    });
    server_http2::Builder::new(TokioExecutor::new())
        .serve_connection(TokioIo::new(io), service)
        .await?;
    Ok(())
}

async fn handle_request(request: Request<Incoming>, scheme: &str, config: &AppConfig, resolver: &DomainResolver, domains: &[String], tls_proxy: &crate::http_proxy::TlsProxy) -> Response<ResponseBody> {
    let method = request.method().clone();
    // CONNECT has different HTTP/2 pseudo-header and tunnel semantics. This
    // proxy currently implements raw CONNECT only on the HTTP/1 entry path;
    // do not reinterpret an HTTP/2 authority-form request as an ordinary URL.
    if http2_connect_unsupported(request.version(), &method) {
        return empty_response(StatusCode::NOT_IMPLEMENTED);
    }
    let target = if request.version() == http::Version::HTTP_2 {
        match request.uri().authority() {
            Some(authority) => {
                let path = request.uri().path_and_query().map(|value| value.as_str()).unwrap_or("/");
                if path == "*" { "*".to_owned() } else { format!("{scheme}://{authority}{path}") }
            }
            None => request.uri().to_string(),
        }
    } else {
        request.uri().to_string()
    };
    // Keep management endpoints scoped to the proxy's own loopback authority.
    // A remote site's /ping or /refresh-ip path must continue through normal routing.
    if scheme == "http" {
        if let Ok(management_uri) = url::Url::parse(&target) {
            let local_host = matches!(
                management_uri.host_str(),
                Some("127.0.0.1" | "localhost" | "::1")
            );
            let local_port = management_uri
                .port()
                .unwrap_or(if management_uri.scheme() == "https" { 443 } else { 80 });
            if local_host
                && local_port == config.fast_github.http_proxy_port
                && matches!(management_uri.path(), "/ping" | "/refresh-ip" | "/flowStatistics")
            {
                if management_uri.path() == "/ping" && method == Method::GET {
                    return management_response(StatusCode::OK, "text/plain; charset=utf-8", "pong".to_owned());
                }
                if management_uri.path() == "/flowStatistics" && method == Method::GET {
                    let body = serde_json::to_string(&resolver.flow_analyzer().statistics())
                        .unwrap_or_else(|_| "{}".to_owned());
                    return management_response(
                        StatusCode::OK,
                        "application/json; charset=utf-8",
                        body,
                    );
                }
                if management_uri.path() == "/refresh-ip"
                    && (method == Method::GET || method == Method::POST)
                {
                    match resolver.refresh_all().await {
                        Ok(hosts) => {
                            let body = serde_json::json!({
                                "success": hosts.success,
                                "domainCount": hosts.domain_count,
                                "errors": hosts.errors,
                            }).to_string();
                            return management_response(
                                StatusCode::OK,
                                "application/json; charset=utf-8",
                                body,
                            );
                        }
                        Err(error) => {
                            let body = serde_json::json!({
                                "success": false,
                                "error": error.to_string(),
                            }).to_string();
                            return management_response(
                                StatusCode::BAD_GATEWAY,
                                "application/json; charset=utf-8",
                                body,
                            );
                        }
                    }
                }
                return empty_response(StatusCode::METHOD_NOT_ALLOWED);
            }
        }
    }

    let host_header = request.headers().get(header::HOST);
    if request.version() == http::Version::HTTP_2 {
        let authority = request.uri().authority();
        let host_values = request.headers().get_all(header::HOST);
        if authority.is_none()
            || !http2_scheme_matches(request.uri().scheme_str(), scheme)
            || host_values.iter().count() > 1
            || host_header.is_some_and(|value| {
                value.to_str().ok().map_or(true, |host| {
                    !http_authorities_match(authority.expect("checked HTTP/2 authority"), host)
                })
            })
        {
            return empty_response(StatusCode::BAD_REQUEST);
        }
    }
    let host = host_header.and_then(|value| value.to_str().ok())
        .or_else(|| request.uri().authority().map(|authority| authority.as_str()));
    if request.version() == http::Version::HTTP_11 && host.is_none() {
        return empty_response(StatusCode::BAD_REQUEST);
    }
    match route_request(method.as_str(), &target, host, scheme, config) {
        Ok(RouteDecision::Respond(rule)) => configured_response(&method, rule),
        Ok(RouteDecision::Forward { uri, request_target, domain, original_host }) => {
            if http2_expect_continue_unsupported(request.version(), request.headers()) {
                return empty_response(StatusCode::EXPECTATION_FAILED);
            }
            let timeout_duration = match domain.as_ref().map(|rule| rule.request_timeout()).transpose() {
                Ok(value) => value.flatten(),
                Err(error) => {
                    tracing::warn!(%error, "invalid configured request timeout");
                    return empty_response(StatusCode::BAD_GATEWAY);
                }
            };
            // Keep one clock for the complete request. If the response body starts
            // after a slow DNS/connect/TLS/header phase, only the remaining budget
            // is available for streaming it.
            let request_started = tokio::time::Instant::now();
            // Each async fn has a distinct opaque Future type. Await both
            // branches inside one async block so the timeout wrapper receives
            // a single concrete future type.
            let forwarding = async {
                if is_upgrade_request(&request) {
                    forward_upgrade_request(request, uri, request_target, original_host, domain, timeout_duration, request_started, resolver, domains, config, tls_proxy).await
                } else {
                    forward_request(request, uri, request_target, original_host, domain, timeout_duration, request_started, resolver, domains, config, tls_proxy).await
                }
            };
            match timeout_duration {
                Some(duration) => match tokio::time::timeout(duration, forwarding).await {
                    Ok(Ok(response)) => response,
                    Ok(Err(error)) => {
                        tracing::debug!(%error, "upstream HTTP forwarding failed");
                        empty_response(StatusCode::BAD_GATEWAY)
                    }
                    Err(_) => empty_response(StatusCode::GATEWAY_TIMEOUT),
                },
                None => match forwarding.await {
                    Ok(response) => response,
                    Err(error) => {
                        tracing::debug!(%error, "upstream HTTP forwarding failed");
                        empty_response(StatusCode::BAD_GATEWAY)
                    }
                }
            }
        },
        Err(error) => {
            tracing::debug!(%error, method = %method, %target, "rejecting malformed HTTP request");
            empty_response(StatusCode::BAD_REQUEST)
        }
    }
}

async fn forward_request(
    request: Request<Incoming>,
    uri: url::Url,
    request_target: String,
    original_host: String,
    domain: Option<crate::config::DomainConfig>,
    body_timeout: Option<Duration>,
    request_started: tokio::time::Instant,
    resolver: &DomainResolver,
    domains: &[String],
    config: &AppConfig,
    tls_proxy: &crate::http_proxy::TlsProxy,
) -> anyhow::Result<Response<ResponseBody>> {
    if has_fastgithub_user_agent(request.headers()) {
        anyhow::bail!("request already contains FastGithub/1.0 User-Agent marker; refusing proxy loop");
    }
    let downstream_version = request.version();
    let host = uri.host_str().context("upstream URI has no host")?;
    let port = uri.port().unwrap_or(if uri.scheme() == "https" { 443 } else { 80 });
    let (stream, upstream_ip) = crate::http_proxy::connect_target(host, port, resolver, domains, config, domain.as_ref()).await?;
    let (io, upstream_http2): (Box<dyn AsyncIo>, bool) = if uri.scheme() == "https" {
        let tls = match tls_proxy.connect_upstream(stream, host, upstream_ip, domain.as_ref(), true).await {
            Ok(tls) => tls,
            Err(error) => {
                resolver.report_failure(upstream_ip).await;
                return Err(error);
            }
        };
        let use_http2 = crate::http_proxy::negotiated_http2(tls.ssl().selected_alpn_protocol());
        (Box::new(tls), use_http2)
    } else if uri.scheme() == "http" {
        (Box::new(stream), false)
    } else {
        anyhow::bail!("unsupported upstream URI scheme '{}'", uri.scheme());
    };
    if upstream_http2 {
        return forward_request_http2(
            request, uri, request_target, original_host, downstream_version, body_timeout, request_started, io,
        ).await;
    }
    let (mut sender, connection) = client_http1::handshake(TokioIo::new(io)).await?;
    let connection_task = tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::debug!(%error, "upstream HTTP connection ended");
        }
    });
    // Keep the driver alive through response-body streaming, but make every
    // early-return/cancellation path abort it through this guard's Drop.
    let connection_guard = UpstreamConnectionGuard(connection_task.abort_handle());

    let (mut parts, body) = request.into_parts();
    parts.uri = build_upstream_request_uri(&uri, &request_target)
        .context("building upstream request URI")?;
    strip_hop_by_hop_headers(&mut parts.headers);
    append_fastgithub_user_agent(&mut parts.headers)?;
    parts.headers.insert(header::HOST, HeaderValue::from_str(&host_header(&uri))?);
    tracing::debug!(%original_host, upstream = %uri, "forwarding HTTP request");

    let expect_continue = should_gate_expect_continue(&parts.headers, &body);
    let (continue_tx, continue_rx) = oneshot::channel();
    let gated_body = ExpectContinueBody::new(body, expect_continue.then_some(continue_rx));
    let mut upstream_request = Request::from_parts(parts, gated_body);
    if expect_continue {
        // Hyper's client polls the body after writing request headers. Gate it
        // until the upstream's actual 100 arrives. Polling Incoming only after
        // this signal also prevents a premature downstream 100 Continue.
        let continue_sender = std::sync::Mutex::new(Some(continue_tx));
        hyper::ext::on_informational(&mut upstream_request, move |response| {
            if response.status() == StatusCode::CONTINUE {
                if let Some(sender) = continue_sender.lock().expect("continue sender mutex").take() {
                    let _ = sender.send(());
                }
            }
        });
    }
    let upstream_response = sender.send_request(upstream_request).await?;
    let (mut response_parts, response_body) = upstream_response.into_parts();
    response_parts.version = downstream_version;
    strip_hop_by_hop_headers(&mut response_parts.headers);
    response_parts.headers.insert(header::SERVER, HeaderValue::from_static("FastGithub"));
    let boxed_body = response_body.map_err(|error| Box::new(error) as BoxError).boxed_unsync();
    let remaining = body_timeout.map(|duration| duration.saturating_sub(request_started.elapsed()));
    let response_body = TimeoutBody::new(boxed_body, remaining, connection_guard).boxed_unsync();
    Ok(Response::from_parts(response_parts, response_body))
}

async fn forward_request_http2(
    request: Request<Incoming>,
    uri: url::Url,
    request_target: String,
    original_host: String,
    downstream_version: http::Version,
    body_timeout: Option<Duration>,
    request_started: tokio::time::Instant,
    io: Box<dyn AsyncIo>,
) -> anyhow::Result<Response<ResponseBody>> {
    let (mut sender, connection) =
        client_http2::handshake(TokioExecutor::new(), TokioIo::new(io)).await?;
    let connection_task = tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::debug!(%error, "upstream HTTP/2 connection ended");
        }
    });
    let connection_guard = UpstreamConnectionGuard(connection_task.abort_handle());

    let (mut parts, body) = request.into_parts();
    let host = uri.host_str().context("upstream HTTP/2 URI has no host")?;
    parts.uri = build_upstream_request_uri(&uri, &request_target)
        .context("building upstream HTTP/2 request URI")?;
    parts.version = http::Version::HTTP_2;
    strip_hop_by_hop_headers(&mut parts.headers);
    parts.headers.remove(header::HOST);
    parts.headers.remove(header::TRANSFER_ENCODING);
    append_fastgithub_user_agent(&mut parts.headers)?;
    tracing::debug!(%original_host, upstream = %uri, "forwarding HTTP/2 request");

    // Hyper's informational callback is currently HTTP/1-only. Keep HTTP/2
    // request bodies flowing rather than gating them on a 100 that we cannot
    // observe through this API.
    let upstream_request = Request::from_parts(parts, body);
    let upstream_response = sender.send_request(upstream_request).await?;
    let (mut response_parts, response_body) = upstream_response.into_parts();
    response_parts.version = downstream_version;
    strip_hop_by_hop_headers(&mut response_parts.headers);
    response_parts.headers.insert(header::SERVER, HeaderValue::from_static("FastGithub"));
    let boxed_body = response_body.map_err(|error| Box::new(error) as BoxError).boxed_unsync();
    let remaining = body_timeout.map(|duration| duration.saturating_sub(request_started.elapsed()));
    let response_body = TimeoutBody::new(boxed_body, remaining, connection_guard).boxed_unsync();
    Ok(Response::from_parts(response_parts, response_body))
}

/// HTTP/2 non-CONNECT requests must use the scheme of the accepted
/// transport. The proxy currently exposes HTTP/2 only after TLS interception.
fn http2_scheme_matches(request_scheme: Option<&str>, transport_scheme: &str) -> bool {
    request_scheme.is_some_and(|value| value.eq_ignore_ascii_case(transport_scheme))
}

/// Compare an HTTP/2 Host field with :authority. Host names are
/// case-insensitive; an explicit port must match the authority exactly.
fn http_authorities_match(authority: &http::uri::Authority, host: &str) -> bool {
    let Ok(host_authority) = host.parse::<http::uri::Authority>() else {
        return false;
    };
    // Userinfo is not valid in an HTTP Host / :authority value. Also reject
    // ports that cannot be represented as u16 instead of treating them as
    // equivalent to an omitted port.
    if authority.as_str().contains('@') || host_authority.as_str().contains('@')
        || (authority.port().is_some() && authority.port_u16().is_none())
        || (host_authority.port().is_some() && host_authority.port_u16().is_none())
    {
        return false;
    }
    authority.host().eq_ignore_ascii_case(host_authority.host())
        && authority.port_u16() == host_authority.port_u16()
}

fn http2_connect_unsupported(version: http::Version, method: &Method) -> bool {
    version == http::Version::HTTP_2 && method == Method::CONNECT
}

/// Detect an HTTP/1.1 protocol upgrade before the ordinary forwarding path
/// strips hop-by-hop headers.
fn is_upgrade_request<B>(request: &Request<B>) -> bool {
    request.version() == http::Version::HTTP_11
        && request.method() == Method::GET
        && has_connection_token(request.headers(), "upgrade")
        && !upgrade_protocols(request.headers()).is_empty()
}

fn has_connection_token(headers: &HeaderMap, token: &str) -> bool {
    headers.get_all(header::CONNECTION).iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|value| value.trim().eq_ignore_ascii_case(token))
}

fn upgrade_protocols(headers: &HeaderMap) -> Vec<String> {
    headers.get_all(header::UPGRADE).iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect()
}

/// A 101 response must select exactly one protocol, and it must be one
/// offered by the downstream client. Accepting a list here would allow a
/// malformed response to switch protocols ambiguously.
fn is_valid_upgrade_response(headers: &HeaderMap, requested_protocols: &[String]) -> bool {
    if !has_connection_token(headers, "upgrade") {
        return false;
    }
    let accepted_protocols = upgrade_protocols(headers);
    accepted_protocols.len() == 1
        && requested_protocols.iter().any(|requested| {
            requested.eq_ignore_ascii_case(&accepted_protocols[0])
        })
}

/// Keep only the HTTP/1 fields required to negotiate an upgrade. Connection
/// remains hop-by-hop, but its value is normalized to the Upgrade token.
fn preserve_upgrade_hop_by_hop_headers(headers: &mut HeaderMap) {
    let nominated = headers.get_all(header::CONNECTION).iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|name| !name.eq_ignore_ascii_case("upgrade"))
        .filter_map(|name| HeaderName::from_bytes(name.as_bytes()).ok())
        .collect::<Vec<_>>();
    for name in nominated {
        headers.remove(name);
    }
    for name in [
        HeaderName::from_static("proxy-connection"),
        HeaderName::from_static("keep-alive"),
        HeaderName::from_static("proxy-authenticate"),
        HeaderName::from_static("proxy-authorization"),
        HeaderName::from_static("te"),
    ] {
        headers.remove(name);
    }
    headers.insert(header::CONNECTION, HeaderValue::from_static("Upgrade"));
}

async fn forward_upgrade_request(
    mut request: Request<Incoming>,
    uri: url::Url,
    request_target: String,
    original_host: String,
    domain: Option<crate::config::DomainConfig>,
    body_timeout: Option<Duration>,
    request_started: tokio::time::Instant,
    resolver: &DomainResolver,
    domains: &[String],
    config: &AppConfig,
    tls_proxy: &crate::http_proxy::TlsProxy,
) -> anyhow::Result<Response<ResponseBody>> {
    if has_fastgithub_user_agent(request.headers()) {
        anyhow::bail!("request already contains FastGithub/1.0 User-Agent marker; refusing proxy loop");
    }
    let requested_protocols = upgrade_protocols(request.headers());
    if requested_protocols.is_empty() || !has_connection_token(request.headers(), "upgrade") {
        anyhow::bail!("malformed HTTP upgrade request");
    }
    let downstream_upgrade = hyper::upgrade::on(&mut request);
    let host = uri.host_str().context("upstream URI has no host")?;
    let port = uri.port().unwrap_or(if uri.scheme() == "https" { 443 } else { 80 });
    let (stream, upstream_ip) = crate::http_proxy::connect_target(host, port, resolver, domains, config, domain.as_ref()).await?;
    let io: Box<dyn AsyncIo> = if uri.scheme() == "https" {
        match tls_proxy.connect_upstream(stream, host, upstream_ip, domain.as_ref(), false).await {
            Ok(tls) => Box::new(tls),
            Err(error) => {
                resolver.report_failure(upstream_ip).await;
                return Err(error);
            }
        }
    } else if uri.scheme() == "http" {
        Box::new(stream)
    } else {
        anyhow::bail!("unsupported upstream URI scheme '{}'", uri.scheme());
    };
    let (mut sender, connection) = client_http1::handshake(TokioIo::new(io)).await?;
    let connection_task = tokio::spawn(async move {
        if let Err(error) = connection.with_upgrades().await {
            tracing::debug!(%error, "upstream HTTP upgrade connection ended");
        }
    });
    let connection_guard = UpstreamConnectionGuard(connection_task.abort_handle());

    let (mut parts, body) = request.into_parts();
    parts.uri = format!("{}://{}{}", uri.scheme(), host_authority(&uri), request_target).parse()
        .context("building upstream upgrade request URI")?;
    preserve_upgrade_hop_by_hop_headers(&mut parts.headers);
    append_fastgithub_user_agent(&mut parts.headers)?;
    parts.headers.insert(header::HOST, HeaderValue::from_str(&host_header(&uri))?);
    let upstream_request = Request::from_parts(parts, body);
    tracing::debug!(%original_host, upstream = %uri, "forwarding HTTP protocol upgrade");

    let mut upstream_response = sender.send_request(upstream_request).await?;
    if upstream_response.status() != StatusCode::SWITCHING_PROTOCOLS {
        let (mut response_parts, response_body) = upstream_response.into_parts();
        strip_hop_by_hop_headers(&mut response_parts.headers);
        response_parts.headers.insert(header::SERVER, HeaderValue::from_static("FastGithub"));
        let boxed_body = response_body.map_err(|error| Box::new(error) as BoxError).boxed_unsync();
        let remaining = body_timeout.map(|duration| duration.saturating_sub(request_started.elapsed()));
        let response_body = TimeoutBody::new(boxed_body, remaining, connection_guard).boxed_unsync();
        return Ok(Response::from_parts(response_parts, response_body));
    }

    if !is_valid_upgrade_response(upstream_response.headers(), &requested_protocols) {
        anyhow::bail!("upstream returned an invalid or unrequested protocol upgrade");
    }

    let upstream_upgrade = hyper::upgrade::on(&mut upstream_response);
    let (mut response_parts, _response_body) = upstream_response.into_parts();
    preserve_upgrade_hop_by_hop_headers(&mut response_parts.headers);
    response_parts.headers.insert(header::SERVER, HeaderValue::from_static("FastGithub"));

    tokio::spawn(async move {
        match tokio::try_join!(downstream_upgrade, upstream_upgrade) {
            Ok((downstream, upstream)) => {
                // Both HTTP connections have handed off their IO. The driver is
                // no longer needed; the upgraded byte streams now own the tunnel.
                drop(connection_guard);
                let mut downstream = TokioIo::new(downstream);
                let mut upstream = TokioIo::new(upstream);
                if let Err(error) = tokio::io::copy_bidirectional(&mut downstream, &mut upstream).await {
                    tracing::debug!(%error, "upgraded protocol tunnel ended");
                }
            }
            Err(error) => {
                drop(connection_guard);
                tracing::debug!(%error, "HTTP protocol upgrade failed");
            }
        }
    });

    Ok(Response::from_parts(response_parts, empty_body()))
}

const FASTGITHUB_USER_AGENT: &str = "FastGithub/1.0";

/// The original HttpClient rejects requests that already carry its marker to
/// prevent a Destination rule from sending traffic back through this proxy.
fn has_fastgithub_user_agent(headers: &HeaderMap) -> bool {
    headers.get_all(header::USER_AGENT).iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(str::split_ascii_whitespace)
        .any(|product| product == FASTGITHUB_USER_AGENT)
}

/// Append the marker without discarding the caller's existing User-Agent.
fn append_fastgithub_user_agent(headers: &mut HeaderMap) -> anyhow::Result<()> {
    let existing = headers.get_all(header::USER_AGENT).iter()
        .map(|value| value.to_str())
        .collect::<Result<Vec<_>, _>>()?;
    let mut value = existing.join(" ");
    if !value.is_empty() {
        value.push(' ');
    }
    value.push_str(FASTGITHUB_USER_AGENT);
    headers.insert(header::USER_AGENT, HeaderValue::from_str(&value)?);
    Ok(())
}

/// Remove hop-by-hop fields, including every field nominated by Connection.
fn strip_hop_by_hop_headers(headers: &mut HeaderMap) {
    let nominated = headers.get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|name| HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .collect::<Vec<_>>();

    for name in nominated {
        headers.remove(name);
    }
    for name in [
        HeaderName::from_static("connection"),
        HeaderName::from_static("proxy-connection"),
        HeaderName::from_static("keep-alive"),
        HeaderName::from_static("proxy-authenticate"),
        HeaderName::from_static("proxy-authorization"),
        HeaderName::from_static("te"),
        HeaderName::from_static("upgrade"),
    ] {
        headers.remove(name);
    }
}

fn build_upstream_request_uri(uri: &url::Url, request_target: &str) -> anyhow::Result<http::Uri> {
    http::Uri::builder()
        .scheme(uri.scheme())
        .authority(host_authority(uri))
        .path_and_query(request_target)
        .build()
        .context("invalid upstream request target")
}

fn host_header(uri: &url::Url) -> String {
    let host = uri.host_str().unwrap_or_default();
    if host.contains(':') { format!("[{host}]") } else { host.to_owned() }
}

fn host_authority(uri: &url::Url) -> String {
    let host = uri.host_str().unwrap_or_default();
    let host = if host.contains(':') { format!("[{host}]") } else { host.to_owned() };
    match uri.port() {
        Some(port) => format!("{host}:{port}"),
        None => host,
    }
}

fn configured_response(method: &Method, rule: crate::config::ResponseConfig) -> Response<ResponseBody> {
    let status = match StatusCode::from_u16(rule.status_code) {
        Ok(status) if (200..=599).contains(&status.as_u16()) => status,
        _ => return empty_response(StatusCode::INTERNAL_SERVER_ERROR),
    };
    let mut response = Response::new(empty_body());
    *response.status_mut() = status;
    match HeaderValue::from_str(&rule.content_type) {
        Ok(value) => { response.headers_mut().insert(header::CONTENT_TYPE, value); }
        Err(_) => return empty_response(StatusCode::INTERNAL_SERVER_ERROR),
    }
    let body = rule.content_value.unwrap_or_default().into_bytes();
    let suppress_body = method == Method::HEAD || matches!(status.as_u16(), 204 | 205 | 304);
    if !matches!(status.as_u16(), 204 | 304) {
        let length = if status.as_u16() == 205 { 0 } else { body.len() };
        if let Ok(value) = HeaderValue::from_str(&length.to_string()) {
            response.headers_mut().insert(header::CONTENT_LENGTH, value);
        }
    }
    if !suppress_body { *response.body_mut() = Full::new(Bytes::from(body)).map_err(|never| match never {}).boxed_unsync(); }
    response.headers_mut().insert(header::SERVER, HeaderValue::from_static("FastGithub"));
    response
}

fn management_response(
    status: StatusCode,
    content_type: &'static str,
    body: String,
) -> Response<ResponseBody> {
    let bytes = Bytes::from(body);
    let mut response = Response::new(
        Full::new(bytes.clone())
            .map_err(|never| match never {})
            .boxed_unsync(),
    );
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(content_type),
    );
    if let Ok(value) = HeaderValue::from_str(&bytes.len().to_string()) {
        response.headers_mut().insert(header::CONTENT_LENGTH, value);
    }
    response.headers_mut().insert(
        header::SERVER,
        HeaderValue::from_static("FastGithub"),
    );
    response
}

fn empty_body() -> ResponseBody {
    Full::new(Bytes::new()).map_err(|never| match never {}).boxed_unsync()
}

fn empty_response(status: StatusCode) -> Response<ResponseBody> {
    let mut response = Response::new(empty_body());
    *response.status_mut() = status;
    response.headers_mut().insert(header::CONTENT_LENGTH, HeaderValue::from_static("0"));
    response.headers_mut().insert(header::SERVER, HeaderValue::from_static("FastGithub"));
    response
}

#[cfg(test)]
mod header_tests {
    use super::strip_hop_by_hop_headers;
    use http::{header, HeaderMap, HeaderValue};

    #[test]
    fn user_agent_marker_detects_proxy_loops_and_preserves_existing_agent() {
        use super::{append_fastgithub_user_agent, has_fastgithub_user_agent};
        use http::{header, HeaderMap, HeaderValue};

        let mut headers = HeaderMap::new();
        headers.insert(header::USER_AGENT, HeaderValue::from_static("curl/8.0"));
        assert!(!has_fastgithub_user_agent(&headers));
        append_fastgithub_user_agent(&mut headers).unwrap();
        assert_eq!(headers.get(header::USER_AGENT).unwrap(), "curl/8.0 FastGithub/1.0");
        assert!(has_fastgithub_user_agent(&headers));

        headers.insert(header::USER_AGENT, HeaderValue::from_static("Other/2.0 (FastGithub/1.0)"));
        assert!(!has_fastgithub_user_agent(&headers));
        headers.insert(header::USER_AGENT, HeaderValue::from_static("FastGithub/1.0"));
        assert!(has_fastgithub_user_agent(&headers));
    }

    #[tokio::test]
    async fn timeout_body_emits_one_error_then_ends() {
        use super::TimeoutBody;
        use bytes::Bytes;
        use http_body_util::{BodyExt, Full};

        let inner = Full::new(Bytes::from_static(b"response"))
            .map_err(|never| match never {})
            .boxed_unsync();
        let connection_task = tokio::spawn(std::future::pending::<()>());
        let connection_guard = super::UpstreamConnectionGuard(connection_task.abort_handle());
        let mut body = TimeoutBody::new(inner, Some(std::time::Duration::ZERO), connection_guard);

        assert!(body.frame().await.expect("timeout error frame").is_err());
        assert!(body.frame().await.is_none());
        assert!(connection_task.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn completed_response_body_releases_upstream_driver() {
        use super::TimeoutBody;
        use bytes::Bytes;
        use http_body_util::{BodyExt, Full};

        let inner = Full::new(Bytes::from_static(b"ok"))
            .map_err(|never| match never {})
            .boxed_unsync();
        let task = tokio::spawn(std::future::pending::<()>());
        let guard = super::UpstreamConnectionGuard(task.abort_handle());
        let mut body = TimeoutBody::new(inner, None, guard);

        assert_eq!(body.frame().await.unwrap().unwrap().into_data().unwrap(), Bytes::from_static(b"ok"));
        assert!(body.frame().await.is_none());
        assert!(task.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn hyper_http1_server_sends_continue_only_when_request_body_is_polled() {
        use bytes::Bytes;
        use http::Response;
        use http_body_util::{BodyExt, Full};
        use hyper::{server::conn::http1 as server_http1, service::service_fn, Request};
        use hyper_util::rt::TokioIo;
        use std::{convert::Infallible, sync::Mutex};
        use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpListener, sync::oneshot};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (received_tx, received_rx) = oneshot::channel();
        let (poll_tx, poll_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let received_tx = Mutex::new(Some(received_tx));
            let poll_rx = Mutex::new(Some(poll_rx));
            let service = service_fn(move |request: Request<hyper::body::Incoming>| {
                let received_tx = received_tx.lock().unwrap().take().unwrap();
                let poll_rx = poll_rx.lock().unwrap().take().unwrap();
                async move {
                    received_tx.send(()).unwrap();
                    // Do not touch Incoming until the test explicitly permits it.
                    poll_rx.await.unwrap();
                    let mut body = request.into_body();
                    while let Some(frame) = body.frame().await {
                        frame.unwrap();
                    }
                    Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(b"ok"))))
                }
            });
            server_http1::Builder::new()
                .keep_alive(false)
                .serve_connection(TokioIo::new(stream), service)
                .await
                .unwrap();
        });

        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream.write_all(
            b"POST /upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: 7\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n",
        ).await.unwrap();
        received_rx.await.unwrap();

        // Merely receiving the request head must not emit an interim response.
        let mut probe = [0_u8; 1];
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(50),
            stream.read(&mut probe),
        ).await.is_err());

        poll_tx.send(()).unwrap();
        let mut interim = Vec::new();
        let mut byte = [0_u8; 1];
        while !interim.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).await.unwrap();
            interim.push(byte[0]);
        }
        assert!(String::from_utf8_lossy(&interim).starts_with("HTTP/1.1 100"));

        stream.write_all(b"payload").await.unwrap();
        let mut final_head = Vec::new();
        while !final_head.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).await.unwrap();
            final_head.push(byte[0]);
        }
        assert!(String::from_utf8_lossy(&final_head).starts_with("HTTP/1.1 200"));
        let mut response_body = [0_u8; 2];
        stream.read_exact(&mut response_body).await.unwrap();
        assert_eq!(&response_body, b"ok");
        server.await.unwrap();
    }

    #[test]
    fn expect_continue_does_not_gate_an_empty_request_body() {
        use bytes::Bytes;
        use http::{header, HeaderMap, HeaderValue};
        use http_body::Body;
        use http_body_util::Full;

        let mut headers = HeaderMap::new();
        headers.insert(header::EXPECT, HeaderValue::from_static("100-continue"));
        let body = Full::new(Bytes::new());
        assert!(expects_continue(&headers));
        assert!(body.is_end_stream());
        assert!(!should_gate_expect_continue(&headers, &body));
    }

    #[tokio::test]
    async fn gated_expect_body_waits_for_upstream_continue() {
        use bytes::Bytes;
        use http::{header, HeaderValue, Request, StatusCode};
        use http_body_util::{BodyExt, Full};
        use hyper::{client::conn::http1 as client_http1, ext::on_informational};
        use hyper_util::rt::TokioIo;
        use std::sync::Mutex;
        use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpListener, sync::oneshot};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut head = Vec::new();
            let mut byte = [0_u8; 1];
            while !head.ends_with(b"\r\n\r\n") {
                stream.read_exact(&mut byte).await.unwrap();
                head.push(byte[0]);
            }
            assert!(String::from_utf8_lossy(&head).to_ascii_lowercase().contains("expect: 100-continue"));
            let mut probe = [0_u8; 1];
            assert!(tokio::time::timeout(
                std::time::Duration::from_millis(100),
                stream.read(&mut probe),
            ).await.is_err());
            stream.write_all(b"HTTP/1.1 100 Continue\r\n\r\n").await.unwrap();
            let mut body = [0_u8; 7];
            stream.read_exact(&mut body).await.unwrap();
            assert_eq!(&body, b"payload");
            stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok").await.unwrap();
        });

        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let (mut sender, connection) = client_http1::handshake(TokioIo::new(stream)).await.unwrap();
        let driver = tokio::spawn(async move { connection.await.unwrap() });
        let (continue_tx, continue_rx) = oneshot::channel();
        let body = ExpectContinueBody::new(
            Full::new(Bytes::from_static(b"payload")).map_err(|never| match never {}),
            Some(continue_rx),
        );
        let mut request = Request::new(body);
        *request.method_mut() = http::Method::POST;
        *request.uri_mut() = "/upload".parse().unwrap();
        request.headers_mut().insert(header::HOST, HeaderValue::from_static("localhost"));
        request.headers_mut().insert(header::EXPECT, HeaderValue::from_static("100-continue"));
        let sender_gate = Mutex::new(Some(continue_tx));
        on_informational(&mut request, move |response| {
            if response.status() == StatusCode::CONTINUE {
                if let Some(sender) = sender_gate.lock().unwrap().take() {
                    let _ = sender.send(());
                }
            }
        });

        let response = sender.send_request(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.into_body().collect().await.unwrap().to_bytes(), Bytes::from_static(b"ok"));
        drop(sender);
        driver.await.unwrap().unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn upstream_disconnect_during_upload_releases_connection_driver() {
        use bytes::Bytes;
        use http::{header, HeaderValue, Request};
        use http_body::Frame;
        use hyper::client::conn::http1 as client_http1;
        use hyper_util::rt::TokioIo;
        use std::{convert::Infallible, pin::Pin, task::{Context, Poll}};
        use tokio::{io::AsyncReadExt, net::TcpListener};

        struct PendingUpload;
        impl http_body::Body for PendingUpload {
            type Data = Bytes;
            type Error = Infallible;

            fn poll_frame(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
                Poll::Pending
            }

            fn is_end_stream(&self) -> bool { false }
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut head = Vec::new();
            let mut byte = [0_u8; 1];
            while !head.ends_with(b"\r\n\r\n") {
                stream.read_exact(&mut byte).await.unwrap();
                head.push(byte[0]);
            }
            assert!(String::from_utf8_lossy(&head).starts_with("POST /upload HTTP/1.1"));
            // Close while the request body is still pending.
        });

        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let (mut sender, connection) = client_http1::handshake(TokioIo::new(stream)).await.unwrap();
        let driver = tokio::spawn(async move { connection.await });
        let guard = super::UpstreamConnectionGuard(driver.abort_handle());

        let mut request = Request::new(PendingUpload);
        *request.method_mut() = http::Method::POST;
        *request.uri_mut() = "/upload".parse().unwrap();
        request.headers_mut().insert(header::HOST, HeaderValue::from_static("localhost"));

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            sender.send_request(request),
        ).await;
        assert!(result.is_ok(), "upstream disconnect should wake the pending send_request");
        assert!(result.unwrap().is_err(), "upstream disconnect must fail the upload");
        drop(guard);
        drop(sender);
        match tokio::time::timeout(std::time::Duration::from_secs(2), driver).await.unwrap() {
            Err(error) if error.is_cancelled() => {}
            Ok(Err(_)) => {}
            other => panic!("unexpected upstream driver completion: {other:?}"),
        }
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn request_timeout_during_upload_aborts_upstream_driver() {
        use bytes::Bytes;
        use http::{header, HeaderValue, Request};
        use http_body::Frame;
        use hyper::client::conn::http1 as client_http1;
        use hyper_util::rt::TokioIo;
        use std::{convert::Infallible, pin::Pin, task::{Context, Poll}};
        use tokio::{io::AsyncReadExt, net::TcpListener};

        struct PendingUpload;
        impl http_body::Body for PendingUpload {
            type Data = Bytes;
            type Error = Infallible;

            fn poll_frame(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
                Poll::Pending
            }

            fn is_end_stream(&self) -> bool { false }
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut head = Vec::new();
            let mut byte = [0_u8; 1];
            while !head.ends_with(b"\r\n\r\n") {
                stream.read_exact(&mut byte).await.unwrap();
                head.push(byte[0]);
            }
            assert!(String::from_utf8_lossy(&head).starts_with("POST /upload HTTP/1.1"));
            // The upload body remains pending. Aborting the driver must close
            // the socket rather than leave a detached connection task behind.
            let mut body_byte = [0_u8; 1];
            assert_eq!(stream.read(&mut body_byte).await.unwrap(), 0);
        });

        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let (mut sender, connection) = client_http1::handshake(TokioIo::new(stream)).await.unwrap();
        let driver = tokio::spawn(async move { connection.await });
        let guard = super::UpstreamConnectionGuard(driver.abort_handle());

        let mut request = Request::new(PendingUpload);
        *request.method_mut() = http::Method::POST;
        *request.uri_mut() = "/upload".parse().unwrap();
        request.headers_mut().insert(header::HOST, HeaderValue::from_static("localhost"));

        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(100),
            sender.send_request(request),
        ).await.is_err(), "pending upload should be bounded by the request timeout");
        drop(guard);
        drop(sender);
        match driver.await {
            Err(error) if error.is_cancelled() => {}
            Ok(Err(_)) => {}
            other => panic!("unexpected upstream driver completion: {other:?}"),
        }
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn canceling_gated_expect_request_aborts_upstream_driver() {
        use bytes::Bytes;
        use http::{header, HeaderValue, Request};
        use http_body_util::Full;
        use hyper::client::conn::http1 as client_http1;
        use hyper_util::rt::TokioIo;
        use tokio::{io::AsyncReadExt, net::TcpListener, sync::oneshot};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut head = Vec::new();
            let mut byte = [0_u8; 1];
            while !head.ends_with(b"\r\n\r\n") {
                stream.read_exact(&mut byte).await.unwrap();
                head.push(byte[0]);
            }
            // No 100 is sent. Cancellation must close the upstream socket
            // without ever polling or transmitting the gated request body.
            assert!(String::from_utf8_lossy(&head).to_ascii_lowercase().contains("expect: 100-continue"));
            let mut body_byte = [0_u8; 1];
            assert_eq!(stream.read(&mut body_byte).await.unwrap(), 0);
        });

        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let (mut sender, connection) = client_http1::handshake(TokioIo::new(stream)).await.unwrap();
        let driver = tokio::spawn(async move { connection.await });
        let connection_guard = super::UpstreamConnectionGuard(driver.abort_handle());

        let (_continue_tx, continue_rx) = oneshot::channel();
        let body = super::ExpectContinueBody::new(
            Full::new(Bytes::from_static(b"payload")).map_err(|never| match never {}),
            Some(continue_rx),
        );
        let mut request = Request::new(body);
        *request.method_mut() = http::Method::POST;
        *request.uri_mut() = "/upload".parse().unwrap();
        request.headers_mut().insert(header::HOST, HeaderValue::from_static("localhost"));
        request.headers_mut().insert(header::EXPECT, HeaderValue::from_static("100-continue"));

        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(100),
            sender.send_request(request),
        ).await.is_err());
        drop(connection_guard);
        match driver.await {
            Err(error) if error.is_cancelled() => {}
            Ok(Err(_)) => {} // send_request cancellation may close the driver first
            other => panic!("unexpected upstream driver completion: {other:?}"),
        }
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn gated_expect_body_stops_when_upstream_rejects_without_continue() {
        use bytes::Bytes;
        use http::{header, HeaderValue, Request, StatusCode};
        use http_body_util::{BodyExt, Full};
        use hyper::client::conn::http1 as client_http1;
        use hyper_util::rt::TokioIo;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;
        use tokio::sync::oneshot;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut head = Vec::new();
            let mut byte = [0_u8; 1];
            while !head.ends_with(b"\r\n\r\n") {
                stream.read_exact(&mut byte).await.unwrap();
                head.push(byte[0]);
            }
            assert!(String::from_utf8_lossy(&head).to_ascii_lowercase().contains("expect: 100-continue"));
            let mut probe = [0_u8; 1];
            assert!(tokio::time::timeout(
                std::time::Duration::from_millis(100),
                stream.read(&mut probe),
            ).await.is_err());
            stream.write_all(b"HTTP/1.1 417 Expectation Failed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
            // The client must not upload the gated body after the final rejection.
            match tokio::time::timeout(
                std::time::Duration::from_millis(100),
                stream.read(&mut probe),
            ).await {
                Err(_) | Ok(Ok(0)) => {}
                Ok(Ok(_)) => panic!("client uploaded request data after a final 417 response"),
                Ok(Err(error)) => panic!("reading rejected request stream: {error}"),
            }
        });

        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let (mut sender, connection) = client_http1::handshake(TokioIo::new(stream)).await.unwrap();
        let driver = tokio::spawn(async move { connection.await.unwrap() });
        let (_continue_tx, continue_rx) = oneshot::channel();
        let body = ExpectContinueBody::new(
            Full::new(Bytes::from_static(b"payload")).map_err(|never| match never {}),
            Some(continue_rx),
        );
        let mut request = Request::new(body);
        *request.method_mut() = http::Method::POST;
        *request.uri_mut() = "/upload".parse().unwrap();
        request.headers_mut().insert(header::HOST, HeaderValue::from_static("localhost"));
        request.headers_mut().insert(header::EXPECT, HeaderValue::from_static("100-continue"));

        let response = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            sender.send_request(request),
        ).await.unwrap().unwrap();
        assert_eq!(response.status(), StatusCode::EXPECTATION_FAILED);
        response.into_body().collect().await.unwrap();
        drop(sender);
        let _ = driver.await;
        server.await.unwrap();
    }

    #[tokio::test]
    async fn hyper_http1_client_reports_upstream_informational_responses() {
        use bytes::Bytes;
        use http::{header, HeaderValue, Request, StatusCode};
        use http_body_util::{BodyExt, Full};
        use hyper::{client::conn::http1 as client_http1, ext::on_informational};
        use hyper_util::rt::TokioIo;
        use std::sync::{Arc, Mutex};
        use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpListener};

        // Hyper's server Service API only returns one final Response, so use a
        // tiny raw peer to emit a real 100 Continue followed by the final reply.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request_head = Vec::new();
            let mut byte = [0_u8; 1];
            while !request_head.ends_with(b"\r\n\r\n") {
                stream.read_exact(&mut byte).await.unwrap();
                request_head.push(byte[0]);
            }
            assert!(String::from_utf8_lossy(&request_head)
                .to_ascii_lowercase()
                .contains("expect: 100-continue"));
            // Hyper's HTTP/1 client does not hold this body until it sees
            // the upstream 100 response: it starts streaming after the head.
            // Read the complete body before allowing the peer to send 100.
            let mut request_body = [0_u8; 7];
            tokio::time::timeout(
                std::time::Duration::from_secs(2),
                stream.read_exact(&mut request_body),
            ).await.unwrap().unwrap();
            assert_eq!(&request_body, b"payload");
            stream.write_all(
                b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
            ).await.unwrap();
        });

        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let (mut sender, connection) = client_http1::handshake(TokioIo::new(stream)).await.unwrap();
        let driver = tokio::spawn(async move { connection.await.unwrap() });
        let informational = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&informational);
        let mut request = Request::new(Full::new(Bytes::from_static(b"payload")));
        *request.method_mut() = http::Method::POST;
        *request.uri_mut() = "/upload".parse().unwrap();
        request.headers_mut().insert(header::HOST, HeaderValue::from_static("localhost"));
        request.headers_mut().insert(header::EXPECT, HeaderValue::from_static("100-continue"));
        on_informational(&mut request, move |response| {
            observed.lock().unwrap().push(response.status());
        });

        let response = sender.send_request(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(informational.lock().unwrap().as_slice(), &[StatusCode::CONTINUE]);
        assert_eq!(response.into_body().collect().await.unwrap().to_bytes(), Bytes::from_static(b"ok"));
        drop(sender);
        driver.await.unwrap().unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn hyper_http1_loopback_preserves_request_and_response_trailers() {
        use super::BoxError;
        use bytes::Bytes;
        use http::{HeaderMap, HeaderValue, Request, Response};
        use http_body::Frame;
        use http_body_util::BodyExt;
        use hyper::{client::conn::http1 as client_http1, server::conn::http1 as server_http1, service::service_fn};
        use hyper_util::rt::TokioIo;
        use std::{collections::VecDeque, convert::Infallible, pin::Pin, task::{Context, Poll}};
        use tokio::net::TcpListener;

        struct Frames(VecDeque<Frame<Bytes>>);
        impl http_body::Body for Frames {
            type Data = Bytes;
            type Error = BoxError;

            fn poll_frame(
                mut self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
                Poll::Ready(self.0.pop_front().map(Ok))
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let service = service_fn(|request: Request<hyper::body::Incoming>| async move {
                let mut body = request.into_body();
                let mut received_data = Vec::new();
                let mut received_trailers = None;
                while let Some(frame) = body.frame().await {
                    let frame = frame.unwrap();
                    if frame.is_data() {
                        received_data.extend_from_slice(&frame.into_data().unwrap());
                    } else if frame.is_trailers() {
                        received_trailers = Some(frame.into_trailers().unwrap());
                    }
                }
                assert_eq!(received_data, b"request-body");
                assert_eq!(
                    received_trailers.unwrap().get("x-request-checksum").unwrap(),
                    "request-ok",
                );

                let mut trailers = HeaderMap::new();
                trailers.insert("x-response-checksum", HeaderValue::from_static("response-ok"));
                let body = Frames(VecDeque::from([
                    Frame::data(Bytes::from_static(b"response-body")),
                    Frame::trailers(trailers),
                ]));
                let mut response = Response::new(body);
                response.headers_mut().insert(
                    "trailer",
                    HeaderValue::from_static("x-response-checksum"),
                );
                Ok::<_, Infallible>(response)
            });
            server_http1::Builder::new()
                .keep_alive(false)
                .serve_connection(TokioIo::new(stream), service)
                .await
                .unwrap();
        });

        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let (mut sender, connection) = client_http1::handshake(TokioIo::new(stream)).await.unwrap();
        let driver = tokio::spawn(async move { connection.await.unwrap() });

        let mut request_trailers = HeaderMap::new();
        request_trailers.insert("x-request-checksum", HeaderValue::from_static("request-ok"));
        let request_body = Frames(VecDeque::from([
            Frame::data(Bytes::from_static(b"request-body")),
            Frame::trailers(request_trailers),
        ]));
        let mut request = Request::new(request_body);
        *request.method_mut() = http::Method::POST;
        *request.uri_mut() = "/upload".parse().unwrap();
        request.headers_mut().insert(http::header::HOST, HeaderValue::from_static("localhost"));
        request.headers_mut().insert(
            "trailer",
            HeaderValue::from_static("x-request-checksum"),
        );

        let response = sender.send_request(request).await.unwrap();
        assert_eq!(response.status(), http::StatusCode::OK);
        let mut body = response.into_body();
        let mut received_data = Vec::new();
        let mut received_trailers = None;
        while let Some(frame) = body.frame().await {
            let frame = frame.unwrap();
            if frame.is_data() {
                received_data.extend_from_slice(&frame.into_data().unwrap());
            } else if frame.is_trailers() {
                received_trailers = Some(frame.into_trailers().unwrap());
            }
        }
        assert_eq!(received_data, b"response-body");
        assert_eq!(
            received_trailers.unwrap().get("x-response-checksum").unwrap(),
            "response-ok",
        );

        drop(sender);
        driver.await.unwrap().unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn timeout_body_preserves_response_trailer_frames() {
        use super::{BoxError, TimeoutBody};
        use bytes::Bytes;
        use http::{HeaderMap, HeaderValue};
        use http_body::Frame;
        use std::{collections::VecDeque, pin::Pin, task::{Context, Poll}};

        struct Frames(VecDeque<Frame<Bytes>>);
        impl http_body::Body for Frames {
            type Data = Bytes;
            type Error = BoxError;

            fn poll_frame(
                mut self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
                Poll::Ready(self.0.pop_front().map(Ok))
            }
        }

        let mut trailers = HeaderMap::new();
        trailers.insert("x-checksum", HeaderValue::from_static("sha256-test"));
        let inner = Frames(VecDeque::from([
            Frame::data(Bytes::from_static(b"payload")),
            Frame::trailers(trailers),
        ]));
        let task = tokio::spawn(std::future::pending::<()>());
        let guard = super::UpstreamConnectionGuard(task.abort_handle());
        let mut body = TimeoutBody::new(inner, None, guard);

        assert_eq!(
            body.frame().await.unwrap().unwrap().into_data().unwrap(),
            Bytes::from_static(b"payload"),
        );
        let forwarded_trailers = body.frame().await.unwrap().unwrap().into_trailers().unwrap();
        assert_eq!(forwarded_trailers.get("x-checksum").unwrap(), "sha256-test");
        assert!(body.frame().await.is_none());
        assert!(task.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn upstream_body_error_releases_connection_driver() {
        use super::{BoxError, TimeoutBody};
        use bytes::Bytes;
        use http_body::Frame;
        use std::{io, pin::Pin, task::{Context, Poll}};

        struct FailingBody;
        impl http_body::Body for FailingBody {
            type Data = Bytes;
            type Error = BoxError;

            fn poll_frame(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
                Poll::Ready(Some(Err(Box::new(io::Error::other("upstream body failed")))))
            }
        }

        let task = tokio::spawn(std::future::pending::<()>());
        let guard = super::UpstreamConnectionGuard(task.abort_handle());
        let mut body = TimeoutBody::new(FailingBody, None, guard);

        assert!(body.frame().await.unwrap().is_err());
        assert!(body.frame().await.is_none());
        assert!(task.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn dropping_response_body_aborts_upstream_driver() {
        use super::TimeoutBody;
        use bytes::Bytes;
        use http_body_util::{BodyExt, Full};

        let inner = Full::new(Bytes::from_static(b"not consumed"))
            .map_err(|never| match never {})
            .boxed_unsync();
        let task = tokio::spawn(std::future::pending::<()>());
        let guard = super::UpstreamConnectionGuard(task.abort_handle());
        let body = TimeoutBody::new(inner, None, guard);

        // Downstream cancellation drops the response body without polling it.
        drop(body);
        assert!(task.await.unwrap_err().is_cancelled());
    }

    #[test]
    fn upstream_request_uri_preserves_asterisk_form() {
        let uri = url::Url::parse("https://example.test/").unwrap();
        let upstream = super::build_upstream_request_uri(&uri, "*").unwrap();
        assert_eq!(upstream.scheme_str(), Some("https"));
        assert_eq!(upstream.authority().unwrap().as_str(), "example.test");
        assert_eq!(upstream.path_and_query().unwrap().as_str(), "*");

        let upstream = super::build_upstream_request_uri(&uri, "/path?q=1").unwrap();
        assert_eq!(upstream.path_and_query().unwrap().as_str(), "/path?q=1");
    }

    #[test]
    fn host_header_uses_destination_host_without_port() {
        let uri = url::Url::parse("https://cdn.example:8443/assets").unwrap();
        assert_eq!(super::host_header(&uri), "cdn.example");
        let ipv6 = url::Url::parse("https://[2001:db8::1]:8443/assets").unwrap();
        assert_eq!(super::host_header(&ipv6), "[2001:db8::1]");
    }

    #[test]
    fn removes_connection_nominated_and_standard_hop_by_hop_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONNECTION, HeaderValue::from_static("x-private, keep-alive"));
        headers.insert("x-private", HeaderValue::from_static("secret"));
        headers.insert("keep-alive", HeaderValue::from_static("timeout=5"));
        headers.insert("proxy-connection", HeaderValue::from_static("keep-alive"));
        headers.insert("proxy-authenticate", HeaderValue::from_static("Basic"));
        headers.insert("proxy-authorization", HeaderValue::from_static("Basic abc"));
        headers.insert("te", HeaderValue::from_static("trailers"));
        headers.insert("trailer", HeaderValue::from_static("x-checksum"));
        headers.insert("upgrade", HeaderValue::from_static("websocket"));
        headers.insert(header::EXPECT, HeaderValue::from_static("100-continue"));
        headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plain"));

        strip_hop_by_hop_headers(&mut headers);

        for name in [
            "connection", "x-private", "keep-alive", "proxy-connection",
            "proxy-authenticate", "proxy-authorization", "te", "upgrade",
        ] {
            assert!(!headers.contains_key(name), "header {name} was retained");
        }
        // Expect is end-to-end request semantics, not a hop-by-hop field.
        // Preserve it so the upstream HTTP/1 peer can participate in
        // the 100-continue exchange.
        assert_eq!(headers.get(header::EXPECT).unwrap(), "100-continue");
        // Trailer declares end-to-end fields carried in the message body;
        // unlike TE, it is not inherently hop-by-hop.
        assert_eq!(headers.get("trailer").unwrap(), "x-checksum");
        assert_eq!(headers.get(header::CONTENT_TYPE).unwrap(), "text/plain");
    }
    #[tokio::test]
    async fn http1_server_keeps_one_connection_open_for_multiple_requests() {
        use super::client_http1;
        use bytes::Bytes;
        use http::{Request, Response, StatusCode};
        use http_body_util::{BodyExt, Full};
        use hyper::{service::service_fn, server::conn::http1 as server_http1};
        use hyper_util::rt::TokioIo;
        use std::{convert::Infallible, sync::{Arc, atomic::{AtomicUsize, Ordering}}};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(AtomicUsize::new(0));
        let server_requests = Arc::clone(&requests);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let service = service_fn(move |_request: Request<hyper::body::Incoming>| {
                server_requests.fetch_add(1, Ordering::SeqCst);
                async {
                    let mut response = Response::new(Full::new(Bytes::from_static(b"ok")));
                    *response.status_mut() = StatusCode::OK;
                    Ok::<_, Infallible>(response)
                }
            });
            server_http1::Builder::new()
                .keep_alive(true)
                .serve_connection(TokioIo::new(stream), service)
                .await
                .unwrap();
        });

        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let (mut sender, connection) = client_http1::handshake(TokioIo::new(stream)).await.unwrap();
        let driver = tokio::spawn(async move { connection.await.unwrap() });

        for _ in 0..2 {
            let request = Request::builder()
                .method(http::Method::GET)
                .uri("/")
                .header(header::HOST, "localhost")
                .body(Full::new(Bytes::new()))
                .unwrap();
            let response = sender.send_request(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.into_body().collect().await.unwrap().to_bytes(), Bytes::from_static(b"ok"));
        }

        assert_eq!(requests.load(Ordering::SeqCst), 2);
        drop(sender);
        driver.await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn http1_server_honors_client_connection_close() {
        use bytes::Bytes;
        use http::Response;
        use http_body_util::Full;
        use hyper::{service::service_fn, server::conn::http1 as server_http1};
        use hyper_util::rt::TokioIo;
        use std::convert::Infallible;
        use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpListener, time::{timeout, Duration}};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let service = service_fn(|_request: hyper::Request<hyper::body::Incoming>| async {
                Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(b"ok"))))
            });
            server_http1::Builder::new()
                .keep_alive(true)
                .serve_connection(TokioIo::new(stream), service)
                .await
                .unwrap();
        });

        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream.write_all(
            b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        ).await.unwrap();
        let mut response = Vec::new();
        timeout(Duration::from_secs(2), stream.read_to_end(&mut response))
            .await
            .expect("Connection: close must terminate the HTTP/1 connection")
            .unwrap();
        let response = String::from_utf8(response).unwrap();
        assert!(response.starts_with("HTTP/1.1 200"));
        assert!(response.ends_with("ok"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn http1_server_terminates_incomplete_request_body_after_client_disconnect() {
        use bytes::Bytes;
        use http::Response;
        use http_body_util::Full;
        use hyper::{service::service_fn, server::conn::http1 as server_http1};
        use hyper_util::rt::TokioIo;
        use std::convert::Infallible;
        use tokio::{io::AsyncWriteExt, net::TcpListener, sync::oneshot, time::{timeout, Duration}};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (body_done_tx, body_done_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let body_done_tx = std::sync::Mutex::new(Some(body_done_tx));
            let service = service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
                let body_done_tx = body_done_tx.lock().unwrap().take();
                async move {
                    let mut body = request.into_body();
                    let mut saw_body_error = false;
                    while let Some(frame) = body.frame().await {
                        if frame.is_err() {
                            saw_body_error = true;
                            break;
                        }
                    }
                    if let Some(sender) = body_done_tx {
                        let _ = sender.send(saw_body_error);
                    }
                    Ok::<_, Infallible>(Response::new(Full::new(Bytes::new())))
                }
            });
            server_http1::Builder::new()
                .keep_alive(true)
                .serve_connection(TokioIo::new(stream), service)
                .await
        });

        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream.write_all(
            b"POST /upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: 100\r\n\r\npartial",
        ).await.unwrap();
        stream.shutdown().await.unwrap();

        let saw_body_error = timeout(Duration::from_secs(2), body_done_rx)
            .await
            .expect("incomplete request body must finish after client disconnect")
            .unwrap();
        assert!(saw_body_error, "truncated Content-Length body should be reported as an error");
        let _connection_result = timeout(Duration::from_secs(2), server)
            .await
            .expect("HTTP/1 connection task must terminate after client disconnect")
            .unwrap();
    }

    #[test]
    fn generated_responses_do_not_force_connection_close() {
        use crate::config::ResponseConfig;

        let configured = super::configured_response(
            &http::Method::GET,
            ResponseConfig {
                status_code: 200,
                content_type: "text/plain".into(),
                content_value: Some("ok".into()),
            },
        );
        assert!(!configured.headers().contains_key(header::CONNECTION));

        let error = super::empty_response(http::StatusCode::BAD_GATEWAY);
        assert!(!error.headers().contains_key(header::CONNECTION));
    }

    #[test]
    fn http2_scheme_must_match_transport_scheme() {
        use super::http2_scheme_matches;
        assert!(http2_scheme_matches(Some("https"), "https"));
        assert!(http2_scheme_matches(Some("HTTPS"), "https"));
        assert!(!http2_scheme_matches(Some("http"), "https"));
        assert!(!http2_scheme_matches(None, "https"));
    }

    #[test]
    fn http2_host_must_match_authority() {
        use super::http_authorities_match;

        let authority = "Example.com:443".parse::<http::uri::Authority>().unwrap();
        assert!(http_authorities_match(&authority, "example.COM:443"));
        assert!(!http_authorities_match(&authority, "other.example:443"));
        assert!(!http_authorities_match(&authority, "example.com"));
        assert!(!http_authorities_match(&authority, "example.com:8443"));
        assert!(!http_authorities_match(&authority, "user@example.com:443"));
        assert!(!http_authorities_match(&authority, "example.com/path"));
        assert!(!http_authorities_match(&authority, "example.com:99999"));
    }

    #[test]
    fn validates_that_upstream_selects_exactly_one_requested_upgrade_protocol() {
        use super::is_valid_upgrade_response;

        let mut headers = HeaderMap::new();
        headers.insert(header::CONNECTION, HeaderValue::from_static("Upgrade"));
        headers.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
        let requested = vec!["websocket".to_owned(), "h2c".to_owned()];
        assert!(is_valid_upgrade_response(&headers, &requested));

        headers.insert(header::UPGRADE, HeaderValue::from_static("WebSocket"));
        assert!(is_valid_upgrade_response(&headers, &requested));

        headers.insert(header::UPGRADE, HeaderValue::from_static("websocket, h2c"));
        assert!(!is_valid_upgrade_response(&headers, &requested));

        headers.insert(header::UPGRADE, HeaderValue::from_static("not-offered"));
        assert!(!is_valid_upgrade_response(&headers, &requested));

        headers.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
        headers.remove(header::CONNECTION);
        assert!(!is_valid_upgrade_response(&headers, &requested));
    }

    #[test]
    fn detects_and_preserves_http1_upgrade_headers() {
        use super::{is_upgrade_request, preserve_upgrade_hop_by_hop_headers};
        use bytes::Bytes;
        use http::{Request, Version};
        use http_body_util::Full;

        let request = Request::builder()
            .method(http::Method::GET)
            .version(Version::HTTP_11)
            .uri("/socket")
            .header(header::CONNECTION, "keep-alive, Upgrade")
            .header(header::UPGRADE, "websocket")
            .body(Full::new(Bytes::new()))
            .unwrap();
        assert!(is_upgrade_request(&request));

        let mut headers = request.headers().clone();
        preserve_upgrade_hop_by_hop_headers(&mut headers);
        assert_eq!(headers.get(header::CONNECTION).unwrap(), "Upgrade");
        assert_eq!(headers.get(header::UPGRADE).unwrap(), "websocket");
        assert!(!headers.contains_key("keep-alive"));

        let ordinary = Request::builder()
            .method(http::Method::GET)
            .version(Version::HTTP_11)
            .uri("/")
            .header(header::CONNECTION, "keep-alive")
            .body(Full::new(Bytes::new()))
            .unwrap();
        assert!(!is_upgrade_request(&ordinary));
    }

    #[tokio::test]
    async fn hyper_http2_loopback_serves_request_and_response_body() {
        use super::{client_http2, server_http2, TokioExecutor};
        use bytes::Bytes;
        use http::{Request, Response, StatusCode, Version};
        use http_body_util::{BodyExt, Full};
        use hyper::{body::Incoming, service::service_fn};
        use hyper_util::rt::TokioIo;
        use std::convert::Infallible;
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let service = service_fn(|request: Request<Incoming>| async move {
                assert_eq!(request.version(), Version::HTTP_2);
                assert_eq!(request.uri().path(), "/h2");
                // Exceed the default HTTP/2 stream flow-control window so
                // the client must keep consuming DATA frames to make progress.
                let response = Response::new(Full::new(Bytes::from(vec![b'x'; 2 * 1024 * 1024])));
                Ok::<_, Infallible>(response)
            });
            server_http2::Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(stream), service)
                .await
                .unwrap();
        });

        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let (mut sender, connection) =
            client_http2::handshake(TokioExecutor::new(), TokioIo::new(stream)).await.unwrap();
        let driver = tokio::spawn(async move { connection.await.unwrap() });

        let request = Request::builder()
            .method(http::Method::GET)
            .version(Version::HTTP_2)
            .uri("https://localhost/h2")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let response = sender.send_request(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body.len(), 2 * 1024 * 1024);
        assert!(body.iter().all(|byte| *byte == b'x'));

        drop(sender);
        driver.await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn hyper_http2_early_response_does_not_wait_for_request_body_completion() {
        use super::{client_http2, server_http2, TokioExecutor, TimeoutBody, UpstreamConnectionGuard};
        use bytes::Bytes;
        use http::{Request, Response, StatusCode, Version};
        use http_body::Frame;
        use http_body_util::BodyExt;
        use hyper::{body::Incoming, service::service_fn};
        use hyper_util::rt::TokioIo;
        use std::{convert::Infallible, pin::Pin, task::{Context, Poll}};
        use tokio::net::TcpListener;

        struct PendingBody;
        impl http_body::Body for PendingBody {
            type Data = Bytes;
            type Error = Infallible;

            fn poll_frame(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
                Poll::Pending
            }

            fn is_end_stream(&self) -> bool { false }
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let service = service_fn(|_request: Request<Incoming>| async move {
                // Deliberately reject without polling the request body.
                Ok::<_, Infallible>(Response::builder()
                    .status(StatusCode::PAYLOAD_TOO_LARGE)
                    .body(http_body_util::Full::new(Bytes::from_static(b"too large")))
                    .unwrap())
            });
            let _ = server_http2::Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(stream), service)
                .await;
        });

        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let (mut sender, connection) =
            client_http2::handshake(TokioExecutor::new(), TokioIo::new(stream)).await.unwrap();
        let driver = tokio::spawn(async move { connection.await });
        let connection_guard = UpstreamConnectionGuard(driver.abort_handle());
        let request = Request::builder()
            .method(http::Method::POST)
            .version(Version::HTTP_2)
            .uri("https://localhost/upload")
            .body(PendingBody)
            .unwrap();

        let response = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            sender.send_request(request),
        ).await.expect("early HTTP/2 response must not wait for request body EOF").unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let response_body = response.into_body()
            .map_err(|error| Box::new(error) as super::BoxError)
            .boxed_unsync();
        let mut response_body = TimeoutBody::new(response_body, None, connection_guard);
        let mut response_bytes = Vec::new();
        while let Some(frame) = response_body.frame().await {
            response_bytes.extend_from_slice(&frame.unwrap().into_data().unwrap());
        }
        assert_eq!(response_bytes, b"too large");

        // Consuming the early response releases the guard and aborts the
        // HTTP/2 driver, which also stops polling the still-pending upload.
        drop(sender);
        assert!(driver.await.unwrap_err().is_cancelled());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn http2_upstream_disconnect_during_upload_releases_driver() {
        use super::{client_http2, server_http2, TokioExecutor, UpstreamConnectionGuard};
        use bytes::Bytes;
        use http::{Request, Response, Version};
        use http_body::Frame;
        use hyper::{body::Incoming, service::service_fn};
        use hyper_util::rt::TokioIo;
        use std::{convert::Infallible, pin::Pin, task::{Context, Poll}};
        use tokio::{net::TcpListener, sync::oneshot};

        struct PendingUpload;
        impl http_body::Body for PendingUpload {
            type Data = Bytes;
            type Error = Infallible;

            fn poll_frame(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
                Poll::Pending
            }

            fn is_end_stream(&self) -> bool { false }
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (request_seen_tx, request_seen_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let request_seen_tx = std::sync::Mutex::new(Some(request_seen_tx));
            let service = service_fn(move |_request: Request<Incoming>| {
                if let Some(sender) = request_seen_tx.lock().unwrap().take() {
                    let _ = sender.send(());
                }
                async move {
                    std::future::pending::<Result<Response<http_body_util::Full<Bytes>>, Infallible>>().await
                }
            });
            let _ = server_http2::Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(stream), service)
                .await;
        });

        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let (mut sender, connection) =
            client_http2::handshake(TokioExecutor::new(), TokioIo::new(stream)).await.unwrap();
        let driver = tokio::spawn(async move { connection.await });
        let guard = UpstreamConnectionGuard(driver.abort_handle());
        let request = Request::builder()
            .method(http::Method::POST)
            .version(Version::HTTP_2)
            .uri("https://localhost/upload")
            .body(PendingUpload)
            .unwrap();

        let send = tokio::spawn(async move { sender.send_request(request).await });
        tokio::time::timeout(std::time::Duration::from_secs(2), request_seen_rx)
            .await.expect("upstream must receive request headers").unwrap();
        // Simulate the upstream process disappearing before it can consume
        // the pending upload stream or send response headers.
        server.abort();
        let _ = server.await;
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), send)
            .await.expect("upstream disconnect must wake send_request").unwrap();
        assert!(result.is_err(), "upstream disconnect must fail the HTTP/2 request");
        drop(guard);
        match tokio::time::timeout(std::time::Duration::from_secs(2), driver).await.unwrap() {
            Err(error) if error.is_cancelled() => {}
            Ok(Err(_)) => {}
            other => panic!("unexpected HTTP/2 upstream driver completion: {other:?}"),
        }
    }

    #[tokio::test]
    async fn http2_request_timeout_during_upload_aborts_upstream_driver() {
        use super::{client_http2, server_http2, TokioExecutor, UpstreamConnectionGuard};
        use bytes::Bytes;
        use http::{Request, Response, Version};
        use http_body::Frame;
        use hyper::{body::Incoming, service::service_fn};
        use hyper_util::rt::TokioIo;
        use std::{convert::Infallible, pin::Pin, task::{Context, Poll}};
        use tokio::{net::TcpListener, sync::oneshot};

        struct PendingUpload;
        impl http_body::Body for PendingUpload {
            type Data = Bytes;
            type Error = Infallible;

            fn poll_frame(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
                Poll::Pending
            }

            fn is_end_stream(&self) -> bool { false }
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (request_seen_tx, request_seen_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let request_seen_tx = std::sync::Mutex::new(Some(request_seen_tx));
            let service = service_fn(move |_request: Request<Incoming>| {
                if let Some(sender) = request_seen_tx.lock().unwrap().take() {
                    let _ = sender.send(());
                }
                async move {
                    std::future::pending::<Result<Response<http_body_util::Full<Bytes>>, Infallible>>().await
                }
            });
            let _ = server_http2::Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(stream), service)
                .await;
        });

        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let (mut sender, connection) =
            client_http2::handshake(TokioExecutor::new(), TokioIo::new(stream)).await.unwrap();
        let driver = tokio::spawn(async move { connection.await });
        let guard = UpstreamConnectionGuard(driver.abort_handle());
        let request = Request::builder()
            .method(http::Method::POST)
            .version(Version::HTTP_2)
            .uri("https://localhost/upload")
            .body(PendingUpload)
            .unwrap();

        let send = tokio::spawn(async move {
            tokio::time::timeout(
                std::time::Duration::from_millis(100),
                sender.send_request(request),
            ).await
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), request_seen_rx)
            .await.expect("upstream must receive request headers").unwrap();
        // Model the request-scoped timeout firing while the upload body remains
        // pending. The timeout drops send_request; the guard then stops h2.
        assert!(send.await.unwrap().is_err(), "pending HTTP/2 upload must hit its timeout");
        drop(guard);
        match tokio::time::timeout(std::time::Duration::from_secs(2), driver).await.unwrap() {
            Err(error) if error.is_cancelled() => {}
            Ok(Err(_)) => {}
            other => panic!("unexpected HTTP/2 upstream driver completion: {other:?}"),
        }
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn hyper_http2_streams_progress_independently() {
        use super::{client_http2, server_http2, TokioExecutor};
        use bytes::Bytes;
        use http::{Request, Response, StatusCode, Version};
        use http_body_util::{BodyExt, Full};
        use hyper::{body::Incoming, service::service_fn};
        use hyper_util::rt::TokioIo;
        use std::convert::Infallible;
        use tokio::{net::TcpListener, sync::oneshot};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (release_slow_tx, release_slow_rx) = oneshot::channel::<()>();
        let (slow_started_tx, slow_started_rx) = oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let release_slow_rx = std::sync::Mutex::new(Some(release_slow_rx));
            let slow_started_tx = std::sync::Mutex::new(Some(slow_started_tx));
            let service = service_fn(move |request: Request<Incoming>| {
                let is_slow = request.uri().path() == "/slow";
                let release_slow_rx = if is_slow {
                    if let Some(started) = slow_started_tx.lock().unwrap().take() {
                        let _ = started.send(());
                    }
                    release_slow_rx.lock().unwrap().take()
                } else {
                    None
                };
                async move {
                    if let Some(release) = release_slow_rx {
                        release.await.unwrap();
                        Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(b"slow"))))
                    } else {
                        Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(b"fast"))))
                    }
                }
            });
            server_http2::Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(stream), service)
                .await
                .unwrap();
        });

        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let (sender, connection) =
            client_http2::handshake(TokioExecutor::new(), TokioIo::new(stream)).await.unwrap();
        let driver = tokio::spawn(async move { connection.await.unwrap() });

        // Keep one stream waiting for its response headers while another stream
        // completes on the same HTTP/2 connection.
        let mut slow_sender = sender.clone();
        let slow = tokio::spawn(async move {
            let request = Request::builder()
                .method(http::Method::GET)
                .version(Version::HTTP_2)
                .uri("https://localhost/slow")
                .body(Full::new(Bytes::new()))
                .unwrap();
            slow_sender.send_request(request).await.unwrap()
        });
        slow_started_rx.await.unwrap();

        let mut fast_sender = sender.clone();
        let fast_request = Request::builder()
            .method(http::Method::GET)
            .version(Version::HTTP_2)
            .uri("https://localhost/fast")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let fast_response = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            fast_sender.send_request(fast_request),
        ).await.expect("fast HTTP/2 stream must not wait for slow stream").unwrap();
        assert_eq!(fast_response.status(), StatusCode::OK);
        assert_eq!(
            fast_response.into_body().collect().await.unwrap().to_bytes(),
            Bytes::from_static(b"fast"),
        );

        // Cancel the pending stream after its sibling has completed. The
        // HTTP/2 connection must remain usable for a subsequent request.
        slow.abort();
        assert!(slow.await.unwrap_err().is_cancelled());

        let mut followup_sender = sender.clone();
        let followup_request = Request::builder()
            .method(http::Method::GET)
            .version(Version::HTTP_2)
            .uri("https://localhost/fast")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let followup_response = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            followup_sender.send_request(followup_request),
        ).await.expect("connection must remain usable after stream cancellation").unwrap();
        assert_eq!(
            followup_response.into_body().collect().await.unwrap().to_bytes(),
            Bytes::from_static(b"fast"),
        );

        drop(release_slow_tx);
        drop(sender);
        drop(fast_sender);
        drop(followup_sender);
        driver.await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn hyper_http1_upgrade_handoff_preserves_half_close_and_bidirectional_bytes() {
        use super::client_http1;
        use bytes::Bytes;
        use http::{Request, Response, StatusCode};
        use http_body_util::Full;
        use hyper::{body::Incoming, service::service_fn, server::conn::http1 as server_http1};
        use hyper_util::rt::TokioIo;
        use std::convert::Infallible;
        use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpListener};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let service = service_fn(|mut request: Request<Incoming>| async move {
                let on_upgrade = hyper::upgrade::on(&mut request);
                tokio::spawn(async move {
                    let upgraded = on_upgrade.await.unwrap();
                    let mut io = TokioIo::new(upgraded);
                    let (mut backend, mut backend_peer) = tokio::io::duplex(64);
                    let backend_task = tokio::spawn(async move {
                        let mut payload = Vec::new();
                        backend.read_to_end(&mut payload).await.unwrap();
                        assert_eq!(payload, b"ping");
                        backend.write_all(b"final").await.unwrap();
                    });
                    tokio::io::copy_bidirectional(&mut io, &mut backend_peer).await.unwrap();
                    backend_task.await.unwrap();
                });
                let mut response = Response::new(Full::new(Bytes::new()));
                *response.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
                response.headers_mut().insert(header::CONNECTION, HeaderValue::from_static("Upgrade"));
                response.headers_mut().insert(header::UPGRADE, HeaderValue::from_static("websocket"));
                Ok::<_, Infallible>(response)
            });
            server_http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .with_upgrades()
                .await
                .unwrap();
        });

        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let (mut sender, connection) = client_http1::handshake(TokioIo::new(stream)).await.unwrap();
        let driver = tokio::spawn(async move { connection.with_upgrades().await.unwrap() });
        let request = Request::builder()
            .method(http::Method::GET)
            .uri("/ws")
            .header(header::HOST, "localhost")
            .header(header::CONNECTION, "Upgrade")
            .header(header::UPGRADE, "websocket")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let response = sender.send_request(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);

        let upgraded = hyper::upgrade::on(response).await.unwrap();
        let mut io = TokioIo::new(upgraded);
        io.write_all(b"ping").await.unwrap();
        // Half-close the client-to-server direction. The peer must still be
        // able to finish its response and send it back before the tunnel ends.
        io.shutdown().await.unwrap();
        let mut final_response = Vec::new();
        io.read_to_end(&mut final_response).await.unwrap();
        assert_eq!(final_response, b"final");

        drop(io);
        drop(sender);
        driver.await.unwrap();
        server.await.unwrap();
    }

    #[test]
    fn http2_connect_is_explicitly_unsupported() {
        use super::http2_connect_unsupported;
        assert!(http2_connect_unsupported(http::Version::HTTP_2, &http::Method::CONNECT));
        assert!(!http2_connect_unsupported(http::Version::HTTP_2, &http::Method::GET));
        assert!(!http2_connect_unsupported(http::Version::HTTP_11, &http::Method::CONNECT));
    }

    #[test]
    fn http2_expect_continue_is_rejected_instead_of_hanging() {
        use super::http2_expect_continue_unsupported;
        let mut headers = HeaderMap::new();
        headers.insert(header::EXPECT, HeaderValue::from_static("100-continue"));
        assert!(http2_expect_continue_unsupported(http::Version::HTTP_2, &headers));
        assert!(!http2_expect_continue_unsupported(http::Version::HTTP_11, &headers));
        headers.remove(header::EXPECT);
        assert!(!http2_expect_continue_unsupported(http::Version::HTTP_2, &headers));
    }

}


#[cfg(test)]
mod flow_io_tests {
    use super::CountingIo;
    use crate::flow::FlowAnalyzer;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn counts_bytes_read_from_and_written_to_client() {
        let (server, mut client) = tokio::io::duplex(64);
        let analyzer = FlowAnalyzer::default();
        let mut counted = CountingIo::new(server, analyzer.clone());

        client.write_all(b"request").await.unwrap();
        let mut request = [0u8; 7];
        counted.read_exact(&mut request).await.unwrap();
        assert_eq!(&request, b"request");

        counted.write_all(b"response").await.unwrap();
        let mut response = [0u8; 8];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"response");

        let stats = analyzer.statistics();
        assert_eq!(stats.total_read, 7);
        assert_eq!(stats.total_write, 8);
    }
}
