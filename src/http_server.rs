use crate::{config::AppConfig, http_route::{route_request, RouteDecision}, resolver::DomainResolver};
use bytes::Bytes;
use http::{header, HeaderMap, HeaderName, HeaderValue, Method, Response, StatusCode};
use http_body_util::{BodyExt, Full, combinators::UnsyncBoxBody};
use hyper::{body::Incoming, client::conn::http1 as client_http1, server::conn::http1, service::service_fn, Request};
use hyper_util::rt::TokioIo;
use anyhow::Context;
use std::{convert::Infallible, error::Error, future::Future, io, pin::Pin, sync::Arc, task::{Context as TaskContext, Poll}, time::Duration};
use tokio::{io::{AsyncRead, AsyncWrite}, sync::oneshot, time::Sleep};

type BoxError = Box<dyn Error + Send + Sync>;
trait AsyncIo: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> AsyncIo for T {}
type ResponseBody = UnsyncBoxBody<Bytes, BoxError>;


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

/// Serve one HTTP/1 connection over a plain TCP or decrypted TLS stream.
/// Already-consumed bytes can be restored by wrapping the stream in PrefixedIo.
pub async fn serve<S>(io: S, scheme: &'static str, config: Arc<AppConfig>, resolver: DomainResolver, domains: Vec<String>, tls_proxy: Arc<crate::http_proxy::TlsProxy>) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
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

async fn handle_request(request: Request<Incoming>, scheme: &str, config: &AppConfig, resolver: &DomainResolver, domains: &[String], tls_proxy: &crate::http_proxy::TlsProxy) -> Response<ResponseBody> {
    let method = request.method().clone();
    let target = request.uri().to_string();
    let host = request.headers().get(header::HOST).and_then(|value| value.to_str().ok());
    if request.version() == http::Version::HTTP_11 && host.is_none() {
        return empty_response(StatusCode::BAD_REQUEST);
    }
    match route_request(method.as_str(), &target, host, scheme, config) {
        Ok(RouteDecision::Respond(rule)) => configured_response(&method, rule),
        Ok(RouteDecision::Forward { uri, request_target, domain, original_host }) => {
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
            let forwarding = if is_upgrade_request(&request) {
                forward_upgrade_request(request, uri, request_target, original_host, domain, resolver, domains, config, tls_proxy)
            } else {
                forward_request(request, uri, request_target, original_host, domain, timeout_duration, request_started, resolver, domains, config, tls_proxy)
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
    let host = uri.host_str().context("upstream URI has no host")?;
    let port = uri.port().unwrap_or(if uri.scheme() == "https" { 443 } else { 80 });
    let (stream, upstream_ip) = crate::http_proxy::connect_target(host, port, resolver, domains, config, domain.as_ref()).await?;
    let io: Box<dyn AsyncIo> = if uri.scheme() == "https" {
        Box::new(tls_proxy.connect_upstream(stream, host, upstream_ip, domain.as_ref()).await?)
    } else if uri.scheme() == "http" {
        Box::new(stream)
    } else {
        anyhow::bail!("unsupported upstream URI scheme '{}'", uri.scheme());
    };
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
    parts.uri = format!("{}://{}{}", uri.scheme(), host_authority(&uri), request_target).parse()
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
    strip_hop_by_hop_headers(&mut response_parts.headers);
    response_parts.headers.insert(header::SERVER, HeaderValue::from_static("FastGithub"));
    let boxed_body = response_body.map_err(|error| Box::new(error) as BoxError).boxed_unsync();
    let remaining = body_timeout.map(|duration| duration.saturating_sub(request_started.elapsed()));
    let response_body = TimeoutBody::new(boxed_body, remaining, connection_guard).boxed_unsync();
    Ok(Response::from_parts(response_parts, response_body))
}


/// Detect an HTTP/1.1 protocol upgrade before the ordinary forwarding path
/// strips hop-by-hop headers.
fn is_upgrade_request(request: &Request<Incoming>) -> bool {
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
        Box::new(tls_proxy.connect_upstream(stream, host, upstream_ip, domain.as_ref()).await?)
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
        let response_body = TimeoutBody::new(boxed_body, None, connection_guard).boxed_unsync();
        return Ok(Response::from_parts(response_parts, response_body));
    }

    let accepted_protocols = upgrade_protocols(upstream_response.headers());
    let valid_upgrade = has_connection_token(upstream_response.headers(), "upgrade")
        && accepted_protocols.iter().any(|accepted| {
            requested_protocols.iter().any(|requested| requested.eq_ignore_ascii_case(accepted))
        });
    if !valid_upgrade {
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
    async fn dropping_response_guard_aborts_upstream_driver() {
        let task = tokio::spawn(std::future::pending::<()>());
        let guard = super::UpstreamConnectionGuard(task.abort_handle());
        drop(guard);
        assert!(task.await.unwrap_err().is_cancelled());
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

}
