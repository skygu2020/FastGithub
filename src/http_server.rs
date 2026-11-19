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
struct ExpectContinueBody {
    inner: Pin<Box<Incoming>>,
    continue_rx: Option<oneshot::Receiver<()>>,
    released: bool,
}

impl ExpectContinueBody {
    fn new(inner: Incoming, continue_rx: Option<oneshot::Receiver<()>>) -> Self {
        Self {
            inner: Box::pin(inner),
            released: continue_rx.is_none(),
            continue_rx,
        }
    }
}

impl http_body::Body for ExpectContinueBody {
    type Data = Bytes;
    type Error = hyper::Error;

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
    http1::Builder::new().keep_alive(false)
        .serve_connection(TokioIo::new(io), service).await?;
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
            let forwarding = forward_request(request, uri, request_target, original_host, domain, timeout_duration, request_started, resolver, domains, config, tls_proxy);
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

    let expect_continue = expects_continue(&parts.headers);
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
    response.headers_mut().insert(header::CONNECTION, HeaderValue::from_static("close"));
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
    response.headers_mut().insert(header::CONNECTION, HeaderValue::from_static("close"));
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
}
