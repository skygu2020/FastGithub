use crate::{config::AppConfig, http_route::{route_request, RouteDecision}, resolver::DomainResolver};
use bytes::Bytes;
use http::{header, HeaderMap, HeaderName, HeaderValue, Method, Response, StatusCode};
use http_body_util::{BodyExt, Full, combinators::UnsyncBoxBody};
use hyper::{body::Incoming, client::conn::http1 as client_http1, server::conn::http1, service::service_fn, Request};
use hyper_util::rt::TokioIo;
use anyhow::Context;
use std::{convert::Infallible, error::Error, future::Future, io, pin::Pin, sync::Arc, task::{Context as TaskContext, Poll}, time::Duration};
use tokio::{io::{AsyncRead, AsyncWrite}, time::Sleep};

type BoxError = Box<dyn Error + Send + Sync>;
trait AsyncIo: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> AsyncIo for T {}
type ResponseBody = UnsyncBoxBody<Bytes, BoxError>;


/// Enforce the configured timeout while the upstream response body is streamed.
/// The forwarding future has its own timeout for DNS, connect, TLS and response headers.
struct TimeoutBody<B> {
    inner: Pin<Box<B>>,
    sleep: Pin<Box<Sleep>>,
}

impl<B> TimeoutBody<B> {
    fn new(inner: B, duration: Duration) -> Self {
        Self { inner: Box::pin(inner), sleep: Box::pin(tokio::time::sleep(duration)) }
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
        if this.sleep.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Some(Err(Box::new(io::Error::new(
                io::ErrorKind::TimedOut,
                "configured HTTP response-body timeout elapsed",
            )))));
        }
        this.inner.as_mut().poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.as_ref().get_ref().is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.as_ref().get_ref().size_hint()
    }
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
            let forwarding = forward_request(request, uri, request_target, original_host, domain, timeout_duration, resolver, domains, config, tls_proxy);
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
    resolver: &DomainResolver,
    domains: &[String],
    config: &AppConfig,
    tls_proxy: &crate::http_proxy::TlsProxy,
) -> anyhow::Result<Response<ResponseBody>> {
    let host = uri.host_str().context("upstream URI has no host")?;
    let port = uri.port().unwrap_or(if uri.scheme() == "https" { 443 } else { 80 });
    let (stream, upstream_ip) = crate::http_proxy::connect_target(host, port, resolver, domain.as_ref().or_else(|| config.domain_config(&original_host))).await?;
    let io: Box<dyn AsyncIo> = if uri.scheme() == "https" {
        Box::new(tls_proxy.connect_upstream(stream, host, &original_host, upstream_ip, domain.as_ref()).await?)
    } else if uri.scheme() == "http" {
        Box::new(stream)
    } else {
        anyhow::bail!("unsupported upstream URI scheme '{}'", uri.scheme());
    };
    let (mut sender, connection) = client_http1::handshake(TokioIo::new(io)).await?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::debug!(%error, "upstream HTTP connection ended");
        }
    });

    let (mut parts, body) = request.into_parts();
    parts.uri = format!("{}://{}{}", uri.scheme(), host_authority(&uri), request_target).parse()
        .context("building upstream request URI")?;
    strip_hop_by_hop_headers(&mut parts.headers);
    parts.headers.insert(header::HOST, HeaderValue::from_str(&host_authority(&uri))?);
    tracing::debug!(%original_host, upstream = %uri, "forwarding HTTP request");
    let upstream_request = Request::from_parts(parts, body);
    let upstream_response = sender.send_request(upstream_request).await?;
    let (mut response_parts, response_body) = upstream_response.into_parts();
    strip_hop_by_hop_headers(&mut response_parts.headers);
    response_parts.headers.insert(header::SERVER, HeaderValue::from_static("FastGithub"));
    let boxed_body = response_body.map_err(|error| Box::new(error) as BoxError).boxed_unsync();
    let response_body = match body_timeout {
        Some(duration) => TimeoutBody::new(boxed_body, duration).boxed_unsync(),
        None => boxed_body,
    };
    Ok(Response::from_parts(response_parts, response_body))
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
        HeaderName::from_static("trailer"),
        HeaderName::from_static("upgrade"),
    ] {
        headers.remove(name);
    }
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
        headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plain"));

        strip_hop_by_hop_headers(&mut headers);

        for name in [
            "connection", "x-private", "keep-alive", "proxy-connection",
            "proxy-authenticate", "proxy-authorization", "te", "trailer", "upgrade",
        ] {
            assert!(!headers.contains_key(name), "header {name} was retained");
        }
        assert_eq!(headers.get(header::CONTENT_TYPE).unwrap(), "text/plain");
    }
}
