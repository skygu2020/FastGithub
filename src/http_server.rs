use crate::{config::AppConfig, http_route::{route_request, RouteDecision}, resolver::DomainResolver};
use bytes::Bytes;
use http::{header, HeaderValue, Method, Response, StatusCode};
use http_body_util::{BodyExt, Full, combinators::UnsyncBoxBody};
use hyper::{body::Incoming, client::conn::http1 as client_http1, server::conn::http1, service::service_fn, Request};
use hyper_util::rt::TokioIo;
use std::{convert::Infallible, error::Error, sync::Arc, time::Duration};
use tokio::io::{AsyncRead, AsyncWrite};

type BoxError = Box<dyn Error + Send + Sync>;
type ResponseBody = UnsyncBoxBody<Bytes, BoxError>;

/// Serve one HTTP/1 connection over a plain TCP or decrypted TLS stream.
/// Already-consumed bytes can be restored by wrapping the stream in PrefixedIo.
pub async fn serve<S>(io: S, scheme: &'static str, config: Arc<AppConfig>, resolver: DomainResolver, domains: Vec<String>) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let service = service_fn(move |request: Request<Incoming>| {
        let config = Arc::clone(&config);
        let resolver = resolver.clone();
        let domains = domains.clone();
        async move { Ok::<_, Infallible>(handle_request(request, scheme, &config, &resolver, &domains).await) }
    });
    http1::Builder::new().keep_alive(false)
        .serve_connection(TokioIo::new(io), service).await?;
    Ok(())
}

async fn handle_request(request: Request<Incoming>, scheme: &str, config: &AppConfig, resolver: &DomainResolver, domains: &[String]) -> Response<ResponseBody> {
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
            let forwarding = forward_request(request, uri, request_target, original_host, resolver, domains, config);
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
    resolver: &DomainResolver,
    domains: &[String],
    config: &AppConfig,
) -> anyhow::Result<Response<ResponseBody>> {
    if uri.scheme() != "http" {
        anyhow::bail!("HTTPS upstream forwarding is not connected yet");
    }
    let host = uri.host_str().context("upstream URI has no host")?;
    let port = uri.port().unwrap_or(80);
    let (stream, _) = crate::http_proxy::connect_target(host, port, resolver, domains, config).await?;
    let io = TokioIo::new(stream);
    let (mut sender, connection) = client_http1::handshake(io).await?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::debug!(%error, "upstream HTTP connection ended");
        }
    });

    let (mut parts, body) = request.into_parts();
    parts.uri = format!("http://{}:{}{}", host_authority(&uri), port, request_target).parse()
        .context("building upstream request URI")?;
    parts.headers.remove(header::CONNECTION);
    parts.headers.remove("proxy-connection");
    parts.headers.remove("keep-alive");
    parts.headers.remove(header::UPGRADE);
    parts.headers.insert(header::HOST, HeaderValue::from_str(&host_authority(&uri))?);
    let upstream_request = Request::from_parts(parts, body);
    let upstream_response = sender.send_request(upstream_request).await?;
    let (mut response_parts, response_body) = upstream_response.into_parts();
    response_parts.headers.remove(header::CONNECTION);
    response_parts.headers.remove("proxy-connection");
    response_parts.headers.remove("keep-alive");
    response_parts.headers.remove(header::UPGRADE);
    response_parts.headers.insert(header::SERVER, HeaderValue::from_static("FastGithub"));
    let boxed_body = response_body.map_err(|error| Box::new(error) as BoxError).boxed_unsync();
    Ok(Response::from_parts(response_parts, boxed_body))
}

fn host_authority(uri: &url::Url) -> String {
    match uri.port() {
        Some(port) => format!("{}:{}", uri.host_str().unwrap_or_default(), port),
        None => uri.host_str().unwrap_or_default().to_owned(),
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
