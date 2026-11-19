use crate::{config::AppConfig, http_route::{route_request, RouteDecision}};
use bytes::Bytes;
use http::{header, HeaderValue, Method, Response, StatusCode};
use http_body_util::Full;
use hyper::{body::Incoming, server::conn::http1, service::service_fn, Request};
use hyper_util::rt::TokioIo;
use std::{convert::Infallible, sync::Arc};
use tokio::io::{AsyncRead, AsyncWrite};

type ResponseBody = Full<Bytes>;

/// Serve one HTTP/1 connection over a plain TCP or decrypted TLS stream.
/// Already-consumed bytes can be restored by wrapping the stream in PrefixedIo.
pub async fn serve<S>(io: S, scheme: &'static str, config: Arc<AppConfig>) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let service = service_fn(move |request: Request<Incoming>| {
        let config = Arc::clone(&config);
        async move { Ok::<_, Infallible>(handle_request(request, scheme, &config)) }
    });
    http1::Builder::new().keep_alive(false)
        .serve_connection(TokioIo::new(io), service).await?;
    Ok(())
}

fn handle_request(request: Request<Incoming>, scheme: &str, config: &AppConfig) -> Response<ResponseBody> {
    let method = request.method().clone();
    let target = request.uri().to_string();
    let host = request.headers().get(header::HOST).and_then(|value| value.to_str().ok());
    if request.version() == http::Version::HTTP_11 && host.is_none() {
        return empty_response(StatusCode::BAD_REQUEST);
    }
    match route_request(method.as_str(), &target, host, scheme, config) {
        Ok(RouteDecision::Respond(rule)) => configured_response(&method, rule),
        Ok(RouteDecision::Forward { .. }) => empty_response(StatusCode::NOT_IMPLEMENTED),
        Err(error) => {
            tracing::debug!(%error, method = %method, %target, "rejecting malformed HTTP request");
            empty_response(StatusCode::BAD_REQUEST)
        }
    }
}

fn configured_response(method: &Method, rule: crate::config::ResponseConfig) -> Response<ResponseBody> {
    let status = match StatusCode::from_u16(rule.status_code) {
        Ok(status) if (200..=599).contains(&status.as_u16()) => status,
        _ => return empty_response(StatusCode::INTERNAL_SERVER_ERROR),
    };
    let mut response = Response::new(Full::new(Bytes::new()));
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
    if !suppress_body { *response.body_mut() = Full::new(Bytes::from(body)); }
    response.headers_mut().insert(header::CONNECTION, HeaderValue::from_static("close"));
    response.headers_mut().insert(header::SERVER, HeaderValue::from_static("FastGithub"));
    response
}

fn empty_response(status: StatusCode) -> Response<ResponseBody> {
    let mut response = Response::new(Full::new(Bytes::new()));
    *response.status_mut() = status;
    response.headers_mut().insert(header::CONTENT_LENGTH, HeaderValue::from_static("0"));
    response.headers_mut().insert(header::CONNECTION, HeaderValue::from_static("close"));
    response.headers_mut().insert(header::SERVER, HeaderValue::from_static("FastGithub"));
    response
}
