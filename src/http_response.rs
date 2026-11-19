use crate::{config::AppConfig, http_route::{route_request, RouteDecision}};
use anyhow::{bail, Context, Result};
use tokio::{io::AsyncWriteExt, net::TcpStream};

/// Short-circuit a matching plain HTTP request with its configured Response.
pub async fn try_respond(stream: &mut TcpStream, header: &[u8], config: &AppConfig) -> Result<bool> {
    let text = std::str::from_utf8(header).context("HTTP request headers are not valid UTF-8")?;
    let mut lines = text.split("\r\n");
    let request_line = lines.next().context("missing HTTP request line")?;
    let mut fields = request_line.split_ascii_whitespace();
    let method = fields.next().context("missing HTTP method")?;
    let target = fields.next().context("missing HTTP request target")?;
    let version = fields.next().context("missing HTTP version")?;
    if fields.next().is_some() || !matches!(version, "HTTP/1.0" | "HTTP/1.1") { bail!("invalid HTTP request line"); }
    if method.eq_ignore_ascii_case("CONNECT") { return Ok(false); }

    let host_header = parse_host_header(lines)?;
    // HTTP/1.1 requires exactly one Host field even when the request-target
    // uses absolute-form. The route layer separately verifies that both
    // authorities identify the same virtual host.
    if version == "HTTP/1.1" && host_header.is_none() {
        bail!("HTTP/1.1 request is missing its Host header");
    }
    let response = match route_request(method, target, host_header, "http", config)? {
        RouteDecision::Respond(response) => response,
        RouteDecision::Forward { .. } => return Ok(false),
    };
    let host = if target.get(..7).is_some_and(|scheme| scheme.eq_ignore_ascii_case("http://")) {
        url::Url::parse(target)?.host_str().unwrap_or_default().to_owned()
    } else {
        host_header.map(host_from_authority).transpose()?.unwrap_or_default()
    };
    validate_response_status(response.status_code)?;
    if response.content_type.chars().any(|character| character.is_control() && character != '\t') {
        bail!("configured Response.ContentType contains an invalid control character");
    }

    let body = response.content_value.as_deref().unwrap_or("").as_bytes();
    let no_body_status = matches!(response.status_code, 204 | 205 | 304);
    let content_length_header = match response.status_code {
        // 204 and 304 have special framing rules; do not claim a zero-length
        // selected representation for either response.
        204 | 304 => String::new(),
        205 => "Content-Length: 0\r\n".to_owned(),
        _ => format!("Content-Length: {}\r\n", body.len()),
    };
    let headers = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\n{}Connection: close\r\nServer: FastGithub\r\n\r\n",
        response.status_code, reason_phrase(response.status_code), response.content_type, content_length_header
    );
    stream.write_all(headers.as_bytes()).await?;
    if !method.eq_ignore_ascii_case("HEAD") && !no_body_status && !body.is_empty() { stream.write_all(body).await?; }
    tracing::debug!(%host, status = response.status_code, "returned configured domain response");
    Ok(true)
}


/// Validate a configured final HTTP status code. Informational responses require
/// a second response, and values above 599 are outside the HTTP status-code range.
fn validate_response_status(status: u16) -> Result<()> {
    if !(200..=599).contains(&status) {
        bail!("configured Response.StatusCode must be between 200 and 599");
    }
    Ok(())
}


fn parse_host_header<'a>(lines: impl Iterator<Item = &'a str>) -> Result<Option<&'a str>> {
    let mut host = None;
    for line in lines {
        if line.is_empty() { break; }
        let (name, value) = line.split_once(':').context("malformed HTTP header field")?;
        if name.eq_ignore_ascii_case("host") {
            if host.is_some() { bail!("duplicate Host header"); }
            host = Some(value.trim());
        }
    }
    Ok(host)
}

fn host_from_authority(authority: &str) -> Result<String> {
    if authority.is_empty() || authority.bytes().any(|byte| byte <= 0x20 || byte >= 0x7f)
        || authority.contains('@') || authority.chars().any(|character| matches!(character, '/' | '\\' | '\r' | '\n')) {
        bail!("invalid HTTP Host authority");
    }
    if let Some(rest) = authority.strip_prefix('[') {
        let (host, suffix) = rest.split_once(']').context("invalid bracketed IPv6 Host")?;
        if !suffix.is_empty() {
            let port = suffix.strip_prefix(':').context("invalid IPv6 Host port")?;
            if port.is_empty() || !port.bytes().all(|byte| byte.is_ascii_digit()) {
                bail!("invalid IPv6 Host port");
            }
            let port = port.parse::<u16>().context("invalid IPv6 Host port")?;
            if port == 0 { bail!("HTTP Host port must be non-zero"); }
        }
        host.parse::<std::net::Ipv6Addr>().context("invalid IPv6 Host address")?;
        return Ok(host.to_owned());
    }
    if authority.matches(':').count() > 1 { bail!("IPv6 Host must use brackets"); }
    let host = match authority.rsplit_once(':') {
        Some((host, port)) => {
            if port.is_empty() || !port.bytes().all(|byte| byte.is_ascii_digit()) {
                bail!("invalid Host port");
            }
            let port = port.parse::<u16>().context("invalid Host port")?;
            if port == 0 { bail!("HTTP Host port must be non-zero"); }
            host
        }
        None => authority,
    };
    validate_http_hostname(host)?;
    Ok(host.to_owned())
}


/// Validate the ASCII DNS hostname syntax used by HTTP Host authorities.
/// Internationalized names must be supplied in their ASCII/Punycode form.
fn validate_http_hostname(host: &str) -> Result<()> {
    if host.is_empty() || host.len() > 253 {
        bail!("invalid HTTP hostname length");
    }
    let hostname = host.strip_suffix('.').unwrap_or(host);
    if hostname.is_empty() {
        bail!("invalid HTTP hostname");
    }
    for label in hostname.split('.') {
        if label.is_empty() || label.len() > 63
            || label.starts_with('-') || label.ends_with('-')
            || !label.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-') {
            bail!("invalid HTTP hostname label");
        }
    }
    Ok(())
}


fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK", 201 => "Created", 202 => "Accepted", 204 => "No Content",
        301 => "Moved Permanently", 302 => "Found", 304 => "Not Modified",
        400 => "Bad Request", 401 => "Unauthorized", 403 => "Forbidden", 404 => "Not Found",
        405 => "Method Not Allowed", 408 => "Request Timeout", 429 => "Too Many Requests",
        500 => "Internal Server Error", 501 => "Not Implemented", 502 => "Bad Gateway",
        503 => "Service Unavailable", _ => "Configured Response",
    }
}

#[cfg(test)]
mod tests {
    use super::{host_from_authority, parse_host_header, validate_response_status};
    #[test]
    fn accepts_only_final_http_status_code_range() {
        assert!(validate_response_status(200).is_ok());
        assert!(validate_response_status(599).is_ok());
        assert!(validate_response_status(100).is_err());
        assert!(validate_response_status(199).is_err());
        assert!(validate_response_status(600).is_err());
        assert!(validate_response_status(999).is_err());
    }

    #[test]
    fn rejects_duplicate_host_headers() {
        assert!(parse_host_header(["Host: one.example", "host: two.example"].into_iter()).is_err());
        assert_eq!(parse_host_header(["X-Test: yes", "Host: one.example"].into_iter()).unwrap(), Some("one.example"));
    }

    #[test]
    fn rejects_malformed_http_host_names_and_ports() {
        for authority in [
            "", ".example.com", "example..com", "-bad.example", "bad-.example",
            "bad_host.example", "example.com:+443", "example.com:abc",
            "example.com:65536", "example.com:0", "[::1]:+443",
        ] {
            assert!(host_from_authority(authority).is_err(), "accepted {authority}");
        }
        assert_eq!(host_from_authority("example.com.:443").unwrap(), "example.com.");
        assert_eq!(host_from_authority("xn--bcher-kva.example").unwrap(), "xn--bcher-kva.example");
    }

    #[test]
    fn parses_host_authorities() {
        assert_eq!(host_from_authority("example.com:443").unwrap(), "example.com");
        assert_eq!(host_from_authority("[2001:db8::1]:443").unwrap(), "2001:db8::1");
        assert!(host_from_authority("user@example.com").is_err());
        assert!(host_from_authority("example.com:bad").is_err());
        assert!(host_from_authority("example.com extra").is_err());
        assert!(host_from_authority("example.com:99999").is_err());
        assert!(host_from_authority("example.com:0").is_err());
        assert!(host_from_authority("example.com:").is_err());
    }
}
