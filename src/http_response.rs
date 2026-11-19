use crate::config::AppConfig;
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

    let host = match request_target_host(target)? {
        Some(host) => host,
        None => match parse_host_header(lines)? {
            Some(value) => host_from_authority(value)?,
            None => return Ok(false),
        },
    };
    let Some(response) = config.domain_config(&host).and_then(|domain| domain.response.as_ref()) else {
        return Ok(false);
    };
    // A 1xx response is provisional and requires a subsequent final response;
    // this short-circuit handler cannot implement that exchange.
    if !(200..=999).contains(&response.status_code) { bail!("configured Response.StatusCode must be between 200 and 999"); }
    if response.content_type.chars().any(|character| character.is_control() && character != '\t') {
        bail!("configured Response.ContentType contains an invalid control character");
    }

    let body = response.content_value.as_deref().unwrap_or("").as_bytes();
    let no_body_status = (100..200).contains(&response.status_code) || matches!(response.status_code, 204 | 304);
    let content_length = if no_body_status { 0 } else { body.len() };
    let headers = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\nServer: FastGithub\r\n\r\n",
        response.status_code, reason_phrase(response.status_code), response.content_type, content_length
    );
    stream.write_all(headers.as_bytes()).await?;
    if !method.eq_ignore_ascii_case("HEAD") && !no_body_status && !body.is_empty() { stream.write_all(body).await?; }
    tracing::debug!(%host, status = response.status_code, "returned configured domain response");
    Ok(true)
}

fn request_target_host(target: &str) -> Result<Option<String>> {
    if target.get(..7).is_some_and(|scheme| scheme.eq_ignore_ascii_case("http://")) {
        let rest = &target[7..];
        let authority = rest.split(|character| character == '/' || character == '?' || character == '#').next().unwrap_or_default();
        return Ok(Some(host_from_authority(authority)?));
    }
    if target.get(..8).is_some_and(|scheme| scheme.eq_ignore_ascii_case("https://")) {
        bail!("HTTPS absolute-form is not valid on the plain HTTP proxy listener");
    }
    if target.starts_with('/') || target == "*" { return Ok(None); }
    bail!("unsupported HTTP request-target form")
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
            let port = port.parse::<u16>().context("invalid IPv6 Host port")?;
            if port == 0 { bail!("HTTP Host port must be non-zero"); }
        }
        host.parse::<std::net::Ipv6Addr>().context("invalid IPv6 Host address")?;
        return Ok(host.to_owned());
    }
    if authority.matches(':').count() > 1 { bail!("IPv6 Host must use brackets"); }
    let host = match authority.rsplit_once(':') {
        Some((host, port)) => {
            let port = port.parse::<u16>().context("invalid Host port")?;
            if port == 0 { bail!("HTTP Host port must be non-zero"); }
            host
        }
        None => authority,
    };
    if host.is_empty() { bail!("empty HTTP Host"); }
    Ok(host.to_owned())
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
    use super::{host_from_authority, parse_host_header, request_target_host};
    #[test]
    fn extracts_absolute_and_origin_form_hosts() {
        assert_eq!(request_target_host("http://example.com:8080/a?q=1").unwrap().as_deref(), Some("example.com"));
        assert_eq!(request_target_host("HTTP://EXAMPLE.COM/a").unwrap().as_deref(), Some("EXAMPLE.COM"));
        assert!(request_target_host("https://example.com/a").is_err());
        assert_eq!(request_target_host("/path?q=1").unwrap(), None);
    }
    #[test]
    fn rejects_duplicate_host_headers() {
        assert!(parse_host_header(["Host: one.example", "host: two.example"].into_iter()).is_err());
        assert_eq!(parse_host_header(["X-Test: yes", "Host: one.example"].into_iter()).unwrap(), Some("one.example"));
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
