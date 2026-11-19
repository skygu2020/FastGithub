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
    if fields.next().is_some() || !version.starts_with("HTTP/1.") { bail!("invalid HTTP request line"); }
    if method.eq_ignore_ascii_case("CONNECT") { return Ok(false); }

    let host = match request_target_host(target)? {
        Some(host) => host,
        None => {
            let host_header = lines.find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("host").then_some(value.trim())
            });
            match host_header {
                Some(value) => host_from_authority(value)?,
                None => return Ok(false),
            }
        }
    };
    let Some(response) = config.domain_config(&host).and_then(|domain| domain.response.as_ref()) else {
        return Ok(false);
    };
    if !(100..=999).contains(&response.status_code) { bail!("configured Response.StatusCode must be between 100 and 999"); }
    if response.content_type.contains(['\r', '\n']) { bail!("configured Response.ContentType contains a line break"); }

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
    if let Some(rest) = target.strip_prefix("http://") {
        let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
        return Ok(Some(host_from_authority(authority)?));
    }
    if target.starts_with("https://") || target.starts_with('/') || target == "*" { return Ok(None); }
    Ok(None)
}

fn host_from_authority(authority: &str) -> Result<String> {
    if authority.is_empty() || authority.contains('@') || authority.contains(['/', '\\', '\r', '\n']) {
        bail!("invalid HTTP Host authority");
    }
    if let Some(rest) = authority.strip_prefix('[') {
        let (host, suffix) = rest.split_once(']').context("invalid bracketed IPv6 Host")?;
        if !suffix.is_empty() && (!suffix.starts_with(':') || !suffix[1..].bytes().all(|b| b.is_ascii_digit())) {
            bail!("invalid IPv6 Host port");
        }
        host.parse::<std::net::Ipv6Addr>().context("invalid IPv6 Host address")?;
        return Ok(host.to_owned());
    }
    if authority.matches(':').count() > 1 { bail!("IPv6 Host must use brackets"); }
    let host = match authority.rsplit_once(':') {
        Some((host, port)) if !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) => host,
        Some(_) if authority.contains(':') => bail!("invalid Host port"),
        _ => authority,
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
    use super::{host_from_authority, request_target_host};
    #[test]
    fn extracts_absolute_and_origin_form_hosts() {
        assert_eq!(request_target_host("http://example.com:8080/a?q=1").unwrap().as_deref(), Some("example.com"));
        assert_eq!(request_target_host("/path?q=1").unwrap(), None);
        assert_eq!(request_target_host("https://example.com/path").unwrap(), None);
    }
    #[test]
    fn parses_host_authorities() {
        assert_eq!(host_from_authority("example.com:443").unwrap(), "example.com");
        assert_eq!(host_from_authority("[2001:db8::1]:443").unwrap(), "2001:db8::1");
        assert!(host_from_authority("user@example.com").is_err());
        assert!(host_from_authority("example.com:bad").is_err());
    }
}
