use crate::{dns::DnsClient, persistence::Persistence, resolver::DomainResolver};
use anyhow::{bail, Context, Result};
use std::{net::{IpAddr, SocketAddr}, time::Duration};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{lookup_host, TcpListener, TcpStream}, time::timeout};

const HEADER_LIMIT: usize = 16 * 1024;
const HEADER_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

pub async fn run(config: crate::config::AppConfig, upstreams: Vec<SocketAddr>) -> Result<()> {
    let port = config.fast_github.http_proxy_port;
    let configured_domains = config.fast_github.domain_configs.keys().cloned().collect::<Vec<_>>();
    let resolver = DomainResolver::new(DnsClient::new(upstreams), Persistence::new("dnsendpoints.json")).await?;
    let listener = TcpListener::bind(("127.0.0.1", port)).await
        .with_context(|| format!("binding HTTP proxy 127.0.0.1:{port}"))?;
    tracing::info!(address = %listener.local_addr()?, "HTTP CONNECT proxy listening");
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, peer) = accepted.context("accepting proxy connection")?;
                let resolver = resolver.clone();
                let domains = configured_domains.clone();
                tokio::spawn(async move {
                    if let Err(error) = handle_connection(stream, resolver, &domains, peer).await {
                        tracing::debug!(%peer, %error, "proxy connection ended");
                    }
                });
            }
            signal = tokio::signal::ctrl_c() => {
                signal.context("waiting for Ctrl-C")?;
                tracing::info!("HTTP proxy shutting down");
                break;
            }
        }
    }
    Ok(())
}

async fn handle_connection(mut client: TcpStream, resolver: DomainResolver, domains: &[String], peer: SocketAddr) -> Result<()> {
    let header = match timeout(HEADER_TIMEOUT, read_header(&mut client)).await {
        Ok(Ok(header)) => header,
        Ok(Err(error)) => return Err(error),
        Err(_) => {
            let _ = client.write_all(b"HTTP/1.1 408 Request Timeout\r\nConnection: close\r\n\r\n").await;
            bail!("request header timed out");
        }
    };
    let (method, authority) = match parse_request_line(&header) {
        Ok(value) => value,
        Err(error) => {
            let _ = client.write_all(b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n").await;
            return Err(error);
        }
    };
    if !method.eq_ignore_ascii_case("CONNECT") {
        client.write_all(b"HTTP/1.1 501 Not Implemented\r\nConnection: close\r\nContent-Length: 0\r\n\r\n").await?;
        bail!("only HTTP CONNECT is implemented in this migration stage");
    }
    let (host, port) = parse_authority(authority)?;
    let mut upstream = match connect_target(&host, port, &resolver, domains).await {
        Ok(stream) => stream,
        Err(error) => {
            let _ = client.write_all(b"HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\nContent-Length: 0\r\n\r\n").await;
            return Err(error).with_context(|| format!("connecting to {host}:{port}"));
        }
    };
    client.write_all(b"HTTP/1.1 200 Connection Established\r\nProxy-Agent: FastGithub-Rust\r\n\r\n").await?;
    tracing::debug!(%peer, %host, port, "HTTP CONNECT tunnel established");
    let (up, down) = tokio::io::copy_bidirectional(&mut client, &mut upstream).await?;
    tracing::debug!(%peer, %host, port, client_to_upstream = up, upstream_to_client = down, "HTTP CONNECT tunnel closed");
    Ok(())
}

async fn read_header(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut header = Vec::with_capacity(1024);
    let mut byte = [0u8; 1];
    while header.len() < HEADER_LIMIT {
        if stream.read(&mut byte).await? == 0 { bail!("client closed before completing HTTP headers"); }
        header.push(byte[0]);
        if header.ends_with(b"\r\n\r\n") { return Ok(header); }
    }
    bail!("HTTP request headers exceed {HEADER_LIMIT} bytes")
}

fn parse_request_line(header: &[u8]) -> Result<(&str, &str)> {
    let text = std::str::from_utf8(header).context("HTTP request headers are not valid UTF-8")?;
    let line = text.split("\r\n").next().context("missing HTTP request line")?;
    let mut fields = line.split_ascii_whitespace();
    let method = fields.next().context("missing HTTP method")?;
    let authority = fields.next().context("missing HTTP request target")?;
    let version = fields.next().context("missing HTTP version")?;
    if fields.next().is_some() || !version.starts_with("HTTP/1.") { bail!("invalid HTTP request line"); }
    Ok((method, authority))
}

fn parse_authority(authority: &str) -> Result<(String, u16)> {
    if authority.contains("://") || authority.contains('/') || authority.contains('@') {
        bail!("CONNECT target must be host:port authority");
    }
    let (host, port) = if let Some(rest) = authority.strip_prefix('[') {
        let (host, suffix) = rest.split_once(']').context("invalid bracketed IPv6 authority")?;
        (host, suffix.strip_prefix(':').context("CONNECT target requires a port")?)
    } else {
        authority.rsplit_once(':').context("CONNECT target requires host:port")?
    };
    let port: u16 = port.parse().context("invalid CONNECT port")?;
    if host.is_empty() || port == 0 { bail!("CONNECT host and port must be non-empty"); }
    Ok((host.to_owned(), port))
}

async fn connect_target(host: &str, port: u16, resolver: &DomainResolver, domains: &[String]) -> Result<TcpStream> {
    let addresses: Vec<IpAddr> = if let Ok(ip) = host.parse() {
        vec![ip]
    } else if domains.iter().any(|pattern| crate::config::domain_matches(pattern, host)) {
        resolver.resolve(host, port).await?
    } else {
        lookup_host((host, port)).await.with_context(|| format!("system DNS lookup for {host}"))?
            .map(|endpoint| endpoint.ip()).collect()
    };
    if addresses.is_empty() { bail!("no IP addresses available for {host}"); }
    let mut failures = Vec::new();
    for ip in addresses {
        let endpoint = SocketAddr::new(ip, port);
        match timeout(CONNECT_TIMEOUT, TcpStream::connect(endpoint)).await {
            Ok(Ok(stream)) => { stream.set_nodelay(true)?; return Ok(stream); }
            Ok(Err(error)) => failures.push(format!("{endpoint}: {error}")),
            Err(_) => failures.push(format!("{endpoint}: connection timed out")),
        }
    }
    bail!("all connection attempts failed: {}", failures.join("; "))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_connect_request_line() {
        let header = b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n";
        assert_eq!(parse_request_line(header).unwrap(), ("CONNECT", "example.com:443"));
    }

    #[test]
    fn parses_ipv4_and_domain_authorities() {
        assert_eq!(parse_authority("github.com:443").unwrap(), ("github.com".into(), 443));
        assert_eq!(parse_authority("127.0.0.1:8443").unwrap(), ("127.0.0.1".into(), 8443));
    }

    #[test]
    fn parses_bracketed_ipv6_authority() {
        assert_eq!(parse_authority("[::1]:443").unwrap(), ("::1".into(), 443));
    }

    #[test]
    fn rejects_invalid_connect_authorities() {
        assert!(parse_authority("https://github.com:443").is_err());
        assert!(parse_authority("github.com").is_err());
        assert!(parse_authority("github.com:0").is_err());
        assert!(parse_authority("user@github.com:443").is_err());
    }

    #[test]
    fn rejects_non_http_request_line() {
        assert!(parse_request_line(b"CONNECT github.com:443\r\n\r\n").is_err());
    }
}
