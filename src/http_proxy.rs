use crate::{cert::CertificateService, config::AppConfig, dns::DnsClient, persistence::Persistence, resolver::DomainResolver};
use anyhow::{bail, Context, Result};
use openssl::ssl::{NameType, SniError, Ssl, SslAcceptor, SslConnector, SslMethod};
use std::{net::{IpAddr, SocketAddr}, pin::Pin, sync::Arc, time::Duration};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{lookup_host, TcpListener, TcpStream}, time::timeout};
use tokio_openssl::SslStream;

const HEADER_LIMIT: usize = 16 * 1024;
const HEADER_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const SYSTEM_DNS_TIMEOUT: Duration = Duration::from_secs(10);
const BAD_REQUEST_RESPONSE: &[u8] = b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n";

pub async fn run(config: AppConfig, upstreams: Vec<SocketAddr>, certificates: Arc<CertificateService>) -> Result<()> {
    let port = config.fast_github.http_proxy_port;
    let configured_domains = config.fast_github.domain_configs.keys().cloned().collect::<Vec<_>>();
    let resolver = DomainResolver::new(DnsClient::new(upstreams), Persistence::new("dnsendpoints.json")).await?;
    let tls_proxy = Arc::new(TlsProxy::new(certificates)?);
    let config = Arc::new(config);
    let listener = TcpListener::bind(("127.0.0.1", port)).await
        .with_context(|| format!("binding HTTP proxy 127.0.0.1:{port}"))?;
    tracing::info!(address = %listener.local_addr()?, "HTTP CONNECT proxy listening");
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, peer) = accepted.context("accepting proxy connection")?;
                let resolver = resolver.clone();
                let domains = configured_domains.clone();
                let tls_proxy = Arc::clone(&tls_proxy);
                let config = Arc::clone(&config);
                tokio::spawn(async move {
                    if let Err(error) = handle_connection(stream, resolver, &domains, config, tls_proxy, peer).await {
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

async fn handle_connection(mut client: TcpStream, resolver: DomainResolver, domains: &[String], config: Arc<AppConfig>, tls_proxy: Arc<TlsProxy>, peer: SocketAddr) -> Result<()> {
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
        if crate::http_response::try_respond(&mut client, &header, &config).await? {
            return Ok(());
        }
        client.write_all(b"HTTP/1.1 501 Not Implemented\r\nConnection: close\r\nContent-Length: 0\r\n\r\n").await?;
        bail!("plain HTTP forwarding is not implemented; only configured Response rules can short-circuit");
    }
    let (host, port) = match parse_authority(authority) {
        Ok(value) => value,
        Err(error) => {
            let _ = client.write_all(BAD_REQUEST_RESPONSE).await;
            return Err(error);
        }
    };
    let (mut upstream, upstream_ip) = match connect_target(&host, port, &resolver, domains, &config).await {
        Ok(connection) => connection,
        Err(error) => {
            let _ = client.write_all(b"HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\nContent-Length: 0\r\n\r\n").await;
            return Err(error).with_context(|| format!("connecting to {host}:{port}"));
        }
    };
    client.write_all(b"HTTP/1.1 200 Connection Established\r\nProxy-Agent: FastGithub-Rust\r\n\r\n").await?;
    tracing::debug!(%peer, %host, port, "HTTP CONNECT tunnel established");
    let is_tls = match timeout(HEADER_TIMEOUT, sniff_tls(&client)).await {
        Ok(result) => result?,
        Err(_) => bail!("timed out waiting for CONNECT tunnel data"),
    };
    let (up, down) = if is_tls {
        tls_proxy.intercept(client, upstream, &host, upstream_ip, &config).await?
    } else {
        tokio::io::copy_bidirectional(&mut client, &mut upstream).await?
    };
    tracing::debug!(%peer, %host, port, client_to_upstream = up, upstream_to_client = down, tls_intercepted = is_tls, "HTTP CONNECT tunnel closed");
    Ok(())
}

async fn sniff_tls(client: &TcpStream) -> Result<bool> {
    let mut prefix = [0u8; 3];
    loop {
        let count = client.peek(&mut prefix).await?;
        if count == 0 {
            bail!("client closed before sending CONNECT tunnel data");
        }
        if let Some(is_tls) = classify_tls_prefix(&prefix[..count]) {
            return Ok(is_tls);
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

fn classify_tls_prefix(prefix: &[u8]) -> Option<bool> {
    match prefix {
        [] => None,
        [content_type, ..] if *content_type != 22 => Some(false),
        [_content_type] => None,
        [_content_type, version, ..] if *version != 3 => Some(false),
        [_, _, _] => Some(true),
        _ => None,
    }
}

struct TlsProxy {
    acceptor: SslAcceptor,
    connector: SslConnector,
    certificates: Arc<CertificateService>,
}

impl TlsProxy {
    fn new(certificates: Arc<CertificateService>) -> Result<Self> {
        let mut acceptor = SslAcceptor::mozilla_intermediate(SslMethod::tls())
            .context("creating inbound TLS acceptor")?;
        let callback_certificates = Arc::clone(&certificates);
        acceptor.set_servername_callback(move |ssl, _alert| {
            if let Some(host) = ssl.servername(NameType::HOST_NAME) {
                let certificate = callback_certificates.server_certificate(host)
                    .map_err(|_| SniError::ALERT_FATAL)?;
                ssl.set_certificate(&certificate.certificate).map_err(|_| SniError::ALERT_FATAL)?;
                ssl.set_private_key(&certificate.private_key).map_err(|_| SniError::ALERT_FATAL)?;
            }
            // No SNI: keep the CONNECT-authority certificate installed on this session.
            Ok(())
        });
        let connector = SslConnector::builder(SslMethod::tls())
            .context("creating upstream TLS connector")?
            .build();
        Ok(Self { acceptor: acceptor.build(), connector, certificates })
    }

    async fn intercept(&self, client: TcpStream, upstream: TcpStream, connect_host: &str, upstream_ip: IpAddr, config: &AppConfig) -> Result<(u64, u64)> {
        let default_certificate = self.certificates.server_certificate(connect_host)?;
        let mut ssl = Ssl::new(self.acceptor.context()).context("creating inbound TLS session")?;
        ssl.set_certificate(&default_certificate.certificate)?;
        ssl.set_private_key(&default_certificate.private_key)?;
        let mut client_tls = SslStream::new(ssl, client).context("wrapping inbound TLS stream")?;
        timeout(CONNECT_TIMEOUT, Pin::new(&mut client_tls).accept())
            .await.context("inbound TLS handshake timed out")??;
        let requested_host = client_tls.ssl().servername(NameType::HOST_NAME).unwrap_or(connect_host).to_owned();

        let domain_config = config.domain_config(&requested_host);
        let configured_sni = domain_config.and_then(|value| value.tls_sni_value(&requested_host, upstream_ip));
        // Preserve normal hostname SNI even when there is no matching domain rule.
        // Only an explicit TlsSni=false setting suppresses the SNI extension.
        let disable_sni = domain_config.is_some_and(|value| value.tls_sni == Some(false));
        let ignore_name_mismatch = domain_config.is_some_and(|value| value.tls_ignore_name_mismatch == Some(true));
        let mut connect_config = self.connector.configure()?;
        connect_config.set_use_server_name_indication(!disable_sni);
        // Keep certificate-chain validation enabled; this option suppresses only
        // the hostname check for servers that return a certificate for another name.
        connect_config.set_verify_hostname(!ignore_name_mismatch);
        let mut upstream_ssl = connect_config.into_ssl(&requested_host)?;
        if let Some(sni) = configured_sni.as_deref() {
            upstream_ssl.set_hostname(sni)?;
        }
        let mut upstream_tls = SslStream::new(upstream_ssl, upstream).context("wrapping upstream TLS stream")?;
        timeout(CONNECT_TIMEOUT, Pin::new(&mut upstream_tls).connect())
            .await.context("upstream TLS handshake timed out")??;
        let (sent, received) = tokio::io::copy_bidirectional(&mut client_tls, &mut upstream_tls).await?;
        Ok((sent, received))
    }
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
    if fields.next().is_some() || !matches!(version, "HTTP/1.0" | "HTTP/1.1") { bail!("invalid HTTP request line"); }
    Ok((method, authority))
}

fn parse_authority(authority: &str) -> Result<(String, u16)> {
    if authority.is_empty()
        || authority.bytes().any(|byte| byte <= 0x20 || byte >= 0x7f)
        || authority.contains("://")
        || authority.chars().any(|character| matches!(character, '/' | '\\' | '@' | '?' | '#')) {
        bail!("CONNECT target must be a valid host:port authority");
    }

    let (host, port) = if let Some(rest) = authority.strip_prefix('[') {
        let (host, suffix) = rest.split_once(']').context("invalid bracketed IPv6 authority")?;
        host.parse::<std::net::Ipv6Addr>().context("invalid CONNECT IPv6 address")?;
        let port = suffix.strip_prefix(':').context("CONNECT target requires a port")?;
        if port.contains(':') || port.is_empty() {
            bail!("invalid CONNECT port");
        }
        (host, port)
    } else {
        let (host, port) = authority.rsplit_once(':')
            .context("CONNECT target requires host:port")?;
        if host.contains(':') {
            bail!("IPv6 CONNECT targets must use brackets");
        }
        if host.is_empty() || !host.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-')
        }) {
            bail!("invalid CONNECT hostname");
        }
        (host, port)
    };

    if port.is_empty() || !port.bytes().all(|byte| byte.is_ascii_digit()) {
        bail!("invalid CONNECT port");
    }
    let port: u16 = port.parse().context("CONNECT port is outside the valid range")?;
    if port == 0 { bail!("CONNECT port must be non-zero"); }
    Ok((host.to_owned(), port))
}

async fn connect_target(
    host: &str,
    port: u16,
    resolver: &DomainResolver,
    domains: &[String],
    config: &AppConfig,
) -> Result<(TcpStream, IpAddr)> {
    let domain_config = config.domain_config(host);

    // FastGithub's DomainConfig.Timeout is an HTTP request-lifecycle timeout,
    // not a TCP dial timeout. Keep connection establishment on its fixed limit;
    // request-scoped cancellation will be applied by the HTTP forwarding layer.
    let connect_timeout = CONNECT_TIMEOUT;
    let mut addresses = Vec::new();
    if let Ok(ip) = host.parse::<IpAddr>() {
        addresses.push(ip);
    } else if domains.iter().any(|pattern| crate::config::domain_matches(pattern, host)) {
        // Match the .NET implementation: a configured IP is the first candidate,
        // not an exclusive override; DNS candidates remain available as fallback.
        if let Some(configured) = domain_config.and_then(|value| value.ip_address.as_deref()) {
            let ip = configured.parse::<IpAddr>()
                .with_context(|| format!("invalid IPAddress '{configured}' for {host}"))?;
            addresses.push(ip);
        }
        match resolver.resolve(host, port).await {
            Ok(resolved) => addresses.extend(resolved),
            Err(error) if !addresses.is_empty() => {
                tracing::warn!(%host, %error, "configured IP is available; continuing without DNS candidates");
            }
            Err(error) => return Err(error).with_context(|| format!("resolving configured domain {host}")),
        }
    } else {
        addresses.extend(timeout(SYSTEM_DNS_TIMEOUT, lookup_host((host, port))).await
            .with_context(|| format!("system DNS lookup for {host} timed out"))?
            .with_context(|| format!("system DNS lookup for {host}"))?
            .map(|endpoint| endpoint.ip()));
    }

    let mut unique_addresses = Vec::with_capacity(addresses.len());
    for ip in addresses {
        if !unique_addresses.contains(&ip) { unique_addresses.push(ip); }
    }
    if unique_addresses.is_empty() { bail!("no IP addresses available for {host}"); }

    let mut failures = Vec::new();
    for ip in unique_addresses {
        let endpoint = SocketAddr::new(ip, port);
        match timeout(connect_timeout, TcpStream::connect(endpoint)).await {
            Ok(Ok(stream)) => { stream.set_nodelay(true)?; return Ok((stream, ip)); }
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
    fn classifies_fragmented_tls_record_prefixes() {
        assert_eq!(classify_tls_prefix(&[]), None);
        assert_eq!(classify_tls_prefix(&[22]), None);
        assert_eq!(classify_tls_prefix(&[22, 3]), None);
        assert_eq!(classify_tls_prefix(&[22, 3, 1]), Some(true));
        assert_eq!(classify_tls_prefix(&[22, 4]), Some(false));
        assert_eq!(classify_tls_prefix(&[23]), Some(false));
    }

    #[test]
    fn bad_request_response_uses_http_crlf_delimiters() {
        assert_eq!(BAD_REQUEST_RESPONSE, b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n");
    }

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
        assert!(parse_authority("2001:db8::1:443").is_err());
        assert!(parse_authority("[not-ipv6]:443").is_err());
        assert!(parse_authority("[::1]443").is_err());
        assert!(parse_authority("[::1]:443:9").is_err());
        assert!(parse_authority("bad_host:443").is_err());
        assert!(parse_authority("github.com: 443").is_err());
        assert!(parse_authority("github.com:+443").is_err());
        assert!(parse_authority("github.com:65536").is_err());
    }

    #[test]
    fn rejects_non_http_request_line() {
        assert!(parse_request_line(b"CONNECT github.com:443\r\n\r\n").is_err());
        assert!(parse_request_line(b"CONNECT github.com:443 HTTP/1.x\r\n\r\n").is_err());
    }
}
