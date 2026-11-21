use crate::{cert::CertificateService, config::AppConfig, dns::DnsClient, persistence::Persistence, resolver::DomainResolver};
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use http::{Request, Response, Uri};
use http_body_util::combinators::UnsyncBoxBody;
use hyper::body::Incoming;
use hyper_util::{
    client::legacy::{connect::{Connected, Connection}, Client},
    rt::{TokioExecutor, TokioIo, TokioTimer},
};
use openssl::ssl::{AlpnError, NameType, SniError, Ssl, SslAcceptor, SslConnector, SslMethod};
use std::{
    error::Error,
    future::Future,
    io,
    net::{IpAddr, SocketAddr},
    pin::Pin,
    sync::Arc,
    task::{Context as TaskContext, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    net::{lookup_host, TcpListener, TcpStream},
    time::timeout,
};
use tower_service::Service;
use tokio_openssl::SslStream;

const HEADER_LIMIT: usize = 16 * 1024;
const HEADER_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

type UpstreamError = Box<dyn Error + Send + Sync>;
pub(crate) type UpstreamBody = UnsyncBoxBody<Bytes, UpstreamError>;
type UpstreamClient = Client<UpstreamConnector, UpstreamBody>;

trait ProxyIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> ProxyIo for T {}

struct ConnectedUpstreamIo {
    inner: Box<dyn ProxyIo>,
    http2: bool,
}

impl AsyncRead for ConnectedUpstreamIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.get_mut().inner).poll_read(cx, buffer)
    }
}

impl AsyncWrite for ConnectedUpstreamIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut *self.get_mut().inner).poll_write(cx, bytes)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.get_mut().inner).poll_shutdown(cx)
    }
}

impl Connection for ConnectedUpstreamIo {
    fn connected(&self) -> Connected {
        let connected = Connected::new();
        if self.http2 { connected.negotiated_h2() } else { connected }
    }
}

#[derive(Clone)]
struct UpstreamConnector {
    resolver: DomainResolver,
    domains: Arc<Vec<String>>,
    config: Arc<AppConfig>,
    tls_connector: Arc<SslConnector>,
}

impl UpstreamConnector {
    async fn connect(&self, uri: Uri) -> Result<TokioIo<ConnectedUpstreamIo>> {
        let host = uri.host().context("upstream URI has no hostname")?;
        let scheme = uri.scheme_str().unwrap_or("http");
        let port = uri.port_u16().unwrap_or(if scheme == "https" { 443 } else { 80 });
        let (stream, ip) = connect_target(
            host,
            port,
            &self.resolver,
            &self.domains,
            &self.config,
            None,
        ).await?;

        let (io, http2): (Box<dyn ProxyIo>, bool) = match scheme {
            "http" => (Box::new(stream), false),
            "https" => {
                let rule = self.config.domain_config(host);
                match connect_upstream_tls(&self.tls_connector, stream, host, ip, rule, true).await {
                    Ok((tls, http2)) => (Box::new(tls), http2),
                    Err(error) => {
                        self.resolver.report_failure(ip).await;
                        return Err(error);
                    }
                }
            }
            other => bail!("unsupported upstream URI scheme '{other}'"),
        };

        Ok(TokioIo::new(ConnectedUpstreamIo { inner: io, http2 }))
    }
}

impl Service<Uri> for UpstreamConnector {
    type Response = TokioIo<ConnectedUpstreamIo>;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let connector = self.clone();
        Box::pin(async move {
            connector.connect(uri).await.map_err(|error| io::Error::other(error.to_string()))
        })
    }
}


/// Only an explicit h2 ALPN selection enables HTTP/2. No ALPN extension,
/// no overlap, and an explicit http/1.1 selection all use the HTTP/1 path.
pub(crate) fn negotiated_http2(selected_alpn: Option<&[u8]>) -> bool {
    selected_alpn == Some(&b"h2"[..])
}

const SYSTEM_DNS_TIMEOUT: Duration = Duration::from_secs(10);
const BAD_REQUEST_RESPONSE: &[u8] = b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n";

pub async fn run(config: AppConfig, upstreams: Vec<SocketAddr>, certificates: Arc<CertificateService>) -> Result<()> {
    let port = config.fast_github.http_proxy_port;
    let configured_domains = config.fast_github.domain_configs.keys().cloned().collect::<Vec<_>>();
    let resolver = DomainResolver::new(DnsClient::new(upstreams), Persistence::new("dnsendpoints.json")).await?;
    let refresh_resolver = resolver.clone();
    let refresh_task = tokio::spawn(async move {
        // Refresh hosts once at startup, then periodically refresh all known endpoints.
        match refresh_resolver.refresh_all().await {
            Ok(hosts) => tracing::info!(
                success = hosts.success,
                domains = hosts.domain_count,
                errors = %hosts.errors.join("; "),
                "initial hosts and DNS/IP refresh completed"
            ),
            Err(error) => tracing::warn!(%error, "initial hosts and DNS/IP refresh failed"),
        }

        let mut interval = tokio::time::interval(Duration::from_secs(30 * 60));
        interval.tick().await;
        loop {
            interval.tick().await;
            match refresh_resolver.refresh_all().await {
                Ok(hosts) => tracing::info!(
                    success = hosts.success,
                    domains = hosts.domain_count,
                    errors = %hosts.errors.join("; "),
                    "periodic hosts and DNS/IP refresh completed"
                ),
                Err(error) => tracing::warn!(%error, "periodic hosts and DNS/IP refresh failed"),
            }
        }
    });
    let config = Arc::new(config);
    let tls_proxy = Arc::new(TlsProxy::new(
        certificates,
        resolver.clone(),
        Arc::clone(&config),
        configured_domains.clone(),
    )?);
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
    refresh_task.abort();
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
        let stream = crate::prefixed_io::PrefixedIo::new(client, header);
        return crate::http_server::serve(stream, "http", config, resolver, domains.to_vec(), Arc::clone(&tls_proxy)).await
            .with_context(|| format!("serving plain HTTP request from {peer}"));
    }
    let (host, port) = match parse_authority(authority) {
        Ok(value) => value,
        Err(error) => {
            let _ = client.write_all(BAD_REQUEST_RESPONSE).await;
            return Err(error);
        }
    };
    let flow = resolver.flow_analyzer();
    flow.record(crate::flow::FlowDirection::Read, header.len());
    const CONNECT_ESTABLISHED: &[u8] =
        b"HTTP/1.1 200 Connection Established\r\nProxy-Agent: FastGithub-Rust\r\n\r\n";
    client.write_all(CONNECT_ESTABLISHED).await?;
    flow.record(crate::flow::FlowDirection::Write, CONNECT_ESTABLISHED.len());
    tracing::debug!(%peer, %host, port, "HTTP CONNECT tunnel established");
    let is_tls = match timeout(HEADER_TIMEOUT, sniff_tls(&client)).await {
        Ok(result) => result?,
        Err(_) => bail!("timed out waiting for CONNECT tunnel data"),
    };
    if is_tls {
        // Intercepted TLS requests are routed by the decrypted HTTP Host and
        // configured Destination; the CONNECT authority is not an upstream.
        tls_proxy.intercept(client, host.as_str(), Arc::clone(&config), resolver.clone(), domains.to_vec(), Arc::clone(&tls_proxy)).await?;
        return Ok(());
    }

    // A raw CONNECT tunnel has no HTTP request to route, so connect to its
    // authority only after determining that the client is not starting TLS.
    let (mut upstream, _upstream_ip) = connect_target(&host, port, &resolver, domains, &config, None)
        .await.with_context(|| format!("connecting to raw CONNECT target {host}:{port}"))?;
    let mut client = crate::http_server::CountingIo::new(client, resolver.flow_analyzer());
    let (up, down) = tokio::io::copy_bidirectional(&mut client, &mut upstream).await?;
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

pub(crate) struct TlsProxy {
    acceptor: SslAcceptor,
    connector: SslConnector,
    certificates: Arc<CertificateService>,
    upstream_client: UpstreamClient,
}

impl TlsProxy {
    fn new(
        certificates: Arc<CertificateService>,
        resolver: DomainResolver,
        config: Arc<AppConfig>,
        domains: Vec<String>,
    ) -> Result<Self> {
        let mut acceptor = SslAcceptor::mozilla_intermediate(SslMethod::tls())
            .context("creating inbound TLS acceptor")?;
        acceptor.set_alpn_select_callback(|_, client_protocols| {
            openssl::ssl::select_next_proto(b"\x02h2\x08http/1.1", client_protocols)
                .ok_or(AlpnError::NOACK)
        });
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
        let upstream_connector = UpstreamConnector {
            resolver,
            domains: Arc::new(domains),
            config,
            tls_connector: Arc::new(connector.clone()),
        };
        let mut client_builder = Client::builder(TokioExecutor::new());
        client_builder
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_timer(TokioTimer::new())
            .pool_max_idle_per_host(8);
        let upstream_client = client_builder.build(upstream_connector);
        Ok(Self {
            acceptor: acceptor.build(),
            connector,
            certificates,
            upstream_client,
        })
    }

    pub(crate) async fn send_pooled_request(
        &self,
        request: Request<UpstreamBody>,
    ) -> Result<Response<Incoming>> {
        self.upstream_client.request(request).await.context("sending pooled upstream HTTP request")
    }

    pub(crate) async fn connect_upstream(
        &self,
        upstream: TcpStream,
        upstream_host: &str,
        upstream_ip: IpAddr,
        domain_config: Option<&crate::config::DomainConfig>,
        allow_http2: bool,
    ) -> Result<SslStream<TcpStream>> {
        connect_upstream_tls(
            &self.connector,
            upstream,
            upstream_host,
            upstream_ip,
            domain_config,
            allow_http2,
        ).await.map(|(tls, _)| tls)
    }

    pub(crate) async fn intercept(&self, client: TcpStream, connect_host: &str, config: Arc<AppConfig>, resolver: DomainResolver, domains: Vec<String>, tls_proxy: Arc<TlsProxy>) -> Result<()> {
        let default_certificate = self.certificates.server_certificate(connect_host)?;
        let mut ssl = Ssl::new(self.acceptor.context()).context("creating inbound TLS session")?;
        ssl.set_certificate(&default_certificate.certificate)?;
        ssl.set_private_key(&default_certificate.private_key)?;
        let mut client_tls = SslStream::new(ssl, client).context("wrapping inbound TLS stream")?;
        timeout(CONNECT_TIMEOUT, Pin::new(&mut client_tls).accept())
            .await.context("inbound TLS handshake timed out")??;
        let requested_host = client_tls.ssl().servername(NameType::HOST_NAME).unwrap_or(connect_host).to_owned();

        tracing::debug!(host = %requested_host, alpn = ?client_tls.ssl().selected_alpn_protocol(), "TLS interception established; handing decrypted HTTP to Hyper");
        if negotiated_http2(client_tls.ssl().selected_alpn_protocol()) {
            crate::http_server::serve_http2(client_tls, "https", config, resolver, domains, tls_proxy).await
        } else {
            crate::http_server::serve(client_tls, "https", config, resolver, domains, tls_proxy).await
        }
    }
}

async fn connect_upstream_tls(
    connector: &SslConnector,
    upstream: TcpStream,
    upstream_host: &str,
    upstream_ip: IpAddr,
    domain_config: Option<&crate::config::DomainConfig>,
    allow_http2: bool,
) -> Result<(SslStream<TcpStream>, bool)> {
    let configured_sni = domain_config.and_then(|rule| rule.tls_sni_value(upstream_host, upstream_ip));
    let disable_sni = domain_config.is_some_and(|rule| rule.tls_sni != Some(true));
    let ignore_name_mismatch = domain_config.is_some_and(|rule| rule.tls_ignore_name_mismatch == Some(true));
    let mut connect_config = connector.configure()?;
    connect_config.set_use_server_name_indication(!disable_sni);
    connect_config.set_verify_hostname(!ignore_name_mismatch);
    let alpn_protocols: &[u8] = if allow_http2 {
        b"\x02h2\x08http/1.1"
    } else {
        b"\x08http/1.1"
    };
    connect_config.set_alpn_protos(alpn_protocols)?;
    let mut ssl = connect_config.into_ssl(upstream_host)?;
    if let Some(sni) = configured_sni.as_deref() {
        ssl.set_hostname(sni)?;
    }
    let mut tls = SslStream::new(ssl, upstream).context("wrapping upstream TLS stream")?;
    timeout(CONNECT_TIMEOUT, Pin::new(&mut tls).connect())
        .await.context("upstream TLS handshake timed out")??;
    let http2 = negotiated_http2(tls.ssl().selected_alpn_protocol());
    Ok((tls, http2))
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

pub(crate) async fn connect_target(
    host: &str,
    port: u16,
    resolver: &DomainResolver,
    domains: &[String],
    config: &AppConfig,
    inherited_rule: Option<&crate::config::DomainConfig>,
) -> Result<(TcpStream, IpAddr)> {
    // Destination changes the upstream host, but the original request's DomainConfig
    // remains authoritative for IPAddress and TLS policy (matching .NET HttpClientHandler).
    let domain_config = inherited_rule.or_else(|| config.domain_config(host));

    // FastGithub's DomainConfig.Timeout is an HTTP request-lifecycle timeout,
    // not a TCP dial timeout. Keep connection establishment on its fixed limit;
    // request-scoped cancellation will be applied by the HTTP forwarding layer.
    let connect_timeout = CONNECT_TIMEOUT;
    let mut addresses = Vec::new();
    if let Ok(ip) = host.parse::<IpAddr>() {
        addresses.push(ip);
    } else if inherited_rule.is_some() || domains.iter().any(|pattern| crate::config::domain_matches(pattern, host)) {
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
            Ok(Ok(stream)) => {
                stream.set_nodelay(true)?;
                resolver.report_success(host, port, ip).await;
                return Ok((stream, ip));
            }
            Ok(Err(error)) => {
                resolver.report_failure(ip).await;
                failures.push(format!("{endpoint}: {error}"));
            }
            Err(_) => {
                resolver.report_failure(ip).await;
                failures.push(format!("{endpoint}: connection timed out"));
            }
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
    #[test]
    fn missing_or_non_h2_alpn_falls_back_to_http1() {
        assert!(!negotiated_http2(None));
        assert!(!negotiated_http2(Some(b"http/1.1")));
        assert!(!negotiated_http2(Some(b"unknown/1.0")));
        assert!(negotiated_http2(Some(b"h2")));
    }

    #[test]
    fn alpn_selection_prefers_h2_and_can_force_http1_for_upgrades() {
        let both = b"\x02h2\x08http/1.1";
        let client = b"\x08http/1.1\x02h2";
        assert_eq!(
            openssl::ssl::select_next_proto(both, client),
            Some(&b"h2"[..]),
        );

        let http1_only = b"\x08http/1.1";
        assert_eq!(
            openssl::ssl::select_next_proto(http1_only, client),
            Some(&b"http/1.1"[..]),
        );
        assert!(openssl::ssl::select_next_proto(http1_only, b"\x02h2").is_none());
    }

}
