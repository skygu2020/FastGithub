use crate::{cert::CertificateService, config::AppConfig, dns::DnsClient, persistence::Persistence, resolver::DomainResolver};
use arc_swap::ArcSwap;
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use http::{Extensions, Request, Response, Uri};
use http_body_util::combinators::UnsyncBoxBody;
use hyper::body::Incoming;
use hyper_util::{
    client::legacy::{connect::{capture_connection, Connected, Connection}, Client},
    rt::{TokioExecutor, TokioIo, TokioTimer},
};
use openssl::ssl::{AlpnError, NameType, SniError, Ssl, SslAcceptor, SslConnector, SslMethod};
use std::{
    error::Error,
    future::Future,
    io,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    task::{Context as TaskContext, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    net::{TcpListener, TcpStream},
    time::timeout,
};
use tower_service::Service;
use tokio_openssl::SslStream;

const HEADER_LIMIT: usize = 16 * 1024;
const HEADER_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
static NEXT_UPSTREAM_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

pub(crate) type UpstreamError = Box<dyn Error + Send + Sync>;
pub(crate) type UpstreamBody = UnsyncBoxBody<Bytes, UpstreamError>;
type UpstreamClient = Client<UpstreamConnector, UpstreamBody>;

struct UpstreamClientEntry {
    client: UpstreamClient,
    expires_at: tokio::time::Instant,
    // Distinguishes an expired/replaced pool entry from an older in-flight request.
    identity: Arc<()>,
}

fn pool_entry_matches_request(entry_identity: &Arc<()>, request_identity: &Arc<()>) -> bool {
    Arc::ptr_eq(entry_identity, request_identity)
}

fn captured_upstream_peer(connected: &Connected) -> Option<UpstreamPeerInfo> {
    let mut extensions = Extensions::new();
    connected.get_extras(&mut extensions);
    extensions.get::<UpstreamPeerInfo>().copied()
}

trait ProxyIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> ProxyIo for T {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct UpstreamPeerInfo {
    pub(crate) peer_ip: IpAddr,
    pub(crate) connection_id: u64,
}

struct ConnectedUpstreamIo {
    inner: Box<dyn ProxyIo>,
    peer_ip: IpAddr,
    connection_id: u64,
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
        tracing::debug!(
            %self.peer_ip,
            connection_id = self.connection_id,
            http2 = self.http2,
            "upstream connection metadata supplied to Hyper"
        );
        let connected = Connected::new().extra(UpstreamPeerInfo {
            peer_ip: self.peer_ip,
            connection_id: self.connection_id,
        });
        if self.http2 { connected.negotiated_h2() } else { connected }
    }
}

#[derive(Clone)]
struct UpstreamConnector {
    resolver: DomainResolver,
    config: Arc<AppConfig>,
    tls_connector: Arc<SslConnector>,
    inherited_rule: Option<crate::config::DomainConfig>,
}

impl UpstreamConnector {
    async fn connect(&self, uri: Uri) -> Result<TokioIo<ConnectedUpstreamIo>> {
        let connect_started = tokio::time::Instant::now();
        let host = uri.host().context("upstream URI has no hostname")?;
        tracing::debug!(%host, %uri, "starting upstream transport connection");
        let scheme = uri.scheme_str().unwrap_or("http");
        let port = uri.port_u16().unwrap_or(if scheme == "https" { 443 } else { 80 });
        let config = Arc::clone(&self.config);
        let domains = config.fast_github.domain_configs.keys().cloned().collect::<Vec<_>>();

        let (io, peer_ip, http2): (Box<dyn ProxyIo>, IpAddr, bool) = match scheme {
            "http" => {
                let (stream, ip) = connect_target(
                    host, port, &self.resolver, &domains, &config, self.inherited_rule.as_ref(),
                ).await?;
                (Box::new(stream), ip, false)
            }
            "https" => {
                let rule = self.inherited_rule.as_ref().or_else(|| config.domain_config(host));
                let mut attempted = Vec::new();
                let mut failures = Vec::new();
                let mut connected = None;

                // A TCP connection is not proof that the endpoint can complete TLS.
                // Try the next candidate when the TLS handshake or certificate check fails.
                for _ in 0..8 {
                    let (stream, ip) = match connect_target_excluding(
                        host, port, &self.resolver, &domains, &config,
                        self.inherited_rule.as_ref(), &attempted,
                    ).await {
                        Ok(value) => value,
                        Err(error) => {
                            failures.push(error.to_string());
                            break;
                        }
                    };
                    attempted.push(ip);
                    match connect_upstream_tls(&self.tls_connector, stream, host, ip, rule, true).await {
                        Ok((tls, http2)) => {
                            connected = Some((Box::new(tls) as Box<dyn ProxyIo>, ip, http2));
                            break;
                        }
                        Err(error) => {
                            self.resolver.report_failure(host, port, ip).await;
                            failures.push(format!("{ip}: TLS handshake failed: {error}"));
                        }
                    }
                }

                connected.ok_or_else(|| anyhow::anyhow!(
                    "no upstream candidate completed TLS for {host}:{port}: {}",
                    failures.join("; ")
                ))?
            }
            other => bail!("unsupported upstream URI scheme '{other}'"),
        };

        let connection_id = NEXT_UPSTREAM_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(
            %host,
            port,
            %peer_ip,
            connection_id,
            http2,
            elapsed_ms = connect_started.elapsed().as_millis(),
            "upstream transport connection ready"
        );
        Ok(TokioIo::new(ConnectedUpstreamIo { inner: io, peer_ip, connection_id, http2 }))
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

const BAD_REQUEST_RESPONSE: &[u8] = b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n";

pub async fn run(config: AppConfig, config_path: PathBuf, upstreams: Vec<SocketAddr>, certificates: Arc<CertificateService>) -> Result<()> {
    let parent_pid = unsafe { libc::getppid() as u32 };
    let port = config.fast_github.http_proxy_port;
    let resolver = DomainResolver::new(DnsClient::new(upstreams), Persistence::new("dnsendpoints.json")).await?;
    let config = Arc::new(ArcSwap::from_pointee(config));
    let tls_proxy = Arc::new(TlsProxy::new(
        certificates,
        resolver.clone(),
        Arc::clone(&config),
    )?);
    let listener = TcpListener::bind(("127.0.0.1", port)).await
        .with_context(|| format!("binding HTTP proxy 127.0.0.1:{port}"))?;
    tracing::info!(address = %listener.local_addr()?, "HTTP CONNECT proxy listening");
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
    let watch_tls_proxy = Arc::clone(&tls_proxy);
    let config_task = tokio::spawn(async move {
        watch_config_file(config_path, watch_tls_proxy).await;
    });
    let mut parent_exit_task = tokio::spawn(wait_for_parent_exit(parent_pid));
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, peer)) => {
                        let resolver = resolver.clone();
                        let tls_proxy = Arc::clone(&tls_proxy);
                        let config = Arc::clone(&config);
                        tokio::spawn(async move {
                            if let Err(error) = handle_connection(stream, resolver, config, tls_proxy, peer).await {
                                tracing::debug!(%peer, %error, "proxy connection ended");
                            }
                        });
                    }
                    Err(error) => tracing::warn!(%error, "accepting proxy connection failed"),
                }
            }
            signal = tokio::signal::ctrl_c() => {
                if let Err(error) = signal {
                    tracing::warn!(%error, "Ctrl-C signal handler failed");
                }
                tracing::info!("HTTP proxy shutting down");
                break;
            }
            _ = &mut parent_exit_task => {
                tracing::info!(parent_pid, "parent process exited; stopping HTTP proxy");
                break;
            }
        }
    }
    refresh_task.abort();
    config_task.abort();
    parent_exit_task.abort();
    Ok(())
}

async fn wait_for_parent_exit(parent_pid: u32) {
    if parent_pid <= 1 {
        std::future::pending::<()>().await;
    }
    loop {
        let stat = match tokio::fs::read_to_string(format!("/proc/{parent_pid}/stat")).await {
            Ok(stat) => stat,
            Err(_) => return,
        };
        let state = stat.rsplit_once(')')
            .and_then(|(_, fields)| fields.split_whitespace().next())
            .unwrap_or("Z");
        if state == "Z" || state == "X" {
            return;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn config_fingerprint(path: &Path) -> Vec<(PathBuf, Option<std::time::SystemTime>)> {
    let mut paths = vec![path.to_path_buf()];
    if let Some(parent) = path.parent() {
        let overlay_dir = parent.join("appsettings");
        if let Ok(entries) = std::fs::read_dir(overlay_dir) {
            paths.extend(entries.filter_map(|entry| entry.ok().map(|entry| entry.path()))
                .filter(|candidate| candidate.file_name().and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("appsettings.") && name.ends_with(".json"))));
        }
    }
    paths.sort();
    paths.dedup();
    paths.into_iter().map(|candidate| {
        let modified = std::fs::metadata(&candidate).and_then(|metadata| metadata.modified()).ok();
        (candidate, modified)
    }).collect()
}

async fn watch_config_file(path: PathBuf, tls_proxy: Arc<TlsProxy>) {
    let mut fingerprint = config_fingerprint(&path);
    let mut interval = tokio::time::interval(Duration::from_secs(2));
    interval.tick().await;
    loop {
        interval.tick().await;
        let next = config_fingerprint(&path);
        if next == fingerprint {
            continue;
        }
        fingerprint = next;
        let load_path = path.clone();
        match tokio::task::spawn_blocking(move || AppConfig::load(&load_path)).await {
            Ok(Ok(updated)) => {
                let domains = updated.fast_github.domain_configs.len();
                match tls_proxy.reload_config(updated) {
                    Ok(()) => tracing::info!(domains, "configuration reloaded; upstream connection pools invalidated"),
                    Err(error) => tracing::warn!(%error, "configuration snapshot could not be swapped"),
                }
            }
            Ok(Err(error)) => {
                tracing::warn!(%error, "configuration reload failed; keeping previous configuration");
            }
            Err(error) => {
                tracing::warn!(%error, "configuration reload task failed; keeping previous configuration");
            }
        }
    }
}

async fn handle_connection(mut client: TcpStream, resolver: DomainResolver, config: Arc<ArcSwap<AppConfig>>, tls_proxy: Arc<TlsProxy>, peer: SocketAddr) -> Result<()> {
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
        return crate::http_server::serve(stream, "http", config, resolver, Vec::new(), Arc::clone(&tls_proxy)).await
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
        tls_proxy.intercept(client, host.as_str(), Arc::clone(&config), resolver.clone(), Vec::new(), Arc::clone(&tls_proxy)).await?;
        return Ok(());
    }

    // A raw CONNECT tunnel has no HTTP request to route, so connect to its
    // authority only after determining that the client is not starting TLS.
    let config_snapshot = config.load_full();
    let domains = config_snapshot.fast_github.domain_configs.keys().cloned().collect::<Vec<_>>();
    let (mut upstream, _upstream_ip) = connect_target(&host, port, &resolver, &domains, &config_snapshot, None)
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
    resolver: DomainResolver,
    config: Arc<ArcSwap<AppConfig>>,
    upstream_clients: Mutex<std::collections::HashMap<String, UpstreamClientEntry>>,
}

impl TlsProxy {
    fn new(
        certificates: Arc<CertificateService>,
        resolver: DomainResolver,
        config: Arc<ArcSwap<AppConfig>>,
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
        Ok(Self {
            acceptor: acceptor.build(),
            connector,
            certificates,
            resolver,
            config,
            upstream_clients: Mutex::new(std::collections::HashMap::new()),
        })
    }

    fn upstream_client(&self, original_host: &str) -> Result<(UpstreamClient, Arc<()>)> {
        let key = original_host.trim_end_matches('.').to_ascii_lowercase();
        let mut lifetime = Duration::from_secs(10);

        loop {
            {
                let mut clients = self.upstream_clients.lock()
                    .map_err(|_| anyhow::anyhow!("upstream client pool lock poisoned"))?;
                if let Some(entry) = clients.get(&key) {
                    if entry.expires_at > tokio::time::Instant::now() {
                        return Ok((entry.client.clone(), Arc::clone(&entry.identity)));
                    }
                    // The first handler is deliberately short-lived; replacements
                    // follow the reference implementation's 100-second lifetime.
                    clients.remove(&key);
                    lifetime = Duration::from_secs(100);
                }
            }

            let config = self.config.load_full();
            let connector = UpstreamConnector {
                resolver: self.resolver.clone(),
                config: Arc::clone(&config),
                tls_connector: Arc::new(self.connector.clone()),
                inherited_rule: config.domain_config(original_host).cloned(),
            };
            let mut builder = Client::builder(TokioExecutor::new());
            builder
                .pool_idle_timeout(Duration::from_secs(90))
                .pool_timer(TokioTimer::new())
                .pool_max_idle_per_host(8);
            let client = builder.build(connector);

            let mut clients = self.upstream_clients.lock()
                .map_err(|_| anyhow::anyhow!("upstream client pool lock poisoned"))?;
            if !Arc::ptr_eq(&config, &self.config.load_full()) {
                continue;
            }
            if let Some(entry) = clients.get(&key) {
                if entry.expires_at > tokio::time::Instant::now() {
                    return Ok((entry.client.clone(), Arc::clone(&entry.identity)));
                }
                clients.remove(&key);
                lifetime = Duration::from_secs(100);
            }
            if clients.len() >= 256 && !clients.contains_key(&key) {
                if let Some(evicted) = clients.keys().next().cloned() {
                    clients.remove(&evicted);
                }
            }
            let identity = Arc::new(());
            clients.insert(key.clone(), UpstreamClientEntry {
                client: client.clone(),
                expires_at: tokio::time::Instant::now() + lifetime,
                identity: Arc::clone(&identity),
            });
            return Ok((client, identity));
        }
    }

    pub(crate) fn reload_config(&self, updated: AppConfig) -> Result<()> {
        let mut clients = self.upstream_clients.lock()
            .map_err(|_| anyhow::anyhow!("upstream client pool lock poisoned"))?;
        self.config.store(Arc::new(updated));
        clients.clear();
        Ok(())
    }

    pub(crate) async fn send_pooled_request(
        &self,
        original_host: &str,
        mut request: Request<UpstreamBody>,
    ) -> Result<Response<Incoming>> {
        let (client, pool_identity) = self.upstream_client(original_host)?;
        let captured_connection = capture_connection(&mut request);
        let method = request.method().clone();
        let uri = request.uri().clone();
        let started = tokio::time::Instant::now();
        tracing::debug!(%original_host, %method, %uri, "sending request through upstream connection pool");

        // Capture the selected connection independently of the response future.
        // This task also records the peer when the caller later times out waiting
        // for response headers and cancels this request future.
        let mut capture_for_log = captured_connection.clone();
        let log_host = original_host.to_owned();
        let log_method = method.clone();
        let log_uri = uri.clone();
        tokio::spawn(async move {
            let metadata = capture_for_log.wait_for_connection_metadata().await;
            if let Some(connected) = metadata.as_ref() {
                let peer_info = captured_upstream_peer(connected);
                tracing::debug!(
                    original_host = %log_host,
                    method = %log_method,
                    uri = %log_uri,
                    peer_ip = ?peer_info.map(|info| info.peer_ip),
                    connection_id = ?peer_info.map(|info| info.connection_id),
                    "upstream request assigned to connection"
                );
            }
        });

        let result = client.request(request).await;
        let connection_metadata = captured_connection.connection_metadata();
        let peer_info = connection_metadata.as_ref().and_then(captured_upstream_peer);
        let error_connect_info = result.as_ref().err()
            .and_then(|error| error.connect_info())
            .and_then(captured_upstream_peer);
        let connector_failed = result.as_ref().err().is_some_and(|error| error.is_connect());
        match &result {
            Ok(response) => tracing::debug!(
                %original_host,
                %method,
                %uri,
                peer_ip = ?peer_info.map(|info| info.peer_ip),
                connection_id = ?peer_info.map(|info| info.connection_id),
                status = response.status().as_u16(),
                elapsed_ms = started.elapsed().as_millis(),
                "upstream response headers received"
            ),
            Err(error) => tracing::warn!(
                %original_host,
                %method,
                %uri,
                %error,
                error_class = if error.is_connect() { "connector" } else { "request_or_connection" },
                peer_ip = ?error_connect_info.or(peer_info).map(|info| info.peer_ip),
                connection_id = ?error_connect_info.or(peer_info).map(|info| info.connection_id),
                elapsed_ms = started.elapsed().as_millis(),
                "upstream pooled request failed"
            ),
        }
        // A connector failure means no usable transport was established. A
        // generic request error can instead be an HTTP/2 stream reset or a
        // connection-level failure already handled by Hyper's pool. Do not
        // discard the entire per-host Client for an unclassified stream error.
        if connector_failed {
            // Do not let a late failure from an old client evict a newer pool
            // entry installed after expiry or configuration reload.
            let key = original_host.trim_end_matches('.').to_ascii_lowercase();
            match self.upstream_clients.lock() {
                Ok(mut clients) => {
                    let is_current = clients.get(&key)
                        .is_some_and(|entry| pool_entry_matches_request(&entry.identity, &pool_identity));
                    if is_current {
                        clients.remove(&key);
                        tracing::debug!(%key, "invalidated upstream pool after connector failure");
                    } else {
                        tracing::debug!(%key, "connector failure belongs to a retired pool entry; keeping replacement");
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, %key, "could not invalidate upstream pool after connector failure");
                }
            }
        }
        result.context("sending pooled upstream HTTP request")
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

    pub(crate) async fn intercept(&self, client: TcpStream, connect_host: &str, config: Arc<ArcSwap<AppConfig>>, resolver: DomainResolver, domains: Vec<String>, tls_proxy: Arc<TlsProxy>) -> Result<()> {
        let default_certificate = self.certificates.server_certificate(connect_host)?;
        let mut ssl = Ssl::new(self.acceptor.context()).context("creating inbound TLS session")?;
        ssl.set_certificate(&default_certificate.certificate)?;
        ssl.set_private_key(&default_certificate.private_key)?;
        let mut client_tls = SslStream::new(ssl, client).context("wrapping inbound TLS stream")?;
        timeout(CONNECT_TIMEOUT, Pin::new(&mut client_tls).accept())
            .await.context("inbound TLS handshake timed out")??;
        let requested_host = client_tls.ssl().servername(NameType::HOST_NAME).unwrap_or(connect_host).to_owned();

        tracing::debug!(host = %requested_host, alpn = ?client_tls.ssl().selected_alpn_protocol(), "TLS interception established; handing decrypted HTTP to Hyper");
        let alpn = client_tls.ssl().selected_alpn_protocol().map(|value| String::from_utf8_lossy(value).into_owned());
        let result = if negotiated_http2(client_tls.ssl().selected_alpn_protocol()) {
            crate::http_server::serve_http2(client_tls, "https", config, resolver, domains, tls_proxy).await
        } else {
            crate::http_server::serve(client_tls, "https", config, resolver, domains, tls_proxy).await
        };
        if let Err(error) = &result {
            tracing::debug!(%requested_host, ?alpn, %error, "intercepted HTTP connection ended with error");
        }
        result
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
    let tls_started = tokio::time::Instant::now();
    tracing::debug!(host = %upstream_host, %upstream_ip, "starting upstream TLS handshake");
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
    tracing::debug!(
        host = %upstream_host,
        %upstream_ip,
        http2,
        elapsed_ms = tls_started.elapsed().as_millis(),
        "upstream TLS handshake completed"
    );
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
    connect_target_excluding(host, port, resolver, domains, config, inherited_rule, &[]).await
}

pub(crate) async fn connect_target_excluding(
    host: &str,
    port: u16,
    resolver: &DomainResolver,
    domains: &[String],
    config: &AppConfig,
    inherited_rule: Option<&crate::config::DomainConfig>,
    excluded: &[IpAddr],
) -> Result<(TcpStream, IpAddr)> {
    // Destination changes the upstream host, but the original request's DomainConfig
    // remains authoritative for IPAddress and TLS policy (matching .NET HttpClientHandler).
    let domain_config = inherited_rule.or_else(|| config.domain_config(host));

    // FastGithub's DomainConfig.Timeout is an HTTP request-lifecycle timeout,
    // not a TCP dial timeout. Keep connection establishment on its fixed limit;
    // request-scoped cancellation will be applied by the HTTP forwarding layer.
    let connect_timeout = CONNECT_TIMEOUT;
    let resolve_started = tokio::time::Instant::now();
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
        // Never consult the operating system resolver here. Its nameserver list
        // may be supplied by an untrusted network (DHCP/VPN/NetworkManager) and
        // can return poisoned answers. All hostnames must use FastGithub's
        // explicitly configured DNS upstreams through DomainResolver.
        addresses.extend(
            resolver.resolve(host, port).await
                .with_context(|| format!("resolving upstream domain {host} via configured DNS"))?
        );
    }

    tracing::debug!(
        %host,
        port,
        candidate_count = addresses.len(),
        candidates = ?addresses,
        elapsed_ms = resolve_started.elapsed().as_millis(),
        "upstream address candidates resolved"
    );
    let mut unique_addresses = Vec::with_capacity(addresses.len());
    for ip in addresses {
        if !excluded.contains(&ip) && !unique_addresses.contains(&ip) {
            unique_addresses.push(ip);
        }
    }
    if unique_addresses.is_empty() { bail!("no IP addresses available for {host}"); }

    let mut failures = Vec::new();
    for ip in unique_addresses {
        let endpoint = SocketAddr::new(ip, port);
        let tcp_started = tokio::time::Instant::now();
        tracing::debug!(%host, %endpoint, "starting upstream TCP connection");
        match timeout(connect_timeout, TcpStream::connect(endpoint)).await {
            Ok(Ok(stream)) => {
                tracing::debug!(
                    %host,
                    %endpoint,
                    elapsed_ms = tcp_started.elapsed().as_millis(),
                    "upstream TCP connection established"
                );
                stream.set_nodelay(true)?;
                resolver.report_success(host, port, ip).await;
                return Ok((stream, ip));
            }
            Ok(Err(error)) => {
                tracing::warn!(
                    %host,
                    %endpoint,
                    %error,
                    elapsed_ms = tcp_started.elapsed().as_millis(),
                    "upstream TCP connection failed"
                );
                resolver.report_failure(host, port, ip).await;
                failures.push(format!("{endpoint}: {error}"));
            }
            Err(_) => {
                tracing::warn!(
                    %host,
                    %endpoint,
                    elapsed_ms = tcp_started.elapsed().as_millis(),
                    "upstream TCP connection timed out"
                );
                resolver.report_failure(host, port, ip).await;
                failures.push(format!("{endpoint}: connection timed out"));
            }
        }
    }
    bail!("all connection attempts failed: {}", failures.join("; "))
}

#[cfg(test)]
mod tests {

    #[test]
    fn config_fingerprint_tracks_overlay_files() {
        let root = std::env::temp_dir().join(format!(
            "fastgithub-config-watch-test-{}",
            std::process::id()
        ));
        let overlay_dir = root.join("appsettings");
        std::fs::create_dir_all(&overlay_dir).unwrap();
        let config_path = root.join("appsettings.json");
        std::fs::write(&config_path, r#"{"FastGithub":{}}"#).unwrap();

        let before = config_fingerprint(&config_path);
        std::fs::write(overlay_dir.join("appsettings.dev.json"), "{}").unwrap();
        let after = config_fingerprint(&config_path);
        assert_ne!(before, after);

        std::fs::remove_dir_all(root).unwrap();
    }
    use super::*;
    use std::{convert::Infallible, sync::{Arc, atomic::{AtomicUsize, Ordering}}};

    #[test]
    fn connected_metadata_preserves_upstream_peer_identity() {
        let expected = UpstreamPeerInfo {
            peer_ip: IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            connection_id: 42,
        };
        let connected = Connected::new().extra(expected);
        assert_eq!(captured_upstream_peer(&connected), Some(expected));
    }

    #[test]
    fn failed_old_pool_entry_does_not_match_replacement_identity() {
        let old_identity = Arc::new(());
        let replacement_identity = Arc::new(());
        assert!(!pool_entry_matches_request(&replacement_identity, &old_identity));
        assert!(pool_entry_matches_request(&old_identity, &old_identity));
    }
    use hyper::service::service_fn;
    use http_body_util::Full;


    struct NeverRequestBody;

    impl http_body::Body for NeverRequestBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
        ) -> Poll<Option<std::result::Result<http_body::Frame<Self::Data>, Self::Error>>> {
            Poll::Pending
        }

        fn is_end_stream(&self) -> bool {
            false
        }
    }

    #[tokio::test]
    async fn pooled_http1_client_delivers_early_final_response_during_upload() {
        use bytes::Bytes;
        use http::{header, Method, Request, Response, StatusCode};
        use http_body_util::{BodyExt, Full};
        use hyper::{server::conn::http1, service::service_fn};
        use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
        use std::{convert::Infallible, sync::Arc};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let service = service_fn(|_request: Request<hyper::body::Incoming>| async move {
                let mut response = Response::new(Full::new(Bytes::from_static(b"rejected")));
                *response.status_mut() = StatusCode::PAYLOAD_TOO_LARGE;
                Ok::<_, Infallible>(response)
            });
            http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await
                .unwrap();
        });

        let config: AppConfig = serde_json::from_str(
            r#"{"FastGithub":{"HttpProxyPort":0,"FallbackDns":[],"DomainConfigs":{}}}"#,
        ).unwrap();
        let resolver = DomainResolver::new(
            DnsClient::new(Vec::new()),
            Persistence::new(std::env::temp_dir().join(format!(
                "fastgithub-early-response-{}-{}.json",
                std::process::id(),
                address.port()
            ))),
        ).await.unwrap();
        let connector = UpstreamConnector {
            resolver,
            config: Arc::new(config),
            tls_connector: Arc::new(SslConnector::builder(SslMethod::tls()).unwrap().build()),
            inherited_rule: None,
        };
        let mut builder = Client::builder(TokioExecutor::new());
        builder
            .pool_idle_timeout(Duration::from_secs(30))
            .pool_timer(TokioTimer::new());
        let client: UpstreamClient = builder.build(connector);

        let body: UpstreamBody = NeverRequestBody
            .map_err(|never| -> UpstreamError { match never {} })
            .boxed_unsync();
        let request = Request::builder()
            .method(Method::POST)
            .uri(format!("http://127.0.0.1:{}/upload", address.port()))
            .header(header::CONTENT_LENGTH, "10")
            .body(body)
            .unwrap();
        let response = tokio::time::timeout(Duration::from_secs(2), client.request(request))
            .await
            .expect("early upstream response must not wait for the unfinished upload")
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(response.into_body().collect().await.unwrap().to_bytes(), Bytes::from_static(b"rejected"));

        drop(client);
        server.abort();
        let _ = server.await;
    }

    #[derive(Clone)]
    struct MockH2Connector {
        connections: Arc<std::sync::atomic::AtomicUsize>,
        requests: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Service<Uri> for MockH2Connector {
        type Response = TokioIo<ConnectedUpstreamIo>;
        type Error = io::Error;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _uri: Uri) -> Self::Future {
            let connections = Arc::clone(&self.connections);
            let requests = Arc::clone(&self.requests);
            Box::pin(async move {
                let (client_io, server_io) = tokio::io::duplex(1024 * 1024);
                connections.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(async move {
                    let service = service_fn(move |_request: Request<hyper::body::Incoming>| {
                        let requests = Arc::clone(&requests);
                        async move {
                            requests.fetch_add(1, Ordering::Relaxed);
                            Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(b"h2"))))
                        }
                    });
                    hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(server_io), service)
                        .await
                        .unwrap();
                });
                Ok(TokioIo::new(ConnectedUpstreamIo {
                    inner: Box::new(client_io),
                    peer_ip: IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                    connection_id: 1,
                    http2: true,
                }))
            })
        }
    }

    #[derive(Clone)]
    struct MockH2GoAwayConnector {
        connections: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Service<Uri> for MockH2GoAwayConnector {
        type Response = TokioIo<ConnectedUpstreamIo>;
        type Error = io::Error;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _uri: Uri) -> Self::Future {
            let connection_id = self.connections.fetch_add(1, Ordering::Relaxed) as u64 + 1;
            Box::pin(async move {
                let (client_io, server_io) = tokio::io::duplex(1024 * 1024);
                tokio::spawn(async move {
                    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
                    let shutdown_tx = std::sync::Mutex::new(Some(shutdown_tx));
                    let service = service_fn(move |_request: Request<hyper::body::Incoming>| {
                        if let Some(sender) = shutdown_tx.lock().unwrap().take() {
                            let _ = sender.send(());
                        }
                        async move {
                            Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(b"h2"))))
                        }
                    });
                    let mut connection = Box::pin(
                        hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                            .serve_connection(TokioIo::new(server_io), service),
                    );
                    tokio::select! {
                        result = &mut connection => {
                            let _ = result;
                        }
                        _ = shutdown_rx => {
                            connection.as_mut().graceful_shutdown();
                            let _ = connection.await;
                        }
                    }
                });
                Ok(TokioIo::new(ConnectedUpstreamIo {
                    inner: Box::new(client_io),
                    peer_ip: IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                    connection_id,
                    http2: true,
                }))
            })
        }
    }

    #[tokio::test]
    async fn pooled_client_reconnects_after_http2_goaway() {
        use bytes::Bytes;
        use http::Version;
        use http_body_util::{BodyExt, Full};
        use hyper_util::rt::TokioExecutor;

        let connections = Arc::new(AtomicUsize::new(0));
        let connector = MockH2GoAwayConnector {
            connections: Arc::clone(&connections),
        };
        let mut builder = Client::builder(TokioExecutor::new());
        builder
            .pool_idle_timeout(Duration::from_secs(30))
            .pool_timer(TokioTimer::new())
            .pool_max_idle_per_host(4);
        let client: UpstreamClient = builder.build(connector);

        let make_request = |path: &'static str| {
            let body: UpstreamBody = Full::new(Bytes::new())
                .map_err(|never| -> UpstreamError { match never {} })
                .boxed_unsync();
            Request::builder()
                .version(Version::HTTP_11)
                .uri(format!("https://goaway.example{path}"))
                .body(body)
                .unwrap()
        };

        let mut first_request = make_request("/first");
        let first_capture = capture_connection(&mut first_request);
        let first_response = client.request(first_request).await.unwrap();
        assert_eq!(
            first_response.into_body().collect().await.unwrap().to_bytes(),
            Bytes::from_static(b"h2"),
        );
        let first_peer = first_capture.connection_metadata()
            .as_ref()
            .and_then(captured_upstream_peer)
            .expect("first request should capture its HTTP/2 connection");

        // The mock peer sends GOAWAY after the first request and drains the
        // active stream. A later request must not be pinned to that connection.
        let mut second_request = make_request("/second");
        let second_capture = capture_connection(&mut second_request);
        let second_response = tokio::time::timeout(
            Duration::from_secs(2),
            client.request(second_request),
        ).await.expect("pooled client must reconnect after GOAWAY").unwrap();
        assert_eq!(
            second_response.into_body().collect().await.unwrap().to_bytes(),
            Bytes::from_static(b"h2"),
        );
        let second_peer = second_capture.connection_metadata()
            .as_ref()
            .and_then(captured_upstream_peer)
            .expect("second request should capture its replacement HTTP/2 connection");

        assert_ne!(first_peer.connection_id, second_peer.connection_id);
        assert_eq!(connections.load(Ordering::Relaxed), 2);

        drop(client);
    }

    #[tokio::test]
    async fn pooled_client_multiplexes_http2_requests_on_one_connection() {
        use bytes::Bytes;
        use http::Version;
        use http_body_util::{BodyExt, Full};
        use hyper_util::rt::TokioExecutor;

        let connections = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(AtomicUsize::new(0));
        let connector = MockH2Connector {
            connections: Arc::clone(&connections),
            requests: Arc::clone(&requests),
        };
        let mut builder = Client::builder(TokioExecutor::new());
        builder
            .pool_idle_timeout(Duration::from_secs(30))
            .pool_timer(TokioTimer::new())
            .pool_max_idle_per_host(4);
        let client: Client<_, UpstreamBody> = builder.build(connector);

        let make_request = |path: &'static str| {
            let body: UpstreamBody = Full::new(Bytes::new())
                .map_err(|never| -> UpstreamError { match never {} })
                .boxed_unsync();
            Request::builder()
                .version(Version::HTTP_11)
                .uri(format!("https://pool.example{path}"))
                .body(body)
                .unwrap()
        };
        let mut first_request = make_request("/one");
        let mut second_request = make_request("/two");
        let first_capture = capture_connection(&mut first_request);
        let second_capture = capture_connection(&mut second_request);
        let (first, second) = tokio::join!(
            client.request(first_request),
            client.request(second_request),
        );
        let first = first.unwrap().into_body().collect().await.unwrap().to_bytes();
        let second = second.unwrap().into_body().collect().await.unwrap().to_bytes();
        assert_eq!(first, Bytes::from_static(b"h2"));
        assert_eq!(second, Bytes::from_static(b"h2"));

        let first_metadata = first_capture.connection_metadata();
        let second_metadata = second_capture.connection_metadata();
        let first_peer = first_metadata.as_ref()
            .and_then(captured_upstream_peer)
            .expect("first request should capture its HTTP/2 connection");
        let second_peer = second_metadata.as_ref()
            .and_then(captured_upstream_peer)
            .expect("second request should capture its HTTP/2 connection");
        assert_eq!(first_peer, second_peer);
        assert_eq!(first_peer.connection_id, 1);
        assert_eq!(first_peer.peer_ip, IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));

        assert_eq!(connections.load(Ordering::Relaxed), 1);
        assert_eq!(requests.load(Ordering::Relaxed), 2);
    }
    #[tokio::test]
    async fn pooled_client_reuses_http1_connection_for_same_origin() {
        use bytes::Bytes;
        use http::{Request, Response};
        use http_body_util::{BodyExt, Full};
        use hyper::{server::conn::http1, service::service_fn};
        use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
        use std::{convert::Infallible, sync::{Arc, atomic::{AtomicUsize, Ordering}}};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let accepted_server = Arc::clone(&accepted);
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                accepted_server.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(async move {
                    let service = service_fn(|_request: Request<hyper::body::Incoming>| async move {
                        Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(b"ok"))))
                    });
                    let _ = http1::Builder::new()
                        .keep_alive(true)
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });

        let config: AppConfig = serde_json::from_str(
            r#"{"FastGithub":{"HttpProxyPort":0,"FallbackDns":[],"DomainConfigs":{}}}"#,
        ).unwrap();
        let resolver = DomainResolver::new(
            DnsClient::new(Vec::new()),
            Persistence::new(std::env::temp_dir().join(format!(
                "fastgithub-pool-test-{}-{}.json",
                std::process::id(),
                address.port()
            ))),
        ).await.unwrap();
        let connector = UpstreamConnector {
            resolver,
            config: Arc::new(config),
            tls_connector: Arc::new(SslConnector::builder(SslMethod::tls()).unwrap().build()),
            inherited_rule: None,
        };
        let mut builder = Client::builder(TokioExecutor::new());
        builder
            .pool_idle_timeout(Duration::from_secs(30))
            .pool_timer(TokioTimer::new())
            .pool_max_idle_per_host(4);
        let client: UpstreamClient = builder.build(connector);

        for _ in 0..2 {
            let body: UpstreamBody = Full::new(Bytes::new())
                .map_err(|never| -> UpstreamError { match never {} })
                .boxed_unsync();
            let request = Request::builder()
                .uri(format!("http://127.0.0.1:{}/", address.port()))
                .body(body)
                .unwrap();
            let response = client.request(request).await.unwrap();
            assert_eq!(response.into_body().collect().await.unwrap().to_bytes(), Bytes::from_static(b"ok"));
        }

        assert_eq!(accepted.load(Ordering::Relaxed), 1);
        drop(client);
        server.abort();
        let _ = server.await;
    }

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
