use std::{
    collections::HashMap,
    io,
    net::{IpAddr, SocketAddr},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use openssl::ssl::{SslConnector, SslMethod};
use tokio::{
    net::TcpStream,
    sync::Mutex,
    time::{timeout, Instant},
};
use tokio_openssl::SslStream;

const DOMAIN_IP_TTL: Duration = Duration::from_secs(10 * 60);
const GOOD_LATENCY_TTL: Duration = Duration::from_secs(5 * 60);
const FAILED_LOCAL_TTL: Duration = Duration::from_secs(60);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone)]
struct Timed<T> {
    value: T,
    expires: Instant,
}

#[derive(Clone)]
pub struct AddressService {
    domain_ips: Arc<Mutex<HashMap<(String, IpAddr), Instant>>>,
    latency: Arc<Mutex<HashMap<(String, SocketAddr), Timed<Option<Duration>>>>>,
}

impl Default for AddressService {
    fn default() -> Self {
        Self {
            domain_ips: Arc::new(Mutex::new(HashMap::new())),
            latency: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl AddressService {
    /// Clear only probe and domain-association caches; request-stage circuit breakers remain.
    pub async fn clear_cache(&self) {
        self.domain_ips.lock().await.clear();
        self.latency.lock().await.clear();
    }

    /// Keep previously selected addresses only while their domain/IP association is fresh.
    pub async fn order_addresses(
        &self,
        host: &str,
        port: u16,
        fresh: Vec<IpAddr>,
        previous: &[IpAddr],
    ) -> Vec<IpAddr> {
        let now = Instant::now();
        let key = host.trim_end_matches('.').to_ascii_lowercase();
        let mut history = self.domain_ips.lock().await;
        history.retain(|_, expires| *expires > now);

        let mut addresses = Vec::new();
        for ip in previous {
            if history
                .get(&(key.clone(), *ip))
                .is_some_and(|expires| *expires > now)
            {
                addresses.push(*ip);
            }
        }
        for ip in fresh {
            history.insert((key.clone(), ip), now + DOMAIN_IP_TTL);
            if !addresses.contains(&ip) {
                addresses.push(ip);
            }
        }
        drop(history);

        let mut jobs = tokio::task::JoinSet::new();
        for ip in addresses.iter().copied() {
            let this = self.clone();
            let host = key.clone();
            jobs.spawn(async move {
                (
                    ip,
                    this.measure(&host, SocketAddr::new(ip, port)).await,
                )
            });
        }

        let mut measured = Vec::new();
        while let Some(result) = jobs.join_next().await {
            if let Ok((ip, Some(elapsed))) = result {
                measured.push((ip, elapsed));
            }
        }
        measured.sort_by_key(|item| item.1);

        // A transient DNS/network failure must not erase every candidate.
        // The real request path will validate fallback candidates and report failures.
        if measured.is_empty() {
            return addresses;
        }

        measured.into_iter().map(|item| item.0).collect()
    }

    async fn measure(&self, host: &str, endpoint: SocketAddr) -> Option<Duration> {
        let key = (host.to_owned(), endpoint);
        let now = Instant::now();
        if let Some(entry) = self.latency.lock().await.get(&key) {
            if entry.expires > now {
                return entry.value;
            }
        }

        let started = Instant::now();
        let result = timeout(CONNECT_TIMEOUT, async {
            let stream = TcpStream::connect(endpoint)
                .await
                .with_context(|| format!("connecting to {endpoint}"))?;

            // TCP reachability alone is insufficient: polluted endpoints often
            // accept TCP and then reset or impersonate the TLS server.
            // Keep chain validation enabled, but skip hostname matching because
            // this is a candidate-IP probe and SNI still uses the original host.
            let connector = SslConnector::builder(SslMethod::tls())
                .context("creating DNS probe TLS connector")?
                .build();
            let mut config = connector.configure().context("configuring DNS probe TLS")?;
            config.set_use_server_name_indication(true);
            config.set_verify_hostname(false);

            let ssl = config
                .into_ssl(host)
                .with_context(|| format!("setting TLS probe server name {host}"))?;
            let mut tls = SslStream::new(ssl, stream).context("creating DNS probe TLS stream")?;
            Pin::new(&mut tls)
                .connect()
                .await
                .with_context(|| format!("TLS probe handshake failed for {host} at {endpoint}"))?;
            Ok::<(), anyhow::Error>(())
        })
        .await;

        let elapsed = match result {
            Ok(Ok(())) => Some(started.elapsed()),
            Ok(Err(error)) => {
                tracing::debug!(%host, %endpoint, %error, "DNS candidate TLS probe failed");
                None
            }
            Err(_) => {
                tracing::debug!(%host, %endpoint, "DNS candidate TCP/TLS probe timed out");
                None
            }
        };

        let ttl = if elapsed.is_some() {
            GOOD_LATENCY_TTL
        } else {
            // Failed probes are short-lived; request-stage failures have their
            // own 90-second circuit breaker in the resolver.
            FAILED_LOCAL_TTL
        };
        self.latency.lock().await.insert(
            key,
            Timed {
                value: elapsed,
                expires: Instant::now() + ttl,
            },
        );
        elapsed
    }
}

fn is_local_network_problem(error: &io::Error) -> bool {
    matches!(error.raw_os_error(), Some(100) | Some(101))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_network_errors_use_linux_errno_values() {
        assert!(is_local_network_problem(&io::Error::from_raw_os_error(100)));
        assert!(is_local_network_problem(&io::Error::from_raw_os_error(101)));
        assert!(!is_local_network_problem(&io::Error::from_raw_os_error(111)));
    }

    #[tokio::test]
    async fn all_failed_probes_preserve_candidate_addresses() {
        let service = AddressService::default();
        let candidates = vec![
            IpAddr::from([192, 0, 2, 1]),
            IpAddr::from([192, 0, 2, 2]),
        ];
        let ordered = service
            .order_addresses("example.invalid", 443, candidates.clone(), &[])
            .await;
        assert_eq!(ordered, candidates);
    }
}
