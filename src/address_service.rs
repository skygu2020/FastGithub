use std::{collections::HashMap, io, net::{IpAddr, SocketAddr}, sync::Arc, time::Duration};
use tokio::{net::TcpStream, sync::Mutex, time::{timeout, Instant}};

const DOMAIN_IP_TTL: Duration = Duration::from_secs(10 * 60);
const GOOD_LATENCY_TTL: Duration = Duration::from_secs(5 * 60);
const FAILED_LOCAL_TTL: Duration = Duration::from_secs(60);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
struct Timed<T> { value: T, expires: Instant }

#[derive(Clone)]
pub struct AddressService {
    domain_ips: Arc<Mutex<HashMap<(String, IpAddr), Instant>>>,
    latency: Arc<Mutex<HashMap<SocketAddr, Timed<Option<Duration>>>>>,
}

impl Default for AddressService {
    fn default() -> Self {
        Self { domain_ips: Arc::new(Mutex::new(HashMap::new())), latency: Arc::new(Mutex::new(HashMap::new())) }
    }
}

impl AddressService {
    /// Keep previously selected addresses only while their domain/IP association is fresh.
    pub async fn order_addresses(&self, host: &str, port: u16, fresh: Vec<IpAddr>, previous: &[IpAddr]) -> Vec<IpAddr> {
        let now = Instant::now();
        let key = host.trim_end_matches('.').to_ascii_lowercase();
        let mut history = self.domain_ips.lock().await;
        history.retain(|_, expires| *expires > now);
        let mut addresses = Vec::new();
        for ip in previous {
            if history.get(&(key.clone(), *ip)).is_some_and(|expires| *expires > now) {
                addresses.push(*ip);
            }
        }
        for ip in fresh {
            let item = (key.clone(), ip);
            history.insert(item, now + DOMAIN_IP_TTL);
            if !addresses.contains(&ip) { addresses.push(ip); }
        }
        drop(history);

        let mut jobs = tokio::task::JoinSet::new();
        for ip in addresses.iter().copied() {
            let this = self.clone();
            jobs.spawn(async move { (ip, this.measure(SocketAddr::new(ip, port)).await) });
        }
        let mut measured = Vec::new();
        while let Some(result) = jobs.join_next().await {
            if let Ok((ip, Some(elapsed))) = result { measured.push((ip, elapsed)); }
        }
        measured.sort_by_key(|item| item.1);
        measured.into_iter().map(|item| item.0).collect()
    }

    async fn measure(&self, endpoint: SocketAddr) -> Option<Duration> {
        let now = Instant::now();
        if let Some(entry) = self.latency.lock().await.get(&endpoint) {
            if entry.expires > now { return entry.value; }
        }
        let started = Instant::now();
        let result = match timeout(CONNECT_TIMEOUT, TcpStream::connect(endpoint)).await {
            Ok(Ok(stream)) => { drop(stream); Some(started.elapsed()) }
            Ok(Err(error)) => {
                let ttl = if is_local_network_problem(&error) { FAILED_LOCAL_TTL } else { GOOD_LATENCY_TTL };
                self.latency.lock().await.insert(endpoint, Timed { value: None, expires: now + ttl });
                return None;
            }
            Err(_) => None,
        };
        self.latency.lock().await.insert(endpoint, Timed { value: result, expires: now + GOOD_LATENCY_TTL });
        result
    }
}

fn is_local_network_problem(error: &io::Error) -> bool {
    matches!(error.raw_os_error(), Some(100) | Some(101))
}
