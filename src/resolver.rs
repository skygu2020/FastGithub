use crate::{
    address_service::AddressService,
    dns::DnsClient,
    flow::FlowAnalyzer,
    hosts::{HostsRefreshResult, HostsService},
    persistence::{AddressRecord, EndpointRecord, Persistence},
};
use anyhow::Result;
use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{RwLock, Semaphore},
    task::JoinSet,
    time::Instant,
};

const ENDPOINT_REFRESH_TTL: Duration = Duration::from_secs(10 * 60);
const FAILED_ADDRESS_TTL: Duration = Duration::from_secs(90);
const MAX_CANDIDATES: usize = 8;

#[derive(Clone)]
struct EndpointState {
    addresses: Vec<IpAddr>,
    refreshed_at: Instant,
}

#[derive(Clone)]
pub struct DomainResolver {
    dns: DnsClient,
    persistence: Persistence,
    addresses: AddressService,
    hosts: HostsService,
    flow: FlowAnalyzer,
    endpoints: Arc<RwLock<HashMap<EndpointRecord, EndpointState>>>,
    failed_ips: Arc<RwLock<HashMap<(String, u16, IpAddr), Instant>>>,
    refresh_lock: Arc<tokio::sync::Mutex<()>>,
}

impl DomainResolver {
    pub async fn new(dns: DnsClient, persistence: Persistence) -> Result<Self> {
        let known = persistence.read_endpoints().await?;
        let saved_addresses = persistence.read_addresses().await?;
        let mut endpoints: HashMap<EndpointRecord, EndpointState> = known
            .into_iter()
            .map(|item| {
                (
                    item,
                    EndpointState {
                        addresses: Vec::new(),
                        refreshed_at: Instant::now() - ENDPOINT_REFRESH_TTL,
                    },
                )
            })
            .collect();

        for record in saved_addresses {
            let endpoint = EndpointRecord {
                host: record.host.trim_end_matches('.').to_ascii_lowercase(),
                port: record.port,
            };
            endpoints.insert(
                endpoint,
                EndpointState {
                    addresses: record.addresses,
                    // Reuse the last verified candidates immediately after restart;
                    // the background refresh revalidates them without delaying requests.
                    refreshed_at: Instant::now(),
                },
            );
        }

        Ok(Self {
            dns,
            persistence,
            addresses: AddressService::default(),
            hosts: HostsService::new()?,
            flow: FlowAnalyzer::default(),
            endpoints: Arc::new(RwLock::new(endpoints)),
            failed_ips: Arc::new(RwLock::new(HashMap::new())),
            refresh_lock: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    /// Resolve candidates in the same priority order as the reference service:
    /// online hosts first, then configured DNS, with built-in GitHub seeds added
    /// as fallback candidates at both stages.
    pub async fn resolve(&self, host: &str, port: u16) -> Result<Vec<IpAddr>> {
        let endpoint = EndpointRecord {
            host: host.trim_end_matches('.').to_ascii_lowercase(),
            port,
        };

        let previous = {
            let endpoints = self.endpoints.read().await;
            if let Some(state) = endpoints.get(&endpoint) {
                if !state.addresses.is_empty()
                    && state.refreshed_at.elapsed() < ENDPOINT_REFRESH_TTL
                {
                    let usable = self.exclude_failed(&endpoint.host, endpoint.port, &state.addresses).await;
                    if !usable.is_empty() {
                        return Ok(usable);
                    }
                }
                state.addresses.clone()
            } else {
                Vec::new()
            }
        };

        self.ensure_endpoint(&endpoint).await;

        let hosts = self.hosts.addresses(&endpoint.host).await;
        let hosts_candidates = append_seed_addresses(&endpoint.host, hosts);
        if !hosts_candidates.is_empty() {
            let probed = self
                .addresses
                .order_addresses(&endpoint.host, port, hosts_candidates, &previous)
                .await;
            let ordered = self.exclude_failed(&endpoint.host, endpoint.port, &merge_candidates(probed, &previous)).await;
            if !ordered.is_empty() {
                self.store_endpoint(endpoint, ordered.clone()).await;
                return Ok(ordered);
            }
        }

        let dns_result = self.dns.resolve(&endpoint.host, port, false).await;
        let dns_candidates = match dns_result {
            Ok(addresses) => append_seed_addresses(&endpoint.host, addresses),
            Err(error) if !previous.is_empty() => {
                tracing::warn!(
                    host = %endpoint.host,
                    port,
                    %error,
                    "DNS refresh failed; retaining previous candidates"
                );
                let retained = self.exclude_failed(&endpoint.host, endpoint.port, &previous).await;
                self.store_endpoint(endpoint.clone(), retained.clone()).await;
                return Ok(retained);
            }
            Err(error) => {
                let seeds = seed_addresses(&endpoint.host);
                if seeds.is_empty() {
                    return Err(error);
                }
                tracing::warn!(host = %endpoint.host, %error, "DNS failed; using built-in seed candidates");
                seeds
            }
        };

        let probed = self
            .addresses
            .order_addresses(&endpoint.host, port, dns_candidates, &previous)
            .await;
        let ordered = self.exclude_failed(&endpoint.host, endpoint.port, &merge_candidates(probed, &previous)).await;
        self.store_endpoint(endpoint, ordered.clone()).await;
        Ok(ordered)
    }

    async fn ensure_endpoint(&self, endpoint: &EndpointRecord) {
        let mut endpoints = self.endpoints.write().await;
        if endpoints.contains_key(endpoint) {
            return;
        }
        endpoints.insert(
            endpoint.clone(),
            EndpointState {
                addresses: Vec::new(),
                refreshed_at: Instant::now() - ENDPOINT_REFRESH_TTL,
            },
        );
        let snapshot: Vec<_> = endpoints.keys().cloned().collect();
        drop(endpoints);
        if let Err(error) = self.persistence.write_endpoints(&snapshot).await {
            tracing::warn!(%error, "could not persist DNS endpoint list");
        }
    }

    async fn store_endpoint(&self, endpoint: EndpointRecord, addresses: Vec<IpAddr>) {
        self.endpoints.write().await.insert(
            endpoint,
            EndpointState {
                addresses,
                refreshed_at: Instant::now(),
            },
        );
        self.save_addresses().await;
    }

    /// Force a full refresh: invalidate DNS/probe caches, update online hosts,
    /// then re-resolve and benchmark every known endpoint.
    pub async fn refresh_all(&self) -> Result<HostsRefreshResult> {
        let _refresh_guard = self.refresh_lock.lock().await;
        self.dns.clear_cache().await;
        self.addresses.clear_cache().await;
        let hosts = self.hosts.refresh().await;
        self.test_speed().await?;
        Ok(hosts)
    }

    /// Refresh every known endpoint with bounded concurrency. A per-endpoint
    /// DNS failure never erases its previous candidate set.
    pub async fn test_speed(&self) -> Result<()> {
        let keys: Vec<_> = self.endpoints.read().await.keys().cloned().collect();
        let semaphore = Arc::new(Semaphore::new(12));
        let mut jobs = JoinSet::new();

        for endpoint in keys {
            let resolver = self.clone();
            let semaphore = Arc::clone(&semaphore);
            jobs.spawn(async move {
                let Ok(_permit) = semaphore.acquire_owned().await else {
                    return;
                };
                resolver.refresh_endpoint(endpoint).await;
            });
        }

        while let Some(result) = jobs.join_next().await {
            if let Err(error) = result {
                tracing::warn!(%error, "DNS endpoint refresh task failed");
            }
        }

        self.save_addresses().await;
        Ok(())
    }

    async fn refresh_endpoint(&self, endpoint: EndpointRecord) {
        let old = self
            .endpoints
            .read()
            .await
            .get(&endpoint)
            .map(|state| state.addresses.clone())
            .unwrap_or_default();

        let hosts = self.hosts.addresses(&endpoint.host).await;
        let candidates = if hosts.is_empty() {
            match self.dns.resolve(&endpoint.host, endpoint.port, false).await {
                Ok(addresses) => append_seed_addresses(&endpoint.host, addresses),
                Err(error) => {
                    tracing::warn!(
                        host = %endpoint.host,
                        port = endpoint.port,
                        %error,
                        "DNS refresh failed; retaining old candidates"
                    );
                    if !old.is_empty() {
                        self.endpoints.write().await.insert(
                            endpoint,
                            EndpointState {
                                addresses: old,
                                refreshed_at: Instant::now(),
                            },
                        );
                    }
                    return;
                }
            }
        } else {
            append_seed_addresses(&endpoint.host, hosts)
        };

        let probed = self
            .addresses
            .order_addresses(&endpoint.host, endpoint.port, candidates, &old)
            .await;
        let merged = merge_candidates(probed, &old);
        let ordered = self.exclude_failed(&endpoint.host, endpoint.port, &merged).await;

        if old.iter().take(3).ne(ordered.iter().take(3)) {
            tracing::info!(
                endpoint = %format!("{}:{}", endpoint.host, endpoint.port),
                addresses = ?ordered.iter().take(3).collect::<Vec<_>>(),
                "DNS endpoint speed order changed"
            );
        }

        // Never replace a known-good candidate list with an empty result.
        let next = if ordered.is_empty() { old } else { ordered };
        self.endpoints.write().await.insert(
            endpoint,
            EndpointState {
                addresses: next,
                refreshed_at: Instant::now(),
            },
        );
    }

    /// Mark an IP as failed for 90 seconds and remove it from current candidate lists.
    pub async fn report_failure(&self, host: &str, port: u16, address: IpAddr) {
        let host = host.trim_end_matches('.').to_ascii_lowercase();
        self.failed_ips.write().await.insert(
            (host.clone(), port, address),
            Instant::now() + FAILED_ADDRESS_TTL,
        );
        let endpoint = EndpointRecord { host, port };
        if let Some(state) = self.endpoints.write().await.get_mut(&endpoint) {
            state.addresses.retain(|candidate| *candidate != address);
        }
    }

    /// Remember a successful endpoint IP so subsequent requests try it first.
    pub async fn report_success(&self, host: &str, port: u16, address: IpAddr) {
        let normalized_host = host.trim_end_matches('.').to_ascii_lowercase();
        self.failed_ips.write().await.remove(&(normalized_host.clone(), port, address));
        let endpoint = EndpointRecord {
            host: normalized_host,
            port,
        };
        if let Some(state) = self.endpoints.write().await.get_mut(&endpoint) {
            if let Some(index) = state.addresses.iter().position(|candidate| *candidate == address) {
                state.addresses.remove(index);
                state.addresses.insert(0, address);
            }
        }
    }

    async fn save_addresses(&self) {
        let snapshot: Vec<_> = self
            .endpoints
            .read()
            .await
            .iter()
            .map(|(endpoint, state)| AddressRecord {
                host: endpoint.host.clone(),
                port: endpoint.port,
                addresses: state.addresses.clone(),
            })
            .collect();
        if let Err(error) = self.persistence.write_addresses(&snapshot).await {
            tracing::warn!(%error, "could not persist verified DNS candidate addresses");
        }
    }

    async fn exclude_failed(&self, host: &str, port: u16, addresses: &[IpAddr]) -> Vec<IpAddr> {
        let now = Instant::now();
        let host = host.trim_end_matches('.').to_ascii_lowercase();
        let mut failed = self.failed_ips.write().await;
        failed.retain(|_, expires| *expires > now);
        addresses
            .iter()
            .copied()
            .filter(|address| !failed.contains_key(&(host.clone(), port, *address)))
            .collect()
    }

    pub fn flow_analyzer(&self) -> FlowAnalyzer {
        self.flow.clone()
    }

    pub async fn endpoints(&self) -> Vec<SocketAddr> {
        self.endpoints
            .read()
            .await
            .keys()
            .filter_map(EndpointRecord::socket_addr)
            .collect()
    }
}

fn append_seed_addresses(host: &str, mut addresses: Vec<IpAddr>) -> Vec<IpAddr> {
    for address in seed_addresses(host) {
        if !addresses.contains(&address) {
            addresses.push(address);
        }
    }
    addresses
}

fn seed_addresses(host: &str) -> Vec<IpAddr> {
    let values: &[&str] = match host.trim_end_matches('.').to_ascii_lowercase().as_str() {
        "github.com" => &[
            "20.27.177.113", "20.205.243.166", "20.29.134.23",
            "20.26.156.215", "20.233.83.145", "140.82.113.3",
        ],
        "api.github.com" => &[
            "20.27.177.113", "20.205.243.166", "20.205.243.168", "20.29.134.23",
        ],
        "gist.github.com" => &[
            "20.205.243.166", "20.27.177.113", "20.26.156.215",
        ],
        "codeload.github.com" => &[
            "20.205.243.165", "20.27.177.113", "20.26.156.215",
        ],
        "raw.githubusercontent.com" | "avatars.githubusercontent.com" | "camo.githubusercontent.com" => &[
            "185.199.110.133", "185.199.109.133", "185.199.111.133", "185.199.108.133",
        ],
        "github.githubassets.com" => &[
            "185.199.108.154", "185.199.110.154", "185.199.109.154", "185.199.111.154",
        ],
        "github.io" => &[
            "185.199.110.153", "185.199.111.153", "185.199.109.153", "185.199.108.153",
        ],
        _ => &[],
    };
    values.iter().filter_map(|value| value.parse().ok()).collect()
}

fn merge_candidates(new_addresses: Vec<IpAddr>, old_addresses: &[IpAddr]) -> Vec<IpAddr> {
    let mut merged =
        Vec::with_capacity((new_addresses.len() + old_addresses.len()).min(MAX_CANDIDATES));
    for address in new_addresses.into_iter().chain(old_addresses.iter().copied()) {
        if !merged.contains(&address) {
            merged.push(address);
            if merged.len() == MAX_CANDIDATES {
                break;
            }
        }
    }
    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merges_new_candidates_before_old_candidates_without_duplicates() {
        let fresh = vec![
            "192.0.2.2".parse().unwrap(),
            "192.0.2.3".parse().unwrap(),
        ];
        let old = vec![
            "192.0.2.1".parse().unwrap(),
            "192.0.2.2".parse().unwrap(),
        ];
        assert_eq!(
            merge_candidates(fresh, &old),
            vec![
                "192.0.2.2".parse::<IpAddr>().unwrap(),
                "192.0.2.3".parse().unwrap(),
                "192.0.2.1".parse().unwrap(),
            ]
        );
    }

    #[test]
    fn candidate_merge_is_bounded() {
        let fresh = (1..=20).map(|last| IpAddr::from([192, 0, 2, last])).collect();
        assert_eq!(merge_candidates(fresh, &[]).len(), MAX_CANDIDATES);
    }

    #[test]
    fn github_seed_pool_contains_core_service_hosts() {
        for host in [
            "github.com",
            "api.github.com",
            "gist.github.com",
            "codeload.github.com",
            "raw.githubusercontent.com",
            "avatars.githubusercontent.com",
            "camo.githubusercontent.com",
            "github.githubassets.com",
            "github.io",
        ] {
            assert!(!seed_addresses(host).is_empty(), "missing seed for {host}");
        }
        assert!(seed_addresses("example.com").is_empty());
    }
}
