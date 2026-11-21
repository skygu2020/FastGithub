use crate::{
    address_service::AddressService,
    dns::DnsClient,
    persistence::{EndpointRecord, Persistence},
};
use anyhow::Result;
use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::RwLock,
    time::Instant,
};

const ENDPOINT_REFRESH_TTL: Duration = Duration::from_secs(10 * 60);
const FAILED_ADDRESS_TTL: Duration = Duration::from_secs(90);
const MAX_CANDIDATES: usize = 16;

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
    endpoints: Arc<RwLock<HashMap<EndpointRecord, EndpointState>>>,
    failed_ips: Arc<RwLock<HashMap<IpAddr, Instant>>>,
}

impl DomainResolver {
    pub async fn new(dns: DnsClient, persistence: Persistence) -> Result<Self> {
        let known = persistence.read_endpoints().await?;
        let endpoints = known
            .into_iter()
            .map(|item| {
                (
                    item,
                    EndpointState {
                        addresses: Vec::new(),
                        refreshed_at: Instant::now(),
                    },
                )
            })
            .collect();
        Ok(Self {
            dns,
            persistence,
            addresses: AddressService::default(),
            endpoints: Arc::new(RwLock::new(endpoints)),
            failed_ips: Arc::new(RwLock::new(HashMap::new())),
        })
    }

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
                    let usable = self.exclude_failed(&state.addresses).await;
                    if !usable.is_empty() {
                        return Ok(usable);
                    }
                }
                state.addresses.clone()
            } else {
                Vec::new()
            }
        };

        if !self.endpoints.read().await.contains_key(&endpoint) {
            let mut endpoints = self.endpoints.write().await;
            endpoints.entry(endpoint.clone()).or_insert_with(|| EndpointState {
                addresses: Vec::new(),
                refreshed_at: Instant::now() - ENDPOINT_REFRESH_TTL,
            });
            let snapshot: Vec<_> = endpoints.keys().cloned().collect();
            drop(endpoints);
            if let Err(error) = self.persistence.write_endpoints(&snapshot).await {
                tracing::warn!(%error, "could not persist DNS endpoint list");
            }
        }

        let fresh = match self.dns.resolve(&endpoint.host, port, false).await {
            Ok(addresses) => addresses,
            Err(error) if !previous.is_empty() => {
                tracing::warn!(host = %endpoint.host, %port, %error, "DNS refresh failed; retaining previous candidates");
                return Ok(self.exclude_failed(&previous).await);
            }
            Err(error) => return Err(error),
        };

        let mut ordered = self
            .addresses
            .order_addresses(&endpoint.host, port, fresh, &previous)
            .await;
        ordered = merge_candidates(ordered, &previous);
        let ordered = self.exclude_failed(&ordered).await;

        self.endpoints.write().await.insert(
            endpoint,
            EndpointState {
                addresses: ordered.clone(),
                refreshed_at: Instant::now(),
            },
        );
        Ok(ordered)
    }

    /// Refresh every endpoint loaded from or discovered during this process.
    /// Per-endpoint failures do not abort the whole refresh; old candidates survive.
    pub async fn test_speed(&self) -> Result<()> {
        let keys: Vec<_> = self.endpoints.read().await.keys().cloned().collect();
        for endpoint in keys {
            let old = self
                .endpoints
                .read()
                .await
                .get(&endpoint)
                .map(|state| state.addresses.clone())
                .unwrap_or_default();

            let fresh = match self.dns.resolve(&endpoint.host, endpoint.port, false).await {
                Ok(addresses) => addresses,
                Err(error) => {
                    tracing::warn!(host = %endpoint.host, port = endpoint.port, %error, "DNS refresh failed; retaining old candidates");
                    continue;
                }
            };

            let probed = self
                .addresses
                .order_addresses(&endpoint.host, endpoint.port, fresh, &old)
                .await;
            let merged = merge_candidates(probed, &old);
            let ordered = self.exclude_failed(&merged).await;

            if old.iter().take(3).ne(ordered.iter().take(3)) {
                tracing::info!(
                    endpoint = %format!("{}:{}", endpoint.host, endpoint.port),
                    addresses = ?ordered.iter().take(3).collect::<Vec<_>>(),
                    "DNS endpoint speed order changed"
                );
            }

            self.endpoints.write().await.insert(
                endpoint,
                EndpointState {
                    addresses: ordered,
                    refreshed_at: Instant::now(),
                },
            );
        }
        Ok(())
    }

    /// Mark an IP as failed for 90 seconds and remove it from current candidate lists.
    pub async fn report_failure(&self, address: IpAddr) {
        self.failed_ips
            .write()
            .await
            .insert(address, Instant::now() + FAILED_ADDRESS_TTL);
        for state in self.endpoints.write().await.values_mut() {
            state.addresses.retain(|candidate| *candidate != address);
        }
    }

    /// Remember a successful endpoint IP so subsequent requests try it first.
    pub async fn report_success(&self, host: &str, port: u16, address: IpAddr) {
        let endpoint = EndpointRecord {
            host: host.trim_end_matches('.').to_ascii_lowercase(),
            port,
        };
        if let Some(state) = self.endpoints.write().await.get_mut(&endpoint) {
            if let Some(index) = state.addresses.iter().position(|candidate| *candidate == address) {
                state.addresses.remove(index);
                state.addresses.insert(0, address);
            }
        }
    }

    async fn exclude_failed(&self, addresses: &[IpAddr]) -> Vec<IpAddr> {
        let now = Instant::now();
        let mut failed = self.failed_ips.write().await;
        failed.retain(|_, expires| *expires > now);
        addresses
            .iter()
            .copied()
            .filter(|address| !failed.contains_key(address))
            .collect()
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

fn merge_candidates(new_addresses: Vec<IpAddr>, old_addresses: &[IpAddr]) -> Vec<IpAddr> {
    let mut merged = Vec::with_capacity((new_addresses.len() + old_addresses.len()).min(MAX_CANDIDATES));
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
}
