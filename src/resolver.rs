use crate::{address_service::AddressService, dns::DnsClient, persistence::{EndpointRecord, Persistence}};
use anyhow::Result;
use std::{collections::HashMap, net::{IpAddr, SocketAddr}, sync::Arc};
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct DomainResolver {
    dns: DnsClient,
    persistence: Persistence,
    addresses: AddressService,
    endpoints: Arc<RwLock<HashMap<EndpointRecord, Vec<IpAddr>>>>,
}

impl DomainResolver {
    pub async fn new(dns: DnsClient, persistence: Persistence) -> Result<Self> {
        let known = persistence.read_endpoints().await?;
        let endpoints = known.into_iter().map(|item| (item, Vec::new())).collect();
        Ok(Self { dns, persistence, addresses: AddressService::default(), endpoints: Arc::new(RwLock::new(endpoints)) })
    }

    pub async fn resolve(&self, host: &str, port: u16) -> Result<Vec<IpAddr>> {
        let endpoint = EndpointRecord { host: host.trim_end_matches('.').to_ascii_lowercase(), port };
        let previous = {
            let mut endpoints = self.endpoints.write().await;
            if let Some(found) = endpoints.get(&endpoint) {
                if !found.is_empty() { return Ok(found.clone()); }
            } else {
                endpoints.insert(endpoint.clone(), Vec::new());
                let snapshot: Vec<_> = endpoints.keys().cloned().collect();
                if let Err(error) = self.persistence.write_endpoints(&snapshot).await {
                    tracing::warn!(%error, "could not persist DNS endpoint list");
                }
            }
            endpoints.get(&endpoint).cloned().unwrap_or_default()
        };
        let fresh = self.dns.resolve(&endpoint.host, port, false).await?;
        let ordered = self.addresses.order_addresses(&endpoint.host, port, fresh, &previous).await;
        self.endpoints.write().await.insert(endpoint, ordered.clone());
        Ok(ordered)
    }

    /// Refresh every endpoint loaded from or discovered during this process.
    pub async fn test_speed(&self) -> Result<()> {
        let keys: Vec<_> = self.endpoints.read().await.keys().cloned().collect();
        for endpoint in keys {
            let fresh = self.dns.resolve(&endpoint.host, endpoint.port, false).await?;
            let old = self.endpoints.read().await.get(&endpoint).cloned().unwrap_or_default();
            let ordered = self.addresses.order_addresses(&endpoint.host, endpoint.port, fresh, &old).await;
            if old.iter().take(3).ne(ordered.iter().take(3)) {
                tracing::info!(endpoint = %format!("{}:{}", endpoint.host, endpoint.port), addresses = ?ordered.iter().take(3).collect::<Vec<_>>(), "DNS endpoint speed order changed");
            }
            self.endpoints.write().await.insert(endpoint, ordered);
        }
        Ok(())
    }

    pub async fn endpoints(&self) -> Vec<SocketAddr> {
        self.endpoints.read().await.keys().filter_map(EndpointRecord::socket_addr).collect()
    }
}
