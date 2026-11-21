use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "camelCase")]
pub struct EndpointRecord {
    pub host: String,
    pub port: u16,
}

impl EndpointRecord {
    pub fn socket_addr(&self) -> Option<SocketAddr> {
        format!("{}:{}", self.host, self.port).parse().ok()
    }
}

/// A previously verified candidate set for one DNS endpoint.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AddressRecord {
    pub host: String,
    pub port: u16,
    #[serde(default)]
    pub addresses: Vec<IpAddr>,
}

#[derive(Clone)]
pub struct Persistence {
    path: Arc<PathBuf>,
    addresses_path: Arc<PathBuf>,
    lock: Arc<Mutex<()>>,
}

impl Persistence {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        let addresses_path = path.with_file_name("dnsaddresses.json");
        Self {
            path: Arc::new(path),
            addresses_path: Arc::new(addresses_path),
            lock: Arc::new(Mutex::new(())),
        }
    }

    pub async fn read_endpoints(&self) -> Result<Vec<EndpointRecord>> {
        let _guard = self.lock.lock().await;
        let path = self.path.as_path();
        if !tokio::fs::try_exists(path).await? {
            return Ok(Vec::new());
        }
        let bytes = tokio::fs::read(path)
            .await
            .with_context(|| format!("reading {}", path.display()))?;
        let records: Vec<EndpointRecord> = serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing {}", path.display()))?;
        let mut unique = BTreeSet::new();
        Ok(records
            .into_iter()
            .filter(|item| {
                !item.host.is_empty() && item.port != 0 && unique.insert(item.clone())
            })
            .collect())
    }

    pub async fn write_endpoints(&self, endpoints: &[EndpointRecord]) -> Result<()> {
        let _guard = self.lock.lock().await;
        let mut unique = BTreeSet::new();
        let records: Vec<_> = endpoints
            .iter()
            .filter(|item| !item.host.is_empty() && item.port != 0)
            .filter(|item| unique.insert((*item).clone()))
            .cloned()
            .collect();
        let bytes = serde_json::to_vec_pretty(&records)?;
        write_atomically(self.path.as_path(), &bytes).await
    }

    pub async fn read_addresses(&self) -> Result<Vec<AddressRecord>> {
        let _guard = self.lock.lock().await;
        let path = self.addresses_path.as_path();
        if !tokio::fs::try_exists(path).await? {
            return Ok(Vec::new());
        }
        let bytes = tokio::fs::read(path)
            .await
            .with_context(|| format!("reading {}", path.display()))?;
        let records: Vec<AddressRecord> = serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing {}", path.display()))?;
        let mut unique = BTreeSet::new();
        Ok(records
            .into_iter()
            .filter_map(|mut item| {
                if item.host.is_empty() || item.port == 0 {
                    return None;
                }
                item.addresses.dedup();
                item.addresses.retain(|ip| !ip.is_unspecified() && !ip.is_multicast());
                if unique.insert((item.host.clone(), item.port)) {
                    Some(item)
                } else {
                    None
                }
            })
            .collect())
    }

    pub async fn write_addresses(&self, addresses: &[AddressRecord]) -> Result<()> {
        let _guard = self.lock.lock().await;
        let mut unique = BTreeSet::new();
        let records: Vec<_> = addresses
            .iter()
            .filter(|item| !item.host.is_empty() && item.port != 0)
            .filter_map(|item| {
                if !unique.insert((item.host.clone(), item.port)) {
                    return None;
                }
                let mut record = item.clone();
                let mut seen = BTreeSet::new();
                record.addresses.retain(|ip| {
                    !ip.is_unspecified() && !ip.is_multicast() && seen.insert(*ip)
                });
                Some(record)
            })
            .collect();
        let bytes = serde_json::to_vec_pretty(&records)?;
        write_atomically(self.addresses_path.as_path(), &bytes).await
    }
}

async fn write_atomically(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            tokio::fs::create_dir_all(parent).await?;
        }
    }
    let temp = temporary_path(path);
    tokio::fs::write(&temp, bytes)
        .await
        .with_context(|| format!("writing {}", temp.display()))?;
    if let Err(error) = tokio::fs::rename(&temp, path).await {
        let _ = tokio::fs::remove_file(&temp).await;
        return Err(error).with_context(|| format!("replacing {}", path.display()));
    }
    Ok(())
}

fn temporary_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".tmp");
    PathBuf::from(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_records_are_case_preserving_and_orderable() {
        let a = EndpointRecord {
            host: "Example.com".into(),
            port: 443,
        };
        let b = EndpointRecord {
            host: "example.com".into(),
            port: 443,
        };
        assert_ne!(a, b);
        assert_eq!(a.socket_addr(), None);
    }

    #[tokio::test]
    async fn address_records_round_trip_deduplicated_candidates() {
        let directory = std::env::temp_dir().join(format!(
            "fastgithub-persistence-{}",
            std::process::id()
        ));
        let persistence = Persistence::new(directory.join("dnsendpoints.json"));
        let record = AddressRecord {
            host: "example.com".into(),
            port: 443,
            addresses: vec![
                "192.0.2.1".parse().unwrap(),
                "192.0.2.1".parse().unwrap(),
                "2001:db8::1".parse().unwrap(),
            ],
        };
        persistence.write_addresses(&[record.clone()]).await.unwrap();
        let loaded = persistence.read_addresses().await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].addresses.len(), 2);
        let _ = tokio::fs::remove_file(directory.join("dnsaddresses.json")).await;
        let _ = tokio::fs::remove_dir_all(directory).await;
    }
}
