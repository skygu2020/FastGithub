use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, net::SocketAddr, path::{Path, PathBuf}, sync::Arc};
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

#[derive(Clone)]
pub struct Persistence {
    path: Arc<PathBuf>,
    lock: Arc<Mutex<()>>,
}

impl Persistence {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: Arc::new(path.into()), lock: Arc::new(Mutex::new(())) }
    }

    pub async fn read_endpoints(&self) -> Result<Vec<EndpointRecord>> {
        let _guard = self.lock.lock().await;
        let path = self.path.as_path();
        if !tokio::fs::try_exists(path).await? { return Ok(Vec::new()); }
        let bytes = tokio::fs::read(path).await.with_context(|| format!("reading {}", path.display()))?;
        let records: Vec<EndpointRecord> = serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing {}", path.display()))?;
        let mut unique = BTreeSet::new();
        Ok(records.into_iter().filter(|item| !item.host.is_empty() && item.port != 0 && unique.insert(item.clone())).collect())
    }

    pub async fn write_endpoints(&self, endpoints: &[EndpointRecord]) -> Result<()> {
        let _guard = self.lock.lock().await;
        let mut unique = BTreeSet::new();
        let records: Vec<_> = endpoints.iter()
            .filter(|item| !item.host.is_empty() && item.port != 0)
            .filter(|item| unique.insert((*item).clone()))
            .cloned().collect();
        let bytes = serde_json::to_vec_pretty(&records)?;
        let path = self.path.as_path();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() { tokio::fs::create_dir_all(parent).await?; }
        }
        let temp = temporary_path(path);
        tokio::fs::write(&temp, bytes).await.with_context(|| format!("writing {}", temp.display()))?;
        if let Err(error) = tokio::fs::rename(&temp, path).await {
            let _ = tokio::fs::remove_file(&temp).await;
            return Err(error).with_context(|| format!("replacing {}", path.display()));
        }
        Ok(())
    }
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
        let a = EndpointRecord { host: "Example.com".into(), port: 443 };
        let b = EndpointRecord { host: "example.com".into(), port: 443 };
        assert_ne!(a, b);
        assert_eq!(a.socket_addr(), None);
    }
}
