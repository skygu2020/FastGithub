use std::{collections::HashMap, net::IpAddr, sync::Arc, time::Duration};

use reqwest::{header::USER_AGENT, Client};
use tokio::{sync::RwLock, task::JoinSet};

const HOSTS_URLS: [&str; 2] = [
    "https://raw.hellogithub.com/hosts.json",
    "https://hosts.gitcdn.top/hosts.txt",
];

#[derive(Debug, Clone, Default)]
pub struct HostsRefreshResult {
    pub success: bool,
    pub domain_count: usize,
    pub errors: Vec<String>,
}

#[derive(Clone)]
pub struct HostsService {
    client: Client,
    // Keep each source's last-known-good snapshot independently. A partial
    // refresh must not erase entries contributed only by a failed source.
    source_mappings: Arc<RwLock<HashMap<String, HashMap<String, Vec<IpAddr>>>>>,
    mapping: Arc<RwLock<HashMap<String, Vec<IpAddr>>>>,
}

impl HostsService {
    pub fn new() -> anyhow::Result<Self> {
        let client = Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(20))
            .build()?;
        Ok(Self {
            client,
            source_mappings: Arc::new(RwLock::new(HashMap::new())),
            mapping: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Fetch all configured public hosts sources concurrently and merge them.
    /// If every source fails, keep the last known-good mapping.
    pub async fn refresh(&self) -> HostsRefreshResult {
        let mut jobs = JoinSet::new();
        for url in HOSTS_URLS {
            let client = self.client.clone();
            jobs.spawn(async move {
                let result = async {
                    let request = client.get(url);
                    // Try the product UA only for gitcdn; keep the other source unchanged
                    // so a successful refresh can identify whether this header helps.
                    let request = if url == "https://hosts.gitcdn.top/hosts.txt" {
                        request.header(USER_AGENT, "FastGithub/1.0")
                    } else {
                        request
                    };
                    let response = request.send().await?.error_for_status()?;
                    response.text().await
                }
                .await;
                (url.to_owned(), result)
            });
        }

        let mut successful_sources = Vec::new();
        let mut errors = Vec::new();
        let mut success_count = 0usize;

        while let Some(result) = jobs.join_next().await {
            match result {
                Ok((url, Ok(content))) => {
                    let parsed = parse_hosts(&content);
                    if parsed.is_empty() {
                        errors.push(format!("{url}: no valid host entries"));
                        continue;
                    }
                    success_count += 1;
                    successful_sources.push((url, parsed));
                }
                Ok((url, Err(error))) => {
                    let detail = format!(
                        "{url}: {error:#} (timeout={}, connect={}, status={:?})",
                        error.is_timeout(),
                        error.is_connect(),
                        error.status()
                    );
                    tracing::warn!(
                        url = %url,
                        %error,
                        timeout = error.is_timeout(),
                        connect = error.is_connect(),
                        status = ?error.status(),
                        "online hosts source download failed"
                    );
                    errors.push(detail);
                },
                Err(error) => errors.push(format!("hosts fetch task failed: {error}")),
            }
        }

        if success_count == 0 {
            tracing::warn!(errors = %errors.join("; "), "all online hosts sources failed; retaining previous mapping");
            return HostsRefreshResult {
                success: false,
                domain_count: self.mapping.read().await.len(),
                errors,
            };
        }

        // Replace only snapshots from sources that returned a valid mapping.
        // Failed/empty sources retain their own last-known-good data, while a
        // source that succeeds can still remove entries it no longer publishes.
        let (domain_count, cached_source_count) = {
            let mut source_mappings = self.source_mappings.write().await;
            for (url, parsed) in successful_sources {
                source_mappings.insert(url, parsed);
            }
            let merged = merge_source_mappings(&source_mappings);
            let domain_count = merged.len();
            let cached_source_count = source_mappings.len();
            *self.mapping.write().await = merged;
            (domain_count, cached_source_count)
        };
        tracing::info!(
            sources = success_count,
            cached_sources = cached_source_count,
            total_sources = HOSTS_URLS.len(),
            domains = domain_count,
            "online hosts mapping refreshed"
        );
        HostsRefreshResult {
            success: true,
            domain_count,
            errors,
        }
    }

    pub async fn addresses(&self, host: &str) -> Vec<IpAddr> {
        self.mapping
            .read()
            .await
            .get(&host.trim_end_matches('.').to_ascii_lowercase())
            .cloned()
            .unwrap_or_default()
    }

    pub async fn domain_count(&self) -> usize {
        self.mapping.read().await.len()
    }
}

fn merge_source_mappings(
    sources: &HashMap<String, HashMap<String, Vec<IpAddr>>>,
) -> HashMap<String, Vec<IpAddr>> {
    let mut merged: HashMap<String, Vec<IpAddr>> = HashMap::new();
    for source in sources.values() {
        for (host, addresses) in source {
            let target = merged.entry(host.clone()).or_default();
            for address in addresses {
                if !target.contains(address) {
                    target.push(*address);
                }
            }
        }
    }
    merged
}

fn parse_hosts(content: &str) -> HashMap<String, Vec<IpAddr>> {
    let trimmed = content.trim_start();
    if trimmed.starts_with('[') || trimmed.starts_with('{') {
        parse_json_hosts(content)
    } else {
        parse_text_hosts(content)
    }
}

fn parse_json_hosts(content: &str) -> HashMap<String, Vec<IpAddr>> {
    let mut mapping: HashMap<String, Vec<IpAddr>> = HashMap::new();
    let Ok(entries) = serde_json::from_str::<Vec<Vec<String>>>(content) else {
        return mapping;
    };

    for entry in entries {
        if entry.len() < 2 {
            continue;
        }
        let Ok(address) = entry[0].parse::<IpAddr>() else {
            continue;
        };
        insert_host(&mut mapping, &entry[1], address);
    }
    mapping
}

fn parse_text_hosts(content: &str) -> HashMap<String, Vec<IpAddr>> {
    let mut mapping: HashMap<String, Vec<IpAddr>> = HashMap::new();
    for line in content.lines() {
        let line = line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let mut fields = line.split_ascii_whitespace();
        let Some(address) = fields.next().and_then(|value| value.parse::<IpAddr>().ok()) else {
            continue;
        };
        if let Some(host) = fields.next() {
            insert_host(&mut mapping, host, address);
        }
    }
    mapping
}

fn insert_host(mapping: &mut HashMap<String, Vec<IpAddr>>, host: &str, address: IpAddr) {
    let host = host.trim().trim_end_matches('.').to_ascii_lowercase();
    if host.is_empty() || host.len() > 253 || host.parse::<IpAddr>().is_ok() {
        return;
    }
    let addresses = mapping.entry(host).or_default();
    if !addresses.contains(&address) {
        addresses.push(address);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merging_sources_keeps_failed_source_snapshot_when_another_source_updates() {
        let first_url = "https://raw.hellogithub.com/hosts.json".to_owned();
        let second_url = "https://hosts.gitcdn.top/hosts.txt".to_owned();
        let mut sources = HashMap::new();
        sources.insert(
            first_url.clone(),
            parse_hosts("192.0.2.10 shared.example\\n192.0.2.11 first-only.example"),
        );
        sources.insert(
            second_url.clone(),
            parse_hosts("192.0.2.20 second-only.example\\n192.0.2.10 shared.example"),
        );

        // The first source succeeds with a newer snapshot. The second source
        // is absent from this refresh and therefore keeps its previous snapshot.
        sources.insert(
            first_url,
            parse_hosts("192.0.2.12 shared.example\\n192.0.2.13 first-new.example"),
        );

        let merged = merge_source_mappings(&sources);
        assert_eq!(
            merged.get("shared.example").unwrap().len(),
            2,
            "addresses from both current and retained source snapshots should merge"
        );
        assert!(merged.contains_key("first-new.example"));
        assert!(!merged.contains_key("first-only.example"));
        assert!(merged.contains_key("second-only.example"));
    }

    #[test]
    fn parses_json_hosts_and_deduplicates_addresses() {
        let parsed = parse_hosts(
            r#"[["192.0.2.1","Example.com"],["192.0.2.1","example.com"],["2001:db8::1","example.com"]]"#,
        );
        assert_eq!(
            parsed.get("example.com").unwrap(),
            &vec![
                "192.0.2.1".parse::<IpAddr>().unwrap(),
                "2001:db8::1".parse().unwrap(),
            ]
        );
    }

    #[test]
    fn parses_hosts_text_and_ignores_comments() {
        let parsed = parse_hosts(
            "# comment\n192.0.2.10 github.com alias.example # trailing comment\ninvalid bad.example\n",
        );
        assert_eq!(
            parsed.get("github.com").unwrap(),
            &vec!["192.0.2.10".parse::<IpAddr>().unwrap()]
        );
        assert!(!parsed.contains_key("bad.example"));
    }
}
