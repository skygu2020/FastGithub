use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct AppConfig {
    #[serde(rename = "FastGithub")]
    pub fast_github: FastGithubConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct FastGithubConfig {
    #[serde(default = "default_proxy_port")]
    pub http_proxy_port: u16,
    #[serde(default = "default_fallback_dns")]
    pub fallback_dns: Vec<SocketAddr>,
    #[serde(default)]
    pub domain_configs: BTreeMap<String, DomainConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct DomainConfig {
    pub tls_sni: Option<bool>,
    pub tls_sni_pattern: Option<String>,
    pub tls_ignore_name_mismatch: Option<bool>,
    pub timeout: Option<String>,
    pub ip_address: Option<String>,
    pub destination: Option<String>,
    pub response: Option<Value>,
}

fn default_proxy_port() -> u16 {
    38457
}

fn default_fallback_dns() -> Vec<SocketAddr> {
    ["8.8.8.8:53", "119.29.29.29:53", "114.114.114.114:53"]
        .into_iter()
        .filter_map(|value| value.parse().ok())
        .collect()
}

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let mut root = read_json5(path)?;
        let overlay_dir = path.parent().unwrap_or_else(|| Path::new(".")).join("appsettings");
        if overlay_dir.is_dir() {
            let mut overlays: Vec<PathBuf> = fs::read_dir(&overlay_dir)
                .with_context(|| format!("reading {}", overlay_dir.display()))?
                .filter_map(|entry| entry.ok().map(|entry| entry.path()))
                .filter(|path| {
                    path.file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| name.starts_with("appsettings.") && name.ends_with(".json"))
                })
                .collect();
            overlays.sort();
            for overlay in overlays {
                let value = read_json5(&overlay)?;
                merge(&mut root, value);
            }
        }
        serde_json::from_value(root).context("decoding FastGithub configuration")
    }

    pub fn matches_domain(&self, host: &str) -> bool {
        self.fast_github
            .domain_configs
            .keys()
            .any(|pattern| wildcard_domain_match(pattern, host))
    }
}

fn read_json5(path: &Path) -> Result<Value> {
    let text = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    json5::from_str(&text).with_context(|| format!("parsing {}", path.display()))
}

fn merge(base: &mut Value, overlay: Value) {
    match (base, overlay) {
        (Value::Object(base), Value::Object(overlay)) => {
            for (key, value) in overlay {
                match base.get_mut(&key) {
                    Some(existing) => merge(existing, value),
                    None => {
                        base.insert(key, value);
                    }
                }
            }
        }
        (base, overlay) => *base = overlay,
    }
}

fn wildcard_domain_match(pattern: &str, host: &str) -> bool {
    let pattern: Vec<char> = pattern.to_ascii_lowercase().chars().collect();
    let host: Vec<char> = host.trim_end_matches('.').to_ascii_lowercase().chars().collect();
    let mut previous = vec![false; host.len() + 1];
    previous[0] = true;

    for token in pattern {
        let mut current = vec![false; host.len() + 1];
        if token == '*' {
            current[0] = previous[0];
            for index in 1..=host.len() {
                current[index] = previous[index] || (host[index - 1] != '.' && current[index - 1]);
            }
        } else {
            for index in 1..=host.len() {
                current[index] = previous[index - 1]
                    && host[index - 1] != '.'
                    && (token == '?' || token == host[index - 1]);
            }
        }
        previous = current;
    }
    previous[host.len()]
}

#[cfg(test)]
mod tests {
    use super::wildcard_domain_match;

    #[test]
    fn wildcard_does_not_cross_label_separator() {
        assert!(wildcard_domain_match("*.github.com", "api.github.com"));
        assert!(!wildcard_domain_match("*.github.com", "a.b.github.com"));
        assert!(!wildcard_domain_match("*.github.com", "github.com"));
    }

    #[test]
    fn matching_is_case_insensitive_and_ignores_trailing_dot() {
        assert!(wildcard_domain_match("*.GitHub.com", "API.GITHUB.COM."));
    }
}
