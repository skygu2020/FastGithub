use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    fs,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    sync::OnceLock,
    time::Instant,
};

static PROCESS_START: OnceLock<Instant> = OnceLock::new();

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

impl DomainConfig {
    /// Resolve the upstream TLS SNI value using the original FastGithub tokens.
    /// None means that the TLS ClientHello must omit the SNI extension.
    pub fn tls_sni_value(&self, domain: &str, address: IpAddr) -> Option<String> {
        if self.tls_sni != Some(true) {
            return None;
        }

        let pattern = self
            .tls_sni_pattern
            .as_deref()
            .filter(|value| !value.is_empty())
            .unwrap_or("@domain");
        let uptime_ms = PROCESS_START
            .get_or_init(Instant::now)
            .elapsed()
            .as_millis() as u64;
        Some(expand_tls_sni_pattern(pattern, domain, address, uptime_ms))
    }
}

fn expand_tls_sni_pattern(pattern: &str, domain: &str, address: IpAddr, uptime_ms: u64) -> String {
    let value = replace_ascii_case_insensitive(pattern, "@domain", domain);
    let value = replace_ascii_case_insensitive(&value, "@random", &uptime_ms.to_string());
    replace_ascii_case_insensitive(&value, "@ipaddress", &address.to_string())
}

fn replace_ascii_case_insensitive(value: &str, needle: &str, replacement: &str) -> String {
    let lower = value.to_ascii_lowercase();
    let needle = needle.to_ascii_lowercase();
    let mut output = String::with_capacity(value.len());
    let mut cursor = 0;

    while let Some(relative) = lower[cursor..].find(&needle) {
        let start = cursor + relative;
        let end = start + needle.len();
        output.push_str(&value[cursor..start]);
        output.push_str(replacement);
        cursor = end;
    }

    output.push_str(&value[cursor..]);
    output
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

pub fn domain_matches(pattern: &str, host: &str) -> bool {
    wildcard_domain_match(pattern, host)
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
    use super::{expand_tls_sni_pattern, wildcard_domain_match, DomainConfig};
    use std::net::IpAddr;

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

    #[test]
    fn tls_sni_is_disabled_unless_explicitly_enabled() {
        let config = DomainConfig::default();
        assert_eq!(config.tls_sni_value("github.com", "192.0.2.1".parse().unwrap()), None);
    }

    #[test]
    fn tls_sni_defaults_to_domain_when_enabled_without_pattern() {
        let config = DomainConfig { tls_sni: Some(true), ..Default::default() };
        assert_eq!(
            config.tls_sni_value("github.com", "192.0.2.1".parse().unwrap()).as_deref(),
            Some("github.com")
        );
    }

    #[test]
    fn expands_sni_tokens_case_insensitively_in_reference_order() {
        let address: IpAddr = "2001:db8::1".parse().unwrap();
        assert_eq!(
            expand_tls_sni_pattern("pre-@DOMAIN-@random-@IPADDRESS", "api.github.com", address, 12345),
            "pre-api.github.com-12345-2001:db8::1"
        );
    }

    #[test]
    fn preserves_literal_custom_sni_text() {
        let address: IpAddr = "192.0.2.9".parse().unwrap();
        assert_eq!(
            expand_tls_sni_pattern("edge.example.net", "origin.example.net", address, 1),
            "edge.example.net"
        );
    }
}
