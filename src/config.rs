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
    /// Return the configured upstream SNI name. SNI is enabled unless explicitly disabled.
    pub fn tls_sni_value(&self, domain: &str, address: IpAddr) -> Option<String> {
        if self.tls_sni == Some(false) {
            return None;
        }
        let pattern = self.tls_sni_pattern.as_deref().filter(|value| !value.is_empty()).unwrap_or("@domain");
        let uptime_ms = system_uptime_ms();
        let value = replace_ascii_case_insensitive(pattern, "@domain", domain);
        let value = replace_ascii_case_insensitive(&value, "@random", &uptime_ms.to_string());
        Some(replace_ascii_case_insensitive(&value, "@ipaddress", &address.to_string()))
    }
}

fn system_uptime_ms() -> u128 {
    fs::read_to_string("/proc/uptime")
        .ok()
        .and_then(|text| text.split_ascii_whitespace().next()?.parse::<f64>().ok())
        .map(|seconds| (seconds * 1000.0) as u128)
        .unwrap_or_else(|| PROCESS_START.get_or_init(Instant::now).elapsed().as_millis())
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

fn default_proxy_port() -> u16 { 38457 }

fn default_fallback_dns() -> Vec<SocketAddr> {
    ["8.8.8.8:53", "119.29.29.29:53", "114.114.114.114:53"]
        .into_iter().filter_map(|value| value.parse().ok()).collect()
}

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let mut root = read_json5(path)?;
        let overlay_dir = path.parent().unwrap_or_else(|| Path::new(".")).join("appsettings");
        if overlay_dir.is_dir() {
            let mut overlays: Vec<PathBuf> = fs::read_dir(&overlay_dir)
                .with_context(|| format!("reading {}", overlay_dir.display()))?
                .filter_map(|entry| entry.ok().map(|entry| entry.path()))
                .filter(|path| path.file_name().and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("appsettings.") && name.ends_with(".json")))
                .collect();
            overlays.sort();
            for overlay in overlays { merge(&mut root, read_json5(&overlay)?); }
        }
        serde_json::from_value(root).context("decoding FastGithub configuration")
    }

    pub fn matches_domain(&self, host: &str) -> bool {
        self.fast_github.domain_configs.keys().any(|pattern| wildcard_domain_match(pattern, host))
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
                    None => { base.insert(key, value); }
                }
            }
        }
        (base, overlay) => *base = overlay,
    }
}

pub fn domain_matches(pattern: &str, host: &str) -> bool { wildcard_domain_match(pattern, host) }

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
                current[index] = previous[index - 1] && host[index - 1] != '.'
                    && (token == '?' || token == host[index - 1]);
            }
        }
        previous = current;
    }
    previous[host.len()]
}

#[cfg(test)]
mod tests {
    use super::{expand_for_test, wildcard_domain_match, DomainConfig};
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
    fn sni_is_enabled_by_default_and_can_be_disabled() {
        let address: IpAddr = "192.0.2.1".parse().unwrap();
        assert_eq!(DomainConfig::default().tls_sni_value("github.com", address).as_deref(), Some("github.com"));
        let disabled = DomainConfig { tls_sni: Some(false), ..Default::default() };
        assert_eq!(disabled.tls_sni_value("github.com", address), None);
        let enabled = DomainConfig { tls_sni: Some(true), ..Default::default() };
        assert_eq!(enabled.tls_sni_value("github.com", address).as_deref(), Some("github.com"));
    }

    #[test]
    fn custom_sni_tokens_are_case_insensitive() {
        let address: IpAddr = "2001:db8::1".parse().unwrap();
        assert_eq!(expand_for_test("pre-@DOMAIN-@random-@IPADDRESS", "api.github.com", address, 12345),
            "pre-api.github.com-12345-2001:db8::1");
    }

    #[test]
    fn custom_sni_literal_is_preserved() {
        let address: IpAddr = "192.0.2.9".parse().unwrap();
        assert_eq!(expand_for_test("edge.example.net", "origin.example.net", address, 1), "edge.example.net");
    }
}

#[cfg(test)]
fn expand_for_test(pattern: &str, domain: &str, address: IpAddr, uptime_ms: u128) -> String {
    let value = replace_ascii_case_insensitive(pattern, "@domain", domain);
    let value = replace_ascii_case_insensitive(&value, "@random", &uptime_ms.to_string());
    replace_ascii_case_insensitive(&value, "@ipaddress", &address.to_string())
}
