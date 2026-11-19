use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    sync::OnceLock,
    time::{Duration, Instant},
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
    pub response: Option<ResponseConfig>,
}


/// A locally generated HTTP response for a matching domain.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ResponseConfig {
    #[serde(default = "default_response_status")]
    pub status_code: u16,
    #[serde(default = "default_response_content_type")]
    pub content_type: String,
    #[serde(default)]
    pub content_value: Option<String>,
}

fn default_response_status() -> u16 { 200 }
fn default_response_content_type() -> String { "text/plain;charset=utf-8".to_owned() }

impl DomainConfig {
    /// Parse the .NET TimeSpan constant format used by FastGithub configuration.
    pub fn request_timeout(&self) -> Result<Option<Duration>> {
        self.timeout.as_deref().map(parse_time_span).transpose()
    }

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

/// Parse non-negative .NET TimeSpan values such as "00:02:00" or "1.02:03:04.5".
fn parse_time_span(value: &str) -> Result<Duration> {
    let (days, clock) = match value.split_once('.') {
        Some((days, clock)) if !days.is_empty() && days.bytes().all(|b| b.is_ascii_digit()) =>
            (days.parse::<u64>().context("invalid TimeSpan day component")?, clock),
        _ => (0, value),
    };
    let parts = clock.split(':').collect::<Vec<_>>();
    if parts.len() != 3 { anyhow::bail!("invalid TimeSpan '{value}'"); }
    let hours = parts[0].parse::<u64>().context("invalid TimeSpan hours")?;
    let minutes = parts[1].parse::<u64>().context("invalid TimeSpan minutes")?;
    let (sec_text, fraction_text) = parts[2].split_once('.').unwrap_or((parts[2], ""));
    let seconds = sec_text.parse::<u64>().context("invalid TimeSpan seconds")?;
    if minutes >= 60 || seconds >= 60 || fraction_text.len() > 9
        || !fraction_text.bytes().all(|b| b.is_ascii_digit()) {
        anyhow::bail!("invalid TimeSpan '{value}'");
    }
    let nanos = if fraction_text.is_empty() { 0 } else {
        fraction_text.parse::<u32>().context("invalid TimeSpan fraction")?
            * 10u32.pow((9 - fraction_text.len()) as u32)
    };
    let total = (days as u128).checked_mul(86400)
        .and_then(|v| v.checked_add((hours as u128) * 3600))
        .and_then(|v| v.checked_add((minutes as u128) * 60))
        .and_then(|v| v.checked_add(seconds as u128))
        .context("TimeSpan is too large")?;
    let nanos_total = total.checked_mul(1_000_000_000)
        .and_then(|v| v.checked_add(nanos as u128))
        .context("TimeSpan is too large")?;
    let secs = u64::try_from(nanos_total / 1_000_000_000)
        .context("TimeSpan exceeds supported duration")?;
    Ok(Duration::new(secs, (nanos_total % 1_000_000_000) as u32))
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
    use super::{expand_for_test, parse_time_span, wildcard_domain_match, DomainConfig, ResponseConfig};
    use std::net::IpAddr;

    #[test]
    fn response_config_uses_original_defaults_and_fields() {
        let response: ResponseConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(response.status_code, 200);
        assert_eq!(response.content_type, "text/plain;charset=utf-8");
        assert_eq!(response.content_value, None);
        let response: ResponseConfig = serde_json::from_str(r#"{"StatusCode":404,"ContentType":"text/plain","ContentValue":"blocked"}"#).unwrap();
        assert_eq!(response.status_code, 404);
        assert_eq!(response.content_type, "text/plain");
        assert_eq!(response.content_value.as_deref(), Some("blocked"));
    }

    #[test]
    fn parses_dotnet_time_span_configuration() {
        assert_eq!(parse_time_span("00:02:00").unwrap(), std::time::Duration::from_secs(120));
        assert_eq!(parse_time_span("1.02:03:04.5").unwrap(), std::time::Duration::new(93_784, 500_000_000));
        assert!(parse_time_span("00:60:00").is_err());
        assert!(parse_time_span("invalid").is_err());
    }

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
