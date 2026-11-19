use crate::{
    config::{AppConfig, DomainConfig, ResponseConfig},
    destination::resolve_destination,
};
use anyhow::{bail, Context, Result};
use url::Url;

/// A request-level routing decision shared by plain HTTP and decrypted HTTPS.
#[derive(Debug, Clone)]
pub enum RouteDecision {
    /// Response takes precedence over Destination, matching the original middleware.
    Respond(ResponseConfig),
    Forward {
        /// The selected upstream URI. Its path/query comes from Destination when configured.
        uri: Url,
        /// The request-target to send upstream (normally origin-form).
        request_target: String,
        /// Snapshot of the matched rule for DNS, SNI, and timeout handling.
        domain: Option<DomainConfig>,
        /// Original authority used for rule selection and upstream TLS verification.
        original_host: String,
    },
}

/// Decide how one HTTP request is handled. The caller supplies the transport scheme:
/// "http" for the plain proxy and "https" for an HTTP request inside intercepted TLS.
pub fn route_request(
    method: &str,
    request_target: &str,
    host_header: Option<&str>,
    scheme: &str,
    config: &AppConfig,
) -> Result<RouteDecision> {
    if !matches!(scheme, "http" | "https") {
        bail!("unsupported incoming request scheme '{scheme}'");
    }
    if method.is_empty() || !method.bytes().all(is_http_token) {
        bail!("invalid HTTP method");
    }

    let absolute = if request_target.starts_with('/') || request_target == "*" {
        None
    } else {
        let uri = Url::parse(request_target).context("invalid absolute-form request target")?;
        if uri.scheme() != scheme || uri.host_str().is_none()
            || !uri.username().is_empty() || uri.password().is_some() || uri.fragment().is_some() {
            bail!("request-target scheme, authority, or fragment is invalid");
        }
        Some(uri)
    };

    let original_host = match absolute.as_ref().and_then(Url::host_str) {
        Some(host) => host.to_owned(),
        None => parse_host_authority(host_header.context("origin-form request requires Host header")?)?,
    };
    let matched = config.domain_config(&original_host);
    if let Some(response) = matched.and_then(|domain| domain.response.as_ref()) {
        return Ok(RouteDecision::Respond(response.clone()));
    }

    let original_uri = match absolute {
        Some(uri) => uri,
        None => {
            let authority = host_header.context("origin-form request requires Host header")?;
            let base = Url::parse(&format!("{scheme}://{}/", authority))
                .context("building request base URI from Host header")?;
            if request_target == "*" { base }
            else { base.join(request_target).context("resolving origin-form request target")? }
        }
    };
    let uri = match matched.and_then(|domain| domain.destination.as_deref()) {
        Some(destination) => resolve_destination(scheme, &original_host, destination)?,
        None => original_uri,
    };
    let request_target = if request_target == "*" && matched.and_then(|d| d.destination.as_deref()).is_none() {
        "*".to_owned()
    } else {
        let path = if uri.path().is_empty() { "/" } else { uri.path() };
        match uri.query() {
            Some(query) => format!("{path}?{query}"),
            None => path.to_owned(),
        }
    };
    Ok(RouteDecision::Forward {
        uri,
        request_target,
        domain: matched.cloned(),
        original_host,
    })
}

fn is_http_token(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte)
}

fn parse_host_authority(authority: &str) -> Result<String> {
    if authority.is_empty() || authority.bytes().any(|byte| byte <= 0x20 || byte >= 0x7f)
        || authority.contains('@') || authority.contains('/') || authority.contains('\\')
        || authority.contains('?') || authority.contains('#') {
        bail!("invalid Host authority");
    }
    let uri = Url::parse(&format!("http://{authority}/")).context("invalid Host authority")?;
    if uri.host_str().is_none() || !uri.username().is_empty() || uri.password().is_some() {
        bail!("invalid Host authority");
    }
    Ok(uri.host_str().expect("host checked").to_owned())
}

#[cfg(test)]
mod tests {
    use super::{route_request, RouteDecision};
    use crate::config::{AppConfig, DomainConfig, FastGithubConfig, ResponseConfig};
    use std::{collections::BTreeMap, net::SocketAddr};

    fn config(rules: &[(&str, DomainConfig)]) -> AppConfig {
        AppConfig {
            fast_github: FastGithubConfig {
                http_proxy_port: 38457,
                fallback_dns: vec!["8.8.8.8:53".parse::<SocketAddr>().unwrap()],
                domain_configs: rules.iter().map(|(name, rule)| ((*name).to_owned(), rule.clone())).collect::<BTreeMap<_, _>>(),
            },
        }
    }

    #[test]
    fn response_precedes_destination() {
        let rule = DomainConfig {
            destination: Some("https://mirror.example/base/".into()),
            response: Some(ResponseConfig { status_code: 403, content_type: "text/plain".into(), content_value: Some("blocked".into()) }),
            ..Default::default()
        };
        let decision = route_request("GET", "/x", Some("origin.example"), "https", &config(&[("origin.example", rule)])).unwrap();
        assert!(matches!(decision, RouteDecision::Respond(_)));
    }

    #[test]
    fn resolves_absolute_and_origin_form_targets() {
        let cfg = config(&[]);
        match route_request("GET", "http://example.com/a?q=1", Some("ignored.example"), "http", &cfg).unwrap() {
            RouteDecision::Forward { uri, request_target, original_host, .. } => {
                assert_eq!(uri.as_str(), "http://example.com/a?q=1");
                assert_eq!(request_target, "/a?q=1");
                assert_eq!(original_host, "example.com");
            }
            _ => panic!("expected forwarding"),
        }
        match route_request("GET", "/path", Some("example.com:8080"), "https", &cfg).unwrap() {
            RouteDecision::Forward { uri, request_target, .. } => {
                assert_eq!(uri.as_str(), "https://example.com:8080/path");
                assert_eq!(request_target, "/path");
            }
            _ => panic!("expected forwarding"),
        }
    }

    #[test]
    fn destination_rewrites_upstream_uri_but_keeps_original_host_for_rule() {
        let rule = DomainConfig { destination: Some("https://cdn.example/assets/".into()), ..Default::default() };
        match route_request("GET", "/logo.png?v=2", Some("origin.example"), "https", &config(&[("origin.example", rule)])).unwrap() {
            RouteDecision::Forward { uri, request_target, original_host, .. } => {
                assert_eq!(uri.as_str(), "https://cdn.example/assets/");
                assert_eq!(request_target, "/assets/");
                assert_eq!(original_host, "origin.example");
            }
            _ => panic!("expected forwarding"),
        }
    }

    #[test]
    fn rejects_invalid_method_host_and_target() {
        let cfg = config(&[]);
        assert!(route_request("GE T", "/", Some("example.com"), "http", &cfg).is_err());
        assert!(route_request("GET", "/", None, "http", &cfg).is_err());
        assert!(route_request("GET", "ftp://example.com/", None, "http", &cfg).is_err());
        assert!(route_request("GET", "http://user@example.com/", None, "http", &cfg).is_err());
    }
}
