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

    // Only origin-form starts with exactly one slash. A leading "//" is a
    // network-path reference, not a valid origin-form request-target; accepting
    // it through Url::join would let the target silently replace the Host authority.
    if request_target.starts_with("//") {
        bail!("network-path request-target is not valid in HTTP origin-form");
    }
    if request_target == "*" && !method.eq_ignore_ascii_case("OPTIONS") {
        bail!("asterisk-form request-target is only valid for OPTIONS");
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

    let (original_host, original_authority) = match absolute.as_ref() {
        Some(uri) => (uri.host_str().expect("absolute URI host checked").to_owned(), authority_from_url(uri)),
        None => {
            let authority = host_header.context("origin-form request requires Host header")?;
            (parse_host_authority(authority)?, authority.to_owned())
        }
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
        Some(destination) => resolve_destination(scheme, &original_authority, destination)?,
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


fn authority_from_url(uri: &Url) -> String {
    let host = uri.host_str().expect("URL host checked");
    let host = if host.contains(':') { format!("[{host}]") } else { host.to_owned() };
    match uri.port() {
        Some(port) => format!("{host}:{port}"),
        None => host,
    }
}

fn is_http_token(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte)
}

fn parse_host_authority(authority: &str) -> Result<String> {
    if authority.is_empty()
        || authority.bytes().any(|byte| byte <= 0x20 || byte >= 0x7f)
        || authority.chars().any(|character| matches!(character, '@' | '/' | '\\' | '?' | '#')) {
        bail!("invalid Host authority");
    }

    let (host, port) = if let Some(rest) = authority.strip_prefix('[') {
        let (host, suffix) = rest.split_once(']').context("invalid bracketed IPv6 Host")?;
        host.parse::<std::net::Ipv6Addr>().context("invalid IPv6 Host address")?;
        if suffix.is_empty() {
            (host, None)
        } else {
            let port = suffix.strip_prefix(':').context("invalid IPv6 Host port")?;
            (host, Some(port))
        }
    } else {
        if authority.matches(':').count() > 1 {
            bail!("IPv6 Host must use brackets");
        }
        match authority.rsplit_once(':') {
            Some((host, port)) => (host, Some(port)),
            None => (authority, None),
        }
    };

    if let Some(port) = port {
        if port.is_empty() || !port.bytes().all(|byte| byte.is_ascii_digit()) {
            bail!("invalid Host port");
        }
        let port = port.parse::<u16>().context("Host port is outside the valid range")?;
        if port == 0 {
            bail!("Host port must be non-zero");
        }
    }

    if host.is_empty() {
        bail!("empty Host name");
    }
    if host.parse::<std::net::IpAddr>().is_err() {
        validate_hostname(host)?;
    }
    Ok(host.to_owned())
}

fn validate_hostname(host: &str) -> Result<()> {
    if host.len() > 253 {
        bail!("HTTP hostname is too long");
    }
    let hostname = host.strip_suffix('.').unwrap_or(host);
    if hostname.is_empty() {
        bail!("invalid HTTP hostname");
    }
    for label in hostname.split('.') {
        if label.is_empty() || label.len() > 63
            || label.starts_with('-') || label.ends_with('-')
            || !label.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-') {
            bail!("invalid HTTP hostname label");
        }
    }
    Ok(())
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
    fn relative_destination_preserves_original_host_port() {
        let rule = DomainConfig { destination: Some("mirror/path".into()), ..Default::default() };
        match route_request("GET", "/x", Some("origin.example:8443"), "https", &config(&[("origin.example", rule)])).unwrap() {
            RouteDecision::Forward { uri, .. } => assert_eq!(uri.as_str(), "https://origin.example:8443/mirror/path"),
            _ => panic!("expected forwarding"),
        }
    }

    #[test]
    fn rejects_network_path_and_non_options_asterisk_targets() {
        let cfg = config(&[]);
        assert!(route_request("GET", "//attacker.example/path", Some("trusted.example"), "http", &cfg).is_err());
        assert!(route_request("GET", "*", Some("trusted.example"), "http", &cfg).is_err());
        assert!(route_request("OPTIONS", "*", Some("trusted.example"), "http", &cfg).is_ok());
    }

    #[test]
    fn rejects_zero_port_in_host_authority() {
        let cfg = config(&[]);
        assert!(route_request("GET", "/", Some("example.com:0"), "http", &cfg).is_err());
    }

    #[test]
    fn rejects_host_authority_url_delimiters() {
        let cfg = config(&[]);
        for authority in [
            "example.com/path", "example.com?query", "example.com#fragment",
            "example.com\\\\evil", "user@example.com",
        ] {
            assert!(route_request("GET", "/", Some(authority), "http", &cfg).is_err(),
                "accepted Host authority {authority}");
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
