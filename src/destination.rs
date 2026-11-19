use anyhow::{bail, Context, Result};
use url::Url;

/// Resolve a configured Destination using the same base-URI rule as the original
/// FastGithub middleware: "{scheme}://{host}/" followed by URI-reference resolution.
pub fn resolve_destination(scheme: &str, host: &str, destination: &str) -> Result<Url> {
    if !matches!(scheme, "http" | "https") {
        bail!("unsupported original request scheme '{scheme}'");
    }
    if host.is_empty() || host.contains(['/', '\\', '?', '#', '@', '\r', '\n']) {
        bail!("invalid original request host");
    }
    let base = Url::parse(&format!("{scheme}://{host}/"))
        .context("constructing original request base URI")?;
    let resolved = base.join(destination)
        .with_context(|| format!("resolving Destination '{destination}' against {base}"))?;
    if !matches!(resolved.scheme(), "http" | "https") {
        bail!("Destination must resolve to an HTTP or HTTPS URI");
    }
    if resolved.host_str().is_none() {
        bail!("Destination URI has no host");
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::resolve_destination;

    #[test]
    fn resolves_absolute_and_relative_destination_uris() {
        assert_eq!(
            resolve_destination("https", "origin.example", "https://cdn.example/base/").unwrap().as_str(),
            "https://cdn.example/base/"
        );
        assert_eq!(
            resolve_destination("https", "origin.example", "mirror/").unwrap().as_str(),
            "https://origin.example/mirror/"
        );
        assert_eq!(
            resolve_destination("https", "origin.example", "/mirror/").unwrap().as_str(),
            "https://origin.example/mirror/"
        );
    }

    #[test]
    fn follows_uri_reference_path_and_query_rules() {
        assert_eq!(
            resolve_destination("http", "origin.example", "../v2?x=1").unwrap().as_str(),
            "http://origin.example/v2?x=1"
        );
        assert_eq!(
            resolve_destination("http", "origin.example", "?x=1").unwrap().as_str(),
            "http://origin.example/?x=1"
        );
    }

    #[test]
    fn rejects_non_http_destination_schemes_and_invalid_hosts() {
        assert!(resolve_destination("ftp", "origin.example", "/").is_err());
        assert!(resolve_destination("http", "origin.example", "file:///etc/passwd").is_err());
        assert!(resolve_destination("http", "bad/host", "/").is_err());
    }
}
