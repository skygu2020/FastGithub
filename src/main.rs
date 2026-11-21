mod address_service;
mod config;
mod cert;
mod dns;
mod dnscrypt;
mod http_proxy;
mod hosts;
mod http_response;
mod http_server;
mod http_route;
mod flow;
mod destination;
mod persistence;
mod prefixed_io;
mod resolver;
mod tls;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    sync::Arc,
};
use fs2::FileExt;
use tracing_subscriber::{fmt::writer::{BoxMakeWriter, MakeWriterExt}, EnvFilter};

#[derive(Debug, Parser)]
#[command(name = "fastgithub", version, about = "Native Rust Linux FastGithub")]
struct Cli {
    /// Main appsettings.json path (defaults to ./appsettings.json).
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Print the effective Linux configuration.
    Check,
    /// Create or load the local CA and print its file paths.
    Cert,
    /// Resolve a host:port using configured DNS and cached TCP endpoint health.
    Resolve { endpoint: String },
    /// Refresh and benchmark all persisted DNS endpoints once.
    TestDns,
    /// Run the original one-second DNS endpoint health refresh loop.
    WatchDns,
}

fn acquire_instance_lock(config_path: &Path) -> Result<File> {
    let directory = config_path.parent().filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let lock_path = directory.join(".fastgithub.lock");
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("opening instance lock {}", lock_path.display()))?;
    FileExt::try_lock_exclusive(&file)
        .with_context(|| format!("another FastGithub instance already holds {}", lock_path.display()))?;
    Ok(file)
}

#[tokio::main]
async fn main() -> Result<()> {
    let log_directory = std::env::current_dir()?.join("logs");
    let appender = tracing_appender::rolling::Builder::default()
        .rotation(tracing_appender::rolling::Rotation::DAILY)
        .filename_prefix("fastgithub")
        .filename_suffix("log")
        .max_log_files(14)
        .build(&log_directory);
    let (writer, _log_guard) = match appender {
        Ok(appender) => {
            let (file_writer, guard) = tracing_appender::non_blocking(appender);
            (BoxMakeWriter::new(std::io::stdout.and(file_writer)), Some(guard))
        }
        Err(error) => {
            eprintln!("daily log file unavailable ({}): {error}; using stdout only", log_directory.display());
            (BoxMakeWriter::new(std::io::stdout), None)
        }
    };
    tracing_subscriber::fmt()
        .with_writer(writer)
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with_target(false).init();

    let cli = Cli::parse();
    let config_path = cli.config.unwrap_or_else(|| std::env::current_dir().unwrap_or_default().join("appsettings.json"));

    match cli.command {
        Some(Command::Resolve { endpoint }) => {
            let config = config::AppConfig::load(&config_path)?;
            let (host, port) = endpoint.rsplit_once(':').context("endpoint must be host:port")?;
            let port: u16 = port.parse().context("invalid endpoint port")?;
            let proxy = dnscrypt::DnscryptProxy::discover().ok();
            let mut upstreams = Vec::new();
            if let Some(proxy) = proxy.as_ref() {
                match proxy.start().await {
                    Ok(endpoint) => { upstreams.push(endpoint); upstreams.push(endpoint); }
                    Err(error) => tracing::warn!(%error, "dnscrypt-proxy unavailable; using fallback DNS"),
                }
            }
            upstreams.extend(config.fast_github.fallback_dns);
            let dns = dns::DnsClient::new(upstreams);
            let persistence = persistence::Persistence::new("dnsendpoints.json");
            let resolver = resolver::DomainResolver::new(dns, persistence).await?;
            for address in resolver.resolve(host, port).await? { println!("{address}"); }
            if let Some(proxy) = proxy { proxy.stop().await; }
        }
        Some(Command::TestDns) => {
            let config = config::AppConfig::load(&config_path)?;
            let proxy = dnscrypt::DnscryptProxy::discover().ok();
            let mut upstreams = Vec::new();
            if let Some(proxy) = proxy.as_ref() {
                match proxy.start().await {
                    Ok(endpoint) => { upstreams.push(endpoint); upstreams.push(endpoint); }
                    Err(error) => tracing::warn!(%error, "dnscrypt-proxy unavailable; using fallback DNS"),
                }
            }
            upstreams.extend(config.fast_github.fallback_dns);
            let dns = dns::DnsClient::new(upstreams);
            let persistence = persistence::Persistence::new("dnsendpoints.json");
            let resolver = resolver::DomainResolver::new(dns, persistence).await?;
            resolver.test_speed().await?;
            if let Some(proxy) = proxy { proxy.stop().await; }
        }
        Some(Command::WatchDns) => {
            let config = config::AppConfig::load(&config_path)?;
            let proxy = dnscrypt::DnscryptProxy::discover().ok();
            let mut upstreams = Vec::new();
            if let Some(proxy) = proxy.as_ref() {
                match proxy.start().await {
                    Ok(endpoint) => { upstreams.push(endpoint); upstreams.push(endpoint); }
                    Err(error) => tracing::warn!(%error, "dnscrypt-proxy unavailable; using fallback DNS"),
                }
            }
            upstreams.extend(config.fast_github.fallback_dns);
            let dns = dns::DnsClient::new(upstreams);
            let persistence = persistence::Persistence::new("dnsendpoints.json");
            let resolver = resolver::DomainResolver::new(dns, persistence).await?;
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            loop {
                if let Err(error) = resolver.test_speed().await {
                    tracing::warn!(%error, "DNS endpoint refresh failed");
                }
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {},
                    result = tokio::signal::ctrl_c() => {
                        result.context("waiting for Ctrl-C")?;
                        break;
                    }
                }
            }
            if let Some(proxy) = proxy { proxy.stop().await; }
        }
        Some(Command::Check) => {
            let config = config::AppConfig::load(&config_path)?;
            println!("{}", serde_json::to_string_pretty(&config)?);
        }
        Some(Command::Cert) => {
            let directory = certificate_directory(&config_path);
            let certificates = cert::CertificateService::load_or_create(&directory)?;
            println!("CA certificate: {}", certificates.ca_certificate_path().display());
            println!("CA private key: {}", certificates.ca_private_key_path().display());
            println!("CA subject: {:?}", certificates.ca_certificate().subject_name());
        }
        None => {
            let _instance_lock = acquire_instance_lock(&config_path)?;
            let config = config::AppConfig::load(&config_path)?;
            let certificate_directory = certificate_directory(&config_path);
            let certificates = Arc::new(cert::CertificateService::load_or_create(&certificate_directory)
                .context("initializing FastGithub CA certificate")?);
            tracing::info!(
                certificate = %certificates.ca_certificate_path().display(),
                private_key = %certificates.ca_private_key_path().display(),
                "FastGithub CA certificate is ready (not installed into system trust store)"
            );
            tracing::info!(version = env!("CARGO_PKG_VERSION"), proxy_port = config.fast_github.http_proxy_port,
                domains = config.fast_github.domain_configs.len(), "FastGithub Rust Linux configuration loaded");
            let proxy = match dnscrypt::DnscryptProxy::discover() {
                Ok(proxy) => Some(proxy),
                Err(error) => {
                    tracing::warn!(%error, "DNSCrypt proxy resources unavailable; using fallback DNS");
                    None
                }
            };
            let mut upstreams = Vec::new();
            if let Some(proxy) = proxy.as_ref() {
                match proxy.start().await {
                    Ok(endpoint) => {
                        upstreams.push(endpoint);
                        upstreams.push(endpoint);
                    }
                    Err(error) => tracing::warn!(%error, "DNSCrypt proxy failed to start; using fallback DNS"),
                }
            }
            upstreams.extend(config.fast_github.fallback_dns.iter().copied());
            let result = http_proxy::run(config, config_path.clone(), upstreams, certificates).await;
            if let Some(proxy) = proxy {
                proxy.stop().await;
            }
            result?;
        }
    }
    Ok(())
}

fn certificate_directory(config_path: &std::path::Path) -> PathBuf {
    config_path.parent().unwrap_or_else(|| std::path::Path::new(".")).join("certs")
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instance_lock_rejects_second_process_and_releases_on_drop() {
        let directory = std::env::temp_dir().join(format!(
            "fastgithub-instance-lock-test-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let config_path = directory.join("appsettings.json");

        let first = acquire_instance_lock(&config_path).unwrap();
        assert!(acquire_instance_lock(&config_path).is_err());
        drop(first);
        assert!(acquire_instance_lock(&config_path).is_ok());

        std::fs::remove_dir_all(directory).unwrap();
    }
}
