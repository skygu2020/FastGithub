mod address_service;
mod config;
mod dns;
mod dnscrypt;
mod http_proxy;
mod persistence;
mod resolver;
mod service;
mod tls;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

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
    /// Install and enable the systemd unit.
    Start,
    /// Stop, disable, and remove the systemd unit.
    Stop,
    /// Print the effective Linux configuration.
    Check,
    /// Resolve a host:port using configured DNS and cached TCP endpoint health.
    Resolve { endpoint: String },
    /// Refresh and benchmark all persisted DNS endpoints once.
    TestDns,
    /// Run the original one-second DNS endpoint health refresh loop.
    WatchDns,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with_target(false).init();

    let cli = Cli::parse();
    let executable = std::env::current_exe()?;
    let config_path = cli.config.unwrap_or_else(|| std::env::current_dir().unwrap_or_default().join("appsettings.json"));

    match cli.command {
        Some(Command::Start) => service::install_and_start(&executable).await?,
        Some(Command::Stop) => service::stop_and_remove(&executable).await?,
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
        None => {
            let config = config::AppConfig::load(&config_path)?;
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
            let result = http_proxy::run(config, upstreams).await;
            if let Some(proxy) = proxy {
                proxy.stop().await;
            }
            result?;
        }
    }
    Ok(())
}
