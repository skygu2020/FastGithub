use anyhow::{bail, Context, Result};
use std::{env, fs, net::{IpAddr, Ipv4Addr, SocketAddr}, path::{Path, PathBuf}, process::{Child, Command, Stdio}, time::Duration};
use tokio::{net::{TcpListener, TcpStream}, sync::Mutex, time::{sleep, timeout}};
use toml_edit::{value, Array, DocumentMut};

const READY_TIMEOUT: Duration = Duration::from_secs(5);

pub struct DnscryptProxy {
    child: Mutex<Option<Child>>,
    endpoint: Mutex<Option<SocketAddr>>,
    executable: PathBuf,
    config: PathBuf,
}

impl DnscryptProxy {
    pub fn discover() -> Result<Self> {
        let root = env::current_dir().context("getting working directory")?;
        let candidates = [
            root.join("dnscrypt-proxy/dnscrypt-proxy"),
            root.join("@dnscrypt-proxy/linux-x64/dnscrypt-proxy"),
            root.join("@dnscrypt-proxy/linux-arm64/dnscrypt-proxy"),
        ];
        let executable = candidates.into_iter().find(|path| path.is_file())
            .context("dnscrypt-proxy binary not found (expected dnscrypt-proxy/ or @dnscrypt-proxy/linux-*)")?;
        let config_candidates = [
            root.join("dnscrypt-proxy/dnscrypt-proxy.toml"),
            root.join("@dnscrypt-proxy/dnscrypt-proxy.toml"),
        ];
        let config = config_candidates.into_iter().find(|path| path.is_file())
            .context("dnscrypt-proxy.toml not found")?;
        Ok(Self { child: Mutex::new(None), endpoint: Mutex::new(None), executable, config })
    }

    pub async fn start(&self) -> Result<SocketAddr> {
        if let Some(endpoint) = *self.endpoint.lock().await {
            if self.is_running().await { return Ok(endpoint); }
        }
        let endpoint = available_loopback_endpoint().await?;
        update_config(&self.config, endpoint).await?;
        let mut guard = self.child.lock().await;
        if let Some(mut child) = guard.take() { let _ = child.kill(); let _ = child.wait(); }
        let child = Command::new(&self.executable)
            .current_dir(self.executable.parent().context("dnscrypt-proxy has no parent directory")?)
            .arg("-config")
            .arg(&self.config)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .with_context(|| format!("starting {}", self.executable.display()))?;
        *guard = Some(child);
        drop(guard);

        let deadline = tokio::time::Instant::now() + READY_TIMEOUT;
        loop {
            if self.child_exited().await? {
                bail!("dnscrypt-proxy exited before its TCP listener became ready");
            }
            if timeout(Duration::from_millis(150), TcpStream::connect(endpoint)).await.is_ok_and(|r| r.is_ok()) {
                *self.endpoint.lock().await = Some(endpoint);
                tracing::info!(%endpoint, "dnscrypt-proxy is ready");
                return Ok(endpoint);
            }
            if tokio::time::Instant::now() >= deadline {
                self.stop().await;
                bail!("dnscrypt-proxy did not listen on {endpoint} within {} seconds", READY_TIMEOUT.as_secs());
            }
            sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn local_endpoint(&self) -> Option<SocketAddr> {
        let endpoint = *self.endpoint.lock().await;
        if endpoint.is_some() && !self.is_running().await {
            *self.endpoint.lock().await = None;
            return None;
        }
        endpoint
    }

    async fn is_running(&self) -> bool {
        match self.child.lock().await.as_mut() {
            Some(child) => child.try_wait().map(|status| status.is_none()).unwrap_or(false),
            None => false,
        }
    }

    async fn child_exited(&self) -> Result<bool> {
        let mut guard = self.child.lock().await;
        match guard.as_mut() {
            Some(child) => Ok(child.try_wait()?.is_some()),
            None => Ok(true),
        }
    }

    pub async fn stop(&self) {
        let mut guard = self.child.lock().await;
        if let Some(mut child) = guard.take() {
            if child.try_wait().ok().flatten().is_none() {
                let _ = child.kill();
            }
            let _ = child.wait();
        }
        *self.endpoint.lock().await = None;
    }
}

async fn available_loopback_endpoint() -> Result<SocketAddr> {
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let endpoint = listener.local_addr()?;
    drop(listener);
    Ok(endpoint)
}

async fn update_config(path: &Path, endpoint: SocketAddr) -> Result<()> {
    let source = tokio::fs::read_to_string(path).await.with_context(|| format!("reading {}", path.display()))?;
    let mut document = source.parse::<DocumentMut>().context("parsing dnscrypt-proxy TOML")?;
    let mut listens = Array::new();
    listens.push(endpoint.to_string());
    document["listen_addresses"] = value(listens);
    document["log_level"] = value(6);
    document["lb_strategy"] = value("ph");
    document["cache_min_ttl"] = value(60);
    document["cache_neg_min_ttl"] = value(60);
    document["cache_max_ttl"] = value(120);
    document["cache_neg_max_ttl"] = value(120);

    let temp = temporary_path(path);
    tokio::fs::write(&temp, document.to_string()).await.with_context(|| format!("writing {}", temp.display()))?;
    if let Err(error) = tokio::fs::rename(&temp, path).await {
        let _ = tokio::fs::remove_file(&temp).await;
        return Err(error).with_context(|| format!("replacing {}", path.display()));
    }
    Ok(())
}

fn temporary_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".tmp");
    PathBuf::from(name)
}
