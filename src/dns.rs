use anyhow::{bail, Context, Result};
use std::{collections::HashMap, net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr}, sync::{atomic::{AtomicU16, Ordering}, Arc}, time::Duration};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpStream, UdpSocket}, sync::Mutex, time::{timeout, Instant}};

const DNS_TIMEOUT: Duration = Duration::from_secs(4);
const MIN_TTL: Duration = Duration::from_secs(30);
const MAX_TTL: Duration = Duration::from_secs(600);
static NEXT_ID: AtomicU16 = AtomicU16::new(1);

#[derive(Clone)]
struct CacheEntry { addresses: Vec<IpAddr>, expires_at: Instant }

#[derive(Clone)]
struct StateEntry { available: bool, expires_at: Instant }

#[derive(Clone)]
pub struct DnsClient {
    upstreams: Arc<Vec<SocketAddr>>,
    cache: Arc<Mutex<HashMap<(SocketAddr, String), CacheEntry>>>,
    state_cache: Arc<Mutex<HashMap<SocketAddr, StateEntry>>>,
    failure_counts: Arc<Mutex<HashMap<SocketAddr, u8>>>,
}

impl DnsClient {
    pub fn new(upstreams: Vec<SocketAddr>) -> Self {
        Self {
            upstreams: Arc::new(upstreams),
            cache: Arc::new(Mutex::new(HashMap::new())),
            state_cache: Arc::new(Mutex::new(HashMap::new())),
            failure_counts: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Clear DNS response and upstream availability caches for an explicit refresh.
    pub async fn clear_cache(&self) {
        self.cache.lock().await.clear();
        self.state_cache.lock().await.clear();
    }

    /// Query configured DNS upstreams, merging unique A and AAAA answers.
    pub async fn resolve(&self, host: &str, port: u16, fast_sort: bool) -> Result<Vec<IpAddr>> {
        let host = host.trim_end_matches('.').to_ascii_lowercase();
        if host.is_empty() { bail!("DNS host must not be empty"); }
        if let Ok(ip) = host.parse::<IpAddr>() { return Ok(vec![ip]); }
        if host == "localhost" { return Ok(vec![IpAddr::V4(Ipv4Addr::LOCALHOST), IpAddr::V6(Ipv6Addr::LOCALHOST)]); }

        let mut addresses = Vec::new();
        for upstream in self.upstreams.iter().copied() {
            // The reference implementation probes TCP only for port 53; custom ports are UDP.
            if upstream.port() == 53 && !self.upstream_available(upstream).await { continue; }
            match self.lookup(upstream, &host).await {
                Ok(found) => for ip in found { if !ip.is_loopback() && !addresses.contains(&ip) { addresses.push(ip); } },
                Err(error) => {
                    tracing::warn!(%host, %upstream, %error, "DNS lookup failed");
                }
            }
        }
        if fast_sort && addresses.len() > 1 { self.prefer_fastest(&mut addresses, port).await; }
        Ok(addresses)
    }

    async fn report_query_failure(&self, upstream: SocketAddr) {
        // Match the reference behavior: only public DNS on port 53 is
        // circuit-broken. The local DNSCrypt proxy uses a private endpoint.
        if upstream.port() != 53 {
            return;
        }
        let count = {
            let mut counts = self.failure_counts.lock().await;
            let count = counts.entry(upstream).or_insert(0);
            *count = count.saturating_add(1);
            *count
        };
        if count >= 3 {
            self.state_cache.lock().await.insert(
                upstream,
                StateEntry {
                    available: false,
                    expires_at: Instant::now() + Duration::from_secs(300),
                },
            );
            tracing::warn!(%upstream, failures = count, "DNS server circuit opened for five minutes");
        }
    }

    async fn report_query_success(&self, upstream: SocketAddr) {
        if upstream.port() != 53 {
            return;
        }
        let mut counts = self.failure_counts.lock().await;
        if let Some(count) = counts.get_mut(&upstream) {
            *count = count.saturating_sub(1);
        }
    }

    async fn upstream_available(&self, upstream: SocketAddr) -> bool {
        let mut cache = self.state_cache.lock().await;
        if let Some(entry) = cache.get(&upstream) {
            if entry.expires_at > Instant::now() {
                return entry.available;
            }
            cache.remove(&upstream);
        }
        true
    }

    async fn lookup(&self, upstream: SocketAddr, host: &str) -> Result<Vec<IpAddr>> {
        let key = (upstream, host.to_owned());
        {
            let mut cache = self.cache.lock().await;
            if let Some(entry) = cache.get(&key) {
                if entry.expires_at > Instant::now() { return Ok(entry.addresses.clone()); }
                cache.remove(&key);
            }
        }
        let mut addresses = Vec::new();
        let mut ttl = MAX_TTL;
        for record_type in [1u16, 28u16] {
            match self.query(upstream, host, record_type).await {
                Ok((found, record_ttl)) => {
                    addresses.extend(found);
                    ttl = ttl.min(record_ttl);
                    if record_type == 1 {
                        self.report_query_success(upstream).await;
                    }
                }
                Err(error) if record_type == 28 => {
                    tracing::debug!(%host, %error, "AAAA query failed");
                }
                Err(error) => {
                    self.report_query_failure(upstream).await;
                    return Err(error);
                }
            }
        }
        addresses.dedup();
        if addresses.is_empty() { ttl = MIN_TTL; }
        self.cache.lock().await.insert(key, CacheEntry { addresses: addresses.clone(), expires_at: Instant::now() + ttl.clamp(MIN_TTL, MAX_TTL) });
        Ok(addresses)
    }

    async fn query(&self, upstream: SocketAddr, host: &str, record_type: u16) -> Result<(Vec<IpAddr>, Duration)> {
        timeout(DNS_TIMEOUT, async {
            let query = build_query(host, record_type)?;
            // DNS uses UDP by default on every port. udp_query retries over
            // TCP only when the response has the RFC 1035 TC (truncated) bit.
            udp_query(upstream, &query, record_type).await
        }).await.context("DNS query timed out")?
    }

    async fn prefer_fastest(&self, addresses: &mut Vec<IpAddr>, port: u16) {
        let mut tasks = tokio::task::JoinSet::new();
        for ip in addresses.iter().copied() {
            tasks.spawn(async move {
                let started = Instant::now();
                let endpoint = SocketAddr::new(ip, port);
                let ok = timeout(CONNECT_TIMEOUT, TcpStream::connect(endpoint)).await.is_ok_and(|r| r.is_ok());
                ok.then_some((ip, started.elapsed()))
            });
        }
        let mut fastest: Option<(IpAddr, Duration)> = None;
        while let Some(result) = tasks.join_next().await {
            if let Ok(Some(candidate)) = result {
                if fastest.as_ref().map_or(true, |best| candidate.1 < best.1) { fastest = Some(candidate); }
            }
        }
        if let Some((ip, _)) = fastest {
            if let Some(index) = addresses.iter().position(|item| *item == ip) { addresses.swap(0, index); }
        }
    }
}
 
async fn tcp_query(upstream: SocketAddr, query: &[u8], record_type: u16) -> Result<(Vec<IpAddr>, Duration)> {
    let mut stream = TcpStream::connect(upstream).await.with_context(|| format!("connect DNS TCP {upstream}"))?;
    stream.write_u16(query.len() as u16).await?;
    stream.write_all(query).await?;
    let length = stream.read_u16().await? as usize;
    if !(12..=65535).contains(&length) { bail!("invalid DNS response length {length}"); }
    let mut response = vec![0; length];
    stream.read_exact(&mut response).await?;
    parse_response(&response, record_type, u16::from_be_bytes([query[0], query[1]]))
}

async fn udp_query(upstream: SocketAddr, query: &[u8], record_type: u16) -> Result<(Vec<IpAddr>, Duration)> {
    let bind = if upstream.is_ipv4() { "0.0.0.0:0" } else { "[::]:0" };
    let socket = UdpSocket::bind(bind).await?;
    socket.connect(upstream).await.with_context(|| format!("connect DNS UDP {upstream}"))?;
    socket.send(query).await?;
    let mut response = vec![0u8; 65535];
    let length = socket.recv(&mut response).await?;
    response.truncate(length);
    if response.len() < 12 { bail!("truncated DNS UDP response"); }
    let expected_id = u16::from_be_bytes([query[0], query[1]]);
    if u16_at(&response, 0)? != expected_id { bail!("DNS UDP response ID does not match query"); }
    // RFC 1035: retry truncated UDP answers over TCP.
    if u16_at(&response, 2)? & 0x0200 != 0 {
        return tcp_query(upstream, query, record_type).await;
    }
    parse_response(&response, record_type, expected_id)
}

fn build_query(host: &str, record_type: u16) -> Result<Vec<u8>> {
    let mut packet = Vec::with_capacity(host.len() + 32);
    packet.extend_from_slice(&NEXT_ID.fetch_add(1, Ordering::Relaxed).to_be_bytes());
    packet.extend_from_slice(&0x0100u16.to_be_bytes());
    packet.extend_from_slice(&1u16.to_be_bytes());
    packet.extend_from_slice(&[0; 6]);
    for label in host.split('.') {
        if label.is_empty() || label.len() > 63 || !label.is_ascii() { bail!("invalid DNS label in {host}"); }
        packet.push(label.len() as u8);
        packet.extend_from_slice(label.as_bytes());
    }
    packet.push(0);
    packet.extend_from_slice(&record_type.to_be_bytes());
    packet.extend_from_slice(&1u16.to_be_bytes());
    Ok(packet)
}

fn parse_response(packet: &[u8], expected_type: u16, expected_id: u16) -> Result<(Vec<IpAddr>, Duration)> {
    if packet.len() < 12 { bail!("truncated DNS header"); }
    if u16_at(packet, 0)? != expected_id { bail!("DNS response ID does not match query"); }
    let flags = u16_at(packet, 2)?;
    if flags & 0x8000 == 0 { bail!("not a DNS response"); }
    if flags & 0x000f != 0 { bail!("DNS rcode {}", flags & 0x000f); }
    let questions = u16_at(packet, 4)? as usize;
    let answers = u16_at(packet, 6)? as usize;
    let mut offset = 12;
    for _ in 0..questions {
        offset = skip_name(packet, offset)?;
        offset = offset.checked_add(4).context("question offset overflow")?;
        if offset > packet.len() { bail!("truncated DNS question"); }
    }
    let mut result = Vec::new();
    let mut ttl = MAX_TTL;
    for _ in 0..answers {
        offset = skip_name(packet, offset)?;
        let kind = u16_at(packet, offset)?;
        let class = u16_at(packet, offset + 2)?;
        let record_ttl = u32_at(packet, offset + 4)?;
        let length = u16_at(packet, offset + 8)? as usize;
        offset += 10;
        let end = offset.checked_add(length).context("record length overflow")?;
        if end > packet.len() { bail!("truncated DNS record"); }
        if class == 1 && kind == expected_type {
            let ip = match (kind, length) {
                (1, 4) => Some(IpAddr::V4(Ipv4Addr::new(packet[offset], packet[offset+1], packet[offset+2], packet[offset+3]))),
                (28, 16) => { let mut octets = [0; 16]; octets.copy_from_slice(&packet[offset..end]); Some(IpAddr::V6(Ipv6Addr::from(octets))) },
                _ => None,
            };
            if let Some(ip) = ip { result.push(ip); ttl = ttl.min(Duration::from_secs(record_ttl as u64)); }
        }
        offset = end;
    }
    if result.is_empty() { ttl = MIN_TTL; }
    Ok((result, ttl))
}

fn skip_name(packet: &[u8], mut offset: usize) -> Result<usize> {
    let mut cursor = offset;
    let mut jumped = false;
    for _ in 0..32 {
        let length = *packet.get(cursor).context("truncated DNS name")?;
        if length & 0xc0 == 0xc0 {
            let second = *packet.get(cursor + 1).context("truncated compression pointer")?;
            if !jumped { offset = cursor + 2; jumped = true; }
            cursor = (((length & 0x3f) as usize) << 8) | second as usize;
        } else if length & 0xc0 != 0 { bail!("invalid DNS label encoding"); }
        else if length == 0 { return Ok(if jumped { offset } else { cursor + 1 }); }
        else {
            cursor = cursor.checked_add(1 + length as usize).context("DNS name overflow")?;
            if cursor > packet.len() { bail!("truncated DNS label"); }
        }
    }
    bail!("DNS compression pointer loop")
}

fn u16_at(data: &[u8], offset: usize) -> Result<u16> {
    let v = data.get(offset..offset+2).context("truncated DNS u16")?;
    Ok(u16::from_be_bytes([v[0], v[1]]))
}
fn u32_at(data: &[u8], offset: usize) -> Result<u32> {
    let v = data.get(offset..offset+4).context("truncated DNS u32")?;
    Ok(u32::from_be_bytes([v[0], v[1], v[2], v[3]]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn response(id: u16, flags: u16) -> Vec<u8> {
        let mut packet = Vec::with_capacity(28);
        packet.extend_from_slice(&id.to_be_bytes());
        packet.extend_from_slice(&flags.to_be_bytes());
        packet.extend_from_slice(&[0, 0, 0, 1, 0, 0, 0, 0]);
        packet.extend_from_slice(&[0xc0, 0x0c, 0, 1, 0, 1]);
        packet.extend_from_slice(&60u32.to_be_bytes());
        packet.extend_from_slice(&[0, 4, 1, 2, 3, 4]);
        packet
    }

    #[test]
    fn builds_question() {
        let packet = build_query("api.github.com", 1).unwrap();
        assert_eq!(&packet[12..], &[3,b'a',b'p',b'i',7,b'g',b'i',b't',b'h',b'u',b'b',3,b'c',b'o',b'm',0,0,1,0,1]);
    }

    #[test]
    fn parses_a_record() {
        let packet = [0,1,0x81,0x80,0,1,0,1,0,0,0,0,1,b'a',0,0,1,0,1,0xc0,0x0c,0,1,0,1,0,0,0,60,0,4,1,2,3,4];
        let (ips, ttl) = parse_response(&packet, 1, 1).unwrap();
        assert_eq!(ips, vec![IpAddr::V4(Ipv4Addr::new(1,2,3,4))]);
        assert_eq!(ttl, Duration::from_secs(60));
    }

    #[tokio::test]
    async fn udp_query_accepts_matching_response() {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let upstream = socket.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut query = [0u8; 512];
            let (length, peer) = socket.recv_from(&mut query).await.unwrap();
            let id = u16::from_be_bytes([query[0], query[1]]);
            socket.send_to(&response(id, 0x8180), peer).await.unwrap();
            length
        });

        let query = build_query("example.com", 1).unwrap();
        let (addresses, ttl) = udp_query(upstream, &query, 1).await.unwrap();
        assert_eq!(addresses, vec![IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4))]);
        assert_eq!(ttl, Duration::from_secs(60));
        assert!(server.await.unwrap() >= 12);
    }

    #[tokio::test]
    async fn udp_query_rejects_mismatched_transaction_id() {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let upstream = socket.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut query = [0u8; 512];
            let (_, peer) = socket.recv_from(&mut query).await.unwrap();
            let id = u16::from_be_bytes([query[0], query[1]]).wrapping_add(1);
            socket.send_to(&response(id, 0x8180), peer).await.unwrap();
        });

        let query = build_query("example.com", 1).unwrap();
        let error = udp_query(upstream, &query, 1).await.unwrap_err();
        assert!(error.to_string().contains("ID does not match"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn udp_truncation_retries_over_tcp() {
        let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let upstream = udp.local_addr().unwrap();
        let tcp = tokio::net::TcpListener::bind(upstream).await.unwrap();

        let udp_server = tokio::spawn(async move {
            let mut query = [0u8; 512];
            let (_, peer) = udp.recv_from(&mut query).await.unwrap();
            let id = u16::from_be_bytes([query[0], query[1]]);
            udp.send_to(&response(id, 0x8380), peer).await.unwrap();
        });
        let tcp_server = tokio::spawn(async move {
            let (mut stream, _) = tcp.accept().await.unwrap();
            let length = stream.read_u16().await.unwrap() as usize;
            let mut query = vec![0; length];
            stream.read_exact(&mut query).await.unwrap();
            let id = u16::from_be_bytes([query[0], query[1]]);
            let packet = response(id, 0x8180);
            stream.write_u16(packet.len() as u16).await.unwrap();
            stream.write_all(&packet).await.unwrap();
        });

        let query = build_query("example.com", 1).unwrap();
        let (addresses, _) = udp_query(upstream, &query, 1).await.unwrap();
        assert_eq!(addresses, vec![IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4))]);
        udp_server.await.unwrap();
        tcp_server.await.unwrap();
    }
}
