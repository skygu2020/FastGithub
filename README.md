# FastGithub Linux — Rust implementation

This repository's active Linux implementation is the native Rust package at the repository root.
It must not load, launch, bundle, or depend on the .NET runtime, ASP.NET Core, Mono, or any .NET
assembly. The existing .NET projects remain the reference implementation. Windows and macOS
deliverables are not changed.

## Build and run

```sh
cargo build --release
./target/release/fastgithub cert
./target/release/fastgithub check
./target/release/fastgithub resolve github.com:443
./target/release/fastgithub test-dns
./target/release/fastgithub watch-dns
```

The Rust implementation includes JSON5 configuration loading and sorted
`appsettings/appsettings.*.json` overlays, label-safe domain wildcard matching, systemd unit
management, a native DNS-over-TCP/UDP client, endpoint persistence/health ordering, bundled
DNSCrypt process management, and an HTTP forward-proxy listener. The proxy supports ordinary
HTTP forwarding, opaque non-TLS CONNECT tunnels, and TLS interception that routes decrypted
HTTP through the same Hyper request layer. Domain rules support Response, Destination, IPAddress,
TLS SNI policy, and request-scoped Timeout. HTTP request and response bodies are streamed; the
downstream HTTP/1 connection supports multiple requests. The local CA lifecycle is implemented:
`fastgithub cert` creates or loads `certs/fastgithub.crt` and `certs/fastgithub.key` beside the
selected configuration file. Normal proxy startup also creates or loads this CA and logs both
paths. The CA is not automatically installed into the operating-system trust store.

The proxy also supports persistent downstream HTTP/1 connections, HTTP/1 Upgrade tunneling (including WebSocket), and HTTP/2 over negotiated TLS ALPN on both the intercepted client side and HTTPS upstream side. HTTP/2 upstream connections are currently per-request; h2c and RFC 8441 extended CONNECT are not implemented. The HTTP/1 Expect: 100-continue gate and body/trailer handling have source-level tests,
but the latest changes still require user-side Cargo build and integration verification.
A bounded TLS ClientHello parser is retained for protocol analysis; the active TLS interception path uses OpenSSL's ClientHello/SNI callback to select per-host certificates.

The active configuration layer preserves the original `TlsSni` semantics: SNI is omitted only
when `TlsSni=false`; otherwise an empty pattern defaults to `@domain`. The pattern supports
case-insensitive `@domain`, `@ipaddress`, and `@random` tokens. The random token currently
uses Linux system uptime milliseconds from `/proc/uptime`, corresponding to the original changing tick
count. The upstream TLS connection applies the configured SNI override while continuing to verify the certificate against the original CONNECT/SNI hostname.

The DNS client queries A and AAAA records, merges upstream answers, caches bounded DNS TTLs, and
can promote the fastest TCP-reachable address. The resolver persists discovered `host:port`
entries in `dnsendpoints.json`. Port 53 uses TCP; custom ports use UDP and retry over TCP when a
response is truncated. UDP transaction IDs are validated before parsing.

The bundled Linux `dnscrypt-proxy` remains in place until a native replacement preserves shipped
behavior. The current TOML enables DNSCrypt and DNS-over-HTTPS and requires no-log/no-filter
resolvers. The Rust DNSCrypt crate does not yet establish parity for DoH, ODoH, or post-quantum
DNSCrypt, so replacing the bundled proxy now would reduce protocol coverage. Do not remove the Go
executable until those gaps are implemented or explicitly accepted and tested.

## Porting constraints

- Rust-only executable and dependency graph.
- Keep the existing FastGithub JSON configuration shape.
- Preserve the proxy listener default (127.0.0.1:38457).
- No .NET SDK/runtime, C#, Mono, ASP.NET Core, or Native AOT in the Linux build.


