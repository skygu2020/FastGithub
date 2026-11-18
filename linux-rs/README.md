# FastGithub Linux — Rust implementation

This directory is the native Linux implementation. It is a separate Rust program and must not
load, launch, bundle, or depend on the .NET runtime, ASP.NET Core, Mono, or any .NET assembly.

The existing .NET projects remain the reference implementation for protocol and behavior while
the Linux implementation is ported feature by feature. Windows and macOS deliverables are not
changed by this work.

## Build

```sh
cd linux-rs
cargo build --release
```

## Run

```sh
./target/release/fastgithub check
./target/release/fastgithub resolve github.com:443
./target/release/fastgithub test-dns
./target/release/fastgithub watch-dns
```

The current Rust implementation includes configuration loading (JSON5 comments and sorted
`appsettings/appsettings.*.json` overlays), label-safe domain wildcard matching, systemd unit
management, and a native DNS-over-TCP/UDP client. The DNS client queries A and AAAA records, merges
upstream answers, caches them using bounded DNS TTLs, and can promote the fastest TCP-reachable
address. The resolver persists discovered `host:port` entries in `dnsendpoints.json`, keeps the
original 10-minute domain/IP history and TCP health cache policy (5-minute normal / 1-minute local
network failure; 5-second connect timeout), and exposes a one-shot `test-dns` refresh command. DNS transport follows the reference behavior:
port 53 uses TCP; custom ports use UDP and retry over TCP when the response is truncated.
UDP transaction IDs are validated before parsing.
The bundled Linux `dnscrypt-proxy` is a client-side encrypted-DNS forwarder and remains the correct component for this role. `DNSCrypt/encrypted-dns-server` is a server-side implementation and is not a drop-in replacement. The proxy can be configured and managed as a child process by Rust:
its TOML listener, `ph` load-balancing strategy, log level, and 60–120 second positive/negative
cache TTLs are applied at startup. The resolver prefers its local listener and falls back to the
configured DNS servers if it cannot start. `test-dns` performs a one-shot refresh; `watch-dns` reproduces the original five-second
initialization delay followed by one-second background refreshes until Ctrl-C. The HTTP
proxy, TLS/SNI handling, flow statistics, and certificate lifecycle are still being ported; normal startup does not claim to provide a working proxy yet.


## DNSCrypt migration status

The bundled `dnscrypt-proxy` remains in place until a native replacement preserves the
shipped configuration's behavior. The current TOML enables both DNSCrypt and DNS-over-HTTPS,
and requires no-log/no-filter resolvers. The Rust `dnscrypt` crate is a promising pure-Rust
DNSCrypt v2 client with pinned provider keys, stamp parsing, certificate refresh, and optional
Anonymized DNSCrypt relays. Its documented API does not currently establish parity for DoH,
ODoH, or post-quantum DNSCrypt, so replacing the bundled proxy now would silently reduce
protocol coverage. Do not remove the Go executable until those compatibility gaps are either
implemented or explicitly accepted and tested.

## Porting constraints

- Rust-only executable and dependency graph.
- Keep the existing FastGithub JSON configuration shape.
- Preserve the Linux proxy listener default (127.0.0.1:38457) and existing behavior where practical.
- No .NET SDK/runtime, C#, Mono, ASP.NET Core, or Native AOT in the Linux build.
