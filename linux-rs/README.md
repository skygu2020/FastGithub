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
```

The current Rust implementation includes configuration loading (JSON5 comments and sorted
`appsettings/appsettings.*.json` overlays), label-safe domain wildcard matching, systemd unit
management, and a native DNS-over-TCP client. The DNS client queries A and AAAA records, merges
upstream answers, caches them using bounded DNS TTLs, and can promote the fastest TCP-reachable
address. It can be exercised with `fastgithub resolve github.com:443`. The HTTP proxy, persistent
endpoint/IP health cache, DNSCrypt lifecycle, TLS/SNI handling, flow statistics, and certificate
lifecycle are still being ported; normal startup does not claim to provide a working proxy yet.

## Porting constraints

- Rust-only executable and dependency graph.
- Keep the existing FastGithub JSON configuration shape.
- Preserve the Linux proxy listener default (127.0.0.1:38457) and existing behavior where practical.
- No .NET SDK/runtime, C#, Mono, ASP.NET Core, or Native AOT in the Linux build.
