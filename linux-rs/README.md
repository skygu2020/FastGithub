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
./target/release/fastgithub --check
sudo ./target/release/fastgithub
```

The bootstrap currently implements configuration loading (including JSON5 comments and sorted
`appsettings/appsettings.*.json` overlays), domain wildcard matching, and systemd unit management.
The proxy, DNS resolution/IP racing, TLS/SNI handling, flow statistics, and certificate lifecycle
are the next implementation stages. The executable intentionally reports that network services
are not yet started rather than pretending the port is complete.

## Porting constraints

- Rust-only executable and dependency graph.
- Keep the existing FastGithub JSON configuration shape.
- Preserve the Linux proxy listener default (127.0.0.1:38457) and existing behavior where practical.
- No .NET SDK/runtime, C#, Mono, ASP.NET Core, or Native AOT in the Linux build.
