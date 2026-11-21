# Linux Rust 核心功能对照清单

对照基线：可访问维护镜像中的 FastGithub v2.6.0。该清单只覆盖 Linux/macOS 本地 HTTP 正向代理路径，不把 Windows 透明代理能力列为 Linux 必须实现项。

## 范围约束

本轮明确不实现任何安装功能：

- 不安装或卸载 systemd 服务。
- 不自动修改系统代理。
- 不自动安装 CA 到系统信任库。
- 不安装系统依赖或修改系统配置。

保留 CA 生成、加载、域名证书签发和代理 TLS 拦截能力；它们是运行核心，不属于安装行为。现有 CLI 中已经存在的服务管理命令不在本轮扩展范围内。

## P0：DNS/IP 选路核心

- [x] IP 探测采用 TCP 连接 + TLS 握手，不再只看 TCP 是否可连接。
- [x] TLS 探测使用原始域名作为 SNI，保持证书链验证，只关闭候选 IP 探测的主机名匹配。
- [x] 在线 hosts 多源并行获取，支持 JSON 数组与 hosts 文本格式。
- [x] hosts 源按域名合并、去重；全部来源失败时保留上一次有效映射。
- [x] hosts 候选优先；hosts 未覆盖时回退 DNS。
- [x] 为 GitHub 核心站点、API、Gist、codeload、raw、头像和静态资源域名增加候选 IP 池。
- [x] DNS 候选与内置 IP 合并，并经过 TCP/TLS 探测排序。
- [x] 请求阶段 TCP 连接失败或 TLS 握手失败时上报失败 IP，90 秒内熔断。
- [x] 请求连接成功后将成功 IP 提升到该域名候选列表首位。
- [x] 新候选优先、旧候选补位；最多保留 8 个候选，全部探测失败时不清空候选池。
- [x] 持久化已验证的域名/IP 候选到 `dnsaddresses.json`，兼容原有 `dnsendpoints.json`。
- [x] 正常代理启动后立即刷新 hosts/DNS/IP，此后每 30 分钟自动刷新。
- [x] 增加仅绑定 loopback 代理入口的 `/ping` 与 `/refresh-ip` 管理路径；手动刷新会清理 DNS 和探测缓存后重建候选。
- [x] 公共 DNS（53 端口）连续查询失败 3 次后熔断 5 分钟；本地 DNSCrypt 端点不参与该熔断。

## P1：HTTP 代理与运行管理

- [x] HTTP/1 上游连接池：按原始域名隔离策略，按 scheme/authority 复用连接，空闲连接 90 秒回收，每个目标最多保留 8 条空闲连接。
- [x] HTTP/2 上游连接池：通过 TLS ALPN 自动识别 h2，按目标 authority 复用连接并使用 HTTP/2 多路复用；WebSocket/Upgrade 使用独立连接，不进入普通请求池。
- [x] HTTP 请求完成日志：方法、authority/path、状态码和耗时；5xx 额外记录 warning。
- [x] 按域名/状态码聚合 HTTP 4xx/5xx 错误计数，提供 `/errorStatistics` 查询接口。
- [x] `/flowStatistics` 查询接口。
- [ ] 配置文件热重载，变更后更新规则并清理相关缓存。
- [ ] 补齐 HTTP/1、HTTP/2 连接池的取消、超时、早期响应和连接失效回收测试（已新增 HTTP/1 顺序复用与 HTTP/2 并发多路复用测试，但尚未执行）。

## P2：运行观测与生命周期

- [x] 流量分析：累计客户端上行/下行字节、5 秒速率窗口（HTTP/1、HTTP/2、普通 HTTP 和原始 CONNECT；TLS 握手本身未计入）。
- [ ] 按日期滚动文件日志。
- [ ] UDP 日志输出。
- [ ] 单实例运行保护。
- [ ] 父进程退出时自动停止代理进程。

## P3：可选辅助功能

- [ ] PAC 脚本及 Linux 代理辅助功能。

## 不移植的 Windows 专属功能

- WinDivert DNS 劫持、TCP 端口重写和透明代理。
- WPF 托盘 UI。
- Windows 系统代理/hosts 冲突处理。
- Windows 专属透明 SSH/Git 监听器。

## 验证状态

本轮通过 GitHub 远程文件接口提交源码，当前环境没有本地工作树和 Cargo 工具链，因此没有执行 `cargo fmt`、`cargo check`、`cargo test` 或端到端网络测试。源码中的测试用例是待执行验证，不代表已通过。合并或部署前必须在 Linux Rust 1.78+ 环境运行：

```sh
cargo fmt --check
cargo check --all-targets
cargo test --all-targets
```

随后还需验证真实代理访问 GitHub、TLS 证书链异常 IP 过滤、hosts 源不可用回退、90 秒熔断恢复、重启后候选 IP 恢复，以及 `/refresh-ip` 手动刷新。
