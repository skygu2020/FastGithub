# HTTP 代理移植记录

本文记录 `linux-rust` 分支当前 HTTP 代理实现，避免把已经完成的功能误判为待实现。原版行为参考：

- `FastGithub.HttpServer/HttpMiddlewares/HttpReverseProxyMiddleware.cs`
- `FastGithub.Configuration/DomainConfig.cs`
- `FastGithub.Configuration/ResponseConfig.cs`
- `FastGithub.Http/HttpClient.cs`
- `FastGithub.Http/HttpClientHandler.cs`
- `FastGithub.Http/HttpClientFactory.cs`

## 已实现

### HTTP/1 请求入口和转发

- 普通 HTTP proxy 请求由 Hyper HTTP/1 server 解析，支持 absolute-form 请求目标。
- CONNECT 根据隧道首批数据判断 TLS；TLS 隧道完成客户端握手和证书动态选择后，将解密流交给同一套 Hyper HTTP/1 请求处理器。
- 未拦截的非 TLS CONNECT 仍作为原始 TCP 隧道转发。
- 上游使用 Hyper HTTP/1 client handshake 和 `send_request`，请求体、响应体通过 Hyper body 流转发，不把整份响应缓存到内存。
- 请求和响应的 hop-by-hop headers，以及 `Connection` 指定的逐跳字段会被移除；请求中的端到端 `Expect` 和 `Trailer` 声明会保留（`TE` 仍按逐跳字段移除）；响应 Server 标记为 `FastGithub`。
- 响应体包装层逐 frame 透传 data 和 trailer frame；单元测试覆盖 `TimeoutBody` 不吞掉 `x-checksum` trailer。另有 TCP loopback 测试验证 Hyper HTTP/1 client/server 对请求和响应 trailer frame 的编码、解析。该测试验证 Hyper 双端互操作，不等同于经过 `forward_request`、TLS 和真实代理入口的完整端到端验证。
- 下游 HTTP/1 server 已启用持久连接；同一客户端连接可连续承载多个请求。Hyper 按 HTTP 版本和请求的 Connection 语义处理关闭。上游转发目前仍为每个请求独立建立 HTTP/1 client connection，不跨请求复用上游连接。
- HTTP/1 server/client connection 均启用 Hyper Upgrade 交接能力；符合条件的 `101` 会在下游和上游之间建立透明双向隧道。

### DomainConfig 处理

- 按原始请求 Host 匹配域名规则；Response 优先于 Destination。
- Response 支持配置状态码、Content-Type 和可选 UTF-8 正文；HEAD 不发送正文，204/205/304 按无正文响应处理。
- Destination 先以 `{scheme}://{original-host}/` 为基准解析 URI reference，再作为上游 destination prefix；原始请求 path/query 会追加到该前缀。
- 相对 Destination、绝对 Destination 和显式原始端口均有单元测试。
- 原始 Host 匹配出的 DomainConfig 会继续用于上游 IPAddress、TLS SNI、证书名称不匹配策略和请求超时，即使 Destination 改写了上游主机名。
- IPAddress 是优先候选，目标域名解析结果仍作为后备候选。
- 对已匹配的 DomainConfig，TlsSni 遵循原版非 nullable bool 的默认值：缺省/false 均不发送 SNI，只有显式 true 才发送；未匹配配置的普通代理请求保留 TLS 库的正常 SNI 行为。启用时默认 SNI 使用最终上游 URI 主机名，自定义 SNI 只改变 ClientHello 的 SNI，不改变证书名称验证目标。证书名称验证目标仍是最终上游 URI 主机名。

### 协议协商

- TLS 拦截入口通过 ALPN 选择 HTTP/2 或 HTTP/1.1；未协商 ALPN 时回退 HTTP/1.1。
- HTTPS 上游连接优先协商 HTTP/2，服务器未选择 `h2` 时回退 HTTP/1.1。HTTP/1 Upgrade/WebSocket 上游连接仅提供 `http/1.1`，避免误协商到不支持传统 Upgrade 的 HTTP/2。
- 当前 HTTP/2 不做跨请求连接池复用；HTTP/2 只覆盖 TLS ALPN 路径，明文 h2c 和 RFC 8441 extended CONNECT 不在本轮范围内。

### 超时

- TCP connect 和上游 TLS handshake 使用固定连接超时。
- DomainConfig.Timeout 按单个 HTTP 请求处理：DNS、连接、TLS 握手、等待上游响应头受同一个请求超时预算约束；响应体流式传输使用剩余预算，而不是从响应头到达后重新计时。
- 响应体超时会以 body error 终止流；此时如果响应头或部分响应体已经发送给客户端，无法再改写成新的 HTTP 状态码。
- 上游 Hyper connection driver 由响应体持有取消守卫：请求发送阶段失败/超时、响应体正常结束、发生超时/上游 body error 或下游丢弃响应体时都会 abort driver，避免遗留后台连接任务；正常读取响应体时 driver 保持运行。 新增响应体直接丢弃时释放 driver 的包装层测试；HTTP/2 提前响应 loopback 也改为让 `TimeoutBody` 持有 driver guard，读取完 413 响应后由 guard 结束仍有 pending 上传体的连接 driver，而不再在测试中手动 abort。

## 与原版仍有差异 / 待完成

1. **持久 HTTP/1 keep-alive：已实现第一阶段**：下游 Hyper HTTP/1 server 允许同一连接处理多个请求，响应生成器不再强制 `Connection: close`；新增同一连接发送两个请求的 TCP loopback 测试。上游仍是每请求独立连接，不做跨请求连接池复用。新增 TCP loopback 用例验证客户端发送 `Connection: close` 后服务端在响应完成时关闭连接，并验证客户端在 `Content-Length` 声明长度未发送完毕时断开后，入站请求体会以错误结束且 HTTP/1 connection task 不会遗留；这些测试尚未执行，真实代理转发链路中的断连与请求超时隔离仍待回归。
2. **HTTP/1 Upgrade / WebSocket：已实现转发主路径**：识别 HTTP/1.1 `Connection: Upgrade` 请求，保留并规范化两端 Upgrade 协商头；上游只有返回匹配的 `101 Switching Protocols` 才建立隧道。Hyper 两端通过 `with_upgrades()` 交接底层 IO，隧道使用双向流式复制；上游拒绝升级时回到普通响应体转发并继续使用原请求 Timeout。该路径支持 WebSocket 及其他符合 HTTP/1 Upgrade 语义的协议。新增 Hyper HTTP/1 Upgrade loopback 半关闭用例：客户端发送数据后只关闭上行写方向，服务端读到 EOF 后仍返回最终数据，验证隧道在一个方向结束后仍能完成反向传输；该用例覆盖 Hyper Upgrade 交接与 `copy_bidirectional` 的半关闭语义，但尚不等同于真实 TLS 代理链路的端到端验证。
3. **HTTP/2：已接入 ALPN 与 Hyper 双端处理**：TLS 拦截服务端通过 ALPN 协商 `h2` / `http/1.1`，协商为 `h2` 时使用 Hyper HTTP/2 server；访问 HTTPS Destination 时上游 TLS 客户端同时提供 `h2` 和 `http/1.1`，按协商结果选择 Hyper HTTP/2 或 HTTP/1 client。HTTP/2 请求仍进入同一套 DomainConfig、Response、Destination 和 Timeout 路由；`OPTIONS *` 的 asterisk-form request-target 会单独保留，并通过结构化 URI builder 转发，不会被错误拼接成主机名的一部分；入口要求非 CONNECT 请求的 `:scheme` 与当前 TLS transport scheme 一致（当前 HTTP/2 仅在 TLS 拦截后提供），并要求 `:authority` 存在；若同时携带 Host 则必须唯一且与 `:authority` 的主机名/端口一致，拒绝冲突值、userinfo 和无效端口，避免规则匹配主机与实际上游目标不一致。HTTP/2 上游请求使用 URI authority 作为 `:authority`，不发送重复 Host/Transfer-Encoding，并把上游响应版本归一为下游版本。上游使用 Hyper legacy Client 的 per-host 连接池；协商为 HTTP/2 时，池连接可跨请求复用并承载多个并发 stream（已有两个并发请求捕获同一 connection_id 的 loopback 覆盖）；明文 HTTP proxy 入口仍使用 HTTP/1，暂不提供 h2c prior-knowledge / upgrade，也未实现 RFC 8441 WebSocket extended CONNECT。HTTP/2 `CONNECT`（包括 extended CONNECT）目前明确返回 `501 Not Implemented`：其 authority-form / 隧道语义与普通 HTTP 请求不同，不能安全地套用普通 URL 转发流程；传统 raw CONNECT 隧道仍仅由 HTTP/1 入口处理。Hyper 当前没有 HTTP/2 server 侧自动发送 100 Continue 的接口，因此 HTTP/2 转发遇到 `Expect: 100-continue` 时明确返回 417，避免客户端等待 100、代理等待请求体造成死锁；HTTP/1 仍使用已实现的上游 100 门控。新增 HTTP/2 loopback 并发 stream 回归用例：一个 stream 等待响应头期间，另一个 stream 应能完成；取消等待中的 stream 后，同一 HTTP/2 连接还应能继续处理后续请求。该用例验证 Hyper HTTP/2 client/server 的多路复用与 stream 取消隔离，不等同于真实 TLS 代理链路的端到端验证。另将 HTTP/2 loopback 响应体扩大到 2 MiB，超过默认 stream flow-control window，验证持续消费 DATA frame 时传输能够完成；该项仍是 Hyper 双端传输回归，不代表真实上游代理链路的端到端测试。ALPN、真实代理链路中的并发 stream 和超时隔离仍待后续回归验证。新增 `hyper_http2_early_response_does_not_wait_for_request_body_completion`：上游在不消费请求体且请求体永不结束时立即返回 413，验证 HTTP/2 client 可及时交付提前响应；该测试仅覆盖 Hyper loopback 行为。ALPN dispatch 也明确限定为只有协商结果精确为 `h2` 才走 HTTP/2；无 ALPN、`http/1.1` 或其他协议值均回退 HTTP/1.1，并有单测覆盖。
4. **代理兼容性与边界测试**：继续补充 Expect: 100-continue、超大/分块请求体、Trailer、客户端提前断开、上游提前断开、超时发生在请求体上传期间等集成测试。当前已补充上游在请求体上传期间主动断开、以及请求超时发生在请求体仍 Pending 时的 Hyper HTTP/1 loopback 用例：前者验证 `send_request` 被断连唤醒并释放 driver，后者验证超时取消上传 future 后由 `UpstreamConnectionGuard` 终止 driver。HTTP/2 路径也新增对应的 Hyper loopback 用例：上游在收到请求头后断开，验证 pending upload 的 `send_request` 返回错误；请求体保持 Pending 时触发请求超时，验证取消发送 future 并释放 `UpstreamConnectionGuard`。HTTP/1 与 HTTP/2 这四项新测试均尚未执行，也不等同于真实代理入口的端到端断连/超时回归。Trailer frame 目前仍主要由包装层单测和 Hyper 双端 loopback 覆盖。
5. **Expect: 100-continue：已加入上游许可门控，仍需构建验证**：Hyper HTTP/1 client 会在收到上游 100 之前开始 poll 请求体；入站 Hyper HTTP/1 server 则在 Service 首次 poll `Incoming` body 时自动向下游发送 100。为协调这两个行为，`forward_request` 现对包含 `Expect: 100-continue` 的请求使用 `ExpectContinueBody`：先将请求头交给上游，body 保持 Pending；通过 `hyper::ext::on_informational()` 观察到上游 100 后才释放 body。这样首次 poll 入站 body 才会触发下游 100。若上游直接返回最终响应（例如 417），body 不会被释放，最终响应可以直接返回，下游上传也不会开始。请求体已经处于 end-stream 状态时，即使带有 Expect 头也不会启用门控，避免空请求体等待永远不会到来的 100。新增测试覆盖空 body 不启用门控、TCP loopback 中 gated body 在上游 100 后才上传、上游返回 417 时不上传，以及等待上游许可期间取消请求时关闭上游 driver；另保留原始 Hyper client 行为基线测试和入站 server 自动 100 触发时机测试。当前尚未在本地运行 cargo test，需用户构建验证 Hyper 版本解析/API 兼容性和测试时序。该实现只处理中继 100 Continue，不转发 103 Early Hints 等其他 1xx；项目 Hyper 依赖是 `version = "1"` 且没有 Cargo.lock，实际解析版本由用户构建环境的 Cargo 决定。
6. **代理标记**：已恢复原版 `FastGithub/1.0` User-Agent 标记：上游请求保留原 User-Agent 并追加标记；若入站请求已经包含该产品标记，则拒绝转发以防止 Destination 造成代理环路。响应 Server 头也会写入 `FastGithub`。
7. **完整互操作测试**：已新增下游 HTTP/1 同连接多请求、Upgrade 头保留/识别与双向字节交接、Hyper HTTP/2 client/server loopback、ALPN 选择和 HTTP/2 Expect 拒绝等主线协议测试；仍未取得可确认的 cargo fmt / cargo test / cargo build 结果。后续回归阶段继续覆盖真实代理链路中的 Expect、分块上传、Trailer、上下游提前断开、超时取消、TLS ALPN 与 WebSocket 双向隧道；Upgrade 的 Hyper loopback 半关闭语义已有独立用例，但真实 TLS 代理链路仍待验证。不能把源码测试数量当成实际通过的构建结果。

## 实现约束

- 不得删除普通 HTTP 与 TLS 解密后共用的 `http_server::serve` 路径。
- 不得移除 `domains` 配置规则列表在 Listener、TLS intercept 和 HTTP server 之间的传递。
- 不得把 HTTP/1 的 Hyper server/client、`send_request` 或 body streaming 替换回原始字节拷贝。
- Destination 改写目标主机时，必须区分 TCP 连接 IP、TLS SNI、证书验证主机名和 HTTP Host。
- DomainConfig 仍按原始请求主机匹配；Destination 只改变上游 URI。
- 每次修改后检查现有测试和调用链，先做最小补丁，再执行格式化、单元测试和构建验证。

## 历史说明

早期迁移阶段曾只有 Response 短路分支，普通 HTTP 未匹配请求返回 501，CONNECT TLS 仅作字节隧道。该状态已经过时，不应作为当前分支功能描述。

## GitHub 多 IP 无响应排查与 C# 对照（2026-11-21）

### 现场测试结果

对 `github.com:443` 使用 `curl --resolve` 固定目标 IP，并分别测试 HTTP/1.1 与 HTTP/2：

| IP | HTTP/1.1 | HTTP/2 | 观察到的阶段 |
|---|---|---|---|
| `20.27.177.113` | TCP connect 超时（5 秒） | TCP connect 超时（5 秒） | 尚未进入 TLS |
| `140.82.114.4` | HTTP 200 | HTTP 200 | TCP、TLS、HTTP 均成功 |
| `140.82.116.3` | TLS 成功，请求发出后 10 秒无响应 | TLS 成功，请求发出后 10 秒无响应 | HTTP 响应阶段无响应 |

同一批 IP 在不同时间的结果并不稳定：此前 `20.27.177.113` 曾返回 HTTP/2 200，之后又出现 TCP 超时。因此不能把单次失败永久解释为该 IP 不可用。

### 与 C# 原版的实现对照

- C# `FastGithub.DomainResolve/IPAddressService.cs` 的 `GetAddressElapsedAsync` 只测 TCP connect 延迟，按连接耗时排序；缓存成功延迟 5 分钟，失败结果按本机网络错误或普通错误分别缓存 1 分钟或 5 分钟。
- C# `FastGithub.Http/HttpClientHandler.cs` 的 `ConnectCallback` 按候选 IP 逐个尝试，每个 TCP connect 最多 10 秒；HTTPS 的 `ConnectAsync` 还会完成 TLS 握手和证书验证。TCP/TLS 连接失败会继续下一个 IP。
- C# 一旦 TCP/TLS 连接成功，就把连接交给 `SocketsHttpHandler` 发送 HTTP 请求。原版没有在请求已经发出、但等待响应头超时时，自动重建请求并切换到另一个 IP 的通用逻辑。
- Rust 的 `AddressService` 比 C# 的预探测更严格：预探测阶段完成 TCP + TLS 握手；真实请求阶段的 `connect_target_excluding` 也会逐个尝试 TCP，`UpstreamConnector` 在 TLS 握手失败时继续下一个候选，并对失败 IP 做 90 秒熔断。
- Rust 的 `send_pooled_request` 只在 Hyper legacy client 明确报告 `Error::is_connect()` 时淘汰当前域名连接池条目；普通请求错误可能来自 HTTP/2 单个 stream reset，也可能来自连接级故障，不能仅凭通用错误类型就销毁整个 per-host Client。Hyper 自身负责管理失效的池连接。请求错误日志同时读取 `Error::connect_info()`，尽可能记录失败关联的 peer IP/connection ID。该策略不把所有请求错误都认定为 stream 错误，只是避免在证据不足时执行整池淘汰。它不会把已发送的请求自动转发到另一个 IP；外层请求超时会取消转发 future，不能假定取消等价于收到一个可安全重放的请求错误。

### 当前结论

1. 这次现场结果不是 HTTP/2 独有故障：`140.82.116.3` 在 HTTP/1.1 和 HTTP/2 下都能完成 TLS、发送请求，但都没有收到响应；`140.82.114.4` 两种协议均正常。
2. 只做 TCP 或 TLS 健康探测无法识别“握手成功、HTTP 请求无响应”的节点。Rust 的 TLS 预探测比 C# 更严格，但仍不能证明 HTTP 应用层可用。
3. 不能把所有请求超时都直接记为 IP 故障：连接池可能复用 HTTP/2 多路复用连接，一个连接承载多个请求；而 POST 等非幂等请求在请求体已经发送后也不能盲目重放。
4. 因此当前不实现无条件的请求级换 IP 重试，也不禁用 HTTP/2。后续如增强应用层健康度，必须先能把具体请求关联到实际上游连接/IP，并区分可重放方法、请求体是否可重放、响应是否已经开始，以及 HTTP/2 共享连接上的并发请求。
5. DNSCrypt / fallback DNS 查询超时是另一条独立问题。持久化候选、hosts 映射和内置 seed 可能仍提供 IP，因此“刷新成功”不代表 DNS 上游查询本身正常。

### 后续工作方向

- 保留当前 TCP/TLS 失败时的候选 IP 故障转移行为。
- 继续检查 Hyper 上游连接池在连接驱动报错、请求取消和响应体中断时的生命周期；特别注意 HTTP/2 多路复用，不能因单个 stream 超时而误杀其他正常 stream。
- 若实现请求级故障转移，先限定为可安全重放且请求体可重建的请求，并以实际连接 IP 为故障记录依据；不对任意 POST/上传请求自动重放。
- 需要在用户环境验证真实代理链路，不能用直连 curl 的结果替代代理端到端测试。

### 本轮代码修正

- `src/http_proxy.rs` 的上游连接池原先在请求错误时直接按 host 删除条目。若旧连接池中的请求较晚失败，而同一 host 的新连接池条目已经建立，旧请求可能误删新条目。
- 现为每个连接池条目增加独立 identity；请求失败时只有当前条目的 identity 与请求使用的 identity 相同，才允许淘汰该条目。旧请求失败不会再误删替换后的连接池。
- 新增 identity 匹配单元测试。该测试尚未执行；当前环境没有本地 Cargo 工具链。
- 该修正解决的是连接池并发替换竞态，不代表已经解决 GitHub 某些 IP 在 TLS 成功后不返回 HTTP 响应的问题，也不增加请求重放或换 IP 重试。



## 上游连接 peer IP 可观测性（2026-11-21）

- `UpstreamConnector::connect` 在建立 TCP/TLS 连接时已经获得实际连接 IP，但此前成功日志只记录 host、port 和 ALPN，未输出选中的 peer IP。
- 现为每条新建上游传输连接分配单调递增的 `connection_id`，并在“连接就绪”和 Hyper 获取连接元数据时记录 `peer_ip`、`connection_id` 与 HTTP/2 协商结果；HTTP 明文与 HTTPS TLS 路径均携带实际选中的 IP。
- 这属于连接级观测，不等同于请求级关联。Hyper legacy client 的 HTTP/2 连接可复用并承载多个并发 stream；当前请求日志仍只有 host/method/URI，不能据此断言某条响应对应哪个 `connection_id`。后续若要建立请求到连接/IP 的准确关联，需要利用 Hyper 提供的连接扩展元数据或调整 connector/connection 驱动架构，并确认其对 HTTP/1 与 HTTP/2 均成立。
- 不因单个 HTTP/2 stream 超时而关闭共享连接；也不在请求已经发送后盲目换 IP 重放。先用新增连接日志与用户侧代理请求日志对照，确定是否存在特定 peer IP 反复出现“TLS 成功、HTTP 无响应”。
- 本次代码尚未由本地 Cargo 工具链格式化、编译或测试；没有触发 GitHub Actions/CI。


## 请求级上游连接关联（2026-11-24）

- 检查项目固定版本 `hyper-util = 0.1.20` 的源码后，确认 `client::legacy::connect::capture_connection` 已存在；`Client::try_send_request` 会在每次请求选定连接后，将该连接的 `Connected` 元数据写入请求携带的捕获句柄。连接池复用 HTTP/2 连接时，也会使用被选中的池连接元数据，而不是要求重新建立连接。
- `Connected::extra<T>()` 会将连接器附加的数据复制到每个响应的 extensions；本项目现在附加 `UpstreamPeerInfo { peer_ip, connection_id }`。`send_pooled_request` 在发出请求前调用 `capture_connection`，响应头到达或请求失败后读取捕获元数据，并在请求日志中记录 peer IP 与连接编号。
- 另启动一个轻量异步观察任务，在连接元数据一旦可用时立即输出 `upstream request assigned to connection`。这样即使外层请求超时取消了等待响应头的 future，仍能留下该请求已经分配到哪个上游连接的日志；如果连接始终未建立，请求扩展被释放后观察任务会结束。
- 新增 `connected_metadata_preserves_upstream_peer_identity` 单元测试，覆盖 `Connected::extra` 到捕获辅助函数的 peer IP/connection ID 传递；同时更新 HTTP/2 mock connector 初始化字段。
- 这一机制解决请求到连接的关联，不代表能够把单个 HTTP/2 stream 的超时判定为整条连接故障。后续应先采集请求分配连接、响应头延迟和 stream 错误日志；不能因一个 stream 超时而关闭仍承载其他请求的 HTTP/2 连接，也不对任意请求自动重放。
- 代码和测试尚未由本地 Cargo 工具链编译或执行；没有触发 GitHub Actions/CI。

- 将既有 HTTP/2 多路复用 loopback 测试扩展为两个并发请求分别调用 `capture_connection`，验证二者捕获到同一个 peer IP 和 `connection_id`，且底层只建立一条 HTTP/2 连接。该用例直接覆盖连接池复用场景下的请求级连接关联，而不仅是单独测试 `Connected::extra`。


## 上游响应体阶段的请求/连接关联（2026-11-24）

- `send_pooled_request` 已能把请求关联到实际选中的上游连接，但此前关联信息只出现在连接分配和响应头日志中；`TimeoutBody` 在响应体超时或 stream 读取失败时只输出通用日志，无法从该条日志反查请求和 HTTP/2 连接。
- 现在从 Hyper 响应 `Extensions` 读取连接器通过 `Connected::extra` 附加的 `UpstreamPeerInfo`，并将 method、原始 Host、request-target、peer IP 和 connection ID 作为轻量上下文传给 pooled response 的 `TimeoutBody`。
- 响应体超时与上游 body stream error 日志会携带这些字段。没有连接元数据时字段保持为空，不会伪造 peer IP 或 connection ID。
- 该包装只增加日志上下文，不缓存、不聚合、不预读响应体；每个 body frame 仍按原有 backpressure 逐帧转发。单个 HTTP/2 stream 超时/失败仍只终止该响应体，不主动关闭共享连接。
- 本次未改变独立 HTTP/1 Upgrade 分支的 body trace；该分支使用专属连接且已有独立 driver guard。没有本地 Cargo 工具链可用，本次未运行 fmt、build 或 test，也没有触发 GitHub Actions/CI。


## Hyper pooled request 错误分类与连接池淘汰边界（2026-11-24）

- 对照固定版本 `hyper-util 0.1.20` 的 `client/legacy/client.rs`：`Error::is_connect()` 公开区分 connector 建连错误；`Error::connect_info()` 在部分已选定连接的请求错误上提供 `Connected` 元数据。HTTP/2 response stream 错误和底层连接 driver 终止不能仅靠 `client.request()` 返回的通用错误一概归类为连接故障。
- `send_pooled_request` 现仅在 `is_connect()` 为 true 时淘汰当前 host 的 Client 条目；普通 `SendRequest` / `ChannelClosed` 等非 connector 错误只记录为 `request_or_connection`，不主动清空整个 per-host Client。Hyper 的 pool/connection 层仍负责处理已失效的底层连接。
- 错误日志优先从 `Error::connect_info()` 读取 peer IP 和 connection ID；没有连接信息时回退到 `capture_connection` 已记录的元数据。连接信息缺失时保留空值。
- 该分类是保守的淘汰策略，不声称已从 `hyper::Error` 精确区分 HTTP/2 RST_STREAM、GOAWAY 和 socket EOF。下一阶段应在不改动 Hyper driver 管理权的前提下，补充可验证的连接关闭/stream reset loopback 场景，观察各自错误类别与 pool 后续请求行为。
- 本轮没有更改请求重放策略；没有本地 Cargo 工具链，未运行 fmt/build/test，也未触发 GitHub Actions/CI。

## HTTP/2 单 stream reset 隔离回归（2026-11-24）

- 新增 `hyper_http2_stream_reset_does_not_poison_sibling_or_followup_streams`：同一条 HTTP/2 loopback 连接上，一个响应体在发送响应头后主动返回 body error，触发该 stream 的失败/reset；并发 sibling stream 仍应完整返回，失败 stream 结束后同一连接上的 follow-up 请求也应成功。
- 该用例补足了此前仅取消等待中 stream 的覆盖：现在分别覆盖请求 future 被取消，以及响应体阶段发生 stream 级失败两种不同路径。测试断言只要求失败 stream 返回 body error，不把 Hyper 的底层错误类型推断为某个精确的 RST_STREAM code，也不据此改变连接池淘汰策略。
- 这是 Hyper HTTP/2 client/server loopback 回归，不是 TLS 代理端到端测试。当前环境没有本地 Rust 工具链，尚未运行 fmt/build/test；没有触发 GitHub Actions/CI。用户手动构建时应重点确认 body error 确实只终止该 stream，且 sibling 与 follow-up 均通过。

## HTTP/2 GOAWAY 后连接池重建回归（2026-11-24）

- 新增 `pooled_client_reconnects_after_http2_goaway`：mock HTTP/2 上游在处理首个请求时发起 graceful shutdown，由 Hyper server 发送 GOAWAY 并排空活动 stream；首个响应消费完成后，再通过同一个 Hyper legacy Client 发起第二个请求。
- 测试要求第二个请求成功、捕获到不同的 `connection_id`，且 connector 总连接数为 2，以验证连接池不继续向已收到 GOAWAY 的 HTTP/2 连接分配新请求。
- 该用例针对连接级 graceful shutdown/GOAWAY，与单 stream body error 隔离测试互补。它不触发主动连接池清空，也不改变 `send_pooled_request` 的错误分类/淘汰策略；连接可用性由 Hyper pool 管理。
- 这是 mock connector + Hyper HTTP/2 loopback 测试。当前没有本地 Rust 工具链，未运行 fmt/build/test；没有触发 GitHub Actions/CI。需由用户手动验证 Hyper 当前解析版本下 GOAWAY 时序及第二次请求确实建立新连接。

## C# 超时语义对照与长响应断线修正（2026-11-24）

对照当前 `master` 的原始 C# 源码：

- `FastGithub.Configuration/DomainConfig.cs` 中 `Timeout` 是可空 `TimeSpan?`，默认 null。
- `FastGithub.Http/HttpClientHandler.cs` 仅在显式设置 `DomainConfig.Timeout` 时创建额外 timeout token，并将其传给 `base.SendAsync`。
- 自定义 `FastGithub.Http.HttpClient` 继承 `HttpMessageInvoker`，不是带默认 100 秒总超时的 `System.Net.Http.HttpClient`。
- 但 `HttpReverseProxyMiddleware` 使用 YARP 1.1.1 的 `IHttpForwarder.SendAsync`；YARP 自身另有默认 100 秒 `ActivityTimeout`。它是空闲超时，不是固定总耗时；成功的请求/响应 body 读写会刷新 deadline。

Rust 之前将默认 100 秒 ActivityTimeout 错误实现成从请求开始计算的固定总耗时，并将剩余期限继续套在响应体上。这会让一个持续有数据、但总时长超过 100 秒的上传/下载被主动中断，属于与原版不兼容的断线风险。

当前修正：
- `configured_request_timeout` 只保留显式 `DomainConfig.Timeout`，用于限制 forwarding / 等待响应头阶段；没有配置时不额外创建该总期限。
- 新增共享 `ActivityTimeout`，默认 100 秒，覆盖普通 HTTP 的连接/等待响应头、请求体上传和响应体下载。
- 请求 body wrapper 的 frame/EOF 活动会刷新共享 deadline；forwarding future 等待连接/响应头时与它竞争同一活动 deadline；收到响应头后重置；response body wrapper 继续使用同一时钟并在成功 frame 后刷新。
- 新增测试覆盖显式/缺省 DomainConfig.Timeout、持续上传 frame 刷新共享 deadline，以及响应体持续有 frame 时总耗时超过单次 idle timeout 仍可完成。

仍有边界差异：YARP 的 `StreamCopier` 在成功 read 和 write 两侧都会刷新活动时钟；Hyper Body wrapper 目前在 frame 产出时刷新，无法直接观察 socket write 完成。HTTP Upgrade/WebSocket 双向复制也尚未接入共享活动时钟。当前环境没有本地 Rust 工具链，未运行 fmt/build/test；没有触发 GitHub Actions/CI。

## YARP 1.1.1 ActivityTimeout 语义补充（2026-11-24）

进一步检查原版依赖的 `Yarp.ReverseProxy 1.1.1` 源码，确认 `ForwarderRequestConfig.Empty` 并非“无默认超时”：
- `HttpForwarder.DefaultTimeout` 为 100 秒，创建 `ActivityCancellationTokenSource`。
- 收到上游响应头后调用 `ResetTimeout()`。
- `StreamCopier.CopyAsync` 在每次成功读取输入数据、每次成功写入输出数据后重置活动计时器；请求体和响应体都覆盖。
- `ForwarderRequestConfig.Empty` 默认请求版本为 HTTP/2.0，版本策略为 `RequestVersionOrLower`。因此不能仅凭 FastGithub 的 URI scheme 被改写成 http，就断言 C# 上游固定使用 HTTP/1.1；最终协议由 SocketsHttpHandler 的版本协商行为决定。

Rust 普通 HTTP 转发现已引入共享 `ActivityTimeout`：100 秒 deadline 同时覆盖等待连接/响应头与请求上传；请求体 frame/EOF 会刷新共享时钟；收到响应头时重置；响应体 wrapper 继续使用同一时钟，并在成功 frame 后刷新。这样持续有数据的普通上传/下载不会因总耗时超过 100 秒而被硬切断。新增测试分别覆盖上传 frame 刷新共享 deadline、响应 frame 刷新 idle timeout。仍有边界差异：YARP 在成功 read 和 write 两侧均刷新，Hyper Body wrapper 只能在 frame 产出时刷新，无法直接观察 socket write 完成；HTTP Upgrade/WebSocket 双向复制也尚未接入共享活动时钟。显式 `DomainConfig.Timeout` 仍单独限制 forwarding/SendAsync 阶段，普通响应体阶段不继续使用该配置期限。

