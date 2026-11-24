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
- 下游 HTTP/1 server 已启用持久连接；同一客户端连接可连续承载多个请求。Hyper 按 HTTP 版本和请求的 Connection 语义处理关闭。上游使用 Hyper legacy Client 的 per-host 连接池；HTTP/1 keep-alive 连接可跨请求复用（已有 loopback 测试验证同源两个请求只建立一条连接）。
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
- 上游 HTTP/2 使用 Hyper legacy Client 的 per-host 连接池，可跨请求复用并在同一连接上多路复用并发 stream；HTTP/2 只覆盖 TLS ALPN 路径，明文 h2c 和 RFC 8441 extended CONNECT 不在本轮范围内。

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

仍有边界差异：YARP 的 `StreamCopier` 在成功 read 和 write 两侧都会刷新活动时钟；Hyper Body wrapper 目前在 frame 产出时刷新，无法直接观察 socket write 完成。当前环境没有本地 Rust 工具链，未运行 fmt/build/test；没有触发 GitHub Actions/CI。

## YARP 1.1.1 ActivityTimeout 语义补充（2026-11-24）

进一步检查原版依赖的 `Yarp.ReverseProxy 1.1.1` 源码，确认 `ForwarderRequestConfig.Empty` 并非“无默认超时”：
- `HttpForwarder.DefaultTimeout` 为 100 秒，创建 `ActivityCancellationTokenSource`。
- 收到上游响应头后调用 `ResetTimeout()`。
- `StreamCopier.CopyAsync` 在每次成功读取输入数据、每次成功写入输出数据后重置活动计时器；请求体和响应体都覆盖。
- `ForwarderRequestConfig.Empty` 默认请求版本为 HTTP/2.0，版本策略为 `RequestVersionOrLower`。因此不能仅凭 FastGithub 的 URI scheme 被改写成 http，就断言 C# 上游固定使用 HTTP/1.1；最终协议由 SocketsHttpHandler 的版本协商行为决定。

Rust 普通 HTTP 转发现已引入共享 `ActivityTimeout`：100 秒 deadline 同时覆盖等待连接/响应头与请求上传；请求体 frame/EOF 会刷新共享时钟；收到响应头时重置；响应体 wrapper 继续使用同一时钟，并在成功 frame 后刷新。这样持续有数据的普通上传/下载不会因总耗时超过 100 秒而被硬切断。新增测试分别覆盖上传 frame 刷新共享 deadline、响应 frame 刷新 idle timeout。仍有边界差异：YARP 在成功 read 和 write 两侧均刷新，Hyper Body wrapper 只能在 frame 产出时刷新，无法直接观察 socket write 完成。HTTP Upgrade/WebSocket 隧道已单独接入共享活动时钟，详见后文；其 DNS/TCP/TLS 建连阶段仍需进一步对齐连接超时。显式 `DomainConfig.Timeout` 仍单独限制 forwarding/SendAsync 阶段，普通响应体阶段不继续使用该配置期限。

## HTTP Upgrade / WebSocket 活动超时（2026-11-24）

- Upgrade 独立 HTTP/1.1 连接分支现创建自己的共享 `ActivityTimeout`，默认 100 秒。
- 上游 IO 在 HTTP/1 handshake 前后通过 `ActivityIo` 包装；成功读取或写入非零字节时刷新活动时钟。发出 Upgrade 请求并等待上游响应头时，使用同一个 deadline。
- 收到非 101 响应时，响应体 `TimeoutBody` 继续使用同一活动时钟，并保留上游连接 driver guard。
- 收到合法 101 后，等待下游和上游 `OnUpgrade` handoff 的阶段受同一活动 deadline 保护；双向隧道的两端 IO 都通过 `ActivityIo` 包装，任一方向有实际字节读写都会刷新 deadline。隧道复制 future 由同一个活动时钟包围；连续无数据活动达到 100 秒时终止隧道并输出 host、request-target 和超时值。
- 新增 `upgraded_io_activity_resets_shared_idle_timeout`，使用 loopback duplex IO 验证跨越单个 40ms deadline 的连续字节读取仍能完成（每个字节间隔 25ms），避免将总连接时长误当作 idle timeout。
- 新增 `upgraded_tunnel_idle_timeout_expires_without_io_progress`，验证隧道两端没有任何读写活动时共享 10ms 测试 deadline 会终止等待；上游响应头阶段超时也会输出 host、request-target 和 100 秒活动期限。

限制：Upgrade 分支的 DNS 解析发生在逐 IP 连接预算之外；每个 HTTPS IP 候选的 TCP 建连与 OpenSSL TLS 握手现共用 10 秒 deadline，但这属于连接候选超时，不由隧道 `ActivityTimeout` 的读写活动刷新。

## C# 与 Rust 上游连接超时边界（2026-11-24）

对照 `FastGithub.Http.HttpClientHandler.ConnectCallback`：
- C# 对每个 IP 候选创建一个 10 秒 CancellationTokenSource，并将同一个 token 传给 `ConnectAsync`；该 10 秒预算同时覆盖 TCP connect 和 HTTPS TLS Authenticate。
- Rust 现由 `connect_target_excluding_with_deadline` 在每个 IP 候选开始时创建一个 10 秒 deadline，并将同一 deadline 传递给后续 `connect_upstream_tls`；TLS 阶段使用 `timeout_at` 消费 TCP 阶段剩余的时间，不再重新获得完整 10 秒。
- pooled HTTPS connector 和 HTTP Upgrade HTTPS 分支都使用这一共享 deadline。TCP 或 TLS 失败后仍排除/跳过该候选并尝试后续 IP；每个新候选会创建自己的 10 秒预算。
- 新增 `tcp_and_tls_phases_share_one_candidate_deadline`，模拟 TCP 阶段消耗部分预算后，TLS 只能使用剩余期限，并验证下一个 IP 候选获得独立预算。
- DNS 地址解析发生在逐 IP 连接预算之外；该预算只对齐 C# 的每候选 TCP/TLS 阶段，不代表 DNS 查询也被限制在这 10 秒内。



## DNS/IP 候选测速超时对照（2026-11-24）

- 原版 `IPAddressService.GetAddressElapsedAsync` 对每个候选 TCP connect 使用 5 秒超时；成功结果按连接耗时排序，失败地址不进入测速结果。成功测速缓存 5 分钟，本机网络不可达/断网错误缓存 1 分钟。
- Rust `AddressService::measure` 除 TCP connect 外还执行 TLS 握手与证书链验证（关闭 hostname matching，但仍使用原始 host 作为 SNI），因此测速比原版更严格。此前 Rust 将 TCP+TLS 整体限制为 2 秒；在其他候选探测成功时，超过 2 秒的慢候选会被排序阶段剔除，并把失败结果缓存 5 分钟，可能把暂时慢的可用节点误判为不可用。
- 现将 Rust 单候选测速预算调整为 5 秒，与原版超时上限对齐。该预算仍覆盖 TCP+TLS 两阶段；真实请求连接仍使用独立的每 IP 10 秒 TCP+TLS deadline。候选测速任务并行执行，不会按候选数量累加 5 秒。
- 若所有候选测速均失败，Rust 仍保留候选地址交由真实请求路径验证；这比原版 `GetAddressesAsync` 返回空列表更保守，避免一次测速故障清空 DNS/hosts 候选。请求阶段明确失败的 IP 仍按 host/port 熔断 90 秒。
- 本次只调整探测预算，不改变候选排序、TLS 验证、失败缓存 TTL 或请求阶段熔断策略。没有本地 Cargo 工具链，未运行 fmt/build/test；没有触发 GitHub Actions/CI。

## DNS 刷新与请求熔断并发安全（2026-11-24）

- 修复 `resolve()` 的锁顺序反转：此前它持有 `endpoints` 读锁时等待 `failed_ips` 锁；`report_failure()` 则先写 `failed_ips` 再写 `endpoints`，两者并发可能形成死锁。现在先在 endpoint 锁内复制候选与刷新时间，立即释放 endpoint 锁，再查询失败表。
- 修复后台 `refresh_endpoint()` 覆盖请求阶段熔断结果的竞态：测速阶段的 `exclude_failed()` 只是一个时间点快照，不能保证随后的 endpoint 写回仍排除刚刚失败的 IP。现在所有候选状态提交都经 `store_endpoint_state()`，持有失败表写锁直到 endpoint 快照提交，并在提交时重新过滤尚未过期的 host/port/IP 熔断项。
- 锁顺序统一为 `failed_ips -> endpoints`。`resolve()` 不再反向嵌套获取这两把锁；`report_failure()` 与 `report_success()` 也按同一顺序更新两个状态。
- 后台批量测速使用不触发单次持久化的 `store_endpoint_state()`，待所有 endpoint 刷新任务完成后再统一 `save_addresses()`；普通按需解析仍通过 `store_endpoint()` 在提交后持久化。
- `store_endpoint_state()` 返回实际提交的过滤后候选列表，按需解析将该列表返回给调用方，避免在熔断发生于测速筛选与 endpoint 提交之间时，内存状态虽已剔除 IP、当前这次 `resolve()` 却仍返回旧列表。后台刷新遇到 DNS 错误保留旧候选时也走同一提交路径，不再直接覆盖 endpoint 状态。
- 这些修正不改变 90 秒熔断期限、测速缓存 TTL、候选排序或请求重试策略。当前无本地 Cargo 工具链，未运行 fmt/build/test；没有触发 GitHub Actions/CI。


## Hosts 源 User-Agent 试验（2026-11-24）

- 针对 `https://hosts.gitcdn.top/hosts.txt` 的请求单独添加 `User-Agent: FastGithub/1.0`，用于验证该站点是否因默认客户端标识而拒绝或延迟响应。
- 只对 gitcdn 源设置该头；`raw.hellogithub.com` 请求保持原样，便于通过同一次并发刷新对照两个来源，避免把试验变量扩散到所有 hosts 服务。
- 这只是诊断性尝试，不代表已经确认 gitcdn 要求该 User-Agent。下一次启动/刷新重点观察 gitcdn 是否仍出现 timeout，以及是否取得有效 hosts 内容；若仍超时，应继续检查 DNS、TCP、TLS 或响应头阶段，不能仅凭结果归因于 UA。
- 未改变 20 秒请求超时、禁用系统代理或来源合并/回退逻辑。当前环境没有本地 Cargo 工具链，未运行 fmt/build/test；没有触发 GitHub Actions/CI。


### Hosts User-Agent 试验结果（用户环境，2026-11-24）

- 用户在加入定向 `User-Agent: FastGithub/1.0` 后重新运行，日志显示 `hosts.gitcdn.top` 与 `raw.hellogithub.com` 均建立连接并成功完成下载；最终记录为 `online hosts mapping refreshed sources=2 total_sources=2 domains=41`，启动汇总为 `success=true domains=41 errors=`（错误列表为空）。
- 这是添加 UA 后 gitcdn 下载成功的正向观测，说明当前实现可正常获取该源；但单次前后对比不能严格证明成功完全由 UA 引起，网络状态变化也是可能因素。暂时保留该 UA，不扩大到其他 hosts 源。
- 同一轮日志仍有若干 DNS 上游查询超时（包括本地 dnscrypt-proxy endpoint 和 `8.8.8.8:53`），但部分域名仍完成解析与候选测速排序，整轮 hosts/DNS 刷新最终成功。候选 TCP/TLS probe timeout 是单个候选测速未在预算内完成，不应直接解释为真实请求必然不可用；需结合后续实际请求结果判断。
- 本轮没有修改 DNS 候选、探测超时或故障熔断策略；没有本地 Cargo 工具链，未运行 fmt/build/test；没有触发 GitHub Actions/CI。


## 加密 DNS 客户端内嵌化可行性分析（2026-11-24）

目标：评估把当前随 Linux 发行物提供的 Go `dnscrypt-proxy` 从外部子进程替换为 Rust 进程内客户端。此阶段只记录协议能力和迁移边界，不删除现有代理、不改 DNS 运行路径。

### 当前实现边界

- `src/dnscrypt.rs` 负责查找外部可执行文件和 TOML，选择 loopback 临时端口，修改 `listen_addresses` / 日志 / 负载均衡 / 缓存参数，启动子进程并等待 TCP 端口就绪；退出时 kill/wait。
- `src/main.rs` 在正常代理启动、`resolve`、`test-dns`、`watch-dns` 中均先尝试启动该进程，再把返回的 loopback `SocketAddr` 放到 DNS 上游列表首位，最后追加 `FastGithub.FallbackDns`。
- `src/dns.rs` 只实现传统 DNS wire query：A/AAAA、UDP 查询、TC 截断后 TCP 重试、响应 ID 校验、TTL 缓存及公共 53 端口失败熔断。它不理解 DNSCrypt、DoH 或 ODoH。
- 因此当前加密协议、resolver 列表/证书更新、上游选择与负载均衡都在 Go 子进程内；Rust 主程序只把它当作一个本地普通 DNS 服务器。仅把 `dnscrypt-proxy` 的 UDP/TCP socket 改成 Rust socket，并不能完成客户端内嵌化。

### 可复用的 Rust 协议组件

1. **DNSCrypt v2：`dnscrypt` crate 0.3.x**
   - 文档确认它是 Tokio 异步、纯 Rust 的 DNSCrypt v2 客户端，支持 resolver stamp、签名证书验证、会话密钥、查询缓存和可选 Anonymized DNSCrypt relay。
   - 它提供 `Client::lookup` / `Client::query`，不需要再启动 loopback DNS 服务；可直接在 DNS 解析调用链中查询并返回 A/AAAA。
   - 该 crate 文档描述的是 DNSCrypt v2，不应据此推断它实现了 PQDNSCrypt。

2. **DoH / DoT / DoQ / DoH3：Hickory Resolver**
   - Hickory 当前文档列出 DoT、DoH over HTTP/2、DoQ、DoH over HTTP/3 的可选协议支持，并提供 resolver pool、缓存和 DNSSEC 能力。
   - 它可以作为多个标准加密 DNS transport 的统一实现，但不是 DNSCrypt/ODoH 的替代品。
   - 项目当前 `rust-version = 1.78`；不能直接把 docs.rs 的最新版作为依赖。选定版本前必须核实该版本 MSRV、Tokio 兼容性和 TLS provider，避免抬高项目最低 Rust 版本或引入第二套冲突的 TLS 栈。

3. **ODoH：`odoh-rs`**
   - 该库实现 RFC 9230 的 ODoH 消息/HPKE 加密协议，文档说明默认套件为 X25519/HKDF-SHA256/AES-GCM-128。
   - 它不是完整 DNS resolver 服务；客户端仍需实现 target-config 获取/缓存、HTTP POST 传输、响应解密、超时、重试和密钥轮换处理。不能只增加依赖就视为 ODoH 已完成。

4. **PQDNSCrypt**
   - 当前检索到的 Rust `dnscrypt` crate 文档只承诺 DNSCrypt v2；没有找到可确认支持 DNSCrypt 2026 后量子握手的 Rust 客户端实现。
   - Go 代理当前上游配置模板已出现 `pqdnscrypt` 选项。实际随项目打包的 Go 版本和 TOML 必须再核对，不能因上游模板出现该选项就假定本项目打包版本已启用。
   - 在找到可验证的实现并通过互通测试前，PQDNSCrypt 是阻止“完全替换外部代理”的明确缺口。

### 与当前 Go 代理的能力差距

| 能力 | 现有 Go 子进程 | 可用 Rust 组件/状态 |
|---|---|---|
| DNSCrypt v2 | 支持 | `dnscrypt` crate 可实现基础客户端 |
| Anonymized DNSCrypt relay | 支持 | `dnscrypt` crate builder 有 relay 配置 |
| DoH | 支持 | Hickory 可提供 DoH；需适配现有配置 |
| DoT / DoQ / DoH3 | 取决于 Go 配置与版本 | Hickory 可选支持 |
| ODoH | 支持 | `odoh-rs` 提供协议层，客户端 transport/配置管理需自行整合 |
| PQDNSCrypt | 新版 Go 代理有配置选项 | 尚无已确认可用的 Rust 等价实现 |
| resolver sources / stamp 列表更新 | Go 代理内建 | 需要自行实现来源下载、签名/格式校验、原子更新 |
| 服务器筛选、测速、负载均衡 | Go 代理内建 | 需要统一调度层；不能只依赖各库默认策略 |
| 证书/配置刷新、缓存、失败退避 | Go 代理内建 | 需要统一生命周期、缓存和并发控制 |

### 推荐的融合架构

不要把多个协议 crate 直接塞进当前 `DnsClient` 的 `Vec<SocketAddr>`。应把“加密 DNS 上游”抽象为异步查询接口，返回统一 DNS 响应/地址结果：

- `EncryptedDnsBackend`：统一查询接口、超时和错误分类。
- `DnsCryptBackend`：封装 `dnscrypt::Client`，先实现 DNSCrypt v2 与 relay。
- `HickoryBackend`：封装 DoH/DoT/DoQ/DoH3，按项目选定版本和 feature 编译。
- `OdohBackend`：后续单独接入 ODoH target config 与 HTTP transport。
- `ResolverCatalog`：读取/更新 resolver stamps 与来源列表，校验后原子替换。
- `ResolverPool`：负责候选选择、并发查询、延迟统计、失败退避和负载均衡；不要让各 backend 各自维护彼此不一致的服务器优先级。
- 当前 `DnsClient` 保留为上层地址解析门面：hosts 优先、A/AAAA 合并、地址去重、TTL 与现有 endpoint 测速/持久化逻辑继续复用。

隐私策略必须显式化：加密上游失败时是否允许退回 `FallbackDns` 明文 DNS，不能默认静默降级。建议配置一个明确的 fallback policy，并在日志中区分“加密查询失败”和“用户允许明文回退”；默认策略应避免意外泄漏查询。

### 分阶段实施顺序

1. **先建立协议能力与配置映射表**：逐项读取随项目打包的 TOML、resolver source、stamp、relay、bootstrap 和缓存设置；记录 Go 代理实际版本及启用项。这是后续验收基线。
2. **实现 DNSCrypt v2 内嵌 backend**：使用静态 resolver stamps 做最小互通，不先实现远程 source 管理；保留 Go 子进程作为可切换的旧 backend。验证 A/AAAA、超时、证书过期/刷新、并发查询和 relay。
3. **接入 Hickory 的 DoH/DoT/DoQ/DoH3**：只选择与 Rust 1.78 兼容的版本/feature；逐协议验证 TLS 校验、HTTP/3 运行时依赖、IPv4/IPv6、缓存和连接复用。
4. **补 resolver catalog 与调度层**：实现来源下载、stamp 解析、筛选、探测、负载均衡、失败退避和热更新，使其行为接近当前 Go 配置，而不是只支持硬编码服务器。
5. **再评估 ODoH 与 PQDNSCrypt**：ODoH 需补齐客户端 transport/config 生命周期；PQDNSCrypt 需先找到可审计、可互通的实现。未完成前继续保留 Go fallback。
6. **双 backend 对照运行**：同一批域名同时查询 Rust backend 与 Go 子进程，比较答案集合、DNSSEC 状态、TTL、错误、延迟和失败恢复；确认无意外明文回退后，才考虑默认切换。
7. **最后移除 Go 发行物与进程管理**：必须在所有启用协议达到验收标准后进行，不能先删二进制再补协议。

### 本阶段结论

- 可以开始把 DNSCrypt v2 作为第一个进程内 Rust backend；这是最小、边界清晰的切入点。
- 不能把 `dnscrypt` crate 当成当前 Go `dnscrypt-proxy` 的完整替代品：DoH/ODoH/PQDNSCrypt、resolver source 管理和统一调度仍有缺口。
- 第一阶段不改 `src/dns.rs` 的现有普通 DNS 行为、不移除 `src/dnscrypt.rs`，先新增可独立测试的 backend，再通过配置开关做双栈验证。
- 当前环境没有本地 Cargo 工具链；本分析没有执行 build/test，也没有触发 GitHub Actions/CI。

参考：
- https://docs.rs/dnscrypt/latest/dnscrypt/
- https://docs.rs/hickory-resolver/latest/hickory_resolver/
- https://docs.rs/odoh-rs/latest/odoh_rs/
- https://github.com/DNSCrypt/dnscrypt-proxy/blob/master/dnscrypt-proxy/example-dnscrypt-proxy.toml


## 原生 DNSCrypt v2 后端首轮接入（2026-11-24）

- Cargo 增加可选 feature `native-dnscrypt`，通过可选依赖 `dnscrypt = 0.3` 引入进程内 DNSCrypt v2 客户端；默认 feature 集合保持为空，不改变现有默认发行行为。
- `DnsClient` 在启用该 feature 时可持有可克隆的 `dnscrypt::Client`，新增 `with_dnscrypt()` 构造入口。查询直接调用 `Client::lookup`，不经过 loopback DNS socket，也不启动 Go 子进程。
- `main.rs` 将 DNS 客户端创建集中到 `build_dns_client()`：默认构建仍发现并启动现有 Go `dnscrypt-proxy`；启用 `--features native-dnscrypt` 时改用 `dnscrypt::Client::default()`，不启动外部代理。
- 原有 `resolve`、`test-dns`、`watch-dns` 和 HTTP 代理启动共用这一选择逻辑。HTTP 代理现在接收已构造的 `DnsClient`，而不是只接收 UDP/TCP 上游地址列表。
- 原生 backend 查询受现有 4 秒 DNS 查询预算约束。成功响应（包括有效的空答案）不会再继续查询明文 fallback DNS；只有 DNSCrypt 错误或超时才进入配置的普通 DNS fallback 列表。这避免在加密查询成功时仍无条件向明文 DNS 泄漏同一域名。
- 当前原生客户端使用 crate 内置 `DEFAULT_RESOLVERS`，尚未读取 Go TOML 中的 resolver stamps、server_names、匿名 relay、ODoH、PQDNSCrypt 或来源更新配置；因此它是可选的 DNSCrypt v2 验证后端，不是 Go 代理的功能等价替代。
- `dnscrypt::Client::lookup` 的公开 API 返回 IP 列表，不暴露 DNS 记录 TTL；该 crate 自身维护响应缓存，当前 FastGithub 的 endpoint 缓存仍按既有机制运行。后续若要精确复用 DNS TTL，应评估 `Client::query` 的记录 API，而不是人为猜测 TTL。
- 手动试验构建命令：`cargo build --release --features native-dnscrypt`。该 feature 是显式 opt-in；普通 `cargo build --release` 仍走 Go 子进程路径。
- 当前环境没有本地 Cargo 工具链，未运行 fmt/build/test，也没有触发 GitHub Actions/CI。

参考：`dnscrypt 0.3.0` 的 `Client::default` / `Client::lookup` API 和默认 resolver 说明见 https://docs.rs/dnscrypt/0.3.0/dnscrypt/struct.Client.html 及 https://docs.rs/dnscrypt/0.3.0/dnscrypt/。


## DNSCrypt crate 的 MSRV 阻断（2026-11-24）

继续核对候选 `dnscrypt 0.3.0` 的实际发布依赖后，发现它不满足项目声明的 Rust 1.78：

- `dnscrypt 0.3.0` 依赖 `ed25519-dalek ^3` 与 `x25519-dalek ^3`。
- 这两个 3.x 版本将 edition 更新为 2024，MSRV 提高到 Rust 1.85；其中 `ed25519-dalek 3.0.0` 的 Cargo 元数据明确声明 `rust-version = "1.85"`。
- `dnscrypt 0.1.1` 也已使用 edition 2024，并在其 Cargo.toml 中明确声明 Rust 1.87。因此回退到该 crate 的较早版本不能解决兼容问题。
- 项目 `Cargo.toml` 仍声明 `rust-version = "1.78"`。在未获准提高项目 MSRV 前，不应把该 crate 加入依赖，即使它是 optional feature，也会增加旧 Cargo 解析和锁文件解析风险。

因此撤回了刚才试接的 `dnscrypt` optional dependency、feature 和调用代码，恢复原有 Go 子进程 DNSCrypt 路径；没有留下一个在声明 MSRV 下不可构建的可选功能。

### 下一步选择

1. **保持 Rust 1.78**：评估将 DNSCrypt v2 协议实现移植/内嵌到项目，并选择明确支持 Rust 1.78 的密码学依赖版本；需要审计协议证书验证、会话密钥、nonce 和加密封包，工作量明显大于接一个 crate。
2. **允许将 MSRV 提高到至少 1.87**：可以重新接入 `dnscrypt 0.3`，但必须同步更新项目最低 Rust 版本要求，并确认部署环境工具链。
3. **先内嵌 DoH/DoT/DoQ/DoH3**：Hickory Resolver 0.25.2 的发布元数据显示 MSRV 1.71.1，支持以 `https-ring`、`tls-ring`、`quic-ring`、`h3-ring` feature 启用对应传输；这条路线不等价于 DNSCrypt，但能先替换 Go 代理中的部分加密 DNS 能力。

本轮撤回了不兼容的代码试验；只保留本节兼容性结论。没有本地 Cargo 工具链，未运行 fmt/build/test，也没有触发 GitHub Actions/CI。

参考：
- https://docs.rs/crate/dnscrypt/0.1.1/source/Cargo.toml.orig
- https://docs.rs/crate/ed25519-dalek/3.0.0
- https://docs.rs/crate/x25519-dalek/3.0.0
- https://docs.rs/crate/hickory-resolver/0.25.2


## 原生 DoH 后端（Hickory）首轮接入（2026-11-24）

由于纯 Rust `dnscrypt` crate 的依赖链要求 Rust 1.85/1.87，与项目 Rust 1.78 声明冲突，本轮先采用兼容项目 MSRV 的 Hickory Resolver 0.25.2，接入一个独立的 DoH 验证 backend；这不是 DNSCrypt v2 的替代实现。

- Cargo 增加 opt-in feature `native-doh`，精确固定 `hickory-resolver = 0.25.2`，关闭默认 feature，只启用 `tokio`、`https-ring`、`webpki-roots`。该版本文档声明 MSRV 1.70，低于项目的 1.78。
- `DnsClient` 在启用 feature 时可持有 `Arc<TokioResolver>`，新增 `with_doh()` 构造入口；使用 Hickory 的 `ResolverConfig::cloudflare_https()` 创建 Cloudflare DoH-only resolver，不读取系统 DNS 配置，也不把 UDP/TCP 53 作为该 resolver 的 transport。
- `DnsClient::resolve` 先执行 DoH A/AAAA lookup，成功后使用 Hickory 返回的 IP 集合；有效空答案也视为加密查询成功，不会继续向明文 fallback DNS 发送同一查询。仅 DoH 错误或超时才使用现有 `FastGithub.FallbackDns` 列表。
- `main.rs` 把 DNS client 创建集中到 `build_dns_client()`：默认构建继续使用原 Go `dnscrypt-proxy`；使用 `--features native-doh` 时不启动 Go 子进程，改为进程内 DoH。HTTP 代理入口改为接收已构造的 `DnsClient`，三个 DNS CLI 命令也共用该选择逻辑。
- Hickory 自己维护 DNS 响应缓存，并在 `LookupIp` 暴露 `valid_until()`；FastGithub 不再对 DoH 结果另建一份按 socket 地址索引的缓存。显式 `DnsClient::clear_cache()` 现同时清理 Hickory resolver cache，避免 `test-dns` / 定期刷新仍读到旧 DoH 结果；当前仍由 Hickory 自己按 DNS TTL 控制正常缓存过期，没有把 `valid_until()` 转换成 FastGithub 自己的 TTL。
- 该 feature 只实现 Cloudflare DoH。它不提供 DNSCrypt、匿名 DNSCrypt relay、ODoH 或 PQDNSCrypt；Go 配置的 resolver catalog、server selection、测速、负载均衡和远程来源更新尚未迁移。
- 手动试验构建命令：`cargo build --release --features native-doh`。默认构建行为不变。当前没有本地 Cargo 工具链，未运行 fmt/build/test，也没有触发 GitHub Actions/CI。

参考：
- Hickory 0.25.2 MSRV 与 DoH feature：https://docs.rs/crate/hickory-resolver/0.25.2
- 0.25.2 feature 列表：https://docs.rs/crate/hickory-resolver/0.25.2/features
- 0.25.2 DoH 测试使用 `ResolverConfig::cloudflare_https()`：https://docs.rs/crate/hickory-resolver/0.25.2/source/src/h2.rs

## 原生 DoH 首次手动解析验证（用户环境，2026-11-24）

用户在启用 `native-doh` 的构建上执行：

```text
./fastgithub resolve github.com:443
2026-11-24T19:30:06.731343Z  INFO using in-process Cloudflare DNS-over-HTTPS; plain DNS servers are fallback only
20.27.177.113
57.155.223.240
20.205.243.166
20.247.166.64
20.29.134.23
20.233.83.145
20.26.156.215
140.82.113.3
```

- 命令正常结束并输出 8 个候选地址；启动日志确认走进程内 Cloudflare DoH 路径，配置的普通 DNS 仅作为错误/超时后的 fallback。
- 本次输出中包含 `57.155.223.240`，与此前记录的 GitHub 候选列表不同，说明本次解析得到的地址集合存在变化；候选 IP 本身会随 DNS/CDN 状态变化，不应硬编码为固定清单。
- 这证明当前用户环境中的 Hickory DoH 查询链路可用，并成功向上层 resolver 提供地址。由于本次没有记录 fallback DNS 查询日志，只能结合代码路径和启动信息判断本次没有观察到明文回退；不能据此证明 DoH 失败时的 fallback 分支也已验证。
- 该命令只验证解析和候选地址输出，不验证通过 HTTP 代理访问 GitHub 时的 TCP/TLS、上游 HTTP 响应或连接池行为。后续应单独用 `native-doh` 构建启动代理，再访问 GitHub 页面并结合 peer IP / connection ID 日志观察。
- 用户尚未提供 Rust 1.78 工具链构建结果；本次记录不代表 `cargo build`、`cargo test` 或 `cargo fmt` 已通过。没有触发 GitHub Actions/CI。

## 原生 DoH 代理访问 GitHub 的首次实测（用户环境，2026-11-24）

用户以启用 `native-doh` 的二进制启动代理并打开 `https://github.com/Cyridd/cmpunlocker/issues`。本次关键日志：

```text
HTTP request completed method=GET target=github.com/Cyridd/cmpunlocker/issues status=200 elapsed_ms=871
HTTP request completed method=GET target=github.com/Cyridd/cmpunlocker/issues/deferred?... status=304 elapsed_ms=436
HTTP request completed method=GET target=github.com/Cyridd/cmpunlocker/issues/views.json status=304 elapsed_ms=313
HTTP request completed method=GET target=github.githubassets.com/assets/...js status=200 elapsed_ms=447
HTTP request completed method=GET target=github.com/.../ws?... status=101 elapsed_ms=1355
HTTP request completed method=POST target=api.github.com/_private/browser/stats status=200 elapsed_ms=920
initial hosts and DNS/IP refresh completed success=true domains=41 errors=
```

- 这是原生 DoH 首次真实代理访问验证：Issues 页面主请求返回 200；GitHub 页面接口与缓存校验请求返回 304；静态 JS 返回 200；WebSocket 返回 101；GitHub API stats 请求返回 200。此前 GitHub 页面请求长时间没有响应头、最终触发 100 秒 activity timeout 并返回 502 的问题，本次访问没有重现。
- 多个 `collector.github.com/github/collect` POST 返回 422，但页面、静态资源、WebSocket 与 API 请求仍成功。当前日志不足以判断 422 的服务端业务原因；不能把它当作代理 TCP/TLS 故障，也不应据此改变转发逻辑。
- 运行期间有三个 DoH 查询在 4 秒预算内未完成：`private-user-images.githubusercontent.com`、`viewscreen.githubusercontent.com`、`release-assets.githubusercontent.com`。代码随后进入配置的普通 DNS fallback；之后这些域名都出现了 endpoint speed order 更新，且整轮 hosts/DNS/IP 刷新最终成功（41 domains、空 errors）。这说明当前 fallback 路径在该轮运行中帮助维持了可用性，但日志没有单独标明每个地址究竟来自 fallback 查询、已有缓存还是 hosts 映射，不能把来源归因到某一种。
- `github.com:443` 的测速排序更新为 `[20.247.166.64, 20.233.83.145, 20.27.177.113]`。后续多个请求/连接尝试记录到这些候选以及 `57.155.223.240` 的 TCP connect deadline 约 10 秒超时。与此同时浏览器请求仍有 304 响应，说明这些候选连接超时没有阻断全部并发请求；不能从聚合日志推断每条成功响应最终使用了哪个 peer IP。
- 这些 TCP 超时发生在 TCP connect 阶段，尚未进入 TLS，更不是此前“TLS 成功但 HTTP 响应头不返回”的同一种故障。应继续观察候选 IP 的实际成功连接和熔断状态；不能仅凭单次超时永久剔除 IP。
- 本次日志没有显示 `upstream transport connection ready` 的 peer_ip/connection_id 记录，因此尚不能将具体成功请求精确关联到一个 GitHub peer IP。后续如需定位慢请求，应保留连接建立日志，并补充请求级关联信息；不要因单个 HTTP/2 stream 失败而关闭共享连接，也不要盲目重放已发送请求。
- 当前 4 秒 DoH timeout 会触发明文 fallback，这是现有实现的明确隐私降级行为，不是 DoH 仍在查询明文 DNS。后续应把 fallback policy 变成显式配置/策略：允许时记录降级原因，不允许时失败关闭；在用户确认策略前不擅自改变默认行为。
- 本轮没有修改连接候选、超时、DoH fallback 或请求重试策略。没有本地 Cargo 工具链，未运行 fmt/build/test；没有触发 GitHub Actions/CI。

## 上游连接失败日志补充协议标识（2026-11-24）

用户实测日志出现一次 pooled request failure：GET 请求分配到 `20.205.243.166`、`connection_id=1`，等待约 32 秒后 Hyper 返回 `client error (SendRequest)`，代理向客户端返回 502。该日志能确认请求关联到某条已有上游连接，但还不足以判断它是 HTTP/1.1 keep-alive 连接问题还是 HTTP/2 stream/connection 问题。

为缩小诊断范围：

- `UpstreamPeerInfo` 现在随 peer IP 和 connection ID 一起记录协商结果 `http2`。
- 连接分配、收到响应头、pooled request failure 日志都输出该字段。
- 上游响应 body stall / body stream failure 日志也输出对应连接协议。
- 该改动只增加诊断元数据，不改变连接池寿命、HTTP 协议协商、错误淘汰策略或请求重试行为。
- 不能把普通 `SendRequest` 错误一概视为连接级故障：HTTP/2 stream reset 不应自动导致整个 per-host client pool 被清除；也不能盲目重放请求，尤其是 POST。

用户 2026-11-24 的同一轮日志还显示：`hosts.gitcdn.top` 在 response headers 阶段耗尽 20 秒请求预算；DoH 对三个域名分别耗尽 4 秒预算；一个 `api.github.com` TCP 候选连接耗尽 10 秒候选期限。这些是三条独立链路，不能合并归因。

本轮只增加 HTTP/1.1/HTTP/2 诊断标识，没有本地 Cargo 工具链可执行 fmt/build/test，也没有触发 GitHub Actions/CI。

## 原生 DoH 明文回退策略配置化（2026-11-24）

根据用户实测，Cloudflare DoH 对部分域名偶发超过当前 4 秒查询预算；此前实现会自动将这些查询发送到 `FastGithub.FallbackDns` 中的普通 DNS。为避免加密查询失败时无提示地泄漏查询名称，本轮将该行为改为显式配置：

- `FastGithubConfig` 新增 `AllowPlainDnsFallback`，serde 默认值为 `false`。
- 启用 `native-doh` 时，默认严格模式只使用内置 Cloudflare DoH；DoH 查询错误或超过 4 秒时返回错误，不再自动访问普通 DNS。
- 用户明确设置 `"AllowPlainDnsFallback": true` 后，DoH 错误/超时才会查询 `FallbackDns`。DoH 正常返回（包括有效空答案）仍不会触发明文查询。
- 启动日志会明确显示 strict mode 或 plaintext fallback explicitly allowed；实际进入回退时也会输出单独警告。
- `FallbackDns` 仍是程序配置中的明确上游列表；没有引入系统 resolver 列表发现，也没有让 Hickory DoH resolver 使用系统 DNS。
- 未启用 `native-doh` 的默认构建仍沿用原 Go DNSCrypt 子进程和既有 DNS 上游行为；新增布尔字段不改变该路径。

如需保留本次实测时“DoH 超时后继续普通 DNS”的行为，在 appsettings 配置中加入：

```json
{
  "FastGithub": {
    "AllowPlainDnsFallback": true
  }
}
```

该选项只控制原生 DoH 失败后的明文回退，不会把普通 DNS 变成 DoH 的首选上游。新增配置默认值有单元测试覆盖；当前环境没有本地 Cargo 工具链，未运行 fmt/build/test，也没有触发 GitHub Actions/CI。
