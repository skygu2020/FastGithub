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
- 上游 Hyper connection driver 由响应体持有取消守卫：请求发送阶段失败/超时、响应体正常结束、发生超时/上游 body error 或下游丢弃响应体时都会 abort driver，避免遗留后台连接任务；正常读取响应体时 driver 保持运行。

## 与原版仍有差异 / 待完成

1. **持久 HTTP/1 keep-alive：已实现第一阶段**：下游 Hyper HTTP/1 server 允许同一连接处理多个请求，响应生成器不再强制 `Connection: close`；新增同一连接发送两个请求的 TCP loopback 测试。上游仍是每请求独立连接，不做跨请求连接池复用。测试尚未执行，后续回归阶段验证连接关闭、客户端断开及请求超时隔离。
2. **HTTP/1 Upgrade / WebSocket：已实现转发主路径**：识别 HTTP/1.1 `Connection: Upgrade` 请求，保留并规范化两端 Upgrade 协商头；上游只有返回匹配的 `101 Switching Protocols` 才建立隧道。Hyper 两端通过 `with_upgrades()` 交接底层 IO，隧道使用双向流式复制；上游拒绝升级时回到普通响应体转发并继续使用原请求 Timeout。该路径支持 WebSocket 及其他符合 HTTP/1 Upgrade 语义的协议。端到端握手、断连和半关闭行为尚待后续回归验证。
3. **HTTP/2：已接入 ALPN 与 Hyper 双端处理**：TLS 拦截服务端通过 ALPN 协商 `h2` / `http/1.1`，协商为 `h2` 时使用 Hyper HTTP/2 server；访问 HTTPS Destination 时上游 TLS 客户端同时提供 `h2` 和 `http/1.1`，按协商结果选择 Hyper HTTP/2 或 HTTP/1 client。HTTP/2 请求仍进入同一套 DomainConfig、Response、Destination 和 Timeout 路由；`OPTIONS *` 的 asterisk-form request-target 会单独保留，并通过结构化 URI builder 转发，不会被错误拼接成主机名的一部分；入口要求非 CONNECT 请求的 `:scheme` 与当前 TLS transport scheme 一致（当前 HTTP/2 仅在 TLS 拦截后提供），并要求 `:authority` 存在；若同时携带 Host 则必须唯一且与 `:authority` 的主机名/端口一致，拒绝冲突值、userinfo 和无效端口，避免规则匹配主机与实际上游目标不一致。HTTP/2 上游请求使用 URI authority 作为 `:authority`，不发送重复 Host/Transfer-Encoding，并把上游响应版本归一为下游版本。当前 HTTP/2 上游连接按请求建立，不做跨请求连接池复用；明文 HTTP proxy 入口仍使用 HTTP/1，暂不提供 h2c prior-knowledge / upgrade，也未实现 RFC 8441 WebSocket extended CONNECT。HTTP/2 `CONNECT`（包括 extended CONNECT）目前明确返回 `501 Not Implemented`：其 authority-form / 隧道语义与普通 HTTP 请求不同，不能安全地套用普通 URL 转发流程；传统 raw CONNECT 隧道仍仅由 HTTP/1 入口处理。Hyper 当前没有 HTTP/2 server 侧自动发送 100 Continue 的接口，因此 HTTP/2 转发遇到 `Expect: 100-continue` 时明确返回 417，避免客户端等待 100、代理等待请求体造成死锁；HTTP/1 仍使用已实现的上游 100 门控。新增 HTTP/2 loopback 并发 stream 回归用例：一个 stream 等待响应头期间，另一个 stream 应能完成；取消等待中的 stream 后，同一 HTTP/2 连接还应能继续处理后续请求。该用例验证 Hyper HTTP/2 client/server 的多路复用与 stream 取消隔离，不等同于真实 TLS 代理链路的端到端验证。另将 HTTP/2 loopback 响应体扩大到 2 MiB，超过默认 stream flow-control window，验证持续消费 DATA frame 时传输能够完成；该项仍是 Hyper 双端传输回归，不代表真实上游代理链路的端到端测试。ALPN、真实代理链路中的并发 stream 和超时隔离仍待后续回归验证。
4. **代理兼容性与边界测试**：继续补充 Expect: 100-continue、超大/分块请求体、Trailer、客户端提前断开、上游提前断开、超时发生在请求体上传期间等集成测试。当前仅完成 Trailer frame 在 `TimeoutBody` 包装层中的透传单测，尚未完成真实 TCP 上下游验证。
5. **Expect: 100-continue：已加入上游许可门控，仍需构建验证**：Hyper HTTP/1 client 会在收到上游 100 之前开始 poll 请求体；入站 Hyper HTTP/1 server 则在 Service 首次 poll `Incoming` body 时自动向下游发送 100。为协调这两个行为，`forward_request` 现对包含 `Expect: 100-continue` 的请求使用 `ExpectContinueBody`：先将请求头交给上游，body 保持 Pending；通过 `hyper::ext::on_informational()` 观察到上游 100 后才释放 body。这样首次 poll 入站 body 才会触发下游 100。若上游直接返回最终响应（例如 417），body 不会被释放，最终响应可以直接返回，下游上传也不会开始。请求体已经处于 end-stream 状态时，即使带有 Expect 头也不会启用门控，避免空请求体等待永远不会到来的 100。新增测试覆盖空 body 不启用门控、TCP loopback 中 gated body 在上游 100 后才上传、上游返回 417 时不上传，以及等待上游许可期间取消请求时关闭上游 driver；另保留原始 Hyper client 行为基线测试和入站 server 自动 100 触发时机测试。当前尚未在本地运行 cargo test，需用户构建验证 Hyper 版本解析/API 兼容性和测试时序。该实现只处理中继 100 Continue，不转发 103 Early Hints 等其他 1xx；项目 Hyper 依赖是 `version = "1"` 且没有 Cargo.lock，实际解析版本由用户构建环境的 Cargo 决定。
6. **代理标记**：已恢复原版 `FastGithub/1.0` User-Agent 标记：上游请求保留原 User-Agent 并追加标记；若入站请求已经包含该产品标记，则拒绝转发以防止 Destination 造成代理环路。响应 Server 头也会写入 `FastGithub`。
7. **完整互操作测试**：已新增下游 HTTP/1 同连接多请求、Upgrade 头保留/识别与双向字节交接、Hyper HTTP/2 client/server loopback、ALPN 选择和 HTTP/2 Expect 拒绝等主线协议测试；仍未取得可确认的 cargo fmt / cargo test / cargo build 结果。后续回归阶段继续覆盖真实代理链路中的 Expect、分块上传、Trailer、上下游提前断开、超时取消、TLS ALPN 与 WebSocket 双向隧道。不能把源码测试数量当成实际通过的构建结果。

## 实现约束

- 不得删除普通 HTTP 与 TLS 解密后共用的 `http_server::serve` 路径。
- 不得移除 `domains` 配置规则列表在 Listener、TLS intercept 和 HTTP server 之间的传递。
- 不得把 HTTP/1 的 Hyper server/client、`send_request` 或 body streaming 替换回原始字节拷贝。
- Destination 改写目标主机时，必须区分 TCP 连接 IP、TLS SNI、证书验证主机名和 HTTP Host。
- DomainConfig 仍按原始请求主机匹配；Destination 只改变上游 URI。
- 每次修改后检查现有测试和调用链，先做最小补丁，再执行格式化、单元测试和构建验证。

## 历史说明

早期迁移阶段曾只有 Response 短路分支，普通 HTTP 未匹配请求返回 501，CONNECT TLS 仅作字节隧道。该状态已经过时，不应作为当前分支功能描述。
