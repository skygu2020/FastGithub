# HTTP 代理移植记录

本文记录 `linux-rust` 分支当前 HTTP/1 代理实现，避免把已经完成的功能误判为待实现。原版行为参考：

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
- 请求和响应的 hop-by-hop headers，以及 `Connection` 指定的逐跳字段会被移除；请求中的端到端 `Expect` 不会被当作逐跳字段删除；响应 Server 标记为 `FastGithub`。
- 当前 HTTP/1 server 显式关闭 keep-alive：每条客户端连接只服务一个请求后关闭。不是持久连接实现。

### DomainConfig 处理

- 按原始请求 Host 匹配域名规则；Response 优先于 Destination。
- Response 支持配置状态码、Content-Type 和可选 UTF-8 正文；HEAD 不发送正文，204/205/304 按无正文响应处理。
- Destination 先以 `{scheme}://{original-host}/` 为基准解析 URI reference，再作为上游 destination prefix；原始请求 path/query 会追加到该前缀。
- 相对 Destination、绝对 Destination 和显式原始端口均有单元测试。
- 原始 Host 匹配出的 DomainConfig 会继续用于上游 IPAddress、TLS SNI、证书名称不匹配策略和请求超时，即使 Destination 改写了上游主机名。
- IPAddress 是优先候选，目标域名解析结果仍作为后备候选。
- 对已匹配的 DomainConfig，TlsSni 遵循原版非 nullable bool 的默认值：缺省/false 均不发送 SNI，只有显式 true 才发送；未匹配配置的普通代理请求保留 TLS 库的正常 SNI 行为。启用时默认 SNI 使用最终上游 URI 主机名，自定义 SNI 只改变 ClientHello 的 SNI，不改变证书名称验证目标。证书名称验证目标仍是最终上游 URI 主机名。

### 超时

- TCP connect 和上游 TLS handshake 使用固定连接超时。
- DomainConfig.Timeout 按单个 HTTP 请求处理：DNS、连接、TLS 握手、等待上游响应头受同一个请求超时预算约束；响应体流式传输使用剩余预算，而不是从响应头到达后重新计时。
- 响应体超时会以 body error 终止流；此时如果响应头或部分响应体已经发送给客户端，无法再改写成新的 HTTP 状态码。
- 上游 Hyper connection driver 由响应体持有取消守卫：请求发送阶段失败/超时、响应体正常结束、发生超时/上游 body error 或下游丢弃响应体时都会 abort driver，避免遗留后台连接任务；正常读取响应体时 driver 保持运行。

## 与原版仍有差异 / 待完成

1. **持久 HTTP/1 keep-alive**：目前 `keep_alive(false)`，每连接单请求。需要先验证 Hyper 的连接复用和请求级 timeout 取消语义，再启用。
2. **Upgrade / WebSocket**：当前移除 Upgrade 等 hop-by-hop headers，不提供 WebSocket 升级隧道。
3. **HTTP/2**：当前依赖和代理实现以 HTTP/1 为主，尚未移植原版可能依赖的 HTTP/2 上游/下游协商行为。
4. **代理兼容性与边界测试**：继续补充 Expect: 100-continue、超大/分块请求体、Trailer、客户端提前断开、上游提前断开、超时发生在请求体上传期间等集成测试。
5. **代理标记**：已恢复原版 `FastGithub/1.0` User-Agent 标记：上游请求保留原 User-Agent 并追加标记；若入站请求已经包含该产品标记，则拒绝转发以防止 Destination 造成代理环路。响应 Server 头也会写入 `FastGithub`。
6. **完整互操作测试**：目前有模块单元测试，但本分支近期修改尚未取得可确认的 cargo fmt / cargo test / cargo build 结果。不能把源码测试数量当成实际通过的构建结果。

## 实现约束

- 不得删除普通 HTTP 与 TLS 解密后共用的 `http_server::serve` 路径。
- 不得移除 `domains` 配置规则列表在 Listener、TLS intercept 和 HTTP server 之间的传递。
- 不得把 HTTP/1 的 Hyper server/client、`send_request` 或 body streaming 替换回原始字节拷贝。
- Destination 改写目标主机时，必须区分 TCP 连接 IP、TLS SNI、证书验证主机名和 HTTP Host。
- DomainConfig 仍按原始请求主机匹配；Destination 只改变上游 URI。
- 每次修改后检查现有测试和调用链，先做最小补丁，再执行格式化、单元测试和构建验证。

## 历史说明

早期迁移阶段曾只有 Response 短路分支，普通 HTTP 未匹配请求返回 501，CONNECT TLS 仅作字节隧道。该状态已经过时，不应作为当前分支功能描述。
