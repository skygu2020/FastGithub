# HTTP 代理语义移植记录

本记录依据原版 `creazyboyone/FastGithub` 的以下实现整理：

- `FastGithub.HttpServer/HttpMiddlewares/HttpReverseProxyMiddleware.cs`
- `FastGithub.Configuration/DomainConfig.cs`
- `FastGithub.Configuration/ResponseConfig.cs`
- `FastGithub.Http/HttpClient.cs`
- `FastGithub.Http/HttpClientHandler.cs`
- `FastGithub.Http/HttpClientFactory.cs`

## 原版请求处理顺序

1. 从 HTTP 请求的 Host 提取域名并查找 DomainConfig。
2. 未匹配配置时调用下一个中间件。
3. 匹配且 `Response` 非空时，直接返回自定义状态码、Content-Type 和可选正文；不会执行上游转发。该分支优先于 Destination。
4. 匹配且 `Response` 为空时，执行反向代理。

`ResponseConfig` 的原版默认值为 StatusCode=200、ContentType=`text/plain;charset=utf-8`、ContentValue=null。

## Destination 的真实含义

原版将默认基准 URI 构造为 `{scheme}://{host}/`。若 Destination 非空，则执行 .NET `new Uri(baseUri, destination)`，所得 URI 字符串作为 YARP Forwarder 的 destinationPrefix。

因此 Destination 是 URI 解析规则，支持相对 URI 和绝对 URI；不是简单的目标 IP 覆盖。相对路径的解析遵循 URI 基准解析语义，不能用字符串拼接代替。

## 原版请求超时和连接行为

`HttpClientHandler.SendAsync` 在每个请求上创建 Timeout 对应的 CancellationTokenSource，并将其与调用方取消令牌链接，再调用底层 SendAsync。因此 DomainConfig.Timeout 是请求发送生命周期超时，不只是 TCP connect timeout。

底层连接另有固定 10 秒 connect timeout，并逐个尝试候选 IP。候选顺序为：
- 请求主机本身是 IP 时，直接使用该 IP；
- 否则先尝试 DomainConfig.IPAddress（如果设置）；
- 再枚举域名解析器返回的地址。

原版关闭自动代理、Cookie、自动重定向和自动解压缩。转发请求会插入 `FastGithub/1.0` User-Agent 标记，防止请求再次转发到 FastGithub 自身；响应 Server 头也会尝试加入 FastGithub 标记。

## 与当前 Rust 实现的差异

截至本记录对应的 `linux-rust` HEAD：

- 入口只接受 CONNECT；普通 HTTP 方法返回 501。
- CONNECT 的 TLS 流量完成 TLS 握手后，当前直接 `copy_bidirectional`，不会解析解密后的 HTTP 请求。
- 因此当前无法在普通 HTTP 或已解密 HTTPS 请求级别实现 Response、Destination 和原版 Timeout 语义。
- 现有 per-domain Timeout 只用于上游 TCP connect；TLS 握手仍使用独立固定超时，隧道数据转发没有总超时。这是有意避免把长下载、长连接和 keep-alive 误判为超时，但不能称为原版请求超时的完整移植。
- 现有 CONNECT authority、域名解析、SNI 和证书拦截逻辑应保留，不能为了支持 HTTP 请求而退化为仅按 IP 转发。

## 后续实现约束

要忠实支持原版功能，代理需要具备 HTTP 请求层，而不只是 TCP 隧道层：

1. 普通 HTTP 代理请求应解析 absolute-form URI，保留 path/query、请求方法、请求体和必要的端到端头。
2. CONNECT 仍先建立隧道并完成客户端 TLS 握手；TLS 解密后也必须进入同一套 HTTP 请求处理路径，才能按每个请求应用 Response、Destination 和 Timeout。
3. 对每个请求，根据原始 scheme/Host 匹配 DomainConfig；Response 分支先于 Destination。
4. Destination 使用标准 URI reference resolution，以 `{scheme}://{host}/` 为基准，不得手工拼接。
5. Timeout 应覆盖单个请求的发送/响应生命周期，而不能套在整个持久 TLS 连接或 `copy_bidirectional` 上。
6. 上游连接仍应使用 FastGithub 域名解析候选和 IPAddress 优先策略；HTTPS 的上游 TLS SNI 与证书验证名称必须分开处理。
7. 需要正确处理请求/响应 framing、chunked body、keep-alive、hop-by-hop headers、Connection: close 和上游错误响应。不得通过只读一段 header 后直接转发字节来声称支持完整 HTTP。

## 当前决策

不在现有 TLS 字节隧道上叠加不完整的 Response/Destination 特判。下一阶段应引入明确的 HTTP/1.1 请求处理层，并让明文 HTTP 与 CONNECT 解密后的 HTTP 共用请求转发逻辑。这样才能避免两套规则逐渐分叉。

## Current HTTP Response implementation (2026-11)

The native listener now recognizes configured `Response` rules for plain HTTP proxy requests. It obtains the host from absolute-form `http://` request targets or the `Host` header, applies the existing ordered domain-rule matcher, and returns the configured status, content type, and optional UTF-8 body without opening an upstream connection. `HEAD` suppresses the body while retaining its representation length; 1xx, 204, and 304 responses are bodyless. The configured content type is checked for CR/LF before it is written to the response.

This is deliberately a narrow first step: unmatched plain HTTP requests still return 501, and HTTPS CONNECT traffic still uses the existing TLS byte tunnel after interception. This does not yet implement the original middleware's complete reverse proxy, `Destination` URI resolution, or per-request timeout behavior. Request-body consumption and persistent HTTP keep-alive are also not implemented on this short-circuit path; the connection is closed after the configured response.


The domain `Timeout` setting is deliberately not used as a TCP connect timeout. The original implementation applies it to the HTTP request lifecycle; until the Rust HTTP forwarding layer exists, TCP connection attempts retain the fixed 10-second connect limit and request-level timeout remains unsupported.
