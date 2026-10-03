# Oria

An Express.js-style web framework for Swift, built on [SwiftNIO](https://github.com/apple/swift-nio).
If you know Express, you already know Oria. It runs on every CPU core from a single process and
speaks HTTP/1.1, HTTP/2 and WebSockets, over plain TCP or TLS.

```swift
import Oria

let app = Oria()

app.use(logger())
app.use(cors())

app.get("/") { req, res in
    res.send("Hello World!")
}

app.get("/users/:id") { req, res in
    try res.json(["id": req.params["id"]])
}

app.ws("/chat") { req, ws in
    for await case .text(let text) in ws.messages {
        try await ws.send("echo: \(text)")
    }
}

try await app.listen(3000)
```

## Why

| | Express (Node) | Oria |
|---|---|---|
| Cores used per process | 1 (needs `cluster`/PM2 for more) | all of them (one event loop per core) |
| Route lookup | linear scan of every layer | compiled radix trie, O(path depth) |
| Handlers | callbacks | `async`/`await`, `throws` |
| Request bodies | needs `express.json()` | `try req.json(MyType.self)`, typed `Codable` |
| Backpressure | manual | end-to-end via `NIOAsyncChannel` |
| HTTP/2, TLS | separate modules (`http2`, `https`, `spdy`) | built in: `config.tls = ...` gives HTTPS + HTTP/2 via ALPN |
| WebSockets | `ws` / `express-ws` packages | built in: `app.ws(...)`, rooms with non-blocking broadcast |

**Benchmarks** (one 4-vCPU Linux VM, release build, no logging; the load generator runs on the same
machine and competes for CPU, so treat these as relative numbers):

| Workload | Tool | Express 4 | Oria |
|---|---|---|---|
| HTTP/1.1 `GET /` plaintext, 256 conns | wrk | 10.3k req/s, p99 923 ms | **70.3k req/s**, p99 9.7 ms |
| HTTP/1.1 `GET /json`, 256 conns | wrk | 10.0k req/s, p99 354 ms | **64.2k req/s**, p99 10.3 ms |
| HTTPS (HTTP/1.1 over TLS) `GET /json`, 256 conns | wrk | - | **58.5k req/s**, p99 11 ms |
| HTTP/2 over TLS `GET /json`, 64 conns × 32 streams | h2load | - | **54.3k req/s**, 0 failed |
| HTTP/2 router + rate limiter + actor, 64 × 32 | h2load | - | **42.2k req/s** |
| HTTP/2 `POST` JSON body, 16 × 100 | h2load | - | **40.8k req/s** |
| WebSocket echo round trips, 256 conns (32 B) | oria-bench | - | **67.4k msg/s**, p99 8.7 ms |
| WebSocket echo round trips, 1000 conns | oria-bench | - | **65.2k msg/s**, p99 33 ms |
| WebSocket echo, 100 conns × 16 KB | oria-bench | - | **30.5k msg/s** (≈500 MB/s each way) |
| WebSocket broadcast, 1 sender → 1000 receivers | oria-bench | - | **75k deliveries/s**, p99 80 ms |
| Idle WebSockets held open | oria-bench | - | **10,000** at ~25 KB each; HTTP still 59k req/s alongside |

Reproduce with `scripts/bench.sh` (HTTP/1.1), `h2load` (HTTP/2) and `oria-bench` (WebSockets; see
below). Broadcast capacity on this machine tops out around 130k deliveries/s; offering more than
that grows latency (queued, not dropped).

## Install

```swift
// Package.swift
dependencies: [
    .package(url: "https://github.com/bxbdhdhvd/Oria.git", branch: "main"),
],
targets: [
    .executableTarget(name: "MyApp", dependencies: [.product(name: "Oria", package: "Oria")]),
]
```

Requires Swift 6.0+ (tested with 6.2.1 on Ubuntu 24.04). macOS 14+ or Linux.

## Guide

### Routing

```swift
app.get("/users", handler: listUsers)
app.post("/users") { req, res in ... }
app.put("/users/:id") { req, res in ... }
app.patch(...); app.delete(...); app.head(...); app.options(...)
app.all("/anything") { req, res in ... }          // every method
app.get("/files/*") { req, res in                  // wildcard: req.params["*"] == "a/b/c.txt"
    res.send(req.params["*"]!)
}
```

Static segments win over `:params`, which win over `*`. `HEAD` falls back to the `GET` route
automatically. Trailing slashes are ignored.

### Middleware

```swift
// Global middleware, like app.use(fn) in Express:
app.use { req, res, next in
    let start = ContinuousClock.now
    try await next()
    res.set("x-response-time", "\(ContinuousClock.now - start)")
}

// Path-scoped:
app.use("/admin") { req, res, next in ... }

// Per-route (up to 3 inline, or pass `middleware: [...]`):
let requireAuth: Middleware = { req, res, next in
    guard req.header("authorization") == "Bearer secret" else {
        return res.status(.unauthorized).json(raw: #"{"error":"unauthorized"}"#)
    }
    try await next()
}
app.get("/admin/stats", requireAuth) { req, res in ... }

// Share data between middleware (Express' res.locals):
req.locals["user"] = user
```

Execution order: every `use` middleware whose path matches the request runs in registration
order, then the matched route's own middleware, then its handler (or the 404 handler). If a
middleware doesn't call `next()`, the chain stops there. **This differs from Express**, where
middleware registered *after* a route only runs if the route calls `next()`.

### Routers (`express.Router()`)

```swift
let api = Router()
api.use(rateLimit(max: 100, window: .seconds(60)))   // only applies under /api
api.get("/users/:id") { req, res in ... }

app.use("/api", api)    // -> GET /api/users/:id
```

Routers nest. Inside a mounted middleware, `req.subpath` is the path relative to the mount point.

### Request

```swift
req.method            // HTTPMethod (.GET, .POST, ...)
req.path              // "/users/42"
req.url               // "/users/42?x=1"
req.params["id"]      // route parameters (percent-decoded)
req.query["page"]     // query string; req.queryAll keeps repeated keys
req.header("x-foo")   // or req.get("x-foo")
req.cookies["sid"]
req.ip                // honors X-Forwarded-For when configuration.trustProxy = true
req.text              // body as a String
req.bytes             // body as [UInt8]
req.body              // body as a ByteBuffer
try req.json(User.self)   // decode JSON, throws 400 on bad input
req.form["field"]     // application/x-www-form-urlencoded
req.is("json")        // content-type check
```

### Response

```swift
res.send("text")                     // text/html by default, like Express
try res.json(user)                   // any Encodable
res.status(.created)                 // or res.status(201), chainable
res.set("x-header", "v").type("json")
res.sendStatus(404)
res.redirect("/login")
res.cookie("sid", token, .init(maxAge: 3600, secure: true))
res.clearCookie("sid")
res.end()

// Streaming (chunked) responses, e.g. server-sent events:
res.type("text/event-stream")
res.stream { writer in
    for await event in events {
        try await writer.write("data: \(event)\n\n")
    }
}
```

### Errors

Throw from any handler or middleware:

```swift
throw HTTPError(.notFound, "User not found")    // -> 404 {"error":"User not found"}
throw HTTPError(418)
throw SomeOtherError()                          // -> 500 {"error":"Internal Server Error"}, logged to stderr
```

When a handler throws, the response is reset before the error handler runs, so headers from a
half-finished response don't leak. Customize either handler:

```swift
app.onError { error, req, res in ... }
app.notFound { req, res in res.status(404).send("Nope") }
```

### WebSockets

```swift
app.ws("/echo") { req, ws in
    for await message in ws.messages {          // ends when the socket closes
        switch message {
        case .text(let text):   try await ws.send(text)
        case .binary(let data): try await ws.send(data)
        }
    }
}                                               // returning closes the socket (1000)

// Route params, query, cookies, headers: `req` is the upgrade request.
app.ws("/rooms/:room", requireAuth) { req, ws in ... }   // middleware runs *before* the upgrade

try await ws.send(json: event)                 // any Encodable as a text frame
await ws.close(code: .policyViolation, reason: "bye")
```

- **Middleware gates the upgrade.** Global and route middleware run before the `101 Switching
  Protocols` response. If one responds (e.g. 401) instead of calling `next()`, the client gets that
  response and no socket opens. Headers middleware sets (cookies, for example) go out with the `101`.
- **Plain requests** to a WebSocket path get `426 Upgrade Required`.
- **Lifecycle:** the server answers pings, pings idle clients (`pingInterval`, default 30 s) and drops
  peers silent for two intervals. Throwing from the handler closes with 1011. On shutdown clients get
  1001 ("going away").

**Broadcasting / rooms.** `ws.send` waits for the socket (backpressure), so don't `await` many sockets
in a loop: one slow phone would stall everyone. Use the non-blocking `WebSocketHub` (or
`ws.enqueue(_:)`):

```swift
let hub = WebSocketHub()

app.ws("/chat/:room") { req, ws in
    let room = req.params["room"]!
    hub.join(room, ws)
    defer { hub.leave(room, ws) }
    for await case .text(let text) in ws.messages {
        hub.broadcast(text, to: room)           // encodes once, queues on every member, returns at once
    }
}
```

Each socket gets a bounded outbox (`outboxLimit`, default 1024 messages). A client that falls that far
behind is disconnected instead of buffering without limit.

`WebSocketOptions` per route: `maxFrameSize`, `maxMessageSize` (both 1 MiB), `allowedOrigins`,
`protocols` (subprotocol negotiation), `pingInterval`, `closeTimeout`, `messageBuffer`, `outboxLimit`.

> **Set `allowedOrigins` for browser apps that rely on cookies.** Browsers don't apply CORS to
> WebSockets, so without it any website can open a socket with your user's cookies (cross-site
> WebSocket hijacking).

### HTTPS and HTTP/2

```swift
var config = Oria.Configuration()
config.tls = try .files(certificateChain: "fullchain.pem", privateKey: "privkey.pem")
// or: try .pem(certificateChain: certString, privateKey: keyString)
// or: TLSOptions(configuration: myNIOSSLConfiguration)   // mTLS, custom ciphers, ...
let app = Oria(configuration: config)
try await app.listen(443)
```

With TLS on, clients negotiate HTTP/2 or HTTP/1.1 via ALPN. Your routes and middleware don't change:
every HTTP/2 stream becomes the same `Request`/`Response`. `wss://` works the same as `ws://`
(browsers open WebSockets over HTTP/1.1). Only TLS 1.2 and 1.3 are accepted. `req.isSecure` is true
on TLS connections (or behind a trusted proxy sending `X-Forwarded-Proto: https`). Set
`config.http2 = false` to offer only HTTP/1.1, and `config.http2MaxConcurrentStreams` (default 100)
to cap streams per connection.

### Built-in middleware

| Middleware | Express equivalent |
|---|---|
| `logger()` | `morgan('dev')` |
| `cors(CORSOptions(...))` | `cors()`. Handles preflight requests. |
| `serveStatic("./public", StaticOptions(...))` | `express.static()`. Non-blocking, streamed, ETag/304 support, blocks path traversal and dotfiles. |
| `rateLimit(max:window:key:)` | `express-rate-limit`. Sharded fixed window, sets `RateLimit-*` and `Retry-After`. |
| `securityHeaders(SecurityHeadersOptions())` | `helmet()`. `nosniff`, `X-Frame-Options`, CSP, `Referrer-Policy`, COOP, and HSTS on HTTPS. |

Body parsing needs no middleware: use `req.json`, `req.form` or `req.text`.

## Scaling and production configuration

```swift
var config = Oria.Configuration()
config.threads = System.coreCount          // event loops (default: one per core)
config.maxBodySize = 1 << 20               // 413 above this, including chunked uploads
config.maxConnections = 50_000             // 503 + close above this
config.idleTimeout = .seconds(60)          // drops idle/slowloris connections (also slow handlers & silent streams)
config.backlog = 4096                      // listen backlog
config.compression = true                  // gzip/deflate when the client accepts it (skipped for SSE)
config.reusePort = true                    // SO_REUSEPORT: run N processes on one port
config.trustProxy = true                   // behind nginx / a load balancer
config.shutdownGracePeriod = .seconds(10)
config.requestReadTimeout = .seconds(30)   // full request must arrive in time (slowloris / slow bodies)
config.maxHeaderSize = 16 * 1024           // request line + headers (431/400 above)
config.maxHeaderCount = 200
config.tls = try .files(certificateChain: "cert.pem", privateKey: "key.pem")
config.http2 = true                        // with TLS: HTTP/2 via ALPN
let app = Oria(configuration: config)
```

How one process scales:

- **All cores, one process.** SwiftNIO runs one event loop per core, and connections are spread across them.
- **Lock-free hot path.** Routes are compiled once into an immutable trie that every thread shares.
- **Backpressure.** Each connection is a structured-concurrency task over `NIOAsyncChannel`, so a slow
  client can't make the server buffer unbounded data. Large accept buffers keep new connections
  flowing under load.
- **Cheap HTTP/2 streams.** Each stream is collected on its event loop and dispatched to one task, with
  no per-stream async-channel machinery (2.5× faster than wrapping every stream).
- **Graceful shutdown.** On SIGINT/SIGTERM the server stops accepting and closes idle keep-alive
  connections at once. In-flight HTTP/1.1 requests and HTTP/2 streams finish (HTTP/1.1 responses carry
  `Connection: close`), WebSockets get a 1001 close frame, and anything left is force-closed after
  `shutdownGracePeriod`. Responses are fully written before a socket closes. Connections caught
  mid-accept or mid-handshake are closed, not leaked or left hanging. Covered by stress tests, plus a
  SIGTERM-under-mixed-load chaos run (10/10 clean exits).
- **Memory.** Live heap returns to baseline after load: 100k WebSockets opened and closed left ~13 MB
  in use. glibc keeps freed memory in per-thread arenas, so RSS can sit above that. If that matters,
  run with `MALLOC_ARENA_MAX=2` or a different allocator.
- **Optional: handlers on the I/O threads.** Call `Oria.runConcurrencyOnEventLoops()` as the first line
  of `main.swift` to make Swift Concurrency run on the event loops. This removes a thread hop per
  read and write. In our tests it lowered tail latency for routes that await actors at very high
  connection counts, but reduced throughput on the same routes. Benchmark your own workload
  before enabling it.

## Security

These defenses are on by default and covered by tests that attack a real socket:

| Attack | Behavior |
|---|---|
| Request smuggling (CL + TE, duplicate/conflicting `Content-Length`, obfuscated `Transfer-Encoding`, bad chunk sizes) | `400` and the connection closes; the smuggled request never reaches a handler |
| Header, URL and header-count bombs | `431`/`400` (`maxHeaderSize`, `maxHeaderCount`) |
| Huge or endless bodies, including chunked | `413` (`maxBodySize`) |
| Slowloris (dribbled headers) and slow-body uploads | closed after `requestReadTimeout`; truncated bodies never reach handlers |
| Garbage / non-HTTP input, unknown methods, bad versions | `400` and an immediate close |
| Response splitting (CR/LF in a header value, e.g. `res.redirect(userInput)`) | refused, `500` instead |
| Parameter flooding | only the first 1000 query/form parameters are parsed |
| Deeply nested JSON | `400` (decoder depth limit), no crash |
| `Upgrade: h2c` or other unknown upgrades | served as normal HTTP (NIO would otherwise drop the request) |
| HTTP/2 rapid reset (CVE-2023-44487), HPACK header bombs | `GOAWAY` and the connection closes; other clients unaffected |
| TLS 1.0 / 1.1 | handshake refused |
| WebSocket: unmasked frames / invalid UTF-8 / oversized frames or messages | close `1002` / `1007` / `1009` |
| Cross-site WebSocket hijacking | `403` when `allowedOrigins` is set |
| Path traversal in `serveStatic` | `403`; dotfiles hidden |
| Slow WebSocket consumers in a broadcast | disconnected after `outboxLimit`; never stall others |

Not handled for you: authentication, CSRF tokens for cookie-authenticated forms, and per-user rate
limits beyond `rateLimit`'s IP key. Put Oria behind a load balancer if you need DDoS absorption.

## Testing your app

No sockets needed:

```swift
import Testing
@testable import MyApp

@Test func getUser() async throws {
    let app = makeApp()
    let res = try await app.test(.GET, "/users/1")
    #expect(res.status == .ok)
    #expect(try res.json(User.self).name == "Ada")
}
```

For end-to-end tests, `let server = try await app.start(port: 0)` binds a free port.
Read it from `server.port` and call `await server.shutdown()` when done.

## Developing Oria

```bash
swift build
swift test                                   # 86 tests: routing, HTTP/1, HTTP/2, TLS, WebSockets, attacks
swift run -c release oria-example            # example app on :3000
scripts/bench.sh 256 10s                     # HTTP/1.1 load test (needs wrk)

# WebSocket load generator
swift run -c release oria-bench echo      ws://127.0.0.1:3000/ws/echo 256 10        # round trips
swift run -c release oria-bench broadcast ws://127.0.0.1:3000/ws/chat/r 1000 200    # fan-out
swift run -c release oria-bench idle      ws://127.0.0.1:3000/ws/echo 10000 30      # capacity

# HTTP/2 (TLS_CERT/TLS_KEY enable HTTPS in the example)
h2load -D 10 -c 64 -m 32 https://127.0.0.1:3443/json
```

Example app environment variables: `PORT`, `THREADS`, `LOG=1`, `COMPRESSION=1`, `REUSE_PORT=1`,
`STATIC_DIR=./public`, `RATE_LIMIT`, `EVENT_LOOP_EXECUTOR=1`, `TLS_CERT` + `TLS_KEY`. Its routes include
`/ws/echo` and `/ws/chat/:room`.

## Not implemented yet

Cleartext HTTP/2 (h2c; browsers only use HTTP/2 over TLS anyway), WebSockets over HTTP/2 (RFC 8441;
browsers fall back to HTTP/1.1), WebSocket compression (permessage-deflate), HTTP/3, multipart
uploads, range requests, view templates.
