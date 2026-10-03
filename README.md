# Oria

An Express.js-style web framework for Swift, built on [SwiftNIO](https://github.com/apple/swift-nio).
If you know Express, you already know Oria. It runs on every CPU core from a single process.

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

**Benchmarks** (same 4-vCPU Linux VM, `wrk -t2` running on the same machine, release build, no logging):

| Endpoint | Express 4 (1 process) | Oria (1 process) |
|---|---|---|
| `GET /` plaintext, 256 conns | 10.3k req/s, p99 923 ms | **74.3k req/s, p99 8.5 ms** |
| `GET /json`, 256 conns | 10.0k req/s, p99 354 ms | **68.1k req/s, p99 9.5 ms** |
| `GET /api/users/:id` (router + rate limiter + actor), 256 conns | - | **60.0k req/s, p99 10.1 ms** |
| `GET /` plaintext, 1000 conns, 30 s | - | **75.5k req/s, p99 31 ms**, 0 timeouts |

Memory: ~40 MB RSS under load. Reproduce with `scripts/bench.sh`. Treat these as relative numbers,
since the load generator competes with the server for CPU.

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

### Built-in middleware

| Middleware | Express equivalent |
|---|---|
| `logger()` | `morgan('dev')` |
| `cors(CORSOptions(...))` | `cors()`. Handles preflight requests. |
| `serveStatic("./public", StaticOptions(...))` | `express.static()`. Non-blocking, streamed, ETag/304 support, blocks path traversal and dotfiles. |
| `rateLimit(max:window:key:)` | `express-rate-limit`. Sharded fixed window, sets `RateLimit-*` and `Retry-After`. |

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
let app = Oria(configuration: config)
```

How one process scales:

- **All cores, one process.** SwiftNIO runs one event loop per core, and connections are spread across them.
- **Lock-free hot path.** Routes are compiled once into an immutable trie that every thread shares.
- **Backpressure.** Each connection is a structured-concurrency task over `NIOAsyncChannel`, so a slow
  client can't make the server buffer unbounded data. Large accept buffers keep new connections
  flowing under load.
- **Graceful shutdown.** On SIGINT/SIGTERM the server stops accepting, closes idle keep-alive
  connections immediately, lets in-flight requests finish (responding with `Connection: close`), and
  force-closes whatever is left after `shutdownGracePeriod`. Connections caught mid-accept during
  shutdown are closed, not leaked. This is covered by a stress test.
- **Optional: handlers on the I/O threads.** Call `Oria.runConcurrencyOnEventLoops()` as the first line
  of `main.swift` to make Swift Concurrency run on the event loops. This removes a thread hop per
  read and write. In our tests it lowered tail latency for routes that await actors at very high
  connection counts, but reduced throughput on the same routes. Benchmark your own workload
  before enabling it.

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
swift test                                   # 39 unit + end-to-end tests
swift run -c release oria-example            # example app on :3000
scripts/bench.sh 256 10s                     # load test (needs wrk)
```

Example app environment variables: `PORT`, `THREADS`, `LOG=1`, `COMPRESSION=1`, `REUSE_PORT=1`,
`STATIC_DIR=./public`, `RATE_LIMIT`, `EVENT_LOOP_EXECUTOR=1`.

## Not implemented yet

HTTP/2, TLS (terminate at a reverse proxy or load balancer for now), WebSockets, multipart
uploads, range requests, view templates.
