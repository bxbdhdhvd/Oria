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
| Backpressure | manual | end-to-end: socket reads pause while a handler or upload consumer is busy |
| Uploads, ranges | `multer`, `send` packages | built in: `app.upload(...)` streams to disk; `res.sendFile` does Range/ETag/304 |
| HTTP/2, TLS | separate modules (`http2`, `https`, `spdy`) | built in: `config.tls = ...` gives HTTPS + HTTP/2 via ALPN |
| WebSockets | `ws` / `express-ws` packages | built in: `app.ws(...)`, rooms with non-blocking broadcast |

## Performance

Measured in a container limited to **2 CPU cores and 256 MB of RAM** (`--cpuset-cpus=0,1
--memory=256m`), with the load generator pinned to the other 2 cores of the same 4-vCPU cloud VM.
Release build, no logging. Reproduce everything below with `scripts/container-bench.sh`.

| Workload | Result |
|---|---|
| HTTP/1.1 `GET /` plaintext, pipelined ×16, 64 conns | **100k req/s** |
| HTTP/1.1 `GET /json`, pipelined ×16, 64 conns | **65k req/s** |
| HTTP/1.1 `GET /` keep-alive (one request at a time), 64 conns | **32.6k req/s**, p50 1.8 ms, p99 4.3 ms |
| HTTP/1.1 `GET /json`, 1 / 2 / 4 conns | 8.7k / 16.1k / 25.7k req/s, **p50 104–132 µs, p99 329 / 570 / 890 µs** |
| Router + rate limiter + `actor` call, keep-alive, 64 conns | 22.2k req/s, p99 5.6 ms |
| HTTP/2 over TLS, 64 conns × 16 streams (h2load) | 25.2k req/s, 0 failed |
| WebSocket echo round trips, 256 conns, 32 B | 31.7k msg/s, p99 17.6 ms |
| 1 GiB raw upload (`PUT`, streamed to disk) | 3.6 s, **server peak RSS 29 MB** |
| 1 GiB multipart upload (`curl -F`) | 4.6 s |
| 32 parallel 32 MiB uploads | 3.5 s, all 200, peak RSS 66 MB |
| 1 GiB file download / 1 MB range from the middle | 2.3 s, byte-identical / `206`, byte-identical |
| 4 GiB generated streaming response | 1.8 s (≈2.3 GB/s) |
| Idle memory | ~26 MB RSS |

**How to read these numbers.**

- The server-side work per request is small. On this VM a request costs about 54 µs of CPU, of
  which the kernel's loopback TCP path (`writev`/`read`/`epoll`) is the largest part. Without
  pipelining, the kernel is the limit: a bare SwiftNIO "hello world" on the same 2 cores does 46k
  req/s. Oria spends about 20% more CPU per request than bare NIO, for routing, middleware,
  `async` handlers and Express-style request/response objects.
- **≥100k req/s on 2 cores** is reached with pipelining (which is how TechEmpower measures
  plaintext) or with more cores. Throughput scales with cores, because each event loop is
  independent and the hot path takes no locks. On a dedicated 4–8 core machine, with the load
  generator elsewhere, expect roughly 2–4× the keep-alive numbers above.
- **Sub-millisecond p99** holds until the CPU saturates: p99 is 0.89 ms at 25.7k req/s on 2 cores.
  Once the CPU is fully busy (64 connections hammering 2 cores), requests queue and p99 is a few
  milliseconds, as with any server. Size for about 60–70% CPU if you need sub-ms
  tails. These are shared, noisy cloud vCPUs, so dedicated hardware tails will be tighter.

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

Requires Swift 6.0+ (tested with 6.2.1 on Ubuntu 24.04; Swift 6.2 enables `Task.immediate` and
`Span`-based parsing, older compilers fall back automatically). macOS 15+ or Linux.

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

### File uploads (`multipart/form-data` and raw bodies)

Upload routes stream the body to disk as it arrives. Memory use stays flat whatever the file size:
a 1 GiB upload peaks at about 29 MB RSS for the whole server. Socket reads pause whenever the
disk falls behind (backpressure), so a fast client can't make the server buffer data.

```swift
app.upload("/avatar", options: UploadOptions(
    maxFileSize: 5 << 20,                  // 413 above this
    maxFiles: 1,
    allowedFileTypes: ["image/png", "image/jpeg"],   // 415 otherwise
    directory: "/var/app/uploads"          // default: the system temp directory
)) { req, res in
    let form = req.uploads!                // MultipartForm
    let name = form.field("displayName")   // text fields (kept in memory, maxFieldSize each)
    guard let file = form.file("avatar") else { throw HTTPError(.badRequest) }
    file.filename        // sanitized: "../../x.png" -> "x.png"
    file.contentType     // as sent by the client: verify it if it matters
    file.size
    try await file.move(to: "/var/app/avatars/\(UUID()).png")   // keep it
    res.status(.created).send("ok")
}                                          // anything not moved is deleted after the response

// Raw body (no multipart): PUT /backups/db.tar
app.upload("/backups/:name", method: .PUT, options: UploadOptions(maxBodySize: 10 << 30)) { req, res in
    let body = req.uploadedBody!           // UploadedFile on disk
    try await body.move(to: "/srv/backups/" + safeName(req.params["name"]!))
    res.sendStatus(.created)
}
```

- **Limits** (`UploadOptions`): `maxBodySize` and `maxFileSize` (1 GiB default), `maxFiles` (20),
  `maxFields` (200), `maxFieldSize` (1 MiB), `maxPartHeaderSize` (16 KiB), `allowedFileTypes`, and
  `idleTimeout` (30 s without body bytes closes the upload; it replaces `requestReadTimeout` so long
  uploads aren't cut off). A declared `Content-Length` above the limit gets `413` before any byte is
  read. `Expect: 100-continue` is supported, so `curl` waits for the go-ahead.
- **Temporary files** are created with mode `0600` and are *transactional*: on Linux they have no
  name (`O_TMPFILE`) until complete, so an aborted upload never leaves a partial file behind. Files
  you don't `move` are deleted after the response.
- **Small forms on normal routes:** `try req.multipart()` parses an in-memory multipart body (bounded
  by `maxBodySize`) with the same limits and errors. Prefer `app.upload` for files.
- Works over HTTP/1.1 (`Content-Length` or chunked) and HTTP/2.

### Files, downloads and Range requests

```swift
app.get("/media/:id") { req, res in
    try await res.sendFile("/srv/media/\(try lookup(req.params["id"]!))", for: req)
}
app.get("/report") { req, res in
    try await res.download("/srv/reports/q3.pdf", filename: "Q3 résumé.pdf", for: req)
}
app.use("/assets", serveStatic("./public", StaticOptions(maxAge: 86400)))   // same engine
```

`sendFile` streams with non-blocking file I/O (256 KiB reads, about 1.2 GB/s for a single download on
the test VM) and implements HTTP caching and ranges in full:

- `Range: bytes=0-99`, open-ended `bytes=500-` and suffix `bytes=-500` ranges give `206` with
  `Content-Range`. Several ranges give `multipart/byteranges`. Video seeking and `curl -C -` resume
  work.
- Overlapping ranges are merged. More than `maxRanges` (16) ranges get the whole file instead, so a
  client can't make the server send the same bytes many times. Unsatisfiable ranges get `416` with
  `Content-Range: bytes */size`.
- `ETag` (strong, from size and mtime with nanoseconds) and `Last-Modified`.
- `If-None-Match` / `If-Modified-Since` give `304`; `If-Match` gives `412`. `If-Range` falls back to
  the full file if it changed.
- `HEAD` returns the headers without a body.
- `download` adds `Content-Disposition: attachment` with an ASCII fallback plus an RFC 5987 UTF-8
  filename.
- `path` is used as given: never build it from unvalidated input (use `serveStatic`, which blocks
  traversal and dotfiles, or validate names yourself).

Generated large responses use `res.stream(length:)`: each `writer.write` waits until the socket can
take more, so a 4 GiB response uses no more memory than one chunk.

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

> **Cross-site WebSocket hijacking is blocked by default.** Browsers don't apply CORS to WebSockets,
> so by default Oria only accepts browser handshakes whose `Origin` matches the `Host` (same
> origin). Clients that send no `Origin` (native apps, servers) are unaffected. If your front end is
> served from another origin, list it: `allowedOrigins: ["https://app.example.com"]`. `["*"]`
> allows any origin.

Malformed handshakes get proper answers: wrong `Sec-WebSocket-Version` gives `426` with
`Sec-WebSocket-Version: 13`, and a missing or invalid key, or a body on the upgrade request, gives `400`.

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
(browsers open WebSockets over HTTP/1.1). Only TLS 1.2 (ECDHE + AEAD suites only, so always
forward secret) and TLS 1.3 are accepted. `req.isSecure` is true
on TLS connections (or behind a trusted proxy sending `X-Forwarded-Proto: https`). Set
`config.http2 = false` to offer only HTTP/1.1, and `config.http2MaxConcurrentStreams` (default 100)
to cap streams per connection.

### Built-in middleware

| Middleware | Express equivalent |
|---|---|
| `logger()` | `morgan('dev')` |
| `cors(CORSOptions(...))` | `cors()`. Handles preflight requests. |
| `serveStatic("./public", StaticOptions(...))` | `express.static()`. Non-blocking, streamed, ETag/304 support, blocks path traversal, dotfiles and symlinks that leave the root. |
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
config.runHandlersOnEventLoops = true      // handlers run on the connection's event loop (no thread hops)
let app = Oria(configuration: config)
```

`runHandlersOnEventLoops` (default `true`) runs request handlers on the connection's own event
loop; see the architecture section below.

### Architecture: how one process scales

- **All cores, one process.** SwiftNIO runs one event loop per core (`config.threads`), and
  connections are spread across them. Routes compile once into an immutable radix trie that every
  thread shares, so lookup takes no locks.
- **One handler per connection, no hops.** Each HTTP/1.1 connection (or HTTP/2 stream) has a single
  NIO `ChannelDuplexHandler` that runs your `async` handler and writes the response. Parsing is
  NIO's llhttp-based decoder, the battle-tested part.
  - On plain HTTP/1.1 this handler also takes over from NIO's pipelining handler and response
    encoder once the connection has made its upgrade decision. It queues pipelined requests itself
    (reads pause meanwhile, at most 4096 queued parts), and `HTTP1Writer` serializes the status
    line, headers and small bodies into one buffer, validating headers in the same pass. A typical
    response is one allocation and one `writev` entry. Streamed responses must match their
    declared `Content-Length`; a short body closes the connection instead of leaving the client
    waiting.
  - Routing happens once per request, when the head arrives. Routes are reference types, and each
    route's full middleware chain is precomputed when no middleware is path-scoped.
  - Handlers run on a custom Swift Concurrency `TaskExecutor` backed by that connection's event loop
    (`Task(executorPreference:)`).
  - On Swift 6.2, `Task.immediate` starts the handler synchronously inside the read callback. A
    handler that doesn't suspend writes its response in the same event-loop tick that read the
    request, as hand-written NIO code would.
  - When a handler does suspend (a database call, an actor), it resumes on the same event loop, so
    the response is written without another thread hop. This executor preference made routes that
    await an actor 28% cheaper in our measurements, and cost nothing measurable elsewhere.
  - Set `config.runHandlersOnEventLoops = false` to use the global concurrent executor instead. That
    is better if your handlers do heavy CPU work, which would otherwise delay other connections on
    the same loop.
- **Flush coalescing.** Responses written during a read cycle are flushed once at the end of it, so
  pipelined requests share one `writev`.
- **Fast JSON.** `res.json` encodes with `FastJSON`, a streaming `Encoder` that writes straight into
  the response buffer instead of building Foundation's intermediate tree. The output is the same
  JSON as `JSONEncoder`, checked by differential and randomized tests. A JSON endpoint costs 11–20% less CPU
  per request (keep-alive / pipelined). Pass `encoder:` to use a configured `JSONEncoder` instead.
- **Per-loop caches.** The `Date` header is formatted at most once per second per event loop, with
  no thread-local lookup.
- **Cheap timers.** Read deadlines, upload idle timeouts and write-stall detection share one
  scheduled check per connection. Arming a deadline is a stored-property write, not a timer
  allocation.
- **Backpressure everywhere.** Upload consumers pull body chunks through a bounded
  `NIOThrowingAsyncSequenceProducer`: the socket stops being read while the disk is busy. Streaming
  responses await the socket. A client that stops reading (slow-read attack) is cut off after
  `idleTimeout`.
- **Memory safety.** Everything is Swift 6 language mode with strict concurrency checking, so data
  races are compile errors. The framework does no manual pointer arithmetic:
  - Multipart boundary search uses bounds-checked `Span` (Swift 6.2).
  - Buffers are `ByteBuffer` with copy-on-write.
  - The few per-connection fields marked `@exclusivity(unchecked)` are confined to one event loop by
    construction.
- **Graceful shutdown.** On SIGINT/SIGTERM the server:
  - stops accepting and closes idle keep-alive connections at once;
  - lets in-flight requests finish (HTTP/1.1 responses carry `Connection: close`);
  - sends WebSockets a 1001 close frame;
  - force-closes whatever is left after `shutdownGracePeriod`.
- **Resilient accept loop.** Transient accept errors (out of file descriptors, `ENOBUFS`, a socket
  option the sandbox refuses) are logged and retried. They never stop the server.
- **Memory.** About 26 MB RSS idle and about 30 KB per idle connection. Uploads and downloads of any
  size stay flat. glibc keeps freed memory in per-thread arenas, so after a spike RSS can sit above
  the live heap. Use `MALLOC_ARENA_MAX=2` if that matters.

## Running in a container

```bash
docker build -t my-oria-app .
docker run -d -p 3000:3000 --cpus=2 --memory=256m \
  -v /srv/uploads:/data --name api my-oria-app
```

The included `Dockerfile` is multi-stage. It compiles with `swift:6.2-noble` and ships the binary on
`swift:6.2-noble-slim`, running as an unprivileged user (uid 10001) with `/data` for uploads.
`docker stop` sends SIGTERM, which triggers the graceful shutdown. Tips:

- Put uploads on a volume (`-v`), not the container's overlay filesystem: file I/O was about 2.5×
  faster in our tests.
- `--network host` (or a Kubernetes pod network) avoids docker-proxy/NAT overhead for small-request
  workloads.
- Set `THREADS` to the CPUs you give the container. Oria defaults to the host's core count, which
  can exceed a `--cpus` limit.
- `scripts/container-bench.sh` builds the image and runs the full benchmark suite above against a
  CPU- and memory-limited container (`CPUS=0,1 MEMORY=256m`).

## Security

These defenses are on by default and covered by tests that attack a real socket (121 tests in total):

| Attack | Behavior |
|---|---|
| Request smuggling (CL + TE, duplicate/conflicting `Content-Length`, obfuscated `Transfer-Encoding`, bad chunk sizes, bare LF) | `400` and the connection closes; the smuggled request never reaches a handler |
| Header, URL and header-count bombs | `431`/`400` (`maxHeaderSize`, `maxHeaderCount`) |
| Huge or endless bodies, including chunked | `413` (`maxBodySize`, per-route `UploadOptions`), refused before reading when `Content-Length` says so |
| Slowloris (dribbled headers), slow-body uploads | closed after `requestReadTimeout` / upload `idleTimeout` |
| Slow-read (client stops reading a large response) | connection reset after `idleTimeout` (abortive close, so the kernel doesn't keep the unsent bytes around either); memory stays flat meanwhile |
| Garbage / non-HTTP input, unknown methods, bad versions | `400` and an immediate close |
| Response splitting (CR/LF in a header value, e.g. `res.redirect(userInput)`) | refused, `500` instead |
| Upload path traversal (`../`, absolute, backslashes, NUL in filenames, a `/` or `.` hidden in a grapheme cluster with a combining mark or zero-width joiner, bidi overrides) | filenames sanitized at the Unicode-scalar level to a base name; temp files are random, `0600`, never visible half-written |
| Upload abuse (too many files/fields, huge part headers, disallowed types, malformed multipart) | `413` / `415` / `400`, temporary files removed |
| Range amplification (`bytes=0-,0-,0-…`, hundreds of ranges) | ranges merged; >16 ranges serve the file once |
| Parameter flooding / deeply nested JSON | first 1000 parameters only / `400` (decoder depth limit) |
| `Upgrade: h2c` or other unknown upgrades | served as normal HTTP |
| HTTP/2 rapid reset (CVE-2023-44487), CONTINUATION flood (CVE-2024-27316), SETTINGS/PING floods, HPACK bombs | `GOAWAY` and the connection closes; other clients unaffected |
| TLS 1.0 / 1.1, non-forward-secret or CBC cipher suites | handshake refused |
| WebSocket: unmasked frames / reserved (RSV) bits / invalid UTF-8 / reserved opcodes / oversized frames or messages | close `1002` / `1002` / `1007` / `1002` / `1009` |
| Cross-site WebSocket hijacking | `403` unless same-origin or in `allowedOrigins` |
| Malformed WebSocket handshakes, upgrade with a body | `426` / `400` |
| Path traversal in `serveStatic`, including encoded separators glued to combining marks | `403` (byte-level checks); dotfiles hidden |
| Symlinks in a served directory pointing outside it | not served (real path must stay under the real root; `followSymlinksOutsideRoot` to opt out) |
| `Expect: 100-continue`, parse errors and refused upgrades with compression on | answered correctly (these bypass the compressor, which would otherwise trap on a response with no matching request) |
| Valid pipelined requests followed by garbage | their responses arrive in order, then `400` and close |
| Slow WebSocket consumers in a broadcast | disconnected after `outboxLimit`; never stall others |
| File-descriptor exhaustion | accept errors are retried, the server keeps running; cap with `maxConnections` |

**Penetration test.** An independent black-box pass ran these attacks against a release build over
HTTP/1.1, HTTPS + HTTP/2 and WebSocket. It covered smuggling variants, limit bypasses, slowloris /
slow-POST / slow-read, upload traversal (verified on disk), malformed multipart, range abuse,
HTTP/2 floods (2000-stream rapid reset, 20k SETTINGS/PING frames, CONTINUATION flood, 40 KB header
lists), TLS downgrade, WebSocket protocol violations, header injection and 800 idle connections.

- Results: **no crashes, no Critical or High findings.** Peak RSS during the floods was 41 MB.
- Fixed since: the two Medium findings (WebSocket origin default, slow-read drain) and the Low ones
  (legacy TLS 1.2 cipher, handshake error answers, upgrade-with-body).
- Remaining Low/Info items:
  - An overflowing chunk size (`FFFFFFFFFFFFFFFF`) waits for `requestReadTimeout` instead of an
    immediate `400`. That is NIO parser behavior and not exploitable for smuggling.
  - `maxConnections` is unlimited by default; set it.
  - An oversized WebSocket frame can end with a TCP reset rather than a visible 1009, because the
    socket closes with unread input.

Not handled for you: authentication, CSRF tokens for cookie-authenticated forms, per-user rate
limits beyond `rateLimit`'s IP key, antivirus/content scanning of uploads, and disk quotas (cap
uploads with `UploadOptions` and monitor free space). Put Oria behind a load balancer if you need
DDoS absorption.

## Can I run a production service on it?

**Yes, for a typical API, upload or WebSocket service, with the checklist below.** Honest
assessment:

**Strengths**
- The protocol layers are SwiftNIO (Apple's networking stack, also used by Vapor, gRPC Swift and
  AWS/Apple services): HTTP/1.1 parsing, HTTP/2 framing and flood protection, TLS (BoringSSL) and
  WebSocket framing. Oria doesn't reimplement those.
- Defenses against the common HTTP attacks are on by default and tested, and an adversarial pass
  found no crash or high-severity issue.
- Predictable resources: about 26 MB idle, flat memory for any body size, and bounded queues
  everywhere (accept buffer, upload chunks, WebSocket outboxes).
- Graceful shutdown that fits containers and rolling deploys.

**Gaps and risks**
- **Young code.** The framework layer (routing, request handler, uploads, ranges) is new and has
  only this test suite and one pen test behind it. It has no production mileage. Vapor or
  Hummingbird are more battle-tested if you need that today.
- **No built-in observability**: no metrics endpoint, structured logging or tracing yet. Add a
  middleware that records latency and status into your metrics system (Prometheus via
  swift-metrics, for example) before going live.
- **API stability.** This is pre-1.0, so pin a commit.
- **Blocking code on the event loop.** With `runHandlersOnEventLoops` (the default), CPU-heavy or
  blocking work in a handler delays every connection on that loop. Move it to
  `Task.detached`/a thread pool, or turn the option off.
- No HTTP/3, h2c or WebSocket compression (see below).

**Production checklist**
1. Run 2+ instances behind a load balancer (or `reusePort` for several processes on one host) for
   zero-downtime deploys. Point health checks at a cheap route.
2. Set `config.maxConnections` (e.g. 20–50k per instance) and raise the file-descriptor limit
   (`ulimit -n`, or LimitNOFILE in systemd) above it.
3. Set `THREADS` to the cores you allocate. Keep CPU under about 70% if you need sub-ms p99.
4. Terminate TLS in Oria (`config.tls`) or at the load balancer. If at the load balancer, set
   `config.trustProxy = true` so `req.ip` and `req.isSecure` are correct.
5. Configure `UploadOptions` per upload route: size, count and type limits. Put the upload directory
   on its own volume and alert on free space.
6. Set `allowedOrigins` on WebSocket routes used by a front end on another origin, and use
   `securityHeaders()` and a strict `cors()` origin list.
7. Add authentication and per-user rate limits for your domain, plus metrics, logging and alerts.
8. Load-test your real handlers in your target container size with `scripts/container-bench.sh` as a
   template.

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
swift test                                   # 121 tests: routing, HTTP/1, HTTP/2, TLS, WebSockets, uploads, ranges, attacks
ORIA_MEMORY_TESTS=1 swift test --filter uploadMemoryStaysFlat   # RSS check, run alone
swift run -c release oria-example            # example app on :3000
scripts/bench.sh 256 10s                     # HTTP/1.1 load test (needs wrk)
scripts/container-bench.sh                   # everything, inside a 2-CPU / 256 MB container
wrk -t2 -c64 -d10s -s scripts/pipeline.lua http://127.0.0.1:3000/ -- 16   # pipelined

# WebSocket load generator
swift run -c release oria-bench echo      ws://127.0.0.1:3000/ws/echo 256 10        # round trips
swift run -c release oria-bench broadcast ws://127.0.0.1:3000/ws/chat/r 1000 200    # fan-out
swift run -c release oria-bench idle      ws://127.0.0.1:3000/ws/echo 10000 30      # capacity

# HTTP/2 (TLS_CERT/TLS_KEY enable HTTPS in the example)
h2load -D 10 -c 64 -m 32 https://127.0.0.1:3443/json
```

Example app environment variables: `PORT`, `THREADS`, `LOG=1`, `COMPRESSION=1`, `REUSE_PORT=1`,
`STATIC_DIR=./public`, `FILES_DIR`, `MAX_BODY`, `RATE_LIMIT`, `BENCH=1` (skip demo middleware),
`HANDLERS_ON_LOOP=0`, `EVENT_LOOP_EXECUTOR=1`, `TLS_CERT` + `TLS_KEY`.

Its routes include:

- `/ws/echo` and `/ws/chat/:room` (WebSockets)
- `POST /upload` (multipart)
- `PUT` / `GET /files/:name` (raw upload, then a ranged download)
- `GET /bytes/:count` (a large generated response)
- `POST /sink` (a streamed body that is discarded)

## Not implemented yet

Cleartext HTTP/2 (h2c; browsers only use HTTP/2 over TLS anyway), WebSockets over HTTP/2 (RFC 8441;
browsers fall back to HTTP/1.1), WebSocket compression (permessage-deflate), HTTP/3, `sendfile(2)`
zero-copy for plaintext file responses, built-in metrics, view templates.
