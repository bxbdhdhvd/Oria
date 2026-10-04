import Atomics
import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOHTTP1
import NIOPosix

/// The application. Create one, register routes, then call `listen`.
///
/// ```swift
/// let app = Oria()
/// app.get("/users/:id") { req, res in
///     try res.json(["id": req.params["id"]])
/// }
/// try await app.listen(3000)
/// ```
public final class Oria: Router, @unchecked Sendable {
    public struct Configuration: Sendable {
        /// Event loop threads. Defaults to one per CPU core so a single process uses the whole machine.
        public var threads: Int = System.coreCount
        /// Listen backlog for pending connections.
        public var backlog: Int32 = 4096
        /// Sets SO_REUSEPORT so several processes can share one port (kernel load-balances between them).
        public var reusePort = false
        /// Requests with a larger body get `413 Payload Too Large`.
        public var maxBodySize = 1 << 20
        /// Hard cap on concurrently open connections; extra connections get `503` and are closed.
        public var maxConnections: Int? = nil
        /// Closes connections that send/receive nothing for this long (slowloris protection).
        /// This also applies mid-request: a handler that takes longer, or a stream (e.g. SSE) that
        /// stays silent longer, gets its connection closed. Send heartbeats or raise/disable it.
        public var idleTimeout: TimeAmount? = .seconds(60)
        /// Maximum time in-flight requests get to finish during a graceful shutdown.
        public var shutdownGracePeriod: TimeAmount = .seconds(10)
        /// gzip/deflate responses when the client supports it.
        public var compression = false
        /// Trust `X-Forwarded-For` when resolving `req.ip` (enable behind a reverse proxy).
        public var trustProxy = false
        /// Value of the `Server` header; nil omits it.
        public var serverName: String? = nil
        /// Install SIGINT/SIGTERM handlers that trigger a graceful shutdown in `listen`.
        public var handleSignals = true
        /// Serve HTTPS. With TLS enabled, clients negotiate HTTP/2 or HTTP/1.1 via ALPN.
        public var tls: TLSOptions? = nil
        /// Offer HTTP/2 to TLS clients (browsers only use HTTP/2 over TLS).
        public var http2 = true
        /// Maximum concurrent HTTP/2 streams per connection.
        public var http2MaxConcurrentStreams = 100
        /// Time allowed to receive a complete request (headers and body), measured from the end of
        /// the previous response or from connection open (including the TLS handshake). Defeats
        /// slowloris-style clients that dribble bytes to dodge `idleTimeout`. This also bounds how
        /// long an idle keep-alive connection stays open.
        public var requestReadTimeout: TimeAmount? = .seconds(30)
        /// Maximum combined size of the request line and headers (`431`/`400` above it).
        public var maxHeaderSize = 16 * 1024
        /// Maximum number of request headers.
        public var maxHeaderCount = 200
        /// Run request handlers on the event loop that owns the connection (a Swift `TaskExecutor`
        /// backed by NIO). A request that doesn't hop to another actor then never changes threads,
        /// which is what gives Oria its latency. Turn it off only if handlers do long CPU-bound work
        /// without awaiting: that would stall other connections on the same loop.
        public var runHandlersOnEventLoops = true

        public init() {}
    }

    public var configuration: Configuration

    private let state = NIOLockedValueBox<(router: CompiledRouter?, version: Int, server: Server?)>((nil, -1, nil))
    private let errorHandlerBox = NIOLockedValueBox<ErrorHandler?>(nil)
    private let notFoundBox = NIOLockedValueBox<Handler?>(nil)

    public init(configuration: Configuration = .init()) {
        self.configuration = configuration
        super.init()
    }

    /// Replaces the default error handler (which sends `{"error": "..."}` with the right status).
    public func onError(_ handler: @escaping ErrorHandler) {
        errorHandlerBox.withLockedValue { $0 = handler }
        state.withLockedValue { $0.router = nil }
    }

    /// Replaces the default 404 handler.
    public func notFound(_ handler: @escaping Handler) {
        notFoundBox.withLockedValue { $0 = handler }
        state.withLockedValue { $0.router = nil }
    }

    // MARK: Lifecycle

    /// Starts the server and suspends until it shuts down (SIGINT/SIGTERM or `shutdown()`).
    public func listen(
        _ port: Int = 3000,
        host: String = "0.0.0.0",
        onListening: (@Sendable (SocketAddress) -> Void)? = nil
    ) async throws {
        let server = try await start(port: port, host: host)
        var signalSources: [any DispatchSourceSignal] = []
        if configuration.handleSignals {
            for sig in [SIGINT, SIGTERM] {
                signal(sig, SIG_IGN)
                let source = DispatchSource.makeSignalSource(signal: sig, queue: .global())
                source.setEventHandler { [server] in
                    Task { await server.shutdown() }
                }
                source.resume()
                signalSources.append(source)
            }
        }
        if let address = server.localAddress {
            if let onListening {
                onListening(address)
            } else {
                let threads =
                    Oria.concurrencyRunsOnEventLoops.load(ordering: .relaxed)
                    ? NIOSingletons.groupLoopCountSuggestion : configuration.threads
                print("Oria listening on http://\(host):\(address.port ?? port) (\(threads) threads)")
                fflush(nil)
            }
        }
        defer { signalSources.forEach { $0.cancel() } }
        try await server.wait()
    }

    /// Starts the server and returns immediately. Useful for tests (`port: 0` picks a free port).
    public func start(port: Int = 3000, host: String = "0.0.0.0") async throws -> Server {
        let router = compiledRouter()
        let server = try await Server.start(app: self, router: router, host: host, port: port)
        state.withLockedValue { $0.server = server }
        return server
    }

    /// Gracefully stops a server started by `listen` or `start`.
    public func shutdown() async {
        let server = state.withLockedValue { $0.server }
        await server?.shutdown()
    }

    // MARK: Request handling

    func compiledRouter() -> CompiledRouter {
        let current = snapshot.version
        return state.withLockedValue { s in
            // Rebuild if routes were added after a previous compile (common in tests).
            if let r = s.router, s.version == current { return r }
            var builder = CompiledRouter.Builder()
            flatten(prefix: [], into: &builder)
            let r = CompiledRouter(
                builder,
                notFound: notFoundBox.withLockedValue { $0 } ?? Oria.defaultNotFound,
                errorHandler: errorHandlerBox.withLockedValue { $0 } ?? Oria.defaultErrorHandler
            )
            s.router = r
            s.version = current
            return r
        }
    }

    /// Runs a request through middleware, routing, and error handling.
    func handle(_ req: Request, _ res: Response, router: CompiledRouter, lookup: CompiledRouter.Lookup? = nil) async {
        let lookup = lookup ?? router.lookup(method: req.method, uri: req.url)
        let segments = lookup.segments
        if let route = lookup.route {
            if !route.paramNames.isEmpty { req.params = lookup.params }
            if !router.hasScopedMiddleware {
                // Common case: the full chain was precomputed for this route.
                do {
                    try await Oria.run(route.chain, 0, route.handler, req, res)
                } catch {
                    res.reset()
                    await router.errorHandler(error, req, res)
                }
                return
            }
            await runChain(req, res, router: router, segments: segments, routeMiddleware: route.middleware, terminal: route.handler)
            return
        }
        let terminal = router.matchWebSocket(segments: segments) != nil ? Oria.upgradeRequired : router.notFound
        await runChain(req, res, router: router, segments: segments, routeMiddleware: [], terminal: terminal)
    }

    /// Runs the middleware for a WebSocket route before upgrading. Returns true if every middleware
    /// called `next()` without responding; otherwise `res` holds the response to send instead.
    func authorizeUpgrade(
        _ req: Request, _ res: Response, router: CompiledRouter, route: CompiledRouter.WebSocketRoute
    ) async -> Bool {
        let segments = req.path.split(separator: "/", omittingEmptySubsequences: true)
        let allowed = NIOLockedValueBox(false)
        await runChain(req, res, router: router, segments: segments, routeMiddleware: route.middleware) { _, res in
            if !res.isSent { allowed.withLockedValue { $0 = true } }
        }
        return allowed.withLockedValue { $0 } && !res.isSent
    }

    private func runChain(
        _ req: Request, _ res: Response, router: CompiledRouter, segments: [Substring],
        routeMiddleware: [Middleware], terminal: @escaping Handler
    ) async {
        do {
            if router.hasScopedMiddleware {
                let scoped = router.middleware(for: segments)
                try await Oria.runScoped(scoped, 0, routeMiddleware, terminal, req, res)
            } else {
                let chain = routeMiddleware.isEmpty ? router.globalMiddleware : router.globalMiddleware + routeMiddleware
                try await Oria.run(chain, 0, terminal, req, res)
            }
        } catch {
            res.reset()
            await router.errorHandler(error, req, res)
        }
    }

    private static func run(
        _ chain: [Middleware], _ index: Int, _ terminal: Handler, _ req: Request, _ res: Response
    ) async throws {
        if index == chain.count {
            try await terminal(req, res)
            return
        }
        try await chain[index](req, res) {
            try await run(chain, index + 1, terminal, req, res)
        }
    }

    private static func runScoped(
        _ scoped: [(Middleware, String?)], _ index: Int, _ routeMiddleware: [Middleware],
        _ terminal: Handler, _ req: Request, _ res: Response
    ) async throws {
        if index == scoped.count {
            req.subpath = req.path
            try await run(routeMiddleware, 0, terminal, req, res)
            return
        }
        let (mw, sub) = scoped[index]
        req.subpath = sub ?? req.path
        try await mw(req, res) {
            try await runScoped(scoped, index + 1, routeMiddleware, terminal, req, res)
        }
    }

    static let upgradeRequired: Handler = { _, res in
        res.status(.upgradeRequired).set("upgrade", "websocket")
        try res.json(["error": "This endpoint requires a WebSocket upgrade"])
    }

    static let defaultNotFound: Handler = { req, res in
        res.status(.notFound)
        try res.json(["error": "Cannot \(req.method.rawValue) \(req.path)"])
    }

    static let defaultErrorHandler: ErrorHandler = { error, _, res in
        if let http = error as? HTTPError {
            res.status(http.status)
            try? res.json(["error": http.message])
        } else {
            FileHandle.standardError.write(Data("[oria] unhandled error: \(error)\n".utf8))
            res.status(.internalServerError)
            try? res.json(["error": "Internal Server Error"])
        }
    }
}

// MARK: - Executor integration

extension Oria {
    static let concurrencyRunsOnEventLoops = ManagedAtomic(false)

    /// Makes Swift Concurrency run on Oria's event-loop threads instead of a separate thread pool.
    ///
    /// Handlers then execute on the same thread that owns the socket, removing a thread hop per read
    /// and write. This noticeably lowers tail latency under heavy load. Call it once, as the first
    /// statement of `main.swift`, before any `Task` or `await` runs. `threads` replaces
    /// `Configuration.threads`.
    ///
    /// - Returns: false if the platform doesn't support it (the app still works normally).
    @discardableResult
    public static func runConcurrencyOnEventLoops(threads: Int = System.coreCount) -> Bool {
        NIOSingletons.groupLoopCountSuggestion = max(1, threads)
        let installed = NIOSingletons.unsafeTryInstallSingletonPosixEventLoopGroupAsConcurrencyGlobalExecutor()
        if installed { concurrencyRunsOnEventLoops.store(true, ordering: .relaxed) }
        return installed
    }
}

// MARK: - Testing without a socket

extension Oria {
    public struct TestResponse: Sendable {
        public let status: HTTPResponseStatus
        public let headers: HTTPHeaders
        public let body: ByteBuffer

        public var text: String { String(buffer: body) }

        public func json<T: Decodable>(_ type: T.Type = T.self) throws -> T {
            try JSONDecoder().decode(T.self, from: Data(text.utf8))
        }
    }

    /// Dispatches a request directly through the app (no networking). Ideal for unit tests.
    public func test(
        _ method: HTTPMethod, _ uri: String, headers: HTTPHeaders = [:], body: String? = nil
    ) async throws -> TestResponse {
        let allocator = ByteBufferAllocator()
        let head = HTTPRequestHead(version: .http1_1, method: method, uri: uri, headers: headers)
        let req = Request(
            head: head, body: body.map { allocator.buffer(string: $0) },
            remoteAddress: try? SocketAddress(ipAddress: "127.0.0.1", port: 0),
            trustProxy: configuration.trustProxy
        )
        let res = Response(allocator: allocator)
        await handle(req, res, router: compiledRouter())

        var out = allocator.buffer(capacity: 0)
        switch res.body {
        case .empty: break
        case .buffer(var b): out.writeBuffer(&b)
        case .stream(_, let producer):
            let collected = NIOLockedValueBox(allocator.buffer(capacity: 0))
            try await producer(
                BodyWriter(allocator: allocator) { chunk in
                    var chunk = chunk
                    _ = collected.withLockedValue { $0.writeBuffer(&chunk) }
                })
            out = collected.withLockedValue { $0 }
        }
        return TestResponse(status: res.statusCode, headers: res.headers, body: method == .HEAD ? allocator.buffer(capacity: 0) : out)
    }
}
