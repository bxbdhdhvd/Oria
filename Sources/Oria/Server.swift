import Atomics
import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOHTTP1
import NIOHTTP2
import NIOHTTPCompression
import NIOPosix
import NIOSSL
import NIOTLS
import NIOWebSocket

/// A running server. Obtain one from `Oria.start(port:host:)` or implicitly via `listen`.
///
/// Architecture:
/// - One `MultiThreadedEventLoopGroup` with a thread per core handles all socket I/O, so a single
///   process saturates the machine (no Node-style `cluster` needed).
/// - Plain connections speak HTTP/1.1 and may upgrade to WebSocket. TLS connections negotiate
///   HTTP/2 or HTTP/1.1 via ALPN; every HTTP/2 stream is converted to the same request type, so
///   routes and middleware work identically on both.
/// - Each connection (and each HTTP/2 stream) is a structured-concurrency task over an
///   `NIOAsyncChannel`, which gives end-to-end backpressure.
/// - The routing table is compiled once and shared immutably across threads (no locks per request).
public final class Server: Sendable {
    typealias Connection = NIOAsyncChannel<HTTPServerRequestPart, HTTPServerResponsePart>
    typealias WebSocketChannel = NIOAsyncChannel<WebSocketFrame, WebSocketFrame>

    /// What a new connection turned into once TLS/ALPN and any upgrade request were processed.
    enum Negotiated: Sendable {
        case http1(Connection, rejection: Response?)
        case websocket(WebSocketChannel, Request, CompiledRouter.WebSocketRoute, subprotocol: String?)
        /// Streams are served by `HTTP2StreamHandler`s; the connection task just waits for close.
        case http2
    }

    struct Accepted: Sendable {
        let channel: Channel
        let negotiated: EventLoopFuture<Negotiated>
        let probe: ConnectionProbe
        let state: ConnectionState
    }

    /// The bound address (useful with `port: 0`).
    public let localAddress: SocketAddress?
    public var port: Int? { localAddress?.port }

    private let serverChannel: Channel
    private let config: Oria.Configuration
    private let app: Oria
    private let router: CompiledRouter
    private let intake: ConnectionIntake
    private let shuttingDown: ManagedAtomic<Bool>
    private let activeConnections = ManagedAtomic(0)
    private let connections = NIOLockedValueBox<[ObjectIdentifier: ConnectionState]>([:])
    private let runTask = NIOLockedValueBox<Task<Void, Never>?>(nil)

    /// Number of currently open client connections.
    public var connectionCount: Int { activeConnections.load(ordering: .relaxed) }

    private init(
        serverChannel: Channel, app: Oria, router: CompiledRouter, intake: ConnectionIntake,
        shuttingDown: ManagedAtomic<Bool>
    ) {
        self.shuttingDown = shuttingDown
        self.serverChannel = serverChannel
        self.config = app.configuration
        self.app = app
        self.router = router
        self.intake = intake
        self.localAddress = serverChannel.localAddress
    }

    static func start(app: Oria, router: CompiledRouter, host: String, port: Int) async throws -> Server {
        let config = app.configuration
        let usesSharedGroup = Oria.concurrencyRunsOnEventLoops.load(ordering: .relaxed)
        let group: any EventLoopGroup =
            usesSharedGroup
            ? MultiThreadedEventLoopGroup.singleton
            : MultiThreadedEventLoopGroup(numberOfThreads: max(1, config.threads))

        let intake = ConnectionIntake()
        let shuttingDown = ManagedAtomic(false)
        let pipeline: PipelineFactory
        do {
            pipeline = try PipelineFactory(app: app, router: router, intake: intake, shuttingDown: shuttingDown)
        } catch {
            if !usesSharedGroup { try? await group.shutdownGracefully() }
            throw error
        }

        var bootstrap = ServerBootstrap(group: group)
            .serverChannelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(AcceptTracker(intake: intake))
                }
            }
            .serverChannelOption(.backlog, value: config.backlog)
            .serverChannelOption(.socketOption(.so_reuseaddr), value: 1)
            .childChannelOption(.socketOption(.tcp_nodelay), value: 1)
            .childChannelOption(.socketOption(.so_reuseaddr), value: 1)
            .childChannelOption(.maxMessagesPerRead, value: 16)
        if config.reusePort {
            bootstrap = bootstrap.serverChannelOption(.socketOption(.so_reuseport), value: 1)
        }

        // Buffer plenty of accepted sockets so the accept loop isn't throttled behind busy request
        // tasks; with NIO's default (high watermark 10) new clients wait in the kernel backlog under load.
        let acceptBuffer = NIOAsyncSequenceProducerBackPressureStrategies.HighLowWatermark(
            lowWatermark: 256, highWatermark: 1024
        )
        let asyncServerChannel: NIOAsyncChannel<Accepted, Never>
        do {
            asyncServerChannel = try await bootstrap.bind(
                host: host, port: port, serverBackPressureStrategy: acceptBuffer
            ) { channel in
                channel.eventLoop.makeCompletedFuture {
                    do {
                        let probe = ConnectionProbe()
                        let state = ConnectionState(channel: channel, input: probe.input)
                        try channel.pipeline.syncOperations.addHandler(probe)
                        return Accepted(
                            channel: channel, negotiated: try pipeline.configure(channel, state: state), probe: probe,
                            state: state
                        )
                    } catch {
                        // NIO leaves a child whose initializer failed unregistered and open.
                        channel.close(promise: nil)
                        throw error
                    }
                }
            }
        } catch {
            if !usesSharedGroup { try? await group.shutdownGracefully() }
            throw error
        }

        let server = Server(
            serverChannel: asyncServerChannel.channel, app: app, router: router, intake: intake,
            shuttingDown: shuttingDown
        )
        let task = Task {
            await withDiscardingTaskGroup { tasks in
                do {
                    try await asyncServerChannel.executeThenClose { inbound in
                        for try await accepted in inbound {
                            tasks.addTask { await server.handle(accepted) }
                        }
                    }
                } catch {
                    // The accept loop ends when the listening socket closes.
                }
            }
            // Every handler has finished. Close whatever is left: sockets dropped mid-accept, and
            // async channels nobody claimed. Leaving them would leak descriptors or trap in NIO.
            let (unclaimed, live) = intake.finalSweep()
            await withDiscardingTaskGroup { tasks in
                for close in unclaimed { tasks.addTask { await close() } }
            }
            for channel in live { channel.close(promise: nil) }
            if !usesSharedGroup { try? await group.shutdownGracefully() }
        }
        server.runTask.withLockedValue { $0 = task }
        return server
    }

    /// Suspends until the server has fully stopped.
    public func wait() async throws {
        let task = runTask.withLockedValue { $0 }
        await task?.value
    }

    /// Stops accepting connections, lets in-flight requests finish (up to the grace period),
    /// closes idle keep-alive connections, sends WebSocket clients a "going away" close frame, then
    /// releases all threads.
    public func shutdown() async {
        if shuttingDown.compareExchange(expected: false, desired: true, ordering: .acquiringAndReleasing).exchanged {
            // Don't await the close future: once the listener closes, the run task may stop the
            // event loops before that future's callback is delivered, and the await never returns.
            // `wait()` below covers completion.
            serverChannel.close(promise: nil)
            for connection in connections.withLockedValue({ Array($0.values) }) {
                connection.shutdown()
            }
            let grace = config.shutdownGracePeriod
            let connections = self.connections
            let force = Task {
                try await Task.sleep(nanoseconds: UInt64(max(0, grace.nanoseconds)))
                for connection in connections.withLockedValue({ Array($0.values) }) {
                    connection.channel.close(promise: nil)
                }
            }
            try? await wait()
            force.cancel()
        } else {
            try? await wait()
        }
    }

    private var isShuttingDown: Bool { shuttingDown.load(ordering: .relaxed) }

    /// NIO's upgrade and ALPN handlers don't always fail their result when the connection closes
    /// first (e.g. after a parse error), which would leave the handler, and therefore graceful
    /// shutdown, waiting forever. This fails the result once the channel closes.
    static func untilClosed<T: Sendable>(_ future: EventLoopFuture<T>, channel: Channel) -> EventLoopFuture<T> {
        let promise = channel.eventLoop.makePromise(of: T.self)
        let completed = ManagedAtomic(false)
        future.whenComplete { result in
            if completed.compareExchange(expected: false, desired: true, ordering: .relaxed).exchanged {
                promise.completeWith(result)
            }
        }
        channel.closeFuture.whenComplete { _ in
            if completed.compareExchange(expected: false, desired: true, ordering: .relaxed).exchanged {
                promise.fail(ChannelError.ioOnClosedChannel)
            }
        }
        return promise.futureResult
    }

    // MARK: Connection handling

    private func handle(_ accepted: Accepted) async {
        let count = activeConnections.wrappingIncrementThenLoad(ordering: .relaxed)
        defer { activeConnections.wrappingDecrement(ordering: .relaxed) }

        let channel = accepted.channel
        let state = accepted.state
        let id = ObjectIdentifier(channel)
        connections.withLockedValue { $0[id] = state }
        defer { _ = connections.withLockedValue { $0.removeValue(forKey: id) } }
        // Whatever happens below (HTTP/2 GOAWAY after a protocol violation, a failed handshake, ...),
        // the socket must not outlive its handler.
        defer { channel.close(promise: nil) }
        if isShuttingDown { state.shutdown() }

        // Covers the TLS handshake and the first request, which negotiation waits for.
        let timer = ReadTimer(channel: channel, timeout: config.requestReadTimeout)
        timer.arm()
        defer { timer.stop() }

        let negotiated: Negotiated
        do {
            negotiated = try await Server.untilClosed(accepted.negotiated, channel: channel).get()
        } catch {
            channel.close(promise: nil)
            return
        }

        switch negotiated {
        case .http1(let connection, let rejection):
            guard intake.claim(connection.channel) else { return }
            let overCapacity = config.maxConnections.map { count > $0 } ?? false
            await serve(
                connection, state: state, timer: timer, http2: false, rejection: rejection, overCapacity: overCapacity,
                drain: { await accepted.probe.drained(on: channel) }
            )

        case .websocket(let wsChannel, let request, let route, let subprotocol):
            timer.cancel()
            guard intake.claim(wsChannel.channel) else { return }
            state.beginRequest()
            await WebSocket.run(
                wsChannel, request: request, route: route, subprotocol: subprotocol,
                onOpen: { socket in
                    state.setShutdownHook { Task { await socket.close(code: .goingAway, reason: "server shutting down") } }
                },
                beforeClose: { await accepted.probe.drained(on: channel) }
            )

        case .http2:
            timer.stop()
            state.markHTTP2()
            if isShuttingDown { state.shutdown() }
            try? await channel.closeFuture.get()
        }
    }

    /// Serves HTTP requests on an HTTP/1.1 connection (keep-alive loop) or a single HTTP/2 stream.
    private func serve(
        _ connection: Connection, state: ConnectionState, timer: ReadTimer, http2: Bool,
        rejection: Response?, overCapacity: Bool, drain: @Sendable () async -> Void
    ) async {
        let channel = connection.channel
        let maxBody = config.maxBodySize
        var rejection = rejection

        do {
            try await connection.executeThenClose { inbound, outbound in
                // executeThenClose closes the socket right away, which would discard response bytes
                // still queued in NIO (a slow client, a big body). Wait until they're written.
                try await serveLoop(inbound, outbound)
                await drain()
            }
        } catch {
            // Client resets, timeouts and protocol errors simply end the connection.
        }

        func serveLoop(
            _ inbound: NIOAsyncChannelInboundStream<HTTPServerRequestPart>,
            _ outbound: NIOAsyncChannelOutboundWriter<HTTPServerResponsePart>
        ) async throws {
                // A WebSocket upgrade refused by middleware or the origin check. NIO consumed that
                // request, so answer right away and close.
                if let refused = rejection {
                    rejection = nil
                    let head = HTTPRequestHead(version: .http1_1, method: .GET, uri: "/")
                    try await write(refused, for: head, keepAlive: false, http2: false, to: outbound)
                    return
                }
                var parts = inbound.makeAsyncIterator()
                while true {
                    timer.arm()
                    guard let part = try await parts.next() else { return }
                    guard case .head(let head) = part else { continue }

                    if overCapacity {
                        try await writeSimple(.serviceUnavailable, version: head.version, http2: http2, to: outbound)
                        return
                    }
                    if let length = head.headers.first(name: "content-length").flatMap(Int.init), length > maxBody {
                        try await writeSimple(.payloadTooLarge, version: head.version, http2: http2, to: outbound)
                        return
                    }

                    // Buffer the body, enforcing the size limit for chunked uploads too.
                    var body: ByteBuffer?
                    var tooLarge = false
                    var complete = false
                    readBody: while let next = try await parts.next() {
                        switch next {
                        case .body(var chunk):
                            if body == nil {
                                body = chunk
                            } else {
                                body!.writeBuffer(&chunk)
                            }
                            if body!.readableBytes > maxBody {
                                tooLarge = true
                                break readBody
                            }
                        case .end:
                            complete = true
                            break readBody
                        case .head:
                            return
                        }
                    }
                    timer.cancel()
                    // The stream ending before `.end` means the connection closed mid-request (read
                    // timeout, client gone). Never run a handler on a truncated body.
                    guard complete || tooLarge else { return }
                    if tooLarge {
                        try await writeSimple(.payloadTooLarge, version: head.version, http2: http2, to: outbound)
                        return
                    }

                    let req = Request(
                        head: head, body: body, remoteAddress: channel.remoteAddress ?? channel.parent?.remoteAddress,
                        trustProxy: config.trustProxy
                    )
                    if config.tls != nil { req.isSecure = true }
                    let res = Response(allocator: channel.allocator)
                    await app.handle(req, res, router: router)

                    var keepAlive = head.isKeepAlive && !isShuttingDown
                    if case .stream(nil, _) = res.body, head.version.isLegacy {
                        keepAlive = false  // HTTP/1.0 has no chunked encoding; the close delimits the body.
                    }
                    try await write(res, for: head, keepAlive: keepAlive, http2: http2, to: outbound)

                    if http2 {
                        // One request per stream. Let the stream finish on its own: closing it now
                        // would reset it while flow control is still holding back response data.
                        while try await parts.next() != nil {}
                        return
                    }
                    state.requestFinished()
                    guard keepAlive, !isShuttingDown else { return }
                }
        }
    }

    private func write(
        _ res: Response, for request: HTTPRequestHead, keepAlive: Bool, http2: Bool,
        to outbound: NIOAsyncChannelOutboundWriter<HTTPServerResponsePart>
    ) async throws {
        guard var headers = Server.prepareHeaders(res, for: request, keepAlive: keepAlive, http2: http2, serverName: config.serverName)
        else {
            try await writeSimple(.internalServerError, version: request.version, http2: http2, to: outbound)
            return
        }
        let code = res.statusCode.code
        let statusForbidsBody = code == 204 || code == 304 || (100..<200).contains(code)
        let omitBody = request.method == .HEAD || statusForbidsBody

        switch res.body {
        case .empty:
            if !statusForbidsBody { headers.replaceOrAdd(name: "content-length", value: "0") }
            let head = HTTPResponseHead(version: request.version, status: res.statusCode, headers: headers)
            try await outbound.write(contentsOf: [.head(head), .end(nil)])

        case .buffer(let buffer):
            if !statusForbidsBody {
                headers.replaceOrAdd(name: "content-length", value: String(buffer.readableBytes))
            }
            let head = HTTPResponseHead(version: request.version, status: res.statusCode, headers: headers)
            if omitBody {
                try await outbound.write(contentsOf: [.head(head), .end(nil)])
            } else {
                try await outbound.write(contentsOf: [.head(head), .body(.byteBuffer(buffer)), .end(nil)])
            }

        case .stream(let length, let producer):
            if let length { headers.replaceOrAdd(name: "content-length", value: String(length)) }
            let head = HTTPResponseHead(version: request.version, status: res.statusCode, headers: headers)
            try await outbound.write(.head(head))
            if !omitBody {
                try await producer(
                    BodyWriter(allocator: res.allocator) { chunk in
                        try await outbound.write(.body(.byteBuffer(chunk)))
                    })
            }
            try await outbound.write(.end(nil))
        }
    }

    private func writeSimple(
        _ status: HTTPResponseStatus, version: HTTPVersion, http2: Bool,
        to outbound: NIOAsyncChannelOutboundWriter<HTTPServerResponsePart>
    ) async throws {
        let body = ByteBuffer(string: "{\"error\":\"\(status.reasonPhrase)\"}")
        var headers = HTTPHeaders()
        headers.add(name: "content-type", value: "application/json; charset=utf-8")
        headers.add(name: "content-length", value: String(body.readableBytes))
        if !http2 { headers.add(name: "connection", value: "close") }
        headers.add(name: "date", value: HTTPDate.now())
        let head = HTTPResponseHead(version: version, status: status, headers: headers)
        try await outbound.write(contentsOf: [.head(head), .body(.byteBuffer(body)), .end(nil)])
    }

    /// Response headers plus date/server/connection. Nil if a header contains CR/LF/NUL (response
    /// splitting): the caller then fails closed with a 500.
    static func prepareHeaders(
        _ res: Response, for request: HTTPRequestHead, keepAlive: Bool, http2: Bool, serverName: String?
    ) -> HTTPHeaders? {
        var headers = res.headers
        guard headersAreSafe(headers) else {
            FileHandle.standardError.write(Data("[oria] refused to send unsafe response header (CR/LF/NUL)\n".utf8))
            return nil
        }
        if !headers.contains(name: "date") { headers.add(name: "date", value: HTTPDate.now()) }
        if let serverName { headers.replaceOrAdd(name: "server", value: serverName) }
        if !http2 {
            if !keepAlive {
                headers.replaceOrAdd(name: "connection", value: "close")
            } else if request.version.isLegacy {
                headers.replaceOrAdd(name: "connection", value: "keep-alive")
            }
        }
        return headers
    }

    static func headersAreSafe(_ headers: HTTPHeaders) -> Bool {
        for (name, value) in headers {
            if name.isEmpty { return false }
            for byte in name.utf8 where byte <= 32 || byte == UInt8(ascii: ":") || byte >= 127 {
                return false
            }
            for byte in value.utf8 where byte == 13 || byte == 10 || byte == 0 {
                return false
            }
        }
        return true
    }
}

// MARK: - Pipeline construction

/// Builds each new connection's channel pipeline: optional TLS + ALPN, HTTP/1.1 with WebSocket
/// upgrades, or HTTP/2.
struct PipelineFactory: Sendable {
    let app: Oria
    let router: CompiledRouter
    let intake: ConnectionIntake
    let config: Oria.Configuration
    let sslContext: NIOSSLContext?
    let decoderLimits: NIOHTTPDecoderLimitConfiguration
    let shuttingDown: ManagedAtomic<Bool>

    init(app: Oria, router: CompiledRouter, intake: ConnectionIntake, shuttingDown: ManagedAtomic<Bool>) throws {
        self.shuttingDown = shuttingDown
        self.app = app
        self.router = router
        self.intake = intake
        self.config = app.configuration
        if var tls = config.tls?.configuration {
            tls.applicationProtocols = config.http2 ? ["h2", "http/1.1"] : ["http/1.1"]
            self.sslContext = try NIOSSLContext(configuration: tls)
        } else {
            self.sslContext = nil
        }
        var limits = NIOHTTPDecoderLimitConfiguration()
        limits.maxHeaderFieldSize = config.maxHeaderSize
        limits.maxHeaderListSize = config.maxHeaderSize
        limits.maxHeaderFieldCount = config.maxHeaderCount
        self.decoderLimits = limits
    }

    /// Runs synchronously on the child channel's event loop.
    func configure(_ channel: Channel, state: ConnectionState) throws -> EventLoopFuture<Server.Negotiated> {
        let sync = channel.pipeline.syncOperations
        if let idle = config.idleTimeout {
            try sync.addHandler(IdleStateHandler(allTimeout: idle))
            try sync.addHandler(IdleCloseHandler())
        }
        guard let sslContext else { return try configureHTTP1(channel) }

        try sync.addHandler(NIOSSLServerHandler(context: sslContext))
        guard config.http2 else { return try configureHTTP1(channel) }

        // ALPN picks the protocol: "h2" gets HTTP/2, anything else (or no ALPN) HTTP/1.1.
        // The ALPN handler buffers inbound bytes until its future completes, so it must complete as
        // soon as the pipeline is configured. The HTTP/1 upgrade decision, which waits for the
        // first request, travels as a nested future (otherwise the two wait on each other forever).
        let factory = self
        let alpn = NIOTypedApplicationProtocolNegotiationHandler<EventLoopFuture<Server.Negotiated>> { result, channel in
            channel.eventLoop.makeCompletedFuture {
                if case .negotiated("h2") = result {
                    try factory.configureHTTP2(channel, state: state)
                    return channel.eventLoop.makeSucceededFuture(.http2)
                }
                return try factory.configureHTTP1(channel)
            }
        }
        try sync.addHandler(alpn)
        return alpn.protocolNegotiationResult.flatMap { $0 }
    }

    private func compressor() -> HTTPResponseCompressor {
        HTTPResponseCompressor(responseCompressionPredicate: { head, _ in
            // Streaming responses such as SSE must not be buffered by the compressor.
            let type = head.headers.first(name: "content-type") ?? ""
            return type.hasPrefix("text/event-stream") ? .doNotCompress : .compressIfPossible
        })
    }

    private enum UpgradeDecision {
        case accept(Request, CompiledRouter.WebSocketRoute, subprotocol: String?)
        case reject(Response)
    }

    func configureHTTP1(_ channel: Channel) throws -> EventLoopFuture<Server.Negotiated> {
        let decision = NIOLockedValueBox<UpgradeDecision?>(nil)
        let factory = self
        var upgraders: [any NIOTypedHTTPServerProtocolUpgrader<Server.Negotiated>] = []
        if router.maxWebSocketFrameSize > 0 {
            upgraders.append(
                NIOTypedWebSocketServerUpgrader<Server.Negotiated>(
                    maxFrameSize: router.maxWebSocketFrameSize,
                    enableAutomaticErrorHandling: true,
                    shouldUpgrade: { channel, head in
                        factory.shouldUpgrade(channel, head, decision: decision)
                    },
                    upgradePipelineHandler: { channel, _ in
                        channel.eventLoop.makeCompletedFuture {
                            guard case .accept(let req, let route, let subprotocol) = decision.withLockedValue({ $0 })
                            else { throw ChannelError.inappropriateOperationForState }
                            try channel.pipeline.syncOperations.addHandler(
                                WebSocketFrameGuard(maxMessageSize: route.options.maxMessageSize)
                            )
                            try channel.pipeline.syncOperations.addHandler(
                                NIOWebSocketFrameAggregator(
                                    minNonFinalFragmentSize: 1,
                                    maxAccumulatedFrameCount: 4096,
                                    maxAccumulatedFrameSize: route.options.maxMessageSize
                                )
                            )
                            let ws = try factory.intake.admit { try Server.WebSocketChannel(wrappingChannelSynchronously: channel) }
                            return .websocket(ws, req, route, subprotocol: subprotocol)
                        }
                    }
                )
            )
        }

        var configuration = NIOUpgradableHTTPServerPipelineConfiguration(
            upgradeConfiguration: .init(
                upgraders: upgraders,
                notUpgradingCompletionHandler: { channel in
                    channel.eventLoop.makeCompletedFuture {
                        if factory.config.compression {
                            try channel.pipeline.syncOperations.addHandler(factory.compressor())
                        }
                        let connection = try factory.intake.admit { try Server.Connection(wrappingChannelSynchronously: channel) }
                        var rejection: Response?
                        if case .reject(let res) = decision.withLockedValue({ $0 }) { rejection = res }
                        return .http1(connection, rejection: rejection)
                    }
                }
            )
        )
        configuration.decoderConfiguration = decoderLimits
        let sync = channel.pipeline.syncOperations
        let result = try sync.configureUpgradableHTTPServerPipeline(configuration: configuration)
        let upgradeHandler = try sync.handler(type: NIOTypedHTTPServerUpgradeHandler<Server.Negotiated>.self)
        try sync.addHandler(UpgradeFilter(router: router), position: .before(upgradeHandler))
        try sync.addHandler(ParseErrorCloser(), position: .before(upgradeHandler))
        return result
    }

    private func shouldUpgrade(
        _ channel: Channel, _ head: HTTPRequestHead, decision: NIOLockedValueBox<UpgradeDecision?>
    ) -> EventLoopFuture<HTTPHeaders?> {
        let path = head.uri.split(separator: "?", maxSplits: 1).first.map(String.init) ?? head.uri
        let segments = path.split(separator: "/", omittingEmptySubsequences: true)
        guard let (route, params) = router.matchWebSocket(segments: segments) else {
            return channel.eventLoop.makeSucceededFuture(nil)  // Not a socket route: plain HTTP handles it.
        }
        let req = Request(head: head, body: nil, remoteAddress: channel.remoteAddress, trustProxy: config.trustProxy)
        req.params = params
        if config.tls != nil { req.isSecure = true }

        // Browsers send Origin on every WebSocket handshake; refuse ones not on the allow-list.
        if let allowed = route.options.allowedOrigins {
            guard let origin = head.headers.first(name: "origin"), allowed.contains(origin) else {
                let res = Response(allocator: channel.allocator)
                res.status(.forbidden).json(raw: #"{"error":"Origin not allowed"}"#)
                decision.withLockedValue { $0 = .reject(res) }
                return channel.eventLoop.makeSucceededFuture(nil)
            }
        }

        let app = self.app
        let router = self.router
        let promise = channel.eventLoop.makePromise(of: HTTPHeaders?.self)
        promise.completeWithTask {
            let res = Response(allocator: channel.allocator)
            guard await app.authorizeUpgrade(req, res, router: router, route: route) else {
                decision.withLockedValue { $0 = .reject(res) }
                return nil
            }
            var headers = res.headers
            let offered = head.headers[canonicalForm: "sec-websocket-protocol"].map { String($0) }
            let subprotocol = route.options.protocols.first { offered.contains($0) }
            if let subprotocol { headers.replaceOrAdd(name: "sec-websocket-protocol", value: subprotocol) }
            decision.withLockedValue { $0 = .accept(req, route, subprotocol: subprotocol) }
            return headers
        }
        return promise.futureResult
    }

    func configureHTTP2(_ channel: Channel, state: ConnectionState) throws {
        var connection = NIOHTTP2Handler.ConnectionConfiguration()
        connection.initialSettings = [
            HTTP2Setting(parameter: .maxConcurrentStreams, value: config.http2MaxConcurrentStreams),
            HTTP2Setting(parameter: .maxHeaderListSize, value: config.maxHeaderSize),
        ]
        let responder = HTTP2Responder(
            app: app, router: router, config: config, state: state, parent: channel, shuttingDown: shuttingDown
        )
        let factory = self
        _ = try channel.pipeline.syncOperations.configureHTTP2Pipeline(
            mode: .server, connectionConfiguration: connection, streamConfiguration: .init()
        ) { stream in
            stream.eventLoop.makeCompletedFuture {
                let sync = stream.pipeline.syncOperations
                try sync.addHandler(HTTP2FramePayloadToHTTP1ServerCodec())
                if factory.config.compression { try sync.addHandler(factory.compressor()) }
                try sync.addHandler(HTTP2StreamHandler(responder: responder))
            }
        }
        try channel.pipeline.syncOperations.addHandler(ConnectionErrorCloser())
    }
}

// MARK: - HTTP/2 streams

/// Runs one HTTP/2 request through the app and writes the response on the stream channel.
struct HTTP2Responder: Sendable {
    let app: Oria
    let router: CompiledRouter
    let config: Oria.Configuration
    let state: ConnectionState
    let parent: Channel
    let shuttingDown: ManagedAtomic<Bool>

    func respond(_ head: HTTPRequestHead, body: ByteBuffer?, on stream: Channel) async {
        let req = Request(head: head, body: body, remoteAddress: parent.remoteAddress, trustProxy: config.trustProxy)
        if config.tls != nil { req.isSecure = true }
        let res = Response(allocator: stream.allocator)
        await app.handle(req, res, router: router)
        await write(res, for: head, on: stream)
    }

    func write(_ res: Response, for request: HTTPRequestHead, on stream: Channel) async {
        guard let headers = Server.prepareHeaders(res, for: request, keepAlive: true, http2: true, serverName: config.serverName)
        else {
            writeSimple(.internalServerError, version: request.version, on: stream)
            return
        }
        var finalHeaders = headers
        let code = res.statusCode.code
        let statusForbidsBody = code == 204 || code == 304 || (100..<200).contains(code)
        let omitBody = request.method == .HEAD || statusForbidsBody
        let version = request.version
        let status = res.statusCode

        switch res.body {
        case .empty:
            if !statusForbidsBody { finalHeaders.replaceOrAdd(name: "content-length", value: "0") }
            let head = HTTPResponseHead(version: version, status: status, headers: finalHeaders)
            stream.eventLoop.execute {
                stream.write(HTTPServerResponsePart.head(head), promise: nil)
                stream.writeAndFlush(HTTPServerResponsePart.end(nil), promise: nil)
            }
        case .buffer(let buffer):
            if !statusForbidsBody { finalHeaders.replaceOrAdd(name: "content-length", value: String(buffer.readableBytes)) }
            let head = HTTPResponseHead(version: version, status: status, headers: finalHeaders)
            // One hop to the event loop for the whole response.
            stream.eventLoop.execute {
                stream.write(HTTPServerResponsePart.head(head), promise: nil)
                if !omitBody { stream.write(HTTPServerResponsePart.body(.byteBuffer(buffer)), promise: nil) }
                stream.writeAndFlush(HTTPServerResponsePart.end(nil), promise: nil)
            }
        case .stream(let length, let producer):
            if let length { finalHeaders.replaceOrAdd(name: "content-length", value: String(length)) }
            let head = HTTPResponseHead(version: version, status: status, headers: finalHeaders)
            do {
                try await stream.writeAndFlush(HTTPServerResponsePart.head(head)).get()
                if !omitBody {
                    // Awaiting each chunk's write gives backpressure (HTTP/2 flow control included).
                    try await producer(
                        BodyWriter(allocator: res.allocator) { chunk in
                            try await stream.writeAndFlush(HTTPServerResponsePart.body(.byteBuffer(chunk))).get()
                        })
                }
                try await stream.writeAndFlush(HTTPServerResponsePart.end(nil)).get()
            } catch {
                stream.close(promise: nil)  // Peer reset the stream or the connection closed.
            }
        }
    }

    func writeSimple(_ status: HTTPResponseStatus, version: HTTPVersion, on stream: Channel) {
        let body = ByteBuffer(string: "{\"error\":\"\(status.reasonPhrase)\"}")
        var headers = HTTPHeaders()
        headers.add(name: "content-type", value: "application/json; charset=utf-8")
        headers.add(name: "content-length", value: String(body.readableBytes))
        headers.add(name: "date", value: HTTPDate.now())
        let head = HTTPResponseHead(version: version, status: status, headers: headers)
        stream.eventLoop.execute {
            stream.write(HTTPServerResponsePart.head(head), promise: nil)
            stream.write(HTTPServerResponsePart.body(.byteBuffer(body)), promise: nil)
            stream.writeAndFlush(HTTPServerResponsePart.end(nil), promise: nil)
        }
    }

    func streamClosed() {
        // Close an idle HTTP/2 connection once draining.
        if state.endRequest() == 0 && shuttingDown.load(ordering: .relaxed) { parent.close(promise: nil) }
    }
}

/// Collects one HTTP/2 request on the stream's event loop (enforcing the body limit and read
/// timeout there, so neither costs a thread hop), then hands it to a task that runs the app.
/// Much cheaper per stream than wrapping every stream in an `NIOAsyncChannel`.
final class HTTP2StreamHandler: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = HTTPServerRequestPart
    typealias OutboundOut = HTTPServerResponsePart

    private let responder: HTTP2Responder
    private var head: HTTPRequestHead?
    private var body: ByteBuffer?
    private var rejected = false
    private var timer: Scheduled<Void>?

    init(responder: HTTP2Responder) { self.responder = responder }

    func handlerAdded(context: ChannelHandlerContext) {
        responder.state.beginRequest()
        let responder = self.responder
        context.channel.closeFuture.whenComplete { _ in responder.streamClosed() }
        if let timeout = responder.config.requestReadTimeout {
            let channel = context.channel
            timer = context.eventLoop.scheduleTask(in: timeout) { channel.close(promise: nil) }
        }
    }

    func handlerRemoved(context: ChannelHandlerContext) {
        timer?.cancel()
        timer = nil
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard !rejected else { return }
        let maxBody = responder.config.maxBodySize
        switch unwrapInboundIn(data) {
        case .head(let head):
            self.head = head
            if let length = head.headers.first(name: "content-length").flatMap(Int.init), length > maxBody {
                reject(context, head.version)
            }
        case .body(var chunk):
            if body == nil {
                body = chunk
            } else {
                body!.writeBuffer(&chunk)
            }
            if body!.readableBytes > maxBody, let head { reject(context, head.version) }
        case .end:
            timer?.cancel()
            timer = nil
            guard let head else { return }
            let body = self.body
            self.head = nil
            self.body = nil
            let responder = self.responder
            let stream = context.channel
            Task { await responder.respond(head, body: body, on: stream) }
        }
    }

    private func reject(_ context: ChannelHandlerContext, _ version: HTTPVersion) {
        rejected = true
        body = nil
        timer?.cancel()
        timer = nil
        responder.writeSimple(.payloadTooLarge, version: version, on: context.channel)
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        context.close(promise: nil)
    }
}

// MARK: - Lifecycle bookkeeping

/// Sits in front of NIO's upgrade handler, which silently drops any request carrying an `Upgrade`
/// header that no upgrader accepts (e.g. `Upgrade: h2c`, or a WebSocket handshake to a plain route).
/// Only WebSocket handshakes for WebSocket routes pass through untouched; for every other request
/// the upgrade headers are removed so it's served as normal HTTP. The decoder stops parsing after an
/// upgrade-flagged request, so those responses carry `Connection: close`.
final class UpgradeFilter: ChannelInboundHandler, RemovableChannelHandler, Sendable {
    typealias InboundIn = HTTPServerRequestPart
    typealias InboundOut = HTTPServerRequestPart

    let router: CompiledRouter

    init(router: CompiledRouter) { self.router = router }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard case .head(var head) = unwrapInboundIn(data) else {
            context.fireChannelRead(data)
            return
        }
        // Only the first request on a connection can upgrade (NIO then removes its handler), so we
        // step aside after it. Forward first: a removed context can no longer pass data on.
        defer { context.pipeline.syncOperations.removeHandler(context: context, promise: nil) }

        let upgrade = head.headers[canonicalForm: "upgrade"]
        guard !upgrade.isEmpty else {
            context.fireChannelRead(data)
            return
        }
        let wantsWebSocket = upgrade.contains { $0.lowercased() == "websocket" }
        let path = head.uri.split(separator: "?", maxSplits: 1).first.map(String.init) ?? head.uri
        if wantsWebSocket
            && router.matchWebSocket(segments: path.split(separator: "/", omittingEmptySubsequences: true)) != nil
        {
            context.fireChannelRead(data)
            return
        }
        head.headers.remove(name: "upgrade")
        head.headers.replaceOrAdd(name: "connection", value: "close")
        context.fireChannelRead(wrapInboundOut(.head(head)))
    }
}

/// NIO answers malformed requests with `400 Connection: close` but leaves the socket open while the
/// upgrade handler waits; close it so garbage can't pin a connection until the read timeout.
final class ParseErrorCloser: ChannelInboundHandler, Sendable {
    typealias InboundIn = HTTPServerRequestPart

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        if error is HTTPParserError {
            context.flush()
            context.close(promise: nil)
        }
        context.fireErrorCaught(error)
    }
}

/// On an HTTP/2 connection error (flood protection, HPACK bombs, protocol violations) NIOHTTP2 sends
/// GOAWAY and reports the error, but leaves the socket open. Flush the GOAWAY and hang up.
final class ConnectionErrorCloser: ChannelInboundHandler, Sendable {
    typealias InboundIn = HTTP2Frame

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        context.flush()
        context.close(promise: nil)
        context.fireErrorCaught(error)
    }
}

/// Makes sure no socket or `NIOAsyncChannel` can leak or be dropped unfinished, including during
/// shutdown races:
/// - every accepted socket is tracked until it closes, so the final sweep can close ones that never
///   reached a handler;
/// - every `NIOAsyncChannel` is owned here from creation until its handler claims it. If its
///   channel closes first, or the server stops, it is finished here instead (dropping one
///   unfinished traps inside NIO).
final class ConnectionIntake: Sendable {
    struct Closed: Error {}

    private struct State {
        var closed = false
        var live: [ObjectIdentifier: Channel] = [:]
        var unclaimed: [ObjectIdentifier: @Sendable () async -> Void] = [:]
    }

    private let state = NIOLockedValueBox(State())

    /// Called on the listener's event loop for every accepted socket.
    func accepted(_ channel: Channel) -> Bool {
        let id = ObjectIdentifier(channel)
        let ok = state.withLockedValue { s in
            if s.closed { return false }
            s.live[id] = channel
            return true
        }
        if ok {
            channel.closeFuture.whenComplete { [state] _ in
                _ = state.withLockedValue { $0.live.removeValue(forKey: id) }
            }
        }
        return ok
    }

    /// Wraps a channel in an `NIOAsyncChannel` and holds it until `claim`.
    func admit<Inbound: Sendable, Outbound: Sendable>(
        _ wrap: () throws -> NIOAsyncChannel<Inbound, Outbound>
    ) throws -> NIOAsyncChannel<Inbound, Outbound> {
        let asyncChannel: NIOAsyncChannel<Inbound, Outbound> = try state.withLockedValue { s in
            if s.closed { throw Closed() }
            let asyncChannel = try wrap()
            s.unclaimed[ObjectIdentifier(asyncChannel.channel)] = {
                try? await asyncChannel.executeThenClose { _, _ in }
            }
            return asyncChannel
        }
        let id = ObjectIdentifier(asyncChannel.channel)
        asyncChannel.channel.closeFuture.whenComplete { [state] _ in
            // Closed before any handler claimed it: finish it here.
            if let finish = state.withLockedValue({ $0.unclaimed.removeValue(forKey: id) }) {
                Task { await finish() }
            }
        }
        return asyncChannel
    }

    /// Takes ownership. Returns false if the channel was already finished here (then skip it).
    func claim(_ channel: Channel) -> Bool {
        state.withLockedValue { $0.unclaimed.removeValue(forKey: ObjectIdentifier(channel)) } != nil
    }

    /// Stops admitting and returns everything that still needs closing.
    func finalSweep() -> (unclaimed: [@Sendable () async -> Void], live: [Channel]) {
        state.withLockedValue { s in
            s.closed = true
            defer {
                s.unclaimed.removeAll()
                s.live.removeAll()
            }
            return (Array(s.unclaimed.values), Array(s.live.values))
        }
    }
}

/// First handler on the listening socket: records each accepted child before NIO hands it to the
/// (asynchronous) child initializer.
final class AcceptTracker: ChannelInboundHandler, Sendable {
    typealias InboundIn = Channel

    let intake: ConnectionIntake

    init(intake: ConnectionIntake) { self.intake = intake }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let child = unwrapInboundIn(data)
        guard intake.accepted(child) else {
            child.close(promise: nil)
            return
        }
        context.fireChannelRead(data)
    }
}

/// Per-connection state for graceful shutdown. A connection is idle when no HTTP/2 stream is open
/// and, for HTTP/1, no request bytes have arrived since the last response. Idle connections are
/// closed at once on shutdown; busy ones finish their current request first. WebSockets get a hook
/// that sends a "going away" close frame.
final class ConnectionState: Sendable {
    private struct State {
        var active = 0
        var http2 = false
        var shutdownHook: (@Sendable () -> Void)?
    }

    let channel: Channel
    private let input: InputActivity
    private let state = NIOLockedValueBox(State())

    init(channel: Channel, input: InputActivity) {
        self.channel = channel
        self.input = input
    }

    func markHTTP2() { state.withLockedValue { $0.http2 = true } }

    func beginRequest() { state.withLockedValue { $0.active += 1 } }

    /// Returns the number of requests still in flight.
    func endRequest() -> Int {
        state.withLockedValue { s in
            s.active = max(0, s.active - 1)
            return s.active
        }
    }

    /// An HTTP/1 response went out; the connection is idle until the next request's bytes arrive.
    func requestFinished() { input.clear() }

    func setShutdownHook(_ hook: @escaping @Sendable () -> Void) {
        state.withLockedValue { $0.shutdownHook = hook }
    }

    func shutdown() {
        enum Action { case hook(@Sendable () -> Void), close, none }
        let action: Action = state.withLockedValue { s in
            if let hook = s.shutdownHook {
                s.shutdownHook = nil
                return .hook(hook)
            }
            guard s.active == 0 else { return .none }
            // HTTP/2 connections always carry control frames, so only open streams count there.
            return s.http2 || !input.pending ? .close : .none
        }
        switch action {
        case .hook(let hook): hook()
        case .close: channel.close(promise: nil)
        case .none: break
        }
    }
}

/// First handler on every connection. It costs nothing on the write path (it's inbound-only) and:
/// - flags the connection busy as soon as request bytes arrive (for graceful shutdown);
/// - lets the server wait until queued response bytes reach the socket before closing. Closing a NIO
///   channel discards unwritten data, and NIO completes writes in order, so an empty write's
///   promise fires only after everything queued before it.
final class ConnectionProbe: ChannelInboundHandler, @unchecked Sendable {
    typealias InboundIn = NIOAny
    typealias OutboundOut = ByteBuffer

    let input = InputActivity()
    private var context: ChannelHandlerContext?  // event-loop confined

    func handlerAdded(context: ChannelHandlerContext) { self.context = context }
    func handlerRemoved(context: ChannelHandlerContext) { self.context = nil }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        input.mark()
        context.fireChannelRead(data)
    }

    /// Completes once every response byte written so far has reached the kernel (at most 30 s).
    func drained(on channel: Channel) async {
        let future: EventLoopFuture<Void> = channel.eventLoop.flatSubmit {
            guard let context = self.context, channel.isActive else { return channel.eventLoop.makeSucceededVoidFuture() }
            let marker = context.writeAndFlush(self.wrapOutboundOut(context.channel.allocator.buffer(capacity: 0)))
            let timeout = channel.eventLoop.makePromise(of: Void.self)
            let task = channel.eventLoop.scheduleTask(in: .seconds(30)) { timeout.succeed() }
            marker.whenComplete { _ in
                task.cancel()
                timeout.succeed()
            }
            return timeout.futureResult
        }
        try? await future.get()
    }
}

/// Whether request bytes arrived since the last HTTP/1 response.
final class InputActivity: Sendable {
    private let flag = ManagedAtomic(false)
    var pending: Bool { flag.load(ordering: .acquiring) }
    func mark() { flag.store(true, ordering: .releasing) }
    func clear() { flag.store(false, ordering: .releasing) }
}

/// Closes a connection (or HTTP/2 stream) that doesn't deliver a complete request in time.
///
/// Arming and disarming are single atomic stores, so the per-request cost is negligible. One checker
/// task per channel wakes up at most once per timeout period and closes the channel if the deadline
/// passed. (Scheduling and cancelling a NIO task per request costs two cross-thread wake-ups.)
final class ReadTimer: Sendable {
    private let channel: Channel
    private let timeoutNanos: Int64
    private let deadline = ManagedAtomic<Int64>(0)  // 0 = disarmed
    private let checking = ManagedAtomic(false)
    private let scheduled = NIOLockedValueBox<(task: Scheduled<Void>?, stopped: Bool)>((nil, false))

    init(channel: Channel, timeout: TimeAmount?) {
        self.channel = channel
        self.timeoutNanos = timeout?.nanoseconds ?? 0
    }

    private static func now() -> Int64 { Int64(DispatchTime.now().uptimeNanoseconds) }

    func arm() {
        guard timeoutNanos > 0, deadline.load(ordering: .relaxed) == 0 else { return }
        deadline.store(Self.now() + timeoutNanos, ordering: .relaxed)
        if checking.compareExchange(expected: false, desired: true, ordering: .relaxed).exchanged {
            schedule(in: timeoutNanos)
        }
    }

    func cancel() {
        deadline.store(0, ordering: .relaxed)
    }

    /// Cancels the checker for good. Call when the connection/stream is finished: a pending checker
    /// would otherwise keep the whole channel alive until it fires.
    func stop() {
        deadline.store(0, ordering: .relaxed)
        let task = scheduled.withLockedValue { s -> Scheduled<Void>? in
            s.stopped = true
            defer { s.task = nil }
            return s.task
        }
        task?.cancel()
    }

    private func schedule(in nanos: Int64) {
        let channel = self.channel
        scheduled.withLockedValue { s in
            guard !s.stopped else { return }
            s.task = channel.eventLoop.scheduleTask(in: .nanoseconds(max(nanos, 1_000_000))) { [self] in
                guard channel.isActive else { return }
                let due = self.deadline.load(ordering: .relaxed)
                let now = Self.now()
                if due != 0 && now >= due {
                    channel.close(promise: nil)
                } else {
                    self.schedule(in: due == 0 ? self.timeoutNanos : due - now)
                }
            }
        }
    }
}

/// Closes a connection when `IdleStateHandler` reports inactivity.
final class IdleCloseHandler: ChannelInboundHandler, Sendable {
    typealias InboundIn = NIOAny

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if event is IdleStateHandler.IdleStateEvent {
            context.close(promise: nil)
        } else {
            context.fireUserInboundEventTriggered(event)
        }
    }
}

extension HTTPVersion {
    /// True for HTTP/1.0 and earlier.
    fileprivate var isLegacy: Bool { major < 1 || (major == 1 && minor == 0) }
}
