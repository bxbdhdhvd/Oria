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
/// - Requests (HTTP/1.1 connections and HTTP/2 streams) are read by an `HTTPRequestHandler` on the
///   event loop; the app's handler runs in a task whose executor *is* that event loop, so a typical
///   request never changes threads. Uploads and streamed responses get backpressure.
/// - The routing table is compiled once and shared immutably across threads (no locks per request).
public final class Server: Sendable {
    typealias WebSocketChannel = NIOAsyncChannel<WebSocketFrame, WebSocketFrame>

    /// What a new connection turned into once TLS/ALPN and any upgrade request were processed.
    enum Negotiated: Sendable {
        /// Requests are served by an `HTTPRequestHandler`; the connection task just waits for close.
        case http1
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
            let env = HandlerEnvironment(
                app: app, router: router, config: config, shuttingDown: shuttingDown,
                executors: LoopExecutors(group: group, enabled: config.runHandlersOnEventLoops)
            )
            pipeline = try PipelineFactory(env: env, intake: intake)
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
                        // Child options are set here rather than with `childChannelOption`: there, one
                        // failing setsockopt is reported on the *listening* channel and ends the accept loop.
                        Server.configureChild(channel)
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
                    // The accept loop ends when the listening socket closes; anything else is worth knowing.
                    if !shuttingDown.load(ordering: .relaxed) {
                        FileHandle.standardError.write(Data("[oria] accept loop failed: \(error)\n".utf8))
                    }
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
    private static let childOptionWarning = ManagedAtomic(false)

    /// Per-connection socket options. Failures (some sandboxes refuse options to unprivileged
    /// processes) are logged once and otherwise ignored: the connection works without them.
    static func configureChild(_ channel: Channel) {
        let options = channel.syncOptions
        try? options?.setOption(.maxMessagesPerRead, value: 16)
        do {
            try options?.setOption(.tcpOption(.tcp_nodelay), value: 1)
        } catch {
            if childOptionWarning.compareExchange(expected: false, desired: true, ordering: .relaxed).exchanged {
                FileHandle.standardError.write(Data("[oria] could not set TCP_NODELAY on accepted connections: \(error)\n".utf8))
            }
        }
    }

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

        _ = count
        switch negotiated {
        case .http1:
            timer.stop()
            try? await channel.closeFuture.get()

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

    /// Response headers plus date/server/connection. Nil if a header contains CR/LF/NUL (response
    /// splitting): the caller then fails closed with a 500.
    static func prepareHeaders(
        _ res: Response, for request: HTTPRequestHead, keepAlive: Bool, http2: Bool, serverName: String?
    ) -> HTTPHeaders? {
        // Take the headers out of the response so the mutations below don't copy them (CoW).
        var headers = res.headers
        res.headers = HTTPHeaders()
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
    let env: HandlerEnvironment
    let intake: ConnectionIntake
    let sslContext: NIOSSLContext?
    let decoderLimits: NIOHTTPDecoderLimitConfiguration
    /// Open connections, counted when the pipeline is built (for `maxConnections`).
    let liveConnections = ManagedAtomic(0)

    var app: Oria { env.app }
    var router: CompiledRouter { env.router }
    var config: Oria.Configuration { env.config }

    init(env: HandlerEnvironment, intake: ConnectionIntake) throws {
        self.env = env
        self.intake = intake
        let config = env.config
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
        let open = liveConnections.wrappingIncrementThenLoad(ordering: .relaxed)
        let counter = liveConnections
        channel.closeFuture.whenComplete { _ in counter.wrappingDecrement(ordering: .relaxed) }
        let overCapacity = config.maxConnections.map { open > $0 } ?? false
        let sync = channel.pipeline.syncOperations
        if let idle = config.idleTimeout {
            try sync.addHandler(IdleStateHandler(allTimeout: idle))
            try sync.addHandler(IdleCloseHandler())
        }
        guard let sslContext else { return try configureHTTP1(channel, state: state, overCapacity: overCapacity) }

        try sync.addHandler(NIOSSLServerHandler(context: sslContext))
        guard config.http2 else { return try configureHTTP1(channel, state: state, overCapacity: overCapacity) }

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
                return try factory.configureHTTP1(channel, state: state, overCapacity: overCapacity)
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

    func configureHTTP1(_ channel: Channel, state: ConnectionState, overCapacity: Bool) throws -> EventLoopFuture<Server.Negotiated> {
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
                        var rejection: Response?
                        if case .reject(let res) = decision.withLockedValue({ $0 }) { rejection = res }
                        // Keep the per-request path short: these only matter before the first request.
                        let sync = channel.pipeline.syncOperations
                        if let closer = try? sync.handler(type: ParseErrorCloser.self) { sync.removeHandler(closer, promise: nil) }
                        if let probe = try? sync.handler(type: ConnectionProbe.self) { sync.removeHandler(probe, promise: nil) }
                        // HTTPRequestHandler enforces read deadlines and write stalls itself.
                        if let idle = try? sync.handler(type: IdleStateHandler.self) { sync.removeHandler(idle, promise: nil) }
                        if let idleCloser = try? sync.handler(type: IdleCloseHandler.self) { sync.removeHandler(idleCloser, promise: nil) }
                        // HTTPRequestHandler queues pipelined requests itself and, without compression,
                        // serializes responses itself (HTTP1Writer): two handler hops fewer per request.
                        // Removing the pipelining handler hands its buffered requests on, in order.
                        if let pipelining = try? sync.handler(type: HTTPServerPipelineHandler.self) {
                            sync.removeHandler(pipelining, promise: nil)
                        }
                        if !factory.config.compression, let encoder = try? sync.handler(type: HTTPResponseEncoder.self) {
                            sync.removeHandler(encoder, promise: nil)
                        }
                        try channel.pipeline.syncOperations.addHandler(
                            HTTPRequestHandler(
                                env: factory.env, mode: .http1(rejection: rejection, overCapacity: overCapacity),
                                state: state, channel: channel
                            )
                        )
                        return .http1
                    }
                }
            )
        )
        configuration.decoderConfiguration = decoderLimits
        // Oria validates response headers itself (and answers 500 instead of failing the write), and
        // `HTTPRequestHandler` answers parse errors with 400: two fewer handlers per request.
        configuration.enableResponseHeaderValidation = false
        configuration.enableErrorHandling = false
        let sync = channel.pipeline.syncOperations
        let result = try sync.configureUpgradableHTTPServerPipeline(configuration: configuration)
        let upgradeHandler = try sync.handler(type: NIOTypedHTTPServerUpgradeHandler<Server.Negotiated>.self)
        try sync.addHandler(UpgradeFilter(router: router), position: .before(upgradeHandler))
        try sync.addHandler(ParseErrorCloser(), position: .before(upgradeHandler))
        return result
    }

    /// `Origin: https://app.example.com:8443` vs `Host: app.example.com:8443` (scheme ignored:
    /// a TLS-terminating proxy changes it). Case-insensitive, default ports normalized.
    static func isSameOrigin(_ origin: String, host: String?) -> Bool {
        guard let host, let schemeEnd = origin.range(of: "://") else { return false }
        let scheme = origin[..<schemeEnd.lowerBound].lowercased()
        func normalize(_ authority: Substring, scheme: String) -> String {
            var value = authority.lowercased()
            for (s, port) in [("http", ":80"), ("ws", ":80"), ("https", ":443"), ("wss", ":443")] where s == scheme {
                if value.hasSuffix(port) { value.removeLast(port.count) }
            }
            return value
        }
        let originHost = normalize(origin[schemeEnd.upperBound...], scheme: scheme)
        let requestHost = normalize(Substring(host), scheme: scheme)
        return !originHost.isEmpty && originHost == requestHost
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

        // Browsers send Origin on every WebSocket handshake and don't apply CORS to it. With an
        // allow-list, only listed origins pass (`"*"` allows any). Without one, browsers may only
        // connect same-origin, which stops cross-site WebSocket hijacking by default; non-browser
        // clients send no Origin and are unaffected.
        let origin = head.headers.first(name: "origin")
        let originAllowed: Bool
        if let allowed = route.options.allowedOrigins {
            originAllowed = allowed.contains("*") || origin.map(allowed.contains) ?? false
        } else {
            originAllowed = origin.map { Self.isSameOrigin($0, host: head.headers.first(name: "host")) } ?? true
        }
        guard originAllowed else {
            let res = Response(allocator: channel.allocator)
            res.status(.forbidden).json(raw: #"{"error":"Origin not allowed"}"#)
            decision.withLockedValue { $0 = .reject(res) }
            return channel.eventLoop.makeSucceededFuture(nil)
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
        let factory = self
        let parent = channel
        _ = try channel.pipeline.syncOperations.configureHTTP2Pipeline(
            mode: .server, connectionConfiguration: connection, streamConfiguration: .init()
        ) { stream in
            stream.eventLoop.makeCompletedFuture {
                let sync = stream.pipeline.syncOperations
                try sync.addHandler(HTTP2FramePayloadToHTTP1ServerCodec())
                if factory.config.compression { try sync.addHandler(factory.compressor()) }
                try sync.addHandler(
                    HTTPRequestHandler(env: factory.env, mode: .http2(parent: parent), state: state, channel: stream)
                )
            }
        }
        try channel.pipeline.syncOperations.addHandler(ConnectionErrorCloser())
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
    typealias OutboundOut = HTTPServerResponsePart

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
            // RFC 6455 §4.2.1: answer bad handshakes instead of leaving the client hanging.
            if let rejection = Self.validateWebSocketHandshake(head) {
                reject(context: context, status: rejection, version: head.version)
                return
            }
            context.fireChannelRead(data)
            return
        }
        head.headers.remove(name: "upgrade")
        head.headers.replaceOrAdd(name: "connection", value: "close")
        context.fireChannelRead(wrapInboundOut(.head(head)))
    }

    static func validateWebSocketHandshake(_ head: HTTPRequestHead) -> HTTPResponseStatus? {
        guard head.method == .GET, head.version == .http1_1 else { return .badRequest }
        // An upgrade request carries no body; bytes after it would be parsed as WebSocket frames.
        if head.headers.contains(name: "transfer-encoding")
            || (head.headers.first(name: "content-length").map { $0 != "0" } ?? false)
        {
            return .badRequest
        }
        guard head.headers.first(name: "sec-websocket-version") == "13" else { return .upgradeRequired }
        guard let key = head.headers.first(name: "sec-websocket-key"),
            let decoded = Data(base64Encoded: key.trimmingCharacters(in: .whitespaces)), decoded.count == 16
        else { return .badRequest }
        return nil
    }

    private func reject(context: ChannelHandlerContext, status: HTTPResponseStatus, version: HTTPVersion) {
        let body = ByteBuffer(string: "{\"error\":\"\(status.reasonPhrase)\"}")
        var headers = HTTPHeaders()
        headers.add(name: "content-type", value: "application/json")
        headers.add(name: "content-length", value: String(body.readableBytes))
        headers.add(name: "connection", value: "close")
        if status == .upgradeRequired {
            headers.add(name: "upgrade", value: "websocket")
            headers.add(name: "sec-websocket-version", value: "13")
        }
        context.write(wrapOutboundOut(.head(HTTPResponseHead(version: version, status: status, headers: headers))), promise: nil)
        context.write(wrapOutboundOut(.body(.byteBuffer(body))), promise: nil)
        context.writeAndFlush(wrapOutboundOut(.end(nil))).whenComplete { _ in context.close(promise: nil) }
    }
}

/// NIO answers malformed requests with `400 Connection: close` but leaves the socket open while the
/// upgrade handler waits; close it so garbage can't pin a connection until the read timeout.
final class ParseErrorCloser: ChannelInboundHandler, RemovableChannelHandler, Sendable {
    typealias InboundIn = HTTPServerRequestPart
    typealias OutboundOut = HTTPServerResponsePart

    /// Malformed first request (before `HTTPRequestHandler` is installed): answer 400 (431 for
    /// oversized headers, 414 for oversized URLs) and close. A peer hanging up mid-request gets no answer.
    func errorCaught(context: ChannelHandlerContext, error: Error) {
        if let parseError = error as? HTTPParserError {
            if parseError != .invalidEOFState {
                let status: HTTPResponseStatus
                switch parseError {
                case .headerOverflow: status = .requestHeaderFieldsTooLarge
                case .invalidURL: status = .uriTooLong
                default: status = .badRequest
                }
                let body = ByteBuffer(string: "{\"error\":\"\(status.reasonPhrase)\"}")
                var headers = HTTPHeaders()
                headers.add(name: "content-type", value: "application/json")
                headers.add(name: "content-length", value: String(body.readableBytes))
                headers.add(name: "connection", value: "close")
                context.write(wrapOutboundOut(.head(HTTPResponseHead(version: .http1_1, status: status, headers: headers))), promise: nil)
                context.write(wrapOutboundOut(.body(.byteBuffer(body))), promise: nil)
                context.writeAndFlush(wrapOutboundOut(.end(nil))).whenComplete { _ in context.close(promise: nil) }
            } else {
                context.close(promise: nil)
            }
            return
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

    /// Accept errors (EMFILE/ENFILE when out of descriptors, ENOBUFS, ECONNABORTED…) are transient:
    /// NIO keeps the listening socket open and retries. They must not reach the async server
    /// channel, where any error ends the accept loop and with it the server.
    func errorCaught(context: ChannelHandlerContext, error: Error) {
        let now = NIODeadline.now()
        if now - lastLog.load(ordering: .relaxed).asDeadline > .seconds(5) {
            lastLog.store(Int64(now.uptimeNanoseconds), ordering: .relaxed)
            FileHandle.standardError.write(Data("[oria] accept error (server keeps running): \(error)\n".utf8))
        }
    }

    private let lastLog = ManagedAtomic<Int64>(0)
}

extension Int64 {
    fileprivate var asDeadline: NIODeadline { .uptimeNanoseconds(UInt64(self)) }
}

/// Per-connection state for graceful shutdown. A connection is idle when no HTTP/2 stream is open
/// and, for HTTP/1, no request bytes have arrived since the last response. Idle connections are
/// closed at once on shutdown; busy ones finish their current request first. WebSockets get a hook
/// that sends a "going away" close frame.
final class ConnectionState: Sendable {
    private struct State {
        var http2 = false
        var shutdownHook: (@Sendable () -> Void)?
    }

    let channel: Channel
    private let input: InputActivity
    /// In-flight requests/streams. An atomic: it changes on every request.
    private let active = ManagedAtomic(0)
    private let state = NIOLockedValueBox(State())

    init(channel: Channel, input: InputActivity) {
        self.channel = channel
        self.input = input
    }

    func markHTTP2() { state.withLockedValue { $0.http2 = true } }

    func beginRequest() { active.wrappingIncrement(ordering: .relaxed) }

    /// Returns the number of requests still in flight.
    func endRequest() -> Int {
        let remaining = active.wrappingDecrementThenLoad(ordering: .relaxed)
        if remaining < 0 {
            active.store(0, ordering: .relaxed)
            return 0
        }
        return remaining
    }

    /// An HTTP/1 response went out; the connection is idle until the next request's bytes arrive.
    func requestFinished() { input.clear() }

    func setShutdownHook(_ hook: @escaping @Sendable () -> Void) {
        state.withLockedValue { $0.shutdownHook = hook }
    }

    func shutdown() {
        enum Action { case hook(@Sendable () -> Void), close, none }
        let busy = active.load(ordering: .relaxed) > 0
        let action: Action = state.withLockedValue { s in
            if let hook = s.shutdownHook {
                s.shutdownHook = nil
                return .hook(hook)
            }
            guard !busy else { return .none }
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
final class ConnectionProbe: ChannelInboundHandler, RemovableChannelHandler, @unchecked Sendable {
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
final class IdleCloseHandler: ChannelInboundHandler, RemovableChannelHandler, Sendable {
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
