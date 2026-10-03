import Atomics
import NIOConcurrencyHelpers
import NIOCore
import NIOHTTP1
import NIOHTTPCompression
import NIOPosix

/// A running HTTP server. Obtain one from `Oria.start(port:host:)` or implicitly via `listen`.
///
/// Architecture:
/// - One `MultiThreadedEventLoopGroup` with a thread per core handles all socket I/O, so a single
///   process saturates the machine (no Node-style `cluster` needed).
/// - Each connection is a structured-concurrency child task reading an `NIOAsyncChannel`, which gives
///   end-to-end backpressure: a slow client never makes the server buffer unbounded data.
/// - The routing table is compiled once and shared immutably across threads (no locks per request).
public final class Server: Sendable {
    typealias Connection = NIOAsyncChannel<HTTPServerRequestPart, HTTPServerResponsePart>

    /// The bound address (useful with `port: 0`).
    public let localAddress: SocketAddress?
    public var port: Int? { localAddress?.port }

    private let group: any EventLoopGroup
    private let serverChannel: Channel
    private let config: Oria.Configuration
    private let shuttingDown = ManagedAtomic(false)
    private let activeConnections = ManagedAtomic(0)
    private let connections = NIOLockedValueBox<[ObjectIdentifier: ConnectionState]>([:])
    private let runTask = NIOLockedValueBox<Task<Void, Never>?>(nil)

    /// Number of currently open client connections.
    public var connectionCount: Int { activeConnections.load(ordering: .relaxed) }

    private init(group: any EventLoopGroup, serverChannel: Channel, config: Oria.Configuration) {
        self.group = group
        self.serverChannel = serverChannel
        self.config = config
        self.localAddress = serverChannel.localAddress
    }

    static func start(app: Oria, router: CompiledRouter, host: String, port: Int) async throws -> Server {
        let config = app.configuration
        let usesSharedGroup = Oria.concurrencyRunsOnEventLoops.load(ordering: .relaxed)
        let group =
            usesSharedGroup
            ? MultiThreadedEventLoopGroup.singleton
            : MultiThreadedEventLoopGroup(numberOfThreads: max(1, config.threads))

        let intake = ConnectionIntake()
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
        let asyncServerChannel: NIOAsyncChannel<Connection, Never>
        do {
            asyncServerChannel = try await bootstrap.bind(
                host: host, port: port, serverBackPressureStrategy: acceptBuffer
            ) { channel in
                intake.initializing(channel)
                return channel.eventLoop.makeCompletedFuture {
                    let sync = channel.pipeline.syncOperations
                    if let idle = config.idleTimeout {
                        try sync.addHandler(IdleStateHandler(allTimeout: idle))
                        try sync.addHandler(IdleCloseHandler())
                    }
                    try sync.configureHTTPServerPipeline(withErrorHandling: true)
                    if config.compression {
                        try sync.addHandler(
                            HTTPResponseCompressor(responseCompressionPredicate: { head, _ in
                                // Streaming responses such as SSE must not be buffered by the compressor.
                                let type = head.headers.first(name: "content-type") ?? ""
                                return type.hasPrefix("text/event-stream") ? .doNotCompress : .compressIfPossible
                            })
                        )
                    }
                    do {
                        return try intake.admit { try Connection(wrappingChannelSynchronously: channel) }
                    } catch {
                        // NIO leaves a child whose initializer failed unregistered and open; close it
                        // ourselves or the socket leaks and the client waits forever.
                        channel.close(promise: nil)
                        throw error
                    }
                }
            }
        } catch {
            if !usesSharedGroup { try? await group.shutdownGracefully() }
            throw error
        }

        let server = Server(group: group, serverChannel: asyncServerChannel.channel, config: config)
        let task = Task {
            await withDiscardingTaskGroup { tasks in
                do {
                    try await asyncServerChannel.executeThenClose { inbound in
                        for try await connection in inbound {
                            intake.claim(connection)
                            tasks.addTask {
                                await server.handle(connection, app: app, router: router)
                            }
                        }
                    }
                } catch {
                    // The accept loop ends when the listening socket closes.
                }
                // Close sockets that were accepted while the listener was shutting down but never
                // reached the accept loop. Dropping them unfinished would trap inside NIO.
                let (orphans, uninitialized) = intake.closeAndDrain()
                for orphan in orphans {
                    tasks.addTask { try? await orphan.executeThenClose { _, _ in } }
                }
                // Sockets accepted but whose initializer hasn't run yet are unregistered, so neither
                // the event loop nor anyone else would ever close them.
                for channel in uninitialized {
                    channel.close(promise: nil)
                }
            }
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
    /// closes idle keep-alive connections, then releases all threads.
    public func shutdown() async {
        if shuttingDown.compareExchange(expected: false, desired: true, ordering: .acquiringAndReleasing).exchanged {
            try? await serverChannel.close()
            for connection in connections.withLockedValue({ Array($0.values) }) {
                connection.closeIfIdle()
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

    // MARK: Connection handling

    private func handle(_ connection: Connection, app: Oria, router: CompiledRouter) async {
        let count = activeConnections.wrappingIncrementThenLoad(ordering: .relaxed)
        defer { activeConnections.wrappingDecrement(ordering: .relaxed) }

        let channel = connection.channel
        let state = ConnectionState(channel: channel)
        let id = ObjectIdentifier(channel)
        connections.withLockedValue { $0[id] = state }
        defer { _ = connections.withLockedValue { $0.removeValue(forKey: id) } }

        let overCapacity = config.maxConnections.map { count > $0 } ?? false
        let maxBody = config.maxBodySize

        do {
            try await connection.executeThenClose { inbound, outbound in
                var parts = inbound.makeAsyncIterator()
                while let part = try await parts.next() {
                    guard case .head(let head) = part else { continue }
                    guard !shuttingDown.load(ordering: .relaxed), state.beginRequest() else { return }

                    if overCapacity {
                        try await writeSimple(.serviceUnavailable, version: head.version, to: outbound)
                        return
                    }
                    if let length = head.headers.first(name: "content-length").flatMap(Int.init), length > maxBody {
                        try await writeSimple(.payloadTooLarge, version: head.version, to: outbound)
                        return
                    }

                    // Buffer the body, enforcing the size limit for chunked uploads too.
                    var body: ByteBuffer?
                    var tooLarge = false
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
                            break readBody
                        case .head:
                            return
                        }
                    }
                    if tooLarge {
                        try await writeSimple(.payloadTooLarge, version: head.version, to: outbound)
                        return
                    }

                    let req = Request(
                        head: head, body: body, remoteAddress: channel.remoteAddress,
                        trustProxy: config.trustProxy
                    )
                    let res = Response(allocator: channel.allocator)
                    await app.handle(req, res, router: router)

                    var keepAlive = head.isKeepAlive && !shuttingDown.load(ordering: .relaxed)
                    if case .stream(nil, _) = res.body, head.version.isLegacy {
                        keepAlive = false  // HTTP/1.0 has no chunked encoding; the close delimits the body.
                    }
                    try await write(res, for: head, keepAlive: keepAlive, to: outbound)
                    guard keepAlive else { return }
                    state.endRequest()
                    if shuttingDown.load(ordering: .relaxed) { return }
                }
            }
        } catch {
            // Client resets, idle timeouts and protocol errors simply end the connection.
        }
    }

    private func write(
        _ res: Response, for request: HTTPRequestHead, keepAlive: Bool,
        to outbound: NIOAsyncChannelOutboundWriter<HTTPServerResponsePart>
    ) async throws {
        var headers = res.headers
        if !headers.contains(name: "date") { headers.add(name: "date", value: HTTPDate.now()) }
        if let name = config.serverName { headers.replaceOrAdd(name: "server", value: name) }
        if !keepAlive {
            headers.replaceOrAdd(name: "connection", value: "close")
        } else if request.version.isLegacy {
            headers.replaceOrAdd(name: "connection", value: "keep-alive")
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
        _ status: HTTPResponseStatus, version: HTTPVersion,
        to outbound: NIOAsyncChannelOutboundWriter<HTTPServerResponsePart>
    ) async throws {
        let body = ByteBuffer(string: "{\"error\":\"\(status.reasonPhrase)\"}")
        var headers = HTTPHeaders()
        headers.add(name: "content-type", value: "application/json; charset=utf-8")
        headers.add(name: "content-length", value: String(body.readableBytes))
        headers.add(name: "connection", value: "close")
        headers.add(name: "date", value: HTTPDate.now())
        let head = HTTPResponseHead(version: version, status: status, headers: headers)
        try await outbound.write(contentsOf: [.head(head), .body(.byteBuffer(body)), .end(nil)])
    }
}

/// Owns every accepted connection from the moment its `NIOAsyncChannel` is created until the accept
/// loop claims it, so none can be dropped unfinished during shutdown.
final class ConnectionIntake: Sendable {
    struct Closed: Error {}

    private struct State {
        var closed = false
        /// Accepted sockets whose child initializer hasn't started yet.
        var accepted: [ObjectIdentifier: Channel] = [:]
        /// Wrapped connections not yet claimed by the accept loop.
        var pending: [ObjectIdentifier: Server.Connection] = [:]
    }

    private let state = NIOLockedValueBox(State())

    /// Called on the listener's event loop for every accepted socket.
    func accepted(_ channel: Channel) -> Bool {
        state.withLockedValue { s in
            if s.closed { return false }
            s.accepted[ObjectIdentifier(channel)] = channel
            return true
        }
    }

    func initializing(_ channel: Channel) {
        _ = state.withLockedValue { $0.accepted.removeValue(forKey: ObjectIdentifier(channel)) }
    }

    /// Wraps a new connection unless the server is shutting down (then the socket is just closed).
    func admit(_ wrap: () throws -> Server.Connection) throws -> Server.Connection {
        try state.withLockedValue { s in
            if s.closed { throw Closed() }
            let connection = try wrap()
            s.pending[ObjectIdentifier(connection.channel)] = connection
            return connection
        }
    }

    func claim(_ connection: Server.Connection) {
        _ = state.withLockedValue { $0.pending.removeValue(forKey: ObjectIdentifier(connection.channel)) }
    }

    /// Stops admitting connections and returns everything that would otherwise leak.
    func closeAndDrain() -> (unclaimed: [Server.Connection], uninitialized: [Channel]) {
        state.withLockedValue { s in
            s.closed = true
            defer {
                s.pending.removeAll()
                s.accepted.removeAll()
            }
            return (Array(s.pending.values), Array(s.accepted.values))
        }
    }
}

/// First handler on the listening socket: records each accepted child before NIO hands it to the
/// (asynchronous) child initializer, so shutdown can close sockets that never got initialized.
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

/// Tracks whether a connection is between requests so shutdown can close idle keep-alive sockets
/// immediately without cutting off a response mid-flight.
final class ConnectionState: Sendable {
    private static let idle: UInt8 = 0
    private static let busy: UInt8 = 1
    private static let closing: UInt8 = 2

    let channel: Channel
    private let phase = ManagedAtomic<UInt8>(ConnectionState.idle)

    init(channel: Channel) { self.channel = channel }

    func beginRequest() -> Bool {
        phase.compareExchange(expected: Self.idle, desired: Self.busy, ordering: .acquiringAndReleasing).exchanged
    }

    func endRequest() {
        phase.store(Self.idle, ordering: .releasing)
    }

    func closeIfIdle() {
        if phase.compareExchange(expected: Self.idle, desired: Self.closing, ordering: .acquiringAndReleasing).exchanged {
            channel.close(promise: nil)
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
