import Atomics
import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOHTTP1
import NIOWebSocket

/// Handles an accepted WebSocket connection. The socket closes when the handler returns.
public typealias WebSocketHandler = @Sendable (Request, WebSocket) async throws -> Void

public struct WebSocketOptions: Sendable {
    /// Largest single frame accepted from a client. Browsers send each message as one frame, so
    /// this normally matches `maxMessageSize`.
    public var maxFrameSize: Int = 1 << 20
    /// Largest message (after reassembling fragments) accepted from a client.
    public var maxMessageSize: Int = 1 << 20
    /// Allowed `Origin` header values. `nil` allows any origin.
    ///
    /// Browsers don't apply CORS to WebSockets, so set this for any socket that relies on cookies,
    /// or a malicious page can open it with the user's credentials (cross-site WebSocket hijacking).
    public var allowedOrigins: [String]?
    /// Supported subprotocols in preference order (`Sec-WebSocket-Protocol`). Empty means none.
    public var protocols: [String] = []
    /// How often the server pings. A peer silent for two intervals is disconnected. `nil` disables it.
    public var pingInterval: TimeAmount? = .seconds(30)
    /// How long to wait for the client's close reply after the server closes.
    public var closeTimeout: TimeAmount = .seconds(5)
    /// Messages buffered for the handler before the server stops reading from the socket.
    public var messageBuffer: Int = 64
    /// Messages `enqueue(_:)` may queue for a slow client before it's disconnected.
    public var outboxLimit: Int = 1024

    public init(
        maxFrameSize: Int = 1 << 20, maxMessageSize: Int = 1 << 20, allowedOrigins: [String]? = nil,
        protocols: [String] = [], pingInterval: TimeAmount? = .seconds(30), closeTimeout: TimeAmount = .seconds(5),
        messageBuffer: Int = 64, outboxLimit: Int = 1024
    ) {
        self.outboxLimit = outboxLimit
        self.maxFrameSize = maxFrameSize
        self.maxMessageSize = maxMessageSize
        self.allowedOrigins = allowedOrigins
        self.protocols = protocols
        self.pingInterval = pingInterval
        self.closeTimeout = closeTimeout
        self.messageBuffer = messageBuffer
    }
}

extension Router {
    /// Registers a WebSocket endpoint.
    ///
    /// ```swift
    /// app.ws("/echo") { req, ws in
    ///     for await message in ws.messages {
    ///         if case .text(let text) = message { try await ws.send(text) }
    ///     }
    /// }
    /// ```
    ///
    /// Global and route middleware run before the upgrade. If a middleware sends a response
    /// (e.g. 401) instead of calling `next()`, the client gets that response and no socket opens.
    @discardableResult
    public func ws(
        _ path: String, middleware: [Middleware] = [], options: WebSocketOptions = .init(),
        handler: @escaping WebSocketHandler
    ) -> Self {
        add(.websocket(path: path, middleware, options, handler))
        return self
    }

    @discardableResult
    public func ws(
        _ path: String, _ m1: @escaping Middleware, options: WebSocketOptions = .init(),
        handler: @escaping WebSocketHandler
    ) -> Self {
        ws(path, middleware: [m1], options: options, handler: handler)
    }

    @discardableResult
    public func ws(
        _ path: String, _ m1: @escaping Middleware, _ m2: @escaping Middleware, options: WebSocketOptions = .init(),
        handler: @escaping WebSocketHandler
    ) -> Self {
        ws(path, middleware: [m1, m2], options: options, handler: handler)
    }
}

/// An open WebSocket connection.
public final class WebSocket: Sendable {
    public enum Message: Sendable, Equatable {
        case text(String)
        case binary(ByteBuffer)
    }

    public struct ClosedError: Error {}

    /// Incoming messages. Ends when the connection closes. Iterate from one task only.
    public var messages: Messages { Messages(queue: queue) }
    /// The negotiated subprotocol, if any.
    public let subprotocol: String?

    private let outbound: NIOAsyncChannelOutboundWriter<WebSocketFrame>
    private let allocator: ByteBufferAllocator
    private let channel: Channel
    let queue: MessageQueue
    private let outbox: NIOLockedValueBox<(frames: [WebSocketFrame], writing: Bool)>
    private let outboxLimit: Int
    private let closeSent = ManagedAtomic(false)
    let lastActivity = ManagedAtomic<Int64>(WebSocket.now())

    init(
        channel: Channel, outbound: NIOAsyncChannelOutboundWriter<WebSocketFrame>, subprotocol: String?,
        bufferSize: Int, outboxLimit: Int = 1024
    ) {
        self.outbox = NIOLockedValueBox(([], false))
        self.outboxLimit = max(1, outboxLimit)
        self.channel = channel
        self.outbound = outbound
        self.allocator = channel.allocator
        self.subprotocol = subprotocol
        self.queue = MessageQueue(capacity: max(1, bufferSize))
    }

    /// True once either side has started the close handshake.
    public var isClosed: Bool { closeSent.load(ordering: .relaxed) }

    public func send(_ text: String) async throws {
        try await send(frame: WebSocketFrame(fin: true, opcode: .text, data: allocator.buffer(string: text)))
    }

    public func send(_ buffer: ByteBuffer) async throws {
        try await send(frame: WebSocketFrame(fin: true, opcode: .binary, data: buffer))
    }

    public func send(bytes: some Sequence<UInt8>) async throws {
        try await send(allocator.buffer(bytes: bytes))
    }

    /// Encodes a value as JSON and sends it as a text message.
    public func send<T: Encodable>(json value: T, encoder: JSONEncoder = Response.defaultEncoder) async throws {
        let data = try encoder.encode(value)
        try await send(frame: WebSocketFrame(fin: true, opcode: .text, data: allocator.buffer(bytes: data)))
    }

    // MARK: Non-blocking sends (fan-out)

    /// Queues a text message without waiting. Use this when one task sends to many sockets (chat
    /// rooms, live feeds): a slow client never stalls the sender. If a client falls more than
    /// `outboxLimit` messages behind, it's disconnected. Returns false if the message was
    /// not queued (socket closed or too slow).
    @discardableResult
    public func enqueue(_ text: String) -> Bool {
        enqueue(frame: WebSocketFrame(fin: true, opcode: .text, data: allocator.buffer(string: text)))
    }

    /// Queues a pre-encoded message. Encode once and enqueue the same buffer to many sockets: the
    /// storage is shared, not copied.
    @discardableResult
    public func enqueue(_ buffer: ByteBuffer, binary: Bool = false) -> Bool {
        enqueue(frame: WebSocketFrame(fin: true, opcode: binary ? .binary : .text, data: buffer))
    }

    private func enqueue(frame: WebSocketFrame) -> Bool {
        guard !isClosed else { return false }
        enum Outcome { case queued, startWriter, overflow }
        let outcome: Outcome = outbox.withLockedValue { box in
            if box.frames.count >= outboxLimit { return .overflow }
            box.frames.append(frame)
            if box.writing { return .queued }
            box.writing = true
            return .startWriter
        }
        switch outcome {
        case .queued:
            return true
        case .startWriter:
            Task { await drainOutbox() }
            return true
        case .overflow:
            // The pipe to this client is clogged, so a close frame would queue behind everything
            // else: drop the connection outright.
            closeSent.store(true, ordering: .releasing)
            channel.close(promise: nil)
            return false
        }
    }

    private func drainOutbox() async {
        while true {
            let batch: [WebSocketFrame] = outbox.withLockedValue { box in
                let frames = box.frames
                box.frames.removeAll(keepingCapacity: true)
                if frames.isEmpty { box.writing = false }
                return frames
            }
            guard !batch.isEmpty else { return }
            guard !isClosed else { continue }  // drop queued frames once closing
            do {
                try await outbound.write(contentsOf: batch)
            } catch {
                outbox.withLockedValue { $0 = ([], false) }
                return
            }
        }
    }

    /// Starts the close handshake. Safe to call more than once.
    public func close(code: WebSocketErrorCode = .normalClosure, reason: String = "") async {
        guard closeSent.compareExchange(expected: false, desired: true, ordering: .acquiringAndReleasing).exchanged
        else { return }
        var data = allocator.buffer(capacity: 2 + reason.utf8.count)
        data.write(webSocketErrorCode: code)
        data.writeString(String(reason.utf8.prefix(123)) ?? "")  // control frames are capped at 125 bytes
        try? await outbound.write(WebSocketFrame(fin: true, opcode: .connectionClose, data: data))
    }

    private func send(frame: WebSocketFrame) async throws {
        guard !isClosed else { throw ClosedError() }
        try await outbound.write(frame)
    }

    func sendControl(_ opcode: WebSocketOpcode, data: ByteBuffer) async {
        guard !isClosed else { return }
        try? await outbound.write(WebSocketFrame(fin: true, opcode: opcode, data: data))
    }

    static func now() -> Int64 { Int64(DispatchTime.now().uptimeNanoseconds) }

    // MARK: Messages sequence

    public struct Messages: AsyncSequence, Sendable {
        public typealias Element = Message
        let queue: MessageQueue

        public struct AsyncIterator: AsyncIteratorProtocol {
            let queue: MessageQueue
            public mutating func next() async -> Message? { await queue.next() }
        }

        public func makeAsyncIterator() -> AsyncIterator { AsyncIterator(queue: queue) }
    }

    // MARK: Connection lifecycle

    private enum Part: Sendable { case reader, handler, pinger, closeTimeout }

    /// Runs the handler, the frame reader and the keep-alive pinger until the connection ends.
    static func run(
        _ asyncChannel: NIOAsyncChannel<WebSocketFrame, WebSocketFrame>, request: Request,
        route: CompiledRouter.WebSocketRoute, subprotocol: String?,
        onOpen: (WebSocket) -> Void = { _ in },
        beforeClose: @Sendable () async -> Void = {}
    ) async {
        let options = route.options
        try? await asyncChannel.executeThenClose { inbound, outbound in
            let socket = WebSocket(
                channel: asyncChannel.channel, outbound: outbound, subprotocol: subprotocol,
                bufferSize: options.messageBuffer, outboxLimit: options.outboxLimit
            )
            onOpen(socket)
            await withTaskGroup(of: Part.self) { group in
                group.addTask {
                    await socket.readLoop(inbound)
                    return .reader
                }
                group.addTask {
                    do {
                        try await route.handler(request, socket)
                        await socket.close(code: .normalClosure)
                    } catch is CancellationError {
                        await socket.close(code: .goingAway)
                    } catch {
                        FileHandle.standardError.write(Data("[oria] websocket handler error: \(error)\n".utf8))
                        await socket.close(code: .unexpectedServerError)
                    }
                    return .handler
                }
                if let interval = options.pingInterval {
                    group.addTask {
                        await socket.pingLoop(every: interval)
                        return .pinger
                    }
                }
                while let finished = await group.next() {
                    switch finished {
                    case .reader:
                        // Peer closed or the connection broke: stop the handler and the pinger.
                        group.cancelAll()
                    case .handler:
                        // Give the client a moment to answer our close frame, then hang up.
                        group.addTask {
                            try? await Task.sleep(nanoseconds: UInt64(max(0, options.closeTimeout.nanoseconds)))
                            return .closeTimeout
                        }
                    case .closeTimeout:
                        socket.queue.finish()
                        try? await asyncChannel.channel.close()
                        group.cancelAll()
                    case .pinger:
                        break
                    }
                }
            }
            await beforeClose()  // make sure our close frame actually leaves before the socket closes
        }
    }

    private func readLoop(_ inbound: NIOAsyncChannelInboundStream<WebSocketFrame>) async {
        defer { queue.finish() }
        do {
            for try await frame in inbound {
                lastActivity.store(Self.now(), ordering: .relaxed)
                switch frame.opcode {
                case .text:
                    let data = frame.unmaskedData
                    guard let text = String(bytes: data.readableBytesView, encoding: .utf8) else {
                        await close(code: .dataInconsistentWithMessage, reason: "invalid UTF-8")
                        return
                    }
                    await queue.push(.text(text))
                case .binary:
                    await queue.push(.binary(frame.unmaskedData))
                case .ping:
                    await sendControl(.pong, data: frame.unmaskedData)
                case .pong:
                    break
                case .connectionClose:
                    var data = frame.unmaskedData
                    let code = data.readWebSocketErrorCode() ?? .normalClosure
                    await close(code: code)
                    return
                default:
                    await close(code: .protocolError)
                    return
                }
            }
        } catch let error as NIOWebSocketFrameAggregator.Error {
            switch error {
            case .accumulatedFrameSizeIsTooLarge, .tooManyFragments:
                await close(code: .messageTooLarge)
            default:
                await close(code: .protocolError)
            }
        } catch {
            // Oversized frames are answered with a close frame by NIO's protocol error handler.
        }
    }

    private func pingLoop(every interval: TimeAmount) async {
        let nanos = max(1, interval.nanoseconds)
        while !Task.isCancelled && !isClosed {
            do { try await Task.sleep(nanoseconds: UInt64(nanos)) } catch { return }
            if Self.now() - lastActivity.load(ordering: .relaxed) > 2 * nanos {
                channel.close(promise: nil)  // Dead peer: no frames (not even pongs) for two intervals.
                return
            }
            await sendControl(.ping, data: allocator.buffer(capacity: 0))
        }
    }
}

/// Thread-safe rooms of WebSockets with non-blocking broadcast.
///
/// ```swift
/// let hub = WebSocketHub()
/// app.ws("/chat/:room") { req, ws in
///     let room = req.params["room"]!
///     hub.join(room, ws)
///     defer { hub.leave(room, ws) }
///     for await case .text(let text) in ws.messages { hub.broadcast(text, to: room) }
/// }
/// ```
///
/// `broadcast` encodes the message once and queues it on every member (see `WebSocket.enqueue`),
/// so it returns immediately and one slow client can't delay the rest.
public final class WebSocketHub: Sendable {
    private let rooms = NIOLockedValueBox<[String: [ObjectIdentifier: WebSocket]]>([:])
    private let allocator = ByteBufferAllocator()

    public init() {}

    public func join(_ room: String, _ socket: WebSocket) {
        rooms.withLockedValue { $0[room, default: [:]][ObjectIdentifier(socket)] = socket }
    }

    public func leave(_ room: String, _ socket: WebSocket) {
        rooms.withLockedValue { rooms in
            rooms[room]?[ObjectIdentifier(socket)] = nil
            if rooms[room]?.isEmpty == true { rooms[room] = nil }
        }
    }

    public func members(of room: String) -> [WebSocket] {
        rooms.withLockedValue { Array(($0[room] ?? [:]).values) }
    }

    public func count(in room: String) -> Int {
        rooms.withLockedValue { $0[room]?.count ?? 0 }
    }

    /// Sends a text message to every member. Returns how many sockets accepted it.
    @discardableResult
    public func broadcast(_ text: String, to room: String) -> Int {
        broadcast(allocator.buffer(string: text), to: room)
    }

    /// Sends a pre-encoded message to every member (`binary: false` sends it as text).
    @discardableResult
    public func broadcast(_ buffer: ByteBuffer, to room: String, binary: Bool = false) -> Int {
        var delivered = 0
        for member in members(of: room) where member.enqueue(buffer, binary: binary) {
            delivered += 1
        }
        return delivered
    }
}

/// Validates raw client frames before reassembly: every frame must be masked (RFC 6455 §5.1), and no
/// frame may exceed the route's message limit (the decoder's limit is shared by all routes).
final class WebSocketFrameGuard: ChannelInboundHandler, Sendable {
    typealias InboundIn = WebSocketFrame
    typealias OutboundOut = WebSocketFrame

    let maxMessageSize: Int

    init(maxMessageSize: Int) { self.maxMessageSize = maxMessageSize }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let frame = unwrapInboundIn(data)
        if frame.maskKey == nil {
            reject(context, .protocolError)
        } else if frame.length > maxMessageSize {
            reject(context, .messageTooLarge)
        } else {
            context.fireChannelRead(data)
        }
    }

    private func reject(_ context: ChannelHandlerContext, _ code: WebSocketErrorCode) {
        var payload = context.channel.allocator.buffer(capacity: 2)
        payload.write(webSocketErrorCode: code)
        let close = WebSocketFrame(fin: true, opcode: .connectionClose, data: payload)
        context.writeAndFlush(wrapOutboundOut(close)).whenComplete { _ in context.close(promise: nil) }
    }
}

/// A bounded single-consumer queue. A full queue suspends the reader, which stops reading the
/// socket, so a client can't exhaust server memory by sending faster than the handler consumes.
final class MessageQueue: Sendable {
    private struct State {
        var buffer: [WebSocket.Message] = []
        var head = 0
        var finished = false
        var consumer: CheckedContinuation<WebSocket.Message?, Never>?
        var producer: CheckedContinuation<Void, Never>?
        var count: Int { buffer.count - head }
    }

    private let capacity: Int
    private let state = NIOLockedValueBox(State())

    init(capacity: Int) { self.capacity = capacity }

    func push(_ message: WebSocket.Message) async {
        await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
            let resume: (() -> Void)? = state.withLockedValue { s in
                if s.finished { return { cont.resume() } }
                if let consumer = s.consumer {
                    s.consumer = nil
                    return {
                        consumer.resume(returning: message)
                        cont.resume()
                    }
                }
                s.buffer.append(message)
                if s.count < capacity { return { cont.resume() } }
                s.producer = cont  // Full: resume when the consumer makes room.
                return nil
            }
            resume?()
        }
    }

    func next() async -> WebSocket.Message? {
        await withCheckedContinuation { (cont: CheckedContinuation<WebSocket.Message?, Never>) in
            let resume: () -> Void = state.withLockedValue { s in
                if s.count > 0 {
                    let message = s.buffer[s.head]
                    s.head += 1
                    if s.head > 64 && s.head * 2 > s.buffer.count {
                        s.buffer.removeFirst(s.head)
                        s.head = 0
                    }
                    let producer = s.producer
                    s.producer = nil
                    return {
                        producer?.resume()
                        cont.resume(returning: message)
                    }
                }
                if s.finished { return { cont.resume(returning: nil) } }
                precondition(s.consumer == nil, "WebSocket.messages must be iterated from a single task")
                s.consumer = cont
                return {}
            }
            resume()
        }
    }

    func finish() {
        let (consumer, producer) = state.withLockedValue { s -> (CheckedContinuation<WebSocket.Message?, Never>?, CheckedContinuation<Void, Never>?) in
            s.finished = true
            defer {
                s.consumer = nil
                s.producer = nil
            }
            return (s.consumer, s.producer)
        }
        consumer?.resume(returning: nil)
        producer?.resume()
    }
}
