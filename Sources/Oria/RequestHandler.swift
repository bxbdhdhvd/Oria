import Atomics
import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOFileSystem
import NIOHTTP1

// MARK: - Running tasks on the event loop

/// A Swift Concurrency `TaskExecutor` backed by a NIO event loop.
///
/// Request handlers start with this executor as their preference, so a request that doesn't hop to
/// another actor runs start to finish on the thread that owns its socket: no thread switches between
/// reading the request, running middleware and writing the response.
final class EventLoopExecutor: TaskExecutor, @unchecked Sendable {
    let eventLoop: EventLoop

    init(_ eventLoop: EventLoop) { self.eventLoop = eventLoop }

    func enqueue(_ job: consuming ExecutorJob) {
        let job = UnownedJob(job)
        eventLoop.execute { job.runSynchronously(on: self.asUnownedTaskExecutor()) }
    }
}

/// The `Date` header for one event loop, formatted at most once per second. Only touched from its
/// own loop, so it needs no lock and no thread-local lookup.
final class LoopDateCache: @unchecked Sendable {
    @exclusivity(unchecked) private var seconds: time_t = -1
    @exclusivity(unchecked) private var value = ""

    func now() -> String {
        let current = time(nil)
        if current != seconds {
            seconds = current
            value = HTTPDate.format(current)
        }
        return value
    }
}

/// Per-event-loop helpers, built once at startup (lock-free lookups afterwards).
struct LoopExecutors: Sendable {
    private let map: [ObjectIdentifier: (EventLoopExecutor?, LoopDateCache)]

    init(group: any EventLoopGroup, enabled: Bool) {
        var map: [ObjectIdentifier: (EventLoopExecutor?, LoopDateCache)] = [:]
        for loop in group.makeIterator() {
            map[ObjectIdentifier(loop)] = (enabled ? EventLoopExecutor(loop) : nil, LoopDateCache())
        }
        self.map = map
    }

    func executor(for loop: EventLoop) -> EventLoopExecutor? { map[ObjectIdentifier(loop)]?.0 }
    func dateCache(for loop: EventLoop) -> LoopDateCache { map[ObjectIdentifier(loop)]?.1 ?? LoopDateCache() }
}

/// Everything a request handler needs, shared by all connections of a server. A class, so passing
/// it around costs one reference count instead of one per member; hot settings are copied out of
/// `Configuration` into plain stored properties.
final class HandlerEnvironment: Sendable {
    let app: Oria
    let router: CompiledRouter
    let config: Oria.Configuration
    let shuttingDown: ManagedAtomic<Bool>
    let executors: LoopExecutors
    let maxBodySize: Int
    let requestReadTimeout: TimeAmount?
    let trustProxy: Bool
    let isTLS: Bool
    let serverName: String?

    init(app: Oria, router: CompiledRouter, config: Oria.Configuration, shuttingDown: ManagedAtomic<Bool>, executors: LoopExecutors) {
        self.app = app
        self.router = router
        self.config = config
        self.shuttingDown = shuttingDown
        self.executors = executors
        self.maxBodySize = config.maxBodySize
        self.requestReadTimeout = config.requestReadTimeout
        self.trustProxy = config.trustProxy
        self.isTLS = config.tls != nil
        self.serverName = config.serverName
    }

    var isShuttingDown: Bool { shuttingDown.load(ordering: .relaxed) }
}

/// Upload state, boxed in a class so the handler's `Phase` enum has no generic payload (copying
/// an enum with a generic payload makes the runtime look up type metadata on every access).
final class UploadState {
    typealias Sink = NIOThrowingAsyncSequenceProducer<
        ByteBuffer, any Error, NIOAsyncSequenceProducerBackPressureStrategies.HighLowWatermark, UploadDelegate
    >.Source

    let head: HTTPRequestHead
    let sink: Sink
    let limit: Int
    let idleTimeout: TimeAmount
    var received = 0

    init(head: HTTPRequestHead, sink: Sink, limit: Int, idleTimeout: TimeAmount) {
        self.head = head
        self.sink = sink
        self.limit = limit
        self.idleTimeout = idleTimeout
    }
}

// MARK: - The request handler

/// Serves HTTP requests on an HTTP/1.1 connection (keep-alive, one request at a time; NIO's
/// pipelining handler queues pipelined requests) or on a single HTTP/2 stream.
///
/// Everything here runs on the channel's event loop. The app handler runs in a task that prefers
/// the same event loop, and results come back through `onLoop`.
final class HTTPRequestHandler: ChannelDuplexHandler, RemovableChannelHandler, @unchecked Sendable {
    typealias InboundIn = HTTPServerRequestPart
    typealias OutboundIn = HTTPServerResponsePart
    typealias OutboundOut = HTTPServerResponsePart

    enum Mode {
        case http1(rejection: Response?, overCapacity: Bool)
        case http2(parent: Channel)
    }

    private enum Phase {
        case idle
        case buffering(HTTPRequestHead, ByteBuffer?, limit: Int)
        case uploading(UploadState)
        case processing
        case closing
    }

    private let env: HandlerEnvironment
    private let mode: Mode
    private let state: ConnectionState
    private let executor: EventLoopExecutor?
    private let dates: LoopDateCache
    private let eventLoop: EventLoop
    private let channel: Channel
    // Event-loop-confined state. Dynamic exclusivity checks are disabled for these hot fields:
    // they're only ever touched from this channel's event loop, one access at a time.
    @exclusivity(unchecked) private var context: ChannelHandlerContext?
    @exclusivity(unchecked) private var phase = Phase.idle
    /// Whether the current request's `.end` has been read (false for early 413/503 answers).
    @exclusivity(unchecked) private var requestComplete = false
    /// True once any part of a response for the current request was written.
    @exclusivity(unchecked) private var responseStarted = false
    @exclusivity(unchecked) private var writeStallDeadline: NIODeadline?
    // Flush coalescing: responses produced while the channel is delivering reads (the usual case
    // with Task.immediate) are flushed once at channelReadComplete, so a batch of pipelined requests
    // costs one write syscall instead of one each.
    @exclusivity(unchecked) private var inReadCycle = false
    @exclusivity(unchecked) private var flushPending = false

    // Read deadline: one checker per channel; arming is a plain store (no NIO task per request).
    @exclusivity(unchecked) private var deadline: NIODeadline?
    @exclusivity(unchecked) private var checker: Scheduled<Void>?
    private var checkInterval: TimeAmount

    // Upload backpressure.
    @exclusivity(unchecked) private var readsPaused = false
    @exclusivity(unchecked) private var pendingRead = false

    /// HTTP/1.1 pipelining: parts that arrive while a response is pending wait here (reads stay
    /// paused meanwhile, so this holds at most what one socket read decoded).
    private enum Queued {
        case part(HTTPServerRequestPart)
        case error(any Error)
    }
    @exclusivity(unchecked) private var queue = CircularBuffer<Queued>(initialCapacity: 0)
    @exclusivity(unchecked) private var draining = false
    private static let maxQueuedParts = 4096

    /// Plain HTTP/1.1 without compression: responses are serialized by `HTTP1Writer` and written
    /// as raw bytes (NIO's encoder and pipelining handler are not in the pipeline).
    private let rawOutput: Bool

    private var isHTTP2: Bool { if case .http2 = mode { return true } else { return false } }

    init(env: HandlerEnvironment, mode: Mode, state: ConnectionState, channel: Channel) {
        self.env = env
        self.channel = channel
        self.eventLoop = channel.eventLoop
        let executor = env.executors.executor(for: channel.eventLoop)
        self.mode = mode
        self.state = state
        self.executor = executor
        self.dates = env.executors.dateCache(for: channel.eventLoop)
        self.checkInterval = env.requestReadTimeout ?? .seconds(30)
        if case .http1 = mode { self.rawOutput = !env.config.compression } else { self.rawOutput = false }
    }

    // MARK: Lifecycle

    func handlerAdded(context: ChannelHandlerContext) {
        self.context = context
        if let timeout = env.requestReadTimeout {
            deadline = .now() + timeout
            scheduleCheck(in: timeout)
        }
        switch mode {
        case .http1(let rejection, _):
            if let rejection {
                // A WebSocket upgrade refused by middleware/origin check. NIO consumed the request.
                context.eventLoop.execute {
                    let head = HTTPRequestHead(version: .http1_1, method: .GET, uri: "/")
                    self.phase = .processing
                    self.write(rejection, for: head, keepAlive: false)
                }
            }
        case .http2(let parent):
            state.beginRequest()
            let state = self.state
            let shuttingDown = env.shuttingDown
            context.channel.closeFuture.whenComplete { _ in
                if state.endRequest() == 0 && shuttingDown.load(ordering: .relaxed) { parent.close(promise: nil) }
            }
        }
    }

    func handlerRemoved(context: ChannelHandlerContext) {
        checker?.cancel()
        checker = nil
        self.context = nil
    }

    func channelInactive(context: ChannelHandlerContext) {
        checker?.cancel()
        checker = nil
        if case .uploading(let upload) = phase {
            upload.sink.finish(ChannelError.eof)
        }
        phase = .closing
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        // A parse error in a pipelined request answers after the responses before it.
        if !isHTTP2, error is HTTPParserError, mustQueue {
            queue.append(.error(error))
            return
        }
        handleError(error, context: context)
    }

    private func handleError(_ error: any Error, context: ChannelHandlerContext) {
        if case .uploading(let upload) = phase { upload.sink.finish(error) }
        // Malformed HTTP: answer 400 (431 for oversized headers) unless a response is already under
        // way, then close. HTTP/2 framing errors are handled by NIOHTTP2.
        // The peer hanging up mid-request (`invalidEOFState`) gets no answer: nobody is listening.
        if !isHTTP2, let parseError = error as? HTTPParserError, !responseStarted, parseError != .invalidEOFState {
            let status: HTTPResponseStatus
            switch parseError {
            case .headerOverflow: status = .requestHeaderFieldsTooLarge
            case .invalidURL: status = .uriTooLong
            default: status = .badRequest
            }
            responseStarted = true
            writeSimple(status, version: .http1_1, close: true)
            return
        }
        phase = .closing
        context.close(promise: nil)
    }

    /// HTTP/1.1 only: a request arriving while the previous response is still pending waits.
    private var mustQueue: Bool {
        if !queue.isEmpty { return true }
        switch phase {
        case .processing, .closing: return true
        case .idle, .buffering, .uploading: return false
        }
    }

    // MARK: Write stalls

    /// A client that stops reading responses (slow-read attack) makes the channel unwritable;
    /// if it stays that way past `idleTimeout`, the connection is closed.
    func channelWritabilityChanged(context: ChannelHandlerContext) {
        if context.channel.isWritable {
            writeStallDeadline = nil
        } else if let idle = env.config.idleTimeout, writeStallDeadline == nil {
            writeStallDeadline = .now() + idle
            // The periodic check may be up to `requestReadTimeout` away: re-arm it for the stall
            // deadline (writability flips are rare, so this stays off the per-request path).
            checker?.cancel()
            scheduleCheck(in: idle)
        }
        context.fireChannelWritabilityChanged()
    }

    // MARK: Reading

    func channelReadComplete(context: ChannelHandlerContext) {
        inReadCycle = false
        if flushPending {
            flushPending = false
            context.flush()
        }
        context.fireChannelReadComplete()
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        inReadCycle = true
        let part = unwrapInboundIn(data)
        if !isHTTP2 && mustQueue {
            if case .closing = phase { return }  // connection is closing: drop what follows
            guard queue.count < Self.maxQueuedParts else {
                closeNow()
                return
            }
            queue.append(.part(part))
            return
        }
        handle(part, context: context)
    }

    private func handle(_ part: HTTPServerRequestPart, context: ChannelHandlerContext) {
        switch part {
        case .head(let head): begin(head, context: context)
        case .body(let chunk): body(chunk, context: context)
        case .end: end(context: context)
        }
    }

    /// Feeds queued pipelined parts to the state machine until a request is pending again.
    private func drainQueue() {
        guard !draining, let context else { return }
        draining = true
        defer { draining = false }
        while !queue.isEmpty && !readsPaused {
            switch phase {
            case .processing, .closing:
                return
            case .idle, .buffering, .uploading:
                switch queue.removeFirst() {
                case .part(let part): handle(part, context: context)
                case .error(let error): handleError(error, context: context)
                }
            }
        }
        if queue.isEmpty, pendingRead, !readsPaused, !mustQueue {
            pendingRead = false
            context.read()
        }
    }

    private func begin(_ head: HTTPRequestHead, context: ChannelHandlerContext) {
        guard case .idle = phase else {
            phase = .closing
            context.close(promise: nil)
            return
        }
        if !isHTTP2 { state.beginRequest() }
        requestComplete = false
        responseStarted = false

        if case .http1(_, let overCapacity) = mode, overCapacity {
            writeSimple(.serviceUnavailable, version: head.version, close: true)
            return
        }

        // Which body policy applies? Upload routes stream to disk with their own limits.
        let upload = env.router.hasUploadRoutes ? env.router.uploadOptions(method: head.method, uri: head.uri) : nil
        let limit = upload?.maxBodySize ?? env.maxBodySize
        if let length = head.headers.first(name: "content-length").flatMap(Int.init), length > limit {
            writeSimple(.payloadTooLarge, version: head.version, close: true)
            return
        }

        // curl and other clients wait for "100 Continue" before sending large bodies.
        if let expect = head.headers.first(name: "expect") {
            guard expect.lowercased() == "100-continue" else {
                writeSimple(.expectationFailed, version: head.version, close: true)
                return
            }
            if head.version != .http1_0 {
                if rawOutput {
                    var buffer = context.channel.allocator.buffer(capacity: 32)
                    HTTP1Writer.writeContinue(into: &buffer)
                    context.writeAndFlush(NIOAny(buffer), promise: nil)
                } else {
                    let interim = HTTPResponseHead(version: head.version, status: .continue)
                    context.writeAndFlush(wrapOutboundOut(.head(interim)), promise: nil)
                }
            }
        }

        if let upload {
            startUpload(head, options: upload, limit: limit, context: context)
        } else {
            phase = .buffering(head, nil, limit: limit)
        }
    }

    private func body(_ chunk: ByteBuffer, context: ChannelHandlerContext) {
        switch phase {
        case .buffering(let head, var body, let limit):
            phase = .processing  // drop our reference so `body` stays uniquely owned (no CoW copy)
            var chunk = chunk
            if body == nil {
                body = chunk
            } else {
                body!.writeBuffer(&chunk)
            }
            if body!.readableBytes > limit {
                writeSimple(.payloadTooLarge, version: head.version, close: true)
                return
            }
            phase = .buffering(head, body, limit: limit)

        case .uploading(let upload):
            upload.received += chunk.readableBytes
            if upload.received > upload.limit {
                phase = .processing
                upload.sink.finish(MultipartError.tooLarge("Request body too large"))
                return
            }
            deadline = .now() + upload.idleTimeout
            if case .stopProducing = upload.sink.yield(chunk) { readsPaused = true }

        default:
            break  // e.g. the rest of a body we already answered with 413
        }
    }

    private func end(context: ChannelHandlerContext) {
        switch phase {
        case .buffering(let head, let body, _):
            deadline = nil
            requestComplete = true
            phase = .processing
            dispatch(head, body: body, context: context)
        case .uploading(let upload):
            deadline = nil
            requestComplete = true
            phase = .processing
            upload.sink.finish()
        default:
            break
        }
    }

    // MARK: Upload backpressure

    func read(context: ChannelHandlerContext) {
        // HTTP/1.1: no more reads while a response is pending (like NIO's pipelining handler), so
        // a pipelining client can't make us buffer unbounded requests.
        if readsPaused || (!isHTTP2 && mustQueue) {
            pendingRead = true
        } else {
            context.read()
        }
    }

    fileprivate func resumeReading() {
        readsPaused = false
        if !queue.isEmpty {
            drainQueue()
            return
        }
        if pendingRead, !(!isHTTP2 && mustQueue), let context {
            pendingRead = false
            context.read()
        }
    }

    // MARK: Running the app

    /// Starts the request task. With the event-loop executor, the task starts *immediately* on the
    /// current thread (Swift 6.2 `Task.immediate`), so a handler that doesn't suspend writes its
    /// response within the same event-loop tick that read the request, like hand-written NIO code.
    /// If it suspends (database call, actor hop), it resumes on this connection's event loop.
    private func spawn(_ operation: sending @escaping @isolated(any) () async -> Void) {
        guard let executor else {
            Task(operation: operation)
            return
        }
        #if compiler(>=6.2)
        if #available(macOS 26.0, iOS 26.0, tvOS 26.0, watchOS 26.0, visionOS 26.0, *), eventLoop.inEventLoop {
            // Starts right here on the event loop. No executor preference: maintaining one costs
            // a locked status-record lookup on every task switch. A handler that suspends resumes
            // on the shared pool, and its response hops back to this loop once (`onLoop`).
            Task.immediate(operation: operation)
            return
        }
        #endif
        Task(executorPreference: executor, operation: operation)
    }

    /// Runs `body` on the event loop: inline if we're already there (the common case with the
    /// event-loop executor), otherwise via one hop.
    private func onLoop(_ body: @escaping @Sendable () -> Void) {
        if eventLoop.inEventLoop { body() } else { eventLoop.execute(body) }
    }

    private func makeRequest(_ head: HTTPRequestHead, body: ByteBuffer?, context: ChannelHandlerContext) -> Request {
        let remote: SocketAddress?
        switch mode {
        case .http1: remote = context.channel.remoteAddress
        case .http2(let parent): remote = parent.remoteAddress
        }
        let req = Request(head: head, body: body, remoteAddress: remote, trustProxy: env.trustProxy)
        if env.isTLS { req.isSecure = true }
        return req
    }

    private func dispatch(_ head: HTTPRequestHead, body: ByteBuffer?, context: ChannelHandlerContext) {
        let req = makeRequest(head, body: body, context: context)
        let res = Response(allocator: context.channel.allocator)
        let env = self.env
        spawn {
            await env.app.handle(req, res, router: env.router)
            await self.send(res, for: head)
        }
    }

    // MARK: Uploads

    private func startUpload(_ head: HTTPRequestHead, options: UploadOptions, limit: Int, context: ChannelHandlerContext) {
        let delegate = UploadDelegate(handler: self, eventLoop: context.eventLoop)
        let producer = NIOThrowingAsyncSequenceProducer.makeSequence(
            elementType: ByteBuffer.self,
            failureType: (any Error).self,
            backPressureStrategy: NIOAsyncSequenceProducerBackPressureStrategies.HighLowWatermark(
                lowWatermark: 2, highWatermark: 8
            ),
            finishOnDeinit: true,
            delegate: delegate
        )
        deadline = .now() + options.idleTimeout
        phase = .uploading(UploadState(head: head, sink: producer.source, limit: limit, idleTimeout: options.idleTimeout))

        let request = makeRequest(head, body: nil, context: context)
        let allocator = context.channel.allocator
        let env = self.env
        let contentType = head.headers.first(name: "content-type")
        let body = producer.sequence
        spawn {
            var temporaryFiles: [String] = []
            do {
                let received = try await UploadReceiver.receive(
                    body, contentType: contentType, options: options, temporaryFiles: &temporaryFiles
                )
                request.uploads = received.form
                request.uploadedBody = received.raw
                let res = Response(allocator: allocator)
                await env.app.handle(request, res, router: env.router)
                await self.send(res, for: head)
            } catch let error as MultipartError {
                await self.sendError(error.httpError, for: head)
            } catch {
                // The client went away or timed out mid-upload: nothing to answer.
                self.onLoop { self.closeNow() }
            }
            for path in temporaryFiles {
                try? await FileSystem.shared.removeItem(at: FilePath(path), strategy: .platformDefault)
            }
        }
    }

    // MARK: Writing

    /// Called from the request task once the app produced a response.
    private func send(_ res: Response, for head: HTTPRequestHead) async {
        if case .stream(let length, let producer) = res.body {
            await sendStream(res, length: length, producer: producer, for: head)
        } else {
            onLoop { self.write(res, for: head, keepAlive: self.keepAlive(for: head, res: res)) }
        }
    }

    private func sendError(_ error: HTTPError, for head: HTTPRequestHead) async {
        onLoop {
            let res = Response(allocator: self.channel.allocator)
            res.status(error.status)
            try? res.json(["error": error.message])
            self.write(res, for: head, keepAlive: false)
        }
    }

    private func keepAlive(for head: HTTPRequestHead, res: Response) -> Bool {
        guard !isHTTP2 else { return true }
        if env.isShuttingDown || !head.isKeepAlive { return false }
        if case .stream(nil, _) = res.body, head.version.major == 1 && head.version.minor == 0 {
            return false  // HTTP/1.0 has no chunked encoding; the close delimits the body.
        }
        return true
    }

    private struct PreparedHead {
        var head: HTTPResponseHead
        var omitBody: Bool
    }

    private func prepare(_ res: Response, for request: HTTPRequestHead, keepAlive: Bool, length: Int?) -> PreparedHead? {
        guard
            var headers = Server.prepareHeaders(
                res, for: request, keepAlive: keepAlive, http2: isHTTP2, serverName: env.serverName
            )
        else { return nil }
        let code = res.statusCode.code
        let statusForbidsBody = code == 204 || code == 304 || (100..<200).contains(code)
        if !statusForbidsBody, let length { headers.replaceOrAdd(name: "content-length", value: String(length)) }
        let head = HTTPResponseHead(version: request.version, status: res.statusCode, headers: headers)
        return PreparedHead(head: head, omitBody: request.method == .HEAD || statusForbidsBody)
    }

    /// Writes a complete buffered response. On the event loop.
    private func write(_ res: Response, for request: HTTPRequestHead, keepAlive: Bool) {
        guard let context else { return }
        var body: ByteBuffer?
        if case .buffer(let buffer) = res.body { body = buffer }
        if rawOutput {
            writeRaw(res, body: body, for: request, keepAlive: keepAlive, context: context)
            return
        }
        guard let prepared = prepare(res, for: request, keepAlive: keepAlive, length: body?.readableBytes ?? 0) else {
            writeSimple(.internalServerError, version: request.version, close: !isHTTP2)
            return
        }
        responseStarted = true
        context.write(wrapOutboundOut(.head(prepared.head)), promise: nil)
        if let body, !prepared.omitBody { context.write(wrapOutboundOut(.body(.byteBuffer(body))), promise: nil) }
        endResponse(keepAlive: keepAlive)
    }

    private static func statusForbidsBody(_ status: HTTPResponseStatus) -> Bool {
        let code = status.code
        return code == 204 || code == 304 || (code >= 100 && code < 200)
    }

    /// HTTP/1.1 fast path: head and small body in one buffer, one write.
    private func writeRaw(
        _ res: Response, body: ByteBuffer?, for request: HTTPRequestHead, keepAlive: Bool, context: ChannelHandlerContext
    ) {
        let forbidsBody = Self.statusForbidsBody(res.statusCode)
        let omitBody = request.method == .HEAD || forbidsBody
        let bodyBytes = body?.readableBytes ?? 0
        let inline = !omitBody && bodyBytes <= HTTP1Writer.coalesceLimit
        var buffer = context.channel.allocator.buffer(capacity: 256 + (inline ? bodyBytes : 0))
        guard
            let head = HTTP1Writer.writeHead(
                into: &buffer, version: request.version, status: res.statusCode, headers: res.headers,
                contentLength: forbidsBody ? nil : bodyBytes, chunked: false, keepAlive: keepAlive,
                serverName: env.serverName, date: dates.now())
        else {
            refuseUnsafeHeaders(version: request.version)
            return
        }
        responseStarted = true
        let keepAlive = keepAlive && !head.closeRequested
        if let body, !omitBody, bodyBytes > 0 {
            if inline {
                buffer.writeImmutableBuffer(body)
            } else {
                context.write(NIOAny(buffer), promise: nil)
                buffer = body
            }
        }
        finish(last: buffer, keepAlive: keepAlive, context: context)
    }

    private func refuseUnsafeHeaders(version: HTTPVersion) {
        FileHandle.standardError.write(Data("[oria] refused to send unsafe response header (CR/LF/NUL)\n".utf8))
        writeSimple(.internalServerError, version: version, close: !isHTTP2)
    }

    private func sendStream(
        _ res: Response, length: Int?, producer: @Sendable (BodyWriter) async throws -> Void, for request: HTTPRequestHead
    ) async {
        if rawOutput {
            await sendStreamRaw(res, length: length, producer: producer, for: request)
            return
        }
        let keepAlive = self.keepAlive(for: request, res: res)
        let channel = self.channel
        guard let prepared = prepare(res, for: request, keepAlive: keepAlive, length: length) else {
            onLoop { self.writeSimple(.internalServerError, version: request.version, close: !self.isHTTP2) }
            return
        }
        onLoop { self.responseStarted = true }
        do {
            try await channel.writeAndFlush(HTTPServerResponsePart.head(prepared.head)).get()
            if !prepared.omitBody {
                // Awaiting each chunk's write is the backpressure (HTTP/2 flow control included).
                try await producer(
                    BodyWriter(allocator: res.allocator) { chunk in
                        try await channel.writeAndFlush(HTTPServerResponsePart.body(.byteBuffer(chunk))).get()
                    })
            }
            onLoop { self.endResponse(keepAlive: keepAlive) }
        } catch {
            onLoop { self.closeNow() }
        }
    }

    /// Bytes written by a streaming producer, checked against the declared length.
    private final class StreamCounter: @unchecked Sendable {
        var written = 0
    }

    private func sendStreamRaw(
        _ res: Response, length: Int?, producer: @Sendable (BodyWriter) async throws -> Void, for request: HTTPRequestHead
    ) async {
        var keepAlive = self.keepAlive(for: request, res: res)
        let forbidsBody = Self.statusForbidsBody(res.statusCode)
        let omitBody = request.method == .HEAD || forbidsBody
        // HTTP/1.0 has no chunked encoding: without a length, closing the connection ends the body.
        let chunked = length == nil && !forbidsBody && (request.version.major > 1 || (request.version.major == 1 && request.version.minor >= 1))
        let allocator = res.allocator
        var buffer = allocator.buffer(capacity: 256)
        guard
            let head = HTTP1Writer.writeHead(
                into: &buffer, version: request.version, status: res.statusCode, headers: res.headers,
                contentLength: forbidsBody ? nil : length, chunked: chunked, keepAlive: keepAlive,
                serverName: env.serverName, date: HTTPDate.now())  // may run off the loop: thread-local cache
        else {
            onLoop { self.refuseUnsafeHeaders(version: request.version) }
            return
        }
        if head.closeRequested { keepAlive = false }
        onLoop { self.responseStarted = true }
        let counter = StreamCounter()
        do {
            try await writeNow([buffer])
            if !omitBody {
                // Awaiting each chunk's write is the backpressure.
                try await producer(
                    BodyWriter(allocator: allocator) { chunk in
                        guard chunk.readableBytes > 0 else { return }
                        counter.written += chunk.readableBytes
                        if let length, counter.written > length {
                            throw HTTPError(.internalServerError, "Streamed more bytes than the declared length")
                        }
                        if chunked {
                            try await self.writeNow([
                                HTTP1Writer.chunkPrefix(chunk.readableBytes, allocator: allocator), chunk,
                                allocator.buffer(staticString: "\r\n"),
                            ])
                        } else {
                            try await self.writeNow([chunk])
                        }
                    })
            }
            // A short body would leave the client waiting for bytes that never come: close instead.
            if let length, !omitBody, counter.written != length { keepAlive = false }
            let finalKeepAlive = keepAlive
            onLoop {
                guard let context = self.context else { return }
                let last = chunked && !omitBody ? allocator.buffer(staticString: "0\r\n\r\n") : allocator.buffer(capacity: 0)
                self.finish(last: last, keepAlive: finalKeepAlive, context: context)
            }
        } catch {
            onLoop { self.closeNow() }
        }
    }

    /// Writes and flushes buffers from a request task; completes when they reached the socket.
    private func writeNow(_ buffers: [ByteBuffer]) async throws {
        let promise = eventLoop.makePromise(of: Void.self)
        onLoop {
            guard let context = self.context else {
                promise.fail(ChannelError.ioOnClosedChannel)
                return
            }
            for buffer in buffers.dropLast() { context.write(NIOAny(buffer), promise: nil) }
            context.writeAndFlush(NIOAny(buffers[buffers.count - 1]), promise: promise)
        }
        try await promise.futureResult.get()
    }

    /// Small canned JSON error. On the event loop.
    private func writeSimple(_ status: HTTPResponseStatus, version: HTTPVersion, close: Bool) {
        guard let context else { return }
        responseStarted = true
        let body = ByteBuffer(string: "{\"error\":\"\(status.reasonPhrase)\"}")
        var headers = HTTPHeaders()
        headers.add(name: "content-type", value: "application/json; charset=utf-8")
        if rawOutput {
            var buffer = context.channel.allocator.buffer(capacity: 256)
            _ = HTTP1Writer.writeHead(
                into: &buffer, version: version, status: status, headers: headers, contentLength: body.readableBytes,
                chunked: false, keepAlive: !close, serverName: env.serverName, date: dates.now())
            buffer.writeImmutableBuffer(body)
            finish(last: buffer, keepAlive: !close, context: context)
            return
        }
        headers.add(name: "content-length", value: String(body.readableBytes))
        if close && !isHTTP2 { headers.add(name: "connection", value: "close") }
        headers.add(name: "date", value: HTTPDate.now())
        context.write(wrapOutboundOut(.head(HTTPResponseHead(version: version, status: status, headers: headers))), promise: nil)
        context.write(wrapOutboundOut(.body(.byteBuffer(body))), promise: nil)
        let promise = completeRequest(keepAlive: !close)
        context.writeAndFlush(wrapOutboundOut(.end(nil)), promise: promise)
        afterResponse()
    }

    /// Writes the final `.end` (parts mode). State is updated *before* the write.
    private func endResponse(keepAlive: Bool) {
        guard let context else { return }
        let promise = completeRequest(keepAlive: keepAlive)
        emit(wrapOutboundOut(.end(nil)), promise: promise, context: context)
        afterResponse()
    }

    /// Writes the last bytes of a raw response and moves on to the next request (or closes).
    private func finish(last: ByteBuffer, keepAlive: Bool, context: ChannelHandlerContext) {
        let promise = completeRequest(keepAlive: keepAlive)
        emit(NIOAny(last), promise: promise, context: context)
        afterResponse()
    }

    /// Flush coalescing: responses written while reads are being delivered (or while draining
    /// pipelined requests) are flushed once, at `channelReadComplete` or the end of this tick.
    private func emit(_ data: NIOAny, promise: EventLoopPromise<Void>?, context: ChannelHandlerContext) {
        guard promise == nil, inReadCycle || draining else {
            context.writeAndFlush(data, promise: promise)
            return
        }
        context.write(data, promise: nil)
        if !flushPending {
            flushPending = true
            // Safety net: flush at the end of this event-loop tick even if no readComplete comes.
            context.eventLoop.execute { [self] in
                if self.flushPending {
                    self.flushPending = false
                    self.context?.flush()
                }
            }
        }
    }

    /// The response's last bytes are about to be queued. Returns a promise to attach to that write
    /// when the connection must close after it (closing a NIO channel drops queued writes).
    private func completeRequest(keepAlive: Bool) -> EventLoopPromise<Void>? {
        let complete = requestComplete
        if isHTTP2 {
            // The stream closes itself once both sides ended. If we answered before reading the
            // whole request (e.g. 413), close it.
            phase = .closing
            return !keepAlive || !complete ? closeAfterWrite() : nil
        }
        _ = state.endRequest()
        if keepAlive && complete && !env.isShuttingDown {
            state.requestFinished()
            phase = .idle
            requestComplete = false
            if let timeout = env.requestReadTimeout { deadline = .now() + timeout }
            return nil
        }
        phase = .closing
        queue.removeAll()
        return closeAfterWrite()
    }

    private func closeAfterWrite() -> EventLoopPromise<Void> {
        let promise = eventLoop.makePromise(of: Void.self)
        promise.futureResult.whenComplete { _ in self.context?.close(promise: nil) }
        return promise
    }

    /// After a response: serve the next pipelined request, or resume reading.
    private func afterResponse() {
        guard !isHTTP2, case .idle = phase else { return }
        if !queue.isEmpty || pendingRead { drainQueue() }
    }

    private func closeNow() {
        phase = .closing
        queue.removeAll()
        context?.close(promise: nil)
    }

    // MARK: Read deadline

    private func scheduleCheck(in delay: TimeAmount) {
        guard let context else { return }
        checker = context.eventLoop.scheduleTask(in: max(delay, .milliseconds(1))) { [self] in
            guard let context = self.context, context.channel.isActive else { return }
            if let stall = self.writeStallDeadline, NIODeadline.now() >= stall {
                self.closeNow()
                return
            }
            let now = NIODeadline.now()
            var next = self.checkInterval
            if let deadline = self.deadline {
                if now >= deadline {
                    if case .uploading(let upload) = self.phase { upload.sink.finish(ChannelError.eof) }
                    self.closeNow()
                    return
                }
                next = min(next, deadline - now)
            }
            if let stall = self.writeStallDeadline { next = min(next, stall - now) }
            self.scheduleCheck(in: next)
        }
    }
}

/// Resumes socket reads when the upload consumer wants more data.
final class UploadDelegate: NIOAsyncSequenceProducerDelegate, @unchecked Sendable {
    private weak var handler: HTTPRequestHandler?
    private let eventLoop: EventLoop

    init(handler: HTTPRequestHandler, eventLoop: EventLoop) {
        self.handler = handler
        self.eventLoop = eventLoop
    }

    func produceMore() {
        eventLoop.execute { self.handler?.resumeReading() }
    }

    func didTerminate() {
        eventLoop.execute { self.handler?.resumeReading() }
    }
}

// MARK: - Streaming upload receiver

/// Consumes a streamed request body: `multipart/form-data` files go to temporary files (0600) as
/// they arrive, text fields stay in memory; any other content type is written to one temporary file.
enum UploadReceiver {
    struct Result {
        var form: MultipartForm?
        var raw: UploadedFile?
    }

    /// Pulls body chunks into the parser on demand.
    private final class Reader<Body: AsyncSequence> where Body.Element == ByteBuffer {
        var iterator: Body.AsyncIterator
        var parser: MultipartParser
        init(_ body: Body, parser: MultipartParser) {
            self.iterator = body.makeAsyncIterator()
            self.parser = parser
        }

        func nextEvent() async throws -> MultipartParser.Event? {
            while true {
                do {
                    if let event = try parser.next() { return event }
                } catch let error as MultipartParser.ParseError {
                    throw MultipartError.malformed("\(error)")
                }
                guard let chunk = try await iterator.next() else { return nil }
                parser.append(chunk)
            }
        }
    }

    static func receive<Body: AsyncSequence>(
        _ body: Body, contentType: String?, options: UploadOptions, temporaryFiles: inout [String]
    ) async throws -> Result where Body.Element == ByteBuffer {
        let directory: String
        if let configured = options.directory {
            directory = configured
        } else {
            directory = try await FileSystem.shared.temporaryDirectory.string
        }

        guard let contentType, let boundary = MultipartParser.boundary(fromContentType: contentType) else {
            // Raw body (e.g. PUT /files/x with application/octet-stream): one temporary file.
            let path = directory + "/oria-upload-" + UUID().uuidString
            temporaryFiles.append(path)
            var size: Int64 = 0
            try await FileSystem.shared.withFileHandle(
                forWritingAt: FilePath(path), options: .newFile(replaceExisting: false, permissions: [.ownerReadWrite])
            ) { handle in
                for try await chunk in body {
                    size += Int64(try await handle.write(contentsOf: chunk, toAbsoluteOffset: size))
                }
            }
            let file = UploadedFile(
                field: "", filename: nil, contentType: contentType ?? "application/octet-stream", size: Int(size),
                storage: .disk(path: path)
            )
            return Result(form: nil, raw: file)
        }

        let reader = Reader(body, parser: MultipartParser(boundary: boundary, maxHeaderSize: options.maxPartHeaderSize))
        var form = MultipartForm()
        var sawEnd = false
        while let event = try await reader.nextEvent() {
            switch event {
            case .partBegin(let part):
                if part.filename != nil {
                    try form.checkFile(part, size: 0, limits: options)
                    let path = directory + "/oria-upload-" + UUID().uuidString
                    temporaryFiles.append(path)
                    var size: Int64 = 0
                    try await FileSystem.shared.withFileHandle(
                        forWritingAt: FilePath(path),
                        options: .newFile(replaceExisting: false, permissions: [.ownerReadWrite])
                    ) { handle in
                        partLoop: while let next = try await reader.nextEvent() {
                            switch next {
                            case .data(let chunk):
                                guard size + Int64(chunk.readableBytes) <= Int64(options.maxFileSize) else {
                                    throw MultipartError.tooLarge("File too large")
                                }
                                size += Int64(try await handle.write(contentsOf: chunk, toAbsoluteOffset: size))
                            case .partEnd:
                                break partLoop
                            case .partBegin, .end:
                                throw MultipartError.malformed("unexpected part boundary")
                            }
                        }
                    }
                    form.files.append(
                        UploadedFile(
                            field: part.name ?? "", filename: part.filename, contentType: part.contentType,
                            size: Int(size), storage: .disk(path: path)
                        ))
                } else {
                    var value = ByteBuffer()
                    fieldLoop: while let next = try await reader.nextEvent() {
                        switch next {
                        case .data(var chunk):
                            guard value.readableBytes + chunk.readableBytes <= options.maxFieldSize else {
                                throw MultipartError.tooLarge("Field \(part.name ?? "") too large")
                            }
                            value.writeBuffer(&chunk)
                        case .partEnd:
                            break fieldLoop
                        case .partBegin, .end:
                            throw MultipartError.malformed("unexpected part boundary")
                        }
                    }
                    guard form.allFields.count < options.maxFields else { throw MultipartError.tooMany("Too many fields") }
                    form.allFields.append((part.name ?? "", String(buffer: value)))
                }
            case .end:
                sawEnd = true
            case .data, .partEnd:
                throw MultipartError.malformed("data outside a part")
            }
        }
        guard sawEnd else { throw MultipartError.malformed("body ended before the closing boundary") }
        return Result(form: form, raw: nil)
    }
}
