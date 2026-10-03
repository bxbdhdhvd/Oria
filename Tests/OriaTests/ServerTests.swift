import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOHTTP1
import NIOPosix
import Testing

@testable import Oria

/// End-to-end tests over real TCP sockets.
@Suite struct ServerTests {
    let slowStarted = NIOLockedValueBox(0)

    /// Waits until the /slow handler is running, so a shutdown is guaranteed to hit it mid-flight.
    func waitForSlowRequest() async throws {
        let deadline = ContinuousClock.now + .seconds(5)
        while slowStarted.withLockedValue({ $0 }) == 0 && ContinuousClock.now < deadline {
            try await Task.sleep(for: .milliseconds(5))
        }
    }

    struct ClientResponse {
        var head: HTTPResponseHead
        var body: String
    }

    typealias ClientChannel = NIOAsyncChannel<HTTPClientResponsePart, HTTPClientRequestPart>

    static func connect(_ port: Int) async throws -> ClientChannel {
        try await ClientBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .connect(host: "127.0.0.1", port: port) { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHTTPClientHandlers()
                    return try ClientChannel(wrappingChannelSynchronously: channel)
                }
            }
    }

    /// Sends several requests over one connection and reads every response.
    static func send(
        _ requests: [(HTTPMethod, String, HTTPHeaders, String?)], port: Int,
        version: HTTPVersion = .http1_1
    ) async throws -> [ClientResponse] {
        let channel = try await connect(port)
        return try await channel.executeThenClose { inbound, outbound in
            var results: [ClientResponse] = []
            var iterator = inbound.makeAsyncIterator()
            for (method, uri, extraHeaders, body) in requests {
                var headers: HTTPHeaders = ["host": "localhost"]
                headers.add(contentsOf: extraHeaders)
                if let body { headers.replaceOrAdd(name: "content-length", value: String(body.utf8.count)) }
                try await outbound.write(.head(HTTPRequestHead(version: version, method: method, uri: uri, headers: headers)))
                if let body { try await outbound.write(.body(.byteBuffer(ByteBuffer(string: body)))) }
                try await outbound.write(.end(nil))

                var head: HTTPResponseHead?
                var collected = ByteBuffer()
                loop: while let part = try await iterator.next() {
                    switch part {
                    case .head(let h): head = h
                    case .body(var b): collected.writeBuffer(&b)
                    case .end: break loop
                    }
                }
                results.append(ClientResponse(head: try #require(head), body: String(buffer: collected)))
            }
            return results
        }
    }

    /// Fires a request and ignores the outcome (refused, reset, or answered are all fine).
    static func attempt(port: Int) async {
        guard let channel = try? await connect(port) else { return }
        try? await channel.executeThenClose { inbound, outbound in
            try await outbound.write(.head(HTTPRequestHead(version: .http1_1, method: .GET, uri: "/", headers: ["host": "x"])))
            try await outbound.write(.end(nil))
            for try await part in inbound { if case .end = part { break } }
        }
    }

    static func get(_ uri: String, port: Int, headers: HTTPHeaders = [:]) async throws -> ClientResponse {
        try await send([(.GET, uri, headers, nil)], port: port)[0]
    }

    func makeApp(_ configure: (inout Oria.Configuration) -> Void = { _ in }) -> Oria {
        var config = Oria.Configuration()
        config.threads = 2
        config.handleSignals = false
        configure(&config)
        let app = Oria(configuration: config)
        app.get("/") { _, res in res.send("hello") }
        let started = slowStarted
        app.get("/slow") { _, res in
            started.withLockedValue { $0 += 1 }
            try await Task.sleep(for: .milliseconds(300))
            res.send("finished")
        }
        app.post("/echo") { req, res in res.send(req.text ?? "") }
        app.get("/big") { _, res in res.type("txt").send(String(repeating: "abcdefgh", count: 10_000)) }
        app.get("/stream") { _, res in
            res.stream { w in for i in 0..<5 { try await w.write("chunk\(i);") } }
        }
        return app
    }

    @Test func servesOverTCPWithKeepAlive() async throws {
        let app = makeApp()
        let server = try await app.start(port: 0, host: "127.0.0.1")
        let port = try #require(server.port)

        let responses = try await Self.send(
            [(.GET, "/", [:], nil), (.POST, "/echo", [:], "ping"), (.GET, "/missing", [:], nil)], port: port
        )
        #expect(responses.map(\.head.status) == [.ok, .ok, .notFound])
        #expect(responses[0].body == "hello")
        #expect(responses[1].body == "ping")
        #expect(responses[0].head.headers.first(name: "date") != nil)
        #expect(responses[0].head.headers.first(name: "content-length") == "5")
        #expect(responses[0].head.headers.first(name: "connection") == nil)
        await server.shutdown()
    }

    /// Regression: closing right after writing used to discard bytes still queued in NIO, silently
    /// truncating large responses sent with `Connection: close` (HTTP/1.0, errors, shutdown).
    @Test func largeResponsesSurviveConnectionClose() async throws {
        let app = makeApp()
        let size = 32 * 1024 * 1024
        let payload = ByteBuffer(repeating: UInt8(ascii: "z"), count: size)
        app.get("/huge") { _, res in res.send(payload) }
        let server = try await app.start(port: 0, host: "127.0.0.1")
        for _ in 0..<3 {
            let client = try await RawClient.connect(port: server.port!)
            try await client.send("GET /huge HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            #expect(await client.waitForClose(timeout: .seconds(20)))
            let received = client.collector.buffer.withLockedValue { $0.readableBytes }
            let head = String(client.text.prefix(300))
            let headLength = head.range(of: "\r\n\r\n").map { head.utf8.distance(from: head.startIndex, to: $0.upperBound) } ?? 0
            #expect(received - headLength == size, "body must arrive complete before the close")
        }
        await server.shutdown()
    }

    @Test func chunkedStreamingResponse() async throws {
        let server = try await makeApp().start(port: 0, host: "127.0.0.1")
        let r = try await Self.get("/stream", port: server.port!)
        #expect(r.body == "chunk0;chunk1;chunk2;chunk3;chunk4;")
        #expect(r.head.headers.first(name: "transfer-encoding") == "chunked")
        await server.shutdown()
    }

    @Test func http10ClosesConnection() async throws {
        let server = try await makeApp().start(port: 0, host: "127.0.0.1")
        let r = try await Self.send([(.GET, "/", [:], nil)], port: server.port!, version: .http1_0)[0]
        #expect(r.body == "hello")
        #expect(r.head.headers.first(name: "connection") == "close")
        await server.shutdown()
    }

    @Test func rejectsOversizedBodies() async throws {
        let server = try await makeApp { $0.maxBodySize = 16 }.start(port: 0, host: "127.0.0.1")
        let r = try await Self.send([(.POST, "/echo", [:], String(repeating: "x", count: 100))], port: server.port!)[0]
        #expect(r.head.status == .payloadTooLarge)
        #expect(r.head.headers.first(name: "connection") == "close")
        await server.shutdown()
    }

    @Test func rejectsOversizedChunkedBodies() async throws {
        let server = try await makeApp { $0.maxBodySize = 16 }.start(port: 0, host: "127.0.0.1")
        let channel = try await Self.connect(server.port!)
        let status = try await channel.executeThenClose { inbound, outbound in
            let head = HTTPRequestHead(
                version: .http1_1, method: .POST, uri: "/echo",
                headers: ["host": "x", "transfer-encoding": "chunked"]
            )
            try await outbound.write(.head(head))
            for _ in 0..<4 { try await outbound.write(.body(.byteBuffer(ByteBuffer(string: "0123456789")))) }
            try await outbound.write(.end(nil))
            for try await part in inbound {
                if case .head(let h) = part { return h.status }
            }
            return HTTPResponseStatus.ok
        }
        #expect(status == .payloadTooLarge)
        await server.shutdown()
    }

    @Test func handlesManyConcurrentClients() async throws {
        let server = try await makeApp { $0.threads = 4 }.start(port: 0, host: "127.0.0.1")
        let port = server.port!
        let ok = try await withThrowingTaskGroup(of: Int.self) { group in
            for _ in 0..<100 {
                group.addTask {
                    let rs = try await Self.send(Array(repeating: (.GET, "/", [:], nil), count: 5), port: port)
                    return rs.filter { $0.body == "hello" }.count
                }
            }
            return try await group.reduce(0, +)
        }
        #expect(ok == 500)
        await server.shutdown()
    }

    @Test func gzipCompression() async throws {
        let server = try await makeApp { $0.compression = true }.start(port: 0, host: "127.0.0.1")
        let r = try await Self.get("/big", port: server.port!, headers: ["accept-encoding": "gzip"])
        #expect(r.head.headers.first(name: "content-encoding") == "gzip")
        #expect(r.body.utf8.count < 80_000)
        let plain = try await Self.get("/big", port: server.port!)
        #expect(plain.head.headers.first(name: "content-encoding") == nil)
        #expect(plain.body.utf8.count == 80_000)
        await server.shutdown()
    }

    @Test func connectionCap() async throws {
        let server = try await makeApp { $0.maxConnections = 1 }.start(port: 0, host: "127.0.0.1")
        let port = server.port!
        // Hold one connection open, then a second one should be refused with 503.
        let held = try await Self.connect(port)
        try await Task.sleep(for: .milliseconds(100))
        let second = try await Self.get("/", port: port)
        #expect(second.head.status == .serviceUnavailable)
        try await held.channel.close()
        await server.shutdown()
    }

    @Test func gracefulShutdownFinishesInFlightRequests() async throws {
        let server = try await makeApp().start(port: 0, host: "127.0.0.1")
        let port = server.port!

        // An idle keep-alive connection must not hold up shutdown.
        let idle = try await Self.connect(port)

        async let slow = Self.get("/slow", port: port)
        try await waitForSlowRequest()

        let clock = ContinuousClock()
        let started = clock.now
        await server.shutdown()
        let elapsed = clock.now - started

        let response = try await slow
        #expect(response.body == "finished")
        #expect(response.head.headers.first(name: "connection") == "close")
        #expect(elapsed < .seconds(5))

        // The listener is closed now.
        await #expect(throws: (any Error).self) { _ = try await Self.connect(port) }
        try? await idle.channel.close()
    }

    /// Regression: connections accepted while the listener closes used to be dropped unfinished,
    /// which traps inside NIO ("Deinited NIOAsyncWriter without calling finish()"), and sockets whose
    /// initializer never ran leaked (clients hung forever).
    @Test(.timeLimit(.minutes(1))) func shutdownDuringConnectionStormDoesNotCrashOrLeak() async throws {
        for _ in 0..<5 {
            let server = try await makeApp { $0.threads = 4 }.start(port: 0, host: "127.0.0.1")
            let port = server.port!
            await withTaskGroup(of: Void.self) { group in
                for _ in 0..<200 {
                    group.addTask { await Self.attempt(port: port) }
                }
                group.addTask {
                    try? await Task.sleep(for: .milliseconds(5))
                    await server.shutdown()
                }
            }
        }
    }

    @Test func idleConnectionsAreClosed() async throws {
        let server = try await makeApp { $0.idleTimeout = .milliseconds(200) }.start(port: 0, host: "127.0.0.1")
        let channel = try await Self.connect(server.port!)
        try await Task.sleep(for: .milliseconds(600))
        #expect(!channel.channel.isActive)
        await server.shutdown()
    }
}
