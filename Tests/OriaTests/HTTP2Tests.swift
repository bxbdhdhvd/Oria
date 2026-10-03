import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOHTTP1
import Testing

@testable import Oria

@Suite(.timeLimit(.minutes(1))) struct HTTP2Tests {
    let slowStarted = NIOLockedValueBox(0)

    func makeApp(_ configure: (inout Oria.Configuration) -> Void = { _ in }) -> Oria {
        let app = Oria(configuration: testConfig {
            $0.tls = try! TestTLS.serverOptions()
            configure(&$0)
        })
        app.use(securityHeaders())
        app.get("/") { req, res in res.send("hello h\(req.version.major)") }
        app.post("/echo") { req, res in res.send(req.text ?? "") }
        app.get("/users/:id") { req, res in try res.json(["id": req.params["id"]!]) }
        app.get("/big") { _, res in res.type("txt").send(String(repeating: "abcdefgh", count: 20_000)) }
        let started = slowStarted
        app.get("/slow") { _, res in
            started.withLockedValue { $0 += 1 }
            try await Task.sleep(for: .milliseconds(300))
            res.send("finished")
        }
        app.get("/stream") { _, res in
            res.stream { w in for i in 0..<3 { try await w.write("chunk\(i);") } }
        }
        return app
    }

    @Test func servesRequestsOverHTTP2() async throws {
        let (server, port) = try await startServer(makeApp())
        let client = try await H2Client.connect(port: port)

        let root = try await client.request(.GET, "/")
        #expect(root.head.status == .ok)
        #expect(root.body == "hello h2")
        #expect(root.head.headers.first(name: "connection") == nil, "connection headers are illegal in HTTP/2")
        #expect(root.head.headers.first(name: "strict-transport-security") != nil)

        #expect(try await client.request(.POST, "/echo", body: "ping").body == "ping")
        #expect(try await client.request(.GET, "/users/7").body == #"{"id":"7"}"#)
        #expect(try await client.request(.GET, "/missing").head.status == .notFound)
        #expect(try await client.request(.GET, "/stream").body == "chunk0;chunk1;chunk2;")
        #expect(try await client.request(.GET, "/big").body.utf8.count == 160_000)
        let head = try await client.request(.HEAD, "/")
        #expect(head.head.status == .ok && head.body.isEmpty)
        client.close()
        await server.shutdown()
    }

    @Test func multiplexesManyStreamsOnOneConnection() async throws {
        let (server, port) = try await startServer(makeApp())
        let client = try await H2Client.connect(port: port)
        let clock = ContinuousClock()
        let start = clock.now
        let bodies = try await withThrowingTaskGroup(of: String.self) { group in
            for _ in 0..<50 { group.addTask { try await client.request(.GET, "/slow").body } }
            return try await group.reduce(into: [String]()) { $0.append($1) }
        }
        // 50 requests that each take 300 ms finish together, proving they ran concurrently.
        #expect(bodies.count == 50 && bodies.allSatisfy { $0 == "finished" })
        #expect(clock.now - start < .seconds(3))
        client.close()
        await server.shutdown()
    }

    @Test func http1StillWorksOverTLS() async throws {
        let (server, port) = try await startServer(makeApp())
        let client = try await RawClient.connect(port: port, tls: true, alpn: ["http/1.1"])
        try await client.send("GET / HTTP/1.1\r\nHost: x\r\n\r\n")
        #expect(try #require(await client.responseHead()).hasPrefix("HTTP/1.1 200"))
        #expect(await client.wait { text, _ in text.contains("hello h1") })
        client.close()

        // A TLS client that offers no ALPN at all gets HTTP/1.1 too.
        let noALPN = try await RawClient.connect(port: port, tls: true, alpn: [])
        try await noALPN.send("GET / HTTP/1.1\r\nHost: x\r\n\r\n")
        #expect(try #require(await noALPN.responseHead()).hasPrefix("HTTP/1.1 200"))
        noALPN.close()
        await server.shutdown()
    }

    @Test func http2DisabledFallsBackToHTTP1() async throws {
        let (server, port) = try await startServer(makeApp { $0.http2 = false })
        let client = try await RawClient.connect(port: port, tls: true, alpn: ["h2", "http/1.1"])
        try await client.send("GET / HTTP/1.1\r\nHost: x\r\n\r\n")
        #expect(try #require(await client.responseHead()).hasPrefix("HTTP/1.1 200"))
        client.close()
        await server.shutdown()
    }

    @Test func bodyLimitAppliesToHTTP2() async throws {
        let (server, port) = try await startServer(makeApp { $0.maxBodySize = 16 })
        let client = try await H2Client.connect(port: port)
        let r = try await client.request(.POST, "/echo", body: String(repeating: "x", count: 100))
        #expect(r.head.status == .payloadTooLarge)
        // The connection survives: other streams still work.
        #expect(try await client.request(.GET, "/").body == "hello h2")
        client.close()
        await server.shutdown()
    }

    @Test func compressionOverHTTP2() async throws {
        let (server, port) = try await startServer(makeApp { $0.compression = true })
        let client = try await H2Client.connect(port: port)
        let r = try await client.request(.GET, "/big", headers: ["accept-encoding": "gzip"])
        #expect(r.head.headers.first(name: "content-encoding") == "gzip")
        client.close()
        await server.shutdown()
    }

    @Test func gracefulShutdownFinishesInFlightStreams() async throws {
        let (server, port) = try await startServer(makeApp())
        let client = try await H2Client.connect(port: port)
        async let slow = client.request(.GET, "/slow")
        let deadline = ContinuousClock.now + .seconds(5)
        while slowStarted.withLockedValue({ $0 }) == 0 && ContinuousClock.now < deadline {
            try await Task.sleep(for: .milliseconds(5))
        }
        await server.shutdown()
        #expect(try await slow.body == "finished")
        client.close()
    }

    @Test func rejectsLegacyTLSVersions() async throws {
        let (server, port) = try await startServer(makeApp())
        let legacy = try TestTLS.clientContext(alpn: ["http/1.1"], maxVersion: .tlsv11)
        let client = try await RawClient.connect(port: port, context: legacy)
        // Fire-and-forget: a write queued behind a failing TLS handshake may never complete client-side.
        client.channel.writeAndFlush(ByteBuffer(string: "GET / HTTP/1.1\r\nHost: x\r\n\r\n"), promise: nil)
        #expect(await client.waitForClose(), "TLS 1.0/1.1 handshakes must be refused")
        #expect(!client.text.contains("HTTP/1.1"))
        await server.shutdown()
    }
}
