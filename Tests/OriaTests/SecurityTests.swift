import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOHTTP1
import NIOHTTP2
import Testing

@testable import Oria

/// Hostile-input tests over real sockets. Each one also checks that the server keeps serving.
@Suite(.timeLimit(.minutes(1))) struct SecurityTests {
    let adminHits = NIOLockedValueBox(0)

    func makeApp(_ configure: (inout Oria.Configuration) -> Void = { _ in }) -> Oria {
        let app = Oria(configuration: testConfig(configure))
        let hits = adminHits
        app.use(securityHeaders())
        app.get("/") { _, res in res.send("ok") }
        app.post("/echo") { req, res in res.send(req.text ?? "") }
        app.get("/admin") { _, res in
            hits.withLockedValue { $0 += 1 }
            res.send("secret admin page")
        }
        app.get("/inject") { req, res in
            res.set("x-user", req.query["v"] ?? "")
            res.send("injected?")
        }
        app.get("/go") { req, res in res.redirect(req.query["next"] ?? "/") }
        app.get("/count") { req, res in res.send(String(req.queryAll.count)) }
        app.post("/json") { req, res in
            struct Payload: Decodable { var name: String }
            res.send(try req.json(Payload.self).name)
        }
        app.get("/seq/:n") { req, res in res.send(req.params["n"]!) }
        return app
    }

    /// Sends raw bytes on a fresh connection and returns everything received until close/timeout.
    func exchange(_ port: Int, _ raw: String, timeout: Duration = .seconds(2)) async throws -> RawClient {
        let client = try await RawClient.connect(port: port)
        try? await client.send(raw)
        await client.wait(timeout: timeout) { _, closed in closed }
        return client
    }

    static func status(_ text: String, in codes: [Int]) -> Bool {
        codes.contains { text.hasPrefix("HTTP/1.1 \($0)") }
    }

    func assertStillServing(_ port: Int) async throws {
        let client = try await exchange(port, "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        #expect(client.text.hasPrefix("HTTP/1.1 200"), "server must keep serving after an attack")
    }

    // MARK: Regressions from the HTTP/1.1 fast-path pen test

    /// `Expect: 100-continue` with response compression once crashed the process (NIO's compressor
    /// paired the interim 1xx head with the request).
    @Test(arguments: [false, true]) func expectContinueNeverCrashes(compression: Bool) async throws {
        let (server, port) = try await startServer(makeApp { $0.compression = compression })
        for raw in [
            "GET / HTTP/1.1\r\nHost: x\r\nExpect: 100-continue\r\nAccept-Encoding: gzip\r\nConnection: close\r\n\r\n",
            "POST /echo HTTP/1.1\r\nHost: x\r\nExpect: 100-continue\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello",
        ] {
            let client = try await exchange(port, raw)
            #expect(client.text.hasPrefix("HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200"), "\(client.text.prefix(80))")
        }
        try await assertStillServing(port)
        await server.shutdown()
    }

    /// Valid pipelined requests followed by garbage in the same write: the valid responses arrive
    /// in order, then a 400, then the connection closes.
    @Test(arguments: [false, true]) func pipelinedRequestsBeforeAParseErrorAreAnswered(compression: Bool) async throws {
        let (server, port) = try await startServer(makeApp { $0.compression = compression })
        let client = try await exchange(
            port, "GET /seq/1 HTTP/1.1\r\nHost: x\r\n\r\nGET /seq/2 HTTP/1.1\r\nHost: x\r\n\r\n\u{1}GARBAGE\r\n\r\n")
        let statuses = client.text.components(separatedBy: "HTTP/1.1 ").dropFirst().map { $0.prefix(3) }
        #expect(statuses == ["200", "200", "400"], "\(client.text)")
        #expect(client.text.contains("\r\n\r\n1") && client.text.contains("\r\n\r\n2"))
        await server.shutdown()
    }

    /// A malformed request later on a keep-alive connection gets a 400 like a first request does.
    @Test(arguments: [false, true]) func laterMalformedRequestsGet400(compression: Bool) async throws {
        let (server, port) = try await startServer(makeApp { $0.compression = compression })
        let client = try await RawClient.connect(port: port)
        try await client.send("GET / HTTP/1.1\r\nHost: x\r\n\r\n")
        #expect(await client.wait { text, _ in text.contains("ok") })
        try await client.send("\u{1}GARBAGE\r\n\r\n")
        #expect(await client.wait { text, closed in closed && text.contains("HTTP/1.1 400") }, "\(client.text)")
        client.close()
        await server.shutdown()
    }

    // MARK: Request smuggling & parser abuse

    @Test func rejectsContentLengthPlusTransferEncoding() async throws {
        let (server, port) = try await startServer(makeApp())
        let smuggle = "POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 6\r\nTransfer-Encoding: chunked\r\n\r\n"
            + "0\r\n\r\nGET /admin HTTP/1.1\r\nHost: x\r\n\r\n"
        let client = try await exchange(port, smuggle)
        #expect(client.text.hasPrefix("HTTP/1.1 400"))
        #expect(!client.text.contains("secret admin page"))
        #expect(adminHits.withLockedValue { $0 } == 0, "smuggled request must never reach a handler")
        try await assertStillServing(port)
        await server.shutdown()
    }

    @Test func rejectsConflictingContentLengths() async throws {
        let (server, port) = try await startServer(makeApp())
        let client = try await exchange(
            port, "POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\nContent-Length: 30\r\n\r\nhello"
        )
        #expect(client.text.hasPrefix("HTTP/1.1 400"))
        await server.shutdown()
    }

    @Test func rejectsObfuscatedTransferEncoding() async throws {
        let (server, port) = try await startServer(makeApp())
        for header in ["Transfer-Encoding : chunked", "Transfer-Encoding: chunked\r\nTransfer-Encoding: x"] {
            let client = try await exchange(
                port, "POST /echo HTTP/1.1\r\nHost: x\r\n\(header)\r\n\r\n0\r\n\r\nGET /admin HTTP/1.1\r\nHost: x\r\n\r\n"
            )
            #expect(!client.text.contains("secret admin page"), "obfuscated TE: \(header)")
            #expect(client.text.hasPrefix("HTTP/1.1 400"), "obfuscated TE: \(header)")
        }
        #expect(adminHits.withLockedValue { $0 } == 0)
        await server.shutdown()
    }

    @Test func rejectsInvalidChunkSizes() async throws {
        let (server, port) = try await startServer(makeApp())
        for chunk in ["ZZ\r\nhello\r\n0\r\n\r\n", "fffffffffffffffffff\r\n", "-5\r\nhello\r\n"] {
            let client = try await exchange(port, "POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n\(chunk)")
            #expect(client.text.hasPrefix("HTTP/1.1 400"), "chunk: \(chunk.debugDescription)")
            #expect(client.isClosed)
        }
        try await assertStillServing(port)
        await server.shutdown()
    }

    @Test func rejectsGarbageAndUnknownMethods() async throws {
        let (server, port) = try await startServer(makeApp { $0.requestReadTimeout = .seconds(4) })
        for raw in ["\u{16}\u{03}\u{01}\u{00}\u{a5}garbage\r\n\r\n", "BREW /pot HTTP/1.1\r\nHost: x\r\n\r\n", "GET / HTTP/9.9\r\n\r\n"] {
            let clock = ContinuousClock()
            let start = clock.now
            let client = try await exchange(port, raw)
            #expect(client.text.hasPrefix("HTTP/1.1 400"), "input: \(raw.debugDescription)")
            // Well under the 4 s read timeout: the parse error itself closed the connection.
            #expect(client.isClosed && clock.now - start < .seconds(2), "malformed input must be closed at once")
        }
        // Blank lines are legal before a request (RFC 9112 §2.2), so this waits for the read timeout.
        let blank = try await exchange(port, "\r\n\r\n\r\n", timeout: .seconds(8))
        #expect(blank.isClosed && blank.text.isEmpty)
        try await assertStillServing(port)
        await server.shutdown()
    }

    // MARK: Size limits

    @Test func rejectsOversizedHeadersAndURLs() async throws {
        let (server, port) = try await startServer(makeApp())
        let bigHeader = try await exchange(port, "GET / HTTP/1.1\r\nHost: x\r\nX-Big: \(String(repeating: "a", count: 20_000))\r\n\r\n")
        #expect(Self.status(bigHeader.text, in: [400, 431]))

        let bigURL = try await exchange(port, "GET /\(String(repeating: "a", count: 100_000)) HTTP/1.1\r\nHost: x\r\n\r\n")
        #expect(Self.status(bigURL.text, in: [400, 414, 431]))

        let manyHeaders = (0..<300).map { "X-H\($0): v\r\n" }.joined()
        let many = try await exchange(port, "GET / HTTP/1.1\r\nHost: x\r\n\(manyHeaders)\r\n")
        #expect(Self.status(many.text, in: [400, 431]))
        try await assertStillServing(port)
        await server.shutdown()
    }

    @Test func parameterFloodIsCapped() async throws {
        let (server, port) = try await startServer(makeApp())
        // 1500 parameters (~11 KB) fit the 16 KB header limit; only the first 1000 are parsed.
        let query = (0..<1500).map { "p\($0)=1" }.joined(separator: "&")
        let client = try await exchange(port, "GET /count?\(query) HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        #expect(client.text.hasPrefix("HTTP/1.1 200"))
        #expect(client.text.hasSuffix("\r\n\r\n1000"))
        await server.shutdown()
    }

    @Test func deeplyNestedJSONDoesNotCrash() async throws {
        let (server, port) = try await startServer(makeApp())
        let bomb = String(repeating: "[", count: 400_000) + String(repeating: "]", count: 400_000)
        let client = try await exchange(
            port, "POST /json HTTP/1.1\r\nHost: x\r\nConnection: close\r\nContent-Length: \(bomb.utf8.count)\r\n\r\n\(bomb)"
        )
        #expect(client.text.hasPrefix("HTTP/1.1 400"))
        try await assertStillServing(port)
        await server.shutdown()
    }

    // MARK: Slow clients

    @Test func slowlorisHeadersAreCutOff() async throws {
        let (server, port) = try await startServer(makeApp {
            $0.requestReadTimeout = .milliseconds(400)
            $0.idleTimeout = .seconds(30)
        })
        let client = try await RawClient.connect(port: port)
        // Keep the connection "active" with one byte every 100 ms so idleTimeout never fires.
        let dribble = Task {
            for byte in "GET / HTTP/1.1\r\nHost: x\r\nX-Slow: ".utf8 + Array(repeating: UInt8(ascii: "a"), count: 100) {
                try await client.send([byte])
                try await Task.sleep(for: .milliseconds(100))
            }
        }
        #expect(await client.waitForClose(timeout: .seconds(3)))
        dribble.cancel()
        await server.shutdown()
    }

    @Test func truncatedBodiesNeverReachHandlers() async throws {
        let app = makeApp { $0.requestReadTimeout = .milliseconds(300) }
        let handled = NIOLockedValueBox(0)
        app.post("/count-calls") { req, res in
            handled.withLockedValue { $0 += 1 }
            res.send("got \(req.bytes.count) bytes")
        }
        let (server, port) = try await startServer(app)
        // Announce 100 bytes, send 10, then stall until the server's read timeout closes the
        // connection. A server-initiated close ends NIO's inbound stream *cleanly*, which once
        // looked exactly like a finished request. Many at once to hit the race.
        let closedByTimeout = await withTaskGroup(of: Bool.self) { group in
            for _ in 0..<50 {
                group.addTask {
                    guard let stalled = try? await RawClient.connect(port: port) else { return false }
                    try? await stalled.send("POST /count-calls HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\n0123456789")
                    return await stalled.waitForClose(timeout: .seconds(5))
                }
            }
            return await group.reduce(true) { $0 && $1 }
        }
        #expect(closedByTimeout)
        // Clients hanging up mid-body. Whether NIO reports that as an error or as a clean end of the
        // stream is a race, so try many times concurrently.
        await withTaskGroup(of: Void.self) { group in
            for _ in 0..<200 {
                group.addTask {
                    guard let quitter = try? await RawClient.connect(port: port) else { return }
                    try? await quitter.send("POST /count-calls HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\n0123456789")
                    quitter.close()
                }
            }
        }
        try await Task.sleep(for: .milliseconds(300))
        #expect(handled.withLockedValue { $0 } == 0, "a handler must never see a truncated body")
        await server.shutdown()
    }

    @Test func slowBodiesAreCutOff() async throws {
        let (server, port) = try await startServer(makeApp {
            $0.requestReadTimeout = .milliseconds(400)
            $0.idleTimeout = .seconds(30)
        })
        let client = try await RawClient.connect(port: port)
        try await client.send("POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 1000\r\n\r\n")
        let dribble = Task {
            for _ in 0..<1000 {
                try await client.send("a")
                try await Task.sleep(for: .milliseconds(100))
            }
        }
        #expect(await client.waitForClose(timeout: .seconds(3)))
        dribble.cancel()
        await server.shutdown()
    }

    @Test func idleKeepAliveConnectionsAreReclaimed() async throws {
        let (server, port) = try await startServer(makeApp { $0.requestReadTimeout = .seconds(1) })
        let client = try await RawClient.connect(port: port)
        try await client.send("GET / HTTP/1.1\r\nHost: x\r\n\r\n")
        #expect(try #require(await client.responseHead()).hasPrefix("HTTP/1.1 200"))
        #expect(await client.waitForClose(timeout: .seconds(4)))
        await server.shutdown()
    }

    // MARK: Injection

    @Test func responseSplittingIsBlocked() async throws {
        let (server, port) = try await startServer(makeApp())
        let split = try await exchange(port, "GET /inject?v=a%0d%0aSet-Cookie:%20pwned=1 HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        #expect(split.text.hasPrefix("HTTP/1.1 500"))
        #expect(!split.text.lowercased().contains("pwned"))

        let redirect = try await exchange(port, "GET /go?next=/x%0d%0aSet-Cookie:%20pwned=1 HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        #expect(redirect.text.hasPrefix("HTTP/1.1 500"))
        #expect(!redirect.text.lowercased().contains("pwned"))

        let safe = try await exchange(port, "GET /inject?v=hello HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        #expect(safe.text.contains("x-user: hello"))
        await server.shutdown()
    }

    @Test func securityHeadersAreSet() async throws {
        let (server, port) = try await startServer(makeApp())
        let head = try await exchange(port, "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").text.lowercased()
        #expect(head.contains("x-content-type-options: nosniff"))
        #expect(head.contains("x-frame-options: deny"))
        #expect(head.contains("content-security-policy: default-src 'self'"))
        #expect(head.contains("referrer-policy: no-referrer"))
        #expect(!head.contains("strict-transport-security"), "HSTS only over HTTPS")
        await server.shutdown()
    }

    // MARK: Protocol behavior

    @Test func pipelinedRequestsAnswerInOrder() async throws {
        let (server, port) = try await startServer(makeApp())
        let requests = (0..<200).map { "GET /seq/\($0) HTTP/1.1\r\nHost: x\r\n\r\n" }.joined()
        let client = try await RawClient.connect(port: port)
        try await client.send(requests + "GET /seq/end HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        #expect(await client.waitForClose())
        let bodies = client.text.components(separatedBy: "HTTP/1.1 200 OK").dropFirst().map {
            $0.components(separatedBy: "\r\n\r\n").last ?? ""
        }
        let expected: [String] = (0..<200).map { String($0) } + ["end"]
        #expect(bodies == expected)
        await server.shutdown()
    }

    @Test func unknownUpgradeRequestsAreServedNormally() async throws {
        let (server, port) = try await startServer(makeApp())
        // curl --http2 on plain http sends this. NIO's upgrade handler would swallow the request.
        let start = ContinuousClock.now
        let client = try await exchange(
            port, "GET / HTTP/1.1\r\nHost: x\r\nConnection: Upgrade, HTTP2-Settings\r\nUpgrade: h2c\r\nHTTP2-Settings: AAMAAABkAAQAoAAAAAIAAAAA\r\n\r\n"
        )
        #expect(client.text.hasPrefix("HTTP/1.1 200"))
        #expect(ContinuousClock.now - start < .seconds(2))
        await server.shutdown()
    }

    // MARK: HTTP/2 attacks

    /// A raw HTTP/2 frame: 3-byte length, type, flags, 31-bit stream id, payload.
    static func h2Frame(type: UInt8, flags: UInt8, stream: UInt32, payload: [UInt8]) -> [UInt8] {
        let n = payload.count
        return [UInt8(n >> 16 & 0xFF), UInt8(n >> 8 & 0xFF), UInt8(n & 0xFF), type, flags,
                UInt8(stream >> 24 & 0x7F), UInt8(stream >> 16 & 0xFF), UInt8(stream >> 8 & 0xFF), UInt8(stream & 0xFF)]
            + payload
    }

    @Test func http2RapidResetFloodIsShutDown() async throws {
        let (server, port) = try await startServer(makeApp { $0.tls = try! TestTLS.serverOptions() })
        // CVE-2023-44487 ("rapid reset"), hand-rolled the way an attacker would: open a stream with
        // HEADERS and cancel it with RST_STREAM at once, 2000 times in a single burst.
        let attacker = try await RawClient.connect(port: port, tls: true, alpn: ["h2"])
        var burst = Array("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".utf8)
        burst += Self.h2Frame(type: 0x4, flags: 0, stream: 0, payload: [])  // SETTINGS
        // HPACK: :method GET, :path /, :scheme https (static table), :authority "x" (literal).
        let headerBlock: [UInt8] = [0x82, 0x84, 0x87, 0x01, 0x01, UInt8(ascii: "x")]
        for i in 0..<2000 {
            let stream = UInt32(2 * i + 1)
            burst += Self.h2Frame(type: 0x1, flags: 0x4, stream: stream, payload: headerBlock)  // HEADERS, END_HEADERS
            burst += Self.h2Frame(type: 0x3, flags: 0, stream: stream, payload: [0, 0, 0, 0x8])  // RST_STREAM CANCEL
        }
        try? await attacker.send(burst)
        #expect(await attacker.waitForClose(timeout: .seconds(5)), "server must drop a connection flooding RST_STREAM")

        let fresh = try await H2Client.connect(port: port)
        #expect(try await fresh.request(.GET, "/").body == "ok")
        fresh.close()
        await server.shutdown()
    }

    @Test func http2HeaderBombIsRejected() async throws {
        let (server, port) = try await startServer(makeApp { $0.tls = try! TestTLS.serverOptions() })
        let client = try await H2Client.connect(port: port)
        let bomb = try? await client.request(.GET, "/", headers: ["x-bomb": String(repeating: "a", count: 64_000)])
        #expect(bomb?.head.status != .ok)
        client.close()
        let fresh = try await H2Client.connect(port: port)
        #expect(try await fresh.request(.GET, "/").body == "ok")
        fresh.close()
        await server.shutdown()
    }
}
