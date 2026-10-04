import Foundation
import NIOCore
import NIOHTTP1
import Testing

@testable import Oria

@Suite(.timeLimit(.minutes(1))) struct WebSocketTests {
    struct Boom: Error {}

    func makeApp(_ configure: (inout Oria.Configuration) -> Void = { _ in }) -> Oria {
        let app = Oria(configuration: testConfig(configure))
        app.use { _, res, next in
            res.set("x-mw", "ran")
            try await next()
        }
        let requireToken: Middleware = { req, res, next in
            guard req.query["token"] == "secret" else {
                res.status(.unauthorized).json(raw: #"{"error":"unauthorized"}"#)
                return
            }
            try await next()
        }
        let echo: WebSocketHandler = { _, ws in
            for await message in ws.messages {
                switch message {
                case .text(let text): try await ws.send(text)
                case .binary(let data): try await ws.send(data)
                }
            }
        }
        app.ws("/echo", handler: echo)
        app.ws("/rooms/:room") { req, ws in
            try await ws.send("joined \(req.params["room"]!)")
        }
        app.ws("/private", requireToken) { _, ws in try await ws.send("welcome") }
        app.ws("/origin", options: .init(allowedOrigins: ["https://good.example"])) { _, ws in try await ws.send("ok") }
        app.ws("/proto", options: .init(protocols: ["chat.v2", "chat.v1"])) { _, ws in
            try await ws.send(ws.subprotocol ?? "none")
        }
        app.ws("/throws") { _, _ in throw Boom() }
        app.ws("/small", options: .init(maxFrameSize: 1024, maxMessageSize: 1024), handler: echo)
        app.ws("/ping", options: .init(pingInterval: .milliseconds(100))) { _, ws in
            for await _ in ws.messages {}
        }
        app.ws("/json") { _, ws in try await ws.send(json: ["hello": "world"]) }
        app.get("/http") { _, res in res.send("plain") }
        return app
    }

    func expectUpgraded(_ head: String) {
        #expect(head.hasPrefix("HTTP/1.1 101"), "expected 101, got: \(head)")
        #expect(head.lowercased().contains("sec-websocket-accept: s3pplmbitxaq9kygzzhzrbk+xoo="))
    }

    @Test func echoesTextAndBinary() async throws {
        let (server, port) = try await startServer(makeApp())
        let (ws, head) = try await RawClient.webSocket(port: port, path: "/echo")
        expectUpgraded(head)
        #expect(head.lowercased().contains("x-mw: ran"), "middleware headers are sent with the 101")

        try await ws.sendText("hello")
        #expect(await ws.readFrame() == .init(opcode: 0x1, payload: Array("hello".utf8)))

        try await ws.send(RawClient.frame(opcode: 0x2, payload: [0, 1, 2, 255]))
        #expect(await ws.readFrame() == .init(opcode: 0x2, payload: [0, 1, 2, 255]))

        // A 200 KB message (one frame, like browsers send it).
        let big = String(repeating: "x", count: 200_000)
        try await ws.sendText(big)
        #expect(await ws.readFrame()?.text == big)
        ws.close()
        await server.shutdown()
    }

    @Test func secureWebSocketOverTLS() async throws {
        let app = makeApp { $0.tls = try! TestTLS.serverOptions() }
        let (server, port) = try await startServer(app)
        let (ws, head) = try await RawClient.webSocket(port: port, path: "/echo", tls: true)
        expectUpgraded(head)
        try await ws.sendText("over tls")
        #expect(await ws.readFrame()?.text == "over tls")
        ws.close()
        await server.shutdown()
    }

    @Test func routeParamsAndServerClose() async throws {
        let (server, port) = try await startServer(makeApp())
        let (ws, head) = try await RawClient.webSocket(port: port, path: "/rooms/lobby")
        expectUpgraded(head)
        #expect(await ws.readFrame()?.text == "joined lobby")
        // The handler returned, so the server starts a normal close (1000).
        let close = await ws.readFrame()
        #expect(close?.opcode == 0x8)
        #expect(close?.closeCode == 1000)
        try await ws.send(RawClient.frame(opcode: 0x8, payload: [0x03, 0xE8]))
        #expect(await ws.waitForClose())
        await server.shutdown()
    }

    @Test func fragmentedMessagesAreReassembled() async throws {
        let (server, port) = try await startServer(makeApp())
        let (ws, _) = try await RawClient.webSocket(port: port, path: "/echo")
        try await ws.sendText("frag", fin: false, opcode: 0x1)
        try await ws.send(RawClient.frame(opcode: 0x9, payload: Array("mid-ping".utf8)))  // control frames may interleave
        try await ws.sendText("men", fin: false, opcode: 0x0)
        try await ws.sendText("ted", fin: true, opcode: 0x0)
        #expect(await ws.readFrame() == .init(opcode: 0xA, payload: Array("mid-ping".utf8)))
        #expect(await ws.readFrame()?.text == "fragmented")
        ws.close()
        await server.shutdown()
    }

    @Test func answersPings() async throws {
        let (server, port) = try await startServer(makeApp())
        let (ws, _) = try await RawClient.webSocket(port: port, path: "/echo")
        try await ws.send(RawClient.frame(opcode: 0x9, payload: Array("are you there".utf8)))
        #expect(await ws.readFrame() == .init(opcode: 0xA, payload: Array("are you there".utf8)))
        ws.close()
        await server.shutdown()
    }

    @Test func sendsJSON() async throws {
        let (server, port) = try await startServer(makeApp())
        let (ws, _) = try await RawClient.webSocket(port: port, path: "/json")
        #expect(await ws.readFrame()?.text == #"{"hello":"world"}"#)
        ws.close()
        await server.shutdown()
    }

    @Test func middlewareCanRejectTheUpgrade() async throws {
        let (server, port) = try await startServer(makeApp())
        let (denied, deniedHead) = try await RawClient.webSocket(port: port, path: "/private")
        #expect(deniedHead.hasPrefix("HTTP/1.1 401"))
        #expect(await denied.wait { text, _ in text.contains("unauthorized") })
        #expect(await denied.waitForClose())

        let (allowed, allowedHead) = try await RawClient.webSocket(port: port, path: "/private?token=secret")
        expectUpgraded(allowedHead)
        #expect(await allowed.readFrame()?.text == "welcome")
        allowed.close()
        await server.shutdown()
    }

    @Test func originAllowListBlocksCrossSiteHijacking() async throws {
        let (server, port) = try await startServer(makeApp())
        let (evil, evilHead) = try await RawClient.webSocket(port: port, path: "/origin", headers: ["Origin": "https://evil.example"])
        #expect(evilHead.hasPrefix("HTTP/1.1 403"))
        evil.close()

        let (missing, missingHead) = try await RawClient.webSocket(port: port, path: "/origin")
        #expect(missingHead.hasPrefix("HTTP/1.1 403"))
        missing.close()

        let (good, goodHead) = try await RawClient.webSocket(port: port, path: "/origin", headers: ["Origin": "https://good.example"])
        expectUpgraded(goodHead)
        #expect(await good.readFrame()?.text == "ok")
        good.close()
        await server.shutdown()
    }

    @Test func sameOriginIsTheDefault() async throws {
        let (server, port) = try await startServer(makeApp())
        let (evil, evilHead) = try await RawClient.webSocket(port: port, path: "/echo", headers: ["Origin": "https://evil.example"])
        #expect(evilHead.hasPrefix("HTTP/1.1 403"), "cross-site pages can't open sockets without an allow-list")
        evil.close()
        let (same, sameHead) = try await RawClient.webSocket(port: port, path: "/echo", headers: ["Origin": "http://localhost"])
        expectUpgraded(sameHead)
        same.close()
        let (native, nativeHead) = try await RawClient.webSocket(port: port, path: "/echo")
        expectUpgraded(nativeHead)  // non-browser clients send no Origin
        native.close()
        #expect(PipelineFactory.isSameOrigin("https://App.example.com:443", host: "app.example.com"))
        #expect(PipelineFactory.isSameOrigin("http://a.test:8080", host: "a.test:8080"))
        #expect(!PipelineFactory.isSameOrigin("http://a.test:8080", host: "a.test"))
        #expect(!PipelineFactory.isSameOrigin("null", host: "a.test"))
        await server.shutdown()
    }

    @Test func badHandshakesGetAnAnswer() async throws {
        let (server, port) = try await startServer(makeApp())
        let base = "GET /echo HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
        let cases: [(String, String)] = [
            ("Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 99\r\n\r\n", "HTTP/1.1 426"),
            ("Sec-WebSocket-Version: 13\r\n\r\n", "HTTP/1.1 400"),
            ("Sec-WebSocket-Key: short\r\nSec-WebSocket-Version: 13\r\n\r\n", "HTTP/1.1 400"),
            ("Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\nContent-Length: 5\r\n\r\nhello", "HTTP/1.1 400"),
        ]
        for (rest, expected) in cases {
            let client = try await RawClient.connect(port: port)
            try await client.send(base + rest)
            let head = await client.responseHead()
            #expect(head?.hasPrefix(expected) == true, "\(rest.prefix(40)) -> \(head ?? "no response")")
            if expected == "HTTP/1.1 426" { #expect(head?.lowercased().contains("sec-websocket-version: 13") == true) }
            client.close()
        }
        await server.shutdown()
    }

    @Test func negotiatesSubprotocol() async throws {
        let (server, port) = try await startServer(makeApp())
        let (ws, head) = try await RawClient.webSocket(
            port: port, path: "/proto", headers: ["Sec-WebSocket-Protocol": "chat.v1, chat.v2"]
        )
        expectUpgraded(head)
        #expect(head.lowercased().contains("sec-websocket-protocol: chat.v2"))
        #expect(await ws.readFrame()?.text == "chat.v2")
        ws.close()
        await server.shutdown()
    }

    @Test func plainRequestsToSocketRoutesGet426() async throws {
        let (server, port) = try await startServer(makeApp())
        let client = try await RawClient.connect(port: port)
        try await client.send("GET /echo HTTP/1.1\r\nHost: x\r\n\r\n")
        #expect(try #require(await client.responseHead()).hasPrefix("HTTP/1.1 426"))
        client.close()

        // An upgrade request to a non-socket route is served as normal HTTP.
        let (other, head) = try await RawClient.webSocket(port: port, path: "/http")
        #expect(head.hasPrefix("HTTP/1.1 200"))
        other.close()
        await server.shutdown()
    }

    @Test func unmaskedClientFramesAreRejected() async throws {
        let (server, port) = try await startServer(makeApp())
        let (ws, _) = try await RawClient.webSocket(port: port, path: "/echo")
        try await ws.sendText("not masked", masked: false)
        #expect(await ws.readFrame()?.closeCode == 1002)
        await server.shutdown()
    }

    @Test func reservedBitsAreRejected() async throws {
        let (server, port) = try await startServer(makeApp())
        for rsv: UInt8 in [0x40, 0x20, 0x10] {
            let (ws, _) = try await RawClient.webSocket(port: port, path: "/echo")
            var frame = RawClient.frame(opcode: 0x1, payload: Array("hi".utf8))
            frame[0] |= rsv  // no extension negotiated: RFC 6455 says fail the connection
            try await ws.send(frame)
            #expect(await ws.readFrame()?.closeCode == 1002, "rsv bit \(rsv)")
            ws.close()
        }
        await server.shutdown()
    }

    @Test func invalidUTF8IsRejected() async throws {
        let (server, port) = try await startServer(makeApp())
        let (ws, _) = try await RawClient.webSocket(port: port, path: "/echo")
        try await ws.send(RawClient.frame(opcode: 0x1, payload: [0xC3, 0x28, 0xFF]))
        #expect(await ws.readFrame()?.closeCode == 1007)
        await server.shutdown()
    }

    @Test func oversizedFramesAndMessagesAreRejected() async throws {
        let (server, port) = try await startServer(makeApp())
        // One frame above maxFrameSize.
        let (ws1, _) = try await RawClient.webSocket(port: port, path: "/small")
        try await ws1.send(RawClient.frame(opcode: 0x2, payload: Array(repeating: 1, count: 4096)))
        #expect(await ws1.readFrame()?.closeCode == 1009)
        #expect(await ws1.waitForClose())

        // Small fragments that add up past maxMessageSize.
        let (ws2, _) = try await RawClient.webSocket(port: port, path: "/small")
        try await ws2.send(RawClient.frame(opcode: 0x2, payload: Array(repeating: 1, count: 600), fin: false))
        try await ws2.send(RawClient.frame(opcode: 0x0, payload: Array(repeating: 1, count: 600), fin: true))
        #expect(await ws2.readFrame()?.closeCode == 1009)
        await server.shutdown()
    }

    @Test func handlerErrorsCloseWith1011() async throws {
        let (server, port) = try await startServer(makeApp())
        let (ws, _) = try await RawClient.webSocket(port: port, path: "/throws")
        #expect(await ws.readFrame()?.closeCode == 1011)
        await server.shutdown()
    }

    @Test func silentPeersAreDisconnected() async throws {
        let (server, port) = try await startServer(makeApp())
        let (ws, _) = try await RawClient.webSocket(port: port, path: "/ping")
        // The server pings every 100 ms; our raw client never answers, so it's dropped after ~200 ms.
        #expect(await ws.readFrame()?.opcode == 0x9)
        #expect(await ws.waitForClose(timeout: .seconds(2)))
        await server.shutdown()
    }

    @Test func shutdownSendsGoingAway() async throws {
        let (server, port) = try await startServer(makeApp())
        let (ws, _) = try await RawClient.webSocket(port: port, path: "/echo")
        try await ws.sendText("x")
        #expect(await ws.readFrame()?.text == "x")
        async let stopped: Void = server.shutdown()
        let close = await ws.readFrame()
        #expect(close?.closeCode == 1001)
        try await ws.send(RawClient.frame(opcode: 0x8, payload: [0x03, 0xE9]))
        await stopped
        #expect(await ws.waitForClose())
    }

    @Test func manyConcurrentSockets() async throws {
        let (server, port) = try await startServer(makeApp())
        let ok = try await withThrowingTaskGroup(of: Int.self) { group in
            for i in 0..<100 {
                group.addTask {
                    let (ws, _) = try await RawClient.webSocket(port: port, path: "/echo")
                    defer { ws.close() }
                    var good = 0
                    for j in 0..<10 {
                        try await ws.sendText("m\(i)-\(j)")
                        if await ws.readFrame()?.text == "m\(i)-\(j)" { good += 1 }
                    }
                    return good
                }
            }
            return try await group.reduce(0, +)
        }
        #expect(ok == 1000)
        await server.shutdown()
    }

    @Test func hubBroadcastsToRoomMembersOnly() async throws {
        let app = Oria(configuration: testConfig())
        let hub = WebSocketHub()
        app.ws("/rooms/:room") { req, ws in
            let room = req.params["room"]!
            hub.join(room, ws)
            defer { hub.leave(room, ws) }
            for await message in ws.messages {
                if case .text(let text) = message { hub.broadcast(text, to: room) }
            }
        }
        let (server, port) = try await startServer(app)
        let (a, _) = try await RawClient.webSocket(port: port, path: "/rooms/lobby")
        let (b, _) = try await RawClient.webSocket(port: port, path: "/rooms/lobby")
        let (other, _) = try await RawClient.webSocket(port: port, path: "/rooms/other")
        while hub.count(in: "lobby") < 2 || hub.count(in: "other") < 1 { try await Task.sleep(for: .milliseconds(5)) }
        try await a.sendText("hello lobby")
        #expect(await a.readFrame()?.text == "hello lobby")
        #expect(await b.readFrame()?.text == "hello lobby")
        #expect(await other.readFrame(timeout: .milliseconds(300)) == nil)
        a.close()
        b.close()
        other.close()
        await server.shutdown()
    }

    @Test func slowClientsCannotStallABroadcast() async throws {
        let app = Oria(configuration: testConfig())
        let hub = WebSocketHub()
        app.ws("/feed", options: .init(outboxLimit: 16)) { _, ws in
            hub.join("feed", ws)
            defer { hub.leave("feed", ws) }
            for await _ in ws.messages {}
        }
        let (server, port) = try await startServer(app)
        let (fast, _) = try await RawClient.webSocket(port: port, path: "/feed")
        let (slow, _) = try await RawClient.webSocket(port: port, path: "/feed")
        try await slow.channel.setOption(.autoRead, value: false)  // stops reading: its buffers fill up
        while hub.count(in: "feed") < 2 { try await Task.sleep(for: .milliseconds(5)) }

        let chunk = ByteBuffer(repeating: UInt8(ascii: "x"), count: 64 * 1024)
        var received = 0
        for _ in 0..<200 {  // 12.5 MB per client: far more than socket buffers hold
            hub.broadcast(chunk, to: "feed", binary: true)
            if await fast.readFrame(timeout: .seconds(5))?.payload.count == chunk.readableBytes { received += 1 }
        }
        #expect(received == 200, "the fast client must get every message despite the stalled one")
        // The stalled client overflowed its outbox and was disconnected, leaving the room.
        let deadline = ContinuousClock.now + .seconds(5)
        while hub.count(in: "feed") > 1 && ContinuousClock.now < deadline { try await Task.sleep(for: .milliseconds(20)) }
        #expect(hub.count(in: "feed") == 1)
        fast.close()
        slow.close()
        await server.shutdown()
    }

    @Test func messageQueueAppliesBackpressure() async throws {
        let queue = MessageQueue(capacity: 2)
        await queue.push(.text("1"))
        let third = Task {
            await queue.push(.text("2"))  // fills the queue: suspends until consumed
            await queue.push(.text("3"))
            return true
        }
        try await Task.sleep(for: .milliseconds(50))
        #expect(await queue.next() == .text("1"))
        #expect(await queue.next() == .text("2"))
        #expect(await third.value)
        #expect(await queue.next() == .text("3"))
        queue.finish()
        #expect(await queue.next() == nil)
    }
}
