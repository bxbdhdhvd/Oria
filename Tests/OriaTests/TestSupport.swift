import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOHTTP1
import NIOHTTP2
import NIOPosix
import NIOSSL
import Testing

@testable import Oria

/// A self-signed certificate generated once per test run with the `openssl` CLI.
enum TestTLS {
    static let files: (cert: String, key: String) = {
        let dir = NSTemporaryDirectory() + "oria-tls-\(UUID().uuidString)"
        try! FileManager.default.createDirectory(atPath: dir, withIntermediateDirectories: true)
        let cert = dir + "/cert.pem"
        let key = dir + "/key.pem"
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
        process.arguments = [
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", key, "-out", cert,
            "-days", "2", "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
        ]
        process.standardOutput = FileHandle.nullDevice
        process.standardError = FileHandle.nullDevice
        try! process.run()
        process.waitUntilExit()
        precondition(process.terminationStatus == 0, "openssl is required for TLS tests")
        return (cert, key)
    }()

    static func serverOptions() throws -> TLSOptions {
        try .files(certificateChain: files.cert, privateKey: files.key)
    }

    static func clientContext(alpn: [String], maxVersion: TLSVersion? = nil) throws -> NIOSSLContext {
        var config = TLSConfiguration.makeClientConfiguration()
        config.certificateVerification = .none
        config.applicationProtocols = alpn
        if let maxVersion {
            config.minimumTLSVersion = .tlsv1
            config.maximumTLSVersion = maxVersion
        }
        return try NIOSSLContext(configuration: config)
    }
}

/// Starts an app on a random port with test-friendly defaults.
func startServer(_ app: Oria) async throws -> (Server, Int) {
    let server = try await app.start(port: 0, host: "127.0.0.1")
    return (server, try #require(server.port))
}

func testConfig(_ configure: (inout Oria.Configuration) -> Void = { _ in }) -> Oria.Configuration {
    var config = Oria.Configuration()
    config.threads = 2
    config.handleSignals = false
    configure(&config)
    return config
}

// MARK: - Raw TCP/TLS client

/// Collects every byte the server sends, so tests can craft arbitrary (even malicious) input and
/// inspect raw output.
final class RawClient: Sendable {
    final class Collector: ChannelInboundHandler, Sendable {
        typealias InboundIn = ByteBuffer
        let buffer = NIOLockedValueBox(ByteBuffer())
        let closed = ManagedAtomicBool()

        func channelRead(context: ChannelHandlerContext, data: NIOAny) {
            var incoming = unwrapInboundIn(data)
            buffer.withLockedValue { $0.writeBuffer(&incoming) }
        }

        func channelInactive(context: ChannelHandlerContext) {
            closed.set()
            context.fireChannelInactive()
        }

        func errorCaught(context: ChannelHandlerContext, error: Error) {
            context.close(promise: nil)
        }
    }

    let channel: Channel
    let collector: Collector

    private init(channel: Channel, collector: Collector) {
        self.channel = channel
        self.collector = collector
    }

    static func connect(
        port: Int, tls: Bool = false, alpn: [String] = ["http/1.1"], context: NIOSSLContext? = nil
    ) async throws -> RawClient {
        let collector = Collector()
        let sslContext = try context ?? (tls ? TestTLS.clientContext(alpn: alpn) : nil)
        let channel = try await ClientBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .channelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    if let sslContext {
                        try channel.pipeline.syncOperations.addHandler(
                            try NIOSSLClientHandler(context: sslContext, serverHostname: nil)
                        )
                    }
                    try channel.pipeline.syncOperations.addHandler(collector)
                }
            }
            .connect(host: "127.0.0.1", port: port).get()
        return RawClient(channel: channel, collector: collector)
    }

    func send(_ string: String) async throws {
        try await channel.writeAndFlush(ByteBuffer(string: string))
    }

    func send(_ bytes: [UInt8]) async throws {
        try await channel.writeAndFlush(ByteBuffer(bytes: bytes))
    }

    var text: String { collector.buffer.withLockedValue { String(buffer: $0) } }
    var isClosed: Bool { collector.closed.value }

    /// Polls until `condition` holds or the timeout passes.
    @discardableResult
    func wait(timeout: Duration = .seconds(3), until condition: (String, Bool) -> Bool) async -> Bool {
        let deadline = ContinuousClock.now + timeout
        while ContinuousClock.now < deadline {
            if condition(text, isClosed) { return true }
            try? await Task.sleep(for: .milliseconds(10))
        }
        return condition(text, isClosed)
    }

    func waitForClose(timeout: Duration = .seconds(3)) async -> Bool {
        await wait(timeout: timeout) { _, closed in closed }
    }

    /// Reads one complete HTTP response head (status line + headers).
    func responseHead(timeout: Duration = .seconds(3)) async -> String? {
        guard await wait(timeout: timeout, until: { text, _ in text.contains("\r\n\r\n") }) else { return nil }
        return collector.buffer.withLockedValue { buffer in
            let all = String(buffer: buffer)
            let head = String(all[..<all.range(of: "\r\n\r\n")!.upperBound])
            buffer.moveReaderIndex(forwardBy: head.utf8.count)
            buffer.discardReadBytes()
            return head
        }
    }

    func close() {
        channel.close(promise: nil)
    }

    // MARK: WebSocket helpers

    /// Opens a WebSocket handshake; returns the response head.
    static func webSocket(
        port: Int, path: String, headers: [String: String] = [:], tls: Bool = false
    ) async throws -> (RawClient, String) {
        let client = try await connect(port: port, tls: tls)
        var request = "GET \(path) HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
        request += "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n"
        for (name, value) in headers { request += "\(name): \(value)\r\n" }
        request += "\r\n"
        try await client.send(request)
        let head = try #require(await client.responseHead())
        return (client, head)
    }

    static func frame(opcode: UInt8, payload: [UInt8], fin: Bool = true, masked: Bool = true) -> [UInt8] {
        var out: [UInt8] = [(fin ? 0x80 : 0) | opcode]
        let maskBit: UInt8 = masked ? 0x80 : 0
        let count = payload.count
        if count < 126 {
            out.append(maskBit | UInt8(count))
        } else if count <= 0xFFFF {
            out.append(maskBit | 126)
            out += [UInt8(count >> 8), UInt8(count & 0xFF)]
        } else {
            out.append(maskBit | 127)
            for shift in stride(from: 56, through: 0, by: -8) { out.append(UInt8((count >> shift) & 0xFF)) }
        }
        if masked {
            let key: [UInt8] = [0x12, 0x34, 0x56, 0x78]
            out += key
            out += payload.enumerated().map { $0.element ^ key[$0.offset % 4] }
        } else {
            out += payload
        }
        return out
    }

    func sendText(_ text: String, fin: Bool = true, opcode: UInt8 = 0x1, masked: Bool = true) async throws {
        try await send(Self.frame(opcode: opcode, payload: Array(text.utf8), fin: fin, masked: masked))
    }

    struct Frame: Equatable {
        var opcode: UInt8
        var payload: [UInt8]
        var text: String { String(decoding: payload, as: UTF8.self) }
        /// Close code for close frames.
        var closeCode: Int? {
            guard opcode == 0x8, payload.count >= 2 else { return nil }
            return Int(payload[0]) << 8 | Int(payload[1])
        }
    }

    /// Reads the next server frame (server frames are never masked).
    func readFrame(timeout: Duration = .seconds(3)) async -> Frame? {
        let deadline = ContinuousClock.now + timeout
        while ContinuousClock.now < deadline {
            let frame: Frame? = collector.buffer.withLockedValue { buffer in
                let bytes = Array(buffer.readableBytesView)
                guard bytes.count >= 2 else { return nil }
                var length = Int(bytes[1] & 0x7F)
                var offset = 2
                if length == 126 {
                    guard bytes.count >= 4 else { return nil }
                    length = Int(bytes[2]) << 8 | Int(bytes[3])
                    offset = 4
                } else if length == 127 {
                    guard bytes.count >= 10 else { return nil }
                    length = bytes[2..<10].reduce(0) { $0 << 8 | Int($1) }
                    offset = 10
                }
                guard bytes.count >= offset + length else { return nil }
                buffer.moveReaderIndex(forwardBy: offset + length)
                buffer.discardReadBytes()
                return Frame(opcode: bytes[0] & 0x0F, payload: Array(bytes[offset..<(offset + length)]))
            }
            if let frame { return frame }
            if isClosed { return nil }
            try? await Task.sleep(for: .milliseconds(5))
        }
        return nil
    }
}

final class ManagedAtomicBool: Sendable {
    private let box = NIOLockedValueBox(false)
    func set() { box.withLockedValue { $0 = true } }
    var value: Bool { box.withLockedValue { $0 } }
}

// MARK: - HTTP/2 client

struct H2Client: Sendable {
    typealias Stream = NIOAsyncChannel<HTTPClientResponsePart, HTTPClientRequestPart>

    let channel: Channel
    let multiplexer: NIOHTTP2Handler.AsyncStreamMultiplexer<Bool>

    static func connect(port: Int) async throws -> H2Client {
        let sslContext = try TestTLS.clientContext(alpn: ["h2"])
        return try await ClientBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .connect(host: "127.0.0.1", port: port) { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(
                        try NIOSSLClientHandler(context: sslContext, serverHostname: nil)
                    )
                    let multiplexer = try channel.pipeline.syncOperations.configureAsyncHTTP2Pipeline(
                        mode: .client
                    ) { stream in stream.eventLoop.makeSucceededFuture(true) }
                    return H2Client(channel: channel, multiplexer: multiplexer)
                }
            }
    }

    struct Response {
        var head: HTTPResponseHead
        var body: String
    }

    func request(
        _ method: HTTPMethod, _ path: String, headers: HTTPHeaders = [:], body: String? = nil
    ) async throws -> Response {
        let stream = try await multiplexer.openStream { channel in
            channel.eventLoop.makeCompletedFuture {
                try channel.pipeline.syncOperations.addHandler(HTTP2FramePayloadToHTTP1ClientCodec(httpProtocol: .https))
                return try Stream(wrappingChannelSynchronously: channel)
            }
        }
        return try await stream.executeThenClose { inbound, outbound in
            var allHeaders: HTTPHeaders = ["host": "localhost"]
            allHeaders.add(contentsOf: headers)
            if let body { allHeaders.add(name: "content-length", value: String(body.utf8.count)) }
            try await outbound.write(.head(HTTPRequestHead(version: .http1_1, method: method, uri: path, headers: allHeaders)))
            if let body { try await outbound.write(.body(.byteBuffer(ByteBuffer(string: body)))) }
            try await outbound.write(.end(nil))
            var head: HTTPResponseHead?
            var collected = ByteBuffer()
            for try await part in inbound {
                switch part {
                case .head(let h): head = h
                case .body(var b): collected.writeBuffer(&b)
                case .end: return Response(head: try #require(head), body: String(buffer: collected))
                }
            }
            throw ChannelError.eof
        }
    }

    func close() { channel.close(promise: nil) }
}
