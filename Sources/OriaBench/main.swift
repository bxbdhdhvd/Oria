// WebSocket load generator for Oria (or any WebSocket server).
//
//   oria-bench echo      <url> <connections> <seconds> [payloadBytes]
//   oria-bench broadcast <url> <receivers> <messages>
//   oria-bench idle      <url> <connections> <seconds>
//
// echo:      every connection sends a message, waits for the echo, repeats. Reports msgs/s + latency.
// broadcast: N receivers join a room, one sender sends M messages; reports deliveries/s + latency.
// idle:      opens N connections and holds them (measure server memory per connection).
import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOHTTP1
import NIOPosix
import NIOWebSocket

typealias WS = NIOAsyncChannel<WebSocketFrame, WebSocketFrame>

/// Unbuffered, so progress shows up immediately even when output is redirected to a file.
func say(_ line: String) { FileHandle.standardOutput.write(Data((line + "\n").utf8)) }

enum Upgrade: Sendable {
    case websocket(WS)
    case notUpgraded
}

struct Target {
    let host: String
    let port: Int
    let path: String

    init(_ url: String) {
        let rest = url.replacingOccurrences(of: "ws://", with: "")
        let slash = rest.firstIndex(of: "/") ?? rest.endIndex
        let hostPort = rest[..<slash].split(separator: ":")
        host = String(hostPort[0])
        port = hostPort.count > 1 ? Int(hostPort[1])! : 80
        path = slash == rest.endIndex ? "/" : String(rest[slash...])
    }
}

let group = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)

func connect(_ target: Target) async throws -> WS {
    let upgrade = try await ClientBootstrap(group: group)
        .channelOption(.socketOption(.tcp_nodelay), value: 1)
        .connect(host: target.host, port: target.port) { channel in
            channel.eventLoop.makeCompletedFuture {
                let upgrader = NIOTypedWebSocketClientUpgrader<Upgrade>(maxFrameSize: 1 << 24) { channel, _ in
                    channel.eventLoop.makeCompletedFuture { .websocket(try WS(wrappingChannelSynchronously: channel)) }
                }
                var headers = HTTPHeaders()
                headers.add(name: "Host", value: "\(target.host):\(target.port)")
                let head = HTTPRequestHead(version: .http1_1, method: .GET, uri: target.path, headers: headers)
                let config = NIOTypedHTTPClientUpgradeConfiguration<Upgrade>(
                    upgradeRequestHead: head, upgraders: [upgrader],
                    notUpgradingCompletionHandler: { $0.eventLoop.makeSucceededFuture(.notUpgraded) }
                )
                return try channel.pipeline.syncOperations.configureUpgradableHTTPClientPipeline(
                    configuration: .init(upgradeConfiguration: config)
                )
            }
        }
    guard case .websocket(let ws) = try await upgrade.get() else { throw ChannelError.inappropriateOperationForState }
    return ws
}

func textFrame(_ text: String) -> WebSocketFrame {
    WebSocketFrame(fin: true, opcode: .text, maskKey: .random(), data: ByteBuffer(string: text))
}

func now() -> UInt64 { DispatchTime.now().uptimeNanoseconds }

func report(_ name: String, count: Int, seconds: Double, latenciesNs: [UInt64]) {
    let sorted = latenciesNs.sorted()
    func pct(_ p: Double) -> String {
        guard !sorted.isEmpty else { return "-" }
        let v = Double(sorted[min(sorted.count - 1, Int(Double(sorted.count) * p))]) / 1e6
        return String(format: "%.2fms", v)
    }
    say("\(name): \(count) messages in \(String(format: "%.1f", seconds))s = \(Int(Double(count) / seconds)) msg/s")
    say("  latency p50 \(pct(0.50))  p90 \(pct(0.90))  p99 \(pct(0.99))  max \(pct(1.0))")
}

/// Opens connections in parallel batches (avoids a SYN storm against the listen backlog).
func openMany(_ target: Target, _ n: Int) async throws -> [WS] {
    var sockets: [WS] = []
    sockets.reserveCapacity(n)
    var opened = 0
    while opened < n {
        let batch = min(200, n - opened)
        let more = try await withThrowingTaskGroup(of: WS.self) { g in
            for _ in 0..<batch { g.addTask { try await connect(target) } }
            var opened: [WS] = []
            for try await ws in g { opened.append(ws) }
            return opened
        }
        sockets += more
        opened += batch
    }
    return sockets
}

let args = CommandLine.arguments
guard args.count >= 5 else {
    say("usage: oria-bench echo|broadcast|idle <ws://host:port/path> <n> <seconds|messages> [payloadBytes]")
    exit(1)
}
let target = Target(args[2])
let n = Int(args[3])!

switch args[1] {
case "echo":
    let seconds = Double(args[4])!
    let payload = String(repeating: "x", count: args.count > 5 ? Int(args[5])! : 32)
    let sockets = try await openMany(target, n)
    say("opened \(sockets.count) connections")
    let deadline = now() + UInt64(seconds * 1e9)
    let start = now()
    let results = try await withThrowingTaskGroup(of: [UInt64].self) { g in
        for ws in sockets {
            g.addTask {
                var latencies: [UInt64] = []
                try await ws.executeThenClose { inbound, outbound in
                    var it = inbound.makeAsyncIterator()
                    while now() < deadline {
                        let t0 = now()
                        try await outbound.write(textFrame(payload))
                        guard let reply = try await it.next(), reply.opcode == .text else { break }
                        latencies.append(now() - t0)
                    }
                    try? await outbound.write(WebSocketFrame(fin: true, opcode: .connectionClose, maskKey: .random(), data: ByteBuffer(bytes: [0x03, 0xE8])))
                }
                return latencies
            }
        }
        var all: [UInt64] = []
        for try await part in g { all += part }
        return all
    }
    report("echo (\(n) connections, \(payload.utf8.count)B)", count: results.count, seconds: Double(now() - start) / 1e9, latenciesNs: results)

case "broadcast":
    let messages = Int(args[4])!
    let receivers = try await openMany(target, n)
    let sender = try await connect(target)
    say("opened \(receivers.count) receivers + 1 sender")
    try await Task.sleep(for: .milliseconds(500))  // let every join register
    let start = now()
    let latencies = NIOLockedValueBox<[UInt64]>([])
    try await withThrowingTaskGroup(of: Void.self) { g in
        for ws in receivers {
            g.addTask {
                var got: [UInt64] = []
                try await ws.executeThenClose { inbound, outbound in
                    for try await frame in inbound {
                        if frame.opcode == .ping {
                            try await outbound.write(WebSocketFrame(fin: true, opcode: .pong, maskKey: .random(), data: frame.unmaskedData))
                            continue
                        }
                        guard frame.opcode == .text else { continue }
                        let sentAt = UInt64(String(buffer: frame.unmaskedData)) ?? 0
                        got.append(now() - sentAt)
                        if got.count == messages { break }
                    }
                }
                latencies.withLockedValue { $0 += got }
            }
        }
        g.addTask {
            try await sender.executeThenClose { inbound, outbound in
                // Drain our own copies of the broadcast (we're a room member too).
                let drain = Task { for try await _ in inbound {} }
                for _ in 0..<messages {
                    try await outbound.write(textFrame(String(now())))
                    try await Task.sleep(for: .milliseconds(2))
                }
                try await Task.sleep(for: .seconds(2))
                drain.cancel()
            }
        }
        try await g.waitForAll()
    }
    let all = latencies.withLockedValue { $0 }
    report("broadcast (\(n) receivers x \(messages) msgs)", count: all.count, seconds: Double(now() - start) / 1e9, latenciesNs: all)

case "idle":
    let seconds = Double(args[4])!
    let sockets = try await openMany(target, n)
    say("holding \(sockets.count) idle connections for \(Int(seconds))s")
    try await Task.sleep(for: .seconds(seconds))
    for ws in sockets { try? await ws.channel.close() }

default:
    say("unknown mode \(args[1])")
}
try await group.shutdownGracefully()
