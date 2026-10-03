import Foundation
import Oria

struct User: Codable, Sendable {
    var id: Int
    var name: String
}

/// A tiny in-memory store, safe to use from every event-loop thread.
actor UserStore {
    private var users: [Int: User] = [1: User(id: 1, name: "Ada")]
    private var nextID = 2

    func all() -> [User] { users.values.sorted { $0.id < $1.id } }
    func find(_ id: Int) -> User? { users[id] }
    func create(name: String) -> User {
        defer { nextID += 1 }
        let user = User(id: nextID, name: name)
        users[user.id] = user
        return user
    }
    func remove(_ id: Int) -> Bool { users.removeValue(forKey: id) != nil }
}

let env = ProcessInfo.processInfo.environment

// Opt-in: run handlers on the I/O threads (must happen before any await). See README before enabling.
if env["EVENT_LOOP_EXECUTOR"] == "1" {
    Oria.runConcurrencyOnEventLoops(threads: env["THREADS"].flatMap(Int.init) ?? System.coreCount)
}
var config = Oria.Configuration()
if let threads = env["THREADS"].flatMap(Int.init) { config.threads = threads }
config.compression = env["COMPRESSION"] == "1"
config.reusePort = env["REUSE_PORT"] == "1"
if env["HANDLERS_ON_LOOP"] == "0" { config.runHandlersOnEventLoops = false }
if let maxBody = env["MAX_BODY"].flatMap(Int.init) { config.maxBodySize = maxBody }
// HTTPS + HTTP/2: TLS_CERT=cert.pem TLS_KEY=key.pem
if let cert = env["TLS_CERT"], let key = env["TLS_KEY"] {
    config.tls = try .files(certificateChain: cert, privateKey: key)
}

let app = Oria(configuration: config)
let store = UserStore()

if env["LOG"] == "1" { app.use(logger()) }
// BENCH=1 skips the demo middleware so benchmarks measure the framework itself.
if env["BENCH"] != "1" { app.use(cors()) }

app.get("/") { _, res in
    res.send("Hello from Oria!")
}

app.get("/json") { _, res in
    try res.json(["message": "Hello, World!"])
}

// A mounted router, like express.Router().
let api = Router()
api.use(rateLimit(max: env["RATE_LIMIT"].flatMap(Int.init) ?? 10_000, window: .seconds(60)))

api.get("/users") { _, res in
    try res.json(await store.all())
}

api.get("/users/:id") { req, res in
    guard let id = req.params["id"].flatMap(Int.init), let user = await store.find(id) else {
        throw HTTPError(.notFound, "User not found")
    }
    try res.json(user)
}

struct NewUser: Decodable { var name: String }

api.post("/users") { req, res in
    let input = try req.json(NewUser.self)
    let user = await store.create(name: input.name)
    try res.status(.created).json(user)
}

api.delete("/users/:id") { req, res in
    guard let id = req.params["id"].flatMap(Int.init), await store.remove(id) else {
        throw HTTPError(.notFound, "User not found")
    }
    res.sendStatus(.noContent)
}

app.use("/api", api)

// Server-sent events / streaming.
app.get("/stream") { _, res in
    res.type("text/event-stream").set("cache-control", "no-cache")
    res.stream { writer in
        for i in 1...3 {
            try await writer.write("data: tick \(i)\n\n")
            try await Task.sleep(for: .milliseconds(100))
        }
    }
}

// WebSockets.
app.ws("/ws/echo") { _, ws in
    for await message in ws.messages {
        switch message {
        case .text(let text): try await ws.send(text)
        case .binary(let data): try await ws.send(data)
        }
    }
}

// Chat rooms: WebSocketHub broadcasts without blocking, so a slow client can't stall a room.
let chat = WebSocketHub()

app.ws("/ws/chat/:room") { req, ws in
    let room = req.params["room"]!
    chat.join(room, ws)
    defer { chat.leave(room, ws) }
    for await message in ws.messages {
        if case .text(let text) = message { chat.broadcast(text, to: room) }
    }
}

// MARK: Uploads, files and large bodies

let filesDir = env["FILES_DIR"] ?? (NSTemporaryDirectory() + "oria-files")
try FileManager.default.createDirectory(atPath: filesDir, withIntermediateDirectories: true)

/// Only plain names: no path separators, no leading dot, bounded length.
func safeName(_ req: Request) throws -> String {
    guard let name = req.params["name"], (1...128).contains(name.utf8.count), !name.hasPrefix("."),
        name.utf8.allSatisfy({ $0 == UInt8(ascii: ".") || $0 == UInt8(ascii: "-") || $0 == UInt8(ascii: "_")
            || (48...57).contains($0) || (65...90).contains($0) || (97...122).contains($0) })
    else { throw HTTPError(.badRequest, "Invalid file name") }
    return name
}

// multipart/form-data, streamed to disk (constant memory whatever the file size).
app.upload("/upload", options: UploadOptions(maxBodySize: 4 << 30, maxFileSize: 2 << 30, maxFiles: 10, directory: filesDir)) { req, res in
    let form = req.uploads ?? MultipartForm()
    try res.json([
        "fields": form.fields.map { "\($0.key)=\($0.value)" }.sorted(),
        "files": form.files.map { "\($0.field):\($0.filename ?? "-"):\($0.size):\($0.contentType)" },
    ])
}

// Raw body upload: PUT /files/backup.tar (also streamed to disk), then moved into place.
app.upload("/files/:name", method: .PUT, options: UploadOptions(maxBodySize: 4 << 30, directory: filesDir)) { req, res in
    let name = try safeName(req)
    guard let body = req.uploadedBody else { throw HTTPError(.badRequest) }
    let target = filesDir + "/" + name
    try? FileManager.default.removeItem(atPath: target)
    try await body.move(to: target)
    try res.status(.created).json(["name": name, "size": String(body.size)])
}

// Download with Range / If-Range / ETag / 304 support (resumable, seekable video, etc.).
app.get("/files/:name") { req, res in
    try await res.sendFile(filesDir + "/" + (try safeName(req)), for: req)
}

// Large generated response: GET /bytes/1048576 streams that many bytes with backpressure.
let megabyte = ByteBuffer(repeating: UInt8(ascii: "x"), count: 1 << 20)
app.get("/bytes/:count") { req, res in
    guard let count = req.params["count"].flatMap(Int.init), (0...(16 << 30)).contains(count) else {
        throw HTTPError(.badRequest)
    }
    res.type("application/octet-stream")
    res.stream(length: count) { writer in
        var left = count
        while left > 0 {
            let n = min(left, megabyte.readableBytes)
            try await writer.write(megabyte.getSlice(at: 0, length: n)!)
            left -= n
        }
    }
}

// Large request body that is consumed as a stream and discarded: reports its size.
app.upload("/sink", options: UploadOptions(maxBodySize: 16 << 30, directory: filesDir)) { req, res in
    res.send(String(req.uploadedBody?.size ?? req.uploads?.files.reduce(0) { $0 + $1.size } ?? 0))
}

if let dir = env["STATIC_DIR"] {
    app.use("/public", serveStatic(dir))
}

let port = env["PORT"].flatMap(Int.init) ?? 3000
try await app.listen(port)
print("Oria stopped gracefully")
