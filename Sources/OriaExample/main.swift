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
// HTTPS + HTTP/2: TLS_CERT=cert.pem TLS_KEY=key.pem
if let cert = env["TLS_CERT"], let key = env["TLS_KEY"] {
    config.tls = try .files(certificateChain: cert, privateKey: key)
}

let app = Oria(configuration: config)
let store = UserStore()

if env["LOG"] == "1" { app.use(logger()) }
app.use(cors())

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

if let dir = env["STATIC_DIR"] {
    app.use("/public", serveStatic(dir))
}

let port = env["PORT"].flatMap(Int.init) ?? 3000
try await app.listen(port)
print("Oria stopped gracefully")
