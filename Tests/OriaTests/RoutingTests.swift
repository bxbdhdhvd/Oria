import Foundation
import NIOHTTP1
import Testing

@testable import Oria

@Suite struct RoutingTests {
    @Test func sendsText() async throws {
        let app = Oria()
        app.get("/") { _, res in res.send("hello") }
        let r = try await app.test(.GET, "/")
        #expect(r.status == .ok)
        #expect(r.text == "hello")
        #expect(r.headers.first(name: "content-type") == "text/html; charset=utf-8")
    }

    @Test func jsonAndParams() async throws {
        struct Out: Codable, Equatable { var id: String; var post: String }
        let app = Oria()
        app.get("/users/:id/posts/:post") { req, res in
            try res.json(Out(id: req.params["id"]!, post: req.params["post"]!))
        }
        let r = try await app.test(.GET, "/users/42/posts/hello%20world")
        #expect(try r.json(Out.self) == Out(id: "42", post: "hello world"))
        #expect(r.headers.first(name: "content-type") == "application/json; charset=utf-8")
    }

    @Test func queryString() async throws {
        let app = Oria()
        app.get("/search") { req, res in
            res.send("\(req.query["q"] ?? "-")|\(req.query["page"] ?? "-")|\(req.queryAll.count)")
        }
        let r = try await app.test(.GET, "/search?q=swift+nio%21&page=2&page=3")
        #expect(r.text == "swift nio!|3|3")
    }

    @Test func notFoundDefault() async throws {
        let app = Oria()
        let r = try await app.test(.GET, "/nope")
        #expect(r.status == .notFound)
        #expect(r.text.contains("Cannot GET /nope"))
    }

    @Test func methodMismatchIs404() async throws {
        let app = Oria()
        app.post("/only-post") { _, res in res.send("ok") }
        #expect(try await app.test(.GET, "/only-post").status == .notFound)
        #expect(try await app.test(.POST, "/only-post").status == .ok)
    }

    @Test func httpErrorAndGenericError() async throws {
        struct Boom: Error {}
        let app = Oria()
        app.get("/teapot") { _, _ in throw HTTPError(418, "short and stout") }
        app.get("/boom") { _, res in
            res.set("x-should-be-cleared", "1")
            throw Boom()
        }
        let teapot = try await app.test(.GET, "/teapot")
        #expect(teapot.status.code == 418)
        #expect(teapot.text == #"{"error":"short and stout"}"#)

        let boom = try await app.test(.GET, "/boom")
        #expect(boom.status == .internalServerError)
        #expect(boom.headers.first(name: "x-should-be-cleared") == nil)
    }

    @Test func middlewareOrderAndLocals() async throws {
        let app = Oria()
        app.use { req, res, next in
            req.locals["trace"] = "a"
            try await next()
            res.set("x-after", "done")
        }
        app.use { req, _, next in
            req.locals["trace"] = (req.locals["trace"] as! String) + "b"
            try await next()
        }
        app.get("/") { req, res in res.send(req.locals["trace"] as! String + "c") }
        let r = try await app.test(.GET, "/")
        #expect(r.text == "abc")
        #expect(r.headers.first(name: "x-after") == "done")
    }

    @Test func routeMiddlewareCanShortCircuit() async throws {
        let app = Oria()
        let requireAuth: Middleware = { req, res, next in
            guard req.header("authorization") == "Bearer secret" else {
                res.status(.unauthorized).json(raw: #"{"error":"unauthorized"}"#)
                return
            }
            try await next()
        }
        app.get("/admin", requireAuth) { _, res in res.send("welcome") }

        #expect(try await app.test(.GET, "/admin").status == .unauthorized)
        let ok = try await app.test(.GET, "/admin", headers: ["authorization": "Bearer secret"])
        #expect(ok.text == "welcome")
    }

    @Test func mountedRoutersAndScopedMiddleware() async throws {
        let app = Oria()
        let api = Router()
        let v1 = Router()
        api.use { _, res, next in
            res.set("x-api", "1")
            try await next()
        }
        v1.get("/items/:id") { req, res in res.send("item \(req.params["id"]!)") }
        api.use("/v1", v1)
        app.use("/api", api)
        app.get("/outside") { _, res in res.send("outside") }
        app.use("/api") { req, res, next in
            res.set("x-subpath", req.subpath)
            try await next()
        }

        let inside = try await app.test(.GET, "/api/v1/items/7")
        #expect(inside.text == "item 7")
        #expect(inside.headers.first(name: "x-api") == "1")
        #expect(inside.headers.first(name: "x-subpath") == "/v1/items/7")

        let outside = try await app.test(.GET, "/outside")
        #expect(outside.headers.first(name: "x-api") == nil)

        // Prefix matching is per segment: /apix is not under /api.
        let notPrefix = try await app.test(.GET, "/apix")
        #expect(notPrefix.headers.first(name: "x-api") == nil)
    }

    @Test func staticBeatsParamBeatsWildcard() async throws {
        let app = Oria()
        app.get("/users/me") { _, res in res.send("me") }
        app.get("/users/:id") { req, res in res.send("id=\(req.params["id"]!)") }
        app.get("/files/*") { req, res in res.send("file=\(req.params["*"]!)") }

        #expect(try await app.test(.GET, "/users/me").text == "me")
        #expect(try await app.test(.GET, "/users/9").text == "id=9")
        #expect(try await app.test(.GET, "/users/9/").text == "id=9")
        #expect(try await app.test(.GET, "/files/a/b/c.txt").text == "file=a/b/c.txt")
        #expect(try await app.test(.GET, "/files").text == "file=")
    }

    @Test func backtracksAcrossBranches() async throws {
        let app = Oria()
        app.get("/a/static/x") { _, res in res.send("static") }
        app.get("/a/:p/y") { req, res in res.send("param \(req.params["p"]!)") }
        #expect(try await app.test(.GET, "/a/static/y").text == "param static")
        #expect(try await app.test(.GET, "/a/static/x").text == "static")
    }

    @Test func headFallsBackToGetWithoutBody() async throws {
        let app = Oria()
        app.get("/") { _, res in res.send("body") }
        let r = try await app.test(.HEAD, "/")
        #expect(r.status == .ok)
        #expect(r.text == "")
    }

    @Test func allMatchesAnyMethod() async throws {
        let app = Oria()
        app.all("/any") { req, res in res.send(req.method.rawValue) }
        #expect(try await app.test(.PATCH, "/any").text == "PATCH")
        #expect(try await app.test(.DELETE, "/any").text == "DELETE")
    }

    @Test func customErrorAndNotFoundHandlers() async throws {
        let app = Oria()
        app.get("/fail") { _, _ in throw HTTPError(.badGateway) }
        app.onError { error, _, res in res.status(.serviceUnavailable).send("custom: \(error)") }
        app.notFound { _, res in res.status(.notFound).send("nothing here") }
        #expect(try await app.test(.GET, "/fail").text == "custom: 502 Bad Gateway")
        #expect(try await app.test(.GET, "/missing").text == "nothing here")
    }

    @Test func routesAddedAfterFirstRequestArePickedUp() async throws {
        let app = Oria()
        app.get("/a") { _, res in res.send("a") }
        #expect(try await app.test(.GET, "/b").status == .notFound)
        app.get("/b") { _, res in res.send("b") }
        #expect(try await app.test(.GET, "/b").text == "b")
    }

    @Test func jsonBodyDecoding() async throws {
        struct In: Decodable { var name: String }
        let app = Oria()
        app.post("/echo") { req, res in res.send(try req.json(In.self).name) }
        #expect(try await app.test(.POST, "/echo", body: #"{"name":"Grace"}"#).text == "Grace")
        #expect(try await app.test(.POST, "/echo", body: "not json").status == .badRequest)
        #expect(try await app.test(.POST, "/echo").status == .badRequest)
    }

    @Test func formBody() async throws {
        let app = Oria()
        app.post("/form") { req, res in res.send(req.form["msg"] ?? "") }
        #expect(try await app.test(.POST, "/form", body: "msg=hi+there%21&x=1").text == "hi there!")
    }

    @Test func cookies() async throws {
        let app = Oria()
        app.get("/") { req, res in
            res.cookie("session", "abc 123", .init(maxAge: 60))
            res.send(req.cookies["theme"] ?? "none")
        }
        let r = try await app.test(.GET, "/", headers: ["cookie": "theme=dark; other=1"])
        #expect(r.text == "dark")
        let setCookie = r.headers.first(name: "set-cookie") ?? ""
        #expect(setCookie.hasPrefix("session=abc%20123; Max-Age=60; Path=/"))
        #expect(setCookie.contains("HttpOnly"))
    }

    @Test func redirectAndSendStatus() async throws {
        let app = Oria()
        app.get("/old") { _, res in res.redirect("/new", status: .movedPermanently) }
        app.get("/gone") { _, res in res.sendStatus(410) }
        let r = try await app.test(.GET, "/old")
        #expect(r.status == .movedPermanently)
        #expect(r.headers.first(name: "location") == "/new")
        #expect(try await app.test(.GET, "/gone").text == "Gone")
    }

    @Test func streamingBody() async throws {
        let app = Oria()
        app.get("/s") { _, res in
            res.stream { w in
                for i in 0..<3 { try await w.write("\(i),") }
            }
        }
        #expect(try await app.test(.GET, "/s").text == "0,1,2,")
    }

    @Test func corsPreflight() async throws {
        let app = Oria()
        app.use(cors(.init(origins: ["https://example.com"], credentials: true)))
        app.get("/data") { _, res in res.send("ok") }

        let pre = try await app.test(
            .OPTIONS, "/data",
            headers: ["origin": "https://example.com", "access-control-request-method": "GET"]
        )
        #expect(pre.status == .noContent)
        #expect(pre.headers.first(name: "access-control-allow-origin") == "https://example.com")
        #expect(pre.headers.first(name: "access-control-allow-credentials") == "true")

        let other = try await app.test(.GET, "/data", headers: ["origin": "https://evil.test"])
        #expect(other.headers.first(name: "access-control-allow-origin") == nil)
    }

    @Test func rateLimiting() async throws {
        let app = Oria()
        app.use(rateLimit(max: 3, window: .seconds(60)))
        app.get("/") { _, res in res.send("ok") }
        for _ in 0..<3 { #expect(try await app.test(.GET, "/").status == .ok) }
        let limited = try await app.test(.GET, "/")
        #expect(limited.status == .tooManyRequests)
        #expect(limited.headers.first(name: "retry-after") != nil)
    }

    @Test func trustProxyIP() async throws {
        var config = Oria.Configuration()
        config.trustProxy = true
        let app = Oria(configuration: config)
        app.get("/ip") { req, res in res.send(req.ip ?? "") }
        let r = try await app.test(.GET, "/ip", headers: ["x-forwarded-for": "203.0.113.9, 10.0.0.1"])
        #expect(r.text == "203.0.113.9")
    }

    @Test func httpDateFormat() {
        #expect(HTTPDate.format(784111777) == "Sun, 06 Nov 1994 08:49:37 GMT")
    }
}

@Suite struct StaticFileTests {
    let dir: String

    init() throws {
        dir = NSTemporaryDirectory() + "oria-static-\(UUID().uuidString)"
        try FileManager.default.createDirectory(atPath: dir + "/sub", withIntermediateDirectories: true)
        try "<h1>home</h1>".write(toFile: dir + "/index.html", atomically: true, encoding: .utf8)
        try "body{}".write(toFile: dir + "/sub/app.css", atomically: true, encoding: .utf8)
        try "secret".write(toFile: dir + "/.env", atomically: true, encoding: .utf8)
        try String(repeating: "x", count: 300_000).write(toFile: dir + "/big.txt", atomically: true, encoding: .utf8)
    }

    func makeApp() -> Oria {
        let app = Oria()
        app.use("/public", serveStatic(dir))
        return app
    }

    @Test func servesFilesWithMimeTypes() async throws {
        let app = makeApp()
        let css = try await app.test(.GET, "/public/sub/app.css")
        #expect(css.status == .ok)
        #expect(css.text == "body{}")
        #expect(css.headers.first(name: "content-type") == "text/css; charset=utf-8")

        let index = try await app.test(.GET, "/public/")
        #expect(index.text == "<h1>home</h1>")
    }

    @Test func streamsLargeFiles() async throws {
        let r = try await makeApp().test(.GET, "/public/big.txt")
        #expect(r.body.readableBytes == 300_000)
    }

    @Test func etagConditionalGet() async throws {
        let app = makeApp()
        let first = try await app.test(.GET, "/public/sub/app.css")
        let etag = try #require(first.headers.first(name: "etag"))
        let second = try await app.test(.GET, "/public/sub/app.css", headers: ["if-none-match": etag])
        #expect(second.status == .notModified)
        #expect(second.text == "")
    }

    @Test func blocksTraversalAndDotfiles() async throws {
        let app = makeApp()
        #expect(try await app.test(.GET, "/public/..%2F..%2Fetc%2Fpasswd").status == .forbidden)
        #expect(try await app.test(.GET, "/public/sub/%2E%2E/%2E%2E/x").status == .forbidden)
        #expect(try await app.test(.GET, "/public/.env").status == .notFound)
        #expect(try await app.test(.GET, "/public/missing.txt").status == .notFound)
    }
}
