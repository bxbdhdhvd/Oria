import NIOConcurrencyHelpers
import NIOHTTP1

/// An Express-style router. Routers can be mounted on an app or another router with `use(_:_:)`.
///
/// Routes and middleware are recorded while you configure the app, then compiled once into an
/// immutable trie when the server starts. Lookups are O(path depth) regardless of how many routes
/// are registered, and the compiled router is shared lock-free across all threads.
///
/// Ordering: all middleware that matches the request path runs first, in registration order,
/// followed by the matched route's own middleware and handler (or the 404 handler).
open class Router: @unchecked Sendable {
    enum Layer {
        case middleware(path: String, Middleware)
        case route(method: String, path: String, [Middleware], Handler)
        case mount(path: String, Router)
        case websocket(path: String, [Middleware], WebSocketOptions, WebSocketHandler)
        case upload(method: String, path: String, [Middleware], UploadOptions, Handler)
    }

    private let lock = NIOLock()
    private var layers: [Layer] = []
    private var version = 0

    public init() {}

    func add(_ layer: Layer) {
        lock.withLock {
            layers.append(layer)
            version &+= 1
        }
    }

    var snapshot: (layers: [Layer], version: Int) {
        lock.withLock { (layers, version) }
    }

    // MARK: Middleware

    /// Adds middleware for every request.
    @discardableResult
    public func use(_ middleware: @escaping Middleware) -> Self {
        add(.middleware(path: "/", middleware))
        return self
    }

    /// Adds middleware for requests whose path starts with `path` (segment-wise).
    @discardableResult
    public func use(_ path: String, _ middleware: @escaping Middleware) -> Self {
        add(.middleware(path: path, middleware))
        return self
    }

    /// Mounts a sub-router under `path`.
    @discardableResult
    public func use(_ path: String, _ router: Router) -> Self {
        precondition(router !== self, "A router cannot be mounted on itself")
        add(.mount(path: path, router))
        return self
    }

    // MARK: Routes
    //
    // Each verb has overloads for 0-3 inline middleware so a trailing closure handler works:
    //   app.get("/admin", requireAuth) { req, res in ... }
    // Use the `middleware:` array overload for longer chains.

    @discardableResult
    public func on(
        _ method: HTTPMethod, _ path: String, middleware: [Middleware] = [], handler: @escaping Handler
    ) -> Self {
        add(.route(method: method.rawValue, path: path, middleware, handler))
        return self
    }

    @discardableResult
    public func get(_ path: String, middleware: [Middleware] = [], handler: @escaping Handler) -> Self {
        add(.route(method: "GET", path: path, middleware, handler))
        return self
    }

    @discardableResult
    public func get(_ path: String, _ m1: @escaping Middleware, handler: @escaping Handler) -> Self {
        get(path, middleware: [m1], handler: handler)
    }

    @discardableResult
    public func get(
        _ path: String, _ m1: @escaping Middleware, _ m2: @escaping Middleware, handler: @escaping Handler
    ) -> Self {
        get(path, middleware: [m1, m2], handler: handler)
    }

    @discardableResult
    public func get(
        _ path: String, _ m1: @escaping Middleware, _ m2: @escaping Middleware, _ m3: @escaping Middleware,
        handler: @escaping Handler
    ) -> Self {
        get(path, middleware: [m1, m2, m3], handler: handler)
    }

    @discardableResult
    public func post(_ path: String, middleware: [Middleware] = [], handler: @escaping Handler) -> Self {
        add(.route(method: "POST", path: path, middleware, handler))
        return self
    }

    @discardableResult
    public func post(_ path: String, _ m1: @escaping Middleware, handler: @escaping Handler) -> Self {
        post(path, middleware: [m1], handler: handler)
    }

    @discardableResult
    public func post(
        _ path: String, _ m1: @escaping Middleware, _ m2: @escaping Middleware, handler: @escaping Handler
    ) -> Self {
        post(path, middleware: [m1, m2], handler: handler)
    }

    @discardableResult
    public func post(
        _ path: String, _ m1: @escaping Middleware, _ m2: @escaping Middleware, _ m3: @escaping Middleware,
        handler: @escaping Handler
    ) -> Self {
        post(path, middleware: [m1, m2, m3], handler: handler)
    }

    @discardableResult
    public func put(_ path: String, middleware: [Middleware] = [], handler: @escaping Handler) -> Self {
        add(.route(method: "PUT", path: path, middleware, handler))
        return self
    }

    @discardableResult
    public func put(_ path: String, _ m1: @escaping Middleware, handler: @escaping Handler) -> Self {
        put(path, middleware: [m1], handler: handler)
    }

    @discardableResult
    public func put(
        _ path: String, _ m1: @escaping Middleware, _ m2: @escaping Middleware, handler: @escaping Handler
    ) -> Self {
        put(path, middleware: [m1, m2], handler: handler)
    }

    @discardableResult
    public func put(
        _ path: String, _ m1: @escaping Middleware, _ m2: @escaping Middleware, _ m3: @escaping Middleware,
        handler: @escaping Handler
    ) -> Self {
        put(path, middleware: [m1, m2, m3], handler: handler)
    }

    @discardableResult
    public func patch(_ path: String, middleware: [Middleware] = [], handler: @escaping Handler) -> Self {
        add(.route(method: "PATCH", path: path, middleware, handler))
        return self
    }

    @discardableResult
    public func patch(_ path: String, _ m1: @escaping Middleware, handler: @escaping Handler) -> Self {
        patch(path, middleware: [m1], handler: handler)
    }

    @discardableResult
    public func patch(
        _ path: String, _ m1: @escaping Middleware, _ m2: @escaping Middleware, handler: @escaping Handler
    ) -> Self {
        patch(path, middleware: [m1, m2], handler: handler)
    }

    @discardableResult
    public func patch(
        _ path: String, _ m1: @escaping Middleware, _ m2: @escaping Middleware, _ m3: @escaping Middleware,
        handler: @escaping Handler
    ) -> Self {
        patch(path, middleware: [m1, m2, m3], handler: handler)
    }

    @discardableResult
    public func delete(_ path: String, middleware: [Middleware] = [], handler: @escaping Handler) -> Self {
        add(.route(method: "DELETE", path: path, middleware, handler))
        return self
    }

    @discardableResult
    public func delete(_ path: String, _ m1: @escaping Middleware, handler: @escaping Handler) -> Self {
        delete(path, middleware: [m1], handler: handler)
    }

    @discardableResult
    public func delete(
        _ path: String, _ m1: @escaping Middleware, _ m2: @escaping Middleware, handler: @escaping Handler
    ) -> Self {
        delete(path, middleware: [m1, m2], handler: handler)
    }

    @discardableResult
    public func delete(
        _ path: String, _ m1: @escaping Middleware, _ m2: @escaping Middleware, _ m3: @escaping Middleware,
        handler: @escaping Handler
    ) -> Self {
        delete(path, middleware: [m1, m2, m3], handler: handler)
    }

    @discardableResult
    public func head(_ path: String, middleware: [Middleware] = [], handler: @escaping Handler) -> Self {
        add(.route(method: "HEAD", path: path, middleware, handler))
        return self
    }

    @discardableResult
    public func head(_ path: String, _ m1: @escaping Middleware, handler: @escaping Handler) -> Self {
        head(path, middleware: [m1], handler: handler)
    }

    @discardableResult
    public func head(
        _ path: String, _ m1: @escaping Middleware, _ m2: @escaping Middleware, handler: @escaping Handler
    ) -> Self {
        head(path, middleware: [m1, m2], handler: handler)
    }

    @discardableResult
    public func head(
        _ path: String, _ m1: @escaping Middleware, _ m2: @escaping Middleware, _ m3: @escaping Middleware,
        handler: @escaping Handler
    ) -> Self {
        head(path, middleware: [m1, m2, m3], handler: handler)
    }

    @discardableResult
    public func options(_ path: String, middleware: [Middleware] = [], handler: @escaping Handler) -> Self {
        add(.route(method: "OPTIONS", path: path, middleware, handler))
        return self
    }

    @discardableResult
    public func options(_ path: String, _ m1: @escaping Middleware, handler: @escaping Handler) -> Self {
        options(path, middleware: [m1], handler: handler)
    }

    @discardableResult
    public func options(
        _ path: String, _ m1: @escaping Middleware, _ m2: @escaping Middleware, handler: @escaping Handler
    ) -> Self {
        options(path, middleware: [m1, m2], handler: handler)
    }

    @discardableResult
    public func options(
        _ path: String, _ m1: @escaping Middleware, _ m2: @escaping Middleware, _ m3: @escaping Middleware,
        handler: @escaping Handler
    ) -> Self {
        options(path, middleware: [m1, m2, m3], handler: handler)
    }

    /// Matches every HTTP method.
    @discardableResult
    public func all(_ path: String, middleware: [Middleware] = [], handler: @escaping Handler) -> Self {
        add(.route(method: CompiledRouter.anyMethod, path: path, middleware, handler))
        return self
    }

    @discardableResult
    public func all(_ path: String, _ m1: @escaping Middleware, handler: @escaping Handler) -> Self {
        all(path, middleware: [m1], handler: handler)
    }

    @discardableResult
    public func all(
        _ path: String, _ m1: @escaping Middleware, _ m2: @escaping Middleware, handler: @escaping Handler
    ) -> Self {
        all(path, middleware: [m1, m2], handler: handler)
    }

    @discardableResult
    public func all(
        _ path: String, _ m1: @escaping Middleware, _ m2: @escaping Middleware, _ m3: @escaping Middleware,
        handler: @escaping Handler
    ) -> Self {
        all(path, middleware: [m1, m2, m3], handler: handler)
    }

    // MARK: Uploads

    /// Registers a route whose body streams to disk instead of memory, with its own limits. Use it
    /// for file uploads of any size: memory stays flat, and the handler runs once the whole body
    /// has arrived.
    ///
    /// - `multipart/form-data` bodies: `req.uploads` holds the text fields and the files (temporary
    ///   files, deleted after the response unless you `move(to:)` them).
    /// - Any other content type: `req.uploadedBody` is the whole body in one temporary file.
    ///
    /// ```swift
    /// app.upload("/avatars", options: .init(maxFileSize: 10 << 20, allowedFileTypes: ["image/png"])) { req, res in
    ///     let file = req.uploads!.file("avatar")!
    ///     try await file.move(to: "/srv/avatars/\(UUID()).png")
    ///     res.status(.created).send("ok")
    /// }
    /// ```
    @discardableResult
    public func upload(
        _ path: String, method: HTTPMethod = .POST, options: UploadOptions = .init(), middleware: [Middleware] = [],
        handler: @escaping Handler
    ) -> Self {
        add(.upload(method: method.rawValue, path: path, middleware, options, handler))
        return self
    }

    @discardableResult
    public func upload(
        _ path: String, _ m1: @escaping Middleware, method: HTTPMethod = .POST, options: UploadOptions = .init(),
        handler: @escaping Handler
    ) -> Self {
        upload(path, method: method, options: options, middleware: [m1], handler: handler)
    }

    // MARK: Compilation

    func flatten(prefix: [String], into builder: inout CompiledRouter.Builder, depth: Int = 0) {
        precondition(depth < 64, "Router mount depth exceeded (cyclic mount?)")
        for layer in snapshot.layers {
            switch layer {
            case .middleware(let path, let mw):
                builder.middleware.append(.init(prefix: prefix + Router.segments(path), middleware: mw))
            case .route(let method, let path, let mws, let handler):
                builder.addRoute(method: method, segments: prefix + Router.segments(path), middleware: mws, handler: handler)
            case .mount(let path, let router):
                router.flatten(prefix: prefix + Router.segments(path), into: &builder, depth: depth + 1)
            case .upload(let method, let path, let mws, let options, let handler):
                builder.addRoute(
                    method: method, segments: prefix + Router.segments(path), middleware: mws, handler: handler,
                    upload: options
                )
            case .websocket(let path, let mws, let options, let handler):
                builder.addWebSocket(
                    segments: prefix + Router.segments(path), middleware: mws, options: options, handler: handler
                )
            }
        }
    }

    static func segments(_ path: String) -> [String] {
        path.split(separator: "/", omittingEmptySubsequences: true).map(String.init)
    }
}

/// The immutable, thread-safe routing table built from a `Router` at startup.
final class CompiledRouter: Sendable {
    static let anyMethod = "*"

    struct ScopedMiddleware: Sendable {
        let prefix: [String]
        let middleware: Middleware
    }

    /// A class so a lookup result costs one reference count to pass around, not one per field.
    final class Route: @unchecked Sendable {
        let paramNames: [String]
        let middleware: [Middleware]
        let handler: Handler
        /// Set for `upload(...)` routes: the body streams to disk with these limits.
        let upload: UploadOptions?
        /// Global middleware + this route's middleware, precomputed when no middleware is
        /// path-scoped (set once in `CompiledRouter.init`, before the router is shared).
        fileprivate(set) var chain: [Middleware]

        init(paramNames: [String], middleware: [Middleware], handler: @escaping Handler, upload: UploadOptions? = nil) {
            self.paramNames = paramNames
            self.middleware = middleware
            self.handler = handler
            self.upload = upload
            self.chain = middleware
        }
    }

    /// The result of routing one request, computed once when its head arrives.
    struct Lookup {
        let segments: [Substring]
        let route: Route?
        let params: [String: String]
    }

    struct WebSocketRoute: Sendable {
        let paramNames: [String]
        let middleware: [Middleware]
        let options: WebSocketOptions
        let handler: WebSocketHandler
    }

    final class Node: @unchecked Sendable {
        var statics: [String: Node] = [:]
        var param: Node?
        var wildcard: Node?
        var routes: [String: Route] = [:]
        var websocket: WebSocketRoute?
    }

    struct Builder {
        var middleware: [ScopedMiddleware] = []
        var maxWebSocketFrameSize = 0
        var hasUploadRoutes = false
        let root = Node()

        mutating func addRoute(
            method: String, segments: [String], middleware: [Middleware], handler: @escaping Handler,
            upload: UploadOptions? = nil
        ) {
            let (node, names) = walk(segments)
            if upload != nil { hasUploadRoutes = true }
            // First registration wins, like Express.
            if node.routes[method] == nil {
                node.routes[method] = Route(paramNames: names, middleware: middleware, handler: handler, upload: upload)
            }
        }

        mutating func addWebSocket(
            segments: [String], middleware: [Middleware], options: WebSocketOptions, handler: @escaping WebSocketHandler
        ) {
            let (node, names) = walk(segments)
            maxWebSocketFrameSize = max(maxWebSocketFrameSize, options.maxFrameSize)
            if node.websocket == nil {
                node.websocket = WebSocketRoute(
                    paramNames: names, middleware: middleware, options: options, handler: handler
                )
            }
        }

        private func walk(_ segments: [String]) -> (Node, [String]) {
            var node = root
            var names: [String] = []
            for (i, seg) in segments.enumerated() {
                if seg.hasPrefix(":") {
                    names.append(String(seg.dropFirst()))
                    if node.param == nil { node.param = Node() }
                    node = node.param!
                } else if seg == "*" {
                    precondition(i == segments.count - 1, "'*' must be the last segment of a route")
                    names.append("*")
                    if node.wildcard == nil { node.wildcard = Node() }
                    node = node.wildcard!
                } else {
                    if let next = node.statics[seg] {
                        node = next
                    } else {
                        let next = Node()
                        node.statics[seg] = next
                        node = next
                    }
                }
            }
            return (node, names)
        }
    }

    let globalMiddleware: [Middleware]
    let scopedMiddleware: [ScopedMiddleware]
    let hasScopedMiddleware: Bool
    let root: Node
    let notFound: Handler
    let errorHandler: ErrorHandler
    /// Largest frame any WebSocket route accepts; 0 when there are no WebSocket routes.
    let maxWebSocketFrameSize: Int
    let hasUploadRoutes: Bool

    init(_ builder: Builder, notFound: @escaping Handler, errorHandler: @escaping ErrorHandler) {
        self.notFound = notFound
        self.errorHandler = errorHandler
        self.maxWebSocketFrameSize = builder.maxWebSocketFrameSize
        self.hasUploadRoutes = builder.hasUploadRoutes
        self.root = builder.root
        self.scopedMiddleware = builder.middleware
        self.hasScopedMiddleware = builder.middleware.contains { !$0.prefix.isEmpty }
        self.globalMiddleware = builder.middleware.map(\.middleware)
        if !hasScopedMiddleware {
            let global = globalMiddleware
            Self.visit(root) { route in route.chain = global + route.middleware }
        }
    }

    private static func visit(_ node: Node, _ body: (Route) -> Void) {
        for route in node.routes.values { body(route) }
        for child in node.statics.values { visit(child, body) }
        if let param = node.param { visit(param, body) }
        if let wildcard = node.wildcard { visit(wildcard, body) }
    }

    /// Routes a request target (path plus optional query).
    func lookup(method: HTTPMethod, uri: String) -> Lookup {
        let path = uri.firstIndex(of: "?").map { uri[..<$0] } ?? uri[...]
        let segments = path.split(separator: "/", omittingEmptySubsequences: true)
        if let (route, params) = match(method: method.rawValue, segments: segments) {
            return Lookup(segments: segments, route: route, params: params)
        }
        return Lookup(segments: segments, route: nil, params: [:])
    }

    /// Finds the route for a method + path. Static segments win over `:params`, which win over `*`.
    func match(method: String, segments: [Substring]) -> (Route, [String: String])? {
        Self.lookup(root, segments) { node in
            if let r = node.routes[method] ?? node.routes[Self.anyMethod] { return r }
            if method == "HEAD" { return node.routes["GET"] }
            return nil
        }.map { ($0.0, Self.params($0.0.paramNames, $0.1)) }
    }

    /// Finds the WebSocket route for a path.
    func matchWebSocket(segments: [Substring]) -> (WebSocketRoute, [String: String])? {
        Self.lookup(root, segments) { $0.websocket }.map { ($0.0, Self.params($0.0.paramNames, $0.1)) }
    }

    private static func params(_ names: [String], _ values: [Substring]) -> [String: String] {
        guard !names.isEmpty else { return [:] }
        var params: [String: String] = [:]
        params.reserveCapacity(names.count)
        for (name, value) in zip(names, values) {
            params[name] = value.contains("%") ? (value.removingPercentEncoding ?? String(value)) : String(value)
        }
        return params
    }

    private static func lookup<R>(
        _ root: Node, _ segments: [Substring], _ pick: (Node) -> R?
    ) -> (R, [Substring])? {
        var values: [Substring] = []
        values.reserveCapacity(4)
        guard let r = match(root, segments[...], pick, &values) else { return nil }
        return (r, values)
    }

    private static func match<R>(
        _ node: Node, _ segments: ArraySlice<Substring>, _ pick: (Node) -> R?, _ values: inout [Substring]
    ) -> R? {
        guard let seg = segments.first else {
            if let r = pick(node) { return r }
            // Allow `/files/*` to match `/files` with an empty wildcard.
            if let wc = node.wildcard, let r = pick(wc) {
                values.append("")
                return r
            }
            return nil
        }
        let rest = segments.dropFirst()
        if let next = node.statics[String(seg)], let r = match(next, rest, pick, &values) {
            return r
        }
        if let next = node.param {
            values.append(seg)
            if let r = match(next, rest, pick, &values) { return r }
            values.removeLast()
        }
        if let wc = node.wildcard, let r = pick(wc) {
            let start = seg.startIndex
            let end = segments.last!.endIndex
            values.append(seg.base[start..<end])
            return r
        }
        return nil
    }

    /// Middleware applicable to `segments`, plus the subpath each one should see.
    func middleware(for segments: [Substring]) -> [(Middleware, String?)] {
        var out: [(Middleware, String?)] = []
        out.reserveCapacity(scopedMiddleware.count)
        for scoped in scopedMiddleware {
            if scoped.prefix.isEmpty {
                out.append((scoped.middleware, nil))
                continue
            }
            guard segments.count >= scoped.prefix.count else { continue }
            var ok = true
            for (a, b) in zip(scoped.prefix, segments) where a != b {
                ok = false
                break
            }
            if ok {
                let sub = "/" + segments.dropFirst(scoped.prefix.count).joined(separator: "/")
                out.append((scoped.middleware, sub))
            }
        }
        return out
    }
}
