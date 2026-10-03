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
    }

    private let lock = NIOLock()
    private var layers: [Layer] = []
    private var version = 0

    public init() {}

    private func add(_ layer: Layer) {
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

    struct Route: Sendable {
        let paramNames: [String]
        let middleware: [Middleware]
        let handler: Handler
    }

    final class Node: @unchecked Sendable {
        var statics: [String: Node] = [:]
        var param: Node?
        var wildcard: Node?
        var routes: [String: Route] = [:]
    }

    struct Builder {
        var middleware: [ScopedMiddleware] = []
        let root = Node()

        mutating func addRoute(method: String, segments: [String], middleware: [Middleware], handler: @escaping Handler) {
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
            // First registration wins, like Express.
            if node.routes[method] == nil {
                node.routes[method] = Route(paramNames: names, middleware: middleware, handler: handler)
            }
        }
    }

    let globalMiddleware: [Middleware]
    let scopedMiddleware: [ScopedMiddleware]
    let hasScopedMiddleware: Bool
    let root: Node
    let notFound: Handler
    let errorHandler: ErrorHandler

    init(_ builder: Builder, notFound: @escaping Handler, errorHandler: @escaping ErrorHandler) {
        self.notFound = notFound
        self.errorHandler = errorHandler
        self.root = builder.root
        self.scopedMiddleware = builder.middleware
        self.hasScopedMiddleware = builder.middleware.contains { !$0.prefix.isEmpty }
        self.globalMiddleware = builder.middleware.map(\.middleware)
    }

    /// Finds the route for a method + path. Static segments win over `:params`, which win over `*`.
    func match(method: String, segments: [Substring]) -> (Route, [String: String])? {
        var values: [Substring] = []
        values.reserveCapacity(4)
        guard let route = Self.match(root, segments[...], method: method, values: &values) else { return nil }
        var params: [String: String] = [:]
        if !route.paramNames.isEmpty {
            params.reserveCapacity(route.paramNames.count)
            for (name, value) in zip(route.paramNames, values) {
                params[name] = value.contains("%") ? (value.removingPercentEncoding ?? String(value)) : String(value)
            }
        }
        return (route, params)
    }

    private static func match(
        _ node: Node, _ segments: ArraySlice<Substring>, method: String, values: inout [Substring]
    ) -> Route? {
        guard let seg = segments.first else {
            if let r = node.routes[method] ?? node.routes[anyMethod] { return r }
            if method == "HEAD", let r = node.routes["GET"] { return r }
            // Allow `/files/*` to match `/files` with an empty wildcard.
            if let wc = node.wildcard, let r = routeFor(wc, method) {
                values.append("")
                return r
            }
            return nil
        }
        let rest = segments.dropFirst()
        if let next = node.statics[String(seg)], let r = match(next, rest, method: method, values: &values) {
            return r
        }
        if let next = node.param {
            values.append(seg)
            if let r = match(next, rest, method: method, values: &values) { return r }
            values.removeLast()
        }
        if let wc = node.wildcard, let r = routeFor(wc, method) {
            let start = seg.startIndex
            let end = segments.last!.endIndex
            values.append(seg.base[start..<end])
            return r
        }
        return nil
    }

    private static func routeFor(_ node: Node, _ method: String) -> Route? {
        if let r = node.routes[method] ?? node.routes[anyMethod] { return r }
        if method == "HEAD" { return node.routes["GET"] }
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
