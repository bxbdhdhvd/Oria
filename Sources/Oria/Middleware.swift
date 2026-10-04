import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOFileSystem
import NIOHTTP1
import NIOPosix

// MARK: - Logger

/// Logs `METHOD /path STATUS 1.23ms` per request (like `morgan('dev')`).
public func logger(
    _ write: @escaping @Sendable (String) -> Void = { FileHandle.standardOutput.write(Data(($0 + "\n").utf8)) }
) -> Middleware {
    { req, res, next in
        let start = ContinuousClock.now
        defer {
            let elapsed = ContinuousClock.now - start
            let ms = Double(elapsed.components.attoseconds) / 1e15 + Double(elapsed.components.seconds) * 1000
            write("\(req.method.rawValue) \(req.url) \(res.statusCode.code) \(String(format: "%.2f", ms))ms")
        }
        try await next()
    }
}

// MARK: - Security headers

public struct SecurityHeadersOptions: Sendable {
    /// `Content-Security-Policy`. nil omits it (set one tailored to your front end).
    public var contentSecurityPolicy: String? = "default-src 'self'; frame-ancestors 'none'; object-src 'none'"
    /// `Strict-Transport-Security` max-age in seconds; sent only on HTTPS requests. nil disables it.
    public var hstsMaxAge: Int? = 15_552_000
    public var frameOptions: String? = "DENY"
    public var referrerPolicy: String? = "no-referrer"
    public var crossOriginOpenerPolicy: String? = "same-origin"
    public init() {}
}

/// Sets defensive response headers, like `helmet()`.
public func securityHeaders(_ options: SecurityHeadersOptions = .init()) -> Middleware {
    { req, res, next in
        res.set("x-content-type-options", "nosniff")
        res.set("x-dns-prefetch-control", "off")
        res.set("x-download-options", "noopen")
        res.set("x-permitted-cross-domain-policies", "none")
        if let csp = options.contentSecurityPolicy { res.set("content-security-policy", csp) }
        if let frame = options.frameOptions { res.set("x-frame-options", frame) }
        if let referrer = options.referrerPolicy { res.set("referrer-policy", referrer) }
        if let coop = options.crossOriginOpenerPolicy { res.set("cross-origin-opener-policy", coop) }
        if let maxAge = options.hstsMaxAge, req.isSecure {
            res.set("strict-transport-security", "max-age=\(maxAge); includeSubDomains")
        }
        try await next()
    }
}

// MARK: - CORS

public struct CORSOptions: Sendable {
    /// `["*"]` allows any origin. Otherwise the request origin is echoed back if it is listed.
    public var origins: [String] = ["*"]
    public var methods = "GET,HEAD,PUT,PATCH,POST,DELETE"
    public var allowedHeaders: String? = nil
    public var exposedHeaders: String? = nil
    public var credentials = false
    public var maxAge: Int? = 600
    public init(
        origins: [String] = ["*"], methods: String = "GET,HEAD,PUT,PATCH,POST,DELETE",
        allowedHeaders: String? = nil, exposedHeaders: String? = nil,
        credentials: Bool = false, maxAge: Int? = 600
    ) {
        self.origins = origins
        self.methods = methods
        self.allowedHeaders = allowedHeaders
        self.exposedHeaders = exposedHeaders
        self.credentials = credentials
        self.maxAge = maxAge
    }
}

/// Cross-origin resource sharing, including automatic preflight responses.
public func cors(_ options: CORSOptions = .init()) -> Middleware {
    { req, res, next in
        let origin = req.header("origin")
        if options.origins.contains("*") && !options.credentials {
            res.set("access-control-allow-origin", "*")
        } else if let origin, options.origins.contains(origin) || options.origins.contains("*") {
            res.set("access-control-allow-origin", origin)
            res.append("vary", "Origin")
        }
        if options.credentials { res.set("access-control-allow-credentials", "true") }
        if let exposed = options.exposedHeaders { res.set("access-control-expose-headers", exposed) }

        if req.method == .OPTIONS, req.header("access-control-request-method") != nil {
            res.set("access-control-allow-methods", options.methods)
            if let allowed = options.allowedHeaders ?? req.header("access-control-request-headers") {
                res.set("access-control-allow-headers", allowed)
            }
            if let maxAge = options.maxAge { res.set("access-control-max-age", String(maxAge)) }
            res.status(.noContent).end()
            return
        }
        try await next()
    }
}

// MARK: - Static files

public struct StaticOptions: Sendable {
    public var index: String? = "index.html"
    /// `Cache-Control: max-age` in seconds.
    public var maxAge: Int = 0
    /// Serve files whose name starts with a dot.
    public var dotfiles = false
    /// Serve files reached through symlinks that point outside `root`. Off by default: a symlink
    /// planted in a served directory must not expose the rest of the file system.
    public var followSymlinksOutsideRoot = false
    public init(index: String? = "index.html", maxAge: Int = 0, dotfiles: Bool = false, followSymlinksOutsideRoot: Bool = false) {
        self.index = index
        self.maxAge = maxAge
        self.dotfiles = dotfiles
        self.followSymlinksOutsideRoot = followSymlinksOutsideRoot
    }
}

/// Serves files from `root` (like `express.static`). Files are streamed with non-blocking I/O,
/// support `Range` requests, ETag/Last-Modified revalidation, and path traversal is rejected.
public func serveStatic(_ root: String, _ options: StaticOptions = .init()) -> Middleware {
    let rootPath = FilePath(root)
    return { req, res, next in
        guard req.method == .GET || req.method == .HEAD else { return try await next() }

        var segments: [String] = []
        for raw in req.subpath.split(separator: "/", omittingEmptySubsequences: true) {
            let seg = raw.removingPercentEncoding ?? String(raw)
            // Byte-level checks: Character comparisons would miss a "/" followed by a combining
            // mark or zero-width joiner (one grapheme cluster), which FilePath still splits on.
            let bytes = seg.utf8
            if seg.utf8.elementsEqual("..".utf8) || bytes.contains(UInt8(ascii: "/"))
                || bytes.contains(UInt8(ascii: "\\")) || bytes.contains(0)
            {
                throw HTTPError(.forbidden)
            }
            if bytes.first == UInt8(ascii: ".") && !options.dotfiles { return try await next() }
            segments.append(seg)
        }

        var path = rootPath
        for seg in segments { path.append(seg) }

        let fs = FileSystem.shared
        guard var info = try? await fs.info(forFileAt: path) else { return try await next() }
        if info.type == .directory {
            guard let index = options.index else { return try await next() }
            path.append(index)
            guard let indexInfo = try? await fs.info(forFileAt: path), indexInfo.type == .regular else {
                return try await next()
            }
            info = indexInfo
        }
        guard info.type == .regular else { return try await next() }

        // Symlinks: the resolved file must still live under the resolved root.
        if !options.followSymlinksOutsideRoot {
            let target = path.string
            let contained = try await NIOThreadPool.singleton.runIfActive { () -> Bool in
                guard let realRoot = StaticRoot.resolve(root), let realTarget = StaticRoot.resolve(target) else {
                    return false
                }
                return realTarget.hasPrefix(realRoot.hasSuffix("/") ? realRoot : realRoot + "/")
            }
            guard contained else { return try await next() }
        }

        try await res.sendFile(path.string, for: req, options: FileOptions(maxAge: options.maxAge))
    }
}

/// `realpath(3)`, run on a thread-pool thread (it touches the file system).
enum StaticRoot {
    static func resolve(_ path: String) -> String? {
        guard let resolved = realpath(path, nil) else { return nil }
        defer { free(resolved) }
        return String(cString: resolved)
    }
}

// MARK: - Rate limiting

/// Fixed-window rate limiter keyed by client IP (or a custom key). State is sharded across
/// 64 locks so it stays cheap under heavy multi-core load.
public func rateLimit(
    max limit: Int,
    window: Duration = .seconds(60),
    key: @escaping @Sendable (Request) -> String = { $0.ip ?? "unknown" }
) -> Middleware {
    let limiter = RateLimiter(limit: limit, window: window)
    return { req, res, next in
        let (allowed, remaining, resetIn) = limiter.hit(key(req))
        res.set("ratelimit-limit", String(limit))
        res.set("ratelimit-remaining", String(remaining))
        res.set("ratelimit-reset", String(resetIn))
        guard allowed else {
            res.set("retry-after", String(resetIn))
            res.status(.tooManyRequests).json(raw: #"{"error":"Too many requests, please try again later."}"#)
            return
        }
        try await next()
    }
}

final class RateLimiter: Sendable {
    private struct Shard {
        var windowStart: ContinuousClock.Instant
        var counts: [String: Int] = [:]
    }

    private let limit: Int
    private let window: Duration
    private let shards: [NIOLockedValueBox<Shard>]

    init(limit: Int, window: Duration) {
        self.limit = limit
        self.window = window
        let now = ContinuousClock.now
        self.shards = (0..<64).map { _ in NIOLockedValueBox(Shard(windowStart: now)) }
    }

    func hit(_ key: String) -> (allowed: Bool, remaining: Int, resetInSeconds: Int) {
        let shard = shards[Int(UInt(bitPattern: key.hashValue) % UInt(shards.count))]
        let now = ContinuousClock.now
        return shard.withLockedValue { s in
            if now - s.windowStart >= window {
                s.windowStart = now
                s.counts.removeAll(keepingCapacity: true)
            }
            let count = (s.counts[key] ?? 0) + 1
            s.counts[key] = count
            let reset = window - (now - s.windowStart)
            let resetSeconds = Int(reset.components.seconds) + (reset.components.attoseconds > 0 ? 1 : 0)
            return (count <= limit, Swift.max(0, limit - count), resetSeconds)
        }
    }
}
