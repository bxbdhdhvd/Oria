import Foundation
import NIOCore
import NIOHTTP1

/// An incoming HTTP request.
///
/// A request is owned by a single handler chain, which runs sequentially, so it is safe
/// to mutate `params`, `locals`, etc. from middleware.
public final class Request: @unchecked Sendable {
    public let method: HTTPMethod
    /// The raw request target, e.g. `/users/42?sort=asc`.
    public let url: String
    /// The path component without the query string, e.g. `/users/42`.
    public let path: String
    /// The raw query string without the leading `?`, if present.
    public let queryString: String?
    public let version: HTTPVersion
    public let headers: HTTPHeaders
    /// The fully buffered request body (bounded by `Configuration.maxBodySize`).
    public let body: ByteBuffer?
    public let remoteAddress: SocketAddress?

    /// Route parameters, e.g. `id` for `/users/:id`. A trailing `*` is captured as `"*"`.
    public internal(set) var params: [String: String] = [:]
    /// The path relative to the current middleware mount point (Express' `req.path` inside a mounted router).
    public internal(set) var subpath: String
    /// Per-request storage for passing data between middleware (Express' `res.locals`).
    public var locals: [String: any Sendable] = [:]

    let trustProxy: Bool
    /// Whether the request arrived over TLS (or via a trusted proxy reporting `X-Forwarded-Proto: https`).
    public internal(set) var isSecure = false

    init(
        head: HTTPRequestHead,
        body: ByteBuffer?,
        remoteAddress: SocketAddress?,
        trustProxy: Bool
    ) {
        self.method = head.method
        self.url = head.uri
        self.version = head.version
        self.headers = head.headers
        self.body = body
        self.remoteAddress = remoteAddress
        self.trustProxy = trustProxy
        if let q = head.uri.firstIndex(of: "?") {
            self.path = String(head.uri[..<q])
            self.queryString = String(head.uri[head.uri.index(after: q)...])
        } else {
            self.path = head.uri
            self.queryString = nil
        }
        self.subpath = self.path
        if trustProxy, head.headers.first(name: "x-forwarded-proto")?.lowercased() == "https" {
            self.isSecure = true
        }
    }

    /// Returns the first value of a header (case-insensitive).
    public func header(_ name: String) -> String? { headers.first(name: name) }

    /// Alias for `header(_:)`, matching Express' `req.get()`.
    public func get(_ name: String) -> String? { headers.first(name: name) }

    /// Parsed query string. Repeated keys keep the last value; use `queryAll` for every value.
    public private(set) lazy var query: [String: String] = {
        var out: [String: String] = [:]
        for (k, v) in queryAll { out[k] = v }
        return out
    }()

    /// Every key/value pair in the query string, in order.
    public private(set) lazy var queryAll: [(String, String)] = Request.parseURLEncoded(queryString ?? "")

    /// Parsed `Cookie` header.
    public private(set) lazy var cookies: [String: String] = {
        var out: [String: String] = [:]
        for header in headers[canonicalForm: "cookie"] {
            for pair in header.split(separator: ";") {
                guard let eq = pair.firstIndex(of: "=") else { continue }
                let key = pair[..<eq].trimmingCharacters(in: .whitespaces)
                let value = pair[pair.index(after: eq)...].trimmingCharacters(in: .whitespaces)
                out[key] = value.removingPercentEncoding ?? value
            }
        }
        return out
    }()

    /// The client IP. Honors `X-Forwarded-For` when `Configuration.trustProxy` is enabled.
    public var ip: String? {
        if trustProxy, let fwd = headers.first(name: "x-forwarded-for"),
            let first = fwd.split(separator: ",").first
        {
            return first.trimmingCharacters(in: .whitespaces)
        }
        return remoteAddress?.ipAddress
    }

    /// The body decoded as UTF-8 text.
    public var text: String? {
        guard let body else { return nil }
        return body.getString(at: body.readerIndex, length: body.readableBytes)
    }

    /// The raw body bytes.
    public var bytes: [UInt8] {
        guard let body else { return [] }
        return body.getBytes(at: body.readerIndex, length: body.readableBytes) ?? []
    }

    /// Decodes a JSON body. Throws `HTTPError(.badRequest)` if the body is missing or malformed.
    public func json<T: Decodable>(_ type: T.Type = T.self, decoder: JSONDecoder = JSONDecoder()) throws -> T {
        guard let body, body.readableBytes > 0 else {
            throw HTTPError(.badRequest, "Request body is empty")
        }
        do {
            return try body.withUnsafeReadableBytes { raw in
                try decoder.decode(T.self, from: Data(raw))
            }
        } catch {
            throw HTTPError(.badRequest, "Invalid JSON body")
        }
    }

    /// Parses an `application/x-www-form-urlencoded` body.
    public var form: [String: String] {
        var out: [String: String] = [:]
        for (k, v) in Request.parseURLEncoded(text ?? "") { out[k] = v }
        return out
    }

    /// Whether the `Content-Type` header matches the given type, e.g. `req.is("json")`.
    public func `is`(_ type: String) -> Bool {
        guard let ct = headers.first(name: "content-type")?.lowercased() else { return false }
        return ct.contains(type.lowercased())
    }

    /// Parameters beyond this are ignored (like Express' `qs`), bounding the work an attacker can force.
    static let maxParameters = 1000

    static func parseURLEncoded(_ string: String) -> [(String, String)] {
        guard !string.isEmpty else { return [] }
        var out: [(String, String)] = []
        for pair in string.split(separator: "&", maxSplits: maxParameters, omittingEmptySubsequences: true)
            .prefix(maxParameters)
        {
            let parts = pair.split(separator: "=", maxSplits: 1, omittingEmptySubsequences: false)
            let key = decodeComponent(parts[0])
            let value = parts.count > 1 ? decodeComponent(parts[1]) : ""
            out.append((key, value))
        }
        return out
    }

    static func decodeComponent(_ s: Substring) -> String {
        if !s.contains("%") && !s.contains("+") { return String(s) }
        let plusFixed = s.replacingOccurrences(of: "+", with: " ")
        return plusFixed.removingPercentEncoding ?? plusFixed
    }
}
