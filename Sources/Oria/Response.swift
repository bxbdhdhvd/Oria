import Foundation
import NIOCore
import NIOHTTP1

/// Writes chunks of a streamed response body (Express' `res.write`).
public struct BodyWriter: Sendable {
    let allocator: ByteBufferAllocator
    let sink: @Sendable (ByteBuffer) async throws -> Void

    public func write(_ buffer: ByteBuffer) async throws {
        try await sink(buffer)
    }

    public func write(_ string: String) async throws {
        try await sink(allocator.buffer(string: string))
    }

    public func write(_ bytes: some Sequence<UInt8>) async throws {
        try await sink(allocator.buffer(bytes: bytes))
    }
}

/// The outgoing HTTP response. Handlers mutate it; the server writes it once the chain completes.
public final class Response: @unchecked Sendable {
    public enum Body: Sendable {
        case empty
        case buffer(ByteBuffer)
        /// A streamed body. If `length` is nil the response uses chunked transfer encoding.
        case stream(length: Int?, @Sendable (BodyWriter) async throws -> Void)
    }

    public var statusCode: HTTPResponseStatus = .ok
    public var headers = HTTPHeaders()
    public var body: Body = .empty
    /// True once `send`/`json`/`end`/... has been called.
    public private(set) var isSent = false

    let allocator: ByteBufferAllocator

    init(allocator: ByteBufferAllocator) {
        self.allocator = allocator
    }

    // MARK: Status & headers

    @discardableResult
    public func status(_ status: HTTPResponseStatus) -> Self {
        statusCode = status
        return self
    }

    @discardableResult
    public func status(_ code: Int) -> Self {
        statusCode = HTTPResponseStatus(statusCode: code)
        return self
    }

    /// Sets (replaces) a header.
    @discardableResult
    public func set(_ name: String, _ value: String) -> Self {
        headers.replaceOrAdd(name: name, value: value)
        return self
    }

    /// Appends a header value without replacing existing ones.
    @discardableResult
    public func append(_ name: String, _ value: String) -> Self {
        headers.add(name: name, value: value)
        return self
    }

    public func get(_ name: String) -> String? { headers.first(name: name) }

    /// Sets the `Content-Type`. Accepts a full MIME type or a shorthand like `json`, `html`, `txt`.
    @discardableResult
    public func type(_ type: String) -> Self {
        set("content-type", type.contains("/") ? type : MIME.type(forExtension: type))
    }

    // MARK: Sending

    /// Sends a text body. Defaults to `text/html` like Express.
    public func send(_ string: String) {
        if !headers.contains(name: "content-type") { set("content-type", "text/html; charset=utf-8") }
        finish(.buffer(allocator.buffer(string: string)))
    }

    public func send(_ buffer: ByteBuffer) {
        if !headers.contains(name: "content-type") { set("content-type", "application/octet-stream") }
        finish(.buffer(buffer))
    }

    public func send(_ bytes: [UInt8]) {
        send(allocator.buffer(bytes: bytes))
    }

    public func send(_ data: Data) {
        send(allocator.buffer(bytes: data))
    }

    /// Encodes any `Encodable` value as JSON.
    /// Sends `value` as JSON. With the default encoder this uses Oria's streaming encoder
    /// (`FastJSON`: same output as `JSONEncoder()`, written straight into the response buffer);
    /// pass a configured `JSONEncoder` for other strategies (dates, key casing, pretty printing).
    public func json<T: Encodable>(_ value: T, encoder: JSONEncoder? = nil) throws {
        let body: ByteBuffer
        if let encoder {
            body = allocator.buffer(bytes: try encoder.encode(value))
        } else {
            var buffer = allocator.buffer(capacity: 128)
            do {
                try FastJSON.encode(value, into: &buffer)
                body = buffer
            } catch {
                body = allocator.buffer(bytes: try Response.defaultEncoder.encode(value))
            }
        }
        if !headers.contains(name: "content-type") { set("content-type", "application/json; charset=utf-8") }
        finish(.buffer(body))
    }

    /// Sends a pre-serialized JSON string.
    public func json(raw: String) {
        set("content-type", "application/json; charset=utf-8")
        finish(.buffer(allocator.buffer(string: raw)))
    }

    /// Sends the status code with its reason phrase as the body, e.g. `res.sendStatus(404)`.
    public func sendStatus(_ code: Int) {
        sendStatus(HTTPResponseStatus(statusCode: code))
    }

    public func sendStatus(_ status: HTTPResponseStatus) {
        statusCode = status
        set("content-type", "text/plain; charset=utf-8")
        finish(.buffer(allocator.buffer(string: status.reasonPhrase)))
    }

    public func redirect(_ location: String, status: HTTPResponseStatus = .found) {
        statusCode = status
        set("location", location)
        finish(.empty)
    }

    /// Ends the response with no (further) body.
    public func end() {
        finish(body)
    }

    /// Streams the body. Use for large payloads, server-sent events, etc.
    public func stream(length: Int? = nil, _ producer: @escaping @Sendable (BodyWriter) async throws -> Void) {
        finish(.stream(length: length, producer))
    }

    // MARK: Cookies

    public struct CookieOptions: Sendable {
        public var maxAge: Int?
        public var path: String? = "/"
        public var domain: String?
        public var secure = false
        public var httpOnly = true
        public var sameSite: String? = "Lax"
        public init(
            maxAge: Int? = nil, path: String? = "/", domain: String? = nil,
            secure: Bool = false, httpOnly: Bool = true, sameSite: String? = "Lax"
        ) {
            self.maxAge = maxAge
            self.path = path
            self.domain = domain
            self.secure = secure
            self.httpOnly = httpOnly
            self.sameSite = sameSite
        }
    }

    @discardableResult
    public func cookie(_ name: String, _ value: String, _ options: CookieOptions = .init()) -> Self {
        let encoded = value.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) ?? value
        var parts = ["\(name)=\(encoded)"]
        if let maxAge = options.maxAge { parts.append("Max-Age=\(maxAge)") }
        if let path = options.path { parts.append("Path=\(path)") }
        if let domain = options.domain { parts.append("Domain=\(domain)") }
        if options.secure { parts.append("Secure") }
        if options.httpOnly { parts.append("HttpOnly") }
        if let sameSite = options.sameSite { parts.append("SameSite=\(sameSite)") }
        return append("set-cookie", parts.joined(separator: "; "))
    }

    @discardableResult
    public func clearCookie(_ name: String, path: String = "/") -> Self {
        append("set-cookie", "\(name)=; Path=\(path); Max-Age=0")
    }

    // MARK: Internals

    private func finish(_ body: Body) {
        self.body = body
        isSent = true
    }

    /// Clears body and headers so an error handler can write a fresh response.
    func reset() {
        statusCode = .ok
        headers = HTTPHeaders()
        body = .empty
        isSent = false
    }

    public static let defaultEncoder: JSONEncoder = {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        encoder.outputFormatting = [.withoutEscapingSlashes]
        return encoder
    }()
}
