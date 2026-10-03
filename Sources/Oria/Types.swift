import NIOHTTP1

/// Continues to the next middleware (or the route handler) in the chain.
public typealias Next = @Sendable () async throws -> Void

/// A terminal route handler: `app.get("/") { req, res in res.send("hi") }`.
public typealias Handler = @Sendable (Request, Response) async throws -> Void

/// Express-style middleware: `app.use { req, res, next in ...; try await next() }`.
public typealias Middleware = @Sendable (Request, Response, Next) async throws -> Void

/// Called when a handler or middleware throws.
public typealias ErrorHandler = @Sendable (any Error, Request, Response) async -> Void

/// Throw this from a handler to send a specific status code to the client.
public struct HTTPError: Error, Sendable, CustomStringConvertible {
    public let status: HTTPResponseStatus
    public let message: String

    public init(_ status: HTTPResponseStatus, _ message: String? = nil) {
        self.status = status
        self.message = message ?? status.reasonPhrase
    }

    public init(_ code: Int, _ message: String? = nil) {
        self.init(HTTPResponseStatus(statusCode: code), message)
    }

    public var description: String { "\(status.code) \(message)" }
}
