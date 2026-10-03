import Foundation
import NIOCore
import NIOFileSystem
import NIOHTTP1

/// A parsed `multipart/form-data` body.
public struct MultipartForm: Sendable {
    /// Text fields in order. Use `field(_:)` / `fields` for lookup.
    public internal(set) var allFields: [(name: String, value: String)] = []
    /// Uploaded files in order.
    public internal(set) var files: [UploadedFile] = []

    public init() {}

    /// Text fields by name (the last value wins when a name repeats).
    public var fields: [String: String] {
        var out: [String: String] = [:]
        for (name, value) in allFields { out[name] = value }
        return out
    }

    public func field(_ name: String) -> String? { allFields.last { $0.name == name }?.value }
    public func file(_ name: String) -> UploadedFile? { files.first { $0.field == name } }
    public func files(_ name: String) -> [UploadedFile] { files.filter { $0.field == name } }
}

/// One uploaded file, kept in memory (small forms parsed with `req.multipart()`) or streamed to a
/// temporary file on disk (`app.upload(...)` routes).
public struct UploadedFile: Sendable {
    public enum Storage: Sendable {
        case memory(ByteBuffer)
        case disk(path: String)
    }

    /// The form field name.
    public let field: String
    /// The client's file name, reduced to a safe base name (no directories, no control
    /// characters). Never use it as a path without your own validation.
    public let filename: String?
    public let contentType: String
    public let size: Int
    public let storage: Storage

    /// The temporary file for disk-backed uploads. It's deleted after the response is sent unless
    /// you `move(to:)` it first.
    public var path: String? {
        if case .disk(let path) = storage { return path }
        return nil
    }

    /// Loads the file into memory. Throws for files larger than `limit`.
    public func bytes(limit: Int = 64 << 20) async throws -> ByteBuffer {
        switch storage {
        case .memory(let buffer): return buffer
        case .disk(let path):
            guard size <= limit else { throw HTTPError(.payloadTooLarge, "File larger than \(limit) bytes") }
            return try await FileSystem.shared.withFileHandle(forReadingAt: FilePath(path)) { handle in
                try await handle.readToEnd(maximumSizeAllowed: .bytes(Int64(limit)))
            }
        }
    }

    /// Moves (disk) or writes (memory) the file to `destination`. Fails if it already exists.
    public func move(to destination: String) async throws {
        switch storage {
        case .disk(let path):
            try await FileSystem.shared.moveItem(at: FilePath(path), to: FilePath(destination))
        case .memory(let buffer):
            try await FileSystem.shared.withFileHandle(
                forWritingAt: FilePath(destination), options: .newFile(replaceExisting: false)
            ) { handle in
                _ = try await handle.write(contentsOf: buffer, toAbsoluteOffset: 0)
            }
        }
    }
}

/// Limits for `app.upload(...)` routes. Uploads stream to disk, so memory use stays flat no matter
/// how large the body is.
public struct UploadOptions: Sendable {
    /// Total request body limit (`413` above it).
    public var maxBodySize: Int = 1 << 30
    /// Per-file limit (`413` above it).
    public var maxFileSize: Int = 1 << 30
    public var maxFiles: Int = 20
    public var maxFields: Int = 200
    /// Per text-field limit; text fields are kept in memory.
    public var maxFieldSize: Int = 1 << 20
    /// Maximum size of one part's header block.
    public var maxPartHeaderSize: Int = 16 * 1024
    /// Where temporary files go. nil uses the system temporary directory.
    public var directory: String? = nil
    /// Allowed per-file content types (e.g. `["image/png", "image/jpeg"]`). nil allows any.
    public var allowedFileTypes: [String]? = nil
    /// The longest the client may go without sending body bytes (replaces `requestReadTimeout`,
    /// which would cut off legitimately long uploads).
    public var idleTimeout: TimeAmount = .seconds(30)

    public init(
        maxBodySize: Int = 1 << 30, maxFileSize: Int = 1 << 30, maxFiles: Int = 20, maxFields: Int = 200,
        maxFieldSize: Int = 1 << 20, directory: String? = nil, allowedFileTypes: [String]? = nil,
        idleTimeout: TimeAmount = .seconds(30)
    ) {
        self.maxBodySize = maxBodySize
        self.maxFileSize = maxFileSize
        self.maxFiles = maxFiles
        self.maxFields = maxFields
        self.maxFieldSize = maxFieldSize
        self.directory = directory
        self.allowedFileTypes = allowedFileTypes
        self.idleTimeout = idleTimeout
    }
}

// MARK: - Parser

/// The headers of one multipart part.
struct PartHeaders: Sendable {
    var name: String?
    var filename: String?
    var contentType: String = "text/plain"
}

/// An incremental `multipart/form-data` parser (RFC 7578 / RFC 2046). Feed it chunks as they arrive
/// and pull events; it holds back only enough bytes to recognise a boundary split across chunks, so
/// memory stays bounded for any body size.
struct MultipartParser {
    enum Event {
        case partBegin(PartHeaders)
        case data(ByteBuffer)
        case partEnd
        case end
    }

    enum ParseError: Error, Equatable {
        case malformed(String)
        case headerTooLarge
    }

    private enum State {
        case preamble, headers, body, afterDelimiter, done
    }

    private let dashBoundary: [UInt8]  // "--boundary"
    private let delimiter: [UInt8]  // "\r\n--boundary"
    private let maxHeaderSize: Int
    private var buffer: ByteBuffer
    private var state = State.preamble

    init(boundary: String, maxHeaderSize: Int = 16 * 1024) {
        self.dashBoundary = Array("--\(boundary)".utf8)
        self.delimiter = Array("\r\n--\(boundary)".utf8)
        self.maxHeaderSize = maxHeaderSize
        self.buffer = ByteBuffer()
    }

    /// Extracts the boundary from a `Content-Type: multipart/form-data; boundary=...` header.
    static func boundary(fromContentType contentType: String) -> String? {
        guard contentType.lowercased().hasPrefix("multipart/") else { return nil }
        for param in contentType.split(separator: ";").dropFirst() {
            let pair = param.split(separator: "=", maxSplits: 1).map { $0.trimmingCharacters(in: .whitespaces) }
            guard pair.count == 2, pair[0].lowercased() == "boundary" else { continue }
            var value = pair[1]
            if value.hasPrefix("\""), value.hasSuffix("\""), value.count >= 2 { value = String(value.dropFirst().dropLast()) }
            // RFC 2046: 1-70 characters.
            guard (1...70).contains(value.utf8.count) else { return nil }
            return value
        }
        return nil
    }

    var isDone: Bool { if case .done = state { return true } else { return false } }

    mutating func append(_ chunk: ByteBuffer) {
        if case .done = state { return }  // epilogue: ignore
        var chunk = chunk
        buffer.writeBuffer(&chunk)
    }

    /// The next event, or nil if more input is needed.
    mutating func next() throws -> Event? {
        while true {
            switch state {
            case .done:
                return nil

            case .preamble:
                // The first boundary may be preceded by a preamble (rarely used, ignored).
                guard let index = Self.firstIndex(of: dashBoundary, in: buffer) else {
                    // Keep a tail that could hold the start of the boundary.
                    let keep = dashBoundary.count - 1
                    if buffer.readableBytes > keep { buffer.moveReaderIndex(forwardBy: buffer.readableBytes - keep) }
                    buffer.discardReadBytes()
                    return nil
                }
                guard buffer.readableBytes >= index + dashBoundary.count + 2 else { return nil }
                buffer.moveReaderIndex(forwardBy: index + dashBoundary.count)
                let suffix = buffer.readBytes(length: 2)!
                if suffix == [0x2D, 0x2D] {  // "--": a form with no parts
                    state = .done
                    return .end
                }
                guard suffix == [0x0D, 0x0A] else { throw ParseError.malformed("boundary not followed by CRLF") }
                state = .headers

            case .headers:
                let block: ByteBuffer
                if buffer.readableBytesView.starts(with: [0x0D, 0x0A]) {
                    buffer.moveReaderIndex(forwardBy: 2)  // a part without headers
                    block = ByteBuffer()
                } else if let end = Self.firstIndex(of: [0x0D, 0x0A, 0x0D, 0x0A], in: buffer) {
                    guard end <= maxHeaderSize else { throw ParseError.headerTooLarge }
                    block = buffer.readSlice(length: end)!
                    buffer.moveReaderIndex(forwardBy: 4)
                } else {
                    if buffer.readableBytes > maxHeaderSize { throw ParseError.headerTooLarge }
                    return nil
                }
                state = .body
                return .partBegin(try Self.parseHeaders(block))

            case .body:
                if let index = Self.firstIndex(of: delimiter, in: buffer) {
                    if index > 0 {
                        // Emit the data first; the delimiter is consumed on the next call.
                        return .data(buffer.readSlice(length: index)!)
                    }
                    buffer.moveReaderIndex(forwardBy: delimiter.count)
                    state = .afterDelimiter
                    return .partEnd
                }
                // No delimiter yet: hand over everything except a tail that might start one.
                let safe = buffer.readableBytes - (delimiter.count - 1)
                guard safe > 0 else { return nil }
                let data = buffer.readSlice(length: safe)!
                buffer.discardReadBytes()
                return .data(data)

            case .afterDelimiter:
                // Transport padding (spaces/tabs) may follow the boundary.
                while let byte = buffer.getInteger(at: buffer.readerIndex, as: UInt8.self), byte == 0x20 || byte == 0x09 {
                    buffer.moveReaderIndex(forwardBy: 1)
                }
                guard buffer.readableBytes >= 2 else { return nil }
                let suffix = buffer.readBytes(length: 2)!
                if suffix == [0x2D, 0x2D] {
                    state = .done
                    buffer.clear()
                    return .end
                }
                guard suffix == [0x0D, 0x0A] else { throw ParseError.malformed("bad bytes after boundary") }
                buffer.discardReadBytes()
                state = .headers
            }
        }
    }

    /// Call when the body ended. Throws if the closing boundary never arrived.
    func finish() throws {
        guard isDone else { throw ParseError.malformed("body ended before the closing boundary") }
    }

    // MARK: Helpers

    /// Finds `needle` in the readable bytes using bounds-checked `Span` access (no raw pointers).
    static func firstIndex(of needle: [UInt8], in buffer: ByteBuffer) -> Int? {
        let count = buffer.readableBytes
        let n = needle.count
        guard n > 0, count >= n else { return nil }
        let first = needle[0]
        #if compiler(>=6.2)
        let span = buffer.readableBytesUInt8Span
        var i = 0
        let last = count - n
        while i <= last {
            if span[i] == first {
                var j = 1
                while j < n && span[i &+ j] == needle[j] { j &+= 1 }
                if j == n { return i }
            }
            i &+= 1
        }
        return nil
        #else
        let view = buffer.readableBytesView
        var i = view.startIndex
        while let hit = view[i...].firstIndex(of: first), hit &+ n <= view.endIndex {
            if view[hit..<(hit &+ n)].elementsEqual(needle) { return hit - view.startIndex }
            i = hit &+ 1
        }
        return nil
        #endif
    }

    static func parseHeaders(_ block: ByteBuffer) throws -> PartHeaders {
        var headers = PartHeaders()
        guard let text = block.getString(at: block.readerIndex, length: block.readableBytes) else {
            throw ParseError.malformed("part headers are not UTF-8")
        }
        for line in text.split(separator: "\r\n", omittingEmptySubsequences: true) {
            guard let colon = line.firstIndex(of: ":") else { throw ParseError.malformed("bad part header") }
            let name = line[..<colon].trimmingCharacters(in: .whitespaces).lowercased()
            let value = line[line.index(after: colon)...].trimmingCharacters(in: .whitespaces)
            switch name {
            case "content-disposition":
                let params = parseParameters(value)
                headers.name = params["name"]
                if let encoded = params["filename*"], let decoded = decodeRFC5987(encoded) {
                    headers.filename = sanitize(filename: decoded)
                } else if let plain = params["filename"] {
                    headers.filename = sanitize(filename: plain)
                }
            case "content-type":
                headers.contentType = value
            default:
                break
            }
        }
        return headers
    }

    /// `form-data; name="a"; filename="b.txt"` → ["name": "a", "filename": "b.txt"].
    static func parseParameters(_ value: String) -> [String: String] {
        var out: [String: String] = [:]
        var current = ""
        var inQuotes = false
        var escaped = false
        var parts: [String] = []
        for ch in value {
            if escaped {
                current.append(ch)
                escaped = false
            } else if ch == "\\" && inQuotes {
                escaped = true
            } else if ch == "\"" {
                inQuotes.toggle()
                current.append(ch)
            } else if ch == ";" && !inQuotes {
                parts.append(current)
                current = ""
            } else {
                current.append(ch)
            }
        }
        parts.append(current)
        for part in parts.dropFirst() {
            let pair = part.split(separator: "=", maxSplits: 1)
            guard pair.count == 2 else { continue }
            let key = pair[0].trimmingCharacters(in: .whitespaces).lowercased()
            var val = pair[1].trimmingCharacters(in: .whitespaces)
            if val.hasPrefix("\""), val.hasSuffix("\""), val.count >= 2 { val = String(val.dropFirst().dropLast()) }
            out[key] = val
        }
        return out
    }

    /// RFC 5987 `UTF-8''na%C3%AFve.txt`.
    static func decodeRFC5987(_ value: String) -> String? {
        let pieces = value.split(separator: "'", maxSplits: 2, omittingEmptySubsequences: false)
        guard pieces.count == 3, pieces[0].lowercased() == "utf-8" else { return nil }
        return String(pieces[2]).removingPercentEncoding
    }

    /// Strips directories, control characters and leading dots; caps the length.
    static func sanitize(filename: String) -> String? {
        let base = filename.split(whereSeparator: { $0 == "/" || $0 == "\\" }).last.map(String.init) ?? ""
        var cleaned = String(base.unicodeScalars.filter { !CharacterSet.controlCharacters.contains($0) })
        while cleaned.hasPrefix(".") { cleaned.removeFirst() }
        cleaned = String(cleaned.prefix(255))
        return cleaned.isEmpty ? nil : cleaned
    }
}

// MARK: - Parsing helpers used by Request and the upload pipeline

enum MultipartError: Error {
    case notMultipart
    case tooLarge(String)
    case tooMany(String)
    case typeNotAllowed(String)
    case malformed(String)

    var httpError: HTTPError {
        switch self {
        case .notMultipart: HTTPError(.unsupportedMediaType, "Expected multipart/form-data")
        case .tooLarge(let what): HTTPError(.payloadTooLarge, what)
        case .tooMany(let what): HTTPError(.payloadTooLarge, what)
        case .typeNotAllowed(let type): HTTPError(.unsupportedMediaType, "File type \(type) not allowed")
        case .malformed(let why): HTTPError(.badRequest, "Malformed multipart body: \(why)")
        }
    }
}

extension MultipartForm {
    /// Parses a fully buffered body in memory (for normal routes and small forms).
    static func parse(_ body: ByteBuffer, contentType: String, limits: UploadOptions = .init()) throws -> MultipartForm {
        guard let boundary = MultipartParser.boundary(fromContentType: contentType) else { throw MultipartError.notMultipart }
        var parser = MultipartParser(boundary: boundary, maxHeaderSize: limits.maxPartHeaderSize)
        parser.append(body)
        var form = MultipartForm()
        var current: PartHeaders?
        var data = ByteBuffer()
        do {
            while let event = try parser.next() {
                switch event {
                case .partBegin(let headers):
                    current = headers
                    data.clear()
                case .data(var chunk):
                    data.writeBuffer(&chunk)
                case .partEnd:
                    guard let part = current else { continue }
                    try form.add(part: part, data: data, limits: limits)
                    current = nil
                case .end:
                    break
                }
            }
            try parser.finish()
        } catch let error as MultipartParser.ParseError {
            throw MultipartError.malformed("\(error)")
        }
        return form
    }

    mutating func add(part: PartHeaders, data: ByteBuffer, limits: UploadOptions) throws {
        let name = part.name ?? ""
        if part.filename != nil {
            try checkFile(part, size: data.readableBytes, limits: limits)
            files.append(
                UploadedFile(
                    field: name, filename: part.filename, contentType: part.contentType, size: data.readableBytes,
                    storage: .memory(data)
                ))
        } else {
            guard allFields.count < limits.maxFields else { throw MultipartError.tooMany("Too many fields") }
            guard data.readableBytes <= limits.maxFieldSize else { throw MultipartError.tooLarge("Field \(name) too large") }
            allFields.append((name, String(buffer: data)))
        }
    }

    func checkFile(_ part: PartHeaders, size: Int, limits: UploadOptions) throws {
        guard files.count < limits.maxFiles else { throw MultipartError.tooMany("Too many files") }
        guard size <= limits.maxFileSize else { throw MultipartError.tooLarge("File too large") }
        if let allowed = limits.allowedFileTypes {
            let type = part.contentType.split(separator: ";").first.map { $0.trimmingCharacters(in: .whitespaces).lowercased() } ?? ""
            guard allowed.contains(where: { $0.lowercased() == type }) else { throw MultipartError.typeNotAllowed(type) }
        }
    }
}
