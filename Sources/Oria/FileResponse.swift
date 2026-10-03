import Foundation
import NIOCore
import NIOFileSystem
import NIOHTTP1

/// Options for `res.sendFile` / `serveStatic`.
public struct FileOptions: Sendable {
    /// `Cache-Control: public, max-age=…` in seconds.
    public var maxAge: Int = 0
    /// Adds `immutable` to `Cache-Control` (for fingerprinted assets).
    public var immutable = false
    /// Overrides the content type derived from the file extension.
    public var contentType: String? = nil
    /// Serve `Range` requests (`206 Partial Content`).
    public var acceptRanges = true
    /// Requests asking for more ranges than this get the whole file (range-abuse defense).
    public var maxRanges = 16
    /// Size of each read from disk.
    public var chunkSize = 256 * 1024

    public init(
        maxAge: Int = 0, immutable: Bool = false, contentType: String? = nil, acceptRanges: Bool = true,
        maxRanges: Int = 16
    ) {
        self.maxAge = maxAge
        self.immutable = immutable
        self.contentType = contentType
        self.acceptRanges = acceptRanges
        self.maxRanges = maxRanges
    }
}

extension Response {
    /// Sends a file from disk, streamed with non-blocking I/O, with full HTTP semantics:
    /// `Range` (single and multiple ranges), `If-Range`, `ETag` / `If-None-Match`,
    /// `Last-Modified` / `If-Modified-Since`, `If-Match` (412) and `HEAD`.
    ///
    /// `path` is used as given: never pass unvalidated user input (use `serveStatic` for that).
    /// Throws `HTTPError(.notFound)` if the file doesn't exist or isn't a regular file.
    public func sendFile(_ path: String, for req: Request, options: FileOptions = .init()) async throws {
        let filePath = FilePath(path)
        guard let info = try? await FileSystem.shared.info(forFileAt: filePath), info.type == .regular else {
            throw HTTPError(.notFound)
        }
        let size = Int64(info.size)
        let modified = info.lastDataModificationTime
        let etag = "\"\(String(size, radix: 16))-\(String(modified.seconds, radix: 16))\(String(modified.nanoseconds, radix: 16))\""
        let lastModified = HTTPDate.format(time_t(modified.seconds))
        let contentType = options.contentType ?? MIME.type(forExtension: filePath.extension ?? "")

        set("etag", etag)
        set("last-modified", lastModified)
        set("cache-control", "public, max-age=\(options.maxAge)" + (options.immutable ? ", immutable" : ""))
        set("accept-ranges", options.acceptRanges ? "bytes" : "none")
        if get("content-type") == nil { set("content-type", contentType) }

        // Preconditions (RFC 9110 §13.2.2).
        if let ifMatch = req.header("if-match"), !FileRange.etagList(ifMatch, matches: etag, weak: false) {
            status(.preconditionFailed).end()
            return
        }
        if let ifNoneMatch = req.header("if-none-match") {
            if FileRange.etagList(ifNoneMatch, matches: etag, weak: true) {
                status(.notModified).end()
                return
            }
        } else if let since = req.header("if-modified-since"), let sinceDate = HTTPDate.parse(since),
            modified.seconds <= sinceDate
        {
            status(.notModified).end()
            return
        }

        var ranges: [ClosedRange<Int64>]? = nil
        if options.acceptRanges, req.method == .GET || req.method == .HEAD, let header = req.header("range") {
            // If-Range: only honor the range if the representation is unchanged.
            var rangeApplies = true
            if let ifRange = req.header("if-range") {
                rangeApplies = ifRange.hasPrefix("\"") || ifRange.hasPrefix("W/") ? ifRange == etag : ifRange == lastModified
            }
            if rangeApplies {
                switch FileRange.parse(header, size: size, maxRanges: options.maxRanges) {
                case .ignore:
                    break
                case .unsatisfiable:
                    set("content-range", "bytes */\(size)")
                    status(.rangeNotSatisfiable).end()
                    return
                case .ranges(let parsed):
                    ranges = parsed
                }
            }
        }

        let chunk = options.chunkSize
        guard let ranges, !ranges.isEmpty else {
            stream(length: Int(size)) { writer in
                try await FileRange.copy(filePath, ranges: [0...(max(size, 1) - 1)], size: size, chunk: chunk, to: writer)
            }
            return
        }

        status(.partialContent)
        if ranges.count == 1, let range = ranges.first {
            set("content-range", "bytes \(range.lowerBound)-\(range.upperBound)/\(size)")
            stream(length: Int(range.count)) { writer in
                try await FileRange.copy(filePath, ranges: [range], size: size, chunk: chunk, to: writer)
            }
            return
        }

        // multipart/byteranges (RFC 9110 §14.6).
        let boundary = "oria-" + UUID().uuidString.replacingOccurrences(of: "-", with: "").lowercased()
        let partHeaders = ranges.enumerated().map { index, range in
            (index == 0 ? "" : "\r\n") + "--\(boundary)\r\nContent-Type: \(contentType)\r\n"
                + "Content-Range: bytes \(range.lowerBound)-\(range.upperBound)/\(size)\r\n\r\n"
        }
        let closing = "\r\n--\(boundary)--\r\n"
        let total = partHeaders.reduce(0) { $0 + $1.utf8.count } + ranges.reduce(0) { $0 + Int($1.count) } + closing.utf8.count
        set("content-type", "multipart/byteranges; boundary=\(boundary)")
        stream(length: total) { writer in
            try await FileSystem.shared.withFileHandle(forReadingAt: filePath) { handle in
                for (header, range) in zip(partHeaders, ranges) {
                    try await writer.write(header)
                    for try await piece in handle.readChunks(in: range.lowerBound..<(range.upperBound + 1), chunkLength: .bytes(Int64(chunk))) {
                        try await writer.write(piece)
                    }
                }
                try await writer.write(closing)
            }
        }
    }

    /// Like `sendFile`, plus `Content-Disposition: attachment` so browsers save the file.
    public func download(_ path: String, filename: String? = nil, for req: Request, options: FileOptions = .init()) async throws {
        let name = filename ?? FilePath(path).lastComponent?.string ?? "download"
        let ascii = String(name.unicodeScalars.map { $0.isASCII && $0 != "\"" && $0 != "\\" && $0.value >= 0x20 ? Character($0) : "_" })
        let encoded = name.addingPercentEncoding(withAllowedCharacters: .alphanumerics) ?? ascii
        set("content-disposition", "attachment; filename=\"\(ascii)\"; filename*=UTF-8''\(encoded)")
        try await sendFile(path, for: req, options: options)
    }
}

/// `Range` header parsing and range copying.
enum FileRange {
    enum Result: Equatable {
        /// Malformed, unsupported unit or too many ranges: serve the whole file (RFC 9110 allows ignoring).
        case ignore
        case unsatisfiable
        case ranges([ClosedRange<Int64>])
    }

    /// Parses `bytes=0-99,200-,-50`. Overlapping and adjacent ranges are merged so a client can't
    /// make the server send the same bytes many times.
    static func parse(_ header: String, size: Int64, maxRanges: Int) -> Result {
        let trimmed = header.trimmingCharacters(in: .whitespaces)
        guard trimmed.lowercased().hasPrefix("bytes=") else { return .ignore }
        let specs = trimmed.dropFirst(6).split(separator: ",", omittingEmptySubsequences: false)
        guard !specs.isEmpty, specs.count <= maxRanges else { return .ignore }
        var ranges: [ClosedRange<Int64>] = []
        for rawSpec in specs {
            let spec = rawSpec.trimmingCharacters(in: .whitespaces)
            guard let dash = spec.firstIndex(of: "-") else { return .ignore }
            let first = spec[..<dash]
            let last = spec[spec.index(after: dash)...]
            if first.isEmpty {
                // Suffix range: the last N bytes.
                guard let count = Int64(last), count >= 0, last.allSatisfy(\.isNumber) else { return .ignore }
                guard count > 0, size > 0 else { continue }
                ranges.append(max(0, size - count)...(size - 1))
            } else {
                guard first.allSatisfy(\.isNumber), let start = Int64(first) else { return .ignore }
                var end = size - 1
                if !last.isEmpty {
                    guard last.allSatisfy(\.isNumber), let parsed = Int64(last) else { return .ignore }
                    guard parsed >= start else { return .ignore }
                    end = min(parsed, size - 1)
                }
                guard start < size else { continue }  // unsatisfiable piece
                ranges.append(start...end)
            }
        }
        guard !ranges.isEmpty else { return .unsatisfiable }
        ranges.sort { $0.lowerBound < $1.lowerBound }
        var merged: [ClosedRange<Int64>] = [ranges[0]]
        for range in ranges.dropFirst() {
            let lastRange = merged[merged.count - 1]
            if range.lowerBound <= lastRange.upperBound + 1 {
                merged[merged.count - 1] = lastRange.lowerBound...max(lastRange.upperBound, range.upperBound)
            } else {
                merged.append(range)
            }
        }
        return .ranges(merged)
    }

    /// `If-None-Match: "a", W/"b"` / `If-Match: *`.
    static func etagList(_ header: String, matches etag: String, weak: Bool) -> Bool {
        let value = header.trimmingCharacters(in: .whitespaces)
        if value == "*" { return true }
        let opaque = etag.hasPrefix("W/") ? String(etag.dropFirst(2)) : etag
        for candidate in value.split(separator: ",") {
            var tag = candidate.trimmingCharacters(in: .whitespaces)
            if tag.hasPrefix("W/") {
                guard weak else { continue }  // strong comparison never matches weak tags
                tag = String(tag.dropFirst(2))
            }
            if tag == opaque { return true }
        }
        return false
    }

    static func copy(
        _ path: FilePath, ranges: [ClosedRange<Int64>], size: Int64, chunk: Int, to writer: BodyWriter
    ) async throws {
        guard size > 0 else { return }
        try await FileSystem.shared.withFileHandle(forReadingAt: path) { handle in
            for range in ranges {
                for try await piece in handle.readChunks(in: range.lowerBound..<(range.upperBound + 1), chunkLength: .bytes(Int64(chunk))) {
                    try await writer.write(piece)
                }
            }
        }
    }
}
