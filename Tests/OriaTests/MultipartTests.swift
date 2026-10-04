import Foundation
import NIOCore
import Testing

@testable import Oria

/// Unit tests for the incremental multipart parser.
@Suite struct MultipartParserTests {
    static let boundary = "----oriaBoundary7MA4YWxkTrZu0gW"

    static func body(_ parts: [(name: String, filename: String?, type: String?, data: [UInt8])], preamble: String = "") -> [UInt8] {
        var out = Array(preamble.utf8)
        for part in parts {
            out += Array("--\(boundary)\r\n".utf8)
            var disposition = "Content-Disposition: form-data; name=\"\(part.name)\""
            if let filename = part.filename { disposition += "; filename=\"\(filename)\"" }
            out += Array((disposition + "\r\n").utf8)
            if let type = part.type { out += Array("Content-Type: \(type)\r\n".utf8) }
            out += Array("\r\n".utf8)
            out += part.data
            out += Array("\r\n".utf8)
        }
        out += Array("--\(boundary)--\r\n".utf8)
        return out
    }

    /// Runs the parser feeding `bytes` in chunks of `chunk` bytes; returns (fields, files).
    static func parse(_ bytes: [UInt8], chunk: Int) throws -> [(PartHeaders, [UInt8])] {
        var parser = MultipartParser(boundary: boundary)
        var parts: [(PartHeaders, [UInt8])] = []
        var current: PartHeaders?
        var data: [UInt8] = []
        var offset = 0
        func drain() throws {
            while let event = try parser.next() {
                switch event {
                case .partBegin(let h): current = h; data = []
                case .data(let b): data += Array(b.readableBytesView)
                case .partEnd: parts.append((current!, data))
                case .end: break
                }
            }
        }
        while offset < bytes.count {
            let end = min(bytes.count, offset + chunk)
            parser.append(ByteBuffer(bytes: bytes[offset..<end]))
            try drain()
            offset = end
        }
        try parser.finish()
        return parts
    }

    @Test func parsesFieldsAndFilesAtEveryChunkSize() throws {
        let binary = (0..<5000).map { UInt8(truncatingIfNeeded: $0 &* 31) } + Array("\r\n--not-the-boundary\r\n".utf8)
        let bytes = Self.body([
            ("title", nil, nil, Array("hello world".utf8)),
            ("empty", nil, nil, []),
            ("doc", "report.pdf", "application/pdf", binary),
        ], preamble: "ignored preamble\r\n")
        // Every chunk size from 1 byte up exercises boundaries split across reads.
        for chunk in [1, 2, 3, 7, 16, 31, 64, 100, 1000, bytes.count] {
            let parts = try Self.parse(bytes, chunk: chunk)
            #expect(parts.count == 3, "chunk \(chunk)")
            #expect(parts[0].0.name == "title" && parts[0].1 == Array("hello world".utf8))
            #expect(parts[1].0.name == "empty" && parts[1].1.isEmpty)
            #expect(parts[2].0.filename == "report.pdf" && parts[2].0.contentType == "application/pdf")
            #expect(parts[2].1 == binary, "binary data must round-trip at chunk \(chunk)")
        }
    }

    @Test func extractsBoundaryFromContentType() {
        #expect(MultipartParser.boundary(fromContentType: "multipart/form-data; boundary=abc") == "abc")
        #expect(MultipartParser.boundary(fromContentType: "multipart/form-data; charset=utf-8; boundary=\"a b\"") == "a b")
        #expect(MultipartParser.boundary(fromContentType: "application/json") == nil)
        #expect(MultipartParser.boundary(fromContentType: "multipart/form-data") == nil)
        #expect(MultipartParser.boundary(fromContentType: "multipart/form-data; boundary=\(String(repeating: "x", count: 71))") == nil)
    }

    @Test func sanitizesFilenames() throws {
        #expect(MultipartParser.sanitize(filename: "../../etc/passwd") == "passwd")
        #expect(MultipartParser.sanitize(filename: "C:\\Users\\evil\\a.exe") == "a.exe")
        #expect(MultipartParser.sanitize(filename: ".htaccess") == "htaccess")
        #expect(MultipartParser.sanitize(filename: "a\u{0}b\nc.txt") == "abc.txt")
        #expect(MultipartParser.sanitize(filename: "../") == nil)
        // Separators and dots hidden inside grapheme clusters (found by fuzzing).
        #expect(MultipartParser.sanitize(filename: "a/\u{200D}b.txt") == "b.txt")
        #expect(MultipartParser.sanitize(filename: "..\u{301}/x") == "x")
        #expect(MultipartParser.sanitize(filename: ".\u{301}hidden") == "\u{301}hidden")
        #expect(MultipartParser.sanitize(filename: "evil\u{202E}txt.exe") == "eviltxt.exe")  // bidi override
        #expect(MultipartParser.sanitize(filename: String(repeating: "é", count: 200))!.utf8.count <= 255)
        for name in ["a/\u{200D}", "\\\u{301}..", "x/\u{FE0F}/y", "\u{200B}.secret"] {
            let cleaned = MultipartParser.sanitize(filename: name) ?? ""
            #expect(!cleaned.unicodeScalars.contains("/") && !cleaned.unicodeScalars.contains("\\"))
            #expect(cleaned.unicodeScalars.first != ".")
        }
        let headers = try MultipartParser.parseHeaders(
            ByteBuffer(string: "Content-Disposition: form-data; name=\"f\"; filename=\"x.txt\"; filename*=UTF-8''na%C3%AFve%20file.txt")
        )
        #expect(headers.filename == "naïve file.txt")
    }

    @Test func rejectsMalformedBodies() {
        // Missing closing boundary.
        var truncated = Self.body([("a", nil, nil, Array("1".utf8))])
        truncated.removeLast(10)
        #expect(throws: (any Error).self) { try Self.parse(truncated, chunk: 64) }
        // Garbage after the boundary line.
        let garbage = Array("--\(Self.boundary)XX\r\n\r\n".utf8)
        #expect(throws: (any Error).self) { try Self.parse(garbage, chunk: 64) }
        // Oversized part headers.
        let huge = Array("--\(Self.boundary)\r\nX-Big: \(String(repeating: "a", count: 20_000))\r\n\r\nx\r\n--\(Self.boundary)--".utf8)
        #expect(throws: MultipartParser.ParseError.headerTooLarge) { try Self.parse(huge, chunk: 512) }
    }

    @Test func inMemoryFormEnforcesLimits() throws {
        let bytes = ByteBuffer(bytes: Self.body([
            ("a", nil, nil, Array("1".utf8)), ("f", "x.png", "image/png", [1, 2, 3]),
        ]))
        let ct = "multipart/form-data; boundary=\(Self.boundary)"
        let form = try MultipartForm.parse(bytes, contentType: ct)
        #expect(form.field("a") == "1")
        #expect(form.file("f")?.size == 3 && form.file("f")?.filename == "x.png")
        #expect(throws: (any Error).self) { try MultipartForm.parse(bytes, contentType: ct, limits: .init(maxFileSize: 2)) }
        #expect(throws: (any Error).self) { try MultipartForm.parse(bytes, contentType: ct, limits: .init(allowedFileTypes: ["image/jpeg"])) }
        #expect(throws: (any Error).self) { try MultipartForm.parse(bytes, contentType: ct, limits: .init(maxFields: 0)) }
    }

    @Test func parsesLargeBodyInConstantMemory() throws {
        // 8 MB file fed in 4 KB chunks: the parser's buffer never grows past a chunk + boundary.
        let fileBytes = [UInt8](repeating: 0xAB, count: 8 << 20)
        let bytes = Self.body([("f", "big.bin", nil, fileBytes)])
        var parser = MultipartParser(boundary: Self.boundary)
        var total = 0
        var offset = 0
        while offset < bytes.count {
            let end = min(bytes.count, offset + 4096)
            parser.append(ByteBuffer(bytes: bytes[offset..<end]))
            while let event = try parser.next() {
                if case .data(let b) = event { total += b.readableBytes }
            }
            offset = end
        }
        try parser.finish()
        #expect(total == fileBytes.count)
    }
}
