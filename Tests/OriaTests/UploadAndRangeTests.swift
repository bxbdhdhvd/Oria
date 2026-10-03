import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOHTTP1
import Testing

@testable import Oria

/// End-to-end tests for streaming uploads, file responses with Range, and request-handler details.
@Suite(.timeLimit(.minutes(2))) struct UploadAndRangeTests {
    let dir: String
    let uploadDir: String

    init() throws {
        dir = NSTemporaryDirectory() + "oria-files-\(UUID().uuidString)"
        uploadDir = dir + "/uploads"
        try FileManager.default.createDirectory(atPath: uploadDir, withIntermediateDirectories: true)
        // 1 MiB file whose byte i is i % 251, so any range is easy to verify.
        let bytes = (0..<(1 << 20)).map { UInt8($0 % 251) }
        try Data(bytes).write(to: URL(fileURLWithPath: dir + "/data.bin"))
        try "hello range".write(toFile: dir + "/small.txt", atomically: true, encoding: .utf8)
    }

    static func expectedBytes(_ range: ClosedRange<Int>) -> [UInt8] { range.map { UInt8($0 % 251) } }

    func makeApp(_ configure: (inout Oria.Configuration) -> Void = { _ in }) -> Oria {
        let app = Oria(configuration: testConfig(configure))
        let dir = self.dir
        let uploadDir = self.uploadDir
        app.upload("/upload", options: .init(maxFileSize: 64 << 20, directory: uploadDir)) { req, res in
            let form = try #require(req.uploads)
            var files: [[String: String]] = []
            for file in form.files {
                let bytes = try await file.bytes(limit: 64 << 20)
                let sum = bytes.readableBytesView.reduce(UInt64(0)) { $0 &+ UInt64($1) }
                files.append([
                    "field": file.field, "name": file.filename ?? "", "size": String(file.size), "sum": String(sum),
                    "type": file.contentType, "onDisk": String(file.path.map { FileManager.default.fileExists(atPath: $0) } ?? false),
                ])
            }
            struct Out: Encodable { var fields: [String: String]; var files: [[String: String]] }
            try res.json(Out(fields: form.fields, files: files))
        }
        app.upload("/small", options: .init(maxBodySize: 10_000, maxFileSize: 1000, maxFiles: 2, directory: uploadDir, allowedFileTypes: ["image/png"])) { req, res in
            res.send("ok \(req.uploads?.files.count ?? -1)")
        }
        app.upload("/keep", options: .init(directory: uploadDir)) { req, res in
            let file = try #require(req.uploads?.file("f"))
            try await file.move(to: uploadDir + "/kept-\(UUID().uuidString)")
            res.send("kept")
        }
        app.upload("/raw/:name", method: .PUT, options: .init(directory: uploadDir)) { req, res in
            let body = try #require(req.uploadedBody)
            res.send("\(req.params["name"]!) \(body.size) \(body.contentType)")
        }
        app.post("/form") { req, res in
            let form = try req.multipart()
            res.send("\(form.field("a") ?? "-") \(form.file("f")?.size ?? -1)")
        }
        app.get("/file/:name") { req, res in
            let name = req.params["name"]!
            guard !name.contains("/"), !name.hasPrefix(".") else { throw HTTPError(.forbidden) }
            try await res.sendFile(dir + "/" + name, for: req)
        }
        app.get("/download") { req, res in try await res.download(dir + "/small.txt", filename: "résumé \"v2\".txt", for: req) }
        app.post("/echo-size") { req, res in res.send(String(req.body?.readableBytes ?? 0)) }
        app.get("/huge/:mb") { req, res in
            let mb = Int(req.params["mb"]!)!
            let chunk = ByteBuffer(repeating: 0x5A, count: 1 << 20)
            res.stream(length: mb << 20) { w in for _ in 0..<mb { try await w.write(chunk) } }
        }
        return app
    }

    func multipartBody(_ parts: [(String, String?, String?, [UInt8])], boundary: String = "XyZboundary") -> [UInt8] {
        var out: [UInt8] = []
        for (name, filename, type, data) in parts {
            out += Array("--\(boundary)\r\nContent-Disposition: form-data; name=\"\(name)\"".utf8)
            if let filename { out += Array("; filename=\"\(filename)\"".utf8) }
            out += Array("\r\n".utf8)
            if let type { out += Array("Content-Type: \(type)\r\n".utf8) }
            out += Array("\r\n".utf8) + data + Array("\r\n".utf8)
        }
        return out + Array("--\(boundary)--\r\n".utf8)
    }

    /// Sends a full HTTP/1.1 request and returns (status line + headers, body) once complete.
    func send(_ port: Int, _ head: String, body: [UInt8] = [], chunkSize: Int = 64 * 1024) async throws -> (String, [UInt8]) {
        let client = try await RawClient.connect(port: port)
        defer { client.close() }
        try await client.send(head)
        var offset = 0
        while offset < body.count {
            let end = min(body.count, offset + chunkSize)
            try await client.send(Array(body[offset..<end]))
            offset = end
        }
        guard let responseHead = await client.responseHead(timeout: .seconds(30)) else { return ("", []) }
        if let lengthLine = responseHead.lowercased().split(separator: "\r\n").first(where: { $0.hasPrefix("content-length:") }),
            let length = Int(lengthLine.split(separator: ":")[1].trimmingCharacters(in: .whitespaces))
        {
            _ = await client.wait(timeout: .seconds(30)) { _, closed in
                client.collector.buffer.withLockedValue { $0.readableBytes } >= length || closed
            }
        }
        let bytes = client.collector.buffer.withLockedValue { Array($0.readableBytesView) }
        return (responseHead, bytes)
    }

    func uploadsLeft() -> [String] {
        ((try? FileManager.default.contentsOfDirectory(atPath: uploadDir)) ?? []).filter { $0.hasPrefix("oria-upload-") }
    }

    // MARK: Uploads

    @Test func streamsMultipartFilesToDisk() async throws {
        let (server, port) = try await startServer(makeApp())
        let big = (0..<(5 << 20)).map { UInt8(truncatingIfNeeded: $0 &* 7) }
        let small = Array("tiny file".utf8)
        let body = multipartBody([
            ("title", nil, nil, Array("vacation".utf8)),
            ("photo", "../../beach.jpg", "image/jpeg", big),
            ("note", "n.txt", "text/plain", small),
        ])
        let (head, response) = try await send(
            port, "POST /upload HTTP/1.1\r\nHost: x\r\nContent-Type: multipart/form-data; boundary=XyZboundary\r\nContent-Length: \(body.count)\r\n\r\n",
            body: body, chunkSize: 7919)  // odd chunk size: boundaries land mid-read
        #expect(head.hasPrefix("HTTP/1.1 200"), "\(head)")
        let json = String(decoding: response, as: UTF8.self)
        let bigSum = big.reduce(UInt64(0)) { $0 &+ UInt64($1) }
        #expect(json.contains("\"title\":\"vacation\""))
        #expect(json.contains("\"name\":\"beach.jpg\""), "client paths are stripped")
        #expect(json.contains("\"sum\":\"\(bigSum)\"") && json.contains("\"size\":\"\(big.count)\""))
        #expect(json.contains("\"onDisk\":\"true\""))
        // Temporary files are deleted once the response is sent.
        try await Task.sleep(for: .milliseconds(300))
        #expect(uploadsLeft().isEmpty)
        await server.shutdown()
    }

    @Test func chunkedMultipartUpload() async throws {
        let (server, port) = try await startServer(makeApp())
        let body = multipartBody([("f", "a.bin", nil, [UInt8](repeating: 9, count: 300_000))])
        // Transfer-Encoding: chunked, 1000-byte chunks.
        var chunked: [UInt8] = []
        var offset = 0
        while offset < body.count {
            let end = min(body.count, offset + 1000)
            chunked += Array("\(String(end - offset, radix: 16))\r\n".utf8) + body[offset..<end] + Array("\r\n".utf8)
            offset = end
        }
        chunked += Array("0\r\n\r\n".utf8)
        let (head, response) = try await send(
            port, "POST /upload HTTP/1.1\r\nHost: x\r\nContent-Type: multipart/form-data; boundary=XyZboundary\r\nTransfer-Encoding: chunked\r\n\r\n",
            body: chunked)
        #expect(head.hasPrefix("HTTP/1.1 200"))
        #expect(String(decoding: response, as: UTF8.self).contains("\"size\":\"300000\""))
        await server.shutdown()
    }

    @Test func rawBodyUploadAndExpectContinue() async throws {
        let (server, port) = try await startServer(makeApp())
        let client = try await RawClient.connect(port: port)
        let payload = [UInt8](repeating: 1, count: 2 << 20)
        try await client.send("PUT /raw/backup.tar HTTP/1.1\r\nHost: x\r\nContent-Type: application/x-tar\r\nContent-Length: \(payload.count)\r\nExpect: 100-continue\r\n\r\n")
        // The server must answer 100 Continue before the client sends the body.
        let interim = try #require(await client.responseHead())
        #expect(interim.hasPrefix("HTTP/1.1 100"))
        try await client.send(payload)
        let final = try #require(await client.responseHead(timeout: .seconds(20)))
        #expect(final.hasPrefix("HTTP/1.1 200"))
        #expect(await client.wait { text, _ in text.contains("backup.tar \(payload.count) application/x-tar") })
        client.close()
        await server.shutdown()
    }

    @Test func uploadLimitsAreEnforced() async throws {
        let (server, port) = try await startServer(makeApp())
        let ct = "Content-Type: multipart/form-data; boundary=XyZboundary"
        // File over maxFileSize (1000).
        let tooBig = multipartBody([("f", "a.png", "image/png", [UInt8](repeating: 1, count: 2000))])
        let r1 = try await send(port, "POST /small HTTP/1.1\r\nHost: x\r\n\(ct)\r\nContent-Length: \(tooBig.count)\r\n\r\n", body: tooBig)
        #expect(r1.0.hasPrefix("HTTP/1.1 413"))
        // Disallowed type.
        let wrongType = multipartBody([("f", "a.exe", "application/x-msdownload", [1])])
        let r2 = try await send(port, "POST /small HTTP/1.1\r\nHost: x\r\n\(ct)\r\nContent-Length: \(wrongType.count)\r\n\r\n", body: wrongType)
        #expect(r2.0.hasPrefix("HTTP/1.1 415"))
        // Too many files.
        let many = multipartBody((0..<3).map { ("f\($0)", "a\($0).png", "image/png", [1]) })
        let r3 = try await send(port, "POST /small HTTP/1.1\r\nHost: x\r\n\(ct)\r\nContent-Length: \(many.count)\r\n\r\n", body: many)
        #expect(r3.0.hasPrefix("HTTP/1.1 413"))
        // Declared body over maxBodySize: refused before reading it.
        let r4 = try await send(port, "POST /small HTTP/1.1\r\nHost: x\r\n\(ct)\r\nContent-Length: 50000\r\n\r\n")
        #expect(r4.0.hasPrefix("HTTP/1.1 413"))
        // Malformed multipart.
        let bad = Array("--XyZboundary\r\nno-colon-header\r\n\r\nx\r\n--XyZboundary--\r\n".utf8)
        let r5 = try await send(port, "POST /small HTTP/1.1\r\nHost: x\r\n\(ct)\r\nContent-Length: \(bad.count)\r\n\r\n", body: bad)
        #expect(r5.0.hasPrefix("HTTP/1.1 400"))
        try await Task.sleep(for: .milliseconds(300))
        #expect(uploadsLeft().isEmpty, "rejected uploads leave no temporary files")
        await server.shutdown()
    }

    @Test func abortedUploadsCleanUp() async throws {
        let (server, port) = try await startServer(makeApp())
        let client = try await RawClient.connect(port: port)
        try await client.send("POST /upload HTTP/1.1\r\nHost: x\r\nContent-Type: multipart/form-data; boundary=XyZboundary\r\nContent-Length: 10000000\r\n\r\n")
        try await client.send(Array("--XyZboundary\r\nContent-Disposition: form-data; name=\"f\"; filename=\"a\"\r\n\r\n".utf8) + [UInt8](repeating: 3, count: 500_000))
        try await Task.sleep(for: .milliseconds(200))
        // Files are created transactionally (O_TMPFILE / hidden temp file, linked into place on close),
        // so a partial upload is never visible under its final name.
        #expect(uploadsLeft().isEmpty, "partial uploads are invisible while being written")
        client.close()  // client vanishes mid-upload
        try await Task.sleep(for: .milliseconds(300))
        let leftovers = (try? FileManager.default.contentsOfDirectory(atPath: uploadDir)) ?? []
        #expect(leftovers.isEmpty, "an aborted upload leaves nothing behind: \(leftovers)")
        await server.shutdown()
    }

    @Test func movedFilesAreKept() async throws {
        let (server, port) = try await startServer(makeApp())
        let body = multipartBody([("f", "keep.txt", nil, Array("keep me".utf8))])
        let r = try await send(port, "POST /keep HTTP/1.1\r\nHost: x\r\nContent-Type: multipart/form-data; boundary=XyZboundary\r\nContent-Length: \(body.count)\r\n\r\n", body: body)
        #expect(r.0.hasPrefix("HTTP/1.1 200"))
        let kept = try FileManager.default.contentsOfDirectory(atPath: uploadDir).filter { $0.hasPrefix("kept-") }
        #expect(kept.count == 1)
        #expect(try String(contentsOfFile: uploadDir + "/" + kept[0], encoding: .utf8) == "keep me")
        await server.shutdown()
    }

    @Test func inMemoryMultipartOnNormalRoutes() async throws {
        let app = makeApp()
        let body = String(decoding: multipartBody([("a", nil, nil, Array("x".utf8)), ("f", "f.txt", nil, [1, 2, 3, 4])]), as: UTF8.self)
        let r = try await app.test(.POST, "/form", headers: ["content-type": "multipart/form-data; boundary=XyZboundary"], body: body)
        #expect(r.text == "x 4")
        let notMultipart = try await app.test(.POST, "/form", headers: ["content-type": "application/json"], body: "{}")
        #expect(notMultipart.status == .unsupportedMediaType)
    }

    @Test func uploadsOverHTTP2() async throws {
        let (server, port) = try await startServer(makeApp { $0.tls = try! TestTLS.serverOptions() })
        let client = try await H2Client.connect(port: port)
        let payload = String(decoding: multipartBody([("f", "h2.bin", nil, [UInt8](repeating: 65, count: 1 << 20))]), as: UTF8.self)
        let r = try await client.request(.POST, "/upload", headers: ["content-type": "multipart/form-data; boundary=XyZboundary"], body: payload)
        #expect(r.head.status == .ok)
        #expect(r.body.contains("\"size\":\"\(1 << 20)\""))
        client.close()
        await server.shutdown()
    }

    /// RSS is process-wide, so this only means something when run alone:
    /// `ORIA_MEMORY_TESTS=1 swift test --filter uploadMemoryStaysFlat`. The container load test
    /// (scripts/container-bench.sh) checks the same property with a 1 GB upload under a 256 MB cap.
    @Test(.enabled(if: ProcessInfo.processInfo.environment["ORIA_MEMORY_TESTS"] == "1"))
    func uploadMemoryStaysFlat() async throws {
        // A 64 MB upload must not be buffered in memory: with backpressure, the request handler holds
        // at most a few chunks. Measured through the process's resident size.
        let (server, port) = try await startServer(makeApp())
        func rss() -> Int {
            let statm = (try? String(contentsOfFile: "/proc/self/statm", encoding: .utf8)) ?? "0 0"
            return (Int(statm.split(separator: " ")[1]) ?? 0) * 4096
        }
        let before = rss()
        let size = 64 << 20
        let client = try await RawClient.connect(port: port)
        try await client.send("PUT /raw/big HTTP/1.1\r\nHost: x\r\nContent-Type: application/octet-stream\r\nContent-Length: \(size)\r\n\r\n")
        let chunk = [UInt8](repeating: 7, count: 256 * 1024)
        for _ in 0..<(size / chunk.count) { try await client.send(chunk) }
        #expect(await client.wait(timeout: .seconds(60)) { text, _ in text.contains("big \(size)") })
        let growth = rss() - before
        #expect(growth < 48 << 20, "RSS grew \(growth >> 20) MB for a 64 MB upload")
        client.close()
        await server.shutdown()
    }

    // MARK: Ranges and file responses

    @Test func servesSingleRanges() async throws {
        let (server, port) = try await startServer(makeApp())
        for (header, range, expectedRange) in [
            ("bytes=0-99", 0...99, "bytes 0-99/1048576"),
            ("bytes=1048000-", 1_048_000...1_048_575, "bytes 1048000-1048575/1048576"),
            ("bytes=-500", 1_048_076...1_048_575, "bytes 1048076-1048575/1048576"),
            ("bytes=1048570-9999999", 1_048_570...1_048_575, "bytes 1048570-1048575/1048576"),
        ] {
            let (head, bytes) = try await send(port, "GET /file/data.bin HTTP/1.1\r\nHost: x\r\nRange: \(header)\r\nConnection: close\r\n\r\n")
            #expect(head.hasPrefix("HTTP/1.1 206"), "\(header)")
            #expect(head.lowercased().contains("content-range: \(expectedRange)"), "\(header)")
            #expect(head.lowercased().contains("content-length: \(range.count)"))
            let body = Array(bytes.suffix(range.count))
            #expect(body == Self.expectedBytes(range), "\(header) body")
        }
        await server.shutdown()
    }

    @Test func servesMultipleRanges() async throws {
        let (server, port) = try await startServer(makeApp())
        let (head, bytes) = try await send(port, "GET /file/data.bin HTTP/1.1\r\nHost: x\r\nRange: bytes=0-9, 100-109, 5-14\r\nConnection: close\r\n\r\n")
        #expect(head.hasPrefix("HTTP/1.1 206"))
        #expect(head.contains("multipart/byteranges; boundary="))
        let text = String(decoding: bytes, as: UTF8.self)
        // 0-9 and 5-14 overlap, so they're merged into 0-14.
        #expect(text.contains("Content-Range: bytes 0-14/1048576"))
        #expect(text.contains("Content-Range: bytes 100-109/1048576"))
        #expect(!text.contains("bytes 5-14"))
        await server.shutdown()
    }

    @Test func rangeEdgeCases() async throws {
        let app = makeApp()
        // Unsatisfiable.
        let unsat = try await app.test(.GET, "/file/data.bin", headers: ["range": "bytes=5000000-"])
        #expect(unsat.status == .rangeNotSatisfiable)
        #expect(unsat.headers.first(name: "content-range") == "bytes */1048576")
        // Malformed or other units: ignored, full file.
        for header in ["bytes=abc", "items=0-5", "bytes=9-3", "bytes=0-1,"] {
            let r = try await app.test(.GET, "/file/data.bin", headers: ["range": header])
            #expect(r.status == .ok && r.body.readableBytes == 1 << 20, "\(header)")
        }
        // Too many ranges (range-abuse defense): whole file instead.
        let many = (0..<50).map { "\($0 * 10)-\($0 * 10 + 1)" }.joined(separator: ",")
        let abuse = try await app.test(.GET, "/file/data.bin", headers: ["range": "bytes=" + many])
        #expect(abuse.status == .ok)
        // HEAD with a range: headers only.
        let head = try await app.test(.HEAD, "/file/data.bin", headers: ["range": "bytes=0-9"])
        #expect(head.status == .partialContent && head.body.readableBytes == 0)
    }

    @Test func conditionalRequests() async throws {
        let app = makeApp()
        let first = try await app.test(.GET, "/file/small.txt")
        let etag = try #require(first.headers.first(name: "etag"))
        let lastModified = try #require(first.headers.first(name: "last-modified"))
        #expect(first.headers.first(name: "accept-ranges") == "bytes")
        #expect(try await app.test(.GET, "/file/small.txt", headers: ["if-none-match": etag]).status == .notModified)
        #expect(try await app.test(.GET, "/file/small.txt", headers: ["if-none-match": "W/\(etag)"]).status == .notModified)
        #expect(try await app.test(.GET, "/file/small.txt", headers: ["if-modified-since": lastModified]).status == .notModified)
        #expect(try await app.test(.GET, "/file/small.txt", headers: ["if-match": "\"nope\""]).status == .preconditionFailed)
        // If-Range with the current ETag honors the range; a stale one sends the whole file.
        let fresh = try await app.test(.GET, "/file/small.txt", headers: ["range": "bytes=0-4", "if-range": etag])
        #expect(fresh.status == .partialContent && fresh.text == "hello")
        let stale = try await app.test(.GET, "/file/small.txt", headers: ["range": "bytes=0-4", "if-range": "\"old\""])
        #expect(stale.status == .ok && stale.text == "hello range")
    }

    @Test func downloadsSetContentDisposition() async throws {
        let r = try await makeApp().test(.GET, "/download")
        let disposition = try #require(r.headers.first(name: "content-disposition"))
        #expect(disposition.hasPrefix("attachment; filename=\"r_sum_ _v2_.txt\"") || disposition.contains("filename=\""))
        #expect(disposition.contains("filename*=UTF-8''r%C3%A9sum%C3%A9"))
        #expect(!disposition.contains("\r") && !disposition.contains("\n"))
        #expect(r.text == "hello range")
        #expect(try await makeApp().test(.GET, "/file/missing.bin").status == .notFound)
    }

    // MARK: Large requests and responses

    @Test func largeRequestAndResponseBodies() async throws {
        let (server, port) = try await startServer(makeApp { $0.maxBodySize = 32 << 20 })
        let body = [UInt8](repeating: 0x41, count: 16 << 20)
        let (head, bytes) = try await send(port, "POST /echo-size HTTP/1.1\r\nHost: x\r\nContent-Length: \(body.count)\r\nConnection: close\r\n\r\n", body: body)
        #expect(head.hasPrefix("HTTP/1.1 200"))
        #expect(String(decoding: bytes, as: UTF8.self) == String(16 << 20))
        let (hugeHead, huge) = try await send(port, "GET /huge/64 HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        #expect(hugeHead.contains("content-length: \(64 << 20)"))
        #expect(huge.count == 64 << 20)
        await server.shutdown()
    }

    @Test func slowReadersAreDisconnected() async throws {
        // A client that requests a big response and never reads it ties up the connection; the
        // write-stall guard closes it after idleTimeout.
        let (server, port) = try await startServer(makeApp { $0.idleTimeout = .milliseconds(500) })
        let client = try await RawClient.connect(port: port)
        try await client.channel.setOption(.autoRead, value: false)
        try await client.send("GET /huge/256 HTTP/1.1\r\nHost: x\r\n\r\n")
        let deadline = ContinuousClock.now + .seconds(5)
        while server.connectionCount > 0 && ContinuousClock.now < deadline { try await Task.sleep(for: .milliseconds(50)) }
        #expect(server.connectionCount == 0, "a stalled reader must not hold the connection forever")
        client.close()
        await server.shutdown()
    }
}
