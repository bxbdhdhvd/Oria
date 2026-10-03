import Foundation
import NIOCore
import NIOHTTP1

/// Serializes HTTP/1.x responses straight into a `ByteBuffer`.
///
/// This replaces NIO's `HTTPResponseEncoder` on plain HTTP/1.1 connections: the status line,
/// headers and (small) body go into one buffer, so a typical response is one allocation and one
/// `writev` entry, with header validation done in the same pass that copies the bytes.
enum HTTP1Writer {
    /// Bodies up to this size are copied behind the head (one buffer); larger ones are written as a
    /// second buffer to avoid the copy.
    static let coalesceLimit = 16 * 1024

    /// `"HTTP/1.1 200 OK\r\n"` for every standard code, built once.
    private static let statusLines: [[UInt8]] = (0..<600).map { code in
        guard code >= 100 else { return [] }
        let status = HTTPResponseStatus(statusCode: code)
        return Array("HTTP/1.1 \(code) \(status.reasonPhrase)\r\n".utf8)
    }

    struct Head {
        /// `Connection: close` requested by the application.
        var closeRequested = false
    }

    /// Appends the status line and headers. Adds `date`, `content-length` (or chunked framing),
    /// `connection` and `server` the way `Server.prepareHeaders` does. Returns nil if a header
    /// contains CR, LF or NUL or an invalid name (the caller answers 500 instead).
    static func writeHead(
        into buffer: inout ByteBuffer, version: HTTPVersion, status: HTTPResponseStatus, headers: HTTPHeaders,
        contentLength: Int?, chunked: Bool, keepAlive: Bool, serverName: String?
    ) -> Head? {
        let code = status.code
        let isCustom: Bool
        if case .custom = status { isCustom = true } else { isCustom = false }
        if version.major == 1 && version.minor == 1 && code < 600 && code >= 100 && !isCustom {
            buffer.writeBytes(statusLines[Int(code)])
        } else {
            buffer.writeString("HTTP/\(version.major).\(version.minor) \(code) \(status.reasonPhrase)\r\n")
        }

        var head = Head()
        var hasDate = false
        for (name, value) in headers {
            guard isValidName(name), isValidValue(value) else { return nil }
            switch classify(name) {
            case .contentLength, .transferEncoding:
                continue  // framing is ours
            case .connection:
                if hasToken(value, "close") { head.closeRequested = true }
                continue  // written below from the final keep-alive decision
            case .server where serverName != nil:
                continue
            case .date:
                hasDate = true
            default:
                break
            }
            buffer.writeString(name)
            buffer.writeStaticString(": ")
            buffer.writeString(value)
            buffer.writeStaticString("\r\n")
        }
        if !hasDate {
            buffer.writeStaticString("date: ")
            buffer.writeString(HTTPDate.now())
            buffer.writeStaticString("\r\n")
        }
        if let serverName, isValidValue(serverName) {
            buffer.writeStaticString("server: ")
            buffer.writeString(serverName)
            buffer.writeStaticString("\r\n")
        }
        if let contentLength {
            buffer.writeStaticString("content-length: ")
            writeDecimal(contentLength, into: &buffer)
            buffer.writeStaticString("\r\n")
        } else if chunked {
            buffer.writeStaticString("transfer-encoding: chunked\r\n")
        }
        if !keepAlive || head.closeRequested {
            buffer.writeStaticString("connection: close\r\n")
        } else if version.major == 1 && version.minor == 0 {
            buffer.writeStaticString("connection: keep-alive\r\n")
        }
        buffer.writeStaticString("\r\n")
        return head
    }

    /// `HTTP/1.1 100 Continue`.
    static func writeContinue(into buffer: inout ByteBuffer) {
        buffer.writeStaticString("HTTP/1.1 100 Continue\r\n\r\n")
    }

    /// Chunked-encoding prefix for a chunk of `count` bytes: `"1f40\r\n"`.
    static func chunkPrefix(_ count: Int, allocator: ByteBufferAllocator) -> ByteBuffer {
        var buffer = allocator.buffer(capacity: 18)
        buffer.writeString(String(count, radix: 16))
        buffer.writeStaticString("\r\n")
        return buffer
    }

    static func writeDecimal(_ value: Int, into buffer: inout ByteBuffer) {
        buffer.writeString(String(value))  // small strings live inline: no allocation
    }

    // MARK: Header checks

    private enum Known { case contentLength, transferEncoding, connection, server, date, other }

    /// Case-insensitive match on the few header names that need special handling.
    private static func classify(_ name: String) -> Known {
        let utf8 = name.utf8
        switch utf8.count {
        case 4: return equalsLowercased(utf8, "date") ? .date : .other
        case 6: return equalsLowercased(utf8, "server") ? .server : .other
        case 10: return equalsLowercased(utf8, "connection") ? .connection : .other
        case 14: return equalsLowercased(utf8, "content-length") ? .contentLength : .other
        case 17: return equalsLowercased(utf8, "transfer-encoding") ? .transferEncoding : .other
        default: return .other
        }
    }

    private static func equalsLowercased(_ utf8: String.UTF8View, _ lower: String) -> Bool {
        utf8.elementsEqual(lower.utf8) { byte, target in
            (byte >= 65 && byte <= 90 ? byte | 0x20 : byte) == target
        }
    }

    /// RFC 9110 token characters.
    static func isValidName(_ name: String) -> Bool {
        if name.isEmpty { return false }
        for byte in name.utf8 where byte <= 32 || byte >= 127 || byte == UInt8(ascii: ":") {
            return false
        }
        return true
    }

    static func isValidValue(_ value: String) -> Bool {
        for byte in value.utf8 where byte == 13 || byte == 10 || byte == 0 {
            return false
        }
        return true
    }

    private static func hasToken(_ value: String, _ token: String) -> Bool {
        value.split(separator: ",").contains { $0.trimmingCharacters(in: .whitespaces).lowercased() == token }
    }
}
