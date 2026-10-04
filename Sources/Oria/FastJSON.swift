import Foundation
import NIOCore

/// A streaming JSON encoder that writes `Encodable` values straight into a `ByteBuffer`.
///
/// `res.json(value)` uses it instead of Foundation's `JSONEncoder`, which builds an intermediate
/// tree of boxed values; this one emits bytes as `encode(to:)` runs, several times faster for
/// typical API payloads. Output matches `Response.defaultEncoder` (`JSONEncoder` with `.iso8601`
/// dates and `.withoutEscapingSlashes`): same number formatting, base64 `Data`, `URL` as a string,
/// `Decimal` as a number, ISO 8601 `Date`s. Keys keep their encoding order.
///
/// It relies on containers being used in nesting order, which synthesized and almost all
/// hand-written `encode(to:)` implementations do. Anything else (reusing a container after a
/// sibling was written, `superEncoder`, two containers from one encoder, NaN/infinity) throws
/// `FastJSON.Unsupported`, and the caller falls back to `JSONEncoder`.
enum FastJSON {
    struct Unsupported: Error {}

    static func encode<T: Encodable>(_ value: T, into buffer: inout ByteBuffer) throws {
        let writer = JSONWriter(buffer: buffer)
        buffer = ByteBuffer()  // hand the storage to the writer: no copy-on-write
        defer { buffer = writer.buffer }
        try writer.encodeValue(value, level: 0)
        writer.sync(to: 0)
        if writer.failed || writer.slotOpen { throw Unsupported() }
    }
}

/// Shared state of one encoding run. Single-threaded by construction.
final class JSONWriter {
    @exclusivity(unchecked) var buffer: ByteBuffer
    /// Open containers, innermost last: true for objects, false for arrays.
    @exclusivity(unchecked) var stack: [Bool] = []
    /// Element counts of the open containers (for commas).
    @exclusivity(unchecked) var counts: [Int] = []
    /// Identity of each open container, so a stale container can't write into a newer sibling.
    @exclusivity(unchecked) var ids: [Int] = []
    @exclusivity(unchecked) var nextID = 0
    /// A key (or array slot, or the top level) is waiting for its value.
    @exclusivity(unchecked) var slotOpen = true
    @exclusivity(unchecked) var failed = false

    init(buffer: ByteBuffer) {
        self.buffer = buffer
        stack.reserveCapacity(8)
        counts.reserveCapacity(8)
        ids.reserveCapacity(8)
    }

    /// Closes containers deeper than `depth` (they're finished once a shallower one is used).
    /// Returns false if the container at `depth` was already closed (out-of-order use).
    @discardableResult
    func sync(to depth: Int) -> Bool {
        if stack.count < depth {
            failed = true
            return false
        }
        while stack.count > depth {
            if slotOpen {
                buffer.writeStaticString("{}")  // a nested value that encoded nothing
                slotOpen = false
            }
            buffer.writeInteger(stack.removeLast() ? UInt8(ascii: "}") : UInt8(ascii: "]"))
            counts.removeLast()
            ids.removeLast()
        }
        return true
    }

    /// Starts an element of the container at `depth`: a comma when needed, then the key.
    func beginElement(depth: Int, id: Int, key: String?) -> Bool {
        guard sync(to: depth), depth > 0, ids[depth - 1] == id, !slotOpen else {
            failed = true
            return false
        }
        if counts[depth - 1] > 0 { buffer.writeInteger(UInt8(ascii: ",")) }
        counts[depth - 1] += 1
        if let key {
            writeString(key)
            buffer.writeInteger(UInt8(ascii: ":"))
        }
        slotOpen = true
        return true
    }

    /// Claims the open slot for a value written at `level`.
    func claimSlot(level: Int) -> Bool {
        guard sync(to: level), slotOpen else {
            failed = true
            return false
        }
        slotOpen = false
        return true
    }

    /// Opens a container; returns its id (-1 on misuse).
    func open(object: Bool, level: Int) -> Int {
        guard claimSlot(level: level) else { return -1 }
        buffer.writeInteger(object ? UInt8(ascii: "{") : UInt8(ascii: "["))
        stack.append(object)
        counts.append(0)
        nextID += 1
        ids.append(nextID)
        return nextID
    }

    // MARK: Values

    func encodeValue<T: Encodable>(_ value: T, level: Int) throws {
        // Types JSONEncoder special-cases (their own Encodable conformance differs).
        if T.self == Data.self {
            guard claimSlot(level: level) else { return }
            writeString((value as! Data).base64EncodedString())
        } else if T.self == URL.self {
            guard claimSlot(level: level) else { return }
            writeString((value as! URL).absoluteString)
        } else if T.self == Decimal.self {
            guard claimSlot(level: level) else { return }
            buffer.writeString((value as! Decimal).description)
        } else if T.self == Date.self {
            guard claimSlot(level: level) else { return }
            writeString((value as! Date).formatted(Date.ISO8601FormatStyle()))
        } else {
            try value.encode(to: JSONEncoderImpl(writer: self, level: level))
            sync(to: level)
            if slotOpen {
                // The value encoded nothing at all: JSONEncoder writes an empty object.
                buffer.writeStaticString("{}")
                slotOpen = false
            }
        }
    }

    func writeNull(level: Int) {
        guard claimSlot(level: level) else { return }
        buffer.writeStaticString("null")
    }

    func writeBool(_ value: Bool, level: Int) {
        guard claimSlot(level: level) else { return }
        if value { buffer.writeStaticString("true") } else { buffer.writeStaticString("false") }
    }

    func writeInteger<I: BinaryInteger & LosslessStringConvertible>(_ value: I, level: Int) {
        guard claimSlot(level: level) else { return }
        buffer.writeString(String(value))
    }

    func writeDouble(_ value: Double, level: Int) {
        guard value.isFinite else {
            failed = true  // JSONEncoder throws; let it produce the error
            return
        }
        guard claimSlot(level: level) else { return }
        writeNumber(value.description)
    }

    func writeFloat(_ value: Float, level: Int) {
        guard value.isFinite else {
            failed = true
            return
        }
        guard claimSlot(level: level) else { return }
        writeNumber(value.description)
    }

    /// `Double.description` minus a trailing `.0`, as JSONEncoder does (`1.0` → `1`).
    private func writeNumber(_ text: String) {
        if text.hasSuffix(".0") {
            buffer.writeString(String(text.dropLast(2)))
        } else {
            buffer.writeString(text)
        }
    }

    func writeStringValue(_ value: String, level: Int) {
        guard claimSlot(level: level) else { return }
        writeString(value)
    }

    /// A quoted, escaped JSON string (`"`, `\` and control characters escaped).
    func writeString(_ value: String) {
        buffer.writeInteger(UInt8(ascii: "\""))
        var needsEscape = false
        for byte in value.utf8 where byte < 0x20 || byte == UInt8(ascii: "\"") || byte == UInt8(ascii: "\\") {
            needsEscape = true
            break
        }
        if !needsEscape {
            buffer.writeString(value)
        } else {
            for byte in value.utf8 {
                switch byte {
                case UInt8(ascii: "\""): buffer.writeStaticString("\\\"")
                case UInt8(ascii: "\\"): buffer.writeStaticString("\\\\")
                case 0x0A: buffer.writeStaticString("\\n")
                case 0x0D: buffer.writeStaticString("\\r")
                case 0x09: buffer.writeStaticString("\\t")
                case 0x08: buffer.writeStaticString("\\b")
                case 0x0C: buffer.writeStaticString("\\f")
                case 0..<0x20:
                    buffer.writeStaticString("\\u00")
                    buffer.writeInteger(Self.hex[Int(byte >> 4)])
                    buffer.writeInteger(Self.hex[Int(byte & 0xF)])
                default:
                    buffer.writeInteger(byte)
                }
            }
        }
        buffer.writeInteger(UInt8(ascii: "\""))
    }

    private static let hex: [UInt8] = Array("0123456789abcdef".utf8)
}

// MARK: - Encoder and containers

private struct JSONEncoderImpl: Encoder {
    let writer: JSONWriter
    /// Container depth this encoder's value lives at.
    let level: Int

    var codingPath: [any CodingKey] { [] }
    var userInfo: [CodingUserInfoKey: Any] { [:] }

    func container<Key: CodingKey>(keyedBy type: Key.Type) -> KeyedEncodingContainer<Key> {
        let id = writer.open(object: true, level: level)
        return KeyedEncodingContainer(KeyedContainer<Key>(writer: writer, depth: level + 1, id: id))
    }

    func unkeyedContainer() -> any UnkeyedEncodingContainer {
        let id = writer.open(object: false, level: level)
        return UnkeyedContainer(writer: writer, depth: level + 1, id: id)
    }

    func singleValueContainer() -> any SingleValueEncodingContainer {
        SingleValueContainer(writer: writer, level: level)
    }
}

private struct KeyedContainer<Key: CodingKey>: KeyedEncodingContainerProtocol {
    let writer: JSONWriter
    let depth: Int
    let id: Int
    var codingPath: [any CodingKey] { [] }

    private func begin(_ key: Key) -> Bool { writer.beginElement(depth: depth, id: id, key: key.stringValue) }

    mutating func encodeNil(forKey key: Key) throws { if begin(key) { writer.writeNull(level: depth) } }
    mutating func encode(_ value: Bool, forKey key: Key) throws { if begin(key) { writer.writeBool(value, level: depth) } }
    mutating func encode(_ value: String, forKey key: Key) throws { if begin(key) { writer.writeStringValue(value, level: depth) } }
    mutating func encode(_ value: Double, forKey key: Key) throws { if begin(key) { writer.writeDouble(value, level: depth) } }
    mutating func encode(_ value: Float, forKey key: Key) throws { if begin(key) { writer.writeFloat(value, level: depth) } }
    mutating func encode(_ value: Int, forKey key: Key) throws { if begin(key) { writer.writeInteger(value, level: depth) } }
    mutating func encode(_ value: Int8, forKey key: Key) throws { if begin(key) { writer.writeInteger(value, level: depth) } }
    mutating func encode(_ value: Int16, forKey key: Key) throws { if begin(key) { writer.writeInteger(value, level: depth) } }
    mutating func encode(_ value: Int32, forKey key: Key) throws { if begin(key) { writer.writeInteger(value, level: depth) } }
    mutating func encode(_ value: Int64, forKey key: Key) throws { if begin(key) { writer.writeInteger(value, level: depth) } }
    mutating func encode(_ value: UInt, forKey key: Key) throws { if begin(key) { writer.writeInteger(value, level: depth) } }
    mutating func encode(_ value: UInt8, forKey key: Key) throws { if begin(key) { writer.writeInteger(value, level: depth) } }
    mutating func encode(_ value: UInt16, forKey key: Key) throws { if begin(key) { writer.writeInteger(value, level: depth) } }
    mutating func encode(_ value: UInt32, forKey key: Key) throws { if begin(key) { writer.writeInteger(value, level: depth) } }
    mutating func encode(_ value: UInt64, forKey key: Key) throws { if begin(key) { writer.writeInteger(value, level: depth) } }
    mutating func encode<T: Encodable>(_ value: T, forKey key: Key) throws {
        if begin(key) { try writer.encodeValue(value, level: depth) }
    }

    mutating func nestedContainer<NestedKey: CodingKey>(
        keyedBy keyType: NestedKey.Type, forKey key: Key
    ) -> KeyedEncodingContainer<NestedKey> {
        let id = begin(key) ? writer.open(object: true, level: depth) : -1
        return KeyedEncodingContainer(KeyedContainer<NestedKey>(writer: writer, depth: depth + 1, id: id))
    }

    mutating func nestedUnkeyedContainer(forKey key: Key) -> any UnkeyedEncodingContainer {
        let id = begin(key) ? writer.open(object: false, level: depth) : -1
        return UnkeyedContainer(writer: writer, depth: depth + 1, id: id)
    }

    mutating func superEncoder() -> any Encoder {
        writer.failed = true
        return JSONEncoderImpl(writer: writer, level: Int.max)
    }

    mutating func superEncoder(forKey key: Key) -> any Encoder {
        writer.failed = true
        return JSONEncoderImpl(writer: writer, level: Int.max)
    }
}

private struct UnkeyedContainer: UnkeyedEncodingContainer {
    let writer: JSONWriter
    let depth: Int
    let id: Int
    var codingPath: [any CodingKey] { [] }
    var count: Int {
        writer.ids.indices.contains(depth - 1) && writer.ids[depth - 1] == id ? writer.counts[depth - 1] : 0
    }

    private func begin() -> Bool { writer.beginElement(depth: depth, id: id, key: nil) }

    mutating func encodeNil() throws { if begin() { writer.writeNull(level: depth) } }
    mutating func encode(_ value: Bool) throws { if begin() { writer.writeBool(value, level: depth) } }
    mutating func encode(_ value: String) throws { if begin() { writer.writeStringValue(value, level: depth) } }
    mutating func encode(_ value: Double) throws { if begin() { writer.writeDouble(value, level: depth) } }
    mutating func encode(_ value: Float) throws { if begin() { writer.writeFloat(value, level: depth) } }
    mutating func encode(_ value: Int) throws { if begin() { writer.writeInteger(value, level: depth) } }
    mutating func encode(_ value: Int8) throws { if begin() { writer.writeInteger(value, level: depth) } }
    mutating func encode(_ value: Int16) throws { if begin() { writer.writeInteger(value, level: depth) } }
    mutating func encode(_ value: Int32) throws { if begin() { writer.writeInteger(value, level: depth) } }
    mutating func encode(_ value: Int64) throws { if begin() { writer.writeInteger(value, level: depth) } }
    mutating func encode(_ value: UInt) throws { if begin() { writer.writeInteger(value, level: depth) } }
    mutating func encode(_ value: UInt8) throws { if begin() { writer.writeInteger(value, level: depth) } }
    mutating func encode(_ value: UInt16) throws { if begin() { writer.writeInteger(value, level: depth) } }
    mutating func encode(_ value: UInt32) throws { if begin() { writer.writeInteger(value, level: depth) } }
    mutating func encode(_ value: UInt64) throws { if begin() { writer.writeInteger(value, level: depth) } }
    mutating func encode<T: Encodable>(_ value: T) throws {
        if begin() { try writer.encodeValue(value, level: depth) }
    }

    mutating func nestedContainer<NestedKey: CodingKey>(keyedBy keyType: NestedKey.Type) -> KeyedEncodingContainer<NestedKey> {
        let id = begin() ? writer.open(object: true, level: depth) : -1
        return KeyedEncodingContainer(KeyedContainer<NestedKey>(writer: writer, depth: depth + 1, id: id))
    }

    mutating func nestedUnkeyedContainer() -> any UnkeyedEncodingContainer {
        let id = begin() ? writer.open(object: false, level: depth) : -1
        return UnkeyedContainer(writer: writer, depth: depth + 1, id: id)
    }

    mutating func superEncoder() -> any Encoder {
        writer.failed = true
        return JSONEncoderImpl(writer: writer, level: Int.max)
    }
}

private struct SingleValueContainer: SingleValueEncodingContainer {
    let writer: JSONWriter
    let level: Int
    var codingPath: [any CodingKey] { [] }

    mutating func encodeNil() throws { writer.writeNull(level: level) }
    mutating func encode(_ value: Bool) throws { writer.writeBool(value, level: level) }
    mutating func encode(_ value: String) throws { writer.writeStringValue(value, level: level) }
    mutating func encode(_ value: Double) throws { writer.writeDouble(value, level: level) }
    mutating func encode(_ value: Float) throws { writer.writeFloat(value, level: level) }
    mutating func encode(_ value: Int) throws { writer.writeInteger(value, level: level) }
    mutating func encode(_ value: Int8) throws { writer.writeInteger(value, level: level) }
    mutating func encode(_ value: Int16) throws { writer.writeInteger(value, level: level) }
    mutating func encode(_ value: Int32) throws { writer.writeInteger(value, level: level) }
    mutating func encode(_ value: Int64) throws { writer.writeInteger(value, level: level) }
    mutating func encode(_ value: UInt) throws { writer.writeInteger(value, level: level) }
    mutating func encode(_ value: UInt8) throws { writer.writeInteger(value, level: level) }
    mutating func encode(_ value: UInt16) throws { writer.writeInteger(value, level: level) }
    mutating func encode(_ value: UInt32) throws { writer.writeInteger(value, level: level) }
    mutating func encode(_ value: UInt64) throws { writer.writeInteger(value, level: level) }
    mutating func encode<T: Encodable>(_ value: T) throws { try writer.encodeValue(value, level: level) }
}
