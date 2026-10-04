import Foundation
import NIOCore
import Testing

@testable import Oria

/// Differential tests: FastJSON must produce JSON equal to `Response.defaultEncoder`'s.
@Suite struct FastJSONTests {
    static func fast<T: Encodable>(_ value: T) throws -> String {
        var buffer = ByteBuffer()
        try FastJSON.encode(value, into: &buffer)
        return String(buffer: buffer)
    }

    static func foundation<T: Encodable>(_ value: T) throws -> String {
        String(decoding: try Response.defaultEncoder.encode(value), as: UTF8.self)
    }

    /// Parses and re-serializes with sorted keys, so equal JSON values compare equal.
    static func canonical(_ s: String) throws -> Data {
        let object = try JSONSerialization.jsonObject(with: Data(s.utf8), options: [.fragmentsAllowed])
        return try JSONSerialization.data(withJSONObject: object, options: [.sortedKeys, .fragmentsAllowed])
    }

    /// Same JSON value (object key order may differ: Foundation uses hash order).
    static func expectEquivalent<T: Encodable>(_ value: T, sourceLocation: SourceLocation = #_sourceLocation) throws {
        let a = try fast(value)
        let b = try foundation(value)
        #expect(try Self.canonical(a) == Self.canonical(b), "fast: \(a)\nfoundation: \(b)", sourceLocation: sourceLocation)
    }

    struct Address: Codable { var street: String; var zip: String? }
    struct User: Codable {
        var id: Int
        var name: String
        var email: String?
        var tags: [String]
        var score: Double
        var ratio: Float
        var active: Bool
        var address: Address
        var history: [Address]
        var meta: [String: Int]
        var created: Date
        var avatar: Data
        var site: URL
        var balance: Decimal
        var small: Int8
        var big: UInt64
        var nothing: String? = nil
    }

    @Test func matchesFoundationForTypicalModels() throws {
        let user = User(
            id: 42, name: "Ada \"Countess\" Lovelace\n\t\\ é 😀 \u{1}", email: nil, tags: ["a/b", "", "x"],
            score: 0.1, ratio: 2.5, active: true, address: Address(street: "1 Main St", zip: nil),
            history: [Address(street: "a", zip: "1"), Address(street: "b", zip: nil)], meta: ["x": 1, "y": -2],
            created: Date(timeIntervalSince1970: 1_700_000_000.75), avatar: Data([0, 1, 2, 255]),
            site: URL(string: "https://example.com/a?b=c&d=e")!, balance: Decimal(string: "12.340")!, small: -128,
            big: .max)
        try Self.expectEquivalent(user)
        try Self.expectEquivalent([user, user])
        try Self.expectEquivalent(["message": "Hello, World!"])
        try Self.expectEquivalent([1: "x", 2: "y"])
        try Self.expectEquivalent("top level string")
        try Self.expectEquivalent(Optional<Int>.none)
        try Self.expectEquivalent([[Int]]([[], [1], [1, 2]]))
        try Self.expectEquivalent([String: [String: Bool]]())
    }

    @Test func numbersAreFormattedExactlyLikeFoundation() throws {
        let doubles: [Double] = [
            0, -0.0, 1, -1, 1.5, 0.1, 1e21, 1e-7, 123456789012345680, .leastNonzeroMagnitude, .greatestFiniteMagnitude,
            .pi, 1e16, 2e15, 100, 1e300, -2.5e-300,
        ]
        for value in doubles { #expect(try Self.fast([value]) == Self.foundation([value]), "\(value)") }
        let floats: [Float] = [0.1, 1, 3.4e38, 1e-7, 16_777_216, -0.5]
        for value in floats { #expect(try Self.fast([value]) == Self.foundation([value]), "\(value)") }
        #expect(try Self.fast([Int.min, Int.max]) == Self.foundation([Int.min, Int.max]))
        #expect(try Self.fast([UInt64.max]) == Self.foundation([UInt64.max]))
        // Strings with every control character and the escapes.
        let all = String((0..<128).map { Character(Unicode.Scalar(UInt8($0))) }) + "é\u{2028}😀"
        #expect(try Self.fast([all]) == Self.foundation([all]))
    }

    @Test func nonFiniteNumbersAreRejected() {
        #expect(throws: (any Error).self) { try Self.fast([Double.nan]) }
        #expect(throws: (any Error).self) { try Self.fast(["x": Double.infinity]) }
    }

    /// A random JSON-shaped value, encoded through every container kind.
    indirect enum Value: Encodable {
        case null, bool(Bool), int(Int), double(Double), string(String)
        case array([Value]), object([(String, Value)]), nestedViaContainers([(String, Value)])

        func encode(to encoder: any Encoder) throws {
            switch self {
            case .null:
                var c = encoder.singleValueContainer()
                try c.encodeNil()
            case .bool(let v):
                var c = encoder.singleValueContainer()
                try c.encode(v)
            case .int(let v):
                var c = encoder.singleValueContainer()
                try c.encode(v)
            case .double(let v):
                var c = encoder.singleValueContainer()
                try c.encode(v)
            case .string(let v):
                var c = encoder.singleValueContainer()
                try c.encode(v)
            case .array(let items):
                var c = encoder.unkeyedContainer()
                for item in items { try c.encode(item) }
            case .object(let pairs):
                var c = encoder.container(keyedBy: AnyKey.self)
                for (k, v) in pairs { try c.encode(v, forKey: AnyKey(k)) }
            case .nestedViaContainers(let pairs):
                // nestedContainer / nestedUnkeyedContainer paths.
                var c = encoder.container(keyedBy: AnyKey.self)
                for (k, v) in pairs {
                    var inner = c.nestedUnkeyedContainer(forKey: AnyKey(k))
                    try inner.encode(v)
                    var obj = inner.nestedContainer(keyedBy: AnyKey.self)
                    try obj.encode(k, forKey: AnyKey("k"))
                }
            }
        }
    }

    struct AnyKey: CodingKey {
        var stringValue: String
        var intValue: Int? { nil }
        init(_ s: String) { stringValue = s }
        init?(stringValue: String) { self.stringValue = stringValue }
        init?(intValue: Int) { nil }
    }

    static func random(_ g: inout some RandomNumberGenerator, depth: Int = 0) -> Value {
        let pick = depth > 4 ? Int.random(in: 0..<5, using: &g) : Int.random(in: 0..<8, using: &g)
        func str() -> String {
            String((0..<Int.random(in: 0..<8, using: &g)).map { _ in
                Character(Unicode.Scalar([0x22, 0x5C, 0x2F, 0x0A, 0x01, 0x41, 0xE9, 0x1F600].randomElement(using: &g)!)!)
            })
        }
        switch pick {
        case 0: return .null
        case 1: return .bool(Bool.random(using: &g))
        case 2: return .int(Int.random(in: Int.min...Int.max, using: &g))
        case 3: return .double(Double.random(in: -1e20...1e20, using: &g))
        case 4: return .string(str())
        case 5: return .array((0..<Int.random(in: 0..<5, using: &g)).map { _ in random(&g, depth: depth + 1) })
        case 6:
            var seen = Set<String>()
            return .object((0..<Int.random(in: 0..<5, using: &g)).compactMap { i in
                let k = str() + "\(i)"
                return seen.insert(k).inserted ? (k, random(&g, depth: depth + 1)) : nil
            })
        default:
            return .nestedViaContainers((0..<Int.random(in: 0..<3, using: &g)).map { i in ("n\(i)", random(&g, depth: depth + 1)) })
        }
    }

    @Test func randomValuesMatchFoundation() throws {
        var g = SystemRandomNumberGenerator()
        for _ in 0..<2000 { try Self.expectEquivalent(Self.random(&g)) }
    }

    /// Containers used out of nesting order: FastJSON refuses, and `res.json` falls back to
    /// JSONEncoder, so the response is still correct.
    struct OutOfOrder: Encodable {
        func encode(to encoder: any Encoder) throws {
            var c = encoder.container(keyedBy: AnyKey.self)
            var first = c.nestedUnkeyedContainer(forKey: AnyKey("first"))
            try c.encode(1, forKey: AnyKey("between"))
            try first.encode(2)  // reusing a container after a sibling was written
        }
    }

    struct UsesSuperEncoder: Encodable {
        func encode(to encoder: any Encoder) throws {
            var c = encoder.container(keyedBy: AnyKey.self)
            try c.encode(1, forKey: AnyKey("a"))
            try "s".encode(to: c.superEncoder())
        }
    }

    struct EncodesNothing: Encodable {
        func encode(to encoder: any Encoder) throws {}
    }

    @Test func unusualEncodersFallBackToFoundation() async throws {
        #expect(throws: FastJSON.Unsupported.self) { try Self.fast(OutOfOrder()) }
        #expect(throws: FastJSON.Unsupported.self) { try Self.fast(UsesSuperEncoder()) }
        try Self.expectEquivalent(["x": EncodesNothing()])

        let app = Oria(configuration: testConfig())
        app.get("/odd") { _, res in try res.json(OutOfOrder()) }
        app.get("/super") { _, res in try res.json(UsesSuperEncoder()) }
        let odd = try await app.test(.GET, "/odd")
        #expect(try JSONSerialization.jsonObject(with: Data(odd.text.utf8)) is [String: Any])
        let sup = try await app.test(.GET, "/super")
        let expected = try Self.foundation(UsesSuperEncoder())
        #expect(sup.status == .ok)
        #expect(try Self.canonical(sup.text) == Self.canonical(expected))
    }
}
