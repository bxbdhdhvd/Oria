// swift-tools-version:6.0
import PackageDescription

let package = Package(
    name: "Oria",
    platforms: [.macOS(.v15)],
    products: [
        .library(name: "Oria", targets: ["Oria"]),
        .executable(name: "oria-example", targets: ["OriaExample"]),
        .executable(name: "oria-bench", targets: ["OriaBench"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.81.0"),
        .package(url: "https://github.com/apple/swift-nio-extras.git", from: "1.24.0"),
        .package(url: "https://github.com/apple/swift-atomics.git", from: "1.2.0"),
        .package(url: "https://github.com/apple/swift-nio-ssl.git", from: "2.29.0"),
        .package(url: "https://github.com/apple/swift-nio-http2.git", from: "1.36.0"),
    ],
    targets: [
        .target(
            name: "Oria",
            dependencies: [
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
                .product(name: "NIOWebSocket", package: "swift-nio"),
                .product(name: "NIOTLS", package: "swift-nio"),
                .product(name: "_NIOFileSystem", package: "swift-nio"),
                .product(name: "NIOSSL", package: "swift-nio-ssl"),
                .product(name: "NIOHTTP2", package: "swift-nio-http2"),
                .product(name: "NIOHTTPCompression", package: "swift-nio-extras"),
                .product(name: "Atomics", package: "swift-atomics"),
            ]
        ),
        .executableTarget(
            name: "OriaExample",
            dependencies: ["Oria"]
        ),
        .executableTarget(
            name: "OriaBench",
            dependencies: [
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
                .product(name: "NIOWebSocket", package: "swift-nio"),
            ]
        ),
        .testTarget(
            name: "OriaTests",
            dependencies: [
                "Oria",
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOSSL", package: "swift-nio-ssl"),
                .product(name: "NIOHTTP2", package: "swift-nio-http2"),
                .product(name: "NIOWebSocket", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
            ]
        ),
    ]
)
