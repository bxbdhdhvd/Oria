// swift-tools-version:6.0
import PackageDescription

let package = Package(
    name: "Oria",
    platforms: [.macOS(.v14)],
    products: [
        .library(name: "Oria", targets: ["Oria"]),
        .executable(name: "oria-example", targets: ["OriaExample"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.81.0"),
        .package(url: "https://github.com/apple/swift-nio-extras.git", from: "1.24.0"),
        .package(url: "https://github.com/apple/swift-atomics.git", from: "1.2.0"),
    ],
    targets: [
        .target(
            name: "Oria",
            dependencies: [
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
                .product(name: "_NIOFileSystem", package: "swift-nio"),
                .product(name: "NIOHTTPCompression", package: "swift-nio-extras"),
                .product(name: "Atomics", package: "swift-atomics"),
            ]
        ),
        .executableTarget(
            name: "OriaExample",
            dependencies: ["Oria"]
        ),
        .testTarget(
            name: "OriaTests",
            dependencies: [
                "Oria",
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
            ]
        ),
    ]
)
