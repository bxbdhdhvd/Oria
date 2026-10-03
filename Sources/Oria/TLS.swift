import NIOSSL

/// HTTPS settings. Assign to `Oria.Configuration.tls` to serve TLS (and HTTP/2 via ALPN).
///
/// ```swift
/// config.tls = try .files(certificateChain: "cert.pem", privateKey: "key.pem")
/// ```
public struct TLSOptions: Sendable {
    /// The underlying swift-nio-ssl configuration. Oria sets the ALPN protocols itself.
    public var configuration: TLSConfiguration

    /// Uses a fully custom configuration (mutual TLS, custom ciphers, ...).
    public init(configuration: TLSConfiguration) {
        self.configuration = configuration
    }

    /// Loads a PEM certificate chain (leaf first) and a PEM private key.
    /// Only TLS 1.2 and 1.3 are accepted.
    public static func files(certificateChain: String, privateKey: String) throws -> TLSOptions {
        let chain = try NIOSSLCertificate.fromPEMFile(certificateChain)
        let key = try NIOSSLPrivateKey(file: privateKey, format: .pem)
        return pem(chain: chain, key: key)
    }

    /// Builds options from in-memory PEM strings (e.g. read from a secret store).
    public static func pem(certificateChain: String, privateKey: String) throws -> TLSOptions {
        let chain = try NIOSSLCertificate.fromPEMBytes(Array(certificateChain.utf8))
        let key = try NIOSSLPrivateKey(bytes: Array(privateKey.utf8), format: .pem)
        return pem(chain: chain, key: key)
    }

    private static func pem(chain: [NIOSSLCertificate], key: NIOSSLPrivateKey) -> TLSOptions {
        var configuration = TLSConfiguration.makeServerConfiguration(
            certificateChain: chain.map { .certificate($0) },
            privateKey: .privateKey(key)
        )
        configuration.minimumTLSVersion = .tlsv12
        // TLS 1.2: forward-secret AEAD suites only (TLS 1.3 suites are always AEAD).
        configuration.cipherSuites = [
            "ECDHE-ECDSA-AES128-GCM-SHA256", "ECDHE-RSA-AES128-GCM-SHA256",
            "ECDHE-ECDSA-AES256-GCM-SHA384", "ECDHE-RSA-AES256-GCM-SHA384",
            "ECDHE-ECDSA-CHACHA20-POLY1305", "ECDHE-RSA-CHACHA20-POLY1305",
        ].joined(separator: ":")
        return TLSOptions(configuration: configuration)
    }
}
