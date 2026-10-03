#if canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#elseif canImport(Darwin)
import Darwin
#endif

/// RFC 7231 IMF-fixdate formatting without Foundation's (slow) DateFormatter.
enum HTTPDate {
    private static let days = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    private static let months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    static func now() -> String {
        format(time(nil))
    }

    static func format(_ seconds: time_t) -> String {
        var t = seconds
        var tm = tm()
        gmtime_r(&t, &tm)
        return "\(days[Int(tm.tm_wday)]), \(pad(tm.tm_mday)) \(months[Int(tm.tm_mon)]) \(tm.tm_year + 1900) "
            + "\(pad(tm.tm_hour)):\(pad(tm.tm_min)):\(pad(tm.tm_sec)) GMT"
    }

    private static func pad(_ v: Int32) -> String {
        v < 10 ? "0\(v)" : String(v)
    }
}

/// Minimal extension → MIME type table for static files and `res.type()`.
public enum MIME {
    static let table: [String: String] = [
        "html": "text/html; charset=utf-8",
        "htm": "text/html; charset=utf-8",
        "css": "text/css; charset=utf-8",
        "js": "text/javascript; charset=utf-8",
        "mjs": "text/javascript; charset=utf-8",
        "json": "application/json; charset=utf-8",
        "map": "application/json; charset=utf-8",
        "txt": "text/plain; charset=utf-8",
        "text": "text/plain; charset=utf-8",
        "csv": "text/csv; charset=utf-8",
        "xml": "application/xml; charset=utf-8",
        "svg": "image/svg+xml",
        "png": "image/png",
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "gif": "image/gif",
        "webp": "image/webp",
        "avif": "image/avif",
        "ico": "image/x-icon",
        "woff": "font/woff",
        "woff2": "font/woff2",
        "ttf": "font/ttf",
        "otf": "font/otf",
        "pdf": "application/pdf",
        "zip": "application/zip",
        "gz": "application/gzip",
        "wasm": "application/wasm",
        "mp4": "video/mp4",
        "webm": "video/webm",
        "mp3": "audio/mpeg",
        "wav": "audio/wav",
    ]

    public static func type(forExtension ext: String) -> String {
        table[ext.lowercased()] ?? "application/octet-stream"
    }
}
