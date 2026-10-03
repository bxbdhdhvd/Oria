import NIOPosix

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

    /// The current date header, formatted at most once per second per thread.
    static func now() -> String {
        let seconds = time(nil)
        let cache = DateCache.current
        if cache.seconds != seconds {
            cache.seconds = seconds
            cache.value = format(seconds)
        }
        return cache.value
    }

    private final class DateCache {
        var seconds: time_t = -1
        var value = ""

        /// One cache per thread: no locks, no sharing.
        static var current: DateCache {
            if let existing = storage.currentValue { return existing }
            let cache = DateCache()
            storage.currentValue = cache
            return cache
        }

        nonisolated(unsafe) static let storage = ThreadSpecificVariable<DateCache>()
    }

    static func format(_ seconds: time_t) -> String {
        var t = seconds
        var tm = tm()
        gmtime_r(&t, &tm)
        return "\(days[Int(tm.tm_wday)]), \(pad(tm.tm_mday)) \(months[Int(tm.tm_mon)]) \(tm.tm_year + 1900) "
            + "\(pad(tm.tm_hour)):\(pad(tm.tm_min)):\(pad(tm.tm_sec)) GMT"
    }

    /// Parses an IMF-fixdate (`Sun, 06 Nov 1994 08:49:37 GMT`) into seconds since 1970.
    static func parse(_ string: String) -> Int64? {
        let parts = string.split(separator: " ")
        guard parts.count == 6, parts[5] == "GMT",
            let day = Int(parts[1]), let month = months.firstIndex(of: String(parts[2])), let year = Int(parts[3])
        else { return nil }
        let clock = parts[4].split(separator: ":").compactMap { Int($0) }
        guard clock.count == 3 else { return nil }
        var tm = tm()
        tm.tm_year = Int32(year - 1900)
        tm.tm_mon = Int32(month)
        tm.tm_mday = Int32(day)
        tm.tm_hour = Int32(clock[0])
        tm.tm_min = Int32(clock[1])
        tm.tm_sec = Int32(clock[2])
        return Int64(timegm(&tm))
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
