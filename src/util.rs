pub fn fnv64(s: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

pub fn content_id(s: &str) -> String {
    format!("{:016x}", fnv64(s))
}

pub fn urlencode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

pub fn truncate(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}

/// Full cause chain: reqwest Display says only "error sending request",
/// the real reason (dns / connect refused / timeout) sits in `.source()`.
pub fn chain(e: &dyn std::error::Error) -> String {
    let mut s = e.to_string();
    let mut src = e.source();
    while let Some(c) = src {
        s.push_str(&format!(": {c}"));
        src = c.source();
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fnv_vectors() {
        assert_eq!(fnv64(""), 0xcbf29ce484222325);
        assert_eq!(fnv64("a"), 0xaf63dc4c8601ec8c);
        assert_eq!(content_id("x").len(), 16);
    }

    #[test]
    fn chain_walks_sources() {
        let e = std::io::Error::new(std::io::ErrorKind::Other, "root");
        let wrapped: Box<dyn std::error::Error> = format!("outer: {e}").into();
        // our wrapped error has no source; chain must not hang/panic
        assert!(chain(&*wrapped).contains("outer"));
        assert_eq!(chain(&e), "root");
    }
}
