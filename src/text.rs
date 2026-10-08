/// Pull `@all` and `@id` out of a post. Routing is later; the record keeps them.
pub fn mentions(text: &str) -> Vec<String> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'@' {
            let start = i + 1;
            let mut j = start;
            while j < bytes.len()
                && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_' || bytes[j] == b'-')
            {
                j += 1;
            }
            if j > start {
                let mention = text[start..j].to_string();
                if !out.iter().any(|m| m == &mention) {
                    out.push(mention);
                }
            }
            i = j;
        } else {
            i += 1;
        }
    }
    out
}

/// Levels 3 and 4 still get stored on the socket, never with a key in them.
pub fn redact(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if let Some(skip) = secret_at(bytes, i) {
            out.push_str("***");
            i += skip;
            continue;
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

fn secret_at(bytes: &[u8], i: usize) -> Option<usize> {
    const MARKERS: &[&[u8]] = &[b"sk-", b"xai-", b"key-", b"inlet_"];
    for marker in MARKERS {
        if bytes[i..].starts_with(marker) {
            let mut j = i + marker.len();
            while j < bytes.len() && is_secret_char(bytes[j]) {
                j += 1;
            }
            if j > i + marker.len() {
                return Some(j - i);
            }
        }
    }
    const AUTH: &[u8] = b"Authorization";
    if bytes[i..].len() >= AUTH.len() && eq_ignore_ascii(bytes, i, AUTH) {
        let mut j = i + AUTH.len();
        while j < bytes.len() && bytes[j] != b'\n' && bytes[j] != b'\r' {
            j += 1;
        }
        return Some(j - i);
    }
    None
}

fn is_secret_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'-' || b == b'.'
}

fn eq_ignore_ascii(bytes: &[u8], i: usize, needle: &[u8]) -> bool {
    bytes[i..i + needle.len()].eq_ignore_ascii_case(needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_mentions() {
        assert_eq!(
            mentions("hey @all and @01ARZ3NDEKTSV4RRFFQ69G5FAV, plus @pi"),
            vec!["all", "01ARZ3NDEKTSV4RRFFQ69G5FAV", "pi"]
        );
        assert!(mentions("no one").is_empty());
    }

    #[test]
    fn strips_keys() {
        let s = redact("Authorization: Bearer sk-abcDEF123 xai-zzz key-one inlet_deadbeef");
        assert!(!s.contains("sk-abc"));
        assert!(!s.contains("xai-zzz"));
        assert!(!s.contains("key-one"));
        assert!(!s.contains("inlet_dead"));
        assert!(!s.to_lowercase().contains("bearer"));
    }
}
