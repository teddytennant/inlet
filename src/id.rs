use std::fs::File;
use std::io::Read;
use std::time::{SystemTime, UNIX_EPOCH};

const CROCKFORD: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub fn ulid() -> String {
    let mut bytes = [0u8; 16];
    let ms = now_ms();
    bytes[0] = (ms >> 40) as u8;
    bytes[1] = (ms >> 32) as u8;
    bytes[2] = (ms >> 24) as u8;
    bytes[3] = (ms >> 16) as u8;
    bytes[4] = (ms >> 8) as u8;
    bytes[5] = ms as u8;
    if let Ok(mut f) = File::open("/dev/urandom") {
        let _ = f.read_exact(&mut bytes[6..]);
    }
    encode(&bytes)
}

fn encode(bytes: &[u8; 16]) -> String {
    let mut out = String::with_capacity(26);
    // 128 bits, 26 Crockford characters. The first character carries 2 bits.
    out.push(CROCKFORD[((bytes[0] & 0b1110_0000) >> 5) as usize] as char);
    let mut acc: u32 = (bytes[0] & 0b0001_1111) as u32;
    let mut bits = 5;
    for &b in &bytes[1..] {
        acc = (acc << 8) | b as u32;
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out.push(CROCKFORD[((acc >> bits) & 31) as usize] as char);
        }
    }
    out
}

pub fn token() -> String {
    let mut bytes = [0u8; 16];
    if let Ok(mut f) = File::open("/dev/urandom") {
        let _ = f.read_exact(&mut bytes);
    }
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ulid_shape() {
        let id = ulid();
        assert_eq!(id.len(), 26);
        assert!(id
            .bytes()
            .all(|b| CROCKFORD.contains(&b) || CROCKFORD.contains(&b.to_ascii_uppercase())));
        assert_ne!(ulid(), ulid());
    }
}
