use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::path::Path;

use crate::error::{err, Result};
use crate::model::{decode_record, encode_record, Decoded, Record};

/// `len u32 | crc32 u32 | json`, little-endian.
/// A short or bad final frame is a torn tail and gets cut.
/// A bad frame with a good frame after it is damage, and the daemon stops.
pub struct Ledger {
    file: File,
    len: u64,
}

pub struct Opened {
    pub ledger: Ledger,
    pub records: Vec<Decoded>,
}

pub fn open(path: &Path) -> Result<Opened> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)?;
    let meta = file.metadata()?;
    let mut buf = Vec::with_capacity(meta.len() as usize);
    file.read_to_end(&mut buf)?;
    let (records, keep) = scan(&buf)?;
    if keep < buf.len() as u64 {
        file.set_len(keep)?;
    }
    file.seek(SeekFrom::Start(keep))?;
    Ok(Opened {
        ledger: Ledger { file, len: keep },
        records,
    })
}

pub fn scan(buf: &[u8]) -> Result<(Vec<Decoded>, u64)> {
    let mut records = Vec::new();
    let mut offset = 0usize;
    while offset + 8 <= buf.len() {
        let len = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(buf[offset + 4..offset + 8].try_into().unwrap());
        let body_at = offset + 8;
        if len > 32 * 1024 * 1024 || body_at + len > buf.len() {
            // Declared body does not fit. Torn tail.
            return Ok((records, offset as u64));
        }
        let body = &buf[body_at..body_at + len];
        let good = crc32(body) == crc && decode_frame(body).is_some();
        if !good {
            let after = body_at + len;
            if frame_at(buf, after) {
                return Err(err("ledger damage: bad record followed by a good one"));
            }
            return Ok((records, offset as u64));
        }
        records.push(decode_frame(body).expect("checked"));
        offset = body_at + len;
    }
    Ok((records, offset as u64))
}

fn decode_frame(body: &[u8]) -> Option<Decoded> {
    let value: serde_json::Value = serde_json::from_slice(body).ok()?;
    decode_record(value).ok()
}

fn frame_at(buf: &[u8], offset: usize) -> bool {
    if offset + 8 > buf.len() {
        return false;
    }
    let len = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(buf[offset + 4..offset + 8].try_into().unwrap());
    let body_at = offset + 8;
    if len == 0 || len > 32 * 1024 * 1024 || body_at + len > buf.len() {
        return false;
    }
    let body = &buf[body_at..body_at + len];
    crc32(body) == crc && decode_frame(body).is_some()
}

impl Ledger {
    pub fn lock(&self) -> Result<()> {
        let rc = unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc != 0 {
            return Err(err("another inlet is holding the ledger"));
        }
        Ok(())
    }

    pub fn append_all(&mut self, recs: &[Record], sync: bool) -> Result<()> {
        for rec in recs {
            let body = encode_record(rec)?;
            if body.len() > u32::MAX as usize {
                return Err(err("record too large"));
            }
            let mut frame = Vec::with_capacity(8 + body.len());
            frame.extend_from_slice(&(body.len() as u32).to_le_bytes());
            frame.extend_from_slice(&crc32(&body).to_le_bytes());
            frame.extend_from_slice(&body);
            self.file.write_all(&frame)?;
            self.len += frame.len() as u64;
        }
        if sync && !recs.is_empty() {
            self.file.sync_all()?;
        }
        Ok(())
    }

    pub fn append(&mut self, rec: &Record, sync: bool) -> Result<()> {
        self.append_all(std::slice::from_ref(rec), sync)
    }
}

pub fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        crc ^= u32::from(b);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Budget, Record};

    fn task(id: &str) -> Record {
        Record::Task {
            id: id.into(),
            parent: None,
            worker: "pi".into(),
            tags: vec!["code".into()],
            goal: "g".into(),
            verifier: None,
            value: 10,
            budget: Budget {
                tokens: 5,
                seconds: 1,
                memory_mb: 32,
                pids: 4,
            },
            retry_of: None,
            recipe: None,
            ts: 1,
        }
    }

    #[test]
    fn crc_vector() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn torn_tail_is_truncated() {
        let dir = std::env::temp_dir().join(format!("inlet-led-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("log");
        {
            let mut led = open(&path).unwrap().ledger;
            led.append(&task("a"), true).unwrap();
            led.append(&task("b"), true).unwrap();
        }
        {
            use std::io::Write;
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[0x2A, 0x00, 0x00]).unwrap();
        }
        let opened = open(&path).unwrap();
        assert_eq!(opened.records.len(), 2);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), opened.ledger.len);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn damage_in_the_middle_refuses() {
        let dir = std::env::temp_dir().join(format!("inlet-dmg-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("log");
        {
            let mut led = open(&path).unwrap().ledger;
            led.append(&task("a"), true).unwrap();
            led.append(&task("b"), true).unwrap();
        }
        let mut buf = std::fs::read(&path).unwrap();
        // Flip a byte inside the first JSON body, past the 8-byte header.
        buf[10] ^= 0xff;
        std::fs::write(&path, &buf).unwrap();
        let msg = open(&path).err().expect("damage").to_string();
        assert!(msg.contains("damage"), "{msg}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
