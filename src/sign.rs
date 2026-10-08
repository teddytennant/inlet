//! Passphrase-wrapped ed25519. The passphrase stays on the tty.

use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::num::NonZeroU32;
use std::os::fd::AsRawFd;

use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, CHACHA20_POLY1305};
use ring::pbkdf2;
use ring::rand::{SecureRandom, SystemRandom};
use ring::signature::{Ed25519KeyPair, KeyPair, UnparsedPublicKey, ED25519};

use crate::error::{err, Result};

const ITERS: u32 = 100_000;
const AAD: &[u8] = b"inlet-policy-key";

pub fn generate(passphrase: &str) -> Result<(Vec<u8>, Vec<u8>)> {
    check_pass(passphrase)?;
    let rng = SystemRandom::new();
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).map_err(|_| err("keygen"))?;
    let pair = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).map_err(|_| err("keygen"))?;
    let public = pair.public_key().as_ref().to_vec();
    let wrapped = seal(passphrase, pkcs8.as_ref())?;
    Ok((public, wrapped))
}

pub fn sign_with(wrapped: &[u8], passphrase: &str, msg: &[u8]) -> Result<Vec<u8>> {
    check_pass(passphrase)?;
    let pkcs8 = open(passphrase, wrapped)?;
    let pair = Ed25519KeyPair::from_pkcs8(&pkcs8).map_err(|_| err("bad passphrase"))?;
    Ok(pair.sign(msg).as_ref().to_vec())
}

pub fn verify(public: &[u8], msg: &[u8], sig: &[u8]) -> Result<()> {
    let key = UnparsedPublicKey::new(&ED25519, public);
    key.verify(msg, sig).map_err(|_| err("bad signature"))
}

pub fn read_passphrase(prompt: &str) -> Result<String> {
    let mut tty = OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/tty")
        .map_err(|_| err("passphrase is read from /dev/tty"))?;
    let fd = tty.as_raw_fd();
    let mut saved: libc::termios = unsafe { std::mem::zeroed() };
    let echoed = unsafe { libc::tcgetattr(fd, &mut saved) } == 0;
    if echoed {
        let mut hidden = saved;
        hidden.c_lflag &= !libc::ECHO;
        unsafe {
            libc::tcsetattr(fd, libc::TCSANOW, &hidden);
        }
    }
    write!(tty, "{prompt}")?;
    tty.flush()?;
    let mut line = String::new();
    let mut reader = BufReader::new(tty.try_clone()?);
    reader.read_line(&mut line)?;
    if echoed {
        unsafe {
            libc::tcsetattr(fd, libc::TCSANOW, &saved);
        }
        let _ = writeln!(tty);
    }
    let pass = line.trim().to_string();
    check_pass(&pass)?;
    Ok(pass)
}

fn check_pass(passphrase: &str) -> Result<()> {
    if passphrase.len() < 4 {
        Err(err("passphrase too short"))
    } else {
        Ok(())
    }
}

fn seal(passphrase: &str, plaintext: &[u8]) -> Result<Vec<u8>> {
    let rng = SystemRandom::new();
    let mut salt = [0u8; 16];
    let mut nonce_bytes = [0u8; 12];
    rng.fill(&mut salt).map_err(|_| err("rng"))?;
    rng.fill(&mut nonce_bytes).map_err(|_| err("rng"))?;
    let key = derive(passphrase, &salt)?;
    let unbound = UnboundKey::new(&CHACHA20_POLY1305, &key).map_err(|_| err("seal"))?;
    let nonce = Nonce::try_assume_unique_for_key(&nonce_bytes).map_err(|_| err("seal"))?;
    let mut body = plaintext.to_vec();
    LessSafeKey::new(unbound)
        .seal_in_place_append_tag(nonce, Aad::from(AAD), &mut body)
        .map_err(|_| err("seal"))?;
    let mut out = Vec::with_capacity(28 + body.len());
    out.extend_from_slice(&salt);
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&body);
    Ok(out)
}

fn open(passphrase: &str, wrapped: &[u8]) -> Result<Vec<u8>> {
    if wrapped.len() < 28 + 16 {
        return Err(err("bad key"));
    }
    let salt = &wrapped[..16];
    let mut nonce_bytes = [0u8; 12];
    nonce_bytes.copy_from_slice(&wrapped[16..28]);
    let key = derive(passphrase, salt)?;
    let unbound = UnboundKey::new(&CHACHA20_POLY1305, &key).map_err(|_| err("bad passphrase"))?;
    let nonce = Nonce::assume_unique_for_key(nonce_bytes);
    let mut body = wrapped[28..].to_vec();
    let plain = LessSafeKey::new(unbound)
        .open_in_place(nonce, Aad::from(AAD), &mut body)
        .map_err(|_| err("bad passphrase"))?;
    Ok(plain.to_vec())
}

fn derive(passphrase: &str, salt: &[u8]) -> Result<[u8; 32]> {
    let mut key = [0u8; 32];
    let iters = NonZeroU32::new(ITERS).ok_or_else(|| err("iters"))?;
    pbkdf2::derive(
        pbkdf2::PBKDF2_HMAC_SHA256,
        iters,
        salt,
        passphrase.as_bytes(),
        &mut key,
    );
    Ok(key)
}

pub fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0xf) as usize] as char);
    }
    out
}

pub fn hex_decode(text: &str) -> Result<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return Err(err("bad signature"));
    }
    let mut out = Vec::with_capacity(text.len() / 2);
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let hi = hex_val(bytes[i])?;
        let lo = hex_val(bytes[i + 1])?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Ok(out)
}

fn hex_val(b: u8) -> Result<u8> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(err("bad signature")),
    }
}

pub fn tags_in(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'#' {
            let start = i + 1;
            let mut j = start;
            while j < bytes.len()
                && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_' || bytes[j] == b'-')
            {
                j += 1;
            }
            if j > start {
                let tag = text[start..j].to_string();
                if !out.iter().any(|t| t == &tag) {
                    out.push(tag);
                }
            }
            i = j;
        } else {
            i += 1;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrap_sign_and_reject_a_bad_passphrase() {
        let (public, wrapped) = generate("correct horse").unwrap();
        assert!(!wrapped.windows(8).any(|w| w == b"correct"));
        let sig = sign_with(&wrapped, "correct horse", b"policy").unwrap();
        verify(&public, b"policy", &sig).unwrap();
        assert!(sign_with(&wrapped, "wrong horse", b"policy").is_err());
        assert!(verify(&public, b"other", &sig).is_err());
        assert_eq!(tags_in("stay out #code and #math"), vec!["code", "math"]);
        assert!(tags_in("no tag").is_empty());
    }
}
