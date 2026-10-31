use anyhow::{Result, bail};
use bytes::{Buf, BytesMut};

pub const MAX_MESSAGE_LEN: usize = 16 * 1024 * 1024;

#[allow(dead_code)]
pub const PG_PROTOCOL_V3: u32 = 196608;
pub const PG_CANCEL_CODE: u32 = 80877102;
pub const PG_SSL_CODE: u32 = 80877103;
pub const PG_GSSENC_CODE: u32 = 80877104;

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpecialStartup {
    SslRequest,
    GssEncRequest,
    CancelRequest { pid: u32, secret: u32 },
    RegularStartup,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum MessageDirection {
    ClientToServer,
    ServerToClient,
}

#[derive(Debug, Clone)]
pub struct PgMessage {
    pub tag: Option<u8>,
    pub payload: Vec<u8>,
}

impl PgMessage {
    #[allow(dead_code)]
    pub fn tag_char(&self) -> String {
        match self.tag {
            Some(t) => (t as char).to_string(),
            None => "Startup".to_string(),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        if let Some(tag) = self.tag {
            buf.push(tag);
        }
        let len = (self.payload.len() + 4) as u32;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&self.payload);
        buf
    }

    pub fn build_error_response(sqlstate: &str, message: &str) -> Self {
        let mut payload = Vec::new();

        payload.push(b'S');
        payload.extend_from_slice(b"ERROR\0");

        payload.push(b'V');
        payload.extend_from_slice(b"ERROR\0");

        payload.push(b'C');
        payload.extend_from_slice(sqlstate.as_bytes());
        payload.push(0);

        payload.push(b'M');
        payload.extend_from_slice(message.as_bytes());
        payload.push(0);

        payload.push(0);

        Self {
            tag: Some(b'E'),
            payload,
        }
    }

    pub fn build_ready_for_query(state: u8) -> Self {
        Self {
            tag: Some(b'Z'),
            payload: vec![state],
        }
    }

    #[allow(dead_code)]
    pub fn build_ready_for_query_error() -> Self {
        Self::build_ready_for_query(b'E')
    }
}

pub fn peek_startup_type(src: &BytesMut) -> Result<Option<SpecialStartup>> {
    if src.len() < 8 {
        return Ok(None);
    }
    let len = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
    if !(8..=MAX_MESSAGE_LEN).contains(&len) {
        bail!("Invalid startup packet length: {}", len);
    }
    if src.len() < len {
        return Ok(None);
    }

    let code = u32::from_be_bytes([src[4], src[5], src[6], src[7]]);
    match code {
        PG_SSL_CODE if len == 8 => Ok(Some(SpecialStartup::SslRequest)),
        PG_GSSENC_CODE if len == 8 => Ok(Some(SpecialStartup::GssEncRequest)),
        PG_CANCEL_CODE if len == 16 => {
            let pid = u32::from_be_bytes([src[8], src[9], src[10], src[11]]);
            let secret = u32::from_be_bytes([src[12], src[13], src[14], src[15]]);
            Ok(Some(SpecialStartup::CancelRequest { pid, secret }))
        }
        _ => Ok(Some(SpecialStartup::RegularStartup)),
    }
}

pub fn decode_startup_message(src: &mut BytesMut) -> Result<Option<PgMessage>> {
    if src.len() < 4 {
        return Ok(None);
    }
    let len = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
    if !(4..=MAX_MESSAGE_LEN).contains(&len) {
        bail!("Invalid startup packet length: {}", len);
    }
    if src.len() < len {
        return Ok(None);
    }
    src.advance(4);
    let payload = src.split_to(len - 4).to_vec();
    Ok(Some(PgMessage { tag: None, payload }))
}

pub fn decode_regular_message(src: &mut BytesMut) -> Result<Option<PgMessage>> {
    if src.len() < 5 {
        return Ok(None);
    }
    let tag = src[0];
    let len = u32::from_be_bytes([src[1], src[2], src[3], src[4]]) as usize;

    if !(4..=MAX_MESSAGE_LEN).contains(&len) {
        bail!(
            "Invalid Postgres packet length: {} (expected 4..={})",
            len,
            MAX_MESSAGE_LEN
        );
    }
    if src.len() < 1 + len {
        return Ok(None);
    }

    src.advance(5);
    let payload = src.split_to(len - 4).to_vec();
    Ok(Some(PgMessage {
        tag: Some(tag),
        payload,
    }))
}

pub fn extract_backend_key_data(msg: &PgMessage) -> Option<(u32, u32)> {
    if msg.tag == Some(b'K') && msg.payload.len() >= 8 {
        let pid = u32::from_be_bytes([
            msg.payload[0],
            msg.payload[1],
            msg.payload[2],
            msg.payload[3],
        ]);
        let secret = u32::from_be_bytes([
            msg.payload[4],
            msg.payload[5],
            msg.payload[6],
            msg.payload[7],
        ]);
        Some((pid, secret))
    } else {
        None
    }
}

pub fn parse_extended_parse(msg: &PgMessage) -> Option<(String, String)> {
    if msg.tag != Some(b'P') {
        return None;
    }
    let mut parts = msg.payload.split(|&b| b == 0);
    let stmt_name = String::from_utf8(parts.next()?.to_vec()).ok()?;
    let query_bytes = parts.next()?;
    let query = String::from_utf8(query_bytes.to_vec()).ok()?;
    Some((stmt_name, query))
}

pub fn parse_extended_bind(msg: &PgMessage) -> Option<(String, String)> {
    if msg.tag != Some(b'B') {
        return None;
    }
    let mut parts = msg.payload.split(|&b| b == 0);
    let portal_name = String::from_utf8(parts.next()?.to_vec()).ok()?;
    let stmt_name = String::from_utf8(parts.next()?.to_vec()).ok()?;
    Some((portal_name, stmt_name))
}

pub fn parse_extended_execute(msg: &PgMessage) -> Option<String> {
    if msg.tag != Some(b'E') {
        return None;
    }
    let mut parts = msg.payload.split(|&b| b == 0);
    let portal_name = String::from_utf8(parts.next()?.to_vec()).ok()?;
    Some(portal_name)
}

pub fn extract_simple_query(msg: &PgMessage) -> Option<String> {
    if msg.tag == Some(b'Q') {
        let trimmed = msg.payload.strip_suffix(&[0]).unwrap_or(&msg.payload);
        String::from_utf8(trimmed.to_vec()).ok()
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use proptest::prelude::*;

    #[test]
    fn test_decode_invalid_length_too_small() {
        let mut buf = BytesMut::from(&[b'Q', 0, 0, 0, 3, b'X'][..]);
        let res = decode_regular_message(&mut buf);
        assert!(res.is_err());
    }

    #[test]
    fn test_decode_invalid_length_too_large() {
        let mut buf = BytesMut::from(&[b'Q', 0xFF, 0xFF, 0xFF, 0xFF, b'X'][..]);
        let res = decode_regular_message(&mut buf);
        assert!(res.is_err());
    }

    #[test]
    fn test_decode_multiple_messages_in_one_buffer() {
        let msg1 = PgMessage {
            tag: Some(b'Q'),
            payload: b"SELECT 1;\0".to_vec(),
        };
        let msg2 = PgMessage {
            tag: Some(b'Q'),
            payload: b"SELECT 2;\0".to_vec(),
        };

        let mut buf = BytesMut::new();
        buf.extend_from_slice(&msg1.to_bytes());
        buf.extend_from_slice(&msg2.to_bytes());

        let decoded1 = decode_regular_message(&mut buf).unwrap().expect("msg1");
        assert_eq!(decoded1.tag, Some(b'Q'));
        assert_eq!(decoded1.payload, msg1.payload);

        let decoded2 = decode_regular_message(&mut buf).unwrap().expect("msg2");
        assert_eq!(decoded2.tag, Some(b'Q'));
        assert_eq!(decoded2.payload, msg2.payload);

        assert!(buf.is_empty());
    }

    #[test]
    fn test_decode_byte_by_byte_stream() {
        let original = PgMessage {
            tag: Some(b'Q'),
            payload: b"SELECT 'streaming test';\0".to_vec(),
        };
        let raw = original.to_bytes();

        let mut buf = BytesMut::new();
        let mut decoded = None;

        for byte in raw {
            buf.extend_from_slice(&[byte]);
            if let Some(msg) = decode_regular_message(&mut buf).unwrap() {
                decoded = Some(msg);
                break;
            }
        }

        let decoded = decoded.expect("message should be decoded once all bytes arrive");
        assert_eq!(decoded.tag, original.tag);
        assert_eq!(decoded.payload, original.payload);
        assert!(buf.is_empty());
    }

    proptest! {
        #[test]
        fn test_decode_random_chunk_slicing(
            payload in prop::collection::vec(any::<u8>(), 0..1024),
            tag in any::<u8>(),
            split_points in prop::collection::vec(1usize..1024, 1..10),
        ) {
            let original = PgMessage {
                tag: Some(tag),
                payload,
            };
            let raw = original.to_bytes();

            let mut buf = BytesMut::new();
            let mut cursor = 0;
            let mut decoded_msg = None;

            for split in split_points {
                if cursor >= raw.len() {
                    break;
                }
                let end = (cursor + split).min(raw.len());
                buf.extend_from_slice(&raw[cursor..end]);
                cursor = end;

                if let Some(msg) = decode_regular_message(&mut buf).unwrap() {
                    decoded_msg = Some(msg);
                    break;
                }
            }

            if cursor < raw.len() && decoded_msg.is_none() {
                buf.extend_from_slice(&raw[cursor..]);
                decoded_msg = decode_regular_message(&mut buf).unwrap();
            }

            let result = decoded_msg.expect("Valid packet must be completely decoded");
            prop_assert_eq!(result.tag, original.tag);
            prop_assert_eq!(result.payload, original.payload);
            prop_assert!(buf.is_empty());
        }
    }
}
