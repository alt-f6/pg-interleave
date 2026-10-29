use anyhow::{Result, bail};
use bytes::{Buf, BytesMut};

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

    /// Constructs an ErrorResponse ('E') binary packet per PostgreSQL v3.0 protocol
    pub fn build_error_response(sqlstate: &str, message: &str) -> Self {
        let mut payload = Vec::new();

        // Field 'S' (Severity): ERROR
        payload.push(b'S');
        payload.extend_from_slice(b"ERROR\0");

        // Field 'V' (Severity non-localized): ERROR
        payload.push(b'V');
        payload.extend_from_slice(b"ERROR\0");

        // Field 'C' (SQLSTATE Code): e.g. 40P01
        payload.push(b'C');
        payload.extend_from_slice(sqlstate.as_bytes());
        payload.push(0);

        // Field 'M' (Message)
        payload.push(b'M');
        payload.extend_from_slice(message.as_bytes());
        payload.push(0);

        // Terminating null byte for fields list
        payload.push(0);

        Self {
            tag: Some(b'E'),
            payload,
        }
    }

    /// Constructs a ReadyForQuery ('Z') packet in transaction error state ('E')
    pub fn build_ready_for_query_error() -> Self {
        Self {
            tag: Some(b'Z'),
            payload: vec![b'E'], // 'I' - idle, 'T' - in transaction, 'E' - in failed transaction
        }
    }
}

pub fn decode_startup_message(src: &mut BytesMut) -> Result<Option<PgMessage>> {
    if src.len() < 4 {
        return Ok(None);
    }
    let len = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
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

    if len < 4 {
        bail!("Invalid Postgres packet length: {}", len);
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

/// Extracts statement name and query string from 'P' (Parse) packet
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

/// Extracts SQL text from simple query packet 'Q'
pub fn extract_simple_query(msg: &PgMessage) -> Option<String> {
    if msg.tag == Some(b'Q') {
        let trimmed = msg.payload.strip_suffix(&[0]).unwrap_or(&msg.payload);
        String::from_utf8(trimmed.to_vec()).ok()
    } else {
        None
    }
}
