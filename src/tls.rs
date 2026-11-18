use anyhow::{bail, Context, Result};

const TLS_HANDSHAKE: u8 = 22;
const CLIENT_HELLO: u8 = 1;
const MAX_TLS_RECORD: usize = 18_432;
const MAX_CLIENT_HELLO: usize = 1 << 20;

/// Extract the host_name SNI from a TLS ClientHello carried by one or more
/// handshake records. The input starts at the first TLS record byte.
pub fn client_hello_sni(records: &[u8]) -> Result<Option<String>> {
    let mut offset = 0usize;
    let mut handshake = Vec::new();
    let mut expected_handshake_len = None;

    while offset < records.len() {
        if records.len() - offset < 5 {
            bail!("truncated TLS record header");
        }
        let content_type = records[offset];
        let version = u16::from_be_bytes([records[offset + 1], records[offset + 2]]);
        let record_len = u16::from_be_bytes([records[offset + 3], records[offset + 4]]) as usize;
        if content_type != TLS_HANDSHAKE {
            bail!("expected TLS handshake record, got content type {content_type}");
        }
        if !(0x0301..=0x0304).contains(&version) {
            bail!("unsupported TLS record version 0x{version:04x}");
        }
        if record_len > MAX_TLS_RECORD {
            bail!("TLS record exceeds maximum length");
        }
        offset += 5;
        let record_end = offset.checked_add(record_len).context("TLS record length overflow")?;
        if record_end > records.len() {
            bail!("truncated TLS record payload");
        }
        handshake.extend_from_slice(&records[offset..record_end]);
        offset = record_end;

        if handshake.len() >= 4 && expected_handshake_len.is_none() {
            if handshake[0] != CLIENT_HELLO {
                bail!("first TLS handshake message is not ClientHello");
            }
            let body_len = ((handshake[1] as usize) << 16)
                | ((handshake[2] as usize) << 8)
                | handshake[3] as usize;
            let total_len = body_len.checked_add(4).context("ClientHello length overflow")?;
            if total_len > MAX_CLIENT_HELLO {
                bail!("ClientHello exceeds maximum supported length");
            }
            expected_handshake_len = Some(total_len);
        }
        if let Some(total_len) = expected_handshake_len {
            if handshake.len() >= total_len {
                return parse_client_hello_body(&handshake[4..total_len]);
            }
        }
    }

    bail!("incomplete TLS ClientHello")
}

fn parse_client_hello_body(body: &[u8]) -> Result<Option<String>> {
    let mut cursor = 0usize;
    take(body, &mut cursor, 2).context("missing ClientHello legacy version")?;
    take(body, &mut cursor, 32).context("missing ClientHello random")?;

    let session_len = take(body, &mut cursor, 1)?[0] as usize;
    take(body, &mut cursor, session_len).context("truncated session ID")?;

    let cipher_len = read_u16(body, &mut cursor)? as usize;
    if cipher_len == 0 || cipher_len % 2 != 0 {
        bail!("invalid ClientHello cipher suite length");
    }
    take(body, &mut cursor, cipher_len).context("truncated cipher suites")?;

    let compression_len = take(body, &mut cursor, 1)?[0] as usize;
    if compression_len == 0 {
        bail!("empty ClientHello compression methods");
    }
    take(body, &mut cursor, compression_len).context("truncated compression methods")?;

    if cursor == body.len() {
        return Ok(None);
    }
    let extensions_len = read_u16(body, &mut cursor)? as usize;
    let extensions_end = cursor.checked_add(extensions_len).context("extensions length overflow")?;
    if extensions_end != body.len() {
        bail!("invalid ClientHello extensions length");
    }

    while cursor < extensions_end {
        let extension_type = read_u16(body, &mut cursor)?;
        let extension_len = read_u16(body, &mut cursor)? as usize;
        let extension = take(body, &mut cursor, extension_len)
            .context("truncated ClientHello extension")?;
        if extension_type == 0 {
            return parse_server_name_extension(extension).map(Some);
        }
    }
    Ok(None)
}

fn parse_server_name_extension(extension: &[u8]) -> Result<String> {
    let mut cursor = 0usize;
    let list_len = read_u16(extension, &mut cursor)? as usize;
    let list_end = cursor.checked_add(list_len).context("SNI list length overflow")?;
    if list_end != extension.len() {
        bail!("invalid SNI server-name list length");
    }
    while cursor < list_end {
        let name_type = take(extension, &mut cursor, 1)?[0];
        let name_len = read_u16(extension, &mut cursor)? as usize;
        let name = take(extension, &mut cursor, name_len).context("truncated SNI name")?;
        if name_type == 0 {
            let value = std::str::from_utf8(name).context("SNI hostname is not UTF-8")?;
            if value.is_empty() || value.bytes().any(|byte| byte.is_ascii_control()) {
                bail!("invalid SNI hostname");
            }
            return Ok(value.to_owned());
        }
    }
    bail!("SNI extension does not contain a host_name")
}

fn read_u16(bytes: &[u8], cursor: &mut usize) -> Result<u16> {
    let value = take(bytes, cursor, 2)?;
    Ok(u16::from_be_bytes([value[0], value[1]]))
}

fn take<'a>(bytes: &'a [u8], cursor: &mut usize, length: usize) -> Result<&'a [u8]> {
    let end = cursor.checked_add(length).context("TLS field length overflow")?;
    if end > bytes.len() {
        bail!("truncated TLS field");
    }
    let value = &bytes[*cursor..end];
    *cursor = end;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hello_records(host: &str, split_at: Option<usize>) -> Vec<u8> {
        let host = host.as_bytes();
        let mut sni = Vec::new();
        sni.extend_from_slice(&((host.len() + 3) as u16).to_be_bytes());
        sni.push(0);
        sni.extend_from_slice(&(host.len() as u16).to_be_bytes());
        sni.extend_from_slice(host);

        let mut body = vec![0x03, 0x03];
        body.extend_from_slice(&[0x42; 32]);
        body.push(0); // session ID
        body.extend_from_slice(&2u16.to_be_bytes());
        body.extend_from_slice(&[0x13, 0x01]);
        body.extend_from_slice(&1u8.to_be_bytes());
        body.push(0); // null compression
        body.extend_from_slice(&((sni.len() + 4) as u16).to_be_bytes());
        body.extend_from_slice(&0u16.to_be_bytes());
        body.extend_from_slice(&(sni.len() as u16).to_be_bytes());
        body.extend_from_slice(&sni);

        let mut handshake = vec![CLIENT_HELLO, 0, 0, 0];
        let length = body.len();
        handshake[1] = ((length >> 16) & 0xff) as u8;
        handshake[2] = ((length >> 8) & 0xff) as u8;
        handshake[3] = (length & 0xff) as u8;
        handshake.extend_from_slice(&body);

        let mut records = Vec::new();
        let chunks: Vec<&[u8]> = match split_at {
            Some(index) => vec![&handshake[..index], &handshake[index..]],
            None => vec![&handshake],
        };
        for chunk in chunks {
            records.extend_from_slice(&[TLS_HANDSHAKE, 0x03, 0x03]);
            records.extend_from_slice(&(chunk.len() as u16).to_be_bytes());
            records.extend_from_slice(chunk);
        }
        records
    }

    #[test]
    fn extracts_server_name_from_client_hello() {
        let records = hello_records("api.github.com", None);
        assert_eq!(client_hello_sni(&records).unwrap().as_deref(), Some("api.github.com"));
    }

    #[test]
    fn handles_client_hello_fragmented_across_records() {
        let records = hello_records("github.com", Some(12));
        assert_eq!(client_hello_sni(&records).unwrap().as_deref(), Some("github.com"));
    }

    #[test]
    fn rejects_truncated_tls_records() {
        assert!(client_hello_sni(&[22, 3, 3, 0, 8, 1, 0]).is_err());
    }

    #[test]
    fn rejects_non_client_hello_handshake() {
        assert!(client_hello_sni(&[22, 3, 3, 0, 4, 2, 0, 0, 0]).is_err());
    }
}
