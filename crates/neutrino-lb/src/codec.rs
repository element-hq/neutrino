//! Opaque body transcoding between JSON and CBOR. v1 carries the full
//! `serde_json::Value` (no integer-key remapping — that is a deferred
//! follow-up paired with CoAP). Empty input maps to empty output so a
//! bodyless GET stays bodyless.

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("invalid JSON body: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid CBOR body: {0}")]
    CborDecode(String),
    #[error("CBOR encode failed: {0}")]
    CborEncode(String),
}

/// JSON bytes → CBOR bytes. Empty stays empty.
pub fn json_to_cbor(json: &[u8]) -> Result<Vec<u8>, CodecError> {
    if json.is_empty() {
        return Ok(Vec::new());
    }
    let value: serde_json::Value = serde_json::from_slice(json)?;
    let mut out = Vec::new();
    ciborium::into_writer(&value, &mut out).map_err(|e| CodecError::CborEncode(e.to_string()))?;
    Ok(out)
}

/// CBOR bytes → JSON bytes. Empty stays empty.
pub fn cbor_to_json(cbor: &[u8]) -> Result<Vec<u8>, CodecError> {
    if cbor.is_empty() {
        return Ok(Vec::new());
    }
    let value: serde_json::Value =
        ciborium::from_reader(cbor).map_err(|e| CodecError::CborDecode(e.to_string()))?;
    Ok(serde_json::to_vec(&value)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(json: &str) -> String {
        let cbor = json_to_cbor(json.as_bytes()).expect("json->cbor");
        let back = cbor_to_json(&cbor).expect("cbor->json");
        String::from_utf8(back).expect("utf8")
    }

    #[test]
    fn roundtrip_preserves_content() {
        // A representative /send transaction body.
        let canonical = r#"{"edus":[],"origin":"a.example","origin_server_ts":1700000000000,"pdus":[{"content":{"body":"hi"},"type":"m.room.message"}]}"#;
        assert_eq!(roundtrip(canonical), canonical);
    }

    #[test]
    fn roundtrip_preserves_large_integers() {
        // depth / origin_server_ts must not be coerced to float.
        let canonical = r#"{"depth":9007199254740993,"ts":1700000000000}"#;
        assert_eq!(roundtrip(canonical), canonical);
    }

    #[test]
    fn empty_body_roundtrips_to_empty() {
        assert_eq!(json_to_cbor(b"").unwrap(), Vec::<u8>::new());
        assert_eq!(cbor_to_json(b"").unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn invalid_json_errors() {
        assert!(matches!(
            json_to_cbor(b"{not json"),
            Err(CodecError::Json(_))
        ));
    }

    #[test]
    fn invalid_cbor_errors() {
        // 0xff is a CBOR "break" with no enclosing indefinite item.
        assert!(matches!(
            cbor_to_json(&[0xff]),
            Err(CodecError::CborDecode(_))
        ));
    }
}
