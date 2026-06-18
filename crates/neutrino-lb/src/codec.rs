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

    // The byte-identity assertions below rely on `serde_json` re-emitting object
    // keys in sorted order (its default `BTreeMap`-backed `Map`). The inputs are
    // pre-sorted and compact, so the round trip is byte-identical. This couples
    // to `serde_json` NOT having the `preserve_order` feature enabled; if that
    // ever changes, switch these to semantic `Value == Value` comparisons.
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
    fn roundtrip_preserves_number_types() {
        // Each value must survive json→cbor→json with its exact numeric type.
        // Compared at the `Value` level (which distinguishes the i64/u64/f64
        // arms of `Number`), so this actually pins the type, not just the
        // digits — `depth`/`origin_server_ts` must never coerce to float, and a
        // u64 above i64::MAX must not overflow or clamp.
        for original in [
            r#"{"n":9007199254740993}"#,     // 2^53+1: exact integer, not f64
            r#"{"n":18446744073709551615}"#, // u64::MAX: above i64::MAX
            r#"{"n":-9007199254740993}"#,    // large negative (i64 arm)
            r#"{"n":1.5}"#,                  // genuine float stays a float
            r#"{"n":0}"#,                    // zero is an integer, not 0.0
        ] {
            let want: serde_json::Value = serde_json::from_str(original).unwrap();
            let back = cbor_to_json(&json_to_cbor(original.as_bytes()).expect("json->cbor"))
                .expect("cbor->json");
            let got: serde_json::Value = serde_json::from_slice(&back).unwrap();
            assert_eq!(got, want, "number type/precision lost for {original}");
        }
    }

    #[test]
    fn roundtrip_preserves_representative_federation_bodies() {
        // The bodies the design spec enumerates, in canonical (sorted-key,
        // compact) form so the round trip is byte-identical. A send_join-style
        // PDU, an /invite v2 envelope, and a get_missing_events request.
        let send_join = r#"{"content":{"membership":"join"},"depth":5,"origin_server_ts":1700000000000,"prev_events":["$abc"],"room_id":"!r","sender":"@u:a.example","state_key":"@u:a.example","type":"m.room.member"}"#;
        let invite = r#"{"event":{"content":{"membership":"invite"},"sender":"@u:a.example","type":"m.room.member"},"invite_room_state":[],"room_version":"12"}"#;
        let get_missing_events = r#"{"earliest_events":["$a"],"latest_events":["$b"],"limit":10}"#;
        for body in [send_join, invite, get_missing_events] {
            assert_eq!(roundtrip(body), body);
        }
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
