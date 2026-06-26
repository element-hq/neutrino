//! Integer-key CBOR body transcoding between JSON and CBOR (port of Dendrite
//! `internal/lb`). Well-known Matrix object keys map to small integers and
//! event-id strings pack into raw 32 bytes; everything else passes through.
//! Empty input maps to empty output so a bodyless GET stays bodyless.

mod keys;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ciborium::value::Value as CborValue;

use keys::{int_to_key, key_to_int};

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("invalid JSON body: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid CBOR body: {0}")]
    CborDecode(String),
    #[error("CBOR encode failed: {0}")]
    CborEncode(String),
}

/// JSON bytes → integer-key CBOR bytes. Empty stays empty.
pub fn json_to_cbor(json: &[u8]) -> Result<Vec<u8>, CodecError> {
    if json.is_empty() {
        return Ok(Vec::new());
    }
    let value: serde_json::Value = serde_json::from_slice(json)?;
    let cbor_value = json_to_cbor_value(value)?;
    let mut out = Vec::new();
    ciborium::into_writer(&cbor_value, &mut out)
        .map_err(|e| CodecError::CborEncode(e.to_string()))?;
    Ok(out)
}

/// Integer-key CBOR bytes → JSON bytes. Empty stays empty.
pub fn cbor_to_json(cbor: &[u8]) -> Result<Vec<u8>, CodecError> {
    if cbor.is_empty() {
        return Ok(Vec::new());
    }
    let cbor_value: CborValue =
        ciborium::from_reader(cbor).map_err(|e| CodecError::CborDecode(e.to_string()))?;
    let json_value = cbor_value_to_json(cbor_value)?;
    Ok(serde_json::to_vec(&json_value)?)
}

/// Recursively convert a JSON value tree into its CBOR form, remapping known
/// object keys to integers and packing event-id strings to raw bytes.
fn json_to_cbor_value(value: serde_json::Value) -> Result<CborValue, CodecError> {
    use serde_json::Value as J;
    Ok(match value {
        J::Null => CborValue::Null,
        J::Bool(b) => CborValue::Bool(b),
        J::Number(n) => {
            if let Some(u) = n.as_u64() {
                CborValue::Integer(u.into())
            } else if let Some(i) = n.as_i64() {
                CborValue::Integer(i.into())
            } else if let Some(f) = n.as_f64() {
                CborValue::Float(f)
            } else {
                return Err(CodecError::CborEncode(format!(
                    "unrepresentable number: {n}"
                )));
            }
        }
        J::String(s) => string_to_cbor(s),
        J::Array(arr) => {
            let mut out = Vec::with_capacity(arr.len());
            for element in arr {
                out.push(json_to_cbor_value(element)?);
            }
            CborValue::Array(out)
        }
        J::Object(map) => {
            let mut out = Vec::with_capacity(map.len());
            for (k, v) in map {
                let key = match key_to_int(&k) {
                    Some(code) => CborValue::Integer(code.into()),
                    None => CborValue::Text(k),
                };
                out.push((key, json_to_cbor_value(v)?));
            }
            CborValue::Map(out)
        }
    })
}

/// Pack an event-id string into 32 raw bytes; anything else stays text. A
/// `$`-prefixed string that fails to base64-decode to exactly 32 bytes falls
/// back to text (mirrors Dendrite — never lose data on a false-positive match).
fn string_to_cbor(s: String) -> CborValue {
    if is_event_id(&s)
        && let Ok(bytes) = URL_SAFE_NO_PAD.decode(&s[1..])
        && bytes.len() == 32
    {
        return CborValue::Bytes(bytes);
    }
    CborValue::Text(s)
}

/// Recursively convert a CBOR value tree into JSON, restoring integer keys to
/// their strings and unpacking 32-byte byte strings to event ids.
fn cbor_value_to_json(value: CborValue) -> Result<serde_json::Value, CodecError> {
    use serde_json::Value as J;
    Ok(match value {
        CborValue::Null => J::Null,
        CborValue::Bool(b) => J::Bool(b),
        CborValue::Integer(i) => {
            let n = i128::from(i);
            if let Ok(v) = i64::try_from(n) {
                J::Number(v.into())
            } else if let Ok(v) = u64::try_from(n) {
                J::Number(v.into())
            } else {
                return Err(CodecError::CborDecode(format!(
                    "integer out of JSON range: {n}"
                )));
            }
        }
        CborValue::Float(f) => match serde_json::Number::from_f64(f) {
            Some(num) => J::Number(num),
            None => return Err(CodecError::CborDecode(format!("non-finite float: {f}"))),
        },
        CborValue::Bytes(b) => {
            if b.len() != 32 {
                return Err(CodecError::CborDecode(format!(
                    "expected 32-byte event-id bytes, got {}",
                    b.len()
                )));
            }
            J::String(format!("${}", URL_SAFE_NO_PAD.encode(&b)))
        }
        CborValue::Text(s) => J::String(s),
        CborValue::Array(arr) => {
            let mut out = Vec::with_capacity(arr.len());
            for element in arr {
                out.push(cbor_value_to_json(element)?);
            }
            J::Array(out)
        }
        CborValue::Map(entries) => {
            let mut map = serde_json::Map::with_capacity(entries.len());
            for (k, v) in entries {
                map.insert(cbor_key_to_string(k)?, cbor_value_to_json(v)?);
            }
            J::Object(map)
        }
        other => {
            return Err(CodecError::CborDecode(format!(
                "unsupported CBOR value: {other:?}"
            )));
        }
    })
}

/// Resolve a CBOR map key to its JSON string. Integer keys map back through the
/// table; an unknown integer becomes its decimal string (Dendrite parity).
fn cbor_key_to_string(key: CborValue) -> Result<String, CodecError> {
    match key {
        CborValue::Integer(i) => {
            let code = i128::from(i);
            match i64::try_from(code).ok().and_then(int_to_key) {
                Some(s) => Ok(s.to_owned()),
                None => Ok(code.to_string()),
            }
        }
        CborValue::Text(s) => Ok(s),
        // Our encoder only ever emits Integer or Text keys. Anything else is
        // corruption: error rather than silently drop it (Dendrite drops such
        // keys; we prefer surfacing the fault over losing data).
        other => Err(CodecError::CborDecode(format!(
            "unsupported CBOR map key: {other:?}"
        ))),
    }
}

/// True if `s` is a v3+ Matrix event ID: `$` followed by exactly 43 url-safe
/// base64 characters (the unpadded base64url of a 32-byte reference hash).
/// Equivalent to Dendrite's `^\$[A-Za-z0-9_-]{43}$`, no regex needed.
fn is_event_id(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 44
        && b[0] == b'$'
        && b[1..]
            .iter()
            .all(|c| c.is_ascii_alphanumeric() || *c == b'-' || *c == b'_')
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
    fn is_event_id_matches_only_dollar_plus_43_base64url() {
        // 43 base64url chars after '$' (a real v12 event-id shape).
        let ok = "$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        assert_eq!(ok.len(), 44);
        assert!(is_event_id(ok));

        // A positive case spanning the whole url-safe alphabet (a-z, 0-9, -, _,
        // A-E = 43 chars), so acceptance isn't vacuously true for just 'A'.
        let mixed = "$abcdefghijklmnopqrstuvwxyz0123456789-_ABCDE";
        assert_eq!(mixed.len(), 44);
        assert!(is_event_id(mixed));

        assert!(!is_event_id("$abc")); // too short
        // One char too long ('$' + 44 chars): the 43-char window is the point.
        assert!(!is_event_id(
            "$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        ));
        assert!(!is_event_id("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")); // no leading $
        assert!(!is_event_id("$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA+")); // '+' not in url-safe alphabet
        assert!(!is_event_id("")); // empty
        // Right length but a disallowed char ('!') in the body.
        assert!(!is_event_id("$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA!"));
    }

    use ciborium::value::Value as RawCbor;

    // Decode raw CBOR bytes into a ciborium Value tree for structural inspection
    // (so tests can assert the *wire* form, not just the JSON round trip).
    fn raw_cbor(bytes: &[u8]) -> RawCbor {
        ciborium::from_reader(bytes).expect("valid cbor")
    }

    #[test]
    fn known_keys_become_integer_keys_on_the_wire() {
        let cbor = json_to_cbor(br#"{"type":"m.room.message","content":{"body":"hi"}}"#).unwrap();
        let RawCbor::Map(entries) = raw_cbor(&cbor) else {
            panic!("expected a CBOR map");
        };
        // "type" -> 2, "content" -> 3 (per the table).
        let has_int_key = |code: i128| {
            entries
                .iter()
                .any(|(k, _)| matches!(k, RawCbor::Integer(i) if i128::from(*i) == code))
        };
        assert!(has_int_key(2), "`type` did not map to integer 2");
        assert!(has_int_key(3), "`content` did not map to integer 3");
    }

    #[test]
    fn unknown_keys_stay_text_and_round_trip() {
        let json = r#"{"not_a_matrix_key":"v"}"#;
        let cbor = json_to_cbor(json.as_bytes()).unwrap();
        let RawCbor::Map(entries) = raw_cbor(&cbor) else {
            panic!("expected map");
        };
        assert!(
            entries
                .iter()
                .any(|(k, _)| matches!(k, RawCbor::Text(t) if t == "not_a_matrix_key")),
            "unknown key was not kept as text"
        );
        assert_eq!(roundtrip(json), json);
    }

    #[test]
    fn event_id_packs_to_32_bytes_and_round_trips() {
        // 32 zero bytes -> base64url -> 43 'A's.
        let event_id = format!("${}", "A".repeat(43));
        let json = format!(r#"{{"prev_events":["{event_id}"]}}"#);
        let cbor = json_to_cbor(json.as_bytes()).unwrap();
        // Find the array under integer key 39 ("prev_events") and assert its
        // first element is 32 raw bytes.
        let RawCbor::Map(entries) = raw_cbor(&cbor) else {
            panic!("expected map");
        };
        let (_, val) = entries
            .iter()
            .find(|(k, _)| matches!(k, RawCbor::Integer(i) if i128::from(*i) == 39))
            .expect("prev_events key");
        let RawCbor::Array(arr) = val else {
            panic!("expected array");
        };
        assert!(
            matches!(&arr[0], RawCbor::Bytes(b) if b.len() == 32),
            "event id was not packed to 32 bytes: {:?}",
            arr[0]
        );
        assert_eq!(roundtrip(&json), json);
    }

    #[test]
    fn msc4242_prev_state_events_maps_and_packs_event_ids() {
        // `prev_state_events` (code 138) is on every event under MSC4242; its
        // values are event IDs, which must auto-pack to 32 bytes like any other
        // event-id-shaped string. Pins both the new key code and the free win.
        let event_id = format!("${}", "A".repeat(43));
        let json = format!(r#"{{"prev_state_events":["{event_id}"]}}"#);
        let cbor = json_to_cbor(json.as_bytes()).unwrap();
        let RawCbor::Map(entries) = raw_cbor(&cbor) else {
            panic!("expected map");
        };
        let (_, val) = entries
            .iter()
            .find(|(k, _)| matches!(k, RawCbor::Integer(i) if i128::from(*i) == 138))
            .expect("prev_state_events should map to integer key 138");
        let RawCbor::Array(arr) = val else {
            panic!("expected array");
        };
        assert!(
            matches!(&arr[0], RawCbor::Bytes(b) if b.len() == 32),
            "event id in prev_state_events was not packed to 32 bytes: {:?}",
            arr[0]
        );
        assert_eq!(roundtrip(&json), json);
    }

    #[test]
    fn non_event_id_dollar_string_stays_text() {
        let json = r#"{"body":"$abc"}"#;
        let cbor = json_to_cbor(json.as_bytes()).unwrap();
        let RawCbor::Map(entries) = raw_cbor(&cbor) else {
            panic!("expected map");
        };
        let (_, val) = &entries[0];
        assert!(matches!(val, RawCbor::Text(t) if t == "$abc"));
        assert_eq!(roundtrip(json), json);
    }

    #[test]
    fn invalid_base64_event_id_shape_falls_back_to_text() {
        // 44 chars, '$' + 43 url-safe-alphabet chars, so is_event_id() is true,
        // but the body is not canonical base64 (trailing bits set), so decode
        // fails and it must round-trip as text rather than corrupt or error.
        let json = format!(r#"{{"x":"${}"}}"#, "_".repeat(43));
        assert_eq!(roundtrip(&json), json);
    }

    #[test]
    fn unknown_integer_key_decodes_to_decimal_string() {
        // Build CBOR by hand: {9999: "v"} where 9999 is not in the table.
        let value = RawCbor::Map(vec![(
            RawCbor::Integer(9999i64.into()),
            RawCbor::Text("v".to_owned()),
        )]);
        let mut cbor = Vec::new();
        ciborium::into_writer(&value, &mut cbor).unwrap();
        let json = cbor_to_json(&cbor).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(v, serde_json::json!({"9999": "v"}));
    }

    #[test]
    fn non_int_non_text_map_key_errors_without_panic() {
        // A CBOR map keyed by a bool — a key type our encoder never emits.
        // Decode must surface it as an error, not panic or silently drop.
        let value = RawCbor::Map(vec![(RawCbor::Bool(true), RawCbor::Text("v".to_owned()))]);
        let mut cbor = Vec::new();
        ciborium::into_writer(&value, &mut cbor).unwrap();
        assert!(matches!(
            cbor_to_json(&cbor),
            Err(CodecError::CborDecode(_))
        ));
    }

    #[test]
    fn non_32_byte_cbor_bytes_errors_without_panic() {
        let value = RawCbor::Bytes(vec![1, 2, 3]); // wrong length
        let mut cbor = Vec::new();
        ciborium::into_writer(&value, &mut cbor).unwrap();
        assert!(matches!(
            cbor_to_json(&cbor),
            Err(CodecError::CborDecode(_))
        ));
    }

    #[test]
    fn top_level_array_and_primitive_round_trip() {
        assert_eq!(roundtrip("[1,2,3]"), "[1,2,3]");
        assert_eq!(roundtrip(r#""hello""#), r#""hello""#);
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
