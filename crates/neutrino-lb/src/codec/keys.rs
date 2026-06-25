//! Integer-key table ported from Dendrite `internal/lb/cbor_v1.go`
//! (`cborv1Keys`, MSC3079 v1). Maps well-known Matrix JSON object keys to small
//! integers for the CBOR wire form. `KEYS` is the single source of truth; the
//! two lookup maps are derived from it.

use std::collections::HashMap;
use std::sync::LazyLock;

/// Well-known Matrix key → integer code. Verbatim port of Dendrite's
/// `cborv1Keys`. Order is irrelevant; the integer values must stay stable so
/// both ends of a federating pair agree on the wire form.
pub(crate) const KEYS: &[(&str, i64)] = &[
    ("event_id", 1),
    ("type", 2),
    ("content", 3),
    ("state_key", 4),
    ("room_id", 5),
    ("sender", 6),
    ("user_id", 7),
    ("origin_server_ts", 8),
    ("unsigned", 9),
    ("prev_content", 10),
    ("state", 11),
    ("timeline", 12),
    ("events", 13),
    ("limited", 14),
    ("prev_batch", 15),
    ("transaction_id", 16),
    ("age", 17),
    ("redacted_because", 18),
    ("next_batch", 19),
    ("presence", 20),
    ("avatar_url", 21),
    ("account_data", 22),
    ("rooms", 23),
    ("join", 24),
    ("membership", 25),
    ("displayname", 26),
    ("body", 27),
    ("msgtype", 28),
    ("format", 29),
    ("formatted_body", 30),
    ("ephemeral", 31),
    ("invite_state", 32),
    ("leave", 33),
    ("third_party_invite", 34),
    ("is_direct", 35),
    ("hashes", 36),
    ("signatures", 37),
    ("depth", 38),
    ("prev_events", 39),
    ("prev_state", 40),
    ("auth_events", 41),
    ("origin", 42),
    ("creator", 43),
    ("join_rule", 44),
    ("history_visibility", 45),
    ("ban", 46),
    ("events_default", 47),
    ("kick", 48),
    ("redact", 49),
    ("state_default", 50),
    ("users", 51),
    ("users_default", 52),
    ("reason", 53),
    ("visibility", 54),
    ("room_alias_name", 55),
    ("name", 56),
    ("topic", 57),
    ("invite", 58),
    ("invite_3pid", 59),
    ("room_version", 60),
    ("creation_content", 61),
    ("initial_state", 62),
    ("preset", 63),
    ("servers", 64),
    ("identifier", 65),
    ("user", 66),
    ("medium", 67),
    ("address", 68),
    ("password", 69),
    ("token", 70),
    ("device_id", 71),
    ("initial_device_display_name", 72),
    ("access_token", 73),
    ("home_server", 74),
    ("well_known", 75),
    ("base_url", 76),
    ("device_lists", 77),
    ("to_device", 78),
    ("peek", 79),
    ("last_seen_ip", 80),
    ("display_name", 81),
    ("typing", 82),
    ("last_seen_ts", 83),
    ("algorithm", 84),
    ("sender_key", 85),
    ("session_id", 86),
    ("ciphertext", 87),
    ("one_time_keys", 88),
    ("timeout", 89),
    ("recent_rooms", 90),
    ("chunk", 91),
    ("m.fully_read", 92),
    ("device_keys", 93),
    ("failures", 94),
    ("device_display_name", 95),
    ("prev_sender", 96),
    ("replaces_state", 97),
    ("changed", 98),
    ("unstable_features", 99),
    ("versions", 100),
    ("devices", 101),
    ("errcode", 102),
    ("error", 103),
    ("room_alias", 104),
    ("edus", 105),
    ("pdus", 106),
    ("edu_type", 107),
    ("message_id", 108),
    ("messages", 109),
    ("retry_after_ms", 110),
    ("self_signing_key", 111),
    ("master_key", 112),
    ("stream_id", 113),
    ("keys", 114),
    ("algorithms", 115),
    ("usage", 116),
    ("prev_id", 117),
    ("m.read", 118),
    ("data", 119),
    ("event_ids", 120),
    ("thread_id", 121),
    ("ts", 122),
    ("join_authorised_via_users_server", 123),
    ("invite_room_state", 124),
    ("event", 125),
    ("m.room.create", 126),
    ("m.room.power_levels", 127),
    ("m.room.join_rules", 128),
    ("m.room.name", 129),
    ("m.room.server_acl", 130),
    ("auth_chain", 131),
    ("m.room.tombstone", 132),
    ("m.room.avatar", 133),
    ("m.room.canonical_alias", 134),
    ("m.room.encryption", 135),
    ("m.room.history_visibility", 136),
    ("notifications", 137),
];

static KEY_TO_INT: LazyLock<HashMap<&'static str, i64>> =
    LazyLock::new(|| KEYS.iter().copied().collect());

static INT_TO_KEY: LazyLock<HashMap<i64, &'static str>> =
    LazyLock::new(|| KEYS.iter().map(|&(k, v)| (v, k)).collect());

/// The integer code for a well-known Matrix key, if any.
pub(crate) fn key_to_int(key: &str) -> Option<i64> {
    KEY_TO_INT.get(key).copied()
}

/// The well-known Matrix key for an integer code, if any.
pub(crate) fn int_to_key(code: i64) -> Option<&'static str> {
    INT_TO_KEY.get(&code).copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    // No duplicate string keys or integer codes silently collapsing the table.
    // (Dendrite's NewCBORCodec rejects duplicate ints at construction.)
    #[test]
    fn table_has_no_duplicates() {
        assert_eq!(KEY_TO_INT.len(), KEYS.len(), "duplicate string key in KEYS");
        assert_eq!(
            INT_TO_KEY.len(),
            KEYS.len(),
            "duplicate integer code in KEYS"
        );
    }

    // Every entry resolves both directions.
    #[test]
    fn lookups_round_trip() {
        for &(k, v) in KEYS {
            assert_eq!(key_to_int(k), Some(v), "key_to_int({k})");
            assert_eq!(int_to_key(v), Some(k), "int_to_key({v})");
        }
    }

    // Spot-check the anchors of the ported table.
    #[test]
    fn known_anchors_present() {
        assert_eq!(key_to_int("event_id"), Some(1));
        assert_eq!(key_to_int("type"), Some(2));
        assert_eq!(key_to_int("notifications"), Some(137));
        assert_eq!(KEYS.len(), 137);
        assert_eq!(key_to_int("not_a_matrix_key"), None);
    }
}
