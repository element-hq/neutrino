//! Phase 2: auth events selection.
//!
//! Under MSC4242, `auth_events` is no longer carried on the wire; the server
//! calculates it from state-before-event using the algorithm in v1.18 §
//! Server-Server API § auth-events-selection.
//!
//! v12 modifies the algorithm by **excluding** the `m.room.create` event
//! from any calculated auth_events. The room is identified by its `room_id`
//! (event ID of the create event with `!` instead of `$`), not by an entry in
//! auth_events.
//!
//! The output of this module is consumed by Phase 4 state resolution
//! (auth chain difference, reverse-topological power ordering, and mainline
//! ordering all walk the auth chain — under MSC4242 that chain is built
//! from these calculated sets, not from a wire field).

use ruma::OwnedEventId;
use serde_json::Value;

use crate::{Event, StateMap};

/// The `(type, state_key)` pairs that the spec asks for as `auth_events` for
/// this event, in insertion order, deduplicated.
///
/// Pure with respect to room state — it inspects the event itself and that's
/// all. Useful for callers that want the *keys* without performing the state
/// lookup (e.g. tests, or storage layers that index by them).
pub fn auth_event_keys(event: &Event) -> Vec<(String, String)> {
    // m.room.create events authorise themselves and have no auth_events.
    if event.event_type == "m.room.create" {
        return Vec::new();
    }

    let mut keys: Vec<(String, String)> = Vec::new();

    // v12 specifically: do NOT include m.room.create.

    // Current m.room.power_levels event.
    add_unique(&mut keys, "m.room.power_levels", String::new());

    // Sender's current m.room.member event.
    add_unique(&mut keys, "m.room.member", event.sender.as_str().to_owned());

    // Membership-event-specific additions.
    if event.event_type == "m.room.member" {
        // Target user's current m.room.member event.
        if let Some(sk) = &event.state_key {
            add_unique(&mut keys, "m.room.member", sk.clone());
        }

        // Parse content for membership-specific lookups. If content is
        // malformed at this point parse_event would have rejected it; the
        // defensive fallback returns no extra entries.
        let content: Value = serde_json::from_str(event.content.get()).unwrap_or(Value::Null);
        let membership = content.get("membership").and_then(Value::as_str);

        // Current m.room.join_rules event when the new membership is one
        // that's gated by join rules.
        if matches!(membership, Some("join") | Some("invite") | Some("knock")) {
            add_unique(&mut keys, "m.room.join_rules", String::new());
        }

        // Third-party invite for `invite` memberships that reference one.
        if membership == Some("invite")
            && let Some(token) = content
                .get("third_party_invite")
                .and_then(|v| v.get("signed"))
                .and_then(|v| v.get("token"))
                .and_then(Value::as_str)
        {
            add_unique(&mut keys, "m.room.third_party_invite", token.to_owned());
        }

        // join_authorised_via_users_server (restricted-join flow): include
        // the authoriser's member event.
        if membership == Some("join")
            && let Some(authoriser) = content
                .get("join_authorised_via_users_server")
                .and_then(Value::as_str)
        {
            add_unique(&mut keys, "m.room.member", authoriser.to_owned());
        }
    }

    keys
}

/// Calculate the `auth_events` for `event` from `state` (state-before-event).
///
/// MSC4242: `auth_events` is no longer on the wire — servers calculate it for
/// every incoming event. This is the authoritative computation; Phase 4 state
/// resolution walks the auth chain by following these calculated sets.
///
/// Entries the spec asks for but that are absent from state are silently
/// dropped (matches the "if present" wording throughout the selection
/// algorithm).
pub fn calculate_auth_events(event: &Event, state: &StateMap<OwnedEventId>) -> Vec<OwnedEventId> {
    auth_event_keys(event)
        .into_iter()
        .filter_map(|key| state.get(&key).cloned())
        .collect()
}

fn add_unique(keys: &mut Vec<(String, String)>, event_type: &str, state_key: String) {
    let key = (event_type.to_string(), state_key);
    if !keys.contains(&key) {
        keys.push(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_id::EventBuilder;
    use neutrino_common::ROOM_VERSION_ID;
    use ruma::room_id;
    use serde_json::{Value, json};
    use std::collections::{HashMap, HashSet};
    use std::sync::atomic::{AtomicU64, Ordering};

    fn eid(s: &str) -> OwnedEventId {
        s.parse().expect("test event id")
    }

    /// Monotonically-increasing origin_server_ts for the fixtures in this
    /// module — events with otherwise-identical fields hash to the same
    /// event_id under v11 redaction (which strips `body`), so we need a
    /// per-fixture ts to keep them distinct.
    fn next_ts() -> u64 {
        static TS: AtomicU64 = AtomicU64::new(1_700_000_000_000);
        TS.fetch_add(1, Ordering::Relaxed)
    }

    /// Build a default-shape `m.room.message` event with a chosen sender —
    /// the auth_event tests only consult `event_type` and `sender` here.
    fn message_from(sender: &str) -> Event {
        EventBuilder::new(sender.parse().expect("sender"), "m.room.message".to_owned())
            .room_id(room_id!("!room:example.org").to_owned())
            .content(json!({ "msgtype": "m.text", "body": "hi" }))
            .origin_server_ts(next_ts())
            .build()
            .expect("valid message")
    }

    fn member(sender: &str, target: &str, content: Value) -> Event {
        EventBuilder::new(sender.parse().expect("sender"), "m.room.member".to_owned())
            .room_id(room_id!("!room:example.org").to_owned())
            .state_key(target.to_owned())
            .content(content)
            .origin_server_ts(next_ts())
            .build()
            .expect("valid member")
    }

    /// Build a state map from `(type, state_key, event_id)` triples.
    fn state(entries: &[(&str, &str, &str)]) -> StateMap<OwnedEventId> {
        let mut map: HashMap<(String, String), OwnedEventId> = HashMap::new();
        for (t, sk, id) in entries {
            map.insert(((*t).to_string(), (*sk).to_string()), eid(id));
        }
        map
    }

    fn as_set(ids: Vec<OwnedEventId>) -> HashSet<OwnedEventId> {
        ids.into_iter().collect()
    }

    // ---------- auth_event_keys ----------

    #[test]
    fn create_event_has_no_auth_events() {
        let ev = EventBuilder::new(
            "@alice:example.org".parse().expect("user id"),
            "m.room.create".to_owned(),
        )
        .state_key(String::new())
        .content(json!({ "room_version": ROOM_VERSION_ID }))
        .origin_server_ts(next_ts())
        .build()
        .expect("valid create");
        assert!(auth_event_keys(&ev).is_empty());
    }

    #[test]
    fn non_member_keys() {
        let ev = message_from("@alice:example.org");
        assert_eq!(
            auth_event_keys(&ev),
            vec![
                ("m.room.power_levels".to_string(), String::new()),
                (
                    "m.room.member".to_string(),
                    "@alice:example.org".to_string()
                ),
            ]
        );
    }

    #[test]
    fn v12_omits_create_event_key() {
        // Sanity: even when state contains a create event, auth_event_keys
        // for a non-create event must not ask for it.
        let ev = message_from("@alice:example.org");
        let keys = auth_event_keys(&ev);
        assert!(
            !keys.iter().any(|(t, _)| t == "m.room.create"),
            "v12 must not request m.room.create"
        );
    }

    #[test]
    fn member_join_includes_join_rules_and_target() {
        let ev = member(
            "@alice:example.org",
            "@bob:example.org",
            json!({ "membership": "join" }),
        );
        assert_eq!(
            auth_event_keys(&ev),
            vec![
                ("m.room.power_levels".to_string(), String::new()),
                (
                    "m.room.member".to_string(),
                    "@alice:example.org".to_string()
                ),
                ("m.room.member".to_string(), "@bob:example.org".to_string()),
                ("m.room.join_rules".to_string(), String::new()),
            ]
        );
    }

    #[test]
    fn member_leave_excludes_join_rules() {
        let ev = member(
            "@alice:example.org",
            "@bob:example.org",
            json!({ "membership": "leave" }),
        );
        let keys = auth_event_keys(&ev);
        assert!(!keys.iter().any(|(t, _)| t == "m.room.join_rules"));
        assert!(
            keys.iter()
                .any(|(t, sk)| t == "m.room.member" && sk == "@bob:example.org")
        );
    }

    #[test]
    fn member_self_membership_dedups_target() {
        // sender == state_key — the (m.room.member, sender) pair must only
        // appear once.
        let ev = member(
            "@alice:example.org",
            "@alice:example.org",
            json!({ "membership": "join" }),
        );
        let keys = auth_event_keys(&ev);
        let member_entries: Vec<_> = keys.iter().filter(|(t, _)| t == "m.room.member").collect();
        assert_eq!(member_entries.len(), 1);
        assert_eq!(member_entries[0].1, "@alice:example.org");
    }

    #[test]
    fn invite_with_third_party_invite_token() {
        let ev = member(
            "@alice:example.org",
            "@bob:example.org",
            json!({
                "membership": "invite",
                "third_party_invite": {
                    "signed": { "token": "tok123" }
                }
            }),
        );
        let keys = auth_event_keys(&ev);
        assert!(
            keys.iter()
                .any(|(t, sk)| t == "m.room.third_party_invite" && sk == "tok123")
        );
    }

    #[test]
    fn invite_without_third_party_invite_omits_it() {
        let ev = member(
            "@alice:example.org",
            "@bob:example.org",
            json!({ "membership": "invite" }),
        );
        let keys = auth_event_keys(&ev);
        assert!(!keys.iter().any(|(t, _)| t == "m.room.third_party_invite"));
    }

    #[test]
    fn join_with_authoriser_includes_authoriser_member_event() {
        let ev = member(
            "@alice:example.org",
            "@alice:example.org",
            json!({
                "membership": "join",
                "join_authorised_via_users_server": "@carol:example.org"
            }),
        );
        let keys = auth_event_keys(&ev);
        assert!(
            keys.iter()
                .any(|(t, sk)| t == "m.room.member" && sk == "@carol:example.org")
        );
    }

    #[test]
    fn join_authorised_only_applies_on_join_membership() {
        // join_authorised_via_users_server on a leave: irrelevant, ignored.
        let ev = member(
            "@alice:example.org",
            "@alice:example.org",
            json!({
                "membership": "leave",
                "join_authorised_via_users_server": "@carol:example.org"
            }),
        );
        let keys = auth_event_keys(&ev);
        assert!(
            !keys
                .iter()
                .any(|(t, sk)| t == "m.room.member" && sk == "@carol:example.org")
        );
    }

    #[test]
    fn unrecognised_membership_excludes_join_rules() {
        // Defensive: an unknown membership value (not in {join, invite,
        // knock}) doesn't pull in join_rules. `auth_rules::check_rule_5_member`
        // rejects unknown values via rule 5.8 (`Rule5_8_UnknownMembership`).
        let ev = member(
            "@alice:example.org",
            "@bob:example.org",
            json!({ "membership": "lurking" }),
        );
        let keys = auth_event_keys(&ev);
        assert!(!keys.iter().any(|(t, _)| t == "m.room.join_rules"));
    }

    // ---------- calculate_auth_events ----------

    #[test]
    fn expected_resolves_keys_through_state() {
        let ev = message_from("@alice:example.org");
        let st = state(&[
            ("m.room.power_levels", "", "$pl:example.org"),
            (
                "m.room.member",
                "@alice:example.org",
                "$alice-mem:example.org",
            ),
        ]);
        assert_eq!(
            as_set(calculate_auth_events(&ev, &st)),
            as_set(vec![eid("$pl:example.org"), eid("$alice-mem:example.org")])
        );
    }

    #[test]
    fn expected_drops_absent_entries() {
        // No power_levels and no sender member event in state → output is empty.
        let ev = message_from("@alice:example.org");
        let st = HashMap::new();
        assert!(calculate_auth_events(&ev, &st).is_empty());
    }

    #[test]
    fn expected_v12_excludes_create_even_when_in_state() {
        // Put a create event in state under (m.room.create, "") — auth_events
        // must not include it.
        let ev = message_from("@alice:example.org");
        let st = state(&[
            ("m.room.create", "", "$create:example.org"),
            ("m.room.power_levels", "", "$pl:example.org"),
            (
                "m.room.member",
                "@alice:example.org",
                "$alice-mem:example.org",
            ),
        ]);
        let result = as_set(calculate_auth_events(&ev, &st));
        assert!(!result.contains(&eid("$create:example.org")));
        assert!(result.contains(&eid("$pl:example.org")));
        assert!(result.contains(&eid("$alice-mem:example.org")));
    }

    #[test]
    fn expected_for_member_join_full_chain() {
        let ev = member(
            "@alice:example.org",
            "@bob:example.org",
            json!({ "membership": "join" }),
        );
        let st = state(&[
            ("m.room.power_levels", "", "$pl:example.org"),
            (
                "m.room.member",
                "@alice:example.org",
                "$alice-mem:example.org",
            ),
            ("m.room.member", "@bob:example.org", "$bob-mem:example.org"),
            ("m.room.join_rules", "", "$jr:example.org"),
        ]);
        assert_eq!(
            as_set(calculate_auth_events(&ev, &st)),
            as_set(vec![
                eid("$pl:example.org"),
                eid("$alice-mem:example.org"),
                eid("$bob-mem:example.org"),
                eid("$jr:example.org"),
            ])
        );
    }
}
