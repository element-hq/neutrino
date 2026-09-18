//! Outbound federated invite-rejection (joining-server side).
//!
//! When a local user declines an **out-of-band invite** — an invite for a room
//! this server is not in, held only as an [`OobMembershipStore`] stub with no
//! room state — the CSAPI `/leave` handler delegates here. We:
//!
//! 1. resolve the inviting server from the invite event's `sender` domain;
//! 2. run a **best-effort** `make_leave` → complete → `send_leave` handshake to
//!    it (so the resident records our departure); and
//! 3. record the completed leave over the invite stub — sync then shows the
//!    room as left — or, if the handshake failed, drop the stub.
//!
//! Step 3 always succeeds locally: the user stops seeing the invite even if the
//! inviting server is unreachable, and we never block the client on the
//! federation round-trip. The handshake failing is not an error — the invite
//! was never real room state for us.
//!
//! ## Security: complete, don't echo
//!
//! The leave event is rebuilt from scratch via the shared
//! [`crate::federation::complete_membership_template`] (type, sender, state_key,
//! content all set by us; only the DAG references come from the resident's
//! template) — never echoing the template's authoritative fields. See that
//! helper's docs for why (template-completion forgery; leave is the worst case).

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use neutrino_store::{OobMembership, OobMembershipStore};
use ruma::{OwnedUserId, RoomId, ServerName, UserId};
use serde_json::json;
use tracing::warn;

use crate::federation::client::FederationClient;
use crate::federation::complete_membership_template;
use crate::{AppState, error_response, lock_app};

/// Reject an out-of-band invite. The caller (`membership::leave`) passes the
/// already-loaded `invite` stub (so we don't re-read it). Always returns
/// `200 {}` unless the local stub write itself fails (a real storage fault).
pub(crate) async fn reject_invite(
    state: &AppState,
    user: OwnedUserId,
    room_id: &RoomId,
    invite: OobMembership,
) -> Response {
    let (store, policy, own_server, federation_proxy) = {
        let app = lock_app(state);
        (
            app.store.clone(),
            app.policy.clone(),
            app.config.server_name.clone(),
            app.config.federation_proxy.clone(),
        )
    };

    // Best-effort federated decline to the inviting server (the invite's sender
    // domain). Any failure (unreachable, refused, malformed template) is logged
    // *with its underlying cause* and swallowed — the local write below is what
    // the client relies on.
    let dest = invite.event.sender.server_name().to_owned();
    let display_name = crate::local_display_name(&store).await;
    let local = match try_federated_leave(
        &own_server,
        federation_proxy.as_deref(),
        &policy,
        &dest,
        room_id,
        &user,
        &display_name,
    )
    .await
    {
        // The completed leave replaces the invite: sync shows the room as left.
        Ok(leave) => {
            store
                .put_oob_membership(room_id, &user, &leave, &invite.room_version)
                .await
        }
        // No leave event to show; drop the stub so the invite at least vanishes.
        Err(e) => {
            warn!(%room_id, %dest, error = %e, "federated leave (invite reject) failed; rejecting locally anyway");
            store.remove_oob_membership(room_id, &user).await
        }
    };
    match local {
        Ok(()) => (StatusCode::OK, Json(json!({}))).into_response(),
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "M_UNKNOWN",
            &e.to_string(),
        ),
    }
}

/// Run the `make_leave` → complete → `send_leave` handshake against the inviting
/// server, returning the completed leave the resident accepted. On failure the
/// reason (including the underlying transport/HTTP error) is what the caller
/// logs before swallowing it.
async fn try_federated_leave(
    own_server: &str,
    proxy: Option<&str>,
    policy: &neutrino_event::EventPolicy,
    dest: &ServerName,
    room_id: &RoomId,
    user: &UserId,
    display_name: &str,
) -> Result<neutrino_event::Event, String> {
    let client =
        FederationClient::new(own_server.to_owned(), proxy).with_signer(policy.signer().cloned());
    let offered: Vec<&str> = policy.versions.ids().collect();
    let template = client
        .make_leave(dest, room_id, user, &offered)
        .await
        .map_err(|e| format!("make_leave request failed: {e}"))?;
    // The room's version, as the resident states it — the leave we build is
    // named under it.
    let version = policy
        .versions
        .get(&template.room_version)
        .cloned()
        .ok_or_else(|| {
            format!(
                "resident room version {} is unsupported",
                template.room_version
            )
        })?;
    let leave = complete_membership_template(
        policy,
        &version,
        &template.event,
        room_id,
        user,
        "leave",
        display_name,
    )
    .ok_or_else(|| "could not complete the leave template".to_string())?;
    client
        .send_leave(dest, room_id, &leave.event_id, &leave.raw)
        .await
        .map_err(|e| format!("send_leave request failed: {e}"))?;
    Ok(leave)
}

#[cfg(test)]
mod tests {
    use super::*;
    use neutrino_event::event_builder::EventBuilder;
    use serde_json::Value;

    /// CVE regression: a hostile make_leave template (wrong type, attacker
    /// content, a foreign state_key/sender) must not be echoed — the completed
    /// leave event is always a clean `leave` membership authored by *our* user.
    /// If someone later rewrites `complete_membership_template` to reuse the
    /// template's fields, this fails.
    #[tokio::test]
    async fn complete_leave_template_rebuilds_and_never_echoes_hostile_template() {
        let our_user = ruma::OwnedUserId::try_from("@victim:us.example").unwrap();
        let attacker = ruma::OwnedUserId::try_from("@attacker:resident.example").unwrap();

        // What a malicious resident might return in the make_leave template: a
        // message event for a *different* user, with arbitrary content. Built
        // via EventBuilder so it is a well-formed, parseable PDU (a hostile
        // server would hand back something valid-on-the-wire).
        let foreign_room = EventBuilder::new(
            attacker.clone(),
            "m.room.create".to_owned(),
            neutrino_event::base_version().clone(),
        )
        .state_key(String::new())
        .content(json!({ "room_version": neutrino_event::ROOM_VERSION_ID }))
        .build()
        .unwrap();
        let foreign_room_id = foreign_room.room_id.clone();
        // state_key is a non-`@` key: an `@`-prefixed state_key must already
        // equal the sender (a build-time format rule), so member-event state_key
        // forgery is impossible anyway — the vector this test pins is type +
        // content echoing.
        let hostile_event = EventBuilder::new(
            attacker,
            "m.room.message".to_owned(),
            neutrino_event::base_version().clone(),
        )
        .room_id(foreign_room_id.clone())
        .state_key("forged-key".to_owned())
        .content(json!({ "body": "forged", "membership": "ban" }))
        .prev_events(vec![foreign_room.event_id.clone()])
        .prev_state_events(vec![foreign_room.event_id.clone()])
        .build()
        .unwrap();
        let hostile = hostile_event.raw;

        // The leave we build must target our own room + our own user.
        let room_id = ruma::RoomId::parse("!room:resident.example").unwrap();
        let event = complete_membership_template(
            &neutrino_event::EventPolicy::trusted_network(),
            neutrino_event::base_version(),
            &hostile,
            &room_id,
            &our_user,
            "leave",
            "Neo",
        )
        .expect("template completes");
        let v: Value = serde_json::from_str(event.raw.get()).unwrap();
        assert_eq!(v["type"], "m.room.member", "type must be ours");
        assert_eq!(v["sender"], our_user.as_str(), "sender must be our user");
        assert_eq!(
            v["content"]["displayname"], "Neo",
            "our user's server-wide display name must be embedded"
        );
        assert_eq!(
            v["state_key"],
            our_user.as_str(),
            "state_key must be our user"
        );
        assert_eq!(
            v["content"]["membership"], "leave",
            "membership must be leave"
        );
        assert!(
            v["content"].get("body").is_none(),
            "attacker content must not survive"
        );
        assert_eq!(
            v["room_id"],
            room_id.as_str(),
            "room_id must be the target room"
        );
        // The DAG references ARE the one thing legitimately carried from the
        // template (so the leave anchors to the resident's heads); assert they
        // come through verbatim — a regression that dropped them would otherwise
        // pass the "never echoes" checks above.
        let foreign = foreign_room.event_id.as_str();
        assert_eq!(
            v["prev_events"],
            json!([foreign]),
            "prev_events must be carried from the template"
        );
        assert_eq!(
            v["prev_state_events"],
            json!([foreign]),
            "prev_state_events must be carried from the template"
        );
    }
    /// A gomatrixserverlib-shaped template (Complement's `make_join` /
    /// `make_leave`): a bare protoevent with no `origin_server_ts`, no `hashes`,
    /// `depth` and `unsigned` present, `auth_events` absent. Only the two DAG
    /// pointer arrays matter; a completion that parsed the template as a full
    /// PDU rejected it with "missing required field: origin_server_ts".
    #[test]
    fn complete_template_accepts_protoevent_without_pdu_fields() {
        let our_user = ruma::OwnedUserId::try_from("@alice:us.example").unwrap();
        let room_id = ruma::RoomId::parse("!room:resident.example").unwrap();
        let head = "$aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let state_head = "$bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let template = serde_json::value::to_raw_value(&json!({
            "type": "m.room.member",
            "sender": our_user.as_str(),
            "room_id": room_id.as_str(),
            "state_key": our_user.as_str(),
            "content": { "membership": "join" },
            "prev_events": [head],
            "prev_state_events": [state_head],
            "depth": 4,
            "unsigned": {},
        }))
        .unwrap();
        let event = complete_membership_template(
            &neutrino_event::EventPolicy::trusted_network(),
            neutrino_event::base_version(),
            &template,
            &room_id,
            &our_user,
            "join",
            "",
        )
        .expect("protoevent template completes");
        let v: Value = serde_json::from_str(event.raw.get()).unwrap();
        assert_eq!(v["prev_events"], json!([head]));
        assert_eq!(v["prev_state_events"], json!([state_head]));
        assert_eq!(v["content"]["membership"], "join");
        assert!(v["origin_server_ts"].is_u64(), "we stamp the timestamp");
    }

    /// Pointer arrays are still validated: a malformed event id fails the
    /// completion rather than being carried into an event we would author.
    #[test]
    fn complete_template_rejects_malformed_dag_pointer() {
        let our_user = ruma::OwnedUserId::try_from("@alice:us.example").unwrap();
        let room_id = ruma::RoomId::parse("!room:resident.example").unwrap();
        let template = serde_json::value::to_raw_value(&json!({
            "prev_events": ["not-an-event-id"],
            "prev_state_events": [],
        }))
        .unwrap();
        assert!(
            complete_membership_template(
                &neutrino_event::EventPolicy::trusted_network(),
                neutrino_event::base_version(),
                &template,
                &room_id,
                &our_user,
                "join",
                "",
            )
            .is_none()
        );
    }
}
