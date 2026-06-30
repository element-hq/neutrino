//! Outbound federated invite-rejection (joining-server side).
//!
//! When a local user declines an **out-of-band invite** — an invite for a room
//! we don't host, held only as an [`InviteStore`] stub with no room state — the
//! CSAPI `/leave` handler delegates here. We:
//!
//! 1. resolve the inviting server from the invite event's `sender` domain;
//! 2. run a **best-effort** `make_leave` → complete → `send_leave` handshake to
//!    it (so the resident records our departure); and
//! 3. **unconditionally** remove the local invite stub.
//!
//! Step 3 is the Synapse "local rejection" behaviour: the user stops seeing the
//! invite even if the inviting server is unreachable, and we never block the
//! client on the federation round-trip. The handshake failing is not an error —
//! the invite was never real room state for us.
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
use neutrino_event::ROOM_VERSION_ID;
use neutrino_store::InviteStore;
use ruma::{OwnedUserId, RoomId, ServerName, UserId};
use serde_json::json;
use tracing::warn;

use crate::federation::client::FederationClient;
use crate::federation::complete_membership_template;
use crate::{AppState, error_response, lock_app};

/// Reject an out-of-band invite. The caller (`membership::leave`) passes the
/// already-loaded `invite` stub (so we don't re-read it). Always returns
/// `200 {}` unless the local stub removal itself fails (a real storage fault).
pub(crate) async fn reject_invite(
    state: &AppState,
    user: OwnedUserId,
    room_id: &RoomId,
    invite: neutrino_event::Event,
) -> Response {
    let (store, own_server, federation_proxy) = {
        let app = lock_app(state);
        (
            app.store.clone(),
            app.config.server_name.clone(),
            app.config.federation_proxy.clone(),
        )
    };

    // Best-effort federated decline to the inviting server (the invite's sender
    // domain). Any failure (unreachable, refused, malformed template) is logged
    // *with its underlying cause* and swallowed — the unconditional local
    // removal below is what the client relies on.
    let dest = invite.sender.server_name().to_owned();
    if let Err(e) = try_federated_leave(
        &own_server,
        federation_proxy.as_deref(),
        &dest,
        room_id,
        &user,
    )
    .await
    {
        warn!(%room_id, %dest, error = %e, "federated leave (invite reject) failed; rejecting locally anyway");
    }

    // Unconditional local rejection: drop the stub so the invite vanishes from
    // sync. This is the part that MUST succeed for the client.
    match store.remove_invite(room_id, &user).await {
        Ok(()) => (StatusCode::OK, Json(json!({}))).into_response(),
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "M_UNKNOWN",
            &e.to_string(),
        ),
    }
}

/// Run the `make_leave` → complete → `send_leave` handshake against the inviting
/// server. Returns a detailed reason (including the underlying transport/HTTP
/// error) on any failure, which the caller logs before swallowing it.
async fn try_federated_leave(
    own_server: &str,
    proxy: Option<&str>,
    dest: &ServerName,
    room_id: &RoomId,
    user: &UserId,
) -> Result<(), String> {
    let client = FederationClient::new(own_server.to_owned(), proxy);
    let template = client
        .make_leave(dest, room_id, user, ROOM_VERSION_ID)
        .await
        .map_err(|e| format!("make_leave request failed: {e}"))?;
    if template.room_version != ROOM_VERSION_ID {
        return Err(format!(
            "resident room version {} is unsupported",
            template.room_version
        ));
    }
    let leave = complete_membership_template(&template.event, room_id, user, "leave")
        .ok_or_else(|| "could not complete the leave template".to_string())?;
    client
        .send_leave(dest, room_id, &leave.event_id, &leave.raw)
        .await
        .map_err(|e| format!("send_leave request failed: {e}"))
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
    #[test]
    fn complete_leave_template_rebuilds_and_never_echoes_hostile_template() {
        let our_user = ruma::OwnedUserId::try_from("@victim:us.example").unwrap();
        let attacker = ruma::OwnedUserId::try_from("@attacker:resident.example").unwrap();

        // What a malicious resident might return in the make_leave template: a
        // message event for a *different* user, with arbitrary content. Built
        // via EventBuilder so it is a well-formed, parseable PDU (a hostile
        // server would hand back something valid-on-the-wire).
        let foreign_room = EventBuilder::new(attacker.clone(), "m.room.create".to_owned())
            .state_key(String::new())
            .content(json!({ "room_version": neutrino_event::ROOM_VERSION_ID }))
            .build()
            .unwrap();
        let foreign_room_id = foreign_room.room_id.clone();
        // state_key is a non-`@` key: an `@`-prefixed state_key must already
        // equal the sender (a build-time format rule), so member-event state_key
        // forgery is impossible anyway — the vector this test pins is type +
        // content echoing.
        let hostile_event = EventBuilder::new(attacker, "m.room.message".to_owned())
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
        let event = complete_membership_template(&hostile, &room_id, &our_user, "leave")
            .expect("template completes");
        let v: Value = serde_json::from_str(event.raw.get()).unwrap();
        assert_eq!(v["type"], "m.room.member", "type must be ours");
        assert_eq!(v["sender"], our_user.as_str(), "sender must be our user");
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
}
