//! Phase 3: v12 state-dependent authorization rules.
//!
//! Functions and `AuthError` variants are named after the spec rule numbers
//! they enforce, so a reader can grep `5.3.2` and find the code. The
//! top-level dispatcher visits the rules in spec order; rules handled in
//! other modules are noted in their slot rather than dropped, with the
//! specific function that owns them:
//!
//! - Rule 1 (`m.room.create`) — 1.1–1.4 in `validate::parse_event` (and
//!   `validate::check_create`); 1.5 terminal allow lives here.
//! - Rule 2 (room exists, room_id ↔ create event) —
//!   `validate::validate_references` (yields `ReferenceError::UnknownRoom`,
//!   `RoomRejected`, `RoomTypeMismatch`).
//! - Rule 3 (`auth_events` checks) — dead under MSC4242. The wire field is
//!   rejected up front by `validate::parse_event`
//!   (`FormatError::AuthEventsPresent`); the algorithm now uses
//!   `auth_events::calculate_auth_events` instead.
//! - Rule 9 (state_key starts with `@`) — `validate::parse_event` raises
//!   `FormatError::StateKeyAtSignSenderMismatch`. (Excludes `m.room.member`;
//!   rule 5 handles that case end-to-end.)
//! - Rule 10.1–10.3 (`m.room.power_levels` content shape) —
//!   `validate::check_power_levels` (`FormatError::PowerLevelsBadIntField`,
//!   `PowerLevelsBadObjectField`, `PowerLevelsBadUsers`).
//! - Signature checks (rule 5.2, third-party `signed.*`) — skipped per
//!   `CLAUDE.md` trusted-network policy.
//!
//! `check_auth_rules` is a pure function over an `Event` and a fully-resolved
//! `StateMap<Arc<Event>>` (state-before-event). The caller is responsible for
//! materialising the state map from a `StateProvider`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use ruma::{OwnedUserId, UserId};
use serde_json::Value;

use crate::{AuthError, Event, StateMap};

// ----------------- public API -----------------

/// Run the v12 authorization rules against `state` (state-before-event).
///
/// Pre:  `event` has passed `validate::parse_event` (wire format is
///       well-formed; for `m.room.member` events, `state_key` and
///       `content.membership` are guaranteed present, so the rule-5
///       `.expect()`s below are safe). The caller has also called
///       `validate::validate_references` (room exists, `prev_state_events`
///       triad ok) — rule 2 references are trusted here. `state` is the
///       resolved state-before-event keyed by `(type, state_key)`,
///       materialised by the caller (Phase 6 `apply`) by state-resolving
///       over `event.prev_state_events` via the `StateProvider`.
/// Post: `Ok(())` means the event passes v12 authorization against `state`
///       and may be applied. `Err(AuthError::*)` names the specific rule
///       that rejected — the event must not be applied. Pure function: no
///       mutation, no I/O. Soft-fail handling (auth-vs-current-state) is
///       the caller's concern — Phase 6 `apply` runs this function twice,
///       once against state-before-event and once against the post-update
///       current state.
pub fn check_auth_rules(event: &Event, state: &StateMap<Arc<Event>>) -> Result<(), AuthError> {
    // Rule 1 (m.room.create):
    //   1.1 prev_events absent       → validate::parse_event (CreateHasPrevEvents)
    //   1.2 room_id absent           → validate::parse_event (CreateHasRoomId)
    //   1.3 room_version recognised  → validate::check_create (UnrecognisedRoomVersion)
    //   1.4 additional_creators ok   → validate::check_create (InvalidAdditionalCreators)
    //   1.5 terminal allow           → here.
    if event.event_type == "m.room.create" {
        return Ok(());
    }
    // Rule 2 (room_id ↔ accepted m.room.create event):
    //   validate::validate_references (UnknownRoom / RoomRejected / RoomTypeMismatch).
    // Rule 3 (auth_events): dead under MSC4242. Wire field rejected by
    // validate::parse_event (AuthEventsPresent); state-res uses
    // auth_events::calculate_auth_events instead.

    let ctx = AuthContext::new(state);

    check_rule_4_federation(event, &ctx)?;

    // Rule 5 (m.room.member): terminal for membership events; see
    // check_rule_5_member. 5.1 (state_key + content.membership presence) is
    // already enforced by validate::check_member.
    if event.event_type == "m.room.member" {
        return check_rule_5_member(event, &ctx);
    }

    check_rule_6_sender_joined(event, &ctx)?;

    // Rule 7 (m.room.third_party_invite): terminal; see
    // check_rule_7_third_party_invite.
    if event.event_type == "m.room.third_party_invite" {
        return check_rule_7_third_party_invite(event, &ctx);
    }

    check_rule_8_required_power_level(event, &ctx)?;

    // Rule 9 (state_key starts with `@`): validate::parse_event raises
    // FormatError::StateKeyAtSignSenderMismatch. (Excluded for m.room.member,
    // which rule 5 owns end-to-end.)

    // Rule 10 (m.room.power_levels): terminal; see
    // check_rule_10_power_levels. 10.1–10.3 (content shape) live in
    // validate::check_power_levels.
    if event.event_type == "m.room.power_levels" {
        return check_rule_10_power_levels(event, &ctx);
    }

    // Rule 11: terminal allow.
    Ok(())
}

// ----------------- AuthContext -----------------

/// Pre-computed views over state-before-event. Built once per
/// `check_auth_rules` call; rule sub-functions only borrow.
struct AuthContext<'a> {
    state: &'a StateMap<Arc<Event>>,
    create_event: &'a Arc<Event>,
    creators: HashSet<OwnedUserId>,
    power_levels: PowerLevels,
    /// `true` if state-before-event has an `m.room.power_levels` event.
    /// Used by rule 10.5 ("no previous power_levels → allow").
    present_power_levels: bool,
    /// `m.federate` from the create event's content, defaulting to `true`
    /// when absent. Pre-parsed so rule 4 doesn't re-walk the create content.
    federate: bool,
    /// `create_event.sender.server_name()` — used by rule 4 to compare
    /// against `event.sender.server_name()`.
    create_domain: String,
}

impl<'a> AuthContext<'a> {
    fn new(state: &'a StateMap<Arc<Event>>) -> Self {
        // `validate::validate_references` (phase 1b) has already verified
        // that this room's create event is in state; reaching auth without
        // it is a state-machine invariant violation by the caller.
        let create_event = state
            .get(&("m.room.create".to_string(), String::new()))
            .expect(
                "validate::validate_references guarantees m.room.create is in state-before-event",
            );

        let create_content = parse_content(create_event);
        let mut creators: HashSet<OwnedUserId> = HashSet::new();
        creators.insert(create_event.sender.clone());
        if let Some(arr) = create_content
            .get("additional_creators")
            .and_then(Value::as_array)
        {
            for entry in arr {
                let s = entry
                    .as_str()
                    .expect("validate::check_create guarantees additional_creators is [string]");
                let uid: OwnedUserId = s.parse().expect(
                    "validate::check_create guarantees additional_creators entries are user ids",
                );
                creators.insert(uid);
            }
        }

        let federate = create_content
            .get("m.federate")
            .and_then(Value::as_bool)
            // `m.federate` "Defaults to true if key does not exist." per
            // <https://spec.matrix.org/v1.18/client-server-api/#mroomcreate>.
            .unwrap_or(true);
        let create_domain = create_event.sender.server_name().as_str().to_owned();

        let pl_event = state.get(&("m.room.power_levels".to_string(), String::new()));
        let present_power_levels = pl_event.is_some();
        let power_levels = PowerLevels::parse(pl_event.map(Arc::as_ref));

        Self {
            state,
            create_event,
            creators,
            power_levels,
            present_power_levels,
            federate,
            create_domain,
        }
    }

    /// User's effective power level. Creators have `i64::MAX` ("cannot be
    /// demoted to a lower power level, even through m.room.power_levels").
    fn user_power(&self, user: &UserId) -> i64 {
        if self.creators.contains(user) {
            return i64::MAX;
        }
        *self
            .power_levels
            .users
            .get(user.as_str())
            .unwrap_or(&self.power_levels.users_default)
    }

    /// Current membership of `user`, or `None` if absent.
    fn membership(&self, user: &UserId) -> Option<String> {
        let ev = self
            .state
            .get(&("m.room.member".to_string(), user.as_str().to_string()))?;
        parse_content(ev)
            .get("membership")
            .and_then(Value::as_str)
            .map(str::to_owned)
    }

    /// Current `join_rule`, defaulting to `invite` if no `m.room.join_rules`
    /// state event is in state.
    ///
    /// The `m.room.join_rules` schema at
    /// <https://spec.matrix.org/v1.18/client-server-api/#mroomjoin_rules>
    /// declares `join_rule` as required and does **not** specify a
    /// "no event present" fallback. We default to `invite` to match
    /// synapse's `event_auth.py` behaviour — this is the most restrictive
    /// reasonable default and matches the rule 5.3.4 invite/knock branch.
    fn join_rule(&self) -> String {
        self.state
            .get(&("m.room.join_rules".to_string(), String::new()))
            .and_then(|ev| {
                parse_content(ev)
                    .get("join_rule")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| "invite".to_owned())
    }

    /// Required power level for `event`'s type. State events use
    /// `events[type]` or `state_default`; non-state use `events[type]` or
    /// `events_default`.
    fn required_power_for(&self, event: &Event) -> i64 {
        if let Some(level) = self.power_levels.events.get(&event.event_type) {
            return *level;
        }
        if event.state_key.is_some() {
            self.power_levels.state_default
        } else {
            self.power_levels.events_default
        }
    }
}

// ----------------- PowerLevels -----------------

struct PowerLevels {
    users_default: i64,
    events_default: i64,
    state_default: i64,
    ban: i64,
    redact: i64,
    kick: i64,
    invite: i64,
    users: HashMap<String, i64>,
    events: HashMap<String, i64>,
    notifications: HashMap<String, i64>,
}

impl PowerLevels {
    fn parse(event: Option<&Event>) -> Self {
        // Field-level defaults per the `m.room.power_levels` schema. Every
        // string below is a verbatim "Defaults to X if unspecified." clause
        // from https://spec.matrix.org/v1.18/client-server-api/#mroompower_levels
        // (cross-ref: data/event-schemas/schema/m.room.power_levels.yaml in
        // matrix-org/matrix-spec).
        let mut out = PowerLevels {
            users_default: 0,  // "Defaults to 0 if unspecified."
            events_default: 0, // "Defaults to 0 if unspecified."
            state_default: 50, // "Defaults to 50 if unspecified."
            ban: 50,           // "Defaults to 50 if unspecified."
            redact: 50,        // "Defaults to 50 if unspecified."
            kick: 50,          // "Defaults to 50 if unspecified."
            invite: 0,         // "Defaults to 0 if unspecified."
            users: HashMap::new(),
            events: HashMap::new(),
            notifications: HashMap::new(),
        };
        let Some(ev) = event else { return out };
        let content = parse_content(ev);
        // Phase 1a (`validate::check_power_levels`) has rejected non-integer
        // scalars (`PowerLevelsBadIntField`) and non-`{string: int}` objects
        // (`PowerLevelsBadObjectField` / `PowerLevelsBadUsers`) — so every
        // value below either matches its expected shape or the field is
        // absent. The `.expect`s exist to convert a future Phase 1a
        // regression into a hard panic at the parse site rather than
        // silently substituting defaults.
        let take_scalar = |field: &'static str, slot: &mut i64| {
            if let Some(v) = content.get(field) {
                *slot = v
                    .as_i64()
                    .expect("validate::check_power_levels guarantees integer scalar");
            }
        };
        take_scalar("users_default", &mut out.users_default);
        take_scalar("events_default", &mut out.events_default);
        take_scalar("state_default", &mut out.state_default);
        take_scalar("ban", &mut out.ban);
        take_scalar("redact", &mut out.redact);
        take_scalar("kick", &mut out.kick);
        take_scalar("invite", &mut out.invite);
        for (target, key) in [
            (&mut out.users, "users"),
            (&mut out.events, "events"),
            (&mut out.notifications, "notifications"),
        ] {
            if let Some(obj) = content.get(key) {
                let obj = obj
                    .as_object()
                    .expect("validate::check_power_levels guarantees map shape");
                for (k, v) in obj {
                    let n = v
                        .as_i64()
                        .expect("validate::check_power_levels guarantees integer map values");
                    target.insert(k.clone(), n);
                }
            }
        }
        out
    }
}

fn parse_content(event: &Event) -> Value {
    // `validate::parse_event` parsed this same raw JSON to land an Event;
    // round-tripping it here cannot fail.
    serde_json::from_str(event.content.get())
        .expect("validate::parse_event guarantees event.content is valid JSON")
}

// ----------------- rule 4 -----------------

fn check_rule_4_federation(event: &Event, ctx: &AuthContext) -> Result<(), AuthError> {
    // "If the content of the m.room.create event in the room state has the
    // property m.federate set to false, and the sender domain of the event
    // does not match the sender domain of the create event, reject."
    if ctx.federate {
        return Ok(());
    }
    let sender_domain = event.sender.server_name().as_str();
    if sender_domain == ctx.create_domain {
        return Ok(());
    }
    Err(AuthError::Rule4FederationDisallowed {
        sender_domain: sender_domain.to_owned(),
        create_domain: ctx.create_domain.clone(),
    })
}

// ----------------- rule 5 -----------------

fn check_rule_5_member(member_event: &Event, ctx: &AuthContext) -> Result<(), AuthError> {
    // Invariant: the top-level dispatcher only calls us for m.room.member.
    debug_assert_eq!(member_event.event_type, "m.room.member");

    // 5.1 (state_key + content.membership presence): enforced upstream by
    // validate::check_member (FormatError::MemberMissingStateKey /
    // MemberMissingMembership) — the `.expect()`s below are guaranteed safe
    // for events that reach Phase 3.
    // 5.2 (signature on join_authorised_via_users_server): skipped per
    // CLAUDE.md trusted-network policy.
    let state_key = member_event
        .state_key
        .as_deref()
        .expect("validate::check_member guarantees state_key on m.room.member");
    let content = parse_content(member_event);
    let membership = content
        .get("membership")
        .and_then(Value::as_str)
        .expect("validate::check_member guarantees content.membership on m.room.member")
        .to_owned();

    match membership.as_str() {
        "join" => check_rule_5_3_join(member_event, ctx, &content, state_key),
        "invite" => check_rule_5_4_invite(member_event, ctx, &content, state_key),
        "leave" => check_rule_5_5_leave(member_event, ctx, state_key),
        "ban" => check_rule_5_6_ban(member_event, ctx, state_key),
        "knock" => check_rule_5_7_knock(member_event, ctx, state_key),
        _ => Err(AuthError::Rule5_8_UnknownMembership(membership)),
    }
}

fn check_rule_5_3_join(
    member_event: &Event,
    ctx: &AuthContext,
    content: &Value,
    state_key: &str,
) -> Result<(), AuthError> {
    let sender = member_event.sender.as_str();

    // 5.3.1: "If the only previous event is an m.room.create and the
    // state_key is the sender of the m.room.create, allow."
    //
    // Under MSC4242 the room/state DAGs are linked separately, so the
    // "only previous event" condition must hold for both `prev_events`
    // and `prev_state_events` — and in both cases the sole entry must be
    // the create event.
    if member_event.prev_events.len() == 1
        && member_event.prev_events[0] == ctx.create_event.event_id
        && member_event.prev_state_events.len() == 1
        && member_event.prev_state_events[0] == ctx.create_event.event_id
        && state_key == ctx.create_event.sender.as_str()
    {
        return Ok(());
    }

    // 5.3.2: sender == state_key
    if sender != state_key {
        return Err(AuthError::Rule5_3_2_JoinSenderMismatch);
    }

    // 5.3.3: sender is banned
    if ctx.membership(member_event.sender.as_ref()).as_deref() == Some("ban") {
        return Err(AuthError::Rule5_3_3_JoinWhileBanned);
    }

    // 5.3.4 / 5.3.5 / 5.3.6 / 5.3.7
    let join_rule = ctx.join_rule();
    let current = ctx.membership(member_event.sender.as_ref());
    match join_rule.as_str() {
        "invite" | "knock" => match current.as_deref() {
            Some("invite") | Some("join") => Ok(()),
            _ => Err(AuthError::Rule5_3_4_JoinNotInvited),
        },
        "restricted" | "knock_restricted" => {
            if matches!(current.as_deref(), Some("join") | Some("invite")) {
                return Ok(());
            }
            let authoriser_str = content
                .get("join_authorised_via_users_server")
                .and_then(Value::as_str)
                .ok_or(AuthError::Rule5_3_5_RestrictedAuthoriserInvalid)?;
            let authoriser: OwnedUserId = authoriser_str
                .parse()
                .map_err(|_| AuthError::Rule5_3_5_RestrictedAuthoriserInvalid)?;
            if ctx.membership(&authoriser).as_deref() != Some("join") {
                return Err(AuthError::Rule5_3_5_RestrictedAuthoriserInvalid);
            }
            if ctx.user_power(&authoriser) < ctx.power_levels.invite {
                return Err(AuthError::Rule5_3_5_RestrictedAuthoriserInvalid);
            }
            Ok(())
        }
        "public" => Ok(()),
        _ => Err(AuthError::Rule5_3_7_JoinNotAllowed),
    }
}

fn check_rule_5_4_invite(
    member_event: &Event,
    ctx: &AuthContext,
    content: &Value,
    state_key: &str,
) -> Result<(), AuthError> {
    let target: OwnedUserId = state_key
        .parse()
        .map_err(|_| AuthError::InvalidMemberStateKey {
            state_key: state_key.to_owned(),
        })?;

    // 5.4.1: third_party_invite path
    if let Some(tpi) = content.get("third_party_invite") {
        return check_third_party_invite_path(member_event, ctx, tpi, &target);
    }

    // 5.4.2: sender joined
    if ctx.membership(member_event.sender.as_ref()).as_deref() != Some("join") {
        return Err(AuthError::Rule5_4_2_InviteSenderNotJoined);
    }

    // 5.4.3: target not already joined or banned
    if matches!(
        ctx.membership(&target).as_deref(),
        Some("join") | Some("ban")
    ) {
        return Err(AuthError::Rule5_4_3_InviteTargetJoinedOrBanned);
    }

    // 5.4.4 / 5.4.5: sender power vs invite level
    let sender_power = ctx.user_power(member_event.sender.as_ref());
    if sender_power >= ctx.power_levels.invite {
        return Ok(());
    }
    Err(AuthError::Rule5_4_5_InvitePowerInsufficient {
        sender: sender_power,
        needed: ctx.power_levels.invite,
    })
}

fn check_third_party_invite_path(
    member_event: &Event,
    ctx: &AuthContext,
    tpi: &Value,
    target: &OwnedUserId,
) -> Result<(), AuthError> {
    // "If target user is banned, reject."
    if ctx.membership(target).as_deref() == Some("ban") {
        return Err(AuthError::Rule5_4_1_ThirdPartyInviteInvalid(
            "target banned",
        ));
    }
    let signed = tpi
        .get("signed")
        .ok_or(AuthError::Rule5_4_1_ThirdPartyInviteInvalid("no signed"))?;
    let mxid = signed
        .get("mxid")
        .and_then(Value::as_str)
        .ok_or(AuthError::Rule5_4_1_ThirdPartyInviteInvalid("no mxid"))?;
    let token = signed
        .get("token")
        .and_then(Value::as_str)
        .ok_or(AuthError::Rule5_4_1_ThirdPartyInviteInvalid("no token"))?;
    if mxid != target.as_str() {
        return Err(AuthError::Rule5_4_1_ThirdPartyInviteInvalid(
            "mxid does not match state_key",
        ));
    }
    let tpi_event = ctx
        .state
        .get(&("m.room.third_party_invite".to_string(), token.to_string()))
        .ok_or(AuthError::Rule5_4_1_ThirdPartyInviteInvalid(
            "no matching m.room.third_party_invite",
        ))?;
    if tpi_event.sender != member_event.sender {
        return Err(AuthError::Rule5_4_1_ThirdPartyInviteInvalid(
            "sender does not match third_party_invite sender",
        ));
    }
    // Signature verification skipped per trusted-network policy. Accept once
    // a `public_key` or `public_keys` is present on the invite.
    let tpi_content = parse_content(tpi_event);
    if tpi_content.get("public_key").is_some() || tpi_content.get("public_keys").is_some() {
        return Ok(());
    }
    Err(AuthError::Rule5_4_1_ThirdPartyInviteInvalid(
        "no public_key(s) on third_party_invite",
    ))
}

fn check_rule_5_5_leave(
    member_event: &Event,
    ctx: &AuthContext,
    state_key: &str,
) -> Result<(), AuthError> {
    let sender = member_event.sender.as_str();

    // 5.5.1: self-leave allowed iff current is invite/join/knock
    if sender == state_key {
        return match ctx.membership(member_event.sender.as_ref()).as_deref() {
            Some("invite") | Some("join") | Some("knock") => Ok(()),
            _ => Err(AuthError::Rule5_5_1_SelfLeaveInvalid),
        };
    }

    let target: OwnedUserId = state_key
        .parse()
        .map_err(|_| AuthError::InvalidMemberStateKey {
            state_key: state_key.to_owned(),
        })?;

    // 5.5.2: kick sender must be joined
    if ctx.membership(member_event.sender.as_ref()).as_deref() != Some("join") {
        return Err(AuthError::Rule5_5_2_LeaveSenderNotJoined);
    }

    let sender_power = ctx.user_power(member_event.sender.as_ref());
    let target_power = ctx.user_power(&target);

    // 5.5.3: unban (target currently banned) requires ban-level power
    if ctx.membership(&target).as_deref() == Some("ban") && sender_power < ctx.power_levels.ban {
        return Err(AuthError::Rule5_5_3_UnbanInsufficient {
            sender: sender_power,
            needed: ctx.power_levels.ban,
        });
    }

    // 5.5.4: kick — sender power ≥ kick level AND target power < sender power
    if sender_power >= ctx.power_levels.kick && target_power < sender_power {
        return Ok(());
    }
    Err(AuthError::Rule5_5_5_KickNotAllowed {
        sender: sender_power,
        target: target_power,
        kick_level: ctx.power_levels.kick,
    })
}

fn check_rule_5_6_ban(
    member_event: &Event,
    ctx: &AuthContext,
    state_key: &str,
) -> Result<(), AuthError> {
    let target: OwnedUserId = state_key
        .parse()
        .map_err(|_| AuthError::InvalidMemberStateKey {
            state_key: state_key.to_owned(),
        })?;

    // 5.6.1: sender joined
    if ctx.membership(member_event.sender.as_ref()).as_deref() != Some("join") {
        return Err(AuthError::Rule5_6_1_BanSenderNotJoined);
    }

    let sender_power = ctx.user_power(member_event.sender.as_ref());
    let target_power = ctx.user_power(&target);

    // 5.6.2: ban — sender power ≥ ban level AND target power < sender power
    if sender_power >= ctx.power_levels.ban && target_power < sender_power {
        return Ok(());
    }
    Err(AuthError::Rule5_6_3_BanNotAllowed {
        sender: sender_power,
        target: target_power,
        ban_level: ctx.power_levels.ban,
    })
}

fn check_rule_5_7_knock(
    member_event: &Event,
    ctx: &AuthContext,
    state_key: &str,
) -> Result<(), AuthError> {
    // 5.7.1: join_rule must be knock or knock_restricted
    let jr = ctx.join_rule();
    if jr != "knock" && jr != "knock_restricted" {
        return Err(AuthError::Rule5_7_1_KnockNotKnockable);
    }
    // 5.7.2: sender == state_key
    if member_event.sender.as_str() != state_key {
        return Err(AuthError::Rule5_7_2_KnockSenderMismatch);
    }
    // 5.7.3: current membership NOT in {ban, invite, join} → allow
    match ctx.membership(member_event.sender.as_ref()).as_deref() {
        Some("ban") | Some("invite") | Some("join") => {
            Err(AuthError::Rule5_7_4_KnockFromInvalidMembership)
        }
        _ => Ok(()),
    }
}

// ----------------- rule 6 -----------------

fn check_rule_6_sender_joined(event: &Event, ctx: &AuthContext) -> Result<(), AuthError> {
    if ctx.membership(event.sender.as_ref()).as_deref() == Some("join") {
        return Ok(());
    }
    Err(AuthError::Rule6_SenderNotJoined)
}

// ----------------- rule 7 -----------------

fn check_rule_7_third_party_invite(
    third_party_invite_event: &Event,
    ctx: &AuthContext,
) -> Result<(), AuthError> {
    // Invariant: the dispatcher only calls us for m.room.third_party_invite.
    debug_assert_eq!(
        third_party_invite_event.event_type,
        "m.room.third_party_invite"
    );

    let sender_power = ctx.user_power(third_party_invite_event.sender.as_ref());
    if sender_power >= ctx.power_levels.invite {
        return Ok(());
    }
    Err(AuthError::Rule7_ThirdPartyInviteInsufficient {
        sender: sender_power,
        needed: ctx.power_levels.invite,
    })
}

// ----------------- rule 8 -----------------

fn check_rule_8_required_power_level(event: &Event, ctx: &AuthContext) -> Result<(), AuthError> {
    let sender_power = ctx.user_power(event.sender.as_ref());
    let required = ctx.required_power_for(event);
    if sender_power >= required {
        return Ok(());
    }
    Err(AuthError::Rule8_RequiredPowerInsufficient {
        event_type: event.event_type.clone(),
        required,
        sender: sender_power,
    })
}

// ----------------- rule 10 -----------------

fn check_rule_10_power_levels(
    power_levels_event: &Event,
    ctx: &AuthContext,
) -> Result<(), AuthError> {
    // Invariant: the dispatcher only calls us for m.room.power_levels.
    debug_assert_eq!(power_levels_event.event_type, "m.room.power_levels");

    // 10.1–10.3 (content shape — integer scalars, {string: int} events /
    // notifications, {valid-user-id: int} users): enforced upstream by
    // validate::check_power_levels (FormatError::PowerLevelsBadIntField /
    // PowerLevelsBadObjectField / PowerLevelsBadUsers). 10.4–10.11 below.
    let content = parse_content(power_levels_event);

    // 10.4: users must not name any creator.
    if let Some(users) = content.get("users").and_then(Value::as_object) {
        for k in users.keys() {
            if let Ok(uid) = k.parse::<OwnedUserId>()
                && ctx.creators.contains(&uid)
            {
                return Err(AuthError::Rule10_4_CreatorInUsers(uid));
            }
        }
    }

    // 10.5: no previous power_levels event → allow.
    if !ctx.present_power_levels {
        return Ok(());
    }

    let new_pl = PowerLevels::parse(Some(power_levels_event));
    let cur_pl = &ctx.power_levels;
    let sender_power = ctx.user_power(power_levels_event.sender.as_ref());

    // Rule 10's alteration gates (10.6 / 10.7 / 10.8 / 10.9 / 10.10) only fire
    // when an entry is being *altered* — added, removed, or changed. Each
    // `if cur == new { continue; }` below short-circuits the unchanged case;
    // copying an existing power level forward unchanged is always allowed.

    // 10.6: scalar default-level alterations.
    let scalars: &[(&str, i64, i64)] = &[
        ("users_default", cur_pl.users_default, new_pl.users_default),
        (
            "events_default",
            cur_pl.events_default,
            new_pl.events_default,
        ),
        ("state_default", cur_pl.state_default, new_pl.state_default),
        ("ban", cur_pl.ban, new_pl.ban),
        ("redact", cur_pl.redact, new_pl.redact),
        ("kick", cur_pl.kick, new_pl.kick),
        ("invite", cur_pl.invite, new_pl.invite),
    ];
    for (field, cur, new) in scalars {
        if cur == new {
            continue;
        }
        if *cur > sender_power {
            return Err(AuthError::Rule10_6_1_CurrentDefaultAboveSender {
                property: (*field).to_string(),
                value: *cur,
                sender: sender_power,
            });
        }
        if *new > sender_power {
            return Err(AuthError::Rule10_6_2_NewDefaultAboveSender {
                property: (*field).to_string(),
                value: *new,
                sender: sender_power,
            });
        }
    }

    // 10.7 / 10.8: events and notifications maps.
    //
    // Raw-presence comparison, matching synapse `event_auth.py` —
    // `_check_power_levels` only treats an entry as unchanged when both the
    // old and new sides have it present AND equal. Adding an entry from
    // absent to explicit-anything (including the spec default) counts as
    // an addition under 10.8; removing one counts as a change under 10.7.
    // No map-wide default is applied for `notifications`; the spec only
    // documents `notifications.room = 50` and does not generalise.
    for (property, cur_map, new_map) in [
        ("events", &cur_pl.events, &new_pl.events),
        (
            "notifications",
            &cur_pl.notifications,
            &new_pl.notifications,
        ),
    ] {
        let mut all_keys: HashSet<&String> = HashSet::new();
        all_keys.extend(cur_map.keys());
        all_keys.extend(new_map.keys());
        for key in all_keys {
            let cur_val = cur_map.get(key);
            let new_val = new_map.get(key);
            if let (Some(cv), Some(nv)) = (cur_val, new_val)
                && cv == nv
            {
                continue;
            }
            if let Some(cv) = cur_val
                && *cv > sender_power
            {
                return Err(AuthError::Rule10_7_CurrentEventEntryAboveSender {
                    property: property.to_string(),
                    key: key.clone(),
                    value: *cv,
                    sender: sender_power,
                });
            }
            if let Some(nv) = new_val
                && *nv > sender_power
            {
                return Err(AuthError::Rule10_8_NewEventEntryAboveSender {
                    property: property.to_string(),
                    key: key.clone(),
                    value: *nv,
                    sender: sender_power,
                });
            }
        }
    }

    // 10.9 / 10.10: users map.
    let mut all_user_keys: HashSet<&String> = HashSet::new();
    all_user_keys.extend(cur_pl.users.keys());
    all_user_keys.extend(new_pl.users.keys());
    for key in all_user_keys {
        let cur_val = cur_pl.users.get(key);
        let new_val = new_pl.users.get(key);
        if cur_val == new_val {
            continue;
        }
        let uid: OwnedUserId = match key.parse() {
            Ok(u) => u,
            // Phase 1a (validate::check_power_levels via PowerLevelsBadUsers)
            // already rejects malformed user ids in the `users` map; this
            // arm is unreachable in practice.
            Err(_) => continue,
        };
        // 10.9: change/removal of an entry other than sender's own — current
        // value must be strictly less than sender's power.
        if power_levels_event.sender != uid
            && let Some(cv) = cur_val
            && *cv >= sender_power
        {
            return Err(AuthError::Rule10_9_CurrentUsersEntryAtOrAboveSender {
                user: uid.clone(),
                value: *cv,
                sender: sender_power,
            });
        }
        // 10.10: addition/change — new value must not exceed sender's power.
        if let Some(nv) = new_val
            && *nv > sender_power
        {
            return Err(AuthError::Rule10_10_NewUsersEntryAboveSender {
                user: uid,
                value: *nv,
                sender: sender_power,
            });
        }
    }

    // 10.11: terminal allow.
    Ok(())
}

// ============== tests ==============

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RoomVersion;
    use crate::validate::parse_event;
    use neutrino_common::ROOM_VERSION_ID;
    use ruma::OwnedEventId;
    use serde_json::value::RawValue;
    use serde_json::{Value, json};

    // ---------- fixture helpers ----------

    fn raw(v: Value) -> Box<RawValue> {
        serde_json::value::to_raw_value(&v).expect("fixture")
    }
    fn eid(s: &str) -> OwnedEventId {
        s.parse().expect("event id")
    }
    fn make(v: Value, id: &str) -> Arc<Event> {
        Arc::new(parse_event(raw(v), eid(id), vec![], RoomVersion::V12).expect("valid event"))
    }

    fn create_event(creator: &str, additional_creators: &[&str]) -> Arc<Event> {
        let mut content = json!({ "room_version": ROOM_VERSION_ID });
        if !additional_creators.is_empty() {
            content["additional_creators"] = json!(
                additional_creators
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>()
            );
        }
        let mut v = json!({
            "type": "m.room.create",
            "sender": creator,
            "content": content,
            "prev_events": [],
            "depth": 0,
            "origin_server_ts": 1_700_000_000_000_u64,
            "hashes": { "sha256": "abc" },
            "state_key": ""
        });
        // Ensure no room_id field on create.
        let _ = v.as_object_mut().unwrap().remove("room_id");
        make(v, "$create:example.org")
    }

    fn member_event(
        sender: &str,
        target: &str,
        membership: &str,
        id: &str,
        prev_events: &[&str],
        extra_content: Value,
    ) -> Arc<Event> {
        let mut content = json!({ "membership": membership });
        for (k, v) in extra_content.as_object().cloned().unwrap_or_default() {
            content[k] = v;
        }
        let v = json!({
            "type": "m.room.member",
            "sender": sender,
            "state_key": target,
            "room_id": "!create:example.org",
            "content": content,
            "prev_events": prev_events,
            "prev_state_events": [],
            "depth": 1,
            "origin_server_ts": 1_700_000_000_000_u64,
            "hashes": { "sha256": "abc" }
        });
        make(v, id)
    }

    fn message_event(sender: &str, id: &str) -> Arc<Event> {
        make(
            json!({
                "type": "m.room.message",
                "sender": sender,
                "room_id": "!create:example.org",
                "content": { "msgtype": "m.text", "body": "hi" },
                "prev_events": [],
                "prev_state_events": [],
                "depth": 1,
                "origin_server_ts": 1_700_000_000_000_u64,
                "hashes": { "sha256": "abc" }
            }),
            id,
        )
    }

    fn state_event(
        event_type: &str,
        state_key: &str,
        sender: &str,
        content: Value,
        id: &str,
    ) -> Arc<Event> {
        make(
            json!({
                "type": event_type,
                "sender": sender,
                "state_key": state_key,
                "room_id": "!create:example.org",
                "content": content,
                "prev_events": [],
                "prev_state_events": [],
                "depth": 1,
                "origin_server_ts": 1_700_000_000_000_u64,
                "hashes": { "sha256": "abc" }
            }),
            id,
        )
    }

    fn power_levels_event(sender: &str, content: Value, id: &str) -> Arc<Event> {
        state_event("m.room.power_levels", "", sender, content, id)
    }

    fn join_rules_event(rule: &str, id: &str) -> Arc<Event> {
        state_event(
            "m.room.join_rules",
            "",
            "@alice:example.org",
            json!({ "join_rule": rule }),
            id,
        )
    }

    /// Build a state map from an iterator of `Arc<Event>`s keyed by their
    /// `(type, state_key)`.
    fn state_map<I: IntoIterator<Item = Arc<Event>>>(events: I) -> StateMap<Arc<Event>> {
        let mut map = StateMap::new();
        for ev in events {
            let key = (
                ev.event_type.clone(),
                ev.state_key.clone().unwrap_or_default(),
            );
            map.insert(key, ev);
        }
        map
    }

    // ---------- rule 4: federation ----------

    #[test]
    fn rule_4_allows_same_domain_when_non_federating() {
        let create = create_event("@alice:example.org", &[]);
        let mut create_content = parse_content(&create);
        create_content["m.federate"] = json!(false);
        // Re-build a create with m.federate: false
        let create = make(
            json!({
                "type": "m.room.create",
                "sender": "@alice:example.org",
                "content": { "room_version": ROOM_VERSION_ID, "m.federate": false },
                "prev_events": [],
                "depth": 0,
                "origin_server_ts": 1_700_000_000_000_u64,
                "hashes": { "sha256": "abc" },
                "state_key": ""
            }),
            "$create:example.org",
        );
        let alice_join = member_event(
            "@alice:example.org",
            "@alice:example.org",
            "join",
            "$j1:example.org",
            &[],
            json!({}),
        );
        let st = state_map([create.clone(), alice_join]);
        let msg = message_event("@alice:example.org", "$m:example.org");
        check_auth_rules(&msg, &st).expect("same-domain sender allowed when federation off");
    }

    #[test]
    fn rule_4_rejects_cross_domain_when_non_federating() {
        let create = make(
            json!({
                "type": "m.room.create",
                "sender": "@alice:example.org",
                "content": { "room_version": ROOM_VERSION_ID, "m.federate": false },
                "prev_events": [],
                "depth": 0,
                "origin_server_ts": 1_700_000_000_000_u64,
                "hashes": { "sha256": "abc" },
                "state_key": ""
            }),
            "$create:example.org",
        );
        let alice_join = member_event(
            "@alice:example.org",
            "@alice:example.org",
            "join",
            "$j1:example.org",
            &[],
            json!({}),
        );
        let bob_join = member_event(
            "@bob:other.org",
            "@bob:other.org",
            "join",
            "$j2:example.org",
            &[],
            json!({}),
        );
        let st = state_map([create.clone(), alice_join, bob_join]);
        let msg = message_event("@bob:other.org", "$m:example.org");
        assert!(matches!(
            check_auth_rules(&msg, &st),
            Err(AuthError::Rule4FederationDisallowed { .. })
        ));
    }

    // ---------- rule 5.3: join ----------

    #[test]
    fn rule_5_3_1_self_join_immediately_after_create() {
        // Both DAGs (room + state) have the create event as their sole
        // previous reference; state_key == create sender. Inlined because
        // the `member_event` helper hard-codes prev_state_events: [].
        let create = create_event("@alice:example.org", &[]);
        let join = make(
            json!({
                "type": "m.room.member",
                "sender": "@alice:example.org",
                "state_key": "@alice:example.org",
                "room_id": "!create:example.org",
                "content": { "membership": "join" },
                "prev_events": ["$create:example.org"],
                "prev_state_events": ["$create:example.org"],
                "depth": 1,
                "origin_server_ts": 1_700_000_000_000_u64,
                "hashes": { "sha256": "abc" }
            }),
            "$j:example.org",
        );
        let st = state_map([create]);
        check_auth_rules(&join, &st).expect("first-join after create allowed");
    }

    #[test]
    fn rule_5_3_2_join_mismatched_sender_rejected() {
        let create = create_event("@alice:example.org", &[]);
        // Public join_rule so we'd otherwise pass 5.3.4
        let jr = join_rules_event("public", "$jr:example.org");
        let join = member_event(
            "@alice:example.org",
            "@bob:example.org",
            "join",
            "$j:example.org",
            &[],
            json!({}),
        );
        let st = state_map([create, jr]);
        assert!(matches!(
            check_auth_rules(&join, &st),
            Err(AuthError::Rule5_3_2_JoinSenderMismatch)
        ));
    }

    #[test]
    fn rule_5_3_3_join_while_banned_rejected() {
        let create = create_event("@alice:example.org", &[]);
        let alice_join = member_event(
            "@alice:example.org",
            "@alice:example.org",
            "join",
            "$j1:example.org",
            &[],
            json!({}),
        );
        let bob_banned = member_event(
            "@alice:example.org",
            "@bob:example.org",
            "ban",
            "$ban:example.org",
            &[],
            json!({}),
        );
        let jr = join_rules_event("public", "$jr:example.org");
        // Need power for alice to have banned bob; rely on creators-have-infinite power.
        let st = state_map([create, alice_join, bob_banned, jr]);
        let bob_join = member_event(
            "@bob:example.org",
            "@bob:example.org",
            "join",
            "$bj:example.org",
            &[],
            json!({}),
        );
        assert!(matches!(
            check_auth_rules(&bob_join, &st),
            Err(AuthError::Rule5_3_3_JoinWhileBanned)
        ));
    }

    #[test]
    fn rule_5_3_4_invite_join_rule_requires_prior_invite() {
        let create = create_event("@alice:example.org", &[]);
        let jr = join_rules_event("invite", "$jr:example.org");
        let st = state_map([create, jr]);
        let join = member_event(
            "@bob:example.org",
            "@bob:example.org",
            "join",
            "$j:example.org",
            &[],
            json!({}),
        );
        assert!(matches!(
            check_auth_rules(&join, &st),
            Err(AuthError::Rule5_3_4_JoinNotInvited)
        ));
    }

    #[test]
    fn rule_5_3_public_join_rule_allows_anyone() {
        let create = create_event("@alice:example.org", &[]);
        let jr = join_rules_event("public", "$jr:example.org");
        let st = state_map([create, jr]);
        let join = member_event(
            "@bob:example.org",
            "@bob:example.org",
            "join",
            "$j:example.org",
            &[],
            json!({}),
        );
        check_auth_rules(&join, &st).expect("public join_rule");
    }

    // ---------- rule 5.4: invite ----------

    #[test]
    fn rule_5_4_invite_by_joined_sender_allowed() {
        let create = create_event("@alice:example.org", &[]);
        let alice_join = member_event(
            "@alice:example.org",
            "@alice:example.org",
            "join",
            "$j1:example.org",
            &[],
            json!({}),
        );
        let st = state_map([create, alice_join]);
        // Alice (creator → infinite power) invites bob.
        let invite = member_event(
            "@alice:example.org",
            "@bob:example.org",
            "invite",
            "$inv:example.org",
            &[],
            json!({}),
        );
        check_auth_rules(&invite, &st).expect("creator invite");
    }

    #[test]
    fn rule_5_4_2_invite_by_non_joined_rejected() {
        let create = create_event("@alice:example.org", &[]);
        let st = state_map([create]);
        // @bob (not a member) tries to invite @carol.
        let invite = member_event(
            "@bob:example.org",
            "@carol:example.org",
            "invite",
            "$inv:example.org",
            &[],
            json!({}),
        );
        assert!(matches!(
            check_auth_rules(&invite, &st),
            Err(AuthError::Rule5_4_2_InviteSenderNotJoined)
        ));
    }

    #[test]
    fn rule_5_4_3_invite_already_joined_rejected() {
        let create = create_event("@alice:example.org", &[]);
        let alice_join = member_event(
            "@alice:example.org",
            "@alice:example.org",
            "join",
            "$aj:example.org",
            &[],
            json!({}),
        );
        let bob_joined = member_event(
            "@alice:example.org",
            "@bob:example.org",
            "join",
            "$bj:example.org",
            &[],
            json!({}),
        );
        let st = state_map([create, alice_join, bob_joined]);
        let reinvite = member_event(
            "@alice:example.org",
            "@bob:example.org",
            "invite",
            "$reinv:example.org",
            &[],
            json!({}),
        );
        assert!(matches!(
            check_auth_rules(&reinvite, &st),
            Err(AuthError::Rule5_4_3_InviteTargetJoinedOrBanned)
        ));
    }

    // ---------- rule 5.5: leave / kick ----------

    #[test]
    fn rule_5_5_1_self_leave_from_join_allowed() {
        let create = create_event("@alice:example.org", &[]);
        let alice_join = member_event(
            "@alice:example.org",
            "@alice:example.org",
            "join",
            "$aj:example.org",
            &[],
            json!({}),
        );
        let st = state_map([create, alice_join]);
        let self_leave = member_event(
            "@alice:example.org",
            "@alice:example.org",
            "leave",
            "$lv:example.org",
            &[],
            json!({}),
        );
        check_auth_rules(&self_leave, &st).expect("self-leave from join");
    }

    #[test]
    fn rule_5_5_1_self_leave_when_not_in_room_rejected() {
        let create = create_event("@alice:example.org", &[]);
        let st = state_map([create]);
        let self_leave = member_event(
            "@bob:example.org",
            "@bob:example.org",
            "leave",
            "$lv:example.org",
            &[],
            json!({}),
        );
        assert!(matches!(
            check_auth_rules(&self_leave, &st),
            Err(AuthError::Rule5_5_1_SelfLeaveInvalid)
        ));
    }

    #[test]
    fn rule_5_5_creator_kick_allowed() {
        let create = create_event("@alice:example.org", &[]);
        let alice_join = member_event(
            "@alice:example.org",
            "@alice:example.org",
            "join",
            "$aj:example.org",
            &[],
            json!({}),
        );
        let bob_joined = member_event(
            "@alice:example.org",
            "@bob:example.org",
            "join",
            "$bj:example.org",
            &[],
            json!({}),
        );
        let st = state_map([create, alice_join, bob_joined]);
        let kick = member_event(
            "@alice:example.org",
            "@bob:example.org",
            "leave",
            "$kick:example.org",
            &[],
            json!({}),
        );
        check_auth_rules(&kick, &st).expect("creator can kick");
    }

    // ---------- rule 5.6: ban ----------

    #[test]
    fn rule_5_6_creator_ban_allowed() {
        let create = create_event("@alice:example.org", &[]);
        let alice_join = member_event(
            "@alice:example.org",
            "@alice:example.org",
            "join",
            "$aj:example.org",
            &[],
            json!({}),
        );
        let bob_joined = member_event(
            "@alice:example.org",
            "@bob:example.org",
            "join",
            "$bj:example.org",
            &[],
            json!({}),
        );
        let st = state_map([create, alice_join, bob_joined]);
        let ban = member_event(
            "@alice:example.org",
            "@bob:example.org",
            "ban",
            "$ban:example.org",
            &[],
            json!({}),
        );
        check_auth_rules(&ban, &st).expect("creator can ban");
    }

    #[test]
    fn rule_5_6_1_non_joined_cannot_ban() {
        let create = create_event("@alice:example.org", &[]);
        let st = state_map([create]);
        let ban = member_event(
            "@bob:example.org",
            "@carol:example.org",
            "ban",
            "$ban:example.org",
            &[],
            json!({}),
        );
        assert!(matches!(
            check_auth_rules(&ban, &st),
            Err(AuthError::Rule5_6_1_BanSenderNotJoined)
        ));
    }

    // ---------- rule 5.7: knock ----------

    #[test]
    fn rule_5_7_1_knock_on_non_knockable_room_rejected() {
        let create = create_event("@alice:example.org", &[]);
        let jr = join_rules_event("invite", "$jr:example.org");
        let st = state_map([create, jr]);
        let knock = member_event(
            "@bob:example.org",
            "@bob:example.org",
            "knock",
            "$knock:example.org",
            &[],
            json!({}),
        );
        assert!(matches!(
            check_auth_rules(&knock, &st),
            Err(AuthError::Rule5_7_1_KnockNotKnockable)
        ));
    }

    #[test]
    fn rule_5_7_knock_on_knockable_room_allowed() {
        let create = create_event("@alice:example.org", &[]);
        let jr = join_rules_event("knock", "$jr:example.org");
        let st = state_map([create, jr]);
        let knock = member_event(
            "@bob:example.org",
            "@bob:example.org",
            "knock",
            "$knock:example.org",
            &[],
            json!({}),
        );
        check_auth_rules(&knock, &st).expect("knock on knockable room");
    }

    // ---------- rule 5.8: unknown membership ----------

    #[test]
    fn rule_5_8_unknown_membership_rejected() {
        let create = create_event("@alice:example.org", &[]);
        let st = state_map([create]);
        let weird = member_event(
            "@alice:example.org",
            "@alice:example.org",
            "lurking",
            "$w:example.org",
            &[],
            json!({}),
        );
        assert!(matches!(
            check_auth_rules(&weird, &st),
            Err(AuthError::Rule5_8_UnknownMembership(m)) if m == "lurking"
        ));
    }

    // ---------- rule 6: sender joined ----------

    #[test]
    fn rule_6_sender_not_joined_rejected() {
        let create = create_event("@alice:example.org", &[]);
        let st = state_map([create]);
        // @bob is not a member; tries to send a message.
        let msg = message_event("@bob:example.org", "$m:example.org");
        assert!(matches!(
            check_auth_rules(&msg, &st),
            Err(AuthError::Rule6_SenderNotJoined)
        ));
    }

    #[test]
    fn rule_6_joined_sender_allowed() {
        let create = create_event("@alice:example.org", &[]);
        let alice_join = member_event(
            "@alice:example.org",
            "@alice:example.org",
            "join",
            "$aj:example.org",
            &[],
            json!({}),
        );
        let st = state_map([create, alice_join]);
        let msg = message_event("@alice:example.org", "$m:example.org");
        check_auth_rules(&msg, &st).expect("joined sender");
    }

    // ---------- rule 8: required power ----------

    #[test]
    fn rule_8_state_event_below_state_default_rejected() {
        let create = create_event("@alice:example.org", &[]);
        let alice_join = member_event(
            "@alice:example.org",
            "@alice:example.org",
            "join",
            "$aj:example.org",
            &[],
            json!({}),
        );
        let bob_join = member_event(
            "@bob:example.org",
            "@bob:example.org",
            "join",
            "$bj:example.org",
            &[],
            json!({}),
        );
        // Power levels with bob at 0 (below state_default=50).
        let pl = power_levels_event(
            "@alice:example.org",
            json!({ "users": { "@alice:example.org": 100 }, "state_default": 50 }),
            "$pl:example.org",
        );
        let st = state_map([create, alice_join, bob_join, pl]);
        // bob tries to set a topic (state event, requires state_default = 50).
        let topic = state_event(
            "m.room.topic",
            "",
            "@bob:example.org",
            json!({ "topic": "spam" }),
            "$topic:example.org",
        );
        assert!(matches!(
            check_auth_rules(&topic, &st),
            Err(AuthError::Rule8_RequiredPowerInsufficient { .. })
        ));
    }

    // ---------- rule 10: power_levels ----------

    #[test]
    fn rule_10_5_first_power_levels_allowed() {
        // No previous m.room.power_levels in state → terminal allow.
        // Don't list alice (creator) in users — that would trip 10.4 first.
        let create = create_event("@alice:example.org", &[]);
        let alice_join = member_event(
            "@alice:example.org",
            "@alice:example.org",
            "join",
            "$aj:example.org",
            &[],
            json!({}),
        );
        let st = state_map([create, alice_join]);
        let pl = power_levels_event(
            "@alice:example.org",
            json!({ "users_default": 0, "state_default": 50 }),
            "$pl:example.org",
        );
        check_auth_rules(&pl, &st).expect("first power_levels event");
    }

    #[test]
    fn rule_10_4_listing_creator_rejected() {
        let create = create_event("@alice:example.org", &[]);
        let alice_join = member_event(
            "@alice:example.org",
            "@alice:example.org",
            "join",
            "$aj:example.org",
            &[],
            json!({}),
        );
        let st = state_map([create, alice_join]);
        let pl = power_levels_event(
            "@alice:example.org",
            // Alice IS a creator; rule 10.4 forbids naming creators in users.
            json!({ "users": { "@alice:example.org": 100 } }),
            "$pl:example.org",
        );
        // 10.4 (creator-in-users) is checked before 10.5 (first-PL allow),
        // so this rejects even with no prior power_levels event in state.
        assert!(matches!(
            check_auth_rules(&pl, &st),
            Err(AuthError::Rule10_4_CreatorInUsers(_))
        ));
    }

    // ---------- rule 5.3.5: restricted joins ----------

    /// Build a `m.room.join_rules` event of type `restricted` (no `allow`
    /// entries — auth checks only verify the authoriser, not allow rules,
    /// matching synapse's `event_auth.py`).
    fn restricted_join_rules(id: &str) -> Arc<Event> {
        state_event(
            "m.room.join_rules",
            "",
            "@alice:example.org",
            json!({ "join_rule": "restricted", "allow": [] }),
            id,
        )
    }

    #[test]
    fn rule_5_3_5_restricted_join_with_valid_authoriser_allowed() {
        let create = create_event("@alice:example.org", &[]);
        let alice_join = member_event(
            "@alice:example.org",
            "@alice:example.org",
            "join",
            "$aj:example.org",
            &[],
            json!({}),
        );
        let jr = restricted_join_rules("$jr:example.org");
        let st = state_map([create, alice_join, jr]);
        // Alice (creator → infinite power, joined) authorises bob's join.
        let join = member_event(
            "@bob:example.org",
            "@bob:example.org",
            "join",
            "$bj:example.org",
            &[],
            json!({ "join_authorised_via_users_server": "@alice:example.org" }),
        );
        check_auth_rules(&join, &st).expect("restricted join with valid authoriser");
    }

    #[test]
    fn rule_5_3_5_restricted_join_missing_authoriser_field_rejected() {
        let create = create_event("@alice:example.org", &[]);
        let jr = restricted_join_rules("$jr:example.org");
        let st = state_map([create, jr]);
        let join = member_event(
            "@bob:example.org",
            "@bob:example.org",
            "join",
            "$bj:example.org",
            &[],
            // No `join_authorised_via_users_server` field.
            json!({}),
        );
        assert!(matches!(
            check_auth_rules(&join, &st),
            Err(AuthError::Rule5_3_5_RestrictedAuthoriserInvalid)
        ));
    }

    #[test]
    fn rule_5_3_5_restricted_join_authoriser_not_joined_rejected() {
        let create = create_event("@alice:example.org", &[]);
        let jr = restricted_join_rules("$jr:example.org");
        let st = state_map([create, jr]);
        let join = member_event(
            "@bob:example.org",
            "@bob:example.org",
            "join",
            "$bj:example.org",
            &[],
            // Carol exists in user-id-space but has no member event in state.
            json!({ "join_authorised_via_users_server": "@carol:example.org" }),
        );
        assert!(matches!(
            check_auth_rules(&join, &st),
            Err(AuthError::Rule5_3_5_RestrictedAuthoriserInvalid)
        ));
    }

    #[test]
    fn rule_5_3_5_restricted_join_authoriser_below_invite_level_rejected() {
        let create = create_event("@alice:example.org", &[]);
        let alice_join = member_event(
            "@alice:example.org",
            "@alice:example.org",
            "join",
            "$aj:example.org",
            &[],
            json!({}),
        );
        let carol_join = member_event(
            "@alice:example.org",
            "@carol:example.org",
            "join",
            "$cj:example.org",
            &[],
            json!({}),
        );
        let jr = restricted_join_rules("$jr:example.org");
        // Carol joined (users_default=0) but invite level is 50.
        let pl = power_levels_event(
            "@alice:example.org",
            json!({ "invite": 50, "users_default": 0 }),
            "$pl:example.org",
        );
        let st = state_map([create, alice_join, carol_join, jr, pl]);
        let join = member_event(
            "@bob:example.org",
            "@bob:example.org",
            "join",
            "$bj:example.org",
            &[],
            json!({ "join_authorised_via_users_server": "@carol:example.org" }),
        );
        assert!(matches!(
            check_auth_rules(&join, &st),
            Err(AuthError::Rule5_3_5_RestrictedAuthoriserInvalid)
        ));
    }

    // ---------- rule 5.4.1: third-party invite path ----------

    /// Build a stored `m.room.third_party_invite` state event with the given
    /// token, optionally carrying a `public_key`. Sender is alice for these
    /// fixtures so the rule 5.4.1 sender-match check succeeds against an
    /// alice-sent invite.
    fn third_party_invite_state_event(token: &str, with_public_key: bool, id: &str) -> Arc<Event> {
        let mut content = json!({ "display_name": "example" });
        if with_public_key {
            content["public_key"] = json!("base64-key-here");
        }
        state_event(
            "m.room.third_party_invite",
            token,
            "@alice:example.org",
            content,
            id,
        )
    }

    fn tpi_member_invite(sender: &str, target: &str, token: &str, id: &str) -> Arc<Event> {
        member_event(
            sender,
            target,
            "invite",
            id,
            &[],
            json!({
                "third_party_invite": {
                    "display_name": "example",
                    "signed": {
                        "mxid": target,
                        "token": token,
                        // signatures omitted per trusted-network policy.
                    }
                }
            }),
        )
    }

    #[test]
    fn rule_5_4_1_third_party_invite_with_matching_invite_allowed() {
        let create = create_event("@alice:example.org", &[]);
        let alice_join = member_event(
            "@alice:example.org",
            "@alice:example.org",
            "join",
            "$aj:example.org",
            &[],
            json!({}),
        );
        let tpi = third_party_invite_state_event("token-abc", true, "$tpi:example.org");
        let st = state_map([create, alice_join, tpi]);
        let invite = tpi_member_invite(
            "@alice:example.org",
            "@bob:example.org",
            "token-abc",
            "$inv:example.org",
        );
        check_auth_rules(&invite, &st).expect("third-party invite with matching tpi allowed");
    }

    #[test]
    fn rule_5_4_1_third_party_invite_missing_tpi_event_rejected() {
        let create = create_event("@alice:example.org", &[]);
        let alice_join = member_event(
            "@alice:example.org",
            "@alice:example.org",
            "join",
            "$aj:example.org",
            &[],
            json!({}),
        );
        // No m.room.third_party_invite event in state.
        let st = state_map([create, alice_join]);
        let invite = tpi_member_invite(
            "@alice:example.org",
            "@bob:example.org",
            "token-missing",
            "$inv:example.org",
        );
        assert!(matches!(
            check_auth_rules(&invite, &st),
            Err(AuthError::Rule5_4_1_ThirdPartyInviteInvalid(_))
        ));
    }

    #[test]
    fn rule_5_4_1_third_party_invite_mxid_mismatch_rejected() {
        let create = create_event("@alice:example.org", &[]);
        let alice_join = member_event(
            "@alice:example.org",
            "@alice:example.org",
            "join",
            "$aj:example.org",
            &[],
            json!({}),
        );
        let tpi = third_party_invite_state_event("token-abc", true, "$tpi:example.org");
        let st = state_map([create, alice_join, tpi]);
        // Member event's state_key (target) is bob, but `signed.mxid` is carol.
        let invite = member_event(
            "@alice:example.org",
            "@bob:example.org",
            "invite",
            "$inv:example.org",
            &[],
            json!({
                "third_party_invite": {
                    "signed": {
                        "mxid": "@carol:example.org",
                        "token": "token-abc",
                    }
                }
            }),
        );
        assert!(matches!(
            check_auth_rules(&invite, &st),
            Err(AuthError::Rule5_4_1_ThirdPartyInviteInvalid(_))
        ));
    }

    #[test]
    fn rule_5_4_1_third_party_invite_no_public_key_rejected() {
        let create = create_event("@alice:example.org", &[]);
        let alice_join = member_event(
            "@alice:example.org",
            "@alice:example.org",
            "join",
            "$aj:example.org",
            &[],
            json!({}),
        );
        let tpi = third_party_invite_state_event("token-abc", false, "$tpi:example.org");
        let st = state_map([create, alice_join, tpi]);
        let invite = tpi_member_invite(
            "@alice:example.org",
            "@bob:example.org",
            "token-abc",
            "$inv:example.org",
        );
        assert!(matches!(
            check_auth_rules(&invite, &st),
            Err(AuthError::Rule5_4_1_ThirdPartyInviteInvalid(_))
        ));
    }

    // ---------- rule 7: m.room.third_party_invite event ----------

    #[test]
    fn rule_7_creator_can_send_third_party_invite() {
        let create = create_event("@alice:example.org", &[]);
        let alice_join = member_event(
            "@alice:example.org",
            "@alice:example.org",
            "join",
            "$aj:example.org",
            &[],
            json!({}),
        );
        let st = state_map([create, alice_join]);
        let tpi_event = state_event(
            "m.room.third_party_invite",
            "token-xyz",
            "@alice:example.org",
            json!({ "display_name": "example", "public_key": "base64" }),
            "$tpi:example.org",
        );
        check_auth_rules(&tpi_event, &st).expect("creator (infinite power) ≥ invite level");
    }

    #[test]
    fn rule_7_low_power_sender_rejected() {
        let create = create_event("@alice:example.org", &[]);
        let alice_join = member_event(
            "@alice:example.org",
            "@alice:example.org",
            "join",
            "$aj:example.org",
            &[],
            json!({}),
        );
        let bob_join = member_event(
            "@bob:example.org",
            "@bob:example.org",
            "join",
            "$bj:example.org",
            &[],
            json!({}),
        );
        // Bob has users_default=0, invite level=50.
        let pl = power_levels_event(
            "@alice:example.org",
            json!({ "invite": 50, "users_default": 0 }),
            "$pl:example.org",
        );
        let st = state_map([create, alice_join, bob_join, pl]);
        let tpi_event = state_event(
            "m.room.third_party_invite",
            "token-xyz",
            "@bob:example.org",
            json!({ "display_name": "example", "public_key": "base64" }),
            "$tpi:example.org",
        );
        assert!(matches!(
            check_auth_rules(&tpi_event, &st),
            Err(AuthError::Rule7_ThirdPartyInviteInsufficient { .. })
        ));
    }

    // ---------- rule 10: non-creator PL sender reject paths ----------

    #[test]
    fn rule_10_6_1_lowering_default_above_sender_rejected() {
        // Old PL has `ban=80` and `state_default=0` so Bob (power 50) can
        // send a PL event in the first place (rule 8). Bob tries to lower
        // `ban` to 40 — current value (80) > sender (50) → 10.6.1.
        let create = create_event("@alice:example.org", &[]);
        let alice_join = member_event(
            "@alice:example.org",
            "@alice:example.org",
            "join",
            "$aj:example.org",
            &[],
            json!({}),
        );
        let bob_join = member_event(
            "@bob:example.org",
            "@bob:example.org",
            "join",
            "$bj:example.org",
            &[],
            json!({}),
        );
        let pl1 = power_levels_event(
            "@alice:example.org",
            json!({
                "users": { "@bob:example.org": 50 },
                "ban": 80,
                "state_default": 0,
            }),
            "$pl1:example.org",
        );
        let st = state_map([create, alice_join, bob_join, pl1]);
        let pl2 = power_levels_event(
            "@bob:example.org",
            json!({
                "users": { "@bob:example.org": 50 },
                "ban": 40,
                "state_default": 0,
            }),
            "$pl2:example.org",
        );
        assert!(matches!(
            check_auth_rules(&pl2, &st),
            Err(AuthError::Rule10_6_1_CurrentDefaultAboveSender { .. })
        ));
    }

    #[test]
    fn rule_10_7_altering_event_entry_above_sender_rejected() {
        // events["m.room.tombstone"] = 100 in old PL; Bob (power 50) tries to
        // lower it to 50. Current (100) > sender (50) → 10.7.
        let create = create_event("@alice:example.org", &[]);
        let alice_join = member_event(
            "@alice:example.org",
            "@alice:example.org",
            "join",
            "$aj:example.org",
            &[],
            json!({}),
        );
        let bob_join = member_event(
            "@bob:example.org",
            "@bob:example.org",
            "join",
            "$bj:example.org",
            &[],
            json!({}),
        );
        let pl1 = power_levels_event(
            "@alice:example.org",
            json!({
                "users": { "@bob:example.org": 50 },
                "events": { "m.room.tombstone": 100 },
                "state_default": 0,
            }),
            "$pl1:example.org",
        );
        let st = state_map([create, alice_join, bob_join, pl1]);
        let pl2 = power_levels_event(
            "@bob:example.org",
            json!({
                "users": { "@bob:example.org": 50 },
                "events": { "m.room.tombstone": 50 },
                "state_default": 0,
            }),
            "$pl2:example.org",
        );
        assert!(matches!(
            check_auth_rules(&pl2, &st),
            Err(AuthError::Rule10_7_CurrentEventEntryAboveSender { .. })
        ));
    }

    #[test]
    fn rule_10_9_demoting_peer_at_sender_power_rejected() {
        // Bob at 50 tries to demote Carol (also at 50) to 0. Rule 10.9
        // requires current value < sender (strict) for non-self entries.
        let create = create_event("@alice:example.org", &[]);
        let alice_join = member_event(
            "@alice:example.org",
            "@alice:example.org",
            "join",
            "$aj:example.org",
            &[],
            json!({}),
        );
        let bob_join = member_event(
            "@bob:example.org",
            "@bob:example.org",
            "join",
            "$bj:example.org",
            &[],
            json!({}),
        );
        let pl1 = power_levels_event(
            "@alice:example.org",
            json!({
                "users": { "@bob:example.org": 50, "@carol:example.org": 50 },
                "state_default": 0,
            }),
            "$pl1:example.org",
        );
        let st = state_map([create, alice_join, bob_join, pl1]);
        let pl2 = power_levels_event(
            "@bob:example.org",
            json!({
                "users": { "@bob:example.org": 50, "@carol:example.org": 0 },
                "state_default": 0,
            }),
            "$pl2:example.org",
        );
        assert!(matches!(
            check_auth_rules(&pl2, &st),
            Err(AuthError::Rule10_9_CurrentUsersEntryAtOrAboveSender { .. })
        ));
    }

    #[test]
    fn rule_10_8_explicit_at_default_is_an_addition_synapse_parity() {
        // Old PL has events_default=30 and no `events["m.room.message"]`
        // entry. New PL adds `events["m.room.message"] = 30`.
        //
        // Synapse `event_auth.py::_check_power_levels` uses raw-presence
        // comparison: old=None, new=Some(30) is *not* unchanged, so it falls
        // through to the 10.8 power check — new (30) > sender (25) → reject.
        // We match.
        let create = create_event("@alice:example.org", &[]);
        let alice_join = member_event(
            "@alice:example.org",
            "@alice:example.org",
            "join",
            "$aj:example.org",
            &[],
            json!({}),
        );
        let bob_join = member_event(
            "@bob:example.org",
            "@bob:example.org",
            "join",
            "$bj:example.org",
            &[],
            json!({}),
        );
        // Bob at power 25, below the events_default of 30. `state_default=0`
        // so Bob clears rule 8 to even reach rule 10.
        let pl1 = power_levels_event(
            "@alice:example.org",
            json!({
                "users": { "@bob:example.org": 25 },
                "events_default": 30,
                "state_default": 0,
            }),
            "$pl1:example.org",
        );
        let st = state_map([create, alice_join, bob_join, pl1]);
        let pl2 = power_levels_event(
            "@bob:example.org",
            json!({
                "users": { "@bob:example.org": 25 },
                "events_default": 30,
                "state_default": 0,
                "events": { "m.room.message": 30 },
            }),
            "$pl2:example.org",
        );
        assert!(matches!(
            check_auth_rules(&pl2, &st),
            Err(AuthError::Rule10_8_NewEventEntryAboveSender { .. })
        ));
    }

    // ---------- creator-vs-creator interactions ----------

    #[test]
    fn rule_10_4_additional_creator_in_users_rejected() {
        // Bob is an additional creator. Alice's PL event listing Bob in
        // `users` must trip 10.4 just like listing Alice (the sender-creator)
        // does — additional creators are treated identically by the rule.
        let create = create_event("@alice:example.org", &["@bob:example.org"]);
        let alice_join = member_event(
            "@alice:example.org",
            "@alice:example.org",
            "join",
            "$aj:example.org",
            &[],
            json!({}),
        );
        let st = state_map([create, alice_join]);
        let pl = power_levels_event(
            "@alice:example.org",
            json!({ "users": { "@bob:example.org": 100 } }),
            "$pl:example.org",
        );
        let err = check_auth_rules(&pl, &st).expect_err("10.4 fires on additional creator");
        match err {
            AuthError::Rule10_4_CreatorInUsers(uid) => {
                assert_eq!(uid.as_str(), "@bob:example.org");
            }
            other => panic!("expected Rule10_4_CreatorInUsers(@bob), got {other:?}"),
        }
    }

    #[test]
    fn creator_cannot_kick_another_creator() {
        // Both Alice and Bob are creators (Bob via additional_creators);
        // both have i64::MAX power. Rule 5.5.4 requires target < sender —
        // i64::MAX < i64::MAX is false, so the kick falls through to 5.5.5.
        let create = create_event("@alice:example.org", &["@bob:example.org"]);
        let alice_join = member_event(
            "@alice:example.org",
            "@alice:example.org",
            "join",
            "$aj:example.org",
            &[],
            json!({}),
        );
        let bob_join = member_event(
            "@bob:example.org",
            "@bob:example.org",
            "join",
            "$bj:example.org",
            &[],
            json!({}),
        );
        let st = state_map([create, alice_join, bob_join]);
        let kick = member_event(
            "@alice:example.org",
            "@bob:example.org",
            "leave",
            "$kick:example.org",
            &[],
            json!({}),
        );
        assert!(matches!(
            check_auth_rules(&kick, &st),
            Err(AuthError::Rule5_5_5_KickNotAllowed { .. })
        ));
    }

    #[test]
    fn creator_can_invite_another_creator() {
        // Inviting an additional creator is fine — invite has no
        // target-outranks-sender check, only sender ≥ invite_level.
        let create = create_event("@alice:example.org", &["@bob:example.org"]);
        let alice_join = member_event(
            "@alice:example.org",
            "@alice:example.org",
            "join",
            "$aj:example.org",
            &[],
            json!({}),
        );
        let st = state_map([create, alice_join]);
        let invite = member_event(
            "@alice:example.org",
            "@bob:example.org",
            "invite",
            "$inv:example.org",
            &[],
            json!({}),
        );
        check_auth_rules(&invite, &st).expect("creator can invite additional creator");
    }

    #[test]
    fn rule_5_5_5_carries_diagnostic_payload() {
        // Bob (power 0) tries to kick Carol (power 0) — sender < kick_level
        // and target == sender; both conjuncts fail. The payload must
        // surface both values plus the level.
        let create = create_event("@alice:example.org", &[]);
        let alice_join = member_event(
            "@alice:example.org",
            "@alice:example.org",
            "join",
            "$aj:example.org",
            &[],
            json!({}),
        );
        let bob_join = member_event(
            "@bob:example.org",
            "@bob:example.org",
            "join",
            "$bj:example.org",
            &[],
            json!({}),
        );
        let carol_join = member_event(
            "@alice:example.org",
            "@carol:example.org",
            "join",
            "$cj:example.org",
            &[],
            json!({}),
        );
        let pl = power_levels_event(
            "@alice:example.org",
            json!({ "kick": 50, "users_default": 0 }),
            "$pl:example.org",
        );
        let st = state_map([create, alice_join, bob_join, carol_join, pl]);
        let kick = member_event(
            "@bob:example.org",
            "@carol:example.org",
            "leave",
            "$kick:example.org",
            &[],
            json!({}),
        );
        match check_auth_rules(&kick, &st) {
            Err(AuthError::Rule5_5_5_KickNotAllowed {
                sender,
                target,
                kick_level,
            }) => {
                assert_eq!(sender, 0);
                assert_eq!(target, 0);
                assert_eq!(kick_level, 50);
            }
            other => panic!("expected Rule5_5_5_KickNotAllowed with payload, got {other:?}"),
        }
    }

    // ---------- InvalidMemberStateKey ----------

    #[test]
    fn invalid_member_state_key_on_invite_rejected() {
        let create = create_event("@alice:example.org", &[]);
        let alice_join = member_event(
            "@alice:example.org",
            "@alice:example.org",
            "join",
            "$aj:example.org",
            &[],
            json!({}),
        );
        let st = state_map([create, alice_join]);
        let invite = member_event(
            "@alice:example.org",
            "not-a-user-id",
            "invite",
            "$inv:example.org",
            &[],
            json!({}),
        );
        assert!(matches!(
            check_auth_rules(&invite, &st),
            Err(AuthError::InvalidMemberStateKey { .. })
        ));
    }

    #[test]
    fn rule_10_6_2_promote_above_self_rejected() {
        // Alice (creator) sets PL with bob at 50, carol nonexistent. Then bob
        // (power 50) tries to set carol at 100 — exceeds bob's level.
        let create = create_event("@alice:example.org", &[]);
        let alice_join = member_event(
            "@alice:example.org",
            "@alice:example.org",
            "join",
            "$aj:example.org",
            &[],
            json!({}),
        );
        let bob_join = member_event(
            "@bob:example.org",
            "@bob:example.org",
            "join",
            "$bj:example.org",
            &[],
            json!({}),
        );
        let pl1 = power_levels_event(
            "@alice:example.org",
            json!({ "users": { "@bob:example.org": 50 }, "users_default": 0, "state_default": 0 }),
            "$pl1:example.org",
        );
        let st = state_map([create, alice_join, bob_join, pl1]);
        let pl2 = power_levels_event(
            "@bob:example.org",
            json!({ "users": { "@bob:example.org": 50, "@carol:example.org": 100 }, "users_default": 0, "state_default": 0 }),
            "$pl2:example.org",
        );
        assert!(matches!(
            check_auth_rules(&pl2, &st),
            Err(AuthError::Rule10_10_NewUsersEntryAboveSender { .. })
        ));
    }
}
