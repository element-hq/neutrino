//! Server-authored event construction and inbound-wire parsing.
//!
//! Two public entry points share the same downstream pipeline:
//!
//! - [`EventBuilder::build`] assembles a v12 / MSC4242 PDU, computes the
//!   content hash, inserts it into the event, computes the reference hash,
//!   derives the event_id, and (for `m.room.create`) lets [`parse_event`]
//!   derive the room_id from the event_id. It runs `parse_event` (wire
//!   format) and then `validate_pdu` (semantic rules) as final
//!   defence-in-depth checks so the bytes we just produced are guaranteed
//!   to round-trip through both validators.
//!
//! - [`from_wire`] is the inbound counterpart: it reads the canonical bytes
//!   as the source of truth, computes the reference hash to derive the
//!   event_id, runs `parse_event`, then `validate_pdu`, and classifies the
//!   outcome — a state-independent auth-rule failure comes back as
//!   [`Wire::Rejected`] (persisted rejected, not dropped at the wire edge),
//!   not an error (see `from_wire`'s docs).
//!
//! See `event-id-design.md` §"Updated `EventBuilder`".

use std::time::{SystemTime, UNIX_EPOCH};

use crate::event_id::{
    ContentHashCheck, EventIdError, b64_unpadded, check_content_hash, content_hash,
    redact_to_canonical_bytes,
};
use crate::room_version::RoomVersion;
use ruma::canonical_json::{CanonicalJsonObject, CanonicalJsonValue, try_from_json_map};
use ruma::{OwnedEventId, OwnedRoomId, OwnedUserId};
use serde::Serialize;
use serde_json::value::{RawValue, to_raw_value};
use serde_json::{Map, Value};

use crate::validate::{SemanticVerdict, parse_event, semantic_verdict, validate_pdu};
use crate::{Event, FormatError};

/// Builder for server-authored Matrix v12 PDUs.
///
/// `new` takes the strictly-required fields (`sender`, `type`, and the room
/// version the event is named under); everything else has a sensible default
/// applied at `build()` time. Setters consume and return `Self` for chaining.
///
/// **For `m.room.create` events**: do not set `room_id` (it's derived from
/// the computed event_id post-hash). The `state_key` must be `""`.
///
/// **For all other events**: `room_id` must be set, otherwise `build()`
/// returns `FormatError::MissingField("room_id")`.
#[derive(Debug, Clone)]
pub struct EventBuilder {
    sender: OwnedUserId,
    event_type: String,
    state_key: Option<String>,
    content: Value,
    room_id: Option<OwnedRoomId>,
    prev_events: Vec<OwnedEventId>,
    prev_state_events: Vec<OwnedEventId>,
    auth_events: Vec<OwnedEventId>,
    origin_server_ts: Option<u64>,
    unsigned: Option<Value>,
    signer: Option<std::sync::Arc<crate::sign::EventSigner>>,
    version: std::sync::Arc<RoomVersion>,
}

impl EventBuilder {
    /// Start a new builder for an event in a room of `version` — whose rules
    /// supply the id derivation, the identity-field stamping and the redaction
    /// keep-list. There is no default: an event cannot be named without its
    /// room's version, and guessing one invents a different event.
    ///
    /// Other defaults: `content` = `{}`, `origin_server_ts` = `now_ms()`
    /// (applied at `build()` time), all parent lists empty, `state_key` /
    /// `room_id` / `unsigned` absent.
    pub fn new(
        sender: OwnedUserId,
        event_type: String,
        version: std::sync::Arc<RoomVersion>,
    ) -> Self {
        Self {
            sender,
            event_type,
            state_key: None,
            content: Value::Object(Map::new()),
            room_id: None,
            prev_events: Vec::new(),
            prev_state_events: Vec::new(),
            auth_events: Vec::new(),
            origin_server_ts: None,
            unsigned: None,
            signer: None,
            version,
        }
    }

    pub fn state_key(mut self, state_key: String) -> Self {
        self.state_key = Some(state_key);
        self
    }

    /// Set the event `content`. Must serialise to a JSON object — non-object
    /// content is rejected at `build()` time with `InvalidFieldType`.
    pub fn content<T: Serialize>(mut self, content: T) -> Self {
        // A failed `to_value` here turns into a non-object Value which is
        // caught by `build()` rather than producing a spurious error from a
        // setter that the caller can't react to.
        self.content = serde_json::to_value(content).unwrap_or(Value::Null);
        self
    }

    pub fn room_id(mut self, room_id: OwnedRoomId) -> Self {
        self.room_id = Some(room_id);
        self
    }

    pub fn prev_events(mut self, ids: Vec<OwnedEventId>) -> Self {
        self.prev_events = ids;
        self
    }

    pub fn prev_state_events(mut self, ids: Vec<OwnedEventId>) -> Self {
        self.prev_state_events = ids;
        self
    }

    /// Set the server-calculated `auth_events` list (MSC4242: not on the
    /// wire). The caller computes this against state-before-event via
    /// `auth_events::calculate_auth_events`.
    pub fn auth_events(mut self, ids: Vec<OwnedEventId>) -> Self {
        self.auth_events = ids;
        self
    }

    pub fn origin_server_ts(mut self, ts: u64) -> Self {
        self.origin_server_ts = Some(ts);
        self
    }

    pub fn unsigned<T: Serialize>(mut self, unsigned: T) -> Self {
        self.unsigned = Some(serde_json::to_value(unsigned).unwrap_or(Value::Null));
        self
    }

    /// Sign the built event with this server's key (signed
    /// deployments; `None` — the default — matches the trusted-network mode,
    /// where events MUST NOT carry signatures). The signature is computed
    /// over the redacted canonical form, so it does not affect the reference
    /// hash or the event id; the signed `signatures` block rides `raw`.
    ///
    /// This also decides whether the event carries a content hash: `hashes`
    /// and `signatures` are two halves of one fact (cryptographic event
    /// integrity), so a signerless build emits neither — see `build`. It does
    /// therefore affect the event id, which covers `hashes`.
    pub fn signer(mut self, signer: Option<std::sync::Arc<crate::sign::EventSigner>>) -> Self {
        self.signer = signer;
        self
    }

    pub fn build(self) -> Result<Event, FormatError> {
        let is_create = self.event_type == "m.room.create";

        // Skeleton: non-create needs a room_id. (Create derives its own from
        // the event_id post-hash; any `.room_id(...)` on a create builder is
        // ignored and would in any case be rejected by `parse_event` if we
        // tried to inject it.)
        if !is_create && self.room_id.is_none() {
            return Err(FormatError::MissingField("room_id"));
        }
        // Skeleton: content must be a JSON object (v12 PDU schema).
        if !self.content.is_object() {
            return Err(FormatError::InvalidFieldType {
                field: "content",
                expected: "object",
            });
        }
        // Skeleton: unsigned, if set, must be a JSON object.
        if let Some(u) = &self.unsigned
            && !u.is_object()
        {
            return Err(FormatError::InvalidFieldType {
                field: "unsigned",
                expected: "object",
            });
        }

        // Assemble the unhashed JSON map: type, sender, content, prev_events,
        // prev_state_events, origin_server_ts, [state_key], [room_id],
        // [unsigned]. `room_id` is omitted iff this is a create event;
        // `auth_events` is struct-only and never appears in the raw (MSC4242).
        let mut map = Map::new();
        map.insert("type".to_owned(), Value::String(self.event_type.clone()));
        map.insert(
            "sender".to_owned(),
            Value::String(self.sender.as_str().to_owned()),
        );
        map.insert("content".to_owned(), self.content);
        map.insert(
            "prev_events".to_owned(),
            Value::Array(
                self.prev_events
                    .iter()
                    .map(|e| Value::String(e.as_str().to_owned()))
                    .collect(),
            ),
        );
        map.insert(
            "prev_state_events".to_owned(),
            Value::Array(
                self.prev_state_events
                    .iter()
                    .map(|e| Value::String(e.as_str().to_owned()))
                    .collect(),
            ),
        );
        let origin_server_ts = self.origin_server_ts.unwrap_or_else(now_ms);
        map.insert("origin_server_ts".to_owned(), Value::from(origin_server_ts));
        if let Some(sk) = &self.state_key {
            map.insert("state_key".to_owned(), Value::String(sk.clone()));
        }
        if !is_create {
            let rid = self.room_id.as_ref().expect("room_id checked above");
            map.insert("room_id".to_owned(), Value::String(rid.as_str().to_owned()));
        }
        if let Some(u) = self.unsigned {
            map.insert("unsigned".to_owned(), u);
        }

        // Convert to canonical-JSON. Surfaces float-in-content, out-of-range
        // integers, duplicate keys (impossible from serde Map but the API
        // exposes them) — anything that can't round-trip canonical JSON.
        let mut canon: CanonicalJsonObject =
            try_from_json_map(map).map_err(FormatError::NonCanonical)?;

        let version = self.version;

        // Stamp the version's identity-bearing fields, if its scheme has any.
        // Must precede the content hash and the signature so both cover them;
        // the base version's scheme inserts nothing, so this is a no-op there.
        version
            .stamp(&mut canon)
            .map_err(id_error_to_format_error)?;

        // Content hash → `hashes.sha256` (canonical-base64 standard alphabet
        // per spec). Order: content hash, insert, then reference hash, so the
        // reference hash covers the inserted content hash.
        //
        // Emitted only in signed deployments. The content hash exists solely so
        // the signature — computed over the *redacted* form, which keeps
        // `hashes` — transitively covers redactable content; with no signature
        // to anchor it, a self-attested hash proves nothing and is ~60 bytes of
        // pure overhead on a bandwidth-constrained link. So on a trusted
        // network (`trusted_network = true`, no signer) it is omitted, exactly
        // as signatures are.
        if self.signer.is_some() {
            let ch = content_hash(&canon);
            let mut hashes = CanonicalJsonObject::new();
            hashes.insert(
                "sha256".to_owned(),
                CanonicalJsonValue::String(b64_unpadded(&ch)),
            );
            canon.insert("hashes".to_owned(), CanonicalJsonValue::Object(hashes));
        }

        // Sign (signed deployments): over the redacted canonical
        // form, which strips `signatures` — so the reference hash below (and
        // therefore the event id) is identical signed or unsigned, and the
        // signature covers the just-inserted content hash.
        //
        // Not an `expect`: the builder's own fields satisfy ruma's redaction
        // preconditions, but `stamp` above can have inserted anything, so a
        // scheme is able to break them (a non-object `hashes`, say). A caller
        // bug in a scheme must surface as an error, not a panic.
        if let Some(signer) = &self.signer {
            signer
                .sign_event(&mut canon, &version)
                .map_err(ref_hash_error_to_format_error)?;
        }

        // Derive the event_id — the same function every receiver runs against
        // these bytes, which is what keeps the id a function of the event
        // rather than of who computed it. For the base version this is the
        // reference hash.
        let event_id = version.event_id(&canon).map_err(id_error_to_format_error)?;

        let raw = serialise_canonical(&canon);

        // Defence-in-depth: round-trip through the wire-format validator
        // (parse_event) then the semantic validator (validate_pdu). For
        // create events, parse_event derives room_id from event_id (sigil
        // swap), so we don't need to do that here.
        //
        // Failures here are caller bugs (not builder bugs): the builder
        // doesn't validate every field validate_pdu does — content shape
        // for `m.room.create` / `m.room.member` / `m.room.power_levels` is
        // rule-checked, count limits on `prev_events` /
        // `prev_state_events`, rule 9 (`@`-prefixed state_key vs sender)
        // etc. Bubble the `FormatError` up so the caller sees the specific
        // reason rather than a panic from inside the builder.
        let event = parse_event(raw, event_id, self.auth_events)?;
        validate_pdu(&event, &version)?;
        Ok(event)
    }
}

/// Parse + event-id-derive an inbound wire event.
///
/// Reads `raw` as the source of truth, computes its reference hash to derive
/// the event_id, verifies the content hash, then runs [`parse_event`] for the
/// structured fields (and the create-event room_id derivation). `auth_events`
/// are supplied by the caller because MSC4242 removes them from the wire.
///
/// **Content hash verification (Matrix S2S §"Validating hashes and signatures
/// on received events")**: if `hashes.sha256` doesn't match the recomputed
/// content hash, the event is redacted before being accepted — `raw` is
/// replaced with the canonical redacted form and that's what `parse_event`
/// sees. The event_id is unaffected (it's already computed over the redacted
/// form). The receiving server is expected to accept the redacted version
/// rather than drop the event entirely.
///
/// **Divergence — an event with no `hashes` at all is accepted as-is**, where
/// the spec would have it redacted. A trusted-network deployment
/// (`trusted_network = true`) emits neither signatures nor content hashes, so
/// the receipt check is driven by what the event carries rather than by local
/// policy: hash present ⇒ held to it, hash absent ⇒ nothing to check. That is
/// safe in signed mode too, because the signature covers the redacted form
/// *including* `hashes` — a relay cannot strip the field without invalidating
/// the signature, and an origin that never emitted one is only choosing not to
/// attest content it authored itself.
///
/// Errors:
/// - `raw` is not a JSON object (`InvalidFieldType { field: "<root>" }`).
/// - The object lacks `type` or has malformed `content`/`hashes`/`signatures`
///   shape (mapped via [`ref_hash_error_to_format_error`]).
/// - Any downstream `parse_event` rejection.
/// - Any `validate_pdu` failure classified [`SemanticVerdict::Drop`]
///   (receipt-check 1: size limits, fan-in caps, create rules) — such an
///   event never enters the system.
///
/// A `validate_pdu` failure classified [`SemanticVerdict::Reject`] (a
/// state-independent auth rule: v12 rules 9 / 5.1 / 10.1–10.3) is NOT an
/// error: the spec's verdict for those is *reject*, so the event comes back
/// as [`Wire::Rejected`] with `rejected = true` already set. Every caller is
/// thereby forced to decide what a rejected wire event means at its ingress
/// (persist-as-rejected, refuse the request, fail the join, …) — it is
/// impossible to obtain a malformed `Event` that still claims to be
/// accepted.
///
/// Returns an [`UnverifiedWire`]: parse/classification verdicts are decided
/// here, but the event's **provenance** (signature check when signing is on,
/// faith on a trusted network) is a separate, unskippable admission step —
/// see [`UnverifiedWire`].
///
/// Performance note: this parses `raw` twice — once here to a
/// `CanonicalJsonValue` for hash computation, then again in `parse_event`.
/// Acceptable for the federation receive path's current scale.
pub fn from_wire(
    raw: Box<RawValue>,
    auth_events: Vec<OwnedEventId>,
    version: &std::sync::Arc<RoomVersion>,
) -> Result<UnverifiedWire, FormatError> {
    let parsed: CanonicalJsonValue = serde_json::from_str(raw.get())?;
    let CanonicalJsonValue::Object(obj) = parsed else {
        return Err(FormatError::InvalidFieldType {
            field: "<root>",
            expected: "object",
        });
    };
    // The same derivation the author ran in `build` — never a transmitted id.
    let event_id = version.event_id(&obj).map_err(id_error_to_format_error)?;

    // Replace raw with the canonical redacted form on content-hash mismatch.
    // An event with no `hashes` at all is taken as-is: that is what a
    // trusted-network peer emits (see `EventBuilder::build`), and redacting
    // every such event would empty every message in the mesh.
    // The version's `extra_redaction_keys` are kept, so the redacted bytes can
    // still re-derive this event's id — receipt-check 3 redacts and carries on,
    // so the id must survive it.
    //
    // Not an `expect`: this used to be guaranteed because `reference_hash`
    // had already run the same redaction a few lines above, but an arbitrary
    // `EventIdScheme::derive` need not touch redaction at all, so a malformed
    // `content`/`hashes`/`signatures` can reach here and fail. Attacker-supplied
    // bytes must not panic the ingress.
    let raw_to_parse = match check_content_hash(&obj) {
        ContentHashCheck::Absent | ContentHashCheck::Matches => raw,
        ContentHashCheck::Mismatch => {
            let bytes =
                redact_to_canonical_bytes(&obj, version).map_err(ref_hash_error_to_format_error)?;
            let s = String::from_utf8(bytes).expect("canonical JSON is valid UTF-8");
            RawValue::from_string(s).expect("canonical JSON parses as a RawValue")
        }
    };
    let event = parse_event(raw_to_parse, event_id, auth_events)?;
    let wire = match validate_pdu(&event, version) {
        Ok(()) => Wire::Valid(event),
        Err(e) => match semantic_verdict(&e) {
            SemanticVerdict::Drop => return Err(e),
            SemanticVerdict::Reject => {
                let mut event = event;
                event.rejected = true;
                Wire::Rejected(event, e)
            }
        },
    };
    Ok(UnverifiedWire {
        wire,
        obj,
        version: version.clone(),
    })
}

/// What an inbound event's wire bytes say about which room version names it.
///
/// Naming an event requires its room version, and the version is only knowable
/// from the room — so every ingress must resolve it *before* calling
/// [`from_wire`]. This is the pure half of that resolution: it reads the two
/// fields the answer depends on and nothing else. The store lookup is the
/// caller's, because this crate has no provider dependency by design.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomVersionKeys {
    /// The room this event claims to be in — look its version up in
    /// `rooms.room_version`. Absent on an `m.room.create`, which carries no
    /// `room_id` at all under v12 (the room id is derived from the create's own
    /// event id), and absent on a malformed event, which `from_wire` will
    /// reject once it is named.
    pub room_id: Option<OwnedRoomId>,
    /// `content.room_version` **of an `m.room.create`** — the one case where an
    /// event says its own version. Look it up in the registry: unknown means we
    /// do not speak this room's version and must refuse it. Absent from a
    /// create is permitted (v12 rule 1.3), and means the base version.
    ///
    /// Only ever set for a create, so it cannot be populated alongside
    /// [`room_id`](Self::room_id) by any well-formed event: a `room_version` in
    /// some other event type's content is that content's own business and says
    /// nothing about which version names the event.
    pub declared: Option<String>,
}

/// Read an inbound event's [`RoomVersionKeys`] from its wire bytes.
///
/// Deliberately not a full parse: only `type`, `room_id` and
/// `content.room_version` are deserialised. The `type` gate is not cosmetic —
/// without it any event could volunteer a `content.room_version` and offer the
/// caller two contradictory answers. Every *other* rejection of malformed bytes
/// belongs to [`from_wire`], which runs after the version is resolved (a create
/// that also carries a `room_id`, say, is refused there as
/// [`FormatError::CreateHasRoomId`]) — duplicating those checks here would let
/// the two disagree.
pub fn room_version_keys(raw: &RawValue) -> RoomVersionKeys {
    #[derive(serde::Deserialize)]
    struct Keys {
        r#type: Option<String>,
        room_id: Option<String>,
        content: Option<Content>,
    }
    #[derive(serde::Deserialize)]
    struct Content {
        room_version: Option<String>,
    }
    let Ok(keys) = serde_json::from_str::<Keys>(raw.get()) else {
        return RoomVersionKeys {
            room_id: None,
            declared: None,
        };
    };
    let is_create = keys.r#type.as_deref() == Some("m.room.create");
    RoomVersionKeys {
        room_id: keys.room_id.and_then(|r| r.parse().ok()),
        declared: is_create
            .then(|| keys.content.and_then(|c| c.room_version))
            .flatten(),
    }
}

/// A parsed-and-classified wire event whose **provenance has not been checked
/// yet**. [`from_wire`] only ever produces this; the only ways to reach
/// [`Wire`] are [`UnverifiedWire::admit_on_faith`] (trusted network) and
/// [`UnverifiedWire::verify`] (signature check) — the compiler demands a
/// provenance decision at every ingress, none can forget it. Federation
/// ingress code should not pick a method itself but route through
/// [`EventSecurity::admit`](crate::sign::EventSecurity::admit), which
/// dispatches on the deployment's security configuration.
#[derive(Debug)]
pub struct UnverifiedWire {
    wire: Wire,
    /// The original parsed object, retained for signature verification.
    /// Signatures are stripped from the signed byte string and preserved by
    /// redaction, so verifying against this is equivalent for both the
    /// as-received and the hash-mismatch-redacted event.
    obj: CanonicalJsonObject,
    /// The room version this event was parsed under, captured so
    /// [`verify`](Self::verify) computes the signed byte string under the same
    /// redaction keep-list the id was derived with — without every ingress
    /// having to carry the version a second time.
    version: std::sync::Arc<RoomVersion>,
}

impl UnverifiedWire {
    /// Admit the event on faith — no signature check. Correct ONLY on a
    /// trusted network (`trusted_network = true`: origin claims are accurate
    /// by assumption, and events carry no signatures at all) and in tests.
    /// Otherwise every ingress reaches [`Wire`] through
    /// [`EventSecurity::admit`](crate::sign::EventSecurity::admit) — a bare
    /// `admit_on_faith` in federation ingress code is a review flag.
    ///
    /// Two sanctioned exceptions carry the check elsewhere rather than skip it:
    /// - The `/send` transaction handler parses on faith and **defers** the
    ///   signature check to the inbound worker, which re-admits every staged
    ///   row via [`EventSecurity::admit_wire`](crate::sign::EventSecurity::admit_wire)
    ///   before applying it. The worker is the sole staged→applied authority
    ///   and staged rows are pre-auth (never served), so a bad-signature PDU is
    ///   dropped there and never reaches room state.
    /// - The `make_*` template rebuild takes only the template's DAG pointers
    ///   and re-signs + re-auths the event it builds, so the template's own
    ///   (necessarily absent) signature is irrelevant.
    pub fn admit_on_faith(self) -> Wire {
        self.wire
    }

    /// Admit the event after verifying a signature by its sender's server
    /// (signed deployments). Failure is Drop-class per the S-S receipt
    /// checks: a PDU failing the signature check never enters the system
    /// (unlike a content-hash mismatch, which [`from_wire`] already handled
    /// by redaction).
    pub async fn verify(
        self,
        resolver: &dyn crate::sign::KeyResolver,
    ) -> Result<Wire, FormatError> {
        let origin = self.event().sender.server_name().as_str().to_owned();
        crate::sign::verify_event_signature(&self.obj, &origin, resolver, &self.version)
            .await
            .map_err(|e| FormatError::SignatureCheck(e.to_string()))?;
        Ok(self.wire)
    }

    /// The classified event, for pre-admission inspection (logging, routing).
    /// Deliberately a reference: the owned event is only obtainable through
    /// the admission methods above.
    pub fn event(&self) -> &Event {
        match &self.wire {
            Wire::Valid(ev) | Wire::Rejected(ev, _) => ev,
        }
    }
}

/// A parsed inbound wire event, classified at the parse boundary.
///
/// This is the only way to obtain an `Event` from wire bytes, so the
/// classification cannot be skipped: an event that fails a state-independent
/// auth rule travels as [`Wire::Rejected`] with `rejected = true` baked in —
/// "malformed but accepted" is unrepresentable. Callers that treat both
/// variants the same (staging, the worker) use [`Wire::into_event`]; callers
/// with an ingress-specific policy (fail the join, 400 the invite, persist
/// backfill as rejected) match.
#[derive(Debug)]
pub enum Wire {
    /// Passed every wire-format and semantic check.
    Valid(Event),
    /// Parseable (event_id derivable) but fails a state-independent auth rule
    /// (v12 rules 9 / 5.1 / 10.1–10.3). `Event.rejected` is already `true`;
    /// the error says which rule condemned it.
    Rejected(Event, FormatError),
}

impl Wire {
    /// The event, whatever its verdict. A [`Wire::Rejected`] event comes out
    /// with `rejected = true` set, so collapsing the variants can never
    /// launder a malformed event into an accepted one.
    pub fn into_event(self) -> Event {
        match self {
            Wire::Valid(ev) | Wire::Rejected(ev, _) => ev,
        }
    }

    /// Borrow the event, whatever its verdict.
    pub fn event(&self) -> &Event {
        match self {
            Wire::Valid(ev) | Wire::Rejected(ev, _) => ev,
        }
    }
}

/// Map an [`EventIdScheme`] failure onto the format vocabulary. A redaction
/// precondition is the reference-hash scheme's only failure mode and already
/// has a `FormatError` shape; anything else is scheme-specific and lands on
/// [`FormatError::EventId`] — both are drop-class (an event that cannot be
/// named cannot enter the system).
fn id_error_to_format_error(err: EventIdError) -> FormatError {
    match err {
        EventIdError::Redaction(e) => ref_hash_error_to_format_error(e),
        EventIdError::Scheme(msg) => FormatError::EventId(msg),
    }
}

fn ref_hash_error_to_format_error(err: ruma::canonical_json::RedactionError) -> FormatError {
    use ruma::canonical_json::RedactionError;
    match err {
        RedactionError::MissingField { path } if path == "type" => {
            // ruma's only documented `MissingField` here is the top-level
            // `type` field. Map to FormatError::MissingField for parity with
            // parse_event's vocabulary.
            FormatError::MissingField("type")
        }
        RedactionError::InvalidType { path, .. } => {
            // ruma's path values for redaction preconditions are one of
            // "type" / "content" / "hashes" / "signatures". Map to
            // InvalidFieldType with a static field name.
            let field: &'static str = match path.as_str() {
                "type" => "type",
                "content" => "content",
                "hashes" => "hashes",
                "signatures" => "signatures",
                _ => "<unknown>",
            };
            FormatError::InvalidFieldType {
                field,
                expected: "object",
            }
        }
        // `RedactionError` is `#[non_exhaustive]`; any future variant lands
        // here as a generic wire-malformed signal.
        _ => FormatError::InvalidFieldType {
            field: "<root>",
            expected: "well-formed v12 PDU",
        },
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn serialise_canonical(obj: &CanonicalJsonObject) -> Box<RawValue> {
    to_raw_value(obj).expect("CanonicalJsonObject is always serialisable to RawValue")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::room_version::base_version;
    use serde_json::json;

    fn user(s: &str) -> OwnedUserId {
        s.parse().expect("user id")
    }

    fn room(s: &str) -> OwnedRoomId {
        s.parse().expect("room id")
    }

    fn eid(s: &str) -> OwnedEventId {
        s.parse().expect("event id")
    }

    // ---------- happy path ----------

    #[test]
    fn build_create_event_derives_room_id_from_event_id() {
        let ev = EventBuilder::new(
            user("@alice:example.org"),
            "m.room.create".to_owned(),
            base_version().clone(),
        )
        .state_key(String::new())
        .content(json!({ "room_version": crate::ROOM_VERSION_ID }))
        .origin_server_ts(1_700_000_000_000)
        .build()
        .expect("create event builds");

        // event_id and room_id share their suffix (43 url-safe-b64 chars).
        assert!(ev.event_id.as_str().starts_with('$'));
        assert!(ev.room_id.as_str().starts_with('!'));
        assert_eq!(&ev.event_id.as_str()[1..], &ev.room_id.as_str()[1..]);
        // room_id is NOT in raw (v12 spec: create events omit room_id on the wire).
        let raw: serde_json::Value = serde_json::from_str(ev.raw.get()).unwrap();
        assert!(raw.get("room_id").is_none());
    }

    /// A signed build carries `hashes.sha256` in the spec's encoding;
    /// a trusted-network build (no signer) carries no `hashes` at all — the
    /// hash exists only to be covered by a signature, so the two travel
    /// together. Wire-shape half of the pair; the id consequence is pinned by
    /// `content_hash_presence_changes_the_event_id`.
    #[test]
    fn hashes_present_iff_signed() {
        let (signer, sender) = node_signer_and_user();
        let build = |signer: Option<std::sync::Arc<crate::sign::EventSigner>>| {
            let ev = EventBuilder::new(
                sender.clone(),
                "m.room.message".to_owned(),
                base_version().clone(),
            )
            .room_id(room("!r:d"))
            .content(json!({ "msgtype": "m.text", "body": "hi" }))
            .origin_server_ts(1)
            .signer(signer)
            .build()
            .expect("builds");
            serde_json::from_str::<serde_json::Value>(ev.raw.get()).expect("raw is JSON")
        };

        // Signed: hashes.sha256 is a 43-char standard-alphabet (no `-`/`_`),
        // unpadded base64 string — 32-byte sha256.
        let hash_str = build(Some(signer))["hashes"]["sha256"]
            .as_str()
            .expect("signed build must carry hashes.sha256")
            .to_owned();
        assert_eq!(hash_str.len(), 43);
        assert!(
            !hash_str.contains(['-', '_']),
            "hashes.sha256 must use STANDARD b64 alphabet (`+`/`/`), not url-safe (`-`/`_`): {hash_str}"
        );

        // Trusted network: no `hashes` key on the wire at all (bytes saved).
        let unsigned = build(None);
        assert!(
            unsigned.get("hashes").is_none(),
            "a signerless build must not carry hashes: {unsigned}"
        );
    }

    /// Unlike a signature, the content hash IS covered by the reference hash,
    /// so omitting it moves the event id. Both sides derive the id from the
    /// bytes they actually see, so this is self-consistent — but it means a
    /// deployment cannot change `trusted_network` and expect stable ids.
    #[test]
    fn content_hash_presence_changes_the_event_id() {
        let (signer, sender) = node_signer_and_user();
        let build = |signer: Option<std::sync::Arc<crate::sign::EventSigner>>| {
            EventBuilder::new(
                sender.clone(),
                "m.room.message".to_owned(),
                base_version().clone(),
            )
            .room_id(room("!r:d"))
            .content(json!({ "body": "x" }))
            .origin_server_ts(1)
            .signer(signer)
            .build()
            .expect("builds")
        };
        assert_ne!(build(Some(signer)).event_id, build(None).event_id);
    }

    /// Wire bytes never carry `event_id` — it's the reference hash, computed
    /// post-canonicalisation and stored only on the `Event` struct as a
    /// sidecar field. The whole `event_view` enrichment pipeline rests on
    /// this invariant; a regression that serialised `event_id` back into
    /// `raw` would let stale ids slip through with no defence beyond a
    /// `Map::insert` overwrite. Pin both arms (create + message).
    #[test]
    fn build_output_raw_lacks_event_id() {
        let create = EventBuilder::new(
            user("@alice:example.org"),
            "m.room.create".to_owned(),
            base_version().clone(),
        )
        .state_key(String::new())
        .content(json!({ "room_version": crate::ROOM_VERSION_ID }))
        .origin_server_ts(1_700_000_000_000)
        .build()
        .expect("create event builds");
        let create_raw: serde_json::Value = serde_json::from_str(create.raw.get()).unwrap();
        assert!(
            create_raw.get("event_id").is_none(),
            "create event wire bytes must not carry event_id: {create_raw}",
        );

        let msg = EventBuilder::new(
            user("@alice:example.org"),
            "m.room.message".to_owned(),
            base_version().clone(),
        )
        .room_id(create.room_id.clone())
        .content(json!({ "msgtype": "m.text", "body": "hi" }))
        .prev_events(vec![create.event_id.clone()])
        .origin_server_ts(1_700_000_000_001)
        .build()
        .expect("message event builds");
        let msg_raw: serde_json::Value = serde_json::from_str(msg.raw.get()).unwrap();
        assert!(
            msg_raw.get("event_id").is_none(),
            "non-create event wire bytes must not carry event_id either: {msg_raw}",
        );
    }

    #[test]
    fn build_message_event_round_trips() {
        let create = EventBuilder::new(
            user("@alice:example.org"),
            "m.room.create".to_owned(),
            base_version().clone(),
        )
        .state_key(String::new())
        .content(json!({ "room_version": crate::ROOM_VERSION_ID }))
        .build()
        .expect("create");
        let msg = EventBuilder::new(
            user("@alice:example.org"),
            "m.room.message".to_owned(),
            base_version().clone(),
        )
        .room_id(create.room_id.clone())
        .content(json!({ "msgtype": "m.text", "body": "hi" }))
        .prev_events(vec![create.event_id.clone()])
        .origin_server_ts(1_700_000_000_001)
        .build()
        .expect("message");

        // event_id format check.
        assert!(msg.event_id.as_str().starts_with('$'));
        assert_eq!(msg.event_id.as_str().len(), 44);
        // room_id matches what the caller passed in.
        assert_eq!(msg.room_id, create.room_id);
        // raw contains the room_id (non-create events keep it on the wire).
        let raw: serde_json::Value = serde_json::from_str(msg.raw.get()).unwrap();
        assert_eq!(raw["room_id"].as_str(), Some(create.room_id.as_str()));
    }

    #[test]
    fn build_is_deterministic_for_identical_inputs() {
        // Same inputs (same ts, same fields) must produce the same event_id —
        // the hash is a pure function of the wire bytes.
        let mk = || {
            EventBuilder::new(
                user("@a:d"),
                "m.room.message".to_owned(),
                base_version().clone(),
            )
            .room_id(room("!r:d"))
            .content(json!({ "body": "x" }))
            .origin_server_ts(1)
            .build()
            .expect("builds")
        };
        assert_eq!(mk().event_id, mk().event_id);
    }

    /// **Redactable** content reaches the event id only through the content
    /// hash. The reference hash is computed over the *redacted* form, where an
    /// `m.room.message` body is already gone; `hashes` (which survives
    /// redaction) is what bound it in. So a trusted-network build — no content
    /// hash — gives two bodies the same id, and a signed build does not.
    ///
    /// The consequence for `trusted_network = true`: two events are kept apart
    /// by `prev_events` / `prev_state_events` and `origin_server_ts`, not by
    /// their bodies. Local sends chain through the room actor (each takes the
    /// previous as its head) and remote ones differ in `sender`, so a collision
    /// needs two same-millisecond events from one sender on identical heads.
    #[test]
    fn redactable_content_reaches_the_event_id_only_when_signed() {
        let (signer, sender) = node_signer_and_user();
        let build = |body: &str, signer: Option<std::sync::Arc<crate::sign::EventSigner>>| {
            EventBuilder::new(
                sender.clone(),
                "m.room.message".to_owned(),
                base_version().clone(),
            )
            .room_id(room("!r:d"))
            .content(json!({ "msgtype": "m.text", "body": body }))
            .origin_server_ts(1)
            .signer(signer)
            .build()
            .expect("builds")
            .event_id
        };
        assert_eq!(
            build("a", None),
            build("b", None),
            "without a content hash, a redacted-away body cannot reach the id"
        );
        assert_ne!(
            build("a", Some(signer.clone())),
            build("b", Some(signer)),
            "a signed build carries the content hash, which does reach the id"
        );
    }

    /// The other half: content the redaction keep-list *preserves* still
    /// diverges the id with no content hash in play — `m.room.member`'s
    /// `membership` survives redaction, so join and leave are distinct events
    /// on a trusted network too.
    #[test]
    fn build_diverges_on_different_non_redactable_content() {
        let build = |membership: &str| {
            EventBuilder::new(
                user("@a:d"),
                "m.room.member".to_owned(),
                base_version().clone(),
            )
            .room_id(room("!r:d"))
            .state_key("@a:d".to_owned())
            .content(json!({ "membership": membership }))
            .origin_server_ts(1)
            .build()
            .expect("builds")
            .event_id
        };
        assert_ne!(build("join"), build("leave"));
    }

    #[test]
    fn build_includes_prev_state_events_in_raw_and_struct() {
        // MSC4242: `prev_state_events` is a top-level wire field carried into
        // the reference hash via the carve-out. Two builds differing only on
        // `prev_state_events` must produce different event_ids, and the
        // `prev_state_events` list must round-trip onto both `raw` and the
        // `Event` struct.
        let ps = vec![eid("$ps1:d"), eid("$ps2:d")];
        let ev = EventBuilder::new(
            user("@a:d"),
            "m.room.member".to_owned(),
            base_version().clone(),
        )
        .room_id(room("!r:d"))
        .state_key("@a:d".to_owned())
        .content(json!({ "membership": "join" }))
        .prev_state_events(ps.clone())
        .origin_server_ts(1)
        .build()
        .expect("builds");
        assert_eq!(ev.prev_state_events, ps);
        let raw: serde_json::Value = serde_json::from_str(ev.raw.get()).unwrap();
        let raw_ps: Vec<&str> = raw["prev_state_events"]
            .as_array()
            .expect("array")
            .iter()
            .map(|v| v.as_str().expect("string"))
            .collect();
        assert_eq!(raw_ps, vec!["$ps1:d", "$ps2:d"]);

        // Differential coverage: changing prev_state_events changes event_id.
        let other = EventBuilder::new(
            user("@a:d"),
            "m.room.member".to_owned(),
            base_version().clone(),
        )
        .room_id(room("!r:d"))
        .state_key("@a:d".to_owned())
        .content(json!({ "membership": "join" }))
        .prev_state_events(vec![eid("$other:d")])
        .origin_server_ts(1)
        .build()
        .expect("builds");
        assert_ne!(ev.event_id, other.event_id);
    }

    #[test]
    fn build_includes_unsigned_object_in_raw() {
        // Happy-path complement to `build_rejects_non_object_unsigned`. The
        // `unsigned` field is the sliding-sync invite-state carrier and must
        // round-trip onto `raw` when set.
        let ev = EventBuilder::new(
            user("@a:d"),
            "m.room.member".to_owned(),
            base_version().clone(),
        )
        .room_id(room("!r:d"))
        .state_key("@b:d".to_owned())
        .content(json!({ "membership": "invite" }))
        .unsigned(json!({ "invite_room_state": [] }))
        .origin_server_ts(1)
        .build()
        .expect("builds");
        let raw: serde_json::Value = serde_json::from_str(ev.raw.get()).unwrap();
        assert_eq!(
            raw["unsigned"]["invite_room_state"]
                .as_array()
                .map(|a| a.len()),
            Some(0)
        );
    }

    #[test]
    fn build_attaches_caller_supplied_auth_events_to_struct_only() {
        let auth_ids = vec![eid("$auth1:d"), eid("$auth2:d")];
        let ev = EventBuilder::new(
            user("@a:d"),
            "m.room.message".to_owned(),
            base_version().clone(),
        )
        .room_id(room("!r:d"))
        .content(json!({}))
        .auth_events(auth_ids.clone())
        .origin_server_ts(1)
        .build()
        .expect("builds");
        assert_eq!(ev.auth_events, auth_ids);
        // Wire bytes must NOT contain auth_events (MSC4242).
        let raw: serde_json::Value = serde_json::from_str(ev.raw.get()).unwrap();
        assert!(raw.get("auth_events").is_none());
    }

    // ---------- error paths ----------

    #[test]
    fn build_rejects_non_create_without_room_id() {
        let err = EventBuilder::new(
            user("@a:d"),
            "m.room.message".to_owned(),
            base_version().clone(),
        )
        .content(json!({}))
        .origin_server_ts(1)
        .build()
        .expect_err("missing room_id");
        assert!(matches!(err, FormatError::MissingField("room_id")));
    }

    #[test]
    fn build_rejects_non_object_content() {
        let err = EventBuilder::new(
            user("@a:d"),
            "m.room.message".to_owned(),
            base_version().clone(),
        )
        .room_id(room("!r:d"))
        .content("not an object")
        .origin_server_ts(1)
        .build()
        .expect_err("non-object content");
        assert!(matches!(
            err,
            FormatError::InvalidFieldType {
                field: "content",
                expected: "object"
            }
        ));
    }

    #[test]
    fn build_rejects_non_object_unsigned() {
        let err = EventBuilder::new(
            user("@a:d"),
            "m.room.message".to_owned(),
            base_version().clone(),
        )
        .room_id(room("!r:d"))
        .content(json!({}))
        .unsigned("oops")
        .origin_server_ts(1)
        .build()
        .expect_err("non-object unsigned");
        assert!(matches!(
            err,
            FormatError::InvalidFieldType {
                field: "unsigned",
                expected: "object"
            }
        ));
    }

    #[test]
    fn build_surfaces_parse_event_error_when_caller_violates_rule_1_1() {
        // The builder doesn't reject every shape that `parse_event` rejects
        // — e.g. a create event with `prev_events` set passes the skeleton
        // checks but trips v12 rule 1.1. The `FormatError` bubbles up so the
        // caller sees the specific rule that fired.
        let err = EventBuilder::new(
            user("@a:d"),
            "m.room.create".to_owned(),
            base_version().clone(),
        )
        .state_key(String::new())
        .content(json!({ "room_version": crate::ROOM_VERSION_ID }))
        .prev_events(vec![eid("$bogus:d")])
        .origin_server_ts(1)
        .build()
        .expect_err("create with prev_events must surface as FormatError");
        assert!(
            matches!(err, FormatError::CreateHasPrevEvents),
            "expected CreateHasPrevEvents, got: {err:?}"
        );
    }

    #[test]
    fn build_rejects_event_over_max_pdu_size() {
        // Local-send path of the S-S §"Size limits" whole-PDU check: an
        // oversized event surfaces from `build()` (via validate_pdu) so the
        // C-S handler can 400 it. Boundary precision lives in validate.rs.
        let err = EventBuilder::new(
            user("@a:d"),
            "m.room.message".to_owned(),
            base_version().clone(),
        )
        .room_id(room("!r:d"))
        .content(json!({ "body": "a".repeat(70_000) }))
        .origin_server_ts(1)
        .build()
        .expect_err("oversized event must not build");
        assert!(matches!(err, FormatError::EventTooLarge));
    }

    // ---------- from_wire ----------

    #[test]
    fn from_wire_round_trips_builder_output() {
        let built = EventBuilder::new(
            user("@a:d"),
            "m.room.message".to_owned(),
            base_version().clone(),
        )
        .room_id(room("!r:d"))
        .content(json!({ "body": "hi" }))
        .origin_server_ts(42)
        .build()
        .expect("builds");

        let parsed = from_wire(built.raw.clone(), Vec::new(), base_version())
            .expect("from_wire")
            .admit_on_faith()
            .into_event();
        // event_id is recomputed from raw — must match the builder's output.
        assert_eq!(parsed.event_id, built.event_id);
        assert_eq!(parsed.room_id, built.room_id);
        assert_eq!(parsed.sender, built.sender);
        assert_eq!(parsed.event_type, built.event_type);
        assert_eq!(parsed.origin_server_ts, built.origin_server_ts);
    }

    #[test]
    fn from_wire_round_trips_create_event_with_derived_room_id() {
        let built = EventBuilder::new(
            user("@a:d"),
            "m.room.create".to_owned(),
            base_version().clone(),
        )
        .state_key(String::new())
        .content(json!({ "room_version": crate::ROOM_VERSION_ID }))
        .origin_server_ts(42)
        .build()
        .expect("create");

        let parsed = from_wire(built.raw.clone(), Vec::new(), base_version())
            .expect("from_wire")
            .admit_on_faith()
            .into_event();
        assert_eq!(parsed.event_id, built.event_id);
        // parse_event re-derived room_id from event_id via sigil swap —
        // must match the builder's.
        assert_eq!(parsed.room_id, built.room_id);
        // Round-tripped raw still lacks `room_id` (v12 create invariant).
        let raw: serde_json::Value = serde_json::from_str(parsed.raw.get()).unwrap();
        assert!(raw.get("room_id").is_none());
    }

    #[test]
    fn from_wire_classifies_semantically_malformed_pdu_as_rejected() {
        // Contract pin: a PDU that parses but fails a state-independent auth
        // rule (here rule 5.1, member without `membership`) comes back as
        // `Wire::Rejected` with `rejected = true` baked in — never dropped at
        // the wire edge, and never obtainable as an accepted `Event`.
        // `EventBuilder::build`, by contrast, still refuses to produce it.
        let raw = serde_json::json!({
            "type": "m.room.member",
            "state_key": "@mallory:remote.example",
            "sender": "@alice:remote.example",
            "room_id": "!Fw7pQdLu79h74bsZabn1UKXoXo7-q5M-cOwQxQxfh2c",
            "content": {},
            "prev_events": [],
            "prev_state_events": [],
            "origin_server_ts": 42,
            "hashes": { "sha256": "wrong" },
        });
        let wire = from_wire(
            RawValue::from_string(raw.to_string()).expect("valid JSON"),
            Vec::new(),
            base_version(),
        )
        .expect("parseable PDU must not be dropped at the wire edge")
        .admit_on_faith();
        let Wire::Rejected(ev, defect) = wire else {
            panic!("rule-5.1 defect must classify as Wire::Rejected");
        };
        assert!(ev.rejected, "the rejected flag must be baked in");
        assert!(matches!(defect, FormatError::MemberMissingMembership));
    }

    #[test]
    fn from_wire_drop_class_defect_is_an_error() {
        // The other half of the classification: a drop-class defect
        // (here the >20 prev_events cap) is an `Err` — the event never
        // enters the system at all.
        let prevs: Vec<String> = (0..21)
            .map(|i| format!("$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA{i:02}"))
            .collect();
        let raw = serde_json::json!({
            "type": "m.room.message",
            "sender": "@alice:remote.example",
            "room_id": "!Fw7pQdLu79h74bsZabn1UKXoXo7-q5M-cOwQxQxfh2c",
            "content": {"msgtype": "m.text", "body": "x"},
            "prev_events": prevs,
            "prev_state_events": [],
            "origin_server_ts": 42,
            "hashes": { "sha256": "wrong" },
        });
        let err = from_wire(
            RawValue::from_string(raw.to_string()).expect("valid JSON"),
            Vec::new(),
            base_version(),
        )
        .expect_err("drop-class defect must not construct an event");
        assert!(matches!(err, FormatError::TooManyPrevEvents));
    }

    #[test]
    fn from_wire_attaches_caller_supplied_auth_events() {
        let built = EventBuilder::new(
            user("@a:d"),
            "m.room.message".to_owned(),
            base_version().clone(),
        )
        .room_id(room("!r:d"))
        .content(json!({}))
        .origin_server_ts(1)
        .build()
        .expect("builds");

        let auth = vec![eid("$x:d"), eid("$y:d")];
        let parsed = from_wire(built.raw.clone(), auth.clone(), base_version())
            .expect("from_wire")
            .admit_on_faith()
            .into_event();
        assert_eq!(parsed.auth_events, auth);
    }

    #[test]
    fn from_wire_rejects_non_object_root() {
        let raw = to_raw_value(&json!([1, 2, 3])).unwrap();
        let err = from_wire(raw, Vec::new(), base_version()).expect_err("array root");
        assert!(matches!(
            err,
            FormatError::InvalidFieldType {
                field: "<root>",
                expected: "object"
            }
        ));
    }

    #[test]
    fn from_wire_redacts_event_with_mismatched_content_hash() {
        // Spec: receive-side redacts events whose content hash doesn't match.
        // Tamper with `hashes.sha256` on a builder-produced event and verify
        // from_wire returns the redacted form (content collapsed to {}) while
        // keeping the SAME event_id (which is computed over the redacted form).
        let built = EventBuilder::new(
            user("@a:d"),
            "m.room.message".to_owned(),
            base_version().clone(),
        )
        .room_id(room("!r:d"))
        .content(json!({ "msgtype": "m.text", "body": "secret" }))
        .origin_server_ts(1)
        .build()
        .expect("builds");

        // Tamper: rewrite `hashes.sha256` to a junk value so verification fails.
        let mut raw_obj: serde_json::Value = serde_json::from_str(built.raw.get()).unwrap();
        raw_obj["hashes"]["sha256"] = json!("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
        let tampered_raw = serde_json::value::to_raw_value(&raw_obj).expect("raw");

        let parsed = from_wire(tampered_raw, Vec::new(), base_version())
            .expect("from_wire")
            .admit_on_faith()
            .into_event();
        // Tampering `hashes.sha256` also changes the reference hash, so the
        // parsed event_id here is the *tampered* hash — not asserted. The point
        // is that a content-hash mismatch REDACTS (body stripped) rather than
        // rejecting the event.
        let parsed_raw: serde_json::Value = serde_json::from_str(parsed.raw.get()).unwrap();
        assert!(
            parsed_raw["content"]
                .as_object()
                .expect("content object")
                .is_empty(),
            "content must be redacted on hash mismatch, got: {}",
            parsed_raw["content"]
        );
        // type / room_id / sender are preserved (V11 keep-list).
        assert_eq!(parsed_raw["type"].as_str(), Some("m.room.message"));
        assert_eq!(parsed_raw["room_id"].as_str(), Some("!r:d"));
        assert_eq!(parsed_raw["sender"].as_str(), Some("@a:d"));
    }

    #[test]
    fn from_wire_accepts_event_with_matching_content_hash() {
        // Builder produces events with a valid content hash; from_wire must
        // accept them as-is (no redaction).
        let built = EventBuilder::new(
            user("@a:d"),
            "m.room.message".to_owned(),
            base_version().clone(),
        )
        .room_id(room("!r:d"))
        .content(json!({ "msgtype": "m.text", "body": "preserved" }))
        .origin_server_ts(1)
        .build()
        .expect("builds");
        let parsed = from_wire(built.raw.clone(), Vec::new(), base_version())
            .expect("from_wire")
            .admit_on_faith()
            .into_event();
        // Content survives — body field still there.
        let parsed_raw: serde_json::Value = serde_json::from_str(parsed.raw.get()).unwrap();
        assert_eq!(parsed_raw["content"]["body"].as_str(), Some("preserved"));
    }

    /// The trusted-network round trip: a build that emitted no `hashes` must
    /// come back through `from_wire` with its content intact. The spec's
    /// "no hash ⇒ redact" rule would strip the body of every message in the
    /// mesh; the divergence documented on `from_wire` is what prevents it.
    #[test]
    fn from_wire_keeps_content_when_hashes_absent() {
        let built = EventBuilder::new(
            user("@a:d"),
            "m.room.message".to_owned(),
            base_version().clone(),
        )
        .room_id(room("!r:d"))
        .content(json!({ "msgtype": "m.text", "body": "preserved" }))
        .origin_server_ts(1)
        .build()
        .expect("builds");
        let raw_json: serde_json::Value = serde_json::from_str(built.raw.get()).unwrap();
        assert!(raw_json.get("hashes").is_none(), "precondition: no hashes");

        let parsed = from_wire(built.raw.clone(), Vec::new(), base_version())
            .expect("from_wire")
            .admit_on_faith()
            .into_event();
        assert_eq!(parsed.event_id, built.event_id);
        let parsed_raw: serde_json::Value = serde_json::from_str(parsed.raw.get()).unwrap();
        assert_eq!(parsed_raw["content"]["body"].as_str(), Some("preserved"));
    }

    /// An event that *does* carry `hashes` is still held to it — the absent
    /// case above is not a licence to serve a hash-shaped placeholder. An empty
    /// `hashes` object counts as a mismatch (redact), not as absence (accept);
    /// the wrong-value arm is `from_wire_redacts_event_with_mismatched_content_hash`.
    #[test]
    fn from_wire_redacts_when_hashes_present_but_empty() {
        let raw = to_raw_value(&json!({
            "type": "m.room.message",
            "sender": "@a:d",
            "room_id": "!r:d",
            "content": { "msgtype": "m.text", "body": "secret" },
            "prev_events": [],
            "prev_state_events": [],
            "origin_server_ts": 1,
            "hashes": {},
        }))
        .unwrap();
        let parsed = from_wire(raw, Vec::new(), base_version())
            .expect("from_wire")
            .admit_on_faith()
            .into_event();
        let parsed_raw: serde_json::Value = serde_json::from_str(parsed.raw.get()).unwrap();
        assert!(
            parsed_raw["content"]
                .as_object()
                .expect("content object")
                .is_empty(),
            "an empty `hashes` must redact, got: {}",
            parsed_raw["content"]
        );
    }

    #[test]
    fn from_wire_rejects_object_without_type() {
        // No `type` field — ruma's `redact_in_place` reports
        // `MissingField { path: "type" }`; from_wire translates to
        // FormatError::MissingField("type").
        let raw = to_raw_value(&json!({
            "sender": "@a:d", "room_id": "!r:d",
            "content": {}, "origin_server_ts": 1
        }))
        .unwrap();
        let err = from_wire(raw, Vec::new(), base_version()).expect_err("missing type");
        assert!(matches!(err, FormatError::MissingField("type")));
    }

    // ---------- pluggable event-id scheme ----------

    /// A scheme with a deliberately different id *shape* to the reference
    /// hash: it stamps the authoring clock reading into `id_ts` and names the
    /// event after it, so ids are short, decimal and time-ordered rather than
    /// 43 chars of base64.
    ///
    /// The clock is injected so the test is deterministic. What matters is
    /// that `stamp` supplies an input `derive` then reads back out of the
    /// bytes — the same two halves any non-hash scheme uses.
    #[derive(Debug)]
    struct TimestampIds(i64);

    impl crate::event_id::EventIdScheme for TimestampIds {
        fn stamp(&self, obj: &mut CanonicalJsonObject) -> Result<(), EventIdError> {
            obj.insert(
                "id_ts".to_owned(),
                CanonicalJsonValue::Integer(self.0.try_into().map_err(|_| {
                    EventIdError::Scheme("clock reading out of canonical range".to_owned())
                })?),
            );
            Ok(())
        }

        fn derive(
            &self,
            obj: &CanonicalJsonObject,
            _version: &RoomVersion,
        ) -> Result<OwnedEventId, EventIdError> {
            let Some(CanonicalJsonValue::Integer(ts)) = obj.get("id_ts") else {
                return Err(EventIdError::Scheme("no id_ts".to_owned()));
            };
            OwnedEventId::try_from(format!("${}", i64::from(*ts)))
                .map_err(|e| EventIdError::Scheme(e.to_string()))
        }
    }

    /// A room version named by [`TimestampIds`] off the given clock reading.
    /// `id_ts` is the id's only input, so the version must keep it through
    /// redaction — that is what `extra_redaction_keys` is for.
    fn timestamp_version(clock: i64) -> std::sync::Arc<RoomVersion> {
        std::sync::Arc::new(RoomVersion {
            id: "org.matrix.neutrino.test.ts",
            rules: ruma::room_version_rules::RoomVersionRules::V12,
            ids: std::sync::Arc::new(TimestampIds(clock)),
            extra_redaction_keys: &["prev_state_events", "id_ts"],
        })
    }

    fn timestamp_ids() -> std::sync::Arc<RoomVersion> {
        timestamp_version(1_700_000_000)
    }

    // ---------- room-version resolution keys ----------

    /// A normal event is named by its room's version, so the only key it
    /// offers is `room_id` — the caller looks that up in the store.
    #[test]
    fn version_keys_of_a_normal_event_are_its_room_id() {
        let built = EventBuilder::new(
            user("@a:d"),
            "m.room.message".to_owned(),
            base_version().clone(),
        )
        .room_id(room("!r:d"))
        .content(json!({ "msgtype": "m.text", "body": "hi" }))
        .origin_server_ts(1)
        .build()
        .expect("builds");

        let keys = room_version_keys(&built.raw);
        assert_eq!(keys.room_id.as_deref(), Some(room("!r:d").as_ref()));
        assert_eq!(keys.declared, None);
    }

    /// A create carries no `room_id` at all under v12 (the room id is derived
    /// from its own event id), so its `content.room_version` is the only thing
    /// that can name it — the one self-describing case.
    #[test]
    fn version_keys_of_a_create_are_its_declared_version() {
        let built = EventBuilder::new(
            user("@a:d"),
            "m.room.create".to_owned(),
            base_version().clone(),
        )
        .state_key(String::new())
        .content(json!({ "room_version": crate::ROOM_VERSION_ID }))
        .origin_server_ts(1)
        .build()
        .expect("builds");

        let keys = room_version_keys(&built.raw);
        assert_eq!(keys.room_id, None, "a v12 create carries no room_id");
        assert_eq!(keys.declared.as_deref(), Some(crate::ROOM_VERSION_ID));
    }

    /// Only a create may declare a version. Without the `type` gate any event
    /// could volunteer a `content.room_version` and hand the caller two
    /// contradictory answers — a `room_id` saying "look me up" and a declared
    /// string saying "name me this way".
    #[test]
    fn only_a_create_may_declare_a_version() {
        // A message whose content happens to carry a `room_version` key: its
        // content is its own business and says nothing about naming.
        let built = EventBuilder::new(
            user("@a:d"),
            "m.room.message".to_owned(),
            base_version().clone(),
        )
        .room_id(room("!r:d"))
        .content(json!({ "msgtype": "m.text", "body": "hi", "room_version": "n" }))
        .origin_server_ts(1)
        .build()
        .expect("builds");
        let keys = room_version_keys(&built.raw);
        assert_eq!(keys.room_id.as_deref(), Some(room("!r:d").as_ref()));
        assert_eq!(keys.declared, None, "a non-create declares nothing");

        // Same for a *state* event of another type, and for hand-rolled bytes
        // that carry both fields — the pair can never both be populated.
        let raw = RawValue::from_string(
            json!({
                "type": "m.room.power_levels",
                "room_id": "!r:d",
                "state_key": "",
                "content": { "room_version": "n" },
            })
            .to_string(),
        )
        .expect("raw");
        let keys = room_version_keys(&raw);
        assert_eq!(keys.room_id.as_deref(), Some(room("!r:d").as_ref()));
        assert_eq!(keys.declared, None);
    }

    /// A `type`-less event declares nothing either: `type` is what makes a
    /// create a create, and `from_wire` rejects its absence once the event is
    /// named (`MissingField("type")`).
    #[test]
    fn version_keys_of_a_typeless_event_declare_nothing() {
        let raw = RawValue::from_string(
            json!({ "room_id": "!r:d", "content": { "room_version": "n" } }).to_string(),
        )
        .expect("raw");
        let keys = room_version_keys(&raw);
        assert_eq!(keys.room_id.as_deref(), Some(room("!r:d").as_ref()));
        assert_eq!(keys.declared, None);
    }

    /// Malformed bytes yield no keys rather than an error: rejecting them is
    /// `from_wire`'s job, which runs once the version is resolved. Two
    /// rejections of the same bytes could otherwise disagree.
    #[test]
    fn version_keys_of_unparseable_bytes_are_empty() {
        let raw = RawValue::from_string("[1,2,3]".to_owned()).expect("raw");
        let keys = room_version_keys(&raw);
        assert_eq!(keys.room_id, None);
        assert_eq!(keys.declared, None);

        // A non-string `content.room_version` on a create is likewise not a
        // version claim; `check_create` is what rejects it, under the resolved
        // version (the base, since nothing was declared).
        let raw = RawValue::from_string(
            json!({ "type": "m.room.create", "content": { "room_version": 12 } }).to_string(),
        )
        .expect("raw");
        assert_eq!(room_version_keys(&raw).declared, None);
    }

    /// The seam's whole point: an id minted by a non-default scheme must be
    /// re-derivable by a *receiver* from the wire bytes alone, with no side
    /// table and nothing transmitted alongside the event.
    #[test]
    fn custom_scheme_id_round_trips_through_from_wire() {
        let built = EventBuilder::new(user("@a:d"), "m.room.message".to_owned(), timestamp_ids())
            .room_id(room("!r:d"))
            .content(json!({ "msgtype": "m.text", "body": "hi" }))
            .origin_server_ts(1)
            .build()
            .expect("builds");

        // Not the reference-hash shape: this is the point of the seam.
        assert_eq!(built.event_id.as_str(), "$1700000000");

        // The stamped input rides `raw` — that is what makes the id derivable.
        let raw_json: serde_json::Value = serde_json::from_str(built.raw.get()).unwrap();
        assert_eq!(raw_json["id_ts"], json!(1_700_000_000));

        // A receiver holding its *own* instance of the scheme — a different
        // clock, so it could never guess the id — still derives the same one,
        // because it reads the answer out of the event.
        let receiver = timestamp_version(999);
        let parsed = from_wire(built.raw.clone(), Vec::new(), &receiver)
            .expect("from_wire")
            .admit_on_faith();
        assert_eq!(parsed.event().event_id, built.event_id);
    }

    /// `stamp` must run before the content hash, or a signed deployment would
    /// emit a `hashes.sha256` that does not cover the id's own inputs — and
    /// `from_wire` would then redact the event as hash-mismatched.
    #[test]
    fn stamped_fields_are_covered_by_the_content_hash() {
        let (signer, sender) = node_signer_and_user();
        let built = EventBuilder::new(sender, "m.room.message".to_owned(), timestamp_ids())
            .room_id(room("!r:d"))
            .content(json!({ "msgtype": "m.text", "body": "hi" }))
            .origin_server_ts(1)
            .signer(Some(signer))
            .build()
            .expect("builds");

        let CanonicalJsonValue::Object(obj) =
            serde_json::from_str::<CanonicalJsonValue>(built.raw.get()).expect("canonical")
        else {
            panic!("expected object");
        };
        assert!(obj.contains_key("id_ts"), "id_ts must ride the wire");
        assert_eq!(
            check_content_hash(&obj),
            ContentHashCheck::Matches,
            "the emitted content hash must cover the stamped `id_ts`"
        );
    }

    /// Receipt-check 3 redacts a content-hash-mismatched event and carries on
    /// with it, so a scheme's inputs must survive redaction — otherwise the
    /// bytes we persist can no longer re-derive the id we just assigned, and
    /// the event dies at the next hop that re-reads it.
    #[test]
    fn extra_redaction_keys_survive_the_hash_mismatch_redaction() {
        let (signer, sender) = node_signer_and_user();
        let built = EventBuilder::new(sender, "m.room.message".to_owned(), timestamp_ids())
            .room_id(room("!r:d"))
            .content(json!({ "msgtype": "m.text", "body": "hi" }))
            .origin_server_ts(1)
            .signer(Some(signer))
            .build()
            .expect("builds");

        // Tamper with `content` so the emitted content hash no longer matches,
        // driving `from_wire` down the redaction path.
        let mut raw_json: serde_json::Value = serde_json::from_str(built.raw.get()).unwrap();
        raw_json["content"]["body"] = json!("tampered");
        let tampered = RawValue::from_string(raw_json.to_string()).expect("raw");

        let version = timestamp_ids();
        let parsed = from_wire(tampered, Vec::new(), &version)
            .expect("from_wire")
            .admit_on_faith();
        let event = parsed.event();
        assert_eq!(event.event_id, built.event_id, "the id must be unchanged");

        // The persisted bytes were replaced by the redacted form — and can
        // still re-derive the id, which is the property under test.
        let CanonicalJsonValue::Object(redacted) =
            serde_json::from_str::<CanonicalJsonValue>(event.raw.get()).expect("canonical")
        else {
            panic!("expected object");
        };
        let redelivered = version
            .event_id(&redacted)
            .expect("redacted bytes still name the event");
        assert_eq!(redelivered, built.event_id);
    }

    /// A scheme whose inputs are missing is drop-class, not a panic and not a
    /// silent fallback: an event nobody can name must not enter the system.
    #[test]
    fn scheme_failure_on_inbound_bytes_is_a_drop_class_format_error() {
        // Built under the default scheme, so `raw` carries no `id_ts`.
        let built = EventBuilder::new(
            user("@a:d"),
            "m.room.message".to_owned(),
            base_version().clone(),
        )
        .room_id(room("!r:d"))
        .content(json!({}))
        .origin_server_ts(1)
        .build()
        .expect("builds");

        let err = from_wire(built.raw, Vec::new(), &timestamp_ids())
            .expect_err("scheme cannot name this event");
        assert!(
            matches!(err, FormatError::EventId(_)),
            "expected FormatError::EventId, got {err:?}"
        );
        assert_eq!(semantic_verdict(&err), SemanticVerdict::Drop);
    }

    // ---------- signing ----------

    /// A node-named signer whose user lives on it, so the built event's
    /// sender-origin is the signer's server name and the wire round-trip can
    /// verify through the NodeIdKeyResolver.
    fn node_signer_and_user() -> (std::sync::Arc<crate::sign::EventSigner>, OwnedUserId) {
        let signer = crate::sign::EventSigner::new(&[7u8; 32], "");
        let name: String = signer
            .public_key()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let signer = std::sync::Arc::new(crate::sign::EventSigner::new(&[7u8; 32], name.clone()));
        let sender: OwnedUserId = format!("@n:{name}").parse().expect("valid user id");
        (signer, sender)
    }

    /// The end-to-end signed-build loop: build with a signer, feed the raw
    /// through from_wire, admit via the signature-verifying path.
    ///
    /// Signature-invariance of the event id (the property co-signing relies on)
    /// is pinned by `sign::tests::co_sign_event_regenerates_raw_and_keeps_id`,
    /// which adds a signature to a finished event. It cannot be pinned here by
    /// comparing a signed to an unsigned *build*: those also differ in whether
    /// `hashes` is emitted, which the id does cover — see
    /// `content_hash_presence_changes_the_event_id`.
    #[tokio::test]
    async fn build_with_signer_round_trips_through_verify() {
        let (signer, sender) = node_signer_and_user();
        let signed = EventBuilder::new(sender, "m.room.message".to_owned(), base_version().clone())
            .room_id(room("!r:d"))
            .content(json!({ "msgtype": "m.text", "body": "signed" }))
            .origin_server_ts(1)
            .signer(Some(signer.clone()))
            .build()
            .expect("builds");

        // The signed raw carries the signature block…
        let raw_json: serde_json::Value = serde_json::from_str(signed.raw.get()).unwrap();
        assert!(
            raw_json["signatures"][signer.server_name()]["ed25519:1"].is_string(),
            "raw must carry signatures[server][ed25519:1]"
        );

        // …and admits through the signature-verifying ingress.
        let unverified =
            from_wire(signed.raw.clone(), Vec::new(), base_version()).expect("from_wire");
        let wire = unverified
            .verify(&crate::sign::NodeIdKeyResolver)
            .await
            .expect("signature verifies");
        assert_eq!(wire.event().event_id, signed.event_id);
    }

    /// An unsigned event refused by the signature-verifying ingress — the
    /// signed-deployment failure mode for events from a pre-signing build.
    #[tokio::test]
    async fn unsigned_event_fails_signed_admission() {
        let (_, sender) = node_signer_and_user();
        let built = EventBuilder::new(sender, "m.room.message".to_owned(), base_version().clone())
            .room_id(room("!r:d"))
            .content(json!({ "body": "unsigned" }))
            .origin_server_ts(1)
            .build()
            .expect("builds");
        let unverified =
            from_wire(built.raw.clone(), Vec::new(), base_version()).expect("from_wire");
        let err = unverified
            .verify(&crate::sign::NodeIdKeyResolver)
            .await
            .expect_err("no signature must not admit");
        assert!(matches!(err, FormatError::SignatureCheck(_)), "{err}");
    }
}
