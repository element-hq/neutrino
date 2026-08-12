//! Event + JSON signing, signature verification, and the key-resolution port.
//!
//! Implements the S-S API "Signing Events" algorithm and the appendices'
//! "Signing JSON" (needed for the `/_matrix/key/v2/server` response, which is
//! signed JSON, not an event). Only used when the deployment runs signed
//! (`EventSecurity::Signed`, i.e. `trusted_network = false`) — on a trusted
//! network events carry no signatures at all.
//!
//! ## What a signature does and does not cover
//!
//! An event signature is computed over the **redacted** canonical form of the
//! event ([`redact_to_canonical_bytes`]) — byte-for-byte the same string the
//! reference hash (and therefore the event id) is computed over. Two
//! consequences, both load-bearing:
//!
//! - Event ids are signature-invariant: adding a signature (including a
//!   resident server co-signing a join/leave/invite) never changes an event's
//!   id, so the reference DAG is untouched by signing.
//! - A signature alone does NOT protect redactable content (an
//!   `m.room.message` body, say) — that is the content hash's job. Receipt
//!   validation therefore needs BOTH checks: signature (provenance) and
//!   content hash (integrity), per the spec's "Validating hashes and
//!   signatures on received events". The converse also holds, which is why a
//!   trusted-network deployment emits neither: an unsigned content hash is
//!   self-attested and proves nothing, so it is dropped as dead weight (see
//!   `EventBuilder::build`).
//!
//! ## Key ids
//!
//! This server never rotates its key (the signing key IS the node identity
//! secret, and for node-named servers the `server_name` is the public key),
//! so it signs under the fixed [`SIGNING_KEY_ID`]. Verification accepts any
//! `ed25519:*` key id an origin offers, resolving each through the
//! [`KeyResolver`] port.
//!
//! ## MSC4242 divergence
//!
//! The redaction underneath preserves `prev_state_events` (see
//! `redact_for_hash`), so signatures over events carrying it diverge from
//! what a non-MSC4242 server would compute. Self-consistent mesh-wide;
//! non-MSC4242 servers are out of scope for this homeserver.

use std::future::Future;
use std::pin::Pin;

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use ruma::canonical_json::{CanonicalJsonObject, CanonicalJsonValue, RedactionError};
use thiserror::Error;

use crate::event_id::{b64_unpadded, canonical, redact_to_canonical_bytes};
use crate::room_version::{RoomVersion, RoomVersions};

/// The fixed key id this server signs under. No rotation mechanics: the
/// signing key is the node identity secret, so the key lives exactly as long
/// as the server identity does.
pub const SIGNING_KEY_ID: &str = "ed25519:1";

/// This server's event/JSON signer: the node identity secret plus the
/// `server_name` the signature is filed under.
///
/// The same 32-byte secret the server derives its identity from
/// (`server_name` = lowercase-hex ed25519 public key for node-named servers)
/// — so a peer holding only the server name can verify without any key
/// lookup ([`NodeIdKeyResolver`]).
pub struct EventSigner {
    key: SigningKey,
    server_name: String,
}

// Hand-written so the secret key can never leak into logs — only the public
// identity is shown.
impl std::fmt::Debug for EventSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventSigner")
            .field("server_name", &self.server_name)
            .finish_non_exhaustive()
    }
}

impl EventSigner {
    pub fn new(secret: &[u8; 32], server_name: impl Into<String>) -> Self {
        Self {
            key: SigningKey::from_bytes(secret),
            server_name: server_name.into(),
        }
    }

    /// The name signatures are filed under (`signatures[server_name]`).
    pub fn server_name(&self) -> &str {
        &self.server_name
    }

    /// The public half, raw. For node-named servers `hex(public_key())` ==
    /// `server_name`; also served by `/_matrix/key/v2/server`.
    pub fn public_key(&self) -> [u8; 32] {
        self.key.verifying_key().to_bytes()
    }

    /// Sign an **event**: compute the signature over the redacted canonical
    /// form and merge it into the full event's `signatures` — never
    /// clobbering signatures already present (co-signing a remote origin's
    /// event adds ours beside theirs).
    ///
    /// Errors only if the event violates redaction preconditions (missing
    /// `type`, non-object `content`/`hashes`/`signatures`).
    /// `version` supplies the redaction rules the signed byte string is
    /// computed under, including the keys that survive redaction — so the
    /// signature covers the event's own name (see
    /// [`RoomVersion::extra_redaction_keys`]).
    pub fn sign_event(
        &self,
        obj: &mut CanonicalJsonObject,
        version: &RoomVersion,
    ) -> Result<(), RedactionError> {
        let bytes = redact_to_canonical_bytes(obj, version)?;
        self.attach(obj, self.sign_bytes(&bytes));
        Ok(())
    }

    /// Sign a plain JSON object (appendices "Signing JSON"): the signature is
    /// over the canonical form minus `signatures`/`unsigned`, with no
    /// redaction. Used for the `/_matrix/key/v2/server` response.
    pub fn sign_json(&self, obj: &mut CanonicalJsonObject) {
        let bytes = signable_json_bytes(obj);
        self.attach(obj, self.sign_bytes(&bytes));
    }

    /// Unpadded-base64 ed25519 signature over raw bytes. Exposed at this
    /// granularity so tests can compose it with other redaction rules (the
    /// spec's golden event vectors predate v11 redaction).
    pub fn sign_bytes(&self, bytes: &[u8]) -> String {
        b64_unpadded(&self.key.sign(bytes).to_bytes())
    }

    /// Co-sign an already-constructed [`Event`](crate::Event): add this
    /// server's signature beside whatever signatures the event already
    /// carries, regenerating `raw`. The event id is untouched (signatures are
    /// outside the reference hash) — this is the resident/invitee side of the
    /// `send_join` / `send_leave` / `invite` round-trips.
    pub fn co_sign(
        &self,
        event: &mut crate::Event,
        version: &RoomVersion,
    ) -> Result<(), CoSignError> {
        let parsed: CanonicalJsonValue = serde_json::from_str(event.raw.get())?;
        let CanonicalJsonValue::Object(mut obj) = parsed else {
            return Err(CoSignError::NonObjectRoot);
        };
        self.sign_event(&mut obj, version)?;
        let bytes = canonical(&obj);
        let s = String::from_utf8(bytes).expect("canonical JSON is valid UTF-8");
        event.raw = serde_json::value::RawValue::from_string(s)
            .expect("canonical JSON parses as a RawValue");
        Ok(())
    }

    /// Merge `signature` into `obj.signatures[self.server_name][SIGNING_KEY_ID]`,
    /// creating the intermediate objects as needed and preserving everything
    /// already there.
    fn attach(&self, obj: &mut CanonicalJsonObject, signature: String) {
        let signatures = obj
            .entry("signatures".to_owned())
            .or_insert_with(|| CanonicalJsonValue::Object(CanonicalJsonObject::new()));
        // A non-object `signatures` can't occur on our own builds and is a
        // redaction-precondition failure on the event path (sign_event errors
        // before reaching here); replace defensively on the JSON path.
        if !matches!(signatures, CanonicalJsonValue::Object(_)) {
            *signatures = CanonicalJsonValue::Object(CanonicalJsonObject::new());
        }
        let CanonicalJsonValue::Object(signatures) = signatures else {
            unreachable!("signatures was just coerced to an object");
        };
        let server = signatures
            .entry(self.server_name.clone())
            .or_insert_with(|| CanonicalJsonValue::Object(CanonicalJsonObject::new()));
        if !matches!(server, CanonicalJsonValue::Object(_)) {
            *server = CanonicalJsonValue::Object(CanonicalJsonObject::new());
        }
        let CanonicalJsonValue::Object(server) = server else {
            unreachable!("server entry was just coerced to an object");
        };
        server.insert(
            SIGNING_KEY_ID.to_owned(),
            CanonicalJsonValue::String(signature),
        );
    }
}

/// Lenient unpadded-base64 decode: tolerates padding and non-zero trailing
/// bits. The Matrix ecosystem's historical decoders do (python's
/// `unpaddedbase64`, libsodium-based stacks), and the spec's own signing
/// seed (`…MW+3XA1`) has a non-zero trailing sextet — a strict decoder
/// cannot even load the golden vectors.
fn b64_decode(s: &str) -> Result<Vec<u8>, base64::DecodeError> {
    use base64::Engine;
    use base64::engine::general_purpose::GeneralPurposeConfig;
    use base64::engine::{DecodePaddingMode, GeneralPurpose};
    const LENIENT: GeneralPurpose = GeneralPurpose::new(
        &base64::alphabet::STANDARD,
        GeneralPurposeConfig::new()
            .with_decode_allow_trailing_bits(true)
            .with_decode_padding_mode(DecodePaddingMode::Indifferent),
    );
    LENIENT.decode(s)
}

/// Canonical bytes of `obj` minus `signatures`/`unsigned` — the "Signing
/// JSON" byte string (no redaction).
fn signable_json_bytes(obj: &CanonicalJsonObject) -> Vec<u8> {
    let mut clone = obj.clone();
    clone.remove("signatures");
    clone.remove("unsigned");
    canonical(&clone)
}

/// Why an event failed signature verification. `Drop`-class at the ingress:
/// per the spec's receipt checks, an event that fails the signature check is
/// dropped (unlike a content-hash mismatch, which redacts and continues).
#[derive(Debug, Error)]
pub enum VerifyError {
    /// The event carries no `signatures[origin]` object at all.
    #[error("event has no signatures entry for origin {origin}")]
    NoSignatureForOrigin { origin: String },

    /// The origin's entry has no `ed25519:*` signature, or none of them
    /// verified against the resolved keys.
    #[error("no ed25519 signature by {origin} verified: {detail}")]
    NoValidSignature { origin: String, detail: String },

    /// The event violates redaction preconditions, so the signed byte string
    /// cannot even be computed.
    #[error("event is not redactable: {0}")]
    Redaction(#[from] RedactionError),
}

/// Why [`EventSigner::co_sign`] failed. Both variants are "can't happen" for
/// an event that came through `from_wire`/`EventBuilder` (its `raw` is
/// canonical and redactable by construction), surfaced as errors rather than
/// panics per the no-unwrap handler rule.
#[derive(Debug, Error)]
pub enum CoSignError {
    #[error("event raw is not valid JSON: {0}")]
    Parse(#[from] serde_json::Error),
    #[error("event raw root is not an object")]
    NonObjectRoot,
    #[error("event is not redactable: {0}")]
    Redaction(#[from] RedactionError),
}

/// Verify that `event` carries a valid signature by `origin` — the general
/// form of the ingress sender-check, for flows that require a signature from
/// a specific *other* server (the inviting side requiring the invitee's
/// co-signature on the returned invite).
pub async fn verify_event_signed_by(
    event: &crate::Event,
    origin: &str,
    resolver: &dyn KeyResolver,
    version: &RoomVersion,
) -> Result<(), VerifyError> {
    let parsed: CanonicalJsonValue =
        serde_json::from_str(event.raw.get()).map_err(|e| VerifyError::NoValidSignature {
            origin: origin.to_owned(),
            detail: format!("event raw is not valid JSON: {e}"),
        })?;
    let CanonicalJsonValue::Object(obj) = parsed else {
        return Err(VerifyError::NoValidSignature {
            origin: origin.to_owned(),
            detail: "event raw root is not an object".to_owned(),
        });
    };
    verify_event_signature(&obj, origin, resolver, version).await
}

/// Key-resolution failure, produced by a [`KeyResolver`].
#[derive(Debug, Error)]
#[error("resolving key {key_id} for {server_name}: {reason}")]
pub struct KeyResolveError {
    pub server_name: String,
    pub key_id: String,
    pub reason: String,
}

/// The key-resolution port: `(server_name, key_id)` → 32-byte ed25519 verify
/// key. The federation medium nominates an implementation when it declares
/// signed deployments — where the keys come from is the medium's
/// business (the node-id namespace, DNS `/_matrix/key/v2/server` lookups, a
/// notary), not this crate's.
///
/// Hand-desugared async (no `async_trait` proc-macro dependency): one method,
/// object-safe, `Send` future.
pub trait KeyResolver: Send + Sync {
    fn verify_key<'a>(
        &'a self,
        server_name: &'a str,
        key_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<[u8; 32], KeyResolveError>> + Send + 'a>>;
}

/// Whether event provenance is cryptographic — the "trust the origin" half
/// of the deployment's security configuration (`sign_messages` in
/// neutrino-main's `SecurityConfig`), composed once at the composition root
/// from the app's `trusted_network` config and threaded everywhere as ONE
/// value: it is both the ingress admission policy
/// ([`admit`](EventSecurity::admit)) and the authoring signer source
/// ([`signer`](EventSecurity::signer)), so "verify inbound but don't sign
/// outbound" (or vice versa) is unrepresentable.
#[derive(Clone)]
pub enum EventSecurity {
    /// Trusted network: origin claims on relayed events are taken on faith;
    /// events carry no signatures at all.
    TrustedNetwork,
    /// Untrusted origins: every locally-authored event is signed, and every
    /// inbound event must carry a valid signature by its sender's server,
    /// keys resolved through the medium's nominated resolver.
    Signed {
        signer: std::sync::Arc<EventSigner>,
        resolver: std::sync::Arc<dyn KeyResolver>,
    },
}

impl EventSecurity {
    /// Admit a parsed wire event under this policy. Signature failure is
    /// Drop-class ([`FormatError::SignatureCheck`](crate::FormatError)).
    pub async fn admit(
        &self,
        unverified: crate::event_builder::UnverifiedWire,
    ) -> Result<crate::Wire, crate::FormatError> {
        match self {
            EventSecurity::TrustedNetwork => Ok(unverified.admit_on_faith()),
            EventSecurity::Signed { resolver, .. } => unverified.verify(resolver.as_ref()).await,
        }
    }

    /// The signer for locally-authored events — `None` on a trusted network
    /// (events MUST NOT carry signatures there).
    pub fn signer(&self) -> Option<&std::sync::Arc<EventSigner>> {
        match self {
            EventSecurity::TrustedNetwork => None,
            EventSecurity::Signed { signer, .. } => Some(signer),
        }
    }
}

/// The two event-level facts a deployment owns, composed once at the
/// composition root and threaded as one value: how provenance is established
/// ([`EventSecurity`]) and which room versions this build understands
/// ([`RoomVersions`]).
///
/// They are independent — every combination is a real deployment — so this is
/// a carrier, not a policy enum. It exists because the one place that needs
/// both at once is the parse+admit seam ([`admit_wire`](Self::admit_wire)),
/// and because threading two parameters through the same dozen signatures
/// would be twice the churn for the same fact.
///
/// Note what is *not* here: how ids are derived. That is a property of each
/// room's version, recovered from `rooms.room_version`, not a process-wide
/// setting — see [`RoomVersion`].
#[derive(Clone, Debug)]
pub struct EventPolicy {
    pub security: EventSecurity,
    pub versions: std::sync::Arc<RoomVersions>,
}

impl EventPolicy {
    /// Compose from the deployment's two facts.
    pub fn new(security: EventSecurity, versions: std::sync::Arc<RoomVersions>) -> Self {
        Self { security, versions }
    }

    /// A trusted-network policy understanding only the base room version — the
    /// shape almost every test wants.
    pub fn trusted_network() -> Self {
        Self::new(
            EventSecurity::TrustedNetwork,
            std::sync::Arc::new(RoomVersions::base_only()),
        )
    }

    /// Parse wire bytes from a room of `version` and admit the result under
    /// this policy in one step: [`from_wire`](crate::event_builder::from_wire)
    /// (id derivation, content-hash verify/redact, format + semantic
    /// classification) composed with [`admit`](EventSecurity::admit)
    /// (signature check under `Signed`, on faith under `TrustedNetwork`). The
    /// single parse+admit seam for inbound federation bytes — the HTTP
    /// handlers and the engine worker/reconcile/gapfill all funnel through
    /// here so the contract can't drift between crates.
    ///
    /// The caller resolves `version`, because naming an event requires knowing
    /// its room and this crate cannot read the store: room-scoped ingress
    /// paths already hold it, the `make_*`/`invite`/`send_join` envelopes carry
    /// it on the wire, and `/send` resolves it per PDU.
    pub async fn admit_wire(
        &self,
        raw: Box<serde_json::value::RawValue>,
        version: &std::sync::Arc<RoomVersion>,
    ) -> Result<crate::Wire, crate::FormatError> {
        self.security
            .admit(crate::event_builder::from_wire(raw, Vec::new(), version)?)
            .await
    }

    /// The signer for locally-authored events — see
    /// [`EventSecurity::signer`].
    pub fn signer(&self) -> Option<&std::sync::Arc<EventSigner>> {
        self.security.signer()
    }
}

// Hand-written so `KeyResolver` needn't be `Debug` (same idiom as
// neutrino-lb's `LbConfig`): the variant name is the useful information.
impl std::fmt::Debug for EventSecurity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventSecurity::TrustedNetwork => write!(f, "EventSecurity::TrustedNetwork"),
            EventSecurity::Signed { signer, .. } => f
                .debug_struct("EventSecurity::Signed")
                .field("signer", signer)
                .finish_non_exhaustive(),
        }
    }
}

/// Key resolution for node-named servers: the `server_name` IS the
/// lowercase-hex ed25519 public key, so resolution is a pure decode — no
/// fetch, no cache, no rotation (the key id is ignored; there is only ever
/// one key, and it cannot rotate without becoming a different server).
pub struct NodeIdKeyResolver;

impl KeyResolver for NodeIdKeyResolver {
    fn verify_key<'a>(
        &'a self,
        server_name: &'a str,
        key_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<[u8; 32], KeyResolveError>> + Send + 'a>> {
        let result = decode_node_id(server_name).ok_or_else(|| KeyResolveError {
            server_name: server_name.to_owned(),
            key_id: key_id.to_owned(),
            reason: "server name is not a 64-char hex node id".to_owned(),
        });
        Box::pin(std::future::ready(result))
    }
}

/// Decode a 64-char hex server name into 32 key bytes. `hex::decode` accepts
/// upper- or lower-case; `try_into` enforces exactly 32 bytes, so any non-hex
/// or wrong-length name falls out as `None` (not a node id).
fn decode_node_id(server_name: &str) -> Option<[u8; 32]> {
    hex::decode(server_name).ok()?.try_into().ok()
}

/// Verify that `obj` carries a valid signature by `origin`: iterate the
/// origin's `ed25519:*` signature entries, resolve each key id through
/// `resolver`, and accept on the first signature that verifies over the
/// event's redacted canonical bytes. Per-key failures are accumulated into
/// the error detail so a rejection log says *why* every candidate failed.
pub async fn verify_event_signature(
    obj: &CanonicalJsonObject,
    origin: &str,
    resolver: &dyn KeyResolver,
    version: &RoomVersion,
) -> Result<(), VerifyError> {
    let bytes = redact_to_canonical_bytes(obj, version)?;
    let entries = origin_signature_entries(obj, origin)?;

    let mut failures = Vec::new();
    for (key_id, sig_b64) in entries {
        match resolver.verify_key(origin, &key_id).await {
            Ok(key) => match verify_one(&bytes, &key, &sig_b64) {
                Ok(()) => return Ok(()),
                Err(why) => failures.push(format!("{key_id}: {why}")),
            },
            Err(e) => failures.push(format!("{key_id}: {e}")),
        }
    }
    Err(VerifyError::NoValidSignature {
        origin: origin.to_owned(),
        detail: if failures.is_empty() {
            "no ed25519:* entries present".to_owned()
        } else {
            failures.join("; ")
        },
    })
}

/// The `(key_id, base64 signature)` pairs `origin` signed `obj` under,
/// restricted to the `ed25519:` algorithm.
fn origin_signature_entries(
    obj: &CanonicalJsonObject,
    origin: &str,
) -> Result<Vec<(String, String)>, VerifyError> {
    let no_sig = || VerifyError::NoSignatureForOrigin {
        origin: origin.to_owned(),
    };
    let Some(CanonicalJsonValue::Object(signatures)) = obj.get("signatures") else {
        return Err(no_sig());
    };
    let Some(CanonicalJsonValue::Object(server)) = signatures.get(origin) else {
        return Err(no_sig());
    };
    let entries: Vec<(String, String)> = server
        .iter()
        .filter(|(key_id, _)| key_id.starts_with("ed25519:"))
        .filter_map(|(key_id, v)| match v {
            CanonicalJsonValue::String(sig) => Some((key_id.clone(), sig.clone())),
            _ => None,
        })
        .collect();
    if entries.is_empty() {
        return Err(no_sig());
    }
    Ok(entries)
}

/// Verify one candidate signature. String error feeds the accumulated
/// per-key failure detail.
fn verify_one(bytes: &[u8], key: &[u8; 32], sig_b64: &str) -> Result<(), String> {
    let sig_bytes =
        b64_decode(sig_b64).map_err(|e| format!("signature is not unpadded base64: {e}"))?;
    let sig_bytes: [u8; 64] = sig_bytes
        .try_into()
        .map_err(|v: Vec<u8>| format!("signature is {} bytes, want 64", v.len()))?;
    let key = VerifyingKey::from_bytes(key).map_err(|e| format!("bad verify key: {e}"))?;
    key.verify(bytes, &Signature::from_bytes(&sig_bytes))
        .map_err(|_| "signature does not verify".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::room_version::base_version;
    use ruma::canonical_json::redact_in_place;
    use ruma::room_version_rules::RoomVersionRules;
    use serde_json::json;

    /// The spec appendices' signing seed
    /// (`YJDBA9Xnr2sVqXD9Vj7XVUnmFZcZrlw8Md7kMW+3XA1`, server `domain`,
    /// key `ed25519:1`).
    fn spec_signer() -> EventSigner {
        let seed = b64_decode("YJDBA9Xnr2sVqXD9Vj7XVUnmFZcZrlw8Md7kMW+3XA1").expect("seed decodes");
        let seed: [u8; 32] = seed.try_into().expect("32-byte seed");
        EventSigner::new(&seed, "domain")
    }

    fn obj(v: serde_json::Value) -> CanonicalJsonObject {
        let CanonicalJsonValue::Object(o) = v.try_into().unwrap() else {
            panic!("expected object");
        };
        o
    }

    fn signature_of<'a>(o: &'a CanonicalJsonObject, server: &str) -> &'a str {
        let CanonicalJsonValue::Object(sigs) = o.get("signatures").expect("signatures") else {
            panic!("signatures not an object");
        };
        let CanonicalJsonValue::Object(by_server) = sigs.get(server).expect("server entry") else {
            panic!("server entry not an object");
        };
        let CanonicalJsonValue::String(sig) = by_server.get(SIGNING_KEY_ID).expect("key id entry")
        else {
            panic!("signature not a string");
        };
        sig
    }

    // ---- appendices "Signing JSON" golden vectors ----

    #[test]
    fn sign_json_spec_vector_empty_object() {
        let mut o = obj(json!({}));
        spec_signer().sign_json(&mut o);
        assert_eq!(
            signature_of(&o, "domain"),
            "K8280/U9SSy9IVtjBuVeLr+HpOB4BQFWbg+UZaADMtTdGYI7Geitb76LTrr5QV/7Xg4ahLwYGYZzuHGZKM5ZAQ"
        );
    }

    #[test]
    fn sign_json_spec_vector_simple_object() {
        let mut o = obj(json!({ "one": 1, "two": "Two" }));
        spec_signer().sign_json(&mut o);
        assert_eq!(
            signature_of(&o, "domain"),
            "KqmLSbO39/Bzb0QIYE82zqLwsA+PDzYIpIRA2sRQ4sL53+sN6/fpNSoqE7BP7vBZhG6kYdD13EIMJpvhJI+6Bw"
        );
    }

    // ---- appendices "Signing Events" golden vectors ----
    //
    // The spec's event vectors are v1-era events computed under **v1
    // redaction** (their redacted form keeps `origin`, `event_id`, …, which
    // v11/v12 strip). Our production `sign_event` is v12-only, so these
    // vectors are exercised by composing the same primitive (`sign_bytes`)
    // with ruma's V1 redaction — pinning the ed25519 + canonical-JSON +
    // unpadded-base64 layers against the spec while the v12 redaction layer
    // is pinned separately by the event_id tests (real homeserver vectors).

    fn v1_signed_bytes(mut o: CanonicalJsonObject) -> Vec<u8> {
        redact_in_place(&mut o, &RoomVersionRules::V1.redaction, None).expect("v1 redaction");
        o.remove("signatures");
        o.remove("unsigned");
        crate::event_id::canonical(&o)
    }

    #[test]
    fn sign_event_spec_vector_minimal_event_via_v1_redaction() {
        let bytes = v1_signed_bytes(obj(json!({
            "auth_events": [],
            "content": {},
            "depth": 3,
            "hashes": { "sha256": "5jM4wQpv6lnBo7CLIghJuHdW+s2CMBJPUOGOC89ncos" },
            "origin": "domain",
            "origin_server_ts": 1_000_000,
            "prev_events": [],
            "room_id": "!x:domain",
            "sender": "@a:domain",
            "type": "X",
            "unsigned": { "age_ts": 1_000_000 }
        })));
        assert_eq!(
            spec_signer().sign_bytes(&bytes),
            "KxwGjPSDEtvnFgU00fwFz+l6d2pJM6XBIaMEn81SXPTRl16AqLAYqfIReFGZlHi5KLjAWbOoMszkwsQma+lYAg"
        );
    }

    #[test]
    fn sign_event_spec_vector_message_event_via_v1_redaction() {
        let bytes = v1_signed_bytes(obj(json!({
            "content": { "body": "Here is the message content" },
            "event_id": "$0:domain",
            "hashes": { "sha256": "onLKD1bGljeBWQhWZ1kaP9SorVmRQNdN5aM2JYU2n/g" },
            "origin": "domain",
            "origin_server_ts": 1_000_000,
            "type": "m.room.message",
            "room_id": "!r:domain",
            "sender": "@u:domain",
            "unsigned": { "age_ts": 1_000_000 }
        })));
        assert_eq!(
            spec_signer().sign_bytes(&bytes),
            "Wm+VzmOUOz08Ds+0NTWb1d4CZrVsJSikkeRxh6aCcUwu6pNC78FunoD7KNWzqFn241eYHYMGCA5McEiVPdhzBA"
        );
    }

    // ---- v12 round-trip through the production path ----

    /// A node-named signer: server_name == hex(pubkey), so the
    /// NodeIdKeyResolver can verify without any lookup.
    fn node_signer(seed_byte: u8) -> EventSigner {
        let secret = [seed_byte; 32];
        let name = hex_of(&SigningKey::from_bytes(&secret).verifying_key().to_bytes());
        EventSigner::new(&secret, name)
    }

    fn hex_of(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn v12_event() -> CanonicalJsonObject {
        obj(json!({
            "type": "m.room.message",
            "sender": "@n:server",
            "room_id": "!room:server",
            "content": { "msgtype": "m.text", "body": "hello" },
            "prev_events": ["$prev:server"],
            "prev_state_events": ["$ps:server"],
            "origin_server_ts": 1_700_000_000_000_u64,
            "hashes": { "sha256": "Y29udGVudGhhc2g" }
        }))
    }

    #[tokio::test]
    async fn sign_then_verify_round_trips_via_node_id_resolver() {
        let signer = node_signer(7);
        let mut event = v12_event();
        signer
            .sign_event(&mut event, base_version())
            .expect("signs");
        verify_event_signature(
            &event,
            signer.server_name(),
            &NodeIdKeyResolver,
            base_version(),
        )
        .await
        .expect("verifies");
    }

    #[tokio::test]
    async fn verify_fails_with_wrong_key() {
        let signer = node_signer(7);
        let imposter = node_signer(8); // different key, different name
        let mut event = v12_event();
        signer
            .sign_event(&mut event, base_version())
            .expect("signs");
        // Claim the imposter's name signed it: resolver derives the imposter
        // key from the name, under which the signature must not verify.
        let CanonicalJsonValue::Object(sigs) = event.remove("signatures").unwrap() else {
            panic!("signatures object");
        };
        let by_signer = sigs.get(signer.server_name()).unwrap().clone();
        let mut forged = CanonicalJsonObject::new();
        forged.insert(imposter.server_name().to_owned(), by_signer);
        event.insert("signatures".to_owned(), CanonicalJsonValue::Object(forged));

        let err = verify_event_signature(
            &event,
            imposter.server_name(),
            &NodeIdKeyResolver,
            base_version(),
        )
        .await
        .expect_err("must not verify");
        assert!(matches!(err, VerifyError::NoValidSignature { .. }), "{err}");
    }

    #[tokio::test]
    async fn verify_fails_when_origin_absent_from_signatures() {
        let signer = node_signer(7);
        let mut event = v12_event();
        signer
            .sign_event(&mut event, base_version())
            .expect("signs");
        let other = node_signer(9);
        let err = verify_event_signature(
            &event,
            other.server_name(),
            &NodeIdKeyResolver,
            base_version(),
        )
        .await
        .expect_err("origin never signed");
        assert!(
            matches!(err, VerifyError::NoSignatureForOrigin { .. }),
            "{err}"
        );
    }

    /// The signature is over the REDACTED form: mutating redactable content
    /// does not break it (that's the content hash's job to catch), while
    /// mutating a protected field does. Pins the division of labour the
    /// ingress relies on — signature check alone is provenance, not
    /// integrity.
    #[tokio::test]
    async fn signature_covers_redacted_form_only() {
        let signer = node_signer(7);
        let mut event = v12_event();
        signer
            .sign_event(&mut event, base_version())
            .expect("signs");

        // Tamper with redactable content → signature still verifies.
        let mut tampered_content = event.clone();
        tampered_content.insert(
            "content".to_owned(),
            json!({ "msgtype": "m.text", "body": "TAMPERED" })
                .try_into()
                .unwrap(),
        );
        verify_event_signature(
            &tampered_content,
            signer.server_name(),
            &NodeIdKeyResolver,
            base_version(),
        )
        .await
        .expect("redactable tampering is invisible to the signature");

        // Tamper with a protected field → signature breaks.
        let mut tampered_sender = event.clone();
        tampered_sender.insert(
            "sender".to_owned(),
            CanonicalJsonValue::String("@evil:server".to_owned()),
        );
        verify_event_signature(
            &tampered_sender,
            signer.server_name(),
            &NodeIdKeyResolver,
            base_version(),
        )
        .await
        .expect_err("protected-field tampering must break the signature");
    }

    /// Co-signing: a second server signing the same event must not clobber
    /// the first signature, and both must verify afterwards. This is the
    /// primitive the send_join/send_leave/invite round-trips rely on.
    #[tokio::test]
    async fn co_signing_preserves_existing_signatures() {
        let origin = node_signer(7);
        let resident = node_signer(8);
        let mut event = v12_event();
        origin
            .sign_event(&mut event, base_version())
            .expect("origin signs");
        resident
            .sign_event(&mut event, base_version())
            .expect("resident co-signs");

        verify_event_signature(
            &event,
            origin.server_name(),
            &NodeIdKeyResolver,
            base_version(),
        )
        .await
        .expect("origin signature survives co-signing");
        verify_event_signature(
            &event,
            resident.server_name(),
            &NodeIdKeyResolver,
            base_version(),
        )
        .await
        .expect("resident signature verifies");
    }

    /// Handler-level co-sign: a full `Event` built+signed by the origin is
    /// co-signed by the resident — `raw` regenerated with BOTH signatures,
    /// event id untouched. This is what send_join/send_leave/invite do to the
    /// received event before persisting and responding.
    #[tokio::test]
    async fn co_sign_event_regenerates_raw_and_keeps_id() {
        let origin = node_signer(7);
        let resident = node_signer(8);
        let sender: ruma::OwnedUserId = format!("@n:{}", origin.server_name())
            .parse()
            .expect("valid user id");
        let mut event = crate::event_builder::EventBuilder::new(
            sender,
            "m.room.member".to_owned(),
            base_version().clone(),
        )
        .room_id("!r:d".parse().expect("room id"))
        .state_key(format!("@n:{}", origin.server_name()))
        .content(json!({ "membership": "join" }))
        .origin_server_ts(1)
        .signer(Some(std::sync::Arc::new(node_signer(7))))
        .build()
        .expect("builds");
        let id_before = event.event_id.clone();

        resident
            .co_sign(&mut event, base_version())
            .expect("co-signs");

        assert_eq!(event.event_id, id_before, "co-sign must not change the id");
        verify_event_signed_by(
            &event,
            origin.server_name(),
            &NodeIdKeyResolver,
            base_version(),
        )
        .await
        .expect("origin signature survives in the regenerated raw");
        verify_event_signed_by(
            &event,
            resident.server_name(),
            &NodeIdKeyResolver,
            base_version(),
        )
        .await
        .expect("resident signature present in the regenerated raw");
        // The regenerated raw still derives the same id (canonical bytes).
        let recomputed = crate::event_id::base_version_event_id(&event.raw).expect("recompute");
        assert_eq!(recomputed, id_before);
    }

    // ---- NodeIdKeyResolver ----

    #[tokio::test]
    async fn node_id_resolver_decodes_hex_name_and_rejects_others() {
        let key = [0xab_u8; 32];
        let name = hex_of(&key);
        let resolved = NodeIdKeyResolver
            .verify_key(&name, SIGNING_KEY_ID)
            .await
            .expect("hex name resolves");
        assert_eq!(resolved, key);

        for bad in ["example.org", "abc123", &name[..62], &format!("{name}zz")] {
            assert!(
                NodeIdKeyResolver
                    .verify_key(bad, SIGNING_KEY_ID)
                    .await
                    .is_err(),
                "{bad:?} must not resolve"
            );
        }
    }

    /// For a node-named signer the advertised name and the public key are the
    /// same fact — pins the identity-derivation symmetry with
    /// `neutrino-main`'s `server_identity_from_secret`.
    #[test]
    fn node_signer_name_is_hex_public_key() {
        let signer = node_signer(7);
        assert_eq!(signer.server_name(), hex_of(&signer.public_key()));
    }
}
