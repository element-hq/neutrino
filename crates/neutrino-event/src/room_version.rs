//! Room versions — the per-room agreement on how events are named and
//! redacted.
//!
//! How an event is named is a room-version property, not a deployment setting:
//! reference-hash ids arrived in room v3, sigil-less room ids in v12. A version
//! rides in the create event's `content.room_version`, is validated on every
//! inbound create, and is persisted in `rooms.room_version` — so it is agreed
//! mesh-wide by construction, and a peer that does not know a version refuses
//! its rooms with an error that already exists.
//!
//! [`RoomVersion`] wraps ruma's [`RoomVersionRules`] rather than replacing it:
//! ruma stays the authority for everything it can express, and we carry only
//! the deltas it cannot ([`RoomVersion::ids`], because
//! `EventIdFormatVersion` is a closed enum).
//!
//! See `room-version-registry-design.md`.

use std::sync::{Arc, LazyLock};

use ruma::room_version_rules::RoomVersionRules;
use thiserror::Error;

use crate::event_id::{EventIdScheme, ReferenceHashIds};

/// One room version this build understands.
#[derive(Debug)]
pub struct RoomVersion {
    /// The wire string in `content.room_version`, stored verbatim in
    /// `rooms.room_version`.
    pub id: &'static str,

    /// Ruma's rules for this version, cloned from the nearest stock version
    /// and tweaked. Authority for redaction, auth, state-res, signatures and
    /// event format.
    ///
    /// Only `.redaction` is consumed today (see
    /// [`redact_for_hash`](crate::event_id::redact_for_hash)): the v12 auth
    /// rules and state-res in `neutrino-room` are hand-written and do not
    /// consult ruma's rules at all. The whole struct is carried anyway because
    /// it is where a second divergence belongs — if something we want to vary
    /// turns out to be expressible in `RoomVersionRules`, vary it there rather
    /// than adding another field here.
    pub rules: RoomVersionRules,

    /// How events in this room are named. Ruma's `EventIdFormatVersion` is a
    /// closed, `non_exhaustive` enum, so a custom derivation cannot live in
    /// [`rules`](Self::rules).
    pub ids: Arc<dyn EventIdScheme>,

    /// This version's divergence from ruma's redaction keep-list.
    pub redaction_keys: RedactionKeys,
}

/// A room version's divergence from ruma's redaction keep-list.
#[derive(Debug)]
pub struct RedactionKeys {
    /// Top-level keys that survive redaction beyond ruma's keep-list.
    ///
    /// Two things end up here. MSC4242's `prev_state_events` — every version
    /// this repo speaks must list it, since the state-DAG parentage field has
    /// to be covered by the reference hash but the v11/v12 keep-list doesn't
    /// (yet) mention it. And the identity-bearing fields a stamping
    /// [`ids`](RoomVersion::ids) scheme writes, which must survive redaction or
    /// the event would change its name when redacted (receipt-check 3 redacts a
    /// content-hash mismatch and carries on with the result).
    pub added: &'static [&'static str],

    /// Top-level keys stripped despite ruma's keep-list. Such a key is covered
    /// by neither the event's name nor a signature, so a wire format may omit
    /// it and rebuild it on receipt.
    pub removed: &'static [&'static str],
}

impl RoomVersion {
    /// The id of an event in a room of this version — the one derivation both
    /// the authoring and the receiving side run over the same bytes.
    ///
    /// The single entry point to [`EventIdScheme::derive`]: it passes `self`,
    /// so a scheme cannot accidentally be handed the redaction rules of a
    /// version other than the one the event actually belongs to.
    pub fn event_id(
        &self,
        obj: &ruma::canonical_json::CanonicalJsonObject,
    ) -> Result<ruma::OwnedEventId, crate::event_id::EventIdError> {
        self.ids.derive(obj, self)
    }

    /// Outbound only: stamp this version's identity-bearing fields into an
    /// event being authored. See [`EventIdScheme::stamp`].
    pub fn stamp(
        &self,
        obj: &mut ruma::canonical_json::CanonicalJsonObject,
    ) -> Result<(), crate::event_id::EventIdError> {
        self.ids.stamp(obj)
    }
}

/// The built-in base version: MSC4242 over room v12, reference-hash ids.
static BASE: LazyLock<Arc<RoomVersion>> = LazyLock::new(|| {
    Arc::new(RoomVersion {
        id: crate::ROOM_VERSION_ID,
        rules: RoomVersionRules::V12,
        ids: Arc::new(ReferenceHashIds),
        redaction_keys: RedactionKeys {
            added: &["prev_state_events"],
            removed: &[],
        },
    })
});

/// The built-in base version — MSC4242 over room v12 with reference-hash ids,
/// the version every non-embedded build creates rooms under and the one the
/// spec's own scheme describes.
///
/// Returned as the shared handle so a caller can both read it (`&RoomVersion`
/// by deref) and clone it into an [`EventBuilder`](crate::event_builder::EventBuilder).
pub fn base_version() -> &'static Arc<RoomVersion> {
    &BASE
}

/// The room versions one running server understands: the built-in base, plus
/// at most one declared by the injected federation medium.
///
/// A medium's version is the one new local rooms are created under — a hard
/// cut-over, since a build either makes rooms of that version or it doesn't.
/// The base stays in the registry regardless so rooms created before the
/// cut-over stay readable.
/// The base version is not a field: it is the same [`base_version`] handle in
/// every registry, so storing a copy would be a second place to keep in step.
#[derive(Debug)]
pub struct RoomVersions {
    medium: Option<Arc<RoomVersion>>,
}

impl RoomVersions {
    /// Compose the registry. `medium` is the version the injected federation
    /// medium declared, if any.
    ///
    /// A medium may not reuse the base version's wire string: two peers both
    /// claiming one version string but naming events differently is precisely
    /// the silent disagreement the registry exists to make impossible.
    pub fn new(medium: Option<RoomVersion>) -> Result<Self, RegistryError> {
        if let Some(m) = &medium
            && m.id == base_version().id
        {
            return Err(RegistryError::ShadowsBase(m.id.to_owned()));
        }
        Ok(Self {
            medium: medium.map(Arc::new),
        })
    }

    /// The registry with no medium version — the dev binary, the tests, and
    /// every build whose medium declares nothing.
    pub fn base_only() -> Self {
        Self { medium: None }
    }

    /// The version under this wire string, or `None` if this build does not
    /// understand it. `None` for an inbound create means refusing the room.
    pub fn get(&self, id: &str) -> Option<&Arc<RoomVersion>> {
        match &self.medium {
            Some(m) if m.id == id => Some(m),
            _ if base_version().id == id => Some(base_version()),
            _ => None,
        }
    }

    /// The built-in base version. Also the version to name a create event
    /// under when it declares none at all (v12 rule 1.3 permits the field to
    /// be absent).
    pub fn base(&self) -> &'static Arc<RoomVersion> {
        base_version()
    }

    /// Every version this build understands, base first. What `make_join`'s
    /// spec-mandated `?ver=` negotiation offers a resident: joining a room
    /// created before a medium's cut-over is still legitimate, so the base is
    /// always on the list.
    pub fn ids(&self) -> impl Iterator<Item = &'static str> {
        std::iter::once(base_version().id).chain(self.medium.iter().map(|m| m.id))
    }

    /// The version new local rooms are created under: the medium's if it
    /// declared one, else the base.
    pub fn default_for_new_rooms(&self) -> &Arc<RoomVersion> {
        self.medium.as_ref().unwrap_or(base_version())
    }
}

/// Failure composing a [`RoomVersions`] registry.
#[derive(Debug, Error)]
pub enum RegistryError {
    #[error(
        "medium declares room version {0:?}, which shadows the built-in version of the same \
         name — two peers claiming one version string must agree on what it means"
    )]
    ShadowsBase(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use ruma::OwnedEventId;
    use ruma::canonical_json::CanonicalJsonObject;

    #[derive(Debug)]
    struct StubIds;
    impl EventIdScheme for StubIds {
        fn derive(
            &self,
            _obj: &CanonicalJsonObject,
            _version: &RoomVersion,
        ) -> Result<OwnedEventId, crate::event_id::EventIdError> {
            Ok(OwnedEventId::try_from("$stub").expect("valid event id"))
        }
    }

    fn medium_version(id: &'static str) -> RoomVersion {
        RoomVersion {
            id,
            rules: RoomVersionRules::V12,
            ids: Arc::new(StubIds),
            redaction_keys: RedactionKeys {
                added: &["prev_state_events", "seq"],
                removed: &[],
            },
        }
    }

    #[test]
    fn base_only_resolves_the_base_and_nothing_else() {
        let versions = RoomVersions::base_only();
        assert_eq!(
            versions.get(crate::ROOM_VERSION_ID).map(|v| v.id),
            Some(crate::ROOM_VERSION_ID)
        );
        assert!(versions.get("n").is_none());
        assert_eq!(versions.default_for_new_rooms().id, crate::ROOM_VERSION_ID);
    }

    #[test]
    fn medium_version_is_the_default_but_base_stays_readable() {
        let versions = RoomVersions::new(Some(medium_version("n"))).expect("distinct ids");
        assert_eq!(versions.default_for_new_rooms().id, "n");
        // The pre-cut-over rooms on disk must still resolve.
        assert_eq!(
            versions.get(crate::ROOM_VERSION_ID).map(|v| v.id),
            Some(crate::ROOM_VERSION_ID)
        );
        assert_eq!(versions.get("n").map(|v| v.id), Some("n"));
        assert!(versions.get("org.matrix.msc4242.13").is_none());
    }

    #[test]
    fn a_medium_may_not_shadow_the_base_version_string() {
        let err = RoomVersions::new(Some(medium_version(crate::ROOM_VERSION_ID)))
            .expect_err("shadowing the base must be refused");
        assert!(matches!(err, RegistryError::ShadowsBase(_)));
    }

    #[test]
    fn base_version_declares_the_msc4242_carve_out() {
        assert_eq!(base_version().redaction_keys.added, ["prev_state_events"]);
        assert!(base_version().redaction_keys.removed.is_empty());
    }
}
