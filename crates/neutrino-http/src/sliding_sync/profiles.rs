//! The sliding-sync profiles extension (MSC4262), serving the profile fields
//! the host owns (`Command::ReplaceProfileField`).

use std::collections::{BTreeMap, BTreeSet};

use neutrino_store::StorageBackend;
use ruma::profile::{ProfileFieldName, UserProfileChanges, UserProfileUpdate};
use ruma::{OwnedUserId, UserId};
use serde_json::Value;

use super::{SyncError, SyncState};

/// One host-owned field's value for each user that has it.
pub type FieldValues = BTreeMap<OwnedUserId, Value>;

/// Every host-owned profile field, by field name.
pub type HostProfiles = BTreeMap<ProfileFieldName, FieldValues>;

/// The updates that bring a connection from `sent` (what it has been told) to
/// the host's current fields, and the new `sent` to record once the response
/// is committed.
///
/// A field the connection has never been told about is reported in full to
/// every current member of the user's joined rooms, `removed` for those
/// without it: the client keeps profiles across server restarts, and nothing
/// else would clear a value it learned from an earlier run. Users who join
/// later are only told about the field once the host changes it.
pub async fn build<S: StorageBackend>(
    state: &SyncState<S>,
    user_id: &UserId,
    sent: &HostProfiles,
) -> Result<(BTreeMap<OwnedUserId, UserProfileUpdate>, HostProfiles), SyncError> {
    let current = state.host_profiles.borrow().clone();
    let members = if current.keys().any(|field| !sent.contains_key(field)) {
        room_members(state, user_id).await?
    } else {
        BTreeSet::new()
    };
    let mut changes = BTreeMap::<OwnedUserId, UserProfileChanges>::new();
    for (field, values) in &current {
        let told = sent.get(field);
        for (user, value) in values {
            if told.and_then(|t| t.get(user)) != Some(value) {
                let change = changes.entry(user.clone()).or_default();
                change.updated.insert(field.clone(), value.clone());
            }
        }
        let known: Vec<&OwnedUserId> = match told {
            Some(told) => told.keys().collect(),
            None => members.iter().collect(),
        };
        for user in known.into_iter().filter(|u| !values.contains_key(*u)) {
            let change = changes.entry(user.clone()).or_default();
            change.removed.push(field.clone());
        }
    }
    let updates = changes
        .into_iter()
        .map(|(user, change)| (user, UserProfileUpdate::Updated(change)))
        .collect();
    Ok((updates, current))
}

async fn room_members<S: StorageBackend>(
    state: &SyncState<S>,
    user_id: &UserId,
) -> Result<BTreeSet<OwnedUserId>, SyncError> {
    let mut members = BTreeSet::new();
    for room_id in state.store.joined_rooms(user_id).await? {
        members.extend(state.store.joined_members(&room_id).await?.into_keys());
    }
    Ok(members)
}
