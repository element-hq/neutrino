# Room state machine — notes for a successor

You are picking up an in-progress task to build a per-room state machine
for neutrino. Read `CLAUDE.md` and `PLAN.md` first. This document covers
what those don't.

## Goal

A v12-only room state machine in `crates/neutrino-state` that, given an
incoming event:

1. Validates its wire format.
2. Resolves state-before-event by state-resolving state-after of each
   `prev_state_event` (MSC4242 — auth_events are not declared on the
   wire under MSC4242; everything auth needs is in state-before).
3. Runs the v12 auth rules against state-before-event.
4. Updates forward extremities + current state if the event is a state
   event and was accepted.
5. Runs the v12 auth rules against the (possibly updated) current state.
   Failure → mark the event soft-failed. Does NOT undo the state update.
6. Emits effects describing what storage / federation should do next.

## Hard scope rules

- **Room version 12 only.** Auth rules per
  <https://spec.matrix.org/v1.18/rooms/v12/>. Other room versions are not
  supported and are not stubbed.
- **MSC4242 (State DAGs) is assumed.** Every event carries
  `prev_state_events` alongside `prev_events`. Read state-before-event by
  state-resolving over state-after of each entry in `prev_state_events`.
  The room DAG is *not* the state DAG — do not confuse them.
- **Trusted network.** No signature checks. No server signing keys.
  Reference hashes still required (event IDs and room IDs derive from
  them), but signatures are not validated.
- **No new dependencies** without asking.

## Architectural sketch (decided)

```rust
pub struct RoomCore {
    room_id: OwnedRoomId,
    version: RoomVersion,
    state_forward_extremities: BTreeSet<OwnedEventId>,
    current_state: Arc<StateMap<Arc<Event>>>,
    // recent_events / current_state_group deferred for now
}

impl RoomCore {
    pub fn apply(
        &mut self,
        event: Event,
        provider: &dyn StateProvider,
    ) -> Result<Vec<Effect>, CoreError> {
        // 1. Format validation.
        // 2. For each prev_state_event, look up state-after via provider.
        //    Resolve state-before-event = state-res over those state maps.
        // 3. Run auth rules against state-before-event.
        // 4. If accepted state event: update forward extremities,
        //    recompute current_state via state-res across new FE set.
        // 5. Run auth rules against the (possibly updated) current_state.
        //    Failure → mark soft_failed. Does NOT undo the state update.
        // 6. Emit effects.
    }
}

pub trait StateProvider {
    /// Resolved state-before-event, keyed by (type, state_key).
    fn state_before(&self, event_id: &EventId) -> Result<Arc<StateMap<OwnedEventId>>>;

    fn events(&self, event_ids: &[EventId]) -> Result<Vec<Event>>;

    /// Auth chain difference + (for v2.1) conflicted subgraph.
    fn auth_chain_difference(
        &self,
        state_sets: &[&StateMap<OwnedEventId>],
        conflicted: Option<&HashSet<OwnedEventId>>,
    ) -> Result<AuthChainDifference>;

    /// Walk auth_events backwards to find the next m.room.power_levels.
    fn power_level_auth_ancestor(&self, event_id: &EventId) -> Result<Option<OwnedEventId>>;
}
```

`StateProvider` is **sync**. State resolution is CPU-bound pure code. Do
not introduce async here.

## Decisions log (do not relitigate)

- Crate: `neutrino-state`. Deps: `ruma`, `serde`, `serde_json`,
  `thiserror`. No `async-trait`, no `tokio`.
- `StateMap<V> = HashMap<(String, String), V>`. (BTreeMap was a wrong
  call — state resolution does not depend on input map iteration order
  because every step that fans into ordered work uses an explicit total
  comparator.)
- `state_before(event_id)` returns state-before-event, not state-after.
  Reason: when accepting writes, the new event sits *on top of*
  state-before — that is the hinge. State-after doesn't expose it.
- `prev_state_events` is read directly off the event, not via the
  provider.
- Soft-fail does **not** gate current-state updates. Matches Synapse:
  `_check_for_soft_fail` only sets `event.internal_metadata.soft_failed = True`;
  state-res runs over the DAG unaware of the flag; the flag gates
  client relay and `_get_prevs_before_rejected` only. See
  `synapse/handlers/federation_event.py:2000+` and
  `synapse/storage/databases/main/events.py:962`.
- IAC pass 1 starts from **empty** state (v12 change), not from
  unconflicted state.
- MSC4242 removes `auth_events` from the wire entirely. Every event
  carries `prev_state_events` instead. Servers state-resolve over the
  `prev_state_events` of an incoming event to derive state-before-event,
  then **calculate** that event's auth_events server-side from
  state-before via the auth-events-selection algorithm. The calculated
  set is what state resolution's internal machinery (auth chain
  difference, reverse-topological power ordering, mainline ordering)
  walks. The state-res algorithm itself is unchanged from synapse
  `state/v2.py` — only the input source changes. No rule 3.2 check
  against a wire field, because there is no wire field.
- Auth events selection is still required: it's the pure function used
  to compute `auth_events` from state-before for every event as
  state-res traverses the auth chain.
- Missing state events during processing → request via
  `/get_missing_events` with `state_dag: true`. No silent fallback to
  defaults; an incomplete state DAG causes rejection.
- State groups deferred.
- Effects interpretation, redaction application, persistence, and the
  storage-backed `StateProvider` are deferred — do not start without
  explicit go-ahead.

## Module map

How the pieces fit together (the orchestration order is the order they
run inside `RoomCore::apply`):

- **types** — `RoomVersion::V12`, `StateMap`, `Event`, `FormatError`,
  `AuthError`, `CoreError`.
- **format validation** — `validate::parse_event`. Pure-JSON wire format
  only, no I/O.
- **reference validation** — `validate::validate_references(event,
  provider)`. Existential checks that need provider lookups:
  - v12 rule 2: room_id corresponds to an accepted `m.room.create`
    event.
  - MSC4242 prev_state_events triad: each entry must exist, belong to
    the same room, have a `state_key`, and not be rejected.
  Lives in the `validate` module — orchestration only *calls* it. The
  `StateProvider` trait grew over time from a minimal `get_event` to
  state-after lookups, auth-chain difference, and power-level auth
  ancestor.
- **auth events selection** — pure fn computing an event's `auth_events`
  from state-before.
- **auth rules v12** — `check_auth_rules`, rule 4 onwards (rule 2 is
  existential and lives in reference validation, not auth). `AuthError`
  variants live here.
- **state resolution** — `separate` + `auth_chain_difference` (incl. the
  v2.1 conflicted subgraph), ported from synapse `state/v2.py`;
  reverse-topological power sort + iterative auth checks (IAC pass 1
  starting from empty state per v12); mainline ordering + IAC pass 2 +
  unconflicted merge → `resolve_state`. The reverse-topo sort is Kahn's
  over the auth chain (the transitive closure of each event's
  **calculated** `auth_events`, per MSC4242); only the `auth_event_ids`
  source changes from synapse (computed, not read off the wire).
- **in-memory StateProvider** — HashMap-backed, pure CPU; enables
  end-to-end testing without storage.
- **`RoomCore::apply`** — full orchestration: format → references →
  state-res → auth rules → update → effects. Never implements validation
  itself.

## v12 format-check hints

Read v12 spec rule by rule. Note: under MSC4242 the wire shape has no
`auth_events` field at all, so any auth_events-related format checks in
the v1.18 v12 spec are dead and are skipped.

- `prev_events` ≤ 20.
- `prev_state_events` must be present on every event (MSC4242).
- `m.room.create` event must **not** carry a `room_id` field (v12 rule
  1.2 — room_id is derived from the create event's reference hash with
  `$` → `!`).
- `additional_creators`, if present in `content` of create event, must
  be a JSON array of strings each passing the same user-id validation
  as `sender`.
- Every event's `room_id` must match the room_id derived from its
  create event (rule 2). This is a state-machine check, not a wire
  check — but the field-existence check (non-create events have it,
  create events don't) is.

## Style and process

Per `CLAUDE.md`:

- `cargo fmt`, `cargo clippy -- -D warnings`, `cargo test` before
  finishing any task.
- Update `PLAN.md` status checkboxes.
- Append decisions to `PLAN.md` decisions log.
- Append a 2-line summary to `LOG.md`.
- Do **not** modify `CLAUDE.md`.
- Do **not** erase lines in `LOG.md`.
- Ask before adding crates or dependencies.
- One clarifying question is better than a wrong implementation.

## Working with this user

The user (Kegan, @kegan:matrix.org) is a Matrix protocol expert at
Element. He is reading your replies on Matrix in real time. He will
catch and call out bogus / speculative claims quickly. Keep design
responses **tight and high-signal**:

- Don't echo back the user's own statements as if they were new points.
- Don't pad with low-confidence speculation. If a point is shaky, leave
  it out — five solid observations beat ten where half are wrong.
- Don't lecture on Matrix concepts he obviously knows (he's the one
  scoping the work).
- During implementation, follow the process above and report concisely
  when done.

## References

You are in a network-sandboxed environment. As of writing, only
`raw.githubusercontent.com`, `spec.matrix.org`, and Claude/Anthropic
endpoints are reachable. Plain `github.com` URLs (including PR pages)
will fail — translate to `raw.githubusercontent.com/<org>/<repo>/<branch>/<path>`.

- v12 spec: <https://spec.matrix.org/v1.18/rooms/v12/>
- Synapse state-res v2:
  <https://raw.githubusercontent.com/element-hq/synapse/refs/heads/develop/synapse/state/v2.py>
- Synapse soft-fail logic (raw):
  <https://raw.githubusercontent.com/element-hq/synapse/refs/heads/develop/synapse/handlers/federation_event.py>
  (`_check_for_soft_fail`)
- MSC4242 (State DAGs):
  <https://raw.githubusercontent.com/matrix-org/matrix-spec-proposals/refs/heads/kegan/placeholder-1/proposals/4242-state-dags.md>
  (v12 spec at spec.matrix.org does **not** yet include MSC4242 text —
  this MSC is the authoritative source for `prev_state_events`, the
  removal of `auth_events` from the wire, and the state DAG model).
- Project rules: `CLAUDE.md`
- Project plan + decisions: `PLAN.md`
- Change log: `LOG.md`

## Open questions for the user

(none at this time — MSC4242 answers all the prior ones; see decisions
log above)
