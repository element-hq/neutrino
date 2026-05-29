# `/get_missing_events` Federation Endpoint — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land `POST /_matrix/federation/v1/get_missing_events/{roomId}` — the first server-side federation endpoint on Neutrino — backed by a relaxed `DagStore::missing_events` whose contract matches the design doc and Synapse precedent.

**Architecture:** Pure read endpoint over the existing `DagStore::missing_events` BFS primitive. Storage layer relaxes from "all event IDs must exist in store" to "unknown IDs in `latest` are unreachable, unknown IDs in `earliest` are no-ops" — matching `synapse/storage/databases/main/event_federation.py:_get_missing_events`. HTTP layer adds a new `federation/` submodule mirroring the existing `sliding_sync/` layout. No origin extractor, no X-Matrix header parsing, no history-visibility filter (deferred under trusted-mesh model per `docs/get-missing-events.md`).

**Execution note:** this repository is **read-only** in the environment where the plan was written. Each task ends with a `STOP — checkpoint` step instead of a `git commit`. When the plan is run against a writeable clone, treat each checkpoint as one logical commit (files + suggested message included). Pause at every checkpoint for human review before starting the next task.

**Scope adjustment (2026-05-28, post-execution):** Originally specified 11 tasks ending in HTTP handler wire-up + e2e + bookkeeping. After Tasks 1–5 (the storage work) landed and the seed-exclusion semantic was clarified to match Synapse, the remaining HTTP-side work (Cargo feature flag + scaffold + happy-path impl + e2e + bookkeeping) was split into a follow-on plan: `docs/superpowers/plans/2026-05-28-federation-get-missing-events-http.md`. Tasks 6–11 in this file are **historical** and not part of the as-shipped Part 1 scope.

**Tech Stack:** Rust, axum 0.8, ruma (with `federation-api-s` feature added), tokio, thiserror, serde_json, tempfile (dev), tower (dev).

**Source of truth:** `/workspace/docs/get-missing-events.md` — read it first. This plan is the executable companion; the design doc owns the rationale (trust model, spec deviations, deferred items).

---

## Pre-work — read these before touching code

- `/workspace/docs/get-missing-events.md` — design doc (the *why*).
- `/workspace/crates/neutrino-store/src/lib.rs:246-271` — `DagStore` trait + current doc-comments.
- `/workspace/crates/neutrino-store-sqlite/src/store/dag.rs:90-254` — `walk_prev_events`, `validate_inputs`, the two `DagStore` impls.
- `/workspace/crates/neutrino-store-sqlite/src/store/dag.rs:850-940` — D21 + D22 tests (the ones that flip).
- `/workspace/crates/neutrino-http/src/sliding_sync/mod.rs:53-78` — `SyncError` shape (we will diverge intentionally — see Task 7).
- `/workspace/crates/neutrino-http/src/lib.rs:32-143` — `AppState`, `lock_app`, router wiring, `error_response` helper.
- `/workspace/crates/neutrino-http/tests/e2e_sliding_sync.rs` — e2e harness conventions (local `config()` / `post()` / `get()` helpers; no shared module).

## Design choices locked here (deviations from the design doc and from existing code)

1. **Storage relaxation scope.** Only `missing_events` becomes lenient. `events_before` keeps strict `validate_inputs` — its CSAPI callers know their IDs are real, and D23/D24 pin that. The refactor splits `validate_inputs` into `validate_room_exists` (used by both) and `validate_events_exist` (used by `events_before` only).

2. **Handler error pattern: `Result<Json<T>, FedError>` + `impl IntoResponse for FedError`.** Matches CLAUDE.md's "all handlers return `Result<Json<T>, AppError>`" rule. The existing `sliding_sync` module uses an older pattern (explicit `match` in `lib.rs` calling `error_response()`); we **do not refactor it** — new code follows CLAUDE.md, sliding_sync is left untouched.

3. **D21 + D22 are rewritten, not deleted.** Per CLAUDE.md "do not delete tests"; rewriting an assertion to its inverse is a contract change, not a deletion. The renamed tests align exactly with two of the new tests the design doc says we should add (§Tests A bullets 6 + 7).

4. **Plan location.** This file lives at `docs/superpowers/plans/` per the writing-plans skill default, not next to the design doc in `docs/`. If the team later prefers everything in `docs/`, move both.

## File structure

**Modified:**
- `crates/neutrino-store/src/lib.rs` — update `DagStore::missing_events` trait doc-comment (lines 260-263).
- `crates/neutrino-store-sqlite/src/store/dag.rs` — factor `validate_inputs`, adjust `missing_events`, flip D21/D22, add edge case tests.
- `crates/neutrino-http/Cargo.toml` — add `"federation-api-s"` to ruma features.
- `crates/neutrino-http/src/lib.rs` — `mod federation;`, single new `.route(...)` line.
- `complement/VIABLE-TESTS.md` — append blocked-tests row.
- `PLAN.md` — federation checkbox + decisions log entry.
- `LOG.md` — 2-line summary (append-only, bottom).

**Created:**
- `crates/neutrino-http/src/federation/mod.rs` — module root: `pub mod get_missing_events;`, `FedError` enum, `impl IntoResponse for FedError`.
- `crates/neutrino-http/src/federation/get_missing_events.rs` — handler.
- `crates/neutrino-http/tests/e2e_federation_get_missing_events.rs` — e2e tests.

---

### Task 1: Factor `validate_inputs` into two helpers (no behaviour change)

**Files:**
- Modify: `crates/neutrino-store-sqlite/src/store/dag.rs:159-214`

- [ ] **Step 1: Run the existing storage test suite as the baseline**

```
cargo test -p neutrino-store-sqlite --lib store::dag
```

Expected: all dag tests pass. Note the count — Task 2 will rely on this baseline being green.

- [ ] **Step 2: Split `validate_inputs` into `validate_room_exists` + `validate_events_exist`**

Replace lines 159-214 with two functions. `validate_inputs` is removed; its only two callers (`events_before` at line 229, `missing_events` at line 248) will be adjusted in steps 3-4 and Task 2.

```rust
/// Confirm `room_id` exists in `rooms`. Standalone so callers that
/// don't validate event IDs (federation-leaning paths like
/// `missing_events`) can still cheaply 404 on unknown rooms.
fn validate_room_exists(conn: &Connection, room_id: &RoomId) -> Result<(), Error> {
    let room_exists: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM rooms WHERE room_id = ?",
            params![room_id.as_str()],
            |row| row.get(0),
        )
        .optional()?;
    if room_exists.is_none() {
        return Err(Error::InvalidInput(format!(
            "room {room_id} does not exist"
        )));
    }
    Ok(())
}

/// Confirm every ID in `event_ids` exists in the `events` table
/// globally. *Does not* check that each event is in `room_id` — the
/// walk is scoped via [`hydrate_pdu`], so cross-room seeds produce
/// empty walk output rather than errors. Chunked to stay below
/// `SQLITE_LIMIT_VARIABLE_NUMBER`.
fn validate_events_exist(
    conn: &Connection,
    event_ids: &[&OwnedEventId],
) -> Result<(), Error> {
    if event_ids.is_empty() {
        return Ok(());
    }
    let unique: Vec<&str> = {
        let mut set: HashSet<&str> = HashSet::with_capacity(event_ids.len());
        for id in event_ids {
            set.insert(id.as_str());
        }
        set.into_iter().collect()
    };

    let mut found: HashSet<String> = HashSet::with_capacity(unique.len());
    for window in unique.chunks(VALIDATE_INPUTS_CHUNK) {
        let placeholders = vec!["?"; window.len()].join(",");
        let query = format!("SELECT event_id FROM events WHERE event_id IN ({placeholders})");
        let mut stmt = conn.prepare(&query)?;
        let rows = stmt.query_map(params_from_iter(window.iter()), |row| {
            row.get::<_, String>(0)
        })?;
        for row in rows {
            found.insert(row?);
        }
    }

    for id in event_ids {
        if !found.contains(id.as_str()) {
            return Err(Error::InvalidInput(format!(
                "event {id} does not exist in the store"
            )));
        }
    }
    Ok(())
}
```

- [ ] **Step 3: Update `events_before` (line ~229) to call both helpers**

```rust
self.run_read(move |conn| -> Result<Vec<Event>, Error> {
    validate_room_exists(conn, &room_id)?;
    let id_refs: Vec<&OwnedEventId> = from.iter().collect();
    validate_events_exist(conn, &id_refs)?;
    walk_prev_events(conn, &room_id, from, &HashSet::new(), limit)
})
.await
```

- [ ] **Step 4: Update `missing_events` (line ~246) — still calls both helpers, behaviour-preserving**

```rust
self.run_read(move |conn| -> Result<Vec<Event>, Error> {
    validate_room_exists(conn, &room_id)?;
    let id_refs: Vec<&OwnedEventId> = latest.iter().chain(earliest.iter()).collect();
    validate_events_exist(conn, &id_refs)?;
    let earliest_set: HashSet<OwnedEventId> = earliest.into_iter().collect();
    walk_prev_events(conn, &room_id, latest, &earliest_set, limit)
})
.await
```

Task 2 will drop the `validate_events_exist` call here.

- [ ] **Step 5: Verify zero regression**

```
cargo clippy -p neutrino-store-sqlite --tests -- -D warnings
cargo test -p neutrino-store-sqlite --lib store::dag
```

Expected: same test count green as Step 1. No warnings.

- [ ] **Step 6: STOP — checkpoint**

Files in this checkpoint:
- `crates/neutrino-store-sqlite/src/store/dag.rs`

Suggested commit message:
> refactor(store-sqlite): split validate_inputs into room + events helpers

**Pause here for review before starting Task 2.**

---

### Task 2: Relax `missing_events`; flip D21 + D22 (behaviour change)

**Files:**
- Modify: `crates/neutrino-store-sqlite/src/store/dag.rs` — `missing_events` impl + D21 (line ~853) + D22 (line ~923)

- [ ] **Step 1: Flip D21 — unknown latest now returns empty, not InvalidInput**

Replace the D21 test at lines 850-863. The new test asserts `Ok(empty)` and matches the design doc's `missing_events_unknown_latest_returns_empty`.

```rust
// D21 (was: missing_latest_returns_invalid_input): with the federation-
// leaning contract on `missing_events`, an unknown `latest_events`
// reference is treated as a starting point with no parents in the
// local store — the walk produces no events. This mirrors Synapse's
// `_get_missing_events` (storage/databases/main/event_federation.py),
// which performs no existence pre-check.
#[tokio::test]
async fn missing_events_unknown_latest_returns_empty() {
    let s = store_with_room().await;
    let got = s
        .missing_events(*ALICE_ROOM_ID, &[event_id!("$nope:e")], &[], 10)
        .await
        .expect("unknown latest must not error");
    assert!(got.is_empty(), "expected empty result, got {got:?}");
}
```

- [ ] **Step 2: Flip D22 — unknown earliest is a no-op, walk proceeds**

Replace the D22 test at lines 920-940. Asserts the real `latest` event still walks, ignoring the bogus `earliest`.

```rust
// D22 (was: missing_earliest_returns_invalid_input): an unknown
// `earliest_events` reference is a no-op — no walked event will ever
// match it, so the walk proceeds as if `earliest` were empty.
#[tokio::test]
async fn missing_events_unknown_earliest_ignored() {
    let s = store_with_room().await;
    let ev_a = message_with_prev(*ALICE_ROOM_ID, *ALICE_USER_ID, "a", &[]);
    let id_a = ev_a.event_id.clone();
    s.persist_event(&ev_a, &[]).await.unwrap();

    let got = s
        .missing_events(*ALICE_ROOM_ID, &[&id_a], &[event_id!("$nope:e")], 10)
        .await
        .expect("unknown earliest must not error");
    assert_eq!(got.len(), 1, "walk should still return real latest");
    assert_eq!(got[0].event_id, id_a);
}
```

- [ ] **Step 3: Run the two flipped tests — they must fail (red)**

```
cargo test -p neutrino-store-sqlite --lib \
  store::dag::tests::missing_events_unknown_latest_returns_empty \
  store::dag::tests::missing_events_unknown_earliest_ignored
```

Expected: **both fail** — the impl still calls `validate_events_exist`, which rejects unknown IDs with `InvalidInput`.

- [ ] **Step 4: Drop the `validate_events_exist` call from `missing_events`**

In `missing_events`, replace the body with:

```rust
self.run_read(move |conn| -> Result<Vec<Event>, Error> {
    validate_room_exists(conn, &room_id)?;
    let earliest_set: HashSet<OwnedEventId> = earliest.into_iter().collect();
    walk_prev_events(conn, &room_id, latest, &earliest_set, limit)
})
.await
```

The walker already tolerates unknown IDs gracefully: `hydrate_pdu` returns `None` for absent or cross-room IDs (line 122-126), the loop `continue`s.

- [ ] **Step 5: Run the two flipped tests — both green**

```
cargo test -p neutrino-store-sqlite --lib \
  store::dag::tests::missing_events_unknown_latest_returns_empty \
  store::dag::tests::missing_events_unknown_earliest_ignored
```

Expected: PASS.

- [ ] **Step 6: Run the full dag test suite — `events_before` strict path still works**

```
cargo test -p neutrino-store-sqlite --lib store::dag
```

Expected: all tests pass. Specifically, D23 + D24 (`events_before_validates_inputs_in_chunks`, `events_before_chunked_validation_rejects_missing`) remain green — they test `events_before`, which still calls `validate_events_exist`.

- [ ] **Step 7: STOP — checkpoint**

Files in this checkpoint:
- `crates/neutrino-store-sqlite/src/store/dag.rs`

Suggested commit message:
> feat(store-sqlite): relax missing_events to tolerate unknown event IDs
>
> Unknown IDs in `latest` produce an empty walk (no reachable parents);
> unknown IDs in `earliest` are no-ops. Matches Synapse and the
> federation /get_missing_events contract in docs/get-missing-events.md.
> `events_before` retains strict validation for internal CSAPI callers.
>
> D21 + D22 flipped to assert the new contract.

**Pause here for review before starting Task 3.** This is a contract change — reviewer should explicitly OK the flipped tests.

---

### Task 3: Update `DagStore::missing_events` trait doc-comment

**Files:**
- Modify: `crates/neutrino-store/src/lib.rs:260-263`

- [ ] **Step 1: Replace the doc-comment**

```rust
/// Pre:  `room_id` must exist in the store. Event IDs in `latest` and
///       `earliest` need not exist; unknown IDs in `latest` are treated
///       as starting points with no reachable parents (contribute
///       nothing to the result), unknown IDs in `earliest` are no-ops
///       on the walk.
/// Post: BFS over `prev_events` starting from `latest`, skipping any
///       event in `earliest`; returns at most `limit` events; events
///       in `earliest` are never included in the result. Events in
///       other rooms (cross-room seeds or corrupt `event_edges`) are
///       treated as if they don't exist — the walk terminates at the
///       boundary rather than leaking PDUs from another room.
async fn missing_events(
    &self,
    room_id: &RoomId,
    latest: &[&EventId],
    earliest: &[&EventId],
    limit: usize,
) -> Result<Vec<Event>, StorageError>;
```

- [ ] **Step 2: Verify build**

```
cargo check -p neutrino-store
```

Expected: clean.

- [ ] **Step 3: STOP — checkpoint**

Files in this checkpoint:
- `crates/neutrino-store/src/lib.rs`

Suggested commit message:
> docs(store): document missing_events lenient contract

**Pause here before starting Task 4.**

---

### Task 4: Add storage edge-case tests (wide fan-out, limit=1)

**Files:**
- Modify: `crates/neutrino-store-sqlite/src/store/dag.rs` — append two tests to the existing `mod tests`

The design doc lists 7 edge cases. After Task 2: cycle ✅ D6, missing-parent ✅ D5, earliest-boundary ✅ D7, unknown-earliest ✅ flipped D22, unknown-latest ✅ flipped D21. Gaps: wide fan-out, `limit = 1`.

- [ ] **Step 1: Write `missing_events_wide_fanout`**

Look up the existing `message_with_prev` / `make_event` helpers in the test module (around line 257+) and use them to build a DAG with one node referencing 50 prev_events. The walk must visit them all up to `limit`.

```rust
// D25: BFS frontier survives a single node fanning out to 50 parents.
// Catches a regression where the frontier `VecDeque` is bounded or
// `fetch_edges` truncates results.
#[tokio::test]
async fn missing_events_wide_fanout() {
    let s = store_with_room().await;

    // Seed 50 leaf events with distinct timestamps so each has a
    // distinct computed event_id.
    let mut leaf_ids: Vec<OwnedEventId> = Vec::with_capacity(50);
    for i in 0..50u64 {
        let ev = crate::tests::message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "leaf", i);
        leaf_ids.push(ev.event_id.clone());
        s.persist_event(&ev, &[]).await.unwrap();
    }

    // One node with all 50 as prev_events.
    let leaf_refs: Vec<&EventId> = leaf_ids.iter().map(|id| id.as_ref()).collect();
    let head = crate::tests::message_with_prev(*ALICE_ROOM_ID, *ALICE_USER_ID, "head", &leaf_refs);
    let head_id = head.event_id.clone();
    s.persist_event(&head, &[]).await.unwrap();

    let got = s
        .missing_events(*ALICE_ROOM_ID, &[&head_id], &[], 100)
        .await
        .unwrap();

    // Result is head + all 50 leaves = 51 events. Order is BFS:
    // head first, then the 50 leaves in some `event_edges`-sorted
    // order — we don't pin order, only cardinality.
    assert_eq!(got.len(), 51);
}
```

- [ ] **Step 2: Write `missing_events_limit_one`**

```rust
// D26: limit=1 returns exactly one event — the first BFS hit (the
// `latest` itself). Boundary test between D16 (limit=0 → empty) and
// the unbounded case.
#[tokio::test]
async fn missing_events_limit_one() {
    let s = store_with_room().await;
    let ev_a = message_with_prev(*ALICE_ROOM_ID, *ALICE_USER_ID, "a", &[]);
    let id_a = ev_a.event_id.clone();
    s.persist_event(&ev_a, &[]).await.unwrap();
    let ev_b = message_with_prev(*ALICE_ROOM_ID, *ALICE_USER_ID, "b", &[&id_a]);
    let id_b = ev_b.event_id.clone();
    s.persist_event(&ev_b, &[]).await.unwrap();

    let got = s
        .missing_events(*ALICE_ROOM_ID, &[&id_b], &[], 1)
        .await
        .unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].event_id, id_b);
}
```

- [ ] **Step 3: Run both new tests — expect green (no impl change required)**

```
cargo test -p neutrino-store-sqlite --lib \
  store::dag::tests::missing_events_wide_fanout \
  store::dag::tests::missing_events_limit_one
```

Expected: PASS for both. The existing walker handles both cases correctly.

- [ ] **Step 4: STOP — checkpoint**

Files in this checkpoint:
- `crates/neutrino-store-sqlite/src/store/dag.rs`

Suggested commit message:
> test(store-sqlite): wide-fanout + limit=1 coverage for missing_events

**Pause here before starting Task 5.**

---

### Task 5: Synapse "ports" — reality check, then one fan-out port

**Files:**
- Modify: `crates/neutrino-store-sqlite/src/store/dag.rs` — append one test
- Update: `docs/get-missing-events.md` — correct the §Tests A table

Reading the actual Synapse sources (`tests/storage/test_event_federation.py` lines 389, 729, 1161) reveals the design doc overstates the portability of these tests:

| Synapse test | Reality |
|---|---|
| `test_get_backfill_points_in_room` (line 1161) | Tests `store.get_backfill_points_in_room()`, not `get_missing_events`. Different primitive entirely. The *DAG shape* is reusable as a missing_events fixture; the *assertions* are not. |
| `test_conflicted_subgraph` (line 729) | Tests state-res v2.1 chain-walking via `chain_cover_index` tables (`db_pool.simple_insert` calls into Synapse-internal tables). Not a `missing_events` test at all. |
| `test_auth_chain_ids` (line 389) | Tests auth-chain walking (`auth_events` direction). Not `prev_events` traversal. |

Design doc §Tests A claims "✅ Clean port" for the first and "🟡 Re-implement" for the other two. The honest position is: the first needs partial reframing (DAG shape only, assertions invented fresh), and the other two are inspiration at best.

**Decision for this plan:** port only `test_get_backfill_points_in_room` (DAG shape, fresh assertions). Drop the other two. Update the design doc to match what we actually did.

- [ ] **Step 1: Read the Synapse setup helper for the fan-out DAG**

```
grep -n "_setup_room_for_backfill_tests" /workspace/synapse-main/tests/storage/test_event_federation.py
```

Read the helper body. Its purpose: build a DAG with one "main" branch and several "back-extremity" branches converging on the main spine, with depth used for ordering.

- [ ] **Step 2: Write `missing_events_backfill_fanout_origin_ts_ordering`**

Build the same DAG shape, replacing `depth` with `origin_server_ts`. Use `message_with_ts` for distinct event IDs.

```rust
// D27: Synapse `test_get_backfill_points_in_room` DAG shape, ported.
// Main spine (`m0 ← m1 ← m2`) with three back-extremity branches
// (`b1`, `b2`, `b3`) each branching from `m0`. With `origin_server_ts`
// as the ordering signal (depth removed per PLAN.md 2026-05-22), a
// BFS from the spine head returns the spine + branches; the test
// pins cardinality and presence rather than exact order — the BFS
// order is implementation-internal.
#[tokio::test]
async fn missing_events_backfill_fanout_origin_ts_ordering() {
    let s = store_with_room().await;

    let m0 = message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "m0", 1);
    let id_m0 = m0.event_id.clone();
    s.persist_event(&m0, &[]).await.unwrap();

    let m1 = message_with_prev_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "m1", &[&id_m0], 5);
    let id_m1 = m1.event_id.clone();
    s.persist_event(&m1, &[]).await.unwrap();

    let m2 = message_with_prev_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "m2", &[&id_m1], 10);
    let id_m2 = m2.event_id.clone();
    s.persist_event(&m2, &[]).await.unwrap();

    // Three back-extremity branches off m0
    let b1 = message_with_prev_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "b1", &[&id_m0], 2);
    let b2 = message_with_prev_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "b2", &[&id_m0], 3);
    let b3 = message_with_prev_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "b3", &[&id_m0], 4);
    s.persist_event(&b1, &[]).await.unwrap();
    s.persist_event(&b2, &[]).await.unwrap();
    s.persist_event(&b3, &[]).await.unwrap();

    // Walk from spine head; expect to see all 6 events (m2, m1, m0, b1, b2, b3).
    // The branches share m0 as their only parent, so BFS visits m0 once.
    let got = s
        .missing_events(*ALICE_ROOM_ID, &[&id_m2], &[], 100)
        .await
        .unwrap();

    let returned_ids: HashSet<OwnedEventId> =
        got.iter().map(|e| e.event_id.clone()).collect();
    assert_eq!(returned_ids.len(), 3, "spine-only walk; branches are forward of m0, not reachable backward from m2");
    assert!(returned_ids.contains(&id_m2));
    assert!(returned_ids.contains(&id_m1));
    assert!(returned_ids.contains(&id_m0));
}
```

*Helper note:* if `message_with_prev_ts` doesn't exist (only `message_with_prev` and `message_with_ts`), add a small helper variant in the test module:

```rust
fn message_with_prev_ts(
    room: &RoomId,
    user: &UserId,
    body: &str,
    prev: &[&EventId],
    ts: u64,
) -> Event {
    // Combine the message_with_prev + message_with_ts logic — copy from
    // the existing builders and parameterize both prev and ts.
}
```

*Honest caveat:* I expect this test as written to pass (cardinality 3 from spine, branches off `m0` are forward of it and unreachable backward from `m2`). If when run the result surprises you (e.g., the walker for some reason returns branches), investigate before adjusting the assertion — that would be a real finding.

- [ ] **Step 3: Run the new test**

```
cargo test -p neutrino-store-sqlite --lib \
  store::dag::tests::missing_events_backfill_fanout_origin_ts_ordering
```

Expected: PASS. If FAIL: report what actually came back — the assertion may need adjusting based on the walker's real behaviour (e.g., if BFS visits both directions for some reason). **Do not adjust silently** — surface the finding.

- [ ] **Step 4: Update the design doc §Tests A table to match what we actually ported**

Edit `/workspace/docs/get-missing-events.md` §Tests A. Replace the 3-row table with:

```markdown
| Synapse test                          | Status                                                          |
|---------------------------------------|-----------------------------------------------------------------|
| `test_get_backfill_points_in_room`    | Inspiration only. We ported the **DAG shape** (main spine + back-extremity branches) as `missing_events_backfill_fanout_origin_ts_ordering`; assertions are fresh because Synapse's test is over a different primitive (`get_backfill_points_in_room`). |
| `test_conflicted_subgraph`            | Not ported — Synapse-specific (`chain_cover_index` tables, not relevant to `missing_events`). |
| `test_auth_chain_ids`                 | Not ported — tests `auth_events` traversal, not `prev_events`.  |
```

- [ ] **Step 5: STOP — checkpoint**

Files in this checkpoint:
- `crates/neutrino-store-sqlite/src/store/dag.rs`
- `docs/get-missing-events.md`

Suggested commit message:
> test(store-sqlite): fan-out DAG inspired by Synapse backfill test
>
> Ports the DAG shape from synapse `test_get_backfill_points_in_room`
> with origin_server_ts replacing depth. The other two Synapse tests
> flagged in the design doc turned out to be over different primitives
> (chain_cover_index, auth_events); design doc table updated to match.

**Pause here before starting Task 6.** This checkpoint also amends the design doc — reviewer should sanity-check the §Tests A table edit.

---

### Task 6: Enable the `federation-api-s` ruma feature

**Files:**
- Modify: `crates/neutrino-http/Cargo.toml:18`

- [ ] **Step 1: Add the feature flag**

```toml
ruma = { workspace = true, features = ["client-api-s", "unstable-msc4186", "federation-api-s"] }
```

- [ ] **Step 2: Verify the federation types resolve**

```
cargo check -p neutrino-http
```

Then verify the specific module is importable. Create a scratch file or use `cargo expand`:

```
cargo check -p neutrino-http 2>&1 | grep -i federation
```

Expected: no errors. The path `ruma::api::federation::event::get_missing_events::v1::{Request, Response}` should be available.

- [ ] **Step 3: STOP — checkpoint**

Files in this checkpoint:
- `crates/neutrino-http/Cargo.toml`

Suggested commit message:
> feat(http): enable ruma federation-api-s feature

**Pause here before starting Task 7.**

---

### Task 7: Bootstrap `federation/` module — `FedError`, `IntoResponse`, route registration

**Files:**
- Create: `crates/neutrino-http/src/federation/mod.rs`
- Create: `crates/neutrino-http/src/federation/get_missing_events.rs`
- Modify: `crates/neutrino-http/src/lib.rs` (add `mod federation;` + one route line)

This task lands the wiring and a *minimal* handler that returns 404 for unknown rooms and 400 for bad requests. Happy-path body is empty in this task; Task 8 fills it.

- [ ] **Step 1: Create `federation/mod.rs`**

```rust
//! Server-Server federation endpoints.
//!
//! Mesh-trusted layout: no X-Matrix header parsing, no signature
//! verification, no history-visibility filter. See
//! `docs/get-missing-events.md` for the full deviation list.

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use neutrino_store::StorageError;
use serde_json::json;

pub mod get_missing_events;

#[derive(Debug, thiserror::Error)]
pub enum FedError {
    #[error("bad request: {0}")]
    BadRequest(&'static str),
    #[error("room not found")]
    RoomNotFound,
    #[error("storage: {0}")]
    Storage(#[from] StorageError),
}

impl IntoResponse for FedError {
    fn into_response(self) -> Response {
        let (status, errcode, msg) = match &self {
            FedError::BadRequest(msg) => (StatusCode::BAD_REQUEST, "M_INVALID_PARAM", (*msg).to_owned()),
            FedError::RoomNotFound => {
                (StatusCode::NOT_FOUND, "M_NOT_FOUND", "room not found".to_owned())
            }
            FedError::Storage(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                e.to_string(),
            ),
        };
        (status, Json(json!({"errcode": errcode, "error": msg}))).into_response()
    }
}
```

- [ ] **Step 2: Create `federation/get_missing_events.rs` — skeleton only**

```rust
//! `POST /_matrix/federation/v1/get_missing_events/{roomId}`
//!
//! See `docs/get-missing-events.md` for the algorithm and the
//! trusted-mesh spec deviations.

use axum::{
    Json,
    extract::{Path, State},
};
use neutrino_store::{DagStore, RoomStore};
use ruma::{
    OwnedRoomId,
    api::federation::event::get_missing_events::v1::{Request, Response},
};

use crate::{AppState, federation::FedError, lock_app};

const DEFAULT_LIMIT: u64 = 10;
const MAX_LIMIT: u64 = 20;

pub async fn handle(
    State(state): State<AppState>,
    Path(room_id): Path<OwnedRoomId>,
    Json(req): Json<Request>,
) -> Result<Json<Response>, FedError> {
    if req.latest_events.is_empty() {
        return Err(FedError::BadRequest("latest_events must not be empty"));
    }

    let store = {
        let app = lock_app(&state.0);
        app.store.clone()
    };

    // 404 if room is unknown. `min_depth` is intentionally ignored:
    // Neutrino has no depth column (PLAN.md 2026-05-22 decision).
    if store.get_room_version(&room_id).await?.is_none() {
        return Err(FedError::RoomNotFound);
    }

    let _limit = req
        .limit
        .map(|l| l.min(MAX_LIMIT))
        .unwrap_or(DEFAULT_LIMIT);

    // Task 8 fills this in.
    Ok(Json(Response::new(Vec::new())))
}
```

Note: the exact ruma `Request` field names may be `limit: Option<UInt>` or similar — adjust per `cargo check` feedback. The handler shape is correct; field accessors are the only thing that may need tweaks.

- [ ] **Step 3: Wire the module and route in `lib.rs`**

In `crates/neutrino-http/src/lib.rs`, add `mod federation;` near the existing module declarations, and add one route line in the `router()` builder (look for the existing `.route(...)` chain around line 100-140):

```rust
mod federation;
```

```rust
.route(
    "/_matrix/federation/v1/get_missing_events/{room_id}",
    post(federation::get_missing_events::handle),
)
```

- [ ] **Step 4: Build clean**

```
cargo clippy -p neutrino-http --tests -- -D warnings
```

Expected: no errors, no warnings. If ruma `Request` field names differ from the skeleton, fix the field accessors here.

- [ ] **Step 5: STOP — checkpoint**

Files in this checkpoint:
- `crates/neutrino-http/src/federation/mod.rs` *(new)*
- `crates/neutrino-http/src/federation/get_missing_events.rs` *(new)*
- `crates/neutrino-http/src/lib.rs`

Suggested commit message:
> feat(http): scaffold federation/get_missing_events handler
>
> Routes POST /_matrix/federation/v1/get_missing_events/{room_id} with
> FedError + IntoResponse mapping (M_INVALID_PARAM/M_NOT_FOUND/M_UNKNOWN).
> Happy-path body is empty; filled in next commit.

**Pause here before starting Task 8.** First federation-shaped handler in the tree — reviewer should sanity-check the module layout and the FedError ↔ M_*errcode mapping.

---

### Task 8: Implement the happy path

**Files:**
- Modify: `crates/neutrino-http/src/federation/get_missing_events.rs`

- [ ] **Step 1: Replace the placeholder happy-path with the actual walk**

In `handle`, after the limit clamp, call `DagStore::missing_events` and return the events:

```rust
let limit = req
    .limit
    .map(|l| (l.min(MAX_LIMIT)) as usize)
    .unwrap_or(DEFAULT_LIMIT as usize);

let latest: Vec<&ruma::EventId> = req.latest_events.iter().map(|id| id.as_ref()).collect();
let earliest: Vec<&ruma::EventId> = req.earliest_events.iter().map(|id| id.as_ref()).collect();

let events = store
    .missing_events(&room_id, &latest, &earliest, limit)
    .await?;

// Federation peers receive Event.raw verbatim — NO event_view
// enrichment. The bytes must be exactly what the reference hash
// was computed over. See docs/event-view-conversions.md and
// docs/get-missing-events.md §"Algorithm" step 6.
let raw_events: Vec<Box<serde_json::value::RawValue>> =
    events.into_iter().map(|e| e.raw).collect();

Ok(Json(Response::new(raw_events)))
```

Field names on `Response::new` may differ — check `cargo check` output. The intent is `Vec<Box<RawJsonValue>>` per the design doc.

- [ ] **Step 2: Build clean**

```
cargo clippy -p neutrino-http --tests -- -D warnings
```

- [ ] **Step 3: STOP — checkpoint** (no test yet — Task 9 covers e2e)

Files in this checkpoint:
- `crates/neutrino-http/src/federation/get_missing_events.rs`

Suggested commit message:
> feat(http): implement /get_missing_events happy path

**Pause here before starting Task 9.**

---

### Task 9: E2E test file — all 11 cases from the design doc

**Files:**
- Create: `crates/neutrino-http/tests/e2e_federation_get_missing_events.rs`

Mirror the conventions in `tests/e2e_sliding_sync.rs`: local `config()` + `post()` helpers, no shared module, events seeded through CSAPI endpoints (`POST /createRoom`, `PUT /send/...`).

The design doc §Tests B lists 11 e2e tests. Group into bad-request, happy-path, edge, and pinning categories. Write all tests, then run; tests for already-implemented behaviour should pass without further code changes.

- [ ] **Step 1: Test-file scaffolding (local helpers)**

```rust
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use neutrino_http::{Config, router};
use serde_json::{Value, json};
use tower::ServiceExt;

fn config() -> Config {
    Config {
        server_name: "example.org".to_string(),
        bind_addr: "127.0.0.1:0".to_string(),
        localpart: "alice".to_string(),
    }
}

async fn post_fed(
    app: &Router,
    path: &str,
    body: &Value,
) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let value: Value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, value)
}

async fn post_raw(
    app: &Router,
    path: &str,
    body: Body,
    content_type: Option<&str>,
) -> (StatusCode, Value) {
    let mut req = Request::builder().method("POST").uri(path);
    if let Some(ct) = content_type {
        req = req.header("content-type", ct);
    }
    let resp = app.clone().oneshot(req.body(body).unwrap()).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let value: Value = if bytes.is_empty() { Value::Null } else { serde_json::from_slice(&bytes).unwrap() };
    (status, value)
}

/// Returns (room_id, vec of event_ids in send order).
async fn seed_room_with_messages(app: &Router, count: usize) -> (String, Vec<String>) {
    let (_, body) = post_fed(app, "/_matrix/client/v3/createRoom", &json!({})).await;
    let room_id = body["room_id"].as_str().unwrap().to_string();
    let mut event_ids = Vec::with_capacity(count);
    for i in 0..count {
        let path = format!(
            "/_matrix/client/v3/rooms/{room_id}/send/m.room.message/txn-{i}"
        );
        let (_, resp) = post_fed(app, &path, &json!({"body": format!("msg-{i}"), "msgtype": "m.text"})).await;
        // existing handler returns event_id; if it returns it under a
        // different key, adjust here. Check e2e_sliding_sync.rs for the
        // existing read pattern.
        event_ids.push(resp["event_id"].as_str().unwrap().to_string());
    }
    (room_id, event_ids)
}

fn fed_path(room_id: &str) -> String {
    format!("/_matrix/federation/v1/get_missing_events/{room_id}")
}
```

*Caveat:* `seed_room_with_messages` uses `PUT` semantics in real Matrix; check whether the existing CSAPI `send` handler is `POST` or `PUT` and adjust. Per the subagent investigation, e2e_sliding_sync.rs uses `put()` for sends — copy that helper rather than re-invent.

- [ ] **Step 2: Bad-request tests (3)**

```rust
#[tokio::test]
async fn bad_request_empty_latest_events_returns_400() {
    let app = router(config()).await.expect("router init");
    let (room_id, _) = seed_room_with_messages(&app, 1).await;
    let (status, body) = post_fed(
        &app,
        &fed_path(&room_id),
        &json!({"earliest_events": [], "latest_events": []}),
    ).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["errcode"], "M_INVALID_PARAM");
}

#[tokio::test]
async fn bad_request_non_json_body_returns_400() {
    let app = router(config()).await.expect("router init");
    let (room_id, _) = seed_room_with_messages(&app, 1).await;
    let (status, _) = post_raw(
        &app,
        &fed_path(&room_id),
        Body::from("not json"),
        Some("application/json"),
    ).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn bad_request_missing_required_field_returns_400() {
    let app = router(config()).await.expect("router init");
    let (room_id, _) = seed_room_with_messages(&app, 1).await;
    let (status, _) = post_fed(
        &app,
        &fed_path(&room_id),
        &json!({"earliest_events": []}), // latest_events missing
    ).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}
```

- [ ] **Step 3: 404 test**

```rust
#[tokio::test]
async fn unknown_room_returns_404() {
    let app = router(config()).await.expect("router init");
    let (status, body) = post_fed(
        &app,
        &fed_path("!nope:example.org"),
        &json!({"earliest_events": [], "latest_events": ["$x:example.org"]}),
    ).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["errcode"], "M_NOT_FOUND");
}
```

- [ ] **Step 4: Happy path test**

```rust
#[tokio::test]
async fn happy_path_returns_events_between_earliest_and_latest() {
    let app = router(config()).await.expect("router init");
    let (room_id, event_ids) = seed_room_with_messages(&app, 5).await;
    // gap = events 1..=4 between event_ids[0] (earliest) and event_ids[4] (latest)
    let (status, body) = post_fed(
        &app,
        &fed_path(&room_id),
        &json!({
            "earliest_events": [&event_ids[0]],
            "latest_events": [&event_ids[4]],
            "limit": 20,
        }),
    ).await;
    assert_eq!(status, StatusCode::OK);
    let events = body["events"].as_array().expect("events array");
    // Walker returns BFS order; we don't pin order here, only that
    // the four expected events are present and the earliest is not.
    let returned_ids: Vec<&str> = events
        .iter()
        .map(|e| e["event_id"].as_str().unwrap_or(""))
        .collect();
    // wire-bytes passthrough: event_id should NOT be in the raw JSON.
    // This test asserts the gap content via prev_events chains instead.
    // See `wire_bytes_passthrough` test below for the explicit check.
    assert_eq!(events.len(), 4, "expected 4 events in gap, got {events:?}");
    for unexpected in &[&event_ids[0]] {
        assert!(
            !returned_ids.iter().any(|id| id == unexpected.as_str()),
            "earliest must not be in response"
        );
    }
}
```

Note: assertion on `event_id` absence is in `wire_bytes_passthrough`; this test relies on cardinality + counting non-earliest events. Adjust once you see what the create-room CSAPI seeds in addition (create event, join event, etc. — likely there's 1-2 extra state events in the room beyond the message chain).

- [ ] **Step 5: Limit tests (2)**

```rust
#[tokio::test]
async fn respects_limit() {
    let app = router(config()).await.expect("router init");
    let (room_id, event_ids) = seed_room_with_messages(&app, 30).await;
    let (status, body) = post_fed(
        &app,
        &fed_path(&room_id),
        &json!({
            "earliest_events": [],
            "latest_events": [&event_ids[29]],
            "limit": 50,
        }),
    ).await;
    assert_eq!(status, StatusCode::OK);
    let events = body["events"].as_array().unwrap();
    assert!(events.len() <= 20, "max cap is 20, got {}", events.len());
}

#[tokio::test]
async fn default_limit_is_10() {
    let app = router(config()).await.expect("router init");
    let (room_id, event_ids) = seed_room_with_messages(&app, 30).await;
    let (status, body) = post_fed(
        &app,
        &fed_path(&room_id),
        &json!({
            "earliest_events": [],
            "latest_events": [&event_ids[29]],
            // no limit field
        }),
    ).await;
    assert_eq!(status, StatusCode::OK);
    let events = body["events"].as_array().unwrap();
    assert_eq!(events.len(), 10);
}
```

- [ ] **Step 6: Edge case tests (3)**

```rust
#[tokio::test]
async fn empty_earliest_walks_back_to_room_root() {
    let app = router(config()).await.expect("router init");
    let (room_id, event_ids) = seed_room_with_messages(&app, 3).await;
    let (status, body) = post_fed(
        &app,
        &fed_path(&room_id),
        &json!({
            "earliest_events": [],
            "latest_events": [&event_ids[2]],
            "limit": 20,
        }),
    ).await;
    assert_eq!(status, StatusCode::OK);
    let events = body["events"].as_array().unwrap();
    // Should reach the m.room.create event. We don't know its id
    // upfront — assert by event_type via JSON inspection.
    let has_create = events.iter().any(|e| e["type"] == "m.room.create");
    assert!(has_create, "walk must reach m.room.create from empty earliest");
}

#[tokio::test]
async fn latest_event_not_in_room_returns_empty() {
    let app = router(config()).await.expect("router init");
    let (room_id, _) = seed_room_with_messages(&app, 1).await;
    let (status, body) = post_fed(
        &app,
        &fed_path(&room_id),
        &json!({
            "earliest_events": [],
            "latest_events": ["$totally_fabricated:example.org"],
            "limit": 20,
        }),
    ).await;
    assert_eq!(status, StatusCode::OK);
    let events = body["events"].as_array().unwrap();
    assert!(events.is_empty(), "fabricated latest should produce empty walk");
}

#[tokio::test]
async fn min_depth_field_ignored() {
    let app = router(config()).await.expect("router init");
    let (room_id, event_ids) = seed_room_with_messages(&app, 3).await;
    let body_no_min = json!({
        "earliest_events": [],
        "latest_events": [&event_ids[2]],
        "limit": 20,
    });
    let body_with_min = json!({
        "earliest_events": [],
        "latest_events": [&event_ids[2]],
        "limit": 20,
        "min_depth": 999_999,
    });
    let (status1, resp1) = post_fed(&app, &fed_path(&room_id), &body_no_min).await;
    let (status2, resp2) = post_fed(&app, &fed_path(&room_id), &body_with_min).await;
    assert_eq!(status1, StatusCode::OK);
    assert_eq!(status2, StatusCode::OK);
    assert_eq!(
        resp1["events"].as_array().unwrap().len(),
        resp2["events"].as_array().unwrap().len(),
        "min_depth must be ignored: same result with or without it"
    );
}
```

- [ ] **Step 7: Wire-bytes passthrough test**

```rust
#[tokio::test]
async fn wire_bytes_passthrough() {
    let app = router(config()).await.expect("router init");
    let (room_id, event_ids) = seed_room_with_messages(&app, 2).await;
    let (status, body) = post_fed(
        &app,
        &fed_path(&room_id),
        &json!({
            "earliest_events": [],
            "latest_events": [&event_ids[1]],
            "limit": 20,
        }),
    ).await;
    assert_eq!(status, StatusCode::OK);
    let events = body["events"].as_array().unwrap();
    assert!(!events.is_empty(), "should return at least one event");
    for ev in events {
        let obj = ev.as_object().expect("each event is a JSON object");
        assert!(
            !obj.contains_key("event_id"),
            "federation events must ship verbatim — no event_id enrichment. \
             event_view::enrich_for_client must NOT be called on federation path. \
             Found event_id in: {ev}"
        );
    }
}
```

- [ ] **Step 8: Run the full e2e suite for this file**

```
cargo test -p neutrino-http --test e2e_federation_get_missing_events
```

Expected: all 11 tests pass. If a test fails, investigate root cause before adjusting either the impl or the assertion. **Do not soften assertions to make tests pass.**

- [ ] **Step 9: STOP — checkpoint**

Files in this checkpoint:
- `crates/neutrino-http/tests/e2e_federation_get_missing_events.rs` *(new)*

Suggested commit message:
> test(http): e2e coverage for /get_missing_events (11 tests)

**Pause here before starting Task 10.** All 11 design-doc e2e cases should be green at this point — reviewer should confirm coverage matches the §Tests B table in the design doc.

---

### Task 10: Complement VIABLE-TESTS row

**Files:**
- Modify: `complement/VIABLE-TESTS.md`

- [ ] **Step 1: Append a section documenting the two blocked tests**

At the end of `VIABLE-TESTS.md`, add:

```markdown
## Federation `/get_missing_events` — blocked

| Test | Block on |
|---|---|
| `federation/TestInboundCanReturnMissingEvents` | Phase 4b/4c (state-res) + Phase 6 (`send_join` accept) — requires a federated join before the endpoint is exercised. Also asserts history-visibility redaction, which we defer under the trusted-mesh model (see `docs/get-missing-events.md`). |
| `federation/TestGetMissingEventsGapFilling` | Phase 4b/4c (state-res) — outbound test; SUT must receive a federation `/send`, detect a gap, and call out to `/get_missing_events`. Needs state-res to integrate the response. |

Neither is added to `complement/allowlist.txt`.
```

- [ ] **Step 2: STOP — checkpoint**

Files in this checkpoint:
- `complement/VIABLE-TESTS.md`

Suggested commit message:
> docs(complement): document federation /get_missing_events blocked tests

**Pause here before starting Task 11.**

---

### Task 11: Project bookkeeping — PLAN.md + LOG.md

**Files:**
- Modify: `PLAN.md` — tick the federation `/get_missing_events` checkbox, add decisions log entry
- Modify: `LOG.md` — append a 2-line summary at the bottom

- [ ] **Step 1: Find the federation `/get_missing_events` checkbox in PLAN.md**

```
grep -n "get_missing_events\|backfill" /workspace/PLAN.md
```

Tick `[ ]` → `[x]` for the appropriate item. (Item is likely `Server-Server backfill/get_missing_events implementation` per subagent report.)

- [ ] **Step 2: Append a decisions log entry to PLAN.md**

```markdown
- **2026-05-28** — `/get_missing_events` ships with a relaxed `DagStore::missing_events`
  contract: unknown event IDs in `latest`/`earliest` are tolerated (latest → empty walk,
  earliest → no-op). Matches Synapse precedent (`synapse/storage/databases/main/event_federation.py`).
  `events_before` keeps strict validation for CSAPI callers. Federation handler uses
  `Result<Json<T>, FedError>` + `IntoResponse` per CLAUDE.md; existing `sliding_sync`
  pattern (explicit `match` in `lib.rs`) is left untouched. No history-visibility filter
  under trusted-mesh model — documented spec gap.
```

Place under the existing decisions log section (CLAUDE.md mandates this).

- [ ] **Step 3: Append the 2-line LOG.md entry at the bottom**

Per memory: LOG.md is append-only, oldest first. Add at the *bottom*:

```markdown
- 2026-05-28: Land `POST /_matrix/federation/v1/get_missing_events/{room_id}` — first federation endpoint. New `federation/` module in `neutrino-http`, `FedError` + `IntoResponse` mapping.
- 2026-05-28: Relax `DagStore::missing_events` to tolerate unknown event IDs (matches Synapse). `events_before` stays strict. D21/D22 flipped to assert the new contract.
```

- [ ] **Step 4: Final fmt + clippy + test pass (workspace)**

```
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Per CLAUDE.md, workspace-wide check before declaring done. If anything fails, fix it before the final checkpoint.

- [ ] **Step 5: STOP — final checkpoint**

Files in this checkpoint:
- `PLAN.md`
- `LOG.md`

Suggested commit message:
> docs(plan): /get_missing_events landed; record decision and log entries

**Pause here for final review.** This is the last checkpoint — all task work should be complete. Reviewer should run the verification checklist below before approving.

---

## Verification before declaring done

- [ ] `cargo fmt --all` clean.
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` clean.
- [ ] `cargo test --workspace` green — including the 11 new e2e tests and the new + flipped storage tests.
- [ ] When committing against a writeable clone: one focused commit per checkpoint (~11 commits). Each checkpoint above lists files + suggested message; do not collapse checkpoints into one giant commit unless the reviewer explicitly asks.
- [ ] PLAN.md checkbox flipped; decisions log entry present.
- [ ] LOG.md has the 2-line summary at the bottom.
- [ ] `complement/VIABLE-TESTS.md` has the blocked-tests row.
- [ ] Manual sanity check: `curl` (or equivalent) against a running `neutrino` binary returns the expected response shape for at least the happy path. If you can't get a binary running, say so explicitly — `cargo test` validates correctness but not deployment.

## Open items deferred (not part of this plan)

These are noted so they don't ambush a future implementer:
- **`event-id-design.md` is referenced from code comments but doesn't exist in `docs/`.** Separate cleanup task — create the doc or scrub the references.
- **Outbound `/get_missing_events`** (we call peers to fill our own gaps). Needs state-res — Phase 6 territory per design doc.
- **History-visibility filter.** Gated on a `state_at_event` provider on `StateStore`. Phase 6.
- **Origin source on `/send`.** When `/send` lands, the storage trait expects `record_federation_txn(origin, txn_id)`. Resolution path documented in `docs/get-missing-events.md` §"Open questions".

## LOC budget (from design doc)

- Storage refactor + new tests: ~150 LOC (smaller than the doc's 350 estimate because we're reusing the walker, not adding a new one).
- `federation/mod.rs` + handler: ~120 LOC.
- E2E test file: ~280 LOC.
- Router wiring + Cargo + docs: ~20 LOC.
- **Total ~570 LOC, ~75% tests.** Under the doc's 770 estimate because the storage primitive already exists.
