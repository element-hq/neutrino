# converge — planned scenarios

These are the convergence scenarios that are **only meaningfully testable
end-to-end** with the real 3-process rig and real network partitions: the bug
they catch lives in delivery timing, async retry, or crash recovery, not in a
pure function. Anything that is deterministic logic (inbound auth rejection,
soft-fail, `prev_state_events` validation, txn dedup, rejected-event permanence,
make_leave/send_leave handshake shape, …) belongs in `neutrino-state` unit/prop
tests or `neutrino-http`'s `federation/tests.rs`, **not** here.

The rig is now **in-process** (`neutrino-testkit`: real `neutrino` child processes
on loopback, proxy partitions, SIGKILL crash/revive — no docker, no `nctl`).

Current coverage: random episodes of room ops + partition cut/heal, a
manufactured `m.room.name` conflict across a single cut (origin_server_ts /
event_id tie-break), the divergence guard, the `/messages` no-lost-writes
oracle, and **crash/revive** (`CRASH_PROB`% of rounds SIGKILL a live server or
re-spawn a dead one; the barrier revives any still-down server and the
convergence gate proves it recovered its committed state + redelivered its
outbox). The scenarios below extend that.

Shared note on the oracle: `/messages?dir=f` returns the room **timeline**, which
includes state-event PDUs (not just messages). So `collect_msg_ids` already sees
superseded/losing state events — several scenarios below can assert *full state
DAG presence* (not just current-state equality) by ledgering the relevant state
event ids the same way messages are ledgered today.

---

## 1. Crash / restart — targeted refinements

The baseline landed: `CRASH_PROB`% of rounds crash a random live server (SIGKILL
the real `neutrino` process) or revive a dead one (re-spawn on the same on-disk
storage); the barrier revives any still-down server, and the convergence gate
proves it recovered its committed state and redelivered its outbox. The crash
target is random and recovery is proven against a live cluster.

What's still **directed rather than random**, and worth adding:

- **Deterministically kill the owing side.** Today the crash victim is uniform
  over live servers. A stronger test cuts a link isolating `hsX` from a peer it
  *owes events to* (or is owed by), issues ops that park rows in `hsX`'s outbox /
  inbound `staged_events`, then crashes `hsX` while the link is still cut and the
  rows are still parked — so recovery must drain durable parked rows, not just
  re-derive current state. Strengthen the oracle by ledgering the specific
  pre-kill event ids and asserting their presence everywhere post-heal.
- **Kill during the heal.** Crash `hsX` *while its outbox is mid-drain* (right
  after a heal) to catch a drain-interrupted-by-restart race — the worker's
  startup re-enumeration of `staged_rooms` must resume cleanly.

---

## 3. Conflicting power-levels + membership keys

**Why e2e-only.** State-res itself is pure (covered by `neutrino-state`'s
state_res/prop tests). What is e2e is that two servers each resolve a
**mainline-ordered** `m.room.power_levels` conflict (or a membership conflict)
against their *own* arrival order and still agree after heal. Today
`fuzz_conflict` only collides `m.room.name`, whose tie-break is the trivial
`origin_server_ts`/`event_id` path — the hard mainline-ordering branch of
state-res v2 is never exercised concurrently.

**Refactor.** Generalize `fuzz_conflict` from "both set m.room.name" to a
`ConflictKind`:
- `Name` (existing).
- `PowerLevels`: both co-admins set the **same target user's** level to two
  different values across the cut. Drives mainline ordering (the conflicting
  events are themselves `m.room.power_levels`, so they reorder the auth mainline).
  Pick a target that is neither `hs1` (anchor) nor either writer, to avoid
  self-demotion auth quirks.
- `Membership`: concurrent member events for the **same** `state_key` across the
  cut — e.g. side A kicks `hs3` while side B re-invites/promotes `hs3`. Must
  respect auth on each side (both writers admins; target below both).

`assert_divergent` already compares full `(type,state_key)→event_id` maps, so it
generalizes unchanged. The barrier already heals + converges.

**Oracle.** `assert_divergent` on the spot (both writes accepted ⇒ servers
differ), then post-heal convergence. For `PowerLevels`, additionally assert the
*resolved* level is identical on all three (it already is, via state equality) —
the value is whichever mainline wins, not asserted to a specific number.

**Open questions.** Membership conflicts can legitimately one-side-reject under a
stale view (record-tier tolerates it); only assert divergence when **both** sides
returned 2xx, exactly as the Name path does today.

---

## 4. Three-way fork

**Why e2e-only.** Three independent resolutions with real arrival-order variance.
`fuzz_conflict` cuts **one** link (two servers); a genuine 3-way fork — all three
partitioned, each writing the same state key — is a distinct topology that the
current driver never produces.

**Scenario.**
1. Require all three servers joined **and** admins (promote `hs2`/`hs3` if needed,
   or only fire when the model already has all three at/above `state_default`).
2. Cut **all three** links (`12`, `13`, `23`) → full isolation.
3. Each server sets the **same** state key (`m.room.name` to start; reuse the
   `ConflictKind` machinery from #3 once it lands) to a distinct value.
4. Assert pairwise divergence across every readable pair (3 pairs), not just one.
5. Heal all links at the barrier; `converge_check`.

**Oracle.** Pairwise `assert_divergent` for `(hs1,hs2)`, `(hs1,hs3)`, `(hs2,hs3)`
while fully cut, then post-heal convergence to one identical resolved state.

**Open questions.** Healing order — heal one link at a time with a convergence
check between, vs heal-all-at-once — exercises different reconciliation paths;
worth parameterizing. Watch the `DEADLINE`: three parked outboxes draining may
need a longer poll budget than the 2-way case.

---

## 5. Deep multi-hop state-DAG gap recovery

**Why e2e-only.** A server offline long enough to miss a **long** chain of state
events must catch up via the MSC4242 `get_missing_events(state_dag)`
exponential-limit walk against a live peer. The walk's ordering/limit-growth is
unit-testable; the live deep catch-up — real async fetch loop, real backoff, real
peer responses — is not. Today episodes are short, so gaps stay shallow and this
path is barely stretched.

**Scenario (a dedicated "long isolation" episode).**
1. Isolate one server (`hsX`) by cutting both its links; keep the other two
   connected as the authoring majority.
2. On the connected majority, run **many** state-producing rounds (names, power
   changes, membership churn) — long enough to build a state-DAG chain deeper
   than a single `get_missing_events` page (so the walk must grow its `limit` and
   traverse multiple hops). Make the depth a configurable knob.
3. Heal `hsX`'s links.
4. `converge_check`.

**Oracle.**
- Convergence of current resolved state (existing hard gate).
- **Full-DAG presence:** ledger the intermediate/superseded state event ids
  produced during isolation (same mechanism as the message ledger — they show up
  in `/messages?dir=f`) and assert they are all present on `hsX` post-heal. This
  proves the deep ancestry was actually fetched, not just the current tips.
- Optional confirmation the walk ran: scan the server's request log for
  `get_missing_events` with `state_dag` from `hsX` after heal.

**Open questions.** How deep is "deep enough" to force `limit` growth — depends on
the page size and the exponential schedule; pick a depth comfortably past one
page. Whether to also vary which server is isolated (a non-anchor vs forcing a
re-join). This scenario subsumes the standalone "full state-DAG presence" check
(losing/superseded events propagate everywhere), so fold that assertion in here.

---

## Not in scope for this file (covered elsewhere, or deferred)

- Inbound auth rejection, soft-fail, rejected-event permanence + cascade,
  `prev_state_events` MUST-rules, txn idempotency/dedup, EDU-stub graceful
  ignore, ban/unban auth, make_leave/send_leave handshake, `/backfill`,
  send_join shape, join-rules — all deterministic; put them in
  `neutrino-state` / `federation/tests.rs`.
- Backoff-drain timing on its own was considered but is implicitly covered:
  every barrier already waits for a healed link to drain on the heal-reset
  backoff within `DEADLINE`.
