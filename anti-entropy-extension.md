# MSCXXXX: Active forward-extremity advertisement (anti-entropy tail-convergence)

The base anti-entropy proposal — [MSCXXXX: Forward-extremity reconciliation on
federation transactions](msc-anti-entropy.md) — piggybacks each server's forward
extremities on every `/send` (request and response) and reconciles on receipt.
That converges any divergence between two servers **that subsequently exchange a
transaction**. It does not converge a divergence whose triggering event reaches a
holder *after* organic traffic to a lagging peer has already stopped: the event
is never carried by any advertisement that peer receives, and — once the room
goes quiet — nothing re-advertises it. This is the "tail of propagation" gap the
base MSC explicitly deferred.

This extension closes it with **active, de-duplicated forward-extremity
advertisement**: when a server's forward extremities change and that change has
not already been conveyed to a joined peer, the server proactively sends that
peer an extremity-only transaction. A per-destination "last advertised" cache and
a trailing-edge debounce hold idle and active-traffic chatter at **zero** — a
standalone advertisement is emitted *only* for the genuinely-uncovered tail, at
most one (coalesced) message per lagging peer.

## Proposal

### The tail-of-propagation gap

Under the base MSC a divergence is closed when the two servers next exchange a
`/send` carrying forward extremities. Consider three servers and this real trace
(timestamps from a partition-fuzz run):

- `hs2` authors its last message `$M` while partitioned, with a stale view that
  omits `hs3` from the fan-out (the base bug class: a concurrently-rejoined peer
  the sender's snapshot still has as `leave`).
- After the heal, `$M` reaches `hs1` at `T+27.3s` (normal `hs2→hs1` delivery).
- But `hs3`'s **last** forward-extremity exchange with `hs1` was `T+25.8s`, and
  with `hs2` `T+26.2s` — *both before `$M` existed on the peer they were talking
  to*. Every reconciliation opportunity `hs3` had predates `$M`.
- The room then goes quiet (no further `/send`). `$M` is held by `hs1` and `hs2`
  but is never advertised to `hs3` again, so `hs3` never learns it exists and the
  rooms stay diverged.

Piggybacking is necessary but not sufficient: it only advertises *as a
side-effect of traffic that is already flowing*, and the tail of a propagation
burst has, by definition, no traffic after it.

### Active advertisement

A server maintains, per destination, the forward extremities it has **most
recently advertised** to that peer (`last_advertised[dest]`, per room). This cache
is updated by *every* outbound transaction that carries `forward_extremities` —
both piggybacked `/send`s (base MSC) and the standalone advertisements defined
here — so it always reflects what the peer was last told.

When applying an event changes a room's forward extremities, the server:

1. Determines the joined peers for that room (the same set the base MSC scopes
   advertisements to) and marks each **dirty**.
2. Arms a short **trailing-edge debounce** timer (re-armed on each subsequent
   change), so a burst of N applied events coalesces into a single flush rather
   than N advertisements.
3. On the timer firing, for each dirty destination it compares the room's current
   forward extremities against `last_advertised[dest]`. If — and only if — they
   **differ**, it sends one extremity-only transaction (see below), covering all
   shared rooms whose advertisement to that peer is stale, and updates the cache.
   If they match (the peer was already told, e.g. by a piggybacked `/send` that
   raced ahead), it sends nothing.

An **extremity-only transaction** is an ordinary
`PUT /_matrix/federation/v1/send/{txnId}` with an empty `pdus` array and a
populated `forward_extremities` (base MSC). The receiver processes it exactly as
any other transaction: it reconciles against the advertised heads and returns its
own forward extremities, so the exchange remains bidirectional and the *sender*
also reconciles from the response.

### Why this is quiet

- **Idle, converged room:** forward extremities don't change, so no timer is ever
  armed and nothing is sent. Zero bytes on the wire.
- **Active traffic:** the piggybacked `/send`s already advertise the latest
  extremities and update `last_advertised`; when the debounce fires it finds the
  cache already matches and suppresses the standalone send. Effectively zero
  extra messages.
- **Tail:** the final FE change after traffic stops is the one case the cache does
  *not* already cover, so exactly one (coalesced, all-rooms) advertisement is sent
  per lagging peer. That is the minimum required for correctness.

The mechanism is **edge-triggered** (on FE change), not periodic, so there are no
idle wake-ups — distinguishing it from a heartbeat timer.

### Worked example (continued)

`hs1` applies `$M` at `T+27.3s`; its timeline forward extremity moves to `$M`.
That FE change marks `hs3` (a joined peer) dirty and arms the debounce. On firing,
`hs1`'s current FEs differ from what it last advertised to `hs3` (which predated
`$M`), so `hs1` sends `hs3` a single extremity-only transaction advertising `$M`.
`hs3` reconciles (base MSC: one `get_missing_events` with `include_latest_events`),
pulls `$M`, and converges — with one message, emitted only because the tail was
genuinely uncovered.

## Potential issues

- **Debounce window is a latency/chatter knob.** Too short risks emitting a
  standalone advertisement that a piggybacked `/send` would have covered a moment
  later (wasted message); too long adds tail-convergence latency. A window on the
  order of a second is comfortably above inter-event spacing within a burst and
  below human-perceptible sync delay. It is a local tuning parameter, not part of
  the wire protocol.

- **`last_advertised` cache memory.** O(destinations × rooms) sets of
  forward-extremity event IDs. Forward-extremity sets are small (usually one), and
  the mesh + shared-room set is bounded for the embedded single-user target, so
  this is negligible; entries can be evicted (a miss just means one redundant
  advertisement, which the receiver de-duplicates).

- **Cache is in-memory; lost on restart.** After a restart a server does not know
  what it last advertised, so the first FE change per peer re-advertises once
  (a small, one-time burst as each room next changes). Acceptable; persisting the
  cache is possible but not worth it.

- **Forward-extremity flap.** A state-resolution reorder can move and then restore
  an extremity; the debounce + cache compare collapse a flap that nets to no
  change into no send, and a flap that nets to a change into a single send.

- **Still one round trip per lagging peer per tail.** Unavoidable: a peer that is
  genuinely missing an event must be told. This is the floor, not overhead.

## Alternatives

- **Periodic heartbeat.** An otherwise-empty `/send` with `forward_extremities` to
  each joined peer on a timer. Simpler (no FE-change signal needed), but it either
  ticks idly (chatter / wake-ups even when fully converged) or needs the same
  `last_advertised` dedup to stay quiet — at which point it is strictly worse than
  edge-triggering, since it adds a timer and up-to-one-interval tail latency for
  no benefit. Rejected as the primary mechanism; the dedup cache is the part worth
  keeping, and this MSC keeps it.

- **Advertise on every applied event (no debounce, no dedup).** Correct but chatty
  — one advertisement per event per peer, exactly what the dedup + debounce exist
  to avoid.

- **Piggyback only (base MSC, no extension).** Leaves the tail-of-propagation gap
  open: a divergence whose event lands after a pair's last exchange never
  reconciles. This MSC exists precisely to close that.

- **Catch-up / full-state push on join.** Re-send room state to a peer when it
  (re)joins. Rejected in the base MSC for bandwidth, and it would not even cover
  this case — the lagging peer was already joined; the missing event post-dates
  its join.

## Security considerations

- **No new amplification surface.** A standalone advertisement is triggered by
  *our own* forward-extremity changes, never by peer input, so a remote party
  cannot induce a server to emit advertisements (let alone a flood). The receiving
  side reuses the base MSC's reconciliation verbatim — advertised heads are only a
  hint to `get_missing_events`, every pulled event is auth-checked and
  state-resolved, and the base MSC's caps / rate-limits / shared-room scoping
  apply unchanged.

- **Cache cannot be poisoned.** `last_advertised` is keyed by what *we* sent, not
  by anything a peer supplies, so a malicious peer cannot manipulate it to
  suppress or force advertisements.

- **Ignoring advertisements is harmless.** A peer that drops our extremity-only
  transactions simply forgoes reconciliation from us; it does not affect our state
  and our next FE change re-advertises (the cache still shows the peer as
  not-yet-told only if our FEs changed again).

- **Extremity-only transactions are cheap.** Empty `pdus`, a small ID list; they
  carry no events and grant no authority.

There are otherwise no security concerns introduced by this proposal.

## Unstable prefix

This extension introduces **no new wire identifiers**. It reuses the base MSC's
`forward_extremities` field on `PUT /_matrix/federation/v1/send/{txnId}`; an
"extremity-only" advertisement is simply that transaction with an empty `pdus`
array. The active-advertisement trigger, the `last_advertised` cache, and the
debounce are all server-local behaviour with no observable wire addition beyond
transactions that a base-MSC implementation already understands (and that a
non-implementing peer already ignores).

## Dependencies

This MSC builds directly on
[MSCXXXX: Forward-extremity reconciliation on federation transactions](msc-anti-entropy.md)
— it reuses that proposal's `forward_extremities` advertisement, its
`include_latest_events` reconciliation, and its receipt-side processing
unchanged; this extension only adds *when* a server proactively advertises. That
in turn builds on
[MSC4242](https://github.com/matrix-org/matrix-spec-proposals/pull/4242) (State
DAGs). It assumes room version 12.
