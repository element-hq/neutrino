# MSCXXXX: Forward-extremity reconciliation on federation transactions

Federation delivery is push-only: an origin server sends each event it creates
to the set of servers it *currently believes* are joined to the room. That set
is a point-in-time snapshot of the sender's resolved room state. Under
concurrent membership changes — especially two servers rejoining a room at once
through different residents — the snapshot can omit a server that is legitimately
in the room, and the omitted event is then never delivered to it. There is no
proactive reconciliation: `get_missing_events` only fires *reactively*, when an
arriving event references a `prev_event` the receiver does not have, so a server
that is simply missing a head it was never told about has no way to discover the
gap. The result is permanent state divergence between servers that both believe
they are fully synced.

This proposal adds a cheap, bidirectional **forward-extremity exchange** to the
existing `PUT /_matrix/federation/v1/send/{txnId}` transaction: the request
carries the origin's forward extremities for the rooms it touches, the response
carries the destination's. Either side, on seeing an advertised extremity it
does not recognise, pulls that event *and* any ancestry it lacks in a **single**
`get_missing_events` request — extended here so the missing head itself is
returned, not just its ancestors — and integrates it normally. A single
transaction therefore reconciles *both* ends, turning every organic transaction
into an anti-entropy round at the cost of a small list of event IDs.

## Proposal

### Background

Under [MSC4242](https://github.com/matrix-org/matrix-spec-proposals/pull/4242)
(State DAGs) a server tracks two frontier sets per room:

- the **timeline forward extremities** — events in the room DAG with no children
  the server has seen; and
- the **state-DAG forward extremities** — the heads of the state DAG, over which
  the server runs state resolution to derive current state.

A server's view of a room is fully described by these head-sets: every other
event is reachable by walking `prev_events` / `prev_state_events` backwards from
them. Two servers hold identical room state **iff** they hold the same events
reachable from their extremities and resolve them identically; because state
resolution is deterministic, equal event sets imply equal resolved state. It
follows that **divergence is always observable as a difference in forward
extremities**, and that closing a divergence reduces to "make each server aware
of the heads the other holds, then let it pull and re-resolve."

Today nothing exchanges heads proactively. `get_missing_events` is only invoked
when an inbound PDU names an unknown `prev_event`; a server that is missing a
head no one referenced to it never learns of it.

### The exchange

This proposal extends the transaction endpoint
`PUT /_matrix/federation/v1/send/{txnId}` additively. No new endpoint is
introduced and no existing field changes meaning.

**Request.** The transaction body MAY include a `forward_extremities` object,
keyed by room ID, listing the origin's extremities for each room referenced by
the PDUs in this transaction (an implementation MAY also include rooms with no
PDUs in the transaction):

```json
{
  "origin": "hs1",
  "origin_server_ts": 1700000000000,
  "pdus": [ /* ... */ ],
  "forward_extremities": {
    "!room:hs1": {
      "timeline": ["$Bc89nmu...", "$KNn3GXg..."],
      "state": ["$Bc89nmu...", "$KNn3GXg..."]
    }
  }
}
```

**Response.** The transaction response MAY include a `forward_extremities`
object of the same shape, listing the *destination's* extremities for the rooms
that appeared in the request's `forward_extremities` (and/or in its PDUs):

```json
{
  "pdus": { "$Bc89nmu...": {} },
  "forward_extremities": {
    "!room:hs1": {
      "timeline": ["$gpmcAjK..."],
      "state": ["$gpmcAjK..."]
    }
  }
}
```

Both arrays list raw event IDs. `timeline` and `state` are given separately so a
peer can distinguish message-DAG divergence from state-DAG divergence; an
implementation MAY advertise only the union if it does not track them separately,
but a server targeting MSC4242 SHOULD advertise both.

### Extending `get_missing_events`

`POST /_matrix/federation/v1/get_missing_events/{roomId}` today does a reverse
walk of the `prev_events` of `latest_events`, returning their **ancestors**,
excluding `earliest_events` and stopping at `limit`. It never returns the
`latest_events` themselves — the normal caller is assumed to already hold them
(it received them via `/send` and is only chasing their missing ancestry).
Anti-entropy breaks that assumption: the advertised head is exactly the event the
caller is missing, so the unmodified endpoint cannot deliver it.

This proposal adds an optional boolean request field, `include_latest_events`
(default `false`). When `true`, the responder additionally includes in its
`events` response any `latest_events` it holds itself, alongside their ancestors
down to (but excluding) `earliest_events`. In the anti-entropy case the responder
*does* hold those events — they are its own advertised forward extremities — so it
can both serve them and walk back from them. The field is additive and
backward-compatible: a responder that has not implemented it ignores it and
behaves as today, and a caller that hasn't never sets it.

With this, **one** request fetches the missing head(s) *and* the ancestry between
them and the caller's own heads. Several unknown heads are passed together in
`latest_events`, so the entire gap is retrieved in a single round trip.

### Reconciliation

After a server has finished its normal processing of a transaction (request side)
or received the transaction response (response side), for each room the server is
itself joined to, it diffs the peer's advertised `forward_extremities` against its
own store. If any advertised event ID (in either `timeline` or `state`) is not
persisted locally, it issues a **single** request:

```
POST /_matrix/federation/v1/get_missing_events/{roomId}
{
  "latest_events":   ["$Bc89nmu..."],        // the missing advertised heads
  "earliest_events": ["$gpmcAjK..."],        // our own forward extremities
  "include_latest_events": true,             // return the heads, not just ancestors
  "state_dag": true,                         // MSC4242: walk the state DAG too
  "limit": 50
}
```

The response carries the missing heads plus the ancestry between them and the
server's own heads. The server integrates those events in causal order through
the **normal receipt-of-PDU pipeline** — authorisation against state-before-event,
soft-fail, state resolution — then re-resolves current state over its updated
state-DAG extremities. No special trust is granted to an event because it was
named as an extremity; an advertisement is only a hint to *pull*. Because the two
servers now share the relevant event set, they converge. If the gap exceeds
`limit`, the server repeats with advanced `earliest_events` until drained (the
existing pagination behaviour of `get_missing_events`).

The exchange is symmetric: the requester learns the responder's heads from the
response, and the responder learns the requester's heads from the request, so a
single transaction in *either* direction reconciles *both* servers. A server that
already holds every advertised extremity does no work beyond the set-membership
checks — no `get_missing_events` is issued at all.

### Worked example

Two servers rejoin a room concurrently through different residents while a third
(`hs1`) is partitioned from both. When the partition heals, `hs1` integrates
`hs2`'s rejoin (`$Bc8…`) and fans it out, but at that instant `hs3`'s own
concurrent rejoin has not yet been applied on `hs1`, so `hs3` is absent from the
fan-out set and is never sent `$Bc8…`. `hs1` ends with `hs2=join`; `hs3` is stuck
at `hs2=leave`.

With this proposal, the next transaction across the `hs1`–`hs3` link carries
forward extremities. `hs1`'s advertised `state` heads include `$Bc8…`; `hs3` does
not have it, so it issues one `get_missing_events` to `hs1` with
`latest_events=[$Bc8…]`, `earliest_events=[hs3's own heads]`,
`include_latest_events=true`. It receives `$Bc8…` plus any ancestry it lacks,
re-resolves `@alice:hs2` and converges on `join`. No full-state transfer is sent;
only the genuinely missing event (and any ancestry the receiver actually lacks)
crosses the wire.

### Scope of this MSC

This MSC specifies only the piggybacked exchange on transactions that are *already
being sent* for other reasons. It deliberately does **not** specify:

- a periodic / heartbeat transaction to reconcile rooms with no organic traffic
  (see *Potential issues* and *Alternatives*); or
- any digest/hash compaction of the extremity lists (see *Alternatives*).

Both are natural follow-ups; this MSC is the minimal building block they would
share.

## Potential issues

- **Quiescent rooms do not reconcile.** The exchange only happens when a
  transaction is sent. Two servers with a standing divergence but no further
  events to exchange remain diverged until the next organic transaction crosses
  their link. This is the principal known gap. It is acceptable as a first step
  because (a) it fully closes divergence for any pair that subsequently exchanges
  *any* event, which covers the common case, and (b) it composes cleanly with a
  later heartbeat transaction (an otherwise-empty `/send` carrying only
  `forward_extremities`) that would close the quiescent case without changing
  this wire format.

- **Redundant heads on every transaction.** With no hashing, a server advertises
  its full extremity set on every transaction even when the two servers are
  already converged. Forward-extremity sets are small in healthy operation
  (typically one element, a handful after a DAG merge), so the overhead is a few
  event IDs per transaction. A future digest form (advertise a hash; send the
  list only on mismatch) reduces the converged-case cost to a single value but is
  out of scope here.

- **Unbounded extremity lists.** A pathologically forked DAG could accumulate
  many forward extremities, inflating the advertised list. Implementations SHOULD
  cap the number of advertised and honoured extremities and treat the exchange as
  best-effort; a truncated advertisement still makes progress on the next round.

- **Extra round trips on divergence.** A mismatch triggers a single
  `get_missing_events` request (all unknown heads in one `latest_events` list),
  paginated only if the gap exceeds `limit`. This is bounded by the actual
  divergence (nothing is fetched when heads match) and reuses an existing endpoint
  with one additive field, so it adds no new failure modes — only work
  proportional to the gap being healed.

## Alternatives

- **Resend current state on every join.** When a server learns a remote user has
  joined, push that server the full current room state. Simple and closes the
  same gap, but the bandwidth cost is unacceptable — it sends the entire state on
  every join regardless of how little (if anything) the joiner is missing, which
  is especially bad over low-bandwidth federation transports. Rejected.

- **Union/fresher fan-out snapshot.** Compute fan-out recipients from the union
  of pre- and post-event joined sets. Cheaper, but does not fix the motivating
  bug: a server rejoining concurrently through a *different* resident is `leave`
  in *both* snapshots at the moment of fan-out, so it is still omitted. Does not
  close the class.

- **Dedicated anti-entropy endpoint with a polling loop.** A new federation
  endpoint that returns a peer's heads, driven by a per-room timer. Cleaner
  separation of concerns, but adds wire surface and a background polling loop, and
  duplicates information the transaction is already a natural carrier for.
  Piggybacking on `/send` needs neither.

- **Two-step fetch via `/event` then `get_missing_events`.** Fetch each unknown
  head with `GET /_matrix/federation/v1/event/{eventId}`, then let the normal
  reactive backfill (`get_missing_events`) pull its ancestry. This needs no change
  to `get_missing_events`, but costs an extra round trip per head and requires
  implementing the single-event endpoint (which a minimal server may not have).
  The `include_latest_events` extension folds head + ancestry into one request
  and reuses an endpoint the server already has, so this two-step form was
  dropped.

- **Digest/Merkle fingerprint instead of raw heads.** Advertise a hash of the
  extremity set and exchange the actual IDs only on mismatch. Strictly better
  bandwidth in the converged steady state, at the cost of an agreed canonical
  hash and a second exchange on mismatch. Deferred as an optimisation layered on
  this MSC's wire format.

- **Per-server stream-position / version vectors.** Track and exchange a vector
  of per-origin stream positions. Heavier persistent state and a new comparison
  basis, when the DAG's forward extremities already are the natural,
  self-describing comparison basis. Rejected as over-engineered for the problem.

## Security considerations

- **Forced lookups / amplification.** A malicious or buggy peer can advertise
  forward extremities for event IDs that do not exist or that it chooses, causing
  the recipient to issue `get_missing_events` requests. Because the advertisement
  is attacker-controlled, this is an amplification/DoS vector: a small
  advertisement can induce a larger backfill request. Mitigations: a server
  SHOULD cap the number of extremities it honours per room per transaction, SHOULD
  apply normal rate limiting to the `get_missing_events` it issues in response,
  and SHOULD ignore advertised rooms it is not joined to.

- **No new authorisation surface.** An advertised extremity grants no authority.
  Anything pulled as a result is integrated through the same receipt-of-PDU
  checks (authorisation against state-before-event, soft-fail against current
  state, state resolution) as any other received PDU. A peer cannot use the
  exchange to inject an event the recipient would not otherwise accept, nor to
  override state resolution: the recipient resolves over its own DAG. The
  exchange only changes *which* events a server knows to ask for, never *whether*
  they are accepted.

- **No new information disclosure to room members.** Forward extremities are
  derivable from the event stream a joined server already receives, so advertising
  them to a server in the room reveals nothing it could not compute. Servers MUST
  only honour advertisements for rooms they share with the peer, so the exchange
  cannot be used to probe membership of, or events in, rooms the peer is not in.

- **`include_latest_events` lets a caller name arbitrary event IDs.** With the
  flag set, a caller can list IDs it does not hold in `latest_events` and receive
  them if the responder has them in that room. This is the same exposure as the
  standard `GET /event/{eventId}` endpoint, and is scoped identically: a responder
  MUST only serve events for a room the caller is a member of, and `limit` bounds
  the response size. It reveals nothing the caller could not already obtain as a
  room member, and grants no authority — the returned events are still subject to
  the caller's own auth and state-resolution on integration.

- **Resource exhaustion.** Unbounded extremity lists or repeated mismatches could
  consume CPU/IO. The caps and rate limits above bound the work to be proportional
  to genuine divergence.

There are otherwise no security concerns introduced by this proposal.

## Unstable prefix

While this proposal is unstable, the new fields MUST be named with the unstable
prefix. Both endpoints keep their stable paths; every new field is additive, so a
peer that has not implemented this MSC ignores them and the exchange degrades to
today's behaviour.

| Proposed final identifier | Purpose | Development identifier |
| --- | --- | --- |
| `forward_extremities` (transaction request + response field) | Per-room forward-extremity advertisement for anti-entropy reconciliation | `org.matrix.mscXXXX.forward_extremities` |
| `include_latest_events` (`get_missing_events` request field) | Return the `latest_events` themselves, not only their ancestors | `org.matrix.mscXXXX.include_latest_events` |

## Dependencies

This MSC builds on
[MSC4242](https://github.com/matrix-org/matrix-spec-proposals/pull/4242) (State
DAGs), which defines the state DAG, its forward extremities, and the
`get_missing_events` form (`state_dag=true`) used to backfill both the timeline
and state DAGs. It assumes room version 12.

It extends that same `get_missing_events` with the `include_latest_events` field
defined above, so a single request can return an advertised head the caller does
not yet hold together with its ancestry. It deliberately requires **no** new
endpoint — in particular it does *not* depend on `GET /event/{eventId}` — so a
minimal server that only implements `/send` + `get_missing_events` need only add
one request field to the endpoint it already has.
