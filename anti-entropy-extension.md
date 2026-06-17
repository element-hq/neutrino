# MSCXXXX: Forward-extremity advertisement on joined-set growth

[MSCXXXX: Forward-extremity reconciliation on federation transactions](msc-anti-entropy.md)
(the base proposal) has each server advertise its per-room forward extremities on
every `/send` transaction, and reconcile against the forward extremities another
server advertises. Two servers that exchange a transaction therefore converge any
divergence between them. A server advertises only as a side effect of traffic it
is already sending, however. If an event reaches a server after the last
transaction it sends to a second server that is behind — for example an event
whose fan-out omitted that second server — no advertisement carries the event to
it, and once the room is quiet there is no further transaction on which to
piggyback. The divergence persists until unrelated future traffic flows between
the two servers.

This MSC proposes that a server additionally advertise its forward extremities
when a second server becomes joined in its current state. The advertisement is
sent only to the newly-joined server, and only when the advertising server holds
forward extremities the newly-joined server may not yet have; in every other case
nothing is sent.

## The unconverged case

A server selects the recipients of an event from its current-state view of the
room's joined members. A server that is joined in reality but not yet shown as
joined in that view — typically one that has (re)joined concurrently, or whose
join the selecting server has not yet applied — is omitted from the fan-out. The
base proposal relies on a later `/send` between the two servers to advertise the
omitted event; when no such transaction occurs, the omission is permanent.

```mermaid
sequenceDiagram
    participant hs1
    participant hs2
    participant hs3
    Note over hs1: joined view = {hs1, hs3}<br/>hs2 still only invited in hs1's view
    hs3->>hs1: send_join $J (hs3 re-joins via hs1)
    Note over hs1: applies $J; fan-out targets {hs3} only
    hs1-->>hs3: $J
    Note over hs2: does not receive $J
    hs2->>hs1: $J2 (hs2's own join, delivered later)
    Note over hs1: applies $J2; hs2 now joined, but a<br/>federation apply does not re-run fan-out
    Note over hs1,hs3: hold $J
    Note over hs2: missing $J; room is now quiet
```

`hs1` and `hs3` agree that `hs3` is joined; `hs2` continues to believe `hs3` has
left. No further traffic corrects this.

## Proposal

### Assumptions

This MSC relies on the base mechanism eventually conveying a server's current
forward extremities to every server in its joined set. A receiver backfills any
ancestry below an advertised forward extremity with `get_missing_events` (base
proposal), so only the forward extremities need reach it, not every individual
event; a durable, retrying outbox is one way to provide this, but is not required.
Given this, the only way a joined server can permanently miss an event is if the
conveying server's current state did not list it as joined at the time — resolved
exactly when its membership becomes `join` in that state.

It further assumes the room is not permanently partitioned: each server can, over
time, exchange transactions with the others directly.

### The advertisement

An advertisement is a `PUT /_matrix/federation/v1/send/{txnId}` with an empty
`pdus` array and a populated `forward_extremities` (base proposal). A server that
receives one processes it as any other transaction: it reconciles against the
advertised forward extremities and returns its own, so the sending server also
reconciles from the response.

### Advertising on joined-set growth

A server SHOULD maintain, per `(destination, room)`, the forward extremities it
most recently advertised to that destination (`last_advertised`), updated by every
outbound transaction carrying `forward_extremities` — whether a piggybacked
`/send` (base proposal) or an advertisement.

When applying an event causes a server `P` to become joined in a room's current
state, having not previously been joined, the server:

1. MUST set `last_advertised[P]` for that room to `P`'s join event (overwriting any
   previous value). `P` holds its own join event, and its ancestry is reachable
   from it, so a server whose forward extremities are exactly that join owes `P`
   nothing.
2. MUST compare the room's current forward extremities against `last_advertised[P]`.
   If they differ — the server holds a forward extremity the join does not cover —
   it MUST send `P` one advertisement and update `last_advertised[P]`. If they are
   equal it MUST NOT send anything.

A server MAY wait a short time — for example up to 30 seconds — before sending, so
that advertisements triggered by several joins at once (multiple servers, or one
server across multiple shared rooms) coalesce into one transaction per destination.

### Worked example

Continuing the unconverged case, `hs1` learns `hs2` is joined when it applies
`$J2`. `hs2` enters `hs1`'s joined set; `hs1` sets `last_advertised[hs2]` to `$J2`
and finds its current forward extremities `{$J, $J2}` differ from it (it holds
`$J`), so it advertises.

```mermaid
sequenceDiagram
    participant hs1
    participant hs2
    participant hs3
    hs2->>hs1: $J2 (hs2's join, delivered after the heal)
    Note over hs1: hs2 enters joined set; current forward<br/>extremities {$J,$J2} differ from $J2 → advertise
    hs1->>hs2: PUT /send (pdus: [], forward_extremities: $J, $J2)
    Note over hs2: holds $J2, not $J
    hs2->>hs1: get_missing_events($J) [include_latest_events]
    hs1-->>hs2: $J
    Note over hs2: applies $J; converged
```

In a room with a linear state DAG the newly-joined server's join is built on the
current forward extremity, so applying it makes that join the sole forward
extremity on every server — equal to the seed — and nothing is sent. An
advertisement arises only when a server holds a forward extremity concurrent with
the join.

## Potential issues

The `last_advertised` cache grows with the number of destinations and shared rooms.
Entries MAY be evicted; a miss costs at most one redundant advertisement, which the
receiving server de-duplicates.

The joining server may already hold more than just their join event.
A server may therefore advertise a forward
extremity the joining server already has, costing one redundant round trip that the
receiver de-duplicates.

A server joining through a resident whose state is behind draws an advertisement
from every up-to-date server. The base proposal's handling of staged-but-unapplied
events collapses these to a single `get_missing_events` — later advertisers find
the forward extremities already staged — leaving a bounded number of small
redundant round trips.

Several servers joining at once can produce up to one advertisement per pair over
the churn, each suppressed by the seed when the joining server is up to date.

Convergence can take more than one round. A server that itself missed the joining
server's join does not learn it is joined, so does not advertise to it, until it
learns of the join transitively: an up-to-date server advertises to the joining
server, which pulls its forward extremities (including the third server's
membership), whose joined set then grows, and so on. Each round strictly increases
shared knowledge over a finite set of events, so this terminates, but not
necessarily in one round.

## Alternatives

Advertise on any forward-extremity change, rather than on joined-set growth. This
is the more general trigger and is correct, but on a one-to-many fan-out the
recipients do not `/send` each other, so their caches are never refreshed and each
advertises to every other — needless transactions that grow with the square of the
room's server count and reconcile nothing. This MSC keeps that correctness but,
given that a joined server is eventually told a server's forward extremities,
narrows the trigger to joined-set growth, so no advertisements arise from ordinary
traffic.

A periodic heartbeat — an otherwise-empty `/send` carrying `forward_extremities` to
each joined server on a timer — either sends when fully converged (needless traffic
and wake-ups) or needs the same de-duplication to stay quiet. A server cannot know
whether it is behind, so the timer would fire for a condition that may never hold.
The joined-set-growth trigger fires only when a reconciliation could be required.

A catch-up or full-state push on join re-sends room state to a server when it
(re)joins. This is rejected in the base proposal for bandwidth, and would not cover
this case: the lagging server was already joined, and the missing event post-dates
its join.

Piggyback only, with no extension, leaves the unconverged case open: a divergence
whose event lands after a pair's last exchange never reconciles.

## Security considerations

A server that rapidly leaves and rejoins causes each other server to emit at most
one advertisement to it per rejoin, and only under genuine divergence. The work
lands on the server that caused it, bounded by the base proposal's rate limits; it
is not amplification against a third server. A server MAY rate-limit advertisements
per destination if another server's churn is abusive.

A server that claims a more-advanced join point than it holds suppresses others'
advertisements to it, since they seed `last_advertised` from its join. Such a join
fails auth and DAG validation, as its `prev_events` must exist and resolve; even if
it did not, the only effect is that the server is not told of events it lacks — it
starves itself, affecting no other server.

`last_advertised` is keyed by what a server itself sent, seeded from a join it
auth-checked, and is never written from free-form input from another server, so a
remote party cannot set it directly. Advertisements carry no events and grant no
authority: an empty `pdus` array and a small list of IDs, whose worst case is a
wasted `get_missing_events` bounded by the base proposal's caps.

## Unstable prefix

This MSC introduces no new wire identifiers. It reuses the base proposal's
`forward_extremities` field on `PUT /_matrix/federation/v1/send/{txnId}`; an
advertisement is that transaction with an empty `pdus` array. The trigger, the
`last_advertised` cache, and the seed-from-join are server-local behaviour, with no
wire addition beyond transactions a base-proposal implementation already
understands and a non-implementing server already ignores.

## Dependencies

This MSC depends on
[MSCXXXX: Forward-extremity reconciliation on federation transactions](msc-anti-entropy.md),
reusing its `forward_extremities` advertisement, its `include_latest_events`
reconciliation, and its receipt-side processing unchanged; this MSC adds only when a
server proactively advertises. It relies on the base mechanism eventually conveying
a server's forward extremities to its joined set, assumes the room is not
permanently partitioned, and — through the base proposal — depends on
[MSC4242](https://github.com/matrix-org/matrix-spec-proposals/pull/4242) (state
DAGs). It assumes room version 12.
