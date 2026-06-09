# neutrino federation test rig

Three single-user neutrino homeservers (`hs1`, `hs2`, `hs3`) in Docker, wired so
you can drive them over federation from the CLI and **cut/heal federation links
on demand** to test partition tolerance.

## Why this topology

Federation resolves `http://{server_name}` directly (raw host:port, no TLS, no
`.well-known`). Each server binds `:80` inside its container and is named after
its compose service (`hs1`/`hs2`/`hs3`), so docker DNS routes federation between
containers.

There is **one bridge network per pair** of servers — `net_ab`, `net_ac`,
`net_bc`. The only network path between two servers is their shared pair-network,
so disconnecting a container from one pair-network severs *that* link
symmetrically (both directions) while the other two links stay up. No proxy and
no changes to neutrino are needed.

Client-Server traffic bypasses the pair-networks entirely — the CLI talks to each
server on a published host port (8001/8002/8003). So you can keep sending events
to a server even while it is partitioned from its peers, which is exactly what a
convergence test needs.

```
        net_ab                 net_bc
  hs1 ───────────── hs2 ───────────── hs3
   └──────────────── net_ac ──────────┘

  CLI ──8001──▶ hs1   CLI ──8002──▶ hs2   CLI ──8003──▶ hs3   (always reachable)
```

Each server has exactly one user: `@alice:hs1`, `@alice:hs2`, `@alice:hs3`.
The default build performs no token check, so the CLI sends no auth.

State is in-memory (a SQLite tempfile per process) and is **wiped on every
restart**.

## Requirements

`docker` (with the compose plugin), `curl`, and `jq`.

## Usage

```sh
./nctl up                       # build image + start all three servers
./nctl ps
./nctl logs hs1                 # follow one server's logs

# --- a federated room across hs1 and hs2 ---
./nctl hs1 create public "the lab"        # -> {"room_id":"!..."}
ROOM='!...'                                # paste the id
./nctl hs1 invite "$ROOM" hs2             # invite @alice:hs2 (federated invite)
./nctl hs2 join   "$ROOM" hs1             # @alice:hs2 joins (hs1 is the resident)
./nctl hs2 sync                            # hs2 now sees the room + state

./nctl hs1 msg  "$ROOM" "hello from hs1"
./nctl hs1 name "$ROOM" "renamed room"
./nctl hs1 kick "$ROOM" hs2 "bye"

# --- partition tolerance (hs3 must be in the room, else its sync is just empty) ---
./nctl hs1 invite "$ROOM" hs3 ; ./nctl hs3 join "$ROOM" hs1
./nctl partition status
./nctl partition cut  ac                  # isolate hs1 <-> hs3 (both ways)
./nctl hs1 msg "$ROOM" "sent during split"
./nctl hs3 sync                            # hs3 has NOT received it (parked in hs1's outbox)
./nctl partition heal ac                  # link restored, outbox drains
./nctl hs3 sync                            # hs3 has converged

./nctl down
```

## Commands

| Command | What it does |
| --- | --- |
| `nctl up` / `down` / `ps` | compose lifecycle (`up` builds + starts) |
| `nctl logs <hs>` | follow a server's logs |
| `nctl fedlog [hs]` | follow federation HTTP requests (all servers, or one) |
| `nctl partition cut\|heal <ab\|ac\|bc>` | sever / restore one federation link |
| `nctl partition status` | show which links are up |
| `nctl <hs> create [public\|private] [name]` | create a room, print its id |
| `nctl <hs> join <roomId> [residentHs]` | join (a `residentHs` hint forces a federated join) |
| `nctl <hs> msg <roomId> <text>` | send an `m.room.message` |
| `nctl <hs> name <roomId> <name>` | set `m.room.name` |
| `nctl <hs> invite\|kick\|ban <roomId> <user> [reason]` | membership change (`user` = `hsN` or full `@mxid`) |
| `nctl <hs> leave <roomId>` | leave |
| `nctl <hs> state <roomId>` | current room state |
| `nctl <hs> members <roomId>` | member list |
| `nctl <hs> sync` | sliding-sync view of all rooms |

## Watching federation requests

neutrino logs one line per inbound HTTP request (`method` + full path) and one
per response (`status` + `latency`), under the `neutrino_http=info` default
filter — so every `/_matrix/federation/...` request a server **receives** is in
its log. Each federation request is received by exactly one server (its
destination), so the union across all three servers is the complete, ordered
stream of federation requests in the rig.

```sh
./nctl fedlog            # follow federation requests on all three servers
./nctl fedlog hs1        # just the requests hs1 receives
```

Sample (the `hs3 |` prefix is the server that received it):

```
hs3  | …Z  INFO request{method=GET uri=/_matrix/federation/v1/make_join/!R…/@alice:hs2}: started processing request
hs3  | …Z  INFO request{method=GET uri=/_matrix/federation/v1/make_join/!R…/@alice:hs2}: finished processing request status=200 latency=1.2ms
hs3  | …Z  INFO request{method=PUT uri=/_matrix/federation/v2/send_join/!R…/$evt}:        finished processing request status=200 latency=3.4ms
```

This shows paths, methods, status, and timing — not request bodies. Most lines
are logged by the **destination**; the sender is usually clear from the path
(`make_join/{room}/{user}` ⇒ the user's server is joining).

**Every outbound federation request is logged on the sender** (`outbound <METHOD>
<path>` with `dest` + ids), so smoke-testing can verify the right requests fire in
the right order without relying on the receiver being up: `make_join`,
`send_join`, `invite`, `make_leave`, `send_leave`, `get_missing_events`, and
`send` transactions. These lines nest inside the triggering C-S request span, so
you can see which client action caused which federation call.

Transactions additionally log one line per *attempt* — so you can watch the
outbox retry a peer that is down or partitioned. Retries reuse the same `txn` and
the `attempt` counter climbs:

```
hs1 | … outbound PUT /_matrix/federation/v1/send dest=hs3 txn=…-1 pdus=1 attempt=1
hs1 | … outbound PUT /_matrix/federation/v1/send dest=hs3 txn=…-1 pdus=1 attempt=2
hs1 | … transaction delivery failed; will retry dest=hs3 … backoff=4s
hs1 | … outbound PUT /_matrix/federation/v1/send dest=hs3 txn=…-1 pdus=1 attempt=3
```

So `partition cut`, send an event, and `fedlog` shows the origin server retrying
with exponential backoff until you `heal`. (Only `/send` transactions retry via
the durable outbox; `make_join`/`send_join`/`invite` are synchronous and surface
their failure to the caller instead.)

## How a federated join flows (and where a cut bites)

```
[CLI -> hs2]  POST /_matrix/client/v3/join/{room}?server_name=hs1   (local to hs2)
   HOP1  hs2 -- GET  make_join ---------------------------> hs1   (template)
         hs2 rebuilds the join event locally (never echoes the template)
   HOP2  hs2 -- PUT  send_join ---------------------------> hs1   (hs1 auth-checks, persists,
                                                                   returns the state DAG)
   HOP3  hs1 -- PUT  /send/{txn} (outbox, async) ---------> hs3   (propagate to the rest)
```

- Cut **hs1↔hs2**: HOP1/HOP2 fail at the transport → the join errors. ("Can't
  join while split from the resident.")
- Cut **hs1↔hs3**: the join still succeeds on hs1+hs2; HOP3 parks in hs1's
  durable outbox and retries. On heal it drains and hs3 converges (pulling any
  missed ancestry via `get_missing_events`). This is the partition-tolerance path.
