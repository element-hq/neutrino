# Convergence fuzzer (`converge`)

A seeded, randomized fuzzer over the three-server federation rig (`hs1`/`hs2`/`hs3`,
see [testrig/README.md](../../testrig/README.md)). It drives random episodes of
room operations interleaved with random partition cut/heal, then heals everything
and asserts the room converges. Run it from the workspace root:

```sh
cargo run -p converge                 # fresh seed (printed); reproduce a failure with:
cargo run -p converge -- 16646        # an explicit seed replays the same op sequence
EPISODES=10 ROUNDS_PER_EPISODE=20 cargo run -p converge -- 16646   # longer run
```

It brings the rig up via `docker compose` (building the image if absent), drives
each server's Client-Server API over HTTP, and delegates partition cut/heal to
[`testrig/nctl`](../../testrig/nctl). The seed drives every random choice through a
single `StdRng`, so a failing run is replayable. Federation delivery timing is
asynchronous and *not* seed-controlled, so the seed pins the intended op sequence
and gives highly reproducible runs, not bit-identical ones. On failure it dumps
the seed, the full op log, each server's resolved state, and the compose logs.

## What it asserts

Three things, checked after every partition is healed and the outboxes drain:

### 1. State convergence (the hard gate)

All servers joined to the room reach a **byte-identical resolved `/state`** — the
map of `(type, state_key) → event_id` is the same on hs1/hs2/hs3. It asserts
*agreement*, never which concurrent write wins: state resolution tie-breaks on
wall-clock `origin_server_ts` (then `event_id`), so the winner isn't predictable
and isn't required to be anything specific — only identical everywhere.

### 2. No lost messages

Every message must appear (via `/messages`) on every server that was joined
**when it was sent** — its "send-time audience". A server that joins *later* is
not required to backfill messages it missed while absent; that matches Matrix
(no retro-delivery to a re-joiner). State events are not checked here — the
resolved-state equality check already pins all current state, and there is no
event-by-id endpoint to confirm superseded state.

### 3. Exact CSAPI status — two-tier

A server auth-checks each operation against its own (possibly stale) view, so
exact status is only predictable when the rig is fully converged:

- **At a barrier (converged):** exact assertions. The admin / power-level /
  membership-validity battery: a non-admin setting the name → `M_FORBIDDEN`;
  an admin promoting them → 2xx; they set the name → 2xx; the admin demotes
  them → 2xx; name again → `M_FORBIDDEN`; leave → 2xx; rejoin → 2xx.
- **Mid-partition:** mutating ops are *record-only*. A 2xx event joins the
  must-converge set; a stale-view rejection is logged, not asserted (it is
  legitimate against a partitioned server's view).

### 4. Divergence actually happens (anti-vacuity guard)

A convergence test that never diverges is green but tests nothing. Two mechanisms
guard against that:

- **Manufactured conflicts.** ~15% of rounds (when two joined servers can both
  author `m.room.name`) deliberately cut the link between them and have each set
  the name to a *different* value. Neither has seen the other's write, so the two
  events are concurrent siblings — the only reliable way to exercise the
  `origin_server_ts` → `event_id` tie-break. When both writes are accepted, the
  rig asserts on the spot that the two servers' `/state` genuinely **differ**; if
  they agree, the partition isn't biting and the run fails immediately.
- **Pre-heal sampling.** At the top of every barrier, before any link is healed,
  the rig checks whether two servers already disagree. A whole run that cuts links
  but *never once* observes divergence (no conflict fired, no pre-heal
  disagreement) is flagged at the end — that pattern means the partitions stopped
  working and every "converged" assertion passed over nothing.

### Structural invariants

- **hs1 is a fixed anchor** — never leaves, is never kicked, is never demoted —
  so a reachable resident and an admin always exist (rejoins and barriers can't
  strand the room).
- **Ban is excluded**: the rig exposes no unban path, so a ban would strand a
  member out of the barrier's all-joined equality check. Churn is leave / kick /
  rejoin only.
- The shadow model (members + power levels) gates op selection and is re-synced
  from real `/state` at every barrier — authoritative there, advisory mid-episode.

## Shape of a run

**Setup (once):** hs1 creates the room → invite + join hs2 & hs3 → barrier →
exact-probe battery.

**Each episode** (default 6 episodes × 12 rounds): every round is, with ~30%
probability, a topology change (cut a live link / heal a downed one); ~15% a
**manufactured conflict** (two co-admins write the same state key across an active
cut — see *Divergence actually happens* above, falling through to a normal
mutation if no two co-admins are joined); else a model-valid mutating op on a
random server — message / set-name / power-change / invite / kick / leave / rejoin
(chosen so it is *meaningful*, not a wall of 403s). All record-tier. After the
rounds: a **barrier**, then the **exact-probe battery**, then the next episode.

**Barrier (the convergence point):**

1. heal all three links;
2. rejoin anyone who churned out, so all three are joined and comparable;
3. poll until all three `/state` maps are identical **and** stable for
   `STABLE_POLLS` polls **and** every send-time-audience message is present
   (within `DEADLINE`, default 90s);
4. re-sync the shadow model from hs1's real state.

### A concrete episode

From seed `16646`, episode 1:

```
hs3 leave → hs3 rejoin → cut 12 → cut 13 → hs2 msg → cut 23 → heal 23
→ cut 23 → heal 12 → heal 23 → cut 23 → hs3 msg
→ barrier (heal all, rejoin, converge) → exact-probes
```

Each episode deliberately interleaves membership churn with partition cut/heal,
lets divergence build up, then forces everything to reconcile and verifies it
did. This is how the rig surfaced the `/leave` mis-route (a joined member's leave
taking the OOB-decline path) and the stale-rejoin divergence (a server re-joining
a room it still hosts not re-syncing missed state).

## Knobs

| Env var | Default | Meaning |
| --- | --- | --- |
| `EPISODES` | 6 | number of fuzz episodes |
| `ROUNDS_PER_EPISODE` | 12 | mutating/topology rounds per episode |
| `DEADLINE` | 90 | seconds for any single convergence poll |
| `STABLE_POLLS` | 2 | consecutive identical polls before "quiescent" |
| `INTERVAL` | 2 | seconds between convergence-poll attempts |
| `STRICT_EVENT_PRESENCE` | 1 | set 0 to skip the message-presence check (state equality stays the hard gate) |

The fuzzer sends no signals itself. A healed link drains on its outbox's own
schedule because `nctl partition heal` already resets the per-destination backoff
on both ends of the link (`SIGUSR2` → `KickBackoff`), so convergence lands well
inside `DEADLINE` without any per-poll nudge from the harness.
