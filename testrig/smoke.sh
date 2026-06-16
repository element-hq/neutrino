#!/usr/bin/env bash
#
# smoke.sh — drive the 3-server rig through the happy + unhappy federation
# paths and assert the outcomes. Built for CI, but runs locally too.
#
#   ./smoke.sh
#
# Assumes the `neutrino-testrig:latest` image already exists (CI builds it with
# a cached layer); if it doesn't, this script builds it once via compose.
#
# Scenarios:
#   1. basic     hs1 creates a room, hs2 joins, messages flow both ways.
#   2. partition cut hs1<->hs2, both send, heal, both converge.
#   3. state-res hs3 joins; hs2 is promoted to admin; hs3 is isolated from both
#                peers; hs1 and hs2 set conflicting room names; on heal hs3 must
#                resolve to the later-timestamped name (HS2) regardless of the
#                order the two name events arrive, and must still receive hs1's
#                losing name event into its timeline.
#
# No fixed sleeps gate the assertions — every wait is a poll with a generous
# deadline — except the two explicit "let the split settle" pauses the test
# design calls for. There is no backoff-kick yet, so a healed link drains on the
# outbox's own retry schedule; the 60s poll deadline is sized to cover it.
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NCTL="$DIR/nctl"
COMPOSE=(docker compose -f "$DIR/docker-compose.yml")
# NEUTRINO_LB=1 layers the in-process-sidecar overlay (CBOR federation). Export
# it so the nctl invocations below pick up the same mode.
if [[ "${NEUTRINO_LB:-0}" == "1" ]]; then
  COMPOSE+=(-f "$DIR/docker-compose.lb.yml")
  export NEUTRINO_LB
fi
declare -A PORT=([hs1]=8001 [hs2]=8002 [hs3]=8003)

DEADLINE=60 # seconds for any single convergence poll
INTERVAL=2  # seconds between poll attempts
ROOM=""     # the room under test, set in scenario 1

log() { printf '\n=== %s ===\n' "$*" >&2; }

fail() {
  echo "SMOKE FAIL: $*" >&2
  echo "---- compose logs ----" >&2
  "${COMPOSE[@]}" logs --no-color >&2 2>&1 || true
  exit 1
}

# Run a mutating nctl action; a non-zero exit is a smoke failure.
act() { "$@" || fail "command failed: $*"; }

cleanup() { "${COMPOSE[@]}" down -v >/dev/null 2>&1 || true; }
trap cleanup EXIT

# Wait until a server answers /versions with 2xx (server booted + bound).
wait_ready() {
  local hs=$1
  local url="http://localhost:${PORT[$hs]}/_matrix/client/versions"
  local deadline=$((SECONDS + DEADLINE))
  echo "  » GET $url (readiness)" >&2
  until curl -fsS "$url" >/dev/null 2>&1; do
    ((SECONDS < deadline)) || fail "$hs never became ready"
    sleep 1
  done
}

# poll_sync <hs> <description> <jq-bool-over-sync-array>
# Re-fetches the sliding-sync view until the jq test is true, or fails at the
# deadline (dumping the last view it saw). Each attempt's request URL is echoed
# by nctl, so the polling is visible.
poll_sync() {
  local hs=$1 desc=$2 test=$3 deadline=$((SECONDS + DEADLINE)) out n=0
  echo "↻ waiting (≤${DEADLINE}s): $desc" >&2
  while :; do
    n=$((n + 1))
    out=$("$NCTL" "$hs" sync) || out='[]'
    if printf '%s' "$out" | jq -e "$test" >/dev/null 2>&1; then
      echo "✓ $desc — after $n poll(s)" >&2
      return 0
    fi
    ((SECONDS < deadline)) || { echo "last sync($hs): $out" >&2; fail "timeout: $desc"; }
    sleep "$INTERVAL"
  done
}

# poll_state <hs> <description> <jq-bool-over-state-array>
poll_state() {
  local hs=$1 desc=$2 test=$3 deadline=$((SECONDS + DEADLINE)) out n=0
  echo "↻ waiting (≤${DEADLINE}s): $desc" >&2
  while :; do
    n=$((n + 1))
    out=$("$NCTL" "$hs" state "$ROOM") || out='[]'
    if printf '%s' "$out" | jq -e "$test" >/dev/null 2>&1; then
      echo "✓ $desc — after $n poll(s)" >&2
      return 0
    fi
    ((SECONDS < deadline)) || { echo "last state($hs): $out" >&2; fail "timeout: $desc"; }
    sleep "$INTERVAL"
  done
}

resolved_name() { # <hs> -> current resolved m.room.name, or empty
  "$NCTL" "$1" state "$ROOM" | jq -r '.[] | select(.type=="m.room.name") | .content.name // ""'
}

# ---- bring the rig up -------------------------------------------------------

command -v jq >/dev/null || fail "smoke.sh needs jq"

log "starting rig"
if docker image inspect neutrino-testrig:latest >/dev/null 2>&1; then
  act "${COMPOSE[@]}" up -d
else
  # No prebuilt image (e.g. a cold local run) — build it once.
  act "${COMPOSE[@]}" up -d --build
fi
wait_ready hs1
wait_ready hs2
wait_ready hs3

# ---- scenario 1: basic send/receive ----------------------------------------

log "scenario 1: basic send/receive"
ROOM=$("$NCTL" hs1 create public "smoke lab" | jq -r '.room_id // empty')
[[ $ROOM == \!* ]] || fail "createRoom did not return a room id (got: '$ROOM')"
echo "room: $ROOM" >&2

act "$NCTL" hs1 invite "$ROOM" hs2
act "$NCTL" hs2 join "$ROOM" hs1

act "$NCTL" hs1 msg "$ROOM" "basic-from-hs1"
poll_sync hs2 "hs2 sees hs1's message" \
  'any(.[]?; .timeline[]? | .content.body == "basic-from-hs1")'

act "$NCTL" hs2 msg "$ROOM" "basic-from-hs2"
poll_sync hs1 "hs1 sees hs2's message" \
  'any(.[]?; .timeline[]? | .content.body == "basic-from-hs2")'

# ---- scenario 2: partition then heal ---------------------------------------

log "scenario 2: partition hs1<->hs2, send on both, heal, converge"
act "$NCTL" partition cut 12
act "$NCTL" hs1 msg "$ROOM" "split-from-hs1"
act "$NCTL" hs2 msg "$ROOM" "split-from-hs2"
sleep 5 # let the split settle so each send has actually failed to federate
act "$NCTL" partition heal 12

poll_sync hs2 "hs2 receives hs1's split message after heal" \
  'any(.[]?; .timeline[]? | .content.body == "split-from-hs1")'
poll_sync hs1 "hs1 receives hs2's split message after heal" \
  'any(.[]?; .timeline[]? | .content.body == "split-from-hs2")'

# ---- scenario 3: concurrent state resolution -------------------------------

log "scenario 3: concurrent room-name resolution across a partition"
act "$NCTL" hs3 join "$ROOM" hs1
poll_state hs3 "hs3 has joined the room" \
  'any(.[]; .type=="m.room.member" and .state_key=="@alice:hs3" and .content.membership=="join")'

# hs2 needs admin power to author m.room.name; promote it and make sure the
# promotion has propagated to every server before we split (hs3 will otherwise
# reject hs2's later name event when it backfills it).
act "$NCTL" hs1 power "$ROOM" hs2 100
for hs in hs2 hs3; do
  poll_state "$hs" "$hs sees hs2 promoted to admin" \
    'any(.[]; .type=="m.room.power_levels" and .content.users["@alice:hs2"]==100)'
done

# Isolate hs3 from both peers; hs1<->hs2 stays up.
act "$NCTL" partition cut 13
act "$NCTL" partition cut 23

# Two conflicting room-name state events. The 1s gap guarantees hs2's event has
# the strictly-later origin_server_ts, so state resolution must pick it (HS2).
act "$NCTL" hs1 name "$ROOM" "HS1"
sleep 1
act "$NCTL" hs2 name "$ROOM" "HS2"

# Heal hs3<->hs2 first: hs3 learns HS2 (and backfills the losing HS1 from hs2).
act "$NCTL" partition heal 23
poll_state hs3 "hs3 resolves the room name to HS2 (later timestamp wins)" \
  'any(.[]; .type=="m.room.name" and .content.name=="HS2")'

# Heal hs3<->hs1: hs1's losing HS1 event now reaches hs3 directly. State res is
# by timestamp, not arrival order, so the resolved name must stay HS2 even
# though HS1 arrives last — and HS1 must show up in hs3's timeline.
act "$NCTL" partition heal 13
poll_sync hs3 "hs3 timeline contains hs1's losing HS1 name event" \
  'any(.[]?; .timeline[]? | (.type=="m.room.name" and .sender=="@alice:hs1" and .content.name=="HS1"))'

name=$(resolved_name hs3)
[[ $name == "HS2" ]] || fail "hs3 resolved name flipped to '$name' after HS1 arrived (expected HS2)"

log "all scenarios passed"
