#!/usr/bin/env bash
#
# converge.sh — seeded, randomized convergence fuzzer over the 3-server rig.
#
#   ./converge.sh [SEED]
#       SEED   integer; reproduces an exact run. Omitted -> a fresh seed is
#              chosen and printed (and re-printed on failure) so any run replays.
#   env knobs: EPISODES (default 6), ROUNDS_PER_EPISODE (12), DEADLINE (90s),
#              INTERVAL (2s), STABLE_POLLS (2), STRICT_EVENT_PRESENCE (1).
#
# WHAT IT TESTS
#   The convergence invariant: after every partition is healed and outboxes
#   drain, every server that is joined to the room agrees on the SAME resolved
#   room state, and no locally-accepted event is lost. It does NOT predict which
#   concurrent write wins (state-res tie-breaks on origin_server_ts then
#   event_id — wall-clock, not seed-controlled); it asserts AGREEMENT, which is
#   timing-independent. The seed fixes the logical op sequence; the invariant is
#   delivery-timing-independent. So: reproducible op sequence + timing-agnostic
#   oracle, not bit-identical runs.
#
# DETERMINISM
#   `RANDOM=$SEED` seeds one PRNG stream; EVERY decision here is drawn from it.
#   nctl's own `txn()` burns $RANDOM too — but nctl runs as a CHILD PROCESS, so
#   its consumption cannot desync this parent stream. INVARIANT: never introduce
#   $RANDOM into a converge.sh helper out of sequence, or you break replay.
#
# TWO-TIER STATUS POLICY
#   A server auth-checks each local op against ITS OWN (possibly stale) view, and
#   pairwise cuts make "which federated events have arrived" timing-dependent.
#   Predicting exact CSAPI status under an arbitrary partition would mean
#   reimplementing per-server state-res + delivery timing — out of scope. So:
#     * EXACT tier — ops issued while fully healed + converged (the shadow model
#       == reality). Status is asserted exactly (2xx, or a specific M_ code).
#       This is where admin / power-level / membership-validity is checked hard.
#     * RECORD tier — mutating ops during the fuzz phase. The model picks ops it
#       believes valid (to avoid a wall of 403s), issues them, and records the
#       actual outcome: a 2xx event joins the must-converge ledger; a non-2xx is
#       a legitimate stale-view rejection mid-partition, logged not asserted.
#
# STATE MANAGED (see the design notes inline)
#   1. PRNG stream            — RANDOM=$SEED, single source of all choices.
#   2. Shadow room model      — MEMBER[hs], PL[hs] + PL defaults; gates op
#                               selection and exact-tier predictions. Advisory
#                               mid-episode; RE-SYNCED from real /state at every
#                               barrier (the only point it is authoritative).
#   3. Topology model         — UP[12|13|23]; only up-links are cut, down healed.
#   4. Message ledger         — MSG_ACCEPTED[event_id]=send-time audience; each
#                               message must appear in /messages on every server
#                               joined when it was sent (a later joiner is not
#                               expected to backfill it). State events covered by
#                               the /state equality check.
#   5. Op log                 — every action + predicted/actual status; dumped
#                               with the seed on failure for exact replay.
#   6. Divergence tracking     — DIVERGENCE_SEEN / CONFLICT_FIRED / ANY_CUT. The
#                               conflict driver forces two co-admins to write the
#                               same state key across an active cut; the guard
#                               then asserts they actually disagree (a partition
#                               that DOESN'T bite makes convergence vacuous).
#
# SCOPE CHOICES (deliberate, see design discussion)
#   * hs1 is the anchor: never leaves / is never kicked / is never demoted, so a
#     reachable resident + an admin always exist (rejoins and barriers can't
#     strand the room).
#   * Churn = leave + kick + rejoin only. BAN is excluded: this server exposes no
#     unban path, so a ban is irreversible and would strand a member out of the
#     barrier's all-joined equality oracle. Ban-correctness is a separate test.
#
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NCTL="$DIR/nctl"
COMPOSE=(docker compose -f "$DIR/docker-compose.yml")
declare -A PORT=([hs1]=8001 [hs2]=8002 [hs3]=8003)
SERVERS=(hs1 hs2 hs3)
LINKS=(12 13 23)

EPISODES=${EPISODES:-6}
ROUNDS_PER_EPISODE=${ROUNDS_PER_EPISODE:-12}
DEADLINE=${DEADLINE:-90}             # seconds for any single convergence poll
INTERVAL=${INTERVAL:-2}              # seconds between poll attempts
STABLE_POLLS=${STABLE_POLLS:-2}      # consecutive equal polls => quiescent
STRICT_EVENT_PRESENCE=${STRICT_EVENT_PRESENCE:-1}

CS="_matrix/client/v3"
SYNC="_matrix/client/unstable/org.matrix.simplified_msc3575/sync"

# Seed: explicit arg reproduces a run; otherwise pick + print a fresh one. The
# bare $RANDOM read here is BEFORE we seed, so it is a fine source of freshness.
SEED=${1:-$RANDOM}
RANDOM=$SEED

# ---- shadow room model (re-synced from reality at every barrier) ------------
declare -A MEMBER=([hs1]=leave [hs2]=leave [hs3]=leave)
declare -A PL=([hs1]=0 [hs2]=0 [hs3]=0)
PL_USERS_DEFAULT=0 PL_STATE_DEFAULT=50 PL_EVENTS_DEFAULT=0 PL_INVITE=0 PL_KICK=50
EV_NAME="" EV_PL=""                    # per-event power overrides ("" = use default)

declare -A UP=([12]=1 [13]=1 [23]=1)   # topology: 1 = link up
declare -A MSG_ACCEPTED=()              # message event_id -> origin hs (must converge)
declare -a OPLOG=()                     # human-readable action trail
SEQ=0                                   # monotonic tag for generated content
ROOM=""
DIVERGENCE_SEEN=0                       # set once any partition is proven to bite
CONFLICT_FIRED=0                        # set once a manufactured conflict is asserted
ANY_CUT=0                               # set once any link is cut (vacuity check)

# ---- output helpers ---------------------------------------------------------

log() { printf '\n=== %s ===\n' "$*" >&2; }
note() { printf '  · %s\n' "$*" >&2; }
oplog() { OPLOG+=("$*"); note "$*"; }

dump() {
  echo "---- SEED=$SEED  (re-run: ./converge.sh $SEED) ----" >&2
  echo "---- op log (${#OPLOG[@]} actions) ----" >&2
  printf '%s\n' "${OPLOG[@]}" >&2
  echo "---- final per-server resolved state ----" >&2
  local hs
  for hs in "${SERVERS[@]}"; do
    echo "## $hs" >&2
    cs "$hs" GET "$CS/rooms/$ROOM/state" || true
    printf '%s\n' "$CS_BODY" | jq -c 'sort_by(.type,.state_key)[] | {type,state_key,name:.content.name,membership:.content.membership,event_id}' >&2 2>/dev/null || true
  done
}

fail() {
  echo "CONVERGE FAIL: $*" >&2
  dump
  echo "---- compose logs ----" >&2
  "${COMPOSE[@]}" logs --no-color >&2 2>&1 || true
  exit 1
}

# ---- low-level HTTP ----------------------------------------------------------

# cs <hs> <METHOD> <path>  -> sets CS_CODE + CS_BODY (raw; no jq transform, so
# the oracle sees real event_ids). Read-only paths only.
CS_CODE="" CS_BODY=""
cs() {
  local url="http://localhost:${PORT[$1]}/$3" raw
  raw=$(curl -sS -w '\n%{http_code}' -X "$2" "$url" 2>/dev/null) || raw=$'\n000'
  CS_CODE=${raw##*$'\n'}
  CS_BODY=${raw%$'\n'*}
}

# issue <nctl args...> -> RC (0 = 2xx), ERR (M_ errcode if any), EVID (if the
# body carried one). Mutations go through nctl so we drive the rig exactly as a
# human would; nctl returns non-zero on non-2xx and prints the body on stdout.
RC=0 ERR="" EVID="" BODY=""
issue() {
  set +e
  BODY=$("$NCTL" "$@" 2>/dev/null)
  RC=$?
  set -e
  ERR=$(printf '%s' "$BODY" | jq -r '.errcode // empty' 2>/dev/null || true)
  EVID=$(printf '%s' "$BODY" | jq -r '.event_id // empty' 2>/dev/null || true)
}

# ---- outbox nudge ------------------------------------------------------------
# During any convergence poll we are waiting on healed outboxes to retry on
# their own exponential backoff. The intent is for SIGUSR2 to RESET those backoff
# timers so a healed link drains immediately instead of waiting out the backoff.
#
# IMPORTANT: neutrino does NOT yet install a SIGUSR2 handler, so today the signal
# hits its DEFAULT disposition — which TERMINATES the process. It is therefore
# OFF by default (it would kill the rig mid-run). Once the server handles SIGUSR2
# (no-op or backoff-reset), run with CONVERGE_SIGUSR2=1 to enable the nudge. We
# hit all three servers — "reset every backoff timer at a heal barrier" is the
# intent, and it's cheaper than tracking which outbox parked what.
SIGUSR2_NUDGE=${CONVERGE_SIGUSR2:-0}
kick_outboxes() {
  [[ $SIGUSR2_NUDGE == 1 ]] || return 0
  local hs
  for hs in "${SERVERS[@]}"; do
    docker kill --signal=USR2 "neutrino-$hs" >/dev/null 2>&1 || true
  done
}

# ---- model accessors ---------------------------------------------------------

mxid() { printf '@alice:%s' "$1"; }
power_of() { printf '%s' "${PL[$1]:-$PL_USERS_DEFAULT}"; }

# pair key -> its two server names (12 -> "hs1 hs2"). Used by the conflict driver
# and the divergence guard to map a link to the two servers it separates.
link_servers() { case "$1" in 12) echo hs1 hs2 ;; 13) echo hs1 hs3 ;; 23) echo hs2 hs3 ;; esac; }

# state_req <event-type> -> power required to send it as state.
state_req() {
  case "$1" in
    m.room.name) printf '%s' "${EV_NAME:-$PL_STATE_DEFAULT}" ;;
    m.room.power_levels) printf '%s' "${EV_PL:-$PL_STATE_DEFAULT}" ;;
    *) printf '%s' "$PL_STATE_DEFAULT" ;;
  esac
}

# Re-derive the whole model from one server's resolved state. Called at every
# barrier, where all servers agree, so any one is ground truth. This is what
# keeps the advisory mid-episode model from drifting forever.
sync_model() {
  cs hs1 GET "$CS/rooms/$ROOM/state"
  [[ $CS_CODE == 2* ]] || fail "sync_model: hs1 /state returned $CS_CODE"
  local s=$CS_BODY plc
  plc=$(printf '%s' "$s" | jq -c 'last(.[]|select(.type=="m.room.power_levels")).content // {}')
  PL_USERS_DEFAULT=$(printf '%s' "$plc" | jq -r '.users_default // 0')
  PL_STATE_DEFAULT=$(printf '%s' "$plc" | jq -r '.state_default // 50')
  PL_EVENTS_DEFAULT=$(printf '%s' "$plc" | jq -r '.events_default // 0')
  PL_INVITE=$(printf '%s' "$plc" | jq -r '.invite // 0')
  PL_KICK=$(printf '%s' "$plc" | jq -r '.kick // 50')
  EV_NAME=$(printf '%s' "$plc" | jq -r '.events["m.room.name"] // empty')
  EV_PL=$(printf '%s' "$plc" | jq -r '.events["m.room.power_levels"] // empty')
  local hs u
  for hs in "${SERVERS[@]}"; do
    u=$(mxid "$hs")
    PL[$hs]=$(printf '%s' "$plc" | jq -r --arg u "$u" '.users[$u] // empty')
    [[ -n ${PL[$hs]} ]] || PL[$hs]=$PL_USERS_DEFAULT
    MEMBER[$hs]=$(printf '%s' "$s" | jq -r --arg u "$u" \
      'last(.[]|select(.type=="m.room.member" and .state_key==$u)).content.membership // "leave"')
  done
}

# Optimistic model update after a 2xx mutation (keeps op selection sensible
# within an episode; the barrier re-sync corrects any partition-time drift).
apply_model() { # <hs> <action> <room> [target] [level]
  local hs=$1 action=$2
  case "$action" in
    power) PL["$4"]=$5 ;;
    invite) MEMBER["$4"]=invite ;;
    kick) MEMBER["$4"]=leave ;;
    leave) MEMBER["$hs"]=leave ;;
    join) MEMBER["$hs"]=join ;;
  esac
}

# NB: must return 0 even when there is no event_id (membership ops return {}),
# or `set -e` would kill the script at the bare `ledger ...` call site. Called
# with the full nctl arg list so it can classify message vs state events: the
# no-lost-writes check verifies messages via /messages (state is covered by the
# resolved-state equality check), so message events are tracked separately.
ledger() { # <hs> <action> ...
  [[ -n $EVID && $2 == msg ]] || return 0
  # Audience = servers joined (per the model) at send time. Only they are
  # required to hold the message: a server that joins *later* is not expected to
  # backfill messages sent while it was absent (Matrix does not retro-deliver to
  # a re-joiner), so requiring it everywhere would false-fail on membership churn.
  local aud="" hs
  for hs in "${SERVERS[@]}"; do
    [[ ${MEMBER[$hs]} == join ]] && aud+="$hs "
  done
  MSG_ACCEPTED[$EVID]=${aud% }
  return 0
}

# ---- mutation drivers (tier-aware) ------------------------------------------

# expect_ok / expect_err: EXACT tier only — model == reality, status is certain.
expect_ok() {
  issue "$@"
  [[ $RC -eq 0 ]] || fail "exact: expected 2xx for [nctl $*] (rc=$RC err=$ERR body=$BODY)"
  ledger "$@"
  apply_model "$@"
  oplog "EXACT ok    [$*] evid=${EVID:-–}"
}
expect_err() { # <wanted-errcode> <nctl args...>
  local want=$1; shift
  issue "$@"
  [[ $RC -ne 0 ]] || fail "exact: expected error $want for [nctl $*] but got 2xx"
  [[ $ERR == "$want" ]] || fail "exact: expected $want for [nctl $*], got '$ERR'"
  oplog "EXACT deny  [$*] -> $ERR"
}

# record: RECORD tier — issue, ledger on success, never assert status.
record() {
  issue "$@"
  if [[ $RC -eq 0 ]]; then
    ledger "$@"
    apply_model "$@"
    oplog "record ok   [$*] evid=${EVID:-–}"
  else
    oplog "record drop [$*] -> ${ERR:-rc$RC} (stale-view reject, ok mid-partition)"
  fi
}

# ---- lightweight single-fact polls (used inside the exact-tier battery) -----

# poll_member <viewer> <subject> <membership>
poll_member() {
  local viewer=$1 subj=$2 want=$3 u; u=$(mxid "$subj")
  local deadline=$((SECONDS + DEADLINE))
  while :; do
    kick_outboxes
    cs "$viewer" GET "$CS/rooms/$ROOM/state"
    if [[ $CS_CODE == 2* ]] && [[ "$(printf '%s' "$CS_BODY" | jq -r --arg u "$u" \
        'last(.[]|select(.type=="m.room.member" and .state_key==$u)).content.membership // "leave"')" == "$want" ]]; then
      return 0
    fi
    ((SECONDS < deadline)) || fail "poll_member: $viewer never saw $subj=$want"
    sleep "$INTERVAL"
  done
}

# poll_power <viewer> <subject> <cmp> <level>   cmp: ge|lt
poll_power() {
  local viewer=$1 subj=$2 cmp=$3 lvl=$4 u; u=$(mxid "$subj")
  local deadline=$((SECONDS + DEADLINE)) seen def
  while :; do
    kick_outboxes
    cs "$viewer" GET "$CS/rooms/$ROOM/state"
    if [[ $CS_CODE == 2* ]]; then
      def=$(printf '%s' "$CS_BODY" | jq -r 'last(.[]|select(.type=="m.room.power_levels")).content.users_default // 0')
      seen=$(printf '%s' "$CS_BODY" | jq -r --arg u "$u" \
        "last(.[]|select(.type==\"m.room.power_levels\")).content.users[\$u] // $def")
      if { [[ $cmp == ge ]] && ((seen >= lvl)); } || { [[ $cmp == lt ]] && ((seen < lvl)); }; then
        return 0
      fi
    fi
    ((SECONDS < deadline)) || fail "poll_power: $viewer never saw $subj $cmp $lvl"
    sleep "$INTERVAL"
  done
}

# ---- convergence oracle ------------------------------------------------------

# state_map <hs> -> canonical "(type,state_key) -> event_id" map (raw event_ids).
state_map() {
  cs "$1" GET "$CS/rooms/$ROOM/state"
  [[ $CS_CODE == 2* ]] || return 1
  printf '%s' "$CS_BODY" | jq -S -c \
    'map({ (.type + " " + (.state_key // "")): .event_id }) | add // {}'
}

# Divergence guard (#1): two servers on opposite sides of an ACTIVE cut, each
# having just locally accepted a write to the SAME state key, MUST now disagree
# on their resolved /state. If they AGREE, the partition is not biting and every
# later "converged" assertion is vacuous — there was never anything to reconcile.
# Fail loudly: a green run that never diverges is a green run that tested nothing.
assert_divergent() { # <a> <b> <ctx>
  local a=$1 b=$2 ctx=$3 ma mb
  ma=$(state_map "$a") || fail "divergence guard: $a /state unreadable ($ctx)"
  mb=$(state_map "$b") || fail "divergence guard: $b /state unreadable ($ctx)"
  [[ $ma != "$mb" ]] || fail "divergence guard: $a and $b agree on /state under an active cut after conflicting writes ($ctx) — the partition is NOT biting, so convergence checks would pass vacuously"
  DIVERGENCE_SEEN=1
  oplog "  divergence confirmed: $a != $b under active cut ($ctx)"
}

# Pre-heal probe, run at the top of every barrier BEFORE any link is healed: if
# two readable servers already disagree, a partition produced real divergence
# this episode. Sticky for the whole run; a run that cuts links but NEVER once
# observes divergence is flagged at the end as likely vacuous. A server that left
# returns non-2xx on /state and is simply skipped (no false "divergence").
sample_divergence() {
  local maps=() hs m i
  for hs in "${SERVERS[@]}"; do
    m=$(state_map "$hs") && maps+=("$m")
  done
  ((${#maps[@]} >= 2)) || return 0
  for ((i = 1; i < ${#maps[@]}; i++)); do
    [[ ${maps[0]} == "${maps[$i]}" ]] && continue
    DIVERGENCE_SEEN=1
    note "pre-heal divergence observed (partitions are biting)"
    return 0
  done
  return 0
}

# Every message-timeline event_id a server holds, paged oldest-first via
# /messages (the only event-retrieval endpoint that exists — there is no
# event-by-id route despite PLAN listing one). Prints ids newline-separated.
collect_msg_ids() { # <hs>
  local hs=$1 from="" url end n
  while :; do
    url="$CS/rooms/$ROOM/messages?dir=f&limit=100"
    [[ -n $from ]] && url="$url&from=$from"
    cs "$hs" GET "$url"
    [[ $CS_CODE == 2* ]] || return 1
    printf '%s' "$CS_BODY" | jq -r '.chunk[]?.event_id // empty'
    end=$(printf '%s' "$CS_BODY" | jq -r '.end // empty')
    n=$(printf '%s' "$CS_BODY" | jq '.chunk | length')
    # Stop when the page is empty or the continuation token stops advancing.
    [[ -z $end || $end == "$from" || $n -eq 0 ]] && break
    from=$end
  done
}

# No-lost-writes: every ledgered MESSAGE event must appear in every joined
# server's /messages timeline. State events are not checked here — the
# resolved-state equality check already pins all *current* state, and there is
# no event-by-id endpoint to confirm superseded state events. Set
# STRICT_EVENT_PRESENCE=0 to skip (state equality remains the hard gate).
messages_present_all() {
  [[ $STRICT_EVENT_PRESENCE == 1 ]] || return 0
  ((${#MSG_ACCEPTED[@]} > 0)) || return 0
  local hs ids eid
  for hs in "${SERVERS[@]}"; do
    [[ ${MEMBER[$hs]} == join ]] || continue
    ids=$(collect_msg_ids "$hs") || return 1
    for eid in "${!MSG_ACCEPTED[@]}"; do
      # Only require the message on servers that were in its send-time audience.
      [[ " ${MSG_ACCEPTED[$eid]} " == *" $hs "* ]] || continue
      case $'\n'"$ids"$'\n' in
        *$'\n'"$eid"$'\n'*) : ;;
        *) return 1 ;;
      esac
    done
  done
  return 0
}

# Wait until all three servers' resolved state is identical, stable for
# STABLE_POLLS, and every ledgered event is present everywhere. This is both the
# quiescence wait and the convergence assertion.
converge_check() {
  local deadline=$((SECONDS + DEADLINE)) stable=0 prev="__none__" m1 m2 m3 ok
  while :; do
    kick_outboxes
    ok=1
    m1=$(state_map hs1) || ok=0
    m2=$(state_map hs2) || ok=0
    m3=$(state_map hs3) || ok=0
    [[ $ok == 1 && $m1 == "$m2" && $m2 == "$m3" ]] || ok=0
    if [[ $ok == 1 && $m1 == "$prev" ]]; then stable=$((stable + 1)); else stable=0; fi
    prev=$m1
    if [[ $ok == 1 && $stable -ge $STABLE_POLLS ]] && messages_present_all; then
      note "converged (state identical; ${#MSG_ACCEPTED[@]} message events present on all servers)"
      return 0
    fi
    ((SECONDS < deadline)) || fail "no convergence within ${DEADLINE}s"
    sleep "$INTERVAL"
  done
}

# ---- barrier: heal everything, restore full membership, converge, re-sync ----

ensure_joined() {
  local hs=$1
  cs "$hs" GET "$CS/rooms/$ROOM/state"
  local cur; cur=$(printf '%s' "$CS_BODY" | jq -r --arg u "$(mxid "$hs")" \
    'last(.[]|select(.type=="m.room.member" and .state_key==$u)).content.membership // "leave"')
  [[ $cur == join ]] && return 0
  issue "$hs" join "$ROOM" hs1 || true
  MEMBER[$hs]=join
  poll_member "$hs" "$hs" join
}

barrier() {
  log "barrier: heal all links, restore membership, converge"
  # Sample divergence BEFORE healing: any real partition this episode shows up as
  # two servers disagreeing here. Sticky for the run's end-of-run vacuity check.
  sample_divergence
  local k
  for k in "${LINKS[@]}"; do
    if [[ ${UP[$k]} != 1 ]]; then issue partition heal "$k" || true; fi
    UP[$k]=1
  done
  # Restore all three to joined so the equality oracle always compares all three
  # (a server that left legitimately has a truncated view and can't be compared).
  ensure_joined hs2
  ensure_joined hs3
  converge_check
  sync_model
}

# ---- exact-tier probe battery (runs only when fully converged) ---------------
# Rigorously checks the admin / power-level / membership-validity contract that
# the record tier can only observe. Picks a joined non-admin and walks it
# through deny -> promote -> allow -> demote -> deny, then a leave/rejoin.
exact_probes() {
  local req na="" s
  req=$(state_req m.room.name)
  for s in hs2 hs3; do
    if [[ ${MEMBER[$s]} == join ]] && (($(power_of "$s") < req)); then na=$s; break; fi
  done
  if [[ -z $na ]]; then
    note "exact-probes: no joined non-admin (everyone is admin); skipping PL battery"
    return 0
  fi
  log "exact-probes: admin/PL/membership contract via $na"

  # 1. non-admin cannot author a state event
  expect_err M_FORBIDDEN "$na" name "$ROOM" "probe-deny-$((SEQ += 1))"
  # 2. admin (hs1) promotes na above state_default
  expect_ok hs1 power "$ROOM" "$na" 100
  poll_power "$na" "$na" ge "$req"          # promotion must reach na's own server
  # 3. na can now author it
  expect_ok "$na" name "$ROOM" "probe-allow-$((SEQ += 1))"
  # 4. admin demotes na back
  expect_ok hs1 power "$ROOM" "$na" 0
  poll_power "$na" "$na" lt "$req"
  # 5. and is denied again
  expect_err M_FORBIDDEN "$na" name "$ROOM" "probe-deny2-$((SEQ += 1))"
  # 6. membership churn validity: leave then rejoin both succeed
  expect_ok "$na" leave "$ROOM"
  poll_member "$na" "$na" leave
  expect_ok "$na" join "$ROOM" hs1
  poll_member "$na" "$na" join
}

# ---- fuzz phase --------------------------------------------------------------

# Pick one valid (actor, op) from the shadow model and run it RECORD-tier. We
# only enumerate model-valid candidates so the fuzz generates meaningful events
# rather than a wall of rejections; the record tier still tolerates a stale-view
# rejection if the acting server's real view disagrees with the model.
fuzz_mutate() {
  local cand=() s t sp tp others
  for s in "${SERVERS[@]}"; do
    sp=$(power_of "$s")
    if [[ ${MEMBER[$s]} == join ]]; then
      ((sp >= PL_EVENTS_DEFAULT)) && cand+=("$s|msg")
      ((sp >= $(state_req m.room.name))) && cand+=("$s|name")
      for t in "${SERVERS[@]}"; do
        [[ $t == "$s" ]] && continue
        tp=$(power_of "$t")
        # power change: any level up to the actor's own (can't raise above self)
        ((sp >= $(state_req m.room.power_levels))) && [[ $t != hs1 ]] && cand+=("$s|power|$t")
        # invite a member who is currently out
        if ((sp >= PL_INVITE)) && [[ ${MEMBER[$t]} == leave || ${MEMBER[$t]} == invite ]]; then
          [[ ${MEMBER[$t]} == leave ]] && cand+=("$s|invite|$t")
        fi
        # kick a present member ranked below the actor (never the anchor hs1)
        if ((sp >= PL_KICK)) && ((sp > tp)) && [[ $t != hs1 ]] \
          && [[ ${MEMBER[$t]} == join || ${MEMBER[$t]} == invite ]]; then
          cand+=("$s|kick|$t")
        fi
      done
      [[ $s != hs1 ]] && cand+=("$s|leave")    # anchor never leaves
    else
      cand+=("$s|join")                         # rejoin via the anchor resident
    fi
  done
  ((${#cand[@]} > 0)) || { record hs1 msg "$ROOM" "fuzz-$((SEQ += 1))"; return; }

  local pick=${cand[$((RANDOM % ${#cand[@]}))]}
  IFS='|' read -r s op t <<<"$pick"
  case "$op" in
    msg) record "$s" msg "$ROOM" "fuzz-$((SEQ += 1))" ;;
    name) record "$s" name "$ROOM" "fuzz-name-$((SEQ += 1))" ;;
    power)
      local lvl=$((RANDOM % ($(power_of "$s") + 1)))
      record "$s" power "$ROOM" "$t" "$lvl"
      ;;
    invite) record "$s" invite "$ROOM" "$t" ;;
    kick) record "$s" kick "$ROOM" "$t" "fuzz-kick" ;;
    leave) record "$s" leave "$ROOM" ;;
    join) record "$s" join "$ROOM" hs1 ;;
  esac
}

# Deliberately manufacture a state-res CONFLICT (#2): pick two joined servers
# that can both author m.room.name, ensure the link between them is cut, then
# have each set the name to a DIFFERENT value. Neither has seen the other's write
# (the link is down), so the two events are concurrent siblings — the only way to
# exercise the origin_server_ts/event_id tie-break. Ordinary fuzz writes one op
# per round and almost never collides two servers on one state key across a
# partition. Both writes are RECORD-tier (a stale-view reject is tolerated); when
# BOTH are accepted, assert_divergent proves the cut actually bit. m.room.name is
# the natural target — same state_key ("") on both sides, and it matches the
# curated concurrent-name scenario in smoke.sh. Returns 1 when no conflict is
# possible (fewer than two co-admins joined) so the caller falls through.
fuzz_conflict() {
  local req pairs=() k a b
  req=$(state_req m.room.name)
  for k in "${LINKS[@]}"; do
    read -r a b <<<"$(link_servers "$k")"
    [[ ${MEMBER[$a]} == join && ${MEMBER[$b]} == join ]] || continue
    (($(power_of "$a") >= req)) && (($(power_of "$b") >= req)) && pairs+=("$k")
  done
  ((${#pairs[@]} > 0)) || return 1

  k=${pairs[$((RANDOM % ${#pairs[@]}))]}
  read -r a b <<<"$(link_servers "$k")"
  if [[ ${UP[$k]} == 1 ]]; then              # need an active cut for concurrency
    issue partition cut "$k" || true
    UP[$k]=0
    ANY_CUT=1
    oplog "topology cut  $k (to manufacture a conflict)"
  fi

  local tag=$((SEQ += 1)) ra rb
  oplog "conflict $k: $a vs $b both set m.room.name under an active cut"
  issue "$a" name "$ROOM" "conflict-$tag-$a"; ra=$RC
  oplog "  $a name -> $([[ $ra -eq 0 ]] && echo ok || echo "${ERR:-rc$ra}")"
  issue "$b" name "$ROOM" "conflict-$tag-$b"; rb=$RC
  oplog "  $b name -> $([[ $rb -eq 0 ]] && echo ok || echo "${ERR:-rc$rb}")"

  if [[ $ra -eq 0 && $rb -eq 0 ]]; then
    CONFLICT_FIRED=1
    assert_divergent "$a" "$b" "conflict-$tag link=$k"
  else
    oplog "  conflict not both-accepted (ra=$ra rb=$rb); divergence not asserted (stale view, ok)"
  fi
  return 0
}

# Cut a random up-link or heal a random down-link. Biased toward healing when
# many links are down so episodes don't spend all their rounds fully isolated.
fuzz_topology() {
  local ups=() downs=() k
  for k in "${LINKS[@]}"; do [[ ${UP[$k]} == 1 ]] && ups+=("$k") || downs+=("$k"); done
  local do_heal=0
  if ((${#downs[@]} > 0)) && { ((${#ups[@]} == 0)) || ((RANDOM % 3 == 0)); }; then do_heal=1; fi
  if ((do_heal == 1)); then
    k=${downs[$((RANDOM % ${#downs[@]}))]}
    issue partition heal "$k" || true
    UP[$k]=1
    oplog "topology heal $k"
  elif ((${#ups[@]} > 0)); then
    k=${ups[$((RANDOM % ${#ups[@]}))]}
    issue partition cut "$k" || true
    UP[$k]=0
    ANY_CUT=1
    oplog "topology cut  $k"
  fi
}

# ~30% topology change; ~15% manufacture a state-res conflict (falling through to
# a normal mutation when no two co-admins are joined); else a model-valid mutation.
fuzz_round() {
  local roll=$((RANDOM % 100))
  if ((roll < 30)); then
    fuzz_topology
  elif ((roll < 45)); then
    fuzz_conflict || fuzz_mutate
  else
    fuzz_mutate
  fi
}

# ---- rig lifecycle -----------------------------------------------------------

cleanup() { "${COMPOSE[@]}" down -v >/dev/null 2>&1 || true; }
trap cleanup EXIT

wait_ready() {
  local hs=$1 url="http://localhost:${PORT[$1]}/_matrix/client/versions"
  local deadline=$((SECONDS + DEADLINE))
  until curl -fsS "$url" >/dev/null 2>&1; do
    ((SECONDS < deadline)) || fail "$hs never became ready"
    sleep 1
  done
}

# ---- main --------------------------------------------------------------------

command -v jq >/dev/null || fail "converge.sh needs jq"
command -v docker >/dev/null || fail "converge.sh needs docker"

log "seed=$SEED  episodes=$EPISODES rounds/episode=$ROUNDS_PER_EPISODE deadline=${DEADLINE}s"

log "starting rig"
if docker image inspect neutrino-testrig:latest >/dev/null 2>&1; then
  "${COMPOSE[@]}" up -d || fail "compose up failed"
else
  "${COMPOSE[@]}" up -d --build || fail "compose up --build failed"
fi
wait_ready hs1
wait_ready hs2
wait_ready hs3

log "create room on hs1, join hs2 + hs3"
issue hs1 create public "converge lab"
[[ $RC -eq 0 ]] || fail "createRoom failed (rc=$RC body=$BODY)"
ROOM=$(printf '%s' "$BODY" | jq -r '.room_id // empty')
[[ $ROOM == \!* ]] || fail "createRoom returned no room id (got '$ROOM')"
note "room=$ROOM"

expect_ok hs1 invite "$ROOM" hs2
expect_ok hs1 invite "$ROOM" hs3
expect_ok hs2 join "$ROOM" hs1
expect_ok hs3 join "$ROOM" hs1

barrier
exact_probes

ep=0
while ((ep < EPISODES)); do
  ep=$((ep + 1))
  log "episode $ep/$EPISODES: $ROUNDS_PER_EPISODE fuzz rounds (record tier)"
  r=0
  while ((r < ROUNDS_PER_EPISODE)); do
    r=$((r + 1))
    fuzz_round
  done
  barrier
  exact_probes
done

# Vacuity check: a run that cut links but NEVER observed divergence (no
# manufactured conflict, no pre-heal disagreement) almost certainly means the
# partitions aren't biting — the convergence assertions then passed over nothing.
# The per-conflict assert_divergent is the hard gate; this is the run-level signal.
if [[ $CONFLICT_FIRED == 1 ]]; then
  note "state-res conflicts exercised; divergence guard held on every manufactured conflict"
elif [[ $ANY_CUT == 1 && $DIVERGENCE_SEEN == 1 ]]; then
  note "no two co-admins coincided on a cut (no conflict manufactured), but partition divergence WAS observed pre-heal"
elif [[ $ANY_CUT == 1 ]]; then
  log "WARNING: links were cut but divergence was never observed and no conflict fired — partitions may not be biting, or this seed under-exercised them (try more EPISODES/ROUNDS_PER_EPISODE, or another seed)"
fi

log "PASS — all $EPISODES episodes converged (seed=$SEED)"
