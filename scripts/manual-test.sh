#!/usr/bin/env bash
# End-to-end manual test of a real three-node cluster.
#
# Runs actual processes over actual HTTP with actual fsyncs — everything the
# unit tests deliberately exclude. Unit tests prove the algorithm; this proves
# the wiring, and the two fail in completely different ways.
#
#   ./scripts/manual-test.sh
#
# Exits non-zero on the first failed assertion.
set -uo pipefail

DIR=${DIR:-/tmp/raftkv-manual}
BIN=${BIN:-/tmp/raftkv}
PASS=0
FAIL=0

RED=$'\033[31m'; GREEN=$'\033[32m'; DIM=$'\033[2m'; OFF=$'\033[0m'

step()  { printf "\n%s── %s %s\n" "$DIM" "$*" "$OFF"; }
ok()    { PASS=$((PASS+1)); printf "  %s✓%s %s\n" "$GREEN" "$OFF" "$*"; }
bad()   { FAIL=$((FAIL+1)); printf "  %s✗%s %s\n" "$RED" "$OFF" "$*"; }
check() { if [ "$2" = "$3" ]; then ok "$1 ($2)"; else bad "$1: got '$2', want '$3'"; fi; }

cleanup() {
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done
  sleep 0.3
  for p in "${PIDS[@]:-}"; do kill -9 "$p" 2>/dev/null; done
}
trap cleanup EXIT

rm -rf "$DIR"; mkdir -p "$DIR"
PIDS=()

start_node() {
  local id=$1 others=$2
  "$BIN" -id "$id" \
    -raft "127.0.0.1:190$(printf '%02d' "$id")" \
    -api  "127.0.0.1:180$(printf '%02d' "$id")" \
    -peers "$others" -data "$DIR" -tick 30 \
    >"$DIR/node$id.log" 2>&1 &
  PIDS+=($!)
  eval "PID_$id=$!"
}

api() { echo "127.0.0.1:180$(printf '%02d' "$1")"; }

# peers_for prints the -peers string for a node: every OTHER node.
peers_for() {
  local self=$1 out=""
  for id in 1 2 3; do
    [ "$id" = "$self" ] && continue
    [ -n "$out" ] && out="$out,"
    out="${out}${id}=http://127.0.0.1:190$(printf '%02d' "$id")"
  done
  echo "$out"
}

# Poll until a leader exists, or give up. No sleeps-and-hope.
wait_for_leader() {
  for _ in $(seq 1 100); do
    for id in "$@"; do
      r=$(curl -s --max-time 1 "http://$(api "$id")/status" 2>/dev/null)
      if echo "$r" | grep -q '"role":"Leader"'; then
        echo "$id"; return 0
      fi
    done
    sleep 0.1
  done
  return 1
}

role_of() { curl -s --max-time 1 "http://$(api "$1")/status" | sed 's/.*"role":"\([^"]*\)".*/\1/'; }
term_of() { curl -s --max-time 1 "http://$(api "$1")/status" | sed 's/.*"term":\([0-9]*\).*/\1/'; }

###############################################################################
step "1. start a three-node cluster"
start_node 1 "$(peers_for 1)"
start_node 2 "$(peers_for 2)"
start_node 3 "$(peers_for 3)"

LEADER=$(wait_for_leader 1 2 3)
if [ -z "$LEADER" ]; then bad "no leader elected within 10s"; exit 1; fi
ok "leader elected: node $LEADER (term $(term_of "$LEADER"))"

FOLLOWERS=()
for id in 1 2 3; do [ "$id" != "$LEADER" ] && FOLLOWERS+=("$id"); done
for f in "${FOLLOWERS[@]}"; do check "node $f is a follower" "$(role_of "$f")" "Follower"; done

TERMS=$(for id in 1 2 3; do term_of "$id"; done | sort -u | wc -l | tr -d ' ')
check "all nodes agree on the term" "$TERMS" "1"

###############################################################################
step "2. write and read back through the leader"
curl -s -XPUT --data-binary 'hello world' "http://$(api "$LEADER")/kv/greeting" >/dev/null
check "linearizable read returns the write" \
  "$(curl -s "http://$(api "$LEADER")/kv/greeting")" "hello world"

###############################################################################
step "3. the write replicated to every node"
sleep 0.5
for id in 1 2 3; do
  check "node $id has the key" "$(curl -s "http://$(api "$id")/keys")" '["greeting"]'
done

###############################################################################
step "4. a follower refuses writes and names the leader"
F=${FOLLOWERS[0]}
CODE=$(curl -s -o /dev/null -w '%{http_code}' -XPUT --data-binary 'x' "http://$(api "$F")/kv/nope")
check "follower rejects a write with 421" "$CODE" "421"
HDR=$(curl -s -D - -o /dev/null -XPUT --data-binary 'x' "http://$(api "$F")/kv/nope" | grep -i '^x-raft-leader' | tr -d '\r' | awk '{print $2}')
if [ -n "$HDR" ]; then ok "follower reports the leader address ($HDR)"; else bad "no X-Raft-Leader header"; fi

###############################################################################
step "5. exactly-once: the same client seq applied twice"
curl -s -XPUT -H 'X-Client-ID: 42' -H 'X-Client-Seq: 1' --data-binary 'v1' \
  "http://$(api "$LEADER")/kv/counter" >/dev/null
curl -s -XPUT -H 'X-Client-ID: 42' -H 'X-Client-Seq: 1' --data-binary 'v2' \
  "http://$(api "$LEADER")/kv/counter" >/dev/null
check "duplicate seq did not overwrite" \
  "$(curl -s "http://$(api "$LEADER")/kv/counter")" "v1"
curl -s -XPUT -H 'X-Client-ID: 42' -H 'X-Client-Seq: 2' --data-binary 'v2' \
  "http://$(api "$LEADER")/kv/counter" >/dev/null
check "a new seq does apply" \
  "$(curl -s "http://$(api "$LEADER")/kv/counter")" "v2"

###############################################################################
step "6. kill the leader; the cluster re-elects and keeps the data"
OLD=$LEADER
eval "kill \$PID_$OLD"
sleep 0.5

NEW=$(wait_for_leader "${FOLLOWERS[@]}")
if [ -z "$NEW" ]; then bad "no leader after the old one died"; else
  ok "re-elected: node $NEW (term $(term_of "$NEW"))"
  if [ "$(term_of "$NEW")" -gt "1" ]; then ok "term advanced past the dead leader's"; fi
  check "committed data survived the failover" \
    "$(curl -s "http://$(api "$NEW")/kv/greeting")" "hello world"
  curl -s -XPUT --data-binary 'after failover' "http://$(api "$NEW")/kv/post" >/dev/null
  check "the new leader accepts writes" \
    "$(curl -s "http://$(api "$NEW")/kv/post")" "after failover"
fi

###############################################################################
step "7. restart the dead node; it catches up from disk + the leader"
start_node "$OLD" "$(peers_for "$OLD")"
sleep 2
GOT=$(curl -s "http://$(api "$OLD")/keys")
if echo "$GOT" | grep -q 'post'; then
  ok "restarted node caught up (keys: $GOT)"
else
  bad "restarted node did not catch up (keys: $GOT)"
fi
check "restarted node is a follower, not a leader" "$(role_of "$OLD")" "Follower"

###############################################################################
step "8. the on-disk log is human-readable JSON lines"
if [ -f "$DIR/raft-$NEW.log" ]; then
  LINES=$(wc -l < "$DIR/raft-$NEW.log" | tr -d ' ')
  KINDS=$(grep -o '"kind":"[a-z]*"' "$DIR/raft-$NEW.log" | sort -u | tr '\n' ' ')
  ok "log has $LINES records, kinds: $KINDS"
else
  bad "no log file at $DIR/raft-$NEW.log"
fi

###############################################################################
printf "\n%s────────────────────────────%s\n" "$DIM" "$OFF"
printf "  passed: %s%d%s   failed: %s%d%s\n" "$GREEN" "$PASS" "$OFF" \
  "$( [ "$FAIL" -gt 0 ] && echo "$RED" || echo "$DIM")" "$FAIL" "$OFF"
[ "$FAIL" -eq 0 ]
