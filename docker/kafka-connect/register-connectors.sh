#!/usr/bin/env bash
# Creates or updates the Debezium connectors from connectors/*.json (PUT /connectors/<name>/config: idempotent).
# Never deletes a connector: deleting would drop its offsets and could re-snapshot the outbox.
#
# Usage: ./register-connectors.sh [connector-name]     (default: every connectors/*.json)
#        CONNECT_URL=http://localhost:9084 ./register-connectors.sh postgres-cdc
#
# A green result only means Connect accepted the config and the task stayed RUNNING for a while. A config that
# cannot handle a change event only fails when such an event arrives: after changing postgres-cdc, write one product
# and check again (see "Changing the outbox connector" in the repo's docs/connectors.md).
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:9084}"
DIR="$(cd "$(dirname "$0")" && pwd)"
SECRETS="$DIR/secrets/postgres-cdc.properties"
STABLE_CHECKS=5      # consecutive RUNNING checks required
INTERVAL=3           # seconds between checks

command -v python3 >/dev/null || { echo "python3 is required (status parsing)" >&2; exit 1; }
if [[ ! -f "$SECRETS" ]]; then
  echo "Missing $SECRETS — copy secrets/postgres-cdc.properties.example and fill it in" >&2
  exit 1
fi
if ! curl -s -o /dev/null "$CONNECT_URL/connectors"; then
  echo "Kafka Connect is not reachable at $CONNECT_URL" >&2
  exit 1
fi

state() { # prints "<connector-state> <task-state>"
  curl -s "$CONNECT_URL/connectors/$1/status" | python3 -c '
import sys, json
try:
    s = json.load(sys.stdin)
    t = s.get("tasks") or []
    print(s["connector"]["state"], t[0]["state"] if t else "NONE")
except Exception:
    print("? ?")'
}

trace() {
  curl -s "$CONNECT_URL/connectors/$1/status" | python3 -c '
import sys, json
try:
    t = json.load(sys.stdin).get("tasks") or [{}]
    print(t[0].get("trace", "(no trace)"))
except Exception:
    print("(no status)")' >&2
}

register() {
  local file=$1 name out code
  name=$(basename "$file" .json)
  out=$(mktemp)
  echo "→ $name: PUT config"
  # -s: never print the request. The config holds only ${file:...} references, never a secret.
  code=$(curl -s -o "$out" -w '%{http_code}' -X PUT -H 'Content-Type: application/json' \
    --data @"$file" "$CONNECT_URL/connectors/$name/config") || code="curl-failed"
  if [[ "$code" != "200" && "$code" != "201" ]]; then
    echo "✗ $name: HTTP $code" >&2
    cat "$out" >&2 || true
    rm -f "$out"
    return 1
  fi
  rm -f "$out"

  # Connect may still report the previous task right after the PUT: require several consecutive RUNNING checks.
  local stable=0 tries=0 connector task
  sleep "$INTERVAL"
  while (( tries < 40 )); do
    read -r connector task < <(state "$name")
    if [[ "$connector" == "FAILED" || "$task" == "FAILED" ]]; then
      echo "✗ $name: connector=$connector task=$task" >&2
      trace "$name"
      echo "  Roll back: restore the previous connectors/$name.json (git) and run this script again;" >&2
      echo "  then: curl -X POST $CONNECT_URL/connectors/$name/tasks/0/restart" >&2
      return 1
    fi
    if [[ "$connector" == "RUNNING" && "$task" == "RUNNING" ]]; then
      stable=$((stable + 1))
      if (( stable >= STABLE_CHECKS )); then
        echo "✓ $name: config accepted, task RUNNING for $((STABLE_CHECKS * INTERVAL)) s."
        echo "  Not proven yet: write one product, then re-check '$CONNECT_URL/connectors/$name/status' and the topic headers."
        return 0
      fi
    else
      stable=0
    fi
    tries=$((tries + 1))
    sleep "$INTERVAL"
  done
  echo "✗ $name: not stable RUNNING (connector=$connector task=$task)" >&2
  trace "$name"
  return 1
}

if [[ $# -gt 0 ]]; then
  register "$DIR/connectors/$1.json"
else
  for f in "$DIR"/connectors/*.json; do register "$f"; done
fi
