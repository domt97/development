#!/usr/bin/env bash
# Creates or updates the Debezium connectors from connectors/*.json (PUT /connectors/<name>/config: idempotent).
# Never deletes a connector: deleting would drop its offsets and could re-snapshot the outbox.
#
# Usage: ./register-connectors.sh [connector-name]     (default: every connectors/*.json)
#        CONNECT_URL=http://localhost:9084 ./register-connectors.sh postgres-cdc
#
# A green result only means Connect accepted the config and the task stayed RUNNING for a while. A config that
# cannot handle a change event only fails when such an event arrives: after changing a connector, run its Verify in
# the repo's docs/connectors.md. Every connector is attempted; the script exits non-zero if any failed.
# inventory-outbox-cdc needs its publication first (kafka-connect/sql/inventory-outbox-cdc.sql): checked before the PUT.
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:9084}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-postgres}"
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

# Connector-specific preconditions; prints why and returns 1 when the connector must not be registered yet.
precheck() {
  case "$1" in
    inventory-outbox-cdc)
      local found
      found=$(docker exec "$POSTGRES_CONTAINER" psql -U postgres -d inventory -tAc \
        "SELECT 1 FROM pg_publication WHERE pubname = 'debezium_inventory_publication'" 2>/dev/null || true)
      if [[ "$found" != "1" ]]; then
        echo "✗ $1: publication debezium_inventory_publication missing in DB inventory." >&2
        echo "  Start inventory-service once (Flyway V5), then from docker/:" >&2
        echo "  docker exec -i postgres psql -1 -v ON_ERROR_STOP=1 -U postgres -d inventory -f - < kafka-connect/sql/inventory-outbox-cdc.sql" >&2
        return 1
      fi
      ;;
  esac
}

hint() {
  case "$1" in
    postgres-cdc) echo "  Not proven yet: write one product, then re-check '$CONNECT_URL/connectors/$1/status' and the topic headers." ;;
    inventory-outbox-cdc) echo "  Not proven yet: run 'Verify inventory-outbox-cdc' in docs/connectors.md." ;;
    *) echo "  Not proven yet: produce one change event and re-check '$CONNECT_URL/connectors/$1/status'." ;;
  esac
}

register() {
  local file=$1 name out code
  name=$(basename "$file" .json)
  precheck "$name" || return 1
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
      echo "  Fix the cause (or restore the previous connectors/$name.json from git) and run this script again;" >&2
      echo "  a task that stays FAILED needs: curl -X POST $CONNECT_URL/connectors/$name/tasks/0/restart" >&2
      return 1
    fi
    if [[ "$connector" == "RUNNING" && "$task" == "RUNNING" ]]; then
      stable=$((stable + 1))
      if (( stable >= STABLE_CHECKS )); then
        echo "✓ $name: config accepted, task RUNNING for $((STABLE_CHECKS * INTERVAL)) s."
        hint "$name"
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
  [[ -f "$DIR/connectors/$1.json" ]] || { echo "No connectors/$1.json" >&2; exit 1; }
  register "$DIR/connectors/$1.json"
else
  failed=""   # a string, not an array: empty arrays break under set -u on macOS's bash 3.2
  for f in "$DIR"/connectors/*.json; do
    register "$f" || failed="$failed $(basename "$f" .json)"
  done
  if [[ -n "$failed" ]]; then
    echo "✗ not registered or not RUNNING:$failed" >&2
    exit 1
  fi
fi
