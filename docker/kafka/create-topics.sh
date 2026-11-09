#!/usr/bin/env bash
# Creates the Kafka topics of the stock commands and replies (ADR 0006 in oms-workspace) before any producer or
# connector needs them: a topic auto-created by the broker gets 1 partition, and adding partitions later moves keys
# (orderId) to other partitions and breaks their order.
#
# Usage (from docker/, the stack running): ./kafka/create-topics.sh
# Idempotent. For an existing topic: wrong partition count → error (never altered); other retention → set.
#
# 6 partitions and 14 days are LOCAL values (room for a few consumer instances; two weeks to inspect or replay).
# Production sizing and retention: open question 30 in oms-workspace.
set -euo pipefail

PARTITIONS=6
RETENTION_MS=1209600000   # 14 days
TOPICS=(
  # replies (inventory-service outbox, connector inventory-outbox-cdc)
  StockReserved StockReservationRejected StockReservationConfirmed StockReservationConfirmationFailed
  StockReservationReleased StockReservationExpired
  # commands (order-service outbox; its connector is not built yet)
  ReserveStock ConfirmStockReservation ReleaseStockReservation
)

# The env overrides stop the CLI from binding the broker's JMX port (JMX_PORT is set on the kafka container).
kt() { docker exec -e JMX_PORT= -e KAFKA_JMX_OPTS= kafka "$@"; }
BOOTSTRAP=kafka:29092

failed=0
for topic in "${TOPICS[@]}"; do
  kt kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic "$topic" \
    --partitions "$PARTITIONS" --replication-factor 1 --config "retention.ms=$RETENTION_MS" >/dev/null

  partitions=$(kt kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic "$topic" \
    | sed -n 's/.*PartitionCount: *\([0-9]*\).*/\1/p' | head -1)
  if [[ "$partitions" != "$PARTITIONS" ]]; then
    echo "✗ $topic has $partitions partitions, expected $PARTITIONS." >&2
    echo "  Never add partitions (keys would move). Locally: stop producers and consumers, then" >&2
    echo "  docker exec -e JMX_PORT= -e KAFKA_JMX_OPTS= kafka kafka-topics --bootstrap-server $BOOTSTRAP --delete --topic $topic" >&2
    echo "  wait a few seconds (the deletion is asynchronous), and run this script again." >&2
    failed=1
    continue
  fi

  retention=$(kt kafka-configs --bootstrap-server "$BOOTSTRAP" --describe --entity-type topics --entity-name "$topic" \
    | tr ' ,' '\n\n' | sed -n 's/^retention\.ms=\([0-9-]*\).*/\1/p' | head -1)
  if [[ "$retention" != "$RETENTION_MS" ]]; then
    kt kafka-configs --bootstrap-server "$BOOTSTRAP" --alter --entity-type topics --entity-name "$topic" \
      --add-config "retention.ms=$RETENTION_MS" >/dev/null
    echo "✓ $topic: $PARTITIONS partitions, retention.ms set to $RETENTION_MS (was ${retention:-broker default})"
  else
    echo "✓ $topic: $PARTITIONS partitions, retention.ms $RETENTION_MS"
  fi
done
exit "$failed"
