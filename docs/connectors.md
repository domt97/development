# Kafka Connect connectors

Connector configs live in `docker/kafka-connect/connectors/<name>.json` (the `config` object only) and are applied
with `docker/kafka-connect/register-connectors.sh`, which `PUT`s each config (create or update, safe to re-run) and
waits until the task has stayed `RUNNING` for ~15 s. Connectors are never deleted: that would drop their offsets.
A green result only means the config was accepted; a config that cannot handle a change event fails only when such an
event arrives, so always finish with the **Verify** step below.

## Secrets

Kafka Connect's `FileConfigProvider` reads them from the git-ignored `docker/kafka-connect/secrets/` folder, mounted
read-only at `/opt/connect-secrets` (`${file:/opt/connect-secrets/postgres-cdc.properties:password}`). They never
appear in git, in the config returned by the REST API, or in the `_connect_configs` topic.

```bash
cd docker
cp kafka-connect/secrets/postgres-cdc.properties.example kafka-connect/secrets/postgres-cdc.properties   # fill in
chmod 644 kafka-connect/secrets/postgres-cdc.properties    # readable by the container user (uid 1000)
docker compose up -d --force-recreate kafka-connect        # picks up the config provider and the mount
```

## First setup (new database only)

Use this only when no `postgres-cdc` connector exists yet **and** stores-service already runs the latest migration
(currently `V9`, which adds `outbox_events.correlation_context`). Otherwise follow **Changing the outbox connector**.

```bash
curl -s localhost:9084/connectors          # must not list a connector on slot debezium_store_v2_slot
./kafka-connect/register-connectors.sh
```

Then **Verify**.

## `postgres-cdc` (stores-service outbox)

Debezium 2.2.1 reads `store.outbox_events` and the `EventRouter` publishes each row to the topic named by
`event_type`, keyed by `aggregate_id`, with `payload` as the value. The column `correlation_context` (stores-service
`V9`) goes to the Kafka header `correlationContext` as JSON text (ADR 0004 in oms-workspace).

## Changing the outbox connector (adding a placed column)

The `EventRouter` reads every `table.fields.additional.placement` column from each change event. Change events written
before the column existed do not have it, and the task is **expected** to fail on them (all events stop until rollback;
nothing is lost, rows wait in the outbox; confirm and record this the first time you test it). So apply the new
config only after the replication slot has passed the migration:

1. Deploy stores-service with the migration (e.g. `V9`).
2. Record the WAL position right after it:
   `docker exec -it postgres psql -U postgres -d store -c "SELECT pg_current_wal_lsn();"`
3. Create or update one product (so the connector reads a change event with the new column).
4. Wait until the slot has confirmed past that position (at least `offset.flush.interval.ms`, 60 s by default):
   `SELECT confirmed_flush_lsn >= '<lsn from step 2>' FROM pg_replication_slots WHERE slot_name = 'debezium_store_v2_slot';`
   must return `t`.
5. `./kafka-connect/register-connectors.sh postgres-cdc`
6. **Verify.**

**Rollback:** restore the previous `connectors/postgres-cdc.json` (git), run the script again, and restart the task if it
stays `FAILED` (`curl -X POST localhost:9084/connectors/postgres-cdc/tasks/0/restart`). The old config ignores the column.

## Verify

1. Create or update one product (with `-H 'X-Request-Id: verify-1'` to follow it).
2. `curl -s localhost:9084/connectors/postgres-cdc/status` — connector and task still `RUNNING`.
3. The record has the header (the env overrides stop the CLI from binding the broker's JMX port):
   `docker exec -e JMX_PORT= -e KAFKA_JMX_OPTS= kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic ProductCreated --from-beginning --property print.headers=true --timeout-ms 10000`
   shows `correlationContext:{"requestId":"verify-1"}` on the new record.
4. inventory-service logs the consumer lines with `requestId=verify-1`.
