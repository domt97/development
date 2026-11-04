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
nothing is lost, rows wait in the outbox). So apply the new config only once the connector has read every change event
written before the migration. Run the commands from `docker/`.

1. Deploy stores-service with the migration (e.g. `V9`) and keep the current connector running.
2. Note when the migration ran:
   `docker exec -it postgres psql -U postgres -d store -c "SELECT version, installed_on FROM store.flyway_schema_history WHERE version = '9';"`
   (drop `store.` if the table is in another schema). `installed_on` is in the database session's local time, while
   `outbox_events.created_at` is written in UTC: convert before comparing (e.g. `22:16` in UTC+7 is `15:16` UTC).
3. Create or update one product **after** that time (an update must change a field, otherwise nothing is written).
4. Check that this newest outbox row has the column and that the current connector has published it:
   - `docker exec -it postgres psql -U postgres -d store -c "SELECT event_type, created_at, correlation_context FROM store.outbox_events ORDER BY created_at DESC LIMIT 3;"`
     — the newest row is after the migration and its `correlation_context` is not `{}` (rows from before the migration
     show the default `{}`);
   - the record is on its topic (`ProductCreated` or `ProductUpdated`, consumer command in **Verify**, without the header
     check).

   Debezium reads the outbox in WAL order, so once it has published a row written after the migration it has also read
   every change event before it: none without the column is left to read.
5. `./kafka-connect/register-connectors.sh postgres-cdc`
6. **Verify.**

Alternative to step 4, by WAL position (slot `debezium_store_v2_slot`):
- record `SELECT pg_current_wal_lsn();` **right after** the migration, before any new outbox row; a position taken later
  is past every event and is only reached by events written after it;
- after writing a product, wait at least `offset.flush.interval.ms` (60 s by default):
  `SELECT confirmed_flush_lsn >= '<lsn>' FROM pg_replication_slots WHERE slot_name = 'debezium_store_v2_slot';` must
  return `t`. `confirmed_flush_lsn` only moves to the last event the connector has processed, never to
  `pg_current_wal_lsn()`, so never compare with the current position.

First application (2026-11-04, local stack, Debezium 2.2.1): applied after V9 with the step 4 check; the task stayed
`RUNNING`, the header reached `ProductCreated` and inventory-service logged the same `requestId`. Whether the task
really fails on a pre-migration event was not tested.

**Rollback:** restore the previous `connectors/postgres-cdc.json` (git), run the script again, and restart the task if it
stays `FAILED` (`curl -X POST localhost:9084/connectors/postgres-cdc/tasks/0/restart`). The old config ignores the column.

## Verify

1. Create or update one product (with `-H 'X-Request-Id: verify-1'` to follow it).
2. `curl -s localhost:9084/connectors/postgres-cdc/status` — connector and task still `RUNNING`.
3. The record has the header (the env overrides stop the CLI from binding the broker's JMX port):
   `docker exec -e JMX_PORT= -e KAFKA_JMX_OPTS= kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic ProductCreated --from-beginning --property print.headers=true --timeout-ms 10000`
   shows `correlationContext:{"requestId":"verify-1"}` on the new record.
4. inventory-service logs the consumer lines with `requestId=verify-1`.
