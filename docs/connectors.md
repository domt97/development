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
# fill in
cp kafka-connect/secrets/postgres-cdc.properties.example kafka-connect/secrets/postgres-cdc.properties
# readable by the container user (uid 1000)
chmod 644 kafka-connect/secrets/postgres-cdc.properties
# picks up the config provider and the mount
docker compose up -d --force-recreate kafka-connect
```

| Connector | Database / table | Slot | Publication | Topics |
|-----------|------------------|------|-------------|--------|
| `postgres-cdc` | `store.outbox_events` (stores-service) | `debezium_store_v2_slot` | `debezium_store_v2_publication` (created by Debezium, `filtered`) | `ProductCreated`, `ProductUpdated` (auto-created) |
| `inventory-outbox-cdc` | `inventory.outbox_events` (inventory-service V5) + `inventory.debezium_heartbeat` | `debezium_inventory_slot` | `debezium_inventory_publication` (created by `sql/inventory-outbox-cdc.sql`, insert-only) | the stock replies of ADR 0006 (pre-created) |

## First setup, and after a Kafka reset

The connectors' configs, offsets and status live in Kafka topics (`_connect_*`, kept in the `kafka_data` volume,
`docs/local-data.md`). When Kafka starts empty (new stack, or `kafka_data` deleted) there is no connector: register
them. Run from `docker/`, in this order:

```bash
# 1. inventory-service started once (Flyway V5 creates inventory.outbox_events); stores-service too for postgres-cdc.
# 2. topics with their partitions, before anything can auto-create them (idempotent)
./kafka/create-topics.sh
# 3. publication + heartbeat table for inventory-outbox-cdc (once per database; idempotent, refuses before V5)
docker exec -i postgres psql -1 -v ON_ERROR_STOP=1 -U postgres -d inventory -f - < kafka-connect/sql/inventory-outbox-cdc.sql
# 4. connectors (every one is attempted; exits non-zero if any is not RUNNING)
# an empty list means nothing is registered yet
curl -s localhost:9084/connectors
./kafka-connect/register-connectors.sh
```

After a **Kafka reset** only steps 2 and 4 are needed (Postgres keeps the slots, the publication and the table): step
2 **before** step 4, otherwise the first reply re-creates its topic with 1 partition.

Then **Verify**. `snapshot.mode=never`: the connector never re-reads the table, it only streams the WAL from its
replication slot:
- slot `debezium_store_v2_slot` still in Postgres (Kafka reset only) → it resumes where the slot stopped: outbox rows
  written while Kafka was down are published, rows published before are **not** published again;
- new database (no slot) → the slot is created now and only rows written from now on are published. Register the
  connector before creating products, or re-save them so they produce new outbox rows. For `inventory-outbox-cdc`,
  replies written before its slot exists are never published (only the expiry can write them today).
  (`snapshot.mode=never` is renamed `no_data` in later Debezium versions: check it on an upgrade.)

stores-service must already run the latest migration (currently `V9`, `outbox_events.correlation_context`); otherwise
see **Changing the outbox connector**.

## `postgres-cdc` (stores-service outbox)

Debezium 2.2.1 reads `store.outbox_events` and the `EventRouter` publishes each row to the topic named by
`event_type`, keyed by `aggregate_id`, with `payload` as the value. The column `correlation_context` (stores-service
`V9`) goes to the Kafka header `correlationContext` as JSON text (ADR 0004 in oms-workspace). `snapshot.mode=never`
(above): with the default `initial`, a connector without offsets would publish every row of `outbox_events` again, and
inventory's `ProductCreatedConsumer` is not idempotent yet.

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

## `inventory-outbox-cdc` (inventory-service outbox, ADR 0006)

inventory-service writes each stock reply to `inventory.outbox_events` with `id` = the payload `eventId` and **deletes
the row in the same transaction** (oms-workspace open question 19): the table is always empty and the INSERT reaches
Kafka only through the WAL. Same EventRouter settings as `postgres-cdc` (topic = `event_type`, key = `aggregate_id`
= orderId, header `id` = `eventId`, header `correlationContext`). What differs:

- **Insert-only publication**, created by `kafka-connect/sql/inventory-outbox-cdc.sql` (`publication.autocreate.mode:
  disabled`): pgoutput never sends the DELETEs, so no delete event reaches the EventRouter and no tombstone can be
  produced. The script refuses to run before inventory-service's V5: inventory-service uses Flyway's
  `baseline-on-migrate`, and a table created in a schema Flyway has not migrated yet would make it skip V1.
- **Heartbeat**: the publication holds only the outbox, so changes to the other inventory tables are not streamed and
  the slot would not advance between replies (WAL kept). Every 60 s Debezium runs `heartbeat.action.query`, which
  inserts a row into `inventory.debezium_heartbeat` and prunes rows older than one hour in the same statement. The
  INSERT is streamed, a `Filter` transform drops it (predicate `isHeartbeatTable`), the EventRouter only handles the
  outbox topic (predicate `isOutbox`), and the offset still moves the slot. Debezium's own heartbeat records go to the
  technical topic `__debezium-heartbeat.inventory` (created by `topic.creation.default.*`, so also 6 partitions and 14
  days; harmless). `debezium_heartbeat` is a table of the connector, not of the
  domain: no merchant data, not managed by Flyway.
- **Topics** are pre-created by `kafka/create-topics.sh` (6 partitions, 14 days: local values; production sizing is
  open question 30 in oms-workspace); `topic.creation.default.*` on the connector is only a safety net.
- **Privileges** (a dedicated user instead of `postgres`): REPLICATION, CONNECT on DB `inventory`, USAGE on schema
  `inventory`, SELECT on `outbox_events` and `debezium_heartbeat`, INSERT and DELETE on `debezium_heartbeat`, USAGE on
  `inventory.debezium_heartbeat_id_seq`. Ownership of the publication is not needed (autocreate disabled).

### Verify inventory-outbox-cdc

inventory-service has no command entry point yet, so a reply is produced by hand, exactly as its adapter writes
replies (several rows, then one `DELETE … IN`, one transaction). Run from `docker/` in bash or zsh; these functions
are shortcuts (`unalias` in case the `kt` alias of `docs/local-data.md` is set in the same shell):

```bash
unalias ktopics offsets psql_inv uuid 2>/dev/null
ktopics() { docker exec -e JMX_PORT= -e KAFKA_JMX_OPTS= kafka kafka-topics --bootstrap-server kafka:29092 "$@"; }
offsets() { docker exec -e JMX_PORT= -e KAFKA_JMX_OPTS= kafka kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server kafka:29092 --topic StockReservationExpired | awk -F: '{s+=$3} END {print s+0}'; }
psql_inv() { docker exec -i postgres psql -U postgres -d inventory "$@"; }
uuid() { psql_inv -tAc "SELECT gen_random_uuid()"; }
```

a. Publication shape — one row `t t f f f`, tables `debezium_heartbeat` and `outbox_events`:
   ```bash
   psql_inv -c "SELECT pubinsert, NOT puballtables, pubupdate, pubdelete, pubtruncate FROM pg_publication WHERE pubname = 'debezium_inventory_publication';"
   psql_inv -c "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = 'debezium_inventory_publication' ORDER BY 2;"
   ```
b. `BEFORE=$(offsets); echo $BEFORE` (no real expiry may run meanwhile; none can today).
c. Two replies in one transaction (run the block as is; the heredoc end marker must stay at the start of its line):

```bash
E1=$(uuid); E2=$(uuid); M=$(uuid); S=$(uuid); O1=$(uuid); O2=$(uuid); I1=$(uuid); I2=$(uuid)
psql_inv -v ON_ERROR_STOP=1 <<SQL
BEGIN;
INSERT INTO inventory.outbox_events (id, merchant_id, aggregate_type, aggregate_id, event_type, payload, correlation_context)
VALUES ('$E1', '$M', 'StockReservation', '$O1', 'StockReservationExpired',
        '{"eventId":"$E1","eventType":"StockReservationExpired","occurredAt":"2026-11-09T12:00:00Z","merchantId":"$M","storeId":"$S","orderId":"$O1","causationId":null,"lines":[{"orderItemId":"$I1","sku":"VERIFY-OUTBOX"}]}',
        '{"requestId":"verify-outbox-1"}'),
       ('$E2', '$M', 'StockReservation', '$O2', 'StockReservationExpired',
        '{"eventId":"$E2","eventType":"StockReservationExpired","occurredAt":"2026-11-09T12:00:00Z","merchantId":"$M","storeId":"$S","orderId":"$O2","causationId":null,"lines":[{"orderItemId":"$I2","sku":"VERIFY-OUTBOX"}]}',
        '{"requestId":"verify-outbox-1"}');
DELETE FROM inventory.outbox_events WHERE id IN ('$E1', '$E2');
COMMIT;
SQL
```

d. After a few seconds: `echo $(( $(offsets) - BEFORE ))` prints **2**. Then
   `docker exec -e JMX_PORT= -e KAFKA_JMX_OPTS= kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic StockReservationExpired --from-beginning --property print.headers=true --property print.key=true --timeout-ms 10000 | grep -E "$E1|$E2"`
   shows two records: key `$O1` / `$O2`, header `id:$E1` / `id:$E2`, `correlationContext:{"requestId": "verify-outbox-1"}`
   (JSONB adds the space).
   The delta of exactly 2 is the proof that no extra record (a tombstone or a delete event) was written.
e. `curl -s localhost:9084/connectors/inventory-outbox-cdc/status` — connector and task still `RUNNING`;
   `psql_inv -tAc "SELECT count(*) FROM inventory.outbox_events"` — `0`.
f. Heartbeat: optionally change a non-outbox row (writes WAL the publication does not stream; with an empty
   `inventories` skip it, the next heartbeat INSERT is after `L` anyway), e.g. adjust stock through inventory-service's REST, or
   `psql_inv -c "UPDATE inventory.inventories SET updated_at = now() WHERE id = (SELECT id FROM inventory.inventories LIMIT 1)"`,
   then `L=$(psql_inv -tAc "SELECT pg_current_wal_lsn()")`. Poll for up to 5 minutes:
   `psql_inv -tAc "SELECT confirmed_flush_lsn > '$L' FROM pg_replication_slots WHERE slot_name = 'debezium_inventory_slot'"`
   must become `t`. Also: `psql_inv -tAc "SELECT now() - max(beat_at) < interval '2 minutes' FROM inventory.debezium_heartbeat"` is `t`;
   `ktopics --list | grep -E 'debezium_heartbeat|__debezium-heartbeat'` shows `__debezium-heartbeat.inventory` and **not**
   `inventory.inventory.debezium_heartbeat`; after an hour the table holds about 60 rows or fewer.
g. Slot health (also the runbook query):
   ```bash
   psql_inv -c "SELECT slot_name, active, wal_status, confirmed_flush_lsn, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS behind FROM pg_replication_slots;"
   ```

The two verify records stay in `StockReservationExpired` for 14 days with unknown orderIds: order-service's consumer
(earliest offset on its first start) must tolerate them, as it must tolerate any reply for an unknown order.

### Runbook (inventory-outbox-cdc)

- **WAL kept / slot lag**: query (g). A stopped or failed connector keeps WAL on DB `inventory` until it runs again;
  `max_slot_wal_keep_size` is unlimited by default, so a long outage can fill the disk. `wal_status` `extended` or
  `unreserved` means act now.
- **Task RUNNING does not prove the heartbeat**: Debezium 2.2 is expected to only log most failures of
  `heartbeat.action.query` (`docker logs --since 10m kafka-connect 2>&1 | grep "Could not execute heartbeat action"`)
  and keep running; some connection errors may fail the task instead. Record which case was tested in *First
  application* (e.g. rename `debezium_heartbeat` for two minutes and rename it back; revoking INSERT has no effect
  while the connector uses the superuser `postgres` of the example secrets file). Check `max(beat_at)` (Verify f).
- **Connector or task FAILED**: read the trace (`curl -s localhost:9084/connectors/inventory-outbox-cdc/status`), fix
  the cause (or restore the previous `connectors/inventory-outbox-cdc.json` from git and re-run the script), then
  `curl -X POST localhost:9084/connectors/inventory-outbox-cdc/tasks/0/restart`.
- **Never drop or recreate `debezium_inventory_publication`** while the slot exists: decoding older WAL then fails.
  Never delete the connector (offsets). To drop DB `inventory`, drop the slot first (Postgres refuses otherwise). The
  slot is in use while Kafka Connect runs (pausing the connector keeps the connection), so stop Connect first:
  `docker compose stop kafka-connect` (this also pauses `postgres-cdc`; its slot keeps stores-service's events), then
  `docker exec -it postgres psql -U postgres -d inventory -c "SELECT pg_drop_replication_slot('debezium_inventory_slot');"`.
  Recreate the database, redo the first setup (inventory-service, SQL), then `docker compose start kafka-connect`:
  the connector creates a new slot (replies written meanwhile are not published).
- **Slot lost or recreated**: the replies written in the gap are gone for good (the outbox is always empty, nothing to
  re-snapshot). Note the time window. order-service's saga recovers by its timeouts (ADR 0006; for a confirm step see
  open question 54 in oms-workspace); locally, re-run the scenario.

### First application

2026-11-09, local stack (Debezium Postgres connector 2.2.1 on cp-kafka-connect 7.3.0, Postgres 16, macOS, zsh).
SQL script, `create-topics.sh` and `register-connectors.sh` as in *First setup*; both connectors `RUNNING`.

- a. publication `t t f f f`, tables `debezium_heartbeat` and `outbox_events`.
- b–d. two replies inserted and deleted in one transaction: offset delta **2**; two records with key = orderId, header
  `id` = eventId and `correlationContext:{"requestId": "verify-outbox-1"}`; JSONB normalises the spacing and the
  payload's key order; no tombstone or delete event.
- e. connector and task still `RUNNING`; `outbox_events` count `0`.
- f. `confirmed_flush_lsn` passed `L` within about 3 minutes (first poll `f` at 21:04:30, `t` at 21:07:01; heartbeat
  60 s + offset flush 60 s); `max(beat_at)` fresh, 4 rows; of the heartbeat topics only `__debezium-heartbeat.inventory`
  exists, no `inventory.inventory.debezium_heartbeat`.
- g. `debezium_inventory_slot` active, `reserved`, 1832 bytes behind.

The condition of open question 19 (one record per reply, no tombstone, task RUNNING) holds locally. **Not tested:** a
failing heartbeat query (runbook), and the behaviour after a Kafka or Connect restart.

