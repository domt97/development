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
| `postgres-cdc` | `store.outbox_events` (stores-service) + `store.debezium_heartbeat` | `debezium_store_v2_slot` | `debezium_store_v2_publication` (`sql/store-outbox-cdc.sql`, insert-only; first created by Debezium's former `filtered` autocreate) | `ProductCreated`, `ProductUpdated` (auto-created) |
| `inventory-outbox-cdc` | `inventory.outbox_events` (inventory-service V5) + `inventory.debezium_heartbeat` | `debezium_inventory_slot` | `debezium_inventory_publication` (`sql/inventory-outbox-cdc.sql`, insert-only) | the stock replies of ADR 0006 (pre-created) |

Both services insert each outbox row with `id` = the payload `eventId` and **delete it in the same transaction**
(inventory-service since inventory-service#40, stores-service since stores-service#59): the
tables are always empty, the INSERT reaches Kafka only through the WAL, and the insert-only publications never send the
DELETEs (`tombstones.on.delete=false` and the EventRouter, which ignores DELETE operations on an outbox, are the second
and third guard). Consequence: a lost or recreated slot loses the events of the gap for good (runbooks below).

## WAL, replication slots and the heartbeat

Read this before the runbooks: it explains why a stopped or idle connector can fill the Postgres disk.
An **LSN** (log sequence number) is a position in the WAL, e.g. `0/11EBA780`. The times below stand in for LSNs.

**The big picture first.**
- **One WAL per Postgres server.** The WAL (write-ahead log, `pg_wal/`) is the server's journal of every change, for
  every database on it. Locally one `postgres` container holds DB `store` and DB `inventory`, so both write to the same
  WAL. Postgres uses it for crash recovery, replicas, point-in-time restore, and logical decoding (Debezium).
- **Many slots, each with its own positions.** A server can have several replication slots (`max_replication_slots`, 10
  in `docker-compose.yml`), one per reader. Locally there are two: `debezium_store_v2_slot` (DB `store`) and
  `debezium_inventory_slot` (DB `inventory`); a physical replica configured with `primary_slot_name` would add a third.
  A logical slot has two positions:
  - `confirmed_flush_lsn`: what its reader reported as done ("done up to X", sent over its replication connection).
    Postgres never guesses it.
  - `restart_lsn`: where decoding would have to restart. Postgres derives it from the reported position and from the
    transactions still open, so it is at or behind `confirmed_flush_lsn`. A long-running transaction keeps it back.
- **WAL is freed only at a checkpoint, and only up to the oldest `restart_lsn`.** A checkpoint (every
  `checkpoint_timeout`, 5 minutes by default, or earlier when `max_wal_size` is reached) writes the changed pages to the
  table files. WAL files before its redo point are then recycled, but never past the oldest `restart_lsn` of all slots,
  past WAL not yet archived, or inside `wal_keep_size`. A slot can only make Postgres keep WAL **longer**. It can never
  make Postgres drop WAL that crash recovery still needs, so no committed data is lost.

  ```
  slot                       reader                 receives                            restart_lsn
  replica_1 (example)        physical replica       all WAL                             17:00
  debezium_inventory_slot    inventory-outbox-cdc   INSERTs of its publication only     16:59
  debezium_store_v2_slot     postgres-cdc           INSERTs of its publication only     09:00  ← oldest
  → the checkpoint may recycle only WAL before 09:00, for the whole server
  ```

**Why a Debezium slot stops.** A logical slot sends Debezium only the changes of the tables in its publication
(insert-only here). Changes to every other table and database are still written to the WAL. The decoding reads past
them (other databases are skipped, other tables are filtered by the publication), and since Postgres 15 transactions
that touch no published table are not sent at all. Debezium reports a position only for a record it has processed:
Kafka Connect commits the offset (every 60 s by default) and Debezium then tells Postgres the LSN. So with no INSERT
into the outbox, Debezium has nothing to report, and the slot stays where the last event was. It becomes the oldest
slot and holds the WAL of the **whole server**, including inventory's and every other write, until the next outbox
INSERT. Then it jumps forward in one step; nothing is reprocessed, because the decoding had been reading the WAL all
along. What goes wrong during the wait:
- the disk fills;
- a connector restart re-reads the whole held range;
- the slot also holds back `catalog_xmin`, so VACUUM cannot clean the system catalogs;
- "connector dead" cannot be told from "no events".

A physical replica, while connected, receives all WAL, so its position keeps moving. On 2026-11-09, before the store
heartbeat existed, query (g) showed `debezium_store_v2_slot` 582 kB behind, while `debezium_inventory_slot`, which
already had its heartbeat, was 1832 bytes behind.

**What the heartbeat does.** Every `heartbeat.interval.ms` (60 s) Debezium runs `heartbeat.action.query`: one statement
that INSERTs a row into `<schema>.debezium_heartbeat` (a table in the same publication as the outbox) and deletes rows
older than one hour. The INSERT reaches Debezium, the `dropHeartbeat` Filter drops it before the EventRouter, the offset
is committed, and the slot moves forward. With a 60 s heartbeat and a 60 s offset flush, it moves about once a minute
and trails by up to about two minutes; in both *First application* runs the check passed after about 2.5 minutes. WAL
is then kept only for the last few minutes. The same interval also writes Debezium's own record `{"ts_ms": …}` to
`__debezium-heartbeat.<topic.prefix>`. That record is built into Debezium and exists only when `heartbeat.interval.ms`
is above 0. It is not the table's row, and no service reads it.

**What it does not do.**
- It does not touch business tables or block queries, and it does not affect backups, replicas or crash recovery. Its
  cost is one small statement a minute per database, on a table of about 60 rows.
- **When the connector stops, the heartbeat stops too**, and the slot holds WAL again until the connector runs.
- **When the heartbeat query fails**, Debezium 2.2 is expected to only log most such failures, and the task stays
  `RUNNING` while the slot stalls (runbook *Task RUNNING does not prove the heartbeat*).

For both cases: alert on WAL kept (`restart_lsn`, query (g)), `wal_status` and `max(beat_at)`, and size
`max_slot_wal_keep_size` (oms-workspace open question 55). A slot that exceeds that limit is invalidated
(`wal_status` `lost`): the task is expected to fail, and the slot must be dropped and recreated, losing the events of the gap
(runbooks below).

## First setup, and after a Kafka reset

The connectors' configs, offsets and status live in Kafka topics (`_connect_*`, kept in the `kafka_data` volume,
`docs/local-data.md`). When Kafka starts empty (new stack, or `kafka_data` deleted) there is no connector: register
them. Run from `docker/`, in this order:

```bash
# 1. stores-service (Flyway V9 or later) and inventory-service (Flyway V5) started once.
# 2. topics with their partitions, before anything can auto-create them (idempotent)
./kafka/create-topics.sh
# 3. publications + heartbeat tables (once per database; idempotent, refuse before V9 / V5;
#    why the heartbeat: "WAL, replication slots and the heartbeat" above)
docker exec -i postgres psql -1 -v ON_ERROR_STOP=1 -U postgres -d store -f - < kafka-connect/sql/store-outbox-cdc.sql
docker exec -i postgres psql -1 -v ON_ERROR_STOP=1 -U postgres -d inventory -f - < kafka-connect/sql/inventory-outbox-cdc.sql
# 4. connectors (every one is attempted; exits non-zero if any is not RUNNING or its publication has the wrong shape)
# an empty list means nothing is registered yet
curl -s localhost:9084/connectors
./kafka-connect/register-connectors.sh
```

After a **Kafka reset** only steps 2 and 4 are needed (Postgres keeps the slots, the publications and the tables):
step 2 **before** step 4, otherwise the first reply re-creates its topic with 1 partition.

Then **Verify**. `snapshot.mode=never`: the connector never re-reads the table, it only streams the WAL from its
replication slot:
- slot still in Postgres (Kafka reset only) → it resumes where the slot stopped: events written while Kafka was down
  are published, events published before are **not** published again;
- new database (no slot) → the slot is created now and only events written from now on are published. Register the
  connectors before creating products (or give each product a real change later); for `inventory-outbox-cdc`, replies
  written before its slot exists are never published (only the expiry can write them today).
  (`snapshot.mode=never` is renamed `no_data` in later Debezium versions: check it on an upgrade.)

## `postgres-cdc` (stores-service outbox)

Debezium 2.2.1 reads `store.outbox_events` and the `EventRouter` publishes each row to the topic named by
`event_type`, keyed by `aggregate_id`, with `payload` as the value and the header `id` = the row `id` = the payload
`eventId`. The column `correlation_context` (stores-service `V9`) goes to the Kafka header `correlationContext` as JSON
text (ADR 0004 in oms-workspace). stores-service must run at least `V9`. Settings as `inventory-outbox-cdc` (below):
insert-only publication from `kafka-connect/sql/store-outbox-cdc.sql` (`publication.autocreate.mode: disabled`),
heartbeat every 60 s into `store.debezium_heartbeat` (dropped by the `Filter` before the EventRouter; Debezium's own
records go to `__debezium-heartbeat.store`, auto-created with 1 partition), `snapshot.mode=never`.

**Switching an existing stack** (the publication was created by Debezium's former `filtered` autocreate with every
operation): run `store-outbox-cdc.sql` **first** (it `ALTER`s the publication to insert-only and adds the heartbeat table,
in place: never drop it, the slot decodes with it), then **at once** `./kafka-connect/register-connectors.sh postgres-cdc`.
The SQL must come first because the new heartbeat query needs the table and the precheck needs the new shape. Whether the
old task, if it restarts in between, re-syncs the publication's table list (`filtered` mode) is not known: Verify (a)
after the PUT shows it, and running the SQL again adds the heartbeat table back. Run the SQL **before** stores-service
`V10` (or later) starts on a stack with this connector: from V10 on, stores-service deletes every outbox row (V10 the old
ones, then each new one in its transaction); without the insert-only publication those DELETEs reach Debezium and only
the EventRouter, untested here, would drop them.

**Privileges** (a dedicated user instead of `postgres`): REPLICATION, CONNECT on DB `store`, USAGE on schema `store`,
SELECT on `outbox_events` and `debezium_heartbeat`, INSERT and DELETE on `debezium_heartbeat`, USAGE on
`store.debezium_heartbeat_id_seq`. Ownership of the publication is not needed (autocreate disabled).

### Verify postgres-cdc

Run from `docker/` in bash or zsh, in one terminal (the functions only exist in the shell they were pasted into):

```bash
unalias ktopics psql_store toffsets 2>/dev/null
ktopics() { docker exec -e JMX_PORT= -e KAFKA_JMX_OPTS= kafka kafka-topics --bootstrap-server kafka:29092 "$@"; }
psql_store() { docker exec -i postgres psql -U postgres -d store "$@"; }
toffsets() { docker exec -e JMX_PORT= -e KAFKA_JMX_OPTS= kafka kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server kafka:29092 --topic "$1" | awk -F: '{s+=$3} END {print s+0}'; }
```

a. After the PUT, publication shape — one row `t t f f f`, tables `debezium_heartbeat` and `outbox_events`:
   `psql_store -c "SELECT pubinsert, NOT puballtables, pubupdate, pubdelete, pubtruncate FROM pg_publication WHERE pubname = 'debezium_store_v2_publication';"`
   and `psql_store -c "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = 'debezium_store_v2_publication' ORDER BY 2;"`.
b. First start of a stores-service with `V10` (deletes the old rows): note `toffsets ProductCreated` and
   `toffsets ProductUpdated` before, start it, check both again — **unchanged** (no delete event, no tombstone).
c. Create a product with `-H 'X-Request-Id: verify-2'`: `ProductCreated` grows by **1**;
   `docker exec -e JMX_PORT= -e KAFKA_JMX_OPTS= kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic ProductCreated --from-beginning --property print.headers=true --timeout-ms 10000 | tail -1`
   shows `id:<x>,correlationContext:{"requestId": "verify-2"}` and a payload with `"eventId": "<x>"` (the same value);
   inventory-service logs the consumer lines with `requestId=verify-2`; `psql_store -tAc "SELECT count(*) FROM store.outbox_events"` is `0`.
d. Update that product with a SKU price `120000.00`: `ProductUpdated` grows by 1 and its payload shows the price as `120000`.
e. Excel import of two products: `ProductCreated` grows by **2**.
f. Heartbeat: `L=$(psql_store -tAc "SELECT pg_current_wal_lsn()")`, then poll for up to 5 minutes
   `psql_store -tAc "SELECT confirmed_flush_lsn > '$L' FROM pg_replication_slots WHERE slot_name = 'debezium_store_v2_slot'"`
   until `t`; `psql_store -tAc "SELECT now() - max(beat_at) < interval '2 minutes' FROM store.debezium_heartbeat"` is `t`;
   `ktopics --list | grep -E 'debezium_heartbeat|__debezium-heartbeat'` shows `__debezium-heartbeat.store`
   and no `store.store.debezium_heartbeat`; `curl -s localhost:9084/connectors/postgres-cdc/status` — `RUNNING`.
g. Optional, one transaction for the product and its event: create a temporary slot
   `psql_store -c "SELECT pg_create_logical_replication_slot('verify_xid', 'test_decoding')"`, create one product, then
   `psql_store -c "SELECT xid, data FROM pg_logical_slot_peek_changes('verify_xid', NULL, NULL) WHERE data ~ 'store_products|outbox_events' "`
   — the `store_products` INSERT and the `outbox_events` INSERT and DELETE have the same `xid`. Then
   `psql_store -c "SELECT pg_drop_replication_slot('verify_xid')"` (**always**: an unused slot keeps WAL).

### Runbook (postgres-cdc)

The inventory runbook below applies with `store` / `debezium_store_v2_slot` / `debezium_store_v2_publication`
(slot lag, heartbeat, FAILED task, never drop the publication, dropping the database). Specific to the catalog:
- **Slot lost or recreated**: the `ProductCreated` / `ProductUpdated` written in the gap are gone for good (the outbox is
  empty; no resync tool, oms-workspace open question 40). Note the window, in the same time base as
  `store.store_products.created_at` / `updated_at` (`TIMESTAMP` without time zone, like `installed_on` below). Find the
  products created or updated in it (`SELECT id, store_id FROM store.store_products WHERE updated_at BETWEEN … OR
  created_at BETWEEN …`). A plain re-save publishes nothing when nothing changed (stores-service skips an unchanged
  update), so give each product a real change and revert it: two `ProductUpdated` with higher versions; inventory applies
  the newer one and creates missing rows from a `ProductUpdated`, which also covers a lost `ProductCreated`.

### First application (postgres-cdc, insert-only + heartbeat)

2026-11-11, local stack (Debezium 2.2.1 on cp-kafka-connect 7.3.0, Postgres 16, macOS, zsh), with this branch and
stores-service#59. Existing stack: the publication had been created by the former `filtered` autocreate.

- SQL (*Switching an existing stack*) and `register-connectors.sh postgres-cdc`, then (a) publication shape: reported by
  the user as expected; output not recorded. Indirect evidence: the script's precheck refused the PUT unless the publication
  was insert-only with exactly both tables at the PUT, and step f shows the new config (heartbeat) running.
- b. before stores-service V10: `ProductCreated` 1, `ProductUpdated` 2, `store.outbox_events` 24 rows; after the start
  with V10 and steps c and d: 2 and 3, outbox `0`. V10 added no record: derived from these totals and the deltas of c
  and d (+1 each); the counts were not re-read between V10 and c.
- c. product created with `X-Request-Id: verify-2`: `ProductCreated` delta **1**; header
  `id:013be7c5-…` = payload `"eventId": "013be7c5-…"` (open question 45 fixed), `correlationContext:{"requestId": "verify-2"}`;
  outbox count `0`. The inventory-service log lines with `requestId=verify-2` were not recorded.
- d. update with price `120000.00`: response `version` 1, `ProductUpdated` delta 1, published price `120000` (`grep -o 'price[^,]*'`
  printed `price\": 120000`, the match ends at the comma, so no `.00`).
- f. `confirmed_flush_lsn` passed `L` within about 2.5 minutes (first poll `f` at 18:58:40, `t` at 19:01:11); `max(beat_at)`
  fresh; of the heartbeat topics only `__debezium-heartbeat.store` and `__debezium-heartbeat.inventory` exist, no
  `store.store.debezium_heartbeat`; connector and task `RUNNING`.
- **Not run:** e (Excel import), g (xid check), a failing heartbeat query, restarts. Whether the old task's `filtered`
  mode would re-sync the table list was not observed (the PUT followed the SQL at once).

## Changing an outbox connector (adding a placed column)

The `EventRouter` reads every `table.fields.additional.placement` column from each change event. Change events written
before the column existed do not have it, and the task is **expected** to fail on them (all events stop until rollback;
nothing is lost, the slot keeps the WAL). So apply the new config only once the connector has read every change event
written before the migration. The outbox tables are always empty, so the check is by **WAL position** (example for
stores-service; for inventory use DB `inventory` and `debezium_inventory_slot`). Run from `docker/`.

1. Deploy the service with the migration (e.g. stores-service `V9`) and keep the current connector running.
2. Right after the migration, before any new event: `docker exec -it postgres psql -U postgres -d store -c "SELECT pg_current_wal_lsn();"`.
   A position taken later is past every event and is only reached by events written after it. (To check when a migration
   ran: `SELECT version, installed_on FROM store.flyway_schema_history WHERE version = '9';` — `installed_on` is in the
   database session's local time.)
3. Wait until `SELECT confirmed_flush_lsn >= '<lsn>' FROM pg_replication_slots WHERE slot_name = 'debezium_store_v2_slot';`
   returns `t`. The heartbeat moves the slot every minute (plus the offset flush, 60 s by default) without any product
   write. `confirmed_flush_lsn` only moves to the last event the connector has processed, never to
   `pg_current_wal_lsn()`, so never compare with the current position.
4. `./kafka-connect/register-connectors.sh postgres-cdc`
5. **Verify** the connector.

First application (2026-11-04, local stack, Debezium 2.2.1, before the delete-after-insert outbox): applied after V9 by
checking the newest outbox row; the task stayed `RUNNING`, the header reached `ProductCreated` and inventory-service logged
the same `requestId`. Whether the task really fails on a pre-migration event was not tested.

**Rollback:** restore the previous `connectors/<name>.json` (git), run the script again, and restart the task if it stays
`FAILED` (`curl -X POST localhost:9084/connectors/<name>/tasks/0/restart`). The old config ignores the column.

## `inventory-outbox-cdc` (inventory-service outbox, ADR 0006)

inventory-service writes each stock reply to `inventory.outbox_events` with `id` = the payload `eventId` and **deletes
the row in the same transaction** (oms-workspace ADR 0006): the table is always empty and the INSERT reaches
Kafka only through the WAL. Same EventRouter settings as `postgres-cdc` (topic = `event_type`, key = `aggregate_id`
= orderId, header `id` = `eventId`, header `correlationContext`). The settings below (insert-only publication,
heartbeat, privileges) are shared by both connectors; specific to inventory are the pre-created topics:

- **Insert-only publication**, created by `kafka-connect/sql/inventory-outbox-cdc.sql` (`publication.autocreate.mode:
  disabled`): pgoutput never sends the DELETEs, so no delete event reaches the EventRouter and no tombstone can be
  produced. The script refuses to run before inventory-service's V5: inventory-service uses Flyway's
  `baseline-on-migrate`, and a table created in a schema Flyway has not migrated yet would make it skip V1.
- **Heartbeat** (why and how: *WAL, replication slots and the heartbeat* above): every 60 s into
  `inventory.debezium_heartbeat` (pruned to one hour), dropped by the `Filter` (predicate `isHeartbeatTable`) before
  the EventRouter (predicate `isOutbox`). Debezium's own records go to `__debezium-heartbeat.inventory` (created by
  `topic.creation.default.*`, so 6 partitions and 14 days; harmless). `debezium_heartbeat` is a table of the
  connector, not of the domain: no merchant data, not managed by Flyway.
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
   psql_inv -c "SELECT slot_name, active, wal_status, confirmed_flush_lsn, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS behind, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS wal_kept FROM pg_replication_slots;"
   ```

The two verify records stay in `StockReservationExpired` for 14 days with unknown orderIds: order-service's consumer
(earliest offset on its first start) must tolerate them, as it must tolerate any reply for an unknown order.

### Runbook (inventory-outbox-cdc)

- **WAL kept / slot lag**: query (g) (`wal_kept` is what the slot holds; `behind` is what the connector has not confirmed). A stopped or failed connector keeps the WAL of the **whole server** (every database) until it runs again (*WAL, replication slots and the heartbeat*);
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
  A repair is done in place with `ALTER PUBLICATION` (pgoutput picks it up at the ALTER's commit; changes before it keep
  the old settings), as `sql/store-outbox-cdc.sql` does for the store publication.
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
- f. `confirmed_flush_lsn` passed `L` within about 2.5 minutes (first poll `f` at 21:04:30, `t` at 21:07:01; heartbeat
  60 s + offset flush 60 s); `max(beat_at)` fresh, 4 rows; of the heartbeat topics only `__debezium-heartbeat.inventory`
  exists, no `inventory.inventory.debezium_heartbeat`.
- g. `debezium_inventory_slot` active, `reserved`, 1832 bytes behind; `debezium_store_v2_slot` (no heartbeat yet) active,
  `reserved`, 582 kB behind.

The condition of ADR 0006 (one record per reply, no tombstone, task RUNNING) holds locally. **Not tested:** a
failing heartbeat query (runbook), and the behaviour after a Kafka or Connect restart.

