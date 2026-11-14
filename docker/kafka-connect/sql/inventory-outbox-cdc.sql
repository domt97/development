-- Setup for the connector inventory-outbox-cdc (docs/connectors.md). Run once per database, after inventory-service
-- has started at least once (Flyway V5), from docker/:
--   docker exec -i postgres psql -1 -v ON_ERROR_STOP=1 -U postgres -d inventory -f - < kafka-connect/sql/inventory-outbox-cdc.sql
-- -1 makes the whole file one transaction (it applies only with -f/-c, hence "-f -" for stdin): an error leaves nothing.
-- Safe to run again. Keep BEGIN/COMMIT out of this file: under -1 they would break the single transaction.

-- 1. Guard. inventory-service runs Flyway with baseline-on-migrate=true: a table created here in a schema Flyway has
--    not migrated yet would make Flyway baseline at version 1 and skip V1. So refuse until V5 is applied.
DO $$
BEGIN
    IF to_regclass('inventory.flyway_schema_history') IS NULL OR to_regclass('inventory.outbox_events') IS NULL THEN
        RAISE EXCEPTION 'inventory-service has not migrated DB inventory yet (V5 outbox_events missing): start it once, then run this file again';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM inventory.flyway_schema_history
                   WHERE success AND version IS NOT NULL
                     AND string_to_array(version, '.')::int[] >= ARRAY[5]) THEN
        RAISE EXCEPTION 'inventory-service Flyway is below V5: start the current inventory-service once, then run this file again';
    END IF;
END $$;

-- 2. Heartbeat table of the connector (not part of the domain, not managed by Flyway, no merchant data).
--    heartbeat.action.query inserts a row every minute and prunes rows older than one hour in the same statement.
--    The INSERT is a published change, so the replication slot advances even when no reply is written.
CREATE TABLE IF NOT EXISTS inventory.debezium_heartbeat
(
    id      BIGSERIAL PRIMARY KEY,
    beat_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 3. Insert-only publication: pgoutput never sends the outbox DELETEs (rows are deleted in the same transaction,
--    ADR 0006), the heartbeat prune, UPDATEs or TRUNCATEs. Debezium is configured with
--    publication.autocreate.mode=disabled and uses exactly this publication.
DO $$
DECLARE
    pub   pg_publication%ROWTYPE;
    tabs  text[];
    bad   int;
BEGIN
    SELECT * INTO pub FROM pg_publication WHERE pubname = 'debezium_inventory_publication';
    IF NOT FOUND THEN
        CREATE PUBLICATION debezium_inventory_publication
            FOR TABLE inventory.outbox_events, inventory.debezium_heartbeat
            WITH (publish = 'insert');
        RETURN;
    END IF;

    IF NOT (pub.pubinsert AND NOT pub.pubupdate AND NOT pub.pubdelete AND NOT pub.pubtruncate) OR pub.puballtables THEN
        RAISE EXCEPTION 'debezium_inventory_publication exists but is not insert-only on listed tables; repair it by hand with ALTER PUBLICATION, never drop or recreate it while the slot exists (docs/connectors.md, runbook)';
    END IF;
    SELECT array_agg(schemaname || '.' || tablename ORDER BY schemaname, tablename) INTO tabs
    FROM pg_publication_tables WHERE pubname = 'debezium_inventory_publication';
    IF tabs IS DISTINCT FROM ARRAY['inventory.debezium_heartbeat', 'inventory.outbox_events'] THEN
        RAISE EXCEPTION 'debezium_inventory_publication has tables %, expected inventory.debezium_heartbeat and inventory.outbox_events', tabs;
    END IF;
    SELECT count(*) INTO bad
    FROM pg_publication_rel r JOIN pg_publication p ON p.oid = r.prpubid
    WHERE p.pubname = 'debezium_inventory_publication' AND (r.prattrs IS NOT NULL OR r.prqual IS NOT NULL);
    IF bad > 0 THEN
        RAISE EXCEPTION 'debezium_inventory_publication has a column list or row filter';
    END IF;
END $$;
