-- Setup for the connector order-outbox-cdc (docs/connectors.md). Run once per database, after order-service has
-- started at least once (Flyway V1), from docker/:
--   docker exec -i postgres psql -1 -v ON_ERROR_STOP=1 -U postgres -d orders -f - < kafka-connect/sql/order-outbox-cdc.sql
-- -1 makes the whole file one transaction (it applies only with -f/-c, hence "-f -" for stdin): an error leaves nothing.
-- Safe to run again. Keep BEGIN/COMMIT out of this file: under -1 they would break the single transaction.

-- 1. Guard. order-service runs Flyway WITHOUT baseline-on-migrate, but a table created here before its first migration
--    would still put a non-Flyway table into schema orders first. Its history table exists from the first start, before
--    any migration (Flyway creates the schema): so require V1 itself, not only the history table.
DO $$
BEGIN
    IF to_regclass('orders.flyway_schema_history') IS NULL OR to_regclass('orders.outbox_events') IS NULL THEN
        RAISE EXCEPTION 'order-service has not migrated DB orders yet (V1 outbox_events missing): start it once, then run this file again';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM orders.flyway_schema_history WHERE success AND version = '1') THEN
        RAISE EXCEPTION 'order-service Flyway V1 is not applied: start the current order-service once, then run this file again';
    END IF;
END $$;

-- 2. Heartbeat table of the connector (not part of the domain, not managed by Flyway, no merchant data).
--    heartbeat.action.query inserts a row every minute and prunes rows older than one hour in the same statement.
--    The INSERT is a published change, so the replication slot advances even when no checkout reply is written.
CREATE TABLE IF NOT EXISTS orders.debezium_heartbeat
(
    id      BIGSERIAL PRIMARY KEY,
    beat_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 3. Insert-only publication: pgoutput never sends the outbox DELETEs (rows are deleted in the same transaction,
--    oms-workspace ADR 0007), the heartbeat prune, UPDATEs or TRUNCATEs. Debezium is configured with
--    publication.autocreate.mode=disabled and uses exactly this publication.
DO $$
DECLARE
    pub   pg_publication%ROWTYPE;
    tabs  text[];
    bad   int;
BEGIN
    SELECT * INTO pub FROM pg_publication WHERE pubname = 'debezium_orders_publication';
    IF NOT FOUND THEN
        CREATE PUBLICATION debezium_orders_publication
            FOR TABLE orders.outbox_events, orders.debezium_heartbeat
            WITH (publish = 'insert');
        RETURN;
    END IF;

    IF NOT (pub.pubinsert AND NOT pub.pubupdate AND NOT pub.pubdelete AND NOT pub.pubtruncate) OR pub.puballtables THEN
        RAISE EXCEPTION 'debezium_orders_publication exists but is not insert-only on listed tables; repair it by hand with ALTER PUBLICATION, never drop or recreate it while the slot exists (docs/connectors.md, runbook)';
    END IF;
    SELECT array_agg(schemaname || '.' || tablename ORDER BY schemaname, tablename) INTO tabs
    FROM pg_publication_tables WHERE pubname = 'debezium_orders_publication';
    IF tabs IS DISTINCT FROM ARRAY['orders.debezium_heartbeat', 'orders.outbox_events'] THEN
        RAISE EXCEPTION 'debezium_orders_publication has tables %, expected orders.debezium_heartbeat and orders.outbox_events', tabs;
    END IF;
    SELECT count(*) INTO bad
    FROM pg_publication_rel r JOIN pg_publication p ON p.oid = r.prpubid
    WHERE p.pubname = 'debezium_orders_publication' AND (r.prattrs IS NOT NULL OR r.prqual IS NOT NULL);
    IF bad > 0 THEN
        RAISE EXCEPTION 'debezium_orders_publication has a column list or row filter';
    END IF;
END $$;
