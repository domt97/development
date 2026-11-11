-- Setup for the connector postgres-cdc (stores-service outbox, docs/connectors.md). Run once per database, after
-- stores-service has started at least once (Flyway V9), and BEFORE stores-service V10 runs on a stack with a connector,
-- from docker/:
--   docker exec -i postgres psql -1 -v ON_ERROR_STOP=1 -U postgres -d store -f - < kafka-connect/sql/store-outbox-cdc.sql
-- -1 makes the whole file one transaction (it applies only with -f/-c, hence "-f -" for stdin): an error leaves nothing.
-- Safe to run again. Keep BEGIN/COMMIT out of this file: under -1 they would break the single transaction.

-- 1. Guard. stores-service runs Flyway with baseline-on-migrate=true: a table created here in a schema Flyway has not
--    migrated yet would make Flyway baseline at version 1 and skip V1. So refuse until V9 (correlation_context) is applied.
DO $$
BEGIN
    IF to_regclass('store.flyway_schema_history') IS NULL OR to_regclass('store.outbox_events') IS NULL THEN
        RAISE EXCEPTION 'stores-service has not migrated DB store yet (outbox_events missing): start it once, then run this file again';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM store.flyway_schema_history
                   WHERE success AND version IS NOT NULL
                     AND string_to_array(version, '.')::int[] >= ARRAY[9]) THEN
        RAISE EXCEPTION 'stores-service Flyway is below V9: start the current stores-service once, then run this file again';
    END IF;
END $$;

-- 2. Heartbeat table of the connector (not part of the domain, not managed by Flyway, no merchant data).
--    heartbeat.action.query inserts a row every minute and prunes rows older than one hour in the same statement.
--    The INSERT is a published change, so the replication slot advances even when no product event is written.
CREATE TABLE IF NOT EXISTS store.debezium_heartbeat
(
    id      BIGSERIAL PRIMARY KEY,
    beat_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 3. Insert-only publication: pgoutput never sends the outbox DELETEs (stores-service deletes each row in the same
--    transaction), the heartbeat prune, UPDATEs or TRUNCATEs. Debezium is configured with
--    publication.autocreate.mode=disabled and uses exactly this publication.
--    The name is the one Debezium's former "filtered" autocreate gave it: the existing slot decodes with it. Such a
--    publication is ALTERed in place, never dropped: pgoutput reads the publication through the catalog as of the change
--    being decoded and picks up an ALTER at its commit, so older changes keep the old settings; a dropped publication
--    makes decoding of older positions fail (docs/connectors.md, runbook).
DO $$
DECLARE
    pub      pg_publication%ROWTYPE;
    tabs     text[];
    filtered int;
BEGIN
    SELECT * INTO pub FROM pg_publication WHERE pubname = 'debezium_store_v2_publication';
    IF NOT FOUND THEN
        CREATE PUBLICATION debezium_store_v2_publication
            FOR TABLE store.outbox_events, store.debezium_heartbeat
            WITH (publish = 'insert');
        RETURN;
    END IF;

    IF pub.puballtables THEN
        RAISE EXCEPTION 'debezium_store_v2_publication is FOR ALL TABLES; repair it by hand (docs/connectors.md)';
    END IF;
    SELECT count(*) INTO filtered
    FROM pg_publication_rel r WHERE r.prpubid = pub.oid AND (r.prattrs IS NOT NULL OR r.prqual IS NOT NULL);
    IF filtered > 0 THEN
        RAISE EXCEPTION 'debezium_store_v2_publication has a column list or row filter; repair it by hand (docs/connectors.md)';
    END IF;
    SELECT array_agg(schemaname || '.' || tablename ORDER BY schemaname, tablename) INTO tabs
    FROM pg_publication_tables WHERE pubname = 'debezium_store_v2_publication';

    -- Former filtered autocreate: all operations, only the outbox.
    IF tabs IS NOT DISTINCT FROM ARRAY['store.outbox_events']
       AND pub.pubinsert AND pub.pubupdate AND pub.pubdelete AND pub.pubtruncate THEN
        ALTER PUBLICATION debezium_store_v2_publication SET (publish = 'insert');
        ALTER PUBLICATION debezium_store_v2_publication ADD TABLE store.debezium_heartbeat;
        RETURN;
    END IF;

    IF NOT (pub.pubinsert AND NOT pub.pubupdate AND NOT pub.pubdelete AND NOT pub.pubtruncate) THEN
        RAISE EXCEPTION 'debezium_store_v2_publication has operations %/%/%/% on tables %; expected insert-only, repair it by hand',
            pub.pubinsert, pub.pubupdate, pub.pubdelete, pub.pubtruncate, tabs;
    END IF;
    -- Insert-only but without the heartbeat table (e.g. a Debezium re-sync of the table list): add it back.
    IF tabs IS NOT DISTINCT FROM ARRAY['store.outbox_events'] THEN
        ALTER PUBLICATION debezium_store_v2_publication ADD TABLE store.debezium_heartbeat;
        RETURN;
    END IF;
    IF tabs IS DISTINCT FROM ARRAY['store.debezium_heartbeat', 'store.outbox_events'] THEN
        RAISE EXCEPTION 'debezium_store_v2_publication has tables %, expected store.debezium_heartbeat and store.outbox_events', tabs;
    END IF;
END $$;
