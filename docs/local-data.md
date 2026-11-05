# Local data: what survives a restart

| Service | Data | Where it is kept |
|---------|------|------------------|
| postgres | databases `store`, `inventory`, Debezium's replication slot | named volume `postgres_data` |
| zookeeper | Kafka cluster id, topic and broker metadata | named volumes `zookeeper_data`, `zookeeper_log` |
| kafka | topics, consumer offsets, Kafka Connect's connector configs, offsets and status (`_connect_*`), Schema Registry's `_schemas` | named volume `kafka_data` |
| redis | cache (append-only file) | named volume `redis_data` |
| kafka-connect, schemaregistry, kafka-ui | nothing of their own (state is in Kafka) | – |
| localstack | S3 objects, SQS messages, EventBridge rules | **not kept** (see below) |

Named volumes survive `docker compose stop/start/restart/down` and recreated containers. Only `docker compose down -v`
or `docker volume rm` deletes them (Compose prefixes them with the project name, e.g. `docker_kafka_data`).

## Resetting

Stop everything that talks to Kafka **first** (`docker compose down`, and stores-service / inventory-service). A client
that keeps running while Kafka is replaced still uses the old cluster's metadata and writes to the new one: Kafka then
auto-creates Kafka Connect's and Schema Registry's internal topics with defaults (1 partition, `cleanup.policy=delete`),
and Kafka Connect refuses to start (see *Kafka Connect exits after a Kafka reset* below).

- **Kafka with ZooKeeper** (always together: they share a cluster id, one without the other fails with
  `InconsistentClusterIdException`):
  ```bash
  cd docker
  docker compose down --remove-orphans          # keeps every volume (only down -v deletes them)
  docker volume rm docker_kafka_data docker_zookeeper_data docker_zookeeper_log   # docker volume ls for the names
  docker compose up -d
  docker logs --since 3m kafka-connect 2>&1 | grep -E "Kafka Connect started|ERROR"   # wait for "started", no ERROR
  ./kafka-connect/register-connectors.sh        # the new Kafka has no connector
  ```
  Then start stores-service and inventory-service. Postgres keeps the slot, so nothing is published twice and nothing
  written meanwhile is lost (`docs/connectors.md`, *First setup, and after a Kafka reset*).
- **Everything:** stop the services, `docker compose down -v`, then `docker compose up -d`, Terraform in `infra/local`,
  and the connector's first setup.

## Switching to these volumes (once)

ZooKeeper and Kafka used to keep their data in anonymous volumes, so the first start with this file gives them empty
volumes, on purpose (old topics are not kept). Postgres keeps its data. Use `down` first, not a plain `up -d`: `up -d`
recreates only `kafka` and `zookeeper`, and the still-running `kafka-connect` breaks its topics (below).

```bash
cd docker
docker compose down --remove-orphans          # stop stores-service and inventory-service too
docker compose up -d
docker logs --since 3m kafka-connect 2>&1 | grep -E "Kafka Connect started|ERROR"
./kafka-connect/register-connectors.sh                    # Kafka is new: the connector must be registered again
curl -s localhost:9084/connectors/postgres-cdc/status     # connector and task RUNNING
```

Then start the services. The old anonymous volumes can be removed with `docker volume prune`.

## Kafka Connect exits after a Kafka reset

Symptoms: outbox rows are written but nothing reaches Kafka; the slot shows `active = f`; the connector is
`UNASSIGNED`, or `curl localhost:9084/...` hangs or answers nothing; `docker ps -a` shows `kafka-connect` as
`Exited (1)` with `Topic '_connect_offsets' ... is required to have 'cleanup.policy=compact' ... found 'cleanup.policy=delete'`.
Cause: a client kept running while Kafka was replaced (above). Nothing is lost: the rows stay in the outbox and the
slot keeps the WAL. Fix, from `docker/`:

```bash
alias kt='docker exec -e JMX_PORT= -e KAFKA_JMX_OPTS= kafka kafka-topics --bootstrap-server kafka:29092'
docker compose stop kafka-connect schemaregistry
kt --describe --topic '_connect_offsets|_connect_status|_connect_configs|_schemas' | grep PartitionCount
#  wrong when Configs has no cleanup.policy=compact (or _connect_offsets has 1 partition instead of 25)
kt --delete --topic '_connect_offsets|_connect_status|_connect_configs|_schemas'
docker compose up -d kafka-connect schemaregistry
docker logs --since 3m kafka-connect 2>&1 | grep -E "Kafka Connect started|ERROR"
kt --describe --topic _connect_offsets | head -1           # PartitionCount: 25, cleanup.policy=compact
./kafka-connect/register-connectors.sh
```

Then restart stores-service and inventory-service (their Kafka clients also hold the old metadata). A restarted container
keeps its old log: always read it with `--since`, or an old "Kafka Connect started" line looks like success.

## LocalStack

The community image keeps nothing: after LocalStack restarts, run `terraform apply` in `docker/infra/local` to recreate
the bucket, queue and schedule (objects uploaded to S3 are gone).
