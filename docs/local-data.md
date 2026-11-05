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

- **Kafka with ZooKeeper** (always together: they share a cluster id, one without the other fails with
  `InconsistentClusterIdException`):
  `docker compose rm -sf kafka zookeeper kafka-connect schemaregistry && docker volume rm docker_kafka_data docker_zookeeper_data docker_zookeeper_log`,
  then `docker compose up -d` and register the connector again (`docs/connectors.md`, *First setup, and after a Kafka
  reset*). Postgres keeps the slot, so nothing is published twice and nothing written meanwhile is lost.
- **Everything:** `docker compose down -v`, then `docker compose up -d`, Terraform in `infra/local`, and the connector's
  first setup.

## Switching to these volumes (once)

ZooKeeper and Kafka used to keep their data in anonymous volumes; the first `docker compose up -d` with this file starts
them empty, on purpose (old topics are not kept). Postgres keeps its data. Afterwards register the connector again:

```bash
cd docker
docker compose up -d
./kafka-connect/register-connectors.sh                    # Kafka is new: the connector must be registered again
curl -s localhost:9084/connectors/postgres-cdc/status     # connector and task RUNNING
```

The old anonymous volumes can be removed with `docker volume prune`.

## LocalStack

The community image keeps nothing: after LocalStack restarts, run `terraform apply` in `docker/infra/local` to recreate
the bucket, queue and schedule (objects uploaded to S3 are gone).
