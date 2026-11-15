# development — local infrastructure of the OMS

Part of the OMS. System-wide map and rules: **`oms-workspace`**
(https://github.com/domt97/oms-workspace — `CLAUDE.md`, `architecture/`). This repo has no application code: it is the
local stack the services run against, and the only place where Kafka topics, Debezium connectors and LocalStack
resources are defined today.

## Owns

| Path | Contains |
|------|----------|
| `docker/docker-compose.yml` | Kafka + Zookeeper, Kafka UI (8080), Schema Registry (9085), Kafka Connect with Debezium (9084), Postgres 16 (5432, DBs `store`, `inventory`, `cart`, `orders`), Redis (6379), LocalStack |
| `docker/postgres/init/01-databases.sh` | Creates the service databases (`store`, `inventory`, `cart`, `orders`) on a fresh volume; idempotent, run it by hand on an existing one |
| `docker/kafka/create-topics.sh` | Message topics with their partition count: stock commands and replies (ADR 0006), checkout (ADR 0007), `ProductCreated` / `ProductUpdated`; not the DLTs (services) or Debezium heartbeat topics |
| `docker/kafka-connect/connectors/*.json` | Connector configs: `postgres-cdc` (stores-service outbox), `inventory-outbox-cdc` |
| `docker/kafka-connect/sql/*.sql` | Publications and heartbeat tables per database |
| `docker/kafka-connect/register-connectors.sh` | Applies every connector config and waits until it is RUNNING |
| `docker/infra/local/*.tf` | Terraform for LocalStack: S3 bucket (with browser-upload CORS and a lifecycle rule for uploaded import files), SQS queue, EventBridge rule and target |
| `docs/connectors.md` | Connector setup, WAL and replication slots, verify steps, runbooks, how to change a connector |
| `docs/local-data.md` | What survives a restart, how to reset, recovering Kafka Connect |

## Changing things — checklist

Read the matching section of `docs/connectors.md` or `docs/local-data.md` first; they hold the order of steps.

1. **New topic**: add it to `docker/kafka/create-topics.sh`. Topic name = message type (workspace system rule 8).
   Never add partitions to an existing topic: keys move and their order breaks.
2. **Connector change** (a new outbox column placed as header or field, a new table): follow *Changing an outbox
   connector* in `docs/connectors.md` — the service migration goes first, the connector second. Connectors are never
   deleted (that drops their offsets). A RUNNING status only means the config was accepted: finish with the **Verify**
   step of that connector.
3. **New AWS resource**: Terraform in `docker/infra/local`, with its dead-letter queue when it is a queue (the existing
   `reservation_expire_queue` has none yet). Then the service config that uses it. LocalStack keeps nothing: after it restarts, `terraform apply` again.
4. **Compose change** (image version, port, volume): check `docs/local-data.md`; a renamed or removed volume loses
   Kafka offsets, connector state or the Postgres replication slots.
5. **Docs**: a changed step, port or resource name is updated in `docs/` here in the same PR, and in
   `oms-workspace/architecture/` (`system-overview.md`, `integration.md`) when another repo depends on it.

Cross-repo order for these changes: workspace skills `change-event` and `add-aws-resource`.

## Verify

There is no CI in this repo. A change is checked by running it: `cd docker && docker compose up -d`, then the steps of
*First setup, and after a Kafka reset* in `docs/connectors.md`, then the **Verify** step of each connector touched.
Say plainly which of these were run and which were not.

## Git workflow

`master` only: branch from `master` (`feat/...`, `fix/...`, `docs/...`) and open PRs into `master`.

## Pitfalls

- Secrets: only `docker/kafka-connect/secrets/*.example` is committed. Never commit a filled-in secrets file.
- One Postgres server holds every service database (`store`, `inventory`, `cart`, `orders`), so they share one WAL: a stopped or idle connector keeps WAL from being
  removed and can fill the disk (`docs/connectors.md`, *WAL, replication slots and the heartbeat*).
- `store.debezium_heartbeat` and `inventory.debezium_heartbeat` are created by the SQL scripts here, not by the
  services' Flyway migrations.
- Topic sizes here (6 partitions, 14 days) are local values, not production sizing.
- `localstack/localstack.sh` is not part of any setup step; Terraform creates the LocalStack resources.
- The aws provider is pinned `< 5.86.0` (`main.tf`): newer versions never finish creating the bucket lifecycle rule against
  LocalStack 3.8.1. Lift the pin only together with a LocalStack upgrade, after `terraform apply` twice gives an empty second plan.
- `S3_SKIP_SIGNATURE_VALIDATION=0` (compose) makes LocalStack check presigned URLs; ordinary signed requests are still not
  checked. A real environment needs its own bucket CORS origins, the lifecycle rule and the signer's IAM
  (`docs/local-data.md`, *LocalStack*).
