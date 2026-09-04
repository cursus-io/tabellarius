# Production CDC reliability

This document defines the runtime contract for operating Tabellarius against a
MySQL InnoDBCluster through MySQL Router. It intentionally contains no
credentials, Secret values, or complete DSNs.

## Ownership

- Tabellarius owns binlog consumption, transaction assembly, durable
  checkpoints, health reporting, and its container image.
- The workload GitOps repository owns Deployment settings, NetworkPolicy,
  Secret references, persistent storage, and immutable image selection.
- Downstream applications own their consumers and the interpretation of the
  published event schema.
- Cursus owns topic storage and consumer-group coordination; it does not own
  source database checkpoints.

## Delivery contract

Tabellarius publishes one message for each committed transaction containing at
least one allowed row change. A checkpoint may advance only after Cursus has
acknowledged that message. Transactions without allowed changes may advance the
checkpoint without publishing.

Delivery is at least once across the narrow interval between broker
acknowledgement and checkpoint replacement. The transaction GTID remains stable
across such a replay, so downstream stores must use it in their unique event key
and handle an already-recorded event as success.

The publisher requires an explicitly pre-created topic, all-replica
acknowledgement, and idempotent production by default. Unsafe publisher settings
are rejected before connecting. `allow_single_replica_publisher` exists only for
isolated single-node development tests and must never be set in production.

MySQL GTID is the canonical transaction identity and resume cursor when GTID is
available. The checkpoint also retains the source binlog file and position for
diagnostics. Legacy file/position checkpoints remain readable only when
`require_gtid` is disabled; they are never guessed or automatically converted.
When `require_gtid` is enabled, Tabellarius verifies the live MySQL GTID mode and
rejects both legacy checkpoints and anonymous GTID events.

Checkpoint writes are atomic. A missing checkpoint is accepted only when the
operator explicitly allows first-start bootstrapping. A present but malformed
checkpoint is always fatal. Tabellarius never silently skips a malformed
checkpoint or advances after a failed publication.

## Failure contract

A terminal binlog-stream error, publisher error, or checkpoint-write error is a
process-fatal condition. The process cancels its workers, closes the publisher
and database connection, reports not-ready, and exits non-zero. Kubernetes then
performs the retry from the last durable checkpoint. A terminal stream is never
polled in a tight retry loop.

SIGINT and SIGTERM are graceful shutdowns. They stop intake, close dependencies,
and return success without advancing an unacknowledged checkpoint.

## Health and metrics

The server exposes an unauthenticated, data-free operational endpoint on the
configured health address:

- `/livez` reports whether the process is alive.
- `/readyz` succeeds only after the binlog stream has started and while no
  terminal failure is recorded.
- `/metrics` exposes stream readiness, last checkpoint time, source-event lag,
  processed-event count, publish failures, checkpoint failures, and stream
  failures. It never exports database credentials, DSNs, row data, or SQL.

The Deployment must configure startup, readiness, and liveness probes and allow
only the cluster monitoring path to the metrics port.

## Security contract

Production uses certificate verification for both SQL metadata connections and
the replication stream. The CA file and optional server name are supplied by
mounted configuration; no certificate or credential is committed to source.

Capture remains table and column allow-list based. Every configured table must
have explicit `include_columns`; newly added columns are therefore excluded by
default. A configured primary key that is absent from source metadata or a row
image is fatal rather than falling back to another column. Row values and full
DDL statements are not written to logs. DDL publication is off unless
explicitly enabled.

The published image runs as a non-root user by default, uses a supported runtime
base, and is produced only from `main` or a version tag. Unit tests run before an
image is pushed. The workflow reports the immutable digest and publishes build
provenance and an SBOM.

## Legacy checkpoint migration

Do not delete or recreate an existing checkpoint volume. Before the first GTID
release rollout:

1. Stop the singleton producer and preserve a copy of the current checkpoint on
   the same persistent volume.
2. Validate that the stored file/position exists in retained binlogs.
3. Derive the executed GTID set ending at that exact committed boundary.
4. Dry-run the converted checkpoint and verify that its file/position and GTID
   identify the same transaction boundary.
5. Atomically install the converted checkpoint and start exactly one producer.

If the exact boundary cannot be derived, stop the rollout. Do not substitute the
current global executed GTID set because doing so can skip unprocessed changes.

## Release gate

An operational release requires all of the following:

- unit, race, lint, build, and restart/recovery tests pass;
- the image was built from the merged `main` commit and selected by digest;
- the MySQL cluster and Cursus cluster are healthy;
- the checkpoint volume is bound and preserved;
- the Deployment becomes ready without crash or image-pull errors;
- checkpoint age and consumer lag converge after a controlled mutation;
- the downstream store contains one event per changed row with no secret
  columns and no duplicate event keys.
