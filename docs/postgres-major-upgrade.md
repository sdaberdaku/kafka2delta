# PostgreSQL Major Upgrade with Debezium CDC

How to take a Postgres source database through a major version upgrade (verified on **16 → 17**) without
re-snapshotting the Debezium connector.

Verified end to end against PostgreSQL 16.15 → 17.11, Debezium 3.0.8.Final, Strimzi 0.42.0 / Kafka 3.7.1.

## The problem

`pg_upgrade` does **not** migrate logical replication slots when the *old* cluster is older than PostgreSQL 17.
Slot migration was added in PG17, and it only applies to upgrades *from* 17 onward. So a 16 → 17 upgrade always
destroys the slot, and the connector's stored LSN becomes unobtainable.

That matters because of how `snapshot.mode` reacts:

| mode | on restart after the upgrade | result |
|---|---|---|
| `when_needed` | stored offset LSN no longer obtainable → snapshots | **every row re-emitted** |
| `no_data` | never snapshots, streams from the new slot | resumes cleanly |

Measured on a 1,802-row test database: restarting with `when_needed` after the slot was dropped re-emitted
503 / 998 / 301 records — a complete duplicate of every table. With `no_data`, topic offsets did not move at all.

## Preconditions

- **Writers must be off** for the whole window. The new slot starts at the current WAL position, so any write
  between dropping the old slot and creating the new one is never captured. There is no way to recover it short
  of a re-snapshot.
- The connector's Kafka Connect offsets must survive. Do not delete the `*_connect_cluster_offsets` topic or
  rename the connector — `no_data` relies on the stored offset to know a snapshot already completed.
- Take a backup. `pg_upgrade` without `--link` leaves the old data directory intact, which is your rollback,
  but it is not a backup.

## Procedure

### 1. Stop writers

Application-level. Everything below assumes no new transactions.

### 2. Drain the slot

Confirm Debezium has consumed everything before you tear anything down.

```sql
SELECT confirmed_flush_lsn,
       pg_current_wal_lsn(),
       pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag_bytes
FROM pg_replication_slots WHERE slot_name = 'debezium_connector';
```

`lag_bytes` will **not** reach zero on an idle database — checkpoint, autovacuum and standby-snapshot WAL records
produce no logical-decoding output, so there is nothing for the slot to acknowledge. A residual of a few KB is
normal and is not unreplicated data.

The meaningful check is reconciliation: table row counts against topic message counts, accounting for the fact
that updates add a message without adding a row, and deletes add a message while removing one.

```sql
SELECT pg_logical_emit_message(true, 'debezium', 'drain-probe');
```

Emitting a logical message and watching `confirmed_flush_lsn` advance proves the pipeline is still live and
acknowledging, rather than silently stalled.

Record `pg_current_wal_lsn()` here — you will compare against it after the upgrade.

### 3. Stop Debezium

```shell
kubectl -n cdc delete kafkaconnector debezium-connector
```

Deleting the `KafkaConnector` resource does not delete its offsets, which is what you want.

### 4. Drop the publication and slot

```sql
DROP PUBLICATION IF EXISTS debezium_connector;
SELECT pg_drop_replication_slot('debezium_connector');
```

`pg_upgrade` refuses to run while a logical slot exists, so this is mandatory, not optional cleanup.

### 5. Stop Postgres

```shell
kubectl -n postgres scale sts postgres --replicas=0
kubectl -n postgres wait --for=delete pod/postgres-0 --timeout=180s
```

The cluster must have shut down cleanly or `pg_upgrade` will refuse to proceed.

### 6. Run pg_upgrade

Use an image carrying **both** major versions' binaries — `tianon/postgres-upgrade:16-to-17` — mounted on the
same PVC. Run as root so its entrypoint can `chown` the data directories, and let it `initdb` the new cluster.

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: pg-upgrade-16-to-17
  namespace: postgres
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      securityContext:
        runAsUser: 0
      containers:
        - name: upgrade
          image: tianon/postgres-upgrade:16-to-17
          workingDir: /var/lib/postgresql/data
          command:
            - bash
            - -c
            - |
              set -e
              docker-upgrade pg_upgrade
              mv "$PGDATAOLD" "${PGDATAOLD}.old16"
              mv "$PGDATANEW" "$PGDATAOLD"
          env:
            - name: PGDATAOLD
              value: /var/lib/postgresql/data/pgdata
            - name: PGDATANEW
              value: /var/lib/postgresql/data/pgdata17
          volumeMounts:
            - name: data
              mountPath: /var/lib/postgresql/data
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: data-postgres-0
```

Swapping the directory names afterwards keeps `PGDATA` stable across upgrades. Do **not** use `pg_upgrade --link`
if you want a rollback path — link mode makes the old cluster unusable.

### 7. Restore the cluster configuration files

**This is the step that is easiest to miss.** `pg_upgrade` migrates data, not configuration. The new cluster's
`pg_hba.conf` and `postgresql.conf` come from the fresh `initdb` inside the upgrade image, so any customisation
is gone.

In our rehearsal the new `pg_hba.conf` was missing the `host all all all scram-sha-256` line that the official
`postgres` image appends at initdb time. Postgres started fine, local `psql` worked, and Debezium failed with:

```
FATAL: no pg_hba.conf entry for host "10.244.0.12", user "postgres", database "postgres", no encryption
```

```shell
kubectl -n postgres exec postgres-0 -- bash -c \
  "cp /var/lib/postgresql/data/pgdata.old16/pg_hba.conf /var/lib/postgresql/data/pgdata/pg_hba.conf && \
   chown postgres:postgres /var/lib/postgresql/data/pgdata/pg_hba.conf"
```

Diff both files from the old cluster before starting the new one. If your settings are passed as command-line
arguments (as with `-c wal_level=logical`), they survive automatically; if they live in `postgresql.conf`, they
do not.

### 8. Start the new version

```shell
kubectl -n postgres patch sts postgres --type=json \
  -p='[{"op":"replace","path":"/spec/template/spec/containers/0/image","value":"postgres:17"}]'
kubectl -n postgres scale sts postgres --replicas=1
```

Verify before letting anything connect:

```sql
SELECT version();                      -- new major version
SHOW wal_level;                        -- must still be 'logical'
SELECT relname, relreplident::text FROM pg_class
  WHERE relname IN ('users','orders','products');   -- 'f' = REPLICA IDENTITY FULL, preserved by pg_upgrade
SELECT pg_current_wal_lsn();           -- must be >= the LSN recorded in step 2
```

The LSN check is the important one. `pg_upgrade` carries the WAL position **forward**
(`0/15E0F68` → `0/8002000` in our run). A dump/restore does not — it resets the WAL near zero, which would leave
the connector's stored offset *ahead* of the new cluster and cause it to silently skip post-upgrade changes.
**Never use dump/restore as a substitute for `pg_upgrade` while preserving CDC offsets.**

### 9. Restart Debezium with `snapshot.mode: no_data`

```yaml
snapshot.mode: no_data
```

Everything else stays identical — same connector name, same `slot.name`, same `publication.name`. With
`publication.autocreate.mode: filtered` Debezium recreates both the publication and the slot itself.

Confirm from the log that it skipped rather than snapshotted:

```
A previous offset indicating a completed snapshot has been found.
According to the connector configuration no snapshot will be executed
Snapshot ended with SnapshotResult [status=SKIPPED, ...]
Retrieved latest position from stored offset 'LSN{...}'
Obtained valid replication slot ReplicationSlot [latestFlushedLsn=LSN{...}]
```

And that topic end offsets have not moved. If they jumped by roughly your row count, it snapshotted — stop and
investigate before writers resume.

### 10. Resume writers and verify

Apply a known insert / update / delete and confirm the topic offsets advance by the expected amounts.

### 11. Afterwards

```shell
# optimizer statistics are NOT transferred by pg_upgrade - the planner is flying blind until this runs
kubectl -n postgres exec postgres-0 -- vacuumdb --all --analyze-in-stages -U postgres
```

Once you are confident, reclaim the old cluster directory (`pgdata.old16`, ~40 MB in our test, proportional to
your database). Keep it through at least one business cycle as the rollback path.

You can revert `snapshot.mode` to `when_needed` for normal operation. Verified: with a healthy slot and valid
offsets, `when_needed` skips the snapshot (`status=SKIPPED`, offsets unchanged). It only snapshots when the
stored offset is unobtainable.

## Rollback

Valid **only until writers resume on the new version**. After that, the new cluster has data the old one does not.

1. Scale Postgres to 0.
2. Swap the directories back: `mv pgdata pgdata17.failed && mv pgdata.old16 pgdata`.
3. Patch the image back to the old major version, scale to 1.
4. Recreate the connector. The old cluster's slot was already dropped in step 4, so use `no_data` again if the
   stored offsets are still valid, or `when_needed` to accept a full re-snapshot.

## Gotchas

| Gotcha | Consequence | Mitigation |
|---|---|---|
| Slots are not migrated from PG < 17 | Stored LSN unobtainable | Drop slot deliberately, resume with `no_data` |
| `when_needed` after slot drop | Full re-snapshot, every record duplicated | Use `no_data` for the restart |
| `pg_hba.conf` / `postgresql.conf` not carried over | Connector cannot authenticate; local psql still works, so it looks healthy | Copy conf files from old data dir before starting |
| Dump/restore instead of `pg_upgrade` | WAL position resets; connector may skip post-upgrade changes | Always `pg_upgrade` |
| Writes during the window | Silently lost, no error anywhere | Writers off from drain until streaming confirmed |
| Optimizer stats not transferred | Bad plans, slow queries after cutover | `vacuumdb --all --analyze-in-stages` |
| `pg_upgrade --link` | No rollback possible | Use default copy mode |
| Slot lag never reaching 0 | Waiting forever on a healthy system | Reconcile counts; probe with `pg_logical_emit_message` |

## Verification checklist

- [ ] Writers stopped
- [ ] Slot drained, row counts reconciled against topic counts
- [ ] Pre-upgrade `pg_current_wal_lsn()` recorded
- [ ] Connector deleted, publication and slot dropped
- [ ] `pg_upgrade` completed, directories swapped
- [ ] `pg_hba.conf` / `postgresql.conf` restored from the old cluster
- [ ] New version starts, `wal_level=logical`, replica identity preserved, LSN moved forward
- [ ] Connector restarted with `no_data`, log shows `status=SKIPPED`, offsets unchanged
- [ ] Post-upgrade writes replicate with expected offset deltas
- [ ] `vacuumdb --analyze-in-stages` run
- [ ] Old data directory retained for rollback
