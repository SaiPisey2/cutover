# cutover

Move a Postgres database to another cloud while your users keep working.

cutover replicates with native logical replication and checks the copy. It
then switches writes during a pause it bounds (default 10 s): PgBouncer holds
queries, nothing fails. Two different people approve the switch. Reverse
replication keeps the old database current until you finish, so rolling
back is one command.

Status: release 1 in progress. This part covers the database. HTTP traffic
shifting, the web console and the end-to-end demo are next.

## Requirements

- Postgres 12+ source, same or newer target, `wal_level=logical` on both
- every table has a primary key (or `REPLICA IDENTITY FULL`)
- PgBouncer 1.21+ in transaction pooling mode, used by every app, with
  `%include <routes_file>` at the end of `pgbouncer.ini`
- `pg_dump` and `psql` on the worker host
- a Temporal server (`temporal server start-dev` is enough to try it)

## Try it

```bash
temporal server start-dev --ip 127.0.0.1 &
go run ./cmd/cutover worker &
go run ./cmd/cutover migrate --plan examples/plan.yaml
go run ./cmd/cutover status shop
go run ./cmd/cutover gate shop discover approve --as alice
```

Every command takes `--temporal HOST:PORT` (default `$CUTOVER_TEMPORAL` or
`127.0.0.1:7233`) and `--namespace` (default `default`). `status` shows the
phase, the open gate, readiness findings, recent events, and, when they
apply, a replication-broken warning and the undo phases a halted unwind left.

Schema changes are frozen during a migration: logical replication does not
carry DDL, and cutover stops at a gate if the schemas drift.

## Gates

`discover`, `prepare`, `rehearse`, `switch` (two different approvers),
`finish`. A failed step stops at `retry-<step>`; approve to try again. During
a rollback, `retry-switch-back` and `retry-undo-<phase>` do the same for the
unwind. `replication-broken` is described in the runbook below.

## Operating notes

- **Approvers are not authenticated.** The name given to `cutover gate --as`
  is recorded as given. Plan C adds accounts. Until then anyone with Temporal
  access can approve.
- **Credentials.** The plan carries only `env:` and `file:` references,
  resolved on the worker. Postgres server logs may record the
  `CREATE SUBSCRIPTION` connection string (`log_min_error_statement`,
  `log_statement=ddl`). Review the server log settings on both sides.
- **TLS and libpq settings go in the DSNs.** cutover strips inherited `PG*`
  environment variables (`PGSSLMODE`, `PGPASSFILE`, ...) from `pg_dump` and
  `psql`.
- **PgBouncer.** Transaction pooling, version 1.21 or later. The directory of
  the routes file must be on a native filesystem as PgBouncer sees it (a
  Docker volume, not a macOS bind mount: stale-size reads truncate the file).
  If PgBouncer is globally paused (`PAUSE` with no database), cutover refuses
  to switch.
- **`max_pause`.** Allowed range 1s to 5m; 5s or more is recommended, since a
  1s switch will often abort cleanly. Once the repoint has succeeded the
  switch never rolls back; resume retries until it succeeds, so a pause can
  exceed `max_pause` if PgBouncer is unreachable.
- **Kubernetes.** Nothing here touches Kubernetes. Never point a test run at
  a production kubeconfig.

## Runbook

### Halted unwind

A rollback runs the undo steps in reverse. If one cannot continue (for
example the reverse link is broken), the workflow fails with the event
`unwind-halted` and `status` lists the remaining phases under pending undo
(`Status.PendingUndo` in `status --json`). Nothing more happens by itself.
Check, then repair by hand:

1. Which side is primary: `cutover status ID` prints "writes on the source"
   or "writes on the target". Confirm in PgBouncer (`SHOW DATABASES;` on the
   admin console) that the route matches. Do not change the route unless you
   understand why the switch stopped.
2. Look at the last events for the failing phase and its message.
3. Remove what is left, only after the application is on the database you
   want to keep. For migration ID `shop` the objects are named
   `cutover_shop_fwd` (source to target) and `cutover_shop_rev` (target to
   source); hyphens in the ID become underscores. Publication, subscription
   and slot share the name.

   On each database, find leftovers:

   ```sql
   SELECT subname FROM pg_subscription WHERE subname LIKE 'cutover\_%';
   SELECT pubname FROM pg_publication WHERE pubname LIKE 'cutover\_%';
   SELECT slot_name, active FROM pg_replication_slots WHERE slot_name LIKE 'cutover\_%';
   ```

   Run these on the database that holds the subscription (the target for
   `cutover_shop_fwd`). Detaching the slot first makes the drop work even if
   the publisher is unreachable:

   ```sql
   ALTER SUBSCRIPTION cutover_shop_fwd DISABLE;
   ALTER SUBSCRIPTION cutover_shop_fwd SET (slot_name = NONE);
   DROP SUBSCRIPTION cutover_shop_fwd;
   ```

   Then on the publisher (the source for `cutover_shop_fwd`) drop the
   publication and the slot. The slot statement fails if the slot is gone
   already, or if it is still active (stop the subscriber first):

   ```sql
   DROP PUBLICATION IF EXISTS cutover_shop_fwd;
   SELECT pg_drop_replication_slot('cutover_shop_fwd');
   ```

   Repeat for `cutover_shop_rev` with the roles swapped (subscription on the
   source, publication and slot on the target). A slot left on a publisher
   holds WAL until it is dropped.
4. Drop the `_cutover` schema (it holds the sentinel table) on both sides
   with `DROP SCHEMA _cutover CASCADE;`. Drop the copied tables on the
   target only if the target is not the primary.

### replication-broken

The switch aborted but the reverse link could not be stopped, so forward
replication was left stopped. Writes are on the source. A second switch is
refused: the only way on is rollback. Approve the `replication-broken` gate
(or reject it, or run `cutover rollback`); all three roll back.
