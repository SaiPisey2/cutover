# cutover

Move an application to another cloud while your users keep working: its
Postgres database and its stateless services.

cutover replicates the database with native logical replication and
checks the copy. It copies each service to the target (Docker Compose or
systemd over SSH, a Kubernetes Deployment, or a copy you run yourself),
mirrors safe requests to it, then moves HTTP traffic in steps through an
Envoy front door it controls. If the target does worse than the source,
traffic goes back by itself. Writes switch to the new database during a
pause cutover bounds (default 10 s): PgBouncer holds queries, nothing
fails. Two different people approve that switch. Reverse replication keeps
the old database current until you finish, so rolling back is one command.

Status: release 1 in progress. The database, traffic and web console parts
are done; the end-to-end demo is next.

## Requirements

- Postgres 12+ source, same or newer target, `wal_level=logical` on both
- every table has a primary key (or `REPLICA IDENTITY FULL`)
- PgBouncer 1.21+ in transaction pooling mode, used by every app, with
  `%include <routes_file>` at the end of `pgbouncer.ini`
- `pg_dump` and `psql` version 17 or later on the worker host
- the worker either runs where PgBouncer reads its routes file, or reaches
  PgBouncer's host over SSH (`database.pgbouncer.host`); the SSH handshake
  happens during the switch, so it spends part of `switch.max_pause`
- each side's `dsn` reaches Postgres directly, never through PgBouncer: the
  sentinel writes and the sequence sync run while PgBouncer holds writes
- the source `dsn` role is a superuser or a member of `pg_read_all_stats`, or
  sessions that bypass PgBouncer cannot be seen (readiness warns
  `direct-clients-unchecked`)
- both `dsn` roles have `SELECT` on every sequence in the plan's schemas
  (sequences are synced in both directions)
- sequences not owned by a plan table (standalone ones) already exist on the
  target: Prepare copies only the plan tables and the sequences they own.
  A sequence only the target has is fine until something uses it; after
  that a rollback cannot sync it back (readiness warns
  `sequence-target-only`)
- a Temporal server (`temporal server start-dev` is enough to try it)
- for services: Envoy 1.39 or later next to `cutover server`, and DNS for
  every route host pointing at that Envoy before the migration starts
- SSH hosts (compose, systemd) listed in a known_hosts file; cutover always
  checks host keys. The SSH user needs root or passwordless sudo
  (`sudo: true`). Compose hosts need Docker Compose 2.20 or later
- Kubernetes (deployment): a kubeconfig file and context per side, named in
  the plan. cutover reads only that file, never `KUBECONFIG` or
  `~/.kube/config`

## The front door

`cutover server` drives Envoy over xDS and keeps the routes in a state
file, so a restart picks up where it left off; Envoy keeps serving while
the server is down. Every change waits until Envoy confirms it.

```sh
head -c 32 /dev/urandom | base64 > /etc/cutover/frontdoor-token
cutover server --state /var/lib/cutover/frontdoor.json \
  --token-file /etc/cutover/frontdoor-token --listen 0.0.0.0:80
cutover envoy-bootstrap > /etc/envoy/cutover.yaml
envoy -c /etc/envoy/cutover.yaml
```

xDS and the API listen on 127.0.0.1 only, so run Envoy, the server and the
worker on one host, or reach the API through an SSH tunnel. Then register
the plan's routes (100% to the source) and point DNS at the front door:

```sh
cutover route apply --plan plan.yaml
cutover route list --plan plan.yaml
```

A finished migration leaves its route pointing only at the target. Running
`route apply` again replaces it with the plan's 100% source route, even if
another plan name owns it, so the same host can be migrated again. Discover
blocks until every route is registered and every route host resolves only
to `frontdoor.addresses`.

## Services

Each service in the plan has a kind:

- `compose`: one service of a Compose project on an SSH host. cutover
  copies its resolved definition (image, environment, ports) to the target
  host as project `cutover-<plan>-<service>` and starts it. Builds, bind
  mounts and named volumes block: that state lives on the source host.
- `systemd`: a unit on an SSH host, plus the files listed in
  `systemd.files` (its binary, environment files, config). cutover never
  overwrites a file or unit it did not create, and marks what it creates
  so rollback removes exactly that.
- `deployment`: a Kubernetes Deployment with the ConfigMaps, Secrets,
  ServiceAccount and Services it uses. Persistent volume claims and host
  paths block.
- `external`: you run both copies; cutover only moves the traffic.

A service with a `route` is reached through the front door. One without
(a background worker) is copied too, and both copies run from Prepare
until you stop the source one.

Rehearse mirrors GET and HEAD requests (set `mirror_methods`; only GET,
HEAD and OPTIONS are allowed, because a mirrored write would run twice
against the shared database) and shows both sides' error rates and p99.
Shift then moves traffic in steps (default 1, 10, 50, 100%), holding each
for `shift.hold`. If the target's error rate is more than
`shift.max_error_delta` points above the source's, or its p99 more than
`shift.max_p99` times the reference, every service goes back to 0% and a
gate asks what to do. The reference p99 is the larger of the source's and
the target's p99 you approved in Rehearse: until the database switches,
the target copies reach it across clouds, and that cost is already in the
number you approved. A step with fewer than `shift.min_requests` target
requests is not judged, and never advances on its own.

## Try it

The example plan reads its credentials from `CUTOVER_SOURCE_DSN`,
`CUTOVER_TARGET_DSN` and `CUTOVER_PGBOUNCER_ADMIN_DSN` (export them) and
from the files `/etc/cutover/source-repl-dsn` and `/etc/cutover/target-repl-dsn`
(create them, one DSN each). Edit the plan to match your setup.

Run the worker on the PgBouncer host, or anywhere that reaches it over SSH
when `database.pgbouncer.host` is set, since it rewrites the routes file at
the switch. `migrate`, `gate` and `rollback` go through the console (see
below), so start the server with `--data` and create an approver first.

```bash
temporal server start-dev --ip 127.0.0.1 &
go run ./cmd/cutover worker &   # on the PgBouncer host, or one that reaches it over SSH
go run ./cmd/cutover approver new --data ./data --name alice   # asks for a password
head -c 32 /dev/urandom | base64 > ./data/frontdoor-token
go run ./cmd/cutover server --data ./data --state ./data/frontdoor.json \
  --token-file ./data/frontdoor-token &
export CUTOVER_CA=./data/ca.crt
go run ./cmd/cutover migrate --plan examples/plan.yaml --as alice
go run ./cmd/cutover status shop
go run ./cmd/cutover gate shop discover approve --as alice
go run ./cmd/cutover rollback shop --as alice --reason "why"   # any time before finish
```

Positional arguments (ID, gate, decision) come before flags.

`worker` and `status` take `--temporal HOST:PORT` (default `$CUTOVER_TEMPORAL`
or `127.0.0.1:7233`) and `--namespace` (default `default`). `status` still
reads Temporal directly; it shows the phase, the open gate, readiness
findings, recent events, and, when they apply, a replication-broken warning
and the undo phases a halted unwind left.

Schema changes are frozen during a migration: logical replication does not
carry DDL, and cutover stops at a gate if the schemas drift.

## The console

`cutover server --data DIR` also serves a web console: the migrations,
each one's timeline, live replication and traffic panels, the open gate
with its evidence, the rehearsal diff, and the audit log. The data
directory holds the approver accounts, the audit log and the console's
TLS certificate.

Create an account for each person who approves:

```sh
cutover approver new --data /var/lib/cutover --name alice
cutover approver list --data /var/lib/cutover
cutover approver disable --data /var/lib/cutover alice
```

Then start the server with the same `--data`. The console listens on
loopback only, at https://127.0.0.1:7700 (`--console` picks another
loopback port). From another machine, use an SSH tunnel:

```sh
ssh -L 7700:127.0.0.1:7700 cutover-host
```

The server makes its own certificate authority and writes it to
`<data>/ca.crt`. Trust that file in your browser, and give it to the CLI
with `--ca`, `CUTOVER_CA` or by copying it to `~/.cutover/ca.crt`. Add
names to the certificate with `--tls-hosts` if you reach the console by
another name.

`migrate`, `gate` and `rollback` sign in to the console like the browser
does: `--server` (default `https://127.0.0.1:7700`), `--ca`, `--as NAME`,
and the password from the terminal or, with `--password-stdin`, from
standard input. The server is the only thing that signals workflows, so
every decision passes the same checks wherever it comes from:

- **Two people switch.** The second approval of `switch` must come from a
  different account whose name also differs from the first approver's.
  Disabling an account and making a new one with the same name does not
  make a second person.
- **Fresh sign-in for the switch.** Approving `switch` needs a session
  younger than 15 minutes; sign in again if yours is older.
- **Decisions are tied to what you saw.** Each decision carries a digest of
  the gate and the evidence shown with it. If either changed since you
  looked (another approval arrived, new checks came in), the server refuses
  with "evidence changed; review the gate again".
- **Everything is logged.** Every sign-in, migration start, gate decision
  and rollback goes to an audit log with the account, the time, the
  decision, the note and the evidence digest. The log is append-only; the
  database refuses updates and deletes.

With `--data`, the server also asks Envoy to log failed requests to
`access.log` beside the `--state` file (or `--access-log PATH`), and the
console shows recent failures per service and side. Envoy must be able to
write that path. Envoy does not log mirrored requests, so target failures
appear once traffic starts shifting, not during the rehearsal.

## Gates

`discover`, `prepare`, `rehearse`, `shift-<weight>`
(one per step: approve to move to that weight, or to restart the ramp;
reject rolls back), `shift-tripped` (the target did worse than the source
and traffic is back at 0%; approve to restart the ramp, reject rolls back),
`switch` (two different approvers), `finish`. A failed step stops at `retry-<step>`; approve to try again. During
a rollback, `retry-switch-back` and `retry-undo-<phase>` do the same for the
unwind. `replication-broken` is described in the runbook below.

## Operating notes

- **Keep Temporal's frontend on loopback.** Approvers are authenticated by
  the console, but anyone who can reach Temporal directly can signal a
  workflow and skip those checks. Run Temporal on 127.0.0.1 (as
  `temporal server start-dev --ip 127.0.0.1` does), on the same host as
  the server and the worker.
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
  to switch. The test harness (`make itest-up`) does bind-mount the routes
  directory from macOS, and works only because cutover always writes the
  routes file at a fixed size (1024 bytes), so a stale size cannot cut it
  short; do not rely on that in production.
- **Timeouts must outlast the hold.** While writes are held PgBouncer queues
  queries, so `query_wait_timeout` in PgBouncer, and every client statement
  or request timeout, must be longer than the worst-case hold. A normal
  switch holds for at most `max_pause`. An aborted switch whose replication
  compensations both time out holds for `max_pause` plus about 1m36s (two
  bounded compensations of up to 48s each) plus however long repoint-back
  takes, before writes resume. PgBouncer's default `query_wait_timeout` is
  120s, which `max_pause` above about 20s exceeds.
- **Sessions that bypass PgBouncer** are checked at Discover and again just
  before the `switch` gate opens; the gate's summary says how many there are,
  or that they could not be checked.
- **`max_pause`.** Allowed range 1s to 5m; 5s or more is recommended, since a
  1s switch will often abort cleanly. Once the repoint has succeeded the
  switch never rolls back; resume retries until it succeeds, so a pause can
  exceed `max_pause` if PgBouncer is unreachable.
- **Kubernetes.** cutover only touches the clusters named by the plan's
  kubeconfig files. Never point a test run at a production kubeconfig.

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
2. Look at the last events for the failing phase and its message. If a
   switch back aborted and could not restart `cutover_<id>_rev`, the target
   holds writes the source has not received: do not recreate that link
   with `copy_data = false`, and do not drop the target's tables.
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

The switch aborted and forward replication was left stopped: either the
reverse link could not be stopped (restarting forward replication too would
make the two loop), or the forward link could not be restarted. Writes are
on the source. A second switch is
refused: the only way on is rollback. Approve the `replication-broken` gate
(or reject it, or run `cutover rollback`); all three roll back.

## License

Apache 2.0
