//go:build integration

// Package itest holds helpers for integration tests against the compose
// harness in test/compose.yaml (make itest-up). Tests fail, never skip,
// when the harness is down.
package itest

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/SaiPisey2/cutover/internal/pgbouncer"
	"github.com/SaiPisey2/cutover/internal/plan"
	"github.com/SaiPisey2/cutover/internal/secret"
)

const (
	SourceDSN = "postgres://postgres@127.0.0.1:55432/app?sslmode=disable"
	TargetDSN = "postgres://postgres@127.0.0.1:55433/app?sslmode=disable"
	PoolDSN   = "postgres://postgres@127.0.0.1:56432/app?sslmode=disable"
	AdminDSN  = "postgres://postgres@127.0.0.1:56432/pgbouncer?sslmode=disable"

	// As each database reaches the other inside the compose network.
	SourceReplDSN = "host=pg-source port=5432 user=postgres dbname=app"
	TargetReplDSN = "host=pg-target port=5432 user=postgres dbname=app"

	// As PgBouncer reaches each database.
	SourceRoute = "host=pg-source port=5432 dbname=app"
	TargetRoute = "host=pg-target port=5432 dbname=app"
)

func repoRoot(t testing.TB) string {
	_, file, _, _ := runtime.Caller(0)
	dir := filepath.Dir(file)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above " + file)
		}
		dir = parent
	}
}

// RoutesFile is the host path of the routes file PgBouncer %includes.
func RoutesFile(t testing.TB) string {
	return filepath.Join(repoRoot(t), "test", "run", "pgbouncer", "routes.ini")
}

// Conn connects or fails the test with the hint to start the harness.
func Conn(t testing.TB, dsn string) *pgx.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect %s: %v (run: make itest-up)", dsn, err)
	}
	t.Cleanup(func() { c.Close(context.Background()) })
	return c
}

// Tools finds pg_dump and psql on PATH or in Homebrew's libpq.
func Tools(t testing.TB) plan.Tools {
	find := func(name string) string {
		if p, err := exec.LookPath(name); err == nil {
			return p
		}
		p := "/opt/homebrew/opt/libpq/bin/" + name
		if _, err := os.Stat(p); err == nil {
			return p
		}
		t.Fatalf("%s not found on PATH or in /opt/homebrew/opt/libpq/bin", name)
		return ""
	}
	return plan.Tools{PgDump: find("pg_dump"), Psql: find("psql")}
}

// Plan returns a valid plan for the harness, with small thresholds, and sets
// the environment variables its references name.
func Plan(t testing.TB) plan.Plan {
	env := map[string]string{
		"CUTOVER_IT_SOURCE_DSN":  SourceDSN,
		"CUTOVER_IT_TARGET_DSN":  TargetDSN,
		"CUTOVER_IT_SOURCE_REPL": SourceReplDSN,
		"CUTOVER_IT_TARGET_REPL": TargetReplDSN,
		"CUTOVER_IT_ADMIN_DSN":   AdminDSN,
	}
	for k, v := range env {
		if tt, ok := t.(*testing.T); ok {
			tt.Setenv(k, v)
		} else {
			os.Setenv(k, v)
		}
	}
	p := plan.Plan{
		Version: 1,
		Name:    "itest",
		Source: plan.Env{Postgres: plan.Postgres{DSN: secret.Ref("env:CUTOVER_IT_SOURCE_DSN"),
			ReplicationDSN: secret.Ref("env:CUTOVER_IT_SOURCE_REPL"), Route: SourceRoute}},
		Target: plan.Env{Postgres: plan.Postgres{DSN: secret.Ref("env:CUTOVER_IT_TARGET_DSN"),
			ReplicationDSN: secret.Ref("env:CUTOVER_IT_TARGET_REPL"), Route: TargetRoute}},
		Database: plan.Database{
			Name:      "app",
			Schemas:   []string{"public"},
			PgBouncer: plan.PgBouncer{AdminDSN: secret.Ref("env:CUTOVER_IT_ADMIN_DSN"), RoutesFile: RoutesFile(t)},
			Tools:     Tools(t),
		},
	}
	p.Thresholds.Replicate = plan.Replicate{MaxLag: plan.Duration(time.Second),
		StableFor: plan.Duration(3 * time.Second), SampleEvery: plan.Duration(500 * time.Millisecond)}
	p.Thresholds.Finish.Watch = plan.Duration(time.Second)
	plan.ApplyDefaults(&p)
	return p
}

// Reset returns the harness to empty: no subscriptions, publications, slots,
// non-system schemas or sentinel on either side, and PgBouncer routed to the source
// and not paused.
func Reset(t testing.TB) {
	t.Helper()
	ctx := context.Background()
	src, tgt := Conn(t, SourceDSN), Conn(t, TargetDSN)
	for _, c := range []*pgx.Conn{src, tgt} {
		mustExec(t, c, `DO $$ DECLARE s record; BEGIN
			FOR s IN SELECT subname FROM pg_subscription WHERE subdbid = (SELECT oid FROM pg_database WHERE datname = current_database()) LOOP
				EXECUTE format('ALTER SUBSCRIPTION %I DISABLE', s.subname);
				EXECUTE format('ALTER SUBSCRIPTION %I SET (slot_name = NONE)', s.subname);
				EXECUTE format('DROP SUBSCRIPTION %I', s.subname);
			END LOOP; END $$`)
	}
	for _, c := range []*pgx.Conn{src, tgt} {
		deadline := time.Now().Add(5 * time.Second)
		for {
			var active int
			if err := c.QueryRow(ctx, "SELECT count(*) FROM pg_replication_slots WHERE active").Scan(&active); err != nil {
				t.Fatal(err)
			}
			if active == 0 {
				break
			}
			if time.Now().After(deadline) {
				mustExec(t, c, "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE active")
				time.Sleep(200 * time.Millisecond)
				continue
			}
			time.Sleep(100 * time.Millisecond)
		}
		mustExec(t, c, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots")
		mustExec(t, c, `DO $$ DECLARE p record; BEGIN
			FOR p IN SELECT pubname FROM pg_publication LOOP
				EXECUTE format('DROP PUBLICATION %I', p.pubname);
			END LOOP; END $$`)
		mustExec(t, c, "SET lock_timeout = '5s'")
		mustExec(t, c, `DO $$ DECLARE n record; BEGIN
			FOR n IN SELECT nspname FROM pg_namespace
				WHERE nspname NOT LIKE 'pg\_%' AND nspname <> 'information_schema' LOOP
				EXECUTE format('DROP SCHEMA %I CASCADE', n.nspname);
			END LOOP;
			CREATE SCHEMA public; END $$`)
	}
	// Docker Desktop's file share (virtiofs) can serve a routes file replaced
	// soon after PgBouncer last read it with the old file's size: new bytes,
	// truncated. A RELOAD of a truncated file has no [databases] entry and
	// drops app, disconnecting its clients (reproduced: 16 of 40 rewrites
	// from this file's old shorter format). Writing the same format as
	// pgbouncer.WriteRoute keeps every harness routes file the same length,
	// because SourceRoute and TargetRoute are the same length.
	if err := pgbouncer.WriteRoute(RoutesFile(t), "app", SourceRoute); err != nil {
		t.Fatal(err)
	}
	admin, err := pgconn.Connect(ctx, AdminDSN)
	if err != nil {
		t.Fatalf("pgbouncer admin: %v (run: make itest-up)", err)
	}
	defer admin.Close(ctx)
	// RELOAD first: if an earlier test left PgBouncer without the app
	// database, ENABLE would fail until the routes file is read again.
	for _, cmd := range []string{"RELOAD", "ENABLE app", "RESUME app", "RECONNECT app"} {
		_, err := admin.Exec(ctx, cmd).ReadAll()
		if err != nil && !strings.Contains(strings.ToLower(err.Error()), "not paused") && !strings.Contains(strings.ToLower(err.Error()), "not disabled") {
			t.Fatalf("pgbouncer %s: %v", cmd, err)
		}
	}
}

func mustExec(t testing.TB, c *pgx.Conn, sql string) {
	t.Helper()
	if _, err := c.Exec(context.Background(), sql); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}
