//go:build integration

package ops_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	"github.com/SaiPisey2/cutover/internal/itest"
	"github.com/SaiPisey2/cutover/internal/ops"
	"github.com/SaiPisey2/cutover/internal/pg"
	"github.com/SaiPisey2/cutover/internal/readiness"
	"github.com/SaiPisey2/cutover/internal/secret"
)

func env(t *testing.T) (*testsuite.TestActivityEnvironment, *ops.Activities) {
	var s testsuite.WorkflowTestSuite
	e := s.NewTestActivityEnvironment()
	a := ops.New()
	e.RegisterActivity(a)
	return e, a
}

func seed(t *testing.T) {
	itest.Reset(t)
	c := itest.Conn(t, itest.SourceDSN)
	if _, err := c.Exec(context.Background(), `CREATE TABLE public.orders (id bigserial PRIMARY KEY, note text);
		INSERT INTO public.orders (note) SELECT 'seed' FROM generate_series(1, 200)`); err != nil {
		t.Fatal(err)
	}
}

func TestDiscoverOnCleanHarness(t *testing.T) {
	seed(t)
	e, a := env(t)
	v, err := e.ExecuteActivity(a.Discover, ops.DiscoverIn{Plan: itest.Plan(t)})
	if err != nil {
		t.Fatal(err)
	}
	var r readiness.Report
	if err := v.Get(&r); err != nil {
		t.Fatal(err)
	}
	if r.Blocked() || len(r.Tables) != 1 || r.Tables[0] != "public.orders" {
		t.Fatalf("%+v", r)
	}
}

func TestPrepareSchemaIsIdempotent(t *testing.T) {
	seed(t)
	e, a := env(t)
	src := itest.Conn(t, itest.SourceDSN)
	// A table outside the plan must not be copied, or Undo (which drops only
	// plan tables) would leave it and a second Prepare would fail.
	if _, err := src.Exec(context.Background(), "CREATE TABLE public.scratch (x int)"); err != nil {
		t.Fatal(err)
	}
	in := ops.TablesIn{Plan: itest.Plan(t), Tables: []string{"public.orders"}}
	for range 2 {
		if _, err := e.ExecuteActivity(a.PrepareSchema, in); err != nil {
			t.Fatal(err)
		}
	}
	// Partially prepared: one of two tables present → permanent error naming the fix.
	if _, err := src.Exec(context.Background(), "CREATE TABLE public.items (sku text PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	in.Tables = []string{"public.items", "public.orders"}
	_, err := e.ExecuteActivity(a.PrepareSchema, in)
	if err == nil || !strings.Contains(err.Error(), "roll back") {
		t.Fatalf("partial: %v", err)
	}
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || !app.NonRetryable() || app.Type() != ops.ErrTypePermanent {
		t.Fatalf("partial must be permanent: %#v", err)
	}
	if _, err := e.ExecuteActivity(a.UndoPrepare, in); err != nil {
		t.Fatal(err)
	}
	if _, err := e.ExecuteActivity(a.UndoPrepare, in); err != nil {
		t.Fatal("UndoPrepare must be idempotent: ", err)
	}
	// Prepare after a rollback starts clean.
	if _, err := e.ExecuteActivity(a.PrepareSchema, in); err != nil {
		t.Fatal("Prepare after UndoPrepare: ", err)
	}
	var n int
	if err := itest.Conn(t, itest.TargetDSN).QueryRow(context.Background(),
		"SELECT count(*) FROM pg_class WHERE relname = 'scratch'").Scan(&n); err != nil || n != 0 {
		t.Fatalf("non-plan table copied: %d %v", n, err)
	}
}

func TestLinkLagAndCompare(t *testing.T) {
	seed(t)
	e, a := env(t)
	p := itest.Plan(t)
	tables := []string{"public.orders"}
	if _, err := e.ExecuteActivity(a.PrepareSchema, ops.TablesIn{Plan: p, Tables: tables}); err != nil {
		t.Fatal(err)
	}
	link := ops.Forward("itest")
	li := ops.LinkIn{Plan: p, Link: link, Tables: tables, CopyData: true}
	for range 2 {
		if _, err := e.ExecuteActivity(a.StartLink, li); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		v, err := e.ExecuteActivity(a.CopyProgress, li)
		if err != nil {
			t.Fatal(err)
		}
		var pr ops.Progress
		_ = v.Get(&pr)
		if pr.Total > 0 && pr.Ready == pr.Total {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("copy stuck at %+v", pr)
		}
		time.Sleep(200 * time.Millisecond)
	}
	v, err := e.ExecuteActivity(a.MeasureLag, ops.LagIn{Plan: p, Link: link, Timeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	var lag ops.LagOut
	_ = v.Get(&lag)
	if !lag.Visible || lag.Lag > 5*time.Second {
		t.Fatalf("lag %+v", lag)
	}
	v, err = e.ExecuteActivity(a.CompareData, ops.TablesIn{Plan: p, Tables: tables})
	if err != nil {
		t.Fatal(err)
	}
	var checks []pg.TableCheck
	_ = v.Get(&checks)
	if len(checks) != 1 || !checks[0].Match || checks[0].Sampled != 200 {
		t.Fatalf("%+v", checks)
	}
	v, err = e.ExecuteActivity(a.CheckSchema, ops.TablesIn{Plan: p, Tables: tables})
	if err != nil {
		t.Fatal(err)
	}
	var diffs []string
	_ = v.Get(&diffs)
	if len(diffs) != 0 {
		t.Fatalf("diffs %q", diffs)
	}
	for range 2 {
		if _, err := e.ExecuteActivity(a.StopLink, li); err != nil {
			t.Fatal(err)
		}
	}
	if n := slots(t, itest.SourceDSN, link.Name); n != 0 {
		t.Fatalf("slot left on publisher: %d", n)
	}
}

func slots(t *testing.T, dsn, name string) int {
	t.Helper()
	var n int
	if err := itest.Conn(t, dsn).QueryRow(context.Background(),
		"SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1", name).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func orphanSlot(t *testing.T, name string) {
	t.Helper()
	if _, err := itest.Conn(t, itest.SourceDSN).Exec(context.Background(),
		"SELECT pg_create_logical_replication_slot($1, 'pgoutput')", name); err != nil {
		t.Fatal(err)
	}
}

// A slot left by an interrupted CREATE SUBSCRIPTION would make the next
// create fail with "replication slot already exists".
func TestStartLinkDropsOrphanSlot(t *testing.T) {
	seed(t)
	e, a := env(t)
	p := itest.Plan(t)
	tables := []string{"public.orders"}
	if _, err := e.ExecuteActivity(a.PrepareSchema, ops.TablesIn{Plan: p, Tables: tables}); err != nil {
		t.Fatal(err)
	}
	link := ops.Forward("itest")
	orphanSlot(t, link.Name)
	li := ops.LinkIn{Plan: p, Link: link, Tables: tables, CopyData: true}
	if _, err := e.ExecuteActivity(a.StartLink, li); err != nil {
		t.Fatal(err)
	}
	if _, err := e.ExecuteActivity(a.StopLink, li); err != nil {
		t.Fatal(err)
	}
}

// DropSubscription and StopLink remove the publisher's slot even when the
// subscription is already gone (detached earlier, or never created).
func TestDropSubscriptionDropsPublisherSlot(t *testing.T) {
	seed(t)
	e, a := env(t)
	p := itest.Plan(t)
	link := ops.Forward("itest")
	li := ops.LinkIn{Plan: p, Link: link, Tables: []string{"public.orders"}}
	orphanSlot(t, link.Name)
	if _, err := e.ExecuteActivity(a.DropSubscription, li); err != nil {
		t.Fatal(err)
	}
	if n := slots(t, itest.SourceDSN, link.Name); n != 0 {
		t.Fatalf("DropSubscription left the slot: %d", n)
	}
	orphanSlot(t, link.Name)
	if _, err := e.ExecuteActivity(a.StopLink, li); err != nil {
		t.Fatal(err)
	}
	if n := slots(t, itest.SourceDSN, link.Name); n != 0 {
		t.Fatalf("StopLink left the slot: %d", n)
	}
}

func TestActivityErrorsAreRedacted(t *testing.T) {
	itest.Reset(t)
	e, a := env(t)
	p := itest.Plan(t)
	t.Setenv("CUTOVER_IT_BAD_DSN", "postgres://postgres:hunter2@127.0.0.1:1/app?sslmode=disable&connect_timeout=1")
	p.Source.Postgres.DSN = secret.Ref("env:CUTOVER_IT_BAD_DSN")
	_, err := e.ExecuteActivity(a.Discover, ops.DiscoverIn{Plan: p})
	if err == nil {
		t.Fatal("want a connection error")
	}
	if strings.Contains(err.Error(), "hunter2") {
		t.Fatalf("password leaked: %v", err)
	}
	// Mistyped DSNs: pgx's parse error echoes the string, masked only up to
	// the first space. Neither the activity error nor the readiness report
	// (the PgBouncer admin DSN lands in PoolErr) may carry a fragment.
	for _, m := range []struct{ dsn, fragment string }{
		{"host=127.0.0.1 user=postgres password=hun ter2 dbname=app", "ter2"},
		{"host=127.0.0.1 user=postgres password='abc def dbname=app", "def"},
		{"postgres://postgres:p@ss w@127.0.0.1/app", "ss w"},
	} {
		t.Setenv("CUTOVER_IT_BAD_DSN", m.dsn)
		_, err := e.ExecuteActivity(a.Discover, ops.DiscoverIn{Plan: p})
		if err == nil || strings.Contains(err.Error(), m.fragment) || !strings.Contains(err.Error(), "env:CUTOVER_IT_BAD_DSN") {
			t.Fatalf("source %q: %v", m.dsn, err)
		}
		q := itest.Plan(t)
		q.Database.PgBouncer.AdminDSN = secret.Ref("env:CUTOVER_IT_BAD_DSN")
		v, err := e.ExecuteActivity(a.Discover, ops.DiscoverIn{Plan: q})
		if err != nil {
			t.Fatal(err)
		}
		var rep readiness.Report
		if err := v.Get(&rep); err != nil {
			t.Fatal(err)
		}
		found := false
		for _, f := range rep.Findings {
			if strings.Contains(f.Message, m.fragment) {
				t.Fatalf("admin %q: %s", m.dsn, f.Message)
			}
			found = found || f.Code == "pgbouncer"
		}
		if !found {
			t.Fatalf("admin %q: no pgbouncer finding: %+v", m.dsn, rep.Findings)
		}
	}
}

func TestPauseRepointResume(t *testing.T) {
	itest.Reset(t)
	e, a := env(t)
	p := itest.Plan(t)
	if _, err := e.ExecuteActivity(a.PausePool, ops.PoolIn{Plan: p, Timeout: 5 * time.Second}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.ExecuteActivity(a.Repoint, ops.RepointIn{Plan: p, To: ops.Target}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.ExecuteActivity(a.ResumePool, ops.PoolIn{Plan: p}); err != nil {
		t.Fatal(err)
	}
	c := itest.Conn(t, itest.PoolDSN)
	var name string
	if err := c.QueryRow(context.Background(), "SHOW cluster_name").Scan(&name); err != nil || name != "target" {
		t.Fatalf("%s %v", name, err)
	}
}

func TestPausePoolUnderGlobalPauseIsPermanent(t *testing.T) {
	itest.Reset(t)
	e, a := env(t)
	p := itest.Plan(t)
	ctx := context.Background()
	admin, err := pgconn.Connect(ctx, itest.AdminDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(ctx, "RESUME").ReadAll()
		admin.Close(ctx)
	})
	if _, err := admin.Exec(ctx, "PAUSE").ReadAll(); err != nil {
		t.Fatal(err)
	}
	_, err = e.ExecuteActivity(a.PausePool, ops.PoolIn{Plan: p, Timeout: 5 * time.Second})
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || !app.NonRetryable() || app.Type() != ops.ErrTypePermanent || !strings.Contains(err.Error(), "globally paused") {
		t.Fatalf("%#v", err)
	}
}

// A plan table missing on the source is reported, not retried forever.
func TestCompareDataMissingSourceTable(t *testing.T) {
	seed(t)
	e, a := env(t)
	p := itest.Plan(t)
	tables := []string{"public.orders"}
	if _, err := e.ExecuteActivity(a.PrepareSchema, ops.TablesIn{Plan: p, Tables: tables}); err != nil {
		t.Fatal(err)
	}
	v, err := e.ExecuteActivity(a.CompareData, ops.TablesIn{Plan: p, Tables: []string{"public.ghost"}})
	if err != nil {
		t.Fatal(err)
	}
	var checks []pg.TableCheck
	_ = v.Get(&checks)
	if len(checks) != 1 || checks[0].Match || !strings.Contains(checks[0].Note, "not on the source") {
		t.Fatalf("%+v", checks)
	}
}

// Cancelling the activity while PAUSE waits for a transaction to drain
// reports cancellation, not a pause timeout.
func TestPausePoolCancelled(t *testing.T) {
	itest.Reset(t)
	p := itest.Plan(t)
	ctx := context.Background()
	pool := itest.Conn(t, itest.PoolDSN)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()); itest.Reset(t) })

	var s testsuite.WorkflowTestSuite
	e := s.NewTestActivityEnvironment()
	bg, cancel := context.WithCancel(context.Background())
	e.SetWorkerOptions(worker.Options{BackgroundActivityContext: bg})
	a := ops.New()
	e.RegisterActivity(a)
	time.AfterFunc(300*time.Millisecond, cancel)
	_, err = e.ExecuteActivity(a.PausePool, ops.PoolIn{Plan: p, Timeout: 10 * time.Second})
	// A cancel through BackgroundActivityContext is not a server cancel, so
	// the SDK reports the activity's context.Canceled as a failure carrying
	// that message; a server cancel is reported as cancelled (fail keeps
	// context.Canceled, see TestFailKeepsCancellation).
	if err == nil || !strings.Contains(err.Error(), "context canceled") || strings.Contains(err.Error(), "did not complete in time") {
		t.Fatalf("want the activity to return context.Canceled, got %v", err)
	}
}
