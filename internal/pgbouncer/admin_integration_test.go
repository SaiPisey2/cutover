//go:build integration

package pgbouncer_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/SaiPisey2/cutover/internal/itest"
	"github.com/SaiPisey2/cutover/internal/pgbouncer"
)

func dial(t *testing.T) *pgbouncer.Admin {
	t.Helper()
	a, err := pgbouncer.Dial(context.Background(), itest.AdminDSN)
	if err != nil {
		t.Fatalf("%v (run: make itest-up)", err)
	}
	t.Cleanup(func() { a.Close(context.Background()) })
	return a
}

func cluster(t *testing.T) string {
	t.Helper()
	c := itest.Conn(t, itest.PoolDSN)
	var name string
	if err := c.QueryRow(context.Background(), "SHOW cluster_name").Scan(&name); err != nil {
		t.Fatal(err)
	}
	return name
}

func TestRepoint(t *testing.T) {
	itest.Reset(t)
	ctx := context.Background()
	a := dial(t)
	if got := cluster(t); got != "source" {
		t.Fatalf("start on %s", got)
	}
	for range 2 { // idempotent
		if err := pgbouncer.Repoint(ctx, a, itest.RoutesFile(t), "app", itest.TargetRoute); err != nil {
			t.Fatal(err)
		}
	}
	db, err := a.Database(ctx, "app")
	if err != nil || db.Host != "pg-target" {
		t.Fatalf("%+v %v", db, err)
	}
	if got := cluster(t); got != "target" {
		t.Fatalf("after repoint: %s", got)
	}
	if _, err := a.Database(ctx, "nope"); !errors.Is(err, pgbouncer.ErrNoDatabase) {
		t.Fatalf("missing db: %v", err)
	}
}

func TestExistingClientFollowsRepoint(t *testing.T) {
	itest.Reset(t)
	ctx := context.Background()
	a := dial(t)
	c := itest.Conn(t, itest.PoolDSN) // stays connected across the switch
	var name string
	if err := c.QueryRow(ctx, "SHOW cluster_name").Scan(&name); err != nil || name != "source" {
		t.Fatalf("%s %v", name, err)
	}
	if err := a.Pause(ctx, "app"); err != nil {
		t.Fatal(err)
	}
	if err := pgbouncer.Repoint(ctx, a, itest.RoutesFile(t), "app", itest.TargetRoute); err != nil {
		t.Fatal(err)
	}
	if err := a.Resume(ctx, "app"); err != nil {
		t.Fatal(err)
	}
	if err := c.QueryRow(ctx, "SHOW cluster_name").Scan(&name); err != nil || name != "target" {
		t.Fatalf("existing client still on %s (%v)", name, err)
	}
}

func TestPauseHoldsQueriesUntilResume(t *testing.T) {
	itest.Reset(t)
	ctx := context.Background()
	a := dial(t)
	// Log in before the pause: after Reset's RECONNECT a new login needs a
	// server connection, which a paused pool never opens.
	c := itest.Conn(t, itest.PoolDSN)
	for range 2 {
		if err := a.Pause(ctx, "app"); err != nil {
			t.Fatal(err)
		}
	}
	if db, _ := a.Database(ctx, "app"); !db.Paused {
		t.Fatal("not paused")
	}
	done := make(chan error, 1)
	go func() { _, err := c.Exec(ctx, "SELECT 1"); done <- err }()
	select {
	case err := <-done:
		t.Fatalf("query ran while paused: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	for range 2 {
		if err := a.Resume(ctx, "app"); err != nil {
			t.Fatal(err)
		}
	}
	if err := <-done; err != nil {
		t.Fatalf("held query failed: %v", err)
	}
}

func TestPauseTimesOutWithOpenTransaction(t *testing.T) {
	itest.Reset(t)
	ctx := context.Background()
	c := itest.Conn(t, itest.PoolDSN)
	tx, err := c.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	a := dial(t)
	pctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	err = a.Pause(pctx, "app")
	cancel()
	if !errors.Is(err, pgbouncer.ErrPauseTimeout) {
		t.Fatalf("got %v, want ErrPauseTimeout", err)
	}
	// The timed-out admin connection is gone; a new one must be able to resume.
	b := dial(t)
	if err := b.Resume(ctx, "app"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("the open transaction must survive an aborted pause: %v", err)
	}
	if db, _ := b.Database(ctx, "app"); db.Paused {
		t.Fatal("still paused after resume")
	}
	if _, err := c.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatal(err)
	}
}

// A retried pause (PausePool's second attempt) finds the database already
// marked paused while a transaction is still in flight. It must wait for
// that transaction, never report success early.
func TestRetriedPauseStillWaitsForDrain(t *testing.T) {
	itest.Reset(t)
	ctx := context.Background()
	c := itest.Conn(t, itest.PoolDSN)
	tx, err := c.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	a := dial(t)
	pctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	err = a.Pause(pctx, "app")
	cancel()
	if !errors.Is(err, pgbouncer.ErrPauseTimeout) {
		t.Fatalf("first attempt: got %v, want ErrPauseTimeout", err)
	}
	b := dial(t)
	t.Cleanup(func() {
		if r, err := pgbouncer.Dial(context.Background(), itest.AdminDSN); err == nil {
			_ = r.Resume(context.Background(), "app")
			r.Close(context.Background())
		}
	})
	if db, _ := b.Database(ctx, "app"); !db.Paused {
		t.Fatal("the first attempt should leave the database marked paused")
	}
	pctx, cancel = context.WithTimeout(ctx, 300*time.Millisecond)
	err = b.Pause(pctx, "app")
	cancel()
	if !errors.Is(err, pgbouncer.ErrPauseTimeout) {
		t.Fatalf("retry with a transaction open: got %v, want ErrPauseTimeout", err)
	}
	d := dial(t)
	done := make(chan error, 1)
	go func() {
		pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		done <- d.Pause(pctx, "app")
	}()
	select {
	case err := <-done:
		t.Fatalf("retry returned before the transaction ended: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("retry after drain: %v", err)
	}
	if err := d.Resume(ctx, "app"); err != nil {
		t.Fatal(err)
	}
}

// A global PAUSE (or SUSPEND) makes PgBouncer refuse PAUSE <db> with
// "already suspended/paused". Pause must say so, not report success.
func TestGlobalPauseIsRefused(t *testing.T) {
	itest.Reset(t)
	ctx := context.Background()
	g, err := pgconn.Connect(ctx, itest.AdminDSN)
	if err != nil {
		t.Fatalf("%v (run: make itest-up)", err)
	}
	t.Cleanup(func() {
		if r, err := pgconn.Connect(context.Background(), itest.AdminDSN); err == nil {
			_, _ = r.Exec(context.Background(), "RESUME").ReadAll()
			r.Close(context.Background())
		}
		g.Close(context.Background())
	})
	if _, err := g.Exec(ctx, "PAUSE").ReadAll(); err != nil {
		t.Fatal(err)
	}
	a := dial(t)
	if err := a.Pause(ctx, "app"); !errors.Is(err, pgbouncer.ErrGlobalPause) {
		t.Fatalf("got %v, want ErrGlobalPause", err)
	}
	if _, err := g.Exec(ctx, "RESUME").ReadAll(); err != nil {
		t.Fatal(err)
	}
	if err := a.Pause(ctx, "app"); err != nil {
		t.Fatalf("after global resume: %v", err)
	}
	if err := a.Resume(ctx, "app"); err != nil {
		t.Fatal(err)
	}
}

func TestPauseWithDoneContextTimesOut(t *testing.T) {
	itest.Reset(t)
	a := dial(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := a.Pause(ctx, "app"); !errors.Is(err, pgbouncer.ErrPauseTimeout) {
		t.Fatalf("got %v, want ErrPauseTimeout", err)
	}
	b := dial(t)
	if db, err := b.Database(context.Background(), "app"); err != nil || db.Paused {
		t.Fatalf("a pause that never ran left %+v %v", db, err)
	}
}
