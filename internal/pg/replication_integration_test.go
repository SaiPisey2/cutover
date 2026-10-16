//go:build integration

package pg_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/SaiPisey2/cutover/internal/itest"
	"github.com/SaiPisey2/cutover/internal/pg"
)

func TestReplicationLifecycle(t *testing.T) {
	itest.Reset(t)
	ctx := context.Background()
	src, tgt := itest.Conn(t, itest.SourceDSN), itest.Conn(t, itest.TargetDSN)
	if _, err := src.Exec(ctx, `CREATE TABLE public.orders (id bigserial PRIMARY KEY, note text);
		INSERT INTO public.orders (note) SELECT 'seed' FROM generate_series(1, 500)`); err != nil {
		t.Fatal(err)
	}
	if err := pg.EnsureSentinel(ctx, src); err != nil {
		t.Fatal(err)
	}
	if err := pg.EnsureSentinel(ctx, tgt); err != nil {
		t.Fatal(err)
	}
	if err := pg.CopySchema(ctx, itest.Tools(t), itest.SourceDSN, itest.TargetDSN, []string{"public"}, []string{"public.orders"}); err != nil {
		t.Fatal(err)
	}
	name := pg.ObjectName("itest", "fwd")
	tables := []string{"public.orders", pg.SentinelTable}
	for range 2 { // idempotent
		if err := pg.EnsurePublication(ctx, src, name, tables); err != nil {
			t.Fatal(err)
		}
		if err := pg.EnsureSubscription(ctx, tgt, name, itest.SourceReplDSN, name, true); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		ready, total, err := pg.CopyProgress(ctx, tgt, name)
		if err != nil {
			t.Fatal(err)
		}
		if total == 2 && ready == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("copy not ready: %d/%d", ready, total)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if _, err := src.Exec(ctx, "INSERT INTO public.orders (note) VALUES ('live')"); err != nil {
		t.Fatal(err)
	}
	tok, err := pg.WriteSentinel(ctx, src)
	if err != nil {
		t.Fatal(err)
	}
	wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	visible, err := pg.WaitSentinel(wctx, tgt, tok, 50*time.Millisecond)
	cancel()
	if err != nil || !visible {
		t.Fatalf("sentinel not visible: %v %v", visible, err)
	}
	var n int
	if err := tgt.QueryRow(ctx, "SELECT count(*) FROM public.orders").Scan(&n); err != nil || n != 501 {
		t.Fatalf("target has %d rows (%v), want 501", n, err)
	}
	// A token that never arrives times out as not visible, not as an error.
	// The timeout may close the connection it ran on, so use a fresh one.
	sctx, scancel := context.WithTimeout(ctx, 300*time.Millisecond)
	visible, err = pg.WaitSentinel(sctx, itest.Conn(t, itest.TargetDSN), "never-written", 50*time.Millisecond)
	scancel()
	if err != nil || visible {
		t.Fatalf("missing token: %v %v", visible, err)
	}
	for range 2 {
		if err := pg.DropSubscription(ctx, tgt, name); err != nil {
			t.Fatal(err)
		}
		if err := pg.DropPublication(ctx, src, name); err != nil {
			t.Fatal(err)
		}
	}
	var slots int
	if err := src.QueryRow(ctx, "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1", name).Scan(&slots); err != nil || slots != 0 {
		t.Fatalf("slot left on source: %d %v", slots, err)
	}
}

func TestEnsureSubscriptionErrorHidesPassword(t *testing.T) {
	itest.Reset(t)
	ctx := context.Background()
	tgt := itest.Conn(t, itest.TargetDSN)
	err := pg.EnsureSubscription(ctx, tgt, "cutover_pw_fwd",
		"host=127.0.0.1 port=1 user=postgres password=s3cr3t-pw dbname=app connect_timeout=2", "nopub", true)
	if err == nil {
		t.Fatal("expected failure")
	}
	if strings.Contains(err.Error(), "s3cr3t-pw") {
		t.Fatalf("password leaked: %v", err)
	}
}

// replSetup creates public.orders on both sides (empty target copy via
// CopySchema) plus the sentinel table, and returns both connections.
func replSetup(t *testing.T) (ctx context.Context, src, tgt *pgx.Conn) {
	t.Helper()
	itest.Reset(t)
	ctx = context.Background()
	src, tgt = itest.Conn(t, itest.SourceDSN), itest.Conn(t, itest.TargetDSN)
	if _, err := src.Exec(ctx, `CREATE TABLE public.orders (id bigserial PRIMARY KEY, note text);
		INSERT INTO public.orders (note) SELECT 'seed' FROM generate_series(1, 10)`); err != nil {
		t.Fatal(err)
	}
	if err := pg.CopySchema(ctx, itest.Tools(t), itest.SourceDSN, itest.TargetDSN, []string{"public"}, []string{"public.orders"}); err != nil {
		t.Fatal(err)
	}
	return ctx, src, tgt
}

func TestDropSubscriptionMissingSlot(t *testing.T) {
	ctx, src, tgt := replSetup(t)
	name := pg.ObjectName("itest", "fwd")
	if err := pg.EnsurePublication(ctx, src, name, []string{"public.orders"}); err != nil {
		t.Fatal(err)
	}
	if err := pg.EnsureSubscription(ctx, tgt, name, itest.SourceReplDSN, name, false); err != nil {
		t.Fatal(err)
	}
	if _, err := tgt.Exec(ctx, "ALTER SUBSCRIPTION "+name+" DISABLE"); err != nil {
		t.Fatal(err)
	}
	if err := pg.DropSlot(ctx, src, name); err != nil {
		t.Fatal(err)
	}
	if err := pg.DropSubscription(ctx, tgt, name); err != nil {
		t.Fatalf("drop with missing slot: %v", err)
	}
	var n int
	if err := tgt.QueryRow(ctx, "SELECT count(*) FROM pg_subscription WHERE subname = $1", name).Scan(&n); err != nil || n != 0 {
		t.Fatalf("subscription remains: %d %v", n, err)
	}
}

func TestDropSlot(t *testing.T) {
	ctx, src, tgt := replSetup(t)
	name := pg.ObjectName("itest", "fwd")
	if err := pg.DropSlot(ctx, src, name); err != nil {
		t.Fatalf("absent slot: %v", err)
	}
	if err := pg.EnsurePublication(ctx, src, name, []string{"public.orders"}); err != nil {
		t.Fatal(err)
	}
	if err := pg.EnsureSubscription(ctx, tgt, name, itest.SourceReplDSN, name, false); err != nil {
		t.Fatal(err)
	}
	// The slot must be active (walsender attached) before we drop it.
	deadline := time.Now().Add(10 * time.Second)
	for {
		var active bool
		if err := src.QueryRow(ctx, "SELECT coalesce(bool_or(active_pid IS NOT NULL), false) FROM pg_replication_slots WHERE slot_name = $1", name).Scan(&active); err != nil {
			t.Fatal(err)
		}
		if active {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("slot never became active")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := pg.DropSlot(ctx, src, name); err != nil {
		t.Fatalf("active slot: %v", err)
	}
	var n int
	if err := src.QueryRow(ctx, "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1", name).Scan(&n); err != nil || n != 0 {
		t.Fatalf("slot remains: %d %v", n, err)
	}
}

func TestEnsureSubscriptionRefreshesNewTable(t *testing.T) {
	ctx, src, tgt := replSetup(t)
	name := pg.ObjectName("itest", "fwd")
	if err := pg.EnsurePublication(ctx, src, name, []string{"public.orders"}); err != nil {
		t.Fatal(err)
	}
	if err := pg.EnsureSubscription(ctx, tgt, name, itest.SourceReplDSN, name, true); err != nil {
		t.Fatal(err)
	}
	for _, c := range []*pgx.Conn{src, tgt} {
		if _, err := c.Exec(ctx, "CREATE TABLE public.items (id int PRIMARY KEY)"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := src.Exec(ctx, "INSERT INTO public.items SELECT generate_series(1, 7)"); err != nil {
		t.Fatal(err)
	}
	if err := pg.EnsurePublication(ctx, src, name, []string{"public.orders", "public.items"}); err != nil {
		t.Fatal(err)
	}
	if err := pg.EnsureSubscription(ctx, tgt, name, itest.SourceReplDSN, name, true); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		ready, total, err := pg.CopyProgress(ctx, tgt, name)
		if err != nil {
			t.Fatal(err)
		}
		var n int
		if err := tgt.QueryRow(ctx, "SELECT count(*) FROM public.items").Scan(&n); err != nil {
			t.Fatal(err)
		}
		if total == 2 && ready == 2 && n == 7 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("not refreshed: %d/%d, %d rows", ready, total, n)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func TestCopyProgressMissingSubscription(t *testing.T) {
	itest.Reset(t)
	tgt := itest.Conn(t, itest.TargetDSN)
	if _, _, err := pg.CopyProgress(context.Background(), tgt, "cutover_nope_fwd"); err == nil {
		t.Fatal("want error for missing subscription")
	}
}

func TestWaitSentinelCancelled(t *testing.T) {
	itest.Reset(t)
	tgt := itest.Conn(t, itest.TargetDSN)
	if err := pg.EnsureSentinel(context.Background(), tgt); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	visible, err := pg.WaitSentinel(ctx, itest.Conn(t, itest.TargetDSN), "x", 50*time.Millisecond)
	if visible || !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v %v, want false context.Canceled", visible, err)
	}
}

func TestEnsureSubscriptionAsNonSuperuser(t *testing.T) {
	ctx, src, tgt := replSetup(t)
	name := pg.ObjectName("itest", "fwd")
	role := "cutover_it_role"
	for _, q := range []string{
		"CREATE ROLE " + role + " LOGIN",
		"GRANT pg_create_subscription TO " + role,
		"GRANT CREATE ON DATABASE app TO " + role,
	} {
		if _, err := tgt.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), itest.TargetDSN)
		if err != nil {
			return
		}
		defer c.Close(context.Background())
		_, _ = c.Exec(context.Background(), "DROP SUBSCRIPTION IF EXISTS "+name)
		_, _ = c.Exec(context.Background(), "DROP OWNED BY "+role)
		_, _ = c.Exec(context.Background(), "DROP ROLE IF EXISTS "+role)
	})
	if err := pg.EnsurePublication(ctx, src, name, []string{"public.orders"}); err != nil {
		t.Fatal(err)
	}
	// The harness uses trust auth, so a non-superuser cannot get a working
	// subscription (password_required) and EnsureSubscription may fail
	// past the catalog read. What must not happen is the catalog read itself
	// being denied: subconninfo is revoked from PUBLIC.
	rc := itest.Conn(t, "postgres://"+role+"@127.0.0.1:55433/app?sslmode=disable")
	for range 2 {
		err := pg.EnsureSubscription(ctx, rc, name, "host=pg-source port=5432 user=postgres password=x dbname=app", name, false)
		if err != nil && (strings.Contains(err.Error(), "pg_subscription") || strings.Contains(err.Error(), "subconninfo")) {
			t.Fatalf("catalog read denied for non-superuser: %v", err)
		}
	}
	_ = src
}
