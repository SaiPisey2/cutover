//go:build integration

package pg_test

import (
	"context"
	"strings"
	"testing"
	"time"

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
