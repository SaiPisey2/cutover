//go:build integration

package pg_test

import (
	"context"
	"testing"

	"github.com/SaiPisey2/cutover/internal/itest"
	"github.com/SaiPisey2/cutover/internal/pg"
)

func TestInspect(t *testing.T) {
	itest.Reset(t)
	ctx := context.Background()
	src := itest.Conn(t, itest.SourceDSN)
	if _, err := src.Exec(ctx, `
		CREATE TABLE public.orders (id bigserial PRIMARY KEY, note text);
		CREATE TABLE public.lines (order_id bigint, n int, PRIMARY KEY (n, order_id));
		CREATE TABLE public.logs (msg text);
		CREATE TABLE public.audit (msg text); ALTER TABLE public.audit REPLICA IDENTITY FULL;
		CREATE TABLE public.events (at date, id int) PARTITION BY RANGE (at);
		CREATE TABLE public.events_2026 PARTITION OF public.events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
		INSERT INTO public.orders (note) VALUES ('a'), ('b');`); err != nil {
		t.Fatal(err)
	}
	f, err := pg.Inspect(ctx, src, []string{"public"})
	if err != nil {
		t.Fatal(err)
	}
	if f.Major() != 17 || f.WalLevel != "logical" || !f.Superuser || !f.CreateSubscription || f.MaxSlots != 10 {
		t.Fatalf("facts: %+v", f)
	}
	got := map[string]pg.Table{}
	for _, tb := range f.Tables {
		got[tb.Qualified()] = tb
	}
	if _, ok := got["public.events_2026"]; ok {
		t.Error("partitions must not be listed separately")
	}
	if !got["public.events"].Partitioned {
		t.Error("events should be partitioned")
	}
	if pk := got["public.lines"].PKCols; len(pk) != 2 || pk[0] != "n" || pk[1] != "order_id" {
		t.Errorf("lines pk in index order: %v", pk)
	}
	if got["public.logs"].Replicable() || !got["public.audit"].Replicable() || !got["public.orders"].Replicable() {
		t.Errorf("replicable: %+v", got)
	}
	if len(f.Sequences) != 1 || f.Sequences[0].Qualified() != "public.orders_id_seq" ||
		f.Sequences[0].LastValue == nil || *f.Sequences[0].LastValue != 2 || f.Sequences[0].Increment != 1 {
		t.Errorf("sequences: %+v", f.Sequences)
	}
	if len(f.Available) == 0 {
		t.Error("available extensions empty")
	}
	if ok, err := pg.CanReplicate(ctx, src, "postgres"); err != nil || !ok {
		t.Errorf("postgres can replicate: %v %v", ok, err)
	}
	if ok, err := pg.CanReplicate(ctx, src, "no_such_role"); err != nil || ok {
		t.Errorf("missing role: %v %v", ok, err)
	}
}
