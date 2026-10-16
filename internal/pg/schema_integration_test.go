//go:build integration

package pg_test

import (
	"context"
	"strings"
	"testing"

	"github.com/SaiPisey2/cutover/internal/itest"
	"github.com/SaiPisey2/cutover/internal/pg"
)

func TestCopySchemaAndDiff(t *testing.T) {
	itest.Reset(t)
	ctx := context.Background()
	src, tgt := itest.Conn(t, itest.SourceDSN), itest.Conn(t, itest.TargetDSN)
	if _, err := src.Exec(ctx, `CREATE TABLE public.orders (id bigserial PRIMARY KEY, note text NOT NULL DEFAULT '');
		CREATE INDEX orders_note ON public.orders (note);`); err != nil {
		t.Fatal(err)
	}
	// billing exists only on the source: CopySchema must create it. scratch
	// is not a plan table: CopySchema must leave it behind.
	t.Cleanup(func() {
		for _, dsn := range []string{itest.SourceDSN, itest.TargetDSN} {
			c := itest.Conn(t, dsn)
			_, _ = c.Exec(context.Background(), "DROP SCHEMA IF EXISTS billing CASCADE")
		}
	})
	if _, err := tgt.Exec(ctx, "DROP SCHEMA IF EXISTS billing CASCADE"); err != nil {
		t.Fatal(err)
	}
	if _, err := src.Exec(ctx, `DROP SCHEMA IF EXISTS billing CASCADE; CREATE SCHEMA billing;
		CREATE TABLE billing.invoices (id bigserial PRIMARY KEY);
		CREATE TABLE public.scratch (x int);`); err != nil {
		t.Fatal(err)
	}
	tables := []string{"billing.invoices", "public.orders"}
	schemas := []string{"billing", "public"}
	if err := pg.CopySchema(ctx, itest.Tools(t), itest.SourceDSN, itest.TargetDSN, schemas, tables); err != nil {
		t.Fatal(err)
	}
	a, err := pg.ReadSchema(ctx, src, tables)
	if err != nil {
		t.Fatal(err)
	}
	b, err := pg.ReadSchema(ctx, tgt, tables)
	if err != nil {
		t.Fatal(err)
	}
	if d := pg.Diff(a, b); len(d) != 0 || len(b) != 2 {
		t.Fatalf("after copy: %q (%d tables)", d, len(b))
	}
	var n int
	if err := tgt.QueryRow(ctx, `SELECT count(*) FROM pg_class c JOIN pg_namespace s ON s.oid = c.relnamespace
		WHERE (s.nspname, c.relname) IN (('public', 'orders_id_seq'), ('billing', 'invoices_id_seq'))`).Scan(&n); err != nil || n != 2 {
		t.Fatalf("owned sequences copied: %d (%v), want 2", n, err)
	}
	if err := tgt.QueryRow(ctx, "SELECT count(*) FROM pg_class WHERE relname = 'scratch'").Scan(&n); err != nil || n != 0 {
		t.Fatalf("non-plan table copied: %d (%v)", n, err)
	}
	if _, err := src.Exec(ctx, "ALTER TABLE public.orders ADD COLUMN extra int"); err != nil {
		t.Fatal(err)
	}
	a, _ = pg.ReadSchema(ctx, src, tables)
	if d := pg.Diff(a, b); len(d) != 1 || !strings.Contains(d[0], "extra") {
		t.Fatalf("drift not detected: %q", d)
	}
	// Single transaction: a failing apply leaves nothing behind.
	err = pg.CopySchema(ctx, itest.Tools(t), itest.SourceDSN, itest.TargetDSN, schemas, tables)
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("second copy should fail on existing objects: %v", err)
	}
}

func TestSentinelAndDropTables(t *testing.T) {
	itest.Reset(t)
	ctx := context.Background()
	tgt := itest.Conn(t, itest.TargetDSN)
	for range 2 {
		if err := pg.EnsureSentinel(ctx, tgt); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tgt.Exec(ctx, "CREATE TABLE public.orders (id bigserial PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := pg.DropTables(ctx, tgt, []string{"public.orders"}); err != nil {
			t.Fatal(err)
		}
		if err := pg.DropSentinel(ctx, tgt); err != nil {
			t.Fatal(err)
		}
	}
	var n int
	if err := tgt.QueryRow(ctx, `SELECT count(*) FROM pg_class WHERE relname IN ('orders', 'orders_id_seq', 'sentinel')`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("left behind %d objects (%v)", n, err)
	}
}
