//go:build integration

package pg_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/SaiPisey2/cutover/internal/itest"
	"github.com/SaiPisey2/cutover/internal/pg"
)

func TestSyncSequences(t *testing.T) {
	itest.Reset(t)
	ctx := context.Background()
	src, tgt := itest.Conn(t, itest.SourceDSN), itest.Conn(t, itest.TargetDSN)
	ddl := "CREATE TABLE public.orders (id bigserial PRIMARY KEY); CREATE SEQUENCE public.unused;"
	if _, err := src.Exec(ctx, ddl+"INSERT INTO public.orders SELECT FROM generate_series(1, 42)"); err != nil {
		t.Fatal(err)
	}
	if _, err := tgt.Exec(ctx, ddl); err != nil {
		t.Fatal(err)
	}
	got, err := pg.SyncSequences(ctx, src, tgt, []string{"public"}, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Name != "public.orders_id_seq" || got[0].Set != 1042 || !got[0].Changed {
		t.Fatalf("sync: %+v", got)
	}
	again, err := pg.SyncSequences(ctx, src, tgt, []string{"public"}, 1000)
	if err != nil || again[0].Changed {
		t.Fatalf("second sync must not move the sequence: %+v %v", again, err)
	}
	var next int64
	if err := tgt.QueryRow(ctx, "SELECT nextval('public.orders_id_seq')").Scan(&next); err != nil || next != 1043 {
		t.Fatalf("next %d %v", next, err)
	}
	if _, err := src.Exec(ctx, "CREATE SEQUENCE public.down INCREMENT -1; SELECT nextval('public.down')"); err != nil {
		t.Fatal(err)
	}
	if _, err := tgt.Exec(ctx, "CREATE SEQUENCE public.down INCREMENT -1"); err != nil {
		t.Fatal(err)
	}
	if _, err := pg.SyncSequences(ctx, src, tgt, []string{"public"}, 1000); err == nil {
		t.Fatal("descending sequences are unsupported and must be an error")
	}
}

func TestSampleDigest(t *testing.T) {
	itest.Reset(t)
	ctx := context.Background()
	src, tgt := itest.Conn(t, itest.SourceDSN), itest.Conn(t, itest.TargetDSN)
	ddl := "CREATE TABLE public.lines (o int, n int, v text, PRIMARY KEY (o, n)); CREATE TABLE public.logs (m text);"
	rows := "INSERT INTO public.lines SELECT i % 7, i, 'v' || i FROM generate_series(1, 300) i;"
	if _, err := src.Exec(ctx, ddl+rows); err != nil {
		t.Fatal(err)
	}
	if _, err := tgt.Exec(ctx, ddl+rows+"ANALYZE public.lines"); err != nil {
		t.Fatal(err)
	}
	tables, err := pg.Tables(ctx, src, []string{"public"})
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]pg.Table{}
	for _, tb := range tables {
		byName[tb.Qualified()] = tb
	}
	a, n, err := pg.SampleDigest(ctx, src, byName["public.lines"], 100)
	if err != nil || n != 100 {
		t.Fatalf("%d %v", n, err)
	}
	b, _, _ := pg.SampleDigest(ctx, tgt, byName["public.lines"], 100)
	if a != b || a == "" {
		t.Fatalf("same rows, different digests: %q %q", a, b)
	}
	if _, err := tgt.Exec(ctx, "UPDATE public.lines SET v = 'changed' WHERE o = 0 AND n = 7"); err != nil {
		t.Fatal(err)
	}
	b, _, _ = pg.SampleDigest(ctx, tgt, byName["public.lines"], 100)
	if a == b {
		t.Fatal("changed row not detected")
	}
	if _, _, err := pg.SampleDigest(ctx, src, byName["public.logs"], 100); !errors.Is(err, pg.ErrNoKey) {
		t.Fatalf("no key: %v", err)
	}
	if est, err := pg.Estimate(ctx, tgt, "public.lines"); err != nil || est != 300 {
		t.Fatalf("estimate %d %v", est, err)
	}
}

func TestSampleDigestCollationAndSettings(t *testing.T) {
	itest.Reset(t)
	ctx := context.Background()
	src, tgt := itest.Conn(t, itest.SourceDSN), itest.Conn(t, itest.TargetDSN)
	rows := "INSERT INTO public.k VALUES ('a','2026-01-01 00:00:00+00'),('B','2026-01-02 00:00:00+00'),('_x','2026-01-03 00:00:00+00'),('Z','2026-01-04 00:00:00+00');"
	if _, err := src.Exec(ctx, "CREATE TABLE public.k (id text PRIMARY KEY, at timestamptz);"+rows); err != nil {
		t.Fatal(err)
	}
	if _, err := tgt.Exec(ctx, `CREATE TABLE public.k (id text COLLATE "POSIX" PRIMARY KEY, at timestamptz);`+rows); err != nil {
		t.Fatal(err)
	}
	tables, err := pg.Tables(ctx, src, []string{"public"})
	if err != nil {
		t.Fatal(err)
	}
	k := tables[0]
	a, _, err := pg.SampleDigest(ctx, src, k, 2)
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := pg.SampleDigest(ctx, tgt, k, 2)
	if err != nil || a != b {
		t.Fatalf("collation changed the sample: %q %q %v", a, b, err)
	}
	if _, err := tgt.Exec(ctx, "SET TimeZone='Asia/Tokyo'; SET DateStyle='German'"); err != nil {
		t.Fatal(err)
	}
	c, _, err := pg.SampleDigest(ctx, tgt, k, 2)
	if err != nil || a != c {
		t.Fatalf("session settings changed the digest: %q %q %v", a, c, err)
	}
	if _, _, err := pg.SampleDigest(ctx, src, k, 0); err == nil {
		t.Fatal("n <= 0 must error")
	}
}

func TestSyncSequencesGuards(t *testing.T) {
	itest.Reset(t)
	ctx := context.Background()
	src, tgt := itest.Conn(t, itest.SourceDSN), itest.Conn(t, itest.TargetDSN)
	if _, err := pg.SyncSequences(ctx, src, tgt, []string{"public"}, -1); err == nil {
		t.Fatal("negative margin must error")
	}
	ddl := "CREATE SEQUENCE public.big MAXVALUE 9223372036854775807;"
	if _, err := src.Exec(ctx, ddl+"SELECT setval('public.big', 9223372036854775800)"); err != nil {
		t.Fatal(err)
	}
	if _, err := tgt.Exec(ctx, ddl); err != nil {
		t.Fatal(err)
	}
	_, err := pg.SyncSequences(ctx, src, tgt, []string{"public"}, 1000)
	if err == nil || !strings.Contains(err.Error(), "public.big") {
		t.Fatalf("overflow must error naming the sequence: %v", err)
	}
}
