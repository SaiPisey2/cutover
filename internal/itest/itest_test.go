//go:build integration

package itest

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestHarness(t *testing.T) {
	ctx := context.Background()
	Reset(t)
	for dsn, want := range map[string]string{SourceDSN: "source", TargetDSN: "target", PoolDSN: "source"} {
		c := Conn(t, dsn)
		var name, wal string
		if err := c.QueryRow(ctx, "SELECT current_setting('cluster_name'), current_setting('wal_level')").Scan(&name, &wal); err != nil {
			t.Fatal(err)
		}
		if name != want || wal != "logical" {
			t.Errorf("%s: cluster %q wal %q, want %q logical", dsn, name, wal, want)
		}
	}
	p := Plan(t)
	if err := p.Validate(); err != nil {
		t.Fatalf("itest plan invalid: %v", err)
	}
}

func TestResetCleansDirtyState(t *testing.T) {
	ctx := context.Background()
	Reset(t)
	src, tgt := Conn(t, SourceDSN), Conn(t, TargetDSN)
	for _, sql := range []string{
		"CREATE TABLE public.t (id int PRIMARY KEY)",
		"CREATE PUBLICATION pub_t FOR TABLE public.t",
		"CREATE SCHEMA _cutover; CREATE TABLE _cutover.sentinel (x int)",
		"CREATE SCHEMA billing; CREATE TABLE billing.inv (id int)",
	} {
		mustExec(t, src, sql)
	}
	for _, sql := range []string{
		"CREATE TABLE public.t (id int PRIMARY KEY)",
		"CREATE SCHEMA billing; CREATE TABLE billing.inv (id int)",
		"CREATE SUBSCRIPTION sub_t CONNECTION '" + SourceReplDSN + "' PUBLICATION pub_t",
		"SELECT pg_create_logical_replication_slot('stray', 'pgoutput')",
	} {
		mustExec(t, tgt, sql)
	}
	// a subscription on the source whose slot lives on the target
	mustExec(t, src, "CREATE SUBSCRIPTION sub_back CONNECTION '"+TargetReplDSN+"' PUBLICATION nothing WITH (connect = false, slot_name = 'stray')")

	admin, err := pgconn.Connect(ctx, AdminDSN)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "PAUSE app").ReadAll(); err != nil {
		t.Fatal(err)
	}
	admin.Close(ctx)
	if err := os.WriteFile(RoutesFile(t), []byte("[databases]\napp = "+TargetRoute+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	Reset(t)

	for name, c := range map[string]*pgx.Conn{"source": src, "target": tgt} {
		for _, q := range []string{
			"SELECT count(*) FROM pg_publication",
			"SELECT count(*) FROM pg_subscription",
			"SELECT count(*) FROM pg_replication_slots",
			"SELECT count(*) FROM pg_namespace WHERE nspname IN ('_cutover','billing')",
			"SELECT count(*) FROM pg_class WHERE relnamespace = 'public'::regnamespace",
		} {
			var n int
			if err := c.QueryRow(ctx, q).Scan(&n); err != nil {
				t.Fatal(err)
			}
			if n != 0 {
				t.Errorf("%s: %s = %d, want 0", name, q, n)
			}
		}
	}
	pc := Conn(t, PoolDSN)
	qctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var name string
	if err := pc.QueryRow(qctx, "SELECT current_setting('cluster_name')").Scan(&name); err != nil {
		t.Fatalf("pool query after Reset: %v", err)
	}
	if name != "source" {
		t.Errorf("pool reaches %q, want source", name)
	}
}
