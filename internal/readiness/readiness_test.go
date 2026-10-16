package readiness

import (
	"slices"
	"strings"
	"testing"

	"github.com/SaiPisey2/cutover/internal/pg"
	"github.com/SaiPisey2/cutover/internal/pgbouncer"
	"github.com/SaiPisey2/cutover/internal/plan"
)

func i64(v int64) *int64 { return &v }

func good() Input {
	p := plan.Plan{Database: plan.Database{Name: "app", Schemas: []string{"public"}}}
	p.Source.Postgres.Route = "host=pg-source port=5432 dbname=app"
	p.Target.Postgres.Route = "host=pg-target port=5432 dbname=app"
	src := pg.Facts{
		VersionNum: 170004, WalLevel: "logical", Superuser: true, CreateSubscription: true, MaxSlots: 10,
		ReplUser: "repl", ReplUserReplication: true,
		Extensions: []pg.Extension{{Name: "plpgsql"}, {Name: "pgcrypto"}},
		Tables:     []pg.Table{{Schema: "public", Name: "orders", ReplIdent: "d", PKCols: []string{"id"}}},
		Sequences:  []pg.Sequence{{Schema: "public", Name: "orders_id_seq", LastValue: i64(5), Increment: 1}},
		Sessions:   []pg.Session{{Addr: "10.0.0.9", Count: 3}},
	}
	tgt := pg.Facts{VersionNum: 170002, WalLevel: "logical", Superuser: true, CreateSubscription: true, MaxSlots: 10,
		ReplUser: "repl", ReplUserReplication: true,
		Extensions: []pg.Extension{{Name: "plpgsql"}, {Name: "pgcrypto"}}}
	return Input{Plan: p, Source: src, Target: tgt, Pool: pgbouncer.Database{Name: "app", Host: "pg-source"}}
}

func codes(r Report, s Severity) []string {
	var out []string
	for _, f := range r.Findings {
		if f.Severity == s {
			out = append(out, f.Code)
		}
	}
	slices.Sort(out)
	return out
}

func TestGoodInputHasNoBlockers(t *testing.T) {
	r := Assess(good())
	if r.Blocked() {
		t.Fatalf("blockers: %+v", r.Findings)
	}
	if !slices.Equal(r.Tables, []string{"public.orders"}) {
		t.Fatalf("tables %v", r.Tables)
	}
}

func TestBlockers(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*Input)
		code string
	}{
		{"old source", func(in *Input) { in.Source.VersionNum = 110000 }, "pg-version"},
		{"older target", func(in *Input) { in.Target.VersionNum = 160000 }, "pg-version"},
		{"wal level", func(in *Input) { in.Source.WalLevel = "replica" }, "wal-level"},
		{"target wal level", func(in *Input) { in.Target.WalLevel = "replica" }, "wal-level"},
		{"no replication role", func(in *Input) { in.Source.ReplUserReplication = false }, "role"},
		{"no target replication role", func(in *Input) { in.Target.ReplUserReplication = false }, "role"},
		{"cannot subscribe", func(in *Input) { in.Target.CreateSubscription = false }, "role"},
		{"no free slot", func(in *Input) { in.Source.UsedSlots = 10 }, "slots"},
		{"no pk", func(in *Input) { in.Source.Tables[0].PKCols = nil }, "no-primary-key"},
		{"unlogged", func(in *Input) { in.Source.Tables[0].Unlogged = true }, "unlogged-table"},
		{"partitioned", func(in *Input) { in.Source.Tables[0].Partitioned = true }, "partitioned"},
		{"missing table", func(in *Input) { in.Plan.Database.Tables = []string{"public.gone"} }, "table-missing"},
		{"target not empty", func(in *Input) { in.Target.Tables = in.Source.Tables }, "target-not-empty"},
		{"extension", func(in *Input) { in.Target.Extensions = in.Target.Extensions[:1] }, "extension-missing"},
		{"descending sequence", func(in *Input) { in.Source.Sequences[0].Increment = -1 }, "sequence"},
		{"mixed-case table", func(in *Input) { in.Source.Tables[0].Name = "Orders" }, "identifier"},
		{"mixed-case schema", func(in *Input) { in.Source.Tables[0].Schema = "Public" }, "identifier"},
		{"mixed-case sequence", func(in *Input) { in.Source.Sequences[0].Name = "Orders_id_seq" }, "identifier"},
		{"pool error", func(in *Input) { in.PoolErr = "connection refused" }, "pgbouncer"},
		{"pool routed elsewhere", func(in *Input) { in.Pool.Host = "pg-target" }, "pgbouncer-route"},
		{"direct clients", func(in *Input) { in.Plan.Database.PgBouncer.Addresses = []string{"10.0.1.0/24"} }, "direct-clients"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := good()
			tc.edit(&in)
			r := Assess(in)
			if !slices.Contains(codes(r, Blocker), tc.code) {
				t.Fatalf("want blocker %q, got %+v", tc.code, r.Findings)
			}
		})
	}
}

func TestUnloggedTableMessageIsDistinct(t *testing.T) {
	in := good()
	in.Source.Tables[0].Unlogged = true
	r := Assess(in)
	if slices.Contains(codes(r, Blocker), "no-primary-key") {
		t.Fatalf("an unlogged table with a key must not be reported as keyless: %+v", r.Findings)
	}
	for _, f := range r.Findings {
		if f.Code == "unlogged-table" && !strings.Contains(f.Message, "UNLOGGED") {
			t.Fatalf("message should tell the operator what to fix: %q", f.Message)
		}
	}
}

func TestOrderIsStable(t *testing.T) {
	in := good()
	in.Source.WalLevel, in.Target.WalLevel = "replica", "replica"
	first := Assess(in)
	for range 20 {
		if got := Assess(in); !slices.Equal(got.Findings, first.Findings) {
			t.Fatalf("order changed:\n%+v\n%+v", first.Findings, got.Findings)
		}
	}
	if first.Findings[0].Severity != Blocker {
		t.Fatalf("blockers first: %+v", first.Findings)
	}
}

func TestPausedPoolWarnsAndClientsAreListed(t *testing.T) {
	in := good()
	in.Pool.Paused = true
	r := Assess(in)
	if !slices.Contains(codes(r, Warning), "pgbouncer-paused") || !slices.Contains(codes(r, Info), "sessions") {
		t.Fatalf("%+v", r.Findings)
	}
	in = good()
	in.Plan.Database.PgBouncer.Addresses = []string{"10.0.0.9"}
	if Assess(in).Blocked() {
		t.Fatal("sessions from the PgBouncer address must not block")
	}
}

func TestFixRound1(t *testing.T) {
	in := good()
	r := Assess(in)
	if !slices.Contains(codes(r, Warning), "direct-clients-unchecked") {
		t.Fatalf("empty allow-list must warn: %+v", r.Findings)
	}
	in.Plan.Database.PgBouncer.Addresses = []string{"10.0.0.9"}
	if slices.Contains(codes(Assess(in), Warning), "direct-clients-unchecked") {
		t.Fatal("allow-list set, must not warn")
	}

	in = good()
	in.Source.Sequences[0].Increment = -1
	for _, f := range Assess(in).Findings {
		if f.Code == "sequence" && !strings.Contains(f.Message, "every sequence in the plan's schemas") {
			t.Fatalf("sequence message: %q", f.Message)
		}
	}

	in = good()
	in.Target.Tables = in.Source.Tables
	for _, f := range Assess(in).Findings {
		if f.Code == "target-not-empty" && !strings.Contains(f.Message, "drop or rename") {
			t.Fatalf("target-not-empty message: %q", f.Message)
		}
	}
	in = good()
	in.Source.Tables[0].Unlogged = true
	for _, f := range Assess(in).Findings {
		if f.Code == "unlogged-table" && !strings.Contains(f.Message, "ACCESS EXCLUSIVE") {
			t.Fatalf("unlogged message: %q", f.Message)
		}
	}

	in = good()
	in.PoolErr = "dial postgres://admin:hunter2@pgb:6432/pgbouncer failed"
	for _, f := range Assess(in).Findings {
		if strings.Contains(f.Message, "hunter2") {
			t.Fatalf("password leaked: %q", f.Message)
		}
	}

	in = good()
	in.Target.UsedSlots = 10
	if !slices.Contains(codes(Assess(in), Blocker), "slots") {
		t.Fatal("target slot shortage must block")
	}

	in = good()
	in.Target.Tables = []pg.Table{{Schema: "public", Name: "other"}}
	if slices.Contains(codes(Assess(in), Blocker), "target-not-empty") {
		t.Fatal("unplanned target table must be ignored")
	}
}
