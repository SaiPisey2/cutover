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
		ReplUser: "repl", ReplUserReplication: true, SeesAllSessions: true,
		Extensions: []pg.Extension{{Name: "plpgsql"}, {Name: "pgcrypto"}},
		Tables:     []pg.Table{{Schema: "public", Name: "orders", ReplIdent: "d", PKCols: []string{"id"}}},
		Sequences:  []pg.Sequence{{Schema: "public", Name: "orders_id_seq", LastValue: i64(5), Increment: 1, OwnedBy: "public.orders"}},
		Sessions:   []pg.Session{{Addr: "10.0.0.9", Count: 3}},
	}
	tgt := pg.Facts{VersionNum: 170002, WalLevel: "logical", Superuser: true, CreateSubscription: true, MaxSlots: 10,
		ReplUser: "repl", ReplUserReplication: true,
		Extensions: []pg.Extension{{Name: "plpgsql"}, {Name: "pgcrypto"}}}
	return Input{Plan: p, Source: src, Target: tgt, Pool: pgbouncer.Database{Name: "app", Host: "pg-source", Port: 5432, DB: "app"}}
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
		{"pool routed to another port", func(in *Input) { in.Pool.Port = 6543 }, "pgbouncer-route"},
		{"pool routed to another database", func(in *Input) { in.Pool.DB = "app_old" }, "pgbouncer-route"},
		{"source route unparsable", func(in *Input) { in.Plan.Source.Postgres.Route = "host=pg-source port=x" }, "pgbouncer-route"},
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

// Prepare copies only the sequences plan tables own. Any other sequence in
// the plan's schemas that the target lacks would make every switch abort
// when SyncSequences runs inside the pause, so it blocks now.
func TestSequenceMissingOnTarget(t *testing.T) {
	in := good()
	in.Source.Sequences = append(in.Source.Sequences, pg.Sequence{Schema: "public", Name: "ticket_no", Increment: 1})
	r := Assess(in)
	if !slices.Contains(codes(r, Blocker), "sequence-missing") {
		t.Fatalf("want sequence-missing: %+v", r.Findings)
	}
	for _, f := range r.Findings {
		if f.Code == "sequence-missing" && (f.Subject != "public.ticket_no" || !strings.Contains(f.Message, "public.ticket_no")) {
			t.Fatalf("must name the sequence: %+v", f)
		}
	}
	// Present on the target: fine.
	in.Target.Sequences = []pg.Sequence{{Schema: "public", Name: "ticket_no", Increment: 1}}
	if r := Assess(in); slices.Contains(codes(r, Blocker), "sequence-missing") {
		t.Fatalf("%+v", r.Findings)
	}
	// Owned by a table outside the plan: Prepare will not copy it either.
	in = good()
	in.Plan.Database.Tables = []string{"public.orders"}
	in.Source.Tables = append(in.Source.Tables, pg.Table{Schema: "public", Name: "other", ReplIdent: "d", PKCols: []string{"id"}})
	in.Source.Sequences = append(in.Source.Sequences, pg.Sequence{Schema: "public", Name: "other_id_seq", Increment: 1, OwnedBy: "public.other"})
	if r := Assess(in); !slices.Contains(codes(r, Blocker), "sequence-missing") {
		t.Fatalf("a sequence owned by a table outside the plan is not copied: %+v", r.Findings)
	}
}

// Without superuser or pg_read_all_stats, other roles' sessions show NULL
// client_addr and application_name and drop out of the session list: the
// check cannot be trusted, and readiness must say so.
func TestDirectClientsUncheckedWithoutStats(t *testing.T) {
	in := good()
	in.Plan.Database.PgBouncer.Addresses = []string{"10.0.0.0/24"}
	if r := Assess(in); slices.Contains(codes(r, Warning), "direct-clients-unchecked") {
		t.Fatalf("%+v", r.Findings)
	}
	in.Source.SeesAllSessions = false
	r := Assess(in)
	if !slices.Contains(codes(r, Warning), "direct-clients-unchecked") {
		t.Fatalf("want direct-clients-unchecked: %+v", r.Findings)
	}
	for _, f := range r.Findings {
		if f.Code == "direct-clients-unchecked" && !strings.Contains(f.Message, "pg_read_all_stats") {
			t.Fatalf("must say what grants visibility: %q", f.Message)
		}
	}
}

func TestDirectClients(t *testing.T) {
	sess := []pg.Session{{Addr: "10.0.0.9", Count: 3}, {Addr: "10.0.1.5", Count: 1}, {Addr: "local", Count: 2}}
	if _, ok := DirectClients(nil, sess); ok {
		t.Fatal("no addresses: cannot check")
	}
	got, ok := DirectClients([]string{"10.0.0.0/24"}, sess)
	if !ok || len(got) != 1 || got[0].Addr != "10.0.1.5" {
		t.Fatalf("%v %+v", ok, got)
	}
}

// The route check matches Repoint's: host (any case), port and dbname, with
// PgBouncer's defaults applied to the plan's route.
func TestPoolRouteDefaults(t *testing.T) {
	in := good()
	in.Plan.Source.Postgres.Route = "host=PG-Source"
	if r := Assess(in); slices.Contains(codes(r, Blocker), "pgbouncer-route") {
		t.Fatalf("default port 5432 and dbname app must match: %+v", r.Findings)
	}
}

// A rollback syncs sequences from the target back to the source. One the
// target has and the source lacks would abort it once used, so say so now.
func TestSequenceOnlyOnTarget(t *testing.T) {
	in := good()
	in.Target.Sequences = append(in.Target.Sequences, pg.Sequence{Schema: "public", Name: "audit_seq", Increment: 1})
	r := Assess(in)
	if r.Blocked() {
		t.Fatalf("must not block: %+v", r.Findings)
	}
	found := false
	for _, f := range r.Findings {
		if f.Code == "sequence-target-only" && f.Severity == Warning && f.Subject == "public.audit_seq" {
			found = true
		}
	}
	if !found {
		t.Fatalf("want sequence-target-only warning: %+v", r.Findings)
	}
}
