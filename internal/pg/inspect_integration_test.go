//go:build integration

package pg_test

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/SaiPisey2/cutover/internal/itest"
	"github.com/SaiPisey2/cutover/internal/pg"
	"github.com/jackc/pgx/v5"
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
		CREATE UNLOGGED TABLE public.scratch (id int PRIMARY KEY);
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
	if !got["public.scratch"].Unlogged || got["public.scratch"].Replicable() || got["public.orders"].Unlogged {
		t.Errorf("unlogged: %+v", got["public.scratch"])
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

func mkRoles(t *testing.T, c *pgx.Conn, stmts ...string) {
	t.Helper()
	ctx := context.Background()
	t.Cleanup(func() {
		for _, r := range []string{"rr_member", "rs_member", "plain_r", "seq_noacc", "seq_sel", "stats_r", "nostats_r", "rds_replication", "rds_superuser",
			"own_m", "own_a", "own_b", "own_c", "own_d"} {
			c.Exec(ctx, "DROP OWNED BY "+r)
			c.Exec(ctx, "DROP ROLE IF EXISTS "+r)
		}
	})
	for _, s := range stmts {
		if _, err := c.Exec(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
}

func TestRdsRoles(t *testing.T) {
	itest.Reset(t)
	ctx := context.Background()
	src := itest.Conn(t, itest.SourceDSN)
	mkRoles(t, src,
		"CREATE ROLE rds_replication", "CREATE ROLE rds_superuser",
		"CREATE ROLE rr_member LOGIN IN ROLE rds_replication",
		"CREATE ROLE rs_member LOGIN IN ROLE rds_superuser",
		"CREATE ROLE plain_r LOGIN")
	if ok, err := pg.CanReplicate(ctx, src, "no_such_role"); err != nil || ok {
		t.Errorf("missing role with rds roles present: %v %v", ok, err)
	}
	for role, want := range map[string]bool{"rr_member": true, "rs_member": true, "plain_r": false} {
		if ok, err := pg.CanReplicate(ctx, src, role); err != nil || ok != want {
			t.Errorf("CanReplicate %s: %v %v", role, ok, err)
		}
	}
	for role, want := range map[string]bool{"rr_member": true, "rs_member": true, "plain_r": false} {
		c := itest.Conn(t, itest.SourceDSN)
		if _, err := c.Exec(ctx, "SET ROLE "+role); err != nil {
			t.Fatal(err)
		}
		f, err := pg.Inspect(ctx, c, []string{"public"})
		if err != nil {
			t.Fatal(err)
		}
		if f.Replication != want || f.Superuser {
			t.Errorf("%s: replication=%v superuser=%v", role, f.Replication, f.Superuser)
		}
	}
}

func TestInspectWithoutRdsRoles(t *testing.T) {
	itest.Reset(t)
	src := itest.Conn(t, itest.SourceDSN)
	mkRoles(t, src, "CREATE ROLE plain_r LOGIN")
	if ok, err := pg.CanReplicate(context.Background(), src, "plain_r"); err != nil || ok {
		t.Errorf("plain role without rds roles existing: %v %v", ok, err)
	}
}

func TestSequencePrivilege(t *testing.T) {
	itest.Reset(t)
	ctx := context.Background()
	src := itest.Conn(t, itest.SourceDSN)
	mkRoles(t, src, "CREATE ROLE seq_noacc LOGIN")
	if _, err := src.Exec(ctx, `CREATE SEQUENCE public.fresh_seq;
		CREATE SEQUENCE public.used_seq; SELECT nextval('public.used_seq');
		GRANT USAGE ON SCHEMA public TO seq_noacc`); err != nil {
		t.Fatal(err)
	}
	f, err := pg.Inspect(ctx, src, []string{"public"})
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range f.Sequences {
		if s.Name == "fresh_seq" && s.LastValue != nil {
			t.Errorf("never-used sequence should have nil LastValue: %+v", s)
		}
	}
	c := itest.Conn(t, itest.SourceDSN)
	if _, err := c.Exec(ctx, "SET ROLE seq_noacc"); err != nil {
		t.Fatal(err)
	}
	_, err = pg.Inspect(ctx, c, []string{"public"})
	if err == nil || !strings.Contains(err.Error(), "public.fresh_seq") {
		t.Fatalf("want error naming public.fresh_seq, got %v", err)
	}
}

func TestSequenceSelectOnly(t *testing.T) {
	itest.Reset(t)
	ctx := context.Background()
	src := itest.Conn(t, itest.SourceDSN)
	mkRoles(t, src, "CREATE ROLE seq_sel LOGIN")
	if _, err := src.Exec(ctx, `CREATE SEQUENCE public.used_seq; SELECT nextval('public.used_seq');
		GRANT USAGE ON SCHEMA public TO seq_sel; GRANT SELECT ON SEQUENCE public.used_seq TO seq_sel`); err != nil {
		t.Fatal(err)
	}
	c := itest.Conn(t, itest.SourceDSN)
	if _, err := c.Exec(ctx, "SET ROLE seq_sel"); err != nil {
		t.Fatal(err)
	}
	f, err := pg.Inspect(ctx, c, []string{"public"})
	if err != nil {
		t.Fatal(err)
	}
	if len(f.Sequences) != 1 || f.Sequences[0].LastValue == nil || *f.Sequences[0].LastValue != 1 {
		t.Fatalf("sequences: %+v", f.Sequences)
	}
}

// OwnedBy names the table a sequence belongs to (serial or identity), which
// is what decides whether Prepare copies it.
func TestSequenceOwnedBy(t *testing.T) {
	itest.Reset(t)
	ctx := context.Background()
	src := itest.Conn(t, itest.SourceDSN)
	if _, err := src.Exec(ctx, `CREATE TABLE public.a (id bigserial PRIMARY KEY);
		CREATE TABLE public.b (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY);
		CREATE SEQUENCE public.standalone`); err != nil {
		t.Fatal(err)
	}
	f, err := pg.Inspect(ctx, src, []string{"public"})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, s := range f.Sequences {
		got[s.Qualified()] = s.OwnedBy
	}
	if len(got) != 3 || got["public.a_id_seq"] != "public.a" || got["public.b_id_seq"] != "public.b" || got["public.standalone"] != "" {
		t.Fatalf("%v", got)
	}
}

// Without superuser or pg_read_all_stats, other roles' sessions are hidden
// from the session check; Inspect says whether it could see them.
func TestSeesAllSessions(t *testing.T) {
	itest.Reset(t)
	ctx := context.Background()
	src := itest.Conn(t, itest.SourceDSN)
	mkRoles(t, src, "CREATE ROLE nostats_r LOGIN", "CREATE ROLE stats_r LOGIN IN ROLE pg_read_all_stats",
		"GRANT USAGE ON SCHEMA public TO nostats_r, stats_r")
	for role, want := range map[string]bool{"": true, "nostats_r": false, "stats_r": true} {
		c := itest.Conn(t, itest.SourceDSN)
		if role != "" {
			if _, err := c.Exec(ctx, "SET ROLE "+role); err != nil {
				t.Fatal(err)
			}
		}
		f, err := pg.Inspect(ctx, c, []string{"public"})
		if err != nil {
			t.Fatalf("%s: %v", role, err)
		}
		if f.SeesAllSessions != want {
			t.Errorf("%q: SeesAllSessions %v, want %v", role, f.SeesAllSessions, want)
		}
	}
}

// Prepare names each plan table's owner and grantees on the target, so
// Inspect reports them, and which roles the connecting role can act as.
func TestOwnersAndRoles(t *testing.T) {
	itest.Reset(t)
	ctx := context.Background()
	src := itest.Conn(t, itest.SourceDSN)
	mkRoles(t, src, "CREATE ROLE own_a", "CREATE ROLE own_b", "CREATE ROLE own_c", "CREATE ROLE own_d",
		"CREATE ROLE own_m LOGIN IN ROLE own_a",
		// A member without SET cannot SET ROLE, so cannot act as own_d (16+).
		"GRANT own_d TO own_m WITH SET FALSE")
	if _, err := src.Exec(ctx, `CREATE TABLE public.owned (id bigserial PRIMARY KEY);
		ALTER TABLE public.owned OWNER TO own_a;
		GRANT SELECT ON public.owned TO own_b, PUBLIC;
		GRANT USAGE ON SEQUENCE public.owned_id_seq TO own_b;
		GRANT USAGE ON SCHEMA public TO own_m;
		GRANT CREATE ON SCHEMA public TO own_b;
		GRANT SELECT (id) ON public.owned TO own_c;
		CREATE POLICY p ON public.owned TO own_d USING (true);
		CREATE POLICY q ON public.owned TO PUBLIC USING (true)`); err != nil {
		t.Fatal(err)
	}
	f, err := pg.Inspect(ctx, src, []string{"public", "no_such_schema"})
	if err != nil {
		t.Fatal(err)
	}
	// Column grants (own_c) and policy roles (own_d) are named too.
	if len(f.Tables) != 1 || f.Tables[0].Owner != "own_a" || !slices.Equal(f.Tables[0].Grantees, []string{"own_a", "own_b", "own_c", "own_d"}) {
		t.Errorf("table: %+v", f.Tables)
	}
	if len(f.Sequences) != 1 || f.Sequences[0].Owner != "own_a" || !slices.Equal(f.Sequences[0].Grantees, []string{"own_a", "own_b"}) {
		t.Errorf("sequence: %+v", f.Sequences)
	}
	if f.User != "postgres" || !f.Roles["own_a"] || !f.Roles["own_c"] || !f.Roles["postgres"] {
		t.Errorf("superuser: user %q roles %v", f.User, f.Roles)
	}
	if _, ok := f.Roles["public"]; ok {
		t.Errorf("PUBLIC is not a role: %v", f.Roles)
	}
	// Schemas that exist map to the roles with CREATE on them.
	if pub, ok := f.SchemaCreate["public"]; !ok || !slices.Contains(pub, "own_b") || !slices.Contains(pub, "postgres") ||
		slices.Contains(pub, "own_c") {
		t.Errorf("schema create: %v", f.SchemaCreate)
	}
	if _, ok := f.SchemaCreate["no_such_schema"]; ok {
		t.Errorf("absent schema must be absent: %v", f.SchemaCreate)
	}

	c := itest.Conn(t, itest.SourceDSN)
	if _, err := c.Exec(ctx, "SET ROLE own_m"); err != nil {
		t.Fatal(err)
	}
	if f, err = pg.Inspect(ctx, c, []string{"public"}); err != nil {
		t.Fatal(err)
	}
	if f.User != "own_m" || f.Superuser {
		t.Errorf("member: user %q superuser %v", f.User, f.Superuser)
	}
	for role, want := range map[string]bool{"own_m": true, "own_a": true, "own_b": false, "own_c": false, "own_d": false, "postgres": false} {
		if got, ok := f.Roles[role]; !ok || got != want {
			t.Errorf("member: Roles[%s] = %v (present %v), want %v", role, got, ok, want)
		}
	}
}
