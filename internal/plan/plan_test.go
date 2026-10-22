package plan

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

const minimal = `
version: 1
name: shop
source:
  postgres: {dsn: env:SRC, replication_dsn: env:SRC_REPL, route: host=a port=5432 dbname=shop}
target:
  postgres: {dsn: env:TGT, replication_dsn: env:TGT_REPL, route: host=b port=5432 dbname=shop}
database:
  name: shop
  pgbouncer: {admin_dsn: env:PGB, routes_file: /etc/pgbouncer/routes.ini}
`

func TestExampleLoads(t *testing.T) {
	p, err := Load("../../examples/plan.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if p.Name != "shop" || p.Thresholds.Switch.MaxPause.D() != 10*time.Second {
		t.Fatalf("unexpected plan: %+v", p)
	}
	if len(p.Services) == 0 || p.FrontDoor == nil {
		t.Fatalf("the example plan shows services and the front door: %+v", p)
	}
}

func TestDefaults(t *testing.T) {
	p, err := Parse([]byte(minimal))
	if err != nil {
		t.Fatal(err)
	}
	th := p.Thresholds
	for name, got := range map[string]time.Duration{
		"max_lag": th.Replicate.MaxLag.D(), "stable_for": th.Replicate.StableFor.D(),
		"sample_every": th.Replicate.SampleEvery.D(), "max_pause": th.Switch.MaxPause.D(),
		"watch": th.Finish.Watch.D(),
	} {
		want := map[string]time.Duration{"max_lag": time.Second, "stable_for": 5 * time.Minute,
			"sample_every": 10 * time.Second, "max_pause": 10 * time.Second, "watch": 24 * time.Hour}[name]
		if got != want {
			t.Errorf("%s = %s, want %s", name, got, want)
		}
	}
	if th.Switch.SequenceMargin != 1000 || th.Rehearse.SampleRows != 1000 {
		t.Errorf("margin %d rows %d", th.Switch.SequenceMargin, th.Rehearse.SampleRows)
	}
	if !reflect.DeepEqual(p.Database.Schemas, []string{"public"}) {
		t.Errorf("schemas %v", p.Database.Schemas)
	}
	if p.Database.Tools.PgDump != "pg_dump" || p.Database.Tools.Psql != "psql" {
		t.Errorf("tools %+v", p.Database.Tools)
	}
}

func TestRejects(t *testing.T) {
	for _, tc := range []struct{ name, edit, want string }{
		{"unknown key", "database:\n  name: shop\n  colour: red\n", "colour"},
		{"bad version", "version: 2\n", "version"},
		{"bad name", "name: Shop!\n", "name"},
		{"password in route", "source:\n  postgres: {dsn: env:SRC, replication_dsn: env:SRC_REPL, route: host=a password=x dbname=shop}\n", "must not contain a password"},
		{"bad ref", "target:\n  postgres: {dsn: postgres://u:p@h/db, replication_dsn: env:T, route: host=b dbname=shop}\n", "target.postgres.dsn"},
		{"table outside schemas", "database:\n  name: shop\n  tables: [billing.invoices]\n  pgbouncer: {admin_dsn: env:PGB, routes_file: /r.ini}\n", "not in database.schemas"},
		{"reserved schema", "database:\n  name: shop\n  schemas: [_cutover]\n  pgbouncer: {admin_dsn: env:PGB, routes_file: /r.ini}\n", "reserved"},
		{"relative routes file", "database:\n  name: shop\n  pgbouncer: {admin_dsn: env:PGB, routes_file: routes.ini}\n", "absolute"},
		{"bad address", "database:\n  name: shop\n  pgbouncer: {admin_dsn: env:PGB, routes_file: /r.ini, addresses: [not-an-ip]}\n", "addresses"},
		{"bad duration", "thresholds:\n  switch: {max_pause: soon}\n", `invalid duration "soon"`},
		{"pause too long", "thresholds:\n  switch: {max_pause: 10m}\n", "max_pause"},
		{"stable shorter than lag", "thresholds:\n  replicate: {max_lag: 10s, stable_for: 5s}\n", "stable_for"},
		{"duplicate schema", "database:\n  name: shop\n  schemas: [public, public]\n  pgbouncer: {admin_dsn: env:PGB, routes_file: /r.ini}\n", `duplicate entry "public"`},
		{"duplicate table", "database:\n  name: shop\n  tables: [public.a, public.a]\n  pgbouncer: {admin_dsn: env:PGB, routes_file: /r.ini}\n", `duplicate entry "public.a"`},
		{"uri route", "source:\n  postgres: {dsn: env:SRC, replication_dsn: env:SRC_REPL, route: \"postgres://u@h/shop host=a\"}\n", "not a URI"},
		{"same database both sides", "target:\n  postgres: {dsn: env:TGT, replication_dsn: env:TGT_REPL, route: host=a dbname=shop}\n", "same database"},
		{"same database, case and defaults", "target:\n  postgres: {dsn: env:TGT, replication_dsn: env:TGT_REPL, route: \"port=5432 host='A'\"}\n", "same database"},
		{"unparseable route", "target:\n  postgres: {dsn: env:TGT, replication_dsn: env:TGT_REPL, route: \"host='b\"}\n", "target.postgres.route"},
		{"route without host", "target:\n  postgres: {dsn: env:TGT, replication_dsn: env:TGT_REPL, route: port=5432 dbname=shop}\n", "target.postgres.route"},
		{"two documents", "---\nversion: 1\n", "one YAML document"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(override(minimal, tc.edit)))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want error containing %q", err, tc.want)
			}
		})
	}
}

func TestBadRefDoesNotLeakDSN(t *testing.T) {
	in := override(minimal, "target:\n  postgres: {dsn: postgres://u:p@h/db, replication_dsn: env:T, route: host=b dbname=shop}\n")
	_, err := Parse([]byte(in))
	if err == nil || !strings.Contains(err.Error(), "target.postgres.dsn") {
		t.Fatalf("want a dsn error, got %v", err)
	}
	for _, leak := range []string{"u:p@h", "postgres://"} {
		if strings.Contains(err.Error(), leak) {
			t.Errorf("error leaks %q: %v", leak, err)
		}
	}
}

func TestEmptyFile(t *testing.T) {
	for _, in := range []string{"", "# only a comment\n"} {
		_, err := Parse([]byte(in))
		if err == nil || !strings.Contains(err.Error(), "plan file is empty") {
			t.Errorf("%q: got %v", in, err)
		}
	}
}

func TestReportsAllProblemsAtOnce(t *testing.T) {
	_, err := Parse([]byte("version: 3\nname: X\n"))
	if err == nil {
		t.Fatal("want errors")
	}
	for _, want := range []string{"version", "name", "source.postgres.dsn", "database.name"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("missing %q in %v", want, err)
		}
	}
}

func TestJSONRoundTrip(t *testing.T) {
	p, err := Parse([]byte(minimal))
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"max_pause":"10s"`) {
		t.Fatalf("durations should marshal as strings: %s", b)
	}
	var q Plan
	if err := json.Unmarshal(b, &q); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(p, q) {
		t.Fatalf("round trip changed the plan:\n%+v\n%+v", p, q)
	}
}

// A plan marshalled to YAML parses back to the same plan: the console's
// API takes plans as YAML.
func TestYAMLRoundTrip(t *testing.T) {
	example, err := Load("../../examples/plan.yaml")
	if err != nil {
		t.Fatal(err)
	}
	small, err := Parse([]byte(minimal))
	if err != nil {
		t.Fatal(err)
	}
	for name, p := range map[string]Plan{"minimal": small, "example": example} {
		b, err := yaml.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(b), "max_pause: 10s") {
			t.Fatalf("%s: durations should marshal as strings:\n%s", name, b)
		}
		q, err := Parse(b)
		if err != nil {
			t.Fatalf("%s: marshalled plan does not parse: %v\n%s", name, err, b)
		}
		if !reflect.DeepEqual(p, q) {
			t.Fatalf("%s: round trip changed the plan:\n%+v\n%+v", name, p, q)
		}
	}
}

// override appends edit's top-level keys to base, replacing base's keys of the same name.
func override(base, edit string) string {
	if strings.HasPrefix(edit, "---") {
		return base + edit
	}
	keys := map[string]bool{}
	for _, line := range strings.Split(edit, "\n") {
		if k, _, ok := strings.Cut(line, ":"); ok && k != "" && !strings.HasPrefix(k, " ") {
			keys[k] = true
		}
	}
	var out []string
	skipping := false
	for _, line := range strings.Split(base, "\n") {
		if k, _, ok := strings.Cut(line, ":"); ok && !strings.HasPrefix(line, " ") {
			skipping = keys[k]
		}
		if !skipping {
			out = append(out, line)
		}
	}
	return strings.Join(out, "\n") + "\n" + edit
}
