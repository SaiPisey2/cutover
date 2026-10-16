// Package readiness decides what can move with zero downtime and what
// cannot, and why. It is pure: facts in, findings out.
package readiness

import (
	"fmt"
	"net/netip"
	"slices"
	"strings"

	"github.com/SaiPisey2/cutover/internal/pg"
	"github.com/SaiPisey2/cutover/internal/pgbouncer"
	"github.com/SaiPisey2/cutover/internal/plan"
)

type Severity string

const (
	Blocker Severity = "blocker"
	Warning Severity = "warning"
	Info    Severity = "info"
)

type Finding struct {
	Severity Severity `json:"severity"`
	Code     string   `json:"code"`
	Subject  string   `json:"subject,omitempty"`
	Message  string   `json:"message"`
}

type Report struct {
	Tables   []string  `json:"tables"`
	Findings []Finding `json:"findings"`
}

func (r Report) Count(s Severity) int {
	n := 0
	for _, f := range r.Findings {
		if f.Severity == s {
			n++
		}
	}
	return n
}

func (r Report) Blocked() bool { return r.Count(Blocker) > 0 }

type Input struct {
	Plan    plan.Plan          `json:"plan"`
	Source  pg.Facts           `json:"source"`
	Target  pg.Facts           `json:"target"`
	Pool    pgbouncer.Database `json:"pool"`
	PoolErr string             `json:"pool_err,omitempty"`
}

func Assess(in Input) Report {
	var r Report
	add := func(s Severity, code, subject, format string, a ...any) {
		r.Findings = append(r.Findings, Finding{Severity: s, Code: code, Subject: subject, Message: fmt.Sprintf(format, a...)})
	}
	src, tgt := in.Source, in.Target

	if src.VersionNum < 120000 {
		add(Blocker, "pg-version", "source", "source is Postgres %d; release 1 needs 12 or later", src.Major())
	}
	if tgt.Major() < src.Major() {
		add(Blocker, "pg-version", "target", "target is Postgres %d, older than the source (%d)", tgt.Major(), src.Major())
	}
	for _, sf := range []struct {
		side string
		f    pg.Facts
	}{{"source", src}, {"target", tgt}} {
		side, f := sf.side, sf.f
		if f.WalLevel != "logical" {
			add(Blocker, "wal-level", side, "%s has wal_level=%s; set wal_level=logical and restart (the target needs it for reverse replication)", side, f.WalLevel)
		}
		if !f.CreateSubscription {
			add(Blocker, "role", side, "the %s role cannot create subscriptions (needs superuser, or pg_create_subscription on 16+)", side)
		}
		if f.MaxSlots-f.UsedSlots < 1 {
			add(Blocker, "slots", side, "%s has no free replication slot (%d of %d used)", side, f.UsedSlots, f.MaxSlots)
		}
		// Forward replication connects to the source, and the reverse link
		// to the target, as the role in that side's replication_dsn.
		if !f.ReplUserReplication {
			add(Blocker, "role", side, "the %s replication_dsn role %q needs REPLICATION (or superuser) on the %s", side, f.ReplUser, side)
		}
	}

	byName := map[string]pg.Table{}
	for _, t := range src.Tables {
		byName[t.Qualified()] = t
	}
	want := in.Plan.Database.Tables
	if len(want) == 0 {
		for _, t := range src.Tables {
			want = append(want, t.Qualified())
		}
	}
	for _, name := range want {
		t, ok := byName[name]
		switch {
		case !ok:
			add(Blocker, "table-missing", name, "%s is in the plan but not on the source", name)
			continue
		case t.Partitioned:
			add(Blocker, "partitioned", name, "%s is partitioned; release 1 does not replicate partitioned tables", name)
		case t.Unlogged:
			add(Blocker, "unlogged-table", name, "%s is UNLOGGED, so its writes never reach the WAL and cannot replicate; run ALTER TABLE %s SET LOGGED first", name, name)
		case !t.Replicable():
			add(Blocker, "no-primary-key", name, "%s has no primary key or replica identity; add one, or set REPLICA IDENTITY FULL", name)
		}
		if !plan.Ident.MatchString(t.Schema) || !plan.Ident.MatchString(t.Name) {
			add(Blocker, "identifier", name, "%s is not a plain lowercase identifier (%s); release 1 does not quote names", name, plan.Ident)
		}
		r.Tables = append(r.Tables, name)
	}
	slices.Sort(r.Tables)
	for _, t := range tgt.Tables {
		if slices.Contains(r.Tables, t.Qualified()) {
			add(Blocker, "target-not-empty", t.Qualified(), "%s already exists on the target; cutover creates the target schema itself", t.Qualified())
		}
	}

	installed := map[string]bool{}
	for _, e := range tgt.Extensions {
		installed[e.Name] = true
	}
	for _, e := range src.Extensions {
		if !installed[e.Name] {
			add(Blocker, "extension-missing", e.Name, "extension %s is on the source but not the target; run CREATE EXTENSION %s on the target first", e.Name, e.Name)
		}
	}
	for _, s := range src.Sequences {
		if s.Increment < 0 {
			add(Blocker, "sequence", s.Qualified(), "%s counts down; release 1 syncs ascending sequences only", s.Qualified())
		}
		if !plan.Ident.MatchString(s.Schema) || !plan.Ident.MatchString(s.Name) {
			add(Blocker, "identifier", s.Qualified(), "%s is not a plain lowercase identifier (%s); release 1 does not quote names", s.Qualified(), plan.Ident)
		}
	}

	switch {
	case in.PoolErr != "":
		add(Blocker, "pgbouncer", in.Plan.Database.Name, "cannot read PgBouncer: %s", in.PoolErr)
	case in.Pool.Host != pgbouncer.HostOf(in.Plan.Source.Postgres.Route):
		add(Blocker, "pgbouncer-route", in.Plan.Database.Name, "PgBouncer routes %s to %q, not the source route %q", in.Plan.Database.Name, in.Pool.Host, pgbouncer.HostOf(in.Plan.Source.Postgres.Route))
	case in.Pool.Paused:
		add(Warning, "pgbouncer-paused", in.Plan.Database.Name, "PgBouncer has %s paused right now", in.Plan.Database.Name)
	}

	prefixes := parsePrefixes(in.Plan.Database.PgBouncer.Addresses)
	var listed []string
	for _, s := range src.Sessions {
		listed = append(listed, fmt.Sprintf("%s (%d)", s.Addr, s.Count))
		if len(prefixes) > 0 && s.Addr != "local" && !within(prefixes, s.Addr) {
			add(Blocker, "direct-clients", s.Addr, "%d sessions from %s reach the source without PgBouncer; their writes would be lost at the switch", s.Count, s.Addr)
		}
	}
	if len(listed) > 0 {
		add(Info, "sessions", "source", "client sessions on the source: %s", strings.Join(listed, ", "))
	}
	add(Info, "size", "source", "%d tables, %d sequences, %d MiB", len(r.Tables), len(src.Sequences), src.SizeBytes>>20)
	add(Info, "ddl-freeze", "", "schema changes are frozen until Finish: logical replication does not carry DDL")
	// Map iteration above varies; sort so the console and evidence digests are stable.
	rank := map[Severity]int{Blocker: 0, Warning: 1, Info: 2}
	slices.SortStableFunc(r.Findings, func(a, b Finding) int {
		if d := rank[a.Severity] - rank[b.Severity]; d != 0 {
			return d
		}
		if c := strings.Compare(a.Code, b.Code); c != 0 {
			return c
		}
		if c := strings.Compare(a.Subject, b.Subject); c != 0 {
			return c
		}
		return strings.Compare(a.Message, b.Message)
	})
	return r
}

func parsePrefixes(addrs []string) []netip.Prefix {
	var out []netip.Prefix
	for _, a := range addrs {
		if p, err := netip.ParsePrefix(a); err == nil {
			out = append(out, p)
		} else if ip, err := netip.ParseAddr(a); err == nil {
			out = append(out, netip.PrefixFrom(ip, ip.BitLen()))
		}
	}
	return out
}

func within(prefixes []netip.Prefix, addr string) bool {
	ip, err := netip.ParseAddr(addr)
	if err != nil {
		return false
	}
	for _, p := range prefixes {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}
