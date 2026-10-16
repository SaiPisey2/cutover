// Package plan loads and validates plan.yaml: what moves, where to, and the
// limits that stop a migration. A plan holds credential references, never
// credentials, so it can travel through workflow history.
package plan

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/SaiPisey2/cutover/internal/route"
	"github.com/SaiPisey2/cutover/internal/secret"
)

type Plan struct {
	Version    int        `yaml:"version" json:"version"`
	Name       string     `yaml:"name" json:"name"`
	Source     Env        `yaml:"source" json:"source"`
	Target     Env        `yaml:"target" json:"target"`
	Database   Database   `yaml:"database" json:"database"`
	Thresholds Thresholds `yaml:"thresholds" json:"thresholds"`
}

type Env struct {
	Postgres Postgres `yaml:"postgres" json:"postgres"`
}

type Postgres struct {
	DSN            secret.Ref `yaml:"dsn" json:"dsn"`
	ReplicationDSN secret.Ref `yaml:"replication_dsn" json:"replication_dsn"`
	Route          string     `yaml:"route" json:"route"`
}

type Database struct {
	Name      string    `yaml:"name" json:"name"`
	Schemas   []string  `yaml:"schemas" json:"schemas"`
	Tables    []string  `yaml:"tables" json:"tables,omitempty"`
	PgBouncer PgBouncer `yaml:"pgbouncer" json:"pgbouncer"`
	Tools     Tools     `yaml:"tools" json:"tools"`
}

type PgBouncer struct {
	AdminDSN   secret.Ref `yaml:"admin_dsn" json:"admin_dsn"`
	RoutesFile string     `yaml:"routes_file" json:"routes_file"`
	Addresses  []string   `yaml:"addresses" json:"addresses,omitempty"`
}

type Tools struct {
	PgDump string `yaml:"pg_dump" json:"pg_dump"`
	Psql   string `yaml:"psql" json:"psql"`
}

type Thresholds struct {
	Replicate Replicate `yaml:"replicate" json:"replicate"`
	Rehearse  Rehearse  `yaml:"rehearse" json:"rehearse"`
	Switch    Switch    `yaml:"switch" json:"switch"`
	Finish    Finish    `yaml:"finish" json:"finish"`
}

type Replicate struct {
	MaxLag      Duration `yaml:"max_lag" json:"max_lag"`
	StableFor   Duration `yaml:"stable_for" json:"stable_for"`
	SampleEvery Duration `yaml:"sample_every" json:"sample_every"`
}

type Rehearse struct {
	SampleRows int `yaml:"sample_rows" json:"sample_rows"`
}

type Switch struct {
	MaxPause       Duration `yaml:"max_pause" json:"max_pause"`
	SequenceMargin int64    `yaml:"sequence_margin" json:"sequence_margin"`
}

type Finish struct {
	Watch Duration `yaml:"watch" json:"watch"`
}

var (
	// NamePattern is the shape of plan and migration names.
	NamePattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,39}$`)
	// Ident is the shape of every schema, table and database name in release 1.
	Ident = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)
)

// ReservedSchema holds cutover's own sentinel table on both sides.
const ReservedSchema = "_cutover"

// Load reads, defaults and validates a plan file.
func Load(path string) (Plan, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Plan{}, err
	}
	return Parse(b)
}

// Parse decodes one YAML document strictly (unknown keys are errors),
// applies defaults and validates.
func Parse(data []byte) (Plan, error) {
	var p Plan
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&p); err != nil {
		if errors.Is(err, io.EOF) {
			return Plan{}, errors.New("plan: plan file is empty")
		}
		return Plan{}, fmt.Errorf("plan: %w", err)
	}
	var extra yaml.Node
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return Plan{}, errors.New("plan: want exactly one YAML document")
	}
	ApplyDefaults(&p)
	if err := p.Validate(); err != nil {
		return Plan{}, err
	}
	return p, nil
}

// ApplyDefaults fills every unset threshold and tool with the spec's default.
func ApplyDefaults(p *Plan) {
	d := &p.Database
	if len(d.Schemas) == 0 {
		d.Schemas = []string{"public"}
	}
	if d.Tools.PgDump == "" {
		d.Tools.PgDump = "pg_dump"
	}
	if d.Tools.Psql == "" {
		d.Tools.Psql = "psql"
	}
	th := &p.Thresholds
	setDur(&th.Replicate.MaxLag, time.Second)
	setDur(&th.Replicate.StableFor, 5*time.Minute)
	setDur(&th.Replicate.SampleEvery, 10*time.Second)
	setDur(&th.Switch.MaxPause, 10*time.Second)
	setDur(&th.Finish.Watch, 24*time.Hour)
	if th.Switch.SequenceMargin == 0 {
		th.Switch.SequenceMargin = 1000
	}
	if th.Rehearse.SampleRows == 0 {
		th.Rehearse.SampleRows = 1000
	}
}

func setDur(d *Duration, v time.Duration) {
	if *d == 0 {
		*d = Duration(v)
	}
}

// Validate reports every problem at once.
func (p Plan) Validate() error {
	var errs []error
	bad := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }

	if p.Version != 1 {
		bad("version: must be 1")
	}
	if !NamePattern.MatchString(p.Name) {
		bad("name: must match %s", NamePattern)
	}
	targets := map[string]route.Target{}
	for side, pg := range map[string]Postgres{"source": p.Source.Postgres, "target": p.Target.Postgres} {
		if err := pg.DSN.Validate(); err != nil {
			bad("%s.postgres.dsn: %v", side, err)
		}
		if err := pg.ReplicationDSN.Validate(); err != nil {
			bad("%s.postgres.replication_dsn: %v", side, err)
		}
		switch {
		case strings.TrimSpace(pg.Route) == "":
			bad("%s.postgres.route: required", side)
		case strings.Contains(strings.ToLower(pg.Route), "password"):
			bad("%s.postgres.route: must not contain a password (PgBouncer authenticates on its own)", side)
		case strings.Contains(pg.Route, "://"):
			bad("%s.postgres.route: must be key=value pairs, not a URI", side)
		default:
			// The [databases] entry is named after database.name, which is
			// PgBouncer's dbname when the route sets none.
			t, err := route.Resolve(pg.Route, p.Database.Name)
			if err != nil {
				bad("%s.postgres.route: %s", side, strings.TrimPrefix(err.Error(), "route: "))
			} else {
				targets[side] = t
			}
		}
	}
	if s, ok := targets["source"]; ok {
		if t, ok := targets["target"]; ok && route.Same(s, t) {
			bad("source and target routes point at the same database (%s:%d/%s)", t.Host, t.Port, t.DBName)
		}
	}
	d := p.Database
	if !Ident.MatchString(d.Name) {
		bad("database.name: must match %s", Ident)
	}
	seen := map[string]bool{}
	for _, s := range d.Schemas {
		if seen[s] {
			bad("database.schemas: duplicate entry %q", s)
		}
		seen[s] = true
		if s == ReservedSchema {
			bad("database.schemas: %s is reserved for cutover", s)
		} else if !Ident.MatchString(s) {
			bad("database.schemas: %q must match %s", s, Ident)
		}
	}
	seenT := map[string]bool{}
	for _, t := range d.Tables {
		if seenT[t] {
			bad("database.tables: duplicate entry %q", t)
		}
		seenT[t] = true
		s, n, ok := strings.Cut(t, ".")
		if !ok || !Ident.MatchString(s) || !Ident.MatchString(n) {
			bad("database.tables: %q must be schema.table", t)
		} else if !slices.Contains(d.Schemas, s) {
			bad("database.tables: %q is not in database.schemas", t)
		}
	}
	if err := d.PgBouncer.AdminDSN.Validate(); err != nil {
		bad("database.pgbouncer.admin_dsn: %v", err)
	}
	if !filepath.IsAbs(d.PgBouncer.RoutesFile) {
		bad("database.pgbouncer.routes_file: must be an absolute path")
	}
	for _, a := range d.PgBouncer.Addresses {
		if _, err := netip.ParsePrefix(a); err != nil {
			if _, err := netip.ParseAddr(a); err != nil {
				bad("database.pgbouncer.addresses: %q is not an IP or CIDR", a)
			}
		}
	}
	th := p.Thresholds
	if th.Replicate.MaxLag <= 0 || th.Replicate.SampleEvery <= 0 {
		bad("thresholds.replicate: max_lag and sample_every must be positive")
	}
	if th.Replicate.StableFor < th.Replicate.MaxLag {
		bad("thresholds.replicate.stable_for: must be at least max_lag")
	}
	if mp := th.Switch.MaxPause.D(); mp < time.Second || mp > 5*time.Minute {
		bad("thresholds.switch.max_pause: must be between 1s and 5m")
	}
	if th.Switch.SequenceMargin < 0 {
		bad("thresholds.switch.sequence_margin: must not be negative")
	}
	if th.Rehearse.SampleRows < 1 {
		bad("thresholds.rehearse.sample_rows: must be at least 1")
	}
	if th.Finish.Watch <= 0 {
		bad("thresholds.finish.watch: must be positive")
	}
	slices.SortFunc(errs, func(a, b error) int { return strings.Compare(a.Error(), b.Error()) })
	return errors.Join(errs...)
}
