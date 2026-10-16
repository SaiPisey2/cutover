// Package pg holds everything cutover does to a Postgres database: facts,
// schema copy, logical replication, sequences and sampled checksums. Every
// function takes an open connection; callers own credentials and lifetimes.
package pg

import (
	"context"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"
)

// AppName is the application_name of cutover's own sessions.
const AppName = "cutover"

type Table struct {
	Schema      string   `json:"schema"`
	Name        string   `json:"name"`
	Partitioned bool     `json:"partitioned"`
	ReplIdent   string   `json:"repl_ident"` // d default, f full, i index, n nothing
	PKCols      []string `json:"pk_cols"`
}

func (t Table) Qualified() string { return t.Schema + "." + t.Name }
func (t Table) Ident() string     { return pgx.Identifier{t.Schema, t.Name}.Sanitize() }

// Replicable reports whether logical replication can carry updates and
// deletes for t. Release 1 does not replicate partitioned tables.
func (t Table) Replicable() bool {
	if t.Partitioned {
		return false
	}
	switch t.ReplIdent {
	case "d":
		return len(t.PKCols) > 0
	case "f", "i":
		return true
	}
	return false
}

type Sequence struct {
	Schema    string `json:"schema"`
	Name      string `json:"name"`
	LastValue *int64 `json:"last_value"`
	Increment int64  `json:"increment"`
}

func (s Sequence) Qualified() string { return s.Schema + "." + s.Name }

type Extension struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type Session struct {
	Addr  string `json:"addr"`
	Count int    `json:"count"`
}

type Facts struct {
	VersionNum         int         `json:"version_num"`
	WalLevel           string      `json:"wal_level"`
	SizeBytes          int64       `json:"size_bytes"`
	Extensions         []Extension `json:"extensions"`
	Available          []string    `json:"available"`
	Tables             []Table     `json:"tables"`
	Sequences          []Sequence  `json:"sequences"`
	Superuser          bool        `json:"superuser"`
	Replication        bool        `json:"replication"`
	CreateSubscription bool        `json:"create_subscription"`
	// ReplUser is the role in this side's replication_dsn: the role the other
	// side's subscription connects as. Inspect leaves both fields empty; the
	// Discover activity fills them with CanReplicate.
	ReplUser            string    `json:"repl_user,omitempty"`
	ReplUserReplication bool      `json:"repl_user_replication"`
	MaxSlots            int       `json:"max_slots"`
	UsedSlots           int       `json:"used_slots"`
	Sessions            []Session `json:"sessions"`
}

func (f Facts) Major() int { return f.VersionNum / 10000 }

// Inspect gathers the facts readiness needs. It only reads.
func Inspect(ctx context.Context, c *pgx.Conn, schemas []string) (Facts, error) {
	var f Facts
	var vs, slots string
	if err := c.QueryRow(ctx, `SELECT current_setting('server_version_num'), current_setting('wal_level'),
		current_setting('max_replication_slots'), pg_database_size(current_database())`).
		Scan(&vs, &f.WalLevel, &slots, &f.SizeBytes); err != nil {
		return f, fmt.Errorf("settings: %w", err)
	}
	var err error
	if f.VersionNum, err = strconv.Atoi(vs); err != nil {
		return f, fmt.Errorf("server_version_num %q: %w", vs, err)
	}
	if f.MaxSlots, err = strconv.Atoi(slots); err != nil {
		return f, fmt.Errorf("max_replication_slots %q: %w", slots, err)
	}
	if err := c.QueryRow(ctx, "SELECT count(*) FROM pg_replication_slots").Scan(&f.UsedSlots); err != nil {
		return f, fmt.Errorf("slots: %w", err)
	}
	if err := c.QueryRow(ctx, "SELECT rolsuper, rolreplication FROM pg_roles WHERE rolname = current_user").
		Scan(&f.Superuser, &f.Replication); err != nil {
		return f, fmt.Errorf("role: %w", err)
	}
	f.CreateSubscription = f.Superuser
	if !f.Superuser && f.VersionNum >= 160000 {
		if err := c.QueryRow(ctx, "SELECT pg_has_role(current_user, 'pg_create_subscription', 'MEMBER')").
			Scan(&f.CreateSubscription); err != nil {
			return f, fmt.Errorf("pg_create_subscription: %w", err)
		}
	}
	rows, _ := c.Query(ctx, "SELECT extname::text, extversion FROM pg_extension ORDER BY 1")
	if f.Extensions, err = pgx.CollectRows(rows, pgx.RowToStructByPos[Extension]); err != nil {
		return f, fmt.Errorf("extensions: %w", err)
	}
	rows, _ = c.Query(ctx, "SELECT name::text FROM pg_available_extensions ORDER BY 1")
	if f.Available, err = pgx.CollectRows(rows, pgx.RowTo[string]); err != nil {
		return f, fmt.Errorf("available extensions: %w", err)
	}
	if f.Tables, err = Tables(ctx, c, schemas); err != nil {
		return f, err
	}
	if f.Sequences, err = Sequences(ctx, c, schemas); err != nil {
		return f, err
	}
	rows, _ = c.Query(ctx, `SELECT coalesce(host(client_addr), 'local'), count(*)::int FROM pg_stat_activity
		WHERE datname = current_database() AND pid <> pg_backend_pid()
		  AND backend_type = 'client backend' AND application_name <> $1
		GROUP BY 1 ORDER BY 1`, AppName)
	if f.Sessions, err = pgx.CollectRows(rows, pgx.RowToStructByPos[Session]); err != nil {
		return f, fmt.Errorf("sessions: %w", err)
	}
	return f, nil
}

// CanReplicate reports whether role exists and is a superuser or has
// REPLICATION. A missing role is false, not an error.
func CanReplicate(ctx context.Context, c *pgx.Conn, role string) (bool, error) {
	var ok bool
	if err := c.QueryRow(ctx, `SELECT coalesce((SELECT rolsuper OR rolreplication FROM pg_roles WHERE rolname = $1), false)`, role).
		Scan(&ok); err != nil {
		return false, fmt.Errorf("role %s: %w", role, err)
	}
	return ok, nil
}

// Tables lists ordinary and partitioned tables (not partitions) in schemas,
// with primary key columns in index order.
func Tables(ctx context.Context, c *pgx.Conn, schemas []string) ([]Table, error) {
	rows, _ := c.Query(ctx, `
		SELECT n.nspname::text, c.relname::text, c.relkind = 'p', c.relreplident::text,
		       coalesce((SELECT array_agg(a.attname::text ORDER BY array_position(i.indkey::int2[], a.attnum))
		                 FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		                 WHERE i.indrelid = c.oid AND i.indisprimary), '{}')
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind IN ('r', 'p') AND NOT c.relispartition AND n.nspname = ANY($1)
		ORDER BY 1, 2`, schemas)
	out, err := pgx.CollectRows(rows, pgx.RowToStructByPos[Table])
	if err != nil {
		return nil, fmt.Errorf("tables: %w", err)
	}
	return out, nil
}

// Sequences lists sequences in schemas. LastValue is nil until first use.
func Sequences(ctx context.Context, c *pgx.Conn, schemas []string) ([]Sequence, error) {
	rows, _ := c.Query(ctx, `SELECT schemaname::text, sequencename::text, last_value, increment_by
		FROM pg_sequences WHERE schemaname = ANY($1) ORDER BY 1, 2`, schemas)
	out, err := pgx.CollectRows(rows, pgx.RowToStructByPos[Sequence])
	if err != nil {
		return nil, fmt.Errorf("sequences: %w", err)
	}
	return out, nil
}
