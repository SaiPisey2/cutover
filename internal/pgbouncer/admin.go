// Package pgbouncer drives PgBouncer's admin console: the database front
// door both app tiers connect through. cutover pauses it, repoints it at the
// other database and resumes it; clients see a short wait, not an error.
package pgbouncer

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrPauseTimeout = errors.New("pgbouncer: pause did not complete in time")
	ErrNoDatabase   = errors.New("pgbouncer: no such database")
)

// Admin is a connection to the admin console (database "pgbouncer"). It
// speaks the simple query protocol only, so it uses pgconn directly.
type Admin struct{ conn *pgconn.PgConn }

func Dial(ctx context.Context, dsn string) (*Admin, error) {
	c, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgbouncer admin: %w", err)
	}
	return &Admin{conn: c}, nil
}

func (a *Admin) Close(ctx context.Context) { _ = a.conn.Close(ctx) }

type Database struct {
	Name   string `json:"name"`
	Host   string `json:"host"`
	Port   int    `json:"port"`
	DB     string `json:"db"`
	Paused bool   `json:"paused"`
}

func (a *Admin) query(ctx context.Context, sql string) ([]map[string]string, error) {
	results, err := a.conn.Exec(ctx, sql).ReadAll()
	if err != nil {
		return nil, err
	}
	var out []map[string]string
	for _, r := range results {
		if r.Err != nil {
			return nil, r.Err
		}
		for _, row := range r.Rows {
			m := map[string]string{}
			for i, fd := range r.FieldDescriptions {
				if row[i] != nil {
					m[fd.Name] = string(row[i])
				}
			}
			out = append(out, m)
		}
	}
	return out, nil
}

// Databases runs SHOW DATABASES. Columns are read by name, so newer
// PgBouncer versions with extra columns still parse.
func (a *Admin) Databases(ctx context.Context) ([]Database, error) {
	rows, err := a.query(ctx, "SHOW DATABASES")
	if err != nil {
		return nil, fmt.Errorf("show databases: %w", err)
	}
	out := make([]Database, 0, len(rows))
	for _, m := range rows {
		port, _ := strconv.Atoi(m["port"])
		out = append(out, Database{Name: m["name"], Host: m["host"], Port: port, DB: m["database"], Paused: m["paused"] == "1"})
	}
	return out, nil
}

func (a *Admin) Database(ctx context.Context, name string) (Database, error) {
	dbs, err := a.Databases(ctx)
	if err != nil {
		return Database{}, err
	}
	for _, d := range dbs {
		if d.Name == name {
			return d, nil
		}
	}
	return Database{}, fmt.Errorf("%w: %s", ErrNoDatabase, name)
}

// Pause waits for in-flight transactions on name to finish and holds new
// ones. In transaction pooling mode that is the pause cutover needs. If ctx
// ends first it returns ErrPauseTimeout; the admin connection is then
// unusable and the caller must Resume on a new one.
//
// It always sends PAUSE, even when SHOW DATABASES already says paused=1:
// PgBouncer sets that column the moment PAUSE arrives, while transactions
// are still draining, so a retried pause that trusted it would report
// success early. A repeated per-database PAUSE waits for the drain again.
// If PgBouncer refuses the repeat instead ("already suspended/paused") (global pause mode, or a
// version that refuses the repeat), Pause polls SHOW POOLS until no server
// connection of name is active.
func (a *Admin) Pause(ctx context.Context, name string) error {
	if _, err := a.Database(ctx, name); err != nil {
		return err
	}
	if _, err := a.query(ctx, "PAUSE "+name); err != nil {
		if ctx.Err() != nil {
			return ErrPauseTimeout
		}
		if !alreadyPaused(err) {
			return fmt.Errorf("pause %s: %w", name, err)
		}
		return a.waitDrained(ctx, name)
	}
	return nil
}

// alreadyPaused matches PgBouncer's refusal of a repeated pause. 1.24
// answers "already suspended/paused" when a global PAUSE is in force.
func alreadyPaused(err error) bool {
	m := strings.ToLower(err.Error())
	return strings.Contains(m, "already paused") || strings.Contains(m, "already suspended")
}

// waitDrained polls SHOW POOLS until name has no active server connection.
func (a *Admin) waitDrained(ctx context.Context, name string) error {
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		rows, err := a.query(ctx, "SHOW POOLS")
		if err != nil {
			if ctx.Err() != nil {
				return ErrPauseTimeout
			}
			return fmt.Errorf("pause %s: show pools: %w", name, err)
		}
		active := 0
		for _, m := range rows {
			if m["database"] == name {
				n, _ := strconv.Atoi(m["sv_active"])
				active += n
			}
		}
		if active == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ErrPauseTimeout
		case <-tick.C:
		}
	}
}

// Resume always sends RESUME, because a pause that timed out leaves the
// database pausing: paused=1 in SHOW DATABASES while transactions still
// drain. Not being paused is fine.
func (a *Admin) Resume(ctx context.Context, name string) error {
	if _, err := a.query(ctx, "RESUME "+name); err != nil &&
		!strings.Contains(strings.ToLower(err.Error()), "not paused") {
		return fmt.Errorf("resume %s: %w", name, err)
	}
	return nil
}

func (a *Admin) Reload(ctx context.Context) error {
	if _, err := a.query(ctx, "RELOAD"); err != nil {
		return fmt.Errorf("reload: %w", err)
	}
	return nil
}

// Reconnect closes name's server connections as they are released, so the
// next transaction opens one to the current host.
func (a *Admin) Reconnect(ctx context.Context, name string) error {
	if _, err := a.query(ctx, "RECONNECT "+name); err != nil {
		return fmt.Errorf("reconnect %s: %w", name, err)
	}
	return nil
}
