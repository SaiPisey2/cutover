// Package ops holds cutover's Temporal activities: the only code that touches
// databases and PgBouncer. Every activity resolves credentials from
// references itself, checks state before acting (so retries are safe), and
// redacts every error it returns.
package ops

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	"github.com/SaiPisey2/cutover/internal/pg"
	"github.com/SaiPisey2/cutover/internal/plan"
	"github.com/SaiPisey2/cutover/internal/redact"
	"github.com/SaiPisey2/cutover/internal/secret"
)

type Side string

const (
	Source Side = "source"
	Target Side = "target"
)

func (s Side) Other() Side {
	if s == Source {
		return Target
	}
	return Source
}

// Link is one direction of replication: a publication on From and a
// subscription (and its slot on From) on To, all three named Name.
type Link struct {
	Name string `json:"name"`
	From Side   `json:"from"`
	To   Side   `json:"to"`
}

func Forward(id string) Link { return Link{Name: pg.ObjectName(id, "fwd"), From: Source, To: Target} }
func Reverse(id string) Link { return Link{Name: pg.ObjectName(id, "rev"), From: Target, To: Source} }

type DiscoverIn struct {
	Plan plan.Plan `json:"plan"`
}

type TablesIn struct {
	Plan   plan.Plan `json:"plan"`
	Tables []string  `json:"tables"`
}

type LinkIn struct {
	Plan     plan.Plan `json:"plan"`
	Link     Link      `json:"link"`
	Tables   []string  `json:"tables"`
	CopyData bool      `json:"copy_data"`
}

type LagIn struct {
	Plan    plan.Plan     `json:"plan"`
	Link    Link          `json:"link"`
	Timeout time.Duration `json:"timeout"`
}

type PoolIn struct {
	Plan    plan.Plan     `json:"plan"`
	Timeout time.Duration `json:"timeout"`
}

type RepointIn struct {
	Plan plan.Plan `json:"plan"`
	To   Side      `json:"to"`
}

type SeqIn struct {
	Plan plan.Plan `json:"plan"`
	From Side      `json:"from"`
	To   Side      `json:"to"`
}

type Progress struct {
	Ready int `json:"ready"`
	Total int `json:"total"`
}

type LagOut struct {
	Lag     time.Duration `json:"lag"`
	Visible bool          `json:"visible"`
}

const (
	ErrTypeTransient = "cutover"
	ErrTypePermanent = "cutover.permanent"
)

type permanent struct{ msg string }

func (p *permanent) Error() string { return p.msg }

// permanentf marks a failure that retrying cannot fix.
func permanentf(format string, a ...any) error { return &permanent{msg: fmt.Sprintf(format, a...)} }

// fail turns any error into a Temporal application error with a redacted
// message and no cause chain: Temporal records the chain in history, and the
// inner errors may still hold a password. An application error built further
// down is rebuilt the same way, keeping its type and retryability. A
// cancellation stays context.Canceled, so Temporal reports the activity
// cancelled rather than failed. Errors isPermanent recognises become
// non-retryable ErrTypePermanent; everything else is ErrTypeTransient.
func fail(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	msg := redact.DSN(err.Error())
	var app *temporal.ApplicationError
	if errors.As(err, &app) {
		if app.NonRetryable() {
			return temporal.NewNonRetryableApplicationError(msg, app.Type(), nil)
		}
		return temporal.NewApplicationError(msg, app.Type())
	}
	if isPermanent(err) {
		return temporal.NewNonRetryableApplicationError(msg, ErrTypePermanent, nil)
	}
	return temporal.NewApplicationError(msg, ErrTypeTransient)
}

// permanentCodes are SQLSTATEs a retry cannot fix: undefined table,
// insufficient privilege, undefined object.
var permanentCodes = map[string]bool{"42P01": true, "42501": true, "42704": true}

// isPermanent reports whether retrying err is pointless. It must be called
// on the original error: the SQLSTATE is lost once the error is flattened.
func isPermanent(err error) bool {
	var p *permanent
	if errors.As(err, &p) {
		return true
	}
	var pe *pgconn.PgError
	if errors.As(err, &pe) && permanentCodes[pe.Code] {
		return true
	}
	// pg_dump and psql refuse a server newer than themselves; covers
	// "aborting because of server version mismatch".
	return strings.Contains(err.Error(), "server version mismatch")
}

func pgFor(p plan.Plan, s Side) plan.Postgres {
	if s == Source {
		return p.Source.Postgres
	}
	return p.Target.Postgres
}

// connect opens a fresh connection per activity call. A connection is never
// reused after a context timeout (pgx closes it).
func connect(ctx context.Context, ref secret.Ref) (*pgx.Conn, error) {
	dsn, err := ref.Resolve()
	if err != nil {
		return nil, err
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn from %s: %s", ref, redact.DSN(err.Error()))
	}
	cfg.RuntimeParams["application_name"] = pg.AppName
	c, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect (%s): %w", ref, err)
	}
	return c, nil
}

// replRole fills f.ReplUser and f.ReplUserReplication from the role named in
// a side's replication_dsn, checked over c (that side's dsn connection). The
// replication DSN's host is how the other database reaches this one, which
// the worker may not be able to reach, so it is parsed, never dialled.
func replRole(ctx context.Context, c *pgx.Conn, ref secret.Ref, f *pg.Facts) error {
	dsn, err := ref.Resolve()
	if err != nil {
		return err
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("parse replication dsn from %s: %s", ref, redact.DSN(err.Error()))
	}
	f.ReplUser = cfg.User
	f.ReplUserReplication, err = pg.CanReplicate(ctx, c, cfg.User)
	return err
}

// heartbeat keeps a heartbeat going while fn runs, so a dead worker is
// noticed within the activity's HeartbeatTimeout rather than at StartToClose.
func heartbeat(ctx context.Context, fn func() error) error {
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(500 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				sdkactivity.RecordHeartbeat(ctx)
			}
		}
	}()
	return fn()
}
