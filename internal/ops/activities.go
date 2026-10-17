package ops

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/SaiPisey2/cutover/internal/pg"
	"github.com/SaiPisey2/cutover/internal/pgbouncer"
	"github.com/SaiPisey2/cutover/internal/plan"
	"github.com/SaiPisey2/cutover/internal/readiness"
	"github.com/SaiPisey2/cutover/internal/redact"
	"github.com/SaiPisey2/cutover/internal/sshx"
	"github.com/SaiPisey2/cutover/internal/svc"
)

// Activities resolves what it needs from each call's input. The two
// fields are test seams; nil means the real thing.
type Activities struct {
	driver     func(plan.Plan, plan.Service) (svc.Driver, error)
	lookupHost func(ctx context.Context, host string) ([]string, error)
}

func New() *Activities { return &Activities{} }

func (a *Activities) driverFor(p plan.Plan, s plan.Service) (svc.Driver, error) {
	if a.driver != nil {
		return a.driver(p, s)
	}
	return svc.For(p, s)
}

func (a *Activities) lookup(ctx context.Context, host string) ([]string, error) {
	if a.lookupHost != nil {
		return a.lookupHost(ctx, host)
	}
	return net.DefaultResolver.LookupHost(ctx, host)
}

func both(ctx context.Context, in TablesIn) (src, tgt *pgx.Conn, closeAll func(), err error) {
	if src, err = connect(ctx, in.Plan.Source.Postgres.DSN); err != nil {
		return nil, nil, nil, fmt.Errorf("source: %w", err)
	}
	if tgt, err = connect(ctx, in.Plan.Target.Postgres.DSN); err != nil {
		src.Close(context.Background())
		return nil, nil, nil, fmt.Errorf("target: %w", err)
	}
	return src, tgt, func() { src.Close(context.Background()); tgt.Close(context.Background()) }, nil
}

func (a *Activities) Discover(ctx context.Context, in DiscoverIn) (_ readiness.Report, err error) {
	defer func() { err = fail(err) }()
	src, tgt, closeAll, err := both(ctx, TablesIn{Plan: in.Plan})
	if err != nil {
		return readiness.Report{}, err
	}
	defer closeAll()
	ri := readiness.Input{Plan: in.Plan}
	if ri.Source, err = pg.Inspect(ctx, src, in.Plan.Database.Schemas); err != nil {
		return readiness.Report{}, fmt.Errorf("source: %w", err)
	}
	if ri.Target, err = pg.Inspect(ctx, tgt, in.Plan.Database.Schemas); err != nil {
		return readiness.Report{}, fmt.Errorf("target: %w", err)
	}
	if err := replRole(ctx, src, in.Plan.Source.Postgres.ReplicationDSN, &ri.Source); err != nil {
		return readiness.Report{}, fmt.Errorf("source: %w", err)
	}
	if err := replRole(ctx, tgt, in.Plan.Target.Postgres.ReplicationDSN, &ri.Target); err != nil {
		return readiness.Report{}, fmt.Errorf("target: %w", err)
	}
	if ad, err := admin(ctx, in.Plan); err != nil {
		ri.PoolErr = redact.DSN(err.Error())
	} else {
		defer ad.Close(context.Background())
		if ri.Pool, err = ad.Database(ctx, in.Plan.Database.Name); err != nil {
			ri.PoolErr = redact.DSN(err.Error())
		}
	}
	if len(in.Plan.Services) > 0 {
		a.servicesInput(ctx, in.Plan, &ri)
	}
	return readiness.Assess(ri), nil
}

// PrepareSchema copies the plan tables' definitions to the target. CopySchema
// is not idempotent, so the target is read first: every plan table present
// and matching the source means an earlier attempt finished; none present
// means copy; anything in between cannot be repaired by retrying.
func (a *Activities) PrepareSchema(ctx context.Context, in TablesIn) (err error) {
	defer func() { err = fail(err) }()
	if len(in.Tables) == 0 {
		return permanentf("prepare: no tables to prepare")
	}
	return heartbeat(ctx, func() error {
		src, tgt, closeAll, err := both(ctx, in)
		if err != nil {
			return err
		}
		defer closeAll()
		for _, c := range []*pgx.Conn{src, tgt} {
			if err := pg.EnsureSentinel(ctx, c); err != nil {
				return err
			}
		}
		have, err := pg.ReadSchema(ctx, tgt, in.Tables)
		if err != nil {
			return err
		}
		switch {
		case len(have) == len(in.Tables):
			want, err := pg.ReadSchema(ctx, src, in.Tables)
			if err != nil {
				return err
			}
			if d := pg.Diff(want, have); len(d) > 0 {
				return permanentf("target already has the plan tables but they differ from the source: %v; roll back Prepare to start clean", d)
			}
			return nil // applied by an earlier attempt
		case len(have) > 0:
			return permanentf("target has %d of %d plan tables (a partial earlier Prepare); roll back Prepare to start clean", len(have), len(in.Tables))
		}
		srcDSN, err := in.Plan.Source.Postgres.DSN.Resolve()
		if err != nil {
			return err
		}
		tgtDSN, err := in.Plan.Target.Postgres.DSN.Resolve()
		if err != nil {
			return err
		}
		return pg.CopySchema(ctx, in.Plan.Database.Tools, srcDSN, tgtDSN, in.Plan.Database.Schemas, in.Tables)
	})
}

// UndoPrepare drops the plan tables on the target and cutover's sentinel
// schema on both sides. It must run after StopLink: the sentinel table is in
// the link's publication, and its schema is dropped with CASCADE.
func (a *Activities) UndoPrepare(ctx context.Context, in TablesIn) (err error) {
	defer func() { err = fail(err) }()
	src, tgt, closeAll, err := both(ctx, in)
	if err != nil {
		return err
	}
	defer closeAll()
	if err := pg.DropTables(ctx, tgt, in.Tables); err != nil {
		return fmt.Errorf("target: drop tables: %w", err)
	}
	if err := pg.DropSentinel(ctx, tgt); err != nil {
		return fmt.Errorf("target: drop sentinel: %w", err)
	}
	if err := pg.DropSentinel(ctx, src); err != nil {
		return fmt.Errorf("source: drop sentinel: %w", err)
	}
	return nil
}

func (a *Activities) CheckSchema(ctx context.Context, in TablesIn) (_ []string, err error) {
	defer func() { err = fail(err) }()
	src, tgt, closeAll, err := both(ctx, in)
	if err != nil {
		return nil, err
	}
	defer closeAll()
	s, err := pg.ReadSchema(ctx, src, in.Tables)
	if err != nil {
		return nil, err
	}
	t, err := pg.ReadSchema(ctx, tgt, in.Tables)
	if err != nil {
		return nil, err
	}
	diffs := pg.Diff(s, t)
	// Every sequence in the plan's schemas is synced inside the switch's
	// pause; one the target lacks would abort every switch there.
	missing, err := missingSequences(ctx, src, tgt, in.Plan.Database.Schemas)
	if err != nil {
		return nil, err
	}
	for _, m := range missing {
		diffs = append(diffs, "sequence "+m+" is on the source but not the target; create it on the target")
	}
	return diffs, nil
}

func missingSequences(ctx context.Context, src, tgt *pgx.Conn, schemas []string) ([]string, error) {
	have, err := pg.SequenceNames(ctx, tgt, schemas)
	if err != nil {
		return nil, fmt.Errorf("target: %w", err)
	}
	want, err := pg.SequenceNames(ctx, src, schemas)
	if err != nil {
		return nil, fmt.Errorf("source: %w", err)
	}
	var out []string
	for _, w := range want {
		if !slices.Contains(have, w) {
			out = append(out, w)
		}
	}
	return out, nil
}

// CompareData samples each table on both sides after a sentinel proves the
// target has caught up with the source read. Rows changing between the two
// reads can still differ, so it retries three times and reports, rather than
// fails: a human judges the evidence at the rehearse gate.
func (a *Activities) CompareData(ctx context.Context, in TablesIn) (out []pg.TableCheck, err error) {
	defer func() { err = fail(err) }()
	err = heartbeat(ctx, func() error {
		var err error
		out, err = compareData(ctx, in)
		return err
	})
	return out, err
}

func compareData(ctx context.Context, in TablesIn) ([]pg.TableCheck, error) {
	src, err := connect(ctx, in.Plan.Source.Postgres.DSN)
	if err != nil {
		return nil, fmt.Errorf("source: %w", err)
	}
	defer src.Close(context.Background())
	tgt, err := connect(ctx, in.Plan.Target.Postgres.DSN)
	if err != nil {
		return nil, fmt.Errorf("target: %w", err)
	}
	// Close whichever target connection is current when this returns: tgt is
	// replaced below after a sentinel timeout.
	defer func() {
		if tgt != nil {
			tgt.Close(context.Background())
		}
	}()
	tables, err := pg.Tables(ctx, src, in.Plan.Database.Schemas)
	if err != nil {
		return nil, err
	}
	byName := map[string]pg.Table{}
	for _, t := range tables {
		byName[t.Qualified()] = t
	}
	rows := in.Plan.Thresholds.Rehearse.SampleRows
	var out []pg.TableCheck
	for _, name := range in.Tables {
		chk := pg.TableCheck{Table: name}
		// Looked up first: Estimate on a missing table is an error, and the
		// report should say what is wrong instead.
		t, ok := byName[name]
		if !ok {
			chk.Note = "not on the source: not sampled"
			out = append(out, chk)
			continue
		}
		if chk.SourceEstimate, err = pg.Estimate(ctx, src, name); err != nil {
			return nil, err
		}
		if chk.TargetEstimate, err = pg.Estimate(ctx, tgt, name); err != nil {
			return nil, err
		}
		if len(t.PKCols) == 0 {
			chk.Note = "no primary key: not sampled"
			out = append(out, chk)
			continue
		}
		for attempt := 1; attempt <= 3; attempt++ {
			sd, n, err := pg.SampleDigest(ctx, src, t, rows)
			if err != nil {
				return nil, err
			}
			tok, err := pg.WriteSentinel(ctx, src)
			if err != nil {
				return nil, err
			}
			wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			seen, err := pg.WaitSentinel(wctx, tgt, tok, 50*time.Millisecond)
			cancel()
			if err != nil {
				return nil, err
			}
			if !seen {
				// WaitSentinel's timeout closed tgt; reconnect for the next table.
				chk.Note = "target did not catch up within 30s"
				tgt.Close(context.Background())
				tgt = nil
				c, err := connect(ctx, in.Plan.Target.Postgres.DSN)
				if err != nil {
					return nil, fmt.Errorf("target: %w", err)
				}
				tgt = c
				break
			}
			td, _, err := pg.SampleDigest(ctx, tgt, t, rows)
			if err != nil {
				return nil, err
			}
			chk.Sampled = n
			if sd == td {
				chk.Match, chk.Note = true, ""
				break
			}
			chk.Note = fmt.Sprintf("sample differs after %d attempts (rows may be changing)", attempt)
		}
		out = append(out, chk)
	}
	return out, nil
}

// Cleanup removes cutover's sentinel from both sides after Finish.
func (a *Activities) Cleanup(ctx context.Context, in TablesIn) (err error) {
	defer func() { err = fail(err) }()
	src, tgt, closeAll, err := both(ctx, in)
	if err != nil {
		return err
	}
	defer closeAll()
	if err := pg.DropSentinel(ctx, src); err != nil {
		return fmt.Errorf("source: drop sentinel: %w", err)
	}
	if err := pg.DropSentinel(ctx, tgt); err != nil {
		return fmt.Errorf("target: drop sentinel: %w", err)
	}
	return nil
}

func subscriptionExists(ctx context.Context, c *pgx.Conn, name string) (bool, error) {
	var ok bool
	err := c.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_subscription WHERE subname = $1
		AND subdbid = (SELECT oid FROM pg_database WHERE datname = current_database()))`, name).Scan(&ok)
	if err != nil {
		return false, fmt.Errorf("subscription %s: %w", name, err)
	}
	return ok, nil
}

// StartLink creates (or reconciles) the publication on From and the
// subscription on To. A slot named for the link on From without its
// subscription on To is left by an interrupted CREATE SUBSCRIPTION, and
// would make the create fail, so it is dropped first.
func (a *Activities) StartLink(ctx context.Context, in LinkIn) (err error) {
	defer func() { err = fail(err) }()
	return heartbeat(ctx, func() error {
		pub, err := connect(ctx, pgFor(in.Plan, in.Link.From).DSN)
		if err != nil {
			return fmt.Errorf("%s: %w", in.Link.From, err)
		}
		defer pub.Close(context.Background())
		sub, err := connect(ctx, pgFor(in.Plan, in.Link.To).DSN)
		if err != nil {
			return fmt.Errorf("%s: %w", in.Link.To, err)
		}
		defer sub.Close(context.Background())
		if err := pg.EnsurePublication(ctx, pub, in.Link.Name, append(append([]string{}, in.Tables...), pg.SentinelTable)); err != nil {
			return err
		}
		exists, err := subscriptionExists(ctx, sub, in.Link.Name)
		if err != nil {
			return err
		}
		if !exists {
			if err := pg.DropSlot(ctx, pub, in.Link.Name); err != nil {
				return fmt.Errorf("%s: orphaned slot: %w", in.Link.From, err)
			}
		}
		// CREATE SUBSCRIPTION echoes a conninfo it cannot parse.
		if err := checkDSN(pgFor(in.Plan, in.Link.From).ReplicationDSN); err != nil {
			return err
		}
		conninfo, err := pgFor(in.Plan, in.Link.From).ReplicationDSN.Resolve()
		if err != nil {
			return err
		}
		return pg.EnsureSubscription(ctx, sub, in.Link.Name, conninfo, in.Link.Name, in.CopyData)
	})
}

// dropSubscription drops the subscription on To, then the slot on From.
// DropSubscription detaches from the slot when From is unreachable, so the
// subscriber is handled first and on its own connection; the slot drop then
// needs From, and until it is reachable the error is retryable and a retry
// only repeats the (idempotent) slot drop. The From connection is returned
// open for the caller's further work, and must be closed.
func dropSubscription(ctx context.Context, in LinkIn) (*pgx.Conn, error) {
	sub, err := connect(ctx, pgFor(in.Plan, in.Link.To).DSN)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", in.Link.To, err)
	}
	defer sub.Close(context.Background())
	if err := pg.DropSubscription(ctx, sub, in.Link.Name); err != nil {
		return nil, err
	}
	pub, err := connect(ctx, pgFor(in.Plan, in.Link.From).DSN)
	if err != nil {
		return nil, fmt.Errorf("%s: subscription dropped, slot not yet: %w", in.Link.From, err)
	}
	if err := pg.DropSlot(ctx, pub, in.Link.Name); err != nil {
		pub.Close(context.Background())
		return nil, fmt.Errorf("%s: %w", in.Link.From, err)
	}
	return pub, nil
}

// StopLink removes the link entirely: subscription, slot and publication.
func (a *Activities) StopLink(ctx context.Context, in LinkIn) (err error) {
	defer func() { err = fail(err) }()
	return heartbeat(ctx, func() error {
		pub, err := dropSubscription(ctx, in)
		if err != nil {
			return err
		}
		defer pub.Close(context.Background())
		return pg.DropPublication(ctx, pub, in.Link.Name)
	})
}

// DropSubscription removes the subscription and its slot, keeping the
// publication.
func (a *Activities) DropSubscription(ctx context.Context, in LinkIn) (err error) {
	defer func() { err = fail(err) }()
	return heartbeat(ctx, func() error {
		pub, err := dropSubscription(ctx, in)
		if err != nil {
			return err
		}
		pub.Close(context.Background())
		return nil
	})
}

func (a *Activities) CopyProgress(ctx context.Context, in LinkIn) (_ Progress, err error) {
	defer func() { err = fail(err) }()
	sub, err := connect(ctx, pgFor(in.Plan, in.Link.To).DSN)
	if err != nil {
		return Progress{}, fmt.Errorf("%s: %w", in.Link.To, err)
	}
	defer sub.Close(context.Background())
	ready, total, err := pg.CopyProgress(ctx, sub, in.Link.Name)
	return Progress{Ready: ready, Total: total}, err
}

// MeasureLag writes a sentinel token on the publisher (directly, never
// through PgBouncer, so it works while the pool is paused) and waits up to
// in.Timeout for it on the subscriber. Visible=false means "not drained in
// time"; it is an answer, not an error.
func (a *Activities) MeasureLag(ctx context.Context, in LagIn) (out LagOut, err error) {
	defer func() { err = fail(err) }()
	if in.Timeout <= 0 {
		return LagOut{}, permanentf("measure lag: timeout must be positive, got %s", in.Timeout)
	}
	err = heartbeat(ctx, func() error {
		pub, err := connect(ctx, pgFor(in.Plan, in.Link.From).DSN)
		if err != nil {
			return fmt.Errorf("%s: %w", in.Link.From, err)
		}
		defer pub.Close(context.Background())
		sub, err := connect(ctx, pgFor(in.Plan, in.Link.To).DSN)
		if err != nil {
			return fmt.Errorf("%s: %w", in.Link.To, err)
		}
		defer sub.Close(context.Background())
		start := time.Now()
		tok, err := pg.WriteSentinel(ctx, pub)
		if err != nil {
			return err
		}
		wctx, cancel := context.WithTimeout(ctx, in.Timeout)
		defer cancel()
		ok, err := pg.WaitSentinel(wctx, sub, tok, 25*time.Millisecond)
		if err != nil {
			return err
		}
		out = LagOut{Lag: time.Since(start), Visible: ok}
		return nil
	})
	return out, err
}

func admin(ctx context.Context, p plan.Plan) (*pgbouncer.Admin, error) {
	dsn, err := p.Database.PgBouncer.AdminDSN.Resolve()
	if err != nil {
		return nil, err
	}
	ad, err := pgbouncer.Dial(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgbouncer (%s): %w", p.Database.PgBouncer.AdminDSN, err)
	}
	return ad, nil
}

// pauseErr maps Pause's errors: cancellation of the activity (ctx) stays
// cancellation; a global pause is someone else's and will
// not lift by retrying; a pause timeout is retryable (a retry sends PAUSE
// again and waits for the drain).
func pauseErr(ctx context.Context, err error) error {
	// Pause reports ErrPauseTimeout whenever its context ends, including when
	// the activity itself was cancelled; that must stay a cancellation.
	if err != nil && errors.Is(ctx.Err(), context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, pgbouncer.ErrGlobalPause) {
		return permanentf("pgbouncer is globally paused; cutover will not pause a pooler someone else paused — resume it first")
	}
	return err
}

// PausePool pauses the plan's database in PgBouncer, bounded by in.Timeout.
func (a *Activities) PausePool(ctx context.Context, in PoolIn) (err error) {
	defer func() { err = fail(err) }()
	if in.Timeout <= 0 {
		return permanentf("pause: timeout must be positive, got %s", in.Timeout)
	}
	return heartbeat(ctx, func() error {
		ad, err := admin(ctx, in.Plan)
		if err != nil {
			return err
		}
		defer ad.Close(context.Background())
		pctx, cancel := context.WithTimeout(ctx, in.Timeout)
		defer cancel()
		return pauseErr(ctx, ad.Pause(pctx, in.Plan.Database.Name))
	})
}

// ResumePool resumes the plan's database. in.Timeout is not used.
func (a *Activities) ResumePool(ctx context.Context, in PoolIn) (err error) {
	defer func() { err = fail(err) }()
	return heartbeat(ctx, func() error {
		ad, err := admin(ctx, in.Plan)
		if err != nil {
			return err
		}
		defer ad.Close(context.Background())
		return ad.Resume(ctx, in.Plan.Database.Name)
	})
}

// Repoint routes the plan's database to in.To's route. pgbouncer.Repoint
// writes the routes file, reloads, checks host, port and dbname, then
// reconnects; all of it is safe to repeat.
func (a *Activities) Repoint(ctx context.Context, in RepointIn) (err error) {
	defer func() { err = fail(err) }()
	if in.To != Source && in.To != Target {
		return permanentf("repoint: unknown side %q", in.To)
	}
	return heartbeat(ctx, func() error {
		ad, err := admin(ctx, in.Plan)
		if err != nil {
			return err
		}
		defer ad.Close(context.Background())
		d := in.Plan.Database
		w := pgbouncer.RouteWriter(pgbouncer.LocalWriter)
		if h := d.PgBouncer.Host; h != nil {
			c, err := sshx.Dial(ctx, *h)
			if err != nil {
				return fmt.Errorf("pgbouncer host: %w", err)
			}
			defer c.Close()
			w = pgbouncer.SSHWriter(c)
		}
		return pgbouncer.RepointWith(ctx, ad, w, d.PgBouncer.RoutesFile, d.Name, pgFor(in.Plan, in.To).Route)
	})
}

func (a *Activities) SyncSequences(ctx context.Context, in SeqIn) (out []pg.SequenceSync, err error) {
	defer func() { err = fail(err) }()
	err = heartbeat(ctx, func() error {
		from, err := connect(ctx, pgFor(in.Plan, in.From).DSN)
		if err != nil {
			return fmt.Errorf("%s: %w", in.From, err)
		}
		defer from.Close(context.Background())
		to, err := connect(ctx, pgFor(in.Plan, in.To).DSN)
		if err != nil {
			return fmt.Errorf("%s: %w", in.To, err)
		}
		defer to.Close(context.Background())
		out, err = pg.SyncSequences(ctx, from, to, in.Plan.Database.Schemas, in.Plan.Thresholds.Switch.SequenceMargin)
		return err
	})
	return out, err
}

// SessionCheck is the pre-switch look at who reaches the source without
// PgBouncer. Checked is false, with Reason, when it could not tell.
type SessionCheck struct {
	Checked bool         `json:"checked"`
	Reason  string       `json:"reason,omitempty"`
	Bypass  []pg.Session `json:"bypass,omitempty"`
}

// Summary is one sentence for the switch gate.
func (s SessionCheck) Summary() string {
	if !s.Checked {
		return "Could not check sessions: " + s.Reason + "."
	}
	if len(s.Bypass) == 0 {
		return "No sessions bypass PgBouncer."
	}
	n := 0
	var l []string
	for _, b := range s.Bypass {
		n += b.Count
		l = append(l, fmt.Sprintf("%s (%d)", b.Addr, b.Count))
	}
	return fmt.Sprintf("%d sessions bypass PgBouncer: %s.", n, strings.Join(l, ", "))
}

// CheckSessions repeats Discover's direct-client check on the source just
// before the switch gate opens: clients connected since Discover would lose
// their writes at the switch.
func (a *Activities) CheckSessions(ctx context.Context, in DiscoverIn) (_ SessionCheck, err error) {
	defer func() { err = fail(err) }()
	src, err := connect(ctx, in.Plan.Source.Postgres.DSN)
	if err != nil {
		return SessionCheck{}, fmt.Errorf("source: %w", err)
	}
	defer src.Close(context.Background())
	sessions, all, err := pg.Sessions(ctx, src)
	if err != nil {
		return SessionCheck{}, err
	}
	bypass, checked := readiness.DirectClients(in.Plan.Database.PgBouncer.Addresses, sessions)
	switch {
	case !checked:
		return SessionCheck{Reason: "pgbouncer.addresses is empty"}, nil
	case !all:
		return SessionCheck{Reason: "the source role cannot see other roles' sessions (needs pg_read_all_stats)"}, nil
	}
	return SessionCheck{Checked: true, Bypass: bypass}, nil
}
