package flow

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/SaiPisey2/cutover/internal/ops"
	"github.com/SaiPisey2/cutover/internal/pg"
)

// bounded returns activity options whose timeouts end at deadline, or false
// when less than a second is left.
func (r *runner) bounded(deadline time.Time) (workflow.Context, bool) {
	rem := deadline.Sub(workflow.Now(r.ctx))
	if rem < time.Second {
		return nil, false
	}
	return workflow.WithActivityOptions(r.ctx, workflow.ActivityOptions{
		ScheduleToCloseTimeout: rem,
		StartToCloseTimeout:    rem,
		HeartbeatTimeout:       2 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{InitialInterval: 200 * time.Millisecond, BackoffCoefficient: 2,
			MaximumInterval: time.Second},
	}), true
}

// switchOver moves writes from `from` to `to` while PgBouncer holds them:
//
//	pause → drain old link → drop old subscription → sync sequences →
//	start next link (copy_data=false) → repoint → resume
//
// Every step runs inside the max_pause budget. If one fails or the budget
// runs out, the completed steps are undone in reverse and writes resume on
// `from`; it returns false. After resume, the old publication is dropped
// outside the budget.
//
// The old subscription is dropped, not disabled, inside the pause: on
// Postgres 12–15 a subscription cannot filter by origin. If the old link
// survived, changes applied by the new reverse link would be published
// back, and they would loop.
//
// Compensations run after the budget, while writes are still held, so the
// replication ones are bounded: restarting the old link and stopping the
// next link each get StartToClose 15s and at most 3 attempts (backoff 1s,
// 2s), about 48s each. If one still fails, it is recorded as
// compensation-failed and skipped; what it leaves (for example a reverse
// subscription) is removed by unwind or Cleanup. Repoint-back and resume are
// retried without limit (r.must): resuming while PgBouncer might still route
// to `to` would send writes to the database the workflow does not consider
// primary. Resume runs last. Worst case, with both bounded compensations
// timing out, writes are held for max_pause + 2 × 48s ≈ max_pause + 1m36s,
// plus however long repoint-back takes to succeed, before ResumePool starts.
func (r *runner) switchOver(from, to ops.Side, old, next ops.Link, tables []string) (bool, error) {
	p := r.in.Plan
	maxPause := p.Thresholds.Switch.MaxPause.D()
	start := workflow.Now(r.ctx)
	deadline := start.Add(maxPause)
	r.event("switch-started", "", fmt.Sprintf("%s to %s, pause budget %s", from, to, maxPause))

	pool := ops.PoolIn{Plan: p}
	oldIn := ops.LinkIn{Plan: p, Link: old, Tables: tables}
	nextIn := ops.LinkIn{Plan: p, Link: next, Tables: tables}

	// compensate runs one bounded compensation: StartToClose 15s, 3 attempts.
	compensate := func(fn, in any) func() error {
		return func() error {
			ctx := workflow.WithActivityOptions(r.ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 15 * time.Second,
				RetryPolicy: &temporal.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 2,
					MaximumInterval: 2 * time.Second, MaximumAttempts: 3},
			})
			return workflow.ExecuteActivity(ctx, fn, in).Get(r.ctx, nil)
		}
	}
	// Resume is always the last compensation: whatever else happened,
	// writes must flow again. It and repoint-back retry without limit.
	resume := func() error { return r.exec(time.Minute, acts.ResumePool, pool, nil) }
	type comp struct {
		run       func() error
		unbounded bool // run through r.must
	}
	var undo []comp
	abort := func(why string) (bool, error) {
		r.event("switch-aborted", "", why+"; writes resume on the "+string(from))
		for i := len(undo) - 1; i >= 0; i-- {
			if undo[i].unbounded {
				if err := r.must(undo[i].run); err != nil {
					return false, err
				}
				continue
			}
			if err := undo[i].run(); err != nil {
				r.event("compensation-failed", "", message(err)+"; gave up so writes can resume, unwind or Cleanup removes what is left")
			}
		}
		return false, r.must(resume)
	}
	remaining := func() time.Duration { return deadline.Sub(workflow.Now(r.ctx)) - 500*time.Millisecond }
	call := func(what string, fn, in, out any) error {
		ctx, ok := r.bounded(deadline)
		if !ok {
			return fmt.Errorf("%s: the %s pause budget is spent", what, maxPause)
		}
		if err := workflow.ExecuteActivity(ctx, fn, in).Get(r.ctx, out); err != nil {
			return fmt.Errorf("%s: %s", what, message(err))
		}
		return nil
	}

	if err := call("pause", acts.PausePool, ops.PoolIn{Plan: p, Timeout: remaining()}, nil); err != nil {
		return abort(err.Error())
	}
	var lag ops.LagOut
	if err := call("drain", acts.MeasureLag, ops.LagIn{Plan: p, Link: old, Timeout: remaining()}, &lag); err != nil {
		return abort(err.Error())
	}
	if !lag.Visible {
		return abort(fmt.Sprintf("replication did not drain within the %s pause budget", maxPause))
	}
	undo = append(undo, comp{run: compensate(acts.StartLink, oldIn)})
	if err := call("stop "+old.Name, acts.DropSubscription, oldIn, nil); err != nil {
		return abort(err.Error())
	}
	var seqs []pg.SequenceSync
	if err := call("sequences", acts.SyncSequences, ops.SeqIn{Plan: p, From: from, To: to}, &seqs); err != nil {
		return abort(err.Error())
	}
	r.st.Sequences = seqs
	undo = append(undo, comp{run: compensate(acts.StopLink, nextIn)})
	if err := call("start "+next.Name, acts.StartLink, nextIn, nil); err != nil {
		return abort(err.Error())
	}
	undo = append(undo, comp{unbounded: true, run: func() error {
		return r.exec(time.Minute, acts.Repoint, ops.RepointIn{Plan: p, To: from}, nil)
	}})
	if err := call("repoint", acts.Repoint, ops.RepointIn{Plan: p, To: to}, nil); err != nil {
		return abort(err.Error())
	}
	if err := call("resume", acts.ResumePool, pool, nil); err != nil {
		return abort(err.Error())
	}
	r.st.Primary = to
	r.event("switched", "", fmt.Sprintf("writes on the %s; held for %s", to, workflow.Now(r.ctx).Sub(start)))
	return true, r.step("drop-old-publication", func() error { return r.exec(2*time.Minute, acts.StopLink, oldIn, nil) })
}
