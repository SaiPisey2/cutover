package flow

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/SaiPisey2/cutover/internal/ops"
	"github.com/SaiPisey2/cutover/internal/pg"
)

// minStep is the least time worth starting a switch step with.
const minStep = 100 * time.Millisecond

// bounded returns activity options whose timeouts end at deadline, or false
// when less than minStep is left.
func (r *runner) bounded(deadline time.Time) (workflow.Context, bool) {
	rem := deadline.Sub(workflow.Now(r.ctx))
	if rem < minStep {
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
// Every step up to the repoint runs inside the max_pause budget. The drain
// may use only the budget minus a reserve of max(250ms, max_pause/4), so the
// steps after it keep time; with max_pause 1s the drain gets about 750ms.
// If a step fails or the budget runs out, the completed steps are undone in
// reverse and writes resume on `from`; it returns (false, nil).
//
// A successful repoint is the point of no return: PgBouncer now routes to
// `to`, so Primary becomes `to` at once and the switch never aborts after
// it. Resume is then retried without limit (r.must) and is not bounded by
// the budget, so if RESUME stalls the pause can last longer than
// max_pause; undoing a switch that has happened would be worse. A repoint
// whose result was lost (the activity failed or timed out, but PgBouncer
// applied it) is still compensated: it counts as failed, the switch aborts
// while the pool is still paused, and repoint-back puts the route back.
// After resume, the old publication is dropped outside the budget.
//
// The old subscription is dropped, not disabled, inside the pause: on
// Postgres 12–15 a subscription cannot filter by origin. If the old link
// survived, changes applied by the new reverse link would be published
// back, and they would loop.
//
// Compensations run after the budget, while writes are still held, so the
// replication ones are bounded: restarting the old link and stopping the
// next link each get StartToClose 15s and at most 3 attempts (backoff 1s,
// 2s), about 48s each. If restarting the old link fails, nothing
// replicates out of `from`: replication-broken is recorded,
// Status.ReplicationBroken is set and switchOver returns (false,
// ErrReplicationBroken) after writes resume, as below.
// If stopping the next link fails, the old link is NOT restarted, because
// both directions live would loop: the event replication-broken is
// recorded, Status.ReplicationBroken is set, and switchOver returns
// (false, ErrReplicationBroken) after writes resume on `from`.
// Repoint-back and resume are retried without limit (r.must): resuming
// while PgBouncer might still route to `to` would send writes to the
// database the workflow does not consider primary. Resume runs last. Worst
// case, with both bounded compensations timing out, writes are held for
// max_pause + 2 × 48s ≈ max_pause + 1m36s, plus however long repoint-back
// takes to succeed, before ResumePool starts.
//
// A timed-out DropSubscription attempt may still commit its DROP after the
// abort restarted the old link, leaving the old link without its
// subscription. That fails safe: the next switch's drain check sees the
// sentinel never arrive and aborts.
//
// Rollback signals that arrive during the switch are not polled: they stay
// queued until the post-switch step (dropping the old publication), which
// polls them. A caller that gets (true, errRollback) must read it as
// "switched, then a rollback was requested": switch back.
func (r *runner) switchOver(from, to ops.Side, old, next ops.Link, tables []string) (bool, error) {
	p := r.in.Plan
	maxPause := p.Thresholds.Switch.MaxPause.D()
	start := workflow.Now(r.ctx)
	deadline := start.Add(maxPause)
	reserve := max(250*time.Millisecond, maxPause/4)
	r.event("switch-started", "", fmt.Sprintf("%s to %s, pause budget %s", from, to, maxPause))

	pool := ops.PoolIn{Plan: p}
	oldIn := ops.LinkIn{Plan: p, Link: old, Tables: tables}
	nextIn := ops.LinkIn{Plan: p, Link: next, Tables: tables}

	// compensate runs one bounded compensation: StartToClose 15s, heartbeat
	// timeout 2s, 3 attempts.
	compensate := func(fn, in any) func() error {
		return func() error {
			ctx := workflow.WithActivityOptions(r.ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 15 * time.Second,
				HeartbeatTimeout:    heldHeartbeat,
				RetryPolicy: &temporal.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 2,
					MaximumInterval: 2 * time.Second, MaximumAttempts: 3},
			})
			return workflow.ExecuteActivity(ctx, fn, in).Get(r.ctx, nil)
		}
	}
	// Resume always runs last and retries without limit, like repoint-back.
	resume := func() error { return r.execHeld(time.Minute, acts.ResumePool, pool, nil) }
	const (
		startOld = iota + 1
		stopNext
		repointBack
	)
	type comp struct {
		kind int
		run  func() error
	}
	var undo []comp
	// Cancellation. Once the pool may be paused, cancelling the workflow
	// must not leave writes held: the unbounded steps (repoint-back, then
	// resume) switch to a disconnected context and run to completion, and
	// only then is the cancellation returned. The bounded replication
	// compensations are skipped; unwind handles the links.
	orig := r.ctx
	defer func() { r.ctx = orig }()
	cancelled := func() bool { return orig.Err() != nil }
	detached := false
	disconnect := func() {
		if !detached {
			detached = true
			r.ctx, _ = workflow.NewDisconnectedContext(orig)
			r.event("cancel-deferred", "", "workflow cancelled while writes may be held; restoring the route and resuming first")
		}
	}
	// hold runs an unbounded step to completion, even across cancellation.
	hold := func(fn func() error) error {
		if cancelled() {
			disconnect()
		}
		err := r.must(fn)
		if err != nil && cancelled() && !detached {
			disconnect()
			err = r.must(fn)
		}
		return err
	}
	abort := func(why string) (bool, error) {
		r.event("switch-aborted", "", why+"; writes resume on the "+string(from))
		r.st.Sequences = nil
		broken := false
		for i := len(undo) - 1; i >= 0; i-- {
			switch u := undo[i]; {
			case u.kind == repointBack:
				if err := hold(u.run); err != nil {
					return false, err
				}
			case cancelled():
				r.event("compensation-skipped", "", "workflow cancelled: replication compensation left to unwind")
			case u.kind == startOld && broken:
				r.event("compensation-skipped", "", "not restarting "+old.Name+": "+next.Name+" may still be live, and both directions would loop")
			default:
				if err := u.run(); err != nil && !cancelled() {
					r.event("compensation-failed", "", message(err)+"; gave up so writes can resume")
					switch u.kind {
					case stopNext:
						broken = true
						r.st.ReplicationBroken = true
						r.event("replication-broken", "", "could not stop "+next.Name+"; "+old.Name+" left stopped, repair replication before switching again")
					case startOld:
						// Nothing replicates out of `from` now: a later switch
						// could never drain, and a switch back that recreated
						// the link without copying would lose the rows
						// written meanwhile.
						broken = true
						r.st.ReplicationBroken = true
						r.event("replication-broken", "", "could not restart "+old.Name+"; nothing replicates out of the "+string(from)+", repair replication before switching again")
					}
				}
			}
		}
		if err := hold(resume); err != nil {
			return false, err
		}
		switch {
		case cancelled():
			return false, orig.Err()
		case broken:
			return false, ErrReplicationBroken
		}
		return false, nil
	}
	// call runs one switch step, bounded by the time left before `until`.
	call := func(until time.Time, what string, fn, in, out any) error {
		ctx, ok := r.bounded(until)
		if !ok {
			return fmt.Errorf("%s: the %s pause budget is spent", what, maxPause)
		}
		if err := workflow.ExecuteActivity(ctx, fn, in).Get(r.ctx, out); err != nil {
			return fmt.Errorf("%s: %s", what, message(err))
		}
		return nil
	}
	// left is the Timeout input for an activity bounded by `until`: the time
	// remaining less a slack (a tenth, at most 500ms) so the activity can
	// answer before Temporal times it out.
	left := func(until time.Time) time.Duration {
		rem := until.Sub(workflow.Now(r.ctx))
		return rem - min(500*time.Millisecond, rem/10)
	}

	if err := call(deadline, "pause", acts.PausePool, ops.PoolIn{Plan: p, Timeout: left(deadline)}, nil); err != nil {
		return abort(err.Error())
	}
	drainBy := deadline.Add(-reserve)
	var lag ops.LagOut
	if err := call(drainBy, "drain", acts.MeasureLag, ops.LagIn{Plan: p, Link: old, Timeout: left(drainBy)}, &lag); err != nil {
		return abort(err.Error())
	}
	if !lag.Visible {
		return abort(fmt.Sprintf("replication did not drain within the %s pause budget", maxPause))
	}
	undo = append(undo, comp{kind: startOld, run: compensate(acts.StartLink, oldIn)})
	if err := call(deadline, "stop "+old.Name, acts.DropSubscription, oldIn, nil); err != nil {
		return abort(err.Error())
	}
	var seqs []pg.SequenceSync
	if err := call(deadline, "sequences", acts.SyncSequences, ops.SeqIn{Plan: p, From: from, To: to}, &seqs); err != nil {
		return abort(err.Error())
	}
	r.st.Sequences = seqs
	undo = append(undo, comp{kind: stopNext, run: compensate(acts.StopLink, nextIn)})
	if err := call(deadline, "start "+next.Name, acts.StartLink, nextIn, nil); err != nil {
		return abort(err.Error())
	}
	undo = append(undo, comp{kind: repointBack, run: func() error {
		return r.execHeld(time.Minute, acts.Repoint, ops.RepointIn{Plan: p, To: from}, nil)
	}})
	if err := call(deadline, "repoint", acts.Repoint, ops.RepointIn{Plan: p, To: to}, nil); err != nil {
		return abort(err.Error())
	}
	// Point of no return.
	r.st.Primary = to
	if err := hold(resume); err != nil {
		return true, err
	}
	if cancelled() {
		r.event("switched", "", fmt.Sprintf("writes on the %s; then cancelled", to))
		return true, orig.Err()
	}
	r.event("switched", "", fmt.Sprintf("writes on the %s; held for %s", to, workflow.Now(r.ctx).Sub(start)))
	return true, r.step("drop-old-publication", func() error { return r.exec(2*time.Minute, acts.StopLink, oldIn, nil) })
}
