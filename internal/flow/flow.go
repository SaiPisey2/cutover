package flow

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/SaiPisey2/cutover/internal/ops"
	"github.com/SaiPisey2/cutover/internal/pg"
	"github.com/SaiPisey2/cutover/internal/readiness"
)

// errHalt marks an undo step that must not be retried and after which the
// unwind must stop: carrying on would act on a database that is primary,
// or switch over replication that is broken.
var errHalt = errors.New("unwind halted")

// Migration moves one application's database from source to target.
// Rejecting any gate, or a rollback signal before Finish, unwinds every
// completed phase in reverse.
func Migration(ctx workflow.Context, in Input) (Result, error) {
	r, err := newRunner(ctx, in)
	if err != nil {
		return Result{}, err
	}
	err = r.run()
	if errors.Is(err, errRollback) {
		if err = r.unwind(); err == nil {
			r.st.State = StateRolledBack
			r.event(r.st.State, "", "")
			return Result{State: r.st.State, Events: r.st.Events}, nil
		}
	}
	switch {
	case err == nil:
		r.st.State = StateFinished
		r.event(r.st.State, "", "")
		return Result{State: r.st.State, Events: r.st.Events}, nil
	case r.ctx.Err() != nil:
		what := "nothing further was undone"
		if r.st.State == StateRollingBack {
			what = "the unwind stopped here"
		}
		r.st.State = StateFailed
		r.event("cancelled", "", fmt.Sprintf("cancelled with writes on the %s; %s", r.st.Primary, what))
	default:
		r.st.State = StateFailed
		r.event("failed", "", message(err))
	}
	return Result{State: r.st.State, Events: r.st.Events}, err
}

func (r *runner) run() error {
	p := r.in.Plan

	// Prepare never runs while readiness reports blockers: target-not-empty
	// is what makes PrepareSchema's "identical tables = my earlier success"
	// safe, and Prepare's undo drops the plan tables.
	r.enter(PhaseDiscover)
	for {
		var rep readiness.Report
		if err := r.step("discover", func() error {
			return r.exec(2*time.Minute, acts.Discover, ops.DiscoverIn{Plan: p}, &rep)
		}); err != nil {
			return err
		}
		r.st.Readiness, r.st.Tables = &rep, rep.Tables
		if !rep.Blocked() {
			if err := r.gate("discover", fmt.Sprintf("%d tables can move with zero downtime. Approve the plan.", len(rep.Tables)), 1); err != nil {
				return err
			}
			break
		}
		if err := r.gate("discover", fmt.Sprintf("%d blockers. Fix them, then approve to check again.", rep.Count(readiness.Blocker)), 1); err != nil {
			return err
		}
	}
	tables := r.st.Tables
	tin := ops.TablesIn{Plan: p, Tables: tables}
	fwd, rev := ops.Forward(r.in.MigrationID), ops.Reverse(r.in.MigrationID)
	fwdIn := ops.LinkIn{Plan: p, Link: fwd, Tables: tables}
	revIn := ops.LinkIn{Plan: p, Link: rev, Tables: tables}

	r.enter(PhasePrepare)
	r.pushUndo(func() error {
		if r.st.Primary != ops.Source {
			return fmt.Errorf("%w: writes are on the %s, so its plan tables are not dropped", errHalt, r.st.Primary)
		}
		return r.exec(10*time.Minute, acts.UndoPrepare, tin, nil)
	})
	if err := r.step("prepare", func() error { return r.exec(time.Hour, acts.PrepareSchema, tin, nil) }); err != nil {
		return err
	}
	if err := r.gate("prepare", "Target schema created. Approve to start replication.", 1); err != nil {
		return err
	}

	r.enter(PhaseReplicate)
	r.pushUndo(func() error { return r.exec(2*time.Minute, acts.StopLink, fwdIn, nil) })
	if err := r.step("start-replication", func() error {
		in := fwdIn
		in.CopyData = true
		return r.exec(5*time.Minute, acts.StartLink, in, nil)
	}); err != nil {
		return err
	}
	if err := r.step("replicate", func() error { return r.waitReplicated(fwd, tables) }); err != nil {
		return err
	}

	r.enter(PhaseRehearse)
	if err := r.step("schema-check", r.schemaCheck(tin)); err != nil {
		return err
	}
	if err := r.step("compare-data", func() error {
		return r.exec(10*time.Minute, acts.CompareData, tin, &r.st.Checks)
	}); err != nil {
		return err
	}
	if err := r.gate("rehearse", summarizeChecks(r.st.Checks), 1); err != nil {
		return err
	}

	r.enter(PhaseShift)
	r.event("skipped", "", "no services in this plan; traffic shifting arrives with services")

	r.enter(PhaseSwitch)
	for {
		if r.st.ReplicationBroken {
			return r.brokenGate()
		}
		if err := r.step("schema-check", r.schemaCheck(tin)); err != nil {
			return err
		}
		if err := r.gate("switch", "Switch the database to the target. Needs two different approvers.", 2); err != nil {
			return err
		}
		ok, err := r.switchOver(ops.Source, ops.Target, fwd, rev, tables)
		if ok {
			// Pushed even if what follows the switch fails, so a rollback
			// after a half-finished switch still switches back.
			r.pushUndo(func() error { return r.switchBack(rev, fwd, tables) })
		}
		switch {
		case errors.Is(err, ErrReplicationBroken):
			// The reverse link may still be live; the unwind stops it
			// before the forward link and Prepare are undone.
			r.pushUndo(func() error { return r.exec(2*time.Minute, acts.StopLink, revIn, nil) })
			return r.brokenGate()
		case ok && r.ctx.Err() != nil:
			r.event("switch-unfinished", "", "cancelled after the switch: the forward publication on the source was not dropped")
			return err
		case err != nil:
			return err // errRollback (switched, then switch back) or cancellation
		}
		if ok {
			break
		}
		// Clean abort: writes are on the source; check and ask again.
	}

	r.enter(PhaseFinish)
	if err := r.watch(p.Thresholds.Finish.Watch.D()); err != nil {
		return err
	}
	if err := r.gate("finish", "Watch period over. Approve to drop reverse replication; rollback ends here.", 1); err != nil {
		return err
	}
	r.closed = "the migration is past Finish"
	r.undo = nil
	return r.step("finish", func() error {
		if err := r.exec(2*time.Minute, acts.StopLink, revIn, nil); err != nil {
			return err
		}
		return r.exec(2*time.Minute, acts.Cleanup, tin, nil)
	})
}

// brokenGate stops at a gate after a switch left replication broken. A
// second switch is refused, so every outcome is a rollback: approving it,
// rejecting it, or a rollback signal.
func (r *runner) brokenGate() error {
	if err := r.gate("replication-broken", "The switch aborted, and forward replication was left stopped (the reverse link "+
		"could not be stopped, or the forward link could not be restarted). Writes are on the source. Switching again is "+
		"refused: approve to roll back.", 1); err != nil {
		return err
	}
	r.event("rollback-requested", "", "replication is broken; rollback is the only safe way on")
	return errRollback
}

// switchBackTries is how many clean aborts in a row switchBack makes
// before it stops at a gate; undoTries is the same for an undo step.
const (
	switchBackTries = 3
	undoTries       = 5
)

// switchBack repeats the reverse switch until it succeeds; an aborted
// attempt has already resumed writes on the target. After switchBackTries
// clean aborts in a row it waits at gate retry-switch-back, and each
// approval buys that many more attempts. It does nothing once the source
// is primary, so a retry after success never switches twice, and it halts
// the unwind if replication is broken.
func (r *runner) switchBack(rev, fwd ops.Link, tables []string) error {
	aborts := 0
	for {
		if r.st.Primary == ops.Source {
			return nil
		}
		if r.st.ReplicationBroken {
			return fmt.Errorf("%w: %w", errHalt, ErrReplicationBroken)
		}
		switch {
		case aborts == switchBackTries:
			if err := r.gate("retry-switch-back", r.lastAbort()+"; writes are on the target; fix replication and approve to try again", 1); err != nil {
				return err
			}
			aborts = 0
		case aborts > 0:
			if err := workflow.Sleep(r.ctx, 10*time.Second); err != nil {
				return err
			}
		}
		ok, err := r.switchOver(ops.Target, ops.Source, rev, fwd, tables)
		if errors.Is(err, ErrReplicationBroken) {
			return fmt.Errorf("%w: %w", errHalt, err)
		}
		if ok || err != nil {
			return err
		}
		aborts++
	}
}

// lastAbort is the reason the latest switch aborted.
func (r *runner) lastAbort() string {
	for i := len(r.st.Events) - 1; i >= 0; i-- {
		if e := r.st.Events[i]; e.Kind == "switch-aborted" {
			why, _, _ := strings.Cut(e.Message, "; writes resume on")
			return "The switch back aborted: " + why
		}
	}
	return "The switch back aborted"
}

func (r *runner) schemaCheck(tin ops.TablesIn) func() error {
	return func() error {
		var diffs []string
		if err := r.exec(2*time.Minute, acts.CheckSchema, tin, &diffs); err != nil {
			return err
		}
		if len(diffs) > 0 {
			return fmt.Errorf("schema drift between source and target (DDL is frozen during a migration): %s", strings.Join(diffs, "; "))
		}
		return nil
	}
}

func summarizeChecks(checks []pg.TableCheck) string {
	match := 0
	for _, c := range checks {
		if c.Match {
			match++
		}
	}
	return fmt.Sprintf("%d of %d tables match on sampled rows. Approve the evidence.", match, len(checks))
}

// waitReplicated runs ReplicateWait as a child and returns when lag is
// stable, or errRollback if a rollback arrives first.
func (r *runner) waitReplicated(link ops.Link, tables []string) error {
	id := WorkflowID(r.in.MigrationID) + "-replicate"
	r.st.ReplicateWorkflowID = id
	cctx, cancel := workflow.WithCancel(r.ctx)
	defer cancel()
	cctx = workflow.WithChildOptions(cctx, workflow.ChildWorkflowOptions{WorkflowID: id})
	fut := workflow.ExecuteChildWorkflow(cctx, ReplicateWait, ReplicateIn{Plan: r.in.Plan, Link: link, Tables: tables})
	for {
		var childErr, rbErr error
		done := false
		sel := workflow.NewSelector(r.ctx)
		sel.AddFuture(fut, func(f workflow.Future) {
			var out ReplicateStatus
			childErr, done = f.Get(r.ctx, &out), true
		})
		sel.AddReceive(r.rbCh, func(c workflow.ReceiveChannel, _ bool) {
			var rb RollbackSignal
			c.Receive(r.ctx, &rb)
			rbErr = r.takeRollback(rb)
		})
		sel.Select(r.ctx)
		if rbErr != nil {
			cancel()
			_ = fut.Get(r.ctx, nil)
			return rbErr
		}
		if done {
			if childErr == nil {
				r.event("replicated", "", fmt.Sprintf("lag at or under %s for %s",
					r.in.Plan.Thresholds.Replicate.MaxLag.D(), r.in.Plan.Thresholds.Replicate.StableFor.D()))
			}
			return childErr
		}
	}
}

// watch keeps reverse replication on for d; a rollback ends it early. A
// cancelled workflow cancels the timer, which is returned as the error,
// never read as the watch period ending.
func (r *runner) watch(d time.Duration) error {
	r.event("watching", "", fmt.Sprintf("reverse replication stays on for %s; rollback is one signal away", d))
	tctx, cancel := workflow.WithCancel(r.ctx)
	defer cancel()
	timer := workflow.NewTimer(tctx, d)
	for {
		fired := false
		var timerErr error
		var rb *RollbackSignal
		sel := workflow.NewSelector(r.ctx)
		sel.AddFuture(timer, func(f workflow.Future) { fired, timerErr = true, f.Get(r.ctx, nil) })
		sel.AddReceive(r.rbCh, func(c workflow.ReceiveChannel, _ bool) {
			var s RollbackSignal
			c.Receive(r.ctx, &s)
			rb = &s
		})
		sel.Select(r.ctx)
		if fired {
			return timerErr
		}
		if err := r.takeRollback(*rb); err != nil {
			return err
		}
	}
}

// unwind runs every undo step in reverse. Rollback signals during it are
// ignored: it is already rolling back. A failing step is retried (see
// runUndo). A step that halts (errHalt), or a cancellation, stops the
// unwind where it is: Status.PendingUndo then lists what is left undone.
func (r *runner) unwind() error {
	r.st.State = StateRollingBack
	r.closed = "already rolling back"
	for i := len(r.undo) - 1; i >= 0; i-- {
		u := r.undo[i]
		r.st.Phase = u.phase
		r.event("undo-started", "", "")
		if err := r.runUndo(u); err != nil {
			r.undo = r.undo[:i+1]
			r.st.PendingUndo = nil
			for k := i; k >= 0; k-- {
				r.st.PendingUndo = append(r.st.PendingUndo, r.undo[k].phase)
			}
			if errors.Is(err, errHalt) {
				r.event("unwind-halted", "", u.phase+": "+message(err))
			} else {
				r.event("undo-failed", "", message(err))
			}
			return err
		}
		r.undo = r.undo[:i]
		r.event("undone", "", "")
	}
	return nil
}

// runUndo tries one undo step up to undoTries times, 10s apart, then waits
// at gate retry-undo-<phase>; each approval buys undoTries more attempts.
// A halting error or a cancellation returns at once.
func (r *runner) runUndo(u undoStep) error {
	for {
		var msg string
		for n := 1; ; n++ {
			err := u.run()
			if err == nil || errors.Is(err, errHalt) || r.ctx.Err() != nil {
				return err
			}
			msg = message(err)
			if n == undoTries {
				r.event("compensation-failed", "", msg)
				break
			}
			r.event("compensation-failed", "", msg+"; retrying in 10s")
			if err := workflow.Sleep(r.ctx, 10*time.Second); err != nil {
				return err
			}
		}
		if err := r.gate("retry-undo-"+u.phase, fmt.Sprintf("Undoing %s failed %d times: %s. Approve to try %d more times.",
			u.phase, undoTries, msg, undoTries), 1); err != nil {
			return err
		}
	}
}
