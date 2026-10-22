package flow

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/SaiPisey2/cutover/internal/ops"
)

var errRollback = errors.New("rollback requested")

// ErrReplicationBroken is returned by a switch whose abort left the old
// direction without replication: either the new link could not be stopped
// (so the old one was not restarted, rather than risk a loop), or the old
// link could not be restarted.
var ErrReplicationBroken = errors.New("replication broken: a switch abort left the old link stopped")

// acts is a nil receiver; its method values name the activities.
var acts *ops.Activities

type undoStep struct {
	phase string
	run   func() error
}

type runner struct {
	ctx    workflow.Context
	in     Input
	st     Status
	gateCh workflow.ReceiveChannel
	rbCh   workflow.ReceiveChannel
	undo   []undoStep
	closed string // non-empty once rollback is no longer possible: the reason
	// baseline is Rehearse's traffic window, per service.
	baseline map[string]ServiceTraffic
}

func newRunner(ctx workflow.Context, in Input) (*runner, error) {
	r := &runner{
		ctx:    ctx,
		in:     in,
		gateCh: workflow.GetSignalChannel(ctx, SignalGate),
		rbCh:   workflow.GetSignalChannel(ctx, SignalRollback),
		st: Status{MigrationID: in.MigrationID, PlanName: in.Plan.Name, State: StateRunning,
			Primary: ops.Source, Events: []Event{}},
	}
	for _, s := range in.Plan.RoutedServices() {
		r.st.Routed = append(r.st.Routed, s.Name)
	}
	err := workflow.SetQueryHandler(ctx, QueryStatus, func() (Status, error) { return r.st, nil })
	return r, err
}

func (r *runner) event(kind, actor, msg string) {
	r.st.Events = append(r.st.Events, Event{At: workflow.Now(r.ctx), Phase: r.st.Phase, Kind: kind, Actor: actor, Message: msg})
	if n := len(r.st.Events); n > maxEvents {
		r.st.Events = slices.Clone(r.st.Events[n-maxEvents:])
	}
}

func (r *runner) enter(phase string) {
	r.st.Phase = phase
	r.event("phase-started", "", "")
}

func (r *runner) pushUndo(run func() error) {
	r.undo = append(r.undo, undoStep{phase: r.st.Phase, run: run})
}

// exec runs one activity with the default policy: five attempts, backing off to 30s.
func (r *runner) exec(timeout time.Duration, fn, in, out any) error {
	return r.execWith(timeout, 0, fn, in, out)
}

// heldHeartbeat is the heartbeat timeout of the steps that run while
// PgBouncer holds writes; their activities heartbeat every 500ms, so a dead
// worker is noticed in seconds rather than at StartToClose.
const heldHeartbeat = 2 * time.Second

// execHeld is exec for a step that runs while writes are held.
func (r *runner) execHeld(timeout time.Duration, fn, in, out any) error {
	return r.execWith(timeout, heldHeartbeat, fn, in, out)
}

func (r *runner) execWith(timeout, heartbeat time.Duration, fn, in, out any) error {
	ctx := workflow.WithActivityOptions(r.ctx, workflow.ActivityOptions{
		StartToCloseTimeout: timeout,
		HeartbeatTimeout:    heartbeat,
		RetryPolicy: &temporal.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 2,
			MaximumInterval: 30 * time.Second, MaximumAttempts: 5},
	})
	return workflow.ExecuteActivity(ctx, fn, in).Get(r.ctx, out)
}

// sleep waits d of workflow time. A rollback signal ends it early with
// errRollback (unless rollback is closed, when it is recorded and
// ignored). A cancelled workflow cancels the timer, which is returned as
// the error, never read as the wait ending.
func (r *runner) sleep(d time.Duration) error {
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

func message(err error) string {
	var app *temporal.ApplicationError
	if errors.As(err, &app) {
		return app.Error()
	}
	var to *temporal.TimeoutError
	if errors.As(err, &to) {
		return "timed out (" + to.TimeoutType().String() + ")"
	}
	return err.Error()
}

func (r *runner) takeRollback(rb RollbackSignal) error {
	if r.closed != "" {
		r.event("rollback-ignored", rb.ApproverID, r.closed)
		return nil
	}
	r.event("rollback-requested", rb.ApproverID, rb.Reason)
	return errRollback
}

// pollRollback consumes pending rollback signals without blocking.
func (r *runner) pollRollback() error {
	for {
		var rb RollbackSignal
		if !r.rbCh.ReceiveAsync(&rb) {
			return nil
		}
		if err := r.takeRollback(rb); err != nil {
			return err
		}
	}
}

// gate waits until `needs` different approvers approve gate `name`. A
// rejection or a rollback signal returns errRollback. Signals that arrived
// while no gate was open are discarded first: an approval only counts for
// a gate the approver could see.
func (r *runner) gate(name, summary string, needs int) error {
	if err := r.ctx.Err(); err != nil {
		return err
	}
	for {
		var s GateSignal
		if !r.gateCh.ReceiveAsync(&s) {
			break
		}
		r.event("signal-ignored", s.ApproverID, fmt.Sprintf("signal for %q arrived while no gate was open", s.Gate))
	}
	if err := r.pollRollback(); err != nil {
		return err
	}
	g := &Gate{Name: name, Phase: r.st.Phase, Needs: needs, Summary: summary, Approvals: []string{}}
	prev := r.st.State
	r.st.Gate, r.st.State = g, StateWaiting
	r.event("gate-opened", "", name+": "+summary)
	defer func() { r.st.Gate, r.st.State = nil, prev }()
	for {
		var sig GateSignal
		var rb *RollbackSignal
		sel := workflow.NewSelector(r.ctx)
		sel.AddReceive(r.ctx.Done(), func(workflow.ReceiveChannel, bool) {})
		sel.AddReceive(r.gateCh, func(c workflow.ReceiveChannel, _ bool) { c.Receive(r.ctx, &sig) })
		sel.AddReceive(r.rbCh, func(c workflow.ReceiveChannel, _ bool) {
			var s RollbackSignal
			c.Receive(r.ctx, &s)
			rb = &s
		})
		sel.Select(r.ctx)
		if err := r.ctx.Err(); err != nil {
			return err // cancelled while waiting
		}
		if rb != nil {
			if err := r.takeRollback(*rb); err != nil {
				return err
			}
			continue
		}
		switch {
		case sig.Gate != name:
			r.event("signal-ignored", sig.ApproverID, fmt.Sprintf("signal for gate %q while %q is open", sig.Gate, name))
		case sig.ApproverID == "":
			r.event("signal-ignored", "", "signal without an approver")
		case sig.Decision == Reject:
			if r.closed != "" {
				r.event("reject-ignored", sig.ApproverID, r.closed)
				continue
			}
			r.event("rejected", sig.ApproverID, sig.Note)
			return errRollback
		case sig.Decision != Approve:
			r.event("signal-ignored", sig.ApproverID, fmt.Sprintf("unknown decision %q", sig.Decision))
		case slices.Contains(g.Approvals, sig.ApproverID):
			r.event("approval-ignored", sig.ApproverID, "a second approval must come from a different account")
		default:
			g.Approvals = append(g.Approvals, sig.ApproverID)
			r.event("approved", sig.ApproverID, sig.Note)
			if len(g.Approvals) >= needs {
				return nil
			}
		}
	}
}

// step runs fn. If it fails, the migration stops at a retry gate that
// explains why: approve retries, rollback unwinds. It never proceeds on a guess.
func (r *runner) step(name string, fn func() error) error {
	for {
		if err := r.pollRollback(); err != nil {
			return err
		}
		err := fn()
		// A cancelled workflow is not a failed step: no retry gate opens.
		if err == nil || errors.Is(err, errRollback) || r.ctx.Err() != nil {
			return err
		}
		msg := message(err)
		r.event("step-failed", "", name+": "+msg)
		// Once rollback is closed (past Finish, or already rolling back),
		// the gate offers only a retry.
		next := "Approve to retry, or roll back."
		if r.closed != "" {
			next = "Approve to retry."
		}
		if err := r.gate("retry-"+name, "Step "+name+" failed: "+msg+". "+next, 1); err != nil {
			return err
		}
	}
}

// must retries a compensation until it succeeds. Compensations restore a
// safe state, so giving up is not an option; each failure is an event.
func (r *runner) must(fn func() error) error {
	for {
		err := fn()
		if err == nil {
			return nil
		}
		r.event("compensation-failed", "", message(err)+"; retrying in 10s")
		if err := workflow.Sleep(r.ctx, 10*time.Second); err != nil {
			return err
		}
	}
}
