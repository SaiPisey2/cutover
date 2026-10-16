package flow

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/SaiPisey2/cutover/internal/ops"
	"github.com/SaiPisey2/cutover/internal/pg"
	"github.com/SaiPisey2/cutover/internal/plan"
)

func runSwitch(t *testing.T, p plan.Plan, back bool, f fails, drainVisible bool) (switchResult, *calls) {
	t.Helper()
	env := newEnv()
	c := &calls{}
	mockAll(env, c, f, drainVisible)
	env.ExecuteWorkflow(switchWorkflow, p, back)
	if !env.IsWorkflowCompleted() || env.GetWorkflowError() != nil {
		t.Fatalf("completed %v err %v", env.IsWorkflowCompleted(), env.GetWorkflowError())
	}
	var res switchResult
	if err := env.GetWorkflowResult(&res); err != nil {
		t.Fatal(err)
	}
	return res, c
}

func assertCalls(t *testing.T, got, want []string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("calls\n got %q\nwant %q", got, want)
	}
}

func hasEvent(evs []Event, kind string) bool {
	return slices.ContainsFunc(evs, func(e Event) bool { return e.Kind == kind })
}

// held returns the time from switch-started to switch-aborted, and the
// abort message.
func held(t *testing.T, evs []Event) (time.Duration, string) {
	t.Helper()
	i := slices.IndexFunc(evs, func(e Event) bool { return e.Kind == "switch-started" })
	j := slices.IndexFunc(evs, func(e Event) bool { return e.Kind == "switch-aborted" })
	if i < 0 || j < 0 {
		t.Fatalf("no switch-started/switch-aborted: %+v", evs)
	}
	return evs[j].At.Sub(evs[i].At), evs[j].Message
}

func TestSwitchOrder(t *testing.T) {
	res, c := runSwitch(t, testPlan(), false, nil, true)
	assertCalls(t, c.list(), []string{"pause", "lag:fwd", "drop-sub:fwd", "seq:source>target",
		"start:rev", "repoint:target", "resume", "stop:fwd"})
	if !res.OK || res.Primary != ops.Target || !hasEvent(res.Events, "switched") {
		t.Fatalf("%+v", res)
	}
}

func TestSwitchBackOrder(t *testing.T) {
	res, c := runSwitch(t, testPlan(), true, nil, true)
	assertCalls(t, c.list(), []string{"pause", "lag:rev", "drop-sub:rev", "seq:target>source",
		"start:fwd", "repoint:source", "resume", "stop:rev"})
	if !res.OK || res.Primary != ops.Source {
		t.Fatalf("%+v", res)
	}
}

func TestSwitchAbortsWhenDrainTooSlow(t *testing.T) {
	res, c := runSwitch(t, testPlan(), false, nil, false)
	assertCalls(t, c.list(), []string{"pause", "lag:fwd", "resume"})
	if res.OK || res.Primary != ops.Source || !hasEvent(res.Events, "switch-aborted") {
		t.Fatalf("%+v", res)
	}
}

func TestSwitchAbortsWhenPauseTimesOut(t *testing.T) {
	res, c := runSwitch(t, testPlan(), false, fails{"pause": boom}, true)
	assertCalls(t, c.list(), []string{"pause", "resume"})
	if res.OK || res.Primary != ops.Source {
		t.Fatalf("%+v", res)
	}
}

func TestSwitchCompensatesWhenRepointFails(t *testing.T) {
	res, c := runSwitch(t, testPlan(), false, fails{"repoint:target": boom}, true)
	assertCalls(t, c.list(), []string{"pause", "lag:fwd", "drop-sub:fwd", "seq:source>target",
		"start:rev", "repoint:target",
		"repoint:source", "stop:rev", "start:fwd", "resume"})
	if res.OK || res.Primary != ops.Source {
		t.Fatalf("%+v", res)
	}
}

func TestSwitchCompensatesWhenReverseFails(t *testing.T) {
	_, c := runSwitch(t, testPlan(), false, fails{"start:rev": boom}, true)
	assertCalls(t, c.list(), []string{"pause", "lag:fwd", "drop-sub:fwd", "seq:source>target",
		"start:rev", "stop:rev", "start:fwd", "resume"})
}

func TestSwitchCompensationsAreBounded(t *testing.T) {
	flaky := temporal.NewApplicationError("flaky", "test")
	res, c := runSwitch(t, testPlan(), false, fails{"start:rev": boom, "stop:rev": flaky}, true)
	// stop:rev is retryable but bounded to 3 attempts. Once it gives up the
	// reverse link may still be live, so the forward link is NOT restarted
	// (both live would loop on Postgres 12–15); writes still resume last.
	assertCalls(t, c.list(), []string{"pause", "lag:fwd", "drop-sub:fwd", "seq:source>target",
		"start:rev", "stop:rev", "stop:rev", "stop:rev", "resume"})
	if res.OK || res.Primary != ops.Source || !hasEvent(res.Events, "compensation-failed") ||
		!hasEvent(res.Events, "replication-broken") || !res.Broken || !res.StBroken {
		t.Fatalf("%+v", res)
	}
}

func TestSwitchAbortsWhenBudgetSpent(t *testing.T) {
	p := testPlan()
	p.Thresholds.Switch.MaxPause = plan.Duration(2 * time.Second)
	env := newEnv()
	env.SetTestTimeout(30 * time.Second)
	c := &calls{}
	var a *ops.Activities
	// The test environment does not time out a mock delayed with .After (it
	// waits on the mock clock and returns), so this one runs for real time,
	// heartbeating so that only the budget can stop it.
	env.OnActivity(a.SyncSequences, mockAny, mockAny).Return(
		func(ctx context.Context, _ ops.SeqIn) ([]pg.SequenceSync, error) {
			end := time.After(3 * time.Second)
			for {
				select {
				case <-end:
					return nil, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(200 * time.Millisecond):
					activity.RecordHeartbeat(ctx)
				}
			}
		}).Once()
	mockAll(env, c, nil, true)
	env.ExecuteWorkflow(switchWorkflow, p, false)
	var res switchResult
	if err := env.GetWorkflowResult(&res); err != nil {
		t.Fatal(err)
	}
	if res.OK || !hasEvent(res.Events, "switch-aborted") {
		t.Fatalf("a slow step must abort the switch: %+v", res)
	}
	got := c.list()
	if got[len(got)-1] != "resume" || slices.Contains(got, "repoint:target") {
		t.Fatalf("calls %q", got)
	}
	// The slow activity itself is cut off at the budget, not waited for.
	if _, msg := held(t, res.Events); !strings.HasPrefix(msg, "sequences: timed out") {
		t.Fatalf("the slow step itself must be cut off at the budget: %s", msg)
	}
}

func TestSwitchAbortsOnGlobalPause(t *testing.T) {
	global := temporal.NewNonRetryableApplicationError("pgbouncer is globally paused; resume it first", ops.ErrTypePermanent, nil)
	res, c := runSwitch(t, testPlan(), false, fails{"pause": global}, true)
	assertCalls(t, c.list(), []string{"pause", "resume"})
	if res.OK || res.Primary != ops.Source {
		t.Fatalf("%+v", res)
	}
	i := slices.IndexFunc(res.Events, func(e Event) bool { return e.Kind == "switch-aborted" })
	if i < 0 || !strings.Contains(res.Events[i].Message, "globally paused") {
		t.Fatalf("the global pause must be surfaced: %+v", res.Events)
	}
}

// A pause that keeps timing out is retried, but never past max_pause: the
// activity is bounded by the remaining budget, then the pool is resumed.
func TestSwitchPauseRetriesStayInsideBudget(t *testing.T) {
	stuck := temporal.NewApplicationError("pause timed out", ops.ErrTypeTransient)
	p := testPlan()
	p.Thresholds.Switch.MaxPause = plan.Duration(3 * time.Second)
	res, c := runSwitch(t, p, false, fails{"pause": stuck}, true)
	got := c.list()
	if got[len(got)-1] != "resume" || c.count("lag:fwd") != 0 || c.count("pause") < 2 {
		t.Fatalf("calls %q", got)
	}
	if res.OK {
		t.Fatalf("%+v", res)
	}
	if d, _ := held(t, res.Events); d > p.Thresholds.Switch.MaxPause.D() {
		t.Fatalf("pause retried for %s, past the budget", d)
	}
}

// gateWorkflow opens gate a, then (after a minute with no gate open) gate b.
func gateWorkflow(ctx workflowContext) (Result, error) {
	r, err := newRunner(ctx, Input{MigrationID: mid, Plan: testPlan()})
	if err != nil {
		return Result{}, err
	}
	if err := r.gate("a", "first", 1); err != nil {
		return Result{}, err
	}
	if err := workflow.Sleep(ctx, time.Minute); err != nil {
		return Result{}, err
	}
	if err := r.gate("b", "second", 1); err != nil {
		return Result{}, err
	}
	return Result{State: StateFinished, Events: r.st.Events}, nil
}

func TestGateDiscardsStaleSignals(t *testing.T) {
	env := newEnv()
	env.RegisterWorkflow(gateWorkflow)
	sig := func(at time.Duration, gate, who string) {
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(SignalGate, GateSignal{Gate: gate, Decision: Approve, ApproverID: who})
		}, at)
	}
	sig(time.Second, "a", "alice")
	sig(30*time.Second, "b", "mallory") // no gate open: must not count for b
	sig(2*time.Minute, "b", "alice")
	env.ExecuteWorkflow(gateWorkflow)
	var res Result
	if err := env.GetWorkflowResult(&res); err != nil {
		t.Fatal(err)
	}
	var approvers []string
	stale := false
	for _, e := range res.Events {
		if e.Kind == "approved" {
			approvers = append(approvers, e.Actor)
		}
		if e.Kind == "signal-ignored" && e.Actor == "mallory" {
			stale = true
		}
	}
	if !stale || !slices.Equal(approvers, []string{"alice", "alice"}) {
		t.Fatalf("approvers %q, events %+v", approvers, res.Events)
	}
}

// Repoint-back and resume never give up: resuming while PgBouncer might
// still route to the target would send writes where Primary does not say.
func TestSwitchRepointBackAndResumeRetryUntilDone(t *testing.T) {
	env := newEnv()
	c := &calls{}
	var a *ops.Activities
	back := mock.MatchedBy(func(in ops.RepointIn) bool { return in.To == ops.Source })
	env.OnActivity(a.Repoint, mockAny, back).Return(
		func(context.Context, ops.RepointIn) error { c.add("repoint:source"); return boom }).Times(2)
	env.OnActivity(a.ResumePool, mockAny, mockAny).Return(
		func(context.Context, ops.PoolIn) error { c.add("resume"); return boom }).Times(2)
	mockAll(env, c, fails{"repoint:target": boom}, true)
	env.ExecuteWorkflow(switchWorkflow, testPlan(), false)
	var res switchResult
	if err := env.GetWorkflowResult(&res); err != nil {
		t.Fatal(err)
	}
	// The forward-path resume is never reached (repoint failed first), so
	// both failing resumes are compensations.
	assertCalls(t, c.since("repoint:target"), []string{"repoint:target",
		"repoint:source", "repoint:source", "repoint:source", "stop:rev", "start:fwd",
		"resume", "resume", "resume"})
	if res.OK || res.Primary != ops.Source {
		t.Fatalf("%+v", res)
	}
}

// Clean aborts are not errors and leave replication intact.
func TestSwitchCleanAbortIsNotBroken(t *testing.T) {
	res, _ := runSwitch(t, testPlan(), false, fails{"start:rev": boom}, true)
	if res.OK || res.Broken || res.StBroken || hasEvent(res.Events, "replication-broken") {
		t.Fatalf("%+v", res)
	}
}

// After a successful repoint the switch has happened: a failing resume is
// retried until it works, never compensated.
func TestSwitchNeverAbortsAfterRepoint(t *testing.T) {
	env := newEnv()
	c := &calls{}
	var a *ops.Activities
	env.OnActivity(a.ResumePool, mockAny, mockAny).Return(
		func(context.Context, ops.PoolIn) error { c.add("resume"); return boom }).Times(2)
	mockAll(env, c, nil, true)
	env.ExecuteWorkflow(switchWorkflow, testPlan(), false)
	var res switchResult
	if err := env.GetWorkflowResult(&res); err != nil {
		t.Fatal(err)
	}
	assertCalls(t, c.list(), []string{"pause", "lag:fwd", "drop-sub:fwd", "seq:source>target",
		"start:rev", "repoint:target", "resume", "resume", "resume", "stop:fwd"})
	if !res.OK || res.Primary != ops.Target || hasEvent(res.Events, "switch-aborted") {
		t.Fatalf("%+v", res)
	}
}

// The drain may not use the whole budget: the steps after it keep a reserve
// of max(1s, max_pause/4).
func TestSwitchDrainLeavesReserve(t *testing.T) {
	env := newEnv()
	c := &calls{}
	var a *ops.Activities
	var got time.Duration
	env.OnActivity(a.MeasureLag, mockAny, mockAny).Return(
		func(_ context.Context, in ops.LagIn) (ops.LagOut, error) {
			c.add("lag:" + short(in.Link))
			got = in.Timeout
			return ops.LagOut{Visible: true}, nil
		}).Once()
	mockAll(env, c, nil, true)
	p := testPlan() // max_pause 10s: reserve 2.5s
	env.ExecuteWorkflow(switchWorkflow, p, false)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	max := p.Thresholds.Switch.MaxPause.D()
	if got <= 0 || got > max-max/4 {
		t.Fatalf("drain timeout %s must leave a %s reserve of %s", got, max/4, max)
	}
}

// An aborted switch must not report sequences as synced.
func TestSwitchAbortClearsSequences(t *testing.T) {
	res, _ := runSwitch(t, testPlan(), false, fails{"start:rev": boom}, true)
	if len(res.Sequences) != 0 {
		t.Fatalf("%+v", res.Sequences)
	}
	res, _ = runSwitch(t, testPlan(), false, nil, true)
	if len(res.Sequences) != 1 {
		t.Fatalf("a switch reports its sequences: %+v", res.Sequences)
	}
}

func TestSwitchCompensatesWhenDropSubFails(t *testing.T) {
	_, c := runSwitch(t, testPlan(), false, fails{"drop-sub:fwd": boom}, true)
	assertCalls(t, c.list(), []string{"pause", "lag:fwd", "drop-sub:fwd", "start:fwd", "resume"})
}

func TestSwitchCompensatesWhenSequencesFail(t *testing.T) {
	_, c := runSwitch(t, testPlan(), false, fails{"seq:source>target": boom}, true)
	assertCalls(t, c.list(), []string{"pause", "lag:fwd", "drop-sub:fwd", "seq:source>target", "start:fwd", "resume"})
}

// The smallest budget still switches: max_pause 1s leaves the drain about
// 750ms (the reserve is max(250ms, max_pause/4)).
func TestSwitchOneSecondBudget(t *testing.T) {
	env := newEnv()
	c := &calls{}
	var a *ops.Activities
	var got time.Duration
	env.OnActivity(a.MeasureLag, mockAny, mockAny).Return(
		func(_ context.Context, in ops.LagIn) (ops.LagOut, error) {
			c.add("lag:" + short(in.Link))
			got = in.Timeout
			return ops.LagOut{Visible: true}, nil
		}).Once()
	mockAll(env, c, nil, true)
	p := testPlan()
	p.Thresholds.Switch.MaxPause = plan.Duration(time.Second)
	env.ExecuteWorkflow(switchWorkflow, p, false)
	var res switchResult
	if err := env.GetWorkflowResult(&res); err != nil {
		t.Fatal(err)
	}
	if got < 600*time.Millisecond || got > 750*time.Millisecond {
		t.Fatalf("drain timeout %s, want about 750ms", got)
	}
	assertCalls(t, c.list(), []string{"pause", "lag:fwd", "drop-sub:fwd", "seq:source>target",
		"start:rev", "repoint:target", "resume", "stop:fwd"})
	if !res.OK || res.Primary != ops.Target {
		t.Fatalf("%+v", res)
	}
}
