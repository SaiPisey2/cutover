package flow

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/SaiPisey2/cutover/internal/ops"
	"github.com/SaiPisey2/cutover/internal/pg"
	"github.com/SaiPisey2/cutover/internal/readiness"
)

func start(t *testing.T, env *testsuite.TestWorkflowEnvironment, actions []action) Result {
	drive(t, env, actions)
	env.ExecuteWorkflow(Migration, Input{MigrationID: mid, Plan: testPlan()})
	return result(t, env)
}

func TestHappyPath(t *testing.T) {
	env := newEnv()
	c := &calls{}
	mockAll(env, c, nil, true)
	r := start(t, env, happy())
	if r.State != StateFinished {
		t.Fatalf("state %s: %q", r.State, kinds(r.Events))
	}
	// Shape: everything before replication, then the switch, then finish.
	tail := c.since("pause")
	assertCalls(t, tail, []string{"pause", "lag:fwd", "drop-sub:fwd", "seq:source>target", "start:rev",
		"repoint:target", "resume", "stop:fwd", "stop:rev", "cleanup"})
	head := c.list()[:3]
	assertCalls(t, head, []string{"discover", "prepare", "start:fwd+copy"})
	if c.count("check-schema") != 2 || c.count("compare") != 1 {
		t.Fatalf("schema checks before rehearse and before switch: %q", c.list())
	}
	if !slices.Contains(kinds(r.Events), "skipped") {
		t.Fatal("shift must be recorded as skipped")
	}
}

func TestSwitchNeedsTwoDifferentApprovers(t *testing.T) {
	env := newEnv()
	c := &calls{}
	mockAll(env, c, nil, true)
	again := action{
		when: func(s Status) bool { return s.Gate != nil && s.Gate.Name == "switch" && len(s.Gate.Approvals) == 1 },
		do: func(env *testsuite.TestWorkflowEnvironment) {
			env.SignalWorkflow(SignalGate, GateSignal{Gate: "switch", Decision: Approve, ApproverID: "alice"})
		},
	}
	stillWaiting := action{
		when: func(s Status) bool {
			if s.Gate == nil || s.Gate.Name != "switch" {
				t.Errorf("alice's second approval passed the gate: %+v", s.Gate)
			}
			return true
		},
		do: func(*testsuite.TestWorkflowEnvironment) {},
	}
	steps := []action{approve("discover", "alice"), approve("prepare", "alice"), approve("rehearse", "alice"),
		approve("switch", "alice"), again, stillWaiting, approve("switch", "bob"), approve("finish", "alice")}
	r := start(t, env, steps)
	if r.State != StateFinished || !slices.Contains(kinds(r.Events), "approval-ignored") {
		t.Fatalf("%s %q", r.State, kinds(r.Events))
	}
}

func TestStaleApprovalDoesNotApproveLaterGate(t *testing.T) {
	env := newEnv()
	c := &calls{}
	mockAll(env, c, nil, true)
	early := func(gate string, when func(Status) bool) action {
		return action{when: when, do: func(env *testsuite.TestWorkflowEnvironment) {
			env.SignalWorkflow(SignalGate, GateSignal{Gate: gate, Decision: Approve, ApproverID: "mallory"})
		}}
	}
	atDiscover := func(s Status) bool { return s.Gate != nil && s.Gate.Name == "discover" }
	inReplicate := func(s Status) bool { return s.Phase == PhaseReplicate && s.Gate == nil }
	rehearseOpen := func(s Status) bool {
		if s.Gate != nil && s.Gate.Name == "rehearse" && len(s.Gate.Approvals) > 0 {
			t.Errorf("a signal sent before the gate opened was counted: %+v", s.Gate)
		}
		return s.Gate != nil && s.Gate.Name == "rehearse"
	}
	steps := []action{
		early("prepare", atDiscover), approve("discover", "alice"), approve("prepare", "alice"),
		early("rehearse", inReplicate),
		{when: rehearseOpen, do: func(*testsuite.TestWorkflowEnvironment) {}},
		approve("rehearse", "alice"), approve("switch", "alice"), approve("switch", "bob"), approve("finish", "alice"),
	}
	r := start(t, env, steps)
	ignored := 0
	for _, e := range r.Events {
		if e.Kind == "signal-ignored" && e.Actor == "mallory" {
			ignored++
		}
	}
	if r.State != StateFinished || ignored != 2 {
		t.Fatalf("%s, %d ignored: %q", r.State, ignored, kinds(r.Events))
	}
}

func TestBlockedReadinessRechecks(t *testing.T) {
	env := newEnv()
	c := &calls{}
	var a *ops.Activities
	env.OnActivity(a.Discover, mockAny, mockAny).Return(func(context.Context, ops.DiscoverIn) (readiness.Report, error) {
		c.add("discover")
		return readiness.Report{Tables: []string{"public.orders"},
			Findings: []readiness.Finding{{Severity: readiness.Blocker, Code: "wal-level"}}}, nil
	}).Once()
	mockAll(env, c, nil, true)
	steps := append([]action{approve("discover", "alice")}, happy()...)
	r := start(t, env, steps)
	if r.State != StateFinished || c.count("discover") != 2 {
		t.Fatalf("%s, discover x%d", r.State, c.count("discover"))
	}
	// Prepare must never run while readiness reports blockers.
	assertCalls(t, c.list()[:3], []string{"discover", "discover", "prepare"})
}

func TestRejectAtRehearseUnwinds(t *testing.T) {
	env := newEnv()
	c := &calls{}
	mockAll(env, c, nil, true)
	steps := []action{approve("discover", "alice"), approve("prepare", "alice"), atGate("rehearse", "alice", Reject)}
	r := start(t, env, steps)
	if r.State != StateRolledBack {
		t.Fatalf("state %s", r.State)
	}
	assertCalls(t, c.since("compare")[1:], []string{"stop:fwd", "undo-prepare"})
	if c.count("pause") != 0 {
		t.Fatal("no switch before rehearse approval")
	}
}

func TestRollbackDuringWatchSwitchesBack(t *testing.T) {
	env := newEnv()
	c := &calls{}
	mockAll(env, c, nil, true)
	rollback := action{
		when: func(s Status) bool { return s.Phase == PhaseFinish && s.Gate == nil },
		do: func(env *testsuite.TestWorkflowEnvironment) {
			env.SignalWorkflow(SignalRollback, RollbackSignal{ApproverID: "alice", Reason: "p99 regressed"})
		},
	}
	steps := append(happy()[:5], rollback)
	r := start(t, env, steps)
	if r.State != StateRolledBack {
		t.Fatalf("state %s %q", r.State, kinds(r.Events))
	}
	back := c.since("stop:fwd")[1:] // after the forward switch's publication drop
	assertCalls(t, back, []string{"pause", "lag:rev", "drop-sub:rev", "seq:target>source", "start:fwd",
		"repoint:source", "resume", "stop:rev", "stop:fwd", "undo-prepare"})
}

func TestRollbackIgnoredAfterFinishApproved(t *testing.T) {
	env := newEnv()
	c := &calls{}
	var a *ops.Activities
	env.OnActivity(a.Cleanup, mockAny, mockAny).Return(func(context.Context, ops.TablesIn) error {
		c.add("cleanup")
		return boom
	}).Once()
	mockAll(env, c, nil, true)
	late := action{
		when: func(s Status) bool { return s.Gate != nil && s.Gate.Name == "retry-finish" },
		do: func(env *testsuite.TestWorkflowEnvironment) {
			env.SignalWorkflow(SignalRollback, RollbackSignal{ApproverID: "alice", Reason: "too late"})
		},
	}
	steps := append(happy(), late, approve("retry-finish", "alice"))
	r := start(t, env, steps)
	if r.State != StateFinished || !slices.Contains(kinds(r.Events), "rollback-ignored") || c.count("undo-prepare") != 0 {
		t.Fatalf("%s %q", r.State, kinds(r.Events))
	}
}

func TestFailedStepWaitsAtRetryGate(t *testing.T) {
	env := newEnv()
	c := &calls{}
	var a *ops.Activities
	env.OnActivity(a.PrepareSchema, mockAny, mockAny).Return(func(context.Context, ops.TablesIn) error {
		c.add("prepare")
		return boom
	}).Once()
	mockAll(env, c, nil, true)
	steps := append([]action{approve("discover", "alice"), approve("retry-prepare", "alice")}, happy()[1:]...)
	r := start(t, env, steps)
	if r.State != StateFinished || c.count("prepare") != 2 {
		t.Fatalf("%s prepare x%d", r.State, c.count("prepare"))
	}
}

func TestSchemaDriftBlocksSwitch(t *testing.T) {
	env := newEnv()
	c := &calls{}
	var a *ops.Activities
	env.OnActivity(a.CheckSchema, mockAny, mockAny).Return(func(context.Context, ops.TablesIn) ([]string, error) {
		c.add("check-schema")
		return nil, nil
	}).Once()
	env.OnActivity(a.CheckSchema, mockAny, mockAny).Return(func(context.Context, ops.TablesIn) ([]string, error) {
		c.add("check-schema")
		return []string{"public.orders.extra: only on source"}, nil
	}).Once()
	mockAll(env, c, nil, true)
	steps := []action{approve("discover", "alice"), approve("prepare", "alice"), approve("rehearse", "alice"),
		approve("retry-schema-check", "alice"), approve("switch", "alice"), approve("switch", "bob"), approve("finish", "alice")}
	r := start(t, env, steps)
	if r.State != StateFinished || c.count("check-schema") != 3 {
		t.Fatalf("%s check x%d", r.State, c.count("check-schema"))
	}
	if i := slices.Index(c.list(), "pause"); i < 0 || slices.Index(c.list()[i:], "check-schema") >= 0 {
		t.Fatal("the drift must be resolved before the switch pauses writes")
	}
}

// A clean abort leaves everything as it was: the schema is checked again
// and the switch gate reopens for two fresh approvals.
func TestCleanSwitchAbortReopensSwitchGate(t *testing.T) {
	env := newEnv()
	c := &calls{}
	var a *ops.Activities
	rev := mock.MatchedBy(func(in ops.LinkIn) bool { return in.Link == ops.Reverse(mid) })
	env.OnActivity(a.StartLink, mockAny, rev).Return(func(context.Context, ops.LinkIn) error {
		c.add("start:rev")
		return boom
	}).Once()
	mockAll(env, c, nil, true)
	steps := append(happy()[:5], approve("switch", "alice"), approve("switch", "bob"), approve("finish", "alice"))
	r := start(t, env, steps)
	if r.State != StateFinished || c.count("pause") != 2 || c.count("check-schema") != 3 {
		t.Fatalf("%s pause x%d check x%d: %q", r.State, c.count("pause"), c.count("check-schema"), kinds(r.Events))
	}
	if slices.Contains(kinds(r.Events), "step-failed") {
		t.Fatalf("a clean abort is not a failed step: %q", kinds(r.Events))
	}
}

// A switch whose abort could not stop the reverse link leaves replication
// broken. No second switch is attempted; the only way on is rollback, which
// stops the reverse link before the rest of the unwind.
func TestBrokenReplicationOnlyRollsBack(t *testing.T) {
	env := newEnv()
	c := &calls{}
	var a *ops.Activities
	rev := mock.MatchedBy(func(in ops.LinkIn) bool { return in.Link == ops.Reverse(mid) })
	env.OnActivity(a.StopLink, mockAny, rev).Return(func(context.Context, ops.LinkIn) error {
		c.add("stop:rev")
		return boom
	}).Once()
	mockAll(env, c, fails{"start:rev": boom}, true)
	// Approving the broken gate is the rollback; the extra switch approvals
	// must never be needed.
	steps := append(happy()[:5], approve("replication-broken", "alice"))
	r := start(t, env, steps)
	if r.State != StateRolledBack || c.count("pause") != 1 || !slices.Contains(kinds(r.Events), "replication-broken") {
		t.Fatalf("%s pause x%d: %q", r.State, c.count("pause"), kinds(r.Events))
	}
	assertCalls(t, c.since("pause"), []string{"pause", "lag:fwd", "drop-sub:fwd", "seq:source>target", "start:rev",
		"stop:rev", "resume", "stop:rev", "stop:fwd", "undo-prepare"})
	if slices.ContainsFunc(r.Events, func(e Event) bool { return e.Kind == "gate-opened" && strings.HasPrefix(e.Message, "retry-switch") }) {
		t.Fatal("a broken switch must not be retryable")
	}
}

// A rollback that arrives during the switch is read after it: switched,
// then switched back.
func TestRollbackDuringSwitchSwitchesBack(t *testing.T) {
	env := newEnv()
	c := &calls{}
	var a *ops.Activities
	env.OnActivity(a.SyncSequences, mockAny, mockAny).Return(func(context.Context, ops.SeqIn) ([]pg.SequenceSync, error) {
		c.add("seq:source>target")
		env.SignalWorkflow(SignalRollback, RollbackSignal{ApproverID: "alice", Reason: "abort"})
		return nil, nil
	}).Once()
	mockAll(env, c, nil, true)
	r := start(t, env, happy()[:5])
	if r.State != StateRolledBack {
		t.Fatalf("state %s %q", r.State, kinds(r.Events))
	}
	assertCalls(t, c.since("repoint:target"), []string{"repoint:target", "resume",
		"pause", "lag:rev", "drop-sub:rev", "seq:target>source", "start:fwd", "repoint:source", "resume",
		"stop:rev", "stop:fwd", "undo-prepare"})
}

func status(t *testing.T, env *testsuite.TestWorkflowEnvironment) Status {
	t.Helper()
	v, err := env.QueryWorkflow(QueryStatus)
	if err != nil {
		t.Fatal(err)
	}
	var st Status
	if err := v.Get(&st); err != nil {
		t.Fatal(err)
	}
	return st
}

func cancelled(t *testing.T, env *testsuite.TestWorkflowEnvironment, actions []action) Status {
	t.Helper()
	env.SetTestTimeout(30 * time.Second)
	drive(t, env, actions)
	env.ExecuteWorkflow(Migration, Input{MigrationID: mid, Plan: testPlan()})
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); !temporal.IsCanceledError(err) {
		t.Fatalf("want a cancellation, got %v", err)
	}
	return status(t, env)
}

func cancelWhen(when func(Status) bool) action {
	return action{when: when, do: func(env *testsuite.TestWorkflowEnvironment) { env.CancelWorkflow() }}
}

// Cancelled right after the repoint: the switch happened, but the
// cancellation is not a failed step and opens no gate.
func TestCancelAfterSwitchOpensNoGate(t *testing.T) {
	env := newEnv()
	c := &calls{}
	var a *ops.Activities
	env.OnActivity(a.ResumePool, mockAny, mockAny).Return(func(context.Context, ops.PoolIn) error {
		c.add("resume")
		env.CancelWorkflow()
		return nil
	}).Once()
	mockAll(env, c, nil, true)
	st := cancelled(t, env, happy()[:5])
	for _, l := range c.since("repoint:target")[1:] {
		if l != "resume" {
			t.Fatalf("nothing but resume may run after a cancelled switch: %q", c.list())
		}
	}
	after := st.Events[slices.IndexFunc(st.Events, func(e Event) bool { return e.Kind == "switch-started" }):]
	if slices.ContainsFunc(after, func(e Event) bool { return e.Kind == "step-failed" || e.Kind == "gate-opened" }) {
		t.Fatalf("cancellation fed into a step or gate: %q", kinds(after))
	}
	if st.Primary != ops.Target || !slices.Contains(kinds(st.Events), "cancelled") {
		t.Fatalf("primary %s: %q", st.Primary, kinds(st.Events))
	}
}

// Cancelled while waiting at a gate: the workflow ends instead of waiting
// forever.
func TestCancelAtGateEnds(t *testing.T) {
	env := newEnv()
	c := &calls{}
	mockAll(env, c, nil, true)
	cancelled(t, env, []action{cancelWhen(func(s Status) bool { return s.Gate != nil && s.Gate.Name == "discover" })})
	assertCalls(t, c.list(), []string{"discover"})
}

// Cancelled during the watch: the watch timer's cancellation must not read
// as the watch period ending.
func TestCancelDuringWatchDoesNotFinish(t *testing.T) {
	env := newEnv()
	c := &calls{}
	mockAll(env, c, nil, true)
	st := cancelled(t, env, append(happy()[:5], cancelWhen(func(s Status) bool { return s.Phase == PhaseFinish && s.Gate == nil })))
	if slices.ContainsFunc(st.Events, func(e Event) bool { return e.Kind == "gate-opened" && strings.HasPrefix(e.Message, "finish") }) {
		t.Fatalf("finish gate opened after cancel: %q", kinds(st.Events))
	}
	if c.count("cleanup") != 0 {
		t.Fatal("cleanup ran after cancel")
	}
}

// Cancelled while a step runs (here, the replication wait): not a failed
// step, so no retry gate.
func TestCancelDuringStepIsNotAFailure(t *testing.T) {
	env := newEnv()
	c := &calls{}
	mockAll(env, c, nil, true)
	steps := []action{approve("discover", "alice"), approve("prepare", "alice"),
		cancelWhen(func(s Status) bool { return s.Phase == PhaseReplicate && s.Gate == nil })}
	st := cancelled(t, env, steps)
	if slices.Contains(kinds(st.Events), "step-failed") || c.count("check-schema") != 0 {
		t.Fatalf("%q", kinds(st.Events))
	}
}

// --- fix round 1 ---

func rollbackInWatch() action {
	return action{
		when: func(s Status) bool { return s.Phase == PhaseFinish && s.Gate == nil },
		do: func(env *testsuite.TestWorkflowEnvironment) {
			env.SignalWorkflow(SignalRollback, RollbackSignal{ApproverID: "alice", Reason: "p99 regressed"})
		},
	}
}

// invisibleRev makes the switch-back drain miss the sentinel n times: each
// is a clean abort of the reverse switch.
func invisibleRev(env *testsuite.TestWorkflowEnvironment, c *calls, n int) {
	var a *ops.Activities
	rev := mock.MatchedBy(func(in ops.LagIn) bool { return in.Link == ops.Reverse(mid) })
	env.OnActivity(a.MeasureLag, mockAny, rev).Return(func(context.Context, ops.LagIn) (ops.LagOut, error) {
		c.add("lag:rev")
		return ops.LagOut{Visible: false}, nil
	}).Times(n)
}

func hasGate(evs []Event, prefix string) bool {
	return slices.ContainsFunc(evs, func(e Event) bool { return e.Kind == "gate-opened" && strings.HasPrefix(e.Message, prefix) })
}

func TestSwitchBackRetriesCleanAborts(t *testing.T) {
	env := newEnv()
	c := &calls{}
	invisibleRev(env, c, 2)
	mockAll(env, c, nil, true)
	r := start(t, env, append(happy()[:5], rollbackInWatch()))
	if r.State != StateRolledBack || c.count("pause") != 4 || hasGate(r.Events, "retry-switch-back") {
		t.Fatalf("%s pause x%d: %q", r.State, c.count("pause"), kinds(r.Events))
	}
}

func TestSwitchBackGateAfterThreeAborts(t *testing.T) {
	env := newEnv()
	c := &calls{}
	invisibleRev(env, c, 3)
	mockAll(env, c, nil, true)
	var summary string
	open := action{
		when: func(s Status) bool {
			if s.Gate != nil && s.Gate.Name == "retry-switch-back" {
				summary = s.Gate.Summary
				return true
			}
			return false
		},
		do: func(*testsuite.TestWorkflowEnvironment) {},
	}
	r := start(t, env, append(happy()[:5], rollbackInWatch(), open, approve("retry-switch-back", "alice")))
	if r.State != StateRolledBack || c.count("pause") != 5 {
		t.Fatalf("%s pause x%d: %q", r.State, c.count("pause"), kinds(r.Events))
	}
	if !strings.Contains(summary, "did not drain") || !strings.Contains(summary, "writes are on the target") {
		t.Fatalf("summary %q", summary)
	}
	assertCalls(t, c.since("repoint:source"), []string{"repoint:source", "resume", "stop:rev", "stop:fwd", "undo-prepare"})
}

func TestBrokenSwitchBackHaltsUnwind(t *testing.T) {
	env := newEnv()
	c := &calls{}
	var a *ops.Activities
	fwd := mock.MatchedBy(func(in ops.LinkIn) bool { return in.Link == ops.Forward(mid) })
	// The forward switch's publication drop succeeds; stopping the new
	// forward link during the switch-back's abort does not.
	env.OnActivity(a.StopLink, mockAny, fwd).Return(func(context.Context, ops.LinkIn) error {
		c.add("stop:fwd")
		return nil
	}).Once()
	env.OnActivity(a.StopLink, mockAny, fwd).Return(func(context.Context, ops.LinkIn) error {
		c.add("stop:fwd")
		return boom
	}).Once()
	fwdNoCopy := mock.MatchedBy(func(in ops.LinkIn) bool { return in.Link == ops.Forward(mid) && !in.CopyData })
	env.OnActivity(a.StartLink, mockAny, fwdNoCopy).Return(func(context.Context, ops.LinkIn) error {
		c.add("start:fwd")
		return boom
	}).Once()
	mockAll(env, c, nil, true)
	drive(t, env, append(happy()[:5], rollbackInWatch()))
	env.ExecuteWorkflow(Migration, Input{MigrationID: mid, Plan: testPlan()})
	if !env.IsWorkflowCompleted() || env.GetWorkflowError() == nil {
		t.Fatalf("want a failed workflow, got %v", env.GetWorkflowError())
	}
	st := status(t, env)
	if st.State != StateFailed || st.Primary != ops.Target || !st.ReplicationBroken ||
		!slices.Contains(kinds(st.Events), "unwind-halted") || len(st.PendingUndo) == 0 {
		t.Fatalf("%s %s pending %q: %q", st.State, st.Primary, st.PendingUndo, kinds(st.Events))
	}
	assertCalls(t, c.since("start:fwd"), []string{"start:fwd", "stop:fwd", "resume"})
	if c.count("pause") != 2 || c.count("undo-prepare") != 0 {
		t.Fatalf("calls %q", c.list())
	}
	assertCalls(t, st.PendingUndo, []string{PhaseSwitch, PhaseReplicate, PhasePrepare})
}

func TestUndoStepGateAfterFiveFailures(t *testing.T) {
	env := newEnv()
	c := &calls{}
	var a *ops.Activities
	env.OnActivity(a.UndoPrepare, mockAny, mockAny).Return(func(context.Context, ops.TablesIn) error {
		c.add("undo-prepare")
		return boom
	}).Times(5)
	mockAll(env, c, nil, true)
	steps := []action{approve("discover", "alice"), approve("prepare", "alice"), atGate("rehearse", "alice", Reject),
		approve("retry-undo-prepare", "alice")}
	r := start(t, env, steps)
	if r.State != StateRolledBack || c.count("undo-prepare") != 6 || !hasGate(r.Events, "retry-undo-prepare") {
		t.Fatalf("%s undo x%d: %q", r.State, c.count("undo-prepare"), kinds(r.Events))
	}
}

func TestRetryGateDuringUnwindOffersNoRollback(t *testing.T) {
	env := newEnv()
	c := &calls{}
	var a *ops.Activities
	rev := mock.MatchedBy(func(in ops.LinkIn) bool { return in.Link == ops.Reverse(mid) })
	env.OnActivity(a.StopLink, mockAny, rev).Return(func(context.Context, ops.LinkIn) error {
		c.add("stop:rev")
		return boom
	}).Once()
	mockAll(env, c, nil, true)
	var summary string
	retry := approve("retry-drop-old-publication", "alice")
	when := retry.when
	retry.when = func(s Status) bool {
		if when(s) {
			summary = s.Gate.Summary
			return true
		}
		return false
	}
	r := start(t, env, append(happy()[:5], rollbackInWatch(), retry))
	if r.State != StateRolledBack || summary == "" || strings.Contains(summary, "roll back") {
		t.Fatalf("%s summary %q", r.State, summary)
	}
}

func TestCancelMidUnwindSaysWhereItStopped(t *testing.T) {
	env := newEnv()
	c := &calls{}
	var a *ops.Activities
	block := blockAndCancel(env, c, "undo-prepare")
	env.OnActivity(a.UndoPrepare, mockAny, mockAny).Return(func(ctx context.Context, _ ops.TablesIn) error { return block(ctx) }).Once()
	mockAll(env, c, nil, true)
	st := cancelled(t, env, []action{approve("discover", "alice"), approve("prepare", "alice"), atGate("rehearse", "alice", Reject)})
	i := slices.IndexFunc(st.Events, func(e Event) bool { return e.Kind == "cancelled" })
	if i < 0 || !strings.Contains(st.Events[i].Message, "the unwind stopped here") {
		t.Fatalf("%+v", st.Events)
	}
	assertCalls(t, st.PendingUndo, []string{PhasePrepare})
}

// A forward switch whose abort could not restart the forward link leaves
// the source with no replication to the target: no second switch, only
// rollback.
func TestFailedForwardRestartOnlyRollsBack(t *testing.T) {
	env := newEnv()
	c := &calls{}
	var a *ops.Activities
	fwdNoCopy := mock.MatchedBy(func(in ops.LinkIn) bool { return in.Link == ops.Forward(mid) && !in.CopyData })
	env.OnActivity(a.StartLink, mockAny, fwdNoCopy).Return(func(context.Context, ops.LinkIn) error {
		c.add("start:fwd")
		return temporal.NewApplicationError("flaky", "test")
	})
	mockAll(env, c, fails{"start:rev": boom}, true)
	steps := append(happy()[:5], approve("replication-broken", "alice"))
	r := start(t, env, steps)
	if r.State != StateRolledBack || c.count("pause") != 1 || !slices.Contains(kinds(r.Events), "replication-broken") {
		t.Fatalf("%s pause x%d: %q", r.State, c.count("pause"), kinds(r.Events))
	}
	assertCalls(t, c.since("pause"), []string{"pause", "lag:fwd", "drop-sub:fwd", "seq:source>target", "start:rev",
		"stop:rev", "start:fwd", "start:fwd", "start:fwd", "resume", "stop:rev", "stop:fwd", "undo-prepare"})
}

// A switch back whose abort could not restart the reverse link halts the
// unwind: the target is primary and nothing replicates its writes back, so
// recreating the link or dropping the target's tables could lose rows.
func TestFailedReverseRestartHaltsUnwind(t *testing.T) {
	env := newEnv()
	c := &calls{}
	var a *ops.Activities
	rev := mock.MatchedBy(func(in ops.LinkIn) bool { return in.Link == ops.Reverse(mid) })
	env.OnActivity(a.StartLink, mockAny, rev).Return(func(context.Context, ops.LinkIn) error {
		c.add("start:rev")
		return nil
	}).Once()
	env.OnActivity(a.StartLink, mockAny, rev).Return(func(context.Context, ops.LinkIn) error {
		c.add("start:rev")
		return temporal.NewApplicationError("flaky", "test")
	})
	fwdNoCopy := mock.MatchedBy(func(in ops.LinkIn) bool { return in.Link == ops.Forward(mid) && !in.CopyData })
	env.OnActivity(a.StartLink, mockAny, fwdNoCopy).Return(func(context.Context, ops.LinkIn) error {
		c.add("start:fwd")
		return boom
	}).Once()
	mockAll(env, c, nil, true)
	drive(t, env, append(happy()[:5], rollbackInWatch()))
	env.ExecuteWorkflow(Migration, Input{MigrationID: mid, Plan: testPlan()})
	if !env.IsWorkflowCompleted() || env.GetWorkflowError() == nil {
		t.Fatalf("want a failed workflow, got %v", env.GetWorkflowError())
	}
	st := status(t, env)
	if st.State != StateFailed || st.Primary != ops.Target || !st.ReplicationBroken ||
		!slices.Contains(kinds(st.Events), "unwind-halted") {
		t.Fatalf("%s %s: %q", st.State, st.Primary, kinds(st.Events))
	}
	assertCalls(t, c.since("start:fwd"), []string{"start:fwd", "stop:fwd", "start:rev", "start:rev", "start:rev", "resume"})
	if c.count("pause") != 2 || c.count("undo-prepare") != 0 {
		t.Fatalf("calls %q", c.list())
	}
}

// The switch gate tells the approvers, from a check run just before it
// opened, whether any session reaches the source without PgBouncer.
func TestSwitchGateShowsSessionCheck(t *testing.T) {
	for _, tc := range []struct {
		name string
		mock func(*testsuite.TestWorkflowEnvironment)
		want string
	}{
		{"bypass", func(env *testsuite.TestWorkflowEnvironment) {
			var a *ops.Activities
			env.OnActivity(a.CheckSessions, mockAny, mockAny).Return(ops.SessionCheck{Checked: true,
				Bypass: []pg.Session{{Addr: "10.0.1.5", Count: 2}}}, nil).Once()
		}, "2 sessions bypass PgBouncer: 10.0.1.5 (2)."},
		{"failed", func(env *testsuite.TestWorkflowEnvironment) {
			var a *ops.Activities
			env.OnActivity(a.CheckSessions, mockAny, mockAny).Return(ops.SessionCheck{}, boom).Once()
		}, "Could not check sessions: boom"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newEnv()
			c := &calls{}
			tc.mock(env)
			mockAll(env, c, nil, true)
			var summary string
			seen := action{
				when: func(s Status) bool {
					if s.Gate != nil && s.Gate.Name == "switch" {
						summary = s.Gate.Summary
						return true
					}
					return false
				},
				do: func(*testsuite.TestWorkflowEnvironment) {},
			}
			steps := append(append(happy()[:3], seen), happy()[3:]...)
			if r := start(t, env, steps); r.State != StateFinished {
				t.Fatalf("%s %q", r.State, kinds(r.Events))
			}
			if !strings.Contains(summary, tc.want) || !strings.Contains(summary, "two different approvers") {
				t.Fatalf("summary %q", summary)
			}
		})
	}
}
