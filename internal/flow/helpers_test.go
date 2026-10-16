package flow

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/SaiPisey2/cutover/internal/ops"
	"github.com/SaiPisey2/cutover/internal/pg"
	"github.com/SaiPisey2/cutover/internal/plan"
	"github.com/SaiPisey2/cutover/internal/readiness"
)

const mid = "shop"

var mockAny = mock.Anything

// workflowContext keeps test workflow signatures short.
type workflowContext = workflow.Context

func testPlan() plan.Plan {
	p := plan.Plan{Version: 1, Name: mid, Database: plan.Database{Name: "app"}}
	p.Thresholds.Finish.Watch = plan.Duration(time.Hour)
	plan.ApplyDefaults(&p)
	return p
}

// calls records activity calls in order, as short labels.
type calls struct {
	mu    sync.Mutex
	names []string
}

func (c *calls) add(s string) { c.mu.Lock(); c.names = append(c.names, s); c.mu.Unlock() }

func (c *calls) list() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string{}, c.names...)
}

// since returns the calls from the first occurrence of label on.
func (c *calls) since(label string) []string {
	l := c.list()
	for i, n := range l {
		if n == label {
			return l[i:]
		}
	}
	return nil
}

func (c *calls) count(label string) int {
	n := 0
	for _, s := range c.list() {
		if s == label {
			n++
		}
	}
	return n
}

func short(l ops.Link) string { return strings.TrimPrefix(l.Name, "cutover_"+mid+"_") }

var boom = temporal.NewNonRetryableApplicationError("boom", "test", nil)

// fails maps a call label to the error that call returns.
type fails map[string]error

// mockAll registers every activity with a recording, succeeding default.
// Register test-specific .Once() expectations BEFORE calling it: testify
// uses the first matching expectation.
func mockAll(env *testsuite.TestWorkflowEnvironment, c *calls, f fails, drainVisible bool) {
	var a *ops.Activities
	ret := func(label string) error { c.add(label); return f[label] }
	env.OnActivity(a.Discover, mock.Anything, mock.Anything).Return(
		func(context.Context, ops.DiscoverIn) (readiness.Report, error) {
			return readiness.Report{Tables: []string{"public.orders"}}, ret("discover")
		})
	env.OnActivity(a.PrepareSchema, mock.Anything, mock.Anything).Return(
		func(context.Context, ops.TablesIn) error { return ret("prepare") })
	env.OnActivity(a.UndoPrepare, mock.Anything, mock.Anything).Return(
		func(context.Context, ops.TablesIn) error { return ret("undo-prepare") })
	env.OnActivity(a.CheckSchema, mock.Anything, mock.Anything).Return(
		func(context.Context, ops.TablesIn) ([]string, error) { return nil, ret("check-schema") })
	env.OnActivity(a.CompareData, mock.Anything, mock.Anything).Return(
		func(context.Context, ops.TablesIn) ([]pg.TableCheck, error) {
			return []pg.TableCheck{{Table: "public.orders", Match: true, Sampled: 1000}}, ret("compare")
		})
	env.OnActivity(a.Cleanup, mock.Anything, mock.Anything).Return(
		func(context.Context, ops.TablesIn) error { return ret("cleanup") })
	env.OnActivity(a.StartLink, mock.Anything, mock.Anything).Return(
		func(_ context.Context, in ops.LinkIn) error {
			label := "start:" + short(in.Link)
			if in.CopyData {
				label += "+copy"
			}
			return ret(label)
		})
	env.OnActivity(a.StopLink, mock.Anything, mock.Anything).Return(
		func(_ context.Context, in ops.LinkIn) error { return ret("stop:" + short(in.Link)) })
	env.OnActivity(a.DropSubscription, mock.Anything, mock.Anything).Return(
		func(_ context.Context, in ops.LinkIn) error { return ret("drop-sub:" + short(in.Link)) })
	env.OnActivity(a.CopyProgress, mock.Anything, mock.Anything).Return(
		func(_ context.Context, in ops.LinkIn) (ops.Progress, error) {
			return ops.Progress{Ready: 2, Total: 2}, ret("progress")
		})
	env.OnActivity(a.MeasureLag, mock.Anything, mock.Anything).Return(
		func(_ context.Context, in ops.LagIn) (ops.LagOut, error) {
			if in.Timeout <= 0 {
				return ops.LagOut{}, temporal.NewNonRetryableApplicationError("lag timeout must be positive", "test", nil)
			}
			return ops.LagOut{Lag: 50 * time.Millisecond, Visible: drainVisible}, ret("lag:" + short(in.Link))
		})
	env.OnActivity(a.PausePool, mock.Anything, mock.Anything).Return(
		func(_ context.Context, in ops.PoolIn) error { return ret("pause") })
	env.OnActivity(a.ResumePool, mock.Anything, mock.Anything).Return(
		func(context.Context, ops.PoolIn) error { return ret("resume") })
	env.OnActivity(a.Repoint, mock.Anything, mock.Anything).Return(
		func(_ context.Context, in ops.RepointIn) error { return ret("repoint:" + string(in.To)) })
	env.OnActivity(a.SyncSequences, mock.Anything, mock.Anything).Return(
		func(_ context.Context, in ops.SeqIn) ([]pg.SequenceSync, error) {
			return []pg.SequenceSync{{Name: "public.orders_id_seq", Set: 1042, Changed: true}}, ret("seq:" + string(in.From) + ">" + string(in.To))
		})
}

func newEnv() *testsuite.TestWorkflowEnvironment {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	env.RegisterActivity(ops.New())
	env.RegisterWorkflow(Migration)
	env.RegisterWorkflow(ReplicateWait)
	env.RegisterWorkflow(switchWorkflow)
	return env
}

type switchResult struct {
	OK        bool
	Primary   ops.Side
	Broken    bool // switchOver returned ErrReplicationBroken
	StBroken  bool // Status.ReplicationBroken
	Sequences []pg.SequenceSync
	Events    []Event
}

// switchWorkflow exercises runner.switchOver alone.
func switchWorkflow(ctx workflowContext, p plan.Plan, back bool) (switchResult, error) {
	r, err := newRunner(ctx, Input{MigrationID: mid, Plan: p})
	if err != nil {
		return switchResult{}, err
	}
	from, to, old, next := ops.Source, ops.Target, ops.Forward(mid), ops.Reverse(mid)
	if back {
		from, to, old, next = ops.Target, ops.Source, ops.Reverse(mid), ops.Forward(mid)
		r.st.Primary = ops.Target
	}
	ok, err := r.switchOver(from, to, old, next, []string{"public.orders"})
	res := switchResult{OK: ok, Primary: r.st.Primary, StBroken: r.st.ReplicationBroken,
		Sequences: r.st.Sequences, Events: r.st.Events}
	if errors.Is(err, ErrReplicationBroken) {
		res.Broken, err = true, nil
	}
	return res, err
}

// action fires when its condition holds on the queried status, once.
type action struct {
	when func(Status) bool
	do   func(env *testsuite.TestWorkflowEnvironment)
}

func atGate(gate, who string, d Decision) action {
	return action{
		when: func(s Status) bool {
			return s.Gate != nil && s.Gate.Name == gate && !slices.Contains(s.Gate.Approvals, who)
		},
		do: func(env *testsuite.TestWorkflowEnvironment) {
			env.SignalWorkflow(SignalGate, GateSignal{Gate: gate, Decision: d, ApproverID: who})
		},
	}
}

func approve(gate, who string) action { return atGate(gate, who, Approve) }

// happy is the full approval sequence.
func happy() []action {
	return []action{approve("discover", "alice"), approve("prepare", "alice"), approve("rehearse", "alice"),
		approve("switch", "alice"), approve("switch", "bob"), approve("finish", "alice")}
}

// maxTicks bounds drive: about a week of workflow time. An action that never
// fires then fails the test at once instead of ticking forever.
const maxTicks = 20000

// drive checks the workflow's status every 30s of workflow time and fires
// actions in order.
func drive(t *testing.T, env *testsuite.TestWorkflowEnvironment, actions []action) {
	i, ticks := 0, 0
	var tick func()
	tick = func() {
		if i >= len(actions) {
			return
		}
		if ticks++; ticks > maxTicks {
			t.Errorf("drive: action %d never fired", i)
			env.CancelWorkflow()
			return
		}
		v, err := env.QueryWorkflow(QueryStatus)
		if err == nil {
			var st Status
			if err := v.Get(&st); err != nil {
				t.Errorf("status: %v", err)
				return
			}
			if actions[i].when(st) {
				actions[i].do(env)
				i++
			}
		}
		env.RegisterDelayedCallback(tick, 30*time.Second)
	}
	env.RegisterDelayedCallback(tick, time.Second)
}

func result(t *testing.T, env *testsuite.TestWorkflowEnvironment) Result {
	t.Helper()
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var r Result
	if err := env.GetWorkflowResult(&r); err != nil {
		t.Fatal(err)
	}
	return r
}

func kinds(evs []Event) []string {
	out := make([]string, len(evs))
	for i, e := range evs {
		out[i] = e.Kind
	}
	return out
}
