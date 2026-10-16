//go:build integration

package flow_test

import (
	"slices"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

	"github.com/SaiPisey2/cutover/internal/flow"
	"github.com/SaiPisey2/cutover/internal/itest"
	"github.com/SaiPisey2/cutover/internal/ops"
	"github.com/SaiPisey2/cutover/internal/plan"
)

type act struct {
	when func(flow.Status) bool
	do   func(*testsuite.TestWorkflowEnvironment)
}

func approve(gate, who string) act {
	return act{
		when: func(s flow.Status) bool {
			return s.Gate != nil && s.Gate.Name == gate && !slices.Contains(s.Gate.Approvals, who)
		},
		do: func(env *testsuite.TestWorkflowEnvironment) {
			env.SignalWorkflow(flow.SignalGate, flow.GateSignal{Gate: gate, Decision: flow.Approve, ApproverID: who})
		},
	}
}

func run(t *testing.T, acts []act, edit func(*plan.Plan)) (flow.Result, itest.LoadResult) {
	t.Helper()
	itest.Reset(t)
	itest.Seed(t, itest.Conn(t, itest.SourceDSN))
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	env.SetTestTimeout(3 * time.Minute)
	env.RegisterWorkflow(flow.Migration)
	env.RegisterWorkflow(flow.ReplicateWait)
	env.RegisterActivity(ops.New())
	i := 0
	var tick func()
	tick = func() {
		if i >= len(acts) {
			return
		}
		if v, err := env.QueryWorkflow(flow.QueryStatus); err == nil {
			var st flow.Status
			if v.Get(&st) == nil && acts[i].when(st) {
				acts[i].do(env)
				i++
			}
		}
		env.RegisterDelayedCallback(tick, 200*time.Millisecond)
	}
	env.RegisterDelayedCallback(tick, 200*time.Millisecond)

	p := itest.Plan(t)
	if edit != nil {
		edit(&p)
	}
	load := itest.StartLoad(t, 4)
	env.ExecuteWorkflow(flow.Migration, flow.Input{MigrationID: "itest", Plan: p})
	lr := load.Stop()
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var res flow.Result
	if err := env.GetWorkflowResult(&res); err != nil {
		t.Fatal(err)
	}
	for _, e := range res.Events {
		if strings.HasPrefix(e.Kind, "switch") || strings.Contains(e.Kind, "fail") || strings.HasPrefix(e.Kind, "undo") {
			t.Logf("event %s/%s: %s", e.Phase, e.Kind, e.Message)
		}
	}
	return res, lr
}

func happy() []act {
	return []act{approve("discover", "alice"), approve("prepare", "alice"), approve("rehearse", "alice"),
		approve("switch", "alice"), approve("switch", "bob"), approve("finish", "alice")}
}

func TestCutoverUnderLoad(t *testing.T) {
	res, lr := run(t, happy(), nil)
	if res.State != flow.StateFinished {
		t.Fatalf("state %s: %+v", res.State, res.Events)
	}
	tgt := itest.Conn(t, itest.TargetDSN)
	itest.Verify(t, tgt, lr)
	// The load must have written on both sides of the switch.
	if o := itest.Origins(t, tgt); o["source"] == 0 || o["target"] == 0 {
		t.Errorf("writes by origin %v: want some on each side", o)
	}
	poolOn(t, "target")
	var subs int
	src := itest.Conn(t, itest.SourceDSN)
	if err := src.QueryRow(t.Context(), "SELECT count(*) FROM pg_subscription").Scan(&subs); err != nil || subs != 0 {
		t.Fatalf("reverse replication left on source: %d %v", subs, err)
	}
}

func TestRollbackAfterSwitchUnderLoad(t *testing.T) {
	rollback := act{
		when: func(s flow.Status) bool {
			return s.Phase == flow.PhaseFinish && s.Primary == ops.Target && s.Gate == nil
		},
		do: func(env *testsuite.TestWorkflowEnvironment) {
			// Let writes land on the target before going back.
			time.Sleep(time.Second)
			env.SignalWorkflow(flow.SignalRollback, flow.RollbackSignal{ApproverID: "alice", Reason: "itest"})
		},
	}
	acts := append(happy()[:5], rollback)
	res, lr := run(t, acts, func(p *plan.Plan) { p.Thresholds.Finish.Watch = plan.Duration(time.Hour) })
	if res.State != flow.StateRolledBack {
		t.Fatalf("state %s: %+v", res.State, res.Events)
	}
	src := itest.Conn(t, itest.SourceDSN)
	itest.Verify(t, src, lr)
	// Writes the target took during the watch came back with the rollback.
	if o := itest.Origins(t, src); o["source"] == 0 || o["target"] == 0 {
		t.Errorf("writes by origin %v: want some on each side", o)
	}
	poolOn(t, "source")
}

func poolOn(t *testing.T, want string) {
	t.Helper()
	var name string
	if err := itest.Conn(t, itest.PoolDSN).QueryRow(t.Context(), "SHOW cluster_name").Scan(&name); err != nil || name != want {
		t.Fatalf("pool on %q (%v), want %q", name, err, want)
	}
}
