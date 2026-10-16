package flow

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/SaiPisey2/cutover/internal/ops"
	"github.com/SaiPisey2/cutover/internal/plan"
)

func replicatePlan() plan.Plan {
	p := testPlan()
	p.Thresholds.Replicate = plan.Replicate{MaxLag: plan.Duration(time.Second),
		StableFor: plan.Duration(30 * time.Second), SampleEvery: plan.Duration(10 * time.Second)}
	return p
}

func TestReplicateWaitNeedsAnUnbrokenWindow(t *testing.T) {
	env := newEnv()
	var a *ops.Activities
	progress := []ops.Progress{{Ready: 0, Total: 2}, {Ready: 2, Total: 2}}
	env.OnActivity(a.CopyProgress, mockAny, mockAny).Return(func(context.Context, ops.LinkIn) (ops.Progress, error) {
		p := progress[0]
		progress = progress[1:]
		return p, nil
	})
	ok, bad := ops.LagOut{Lag: 100 * time.Millisecond, Visible: true}, ops.LagOut{Lag: 3 * time.Second, Visible: true}
	lags := []ops.LagOut{ok, ok, bad, ok, ok, ok, ok}
	env.OnActivity(a.MeasureLag, mockAny, mockAny).Return(func(context.Context, ops.LagIn) (ops.LagOut, error) {
		if len(lags) == 0 {
			return ops.LagOut{}, temporal.NewNonRetryableApplicationError("sampled past the stable window", "test", nil)
		}
		l := lags[0]
		lags = lags[1:]
		return l, nil
	})
	env.ExecuteWorkflow(ReplicateWait, ReplicateIn{Plan: replicatePlan(), Link: ops.Forward(mid), Tables: []string{"public.orders"}})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var st ReplicateStatus
	if err := env.GetWorkflowResult(&st); err != nil {
		t.Fatal(err)
	}
	if len(lags) != 0 || st.Samples != 7 || !st.CopyDone {
		t.Fatalf("left %d samples, status %+v", len(lags), st)
	}
}

func TestReplicateWaitContinuesAsNew(t *testing.T) {
	old := replicateIterations
	replicateIterations = 3
	t.Cleanup(func() { replicateIterations = old })
	env := newEnv()
	var a *ops.Activities
	env.OnActivity(a.MeasureLag, mockAny, mockAny).Return(ops.LagOut{Lag: 5 * time.Second, Visible: true}, nil)
	env.ExecuteWorkflow(ReplicateWait, ReplicateIn{Plan: replicatePlan(), Link: ops.Forward(mid), CopyDone: true})
	var can *workflow.ContinueAsNewError
	if err := env.GetWorkflowError(); !errors.As(err, &can) {
		t.Fatalf("want continue-as-new, got %v", err)
	}
	// The next run carries the state on, including the samples taken.
	var next ReplicateIn
	if err := converter.GetDefaultDataConverter().FromPayloads(can.Input, &next); err != nil {
		t.Fatal(err)
	}
	if !next.CopyDone || next.Samples != 3 || next.StableSince != nil {
		t.Fatalf("next run %+v", next)
	}
}

// Samples count across continue-as-new: the next run starts from them.
func TestReplicateWaitCarriesSamples(t *testing.T) {
	env := newEnv()
	var a *ops.Activities
	env.OnActivity(a.MeasureLag, mockAny, mockAny).Return(ops.LagOut{Lag: 10 * time.Millisecond, Visible: true}, nil)
	env.ExecuteWorkflow(ReplicateWait, ReplicateIn{Plan: replicatePlan(), Link: ops.Forward(mid), CopyDone: true, Samples: 10})
	var st ReplicateStatus
	if err := env.GetWorkflowResult(&st); err != nil {
		t.Fatal(err)
	}
	if st.Samples != 14 {
		t.Fatalf("samples %d", st.Samples)
	}
}
