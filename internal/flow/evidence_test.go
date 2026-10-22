package flow

import (
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/SaiPisey2/cutover/internal/plan"
)

func gated() Status {
	return Status{
		MigrationID: "m1", Phase: PhaseSwitch, State: StateWaiting, Weight: 100,
		Gate:         &Gate{Name: "switch", Phase: PhaseSwitch, Needs: 2, Approvals: []string{"a"}, Summary: "ready"},
		ShiftVerdict: "healthy",
		Events:       []Event{{Kind: "x"}},
	}
}

func TestEvidenceDigestIgnoresEvents(t *testing.T) {
	a, b := gated(), gated()
	b.Events = append(b.Events, Event{Kind: "y", Message: "noise"})
	if EvidenceDigest(a) == "" || EvidenceDigest(a) != EvidenceDigest(b) {
		t.Fatalf("digests differ: %q vs %q", EvidenceDigest(a), EvidenceDigest(b))
	}
}

func TestEvidenceDigestChangesWithApprovals(t *testing.T) {
	a, b := gated(), gated()
	b.Gate = &Gate{Name: "switch", Phase: PhaseSwitch, Needs: 2, Approvals: []string{"a", "b"}, Summary: "ready"}
	if EvidenceDigest(a) == EvidenceDigest(b) {
		t.Fatal("an added approval must change the digest")
	}
}

func TestEvidenceDigestEmptyWithoutGate(t *testing.T) {
	st := gated()
	st.Gate = nil
	if d := EvidenceDigest(st); d != "" {
		t.Fatalf("digest %q without a gate", d)
	}
}

func statusOnly(ctx workflow.Context, in Input) error {
	if _, err := newRunner(ctx, in); err != nil {
		return err
	}
	return workflow.Sleep(ctx, time.Hour)
}

func TestStatusCarriesPlanNameAndRouted(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(statusOnly)
	p := plan.Plan{Name: "shop", Services: []plan.Service{
		{Name: "api", Route: &plan.Route{Host: "a.example.com", Prefix: "/"}},
		{Name: "worker"},
		{Name: "web", Route: &plan.Route{Host: "b.example.com", Prefix: "/"}},
	}}
	var st Status
	env.RegisterDelayedCallback(func() {
		v, err := env.QueryWorkflow(QueryStatus)
		if err != nil {
			t.Error(err)
			return
		}
		if err := v.Get(&st); err != nil {
			t.Error(err)
		}
	}, time.Minute)
	env.ExecuteWorkflow(statusOnly, Input{MigrationID: "m1", Plan: p})
	if st.PlanName != "shop" || len(st.Routed) != 2 || st.Routed[0] != "api" || st.Routed[1] != "web" {
		t.Fatalf("plan_name %q routed %q", st.PlanName, st.Routed)
	}
}
