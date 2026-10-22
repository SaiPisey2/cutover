package console

import (
	"context"
	"errors"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/mocks"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/stretchr/testify/mock"

	"github.com/SaiPisey2/cutover/internal/flow"
	"github.com/SaiPisey2/cutover/internal/plan"
)

func encoded(t *testing.T, v any) converter.EncodedValue {
	t.Helper()
	p, err := converter.GetDefaultDataConverter().ToPayloads(v)
	if err != nil {
		t.Fatal(err)
	}
	return &mockValue{p: p}
}

type mockValue struct{ p *commonpb.Payloads }

func (m *mockValue) HasValue() bool { return m.p != nil }
func (m *mockValue) Get(out any) error {
	return converter.GetDefaultDataConverter().FromPayloads(m.p, out)
}

func TestTemporalMapsErrors(t *testing.T) {
	cases := []struct {
		name string
		in   error
		want error
	}{
		{"not found", serviceerror.NewNotFound("gone"), ErrNoMigration},
		{"unavailable", serviceerror.NewUnavailable("down"), ErrTemporal},
		{"deadline", context.DeadlineExceeded, ErrTemporal},
		{"canceled", context.Canceled, ErrTemporal},
		{"already started", serviceerror.NewWorkflowExecutionAlreadyStarted("dup", "req", "run"), ErrExists},
		{"query failed", serviceerror.NewQueryFailed("nope"), ErrTemporal},
	}
	ctx := context.Background()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &mocks.Client{}
			c.On("QueryWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, tc.in)
			c.On("SignalWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(tc.in)
			c.On("ExecuteWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, tc.in)
			c.On("ListWorkflow", mock.Anything, mock.Anything).Return(nil, tc.in)
			g := NewTemporal(c)
			_, errStatus := g.Status(ctx, "m1")
			_, errRepl := g.Replicate(ctx, "migration-m1-replicate")
			errStart := g.Start(ctx, "m1", plan.Plan{})
			errGate := g.Gate(ctx, "m1", flow.GateSignal{})
			errRb := g.Rollback(ctx, "m1", flow.RollbackSignal{})
			_, errList := g.List(ctx)
			for name, err := range map[string]error{"status": errStatus, "replicate": errRepl, "start": errStart,
				"gate": errGate, "rollback": errRb, "list": errList} {
				if !errors.Is(err, tc.want) {
					t.Errorf("%s: %v, want %v", name, err, tc.want)
				}
				var raw *serviceerror.NotFound
				if tc.want != ErrNoMigration && errors.As(err, &raw) {
					t.Errorf("%s leaks raw error", name)
				}
			}
		})
	}
}

func TestTemporalStartOptions(t *testing.T) {
	c := &mocks.Client{}
	c.On("ExecuteWorkflow", mock.Anything, mock.MatchedBy(func(o client.StartWorkflowOptions) bool {
		return o.ID == "migration-m1" && o.TaskQueue == flow.TaskQueue && o.WorkflowExecutionErrorWhenAlreadyStarted
	}), mock.Anything, mock.MatchedBy(func(in flow.Input) bool { return in.MigrationID == "m1" && in.Plan.Name == "p" })).
		Return(&mocks.WorkflowRun{}, nil)
	if err := NewTemporal(c).Start(context.Background(), "m1", plan.Plan{Name: "p"}); err != nil {
		t.Fatal(err)
	}
	c.AssertExpectations(t)
}

func TestTemporalStatusAndReplicate(t *testing.T) {
	c := &mocks.Client{}
	c.On("QueryWorkflow", mock.Anything, "migration-m1", "", flow.QueryStatus).
		Return(encoded(t, flow.Status{MigrationID: "m1", Phase: flow.PhaseSwitch, PlanName: "shop", Routed: []string{"api"}}), nil)
	c.On("QueryWorkflow", mock.Anything, "migration-m1-replicate", "", flow.QueryStatus).
		Return(encoded(t, flow.ReplicateStatus{CopyDone: true, Samples: 3}), nil)
	g := NewTemporal(c)
	st, err := g.Status(context.Background(), "m1")
	if err != nil || st.Phase != flow.PhaseSwitch || st.PlanName != "shop" || len(st.Routed) != 1 {
		t.Fatalf("status %+v err %v", st, err)
	}
	rs, err := g.Replicate(context.Background(), "migration-m1-replicate")
	if err != nil || !rs.CopyDone || rs.Samples != 3 {
		t.Fatalf("replicate %+v err %v", rs, err)
	}
}

func exec(id string, s enumspb.WorkflowExecutionStatus, at time.Time) *workflowpb.WorkflowExecutionInfo {
	return &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: "run-" + id},
		Status:    s, StartTime: timestamppb.New(at),
	}
}

func TestTemporalList(t *testing.T) {
	at := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	c := &mocks.Client{}
	c.On("ListWorkflow", mock.Anything, mock.MatchedBy(func(r *workflowservice.ListWorkflowExecutionsRequest) bool {
		return r.Query == "WorkflowType = 'Migration'" && r.PageSize == 100
	})).Return(&workflowservice.ListWorkflowExecutionsResponse{Executions: []*workflowpb.WorkflowExecutionInfo{
		exec("migration-a", enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, at),
		exec("migration-b", enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, at),
		exec("migration-c", enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, at),
		exec("migration-d", enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, at),
	}}, nil)
	c.On("QueryWorkflow", mock.Anything, "migration-a", "run-migration-a", flow.QueryStatus).
		Return(encoded(t, flow.Status{MigrationID: "a", Phase: flow.PhaseSwitch, State: flow.StateWaiting,
			Gate: &flow.Gate{Name: "switch"}}), nil)
	c.On("QueryWorkflow", mock.Anything, "migration-b", "run-migration-b", flow.QueryStatus).
		Return(nil, serviceerror.NewQueryFailed("no worker"))
	c.On("QueryWorkflow", mock.Anything, "migration-c", "run-migration-c", flow.QueryStatus).
		Return(nil, context.DeadlineExceeded)
	c.On("QueryWorkflow", mock.Anything, "migration-d", "run-migration-d", flow.QueryStatus).
		Return(encoded(t, flow.Status{MigrationID: "d", Phase: flow.PhaseFinish, State: flow.StateRolledBack}), nil)

	got, err := NewTemporal(c).List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []MigrationSummary{
		{"a", flow.PhaseSwitch, flow.StateWaiting, "switch", at},
		{"b", "", flow.StateFinished, "", at},
		{"c", "", flow.StateFailed, "", at},
		{"d", flow.PhaseFinish, flow.StateRolledBack, "", at},
	}
	if len(got) != len(want) {
		t.Fatalf("got %+v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("row %d: %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestTemporalListRunningQueryFailure(t *testing.T) {
	c := &mocks.Client{}
	c.On("ListWorkflow", mock.Anything, mock.Anything).Return(&workflowservice.ListWorkflowExecutionsResponse{
		Executions: []*workflowpb.WorkflowExecutionInfo{exec("migration-a", enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, time.Now())}}, nil)
	c.On("QueryWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, serviceerror.NewUnavailable("x"))
	if _, err := NewTemporal(c).List(context.Background()); !errors.Is(err, ErrTemporal) {
		t.Fatalf("err %v", err)
	}
}
