package console

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	"github.com/SaiPisey2/cutover/internal/flow"
	"github.com/SaiPisey2/cutover/internal/plan"
)

var (
	// ErrTemporal is any failure talking to Temporal: connection, deadline,
	// unavailable, or an unexpected reply. The original error is wrapped
	// for logs; it must never reach an API response.
	ErrTemporal = errors.New("temporal unavailable")
	// ErrNoMigration means Temporal has no such workflow.
	ErrNoMigration = errors.New("no such migration")
	// ErrExists means a migration with that ID is already running.
	ErrExists = errors.New("migration already exists")
)

// MigrationSummary is one row of the migration list.
type MigrationSummary struct {
	ID, Phase, State, Gate string
	Started                time.Time
}

// Temporal is the console's view of the workflow engine.
type Temporal interface {
	List(ctx context.Context) ([]MigrationSummary, error)
	Status(ctx context.Context, id string) (flow.Status, error)
	Replicate(ctx context.Context, workflowID string) (flow.ReplicateStatus, error)
	Start(ctx context.Context, id string, p plan.Plan) error
	Gate(ctx context.Context, id string, s flow.GateSignal) error
	Rollback(ctx context.Context, id string, s flow.RollbackSignal) error
}

const (
	listQuery    = "WorkflowType = 'Migration'"
	listPageSize = 100
)

type temporalGateway struct{ c client.Client }

// NewTemporal returns the Temporal gateway over c.
func NewTemporal(c client.Client) Temporal { return &temporalGateway{c: c} }

// mapErr turns any SDK error into one of the package's three errors.
func mapErr(err error) error {
	var nf *serviceerror.NotFound
	var dup *serviceerror.WorkflowExecutionAlreadyStarted
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrNoMigration), errors.Is(err, ErrExists), errors.Is(err, ErrTemporal):
		return err
	case errors.As(err, &nf):
		return fmt.Errorf("%w: %w", ErrNoMigration, err)
	case errors.As(err, &dup):
		return fmt.Errorf("%w: %w", ErrExists, err)
	default:
		return fmt.Errorf("%w: %w", ErrTemporal, err)
	}
}

func (g *temporalGateway) Status(ctx context.Context, id string) (flow.Status, error) {
	return g.status(ctx, flow.WorkflowID(id), "")
}

func (g *temporalGateway) status(ctx context.Context, workflowID, runID string) (flow.Status, error) {
	var st flow.Status
	v, err := g.c.QueryWorkflow(ctx, workflowID, runID, flow.QueryStatus)
	if err != nil {
		return st, mapErr(err)
	}
	if err := v.Get(&st); err != nil {
		return flow.Status{}, mapErr(err)
	}
	return st, nil
}

func (g *temporalGateway) Replicate(ctx context.Context, workflowID string) (flow.ReplicateStatus, error) {
	var st flow.ReplicateStatus
	v, err := g.c.QueryWorkflow(ctx, workflowID, "", flow.QueryStatus)
	if err != nil {
		return st, mapErr(err)
	}
	if err := v.Get(&st); err != nil {
		return flow.ReplicateStatus{}, mapErr(err)
	}
	return st, nil
}

func (g *temporalGateway) Start(ctx context.Context, id string, p plan.Plan) error {
	_, err := g.c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                                       flow.WorkflowID(id),
		TaskQueue:                                flow.TaskQueue,
		WorkflowExecutionErrorWhenAlreadyStarted: true,
	}, flow.Migration, flow.Input{MigrationID: id, Plan: p})
	return mapErr(err)
}

func (g *temporalGateway) Gate(ctx context.Context, id string, s flow.GateSignal) error {
	return mapErr(g.c.SignalWorkflow(ctx, flow.WorkflowID(id), "", flow.SignalGate, s))
}

func (g *temporalGateway) Rollback(ctx context.Context, id string, s flow.RollbackSignal) error {
	return mapErr(g.c.SignalWorkflow(ctx, flow.WorkflowID(id), "", flow.SignalRollback, s))
}

// StateUnavailable is the state shown for a running migration whose status
// could not be read in time.
const StateUnavailable = "unavailable"

// rowTimeout bounds each per-row status query in List; a var so tests can
// shrink it.
var rowTimeout = 3 * time.Second

const listConcurrency = 8

// List returns the newest 100 migrations, in Temporal's order. Every one is
// asked for its status, a few at a time, each under its own timeout. A row
// whose query fails still appears: a closed one shows a state derived from
// how it closed, a running one shows StateUnavailable. If ctx itself ends,
// List fails with ErrTemporal instead of degrading the rest.
func (g *temporalGateway) List(ctx context.Context) ([]MigrationSummary, error) {
	resp, err := g.c.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Query: listQuery, PageSize: listPageSize,
	})
	if err != nil {
		return nil, mapErr(err)
	}
	execs := resp.GetExecutions()
	out := make([]MigrationSummary, len(execs))
	sem := make(chan struct{}, listConcurrency)
	var wg sync.WaitGroup
	for i, ex := range execs {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer func() { <-sem; wg.Done() }()
			out[i] = g.row(ctx, ex)
		}()
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrTemporal, err)
	}
	return out, nil
}

func (g *temporalGateway) row(ctx context.Context, ex *workflowpb.WorkflowExecutionInfo) MigrationSummary {
	wid, rid := ex.GetExecution().GetWorkflowId(), ex.GetExecution().GetRunId()
	sum := MigrationSummary{ID: strings.TrimPrefix(wid, flow.WorkflowID("")), Started: ex.GetStartTime().AsTime()}
	qctx, cancel := context.WithTimeout(ctx, rowTimeout)
	defer cancel()
	st, err := g.status(qctx, wid, rid)
	switch {
	case err == nil:
		if st.MigrationID != "" {
			sum.ID = st.MigrationID
		}
		sum.Phase, sum.State = st.Phase, st.State
		if st.Gate != nil {
			sum.Gate = st.Gate.Name
		}
	case ex.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING:
		sum.State = StateUnavailable
	case ex.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		sum.State = flow.StateFinished
	default:
		sum.State = flow.StateFailed
	}
	return sum
}
