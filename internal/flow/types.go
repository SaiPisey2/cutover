// Package flow holds cutover's workflows. Workflows decide; activities in
// package ops act. Nothing here does I/O or reads the clock except through
// Temporal's APIs, so a replay after a crash takes the same path.
package flow

import (
	"time"

	"github.com/SaiPisey2/cutover/internal/ops"
	"github.com/SaiPisey2/cutover/internal/pg"
	"github.com/SaiPisey2/cutover/internal/plan"
	"github.com/SaiPisey2/cutover/internal/readiness"
	"github.com/SaiPisey2/cutover/internal/traffic"
)

const (
	TaskQueue      = "cutover"
	SignalGate     = "gate"
	SignalRollback = "rollback"
	QueryStatus    = "status"
	maxEvents      = 200
)

type Decision string

const (
	Approve Decision = "approve"
	Reject  Decision = "reject"
)

const (
	StateRunning     = "running"
	StateWaiting     = "waiting"
	StateRollingBack = "rolling_back"
	StateRolledBack  = "rolled_back"
	StateFinished    = "finished"
	StateFailed      = "failed"
)

// Phases, in order. Shift moves HTTP traffic to the target whenever the
// plan has routed services; otherwise it is recorded as skipped so the
// timeline is complete.
const (
	PhaseDiscover  = "discover"
	PhasePrepare   = "prepare"
	PhaseReplicate = "replicate"
	PhaseRehearse  = "rehearse"
	PhaseShift     = "shift"
	PhaseSwitch    = "switch"
	PhaseFinish    = "finish"
)

type Input struct {
	MigrationID string    `json:"migration_id"`
	Plan        plan.Plan `json:"plan"`
}

// GateSignal is one decision at a gate. ApproverID is trusted as given in
// Plan A; Plan C's server authenticates it before signalling.
type GateSignal struct {
	Gate       string   `json:"gate"`
	Decision   Decision `json:"decision"`
	ApproverID string   `json:"approver_id"`
	Note       string   `json:"note,omitempty"`
}

type RollbackSignal struct {
	ApproverID string `json:"approver_id"`
	Reason     string `json:"reason"`
}

type Event struct {
	At      time.Time `json:"at"`
	Phase   string    `json:"phase"`
	Kind    string    `json:"kind"`
	Actor   string    `json:"actor,omitempty"`
	Message string    `json:"message,omitempty"`
}

type Gate struct {
	Name      string   `json:"name"`
	Phase     string   `json:"phase"`
	Needs     int      `json:"needs"`
	Approvals []string `json:"approvals"`
	Summary   string   `json:"summary"`
}

type Status struct {
	MigrationID         string            `json:"migration_id"`
	Phase               string            `json:"phase"`
	State               string            `json:"state"`
	Primary             ops.Side          `json:"primary"`
	Gate                *Gate             `json:"gate,omitempty"`
	Readiness           *readiness.Report `json:"readiness,omitempty"`
	Tables              []string          `json:"tables,omitempty"`
	ReplicateWorkflowID string            `json:"replicate_workflow_id,omitempty"`
	Checks              []pg.TableCheck   `json:"checks,omitempty"`
	Sequences           []pg.SequenceSync `json:"sequences,omitempty"`
	// ReplicationBroken is set when a switch abort could not stop the new
	// link, so the old one was left stopped: replication runs in neither
	// direction safely until an operator repairs it.
	ReplicationBroken bool `json:"replication_broken,omitempty"`
	// PendingUndo lists, in the order they would run, the undo phases a
	// stopped unwind left undone.
	PendingUndo []string `json:"pending_undo,omitempty"`
	// Weight is the percentage of routed traffic the front door sends to the target.
	Weight int `json:"weight"`
	// Traffic is the latest window: Rehearse's, or the current Shift step's.
	Traffic []ServiceTraffic `json:"traffic,omitempty"`
	// ShiftVerdict is the last Shift window's verdict in one phrase, for the switch gate.
	ShiftVerdict string  `json:"shift_verdict,omitempty"`
	Events       []Event `json:"events"`
}

// ServiceTraffic is one routed service's traffic over a window: Rehearse's
// mirroring window, or the current Shift step so far.
type ServiceTraffic struct {
	Service string           `json:"service"`
	Source  traffic.Summary  `json:"source"`
	Target  traffic.Summary  `json:"target"`
	Verdict *traffic.Verdict `json:"verdict,omitempty"`
}

type Result struct {
	State  string  `json:"state"`
	Events []Event `json:"events"`
}
