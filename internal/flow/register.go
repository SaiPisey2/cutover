package flow

import (
	"go.temporal.io/sdk/worker"

	"github.com/SaiPisey2/cutover/internal/ops"
)

// WorkflowID is the Temporal workflow ID of a migration.
func WorkflowID(migrationID string) string { return "migration-" + migrationID }

func RegisterWorkflows(r worker.WorkflowRegistry) {
	r.RegisterWorkflow(Migration)
	r.RegisterWorkflow(ReplicateWait)
}

// Register registers the workflows and the real activities.
func Register(r worker.Registry) {
	RegisterWorkflows(r)
	r.RegisterActivity(ops.New())
}
