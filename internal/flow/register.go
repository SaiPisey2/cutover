package flow

import (
	"time"

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

// WorkerOptions are the options every cutover worker runs with.
//
// StickyScheduleToStartTimeout is 1s, not the SDK's 5s. A workflow's next
// task goes first to the worker that ran the last one; if that worker died
// mid-switch, the task waits this long before any other worker may take
// it. With 5s on top of the 2s heartbeat timeout that frees the dead
// worker's activity, a crash would eat about 7s of the default 10s pause
// budget, while the drain must finish by 7.5s.
func WorkerOptions() worker.Options {
	return worker.Options{StickyScheduleToStartTimeout: time.Second}
}
