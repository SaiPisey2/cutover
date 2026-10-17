package svc

import (
	"context"

	"github.com/SaiPisey2/cutover/internal/plan"
)

// external is a service whose two copies the operator runs; cutover only
// moves its traffic. Health comes from the front door's checks.
type external struct{ svc plan.Service }

func (d external) Inspect(context.Context, Side, Side) (Facts, error) {
	return Facts{Service: d.svc.Name, Kind: plan.KindExternal}, nil
}
func (external) Prepare(context.Context, Side, Side) error { return nil }
func (external) Healthy(context.Context, Side) (bool, string, error) {
	return true, "run by you; the front door checks it", nil
}
func (external) Undo(context.Context, Side) error { return nil }
