package flow

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/SaiPisey2/cutover/internal/ops"
	"github.com/SaiPisey2/cutover/internal/plan"
)

// replicateIterations bounds one run's history; the wait then continues as
// new with its state, so a copy that takes days stays within limits.
var replicateIterations = 500

type ReplicateIn struct {
	Plan        plan.Plan  `json:"plan"`
	Link        ops.Link   `json:"link"`
	Tables      []string   `json:"tables"`
	CopyDone    bool       `json:"copy_done"`
	StableSince *time.Time `json:"stable_since,omitempty"`
}

type ReplicateStatus struct {
	Copy        ops.Progress  `json:"copy"`
	CopyDone    bool          `json:"copy_done"`
	Lag         time.Duration `json:"lag"`
	Visible     bool          `json:"visible"`
	StableSince *time.Time    `json:"stable_since,omitempty"`
	Samples     int           `json:"samples"`
}

func lagTimeout(th plan.Replicate) time.Duration { return max(5*time.Second, 5*th.MaxLag.D()) }

// ReplicateWait waits for the initial copy, then until lag stays at or
// under max_lag for stable_for without a break. Any slow or unseen sample
// restarts the window.
func ReplicateWait(ctx workflow.Context, in ReplicateIn) (ReplicateStatus, error) {
	st := ReplicateStatus{CopyDone: in.CopyDone, StableSince: in.StableSince}
	if err := workflow.SetQueryHandler(ctx, QueryStatus, func() (ReplicateStatus, error) { return st, nil }); err != nil {
		return st, err
	}
	th := in.Plan.Thresholds.Replicate
	actx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: lagTimeout(th) + 30*time.Second,
		RetryPolicy: &temporal.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 2,
			MaximumInterval: 30 * time.Second, MaximumAttempts: 5},
	})
	li := ops.LinkIn{Plan: in.Plan, Link: in.Link, Tables: in.Tables}
	for i := 0; ; i++ {
		if i == replicateIterations {
			return st, workflow.NewContinueAsNewError(ctx, ReplicateWait, ReplicateIn{Plan: in.Plan, Link: in.Link,
				Tables: in.Tables, CopyDone: st.CopyDone, StableSince: st.StableSince})
		}
		if !st.CopyDone {
			if err := workflow.ExecuteActivity(actx, acts.CopyProgress, li).Get(ctx, &st.Copy); err != nil {
				return st, err
			}
			st.CopyDone = st.Copy.Total > 0 && st.Copy.Ready == st.Copy.Total
		} else {
			var lag ops.LagOut
			if err := workflow.ExecuteActivity(actx, acts.MeasureLag,
				ops.LagIn{Plan: in.Plan, Link: in.Link, Timeout: lagTimeout(th)}).Get(ctx, &lag); err != nil {
				return st, err
			}
			st.Samples++
			st.Lag, st.Visible = lag.Lag, lag.Visible
			now := workflow.Now(ctx)
			if lag.Visible && lag.Lag <= th.MaxLag.D() {
				if st.StableSince == nil {
					st.StableSince = &now
				}
				if now.Sub(*st.StableSince) >= th.StableFor.D() {
					return st, nil
				}
			} else {
				st.StableSince = nil
			}
		}
		if err := workflow.Sleep(ctx, th.SampleEvery.D()); err != nil {
			return st, err
		}
	}
}
