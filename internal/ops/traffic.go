package ops

import (
	"context"
	"errors"

	"github.com/SaiPisey2/cutover/internal/frontdoor"
	"github.com/SaiPisey2/cutover/internal/plan"
	"github.com/SaiPisey2/cutover/internal/traffic"
)

type TrafficIn struct {
	Plan    plan.Plan         `json:"plan"`
	Service string            `json:"service"`
	Desired frontdoor.Desired `json:"desired"`
}

type ServiceCounters struct {
	Service string            `json:"service"`
	Source  *traffic.Counters `json:"source,omitempty"`
	Target  *traffic.Counters `json:"target,omitempty"`
}

// FrontDoorClient resolves the plan's front-door token and returns a client.
func FrontDoorClient(p plan.Plan) (*frontdoor.Client, error) {
	if p.FrontDoor == nil {
		return nil, permanentf("the plan has no frontdoor section")
	}
	tok, err := p.FrontDoor.Token.Resolve()
	if err != nil {
		return nil, err
	}
	return frontdoor.NewClient(p.FrontDoor.API, tok), nil
}

func frontDoorErr(err error) error {
	var ae *frontdoor.APIError
	if errors.As(err, &ae) && ae.Permanent() {
		return permanentf("%s", ae.Error())
	}
	return err
}

// SetTraffic makes the front door route one service as desired. It is a
// full replacement of that service's route, so a retry is harmless.
func (a *Activities) SetTraffic(ctx context.Context, in TrafficIn) (err error) {
	defer func() { err = fail(err) }()
	s, err := service(in.Plan, in.Service)
	if err != nil {
		return err
	}
	r, err := frontdoor.FromPlan(in.Plan, s, in.Desired)
	if err != nil {
		return permanentf("%s", err.Error())
	}
	c, err := FrontDoorClient(in.Plan)
	if err != nil {
		return err
	}
	return heartbeat(ctx, func() error { return frontDoorErr(c.Apply(ctx, r)) })
}

// SampleTraffic returns each routed service's cumulative counters.
func (a *Activities) SampleTraffic(ctx context.Context, in ServicesIn) (_ []ServiceCounters, err error) {
	defer func() { err = fail(err) }()
	c, err := FrontDoorClient(in.Plan)
	if err != nil {
		return nil, err
	}
	var out []ServiceCounters
	for _, s := range in.Plan.RoutedServices() {
		st, err := c.Stats(ctx, in.Plan.Name, s.Name)
		if err != nil {
			return nil, frontDoorErr(err)
		}
		out = append(out, ServiceCounters{Service: s.Name, Source: st.Source, Target: st.Target})
	}
	return out, nil
}
