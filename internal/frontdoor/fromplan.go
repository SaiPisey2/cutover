package frontdoor

import (
	"fmt"
	"slices"

	"github.com/SaiPisey2/cutover/internal/plan"
)

// Desired is how a migration wants one service's traffic: which copies
// are in the route, the target's share, and whether to mirror.
type Desired struct {
	Source bool `json:"source"`
	Target bool `json:"target"`
	Weight int  `json:"weight"`
	Mirror bool `json:"mirror"`
}

// FromPlan builds the front-door route for s. Mirroring is for a
// zero-weight target only (the route validator rejects both at once) and
// is skipped for a service with mirror_methods: [].
func FromPlan(p plan.Plan, s plan.Service, d Desired) (Route, error) {
	if s.Route == nil {
		return Route{}, fmt.Errorf("service %s has no route", s.Name)
	}
	r := Route{Owner: p.Name, Service: s.Name, Host: s.Route.Host, Prefix: s.Route.Prefix, HealthPath: s.Route.HealthPath, TargetWeight: d.Weight}
	if d.Source {
		u, err := ParseUpstream(s.Source.Endpoint)
		if err != nil {
			return Route{}, fmt.Errorf("service %s source: %w", s.Name, err)
		}
		r.Source = &u
	}
	if d.Target {
		u, err := ParseUpstream(s.Target.Endpoint)
		if err != nil {
			return Route{}, fmt.Errorf("service %s target: %w", s.Name, err)
		}
		r.Target = &u
	}
	if d.Mirror && d.Weight == 0 && len(s.MirrorMethods) > 0 {
		r.Mirror, r.MirrorMethods = true, slices.Clone(s.MirrorMethods)
	}
	return r, r.Validate()
}
