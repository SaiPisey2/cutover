package frontdoor

import (
	"testing"

	"github.com/SaiPisey2/cutover/internal/plan"
)

func TestFromPlan(t *testing.T) {
	p := plan.Plan{Name: "shop"}
	s := plan.Service{Name: "api", Route: &plan.Route{Host: "shop.example.com", Prefix: "/", HealthPath: "/healthz"},
		MirrorMethods: []string{"GET", "HEAD"}, Source: plan.Placement{Endpoint: "10.0.1.20:8080"}, Target: plan.Placement{Endpoint: "10.8.0.20:8080"}}
	r, err := FromPlan(p, s, Desired{Source: true})
	if err != nil || r.Target != nil || r.Source.Port != 8080 || r.Key() != "shop/api" {
		t.Fatalf("source only: %+v %v", r, err)
	}
	r, err = FromPlan(p, s, Desired{Source: true, Target: true, Weight: 0, Mirror: true})
	if err != nil || r.TargetWeight != 0 || !r.Mirror || len(r.MirrorMethods) != 2 || r.Target.Address != "10.8.0.20" {
		t.Fatalf("both: %+v %v", r, err)
	}
	if r, err := FromPlan(p, s, Desired{Source: true, Target: true, Weight: 10, Mirror: true}); err != nil || r.Mirror || r.TargetWeight != 10 {
		t.Fatalf("a weighted route is never mirrored: %+v %v", r, err)
	}
	s.MirrorMethods = []string{}
	if r, err := FromPlan(p, s, Desired{Source: true, Target: true, Mirror: true}); err != nil || r.Mirror {
		t.Fatalf("mirroring off for the service: %+v %v", r, err)
	}
	if _, err := FromPlan(p, plan.Service{Name: "worker"}, Desired{Source: true}); err == nil {
		t.Fatal("a service without a route has no front-door route")
	}
}
