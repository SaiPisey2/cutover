package readiness

import (
	"slices"
	"strings"
	"testing"

	"github.com/SaiPisey2/cutover/internal/frontdoor"
	"github.com/SaiPisey2/cutover/internal/plan"
	"github.com/SaiPisey2/cutover/internal/svc"
)

func withServices() Input {
	in := good()
	in.Plan.Name = "shop"
	in.Plan.FrontDoor = &plan.FrontDoor{API: "http://127.0.0.1:7600", Token: "env:T", Addresses: []string{"203.0.113.10", "2001:db8::10"}}
	in.Plan.Services = []plan.Service{
		{Name: "api", Kind: plan.KindExternal, Route: &plan.Route{Host: "shop.example.com", Prefix: "/", HealthPath: "/healthz"},
			MirrorMethods: []string{"GET"}, Source: plan.Placement{Endpoint: "10.0.1.20:8080"}, Target: plan.Placement{Endpoint: "10.8.0.20:8080"}},
		{Name: "worker", Kind: plan.KindDeployment, Deployment: &plan.Deployment{Namespace: "shop", Name: "worker"}, MirrorMethods: []string{"GET"}},
	}
	in.Services = []svc.Facts{{Service: "api", Kind: plan.KindExternal}, {Service: "worker", Kind: plan.KindDeployment}}
	in.Routes = []frontdoor.Route{{Owner: "shop", Service: "api", Host: "shop.example.com", Prefix: "/", HealthPath: "/healthz",
		Source: &frontdoor.Upstream{Address: "10.0.1.20", Port: 8080}}}
	in.DNS = map[string][]string{"shop.example.com": {"203.0.113.10", "2001:db8::10"}}
	return in
}

func TestServicesReady(t *testing.T) {
	r := Assess(withServices())
	if r.Blocked() {
		t.Fatalf("unexpected blockers: %+v", r.Findings)
	}
	if !slices.Contains(codes(r, Warning), "service-unrouted") {
		t.Fatalf("the background worker must be called out: %+v", r.Findings)
	}
}

func TestServiceFindings(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*Input)
		sev  Severity
		code string
	}{
		{"driver blocker", func(in *Input) {
			in.Services[1].Findings = []svc.Finding{{Blocker: true, Code: "k8s-state", Subject: "shop/worker", Message: "mounts a PVC"}}
		}, Blocker, "k8s-state"},
		{"driver warning", func(in *Input) {
			in.Services[1].Findings = []svc.Finding{{Code: "k8s-sa-token", Subject: "Secret/x", Message: "token"}}
		}, Warning, "k8s-sa-token"},
		{"unreachable", func(in *Input) { in.ServiceErrs = map[string]string{"worker": "source: dial tcp: refused"} }, Blocker, "service-unreachable"},
		{"front door down", func(in *Input) { in.Routes, in.RoutesErr = nil, "connection refused" }, Blocker, "frontdoor-unreachable"},
		{"not registered", func(in *Input) { in.Routes = nil }, Blocker, "route-not-registered"},
		{"mismatch endpoint", func(in *Input) { in.Routes[0].Source.Port = 9999 }, Blocker, "route-mismatch"},
		{"mismatch health", func(in *Input) { in.Routes[0].HealthPath = "/" }, Blocker, "route-mismatch"},
		{"busy", func(in *Input) {
			in.Routes[0].Target, in.Routes[0].TargetWeight = &frontdoor.Upstream{Address: "10.8.0.20", Port: 8080}, 10
		}, Blocker, "route-busy"},
		{"busy mirror", func(in *Input) { in.Routes[0].Mirror = true }, Blocker, "route-busy"},
		{"mirror off", func(in *Input) { in.Plan.Services[0].MirrorMethods = []string{} }, Info, "mirror-off"},
		{"dns error", func(in *Input) { in.DNS, in.DNSErr = nil, map[string]string{"shop.example.com": "no such host"} }, Blocker, "dns-lookup"},
		{"dns elsewhere", func(in *Input) { in.DNS["shop.example.com"] = []string{"203.0.113.10", "198.51.100.7"} }, Blocker, "dns-not-front-door"},
		{"dns empty", func(in *Input) { in.DNS["shop.example.com"] = nil }, Blocker, "dns-not-front-door"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := withServices()
			tc.edit(&in)
			r := Assess(in)
			if !slices.Contains(codes(r, tc.sev), tc.code) {
				t.Fatalf("want %s %s, got %+v", tc.sev, tc.code, r.Findings)
			}
		})
	}
}

func TestRouteNotRegisteredSaysHowToFix(t *testing.T) {
	in := withServices()
	in.Routes = nil
	found := false
	for _, f := range Assess(in).Findings {
		if f.Code == "route-not-registered" {
			found = true
			if !strings.Contains(f.Message, "cutover route apply --plan") {
				t.Fatalf("the message must name the fix: %s", f.Message)
			}
		}
	}
	if !found {
		t.Fatal("no route-not-registered finding")
	}
}

func TestFrontDoorDownReportedOnce(t *testing.T) {
	in := withServices()
	in.Plan.Services = append(in.Plan.Services, plan.Service{Name: "web", Kind: plan.KindExternal,
		Route: &plan.Route{Host: "www.example.com", Prefix: "/", HealthPath: "/"}, MirrorMethods: []string{"GET"},
		Source: plan.Placement{Endpoint: "10.0.1.30:80"}, Target: plan.Placement{Endpoint: "10.8.0.30:80"}})
	in.DNS["www.example.com"] = []string{"203.0.113.10"}
	in.Routes, in.RoutesErr = nil, "refused"
	n := 0
	for _, f := range Assess(in).Findings {
		if f.Code == "frontdoor-unreachable" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("front door down reported %d times", n)
	}
}

func TestPlanWithoutServicesHasNoServiceFindings(t *testing.T) {
	r := Assess(good())
	for _, f := range r.Findings {
		if strings.HasPrefix(f.Code, "route-") || strings.HasPrefix(f.Code, "dns-") || strings.HasPrefix(f.Code, "service-") {
			t.Fatalf("%+v", f)
		}
	}
}

// The front door treats "/x" and "/x/" as one prefix, so they are not a mismatch.
func TestTrailingSlashPrefixIsNotAMismatch(t *testing.T) {
	in := withServices()
	in.Plan.Services[0].Route.Prefix = "/x"
	in.Routes[0].Prefix = "/x/"
	if r := Assess(in); r.Blocked() {
		t.Fatalf("unexpected blockers: %+v", r.Findings)
	}
}

// Service findings join Assess's one stable order: severity, code, subject, message.
func TestAssessSortedWithServiceFindings(t *testing.T) {
	in := withServices()
	in.Services[1].Findings = []svc.Finding{
		{Code: "k8s-sa-token", Subject: "Secret/x", Message: "token"},
		{Blocker: true, Code: "k8s-state", Subject: "shop/worker", Message: "mounts a PVC"},
	}
	in.Routes = nil
	in.DNS["shop.example.com"] = []string{"198.51.100.7"}
	r := Assess(in)
	rank := map[Severity]int{Blocker: 0, Warning: 1, Info: 2}
	for i := 1; i < len(r.Findings); i++ {
		a, b := r.Findings[i-1], r.Findings[i]
		ka := []any{rank[a.Severity], a.Code, a.Subject, a.Message}
		kb := []any{rank[b.Severity], b.Code, b.Subject, b.Message}
		if rank[a.Severity] > rank[b.Severity] ||
			(a.Severity == b.Severity && (a.Code > b.Code ||
				(a.Code == b.Code && (a.Subject > b.Subject || (a.Subject == b.Subject && a.Message > b.Message))))) {
			t.Fatalf("unsorted at %d: %v before %v", i, ka, kb)
		}
	}
	var blockers []string
	for _, f := range r.Findings {
		if f.Severity == Blocker {
			blockers = append(blockers, f.Code)
		}
	}
	if !slices.Contains(blockers, "k8s-state") || !slices.Contains(blockers, "route-not-registered") || !slices.Contains(blockers, "dns-not-front-door") {
		t.Fatalf("service findings missing: %v", blockers)
	}
}
