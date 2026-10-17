package ops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"go.temporal.io/sdk/temporal"

	"github.com/SaiPisey2/cutover/internal/frontdoor"
	"github.com/SaiPisey2/cutover/internal/plan"
	"github.com/SaiPisey2/cutover/internal/readiness"
	"github.com/SaiPisey2/cutover/internal/svc"
	"github.com/SaiPisey2/cutover/internal/traffic"
)

type readinessInput = readiness.Input

// fakeFrontDoor is a stand-in for cutover server's API.
type fakeFrontDoor struct {
	mu     sync.Mutex
	puts   []frontdoor.Route
	routes []frontdoor.Route
	health map[string]frontdoor.Health
	stats  map[string]frontdoor.Stats
	status int // non-zero: every PUT fails with this
}

func (f *fakeFrontDoor) serve(t *testing.T) string {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fd-token" {
			w.WriteHeader(401)
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		switch {
		case r.Method == "PUT":
			if f.status != 0 {
				w.WriteHeader(f.status)
				json.NewEncoder(w).Encode(map[string]string{"error": "conflict with another route"})
				return
			}
			var rt frontdoor.Route
			b, _ := io.ReadAll(r.Body)
			json.Unmarshal(b, &rt)
			f.puts = append(f.puts, rt)
			w.Write([]byte("{}"))
		case r.URL.Path == "/v1/routes":
			json.NewEncoder(w).Encode(map[string]any{"routes": f.routes})
		case strings.HasSuffix(r.URL.Path, "/health"):
			json.NewEncoder(w).Encode(f.health[strings.Split(r.URL.Path, "/")[4]])
		case strings.HasSuffix(r.URL.Path, "/stats"):
			json.NewEncoder(w).Encode(f.stats[strings.Split(r.URL.Path, "/")[4]])
		default:
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func trafficPlan(t *testing.T, api string) plan.Plan {
	t.Setenv("CUTOVER_TEST_FD_TOKEN", "fd-token")
	return plan.Plan{Name: "shop",
		FrontDoor: &plan.FrontDoor{API: api, Token: "env:CUTOVER_TEST_FD_TOKEN", Addresses: []string{"203.0.113.10"}},
		Services: []plan.Service{
			{Name: "api", Kind: plan.KindExternal, Route: &plan.Route{Host: "shop.example.com", Prefix: "/", HealthPath: "/healthz"},
				MirrorMethods: []string{"GET"}, Source: plan.Placement{Endpoint: "10.0.1.20:8080"}, Target: plan.Placement{Endpoint: "10.8.0.20:8080"}},
			{Name: "worker", Kind: plan.KindExternal},
		}}
}

// nonRetryable: ops.go already has an isPermanent of its own.
func nonRetryable(err error) bool {
	var app *temporal.ApplicationError
	return errors.As(err, &app) && app.NonRetryable()
}

func TestSetTraffic(t *testing.T) {
	fd := &fakeFrontDoor{}
	p := trafficPlan(t, fd.serve(t))
	a := New()
	err := a.SetTraffic(context.Background(), TrafficIn{Plan: p, Service: "api", Desired: frontdoor.Desired{Source: true, Target: true, Weight: 0, Mirror: true}})
	if err != nil {
		t.Fatal(err)
	}
	if len(fd.puts) != 1 || fd.puts[0].TargetWeight != 0 || !fd.puts[0].Mirror || fd.puts[0].Target.Port != 8080 {
		t.Fatalf("put %+v", fd.puts)
	}
	fd.status = 409
	err = a.SetTraffic(context.Background(), TrafficIn{Plan: p, Service: "api", Desired: frontdoor.Desired{Source: true}})
	if !nonRetryable(err) || !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("a 409 is permanent: %v", err)
	}
	fd.status = 502
	if err := a.SetTraffic(context.Background(), TrafficIn{Plan: p, Service: "api", Desired: frontdoor.Desired{Source: true}}); err == nil || nonRetryable(err) {
		t.Fatalf("a 502 (Envoy did not confirm) is retryable: %v", err)
	}
	if err := a.SetTraffic(context.Background(), TrafficIn{Plan: p, Service: "worker", Desired: frontdoor.Desired{Source: true}}); !nonRetryable(err) {
		t.Fatalf("a service without a route: %v", err)
	}
}

func TestSampleTraffic(t *testing.T) {
	fd := &fakeFrontDoor{stats: map[string]frontdoor.Stats{"api": {Source: &traffic.Counters{Requests: 100}, Target: &traffic.Counters{Requests: 7}}}}
	p := trafficPlan(t, fd.serve(t))
	got, err := New().SampleTraffic(context.Background(), ServicesIn{Plan: p})
	if err != nil || len(got) != 1 || got[0].Service != "api" || got[0].Target.Requests != 7 {
		t.Fatalf("%+v %v", got, err)
	}
}

func TestCheckHealth(t *testing.T) {
	fd := &fakeFrontDoor{health: map[string]frontdoor.Health{"api": {Source: frontdoor.Healthy, Target: frontdoor.Pending}}}
	p := trafficPlan(t, fd.serve(t))
	h, err := New().CheckHealth(context.Background(), ServiceIn{Plan: p, Service: "api"})
	if err != nil || h.Healthy || !strings.Contains(h.Detail, "pending") {
		t.Fatalf("%+v %v", h, err)
	}
	fd.health["api"] = frontdoor.Health{Source: frontdoor.Healthy, Target: frontdoor.Healthy}
	if h, err := New().CheckHealth(context.Background(), ServiceIn{Plan: p, Service: "api"}); err != nil || !h.Healthy {
		t.Fatalf("%+v %v", h, err)
	}
	// A service without a route: only its driver decides.
	if h, err := New().CheckHealth(context.Background(), ServiceIn{Plan: p, Service: "worker"}); err != nil || !h.Healthy {
		t.Fatalf("%+v %v", h, err)
	}
}

type fakeDriver struct {
	facts   svc.Facts
	err     error
	prepare error
}

func (f fakeDriver) Inspect(context.Context, svc.Side, svc.Side) (svc.Facts, error) {
	return f.facts, f.err
}
func (f fakeDriver) Prepare(context.Context, svc.Side, svc.Side) error       { return f.prepare }
func (f fakeDriver) Healthy(context.Context, svc.Side) (bool, string, error) { return true, "", nil }
func (f fakeDriver) Undo(context.Context, svc.Side) error                    { return nil }

func TestServicesInput(t *testing.T) {
	fd := &fakeFrontDoor{routes: []frontdoor.Route{{Owner: "shop", Service: "api"}}}
	p := trafficPlan(t, fd.serve(t))
	a := &Activities{
		driver: func(_ plan.Plan, s plan.Service) (svc.Driver, error) {
			if s.Name == "worker" {
				return fakeDriver{err: errors.New("dial: postgres://app:hunter2@db/x refused")}, nil
			}
			return fakeDriver{facts: svc.Facts{Service: s.Name, Kind: s.Kind}}, nil
		},
		lookupHost: func(_ context.Context, h string) ([]string, error) {
			if h == "shop.example.com" {
				return []string{"203.0.113.10"}, nil
			}
			return nil, fmt.Errorf("no such host %s", h)
		},
	}
	var in readinessInput
	a.servicesInput(context.Background(), p, &in)
	if len(in.Services) != 1 || in.Services[0].Service != "api" {
		t.Fatalf("services %+v", in.Services)
	}
	if e := in.ServiceErrs["worker"]; e == "" || strings.Contains(e, "hunter2") {
		t.Fatalf("worker error must be recorded and redacted: %q", e)
	}
	if len(in.Routes) != 1 || in.RoutesErr != "" {
		t.Fatalf("routes %+v %q", in.Routes, in.RoutesErr)
	}
	if got := in.DNS["shop.example.com"]; len(got) != 1 {
		t.Fatalf("dns %+v", in.DNS)
	}
}

func TestPrepareServiceBlockedIsPermanent(t *testing.T) {
	p := trafficPlan(t, "http://127.0.0.1:1")
	a := &Activities{driver: func(plan.Plan, plan.Service) (svc.Driver, error) {
		return fakeDriver{prepare: fmt.Errorf("%w: shop/api: bind mount", svc.ErrBlocked)}, nil
	}}
	if err := a.PrepareService(context.Background(), ServiceIn{Plan: p, Service: "api"}); !nonRetryable(err) {
		t.Fatalf("got %v", err)
	}
}
