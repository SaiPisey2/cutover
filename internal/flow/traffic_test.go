package flow

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/SaiPisey2/cutover/internal/frontdoor"
	"github.com/SaiPisey2/cutover/internal/ops"
	"github.com/SaiPisey2/cutover/internal/plan"
	"github.com/SaiPisey2/cutover/internal/traffic"
)

func servicesPlan() plan.Plan {
	p := testPlan()
	p.FrontDoor = &plan.FrontDoor{API: "http://127.0.0.1:7600", Token: "env:T", Addresses: []string{"203.0.113.10"}}
	p.Services = []plan.Service{
		{Name: "api", Kind: plan.KindExternal, Route: &plan.Route{Host: "shop.example.com", Prefix: "/", HealthPath: "/healthz"},
			MirrorMethods: []string{"GET"}, Source: plan.Placement{Endpoint: "10.0.1.20:8080"}, Target: plan.Placement{Endpoint: "10.8.0.20:8080"}},
		{Name: "worker", Kind: plan.KindExternal},
	}
	p.Thresholds.Shift = plan.Shift{Steps: []int{10, 100}, Hold: plan.Duration(time.Minute), SampleEvery: plan.Duration(10 * time.Second),
		MaxErrorDelta: 0.5, MaxP99: 2, MinRequests: 50}
	p.Thresholds.Rehearse.Window = plan.Duration(time.Minute)
	return p
}

func ctr(req, errs uint64, p99 float64) *traffic.Counters {
	b := []traffic.Bucket{{LE: 10}, {LE: 20}, {LE: 50}, {LE: 100}}
	for i := range b {
		if b[i].LE >= p99 {
			b[i].Count = req
		}
	}
	return &traffic.Counters{Requests: req, Errors: errs, LatencyCount: req, Buckets: b}
}

// sampler returns cumulative counters for call n (0, 1, 2, ...).
type sampler func(n uint64) []ops.ServiceCounters

func healthy(n uint64) []ops.ServiceCounters {
	return []ops.ServiceCounters{{Service: "api", Source: ctr(1000*n, n, 20), Target: ctr(500*n, 0, 20)}}
}

func desc(d frontdoor.Desired) string {
	s := ""
	if d.Source {
		s += "s"
	}
	if d.Target {
		s += "t"
	}
	s += fmt.Sprint(d.Weight)
	if d.Mirror {
		s += "m"
	}
	return s
}

// mockTraffic registers the service and traffic activities. Call it
// before mockAll, which registers succeed-by-default versions.
func mockTraffic(env *testsuite.TestWorkflowEnvironment, c *calls, f fails, sample *sampler) {
	var a *ops.Activities
	var mu sync.Mutex
	var n uint64
	env.OnActivity(a.PrepareService, mock.Anything, mock.Anything).Return(
		func(_ context.Context, in ops.ServiceIn) error {
			c.add("prep:" + in.Service)
			return f["prep:"+in.Service]
		})
	env.OnActivity(a.UndoService, mock.Anything, mock.Anything).Return(
		func(_ context.Context, in ops.ServiceIn) error { c.add("undo-svc:" + in.Service); return nil })
	env.OnActivity(a.CheckHealth, mock.Anything, mock.Anything).Return(
		func(_ context.Context, in ops.ServiceIn) (ops.Health, error) {
			c.add("health:" + in.Service)
			return ops.Health{Healthy: true, Detail: "ok"}, nil
		})
	env.OnActivity(a.SetTraffic, mock.Anything, mock.Anything).Return(
		func(_ context.Context, in ops.TrafficIn) error {
			label := "traffic:" + in.Service + ":" + desc(in.Desired)
			c.add(label)
			return f[label]
		})
	env.OnActivity(a.SampleTraffic, mock.Anything, mock.Anything).Return(
		func(context.Context, ops.ServicesIn) ([]ops.ServiceCounters, error) {
			mu.Lock()
			defer mu.Unlock()
			c.add("sample")
			if err := f["sample"]; err != nil {
				return nil, err
			}
			n++
			return (*sample)(n), nil
		})
}

func startServices(t *testing.T, env *testsuite.TestWorkflowEnvironment, p plan.Plan, actions []action) Result {
	drive(t, env, actions)
	env.ExecuteWorkflow(Migration, Input{MigrationID: mid, Plan: p})
	return result(t, env)
}

func servicesHappy() []action {
	return []action{approve("discover", "alice"), approve("prepare", "alice"), approve("rehearse", "alice"),
		approve("shift-10", "alice"), approve("shift-100", "alice"),
		approve("switch", "alice"), approve("switch", "bob"), approve("finish", "alice")}
}

func index(list []string, label string) int { return slices.Index(list, label) }

func TestServicesHappyPath(t *testing.T) {
	env := newEnv()
	c := &calls{}
	s := sampler(healthy)
	mockTraffic(env, c, nil, &s)
	mockAll(env, c, nil, true)
	r := startServices(t, env, servicesPlan(), servicesHappy())
	if r.State != StateFinished {
		t.Fatalf("state %s: %q", r.State, kinds(r.Events))
	}
	l := c.list()
	order := []string{"prepare", "prep:api", "prep:worker", "traffic:api:st0", "health:api", "health:worker",
		"traffic:api:st0m", "traffic:api:st10", "traffic:api:st100", "pause", "cleanup", "traffic:api:t100"}
	last := -1
	for _, want := range order {
		i := slices.Index(l[last+1:], want)
		if i < 0 {
			t.Fatalf("%s missing or out of order in %q", want, l)
		}
		last += i + 1
	}
	// Mirroring stops after the Rehearse gate, before any weight moves.
	if m, w := index(l, "traffic:api:st0m"), index(l, "traffic:api:st10"); !(m < w && slices.Contains(l[m+1:w], "traffic:api:st0")) {
		t.Fatalf("mirroring must be turned off between Rehearse and Shift: %q", l)
	}
	if slices.Contains(kinds(r.Events), "skipped") {
		t.Fatal("shift is not skipped when the plan has routed services")
	}
}

func TestRehearseGateShowsTraffic(t *testing.T) {
	env := newEnv()
	c := &calls{}
	s := sampler(healthy)
	mockTraffic(env, c, nil, &s)
	mockAll(env, c, nil, true)
	var summary string
	look := action{
		when: func(st Status) bool {
			if st.Gate != nil && st.Gate.Name == "rehearse" {
				summary = st.Gate.Summary
				if len(st.Traffic) != 1 || st.Traffic[0].Target.Requests == 0 {
					t.Errorf("rehearse traffic not in status: %+v", st.Traffic)
				}
				return true
			}
			return false
		},
		do: func(env *testsuite.TestWorkflowEnvironment) {
			env.SignalWorkflow(SignalGate, GateSignal{Gate: "rehearse", Decision: Reject, ApproverID: "alice"})
		},
	}
	r := startServices(t, env, servicesPlan(), []action{approve("discover", "alice"), approve("prepare", "alice"), look})
	if r.State != StateRolledBack || !strings.Contains(summary, "api:") {
		t.Fatalf("%s %q summary %q", r.State, kinds(r.Events), summary)
	}
}

func TestShiftPreapproveAsksOnce(t *testing.T) {
	env := newEnv()
	c := &calls{}
	s := sampler(healthy)
	mockTraffic(env, c, nil, &s)
	mockAll(env, c, nil, true)
	p := servicesPlan()
	p.Thresholds.Shift.Preapprove = true
	steps := []action{approve("discover", "alice"), approve("prepare", "alice"), approve("rehearse", "alice"),
		approve("shift-10", "alice"), approve("switch", "alice"), approve("switch", "bob"), approve("finish", "alice")}
	r := startServices(t, env, p, steps)
	if r.State != StateFinished {
		t.Fatalf("%s %q", r.State, kinds(r.Events))
	}
	for _, e := range r.Events {
		if e.Kind == "gate-opened" && strings.HasPrefix(e.Message, "shift-100") {
			t.Fatal("a preapproved ramp must not ask again at 100%")
		}
	}
}

func brokenAfter(trip uint64) sampler {
	return func(n uint64) []ops.ServiceCounters {
		out := healthy(n)
		if n > trip {
			out[0].Target = ctr(500*n, 250*(n-trip), 20) // half of the new target requests fail
		}
		return out
	}
}

func TestShiftTripsAndRollsBack(t *testing.T) {
	env := newEnv()
	c := &calls{}
	s := brokenAfter(3) // rehearse takes samples 1-2, the step's start is 3
	mockTraffic(env, c, nil, &s)
	mockAll(env, c, nil, true)
	reject := atGate("shift-tripped", "alice", Reject)
	steps := []action{approve("discover", "alice"), approve("prepare", "alice"), approve("rehearse", "alice"),
		approve("shift-10", "alice"), reject}
	r := startServices(t, env, servicesPlan(), steps)
	if r.State != StateRolledBack || !slices.Contains(kinds(r.Events), "shift-tripped") {
		t.Fatalf("%s %q", r.State, kinds(r.Events))
	}
	l := c.list()
	i := index(l, "traffic:api:st10")
	if i < 0 || !slices.Contains(l[i+1:], "traffic:api:st0") {
		t.Fatalf("weight must return to 0 after the trip: %q", l)
	}
	tail := c.since("traffic:api:s0")
	if len(tail) < 3 {
		t.Fatalf("the unwind must route api back to the source, then undo both services: %q", l)
	}
	assertCalls(t, tail[:3], []string{"traffic:api:s0", "undo-svc:worker", "undo-svc:api"})
	for _, e := range r.Events {
		if e.Kind == "shift-tripped" && !strings.Contains(e.Message, "api: target error rate") {
			t.Fatalf("trip reason %q", e.Message)
		}
	}
}

func TestShiftTrippedApprovalRestartsRamp(t *testing.T) {
	env := newEnv()
	c := &calls{}
	broken := true
	var mu sync.Mutex
	s := sampler(func(n uint64) []ops.ServiceCounters {
		mu.Lock()
		defer mu.Unlock()
		if broken {
			return brokenAfter(3)(n)
		}
		return healthy(n)
	})
	mockTraffic(env, c, nil, &s)
	mockAll(env, c, nil, true)
	fix := action{
		when: func(st Status) bool { return st.Gate != nil && st.Gate.Name == "shift-tripped" },
		do: func(env *testsuite.TestWorkflowEnvironment) {
			mu.Lock()
			broken = false
			mu.Unlock()
			env.SignalWorkflow(SignalGate, GateSignal{Gate: "shift-tripped", Decision: Approve, ApproverID: "alice"})
		},
	}
	steps := []action{approve("discover", "alice"), approve("prepare", "alice"), approve("rehearse", "alice"),
		approve("shift-10", "alice"), fix, approve("shift-100", "alice"),
		approve("switch", "alice"), approve("switch", "bob"), approve("finish", "alice")}
	r := startServices(t, env, servicesPlan(), steps)
	if r.State != StateFinished {
		t.Fatalf("%s %q", r.State, kinds(r.Events))
	}
	if c.count("traffic:api:st10") != 2 {
		t.Fatalf("the ramp restarts at its first step: %q", c.list())
	}
	opened := 0
	for _, e := range r.Events {
		if e.Kind == "gate-opened" && strings.HasPrefix(e.Message, "shift-10:") {
			opened++
		}
	}
	if opened != 1 {
		t.Fatalf("approving shift-tripped is the approval to restart; shift-10 opened %d times", opened)
	}
}

func TestShiftUnjudgedOpensNextGate(t *testing.T) {
	env := newEnv()
	c := &calls{}
	s := sampler(func(n uint64) []ops.ServiceCounters {
		return []ops.ServiceCounters{{Service: "api", Source: ctr(1000*n, 0, 20), Target: ctr(2*n, 0, 20)}}
	})
	mockTraffic(env, c, nil, &s)
	mockAll(env, c, nil, true)
	p := servicesPlan()
	p.Thresholds.Shift.Preapprove = true
	r := startServices(t, env, p, servicesHappy())
	if r.State != StateFinished || !slices.Contains(kinds(r.Events), "shift-unjudged") {
		t.Fatalf("%s %q", r.State, kinds(r.Events))
	}
	// servicesHappy approves shift-100: it only completes if that gate opened.
}

func TestShiftBlindStatsTrips(t *testing.T) {
	env := newEnv()
	c := &calls{}
	s := sampler(healthy)
	f := fails{}
	mockTraffic(env, c, f, &s)
	mockAll(env, c, nil, true)
	breakStats := action{
		when: func(st Status) bool { return st.Gate != nil && st.Gate.Name == "shift-10" },
		do: func(env *testsuite.TestWorkflowEnvironment) {
			f["sample"] = temporal.NewNonRetryableApplicationError("front door: connection refused", "test", nil)
			env.SignalWorkflow(SignalGate, GateSignal{Gate: "shift-10", Decision: Approve, ApproverID: "alice"})
		},
	}
	steps := []action{approve("discover", "alice"), approve("prepare", "alice"), approve("rehearse", "alice"),
		breakStats, atGate("shift-tripped", "alice", Reject)}
	r := startServices(t, env, servicesPlan(), steps)
	if r.State != StateRolledBack {
		t.Fatalf("%s %q", r.State, kinds(r.Events))
	}
	found := false
	for _, e := range r.Events {
		if e.Kind == "shift-tripped" && strings.Contains(e.Message, "cannot read front-door statistics") {
			found = true
		}
	}
	if !found || !slices.Contains(c.since("traffic:api:st10"), "traffic:api:st0") {
		t.Fatalf("blind stats must send traffic back and stop: %q / %q", kinds(r.Events), c.list())
	}
}

func TestRollbackAfterSwitchUndoesTraffic(t *testing.T) {
	env := newEnv()
	c := &calls{}
	s := sampler(healthy)
	mockTraffic(env, c, nil, &s)
	mockAll(env, c, nil, true)
	rb := action{
		when: func(st Status) bool { return st.Phase == PhaseFinish && st.Gate == nil },
		do: func(env *testsuite.TestWorkflowEnvironment) {
			env.SignalWorkflow(SignalRollback, RollbackSignal{ApproverID: "alice", Reason: "test"})
		},
	}
	steps := []action{approve("discover", "alice"), approve("prepare", "alice"), approve("rehearse", "alice"),
		approve("shift-10", "alice"), approve("shift-100", "alice"), approve("switch", "alice"), approve("switch", "bob"), rb}
	r := startServices(t, env, servicesPlan(), steps)
	if r.State != StateRolledBack {
		t.Fatalf("%s %q", r.State, kinds(r.Events))
	}
	tail := c.since("repoint:source")
	// After the database is back: traffic to 0, mirroring off, then the
	// target copies go.
	want := []string{"traffic:api:st0", "traffic:api:st0", "traffic:api:s0", "undo-svc:worker", "undo-svc:api"}
	got := slices.DeleteFunc(slices.Clone(tail), func(s string) bool { return !strings.HasPrefix(s, "traffic:") && !strings.HasPrefix(s, "undo-svc:") })
	assertCalls(t, got, want)
}

func TestPrepareServiceFailureOpensRetryGate(t *testing.T) {
	env := newEnv()
	c := &calls{}
	s := sampler(healthy)
	f := fails{"prep:api": temporal.NewNonRetryableApplicationError("compose up failed", "test", nil)}
	mockTraffic(env, c, f, &s)
	mockAll(env, c, nil, true)
	retry := action{
		when: func(st Status) bool { return st.Gate != nil && st.Gate.Name == "retry-prepare-api" },
		do: func(env *testsuite.TestWorkflowEnvironment) {
			delete(f, "prep:api")
			env.SignalWorkflow(SignalGate, GateSignal{Gate: "retry-prepare-api", Decision: Approve, ApproverID: "alice"})
		},
	}
	steps := []action{approve("discover", "alice"), retry, approve("prepare", "alice"), approve("rehearse", "alice"),
		approve("shift-10", "alice"), approve("shift-100", "alice"), approve("switch", "alice"), approve("switch", "bob"), approve("finish", "alice")}
	r := startServices(t, env, servicesPlan(), steps)
	if r.State != StateFinished || c.count("prep:api") != 2 {
		t.Fatalf("%s %q", r.State, c.list())
	}
}

// latBuckets are the histogram bounds the latency tests use, in ms.
var latBuckets = []float64{10, 20, 50, 100, 200}

// lat returns cumulative histogram counters: for each (le, n) pair, n
// requests that took at most le ms.
func lat(pairs ...float64) *traffic.Counters {
	c := &traffic.Counters{}
	for _, le := range latBuckets {
		c.Buckets = append(c.Buckets, traffic.Bucket{LE: le})
	}
	for i := 0; i < len(pairs); i += 2 {
		le, n := pairs[i], uint64(pairs[i+1])
		c.Requests += n
		c.LatencyCount += n
		for j := range c.Buckets {
			if c.Buckets[j].LE >= le {
				c.Buckets[j].Count += n
			}
		}
	}
	return c
}

// latencySampler: the source always answers within 20ms. During Rehearse
// the target answers within 50ms; in Shift its requests take up to stepMs.
func latencySampler(stepMs float64) sampler {
	return phases(batch{20, 1000}, batch{20, 1000}, batch{50, 500}, batch{stepMs, 500})
}

// Ruling B8: the reference p99 includes the baseline target p99 when the
// target served min_requests during Rehearse. The target's 50ms there
// makes 100ms within 2x; against the source's 20ms alone it would trip.
func TestShiftP99ReferenceIncludesBaselineTarget(t *testing.T) {
	env := newEnv()
	c := &calls{}
	s := latencySampler(60) // lands in the 100ms bucket
	mockTraffic(env, c, nil, &s)
	mockAll(env, c, nil, true)
	r := startServices(t, env, servicesPlan(), servicesHappy())
	if r.State != StateFinished || slices.Contains(kinds(r.Events), "shift-tripped") || slices.Contains(kinds(r.Events), "shift-unjudged") {
		t.Fatalf("%s %q", r.State, kinds(r.Events))
	}
}

func TestShiftP99OverBaselineTargetTrips(t *testing.T) {
	env := newEnv()
	c := &calls{}
	s := latencySampler(120) // lands in the 200ms bucket: more than 2x 50ms
	mockTraffic(env, c, nil, &s)
	mockAll(env, c, nil, true)
	steps := []action{approve("discover", "alice"), approve("prepare", "alice"), approve("rehearse", "alice"),
		approve("shift-10", "alice"), atGate("shift-tripped", "alice", Reject)}
	r := startServices(t, env, servicesPlan(), steps)
	if r.State != StateRolledBack {
		t.Fatalf("%s %q", r.State, kinds(r.Events))
	}
	found := false
	for _, e := range r.Events {
		if e.Kind == "shift-tripped" && strings.Contains(e.Message, "api: target p99 200ms vs reference 50ms") {
			found = true
		}
	}
	if !found {
		t.Fatalf("want a p99 trip against the 50ms baseline target: %q", kinds(r.Events))
	}
}

// With no reference p99 (the source reports no latency, in Rehearse or
// now, and the target served too little in Rehearse to count), a target
// with latency data is unjudged, so a preapproved ramp asks again rather
// than advance on error rate alone.
func TestShiftNoReferenceP99IsUnjudged(t *testing.T) {
	env := newEnv()
	c := &calls{}
	s := sampler(func(n uint64) []ops.ServiceCounters {
		target := 10 * min(n, 2) // 10 requests in the Rehearse window
		if n > 2 {
			target += 500 * (n - 2)
		}
		return []ops.ServiceCounters{{Service: "api",
			Source: &traffic.Counters{Requests: 1000 * n},
			Target: ctr(target, 0, 20)}}
	})
	mockTraffic(env, c, nil, &s)
	mockAll(env, c, nil, true)
	p := servicesPlan()
	p.Thresholds.Shift.Preapprove = true
	r := startServices(t, env, p, servicesHappy())
	if r.State != StateFinished {
		t.Fatalf("%s %q", r.State, kinds(r.Events))
	}
	found := false
	for _, e := range r.Events {
		if e.Kind == "shift-unjudged" && strings.Contains(e.Message, "api: no reference p99") {
			found = true
		}
	}
	if !found {
		t.Fatalf("want shift-unjudged for a missing reference p99: %q", kinds(r.Events))
	}
	// servicesHappy approves shift-100: it only completes if that gate opened.
}

// phases builds a sampler from per-sample request batches: each side
// gets n requests at most le ms per Rehearse sample (1-2), and another
// batch per sample from the first Shift step's start (3) on.
type batch struct {
	le float64
	n  uint64
}

func phases(srcRehearse, srcShift, tgtRehearse, tgtShift batch) sampler {
	return func(n uint64) []ops.ServiceCounters {
		rehearse, step := min(n, 2), uint64(0)
		if n > 2 {
			step = n - 2
		}
		side := func(r, s batch) *traffic.Counters {
			return lat(r.le, float64(r.n*rehearse), s.le, float64(s.n*step))
		}
		return []ops.ServiceCounters{{Service: "api", Source: side(srcRehearse, srcShift), Target: side(tgtRehearse, tgtShift)}}
	}
}

// preapprovedRamp runs a preapproved ramp that asks only at shift-10: it
// finishes only if every step was judged and none tripped.
func preapprovedRamp(t *testing.T, s sampler) Result {
	t.Helper()
	env := newEnv()
	c := &calls{}
	mockTraffic(env, c, nil, &s)
	mockAll(env, c, nil, true)
	p := servicesPlan()
	p.Thresholds.Shift.Preapprove = true
	steps := []action{approve("discover", "alice"), approve("prepare", "alice"), approve("rehearse", "alice"),
		approve("shift-10", "alice"), approve("switch", "alice"), approve("switch", "bob"), approve("finish", "alice")}
	r := startServices(t, env, p, steps)
	if r.State != StateFinished || slices.Contains(kinds(r.Events), "shift-tripped") || slices.Contains(kinds(r.Events), "shift-unjudged") {
		t.Fatalf("%s %q", r.State, kinds(r.Events))
	}
	return r
}

// Ruling B8: when the source serves fewer than min_requests in the
// window, the baseline source p99 (50ms) is the reference; the target
// served too little in Rehearse to count. 100ms is within 2x.
func TestShiftP99ReferenceFallsBackToBaselineSource(t *testing.T) {
	preapprovedRamp(t, phases(batch{50, 1000}, batch{20, 10}, batch{20, 10}, batch{60, 500}))
}

// Ruling B8: this window's source p99 (50ms, over min_requests) counts
// when it is above the baseline's (20ms). 100ms is within 2x of 50ms.
func TestShiftP99ReferenceUsesWindowSource(t *testing.T) {
	preapprovedRamp(t, phases(batch{20, 1000}, batch{50, 1000}, batch{20, 10}, batch{60, 500}))
}

// Ruling B8: a source that served fewer than min_requests in the window
// does not set the reference, however slow its few requests were. The
// target's 100ms trips against the baseline source's 20ms.
func TestShiftP99IgnoresThinWindowSource(t *testing.T) {
	env := newEnv()
	c := &calls{}
	s := phases(batch{20, 1000}, batch{200, 10}, batch{20, 10}, batch{60, 500})
	mockTraffic(env, c, nil, &s)
	mockAll(env, c, nil, true)
	steps := []action{approve("discover", "alice"), approve("prepare", "alice"), approve("rehearse", "alice"),
		approve("shift-10", "alice"), atGate("shift-tripped", "alice", Reject)}
	r := startServices(t, env, servicesPlan(), steps)
	found := false
	for _, e := range r.Events {
		if e.Kind == "shift-tripped" && strings.Contains(e.Message, "api: target p99 100ms vs reference 20ms") {
			found = true
		}
	}
	if r.State != StateRolledBack || !found {
		t.Fatalf("%s %q", r.State, kinds(r.Events))
	}
}
