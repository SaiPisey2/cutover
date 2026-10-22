//go:build integration

package flow_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/SaiPisey2/cutover/internal/flow"
	"github.com/SaiPisey2/cutover/internal/frontdoor"
	"github.com/SaiPisey2/cutover/internal/itest"
	"github.com/SaiPisey2/cutover/internal/plan"
	"github.com/SaiPisey2/cutover/internal/secret"
)

// app is one copy of the demo application.
type app struct {
	side    string
	addr    string
	failing atomic.Bool
	mu      sync.Mutex
	gets    int
	posts   int
}

func startApp(t *testing.T, side string) *app {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), itest.PoolDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	a := &app{side: side, addr: l.Addr().String()}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Side", side)
		if r.URL.Path == "/healthz" {
			return
		}
		if a.failing.Load() {
			http.Error(w, "injected fault", 500)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		switch r.Method {
		case "GET", "HEAD":
			a.mu.Lock()
			a.gets++
			a.mu.Unlock()
			var n int64
			if err := pool.QueryRow(ctx, "SELECT count(*) FROM public.items").Scan(&n); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			fmt.Fprint(w, n)
		case "POST":
			a.mu.Lock()
			a.posts++
			a.mu.Unlock()
			var id int64
			if err := pool.QueryRow(ctx, "INSERT INTO public.items (note) VALUES ($1) RETURNING id", side).Scan(&id); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			fmt.Fprint(w, id)
		}
	})}
	go srv.Serve(l)
	t.Cleanup(func() { srv.Close() })
	return a
}

func (a *app) counts() (int, int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.gets, a.posts
}

type rig struct {
	front  string
	client *frontdoor.Client
	plan   plan.Plan
	src    *app
	tgt    *app
	tc     client.Client
	// http keeps a connection per load worker alive (the default two idle
	// connections per host churn through ephemeral ports under load), and
	// times out so a wedged front door fails the test instead of hanging it.
	http *http.Client
}

func newTrafficRig(t *testing.T) *rig {
	t.Helper()
	itest.Reset(t)
	src := itest.Conn(t, itest.SourceDSN)
	if _, err := src.Exec(context.Background(), "CREATE TABLE public.items (id bigserial PRIMARY KEY, note text)"); err != nil {
		t.Fatal(err)
	}
	r := &rig{src: startApp(t, "source"), tgt: startApp(t, "target"),
		http: &http.Client{Timeout: 20 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 16}}}
	t.Cleanup(r.http.CloseIdleConnections)

	// The front door: cutover server in-process, Envoy natively.
	frontPort, adminPort := itest.FreePort(t), itest.FreePort(t)
	xdsAddr := "127.0.0.1:" + strconv.Itoa(itest.FreePort(t))
	apiAddr := "127.0.0.1:" + strconv.Itoa(itest.FreePort(t))
	x, err := frontdoor.NewXDS(frontdoor.XDSConfig{Listener: frontdoor.Listener{Address: "127.0.0.1", Port: frontPort},
		Store: &frontdoor.Store{Path: t.TempDir() + "/fd.json"}, Drain: time.Second, AckTimeout: 15 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	xl, err := frontdoor.ListenXDS(xdsAddr)
	if err != nil {
		t.Fatal(err)
	}
	go x.Serve(xl)
	t.Cleanup(x.Stop)
	boot, err := frontdoor.Bootstrap(frontdoor.BootstrapConfig{XDS: xdsAddr, Admin: "127.0.0.1:" + strconv.Itoa(adminPort)})
	if err != nil {
		t.Fatal(err)
	}
	admin := itest.StartEnvoy(t, boot, adminPort)
	al, err := frontdoor.ListenAPI(apiAddr)
	if err != nil {
		t.Fatal(err)
	}
	const token = "traffic-itest-token-0123456789"
	api := &frontdoor.API{XDS: x, Admin: &frontdoor.Admin{Base: admin, HTTP: &http.Client{Timeout: 5 * time.Second}}, Token: token}
	hs := &http.Server{Handler: api.Handler()}
	go hs.Serve(al)
	t.Cleanup(func() { hs.Close() })
	r.front = "http://127.0.0.1:" + strconv.Itoa(frontPort)
	r.client = frontdoor.NewClient("http://"+apiAddr, token)

	t.Setenv("CUTOVER_IT_FD_TOKEN", token)
	p := itest.Plan(t)
	p.Name = "traffic"
	p.FrontDoor = &plan.FrontDoor{API: "http://" + apiAddr, Token: secret.Ref("env:CUTOVER_IT_FD_TOKEN"), Addresses: []string{"127.0.0.1", "::1"}}
	p.Services = []plan.Service{{Name: "app", Kind: plan.KindExternal,
		Route:         &plan.Route{Host: "localhost", Prefix: "/", HealthPath: "/healthz"},
		MirrorMethods: []string{"GET"},
		Source:        plan.Placement{Endpoint: r.src.addr}, Target: plan.Placement{Endpoint: r.tgt.addr}}}
	p.Thresholds.Rehearse.Window = plan.Duration(3 * time.Second)
	p.Thresholds.Shift = plan.Shift{Steps: []int{10, 100}, Hold: plan.Duration(4 * time.Second), SampleEvery: plan.Duration(time.Second),
		MaxErrorDelta: 0.5, MaxP99: 50, MinRequests: 5}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	r.plan = p

	// What `cutover route apply` does.
	route, err := frontdoor.FromPlan(p, p.Services[0], frontdoor.Desired{Source: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// The server answers at once while Envoy has not connected yet; wait
	// for the stream, not longer.
	for {
		err := r.client.Apply(ctx, route)
		if err == nil {
			break
		}
		var ae *frontdoor.APIError
		if !errors.As(err, &ae) || !strings.Contains(ae.Message, "no Envoy is connected") || ctx.Err() != nil {
			t.Fatalf("route apply: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}

	c, _ := itest.StartTemporal(t)
	w := worker.New(c, flow.TaskQueue, flow.WorkerOptions())
	flow.Register(w)
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)
	r.tc = c
	return r
}

// httpLoad drives the front door until stopped.
type httpLoad struct {
	stop   chan struct{}
	once   sync.Once
	wg     sync.WaitGroup
	mu     sync.Mutex
	ok     int
	failed []failure
	ids    []int64
}

// failure is one failed request and when it was sent.
type failure struct {
	started time.Time
	what    string
}

func (r *rig) do(method string) (int, string, string, error) {
	req, _ := http.NewRequest(method, r.front+"/items", nil)
	req.Host = "localhost"
	resp, err := r.http.Do(req)
	if err != nil {
		return 0, "", "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, strings.TrimSpace(string(b)), resp.Header.Get("X-Side"), nil
}

// startLoad starts the load; it stops at l.Stop or when the test ends.
func (r *rig) startLoad(t *testing.T, workers int) *httpLoad {
	l := &httpLoad{stop: make(chan struct{})}
	t.Cleanup(l.Stop)
	for i := 0; i < workers; i++ {
		l.wg.Add(1)
		go func(i int) {
			defer l.wg.Done()
			for n := 0; ; n++ {
				select {
				case <-l.stop:
					return
				default:
				}
				m := "GET"
				if (n+i)%2 == 0 {
					m = "POST"
				}
				started := time.Now()
				code, body, _, err := r.do(m)
				var id int64
				if err == nil && code == 200 && m == "POST" {
					// An acknowledged write must answer its id.
					id, err = strconv.ParseInt(body, 10, 64)
				}
				l.mu.Lock()
				if err != nil || code != 200 {
					l.failed = append(l.failed, failure{started, fmt.Sprintf("%s %d %v %s", m, code, err, body)})
				} else {
					l.ok++
					if m == "POST" {
						l.ids = append(l.ids, id)
					}
				}
				l.mu.Unlock()
				time.Sleep(5 * time.Millisecond)
			}
		}(i)
	}
	return l
}

// Stop stops the load and waits for it; calling it again is a no-op.
func (l *httpLoad) Stop() {
	l.once.Do(func() { close(l.stop) })
	l.wg.Wait()
}

func (r *rig) start(t *testing.T, id string) client.WorkflowRun {
	t.Helper()
	run, err := r.tc.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{ID: flow.WorkflowID(id), TaskQueue: flow.TaskQueue},
		flow.Migration, flow.Input{MigrationID: id, Plan: r.plan})
	if err != nil {
		t.Fatal(err)
	}
	return run
}

// requireLatency fails unless the window st reports timed requests on
// the target, and on the source too when withSource is set (the source
// is the p99 reference; at 100% it carries no traffic, so it has none).
// Without them latency is never judged.
func requireLatency(t *testing.T, window string, st flow.Status, withSource bool) {
	t.Helper()
	if len(st.Traffic) != 1 || !st.Traffic[0].Target.P99Known || (withSource && !st.Traffic[0].Source.P99Known) {
		t.Fatalf("the %s window has no latency data: %+v", window, st.Traffic)
	}
	t.Logf("%s window: source %s; target %s", window, flow.Describe(st.Traffic[0].Source), flow.Describe(st.Traffic[0].Target))
}

func TestTrafficAndDatabaseCutoverUnderLoad(t *testing.T) {
	r := newTrafficRig(t)
	load := r.startLoad(t, 6)
	run := r.start(t, "traffic")
	id := flow.WorkflowID("traffic")
	for _, g := range []string{"discover", "prepare"} {
		decide(t, r.tc, id, g, "alice", flow.Approve)
	}
	st := waitFor(t, r.tc, id, "rehearse", 2*time.Minute, func(s flow.Status) bool { return s.Gate != nil && s.Gate.Name == "rehearse" })
	if len(st.Traffic) != 1 || st.Traffic[0].Target.Requests == 0 {
		t.Fatalf("rehearse saw no mirrored traffic: %+v", st.Traffic)
	}
	gets, posts := r.tgt.counts()
	if gets == 0 || posts != 0 {
		t.Fatalf("in Rehearse the target got %d GETs and %d POSTs; want mirrored GETs only", gets, posts)
	}
	requireLatency(t, "rehearse", st, true)
	for _, g := range []string{"rehearse", "shift-10"} {
		decide(t, r.tc, id, g, "alice", flow.Approve)
	}
	// The gate after each Shift step shows that step's window: the
	// target's latency must have been measured, or the p99 rule never ran.
	st = waitFor(t, r.tc, id, "shift-100", 2*time.Minute, func(s flow.Status) bool { return s.Gate != nil && s.Gate.Name == "shift-100" })
	requireLatency(t, "shift-10", st, true)
	decide(t, r.tc, id, "shift-100", "alice", flow.Approve)
	st = waitFor(t, r.tc, id, "switch", 2*time.Minute, func(s flow.Status) bool { return s.Gate != nil && s.Gate.Name == "switch" })
	requireLatency(t, "shift-100", st, false)
	for _, step := range []struct{ gate, who string }{{"switch", "alice"}, {"switch", "bob"}, {"finish", "alice"}} {
		decide(t, r.tc, id, step.gate, step.who, flow.Approve)
	}
	var res flow.Result
	if err := run.Get(context.Background(), &res); err != nil {
		t.Fatal(err)
	}
	load.Stop()
	if res.State != flow.StateFinished {
		t.Fatalf("state %s", res.State)
	}
	// Every Shift step was judged: none passed for want of evidence.
	for _, e := range res.Events {
		if e.Kind == "shift-unjudged" {
			t.Fatalf("a Shift step went unjudged: %s", e.Message)
		}
	}
	if len(load.failed) > 0 {
		t.Fatalf("%d of %d requests failed; first: %s", len(load.failed), load.ok+len(load.failed), load.failed[0].what)
	}
	// After Finish the front door knows only the target.
	for i := 0; i < 20; i++ {
		if code, _, side, err := r.do("GET"); err != nil || code != 200 || side != "target" {
			t.Fatalf("after finish: %d %q %v", code, side, err)
		}
	}
	// Every acknowledged write is on the target exactly once.
	tgt := itest.Conn(t, itest.TargetDSN)
	rows, err := tgt.Query(context.Background(), "SELECT id FROM public.items ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	var have []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		have = append(have, v)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	slices.Sort(load.ids)
	if !slices.Equal(have, load.ids) {
		t.Fatalf("target holds %d rows, %d writes were acknowledged", len(have), len(load.ids))
	}
	if len(load.ids) < 50 {
		t.Fatalf("only %d writes ran; the load did not cover the migration", len(load.ids))
	}
	t.Logf("%d requests, %d acknowledged writes, 0 failed", load.ok, len(load.ids))
}

func TestTrafficTripwireRollsBack(t *testing.T) {
	r := newTrafficRig(t)
	load := r.startLoad(t, 6)
	run := r.start(t, "tripwire")
	id := flow.WorkflowID("tripwire")
	for _, g := range []string{"discover", "prepare", "rehearse"} {
		decide(t, r.tc, id, g, "alice", flow.Approve)
	}
	waitFor(t, r.tc, id, "shift-10", 2*time.Minute, func(s flow.Status) bool { return s.Gate != nil && s.Gate.Name == "shift-10" })
	r.tgt.failing.Store(true) // the target breaks just as traffic moves
	decide(t, r.tc, id, "shift-10", "alice", flow.Approve)
	began := time.Now()
	st := waitFor(t, r.tc, id, "shift-tripped", time.Minute, func(s flow.Status) bool { return s.Gate != nil && s.Gate.Name == "shift-tripped" })
	tripped := time.Now()
	took := tripped.Sub(began)
	if took > 4*time.Second+2*time.Second {
		t.Fatalf("the tripwire took %s; it must fire within one hold (4s) plus 2s to apply weights", took)
	}
	if st.Weight != 0 {
		t.Fatalf("weight %d after the trip, want 0", st.Weight)
	}
	// Traffic is back on the source while the gate waits.
	for i := 0; i < 20; i++ {
		if code, _, side, err := r.do("GET"); err != nil || code != 200 || side != "source" {
			t.Fatalf("after the trip: %d from %q %v", code, side, err)
		}
	}
	decide(t, r.tc, id, "shift-tripped", "alice", flow.Reject)
	var res flow.Result
	if err := run.Get(context.Background(), &res); err != nil {
		t.Fatal(err)
	}
	load.Stop()
	if res.State != flow.StateRolledBack {
		t.Fatalf("state %s", res.State)
	}
	routes, err := r.client.Routes(context.Background())
	if err != nil || len(routes) != 1 || routes[0].Target != nil || routes[0].Source == nil {
		t.Fatalf("after rollback the route is source only: %+v %v", routes, err)
	}
	// Every failure came from the broken target, and from a request sent
	// before the trip was reported: once the gate is open the weights are
	// back on the source (requests in flight then may still fail).
	for _, f := range load.failed {
		if !strings.Contains(f.what, "injected fault") {
			t.Fatalf("a failure not caused by the injected fault: %s", f.what)
		}
		if f.started.After(tripped) {
			t.Fatalf("a request sent %s after the trip was reported still reached the broken target: %s", f.started.Sub(tripped), f.what)
		}
	}
	t.Logf("tripped %s after the shift was approved; %d requests, %d failed (all injected)", took, load.ok+len(load.failed), len(load.failed))
}
