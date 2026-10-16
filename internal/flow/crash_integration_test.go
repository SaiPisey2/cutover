//go:build integration

package flow_test

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	"github.com/SaiPisey2/cutover/internal/flow"
	"github.com/SaiPisey2/cutover/internal/itest"
	"github.com/SaiPisey2/cutover/internal/ops"
	"github.com/SaiPisey2/cutover/internal/pg"
	"github.com/SaiPisey2/cutover/internal/plan"
)

// holdMargin is how far past max_pause a client statement may wait. The
// budget covers pause through repoint; what runs after it is resume (one
// PgBouncer RESUME) and, on an abort, the compensations (here one
// StartLink: a CREATE SUBSCRIPTION with copy_data=false), each a single
// activity on a local dev server plus a workflow task. Each is well under
// a second against the harness; two seconds leaves room for -race and a
// loaded laptop, and is still far below anything that would hide a budget
// that is not enforced.
const holdMargin = 2 * time.Second

// devServer starts a real Temporal dev server (in memory, headless) on a
// free 127.0.0.1 port, using the installed temporal CLI so nothing is
// downloaded.
func devServer(t *testing.T) client.Client {
	t.Helper()
	bin, err := exec.LookPath("temporal")
	if err != nil {
		bin = "/opt/homebrew/bin/temporal"
		if _, err := os.Stat(bin); err != nil {
			t.Fatal("temporal CLI not found on PATH or in /opt/homebrew/bin (brew install temporal)")
		}
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hostPort := l.Addr().String()
	l.Close()
	srv, err := testsuite.StartDevServer(context.Background(), testsuite.DevServerOptions{ExistingPath: bin, LogLevel: "error",
		ClientOptions: &client.Options{HostPort: hostPort, Namespace: "default",
			Logger: sdklog.NewStructuredLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Stop() })
	return srv.Client()
}

// waitFor polls the workflow's status until ok holds, or fails the test.
// Gates other than the one expected are logged once each, so a test stuck
// at a retry gate says why.
func waitFor(t *testing.T, c client.Client, id, expect string, d time.Duration, ok func(flow.Status) bool) flow.Status {
	t.Helper()
	deadline := time.Now().Add(d)
	seen := map[string]bool{}
	var last flow.Status
	var lastErr error
	for time.Now().Before(deadline) {
		qctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		v, err := c.QueryWorkflow(qctx, id, "", flow.QueryStatus)
		cancel()
		lastErr = err
		if err == nil {
			var st flow.Status
			if lastErr = v.Get(&st); lastErr == nil {
				last = st
				if g := st.Gate; g != nil && g.Name != expect && !seen[g.Name] {
					seen[g.Name] = true
					t.Logf("unexpected gate %q open (waiting for %q): %s", g.Name, expect, g.Summary)
				}
				if ok(st) {
					return st
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("waited %s for %s (last error %v); last status phase=%s state=%s events=%+v", d, expect, lastErr, last.Phase, last.State, last.Events)
	return last
}

// decide waits for gate to be open without who's approval, then signals.
func decide(t *testing.T, c client.Client, id, gate, who string, d flow.Decision) {
	t.Helper()
	waitFor(t, c, id, gate, 2*time.Minute, func(s flow.Status) bool {
		return s.Gate != nil && s.Gate.Name == gate && !slices.Contains(s.Gate.Approvals, who)
	})
	if err := c.SignalWorkflow(context.Background(), id, "", flow.SignalGate,
		flow.GateSignal{Gate: gate, Decision: d, ApproverID: who}); err != nil {
		t.Fatal(err)
	}
}

// toSwitch approves every gate up to and including the switch.
func toSwitch(t *testing.T, c client.Client, id string) {
	t.Helper()
	for _, s := range []struct{ gate, who string }{{"discover", "alice"}, {"prepare", "alice"}, {"rehearse", "alice"},
		{"switch", "alice"}, {"switch", "bob"}} {
		decide(t, c, id, s.gate, s.who, flow.Approve)
	}
}

func event(events []flow.Event, kind string) (flow.Event, bool) {
	i := slices.IndexFunc(events, func(e flow.Event) bool { return e.Kind == kind })
	if i < 0 {
		return flow.Event{}, false
	}
	return events[i], true
}

func logEvents(t *testing.T, events []flow.Event) {
	t.Helper()
	for _, e := range events {
		if strings.HasPrefix(e.Kind, "switch") || strings.Contains(e.Kind, "fail") || strings.HasPrefix(e.Kind, "undo") ||
			strings.HasPrefix(e.Kind, "compensation") || e.Kind == "rejected" {
			t.Logf("event %s %s/%s: %s", e.At.Format("15:04:05.000"), e.Phase, e.Kind, e.Message)
		}
	}
}

// crashing is the real activities, except that MeasureLag blocks forever
// once the pool is paused: the worker "dies" mid-drain with writes held.
// It keeps heartbeating while it blocks, so the heartbeat timeout never
// ends the attempt; only w1.Stop() does.
type crashing struct {
	*ops.Activities
	paused atomic.Bool
	hit    chan struct{}
}

func (c *crashing) PausePool(ctx context.Context, in ops.PoolIn) error {
	err := c.Activities.PausePool(ctx, in)
	if err == nil {
		c.paused.Store(true)
	}
	return err
}

func (c *crashing) MeasureLag(ctx context.Context, in ops.LagIn) (ops.LagOut, error) {
	if c.paused.Load() {
		select {
		case c.hit <- struct{}{}:
		default:
		}
		tick := time.NewTicker(200 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return ops.LagOut{}, ctx.Err()
			case <-tick.C:
				sdkactivity.RecordHeartbeat(ctx)
			}
		}
	}
	return c.Activities.MeasureLag(ctx, in)
}

func TestWorkerCrashDuringSwitch(t *testing.T) {
	itest.Reset(t)
	itest.Seed(t, itest.Conn(t, itest.SourceDSN))
	ctx := context.Background()
	c := devServer(t)

	crash := &crashing{Activities: ops.New(), hit: make(chan struct{}, 1)}
	w1 := worker.New(c, flow.TaskQueue, worker.Options{Identity: "w1"})
	flow.RegisterWorkflows(w1)
	w1.RegisterActivity(crash)
	if err := w1.Start(); err != nil {
		t.Fatal(err)
	}
	stopped := false
	defer func() {
		if !stopped {
			w1.Stop()
		}
	}()

	p := itest.Plan(t)
	maxPause := 30 * time.Second // room for a restart
	p.Thresholds.Switch.MaxPause = plan.Duration(maxPause)
	id := flow.WorkflowID("crash")
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id, TaskQueue: flow.TaskQueue},
		flow.Migration, flow.Input{MigrationID: "crash", Plan: p})
	if err != nil {
		t.Fatal(err)
	}
	load := itest.StartLoad(t, 4)
	toSwitch(t, c, id)

	select {
	case <-crash.hit:
	case <-time.After(2 * time.Minute):
		t.Fatal("never reached the drain")
	}
	crashed := time.Now()
	w1.Stop() // the worker dies with PgBouncer paused
	stopped = true
	w2 := worker.New(c, flow.TaskQueue, worker.Options{Identity: "w2"})
	flow.Register(w2)
	if err := w2.Start(); err != nil {
		t.Fatal(err)
	}
	defer w2.Stop()
	t.Logf("w1 stopped and w2 started in %s", time.Since(crashed).Round(time.Millisecond))

	decide(t, c, id, "finish", "alice", flow.Approve)
	wctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	var res flow.Result
	if err := run.Get(wctx, &res); err != nil {
		t.Fatal(err)
	}
	lr := load.Stop()
	logEvents(t, res.Events)
	if res.State != flow.StateFinished {
		t.Fatalf("state %s: %+v", res.State, res.Events)
	}
	sw, ok := event(res.Events, "switched")
	if !ok {
		t.Fatal("the new worker must complete the switch, not abort it")
	}
	if _, ok := event(res.Events, "switch-aborted"); ok {
		t.Error("the switch aborted before it completed")
	}
	if st, ok := event(res.Events, "switch-started"); ok {
		t.Logf("switch: started %s, switched %s later (%s)", st.At.Format("15:04:05.000"), sw.At.Sub(st.At).Round(time.Millisecond), sw.Message)
	}

	// The history shows who did what: w1 paused, the drain that w1 was
	// running when it died was retried and completed by w2, and w2 did
	// the repoint.
	started := map[int64]string{} // scheduled event ID → activity type
	var doneBy []string           // "type@identity#attempt", in history order
	it := c.GetWorkflowHistory(ctx, id, run.GetRunID(), false, 0)
	for it.HasNext() {
		e, err := it.Next()
		if err != nil {
			t.Fatal(err)
		}
		if a := e.GetActivityTaskScheduledEventAttributes(); a != nil {
			started[e.GetEventId()] = a.GetActivityType().GetName()
		}
		if a := e.GetActivityTaskStartedEventAttributes(); a != nil {
			doneBy = append(doneBy, started[a.GetScheduledEventId()]+"@"+a.GetIdentity()+"#"+strconv.Itoa(int(a.GetAttempt())))
		}
	}
	pause := slices.IndexFunc(doneBy, func(s string) bool { return strings.HasPrefix(s, "PausePool@") })
	if pause < 0 || !strings.HasPrefix(doneBy[pause], "PausePool@w1#") {
		t.Fatalf("pause not run by w1: %v", doneBy)
	}
	after := doneBy[pause+1:]
	t.Logf("activities from the pause on (type@worker#attempt): %v", after)
	if len(after) == 0 || !strings.HasPrefix(after[0], "MeasureLag@w2#") || after[0] == "MeasureLag@w2#1" {
		t.Errorf("the drain after the pause should be a retry run by w2, got %v", after)
	}
	if !slices.ContainsFunc(after, func(s string) bool { return strings.HasPrefix(s, "Repoint@w2#") }) {
		t.Errorf("w2 did not repoint: %v", after)
	}

	tgt := itest.Conn(t, itest.TargetDSN)
	itest.Verify(t, tgt, lr)
	if o := itest.Origins(t, tgt); o["source"] == 0 || o["target"] == 0 {
		t.Errorf("writes by origin %v: want some on each side", o)
	}
	poolOn(t, "target")
	// The hold includes the restart, but it is still inside the budget:
	// everything up to the repoint is bounded by max_pause.
	if bound := maxPause + holdMargin; lr.Slowest() >= bound {
		t.Errorf("slowest statement %s, want under max_pause %s + %s", lr.Slowest(), maxPause, holdMargin)
	}
}

// slowSequences is the real activities, except that the first
// SyncSequences hangs for three pause budgets, heartbeating, as a step
// stuck on a slow database would: it ignores its context. The heartbeat
// keeps the attempt alive; only the switch's budget can end it. returned
// is closed when the stuck call gives up, so the test can wait for it.
type slowSequences struct {
	*ops.Activities
	hang     time.Duration
	calls    atomic.Int32
	returned chan struct{}
}

func (s *slowSequences) SyncSequences(ctx context.Context, in ops.SeqIn) ([]pg.SequenceSync, error) {
	if s.calls.Add(1) == 1 {
		defer close(s.returned)
		tick := time.NewTicker(200 * time.Millisecond)
		defer tick.Stop()
		for end := time.After(s.hang); ; {
			select {
			case <-end:
				return nil, ctx.Err()
			case <-tick.C:
				if ctx.Err() == nil {
					sdkactivity.RecordHeartbeat(ctx)
				}
			}
		}
	}
	return s.Activities.SyncSequences(ctx, in)
}

// TestPauseBudgetOnRealClock runs the switch on a real Temporal server,
// whose clock moves while activities run, with a step that would hold
// writes past max_pause. The switch must abort at the budget and resume
// writes on the source.
func TestPauseBudgetOnRealClock(t *testing.T) {
	itest.Reset(t)
	src := itest.Conn(t, itest.SourceDSN)
	itest.Seed(t, src)
	ctx := context.Background()
	c := devServer(t)

	maxPause := 2 * time.Second
	slow := &slowSequences{Activities: ops.New(), hang: 3 * maxPause, returned: make(chan struct{})}
	w := worker.New(c, flow.TaskQueue, worker.Options{Identity: "w"})
	flow.RegisterWorkflows(w)
	w.RegisterActivity(slow)
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	p := itest.Plan(t)
	p.Thresholds.Switch.MaxPause = plan.Duration(maxPause)
	id := flow.WorkflowID("budget")
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id, TaskQueue: flow.TaskQueue},
		flow.Migration, flow.Input{MigrationID: "budget", Plan: p})
	if err != nil {
		t.Fatal(err)
	}
	load := itest.StartLoad(t, 4)
	toSwitch(t, c, id)

	// After the abort the switch gate opens again, with writes on the source.
	st := waitFor(t, c, id, "switch", 2*time.Minute, func(s flow.Status) bool {
		_, aborted := event(s.Events, "switch-aborted")
		return aborted && s.Gate != nil && s.Gate.Name == "switch"
	})
	logEvents(t, st.Events)
	started, _ := event(st.Events, "switch-started")
	aborted, _ := event(st.Events, "switch-aborted")
	if !strings.HasPrefix(aborted.Message, "sequences:") {
		t.Errorf("abort reason %q, want the sequences step", aborted.Message)
	}
	// Event times are the server's workflow clock: the abort comes at the
	// budget, not when the stuck step gives up (3 × max_pause).
	took := aborted.At.Sub(started.At)
	t.Logf("switch-started to switch-aborted: %s (max_pause %s)", took.Round(time.Millisecond), maxPause)
	if took < maxPause-50*time.Millisecond || took >= maxPause+time.Second {
		t.Errorf("aborted %s after the switch started, want at the %s budget", took, maxPause)
	}
	if _, ok := event(st.Events, "switched"); ok {
		t.Error("switched despite the abort")
	}
	if st.Primary != ops.Source {
		t.Errorf("primary %s after the abort, want source", st.Primary)
	}
	poolOn(t, "source")
	// Writes are flowing again: the source's row count keeps rising.
	count := func() int64 {
		var n int64
		if err := src.QueryRow(ctx, "SELECT count(*) FROM public.orders WHERE writer > 0").Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	n0 := count()
	for d := time.Now().Add(10 * time.Second); count() < n0+20; time.Sleep(100 * time.Millisecond) {
		if time.Now().After(d) {
			t.Fatalf("writes did not resume on the source: %d rows, then %d", n0, count())
		}
	}

	decide(t, c, id, "switch", "alice", flow.Reject)
	wctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	var res flow.Result
	if err := run.Get(wctx, &res); err != nil {
		t.Fatal(err)
	}
	lr := load.Stop()
	if res.State != flow.StateRolledBack {
		t.Fatalf("state %s: %+v", res.State, res.Events)
	}
	if n := slow.calls.Load(); n != 1 {
		t.Errorf("SyncSequences ran %d times, want 1", n)
	}
	// Let the stuck call finish here rather than in the next test.
	select {
	case <-slow.returned:
	case <-time.After(slow.hang):
		t.Error("the stuck SyncSequences never returned")
	}
	itest.Verify(t, src, lr)
	if o := itest.Origins(t, src); o["target"] != 0 || o["source"] == 0 {
		t.Errorf("writes by origin %v: want all on the source", o)
	}
	poolOn(t, "source")
	if bound := maxPause + holdMargin; lr.Slowest() >= bound {
		t.Errorf("slowest statement %s, want under max_pause %s + %s", lr.Slowest(), maxPause, holdMargin)
	}
}
