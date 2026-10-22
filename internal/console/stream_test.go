package console

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SaiPisey2/cutover/internal/flow"
	"github.com/stretchr/testify/require"
)

type sseEvent struct{ name, data string }

// fastStream makes the stream poll and beat quickly for one test and
// restores the defaults after. It runs before the server starts, so the
// restore (registered first) runs after the server closes.
func fastStream(t *testing.T, poll, beat time.Duration) {
	t.Helper()
	oldPoll, oldList, oldBeat := streamPoll, streamListPoll, streamHeartbeat
	streamPoll, streamListPoll, streamHeartbeat = poll, 2*poll, beat
	t.Cleanup(func() { streamPoll, streamListPoll, streamHeartbeat = oldPoll, oldList, oldBeat })
}

func fastWrite(t *testing.T, d time.Duration) {
	t.Helper()
	old := streamWriteTimeout
	streamWriteTimeout = d
	t.Cleanup(func() { streamWriteTimeout = old })
}

// countingTemporal counts Status and List calls and lets a test change
// the status by poll number.
type countingTemporal struct {
	*fakeTemporal
	cmu      sync.Mutex
	statuses int
	lists    int
	// onStatus, when set, runs before the n-th Status call (1-based).
	onStatus func(n int)
	// running and maxRunning count Status calls in flight.
	running, maxRunning int
}

func (c *countingTemporal) Status(ctx context.Context, id string) (flow.Status, error) {
	c.cmu.Lock()
	c.statuses++
	c.running++
	c.maxRunning = max(c.maxRunning, c.running)
	n, hook := c.statuses, c.onStatus
	c.cmu.Unlock()
	defer func() { c.cmu.Lock(); c.running--; c.cmu.Unlock() }()
	if hook != nil {
		hook(n)
	}
	return c.fakeTemporal.Status(ctx, id)
}

func (c *countingTemporal) List(ctx context.Context) ([]MigrationSummary, error) {
	c.cmu.Lock()
	c.lists++
	c.cmu.Unlock()
	return c.fakeTemporal.List(ctx)
}

func (c *countingTemporal) counts() (status, list int) {
	c.cmu.Lock()
	defer c.cmu.Unlock()
	return c.statuses, c.lists
}

type streamEnv struct {
	*apiEnv
	temporal *countingTemporal
	api      *api
	srv      *httptest.Server
	hc       *http.Client
	cancel   context.CancelFunc
}

// newStreamEnv serves the API through NewServer, as the console listener
// does. tls selects TLS (h2 negotiates HTTP/2 over it) with the
// listener-wide read and write timeouts set to timeout.
func newStreamEnv(t *testing.T, tls, h2 bool, timeout time.Duration) *streamEnv {
	t.Helper()
	e := newAPIEnv(t)
	ct := &countingTemporal{fakeTemporal: e.fake}
	ctx, cancel := context.WithCancel(context.Background())
	mux := http.NewServeMux()
	h := routes(mux, e.a, Deps{Temporal: ct, Store: e.st, Log: e.a.Log, Context: ctx})
	e.mux = mux
	srv := httptest.NewUnstartedServer(NewServer(e.a, mux, testUI))
	srv.EnableHTTP2 = h2
	if timeout > 0 {
		srv.Config.ReadTimeout, srv.Config.WriteTimeout = timeout, timeout
	}
	if tls {
		srv.StartTLS()
	} else {
		srv.Start()
	}
	se := &streamEnv{apiEnv: e, temporal: ct, api: h, srv: srv, hc: srv.Client(), cancel: cancel}
	// Cleanups run last-first: streams close, the server closes, the
	// hub's context ends, and then no hub goroutine may be left.
	t.Cleanup(func() {
		cancel()
		se.requireHubStopped(t)
	})
	t.Cleanup(srv.Close)
	return se
}

// requireHubStopped fails unless every hub goroutine exits within 2s.
func (e *streamEnv) requireHubStopped(t *testing.T) {
	t.Helper()
	done := make(chan struct{})
	go func() { e.api.hub.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("hub goroutines still running")
	}
}

// open sends GET path as u. On 200 the events are read on a goroutine
// into the returned channel, closed when the server ends the stream.
// Closing the body is the browser leaving.
func (e *streamEnv) open(t *testing.T, u user, path string) (*http.Response, <-chan sseEvent, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	timer := time.AfterFunc(3*time.Second, cancel)
	req, _ := http.NewRequestWithContext(ctx, "GET", e.srv.URL+path, nil)
	req.Header.Set("Cookie", SessionCookie+"="+u.cookie)
	resp, err := e.hc.Do(req)
	timer.Stop()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	closeFn := func() { resp.Body.Close(); cancel() }
	t.Cleanup(closeFn)
	ch := make(chan sseEvent, 4096)
	if resp.StatusCode != 200 {
		close(ch)
		return resp, ch, closeFn
	}
	go func() {
		defer close(ch)
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 0, 64<<10), 4<<20)
		var ev sseEvent
		for sc.Scan() {
			line := sc.Text()
			switch {
			case line == "":
				if ev.name != "" {
					ch <- ev
				}
				ev = sseEvent{}
			case strings.HasPrefix(line, ":"):
				ch <- sseEvent{name: ":", data: strings.TrimSpace(line[1:])}
			case strings.HasPrefix(line, "event: "):
				ev.name = line[len("event: "):]
			case strings.HasPrefix(line, "data: "):
				ev.data = line[len("data: "):]
			}
		}
	}()
	return resp, ch, closeFn
}

func next(t *testing.T, ch <-chan sseEvent, d time.Duration) sseEvent {
	t.Helper()
	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatal("stream ended")
		}
		return ev
	case <-time.After(d):
		t.Fatal("no event in time")
	}
	return sseEvent{}
}

// collect reads events for d, skipping heartbeats.
func collect(ch <-chan sseEvent, d time.Duration) (evs []sseEvent, closed bool) {
	deadline := time.After(d)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return evs, true
			}
			if ev.name != ":" {
				evs = append(evs, ev)
			}
		case <-deadline:
			return evs, false
		}
	}
}

func named(evs []sseEvent, name string) []sseEvent {
	var out []sseEvent
	for _, ev := range evs {
		if ev.name == name {
			out = append(out, ev)
		}
	}
	return out
}

// waitFor polls cond every 5ms for up to 3s.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestStreamSendsStatusOnChangeOnly(t *testing.T) {
	fastStream(t, 20*time.Millisecond, time.Hour)
	e := newStreamEnv(t, false, false, 0)
	e.fake.open("m1", "prepare", 1)
	e.fake.list = []MigrationSummary{{ID: "m1", Phase: "prepare", State: flow.StateWaiting}}
	// Polls 1-2 see the gate's first summary, polls 3-5 a new one.
	e.temporal.onStatus = func(n int) {
		if n == 3 {
			e.fake.mu.Lock()
			e.fake.statuses["m1"].Gate.Summary = "changed"
			e.fake.mu.Unlock()
		}
	}
	alice := e.session("a1")
	resp, ch, _ := e.open(t, alice, "/api/stream?migration=m1")
	require.Equal(t, 200, resp.StatusCode)
	require.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))
	require.Equal(t, "no-store", resp.Header.Get("Cache-Control"))
	require.Equal(t, "no", resp.Header.Get("X-Accel-Buffering"))
	hello := next(t, ch, 2*time.Second)
	require.Equal(t, "hello", hello.name)
	require.Equal(t, "{}", hello.data)

	waitFor(t, "5 status polls", func() bool { n, _ := e.temporal.counts(); return n >= 5 })
	e.temporal.cmu.Lock()
	e.temporal.onStatus = nil
	e.temporal.cmu.Unlock()
	evs, closed := collect(ch, 100*time.Millisecond)
	require.False(t, closed)
	statuses := named(evs, "status")
	require.Len(t, statuses, 2, "%v", evs)
	require.NotContains(t, statuses[0].data, `"changed"`)
	require.Contains(t, statuses[1].data, `"changed"`)
	// The event is the GET's body, byte for byte.
	get := e.do(&alice, "GET", "/api/migrations/m1", "")
	require.Equal(t, 200, get.Code)
	require.Equal(t, get.Body.String(), statuses[1].data)
	// The list never changed: one event.
	lists := named(evs, "migrations")
	require.Len(t, lists, 1)
	var rows []migrationRow
	require.NoError(t, json.Unmarshal([]byte(lists[0].data), &rows))
	require.Equal(t, "m1", rows[0].ID)
}

func TestStreamRejectsABadMigrationID(t *testing.T) {
	e := newStreamEnv(t, false, false, 0)
	alice := e.session("a1")
	resp, _, _ := e.open(t, alice, "/api/stream?migration=Bad_ID")
	require.Equal(t, 400, resp.StatusCode)
	resp, _, _ = e.open(t, user{cookie: "nope"}, "/api/stream")
	require.Equal(t, 401, resp.StatusCode)
	require.NotEqual(t, "text/event-stream", resp.Header.Get("Content-Type"))
}

// holdIdle reads an idle stream for d and returns how many heartbeats
// came, failing if the stream ended before d was up.
func holdIdle(t *testing.T, ch <-chan sseEvent, d time.Duration) int {
	t.Helper()
	pings := 0
	deadline := time.After(d)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatalf("the idle stream ended after %d heartbeats", pings)
			}
			if ev.name == ":" {
				pings++
			}
		case <-deadline:
			return pings
		}
	}
}

// Over HTTP/2 a write deadline is a timer on the whole stream: left set
// after a write, it fires while the stream sits idle and resets it. The
// write timeout here is far shorter than the heartbeat, so a deadline
// that outlived its write would kill the stream before the first ping.
func TestIdleStreamSurvivesOverHTTP2(t *testing.T) {
	fastStream(t, 20*time.Millisecond, 150*time.Millisecond)
	fastWrite(t, 100*time.Millisecond)
	e := newStreamEnv(t, true, true, 500*time.Millisecond)
	resp, ch, _ := e.open(t, e.session("a1"), "/api/stream")
	if resp.StatusCode != 200 || resp.ProtoMajor != 2 {
		t.Fatalf("stream %d over HTTP/%d, want 200 over HTTP/2", resp.StatusCode, resp.ProtoMajor)
	}
	require.Equal(t, "hello", next(t, ch, 2*time.Second).name)
	if pings := holdIdle(t, ch, 1200*time.Millisecond); pings < 4 {
		t.Errorf("%d heartbeats in 1.2s at one per 150ms", pings)
	}
}

// The same over HTTP/1.1, where the listener's timeouts are deadlines on
// the connection that a long-lived response outlives.
func TestIdleStreamSurvivesOverHTTP1(t *testing.T) {
	fastStream(t, 20*time.Millisecond, 150*time.Millisecond)
	fastWrite(t, 100*time.Millisecond)
	e := newStreamEnv(t, true, false, 500*time.Millisecond)
	resp, ch, _ := e.open(t, e.session("a1"), "/api/stream")
	if resp.StatusCode != 200 || resp.ProtoMajor != 1 {
		t.Fatalf("stream %d over HTTP/%d, want 200 over HTTP/1.1", resp.StatusCode, resp.ProtoMajor)
	}
	require.Equal(t, "hello", next(t, ch, 2*time.Second).name)
	if pings := holdIdle(t, ch, 1200*time.Millisecond); pings < 4 {
		t.Errorf("%d heartbeats in 1.2s at one per 150ms", pings)
	}
}

// The shipped defaults keep a heartbeat inside every write timeout, so
// even a deadline that somehow outlived its write is renewed before it
// could fire on an idle stream.
func TestHeartbeatIsShorterThanTheWriteTimeout(t *testing.T) {
	if streamHeartbeat >= streamWriteTimeout {
		t.Errorf("heartbeat %v, write timeout %v: the heartbeat must come first", streamHeartbeat, streamWriteTimeout)
	}
}

func TestStreamEndsWhenSessionExpires(t *testing.T) {
	fastStream(t, 20*time.Millisecond, time.Hour)
	e := newStreamEnv(t, false, false, 0)
	alice := e.session("a1")
	_, ch, _ := e.open(t, alice, "/api/stream")
	require.Equal(t, "hello", next(t, ch, 2*time.Second).name)
	require.NoError(t, e.st.RevokeSession(context.Background(), hashOf(alice.cookie), e.a.Now()))
	evs, closed := collect(ch, 2*time.Second)
	require.True(t, closed, "the stream stayed open: %v", evs)
	require.NotEmpty(t, evs)
	last := evs[len(evs)-1]
	require.Equal(t, sseEvent{name: "expired", data: "{}"}, last)

	// Expiry by time ends it the same way.
	bob := e.session("a1")
	_, ch, _ = e.open(t, bob, "/api/stream")
	require.Equal(t, "hello", next(t, ch, 2*time.Second).name)
	e.advance(2 * time.Hour)
	evs, closed = collect(ch, 2*time.Second)
	require.True(t, closed)
	require.Equal(t, "expired", evs[len(evs)-1].name)
}

func TestStreamCaps(t *testing.T) {
	fastStream(t, 20*time.Millisecond, time.Hour)
	e := newStreamEnv(t, false, false, 0)
	alice := e.session("a1")
	var closers []func()
	for i := 0; i < maxStreamsPerSession; i++ {
		resp, ch, c := e.open(t, alice, "/api/stream")
		require.Equal(t, 200, resp.StatusCode)
		require.Equal(t, "hello", next(t, ch, 2*time.Second).name)
		closers = append(closers, c)
	}
	resp, _, _ := e.open(t, alice, "/api/stream")
	require.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))
	b, _ := io.ReadAll(resp.Body)
	require.JSONEq(t, `{"error":"too many live streams"}`, string(b))

	// A closed stream frees its slot.
	closers[0]()
	waitFor(t, "a free slot", func() bool {
		resp, ch, c := e.open(t, alice, "/api/stream")
		if resp.StatusCode != 200 {
			return false
		}
		next(t, ch, 2*time.Second)
		closers[0] = c
		return true
	})
	for _, c := range closers {
		c()
	}

	// The total cap holds across sessions.
	n := 0
	for i := 0; n < maxStreams; i++ {
		u := e.session("a1")
		for j := 0; j < maxStreamsPerSession && n < maxStreams; j++ {
			var resp *http.Response
			var ch <-chan sseEvent
			waitFor(t, fmt.Sprintf("stream %d", n), func() bool {
				resp, ch, _ = e.open(t, u, "/api/stream")
				return resp.StatusCode == 200
			})
			next(t, ch, 2*time.Second)
			n++
		}
	}
	resp, _, _ = e.open(t, e.session("a1"), "/api/stream")
	require.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
}

func TestTemporalOutageIsReportedOnce(t *testing.T) {
	fastStream(t, 20*time.Millisecond, time.Hour)
	e := newStreamEnv(t, false, false, 0)
	e.fake.open("m1", "prepare", 1)
	_, ch, _ := e.open(t, e.session("a1"), "/api/stream?migration=m1")
	require.Equal(t, "hello", next(t, ch, 2*time.Second).name)
	evs, _ := collect(ch, 150*time.Millisecond)
	require.Len(t, named(evs, "status"), 1)

	setErr := func(err error) {
		e.fake.mu.Lock()
		e.fake.err = err
		e.fake.mu.Unlock()
	}
	down := fmt.Errorf("%w: connection refused", ErrTemporal)
	setErr(down)
	evs, closed := collect(ch, 300*time.Millisecond)
	require.False(t, closed)
	require.Equal(t, []sseEvent{{name: "unavailable", data: "{}"}}, evs)

	// Recovery is silent when nothing changed.
	setErr(nil)
	evs, _ = collect(ch, 200*time.Millisecond)
	require.Empty(t, evs)

	// A second outage is reported again, once.
	setErr(down)
	evs, _ = collect(ch, 300*time.Millisecond)
	require.Equal(t, []sseEvent{{name: "unavailable", data: "{}"}}, evs)
	setErr(nil)
}

// Streams share one poller: two streams on one migration cost what one
// does, and when the last leaves the poller stops.
func TestStreamsShareOnePollerThatStopsWhenIdle(t *testing.T) {
	fastStream(t, 50*time.Millisecond, time.Hour)
	e := newStreamEnv(t, false, false, 0)
	e.fake.open("m1", "prepare", 1)
	alice := e.session("a1")
	_, ch1, close1 := e.open(t, alice, "/api/stream?migration=m1")
	_, ch2, close2 := e.open(t, alice, "/api/stream?migration=m1")
	require.Equal(t, "hello", next(t, ch1, 2*time.Second).name)
	require.Equal(t, "hello", next(t, ch2, 2*time.Second).name)

	s0, l0 := e.temporal.counts()
	time.Sleep(520 * time.Millisecond) // about 10 status and 5 list polls
	s1, l1 := e.temporal.counts()
	// One poller: at most one status poll per interval (plus the
	// immediate one a new subscriber triggers), not one per stream.
	require.LessOrEqual(t, s1-s0, 13, "status polls")
	require.GreaterOrEqual(t, s1-s0, 5, "status polls")
	require.LessOrEqual(t, l1-l0, 7, "list polls")

	close1()
	close2()
	e.requireHubStopped(t)
	s2, l2 := e.temporal.counts()
	time.Sleep(200 * time.Millisecond)
	s3, l3 := e.temporal.counts()
	require.Equal(t, s2, s3, "status polled with nobody listening")
	require.Equal(t, l2, l3, "list polled with nobody listening")

	// It starts again for the next stream.
	_, ch3, _ := e.open(t, alice, "/api/stream?migration=m1")
	require.Equal(t, "hello", next(t, ch3, 2*time.Second).name)
	evs, _ := collect(ch3, 200*time.Millisecond)
	require.Len(t, named(evs, "status"), 1)
}

// Ending the hub's context ends every stream and every hub goroutine, and
// a stream opened afterwards is refused.
func TestHubStopsOnShutdown(t *testing.T) {
	fastStream(t, 20*time.Millisecond, time.Hour)
	e := newStreamEnv(t, false, false, 0)
	e.fake.open("m1", "prepare", 1)
	alice := e.session("a1")
	_, ch, _ := e.open(t, alice, "/api/stream?migration=m1")
	require.Equal(t, "hello", next(t, ch, 2*time.Second).name)
	e.cancel()
	_, closed := collect(ch, 2*time.Second)
	require.True(t, closed, "the stream outlived the hub")
	e.requireHubStopped(t)
	resp, _, _ := e.open(t, alice, "/api/stream")
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

// A failing replicate query keeps the last progress in the status (no
// flapping field, so no event) and is logged once, not every poll.
func TestStreamKeepsReplicateWhileItsQueryFails(t *testing.T) {
	fastStream(t, 20*time.Millisecond, time.Hour)
	e := newStreamEnv(t, false, false, 0)
	e.fake.open("m1", "prepare", 1)
	e.fake.mu.Lock()
	e.fake.statuses["m1"].ReplicateWorkflowID = "r1"
	e.fake.replicate["r1"] = flow.ReplicateStatus{CopyDone: true, Samples: 7}
	e.fake.mu.Unlock()
	_, ch, closeFn := e.open(t, e.session("a1"), "/api/stream?migration=m1")
	require.Equal(t, "hello", next(t, ch, 2*time.Second).name)
	evs, _ := collect(ch, 150*time.Millisecond)
	statuses := named(evs, "status")
	require.Len(t, statuses, 1)
	require.Contains(t, statuses[0].data, `"samples":7`)

	e.fake.mu.Lock()
	delete(e.fake.replicate, "r1") // Replicate now fails
	e.fake.mu.Unlock()
	n0, _ := e.temporal.counts()
	evs, _ = collect(ch, 300*time.Millisecond)
	n1, _ := e.temporal.counts()
	require.GreaterOrEqual(t, n1-n0, 5, "status polls while replicate failed")
	require.Empty(t, evs, "replicate flapped")

	closeFn()
	e.requireHubStopped(t) // no writer left on the log
	require.Equal(t, 1, strings.Count(e.logs.String(), "console stream: replicate status"), e.logs.String())
}

// A status poll still running when the next tick comes is not started
// again, so an older answer never lands after a newer one.
func TestStatusPollsForOneMigrationDoNotOverlap(t *testing.T) {
	fastStream(t, 10*time.Millisecond, time.Hour)
	e := newStreamEnv(t, false, false, 0)
	e.fake.open("m1", "prepare", 1)
	e.temporal.onStatus = func(int) { time.Sleep(60 * time.Millisecond) }
	_, ch, _ := e.open(t, e.session("a1"), "/api/stream?migration=m1")
	require.Equal(t, "hello", next(t, ch, 2*time.Second).name)
	waitFor(t, "4 status polls", func() bool { n, _ := e.temporal.counts(); return n >= 4 })
	e.temporal.cmu.Lock()
	defer e.temporal.cmu.Unlock()
	require.Equal(t, 1, e.temporal.maxRunning, "status polls overlapped")
}
