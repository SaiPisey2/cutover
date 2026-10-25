package loadgen

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type orders struct {
	mu     sync.Mutex
	issued map[int64]bool
	next   int64
	n      atomic.Int64
	every  int64 // fail every Nth request with 503 when > 0
	host   atomic.Value
}

func (o *orders) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	o.host.Store(r.Host)
	if n := o.n.Add(1); o.every > 0 && n%o.every == 0 {
		http.Error(w, "injected fault", http.StatusServiceUnavailable)
		return
	}
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/orders":
		io.Copy(io.Discard, r.Body)
		o.mu.Lock()
		o.next++
		id := o.next
		o.issued[id] = true
		o.mu.Unlock()
		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, `{"id":%d}`, id)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/orders/"):
		id, _ := strconv.ParseInt(strings.TrimPrefix(r.URL.Path, "/orders/"), 10, 64)
		o.mu.Lock()
		ok := o.issued[id]
		o.mu.Unlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		fmt.Fprint(w, `{}`)
	case r.URL.Path == "/healthz":
		fmt.Fprint(w, "ok")
	default:
		http.NotFound(w, r)
	}
}

func newServer(every int64) (*orders, *httptest.Server) {
	o := &orders{issued: map[int64]bool{}, every: every}
	return o, httptest.NewServer(o)
}

func cfg(target string) Config {
	return Config{Target: target, Writers: 2, Readers: 2, Interval: 5 * time.Millisecond, Timeout: time.Second}
}

func TestCountsAndIDs(t *testing.T) {
	o, srv := newServer(0)
	defer srv.Close()
	run := Start(context.Background(), cfg(srv.URL))
	time.Sleep(300 * time.Millisecond)
	rep := run.Stop()
	if rep.Requests == 0 || rep.Failures != 0 {
		t.Fatalf("requests=%d failures=%d samples=%v", rep.Requests, rep.Failures, rep.FailureSamples)
	}
	if len(rep.IDs) == 0 {
		t.Fatal("no ids recorded")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	seen := map[int64]bool{}
	for _, id := range rep.IDs {
		if seen[id] {
			t.Fatalf("duplicate id %d", id)
		}
		seen[id] = true
		if !o.issued[id] {
			t.Fatalf("id %d was never issued", id)
		}
	}
	if rep.Started.IsZero() || !rep.Stopped.After(rep.Started) {
		t.Fatalf("bad times %v %v", rep.Started, rep.Stopped)
	}
}

func TestFailuresRecorded(t *testing.T) {
	_, srv := newServer(5)
	defer srv.Close()
	c := cfg(srv.URL)
	c.Interval = time.Millisecond
	run := Start(context.Background(), c)
	time.Sleep(500 * time.Millisecond)
	rep := run.Stop()
	if rep.Failures == 0 {
		t.Fatal("expected failures")
	}
	if len(rep.FailureSamples) == 0 || len(rep.FailureSamples) > 50 {
		t.Fatalf("samples = %d", len(rep.FailureSamples))
	}
	if rep.Failures <= 50 {
		t.Logf("only %d failures, cap not exercised", rep.Failures)
	}
	for _, f := range rep.FailureSamples {
		if f.Status != 503 || f.At.IsZero() || f.Method == "" || f.Path == "" {
			t.Fatalf("bad sample %+v", f)
		}
	}
}

func TestTransportErrorIsFailure(t *testing.T) {
	_, srv := newServer(0)
	url := srv.URL
	srv.Close()
	run := Start(context.Background(), cfg(url))
	time.Sleep(100 * time.Millisecond)
	rep := run.Stop()
	if rep.Failures == 0 || rep.Failures != rep.Requests {
		t.Fatalf("requests=%d failures=%d", rep.Requests, rep.Failures)
	}
	if s := rep.FailureSamples[0]; s.Err == "" || s.Status != 0 {
		t.Fatalf("bad sample %+v", s)
	}
}

func TestHostHeaderSent(t *testing.T) {
	o, srv := newServer(0)
	defer srv.Close()
	c := cfg(srv.URL)
	c.Host = "localhost"
	run := Start(context.Background(), c)
	time.Sleep(100 * time.Millisecond)
	run.Stop()
	if h, _ := o.host.Load().(string); h != "localhost" {
		t.Fatalf("host = %q", h)
	}
}

func TestStopIsPromptAndIdempotent(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-block:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(block)
	c := cfg(srv.URL)
	c.Timeout = 200 * time.Millisecond
	run := Start(context.Background(), c)
	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	a := run.Stop()
	if d := time.Since(start); d > 2*c.Timeout {
		t.Fatalf("Stop took %v", d)
	}
	b := run.Stop()
	if a.Requests != b.Requests || a.Failures != b.Failures {
		t.Fatalf("second Stop differs: %+v vs %+v", a, b)
	}
}

func TestResultsAfterCancelAreCounted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := &Run{ctx: ctx}
	now := time.Now()
	r.record("GET", "/healthz", 503, now, nil, 0)
	r.record("POST", "/orders", 201, now, nil, 7)
	r.record("GET", "/healthz", 0, now, context.Canceled, 0)
	if r.rep.Requests != 2 || r.rep.Failures != 1 || len(r.rep.IDs) != 1 {
		t.Fatalf("%+v", r.rep)
	}
}
