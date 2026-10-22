//go:build integration

package frontdoor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SaiPisey2/cutover/internal/itest"
)

// backend is one copy of a test service. It answers 200 with its side's
// name, 503 when failing is set, or nothing until Envoy gives up when
// stalling is set; it counts requests by method and
// records the Host header it last saw.
type backend struct {
	side     string
	srv      *http.Server
	l        net.Listener
	failing  atomic.Bool
	stalling atomic.Bool
	mu       sync.Mutex
	byMethod map[string]int
	host     string
}

func startBackend(t *testing.T, side string) *backend {
	t.Helper()
	b := &backend{side: side, byMethod: map[string]int{}}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b.l = l
	b.srv = &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		if r.URL.Path == "/healthz" {
			w.WriteHeader(200)
			return
		}
		b.mu.Lock()
		b.byMethod[r.Method]++
		b.host = r.Host
		b.mu.Unlock()
		if b.stalling.Load() {
			// Hold the request until Envoy gives up on it and resets
			// the connection (or the test ends).
			select {
			case <-r.Context().Done():
			case <-time.After(30 * time.Second):
			}
			return
		}
		if b.failing.Load() {
			w.WriteHeader(503)
			return
		}
		fmt.Fprint(w, side)
	})}
	go b.srv.Serve(l)
	t.Cleanup(func() { b.srv.Close() })
	return b
}

func (b *backend) upstream() *Upstream {
	return &Upstream{Address: "127.0.0.1", Port: b.l.Addr().(*net.TCPAddr).Port}
}

func (b *backend) count(method string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.byMethod[method]
}

func (b *backend) total() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, c := range b.byMethod {
		n += c
	}
	return n
}

type frontdoorRig struct {
	xds      *XDS
	xdsAddr  string
	store    *Store
	admin    *Admin
	front    string // http://127.0.0.1:port
	listener Listener
	// client keeps a connection per load worker alive; the default two
	// idle connections per host would churn through ephemeral ports.
	client *http.Client
}

func newRig(t *testing.T) *frontdoorRig {
	t.Helper()
	return newRigWithLog(t, "")
}

// newRigWithLog starts a rig whose listener writes failed requests to
// accessLog, or writes no access log when it is empty.
func newRigWithLog(t *testing.T, accessLog string) *frontdoorRig {
	t.Helper()
	front := itest.FreePort(t)
	adminPort := itest.FreePort(t)
	xdsPort := itest.FreePort(t)
	rig := &frontdoorRig{
		store:    newStore(t),
		listener: Listener{Address: "127.0.0.1", Port: front, AccessLog: accessLog},
		front:    "http://127.0.0.1:" + strconv.Itoa(front),
		xdsAddr:  "127.0.0.1:" + strconv.Itoa(xdsPort),
		client:   &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 16}},
	}
	t.Cleanup(rig.client.CloseIdleConnections)
	rig.startXDS(t)
	boot, err := Bootstrap(BootstrapConfig{XDS: rig.xdsAddr, Admin: "127.0.0.1:" + strconv.Itoa(adminPort)})
	if err != nil {
		t.Fatal(err)
	}
	rig.admin = &Admin{Base: itest.StartEnvoy(t, boot, adminPort), HTTP: &http.Client{Timeout: 5 * time.Second}}
	return rig
}

func (r *frontdoorRig) startXDS(t *testing.T) {
	t.Helper()
	x, err := NewXDS(XDSConfig{Listener: r.listener, Store: r.store, Drain: time.Second, AckTimeout: 15 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	l, err := ListenXDS(r.xdsAddr)
	if err != nil {
		t.Fatal(err)
	}
	go x.Serve(l)
	t.Cleanup(x.Stop)
	r.xds = x
}

func (r *frontdoorRig) apply(t *testing.T, rt Route) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Apply fails at once while no Envoy is connected: right after
	// Envoy starts, and after an xDS server restart until Envoy's
	// reconnect backoff fires. Wait for the stream, not longer.
	for {
		err := r.xds.Apply(ctx, rt)
		if err == nil {
			return
		}
		if !errors.Is(err, errNoEnvoy) || ctx.Err() != nil {
			t.Fatalf("apply %+v: %v", rt, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// do sends one request through the front door with Host: shop.test.
func (r *frontdoorRig) do(method string) (int, string, error) {
	req, _ := http.NewRequest(method, r.front+"/", strings.NewReader("x"))
	req.Host = "shop.test"
	resp, err := r.client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), nil
}

func (r *frontdoorRig) waitHealthy(t *testing.T, cluster string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if h, err := r.admin.Health(context.Background(), cluster); err == nil && h == Healthy {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("%s never became healthy", cluster)
}

func shopRoute(src, tgt *backend) Route {
	r := Route{Owner: "shop", Service: "api", Host: "shop.test", Prefix: "/", HealthPath: "/healthz"}
	if src != nil {
		r.Source = src.upstream()
	}
	if tgt != nil {
		r.Target = tgt.upstream()
	}
	return r
}

func TestRoutesAndWeights(t *testing.T) {
	rig := newRig(t)
	src, tgt := startBackend(t, "source"), startBackend(t, "target")
	rig.apply(t, shopRoute(src, nil))
	for i := 0; i < 50; i++ {
		code, body, err := rig.do("GET")
		if err != nil || code != 200 || body != "source" {
			t.Fatalf("request %d: %d %q %v", i, code, body, err)
		}
	}
	r := shopRoute(src, tgt)
	rig.apply(t, r)
	rig.waitHealthy(t, "c_shop_api_target")
	r.TargetWeight = 50
	rig.apply(t, r)
	hits := 0
	for i := 0; i < 400; i++ {
		_, body, err := rig.do("GET")
		if err != nil {
			t.Fatal(err)
		}
		if body == "target" {
			hits++
		}
	}
	if hits < 120 || hits > 280 {
		t.Fatalf("50%% weight sent %d of 400 to the target", hits)
	}
	r.TargetWeight = 100
	rig.apply(t, r)
	for i := 0; i < 50; i++ {
		if _, body, _ := rig.do("GET"); body != "target" {
			t.Fatalf("100%%: got %q", body)
		}
	}
}

func TestMirrorSafeMethodsOnly(t *testing.T) {
	rig := newRig(t)
	src, tgt := startBackend(t, "source"), startBackend(t, "target")
	r := shopRoute(src, tgt)
	r.Mirror, r.MirrorMethods = true, []string{"GET"}
	rig.apply(t, r)
	for i := 0; i < 50; i++ {
		for _, m := range []string{"GET", "POST"} {
			if code, body, err := rig.do(m); err != nil || code != 200 || body != "source" {
				t.Fatalf("%s: %d %q %v", m, code, body, err)
			}
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for tgt.count("GET") < 50 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if tgt.count("GET") != 50 || tgt.count("POST") != 0 {
		t.Fatalf("target saw GET %d POST %d; want 50 and 0", tgt.count("GET"), tgt.count("POST"))
	}
	tgt.mu.Lock()
	host := tgt.host
	tgt.mu.Unlock()
	if host != "shop.test" {
		t.Fatalf("mirrored requests carry Host %q, want shop.test (no -shadow suffix)", host)
	}
}

// TestStatsCountEachFailureOnce pins traffic.ParsePrometheus against a
// real Envoy: every client request through the front door counts as
// exactly one request, and every failed one as exactly one error,
// whatever way it failed. If Envoy disagrees, change the formula in
// internal/traffic/prom.go, not this test.
func TestStatsCountEachFailureOnce(t *testing.T) {
	rig := newRig(t)
	src, tgt := startBackend(t, "source"), startBackend(t, "target")
	r := shopRoute(src, tgt)
	rig.apply(t, r)
	rig.waitHealthy(t, "c_shop_api_target")
	r.TargetWeight = 100
	rig.apply(t, r)
	cluster := "c_shop_api_target"
	stats := func() (uint64, uint64) {
		all, err := rig.admin.Stats(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		return all[cluster].Requests, all[cluster].Errors
	}
	req0, err0 := stats()
	// expect checks the counters moved by exactly the requests sent
	// since the previous call, and by errors of them.
	expect := func(what string, requests, errors uint64) {
		t.Helper()
		time.Sleep(200 * time.Millisecond)
		req1, err1 := stats()
		if req1-req0 != requests || err1-err0 != errors {
			t.Fatalf("%s: requests +%d errors +%d, want +%d and +%d; adjust traffic.ParsePrometheus's formula until this is exact",
				what, req1-req0, err1-err0, requests, errors)
		}
		req0, err0 = req1, err1
	}
	send := func(n int, want int) {
		t.Helper()
		for i := 0; i < n; i++ {
			if code, _, err := rig.do("GET"); code != want {
				t.Fatalf("want %d, got %d %v", want, code, err)
			}
		}
	}

	send(20, 200)
	expect("20 good requests", 20, 0)

	tgt.failing.Store(true)
	send(40, 503)
	expect("40 requests the target answered 503", 40, 40)

	// A target that never answers: Envoy's route timeout (the default
	// 15s; the generated routes set none) answers 504. Four requests run
	// at once so the case costs one timeout, not four.
	tgt.failing.Store(false)
	tgt.stalling.Store(true)
	var wg sync.WaitGroup
	codes := make([]int, 4)
	for i := range codes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			codes[i], _, _ = rig.do("GET")
		}(i)
	}
	wg.Wait()
	for _, c := range codes {
		if c != 504 {
			t.Fatalf("want 504 from a stalled target, got %v", codes)
		}
	}
	expect("4 timed-out requests", 4, 4)

	// A target that refuses connections: Envoy answers 503 itself after
	// its retries (three connection attempts per request). Each failed
	// client request still counts once.
	tgt.srv.Close()
	send(20, 503)
	expect("20 refused requests", 20, 20)
}

// load sends GETs and POSTs through the front door from 8 workers until
// stop is called, and returns how many requests failed and why.
func load(rig *frontdoorRig) (stop func() (ok int, failed []string)) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var ok int
	var failed []string
	done := make(chan struct{})
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-done:
					return
				default:
				}
				m := "GET"
				if i%2 == 1 {
					m = "POST"
				}
				code, _, err := rig.do(m)
				mu.Lock()
				if err != nil || code != 200 {
					failed = append(failed, fmt.Sprintf("%s: %d %v", m, code, err))
				} else {
					ok++
				}
				mu.Unlock()
			}
		}(w)
	}
	return func() (int, []string) {
		close(done)
		wg.Wait()
		return ok, failed
	}
}

func TestNoFailedRequestsAcrossChanges(t *testing.T) {
	rig := newRig(t)
	src, tgt := startBackend(t, "source"), startBackend(t, "target")
	rig.apply(t, shopRoute(src, nil))
	stop := load(rig)
	r := shopRoute(src, tgt)
	rig.apply(t, r)
	rig.waitHealthy(t, "c_shop_api_target")
	r.Mirror, r.MirrorMethods = true, []string{"GET", "HEAD"}
	rig.apply(t, r)
	r.Mirror = false
	rig.apply(t, r)
	for _, w := range []int{1, 10, 50, 100} {
		r.TargetWeight = w
		rig.apply(t, r)
		time.Sleep(300 * time.Millisecond)
	}
	r.Source = nil // Finish: the source leaves the front door
	rig.apply(t, r)
	time.Sleep(300 * time.Millisecond)
	r.Source = src.upstream() // and comes back (a rollback before Finish would never get here, but the path must be safe)
	rig.apply(t, r)
	rig.waitHealthy(t, "c_shop_api_source")
	r.TargetWeight = 0
	rig.apply(t, r)
	r.Target = nil
	rig.apply(t, r)
	time.Sleep(300 * time.Millisecond)
	ok, failed := stop()
	if len(failed) > 0 {
		t.Fatalf("%d of %d requests failed across route changes; first: %s", len(failed), ok+len(failed), failed[0])
	}
	if ok < 200 {
		t.Fatalf("only %d requests ran; the load did not exercise the changes", ok)
	}
}

func TestServerRestartKeepsRoutes(t *testing.T) {
	rig := newRig(t)
	src, tgt := startBackend(t, "source"), startBackend(t, "target")
	r := shopRoute(src, tgt)
	rig.apply(t, r)
	rig.waitHealthy(t, "c_shop_api_target")
	stop := load(rig)
	rig.xds.Stop()
	time.Sleep(2 * time.Second) // Envoy keeps serving its last configuration
	rig.startXDS(t)             // same store, same address
	r.TargetWeight = 10
	rig.apply(t, r) // waits for the reconnected Envoy's ACK
	time.Sleep(500 * time.Millisecond)
	ok, failed := stop()
	if len(failed) > 0 {
		t.Fatalf("%d of %d requests failed across a server restart; first: %s", len(failed), ok+len(failed), failed[0])
	}
	if tgt.total() == 0 {
		t.Fatal("the change after the restart never reached Envoy")
	}
}

// TestNoFailedRequestsOnJumps applies changes that add a cluster and
// route to it, or unroute a cluster and remove it, in one Apply. Only
// the staging in xds.go keeps these from failing requests.
func TestNoFailedRequestsOnJumps(t *testing.T) {
	rig := newRig(t)
	src, tgt := startBackend(t, "source"), startBackend(t, "target")
	rig.apply(t, shopRoute(src, nil))
	stop := load(rig)
	for i := 0; i < 3; i++ {
		r := shopRoute(src, tgt)
		r.TargetWeight = 50
		rig.apply(t, r) // the target cluster arrives and takes half the traffic
		time.Sleep(300 * time.Millisecond)
		rig.apply(t, shopRoute(src, nil)) // and leaves while it has it
		time.Sleep(300 * time.Millisecond)
	}
	ok, failed := stop()
	if len(failed) > 0 {
		t.Fatalf("%d of %d requests failed across route changes; first: %s", len(failed), ok+len(failed), failed[0])
	}
	if tgt.total() == 0 {
		t.Fatal("the target never received traffic")
	}
}

// get sends one GET for path (which may carry a query) through the
// front door with Host: shop.test.
func (r *frontdoorRig) get(path string) (int, error) {
	req, _ := http.NewRequest("GET", r.front+path, nil)
	req.Host = "shop.test"
	resp, err := r.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

// failuresAt returns fs's entries whose path is path.
func failuresAt(fs []Failure, path string) []Failure {
	var out []Failure
	for _, f := range fs {
		if f.Path == path {
			out = append(out, f)
		}
	}
	return out
}

func TestAccessLogSamplesFailures(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "access.log")
	rig := newRigWithLog(t, logPath)
	fails := NewFailures(logPath, 50)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fails.Run(ctx) }() // before Envoy creates the file
	t.Cleanup(func() {
		cancel()
		<-done
	})
	src, tgt := startBackend(t, "source"), startBackend(t, "target")
	r := shopRoute(src, tgt)
	rig.apply(t, r)
	rig.waitHealthy(t, "c_shop_api_target")

	// Shift: every request goes to a target that answers 503.
	r.TargetWeight = 100
	rig.apply(t, r)
	tgt.failing.Store(true)
	for i := 0; i < 10; i++ {
		if code, err := rig.get(fmt.Sprintf("/orders?token=s3cret&i=%d", i)); code != 503 {
			t.Fatalf("shift request %d: %d %v", i, code, err)
		}
	}
	// Envoy flushes the file about once a second.
	deadline := time.Now().Add(10 * time.Second)
	for len(fails.Recent("c_shop_api_target")) < 10 && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	got := fails.Recent("c_shop_api_target")
	if len(got) != 10 {
		t.Fatalf("target has %d logged failures, want 10: %+v", len(got), got)
	}
	for _, f := range got {
		if f.Status != 503 && f.Status != 500 {
			t.Fatalf("status %d: %+v", f.Status, f)
		}
		if strings.Contains(f.Path, "?") || f.Path != "/orders" {
			t.Fatalf("path %q must carry no query: %+v", f.Path, f)
		}
		if f.Method != "GET" || f.Cluster != "c_shop_api_target" || f.RequestID == "" || f.At.IsZero() {
			t.Fatalf("incomplete entry %+v", f)
		}
	}
	if s, _ := fails.Failures("shop", "api"); len(s) != 0 {
		t.Fatalf("the source served nothing, yet has failures %+v", s)
	}

	// Mirroring: the source answers, and GETs are shadowed to the
	// still-failing target.
	//
	// Measured against Envoy 1.39.4: the listener's access log does not
	// record shadows. It logs the user's request, which the source
	// answered 200 with no response flag, so the filter drops it; the
	// shadow's 503 from the target never reaches the log. During
	// mirroring the target shows no samples, which is why the console
	// carries the C9 note. If a future Envoy starts logging shadows, this
	// assertion fails: assert the entries instead and drop the note.
	r.TargetWeight, r.Mirror, r.MirrorMethods = 0, true, []string{"GET"}
	rig.apply(t, r)
	before := tgt.count("GET")
	for i := 0; i < 10; i++ {
		if code, err := rig.get("/mirrored?token=s3cret"); code != 200 {
			t.Fatalf("mirrored request %d: %d %v", i, code, err)
		}
	}
	deadline = time.Now().Add(5 * time.Second)
	for tgt.count("GET")-before < 10 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if n := tgt.count("GET") - before; n != 10 {
		t.Fatalf("the target saw %d shadows, want 10", n)
	}
	time.Sleep(3 * time.Second) // past Envoy's one-second flush, with room
	s, tg := fails.Failures("shop", "api")
	if m := failuresAt(tg, "/mirrored"); len(m) != 0 {
		t.Fatalf("Envoy now logs failed shadows (%d entries: %+v); assert them and drop the console's C9 note", len(m), m)
	}
	if len(tg) != 10 || len(s) != 0 {
		t.Fatalf("mirroring changed the samples: source %d target %d, want 0 and 10", len(s), len(tg))
	}
	b, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "s3cret") || strings.Contains(string(b), "mirrored") {
		t.Fatalf("the log holds a query or a shadow:\n%s", b)
	}
}
