package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SaiPisey2/cutover/internal/console"
	"github.com/SaiPisey2/cutover/internal/flow"
	"github.com/SaiPisey2/cutover/internal/frontdoor"
	"github.com/SaiPisey2/cutover/internal/traffic"
)

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func tokenFile(t *testing.T, tok string) string {
	p := filepath.Join(t.TempDir(), "token")
	os.WriteFile(p, []byte(tok+"\n"), 0o600)
	return p
}

func TestEnvoyBootstrap(t *testing.T) {
	noDial(t)
	code, out, _ := exec("envoy-bootstrap", "--xds", "127.0.0.1:18000", "--admin", "127.0.0.1:9901")
	if code != 0 || !strings.Contains(out, "cluster_name: cutover_xds") {
		t.Fatalf("%d %s", code, out)
	}
	if code, _, errb := exec("envoy-bootstrap", "--xds", "0.0.0.0:18000"); code != 2 || !strings.Contains(errb, "loopback") {
		t.Fatalf("%d %s", code, errb)
	}
}

func TestServerFlags(t *testing.T) {
	noDial(t)
	state := filepath.Join(t.TempDir(), "fd.json")
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"server", "--token-file", tokenFile(t, "0123456789abcdef")}, "--state is required"},
		{[]string{"server", "--state", state}, "--token-file is required"},
		{[]string{"server", "--state", state, "--token-file", tokenFile(t, "short")}, "at least 16"},
		{[]string{"server", "--state", state, "--token-file", tokenFile(t, "0123456789abcdef"), "--api", "0.0.0.0:7600"}, "loopback"},
		{[]string{"server", "--state", state, "--token-file", tokenFile(t, "0123456789abcdef"), "--listen", "frontdoor:8080"}, "--listen"},
		{[]string{"server", "--state", state, "--token-file", tokenFile(t, "0123456789abcdef"), "--xds", "0.0.0.0:18000"}, "loopback"},
	} {
		code, _, errb := exec(tc.args...)
		if code != 2 || !strings.Contains(errb, tc.want) {
			t.Errorf("%v: %d %q", tc.args, code, errb)
		}
	}
}

func TestServerServesAPI(t *testing.T) {
	noDial(t)
	api := "127.0.0.1:" + strconv.Itoa(freePort(t))
	args := []string{"server", "--state", filepath.Join(t.TempDir(), "fd.json"), "--token-file", tokenFile(t, "0123456789abcdef"),
		"--api", api, "--xds", "127.0.0.1:" + strconv.Itoa(freePort(t)), "--listen", "127.0.0.1:" + strconv.Itoa(freePort(t))}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var code int
	var errb bytes.Buffer
	wg.Add(1)
	go func() { defer wg.Done(); code = run(ctx, args, io.Discard, &errb) }()
	var resp *http.Response
	for i := 0; i < 50; i++ {
		req, _ := http.NewRequest("GET", "http://"+api+"/v1/routes", nil)
		req.Header.Set("Authorization", "Bearer 0123456789abcdef")
		var err error
		if resp, err = http.DefaultClient.Do(req); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if resp == nil || resp.StatusCode != 200 {
		t.Fatalf("api never answered: %+v", resp)
	}
	resp.Body.Close()
	cancel()
	wg.Wait()
	if code != 0 || strings.Contains(errb.String(), "0123456789abcdef") {
		t.Fatalf("exit %d, log %q", code, errb.String())
	}
}

// routePlan writes a plan whose front door is a fake API at url.
func routePlan(t *testing.T, url string) string {
	t.Setenv("CUTOVER_TEST_FD_TOKEN", "fd-token")
	y := `
version: 1
name: shop
source:
  postgres: {dsn: env:SRC, replication_dsn: env:SRC_REPL, route: host=a port=5432 dbname=shop}
target:
  postgres: {dsn: env:TGT, replication_dsn: env:TGT_REPL, route: host=b port=5432 dbname=shop}
database:
  name: shop
  pgbouncer: {admin_dsn: env:PGB, routes_file: /etc/pgbouncer/routes.ini}
frontdoor: {api: ` + url + `, token: env:CUTOVER_TEST_FD_TOKEN, addresses: [203.0.113.10]}
services:
  - name: api
    kind: external
    route: {host: shop.example.com, health_path: /healthz}
    source: {endpoint: 10.0.1.20:8080}
    target: {endpoint: 10.8.0.20:8080}
`
	p := filepath.Join(t.TempDir(), "plan.yaml")
	os.WriteFile(p, []byte(y), 0o600)
	return p
}

type fakeAPI struct {
	mu     sync.Mutex
	routes []frontdoor.Route
	puts   []frontdoor.Route
}

func (f *fakeAPI) start(t *testing.T) string {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if r.Method == "PUT" {
			var rt frontdoor.Route
			json.NewDecoder(r.Body).Decode(&rt)
			f.puts = append(f.puts, rt)
			w.Write([]byte("{}"))
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"routes": f.routes})
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestRouteApply(t *testing.T) {
	noDial(t)
	f := &fakeAPI{}
	code, out, errb := exec("route", "apply", "--plan", routePlan(t, f.start(t)))
	if code != 0 || len(f.puts) != 1 || f.puts[0].Target != nil || f.puts[0].Source.Port != 8080 {
		t.Fatalf("%d %q %q %+v", code, out, errb, f.puts)
	}
	if !strings.Contains(out, "shop/api") {
		t.Fatalf("output %q", out)
	}
}

func TestRouteApplyRefusesBusyRoute(t *testing.T) {
	noDial(t)
	f := &fakeAPI{routes: []frontdoor.Route{{Owner: "shop", Service: "api", Host: "shop.example.com", Prefix: "/", HealthPath: "/healthz",
		Source: &frontdoor.Upstream{Address: "10.0.1.20", Port: 8080}, Target: &frontdoor.Upstream{Address: "10.8.0.20", Port: 8080}, TargetWeight: 10}}}
	code, _, errb := exec("route", "apply", "--plan", routePlan(t, f.start(t)))
	if code != 1 || !strings.Contains(errb, "in use by a migration") || len(f.puts) != 0 {
		t.Fatalf("%d %q %+v", code, errb, f.puts)
	}
}

func TestRouteList(t *testing.T) {
	noDial(t)
	f := &fakeAPI{routes: []frontdoor.Route{{Owner: "shop", Service: "api", Host: "shop.example.com", Prefix: "/", HealthPath: "/healthz",
		Source: &frontdoor.Upstream{Address: "10.0.1.20", Port: 8080}}}}
	code, out, _ := exec("route", "list", "--plan", routePlan(t, f.start(t)))
	if code != 0 || !strings.Contains(out, "shop/api") || !strings.Contains(out, "10.0.1.20:8080") {
		t.Fatalf("%d %q", code, out)
	}
}

func TestStatusShowsTraffic(t *testing.T) {
	var b bytes.Buffer
	printStatus(&b, flow.Status{MigrationID: "shop", Phase: flow.PhaseShift, State: flow.StateRunning, Weight: 10,
		Traffic: []flow.ServiceTraffic{{Service: "api",
			Source: traffic.Summary{Requests: 900, ErrorRate: 0.1, P99Known: true, P99Ms: 20},
			Target: traffic.Summary{Requests: 100, ErrorRate: 0, P99Known: true, P99Ms: 30}}}})
	out := b.String()
	if !strings.Contains(out, "traffic: 10% on the target") || !strings.Contains(out, "api: source 900 requests") {
		t.Fatalf("%s", out)
	}
}

func TestRouteHelpExitsZero(t *testing.T) {
	noDial(t)
	for _, args := range [][]string{{"route", "-h"}, {"route", "--help"}, {"route", "apply", "-h"}} {
		if code, _, errb := exec(args...); code != 0 || !strings.Contains(errb, "usage") {
			t.Errorf("%v: %d %q", args, code, errb)
		}
	}
}

func TestServerRefusesNonLoopbackAdmin(t *testing.T) {
	noDial(t)
	args := []string{"server", "--state", filepath.Join(t.TempDir(), "fd.json"), "--token-file", tokenFile(t, "0123456789abcdef"),
		"--envoy-admin", "http://10.0.0.5:9901"}
	if code, _, errb := exec(args...); code != 2 || !strings.Contains(errb, "--envoy-admin") {
		t.Fatalf("%d %q", code, errb)
	}
}

func TestServerEnvoyAdminMustBeLoopbackURL(t *testing.T) {
	noDial(t)
	for _, admin := range []string{"127.0.0.1:9901", "ftp://127.0.0.1:9901", "http://127.0.0.1", "http://10.0.0.5:9901"} {
		args := []string{"server", "--state", filepath.Join(t.TempDir(), "fd.json"), "--token-file", tokenFile(t, "0123456789abcdef"),
			"--envoy-admin", admin}
		if code, _, errb := exec(args...); code != 2 || !strings.Contains(errb, "--envoy-admin") {
			t.Errorf("%s: %d %q", admin, code, errb)
		}
	}
	if err := checkAdmin("http://127.0.0.1:9901"); err != nil {
		t.Fatal(err)
	}
	if err := checkAdmin("127.0.0.1:9901"); err == nil || !strings.Contains(err.Error(), "http://127.0.0.1:9901") {
		t.Fatalf("a bare host:port needs a helpful error, got %v", err)
	}
}

// After Finish the route is the target alone at 100%; apply re-registers it.
func TestRouteApplyReplacesFinishedRoute(t *testing.T) {
	noDial(t)
	f := &fakeAPI{routes: []frontdoor.Route{{Owner: "shop", Service: "api", Host: "shop.example.com", Prefix: "/", HealthPath: "/healthz",
		Target: &frontdoor.Upstream{Address: "10.8.0.20", Port: 8080}, TargetWeight: 100}}}
	code, out, errb := exec("route", "apply", "--plan", routePlan(t, f.start(t)))
	if code != 0 || len(f.puts) != 1 || f.puts[0].Target != nil || f.puts[0].Source == nil {
		t.Fatalf("%d %q %q %+v", code, out, errb, f.puts)
	}
}

func TestRouteApplyIsAllOrNothing(t *testing.T) {
	noDial(t)
	f := &fakeAPI{routes: []frontdoor.Route{{Owner: "shop", Service: "web", Host: "www.example.com", Prefix: "/", HealthPath: "/healthz",
		Source: &frontdoor.Upstream{Address: "10.0.1.21", Port: 8080}, TargetWeight: 10}}}
	p := routePlan(t, f.start(t))
	b, _ := os.ReadFile(p)
	b = append(b, []byte(`  - name: web
    kind: external
    route: {host: www.example.com, health_path: /healthz}
    source: {endpoint: 10.0.1.21:8080}
    target: {endpoint: 10.8.0.21:8080}
`)...)
	os.WriteFile(p, b, 0o600)
	code, _, errb := exec("route", "apply", "--plan", p)
	if code != 1 || !strings.Contains(errb, "in use by a migration") || len(f.puts) != 0 {
		t.Fatalf("%d %q %+v", code, errb, f.puts)
	}
}

func TestServerConsoleFlags(t *testing.T) {
	noDial(t)
	state := filepath.Join(t.TempDir(), "fd.json")
	tok := tokenFile(t, "0123456789abcdef")
	data := filepath.Join(t.TempDir(), "data")
	base := []string{"server", "--state", state, "--token-file", tok}
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"--console", "127.0.0.1:7701"}, "--console needs --data"},
		{[]string{"--tls-hosts", "localhost"}, "--tls-hosts needs --data"},
		{[]string{"--access-log", "/tmp/a.log"}, "--access-log needs --data"},
		{[]string{"--data", data, "--console", "0.0.0.0:7700"}, "loopback"},
		{[]string{"--data", data, "--console", "10.0.0.5:7700"}, "loopback"},
		{[]string{"--data", data, "--console", "[::]:7700"}, "loopback"},
		{[]string{"--data", data, "--console", "127.0.0.1"}, "--console"},
		{[]string{"--data", data, "--tls-hosts", "bad host"}, "--tls-hosts"},
		{[]string{"--data", data, "--tls-hosts", ""}, "--tls-hosts"},
		{[]string{"--data", data, "--tls-hosts", "127.0.0.1,"}, "--tls-hosts"},
		{[]string{"--data", data, "--tls-hosts", "a..b"}, "--tls-hosts"},
		{[]string{"--data", data, "--tls-hosts", "-a.example.com"}, "--tls-hosts"},
		{[]string{"--data", data, "--tls-hosts", "a_b.example.com"}, "--tls-hosts"},
		{[]string{"--data", data, "--tls-hosts", strings.Repeat("a", 64) + ".com"}, "--tls-hosts"},
		{[]string{"--data", data, "--tls-hosts", "1.2.3"}, "--tls-hosts"},
		{[]string{"--data", data, "--tls-hosts", "999.1.1.1"}, "--tls-hosts"},
		{[]string{"--data", data, "--tls-hosts", "localhost,10"}, "--tls-hosts"},
	} {
		code, _, errb := exec(append(append([]string{}, base...), tc.args...)...)
		if code != 2 || !strings.Contains(errb, tc.want) {
			t.Errorf("%v: %d %q", tc.args, code, errb)
		}
	}
	if _, err := os.Stat(data); !os.IsNotExist(err) {
		t.Fatalf("a refused start must not create the data directory: %v", err)
	}
	for _, h := range []string{"127.0.0.1", "::1", "localhost", "console.example.com", "a-1.b2", "1.2.3.example"} {
		if err := checkTLSHost(h); err != nil {
			t.Errorf("%s: %v", h, err)
		}
	}
}

// startServer runs `cutover server` with args until the returned stop is
// called, which returns the exit code and log.
func startServer(t *testing.T, args ...string) (stop func() (int, string)) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var code int
	var errb syncBuffer
	wg.Add(1)
	go func() { defer wg.Done(); code = run(ctx, append([]string{"server"}, args...), io.Discard, &errb) }()
	var once sync.Once
	stop = func() (int, string) {
		once.Do(func() { cancel(); wg.Wait() })
		return code, errb.String()
	}
	t.Cleanup(func() { stop() })
	return stop
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestServerServesConsole(t *testing.T) {
	noDial(t)
	isolate(t)
	data := filepath.Join(t.TempDir(), "data")
	// An approver made the way an operator would.
	if code, _, errb := runApprover(t, goodPassword+"\n", "new", "--data", data, "--name", "alice", "--password-stdin"); code != 0 {
		t.Fatalf("approver: %d %q", code, errb)
	}
	stateDir := t.TempDir()
	consoleAddr := "127.0.0.1:" + strconv.Itoa(freePort(t))
	stop := startServer(t, "--state", filepath.Join(stateDir, "fd.json"), "--token-file", tokenFile(t, "0123456789abcdef"),
		"--api", "127.0.0.1:"+strconv.Itoa(freePort(t)), "--xds", "127.0.0.1:"+strconv.Itoa(freePort(t)),
		"--listen", "127.0.0.1:"+strconv.Itoa(freePort(t)),
		"--data", data, "--console", consoleAddr,
		// Nothing listens here: the console must start anyway.
		"--temporal", "127.0.0.1:"+strconv.Itoa(freePort(t)))

	caPath := filepath.Join(data, "ca.crt")
	var ca []byte
	for i := 0; i < 100; i++ {
		if b, err := os.ReadFile(caPath); err == nil {
			ca = b
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if ca == nil {
		code, log := stop()
		t.Fatalf("no ca.crt; exit %d log %q", code, log)
	}
	if fi, err := os.Stat(caPath); err != nil || fi.Mode().Perm() != 0o644 {
		t.Fatalf("ca.crt mode: %v %v", fi, err)
	}
	c, err := console.NewClient("https://"+consoleAddr, ca)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		if err = c.Login(ctx, "alice", goodPassword); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	// A cookie for a raw stream request; the client keeps its own.
	hc := &http.Client{Transport: &http.Transport{ForceAttemptHTTP2: true, TLSClientConfig: &tls.Config{RootCAs: pool(t, ca)}}}
	resp, err := hc.Post("https://"+consoleAddr+"/api/login", "application/json",
		strings.NewReader(`{"name":"alice","password":"`+goodPassword+`"}`))
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("raw login: %v %+v", err, resp)
	}
	resp.Body.Close()
	req, _ := http.NewRequest("GET", "https://"+consoleAddr+"/api/stream", nil)
	for _, ck := range resp.Cookies() {
		req.AddCookie(ck)
	}
	sresp, err := hc.Do(req)
	if err != nil || sresp.StatusCode != 200 {
		t.Fatalf("stream: %v %+v", err, sresp)
	}
	defer sresp.Body.Close()
	// Over HTTP/2 a write deadline is a timer on the whole stream: the
	// shutdown check below must hold for the protocol browsers use.
	if sresp.Proto != "HTTP/2.0" {
		t.Fatalf("stream over %s, want HTTP/2.0", sresp.Proto)
	}

	// Temporal is down: the API says so with its fixed text.
	_, err = c.Status(ctx, "shop")
	var ae *console.APIError
	if !errors.As(err, &ae) || ae.Status != http.StatusServiceUnavailable || ae.Message != "temporal unavailable" {
		t.Fatalf("status with temporal down: %v", err)
	}

	// An open stream must not hold shutdown to its 10s limit.
	began := time.Now()
	code, log := stop()
	if took := time.Since(began); took > 5*time.Second {
		t.Fatalf("shutdown took %v with a stream open", took)
	}
	if code != 0 {
		t.Fatalf("exit %d log %q", code, log)
	}
	if want := "console: https://" + consoleAddr + " (CA " + caPath + ")"; !strings.Contains(log, want) {
		t.Fatalf("log lacks %q:\n%s", want, log)
	}
	if strings.Contains(log, goodPassword) || strings.Contains(log, "0123456789abcdef") {
		t.Fatalf("log holds a secret:\n%s", log)
	}
}

func pool(t *testing.T, pemBytes []byte) *x509.CertPool {
	p := x509.NewCertPool()
	if !p.AppendCertsFromPEM(pemBytes) {
		t.Fatal("bad CA")
	}
	return p
}

func TestKeepRunningRestartsAfterAnError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	runs, logged := 0, 0
	done := make(chan struct{})
	go func() {
		keepRunning(ctx, func(ctx context.Context) error {
			mu.Lock()
			runs++
			n := runs
			mu.Unlock()
			if n == 3 {
				cancel()
				<-ctx.Done()
				return ctx.Err()
			}
			return errors.New("permission denied")
		}, time.Millisecond, func(error) { mu.Lock(); logged++; mu.Unlock() })
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("keepRunning did not stop with its context")
	}
	if runs != 3 || logged != 2 {
		t.Fatalf("runs %d logged %d", runs, logged)
	}
}
