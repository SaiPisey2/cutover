package main

import (
	"bytes"
	"context"
	"encoding/json"
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
