//go:build e2e

package e2e

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.yaml.in/yaml/v3"

	"github.com/SaiPisey2/cutover/internal/console"
	"github.com/SaiPisey2/cutover/internal/flow"
	"github.com/SaiPisey2/cutover/internal/frontdoor"
	"github.com/SaiPisey2/cutover/internal/plan"
	"github.com/SaiPisey2/cutover/internal/secret"
)

const (
	migrationID = "shop"
	apiURL      = "http://127.0.0.1:7600"
	consoleURL  = "https://127.0.0.1:7700"
	listenAddr  = "127.0.0.1:18080"
	xdsAddr     = "127.0.0.1:18000"
	pgBAddress  = "172.30.0.12"
	// driveTimeout bounds one drive call: a whole migration with the D9
	// thresholds takes a few minutes.
	driveTimeout = 10 * time.Minute
)

// env is one run of the real binaries against the fixture.
type env struct {
	bin, dir       string
	temporal       string // Temporal frontend host:port
	server, worker *proc
	envoyAdmin     string
	pw             string // both approvers' password, generated per run
	alice, bob     *console.Client
	dbPassword     string
}

// proc is a subprocess whose output goes to test/run/e2e/logs/<name>.log.
type proc struct {
	name string
	cmd  *exec.Cmd
	done chan struct{}
}

func (e *env) home() string  { return filepath.Join(e.dir, "home") }
func (e *env) token() string { return filepath.Join(e.dir, "token") }
func (e *env) data() string  { return filepath.Join(e.dir, "data") }

// baseEnv is every subprocess's environment: nothing inherited but PATH,
// a HOME of the run's own, and a KUBECONFIG that cannot load.
func (e *env) baseEnv() []string {
	return []string{"PATH=" + os.Getenv("PATH"), "HOME=" + e.home(), "TMPDIR=" + os.TempDir(), "KUBECONFIG=/nonexistent"}
}

// start runs bin with output to its log file.
func (e *env) start(t testing.TB, name, bin string, env []string, stdin io.Reader, args ...string) *proc {
	t.Helper()
	log, err := os.OpenFile(runFile(t, "logs", name+".log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, args...)
	cmd.Dir, cmd.Env, cmd.Stdin, cmd.Stdout, cmd.Stderr = e.dir, env, stdin, log, log
	if err := cmd.Start(); err != nil {
		log.Close()
		t.Fatalf("start %s: %v", name, err)
	}
	p := &proc{name: name, cmd: cmd, done: make(chan struct{})}
	go func() {
		_ = cmd.Wait()
		log.Close()
		close(p.done)
	}()
	return p
}

// run runs a short command to completion and fails the test if it fails.
func (e *env) run(t testing.TB, name string, stdin string, args ...string) {
	t.Helper()
	p := e.start(t, name, e.bin, e.baseEnv(), strings.NewReader(stdin), args...)
	select {
	case <-p.done:
	case <-time.After(time.Minute):
		p.stop(syscall.SIGKILL)
		t.Fatalf("%s did not finish in 1m; see %s", name, runFile(t, "logs", name+".log"))
	}
	if !p.cmd.ProcessState.Success() {
		t.Fatalf("%s: %s; see %s", name, p.cmd.ProcessState, runFile(t, "logs", name+".log"))
	}
}

func (p *proc) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

// stop sends sig, then SIGKILL after 10s, and waits for the exit.
func (p *proc) stop(sig syscall.Signal) {
	if p == nil || p.exited() {
		return
	}
	_ = p.cmd.Process.Signal(sig)
	select {
	case <-p.done:
	case <-time.After(10 * time.Second):
		_ = p.cmd.Process.Kill()
		<-p.done
	}
}

func randomHex(t testing.TB, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func freePort(t testing.TB) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// waitUntil polls ok every 200ms until it holds or d passes, failing the
// test early if p (when set) exits.
func waitUntil(t testing.TB, what string, d time.Duration, p *proc, ok func() error) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		err := ok()
		if err == nil {
			return
		}
		if p != nil && p.exited() {
			t.Fatalf("%s exited waiting for %s; see %s", p.name, what, runFile(t, "logs", p.name+".log"))
		}
		if time.Now().After(deadline) {
			t.Fatalf("waited %s for %s: %v", d, what, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// setup builds cutover and starts Temporal, the server (with the console),
// Envoy and the worker, then signs alice and bob in. Everything stops when
// the test ends.
func setup(t *testing.T) *env {
	t.Helper()
	e := &env{dir: t.TempDir(), dbPassword: dbPassword(t), pw: randomHex(t, 16)}
	var temporal, envoy *proc
	t.Cleanup(func() {
		e.worker.stop(syscall.SIGTERM)
		e.server.stop(syscall.SIGTERM)
		envoy.stop(syscall.SIGTERM)
		temporal.stop(syscall.SIGTERM)
	})
	if err := os.MkdirAll(e.home(), 0o700); err != nil {
		t.Fatal(err)
	}
	// Fresh logs per run: later tasks search every one of them.
	old, _ := filepath.Glob(runFile(t, "logs", "*.log"))
	for _, f := range old {
		os.Remove(f)
	}
	if err := os.WriteFile(e.token(), []byte(randomHex(t, 24)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	e.bin = filepath.Join(t.TempDir(), "cutover")
	build := exec.Command("go", "build", "-o", e.bin, "./cmd/cutover")
	build.Dir, build.Env = repoRoot(t), append(os.Environ(), "KUBECONFIG=/nonexistent")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}

	tport := freePort(t)
	e.temporal = "127.0.0.1:" + strconv.Itoa(tport)
	temporal = e.start(t, "temporal", temporalBin(), e.baseEnv(), nil, "server", "start-dev", "--headless",
		"--ip", "127.0.0.1", "--port", strconv.Itoa(tport), "--http-port", strconv.Itoa(freePort(t)),
		"--metrics-port", strconv.Itoa(freePort(t)), "--log-level", "error")
	waitUntil(t, "temporal", time.Minute, temporal, func() error {
		c, err := e.temporalClient()
		if err == nil {
			c.Close()
		}
		return err
	})

	for _, name := range []string{"alice", "bob"} {
		e.run(t, "approver-"+name, e.pw+"\n", "approver", "new", "--data", e.data(), "--name", name, "--password-stdin")
	}

	adminAddr := "127.0.0.1:" + strconv.Itoa(freePort(t))
	e.envoyAdmin = "http://" + adminAddr
	e.server = e.start(t, "server", e.bin, e.baseEnv(), nil, "server",
		"--state", filepath.Join(e.dir, "frontdoor.json"), "--token-file", e.token(), "--data", e.data(),
		"--listen", listenAddr, "--envoy-admin", e.envoyAdmin, "--xds", xdsAddr, "--temporal", e.temporal)

	bc := exec.Command(e.bin, "envoy-bootstrap", "--admin", adminAddr, "--xds", xdsAddr)
	bc.Env = e.baseEnv()
	boot, err := bc.Output()
	if err != nil {
		t.Fatalf("envoy-bootstrap: %v", err)
	}
	bootPath := filepath.Join(e.dir, "envoy.yaml")
	if err := os.WriteFile(bootPath, boot, 0o600); err != nil {
		t.Fatal(err)
	}
	envoyBin := os.Getenv("CUTOVER_ENVOY")
	if envoyBin == "" {
		if envoyBin, err = exec.LookPath("envoy"); err != nil {
			t.Fatal("envoy not found: set CUTOVER_ENVOY or put envoy on PATH")
		}
	}
	envoy = e.start(t, "envoy", envoyBin, e.baseEnv(), nil, "-c", bootPath, "--disable-hot-restart", "--log-level", "warn")

	e.startWorker(t)

	ca := filepath.Join(e.data(), "ca.crt")
	waitUntil(t, "the console", 30*time.Second, e.server, func() error {
		caPEM, err := os.ReadFile(ca)
		if err != nil {
			return err
		}
		e.alice, err = e.login(caPEM, "alice")
		if err != nil {
			return err
		}
		e.bob, err = e.login(caPEM, "bob")
		return err
	})
	// /ready turns 200 once Envoy has its listener from the server.
	waitUntil(t, "envoy to connect", 30*time.Second, envoy, func() error {
		resp, err := http.Get(e.envoyAdmin + "/ready")
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return errors.New(resp.Status)
		}
		return nil
	})
	waitUntil(t, "the worker to poll", 30*time.Second, e.worker, func() error {
		c, err := e.temporalClient()
		if err != nil {
			return err
		}
		defer c.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		q, err := c.DescribeTaskQueue(ctx, flow.TaskQueue, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
		if err != nil {
			return err
		}
		if len(q.GetPollers()) == 0 {
			return errors.New("no pollers")
		}
		return nil
	})
	return e
}

// temporalBin is the Temporal CLI: on PATH, or where Homebrew puts it.
func temporalBin() string {
	if p, err := exec.LookPath("temporal"); err == nil {
		return p
	}
	return "/opt/homebrew/bin/temporal"
}

func (e *env) login(caPEM []byte, name string) (*console.Client, error) {
	c, err := console.NewClient(consoleURL, caPEM)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return c, c.Login(ctx, name, e.pw)
}

func (e *env) temporalClient() (client.Client, error) {
	return client.Dial(client.Options{HostPort: e.temporal, Namespace: "default",
		Logger: sdklog.NewStructuredLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))})
}

// startWorker starts the worker with the secrets the plan's env: refs
// name. They exist only in its environment.
func (e *env) startWorker(t testing.TB) {
	t.Helper()
	dsn := func(port int) string {
		return (&url.URL{Scheme: "postgres", User: url.UserPassword("postgres", e.dbPassword),
			Host: "127.0.0.1:" + strconv.Itoa(port), Path: "/shop", RawQuery: "sslmode=disable"}).String()
	}
	quoted := "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(e.dbPassword) + "'"
	repl := func(host string) string {
		return "host=" + host + " port=5432 user=postgres dbname=shop password=" + quoted
	}
	env := append(e.baseEnv(),
		"E2E_SRC_DSN="+dsn(pgAPort), "E2E_TGT_DSN="+dsn(pgBPort),
		"E2E_SRC_REPL="+repl(pgAAddress), "E2E_TGT_REPL="+repl(pgBAddress),
		"E2E_PGB_ADMIN="+pgbAdmin)
	e.worker = e.start(t, "worker", e.bin, env, nil, "worker", "--temporal", e.temporal)
}

// killWorker kills the worker outright, as a crash would.
func (e *env) killWorker() { e.worker.stop(syscall.SIGKILL) }

// writePlan writes the plan for migration shop: the demo API on the Docker
// hosts, its worker on the kind clusters, and database shop.
func writePlan(t testing.TB, e *env) string {
	t.Helper()
	side := func(cloud, secrets string, sshPort int, addr string) plan.Env {
		h := sshHost(t, sshPort)
		h.Sudo = cloud == "b"
		return plan.Env{
			Postgres: plan.Postgres{DSN: secret.Ref("env:E2E_" + secrets + "_DSN"),
				ReplicationDSN: secret.Ref("env:E2E_" + secrets + "_REPL"),
				Route:          "host=" + addr + " port=5432 dbname=shop"},
			Hosts: map[string]plan.Host{"dockerhost": h},
			Kube:  &plan.Kube{Kubeconfig: runFile(t, cloud+".kubeconfig"), Context: "kind-cutover-e2e-" + cloud},
		}
	}
	p := plan.Plan{
		Version: 1,
		Name:    migrationID,
		Source:  side("a", "SRC", sshAPort, pgAAddress),
		Target:  side("b", "TGT", sshBPort, pgBAddress),
		Database: plan.Database{
			Name:    "shop",
			Schemas: []string{"public"},
			PgBouncer: plan.PgBouncer{AdminDSN: "env:E2E_PGB_ADMIN", RoutesFile: runFile(t, "pgbouncer", "routes.ini"),
				Addresses: []string{"172.30.0.10/32"}},
			Tools: pgTools(t),
		},
		FrontDoor: &plan.FrontDoor{API: apiURL, Token: secret.Ref("file:" + e.token()), Addresses: []string{"127.0.0.1", "::1"}},
		Services: []plan.Service{
			{Name: "api", Kind: plan.KindCompose, Compose: &plan.Compose{Project: "shop", Service: "api"},
				Route:  &plan.Route{Host: "localhost", Prefix: "/", HealthPath: "/healthz"},
				Source: plan.Placement{Host: "dockerhost", Endpoint: apiA},
				Target: plan.Placement{Host: "dockerhost", Endpoint: apiB}},
			{Name: "worker", Kind: plan.KindDeployment, Deployment: &plan.Deployment{Namespace: "shop", Name: "shop-worker"}},
		},
	}
	sec := func(d time.Duration) plan.Duration { return plan.Duration(d) }
	p.Thresholds = plan.Thresholds{
		Replicate: plan.Replicate{MaxLag: sec(time.Second), StableFor: sec(3 * time.Second), SampleEvery: sec(500 * time.Millisecond)},
		Rehearse:  plan.Rehearse{SampleRows: 1000, Window: sec(10 * time.Second)},
		Shift: plan.Shift{Steps: []int{10, 100}, Hold: sec(8 * time.Second), SampleEvery: sec(time.Second),
			MaxErrorDelta: 0.5, MaxP99: 50, MinRequests: 20},
		Switch: plan.Switch{MaxPause: sec(10 * time.Second), SequenceMargin: 1000},
		Finish: plan.Finish{Watch: sec(3 * time.Second)},
	}
	plan.ApplyDefaults(&p)
	if err := p.Validate(); err != nil {
		t.Fatalf("e2e plan invalid: %v", err)
	}
	y, err := yaml.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(e.dir, "plan.yaml")
	if err := os.WriteFile(path, y, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// pgTools finds pg_dump and psql on PATH or in Homebrew's libpq, as
// itest.Tools does (internal/itest builds only with the integration tag).
func pgTools(t testing.TB) plan.Tools {
	find := func(name string) string {
		if p, err := exec.LookPath(name); err == nil {
			return p
		}
		p := "/opt/homebrew/opt/libpq/bin/" + name
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("%s not found on PATH or in /opt/homebrew/opt/libpq/bin", name)
		}
		return p
	}
	return plan.Tools{PgDump: find("pg_dump"), Psql: find("psql")}
}

// routeApply registers the plan's routes at 100% source.
func routeApply(t testing.TB, e *env, planPath string) {
	t.Helper()
	e.run(t, "route-apply", "", "route", "apply", "--plan", planPath)
}

// requireClean fails the test unless the fixture is as make e2e-up left
// it, no migration shop runs, and every front door route is source-only
// (see sourceOnly).
func requireClean(t testing.TB, e *env) {
	t.Helper()
	problems := fixtureProblems(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if c, err := e.temporalClient(); err != nil {
		problems = append(problems, "temporal: "+err.Error())
	} else {
		d, err := c.DescribeWorkflowExecution(ctx, flow.WorkflowID(migrationID), "")
		var nf *serviceerror.NotFound
		switch {
		case errors.As(err, &nf):
		case err != nil:
			problems = append(problems, "temporal: "+err.Error())
		case d.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING:
			problems = append(problems, flow.WorkflowID(migrationID)+" is running")
		}
		c.Close()
	}
	tok, err := os.ReadFile(e.token())
	if err != nil {
		t.Fatal(err)
	}
	routes, err := frontdoor.NewClient(apiURL, strings.TrimSpace(string(tok))).Routes(ctx)
	switch {
	case err != nil:
		problems = append(problems, "front door: "+err.Error())
	default:
		for _, r := range routes {
			if !sourceOnly(r) {
				problems = append(problems, fmt.Sprintf("front door: route %s sends or mirrors traffic to the target", r.Key()))
			}
		}
	}
	if len(problems) > 0 {
		t.Logf("not clean:\n  %s", strings.Join(problems, "\n  "))
		t.Fatal("fixture is not clean; " + hint)
	}
}

// sourceOnly reports whether r is what route apply registers: all traffic
// on the source, which is not cloud B, and nothing sent or mirrored to a
// target. A rollback leaves the route like this, and no command removes a
// route, so it counts as clean; the next route apply re-registers it as is.
func sourceOnly(r frontdoor.Route) bool {
	return r.Source != nil && r.Source.String() != apiB && r.Target == nil && r.TargetWeight == 0 && !r.Mirror
}

// drive follows migration shop's status over the console's event stream
// and decides each open gate as decide says, once per approval it still
// needs, until the phase, state or open gate is until. It returns that
// status. A terminal state other than until, or driveTimeout, fails the
// test with the last status seen.
func drive(t testing.TB, e *env, until string, decide func(gate flow.Gate) (who *console.Client, approve bool)) flow.Status {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), driveTimeout)
	events := make(chan flow.Status)
	followed := make(chan struct{})
	go func() {
		defer close(followed)
		e.follow(ctx, t, events)
	}()
	// The follower logs to t, so it must be gone before drive returns.
	defer func() { cancel(); <-followed }()
	var last flow.Status
	acted := "" // the gate and approval count last decided
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("drive: no %q in %s; last status: phase %s, state %s, gate %s, events %s",
				until, driveTimeout, last.Phase, last.State, gateString(last.Gate), lastEvents(last))
		case st := <-events:
			last = st
			if st.Phase == until || st.State == until || (st.Gate != nil && st.Gate.Name == until) {
				return st
			}
			switch st.State {
			case flow.StateFinished, flow.StateFailed, flow.StateRolledBack:
				t.Fatalf("drive: migration ended %s waiting for %q; phase %s, events %s", st.State, until, st.Phase, lastEvents(st))
			}
			g := st.Gate
			if g == nil {
				acted = ""
				continue
			}
			key := gateString(g)
			if key == acted || len(g.Approvals) >= g.Needs {
				continue
			}
			who, approve := decide(*g)
			d := flow.Reject
			if approve {
				d = flow.Approve
			}
			err := who.Decide(ctx, migrationID, g.Name, d, "e2e")
			var ae *console.APIError
			if errors.As(err, &ae) && ae.Status == http.StatusConflict &&
				(ae.Message == "evidence changed; review the gate again" || strings.HasPrefix(ae.Message, "gate is not open")) {
				// The status moved on; the next event says how.
				t.Logf("drive: %s %s: %s", d, g.Name, ae.Message)
				continue
			}
			if err != nil {
				t.Fatalf("drive: %s %s: %v", d, g.Name, err)
			}
			t.Logf("drive: %s %s (%d/%d before)", d, g.Name, len(g.Approvals), g.Needs)
			acted = key
		}
	}
}

func gateString(g *flow.Gate) string {
	if g == nil {
		return "none"
	}
	return fmt.Sprintf("%s/%d", g.Name, len(g.Approvals))
}

func lastEvents(st flow.Status) string {
	ev := st.Events[max(0, len(st.Events)-5):]
	var b strings.Builder
	for _, x := range ev {
		fmt.Fprintf(&b, "\n  %s %s %s %s", x.Phase, x.Kind, x.Actor, x.Message)
	}
	return b.String()
}

// follow sends each status event from /api/stream?migration=shop to out
// until ctx ends, on a session of its own (bob's), opening the stream
// again whenever it ends.
func (e *env) follow(ctx context.Context, t testing.TB, out chan<- flow.Status) {
	for ctx.Err() == nil {
		err := e.stream(ctx, out)
		if ctx.Err() != nil {
			return
		}
		t.Logf("drive: stream ended (%v); opening it again", err)
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
	}
}

// session signs name in on an HTTP/2 client of its own, for console reads
// console.Client has no method for. The caller closes its idle connections.
func (e *env) session(ctx context.Context, name string) (*http.Client, error) {
	caPEM, err := os.ReadFile(filepath.Join(e.data(), "ca.crt"))
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEM)
	jar, _ := cookiejar.New(nil)
	hc := &http.Client{Jar: jar, Transport: &http.Transport{ForceAttemptHTTP2: true,
		TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}}
	body, _ := json.Marshal(map[string]string{"name": name, "password": e.pw})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, consoleURL+"/api/login", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := hc.Do(req)
	if err != nil {
		hc.CloseIdleConnections()
		return nil, err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		hc.CloseIdleConnections()
		return nil, fmt.Errorf("login: %s", resp.Status)
	}
	return hc, nil
}

func (e *env) stream(ctx context.Context, out chan<- flow.Status) error {
	hc, err := e.session(ctx, "bob")
	if err != nil {
		return err
	}
	defer hc.CloseIdleConnections()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, consoleURL+"/api/stream?migration="+migrationID, nil)
	req.Header.Set("Accept", "text/event-stream")
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("stream: %s", resp.Status)
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64<<10), 8<<20)
	var name string
	for sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "event: "):
			name = strings.TrimPrefix(line, "event: ")
			if name == "expired" {
				return errors.New("session expired")
			}
		case strings.HasPrefix(line, "data: ") && name == "status":
			var st console.StatusReply
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &st); err != nil {
				return err
			}
			select {
			case out <- st.Status:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	return io.EOF
}

func TestSourceOnly(t *testing.T) {
	src := &frontdoor.Upstream{Address: "127.0.0.1", Port: 28080}
	tgt := &frontdoor.Upstream{Address: "127.0.0.1", Port: 38080}
	for _, c := range []struct {
		name string
		r    frontdoor.Route
		want bool
	}{
		{"as route apply leaves it", frontdoor.Route{Source: src}, true},
		{"target at weight 0", frontdoor.Route{Source: src, Target: tgt}, false},
		{"target weight", frontdoor.Route{Source: src, Target: tgt, TargetWeight: 10}, false},
		{"weight without a target", frontdoor.Route{Source: src, TargetWeight: 10}, false},
		{"mirror", frontdoor.Route{Source: src, Target: tgt, Mirror: true}, false},
		{"mirror without a target", frontdoor.Route{Source: src, Mirror: true}, false},
		{"source on cloud b", frontdoor.Route{Source: tgt}, false},
		{"finished", frontdoor.Route{Target: tgt, TargetWeight: 100}, false},
	} {
		if got := sourceOnly(c.r); got != c.want {
			t.Errorf("%s: sourceOnly = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestHarnessStarts proves the harness without migrating: every binary
// starts, both approvers sign in, and the fixture is still clean.
func TestHarnessStarts(t *testing.T) {
	e := setup(t)
	requireClean(t, e)
}
