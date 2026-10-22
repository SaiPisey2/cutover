package main

import (
	"bytes"
	"context"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/client"

	"github.com/SaiPisey2/cutover/internal/console"
	"github.com/SaiPisey2/cutover/internal/flow"
	"github.com/SaiPisey2/cutover/internal/plan"
	"github.com/SaiPisey2/cutover/internal/store"
	"github.com/SaiPisey2/cutover/ui"
)

func noDial(t *testing.T) {
	old := dial
	dial = func(string, string, io.Writer) (client.Client, error) {
		t.Fatal("must not reach Temporal")
		return nil, nil
	}
	t.Cleanup(func() { dial = old })
}

func exec(args ...string) (int, string, string) {
	var out, errb bytes.Buffer
	code := run(context.Background(), args, &out, &errb)
	return code, out.String(), errb.String()
}

// noServer makes any password prompt or console request fail the test,
// and clears the ambient settings a developer's shell or home might hold.
func noServer(t *testing.T) {
	t.Helper()
	isolate(t)
	t.Setenv("CUTOVER_SERVER", "https://127.0.0.1:1")
	oldT, oldR := isTerminal, readPassword
	isTerminal = func() bool { return true }
	readPassword = func() ([]byte, error) { t.Error("must not prompt for a password"); return nil, io.EOF }
	t.Cleanup(func() { isTerminal, readPassword = oldT, oldR })
}

// isolate keeps the CLI's defaults away from the real environment.
func isolate(t *testing.T) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("CUTOVER_CA", "")
	t.Setenv("CUTOVER_SERVER", "")
}

func TestUsageErrors(t *testing.T) {
	noDial(t)
	noServer(t)
	bad := filepath.Join(t.TempDir(), "plan.yaml")
	if err := os.WriteFile(bad, []byte("version: 2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		args []string
		want string
	}{
		{nil, "usage: cutover"},
		{[]string{"teleport"}, "unknown command"},
		{[]string{"migrate"}, "--plan is required"},
		{[]string{"migrate", "--plan", bad}, "version"},
		{[]string{"migrate", "--plan", "../../examples/plan.yaml", "--id", "Bad_ID"}, "--id"},
		{[]string{"status"}, "migration ID"},
		{[]string{"gate", "shop", "switch"}, "approve or reject"},
		{[]string{"gate", "shop", "switch", "maybe", "--as", "alice"}, "approve or reject"},
		{[]string{"gate", "shop", "switch", "approve"}, "--as is required"},
		{[]string{"rollback", "shop", "--as", "alice"}, "--reason is required"},
		{[]string{"rollback", "shop", "--reason", "r"}, "--as is required"},
		{[]string{"rollback", "Shop", "--as", "alice", "--reason", "r"}, "migration ID"},
		{[]string{"migrate", "--plan", "../../examples/plan.yaml"}, "--as is required"},
		{[]string{"gate", "Shop", "switch", "approve", "--as", "alice"}, "migration ID"},
		{[]string{"gate", "shop", "switch", "approve", "--as", "alice", "--server", "http://127.0.0.1:7700"}, "--server"},
		{[]string{"gate", "shop", "switch", "approve", "--as", "alice"}, "--ca"},
		{[]string{"migrate", "--plan", "../../examples/plan.yaml", "--as", "alice"}, "--ca"},
	} {
		code, _, stderr := exec(tc.args...)
		if code != 2 || !strings.Contains(stderr, tc.want) {
			t.Errorf("%v: code %d stderr %q, want 2 and %q", tc.args, code, stderr, tc.want)
		}
	}
}

func TestHelpExitsZero(t *testing.T) {
	noDial(t)
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"worker", "-h"}, "-temporal"},
		{[]string{"migrate", "-h"}, "-plan"},
	} {
		code, _, stderr := exec(tc.args...)
		if code != 0 || !strings.Contains(stderr, tc.want) || strings.Contains(stderr, "help requested") {
			t.Errorf("%v: code %d stderr %q, want 0 and the flag list", tc.args, code, stderr)
		}
	}
}

func TestPrintStatusShowsBrokenAndPendingUndo(t *testing.T) {
	var b bytes.Buffer
	printStatus(&b, flow.Status{
		MigrationID: "shop", Phase: flow.PhaseSwitch, State: flow.StateWaiting, Primary: "source",
		Gate:              &flow.Gate{Name: "replication-broken", Needs: 1, Summary: "roll back"},
		ReplicationBroken: true,
		PendingUndo:       []string{"replicate", "prepare"},
		Events:            []flow.Event{{At: time.Unix(0, 0), Phase: "switch", Kind: "unwind-halted", Message: "x"}},
	})
	out := b.String()
	for _, w := range []string{"gate replication-broken", "REPLICATION BROKEN", "replicate, prepare", "unwind-halted"} {
		if !strings.Contains(out, w) {
			t.Errorf("status output lacks %q:\n%s", w, out)
		}
	}
	b.Reset()
	printStatus(&b, flow.Status{MigrationID: "shop", Phase: flow.PhaseFinish, State: flow.StateRunning, Primary: "target"})
	if strings.Contains(b.String(), "BROKEN") || strings.Contains(b.String(), "pending undo") {
		t.Errorf("clean status shows warnings:\n%s", b.String())
	}
}

func TestHelpEverywhere(t *testing.T) {
	noDial(t)
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"status", "-h"}, "-json"},
		{[]string{"gate", "--help"}, "-note"},
		{[]string{"rollback", "-h"}, "-reason"},
		{[]string{"-h"}, "usage: cutover"},
		{[]string{"--help"}, "usage: cutover"},
		{[]string{"help"}, "positional arguments come before flags"},
	} {
		code, out, stderr := exec(tc.args...)
		if code != 0 || !strings.Contains(out+stderr, tc.want) || strings.Contains(out+stderr, "help requested") {
			t.Errorf("%v: code %d out %q stderr %q, want 0 and %q", tc.args, code, out, stderr, tc.want)
		}
	}
}

func TestMissingPlanIsRuntimeError(t *testing.T) {
	noDial(t)
	code, _, stderr := exec("migrate", "--plan", filepath.Join(t.TempDir(), "nope.yaml"))
	if code != 1 || stderr == "" {
		t.Errorf("code %d stderr %q, want 1", code, stderr)
	}
}

// fakeTemporal stands in for Temporal behind a real console API.
type fakeTemporal struct {
	mu        sync.Mutex
	statuses  map[string]flow.Status
	gates     []flow.GateSignal
	rollbacks []flow.RollbackSignal
	starts    []string
	plans     []plan.Plan
}

func (f *fakeTemporal) List(context.Context) ([]console.MigrationSummary, error) { return nil, nil }
func (f *fakeTemporal) Replicate(context.Context, string) (flow.ReplicateStatus, error) {
	return flow.ReplicateStatus{}, console.ErrNoMigration
}
func (f *fakeTemporal) Status(_ context.Context, id string) (flow.Status, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	st, ok := f.statuses[id]
	if !ok {
		return st, console.ErrNoMigration
	}
	return st, nil
}
func (f *fakeTemporal) Start(_ context.Context, id string, p plan.Plan) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.starts = append(f.starts, id)
	f.plans = append(f.plans, p)
	return nil
}
func (f *fakeTemporal) Gate(_ context.Context, _ string, s flow.GateSignal) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gates = append(f.gates, s)
	return nil
}
func (f *fakeTemporal) Rollback(_ context.Context, _ string, s flow.RollbackSignal) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rollbacks = append(f.rollbacks, s)
	return nil
}

func (f *fakeTemporal) open(id, gate string, needs int, approvals ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statuses[id] = flow.Status{MigrationID: id, Phase: gate, State: flow.StateWaiting,
		Gate: &flow.Gate{Name: gate, Phase: gate, Needs: needs, Approvals: append([]string{}, approvals...), Summary: "s"}}
}

var (
	pwHashOnce sync.Once
	pwHash     string
)

// consoleEnv is a TLS console over a real store and a fake Temporal,
// with the CLI pointed at it through the environment.
type consoleEnv struct {
	fake *fakeTemporal
	st   *store.Store
}

func newConsoleEnv(t *testing.T) *consoleEnv {
	t.Helper()
	isolate(t)
	noDial(t)
	pwHashOnce.Do(func() {
		var err error
		if pwHash, err = console.HashPassword(goodPassword); err != nil {
			t.Fatal(err)
		}
	})
	st, err := store.Open(filepath.Join(t.TempDir(), "c.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	for id, name := range map[string]string{"a1": "alice", "b2": "bob"} {
		if err := st.CreateApprover(context.Background(), store.Approver{ID: id, Name: name, PasswordHash: pwHash, Created: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	f := &fakeTemporal{statuses: map[string]flow.Status{}}
	auth := console.NewAuth(st, nil)
	mux := http.NewServeMux()
	console.Routes(mux, auth, console.Deps{Temporal: f, Store: st})
	srv := httptest.NewTLSServer(console.NewServer(auth, mux, ui.FS()))
	t.Cleanup(srv.Close)
	ca := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CUTOVER_SERVER", srv.URL)
	t.Setenv("CUTOVER_CA", ca)
	return &consoleEnv{fake: f, st: st}
}

// execIn runs the CLI with in as its stdin.
func execIn(in string, args ...string) (int, string, string) {
	old := stdin
	stdin = strings.NewReader(in)
	defer func() { stdin = old }()
	return exec(args...)
}

func TestGateViaServer(t *testing.T) {
	e := newConsoleEnv(t)
	e.fake.open("m1", "prepare", 1)
	code, out, errb := execIn(goodPassword+"\n", "gate", "m1", "prepare", "approve", "--as", "alice", "--password-stdin", "--note", "fine")
	if code != 0 {
		t.Fatalf("code %d out %q err %q", code, out, errb)
	}
	if len(e.fake.gates) != 1 || e.fake.gates[0] != (flow.GateSignal{Gate: "prepare", Decision: flow.Approve, ApproverID: "alice#a1", Note: "fine"}) {
		t.Fatalf("gates %+v", e.fake.gates)
	}
	if strings.Contains(out+errb, goodPassword) || strings.Contains(errb, "not authenticated") {
		t.Fatalf("output %q %q", out, errb)
	}
	rows, _ := e.st.AuditPage(context.Background(), "m1", 0, 10)
	if len(rows) != 1 || rows[0].Kind != "gate-decision" || rows[0].ActorID != "alice#a1" || rows[0].Evidence == "" {
		t.Fatalf("audit %+v", rows)
	}
}

func TestGateFromTerminal(t *testing.T) {
	e := newConsoleEnv(t)
	e.fake.open("m1", "prepare", 1)
	oldT, oldR := isTerminal, readPassword
	defer func() { isTerminal, readPassword = oldT, oldR }()
	isTerminal = func() bool { return true }
	readPassword = func() ([]byte, error) { return []byte(goodPassword), nil }
	if code, _, errb := exec("gate", "m1", "prepare", "reject", "--as", "bob"); code != 0 || len(e.fake.gates) != 1 || e.fake.gates[0].ApproverID != "bob#b2" {
		t.Fatalf("code %d err %q gates %+v", code, errb, e.fake.gates)
	}
}

func TestGateWithoutPasswordOnPipeFails(t *testing.T) {
	e := newConsoleEnv(t)
	e.fake.open("m1", "prepare", 1)
	oldT := isTerminal
	defer func() { isTerminal = oldT }()
	isTerminal = func() bool { return false }
	code, _, errb := execIn(goodPassword+"\n", "gate", "m1", "prepare", "approve", "--as", "alice")
	if code != 2 || !strings.Contains(errb, "no terminal; use --password-stdin") || len(e.fake.gates) != 0 {
		t.Fatalf("code %d err %q gates %+v", code, errb, e.fake.gates)
	}
	code, _, errb = execIn("", "gate", "m1", "prepare", "approve", "--as", "alice", "--password-stdin")
	if code != 2 || !strings.Contains(errb, "no password on stdin") {
		t.Fatalf("empty stdin: code %d err %q", code, errb)
	}
}

func TestGateRefusesClosedGate(t *testing.T) {
	e := newConsoleEnv(t)
	e.fake.open("shop", "prepare", 1)
	code, _, errb := execIn(goodPassword+"\n", "gate", "shop", "switch", "approve", "--as", "alice", "--password-stdin")
	if code != 1 || !strings.Contains(errb, "gate is not open (open: prepare)") || len(e.fake.gates) != 0 {
		t.Errorf("code %d stderr %q gates %v", code, errb, e.fake.gates)
	}
	e.fake.mu.Lock()
	e.fake.statuses["shop"] = flow.Status{MigrationID: "shop"}
	e.fake.mu.Unlock()
	code, _, errb = execIn(goodPassword+"\n", "gate", "shop", "switch", "approve", "--as", "alice", "--password-stdin")
	if code != 1 || !strings.Contains(errb, "(open: none)") || len(e.fake.gates) != 0 {
		t.Errorf("code %d stderr %q gates %v", code, errb, e.fake.gates)
	}
}

func TestSecondSwitchApprovalNeedsAnotherPerson(t *testing.T) {
	e := newConsoleEnv(t)
	e.fake.open("shop", "switch", 2, "Alice#zz")
	code, _, errb := execIn(goodPassword+"\n", "gate", "shop", "switch", "approve", "--as", "alice", "--password-stdin")
	if code != 1 || !strings.Contains(errb, "cutover: the second approval must come from a different person") || len(e.fake.gates) != 0 {
		t.Fatalf("code %d stderr %q gates %v", code, errb, e.fake.gates)
	}
}

func TestWrongPasswordIsRefused(t *testing.T) {
	e := newConsoleEnv(t)
	e.fake.open("m1", "prepare", 1)
	code, _, errb := execIn("not the password\n", "gate", "m1", "prepare", "approve", "--as", "alice", "--password-stdin")
	if code != 1 || !strings.Contains(errb, "unknown name or wrong password") || len(e.fake.gates) != 0 {
		t.Fatalf("code %d stderr %q", code, errb)
	}
}

func TestRollbackViaServer(t *testing.T) {
	e := newConsoleEnv(t)
	e.fake.open("shop", "prepare", 1)
	code, _, errb := execIn(goodPassword+"\n", "rollback", "shop", "--as", "alice", "--reason", "pager", "--password-stdin")
	if code != 0 || strings.Contains(errb, "not authenticated") || len(e.fake.rollbacks) != 1 ||
		e.fake.rollbacks[0] != (flow.RollbackSignal{ApproverID: "alice#a1", Reason: "pager"}) {
		t.Errorf("code %d stderr %q rollbacks %+v", code, errb, e.fake.rollbacks)
	}
}

func TestMigrateViaServer(t *testing.T) {
	e := newConsoleEnv(t)
	code, out, errb := execIn(goodPassword+"\n", "migrate", "--plan", "../../examples/plan.yaml", "--id", "shop-2", "--as", "alice", "--password-stdin")
	if code != 0 || !strings.Contains(out, "started migration shop-2") {
		t.Fatalf("code %d out %q err %q", code, out, errb)
	}
	if len(e.fake.starts) != 1 || e.fake.starts[0] != "shop-2" || e.fake.plans[0].Name != "shop" {
		t.Fatalf("starts %v", e.fake.starts)
	}
	rows, _ := e.st.AuditPage(context.Background(), "shop-2", 0, 10)
	if len(rows) != 1 || rows[0].Kind != "migration-started" || rows[0].ActorID != "alice#a1" {
		t.Fatalf("audit %+v", rows)
	}
}

func TestCADefaultsToHomeFile(t *testing.T) {
	e := newConsoleEnv(t)
	e.fake.open("m1", "prepare", 1)
	ca, err := os.ReadFile(os.Getenv("CUTOVER_CA"))
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("CUTOVER_CA", "")
	home := os.Getenv("HOME")
	if err := os.MkdirAll(filepath.Join(home, ".cutover"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".cutover", "ca.crt"), ca, 0o644); err != nil {
		t.Fatal(err)
	}
	if code, _, errb := execIn(goodPassword+"\n", "gate", "m1", "prepare", "approve", "--as", "alice", "--password-stdin"); code != 0 {
		t.Fatalf("code %d err %q", code, errb)
	}
}

func TestUnreadableCANamesItsSource(t *testing.T) {
	noServer(t)
	missing := filepath.Join(t.TempDir(), "nope.crt")
	code, _, errb := execIn(goodPassword+"\n", "gate", "m1", "prepare", "approve", "--as", "alice", "--password-stdin", "--ca", missing)
	if code != 1 || !strings.Contains(errb, "--ca "+missing+": ") {
		t.Fatalf("flag: code %d err %q", code, errb)
	}
	t.Setenv("CUTOVER_CA", missing)
	code, _, errb = execIn(goodPassword+"\n", "gate", "m1", "prepare", "approve", "--as", "alice", "--password-stdin")
	if code != 1 || !strings.Contains(errb, "CUTOVER_CA="+missing+": ") {
		t.Fatalf("env: code %d err %q", code, errb)
	}
}

func TestUnreachableServerGetsAHint(t *testing.T) {
	e := newConsoleEnv(t)
	e.fake.open("m1", "prepare", 1)
	dead := "https://127.0.0.1:" + strconv.Itoa(freePort(t))
	code, _, errb := execIn(goodPassword+"\n", "gate", "m1", "prepare", "approve", "--as", "alice", "--password-stdin", "--server", dead)
	want := `is "cutover server --data DIR" running at ` + dead + ", and does --ca point at DIR/ca.crt?"
	if code != 1 || !strings.Contains(errb, want) {
		t.Fatalf("code %d err %q", code, errb)
	}
	// A refusal from a console that answered gets no hint.
	code, _, errb = execIn("not the password\n", "gate", "m1", "prepare", "approve", "--as", "alice", "--password-stdin")
	if code != 1 || strings.Contains(errb, "is \"cutover server") {
		t.Fatalf("api refusal: code %d err %q", code, errb)
	}
}
