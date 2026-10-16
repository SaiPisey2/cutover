package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	"github.com/SaiPisey2/cutover/internal/flow"
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

func TestUsageErrors(t *testing.T) {
	noDial(t)
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

type encoded struct{ st flow.Status }

func (e encoded) HasValue() bool { return true }
func (e encoded) Get(p interface{}) error {
	b, _ := json.Marshal(e.st)
	return json.Unmarshal(b, p)
}

type fakeClient struct {
	client.Client
	st      flow.Status
	signals []string
}

func (f *fakeClient) QueryWorkflow(context.Context, string, string, string, ...interface{}) (converter.EncodedValue, error) {
	return encoded{f.st}, nil
}
func (f *fakeClient) SignalWorkflow(_ context.Context, _, _, name string, _ interface{}) error {
	f.signals = append(f.signals, name)
	return nil
}
func (f *fakeClient) Close() {}

func fake(t *testing.T, st flow.Status) *fakeClient {
	f := &fakeClient{st: st}
	old := dial
	dial = func(string, string, io.Writer) (client.Client, error) { return f, nil }
	t.Cleanup(func() { dial = old })
	return f
}

func TestGateRefusesClosedGate(t *testing.T) {
	f := fake(t, flow.Status{Gate: &flow.Gate{Name: "prepare"}})
	code, _, stderr := exec("gate", "shop", "switch", "approve", "--as", "alice")
	if code != 1 || !strings.Contains(stderr, "gate switch is not open (open: prepare)") || len(f.signals) != 0 {
		t.Errorf("code %d stderr %q signals %v", code, stderr, f.signals)
	}
	f.st.Gate = nil
	code, _, stderr = exec("gate", "shop", "switch", "approve", "--as", "alice")
	if code != 1 || !strings.Contains(stderr, "(open: none)") || len(f.signals) != 0 {
		t.Errorf("code %d stderr %q signals %v", code, stderr, f.signals)
	}
	f.st.Gate = &flow.Gate{Name: "switch"}
	code, _, _ = exec("gate", "shop", "switch", "approve", "--as", "alice")
	if code != 0 || len(f.signals) != 1 {
		t.Errorf("open gate: code %d signals %v", code, f.signals)
	}
}

func TestRollbackNote(t *testing.T) {
	f := fake(t, flow.Status{})
	code, _, stderr := exec("rollback", "shop", "--as", "a", "--reason", "r")
	if code != 0 || !strings.Contains(stderr, "not authenticated in this release") || len(f.signals) != 1 {
		t.Errorf("code %d stderr %q", code, stderr)
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
