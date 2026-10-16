package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/client"

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
