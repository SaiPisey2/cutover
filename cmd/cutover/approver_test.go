package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const goodPassword = "correct horse battery"

func runApprover(t *testing.T, in string, args ...string) (int, string, string) {
	t.Helper()
	old := stdin
	stdin = strings.NewReader(in)
	defer func() { stdin = old }()
	var out, errb bytes.Buffer
	code := run(context.Background(), append([]string{"approver"}, args...), &out, &errb)
	return code, out.String(), errb.String()
}

func TestApproverNewFromStdin(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	code, out, errs := runApprover(t, goodPassword+"\n", "new", "--data", dir, "--name", "alice", "--password-stdin")
	if code != 0 || !strings.HasPrefix(out, "created approver alice (ID alice#") {
		t.Fatalf("code %d out %q err %q", code, out, errs)
	}
	code, out, _ = runApprover(t, "", "list", "--data", dir)
	if code != 0 || !strings.Contains(out, "alice") || !strings.Contains(out, "active") || !strings.Contains(out, "NAME") {
		t.Fatalf("list: %d %q", code, out)
	}
	if fi, err := os.Stat(filepath.Join(dir, "cutover.db")); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("db mode: %v %v", fi, err)
	}
	if fi, _ := os.Stat(dir); fi.Mode().Perm() != 0o700 {
		t.Fatalf("dir mode %v", fi.Mode().Perm())
	}
}

func TestApproverNewRefusesWeakOrBadInput(t *testing.T) {
	dir := t.TempDir()
	code, _, errs := runApprover(t, "short\n", "new", "--data", dir, "--name", "alice", "--password-stdin")
	if code != 2 || !strings.Contains(errs, "12") {
		t.Fatalf("weak: %d %q", code, errs)
	}
	for _, n := range []string{"Alice", "a", "al ice"} {
		if code, _, _ := runApprover(t, goodPassword+"\n", "new", "--data", dir, "--name", n, "--password-stdin"); code != 2 {
			t.Errorf("name %q: code %d", n, code)
		}
	}
	if code, _, errs := runApprover(t, goodPassword+"\n", "new", "--data", dir, "--name", "alice"); code != 2 || !strings.Contains(errs, "no terminal; use --password-stdin") {
		t.Errorf("no tty: %d %q", code, errs)
	}
	runApprover(t, goodPassword+"\n", "new", "--data", dir, "--name", "bob", "--password-stdin")
	code, _, errs = runApprover(t, goodPassword+"\n", "new", "--data", dir, "--name", "bob", "--password-stdin")
	if code != 1 || !strings.Contains(errs, "disable it first") {
		t.Fatalf("dup: %d %q", code, errs)
	}
}

func TestApproverNewFromTerminal(t *testing.T) {
	oldT, oldR := isTerminal, readPassword
	defer func() { isTerminal, readPassword = oldT, oldR }()
	isTerminal = func() bool { return true }
	pws := []string{goodPassword, goodPassword}
	readPassword = func() ([]byte, error) { p := pws[0]; pws = pws[1:]; return []byte(p), nil }
	dir := t.TempDir()
	if code, _, errs := runApprover(t, "", "new", "--data", dir, "--name", "carol"); code != 0 {
		t.Fatalf("code %d %q", code, errs)
	}
	pws = []string{goodPassword, goodPassword + "x"}
	if code, _, _ := runApprover(t, "", "new", "--data", dir, "--name", "dave"); code != 2 {
		t.Fatalf("mismatch code %d", code)
	}
}

func TestApproverDisable(t *testing.T) {
	dir := t.TempDir()
	runApprover(t, goodPassword+"\n", "new", "--data", dir, "--name", "alice", "--password-stdin")
	if code, _, errs := runApprover(t, "", "disable", "--data", dir, "alice"); code != 0 {
		t.Fatalf("disable: %d %q", code, errs)
	}
	if _, out, _ := runApprover(t, "", "list", "--data", dir); !strings.Contains(out, "disabled") {
		t.Fatalf("list: %q", out)
	}
	if code, _, _ := runApprover(t, "", "disable", "--data", dir, "nobody"); code != 1 {
		t.Fatalf("unknown: %d", code)
	}
	// the name is free again
	if code, _, errs := runApprover(t, goodPassword+"\n", "new", "--data", dir, "--name", "alice", "--password-stdin"); code != 0 {
		t.Fatalf("reuse: %d %q", code, errs)
	}
}

func TestApproverNeverPrintsThePassword(t *testing.T) {
	dir := t.TempDir()
	pw := "s3cret-passphrase-xyz"
	_, out, errs := runApprover(t, pw+"\n", "new", "--data", dir, "--name", "alice", "--password-stdin")
	_, out2, errs2 := runApprover(t, pw+"\n", "new", "--data", dir, "--name", "alice", "--password-stdin")
	_, out3, errs3 := runApprover(t, "", "list", "--data", dir)
	if all := out + errs + out2 + errs2 + out3 + errs3; strings.Contains(all, pw) {
		t.Fatalf("password printed: %q", all)
	}
}

func TestApproverHelp(t *testing.T) {
	for _, a := range [][]string{{"-h"}, {"new", "-h"}, {"list", "-h"}, {"disable", "-h"}} {
		if code, _, _ := runApprover(t, "", a...); code != 0 {
			t.Errorf("%v: code %d", a, code)
		}
	}
}
