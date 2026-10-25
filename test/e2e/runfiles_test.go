//go:build e2e

// Package e2e runs cutover end to end against the kind fixture that
// make e2e-up builds. Tests fail, never skip, when the fixture is down.
package e2e

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

const hint = "run: make e2e-down e2e-up"

func repoRoot(t testing.TB) string {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	dir := filepath.Dir(file)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above " + file)
		}
		dir = parent
	}
}

// runFile is the absolute path of a file make e2e-up wrote under test/run/e2e.
func runFile(t testing.TB, parts ...string) string {
	t.Helper()
	return filepath.Join(append([]string{repoRoot(t), "test", "run", "e2e"}, parts...)...)
}

// dbPassword is the generated password of postgres and shop on both databases.
func dbPassword(t testing.TB) string {
	t.Helper()
	b, err := os.ReadFile(runFile(t, "pg-password"))
	if err != nil {
		t.Fatalf("read pg-password: %v (%s)", err, hint)
	}
	pw := strings.TrimSpace(string(b))
	if pw == "" {
		t.Fatalf("pg-password is empty (%s)", hint)
	}
	return pw
}
