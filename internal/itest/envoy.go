//go:build integration

package itest

import (
	"context"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// FreePort returns a loopback TCP port that was free a moment ago.
func FreePort(t testing.TB) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// StartEnvoy runs Envoy natively with the given bootstrap and waits until
// its admin reports ready. Docker Desktop cannot reach services bound to
// the host's loopback, so Envoy is never run in a container here.
func StartEnvoy(t testing.TB, bootstrap string, adminPort int) string {
	t.Helper()
	bin := os.Getenv("CUTOVER_ENVOY")
	if bin == "" {
		p, err := exec.LookPath("envoy")
		if err != nil {
			t.Fatalf("envoy not found: set CUTOVER_ENVOY to the envoy binary or put envoy on PATH (macOS: brew install envoy)")
		}
		bin = p
	}
	dir := t.TempDir()
	cfg := filepath.Join(dir, "envoy.yaml")
	if err := os.WriteFile(cfg, []byte(bootstrap), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, bin, "-c", cfg, "--disable-hot-restart", "--log-level", "warn",
		"--log-path", filepath.Join(dir, "envoy.log"))
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("start envoy %s: %v (set CUTOVER_ENVOY to the envoy binary or put envoy on PATH)", bin, err)
	}
	t.Cleanup(func() {
		cancel()
		_ = cmd.Wait()
		if t.Failed() {
			if b, err := os.ReadFile(filepath.Join(dir, "envoy.log")); err == nil {
				t.Logf("envoy log:\n%s", b)
			}
		}
	})
	admin := "http://127.0.0.1:" + strconv.Itoa(adminPort)
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(admin + "/ready")
		if err == nil {
			resp.Body.Close()
			// /ready is 200 once Envoy is LIVE; it stays 503 while it
			// waits for its first xDS configuration, which is fine here:
			// the admin is up, and the caller waits for routes itself.
			if resp.StatusCode == 200 || resp.StatusCode == 503 {
				return admin
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("envoy admin %s did not come up in 20s", admin)
	return ""
}
