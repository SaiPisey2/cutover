//go:build integration

package itest

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"testing"

	"go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/testsuite"
)

// QuietLogger keeps the Temporal SDK's client log at warnings and above.
func QuietLogger() sdklog.Logger {
	return sdklog.NewStructuredLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
}

// StartTemporal starts a real Temporal dev server (in memory, headless) on
// a free 127.0.0.1 port, using the installed temporal CLI so nothing is
// downloaded. It returns a client and the server's host:port; both end
// with the test.
func StartTemporal(t testing.TB) (client.Client, string) {
	t.Helper()
	bin, err := exec.LookPath("temporal")
	if err != nil {
		bin = "/opt/homebrew/bin/temporal"
		if _, err := os.Stat(bin); err != nil {
			t.Fatal("temporal CLI not found on PATH or in /opt/homebrew/bin (brew install temporal)")
		}
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hostPort := l.Addr().String()
	l.Close()
	srv, err := testsuite.StartDevServer(context.Background(), testsuite.DevServerOptions{ExistingPath: bin, LogLevel: "error",
		ClientOptions: &client.Options{HostPort: hostPort, Namespace: "default", Logger: QuietLogger()}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Stop() })
	return srv.Client(), hostPort
}
