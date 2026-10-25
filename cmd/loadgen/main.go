// Command loadgen drives HTTP load at the demo API and prints a JSON report.
// It exits 1 if any request failed.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/SaiPisey2/cutover/internal/loadgen"
)

func main() {
	target := flag.String("target", "http://127.0.0.1:18080", "base URL to load")
	host := flag.String("host", "", "Host header to send")
	writers := flag.Int("writers", 2, "concurrent writers (POST /orders)")
	readers := flag.Int("readers", 2, "concurrent readers")
	interval := flag.Duration("interval", 20*time.Millisecond, "pause between requests per worker")
	duration := flag.Duration("duration", 10*time.Second, "how long to run")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	run := loadgen.Start(ctx, loadgen.Config{
		Target: *target, Host: *host, Writers: *writers, Readers: *readers,
		Interval: *interval, Timeout: 5 * time.Second,
	})
	select {
	case <-time.After(*duration):
	case <-ctx.Done():
	}
	rep := run.Stop()
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(rep); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
	if rep.Failures > 0 {
		os.Exit(1)
	}
}
