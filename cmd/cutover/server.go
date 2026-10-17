package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/SaiPisey2/cutover/internal/frontdoor"
	"github.com/SaiPisey2/cutover/internal/ops"
	"github.com/SaiPisey2/cutover/internal/plan"
)

func serverCmd(ctx context.Context, args []string, stderr io.Writer) error {
	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	fs.SetOutput(stderr)
	state := fs.String("state", "", "front-door state file (routes survive restarts)")
	tokenPath := fs.String("token-file", "", "file holding the API token (at least 16 bytes)")
	listen := fs.String("listen", "0.0.0.0:8080", "address Envoy's user-facing listener binds (IP:port)")
	xdsAddr := fs.String("xds", "127.0.0.1:18000", "xDS address (loopback IP:port or unix:/path)")
	apiAddr := fs.String("api", "127.0.0.1:7600", "API address (loopback)")
	admin := fs.String("envoy-admin", "http://127.0.0.1:9901", "Envoy admin URL")
	drain := fs.Duration("drain", 5*time.Second, "wait between unrouting a cluster and removing it")
	if err := parse(fs, args); err != nil {
		return err
	}
	switch {
	case *state == "":
		return usagef("server: --state is required")
	case *tokenPath == "":
		return usagef("server: --token-file is required")
	}
	if err := frontdoor.RequireLoopback(*apiAddr); err != nil {
		return usagef("server: --api: %v", err)
	}
	if !strings.HasPrefix(*xdsAddr, "unix:") {
		if err := frontdoor.RequireLoopback(*xdsAddr); err != nil {
			return usagef("server: --xds: %v", err)
		}
	}
	if err := frontdoor.RequireLoopback(strings.TrimPrefix(strings.TrimPrefix(*admin, "http://"), "https://")); err != nil {
		return usagef("server: --envoy-admin: %v", err)
	}
	lh, lp, err := net.SplitHostPort(*listen)
	port, perr := strconv.Atoi(lp)
	if err != nil || net.ParseIP(lh) == nil || perr != nil || port < 1 || port > 65535 {
		return usagef("server: --listen %q must be IP:port", *listen)
	}
	tb, err := os.ReadFile(*tokenPath)
	if err != nil {
		return err
	}
	token := strings.TrimSpace(string(tb))
	if len(token) < 16 {
		return usagef("server: the token in %s must be at least 16 bytes", *tokenPath)
	}
	x, err := frontdoor.NewXDS(frontdoor.XDSConfig{Listener: frontdoor.Listener{Address: lh, Port: port},
		Store: &frontdoor.Store{Path: *state}, Drain: *drain})
	if err != nil {
		return err
	}
	xl, err := frontdoor.ListenXDS(*xdsAddr)
	if err != nil {
		return err
	}
	al, err := frontdoor.ListenAPI(*apiAddr)
	if err != nil {
		xl.Close()
		return err
	}
	api := &frontdoor.API{XDS: x, Admin: &frontdoor.Admin{Base: strings.TrimSuffix(*admin, "/"), HTTP: &http.Client{Timeout: 10 * time.Second}}, Token: token}
	hs := &http.Server{Handler: api.Handler(), ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 4 * time.Minute}

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	errs := make(chan error, 2)
	go func() { errs <- x.Serve(xl) }()
	go func() { errs <- hs.Serve(al) }()
	fmt.Fprintf(stderr, "cutover server: xds on %s, api on %s, envoy listener %s\n", *xdsAddr, *apiAddr, *listen)
	select {
	case <-ctx.Done():
	case err := <-errs:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			x.Stop()
			_ = hs.Close()
			return err
		}
	}
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = hs.Shutdown(sctx)
	x.Stop()
	return nil
}

func bootstrapCmd(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("envoy-bootstrap", flag.ContinueOnError)
	fs.SetOutput(stderr)
	xdsAddr := fs.String("xds", "127.0.0.1:18000", "cutover server's xDS address")
	admin := fs.String("admin", "127.0.0.1:9901", "Envoy admin address (loopback)")
	if err := parse(fs, args); err != nil {
		return err
	}
	y, err := frontdoor.Bootstrap(frontdoor.BootstrapConfig{XDS: *xdsAddr, Admin: *admin})
	if err != nil {
		return usagef("envoy-bootstrap: %v", err)
	}
	_, err = io.WriteString(stdout, y)
	return err
}

func routeCmd(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if wantsHelp(args) {
		fmt.Fprintln(stderr, "usage: cutover route apply|list --plan FILE")
		return flag.ErrHelp
	}
	if len(args) == 0 || (args[0] != "apply" && args[0] != "list") {
		return usagef("route: want apply or list")
	}
	sub := args[0]
	fs := flag.NewFlagSet("route "+sub, flag.ContinueOnError)
	fs.SetOutput(stderr)
	planPath := fs.String("plan", "", "plan file")
	if err := parse(fs, args[1:]); err != nil {
		return err
	}
	if *planPath == "" {
		return usagef("route %s: --plan is required", sub)
	}
	p, err := plan.Load(*planPath)
	if err != nil {
		if isPathError(err) {
			return err
		}
		return usagef("%v", err)
	}
	c, err := ops.FrontDoorClient(p)
	if err != nil {
		return err
	}
	routes, err := c.Routes(ctx)
	if err != nil {
		return err
	}
	if sub == "list" {
		for _, r := range routes {
			fmt.Fprintf(stdout, "%-24s %s%s  source %s  target %s  weight %d%%\n", r.Key(), r.Host, r.Prefix, upstream(r.Source), upstream(r.Target), r.TargetWeight)
		}
		return nil
	}
	for _, s := range p.RoutedServices() {
		want, err := frontdoor.FromPlan(p, s, frontdoor.Desired{Source: true})
		if err != nil {
			return err
		}
		for _, r := range routes {
			if r.Key() == want.Key() && (r.Target != nil || r.TargetWeight != 0 || r.Mirror) {
				return fmt.Errorf("route %s is in use by a migration; it changes only through the migration", r.Key())
			}
		}
		if err := c.Apply(ctx, want); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "registered %s: %s%s -> %s\n", want.Key(), want.Host, want.Prefix, want.Source.String())
	}
	return nil
}

func upstream(u *frontdoor.Upstream) string {
	if u == nil {
		return "-"
	}
	return u.String()
}
