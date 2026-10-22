package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"

	"github.com/SaiPisey2/cutover/internal/console"
	"github.com/SaiPisey2/cutover/internal/frontdoor"
	"github.com/SaiPisey2/cutover/internal/ops"
	"github.com/SaiPisey2/cutover/internal/plan"
	"github.com/SaiPisey2/cutover/internal/store"
	"github.com/SaiPisey2/cutover/internal/tlsutil"
	"github.com/SaiPisey2/cutover/ui"
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
	data := fs.String("data", "", "data directory (approvers, audit log, TLS); enables the console")
	consoleAddr := fs.String("console", "127.0.0.1:7700", "console address (loopback; needs --data)")
	tlsHosts := fs.String("tls-hosts", "127.0.0.1,localhost", "names and IPs the console's certificate covers (needs --data)")
	temporalAddr := fs.String("temporal", envOr("CUTOVER_TEMPORAL", "127.0.0.1:7233"), "Temporal frontend the console uses")
	namespace := fs.String("namespace", "default", "Temporal namespace the console uses")
	accessLog := fs.String("access-log", "", "Envoy access log of failed requests (default: access.log beside --state; needs --data)")
	if err := parse(fs, args); err != nil {
		return err
	}
	switch {
	case *state == "":
		return usagef("server: --state is required")
	case *tokenPath == "":
		return usagef("server: --token-file is required")
	}
	var hosts []string
	if *data == "" {
		// Without --data the server is exactly the v0.2.0 front door.
		var stray string
		fs.Visit(func(f *flag.Flag) {
			if stray == "" && (f.Name == "console" || f.Name == "tls-hosts" || f.Name == "access-log") {
				stray = f.Name
			}
		})
		if stray != "" {
			return usagef("server: --%s needs --data (the console is off without it)", stray)
		}
	} else {
		if _, _, err := net.SplitHostPort(*consoleAddr); err != nil {
			return usagef("server: --console %q must be host:port", *consoleAddr)
		}
		if frontdoor.RequireLoopback(*consoleAddr) != nil {
			return usagef("server: --console %s is not a loopback address; the console listens on loopback only (reach it from elsewhere through an SSH tunnel)", *consoleAddr)
		}
		for _, h := range strings.Split(*tlsHosts, ",") {
			h = strings.TrimSpace(h)
			if err := checkTLSHost(h); err != nil {
				return usagef("server: --tls-hosts: %v", err)
			}
			hosts = append(hosts, h)
		}
		if *accessLog == "" {
			*accessLog = filepath.Join(filepath.Dir(*state), "access.log")
		}
		// Envoy opens the path from its own working directory.
		abs, err := filepath.Abs(*accessLog)
		if err != nil {
			return err
		}
		*accessLog = abs
	}
	if err := frontdoor.RequireLoopback(*apiAddr); err != nil {
		return usagef("server: --api: %v", err)
	}
	if !strings.HasPrefix(*xdsAddr, "unix:") {
		if err := frontdoor.RequireLoopback(*xdsAddr); err != nil {
			return usagef("server: --xds: %v", err)
		}
	}
	if err := checkAdmin(*admin); err != nil {
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
	var con *consoleServer
	listener := frontdoor.Listener{Address: lh, Port: port}
	if *data != "" {
		con, err = newConsole(*data, hosts, *temporalAddr, *namespace, *accessLog, stderr)
		if err != nil {
			return err
		}
		defer con.close()
		listener.AccessLog = *accessLog
	}
	x, err := frontdoor.NewXDS(frontdoor.XDSConfig{Listener: listener,
		Store: &frontdoor.Store{Path: *state}, Drain: *drain})
	if err != nil {
		return err
	}
	// Every listener is bound before any serves.
	xl, err := frontdoor.ListenXDS(*xdsAddr)
	if err != nil {
		x.Stop()
		return err
	}
	al, err := frontdoor.ListenAPI(*apiAddr)
	if err != nil {
		xl.Close()
		x.Stop()
		return err
	}
	var cl net.Listener
	if con != nil {
		if cl, err = net.Listen("tcp", *consoleAddr); err != nil {
			al.Close()
			xl.Close()
			x.Stop()
			return err
		}
	}
	api := &frontdoor.API{XDS: x, Admin: &frontdoor.Admin{Base: strings.TrimSuffix(*admin, "/"), HTTP: &http.Client{Timeout: 10 * time.Second}}, Token: token}
	hs := &http.Server{Handler: api.Handler(), ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 4 * time.Minute}

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	errs := make(chan error, 3)
	go func() { errs <- x.Serve(xl) }()
	go func() { errs <- hs.Serve(al) }()
	fmt.Fprintf(stderr, "cutover server: xds on %s, api on %s, envoy listener %s\n", *xdsAddr, *apiAddr, *listen)
	if con != nil {
		con.start(ctx)
		go func() { errs <- con.http.ServeTLS(cl, "", "") }()
		fmt.Fprintf(stderr, "console: https://%s (CA %s)\n", cl.Addr(), con.caPath)
	}
	select {
	case <-ctx.Done():
	case err := <-errs:
		x.Stop()
		_ = hs.Close()
		if con != nil {
			con.stop(nil)
		}
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			err = errors.New("a server stopped before it was asked to")
		}
		return err
	}
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if con != nil {
		con.stop(sctx)
	}
	_ = hs.Shutdown(sctx)
	x.Stop()
	return nil
}

// checkTLSHost accepts an IP address or an RFC 1123 DNS name.
func checkTLSHost(h string) error {
	if net.ParseIP(h) != nil {
		return nil
	}
	if h == "" || len(h) > 253 {
		return fmt.Errorf("%q is not an IP address or DNS name", h)
	}
	for _, label := range strings.Split(h, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return fmt.Errorf("%q is not an IP address or DNS name", h)
		}
		for _, r := range label {
			if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-') {
				return fmt.Errorf("%q is not an IP address or DNS name", h)
			}
		}
	}
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
	// Check every service before applying any, so a busy route leaves
	// the front door untouched.
	var wants []frontdoor.Route
	for _, s := range p.RoutedServices() {
		want, err := frontdoor.FromPlan(p, s, frontdoor.Desired{Source: true})
		if err != nil {
			return err
		}
		for _, r := range routes {
			if r.Key() == want.Key() && !r.Finished() && (r.Target != nil || r.TargetWeight != 0 || r.Mirror) {
				return fmt.Errorf("route %s is in use by a migration; it changes only through the migration", r.Key())
			}
		}
		wants = append(wants, want)
	}
	for _, want := range wants {
		if err := c.Apply(ctx, want); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "registered %s: %s%s -> %s\n", want.Key(), want.Host, want.Prefix, want.Source.String())
	}
	return nil
}

// checkAdmin requires an http(s) URL with an explicit loopback host:port.
func checkAdmin(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("%q must be a URL like http://127.0.0.1:9901", raw)
	}
	if u.Port() == "" {
		return fmt.Errorf("%q needs an explicit port, like http://127.0.0.1:9901", raw)
	}
	return frontdoor.RequireLoopback(u.Host)
}

func upstream(u *frontdoor.Upstream) string {
	if u == nil {
		return "-"
	}
	return u.String()
}

// consoleServer is the console listener and what it runs on: the store,
// a lazy Temporal client and the access-log follower.
type consoleServer struct {
	st       *store.Store
	tc       client.Client
	failures *frontdoor.Failures
	http     *http.Server
	caPath   string
	log      *slog.Logger

	// hubCancel ends the live streams; baseCancel ends every request's
	// context; runCancel stops the access-log follower.
	hubCancel, baseCancel, runCancel context.CancelFunc
	runDone                          chan struct{}
}

// accessLogRestart is how long a failed access-log follower waits before
// it starts again.
const accessLogRestart = 5 * time.Second

// newConsole opens the store, loads or creates the TLS pair, writes the CA
// for clients to <data>/ca.crt and makes a Temporal client that dials on
// first use, so a Temporal that is down never blocks startup (the API
// answers 503 until it is back).
func newConsole(data string, hosts []string, temporalAddr, namespace, accessLog string, stderr io.Writer) (*consoleServer, error) {
	log := slog.New(slog.NewTextHandler(stderr, nil))
	st, err := openData(data)
	if err != nil {
		return nil, err
	}
	cert, caPEM, err := tlsutil.LoadOrCreate(filepath.Join(data, "tls"), hosts)
	if err != nil {
		st.Close()
		return nil, err
	}
	caPath := filepath.Join(data, "ca.crt")
	if err := writePublic(caPath, caPEM); err != nil {
		st.Close()
		return nil, err
	}
	tc, err := client.NewLazyClient(client.Options{HostPort: temporalAddr, Namespace: namespace,
		Logger: sdklog.NewStructuredLogger(slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))})
	if err != nil {
		st.Close()
		return nil, err
	}
	c := &consoleServer{st: st, tc: tc, failures: frontdoor.NewFailures(accessLog, 50), caPath: caPath, log: log}
	hubCtx, hubCancel := context.WithCancel(context.Background())
	baseCtx, baseCancel := context.WithCancel(context.Background())
	c.hubCancel, c.baseCancel = hubCancel, baseCancel
	auth := console.NewAuth(st, log)
	mux := http.NewServeMux()
	console.Routes(mux, auth, console.Deps{Temporal: console.NewTemporal(tc), Store: st, Failures: c.failures, Log: log, Context: hubCtx})
	c.http = &http.Server{
		Handler:           console.NewServer(auth, mux, ui.FS()),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return baseCtx },
		TLSConfig:         &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}},
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}
	return c, nil
}

// start follows the access log under ctx until stop.
func (c *consoleServer) start(ctx context.Context) {
	ctx, c.runCancel = context.WithCancel(ctx)
	c.runDone = make(chan struct{})
	go func() {
		defer close(c.runDone)
		keepRunning(ctx, c.failures.Run, accessLogRestart, func(err error) {
			c.log.Error("console: access log follower stopped; restarting", "err", err, "in", accessLogRestart)
		})
	}()
}

// stop ends the live streams first -- Shutdown would otherwise wait on
// them until sctx ran out -- then shuts the listener down, closing it at
// once when sctx is nil or runs out.
func (c *consoleServer) stop(sctx context.Context) {
	c.hubCancel()
	if sctx == nil || c.http.Shutdown(sctx) != nil {
		_ = c.http.Close()
	}
	c.baseCancel()
	if c.runCancel != nil {
		c.runCancel()
		<-c.runDone
	}
}

func (c *consoleServer) close() {
	c.hubCancel()
	c.baseCancel()
	c.tc.Close()
	c.st.Close()
}

// keepRunning runs run until ctx ends, starting it again `every` after an
// error: a follower that hits a bad file must not take the server down.
func keepRunning(ctx context.Context, run func(context.Context) error, every time.Duration, logErr func(error)) {
	for {
		err := run(ctx)
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			err = errors.New("returned without an error")
		}
		logErr(err)
		t := time.NewTimer(every)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}

// writePublic replaces path with data, readable by all, atomically.
func writePublic(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+"-*")
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Chmod(0o644)
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(f.Name(), path)
	}
	if err != nil {
		os.Remove(f.Name())
	}
	return err
}
