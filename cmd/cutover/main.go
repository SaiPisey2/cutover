// Command cutover moves a Postgres database between clouds with zero failed
// queries: replicate, rehearse, switch writes under a bounded pause with two
// approvers, keep reverse replication for rollback.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	fs2 "io/fs"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"

	"github.com/SaiPisey2/cutover/internal/console"
	"github.com/SaiPisey2/cutover/internal/flow"
	"github.com/SaiPisey2/cutover/internal/plan"
)

const usage = `usage: cutover <command> [arguments] [flags]

commands:
  worker                                   run the Temporal worker
  migrate --plan FILE [--id ID] --as NAME  validate a plan and start a migration
  status ID [--json]                       show phase, gate and recent events
  gate ID GATE approve|reject --as NAME [--note TEXT]
  rollback ID --as NAME --reason TEXT      roll back (any time before Finish)
  server --state FILE --token-file FILE    run the front door's control plane (xDS + API),
         [--data DIR]                      and with --data the console
  envoy-bootstrap                          print the Envoy bootstrap for the front door
  route apply|list --plan FILE             register the plan's routes at 100% source, or list routes
  approver new|list|disable --data DIR     manage console approver accounts

positional arguments come before flags (ID, GATE and the decision are positional).

flags for worker and status:
  --temporal HOST:PORT   Temporal frontend (default $CUTOVER_TEMPORAL or 127.0.0.1:7233)
  --namespace NS         Temporal namespace (default "default")

flags for migrate, gate and rollback, which go through the console:
  --server URL           console (default $CUTOVER_SERVER or https://127.0.0.1:7700)
  --ca FILE              CA certificate to trust (default $CUTOVER_CA, or ~/.cutover/ca.crt if it exists)
  --as NAME              your approver account
  --password-stdin       read the password from stdin instead of the terminal
`

var dial = func(hostPort, namespace string, stderr io.Writer) (client.Client, error) {
	logger := sdklog.NewStructuredLogger(slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	return client.Dial(client.Options{HostPort: hostPort, Namespace: namespace, Logger: logger})
}

type usageError struct{ msg string }

func (u *usageError) Error() string { return u.msg }

func usagef(format string, a ...any) error { return &usageError{msg: fmt.Sprintf(format, a...)} }

// parse returns flag.ErrHelp unwrapped, so -h exits 0 after the FlagSet has
// printed its flags, and wraps every other parse error as a usage error.
func parse(fs *flag.FlagSet, args []string) error {
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return err
		}
		return usagef("%v", err)
	}
	return nil
}

func main() { os.Exit(run(context.Background(), os.Args[1:], os.Stdout, os.Stderr)) }

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	switch args[0] {
	case "-h", "--help", "help", "-help":
		fmt.Fprint(stderr, usage)
		return 0
	}
	err := dispatch(ctx, args[0], args[1:], stdout, stderr)
	var ue *usageError
	switch {
	case err == nil:
		return 0
	case errors.Is(err, flag.ErrHelp):
		return 0
	case errors.As(err, &ue):
		fmt.Fprintf(stderr, "cutover: %s\n", ue.msg)
		return 2
	default:
		fmt.Fprintf(stderr, "cutover: %v\n", err)
		return 1
	}
}

func isPathError(err error) bool {
	var pe *fs2.PathError
	return errors.As(err, &pe)
}

func wantsHelp(args []string) bool {
	for _, a := range args {
		if a == "-h" || a == "-help" || a == "--help" {
			return true
		}
	}
	return false
}

// positional takes n leading arguments that are not flags.
func positional(args []string, n int, what string) ([]string, []string, error) {
	if len(args) < n {
		return nil, nil, usagef("%s", what)
	}
	for _, a := range args[:n] {
		if strings.HasPrefix(a, "-") {
			return nil, nil, usagef("%s", what)
		}
	}
	return args[:n], args[n:], nil
}

func dispatch(ctx context.Context, cmd string, args []string, stdout, stderr io.Writer) error {
	switch cmd {
	case "server":
		return serverCmd(ctx, args, stderr)
	case "envoy-bootstrap":
		return bootstrapCmd(args, stdout, stderr)
	case "route":
		return routeCmd(ctx, args, stdout, stderr)
	case "approver":
		return approverCmd(ctx, args, stdout, stderr)
	}
	switch cmd {
	case "migrate", "gate", "rollback":
		return consoleCmd(ctx, cmd, args, stdout, stderr)
	}
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.SetOutput(stderr)
	hostPort := fs.String("temporal", envOr("CUTOVER_TEMPORAL", "127.0.0.1:7233"), "Temporal frontend")
	ns := fs.String("namespace", "default", "Temporal namespace")
	connect := func() (client.Client, error) { return dial(*hostPort, *ns, stderr) }

	switch cmd {
	case "worker":
		if err := parse(fs, args); err != nil {
			return err
		}
		c, err := connect()
		if err != nil {
			return err
		}
		defer c.Close()
		w := worker.New(c, flow.TaskQueue, flow.WorkerOptions())
		flow.Register(w)
		return w.Run(worker.InterruptCh())

	case "status":
		asJSON := fs.Bool("json", false, "print the full status as JSON")
		if wantsHelp(args) {
			return parse(fs, []string{"-h"})
		}
		pos, rest, err := positional(args, 1, "status: migration ID required")
		if err != nil {
			return err
		}
		if err := parse(fs, rest); err != nil {
			return err
		}
		c, err := connect()
		if err != nil {
			return err
		}
		defer c.Close()
		st, err := queryStatus(ctx, c, pos[0])
		if err != nil {
			return err
		}
		if *asJSON {
			enc := json.NewEncoder(stdout)
			enc.SetIndent("", "  ")
			return enc.Encode(st)
		}
		printStatus(stdout, st)
		return nil
	}
	return usagef("unknown command %q\n\n%s", cmd, usage)
}

// envOr is the environment variable's value, or def when it is unset or
// empty.
func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

// consoleCmd runs migrate, gate and rollback through the console. Every
// local check -- the plan, the IDs, the decision, the flags, the server
// URL and the CA -- comes before the password prompt and any request.
func consoleCmd(ctx context.Context, cmd string, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.SetOutput(stderr)
	server := fs.String("server", envOr("CUTOVER_SERVER", "https://127.0.0.1:7700"), "console URL")
	caPath := fs.String("ca", os.Getenv("CUTOVER_CA"), "CA certificate to trust (default ~/.cutover/ca.crt if it exists)")
	pwStdin := fs.Bool("password-stdin", false, "read the password from stdin instead of the terminal")

	var act func(context.Context, *console.Client) error
	var as *string
	switch cmd {
	case "migrate":
		planPath := fs.String("plan", "", "plan file")
		id := fs.String("id", "", "migration ID (default: the plan's name)")
		as = fs.String("as", "", "your approver account")
		if err := parse(fs, args); err != nil {
			return err
		}
		if *planPath == "" {
			return usagef("migrate: --plan is required")
		}
		// Read once: the plan checked here is the plan sent.
		raw, err := os.ReadFile(*planPath)
		if err != nil {
			return err
		}
		p, err := plan.Parse(raw)
		if err != nil {
			return usagef("%v", err)
		}
		mid := p.Name
		if *id != "" {
			mid = *id
		}
		if !plan.NamePattern.MatchString(mid) {
			return usagef("migrate: --id must match %s", plan.NamePattern)
		}
		if *as == "" {
			return usagef("migrate: --as is required")
		}
		act = func(ctx context.Context, c *console.Client) error {
			got, err := c.Start(ctx, mid, raw)
			if err != nil {
				return err
			}
			fmt.Fprintf(stdout, "started migration %s\n", got)
			return nil
		}

	case "gate":
		as = fs.String("as", "", "your approver account")
		note := fs.String("note", "", "note recorded with the decision")
		if wantsHelp(args) {
			return parse(fs, []string{"-h"})
		}
		pos, rest, err := positional(args, 3, "gate: want ID GATE approve or reject")
		if err != nil {
			return err
		}
		if err := parse(fs, rest); err != nil {
			return err
		}
		d := flow.Decision(pos[2])
		if d != flow.Approve && d != flow.Reject {
			return usagef("gate: decision must be approve or reject")
		}
		if *as == "" {
			return usagef("gate: --as is required")
		}
		if !plan.NamePattern.MatchString(pos[0]) {
			return usagef("gate: migration ID must match %s", plan.NamePattern)
		}
		act = func(ctx context.Context, c *console.Client) error {
			if err := c.Decide(ctx, pos[0], pos[1], d, *note); err != nil {
				return err
			}
			fmt.Fprintf(stdout, "%s recorded at gate %s of %s\n", d, pos[1], pos[0])
			return nil
		}

	case "rollback":
		as = fs.String("as", "", "your approver account")
		reason := fs.String("reason", "", "why")
		if wantsHelp(args) {
			return parse(fs, []string{"-h"})
		}
		pos, rest, err := positional(args, 1, "rollback: migration ID required")
		if err != nil {
			return err
		}
		if err := parse(fs, rest); err != nil {
			return err
		}
		if *as == "" {
			return usagef("rollback: --as is required")
		}
		if *reason == "" {
			return usagef("rollback: --reason is required")
		}
		if !plan.NamePattern.MatchString(pos[0]) {
			return usagef("rollback: migration ID must match %s", plan.NamePattern)
		}
		act = func(ctx context.Context, c *console.Client) error {
			if err := c.Rollback(ctx, pos[0], *reason); err != nil {
				return err
			}
			fmt.Fprintf(stdout, "rollback of %s requested\n", pos[0])
			return nil
		}
	}

	if u, err := url.Parse(*server); err != nil || u.Scheme != "https" || u.Host == "" {
		return usagef("%s: --server %q must be an https URL like https://127.0.0.1:7700", cmd, *server)
	}
	// Named in a read error, so the operator knows which setting to fix.
	var caSource string
	switch {
	case *caPath == "":
	case explicit(fs, "ca"):
		caSource = "--ca " + *caPath
	default:
		caSource = "CUTOVER_CA=" + *caPath
	}
	caPEM, err := readCA(*caPath, caSource)
	if err != nil {
		return err
	}
	c, err := console.NewClient(*server, caPEM)
	if err != nil {
		return usagef("%s: %v", cmd, err)
	}
	pw, err := loginPassword(*pwStdin, *as, cmd, stderr)
	if err != nil {
		return err
	}
	err = c.Login(ctx, *as, pw)
	if err == nil {
		err = act(ctx, c)
	}
	var ae *console.APIError
	if err != nil && !errors.As(err, &ae) {
		// The console never answered: say what to check.
		return fmt.Errorf("%w\nis \"cutover server --data DIR\" running at %s, and does --ca point at DIR/ca.crt?", err, *server)
	}
	return err
}

// explicit reports whether the flag was given on the command line.
func explicit(fs *flag.FlagSet, name string) bool {
	found := false
	fs.Visit(func(f *flag.Flag) { found = found || f.Name == name })
	return found
}

// readCA reads the CA certificate the CLI trusts: path, else
// ~/.cutover/ca.crt if it exists. No CA at all is a usage error; one that
// cannot be read is not. source names
// where path came from, for a read error.
func readCA(path, source string) ([]byte, error) {
	if path == "" {
		home, err := os.UserHomeDir()
		if err == nil {
			def := filepath.Join(home, ".cutover", "ca.crt")
			if _, err := os.Stat(def); err == nil {
				path = def
			}
		}
	}
	if path == "" {
		return nil, usagef("no CA certificate: pass --ca FILE or set CUTOVER_CA (the server writes DATA/ca.crt), or copy it to ~/.cutover/ca.crt")
	}
	b, err := os.ReadFile(path)
	if err != nil && source != "" {
		return nil, fmt.Errorf("%s: %w", source, err)
	}
	return b, err
}

// loginPassword reads the account's password: once from the terminal
// without echo, or one line from stdin with --password-stdin.
func loginPassword(fromStdin bool, name, cmd string, stderr io.Writer) (string, error) {
	if fromStdin {
		pw, ok := stdinLine()
		if !ok || pw == "" {
			return "", usagef("%s: no password on stdin", cmd)
		}
		return pw, nil
	}
	if !isTerminal() {
		return "", usagef("no terminal; use --password-stdin")
	}
	fmt.Fprintf(stderr, "password for %s: ", name)
	b, err := readPassword()
	fmt.Fprintln(stderr)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func queryStatus(ctx context.Context, c client.Client, id string) (flow.Status, error) {
	var st flow.Status
	v, err := c.QueryWorkflow(ctx, flow.WorkflowID(id), "", flow.QueryStatus)
	if err != nil {
		return st, err
	}
	return st, v.Get(&st)
}

func printStatus(w io.Writer, st flow.Status) {
	fmt.Fprintf(w, "migration %s: phase %s, %s, writes on the %s\n", st.MigrationID, st.Phase, st.State, st.Primary)
	if g := st.Gate; g != nil {
		fmt.Fprintf(w, "gate %s (%d of %d approvals", g.Name, len(g.Approvals), g.Needs)
		if len(g.Approvals) > 0 {
			fmt.Fprintf(w, ": %s", strings.Join(g.Approvals, ", "))
		}
		fmt.Fprintf(w, "): %s\n", g.Summary)
	}
	if st.Weight > 0 || len(st.Traffic) > 0 {
		fmt.Fprintf(w, "traffic: %d%% on the target\n", st.Weight)
		for _, t := range st.Traffic {
			fmt.Fprintf(w, "  %s: source %s; target %s\n", t.Service, flow.Describe(t.Source), flow.Describe(t.Target))
		}
	}
	if st.ReplicationBroken {
		fmt.Fprintln(w, "REPLICATION BROKEN: replication is unsafe in both directions; the only way on is rollback")
	}
	if len(st.PendingUndo) > 0 {
		fmt.Fprintf(w, "unwind halted, pending undo: %s\n", strings.Join(st.PendingUndo, ", "))
	}
	if r := st.Readiness; r != nil {
		for _, f := range r.Findings {
			if f.Severity != "info" {
				fmt.Fprintf(w, "  %-8s %s\n", f.Severity, f.Message)
			}
		}
	}
	evs := st.Events
	if len(evs) > 10 {
		evs = evs[len(evs)-10:]
	}
	fmt.Fprintln(w, "recent events:")
	for _, e := range evs {
		fmt.Fprintf(w, "  %s  %-9s %-18s %-8s %s\n", e.At.UTC().Format("2006-01-02T15:04:05Z"), e.Phase, e.Kind, e.Actor, e.Message)
	}
}
