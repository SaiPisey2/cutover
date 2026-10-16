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
	"log/slog"
	"os"
	"strings"

	"go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"

	"github.com/SaiPisey2/cutover/internal/flow"
	"github.com/SaiPisey2/cutover/internal/plan"
)

const usage = `usage: cutover <command> [arguments] [flags]

commands:
  worker                                   run the Temporal worker
  migrate --plan FILE [--id ID]            validate a plan and start a migration
  status ID [--json]                       show phase, gate and recent events
  gate ID GATE approve|reject --as NAME [--note TEXT]
  rollback ID --as NAME --reason TEXT      roll back (any time before Finish)

flags for every command:
  --temporal HOST:PORT   Temporal frontend (default $CUTOVER_TEMPORAL or 127.0.0.1:7233)
  --namespace NS         Temporal namespace (default "default")
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
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.SetOutput(stderr)
	def := os.Getenv("CUTOVER_TEMPORAL")
	if def == "" {
		def = "127.0.0.1:7233"
	}
	hostPort := fs.String("temporal", def, "Temporal frontend")
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
		w := worker.New(c, flow.TaskQueue, worker.Options{})
		flow.Register(w)
		return w.Run(worker.InterruptCh())

	case "migrate":
		planPath := fs.String("plan", "", "plan file")
		id := fs.String("id", "", "migration ID (default: the plan's name)")
		if err := parse(fs, args); err != nil {
			return err
		}
		if *planPath == "" {
			return usagef("migrate: --plan is required")
		}
		p, err := plan.Load(*planPath)
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
		c, err := connect()
		if err != nil {
			return err
		}
		defer c.Close()
		wr, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: flow.WorkflowID(mid), TaskQueue: flow.TaskQueue},
			flow.Migration, flow.Input{MigrationID: mid, Plan: p})
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "started migration %s (workflow %s, run %s)\n", mid, wr.GetID(), wr.GetRunID())
		return nil

	case "status":
		pos, rest, err := positional(args, 1, "status: migration ID required")
		if err != nil {
			return err
		}
		asJSON := fs.Bool("json", false, "print the full status as JSON")
		if err := parse(fs, rest); err != nil {
			return err
		}
		c, err := connect()
		if err != nil {
			return err
		}
		defer c.Close()
		v, err := c.QueryWorkflow(ctx, flow.WorkflowID(pos[0]), "", flow.QueryStatus)
		if err != nil {
			return err
		}
		var st flow.Status
		if err := v.Get(&st); err != nil {
			return err
		}
		if *asJSON {
			enc := json.NewEncoder(stdout)
			enc.SetIndent("", "  ")
			return enc.Encode(st)
		}
		printStatus(stdout, st)
		return nil

	case "gate":
		pos, rest, err := positional(args, 3, "gate: want ID GATE approve or reject")
		if err != nil {
			return err
		}
		as := fs.String("as", "", "approver name")
		note := fs.String("note", "", "note recorded with the decision")
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
		c, err := connect()
		if err != nil {
			return err
		}
		defer c.Close()
		fmt.Fprintln(stderr, "note: --as is not authenticated in this release; anyone with Temporal access can approve")
		return c.SignalWorkflow(ctx, flow.WorkflowID(pos[0]), "", flow.SignalGate,
			flow.GateSignal{Gate: pos[1], Decision: d, ApproverID: *as, Note: *note})

	case "rollback":
		pos, rest, err := positional(args, 1, "rollback: migration ID required")
		if err != nil {
			return err
		}
		as := fs.String("as", "", "who is rolling back")
		reason := fs.String("reason", "", "why")
		if err := parse(fs, rest); err != nil {
			return err
		}
		if *as == "" {
			return usagef("rollback: --as is required")
		}
		if *reason == "" {
			return usagef("rollback: --reason is required")
		}
		c, err := connect()
		if err != nil {
			return err
		}
		defer c.Close()
		return c.SignalWorkflow(ctx, flow.WorkflowID(pos[0]), "", flow.SignalRollback,
			flow.RollbackSignal{ApproverID: *as, Reason: *reason})
	}
	return usagef("unknown command %q\n\n%s", cmd, usage)
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
