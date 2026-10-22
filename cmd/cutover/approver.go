package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
	"unicode"

	"golang.org/x/term"

	"github.com/SaiPisey2/cutover/internal/console"
	"github.com/SaiPisey2/cutover/internal/store"
)

// stdin, isTerminal and readPassword are package vars so tests never touch
// the real terminal. The password prompts of other commands reuse them.
var (
	stdin        io.Reader = os.Stdin
	isTerminal             = func() bool { return term.IsTerminal(int(os.Stdin.Fd())) }
	readPassword           = func() ([]byte, error) { return term.ReadPassword(int(os.Stdin.Fd())) }
)

const approverUsage = `usage: cutover approver new --data DIR --name NAME [--password-stdin]
       cutover approver list --data DIR
       cutover approver disable --data DIR NAME
`

// openData creates the data directory private to the user and opens the
// console database in it.
func openData(dir string) (*store.Store, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return nil, err
	}
	return store.Open(dir + "/cutover.db")
}

func approverCmd(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return usagef("approver: want new, list or disable\n\n%s", approverUsage)
	}
	if args[0] == "-h" || args[0] == "-help" || args[0] == "--help" {
		fmt.Fprint(stderr, approverUsage)
		return flag.ErrHelp
	}
	sub, rest := args[0], args[1:]
	fs := flag.NewFlagSet("approver "+sub, flag.ContinueOnError)
	fs.SetOutput(stderr)
	data := fs.String("data", "", "data directory holding cutover.db")
	switch sub {
	case "new":
		name := fs.String("name", "", "account name")
		pwStdin := fs.Bool("password-stdin", false, "read the password from stdin instead of the terminal")
		if err := parse(fs, rest); err != nil {
			return err
		}
		if fs.NArg() != 0 {
			return usagef("approver new: unexpected argument %q", fs.Arg(0))
		}
		if *data == "" {
			return usagef("approver new: --data is required")
		}
		if !console.NamePattern.MatchString(*name) {
			return usagef("approver new: --name must match %s", console.NamePattern)
		}
		pw, err := newPassword(*pwStdin, stderr)
		if err != nil {
			return err
		}
		if err := console.ValidatePassword(pw); err != nil {
			return usagef("approver new: %v", err)
		}
		return approverNew(ctx, *data, *name, pw, stdout)

	case "list":
		if err := parse(fs, rest); err != nil {
			return err
		}
		if *data == "" || fs.NArg() != 0 {
			return usagef("approver list: want --data DIR and no other arguments")
		}
		return approverList(ctx, *data, stdout)

	case "disable":
		if wantsHelp(rest) {
			return parse(fs, []string{"-h"})
		}
		// NAME may come before or after the flags.
		var name string
		if len(rest) > 0 && !strings.HasPrefix(rest[0], "-") {
			name, rest = rest[0], rest[1:]
		}
		if err := parse(fs, rest); err != nil {
			return err
		}
		if name == "" && fs.NArg() == 1 {
			name = fs.Arg(0)
		} else if fs.NArg() != 0 {
			return usagef("approver disable: want one NAME")
		}
		if *data == "" || name == "" {
			return usagef("approver disable: want NAME and --data DIR")
		}
		return approverDisable(ctx, *data, name, stdout)
	}
	return usagef("approver: unknown command %q\n\n%s", sub, approverUsage)
}

// newPassword reads the new account's password: twice from the terminal
// without echo, or one line from stdin with --password-stdin.
func newPassword(fromStdin bool, stderr io.Writer) (string, error) {
	if fromStdin {
		line, err := bufio.NewReaderSize(io.LimitReader(stdin, 1024), 1024).ReadString('\n')
		if err != nil && !(errors.Is(err, io.EOF) && line != "") {
			return "", usagef("approver new: no password on stdin")
		}
		return strings.TrimRight(line, "\r\n"), nil
	}
	if !isTerminal() {
		return "", usagef("no terminal; use --password-stdin")
	}
	fmt.Fprint(stderr, "password: ")
	a, err := readPassword()
	fmt.Fprintln(stderr)
	if err != nil {
		return "", err
	}
	fmt.Fprint(stderr, "again: ")
	b, err := readPassword()
	fmt.Fprintln(stderr)
	if err != nil {
		return "", err
	}
	if string(a) != string(b) {
		return "", usagef("approver new: the passwords differ")
	}
	return string(a), nil
}

func approverNew(ctx context.Context, dir, name, pw string, stdout io.Writer) error {
	hash, err := console.HashPassword(pw)
	if err != nil {
		return err
	}
	st, err := openData(dir)
	if err != nil {
		return err
	}
	defer st.Close()
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return err
	}
	now := time.Now().UTC()
	a := store.Approver{ID: hex.EncodeToString(b), Name: name, PasswordHash: hash, Created: now}
	if err := st.CreateApprover(ctx, a); err != nil {
		if errors.Is(err, store.ErrConflict) {
			return fmt.Errorf("an approver named %q already exists; disable it first", name)
		}
		return err
	}
	full := console.ApproverID(name, a.ID)
	if _, err := st.AppendAudit(ctx, store.AuditRow{At: now, ActorID: "cli", ActorName: "cli", Kind: "approver-created", Note: full}); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "created approver %s (ID %s)\n", name, full)
	return nil
}

func approverList(ctx context.Context, dir string, stdout io.Writer) error {
	st, err := openData(dir)
	if err != nil {
		return err
	}
	defer st.Close()
	l, err := st.ListApprovers(ctx)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tID\tCREATED\tSTATE")
	for _, a := range l {
		state := "active"
		if a.Disabled != nil {
			state = "disabled"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", printable(a.Name), printable(console.ApproverID(a.Name, a.ID)), a.Created.Format(time.RFC3339), state)
	}
	return tw.Flush()
}

func approverDisable(ctx context.Context, dir, name string, stdout io.Writer) error {
	st, err := openData(dir)
	if err != nil {
		return err
	}
	defer st.Close()
	a, err := st.ApproverByName(ctx, name)
	if err == nil {
		err = st.DisableApprover(ctx, name, time.Now().UTC())
	}
	if errors.Is(err, store.ErrNotFound) {
		return fmt.Errorf("no active approver named %q", printable(name))
	}
	if err != nil {
		return err
	}
	if _, err := st.AppendAudit(ctx, store.AuditRow{At: time.Now().UTC(), ActorID: "cli", ActorName: "cli", Kind: "approver-disabled", Note: console.ApproverID(a.Name, a.ID)}); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "disabled approver %s\n", printable(console.ApproverID(a.Name, a.ID)))
	return nil
}

// printable quotes a value holding a non-printable rune, so a name that
// reached the store by another path cannot repaint the operator's terminal.
func printable(v string) string {
	for _, r := range v {
		if !unicode.IsPrint(r) || unicode.Is(unicode.Cf, r) {
			return strconv.Quote(v)
		}
	}
	return v
}
