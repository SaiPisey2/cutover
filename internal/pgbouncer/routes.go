package pgbouncer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/SaiPisey2/cutover/internal/route"
)

// RoutesFileSize is the exact size of every routes file WriteRoute writes.
// A fixed size means a reader that sees the new file with the old file's
// size still reads all of it. Docker Desktop's file share does exactly that
// shortly after a replace, and a truncated file makes RELOAD drop the
// database and disconnect its clients.
const RoutesFileSize = 1024

const managedComment = "; managed by cutover; do not edit while a migration runs"

// HostOf returns the host= value of a keyword/value connection string, or
// "" when it has none or does not parse.
func HostOf(connstr string) string {
	kv, err := route.Parse(connstr)
	if err != nil {
		return ""
	}
	return kv["host"]
}

// WriteRoute atomically replaces the routes file pgbouncer.ini %includes.
// cutover owns that file; it holds one [databases] entry on line 2, then a
// comment padded so the file is exactly RoutesFileSize bytes. A route that
// does not fit, or does not parse, is refused and the file is left alone.
func WriteRoute(path, db, connstr string) error {
	if err := checkName(db); err != nil {
		return err
	}
	if strings.ContainsAny(connstr, "\r\n") {
		return fmt.Errorf("routes: newline in connection string")
	}
	if _, err := route.Resolve(connstr, db); err != nil {
		return fmt.Errorf("routes: %w", err)
	}
	head := "[databases]\n" + db + " = " + connstr + "\n" + managedComment
	pad := RoutesFileSize - len(head) - 1
	if pad < 0 {
		return fmt.Errorf("routes: route for %s is %d bytes too long for the %d-byte routes file", db, -pad, RoutesFileSize)
	}
	body := head + strings.Repeat(" ", pad) + "\n"

	tmp, err := os.CreateTemp(filepath.Dir(path), ".routes-*")
	if err != nil {
		return fmt.Errorf("routes: %w", err)
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString(body); err != nil {
		tmp.Close()
		return fmt.Errorf("routes: %w", err)
	}
	if err := tmp.Chmod(0o644); err != nil {
		tmp.Close()
		return fmt.Errorf("routes: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("routes: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("routes: %w", err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("routes: %w", err)
	}
	return nil
}

// Repoint routes db to connstr: write the file, reload, confirm PgBouncer
// reports the new host, port and database, then recycle server connections.
// Any mismatch fails before RECONNECT, so connections are never recycled
// onto a route PgBouncer did not take (RELOAD keeps the old config when it
// cannot read the new file).
//
// RECONNECT is a safeguard: PgBouncer 1.24 already recycles server
// connections when RELOAD changes a database's host, but versions or
// changes where RELOAD does not would leave clients on the old database.
func Repoint(ctx context.Context, a *Admin, path, db, connstr string) error {
	if err := checkName(db); err != nil {
		return err
	}
	want, err := route.Resolve(connstr, db)
	if err != nil {
		return fmt.Errorf("repoint %s: %w", db, err)
	}
	if err := WriteRoute(path, db, connstr); err != nil {
		return err
	}
	if err := a.Reload(ctx); err != nil {
		return err
	}
	dbs, err := a.Databases(ctx)
	if err != nil {
		return err
	}
	if err := checkRoute(dbs, db, want); err != nil {
		return fmt.Errorf("%w (is %s the file pgbouncer.ini includes?)", err, path)
	}
	return a.Reconnect(ctx, db)
}

// checkRoute compares what SHOW DATABASES reports for db with the route's
// target. Hosts compare exactly: PgBouncer reports the string it was given.
func checkRoute(dbs []Database, db string, want route.Target) error {
	d, err := find(dbs, db)
	if err != nil {
		return fmt.Errorf("repoint %s: %w", db, err)
	}
	if d.Host == "" || d.Host != want.Host || d.Port != want.Port || d.DB != want.DBName {
		return fmt.Errorf("repoint %s: pgbouncer reports host=%q port=%d dbname=%q after reload, want host=%q port=%d dbname=%q",
			db, d.Host, d.Port, d.DB, want.Host, want.Port, want.DBName)
	}
	return nil
}
