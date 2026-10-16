package pgbouncer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// HostOf returns the host= value of a keyword/value connection string.
func HostOf(connstr string) string {
	for _, f := range strings.Fields(connstr) {
		if v, ok := strings.CutPrefix(f, "host="); ok {
			return v
		}
	}
	return ""
}

// WriteRoute atomically replaces the routes file pgbouncer.ini %includes.
// cutover owns that file; it holds one [databases] entry.
func WriteRoute(path, db, connstr string) error {
	if strings.ContainsAny(db+connstr, "\r\n") {
		return fmt.Errorf("routes: newline in database or connection string")
	}
	body := "; managed by cutover; do not edit while a migration runs\n[databases]\n" + db + " = " + connstr + "\n"
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
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("routes: %w", err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("routes: %w", err)
	}
	return nil
}

// Repoint routes db to connstr: write the file, reload, confirm PgBouncer
// sees the new host, and recycle server connections.
func Repoint(ctx context.Context, a *Admin, path, db, connstr string) error {
	if err := WriteRoute(path, db, connstr); err != nil {
		return err
	}
	if err := a.Reload(ctx); err != nil {
		return err
	}
	d, err := a.Database(ctx, db)
	if err != nil {
		return err
	}
	if want := HostOf(connstr); d.Host != want {
		return fmt.Errorf("repoint %s: pgbouncer reports host %q after reload, want %q (is %s the file pgbouncer.ini includes?)", db, d.Host, want, path)
	}
	return a.Reconnect(ctx, db)
}
