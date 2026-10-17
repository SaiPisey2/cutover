//go:build integration

package pgbouncer_test

import (
	"context"
	"testing"

	"github.com/SaiPisey2/cutover/internal/itest"
	"github.com/SaiPisey2/cutover/internal/pgbouncer"
	"github.com/SaiPisey2/cutover/internal/sshx"
)

// The sshd container mounts PgBouncer's routes directory at the same
// path PgBouncer reads it: writing it over SSH is what a worker on
// another host does.
func TestRepointOverSSH(t *testing.T) {
	itest.Reset(t)
	ctx := context.Background()
	c, err := sshx.Dial(ctx, itest.SSHHost(t, "cutover"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	a, err := pgbouncer.Dial(ctx, itest.AdminDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(ctx)
	if err := pgbouncer.RepointWith(ctx, a, pgbouncer.SSHWriter(c), "/etc/pgbouncer/routes/routes.ini", "app", itest.TargetRoute); err != nil {
		t.Fatal(err)
	}
	d, err := a.Database(ctx, "app")
	if err != nil || d.Host != "pg-target" {
		t.Fatalf("after an SSH repoint pgbouncer routes app to %+v (%v)", d, err)
	}
	size, err := c.Run(ctx, c.Root("stat -c %s /etc/pgbouncer/routes/routes.ini"), nil)
	if err != nil || string(size) != "1024\n" {
		t.Fatalf("routes file size %q %v; it must stay exactly RoutesFileSize", size, err)
	}
	// Put the harness back the way Reset expects it.
	if err := pgbouncer.Repoint(ctx, a, itest.RoutesFile(t), "app", itest.SourceRoute); err != nil {
		t.Fatal(err)
	}
}
