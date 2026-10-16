//go:build integration

package itest

import (
	"context"
	"testing"
)

func TestHarness(t *testing.T) {
	ctx := context.Background()
	Reset(t)
	for dsn, want := range map[string]string{SourceDSN: "source", TargetDSN: "target", PoolDSN: "source"} {
		c := Conn(t, dsn)
		var name, wal string
		if err := c.QueryRow(ctx, "SELECT current_setting('cluster_name'), current_setting('wal_level')").Scan(&name, &wal); err != nil {
			t.Fatal(err)
		}
		if name != want || wal != "logical" {
			t.Errorf("%s: cluster %q wal %q, want %q logical", dsn, name, wal, want)
		}
	}
	p := Plan(t)
	if err := p.Validate(); err != nil {
		t.Fatalf("itest plan invalid: %v", err)
	}
}
