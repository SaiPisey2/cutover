package pgbouncer

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/SaiPisey2/cutover/internal/route"
)

func TestHostOf(t *testing.T) {
	for in, want := range map[string]string{
		"host=pg-target port=5432 dbname=app": "pg-target",
		"dbname=app  host=10.0.0.7":           "10.0.0.7",
		"dbname=app":                          "",
		"host='a":                             "",
	} {
		if got := HostOf(in); got != want {
			t.Errorf("HostOf(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestWriteRouteIsAtomicAndComplete(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "routes.ini")
	if err := WriteRoute(p, "app", "host=b port=5432 dbname=app"); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != RoutesFileSize {
		t.Fatalf("size %d, want %d", len(b), RoutesFileSize)
	}
	lines := strings.Split(string(b), "\n")
	if len(lines) != 4 || lines[0] != "[databases]" || lines[1] != "app = host=b port=5432 dbname=app" || lines[3] != "" {
		t.Fatalf("layout: %q", lines)
	}
	if !strings.HasPrefix(lines[2], "; managed by cutover; do not edit while a migration runs") {
		t.Fatalf("comment: %q", lines[2])
	}
	st, _ := os.Stat(p)
	if st.Mode().Perm() != 0o644 {
		t.Fatalf("mode %v", st.Mode())
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatalf("temp file left behind: %v", entries)
	}
	if err := WriteRoute(p, "app", "host=b\npassword=x"); err == nil {
		t.Fatal("newlines must be rejected")
	}
	// Every file is the same size, whatever the route.
	if err := WriteRoute(p, "app", "host=a-much-longer-host-name.example.internal port=6432 dbname=app"); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(p); len(b) != RoutesFileSize {
		t.Fatalf("size %d", len(b))
	}
}

func TestWriteRouteRejectsWithoutWriting(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "routes.ini")
	if err := WriteRoute(p, "app", "host=a"); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(p)
	for name, c := range map[string][2]string{
		"too long":    {"app", "host=" + strings.Repeat("h", RoutesFileSize)},
		"just over":   {"app", "host=" + strings.Repeat("h", RoutesFileSize-len("[databases]\napp = host=\n")-len(managedComment)-1+1)},
		"bad db name": {"App; x", "host=a"},
		"bad route":   {"app", "host='a"},
	} {
		if err := WriteRoute(p, c[0], c[1]); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
	after, _ := os.ReadFile(p)
	if string(after) != string(before) {
		t.Fatal("a rejected route changed the file")
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatalf("temp file left behind: %v", entries)
	}
	// The longest route that fits is accepted.
	fit := "host=" + strings.Repeat("h", RoutesFileSize-len("[databases]\napp = host=\n")-len(managedComment)-1)
	if err := WriteRoute(p, "app", fit); err != nil {
		t.Fatalf("longest fitting route: %v", err)
	}
	if b, _ := os.ReadFile(p); len(b) != RoutesFileSize {
		t.Fatalf("size %d", len(b))
	}
}

// Rows shaped like SHOW DATABASES output.
func showDatabases() []map[string]string {
	return []map[string]string{
		{"name": "app", "host": "pg-target", "port": "5432", "database": "app", "paused": "0", "disabled": "0", "pool_size": "20"},
		{"name": "pgbouncer", "port": "6432", "database": "pgbouncer", "paused": "0"},
	}
}

func TestCheckRoute(t *testing.T) {
	dbs := parseDatabases(showDatabases())
	if dbs[0] != (Database{Name: "app", Host: "pg-target", Port: 5432, DB: "app"}) {
		t.Fatalf("%+v", dbs[0])
	}
	ok := []route.Target{{Host: "pg-target", Port: 5432, DBName: "app"}}
	for _, w := range ok {
		if err := checkRoute(dbs, "app", w); err != nil {
			t.Errorf("%+v: %v", w, err)
		}
	}
	for _, w := range []route.Target{
		{Host: "pg-source", Port: 5432, DBName: "app"},
		{Host: "pg-target", Port: 5433, DBName: "app"},
		{Host: "pg-target", Port: 5432, DBName: "shop"},
		{Host: "PG-TARGET", Port: 5432, DBName: "app"},
	} {
		if err := checkRoute(dbs, "app", w); err == nil {
			t.Errorf("%+v: mismatch not detected", w)
		}
	}
	if err := checkRoute(dbs, "nope", ok[0]); !errors.Is(err, ErrNoDatabase) {
		t.Errorf("missing: %v", err)
	}
	// An entry PgBouncer shows with an empty host never matches.
	empty := parseDatabases([]map[string]string{{"name": "app", "port": "5432", "database": "app"}})
	if err := checkRoute(empty, "app", route.Target{Port: 5432, DBName: "app"}); err == nil {
		t.Error("empty host accepted")
	}
}

// Name checks run before the connection is touched, so a nil one is fine.
func TestAdminRejectsBadNames(t *testing.T) {
	a := &Admin{}
	ctx := context.Background()
	for _, name := range []string{"", "App", "app; RESUME", "app pgbouncer", "1app", strings.Repeat("a", 64)} {
		if err := a.Pause(ctx, name); err == nil {
			t.Errorf("Pause(%q)", name)
		}
		if err := a.Resume(ctx, name); err == nil {
			t.Errorf("Resume(%q)", name)
		}
		if err := a.Reconnect(ctx, name); err == nil {
			t.Errorf("Reconnect(%q)", name)
		}
		if _, err := a.Database(ctx, name); err == nil {
			t.Errorf("Database(%q)", name)
		}
		if err := Repoint(ctx, a, filepath.Join(t.TempDir(), "r.ini"), name, "host=a"); err == nil {
			t.Errorf("Repoint(%q)", name)
		}
	}
}
