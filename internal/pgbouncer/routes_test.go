package pgbouncer

import (
	"os"
	"path/filepath"
	"testing"
)

func TestHostOf(t *testing.T) {
	for in, want := range map[string]string{
		"host=pg-target port=5432 dbname=app": "pg-target",
		"dbname=app  host=10.0.0.7":           "10.0.0.7",
		"dbname=app":                          "",
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
	want := "; managed by cutover; do not edit while a migration runs\n[databases]\napp = host=b port=5432 dbname=app\n"
	if string(b) != want {
		t.Fatalf("got %q", b)
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
}
