package secret

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidate(t *testing.T) {
	for _, tc := range []struct {
		ref  Ref
		want string // "" means valid
	}{
		{"env:CUTOVER_DSN", ""},
		{"file:/etc/cutover/dsn", ""},
		{"", "want env:NAME or file:/path"},
		{"env:", "want env:NAME or file:/path"},
		{"env:1BAD", "not a valid environment variable name"},
		{"file:relative/dsn", "must be an absolute path"},
		{"vault:secret/x", "unknown kind"},
		{"postgres://u:p@h/db", "unknown kind"},
	} {
		err := tc.ref.Validate()
		if tc.want == "" {
			if err != nil {
				t.Errorf("%q: unexpected error %v", tc.ref, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%q: got %v, want error containing %q", tc.ref, err, tc.want)
		}
	}
}

func TestValidateNeverEchoesAnUnknownValue(t *testing.T) {
	err := Ref("postgres://u:hunter2@h/db").Validate()
	if err == nil || strings.Contains(err.Error(), "hunter2") {
		t.Fatalf("error must not echo the value: %v", err)
	}
}

func TestResolveEnv(t *testing.T) {
	t.Setenv("CUTOVER_TEST_SECRET", "postgres://a@b/c")
	got, err := Ref("env:CUTOVER_TEST_SECRET").Resolve()
	if err != nil || got != "postgres://a@b/c" {
		t.Fatalf("got %q, %v", got, err)
	}
	t.Setenv("CUTOVER_TEST_SECRET", "")
	if _, err := Ref("env:CUTOVER_TEST_SECRET").Resolve(); err == nil {
		t.Fatal("empty variable must be an error")
	}
	if _, err := Ref("env:CUTOVER_TEST_UNSET_VARIABLE").Resolve(); err == nil {
		t.Fatal("unset variable must be an error")
	}
}

func TestResolveFileTrimsTrailingNewline(t *testing.T) {
	p := filepath.Join(t.TempDir(), "dsn")
	if err := os.WriteFile(p, []byte("host=db password=x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := Ref("file:" + p).Resolve()
	if err != nil || got != "host=db password=x" {
		t.Fatalf("got %q, %v", got, err)
	}
	empty := filepath.Join(t.TempDir(), "empty")
	if err := os.WriteFile(empty, []byte("\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Ref("file:" + empty).Resolve(); err == nil {
		t.Fatal("empty file must be an error")
	}
}
