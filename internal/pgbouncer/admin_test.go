package pgbouncer

import (
	"context"
	"strings"
	"testing"
)

// pgconn echoes a connection string it cannot parse, masking only up to the
// first space; Dial must say nothing about it.
func TestDialMistypedDSNLeaksNothing(t *testing.T) {
	for _, m := range []struct{ dsn, fragment string }{
		{"host=h user=u password=hun ter2 dbname=pgbouncer", "ter2"},
		{"host=h user=u password='abc def dbname=pgbouncer", "def"},
		{"postgres://u:p@ss w@h/pgbouncer", "ss w"},
	} {
		_, err := Dial(context.Background(), m.dsn)
		if err == nil || strings.Contains(err.Error(), m.fragment) || strings.Contains(err.Error(), "password") {
			t.Fatalf("%q: %v", m.dsn, err)
		}
	}
}
