package pg

import (
	"errors"
	"strings"
	"testing"

	"github.com/SaiPisey2/cutover/internal/redact"
)

func TestObjectName(t *testing.T) {
	if got := ObjectName("shop-eu", "fwd"); got != "cutover_shop_eu_fwd" {
		t.Fatal(got)
	}
	long := "a234567890123456789012345678901234567890" // 40, the longest valid name
	if got := ObjectName(long, "rev"); len(got) > 63 {
		t.Fatalf("%d chars", len(got))
	}
}

func TestLiteral(t *testing.T) {
	if got := literal("host=a password='x'y"); got != `E'host=a password=''x''y'` {
		t.Fatal(got)
	}
}

func TestLiteralBackslash(t *testing.T) {
	if got := literal(`a\b'c`); got != `E'a\\b''c'` {
		t.Fatal(got)
	}
}

func TestSubscriptionErrorsMaskConninfo(t *testing.T) {
	err := redact.Err(errors.New(`could not connect: host=db user=app password=s3cr3t dbname=app`))
	if strings.Contains(err.Error(), "s3cr3t") {
		t.Fatal(err)
	}
}

func TestCanDetach(t *testing.T) {
	for _, tc := range []struct {
		msg  string
		want bool
	}{
		{`ERROR: could not drop replication slot "s" on publisher: ERROR:  replication slot "s" does not exist`, true},
		{`ERROR: could not connect to publisher when attempting to drop replication slot "s": connection refused`, true},
		{`ERROR: could not drop replication slot "s" on publisher: ERROR:  permission denied to use replication slots`, false},
		{`ERROR: could not drop replication slot "s" on publisher: ERROR:  replication slot "s" is active for PID 42`, false},
		{`ERROR: must be owner of subscription s`, false},
	} {
		if got := canDetach(errors.New(tc.msg)); got != tc.want {
			t.Errorf("canDetach(%q) = %v, want %v", tc.msg, got, tc.want)
		}
	}
}
