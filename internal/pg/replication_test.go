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
