package pg

import "testing"

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
	if got := literal("host=a password='x'y"); got != `'host=a password=''x''y'` {
		t.Fatal(got)
	}
}
