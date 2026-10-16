package pg

import (
	"reflect"
	"testing"
)

func TestDiff(t *testing.T) {
	src := Schema{
		"public.orders": {{Name: "id", Type: "bigint", NotNull: true}, {Name: "note", Type: "text"}},
		"public.items":  {{Name: "sku", Type: "text", NotNull: true}},
	}
	dst := Schema{
		"public.orders": {{Name: "extra", Type: "int"}, {Name: "id", Type: "bigint", NotNull: true}, {Name: "note", Type: "character varying(10)"}},
	}
	want := []string{
		"public.items: missing on target",
		"public.orders.extra: only on target",
		"public.orders.note: text on source, character varying(10) on target",
	}
	if got := Diff(src, dst); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q\nwant %q", got, want)
	}
	if got := Diff(src, src); len(got) != 0 {
		t.Fatalf("identical schemas differ: %q", got)
	}
}
