package pg

import (
	"reflect"
	"slices"
	"strings"
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

func TestCappedBuffer(t *testing.T) {
	b := &cappedBuffer{max: 4}
	if n, err := b.Write([]byte("abcd")); n != 4 || err != nil {
		t.Fatalf("within cap: %d %v", n, err)
	}
	if _, err := b.Write([]byte("e")); err == nil {
		t.Fatal("over cap must error")
	}
}

func TestChildEnvDropsPG(t *testing.T) {
	t.Setenv("PGSERVICE", "x")
	t.Setenv("PGOPTIONS", "y")
	for _, e := range childEnv("pw") {
		if strings.HasPrefix(e, "PGSERVICE") || strings.HasPrefix(e, "PGOPTIONS") {
			t.Fatalf("leaked %s", e)
		}
	}
	if got := childEnv("pw"); !slices.Contains(got, "PGPASSWORD=pw") {
		t.Fatal("no PGPASSWORD")
	}
}
