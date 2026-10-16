package pg

import "testing"

func TestOrderBy(t *testing.T) {
	cols := []string{"a", "n"}
	if got, want := orderBy(cols, []bool{true, false}, false), `"a" COLLATE "C", "n"`; got != want {
		t.Fatalf("got %s want %s", got, want)
	}
	if got, want := orderBy(cols, []bool{true, false}, true), `"a" COLLATE "C" DESC, "n" DESC`; got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}
