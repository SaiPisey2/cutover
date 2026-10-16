package pg

import "testing"

func TestReplicable(t *testing.T) {
	for _, tc := range []struct {
		t    Table
		want bool
	}{
		{Table{ReplIdent: "d", PKCols: []string{"id"}}, true},
		{Table{ReplIdent: "d"}, false},
		{Table{ReplIdent: "f"}, true},
		{Table{ReplIdent: "i"}, true},
		{Table{ReplIdent: "n", PKCols: []string{"id"}}, false},
		{Table{ReplIdent: "d", PKCols: []string{"id"}, Partitioned: true}, false},
	} {
		if got := tc.t.Replicable(); got != tc.want {
			t.Errorf("%+v: got %v", tc.t, got)
		}
	}
}

func TestNames(t *testing.T) {
	tb := Table{Schema: "public", Name: "orders"}
	if tb.Qualified() != "public.orders" || tb.Ident() != `"public"."orders"` {
		t.Fatalf("%s %s", tb.Qualified(), tb.Ident())
	}
	if (Facts{VersionNum: 170004}).Major() != 17 {
		t.Fatal("major")
	}
}
