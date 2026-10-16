package redact

import (
	"errors"
	"fmt"
	"testing"
)

func TestDSN(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"postgres://app:s3cr%40t@db:5432/app", "postgres://app:xxxxx@db:5432/app"},
		{"postgresql://app:pw@db/app?sslmode=require", "postgresql://app:xxxxx@db/app?sslmode=require"},
		{"postgres://app@db/app", "postgres://app@db/app"},
		{"postgres://db/app?password=pw&sslmode=disable", "postgres://db/app?password=xxxxx&sslmode=disable"},
		{"host=db user=app password=pw dbname=app", "host=db user=app password=xxxxx dbname=app"},
		{"host=db password = 'p w\\'x' dbname=app", "host=db password=xxxxx dbname=app"},
		{"password=pw host=db", "password=xxxxx host=db"},
		{"dial postgres://u:pw@h/db: connection refused", "dial postgres://u:xxxxx@h/db: connection refused"},
		{`parse "password=pw host=db": bad`, `parse "password=xxxxx host=db": bad`},
		{"(password=pw)", "(password=xxxxx"},
		{"host=db sslpassword=keep", "host=db sslpassword=keep"},
		{"postgres://u:p@ss@h/db", "postgres://u:xxxxx@h/db"},
		{"host=db password='abc def", "host=db password=xxxxx"},
		{"no secrets here", "no secrets here"},
	} {
		if got := DSN(tc.in); got != tc.want {
			t.Errorf("DSN(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestSplit(t *testing.T) {
	for _, tc := range []struct{ in, clean, pw string }{
		{"postgres://app:s3cr%40t@db:5432/app?sslmode=disable", "postgres://app@db:5432/app?sslmode=disable", "s3cr@t"},
		{"postgres://db/app?password=pw&sslmode=disable", "postgres://db/app?sslmode=disable", "pw"},
		{"postgres://app@db/app", "postgres://app@db/app", ""},
		{"host=db user=app password=pw dbname=app", "host=db user=app dbname=app", "pw"},
		{"host=db password='p w\\'x' dbname=app", "host=db dbname=app", "p w'x"},
		{"postgres://u:p%40ss@h/db", "postgres://u@h/db", "p@ss"},
		{"postgres://u:p@ss@h/db", "postgres://u@h/db", "p@ss"},
	} {
		clean, pw := Split(tc.in)
		if clean != tc.clean || pw != tc.pw {
			t.Errorf("Split(%q) = %q, %q; want %q, %q", tc.in, clean, pw, tc.clean, tc.pw)
		}
	}
}

func TestErrIsFlatAndMasked(t *testing.T) {
	inner := errors.New("connect postgres://u:pw@h/db failed")
	err := Err(fmt.Errorf("discover: %w", inner))
	if err.Error() != "discover: connect postgres://u:xxxxx@h/db failed" {
		t.Fatalf("got %q", err.Error())
	}
	if errors.Unwrap(err) != nil {
		t.Fatal("Err must not unwrap: Temporal walks Unwrap chains into history")
	}
	if Err(nil) != nil {
		t.Fatal("Err(nil) must be nil")
	}
}
