package route

import (
	"maps"
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	for in, want := range map[string]map[string]string{
		"host=pg-target port=5432 dbname=app": {"host": "pg-target", "port": "5432", "dbname": "app"},
		"  dbname=app \t host=10.0.0.7  ":     {"dbname": "app", "host": "10.0.0.7"},
		"host = a   port= 5434":               {"host": "a", "port": "5434"},
		"host='pg target' dbname='ap''p'":     {"host": "pg target", "dbname": "ap'p"},
		"host='' dbname=x":                    {"host": "", "dbname": "x"},
		"":                                    {},
	} {
		got, err := Parse(in)
		if err != nil || !maps.Equal(got, want) {
			t.Errorf("Parse(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
}

func TestParseRejects(t *testing.T) {
	for in, why := range map[string]string{
		"host":           "no =",
		"=a":             "empty key",
		"host=a host=b":  "duplicate",
		"host='a":        "unterminated quote",
		"host='a'b":      "junk after quote",
		`host='a\'b'`:    "backslash (PgBouncer does not take \\' escapes)",
		`dbname=a\ b`:    "backslash",
		"host= port=1":   "empty unquoted value",
		"host=a\nport=1": "newline",
		"host=a\x00":     "control character",
		"ho-st=a":        "bad key character",
	} {
		if got, err := Parse(in); err == nil {
			t.Errorf("Parse(%q) = %v, want error (%s)", in, got, why)
		}
	}
}

func TestResolve(t *testing.T) {
	got, err := Resolve("host=pg-target dbname=shop", "app")
	if err != nil || got != (Target{Host: "pg-target", Port: 5432, DBName: "shop"}) {
		t.Fatalf("%+v %v", got, err)
	}
	got, err = Resolve("host=b port=6000", "app")
	if err != nil || got != (Target{Host: "b", Port: 6000, DBName: "app"}) {
		t.Fatalf("defaults: %+v %v", got, err)
	}
	for _, in := range []string{"port=5432 dbname=app", "host='' dbname=app", "host=a port=0", "host=a port=65536", "host=a port=5432,5433", "host=a port=x", "host='a"} {
		if _, err := Resolve(in, "app"); err == nil {
			t.Errorf("Resolve(%q) should fail", in)
		}
	}
}

// No error may quote any part of the input: a token that looks like a key
// can be the tail of a secret, as "secret" is in password=my secret.
func TestErrorsDoNotEchoValues(t *testing.T) {
	for _, in := range []string{
		"host=a sslpassword='s3cret",
		"password=my s3cret",
		"host=a s3cret",
		"s3cret=a s3cret=b",
		"s3cret= port=1",
		"host='a's3cret",
		"host=a's3cret",
		"host=a=s3cret",
		"=s3cret",
		"host=a -s3cret",
	} {
		_, err := Parse(in)
		if err == nil {
			t.Errorf("Parse(%q) should fail", in)
			continue
		}
		if strings.Contains(err.Error(), "s3cret") || strings.Contains(err.Error(), "host") || strings.Contains(err.Error(), "password") {
			t.Errorf("Parse(%q) error echoes input: %v", in, err)
		}
	}
}

func TestContainsPassword(t *testing.T) {
	for in, want := range map[string]bool{
		"host=a password=x":      true,
		"host=a PassWord='x'":    true,
		"host=a sslpassword=x":   true,
		"host=a dbname=password": true,
		"host=a port=5432":       false,
	} {
		if got := ContainsPassword(in); got != want {
			t.Errorf("ContainsPassword(%q) = %v", in, got)
		}
	}
}

func TestResolveRejectsMultiHost(t *testing.T) {
	for _, in := range []string{"host=a,b port=5432", "host=a,b", "host='a, b'", "host=a, port=5432"} {
		_, err := Resolve(in, "app")
		if err == nil {
			t.Errorf("Resolve(%q) should fail", in)
		} else if in == "host=a,b port=5432" && !strings.Contains(err.Error(), "one host") {
			t.Errorf("unclear error: %v", err)
		}
	}
}

func TestSame(t *testing.T) {
	a := Target{Host: "db.example", Port: 5432, DBName: "app"}
	if !Same(a, Target{Host: "DB.Example", Port: 5432, DBName: "app"}) {
		t.Error("host case should not matter")
	}
	for _, b := range []Target{{Host: "db2", Port: 5432, DBName: "app"}, {Host: "db.example", Port: 5433, DBName: "app"}, {Host: "db.example", Port: 5432, DBName: "App"}} {
		if Same(a, b) {
			t.Errorf("%+v reported same", b)
		}
	}
}
