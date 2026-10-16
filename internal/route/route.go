// Package route parses the key=value connection strings that PgBouncer's
// [databases] section takes, the way PgBouncer itself reads them.
//
// PgBouncer 1.24 is not libpq here. In a quoted value a doubled quote is
// one quote, and there are no backslash escapes:
//
//	dbname='ap''p'    is ap'p
//	dbname='ap\'p'    makes PgBouncer reject the whole include file
//
// An unquoted backslash is an error too. Parse therefore rejects every
// backslash, so a route means the same thing to cutover and to PgBouncer.
package route

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Target is the database a route points at, with PgBouncer's defaults filled in.
type Target struct {
	Host   string
	Port   int
	DBName string
}

// DefaultPort is the port PgBouncer uses when a route sets none.
const DefaultPort = 5432

// Parse splits a route into its keys and values. Errors give a byte
// position and the kind of problem, never any part of the input: a token
// that looks like a key can be the tail of a secret (password=my secret).
func Parse(s string) (map[string]string, error) {
	out := map[string]string{}
	i := 0
	skip := func() {
		for i < len(s) && (s[i] == ' ' || s[i] == '\t') {
			i++
		}
	}
	for _, r := range s {
		switch {
		case r == '\\':
			return nil, errors.New("route: backslashes are not allowed")
		case r < 0x20 && r != '\t', r == 0x7f:
			return nil, errors.New("route: control characters are not allowed")
		}
	}
	for {
		skip()
		if i == len(s) {
			return out, nil
		}
		start := i
		for i < len(s) && isKeyChar(s[i]) {
			i++
		}
		key, kstart := s[start:i], start
		if key == "" {
			return nil, fmt.Errorf("route: expected a key at byte %d", start)
		}
		skip()
		if i == len(s) || s[i] != '=' {
			return nil, fmt.Errorf("route: the key at byte %d has no =", start)
		}
		i++
		skip()
		var val strings.Builder
		vstart := i
		if i < len(s) && s[i] == '\'' {
			i++
			closed := false
			for i < len(s) {
				if s[i] == '\'' {
					if i+1 < len(s) && s[i+1] == '\'' {
						val.WriteByte('\'')
						i += 2
						continue
					}
					i++
					closed = true
					break
				}
				val.WriteByte(s[i])
				i++
			}
			if !closed {
				return nil, fmt.Errorf("route: unterminated quote in the value at byte %d", vstart)
			}
			if i < len(s) && s[i] != ' ' && s[i] != '\t' {
				return nil, fmt.Errorf("route: text after a closing quote at byte %d", i)
			}
		} else {
			start := i
			for i < len(s) && s[i] != ' ' && s[i] != '\t' {
				if s[i] == '\'' || s[i] == '=' {
					return nil, fmt.Errorf("route: unexpected quote or = inside an unquoted value at byte %d", i)
				}
				i++
			}
			if i == start {
				return nil, fmt.Errorf("route: empty value at byte %d (an empty value must be written '')", start)
			}
			val.WriteString(s[start:i])
		}
		if _, dup := out[key]; dup {
			return nil, fmt.Errorf("route: the key at byte %d is set twice", kstart)
		}
		out[key] = val.String()
	}
}

func isKeyChar(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
}

// Resolve parses a route and applies PgBouncer's defaults: port 5432, and
// dbname equal to the [databases] entry name (defaultDB). host is required.
func Resolve(s, defaultDB string) (Target, error) {
	kv, err := Parse(s)
	if err != nil {
		return Target{}, err
	}
	t := Target{Host: kv["host"], Port: DefaultPort, DBName: defaultDB}
	if t.Host == "" {
		return Target{}, errors.New("route: host is required")
	}
	if strings.Contains(t.Host, ",") {
		return Target{}, errors.New("route: host must name one host; multi-host routes are not supported in release 1")
	}
	if p, ok := kv["port"]; ok {
		n, err := strconv.Atoi(p)
		if err != nil || n < 1 || n > 65535 {
			return Target{}, errors.New("route: port must be a single number from 1 to 65535")
		}
		t.Port = n
	}
	if d, ok := kv["dbname"]; ok {
		t.DBName = d
	}
	return t, nil
}

// ContainsPassword reports whether a route mentions a password in any
// spelling (password=, sslpassword=, or the word in a value). Routes are
// written to PgBouncer's config and shown to operators; PgBouncer
// authenticates on its own, so a route never needs one.
func ContainsPassword(s string) bool {
	return strings.Contains(strings.ToLower(s), "password")
}

// Same reports whether two targets are the same database. Host names
// compare without case, as DNS does.
func Same(a, b Target) bool {
	return strings.EqualFold(a.Host, b.Host) && a.Port == b.Port && a.DBName == b.DBName
}
