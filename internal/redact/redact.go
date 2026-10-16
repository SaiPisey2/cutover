// Package redact keeps database passwords out of logs, errors and history.
package redact

import (
	"net/url"
	"regexp"
	"strings"
)

const mask = "xxxxx"

var (
	urlUserinfo = regexp.MustCompile(`(?i)(postgres(?:ql)?://[^:/@\s]*:)[^@\s]*@`)
	urlQuery    = regexp.MustCompile(`(?i)([?&]password=)[^&\s]*`)
	keyword     = regexp.MustCompile(`(?i)(^|\s)password\s*=\s*('(?:[^'\\]|\\.)*'|[^\s']\S*)`)
)

// DSN returns s with every Postgres password it contains replaced by xxxxx.
// It handles URL userinfo, URL query parameters and keyword/value strings,
// and is safe to apply to whole error messages.
func DSN(s string) string {
	s = urlUserinfo.ReplaceAllString(s, "${1}"+mask+"@")
	s = urlQuery.ReplaceAllString(s, "${1}"+mask)
	return keyword.ReplaceAllString(s, "${1}password="+mask)
}

// Split returns dsn without its password, and the password, so a child
// process can get the password through PGPASSWORD instead of its argv.
func Split(dsn string) (clean, password string) {
	if strings.Contains(dsn, "://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return DSN(dsn), ""
		}
		if u.User != nil {
			if pw, ok := u.User.Password(); ok {
				password = pw
				u.User = url.User(u.User.Username())
			}
		}
		q := u.Query()
		if pw := q.Get("password"); pw != "" {
			password = pw
			q.Del("password")
			u.RawQuery = q.Encode()
		}
		return u.String(), password
	}
	m := keyword.FindStringSubmatchIndex(dsn)
	if m == nil {
		return dsn, ""
	}
	val := dsn[m[4]:m[5]]
	if strings.HasPrefix(val, "'") {
		val = strings.TrimSuffix(strings.TrimPrefix(val, "'"), "'")
		val = strings.NewReplacer(`\'`, `'`, `\\`, `\`).Replace(val)
	}
	clean = strings.Join(strings.Fields(dsn[:m[0]]+" "+dsn[m[1]:]), " ")
	return clean, val
}

type flat struct{ msg string }

func (f *flat) Error() string { return f.msg }

// Err returns an error with err's message masked. It deliberately does not
// unwrap: Temporal records the Unwrap chain in history, and the inner errors
// still hold the password.
func Err(err error) error {
	if err == nil {
		return nil
	}
	return &flat{msg: DSN(err.Error())}
}
