// Package secret resolves credential references. A reference says where a
// secret lives ("env:NAME" or "file:/abs/path"). Only the reference travels
// through workflow history, logs and API responses; the value is read inside
// the activity that needs it.
package secret

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// Ref is a credential reference.
type Ref string

var envName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Validate checks the reference's shape without reading the secret. Errors
// never echo the value, because a mistyped value may be the secret itself.
func (r Ref) Validate() error {
	kind, rest, ok := strings.Cut(string(r), ":")
	if !ok || rest == "" {
		return fmt.Errorf("secret reference: want env:NAME or file:/path")
	}
	switch kind {
	case "env":
		if !envName.MatchString(rest) {
			return fmt.Errorf("secret reference env: not a valid environment variable name")
		}
	case "file":
		if !filepath.IsAbs(rest) {
			return fmt.Errorf("secret reference file: must be an absolute path")
		}
	default:
		return fmt.Errorf("secret reference: unknown kind (want env:NAME or file:/path)")
	}
	return nil
}

// Resolve reads the secret. An unset or empty secret is an error.
func (r Ref) Resolve() (string, error) {
	if err := r.Validate(); err != nil {
		return "", err
	}
	kind, rest, _ := strings.Cut(string(r), ":")
	var v string
	if kind == "env" {
		v = os.Getenv(rest)
	} else {
		b, err := os.ReadFile(rest)
		if err != nil {
			return "", fmt.Errorf("secret reference %s: %w", r, err)
		}
		v = strings.TrimRight(string(b), "\r\n")
	}
	if v == "" {
		return "", fmt.Errorf("secret reference %s: empty or not set", r)
	}
	return v, nil
}
