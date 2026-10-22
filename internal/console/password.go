// Package console is the approver-facing console: accounts, sessions and
// the HTTP API behind the UI.
package console

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"regexp"
	"strings"

	"golang.org/x/crypto/argon2"
)

// NamePattern is the shape of an approver name.
var NamePattern = regexp.MustCompile("^[a-z][a-z0-9._-]{1,31}$")

const (
	minPassword = 12
	maxPassword = 256

	argonMemory  = 64 * 1024
	argonTime    = 3
	argonThreads = 4
	argonKeyLen  = 32
	argonSaltLen = 16
)

// hashSlots bounds concurrent argon2id work: each operation holds 64 MiB,
// so unbounded logins could exhaust memory.
var hashSlots = make(chan struct{}, 2)

func withSlot[T any](f func() T) T {
	hashSlots <- struct{}{}
	defer func() { <-hashSlots }()
	return f()
}

// ValidatePassword accepts 12 to 256 bytes.
func ValidatePassword(pw string) error {
	if len(pw) < minPassword || len(pw) > maxPassword {
		return fmt.Errorf("password must be %d to %d characters", minPassword, maxPassword)
	}
	return nil
}

// HashPassword returns an argon2id PHC string with a fresh random salt.
func HashPassword(pw string) (string, error) {
	salt := make([]byte, argonSaltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	key := withSlot(func() []byte {
		return argon2.IDKey([]byte(pw), salt, argonTime, argonMemory, argonThreads, argonKeyLen)
	})
	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s", argon2.Version,
		argonMemory, argonTime, argonThreads,
		base64.RawStdEncoding.EncodeToString(salt), base64.RawStdEncoding.EncodeToString(key)), nil
}

// CheckPassword reports whether pw matches the PHC string. Any parse
// problem is a mismatch.
func CheckPassword(phc, pw string) bool {
	parts := strings.Split(phc, "$")
	// "", argon2id, v=19, m=..,t=..,p=.., salt, key
	if len(parts) != 6 || parts[0] != "" || parts[1] != "argon2id" {
		return false
	}
	var v int
	if n, err := fmt.Sscanf(parts[2], "v=%d", &v); err != nil || n != 1 || v != argon2.Version || parts[2] != fmt.Sprintf("v=%d", v) {
		return false
	}
	var m, t uint32
	var p uint8
	if n, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &m, &t, &p); err != nil || n != 3 ||
		parts[3] != fmt.Sprintf("m=%d,t=%d,p=%d", m, t, p) {
		return false
	}
	// Refuse parameters that would let a stored hash demand unbounded work.
	if m == 0 || m > 1<<18 || t == 0 || t > 16 || p == 0 {
		return false
	}
	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil || len(salt) == 0 {
		return false
	}
	want, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil || len(want) == 0 || len(want) > 128 {
		return false
	}
	got := withSlot(func() []byte {
		return argon2.IDKey([]byte(pw), salt, t, m, p, uint32(len(want)))
	})
	return subtle.ConstantTimeCompare(got, want) == 1
}
