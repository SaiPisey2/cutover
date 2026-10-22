package console

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPasswordRoundTrip(t *testing.T) {
	h1, err := HashPassword("correct horse battery")
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(h1, "$argon2id$v=19$m=65536,t=3,p=4$"), h1)
	require.True(t, CheckPassword(h1, "correct horse battery"))
	require.False(t, CheckPassword(h1, "wrong horse battery"))
	h2, err := HashPassword("correct horse battery")
	require.NoError(t, err)
	require.NotEqual(t, h1, h2)
}

func TestCheckPasswordRejectsMalformed(t *testing.T) {
	for _, phc := range []string{"", "x", "$argon2id$v=19$m=65536,t=3,p=4$AAAA", "$argon2i$v=19$m=65536,t=3,p=4$AAAA$AAAA",
		"$argon2id$v=19$m=999999999,t=3,p=4$AAAA$AAAA",
		"$argon2id$v=19$m=262145,t=3,p=4$AAAA$AAAA", "$argon2id$v=19$m=65536,t=3,p=4xyz$AAAA$AAAA", "$argon2id$v=19$m=65536,t=3,p=04$AAAA$AAAA", "$argon2id$v=19x$m=65536,t=3,p=4$AAAA$AAAA", "$argon2id$v=19$m=65536,t=3,p=4$!!$AAAA"} {
		require.False(t, CheckPassword(phc, "whatever pass word"), phc)
	}
}

func TestValidatePassword(t *testing.T) {
	require.Error(t, ValidatePassword(strings.Repeat("a", 11)))
	require.NoError(t, ValidatePassword(strings.Repeat("a", 12)))
	require.NoError(t, ValidatePassword(strings.Repeat("a", 256)))
	require.Error(t, ValidatePassword(strings.Repeat("a", 257)))
}

func TestCheckPasswordParamsMustBeExact(t *testing.T) {
	h, err := HashPassword("correct horse battery")
	require.NoError(t, err)
	require.True(t, CheckPassword(h, "correct horse battery"))
	for _, bad := range []string{"p=4xyz$", "p=04$"} {
		mut := strings.Replace(h, "p=4$", bad, 1)
		require.False(t, CheckPassword(mut, "correct horse battery"), mut)
	}
	require.False(t, CheckPassword(strings.Replace(h, "v=19", "v=19x", 1), "correct horse battery"))
}
