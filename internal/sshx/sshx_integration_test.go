//go:build integration

package sshx_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/SaiPisey2/cutover/internal/itest"
	"github.com/SaiPisey2/cutover/internal/secret"
	"github.com/SaiPisey2/cutover/internal/sshx"
)

func dial(t *testing.T, user string) *sshx.Client {
	t.Helper()
	c, err := sshx.Dial(context.Background(), itest.SSHHost(t, user))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestRunAndQuote(t *testing.T) {
	c := dial(t, "cutover")
	weird := `it's "$HOME"; rm -rf / $(id)`
	out, err := c.Run(context.Background(), "printf %s "+sshx.Quote(weird), nil)
	if err != nil || string(out) != weird {
		t.Fatalf("%q %v", out, err)
	}
	out, err = c.Run(context.Background(), "cat", []byte("from stdin"))
	if err != nil || string(out) != "from stdin" {
		t.Fatalf("stdin: %q %v", out, err)
	}
}

func TestRunErrorOmitsStdin(t *testing.T) {
	c := dial(t, "cutover")
	_, err := c.Run(context.Background(), "cat >/dev/null; echo oops >&2; exit 3", []byte("TOPSECRET-VALUE"))
	var ee *sshx.ExitError
	if !errors.As(err, &ee) || ee.Code != 3 || !strings.Contains(err.Error(), "oops") {
		t.Fatalf("got %v", err)
	}
	if strings.Contains(err.Error(), "TOPSECRET") {
		t.Fatal("stdin leaked into the error")
	}
}

func TestWriteReadFile(t *testing.T) {
	c := dial(t, "cutover")
	ctx := context.Background()
	p := "/srv/cutover-test/sub/file.txt"
	data := []byte("line one\nline 'two'\n")
	if err := c.WriteFile(ctx, p, 0o640, data); err != nil {
		t.Fatal(err)
	}
	got, err := c.ReadFile(ctx, p)
	if err != nil || string(got) != string(data) {
		t.Fatalf("%q %v", got, err)
	}
	mode, err := c.Run(ctx, c.Root("stat -c %a "+sshx.Quote(p)), nil)
	if err != nil || strings.TrimSpace(string(mode)) != "640" {
		t.Fatalf("mode %q %v", mode, err)
	}
	left, _ := c.Run(ctx, c.Root("ls -a /srv/cutover-test/sub"), nil)
	if strings.Contains(string(left), ".cutover-") {
		t.Fatalf("temp file left behind: %s", left)
	}
	ok, err := c.Exists(ctx, p)
	if err != nil || !ok {
		t.Fatalf("exists %v %v", ok, err)
	}
	ok, err = c.Exists(ctx, p+".nope")
	if err != nil || ok {
		t.Fatalf("missing file reported present: %v %v", ok, err)
	}
}

func TestWithoutSudoCannotWriteAsRoot(t *testing.T) {
	c := dial(t, "nosudo")
	if err := c.WriteFile(context.Background(), "/etc/cutover-nosudo/x", 0o644, []byte("x")); err == nil {
		t.Fatal("a user without sudo must not be able to write under /etc")
	}
}

func TestWrongHostKeyRefused(t *testing.T) {
	h := itest.SSHHost(t, "cutover")
	h.KnownHosts = itest.SSHKnownHostsWithWrongKey(t)
	_, err := sshx.Dial(context.Background(), h)
	if err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("got %v", err)
	}
}

func TestHostMissingFromKnownHosts(t *testing.T) {
	h := itest.SSHHost(t, "cutover")
	empty := filepath.Join(t.TempDir(), "known_hosts")
	os.WriteFile(empty, nil, 0o600)
	h.KnownHosts = empty
	_, err := sshx.Dial(context.Background(), h)
	if err == nil || !strings.Contains(err.Error(), "is not in") {
		t.Fatalf("got %v", err)
	}
}

func TestPassphraseKeyRefusedWithoutEcho(t *testing.T) {
	h := itest.SSHHost(t, "cutover")
	_, priv, _ := ed25519.GenerateKey(rand.Reader)
	block, err := ssh.MarshalPrivateKeyWithPassphrase(priv, "", []byte("pw"))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "key")
	pemBytes := pem.EncodeToMemory(block)
	os.WriteFile(path, pemBytes, 0o600)
	h.Key = secret.Ref("file:" + path)
	_, err = sshx.Dial(context.Background(), h)
	if err == nil || !strings.Contains(err.Error(), "passphrase") || strings.Contains(err.Error(), "OPENSSH") {
		t.Fatalf("got %v", err)
	}
}

func TestCancelStopsCommand(t *testing.T) {
	c := dial(t, "cutover")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	began := time.Now()
	_, err := c.Run(ctx, "sleep 30", nil)
	if err == nil || time.Since(began) > 5*time.Second {
		t.Fatalf("cancelled command: %v after %s", err, time.Since(began))
	}
}
