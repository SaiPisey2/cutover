//go:build integration

package itest

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"

	"github.com/SaiPisey2/cutover/internal/plan"
	"github.com/SaiPisey2/cutover/internal/secret"
)

const SSHPort = 52222

// composeExec runs a command in the sshd container with stdin.
func composeExec(t testing.TB, stdin []byte, args ...string) []byte {
	t.Helper()
	full := append([]string{"compose", "-f", filepath.Join(repoRoot(t), "test", "compose.yaml"), "exec", "-T", "sshd"}, args...)
	cmd := exec.Command("docker", full...)
	cmd.Stdin = bytes.NewReader(stdin)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v: %s (run: make itest-up)", strings.Join(full, " "), err, out)
	}
	return out
}

// hostKeyLine returns the sshd container's known_hosts line.
func hostKeyLine(t testing.TB) string {
	pub := strings.TrimSpace(string(composeExec(t, nil, "cat", "/etc/ssh/ssh_host_ed25519_key.pub")))
	f := strings.Fields(pub)
	return "[127.0.0.1]:52222 " + f[0] + " " + f[1] + "\n"
}

// SSHHost returns a plan host for user in the sshd container: a fresh
// ed25519 key (written to a 0600 file, referenced as file:), authorized
// for that user, and a known_hosts file holding the container's real key.
func SSHHost(t testing.TB, user string) plan.Host {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	block, err := ssh.MarshalPrivateKey(priv, "")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "id_ed25519")
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(block), 0o600); err != nil {
		t.Fatal(err)
	}
	sshPub, err := ssh.NewPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	composeExec(t, ssh.MarshalAuthorizedKey(sshPub), "sh", "-c",
		"cat >> /home/"+user+"/.ssh/authorized_keys && chown "+user+" /home/"+user+"/.ssh/authorized_keys && chmod 600 /home/"+user+"/.ssh/authorized_keys")
	kh := filepath.Join(dir, "known_hosts")
	if err := os.WriteFile(kh, []byte(hostKeyLine(t)), 0o600); err != nil {
		t.Fatal(err)
	}
	return plan.Host{Address: "127.0.0.1", Port: SSHPort, User: user, Key: secret.Ref("file:" + keyPath), KnownHosts: kh, Sudo: user == "cutover"}
}

// SSHKnownHostsWithWrongKey returns a known_hosts file that lists the
// sshd container's address with some other key.
func SSHKnownHostsWithWrongKey(t testing.TB) string {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	k, _ := ssh.NewPublicKey(pub)
	path := filepath.Join(t.TempDir(), "known_hosts")
	line := "[127.0.0.1]:52222 " + strings.TrimSpace(string(ssh.MarshalAuthorizedKey(k))) + "\n"
	if err := os.WriteFile(path, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
