package sshx

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/SaiPisey2/cutover/internal/plan"
	"github.com/SaiPisey2/cutover/internal/secret"
)

func TestQuote(t *testing.T) {
	for _, s := range []string{"plain", "it's", `"$HOME"`, "a b; rm -rf /", "", "x\ny", `back\slash`, "$(id)"} {
		out, err := exec.Command("sh", "-c", "printf %s "+Quote(s)).Output()
		if err != nil || string(out) != s {
			t.Errorf("Quote(%q) round-trips as %q (%v)", s, out, err)
		}
	}
}

func TestRootWrapsInShell(t *testing.T) {
	c := &Client{sudo: true}
	if got := c.Root("echo hi"); got != `sudo -n sh -c 'echo hi'` {
		t.Fatal(got)
	}
	c.sudo = false
	if got := c.Root("echo hi"); got != `sh -c 'echo hi'` {
		t.Fatal(got)
	}
}

// A server that accepts the TCP connection and never speaks SSH must not
// hang Dial: the handshake is bounded by the context.
func TestDialHandshakeBoundedByContext(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			defer c.Close()
		}
	}()
	addr := ln.Addr().(*net.TCPAddr)

	dir := t.TempDir()
	_, priv, _ := ed25519.GenerateKey(rand.Reader)
	block, err := ssh.MarshalPrivateKey(priv, "")
	if err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(dir, "key")
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(block), 0o600); err != nil {
		t.Fatal(err)
	}
	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	k, _ := ssh.NewPublicKey(pub)
	kh := filepath.Join(dir, "known_hosts")
	line := "[127.0.0.1]:" + strconv.Itoa(addr.Port) + " " + string(ssh.MarshalAuthorizedKey(k))
	if err := os.WriteFile(kh, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	h := plan.Host{Address: "127.0.0.1", Port: addr.Port, User: "u", Key: secret.Ref("file:" + keyPath), KnownHosts: kh}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	began := time.Now()
	_, err = Dial(ctx, h)
	if err == nil {
		t.Fatal("Dial succeeded against a server that never speaks SSH")
	}
	if d := time.Since(began); d > 3*time.Second {
		t.Fatalf("Dial took %s", d)
	}
}
