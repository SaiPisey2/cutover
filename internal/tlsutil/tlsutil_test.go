package tlsutil

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func verify(t *testing.T, caPEM []byte, leaf []byte, host string) error {
	t.Helper()
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatal("bad CA PEM")
	}
	c, err := x509.ParseCertificate(leaf)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Verify(x509.VerifyOptions{Roots: pool, DNSName: host})
	return err
}

func TestCreatesACAAndACertForTheHosts(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "tls")
	cert, caPEM, err := LoadOrCreate(dir, []string{"127.0.0.1", "localhost"})
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range []string{"127.0.0.1", "localhost"} {
		if err := verify(t, caPEM, cert.Certificate[0], h); err != nil {
			t.Errorf("%s: %v", h, err)
		}
	}
	if err := verify(t, caPEM, cert.Certificate[0], "evil.example"); err == nil {
		t.Error("certificate verified for a host it was not issued for")
	}
}

func TestKeysAreOwnerOnly(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "tls")
	if _, _, err := LoadOrCreate(dir, []string{"127.0.0.1"}); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"ca.key", "server.key"} {
		st, err := os.Stat(filepath.Join(dir, f))
		if err != nil {
			t.Fatal(err)
		}
		if st.Mode().Perm() != 0o600 {
			t.Errorf("%s mode = %v, want 0600", f, st.Mode().Perm())
		}
	}
	st, _ := os.Stat(dir)
	if st.Mode().Perm() != 0o700 {
		t.Errorf("dir mode = %v, want 0700", st.Mode().Perm())
	}
}

// Every client handed the CA trusts it. Regenerating it on restart
// would break every client at once.
func TestSecondCallKeepsTheCA(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "tls")
	_, a, err := LoadOrCreate(dir, []string{"127.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	_, b, err := LoadOrCreate(dir, []string{"127.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Error("CA changed between calls")
	}
}

func TestANewHostReissuesTheServerCertUnderTheSameCA(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "tls")
	_, ca1, err := LoadOrCreate(dir, []string{"127.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	cert, ca2, err := LoadOrCreate(dir, []string{"127.0.0.1", "gate.internal"})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ca1, ca2) {
		t.Error("CA changed")
	}
	if err := verify(t, ca2, cert.Certificate[0], "gate.internal"); err != nil {
		t.Errorf("new host not covered: %v", err)
	}
}

// On a first run `serve` and `session new` can both find no CA and create
// one. Two CAs meant clients trusted one while the server presented a
// certificate from the other, and clients failed TLS with nothing to say
// why.
func TestConcurrentFirstRunsAgreeOnOneCA(t *testing.T) {
	const callers = 8
	for round := 0; round < 20; round++ {
		dir := filepath.Join(t.TempDir(), "tls")
		type result struct {
			cert tls.Certificate
			ca   []byte
			err  error
		}
		res := make(chan result, callers)
		var start sync.WaitGroup
		start.Add(1)
		for i := 0; i < callers; i++ {
			go func() {
				start.Wait()
				c, ca, err := LoadOrCreate(dir, []string{"127.0.0.1"})
				res <- result{c, ca, err}
			}()
		}
		start.Done()
		var first []byte
		for i := 0; i < callers; i++ {
			r := <-res
			if r.err != nil {
				t.Fatalf("round %d: %v", round, r.err)
			}
			if first == nil {
				first = r.ca
			} else if !bytes.Equal(first, r.ca) {
				t.Fatalf("round %d: two callers returned different CAs", round)
			}
			if err := verify(t, r.ca, r.cert.Certificate[0], "127.0.0.1"); err != nil {
				t.Fatalf("round %d: serving certificate not issued by the CA returned with it: %v", round, err)
			}
		}
		onDisk, err := os.ReadFile(filepath.Join(dir, "ca.crt"))
		if err != nil || !bytes.Equal(onDisk, first) {
			t.Fatalf("round %d: ca.crt on disk is not the CA every caller returned (%v)", round, err)
		}
		if left, _ := filepath.Glob(filepath.Join(dir, ".*")); len(left) != 0 {
			t.Fatalf("round %d: temporary files left behind: %v", round, left)
		}
	}
}

// A CA key without its certificate, or the reverse, is a CA that cannot
// be completed. Replacing it silently would break every client that
// embeds the old certificate, so it is an error a person resolves.
func TestAnIncompleteCAIsNotReplaced(t *testing.T) {
	for _, keep := range []string{"ca.key", "ca.crt"} {
		dir := filepath.Join(t.TempDir(), "tls")
		if _, _, err := LoadOrCreate(dir, []string{"127.0.0.1"}); err != nil {
			t.Fatal(err)
		}
		other := map[string]string{"ca.key": "ca.crt", "ca.crt": "ca.key"}[keep]
		before, _ := os.ReadFile(filepath.Join(dir, keep))
		os.Remove(filepath.Join(dir, other))
		old := caWait
		caWait = 100 * time.Millisecond
		_, _, err := LoadOrCreate(dir, []string{"127.0.0.1"})
		caWait = old
		if err == nil {
			t.Errorf("only %s present: LoadOrCreate succeeded", keep)
		}
		after, _ := os.ReadFile(filepath.Join(dir, keep))
		if !bytes.Equal(before, after) {
			t.Errorf("only %s present: it was rewritten", keep)
		}
	}
}

// Two processes renewing the serving pair at once can leave one's
// certificate beside the other's key. That must heal on the next start,
// not stop serve from starting.
func TestAMismatchedServerPairIsReissued(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "tls")
	if _, _, err := LoadOrCreate(dir, []string{"127.0.0.1"}); err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(t.TempDir(), "tls")
	if _, _, err := LoadOrCreate(other, []string{"127.0.0.1"}); err != nil {
		t.Fatal(err)
	}
	k, _ := os.ReadFile(filepath.Join(other, "server.key"))
	if err := os.WriteFile(filepath.Join(dir, "server.key"), k, 0o600); err != nil {
		t.Fatal(err)
	}
	cert, ca, err := LoadOrCreate(dir, []string{"127.0.0.1"})
	if err != nil {
		t.Fatalf("a mismatched pair stopped LoadOrCreate: %v", err)
	}
	if err := verify(t, ca, cert.Certificate[0], "127.0.0.1"); err != nil {
		t.Error(err)
	}
}

// LoadCA is for commands that only print the CA (a client's trust bundle): it
// must neither create a CA nor touch the serving pair serve is using.
func TestLoadCAReadsWithoutWriting(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "tls")
	if _, err := LoadCA(dir); !errors.Is(err, ErrNoCA) {
		t.Fatalf("absent CA: err = %v, want ErrNoCA", err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("LoadCA created the directory: %v", err)
	}
	_, want, err := LoadOrCreate(dir, []string{"127.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"server.crt", "server.key"} {
		if err := os.Remove(filepath.Join(dir, f)); err != nil {
			t.Fatal(err)
		}
	}
	got, err := LoadCA(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Error("LoadCA returned a different CA")
	}
	if _, err := os.Stat(filepath.Join(dir, "server.crt")); !os.IsNotExist(err) {
		t.Errorf("LoadCA wrote a serving certificate: %v", err)
	}
}
