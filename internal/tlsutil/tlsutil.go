// Package tlsutil keeps a small local certificate authority and the
// serving certificate the cutover console presents. It is adapted from
// the same package in blastgate. The CA is created once and kept: every
// client that was handed it trusts it, so replacing it would break them
// all at once. The serving certificate is
// reissued whenever it no longer covers the configured hosts or is within
// 30 days of expiry.
//
// On a first run two processes may both find no CA. Creation
// is therefore claimed and published with hard links, which never replace
// an existing file: the process whose link to ca.key succeeds owns the CA,
// and only it then links ca.crt. ca.crt existing is the commit mark -- a
// reader that finds ca.key alone waits for the owner to finish, and a
// process that loses the claim reads the winner's CA instead of its own.
// The serving pair is written key first, then certificate, each by
// temp-file-and-rename, and a pair that does not match is reissued.
package tlsutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

const renewBefore = 30 * 24 * time.Hour

func LoadOrCreate(dir string, hosts []string) (tls.Certificate, []byte, error) {
	if len(hosts) == 0 {
		return tls.Certificate{}, nil, errors.New("no TLS hosts configured")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return tls.Certificate{}, nil, err
	}
	// MkdirAll leaves an existing directory's mode alone.
	if err := os.Chmod(dir, 0o700); err != nil {
		return tls.Certificate{}, nil, err
	}
	ca, caKey, caPEM, err := loadOrCreateCA(dir)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("certificate authority: %w", err)
	}
	certPEM, keyPEM, err := read(dir, "server")
	if err == nil && usable(certPEM, ca, hosts) {
		// Two processes renewing at once can leave one's certificate
		// beside the other's key; that is reissued, not fatal.
		if pair, err := tls.X509KeyPair(certPEM, keyPEM); err == nil {
			return pair, caPEM, nil
		}
	}
	certPEM, keyPEM, err = issue(ca, caKey, hosts)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("serving certificate: %w", err)
	}
	if err := write(dir, "server", certPEM, keyPEM); err != nil {
		return tls.Certificate{}, nil, err
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	return pair, caPEM, err
}

// ErrNoCA is LoadCA finding no finished CA in dir.
var ErrNoCA = errors.New("no certificate authority yet")

// LoadCA returns the CA certificate PEM without creating anything: no
// directory, no CA, and no serving certificate. It is for commands that
// only hand the CA to someone else (a client's trust bundle); going
// through LoadOrCreate there would reissue the serving pair under a
// running console whenever the command's host list differed from its own.
// A CA still being created counts as absent. The key is read and checked
// against the certificate so a half-restored CA is caught here, not when
// a client first fails to verify the console.
func LoadCA(dir string) ([]byte, error) {
	certPEM, keyPEM, err := readCA(dir)
	if errors.Is(err, errNoCA) || errors.Is(err, errCAInProgress) {
		return nil, ErrNoCA
	}
	if err != nil {
		return nil, err
	}
	if _, _, err := parse(certPEM, keyPEM); err != nil {
		return nil, fmt.Errorf("certificate authority: %w", err)
	}
	return certPEM, nil
}

// caWait bounds how long a caller waits for another process that has
// claimed the CA to publish its certificate. A variable so a test of the
// crashed-owner case need not wait the full time.
var caWait = 5 * time.Second

var (
	errNoCA         = errors.New("no CA yet")
	errCAInProgress = errors.New("CA being created")
)

func loadOrCreateCA(dir string) (*x509.Certificate, *ecdsa.PrivateKey, []byte, error) {
	deadline := time.Now().Add(caWait)
	for {
		certPEM, keyPEM, err := readCA(dir)
		switch {
		case err == nil:
			cert, key, err := parse(certPEM, keyPEM)
			return cert, key, certPEM, err
		case errors.Is(err, errNoCA):
			cert, key, certPEM, err := createCA(dir)
			if err != nil || cert != nil {
				return cert, key, certPEM, err
			}
			// Another process claimed the CA first; read theirs.
		case errors.Is(err, errCAInProgress):
			if time.Now().After(deadline) {
				return nil, nil, nil, fmt.Errorf("%s exists without ca.crt: a CA was being created and never finished; remove %[1]s to create a new CA", filepath.Join(dir, "ca.key"))
			}
			time.Sleep(20 * time.Millisecond)
		default:
			return nil, nil, nil, err
		}
	}
}

// readCA tells a finished CA from none, from one still being created.
// A certificate without its key is never "none": creating a new CA over
// it would silently break every client that trusts the old one.
func readCA(dir string) ([]byte, []byte, error) {
	certPEM, err := os.ReadFile(filepath.Join(dir, "ca.crt"))
	if errors.Is(err, os.ErrNotExist) {
		if _, kerr := os.Stat(filepath.Join(dir, "ca.key")); kerr == nil {
			return nil, nil, errCAInProgress
		} else if errors.Is(kerr, os.ErrNotExist) {
			return nil, nil, errNoCA
		} else {
			return nil, nil, kerr
		}
	}
	if err != nil {
		return nil, nil, err
	}
	keyPEM, err := os.ReadFile(filepath.Join(dir, "ca.key"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil, fmt.Errorf("%s exists without ca.key; restore the key, or remove ca.crt to create a new CA (every client will need the new CA)", filepath.Join(dir, "ca.crt"))
	}
	return certPEM, keyPEM, err
}

// createCA returns a nil certificate and no error when another process
// claimed the CA first.
func createCA(dir string) (*x509.Certificate, *ecdsa.PrivateKey, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, nil, err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          serial(),
		Subject:               pkix.Name{CommonName: "cutover local CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.AddDate(10, 0, 0),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, nil, err
	}
	certPEM, keyPEM, err := encode(der, key)
	if err != nil {
		return nil, nil, nil, err
	}
	tmpKey, err := temp(dir, "ca.key", keyPEM, 0o600)
	if err != nil {
		return nil, nil, nil, err
	}
	defer os.Remove(tmpKey)
	tmpCrt, err := temp(dir, "ca.crt", certPEM, 0o644)
	if err != nil {
		return nil, nil, nil, err
	}
	defer os.Remove(tmpCrt)
	// The claim: link fails if ca.key exists, where rename would replace it.
	if err := os.Link(tmpKey, filepath.Join(dir, "ca.key")); err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, nil, nil, nil
		}
		return nil, nil, nil, err
	}
	// The publish. Only the claim's owner gets here, so ca.crt cannot
	// exist unless someone put it there by hand -- and then it is kept.
	if err := os.Link(tmpCrt, filepath.Join(dir, "ca.crt")); err != nil {
		return nil, nil, nil, err
	}
	cert, _ := x509.ParseCertificate(der)
	return cert, key, certPEM, nil
}

func issue(ca *x509.Certificate, caKey *ecdsa.PrivateKey, hosts []string) ([]byte, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial(),
		Subject:      pkix.Name{CommonName: "cutover"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.AddDate(1, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else {
			tmpl.DNSNames = append(tmpl.DNSNames, h)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		return nil, nil, err
	}
	return encode(der, key)
}

func usable(certPEM []byte, ca *x509.Certificate, hosts []string) bool {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return false
	}
	c, err := x509.ParseCertificate(block.Bytes)
	if err != nil || time.Until(c.NotAfter) < renewBefore {
		return false
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	for _, h := range hosts {
		if _, err := c.Verify(x509.VerifyOptions{Roots: pool, DNSName: h}); err != nil {
			return false
		}
	}
	return true
}

func serial() *big.Int {
	n, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	return n
}

func encode(der []byte, key *ecdsa.PrivateKey) ([]byte, []byte, error) {
	kb, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}), nil
}

func parse(certPEM, keyPEM []byte) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	cb, _ := pem.Decode(certPEM)
	kb, _ := pem.Decode(keyPEM)
	if cb == nil || kb == nil {
		return nil, nil, errors.New("unreadable PEM")
	}
	cert, err := x509.ParseCertificate(cb.Bytes)
	if err != nil {
		return nil, nil, err
	}
	key, err := x509.ParseECPrivateKey(kb.Bytes)
	if err != nil {
		return nil, nil, err
	}
	// A CA key that does not sign for the CA certificate would issue
	// serving certificates no client trusts.
	if !key.PublicKey.Equal(cert.PublicKey) {
		return nil, nil, errors.New("CA key does not match the CA certificate")
	}
	return cert, key, nil
}

func read(dir, name string) ([]byte, []byte, error) {
	c, err := os.ReadFile(filepath.Join(dir, name+".crt"))
	if err != nil {
		return nil, nil, err
	}
	k, err := os.ReadFile(filepath.Join(dir, name+".key"))
	return c, k, err
}

// write replaces the pair one file at a time, each atomically, so a reader
// never sees a half-written file. Key before certificate: a reader that
// catches the moment between sees a mismatch, which LoadOrCreate reissues.
func write(dir, name string, certPEM, keyPEM []byte) error {
	for _, f := range []struct {
		ext  string
		data []byte
		mode os.FileMode
	}{{".key", keyPEM, 0o600}, {".crt", certPEM, 0o644}} {
		tmp, err := temp(dir, name+f.ext, f.data, f.mode)
		if err != nil {
			return err
		}
		if err := os.Rename(tmp, filepath.Join(dir, name+f.ext)); err != nil {
			os.Remove(tmp)
			return err
		}
	}
	return nil
}

// temp writes data to a new hidden file in dir, synced so a link or rename
// of it never publishes an empty file after a crash, and returns its path.
func temp(dir, name string, data []byte, mode os.FileMode) (string, error) {
	f, err := os.CreateTemp(dir, "."+name+"-*")
	if err != nil {
		return "", err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Chmod(mode)
	}
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(f.Name())
		return "", err
	}
	return f.Name(), nil
}
