// Package sshx runs commands and moves files over SSH. Host keys are
// always checked against the plan's known_hosts file; there is no way to
// skip that. Secrets travel only on stdin, and stdin never appears in an
// error.
package sshx

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/SaiPisey2/cutover/internal/plan"
)

const (
	maxOutput        = 16 << 20
	maxStderr        = 4 << 10
	handshakeTimeout = 10 * time.Second
)

type Client struct {
	c    *ssh.Client
	sudo bool
	addr string
}

type ExitError struct {
	Command string
	Code    int
	Stderr  string
}

func (e *ExitError) Error() string {
	msg := fmt.Sprintf("%s: exit %d", e.Command, e.Code)
	if e.Stderr != "" {
		msg += ": " + e.Stderr
	}
	return msg
}

// Quote returns s as one single-quoted POSIX shell word.
func Quote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }

func Dial(ctx context.Context, h plan.Host) (*Client, error) {
	addr := net.JoinHostPort(h.Address, strconv.Itoa(h.Port))
	key, err := h.Key.Resolve()
	if err != nil {
		return nil, err
	}
	signer, err := ssh.ParsePrivateKey([]byte(key))
	if err != nil {
		var pm *ssh.PassphraseMissingError
		if errors.As(err, &pm) {
			return nil, fmt.Errorf("ssh key from %s has a passphrase; cutover needs a key without one", h.Key)
		}
		return nil, fmt.Errorf("ssh key from %s is not a private key", h.Key)
	}
	cb, algos, err := hostKeys(h.KnownHosts, addr)
	if err != nil {
		return nil, err
	}
	cfg := &ssh.ClientConfig{
		User:              h.User,
		Auth:              []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback:   cb,
		HostKeyAlgorithms: algos,
		Timeout:           handshakeTimeout,
	}
	d := net.Dialer{Timeout: handshakeTimeout}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("ssh %s: %w", addr, err)
	}

	// ClientConfig.Timeout covers only ssh.Dial, so bound the handshake
	// here: a deadline on the conn, and an abort when ctx is cancelled.
	deadline := time.Now().Add(handshakeTimeout)
	if cd, ok := ctx.Deadline(); ok && cd.Before(deadline) {
		deadline = cd
	}
	_ = conn.SetDeadline(deadline)
	stop := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		select {
		case <-ctx.Done():
			conn.Close()
		case <-stop:
		}
	}()
	cc, chans, reqs, err := ssh.NewClientConn(conn, addr, cfg)
	close(stop)
	<-stopped
	if err == nil && ctx.Err() != nil {
		cc.Close()
		conn.Close()
		return nil, fmt.Errorf("ssh %s: %w", addr, ctx.Err())
	}
	if err != nil {
		conn.Close()
		if ctx.Err() != nil {
			return nil, fmt.Errorf("ssh %s: %w", addr, ctx.Err())
		}
		var ke *knownhosts.KeyError
		if errors.As(err, &ke) && len(ke.Want) > 0 {
			return nil, fmt.Errorf("ssh %s: host key does not match %s (the host changed, or someone is in the middle)", addr, h.KnownHosts)
		}
		return nil, fmt.Errorf("ssh %s: %w", addr, err)
	}
	_ = conn.SetDeadline(time.Time{})
	return &Client{c: ssh.NewClient(cc, chans, reqs), sudo: h.Sudo, addr: addr}, nil
}

// hostKeys returns the known_hosts callback and the host key algorithms
// to offer: the types listed for this host. Offering only those makes the
// server present a key the file can actually verify.
func hostKeys(file, addr string) (ssh.HostKeyCallback, []string, error) {
	cb, err := knownhosts.New(file)
	if err != nil {
		return nil, nil, fmt.Errorf("known_hosts %s: %w", file, err)
	}
	b, err := os.ReadFile(file)
	if err != nil {
		return nil, nil, err
	}
	want := knownhosts.Normalize(addr)
	var algos []string
	sc := bufio.NewScanner(bytes.NewReader(b))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		_, hosts, key, _, _, err := ssh.ParseKnownHosts([]byte(line))
		if err != nil {
			continue
		}
		for _, h := range hosts {
			if h == want || hashedMatch(h, want) {
				switch t := key.Type(); t {
				case ssh.KeyAlgoRSA:
					algos = append(algos, ssh.KeyAlgoRSASHA512, ssh.KeyAlgoRSASHA256, ssh.KeyAlgoRSA)
				default:
					algos = append(algos, t)
				}
			}
		}
	}
	if err := sc.Err(); err != nil {
		return nil, nil, fmt.Errorf("known_hosts %s: %w", file, err)
	}
	if len(algos) == 0 {
		return nil, nil, fmt.Errorf("ssh %s is not in %s; add its key (check it first) before cutover connects", addr, file)
	}
	return cb, algos, nil
}

// hashedMatch reports whether h is an OpenSSH hashed known_hosts host
// (|1|salt|hash) for the normalized name want.
func hashedMatch(h, want string) bool {
	parts := strings.Split(h, "|")
	if len(parts) != 4 || parts[1] != "1" {
		return false
	}
	salt, err := base64.StdEncoding.DecodeString(parts[2])
	if err != nil {
		return false
	}
	sum, err := base64.StdEncoding.DecodeString(parts[3])
	if err != nil {
		return false
	}
	m := hmac.New(sha1.New, salt)
	m.Write([]byte(want))
	return hmac.Equal(m.Sum(nil), sum)
}

// limited keeps at most max bytes and records whether more arrived.
type limited struct {
	buf  bytes.Buffer
	max  int
	over bool
	tail bool // keep the last max bytes instead of the first
}

func (l *limited) Write(p []byte) (int, error) {
	if l.tail {
		l.buf.Write(p)
		if extra := l.buf.Len() - l.max; extra > 0 {
			l.buf.Next(extra)
		}
		return len(p), nil
	}
	room := l.max - l.buf.Len()
	if len(p) > room {
		l.over = true
		if room > 0 {
			l.buf.Write(p[:room])
		}
		return len(p), nil
	}
	return l.buf.Write(p)
}

// Run runs cmd in a new session. stdin, when not nil, is fed to the
// command and never echoed anywhere. A cancelled ctx abandons the
// session: the remote command may keep running (under sudo the kill
// reaches sudo, not its child), and a WriteFile temp file may be left
// behind. The checksum in WriteFile guarantees the target is never
// replaced by a partial file.
func (c *Client) Run(ctx context.Context, cmd string, stdin []byte) ([]byte, error) {
	s, err := c.c.NewSession()
	if err != nil {
		return nil, fmt.Errorf("ssh %s: %w", c.addr, err)
	}
	defer s.Close()
	out := &limited{max: maxOutput}
	errb := &limited{max: maxStderr, tail: true}
	s.Stdout, s.Stderr = out, errb
	if stdin != nil {
		s.Stdin = bytes.NewReader(stdin)
	}
	done := make(chan error, 1)
	go func() { done <- s.Run(cmd) }()
	select {
	case err = <-done:
	case <-ctx.Done():
		_ = s.Signal(ssh.SIGKILL)
		s.Close()
		<-done
		return nil, fmt.Errorf("ssh %s: %s: %w", c.addr, summary(cmd), ctx.Err())
	}
	if err != nil {
		var ee *ssh.ExitError
		if errors.As(err, &ee) {
			return nil, &ExitError{Command: "ssh " + c.addr + ": " + summary(cmd), Code: ee.ExitStatus(), Stderr: strings.TrimSpace(errb.buf.String())}
		}
		return nil, fmt.Errorf("ssh %s: %s: %w", c.addr, summary(cmd), err)
	}
	if out.over {
		return nil, fmt.Errorf("ssh %s: %s: output over %d bytes", c.addr, summary(cmd), maxOutput)
	}
	return out.buf.Bytes(), nil
}

// summary shortens a command for error messages. Commands never carry
// secrets (those go on stdin), but scripts can be long.
func summary(cmd string) string {
	cmd = strings.Join(strings.Fields(cmd), " ")
	if len(cmd) > 160 {
		return cmd[:160] + "…"
	}
	return cmd
}

// Root wraps cmd so it runs as root when the host uses sudo.
func (c *Client) Root(cmd string) string {
	if c.sudo {
		return "sudo -n sh -c " + Quote(cmd)
	}
	return "sh -c " + Quote(cmd)
}

var errNotAbs = errors.New("ssh path must be absolute")

// WriteFile replaces p atomically: a temp file in the same directory,
// filled, verified against the sha256 of data, mode set, synced, then
// renamed over p. A transfer cut short fails the check and leaves p
// untouched. Parent directories are created.
func (c *Client) WriteFile(ctx context.Context, p string, mode os.FileMode, data []byte) error {
	if !path.IsAbs(p) {
		return fmt.Errorf("%w: %q", errNotAbs, p)
	}
	sum := sha256.Sum256(data)
	script := fmt.Sprintf(`set -e
mkdir -p -- %s
t=$(mktemp -- %s)
trap 'rm -f -- "$t"' EXIT
cat > "$t"
got=$(sha256sum < "$t")
if [ "${got%%%% *}" != %s ]; then echo "checksum mismatch: transfer incomplete" >&2; exit 1; fi
chmod %o "$t"
sync
mv -f -- "$t" %s
trap - EXIT
sync`, Quote(path.Dir(p)), Quote(p+".cutover-XXXXXX"), Quote(hex.EncodeToString(sum[:])), mode.Perm(), Quote(p))
	if data == nil {
		data = []byte{}
	}
	_, err := c.Run(ctx, c.Root(script), data)
	return err
}

func (c *Client) ReadFile(ctx context.Context, p string) ([]byte, error) {
	if !path.IsAbs(p) {
		return nil, fmt.Errorf("%w: %q", errNotAbs, p)
	}
	return c.Run(ctx, c.Root("cat -- "+Quote(p)), nil)
}

func (c *Client) Exists(ctx context.Context, p string) (bool, error) {
	if !path.IsAbs(p) {
		return false, fmt.Errorf("%w: %q", errNotAbs, p)
	}
	out, err := c.Run(ctx, c.Root("if test -e "+Quote(p)+"; then echo yes; else echo no; fi"), nil)
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(string(out)) == "yes", nil
}

func (c *Client) Close() error { return c.c.Close() }
