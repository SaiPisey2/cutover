package console

// This file is the console API's client, used by the CLI's migrate, gate
// and rollback: the server is the only signaller, so the command line
// signs in like the UI does and goes through the same checks.

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/SaiPisey2/cutover/internal/flow"
)

const (
	clientTimeout   = 60 * time.Second
	clientReplyMax  = 8 << 20
	clientErrorMax  = 4 << 10
	errGateClosed   = "gate is not open"
	errBadLoginText = "sign-in failed: unknown name or wrong password"
)

// Client talks to one console over TLS, trusting only the CA it was given.
// It keeps the session cookie and CSRF token from Login.
type Client struct {
	base *url.URL
	hc   *http.Client
	csrf string
}

// APIError is a refusal from the server. Message is the server's fixed
// error text.
type APIError struct {
	Status  int
	Message string
}

func (e *APIError) Error() string { return e.Message }

// NewClient returns a client for the console at base (an https URL),
// trusting only the certificates in caPEM. It follows no redirects.
func NewClient(base string, caPEM []byte) (*Client, error) {
	u, err := url.Parse(base)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("server %q must be an https URL like https://127.0.0.1:7700", base)
	}
	u.Path = strings.TrimSuffix(u.Path, "/")
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("the CA file holds no PEM certificate")
	}
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, err
	}
	tr := &http.Transport{
		// No proxy: the console is on loopback or reached through a
		// tunnel, and a session cookie must not pass through a proxy the
		// environment happens to name.
		Proxy:               nil,
		TLSClientConfig:     &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
		TLSHandshakeTimeout: 10 * time.Second,
		ForceAttemptHTTP2:   true,
	}
	hc := &http.Client{
		Transport: tr,
		Jar:       jar,
		Timeout:   clientTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return &Client{base: u, hc: hc}, nil
}

// Login signs in and keeps the session's CSRF token.
func (c *Client) Login(ctx context.Context, name, password string) error {
	var out struct {
		CSRF string `json:"csrf"`
	}
	err := c.do(ctx, http.MethodPost, "/api/login", map[string]string{"name": name, "password": password}, &out, http.StatusOK)
	var ae *APIError
	if errors.As(err, &ae) && ae.Status == http.StatusUnauthorized {
		return &APIError{Status: ae.Status, Message: errBadLoginText}
	}
	if err != nil {
		return err
	}
	if out.CSRF == "" {
		return errors.New("the server's sign-in reply has no csrf token")
	}
	c.csrf = out.CSRF
	return nil
}

// Start starts a migration from the plan's YAML. id may be empty for the
// plan's name; the ID the server used is returned.
func (c *Client) Start(ctx context.Context, id string, planYAML []byte) (string, error) {
	var out struct {
		ID string `json:"id"`
	}
	err := c.do(ctx, http.MethodPost, "/api/migrations", startBody{ID: id, Plan: string(planYAML)}, &out, http.StatusCreated)
	return out.ID, err
}

// Status returns the migration's status and evidence digest.
func (c *Client) Status(ctx context.Context, id string) (StatusReply, error) {
	var out StatusReply
	err := c.do(ctx, http.MethodGet, "/api/migrations/"+url.PathEscape(id), nil, &out, http.StatusOK)
	return out, err
}

// Decide decides gate. It reads the status first and refuses unless that
// gate is the open one, then sends the digest of the evidence it read, so
// the server refuses the decision if the gate changed in between.
func (c *Client) Decide(ctx context.Context, id, gate string, d flow.Decision, note string) error {
	st, err := c.Status(ctx, id)
	if err != nil {
		return err
	}
	if g := st.Status.Gate; g == nil || g.Name != gate {
		open := "none"
		if g != nil {
			open = clean(g.Name)
		}
		return &APIError{Status: http.StatusConflict, Message: errGateClosed + " (open: " + open + ")"}
	}
	body := decideBody{Decision: d, Note: note, EvidenceDigest: st.EvidenceDigest}
	return c.do(ctx, http.MethodPost, "/api/migrations/"+url.PathEscape(id)+"/gates/"+url.PathEscape(gate), body, nil, http.StatusAccepted)
}

// Rollback asks the migration to roll back.
func (c *Client) Rollback(ctx context.Context, id, reason string) error {
	return c.do(ctx, http.MethodPost, "/api/migrations/"+url.PathEscape(id)+"/rollback", rollbackBody{Reason: reason}, nil, http.StatusAccepted)
}

// do sends one request and decodes a reply with status want into out
// (when out is not nil). Any other status is an *APIError.
func (c *Client) do(ctx context.Context, method, path string, in, out any, want int) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	u := *c.base
	u.Path += path
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	if method != http.MethodGet && c.csrf != "" {
		req.Header.Set(CSRFHeader, c.csrf)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != want {
		return replyError(resp)
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, clientErrorMax))
		return nil
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, clientReplyMax)).Decode(out); err != nil {
		return fmt.Errorf("unreadable reply from the server (%d)", resp.StatusCode)
	}
	return nil
}

// replyError reads the server's {"error": "..."} body. A reply without
// one (a proxy's page, a redirect) is described by its status alone.
func replyError(resp *http.Response) error {
	var e struct {
		Error string `json:"error"`
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, clientErrorMax))
	if json.Unmarshal(b, &e) == nil && e.Error != "" {
		return &APIError{Status: resp.StatusCode, Message: clean(e.Error)}
	}
	return &APIError{Status: resp.StatusCode, Message: "the server answered " + strconv.Itoa(resp.StatusCode) + " " + http.StatusText(resp.StatusCode)}
}

// clean quotes text holding a non-printable rune, so a reply cannot
// repaint the operator's terminal.
func clean(s string) string {
	for _, r := range s {
		if !unicode.IsPrint(r) || unicode.Is(unicode.Cf, r) {
			return strconv.Quote(s)
		}
	}
	return s
}
