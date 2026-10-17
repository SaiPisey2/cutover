package frontdoor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client calls cutover server's API.
type Client struct {
	Base  string
	Token string
	HTTP  *http.Client
}

// NewClient's timeout covers Apply's worst case: three ACK waits plus the drain.
func NewClient(base, token string) *Client {
	return &Client{Base: strings.TrimSuffix(base, "/"), Token: token, HTTP: &http.Client{
		Timeout: 3 * time.Minute,
		// never re-send the bearer token to a redirect target
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

type APIError struct {
	Status  int
	Message string
}

func (e *APIError) Error() string { return fmt.Sprintf("front door: HTTP %d: %s", e.Status, e.Message) }

// Permanent reports whether retrying the same request cannot help.
func (e *APIError) Permanent() bool {
	switch e.Status {
	case 400, 401, 403, 404, 409, 413:
		return true
	}
	return false
}

// checkBase refuses to send the bearer token over plain http to a
// non-loopback host.
func checkBase(base string) error {
	u, err := url.Parse(base)
	if err != nil {
		return fmt.Errorf("front door: bad base URL: %w", err)
	}
	switch u.Scheme {
	case "https":
		return nil
	case "http":
		h := u.Hostname()
		if h == "localhost" {
			return nil
		}
		if ip := net.ParseIP(h); ip != nil && ip.IsLoopback() {
			return nil
		}
	}
	return fmt.Errorf("front door: refusing to send the token to %q: use https, or http on a loopback address", base)
}

func (c *Client) do(ctx context.Context, method, path string, in, out any) error {
	if err := checkBase(c.Base); err != nil {
		return err
	}
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.Base+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("front door: %w", err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return fmt.Errorf("front door: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		var e struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(b, &e)
		if e.Error == "" {
			e.Error = http.StatusText(resp.StatusCode)
		}
		return &APIError{Status: resp.StatusCode, Message: e.Error}
	}
	if out != nil {
		return json.Unmarshal(b, out)
	}
	return nil
}

func routePath(owner, service string) string {
	return "/v1/routes/" + url.PathEscape(owner) + "/" + url.PathEscape(service)
}

func (c *Client) Apply(ctx context.Context, r Route) error {
	return c.do(ctx, http.MethodPut, routePath(r.Owner, r.Service), r, nil)
}

func (c *Client) Routes(ctx context.Context) ([]Route, error) {
	var out struct {
		Routes []Route `json:"routes"`
	}
	err := c.do(ctx, http.MethodGet, "/v1/routes", nil, &out)
	return out.Routes, err
}

func (c *Client) Health(ctx context.Context, owner, service string) (Health, error) {
	var h Health
	err := c.do(ctx, http.MethodGet, routePath(owner, service)+"/health", nil, &h)
	return h, err
}

func (c *Client) Stats(ctx context.Context, owner, service string) (Stats, error) {
	var s Stats
	err := c.do(ctx, http.MethodGet, routePath(owner, service)+"/stats", nil, &s)
	return s, err
}
