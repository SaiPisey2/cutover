package console

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/SaiPisey2/cutover/ui"
	"github.com/stretchr/testify/require"
)

const testIndex = `<!doctype html><meta charset="utf-8"><title>t</title>`

var testUI = fstest.MapFS{
	"index.html":      {Data: []byte(testIndex)},
	"assets/app-1.js": {Data: []byte(`console.log("ui")`)},
}

// stubAPI answers anything under /api with 200 so the shell's own
// handling, not a route table, is what the tests see.
var stubAPI = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
})

func newServerTS(t *testing.T) *httptest.Server {
	t.Helper()
	a := NewAuth(nil, slog.New(slog.DiscardHandler))
	srv := httptest.NewServer(NewServer(a, stubAPI, testUI))
	t.Cleanup(srv.Close)
	return srv
}

func send(t *testing.T, srv *httptest.Server, method, path string, hdr map[string]string) (*http.Response, string) {
	t.Helper()
	req, err := http.NewRequest(method, srv.URL+path, nil)
	require.NoError(t, err)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	c := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := c.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

var securityHeaders = map[string]string{
	"Content-Security-Policy": "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'",
	"X-Content-Type-Options":  "nosniff",
	"Referrer-Policy":         "no-referrer",
	"X-Frame-Options":         "DENY",
}

func TestSecurityHeaders(t *testing.T) {
	srv := newServerTS(t)
	for _, tc := range []struct {
		method, path string
		code         int
		noStore      bool
	}{
		{"GET", "/", 200, false},
		{"GET", "/index.html", 200, false},
		{"GET", "/assets/app-1.js", 200, false},
		{"GET", "/migrations/abc", 200, false},
		{"GET", "/assets/gone-0.js", 404, false},
		{"POST", "/", 405, false},
		{"GET", "/api/me", 200, true},
		{"GET", "/api", 200, true},
		{"GET", "//api/me", http.StatusTemporaryRedirect, true},
		{"GET", "/./api/me", http.StatusTemporaryRedirect, true},
		{"GET", "/api%2fme", 404, true},
	} {
		resp, body := send(t, srv, tc.method, tc.path, nil)
		require.Equal(t, tc.code, resp.StatusCode, "%s %s: %s", tc.method, tc.path, body)
		for k, v := range securityHeaders {
			require.Equal(t, v, resp.Header.Get(k), "%s %s: %s", tc.method, tc.path, k)
		}
		if tc.noStore {
			require.Equal(t, "no-store", resp.Header.Get("Cache-Control"), "%s %s", tc.method, tc.path)
		} else {
			require.NotEqual(t, "no-store", resp.Header.Get("Cache-Control"), "%s %s", tc.method, tc.path)
		}
	}
}

func TestNoCORS(t *testing.T) {
	srv := newServerTS(t)
	origin := map[string]string{"Origin": "https://evil.example"}
	preflight := map[string]string{"Origin": "https://evil.example", "Access-Control-Request-Method": "POST",
		"Access-Control-Request-Headers": CSRFHeader}
	for _, tc := range []struct {
		method, path string
		hdr          map[string]string
	}{
		{"GET", "/", origin},
		{"GET", "/api/me", origin},
		{"OPTIONS", "/api/login", preflight},
		{"OPTIONS", "/migrations/x", preflight},
	} {
		resp, _ := send(t, srv, tc.method, tc.path, tc.hdr)
		for k := range resp.Header {
			require.False(t, strings.HasPrefix(strings.ToLower(k), "access-control-"), "%s %s: %s", tc.method, tc.path, k)
		}
	}
}

func TestSPAFallback(t *testing.T) {
	srv := newServerTS(t)
	resp, body := send(t, srv, "GET", "/migrations/x", nil)
	require.Equal(t, 200, resp.StatusCode)
	require.Equal(t, testIndex, body)
	require.Equal(t, "no-cache", resp.Header.Get("Cache-Control"))
	require.True(t, strings.HasPrefix(resp.Header.Get("Content-Type"), "text/html"))

	resp, body = send(t, srv, "GET", "/assets/missing.js", nil)
	require.Equal(t, 404, resp.StatusCode)
	require.NotContains(t, body, "<title>")

	resp, _ = send(t, srv, "POST", "/assets/app-1.js", nil)
	require.Equal(t, 405, resp.StatusCode)

	// A range cannot turn index.html into a cacheable 416.
	resp, body = send(t, srv, "GET", "/", map[string]string{"Range": "bytes=9999-"})
	require.Equal(t, 200, resp.StatusCode)
	require.Equal(t, testIndex, body)
	require.Equal(t, "no-cache", resp.Header.Get("Cache-Control"))
}

func TestAPIPathsNeverGetTheApp(t *testing.T) {
	srv := newServerTS(t)
	for _, p := range []string{"/api%2fme", "/api%2F..%2Findex.html"} {
		resp, body := send(t, srv, "GET", p, nil)
		require.Equal(t, 404, resp.StatusCode, p)
		require.NotContains(t, body, "<title>", p)
		require.JSONEq(t, `{"error":"not found"}`, body, p)
	}
}

func TestEmbeddedUIIsServable(t *testing.T) {
	srv := httptest.NewServer(NewServer(NewAuth(nil, slog.New(slog.DiscardHandler)), stubAPI, ui.FS()))
	t.Cleanup(srv.Close)
	resp, body := send(t, srv, "GET", "/", nil)
	require.Equal(t, 200, resp.StatusCode)
	// The one script is a file from 'self'; the CSP blocks anything inline.
	require.Regexp(t, `<script type="module"[^>]* src="/assets/index-[^"]+\.js"></script>`, body)
	require.NotRegexp(t, `<script(\s[^>]*)?>\s*[^<\s]`, body)
	require.NotContains(t, body, "style=")

	js := regexp.MustCompile(`src="(/assets/index-[^"]+\.js)"`).FindStringSubmatch(body)
	require.Len(t, js, 2)
	resp, _ = send(t, srv, "GET", js[1], nil)
	require.Equal(t, 200, resp.StatusCode)
}
