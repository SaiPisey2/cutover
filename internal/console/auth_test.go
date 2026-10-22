package console

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SaiPisey2/cutover/internal/store"
	"github.com/stretchr/testify/require"
)

const alicePW = "correct horse battery"

var (
	hashOnce  sync.Once
	aliceHash string
)

type env struct {
	t    *testing.T
	st   *store.Store
	a    *Auth
	logs *bytes.Buffer
	now  time.Time
	mu   sync.Mutex
}

func newEnv(t *testing.T) *env {
	t.Helper()
	hashOnce.Do(func() {
		var err error
		aliceHash, err = HashPassword(alicePW)
		require.NoError(t, err)
	})
	st, err := store.Open(filepath.Join(t.TempDir(), "c.db"))
	require.NoError(t, err)
	t.Cleanup(func() { st.Close() })
	e := &env{t: t, st: st, logs: &bytes.Buffer{}, now: time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)}
	require.NoError(t, st.CreateApprover(context.Background(), store.Approver{
		ID: "a1", Name: "alice", PasswordHash: aliceHash, Created: e.now}))
	e.a = NewAuth(st, slog.New(slog.NewTextHandler(e.logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	e.a.Now = func() time.Time { e.mu.Lock(); defer e.mu.Unlock(); return e.now }
	return e
}

func (e *env) advance(d time.Duration) { e.mu.Lock(); e.now = e.now.Add(d); e.mu.Unlock() }

func (e *env) login(body, ip string, cookies ...*http.Cookie) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "/api/login", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.RemoteAddr = ip + ":1234"
	for _, c := range cookies {
		r.AddCookie(c)
	}
	w := httptest.NewRecorder()
	e.a.Login(w, r)
	return w
}

func goodBody() string { return `{"name":"alice","password":"` + alicePW + `"}` }

func (e *env) loggedIn() (*http.Cookie, string) {
	e.t.Helper()
	w := e.login(goodBody(), "10.0.0.1")
	require.Equal(e.t, 200, w.Code, w.Body.String())
	var out map[string]string
	require.NoError(e.t, json.Unmarshal(w.Body.Bytes(), &out))
	for _, c := range w.Result().Cookies() {
		if c.Name == SessionCookie {
			return c, out["csrf"]
		}
	}
	e.t.Fatal("no cookie")
	return nil, ""
}

func (e *env) audit(kind string) []store.AuditRow {
	rows, err := e.st.AuditPage(context.Background(), "", 0, 100)
	require.NoError(e.t, err)
	var out []store.AuditRow
	for _, r := range rows {
		if r.Kind == kind {
			out = append(out, r)
		}
	}
	return out
}

func TestLoginSetsStrictCookieAndReturnsCSRF(t *testing.T) {
	e := newEnv(t)
	w := e.login(goodBody(), "10.0.0.1")
	require.Equal(t, 200, w.Code, w.Body.String())
	var out map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Equal(t, "alice", out["name"])
	require.Equal(t, "alice#a1", out["id"])
	require.NotEmpty(t, out["csrf"])
	cs := w.Result().Cookies()
	require.Len(t, cs, 1)
	c := cs[0]
	require.Equal(t, SessionCookie, c.Name)
	require.True(t, c.HttpOnly)
	require.True(t, c.Secure)
	require.Equal(t, http.SameSiteStrictMode, c.SameSite)
	require.Equal(t, "/", c.Path)
	require.Equal(t, 12*3600, c.MaxAge)
	rows := e.audit("login")
	require.Len(t, rows, 1)
	require.Equal(t, "alice#a1", rows[0].ActorID)
	require.Equal(t, "alice", rows[0].ActorName)
}

func TestLoginFailuresLookTheSame(t *testing.T) {
	e := newEnv(t)
	bad := e.login(`{"name":"alice","password":"wrong wrong wrong"}`, "10.0.0.1")
	unknown := e.login(`{"name":"nobody","password":"wrong wrong wrong"}`, "10.0.0.1")
	for _, w := range []*httptest.ResponseRecorder{bad, unknown} {
		require.Equal(t, 401, w.Code)
		require.JSONEq(t, `{"error":"unauthenticated"}`, w.Body.String())
		require.Empty(t, w.Result().Cookies())
	}
	rows := e.audit("login-failed")
	require.Len(t, rows, 2)
	names := []string{rows[0].ActorName, rows[1].ActorName}
	require.ElementsMatch(t, []string{"alice", "nobody"}, names)
}

func TestLoginFailedAuditNameIsValidated(t *testing.T) {
	e := newEnv(t)
	long := strings.Repeat("a", 200)
	for _, n := range []string{"Bad Name\n", long, ""} {
		b, _ := json.Marshal(map[string]string{"name": n, "password": "x"})
		require.Equal(t, 401, e.login(string(b), "10.0.0.1").Code)
		e.advance(time.Minute)
	}
	for _, r := range e.audit("login-failed") {
		require.Equal(t, "(invalid)", r.ActorName)
	}
	require.Len(t, e.audit("login-failed"), 3)
}

func TestDisabledApproverCannotLogIn(t *testing.T) {
	e := newEnv(t)
	require.NoError(t, e.st.DisableApprover(context.Background(), "alice", e.now))
	require.Equal(t, 401, e.login(goodBody(), "10.0.0.1").Code)
}

func TestUnknownNameStillHashes(t *testing.T) {
	e := newEnv(t)
	calls := 0
	e.a.check = func(phc, pw string) bool { calls++; return CheckPassword(phc, pw) }
	require.Equal(t, 401, e.login(`{"name":"nobody","password":"whatever pass"}`, "10.0.0.1").Code)
	require.Equal(t, 1, calls)
}

func TestLoginNeedsJSON(t *testing.T) {
	e := newEnv(t)
	r := httptest.NewRequest("POST", "/api/login", strings.NewReader(goodBody()))
	r.Header.Set("Content-Type", "text/plain")
	r.RemoteAddr = "10.0.0.1:1"
	w := httptest.NewRecorder()
	e.a.Login(w, r)
	require.Equal(t, 415, w.Code)
	for _, body := range []string{
		`{"name":"alice","name":"alice","password":"` + alicePW + `"}`,
		`{"name":"alice","password":"` + alicePW + `","extra":"x"}`,
		`{"name":"alice","password":"` + alicePW + `"} {}`,
		`{"name":"alice"}`,
		`{"NAME":"alice","password":"` + alicePW + `"}`,
		`{"name":"alice","password":5}`,
		`[]`,
		``,
	} {
		e.advance(time.Minute)
		require.Equal(t, 400, e.login(body, "10.0.0.1").Code, body)
	}
}

func TestLoginIsRateLimitedPerIP(t *testing.T) {
	e := newEnv(t)
	bad := `{"name":"nobody","password":"x"}`
	for i := 0; i < 5; i++ {
		require.Equal(t, 401, e.login(bad, "10.0.0.1").Code)
	}
	w := e.login(bad, "10.0.0.1")
	require.Equal(t, 429, w.Code)
	require.JSONEq(t, `{"error":"too many login attempts"}`, w.Body.String())
	require.Equal(t, 200, e.login(goodBody(), "10.0.0.2").Code)
	e.advance(time.Minute)
	require.Equal(t, 401, e.login(bad, "10.0.0.1").Code)
}

func TestLimiterIgnoresForwardedHeaderAndBoundsMemory(t *testing.T) {
	e := newEnv(t)
	for i := 0; i < 6; i++ {
		r := httptest.NewRequest("POST", "/api/login", strings.NewReader(`{"name":"nobody","password":"x"}`))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-Forwarded-For", "9.9.9."+string(rune('0'+i)))
		r.RemoteAddr = "10.0.0.1:1"
		w := httptest.NewRecorder()
		e.a.Login(w, r)
		if i == 5 {
			require.Equal(t, 429, w.Code)
		}
	}
	l := newLimiter()
	now := time.Now()
	for i := 0; i < limiterHosts+10; i++ {
		l.allow("h"+string(rune(i+0x100)), now)
	}
	require.Equal(t, limiterHosts, l.size())
	require.False(t, l.allow("brand-new-host", now))
	require.True(t, l.allow("h"+string(rune(0x100)), now))
}

func TestStateChangingCallsNeedTheCSRFHeader(t *testing.T) {
	e := newEnv(t)
	cookie, csrf := e.loggedIn()
	called := 0
	h := e.a.Require(func(w http.ResponseWriter, r *http.Request, s store.Session) {
		called++
		require.Equal(t, "alice", s.ApproverName)
		w.WriteHeader(204)
	})
	do := func(method string, c *http.Cookie, hdr string) int {
		r := httptest.NewRequest(method, "/x", nil)
		if c != nil {
			r.AddCookie(c)
		}
		if hdr != "" {
			r.Header.Set(CSRFHeader, hdr)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}
	require.Equal(t, 401, do("POST", nil, csrf))
	require.Equal(t, 403, do("POST", cookie, ""))
	require.Equal(t, 403, do("POST", cookie, "wrong"))
	require.Equal(t, 0, called)
	require.Equal(t, 204, do("POST", cookie, csrf))
	require.Equal(t, 204, do("GET", cookie, ""))
	require.Equal(t, 2, called)
	// Unknown cookie with a header: still 401, not 403.
	require.Equal(t, 401, do("POST", &http.Cookie{Name: SessionCookie, Value: "nope"}, csrf))
}

func TestSessionExpires(t *testing.T) {
	e := newEnv(t)
	cookie, _ := e.loggedIn()
	h := e.a.Require(func(w http.ResponseWriter, r *http.Request, s store.Session) { w.WriteHeader(204) })
	get := func() int {
		r := httptest.NewRequest("GET", "/x", nil)
		r.AddCookie(cookie)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}
	require.Equal(t, 204, get())
	e.advance(12*time.Hour + time.Second)
	require.Equal(t, 401, get())
}

func TestMe(t *testing.T) {
	e := newEnv(t)
	cookie, csrf := e.loggedIn()
	r := httptest.NewRequest("GET", "/api/me", nil)
	r.AddCookie(cookie)
	w := httptest.NewRecorder()
	e.a.Me(w, r)
	require.Equal(t, 200, w.Code)
	var out map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Equal(t, "alice", out["name"])
	require.Equal(t, "alice#a1", out["id"])
	require.Equal(t, csrf, out["csrf"])
	require.Equal(t, e.now.Format(time.RFC3339), out["session_created"])
	w = httptest.NewRecorder()
	e.a.Me(w, httptest.NewRequest("GET", "/api/me", nil))
	require.Equal(t, 401, w.Code)
}

func TestLogoutRevokes(t *testing.T) {
	e := newEnv(t)
	cookie, csrf := e.loggedIn()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/logout", e.a.Logout)
	mux.HandleFunc("GET /api/me", e.a.Me)
	r := httptest.NewRequest("POST", "/api/logout", nil)
	r.AddCookie(cookie)
	r.Header.Set(CSRFHeader, csrf)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	require.Equal(t, 204, w.Code)
	r = httptest.NewRequest("GET", "/api/me", nil)
	r.AddCookie(cookie)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	require.Equal(t, 401, w.Code)
}

func TestLogoutChecksCSRFItself(t *testing.T) {
	e := newEnv(t)
	cookie, csrf := e.loggedIn()
	post := func(hdr string) int {
		r := httptest.NewRequest("POST", "/api/logout", nil)
		r.AddCookie(cookie)
		if hdr != "" {
			r.Header.Set(CSRFHeader, hdr)
		}
		w := httptest.NewRecorder()
		e.a.Logout(w, r)
		return w.Code
	}
	require.Equal(t, 403, post(""))
	require.Equal(t, 403, post("wrong"))
	r := httptest.NewRequest("GET", "/api/me", nil)
	r.AddCookie(cookie)
	w := httptest.NewRecorder()
	e.a.Me(w, r)
	require.Equal(t, 200, w.Code)
	require.Equal(t, 204, post(csrf))
}

func TestNewLoginRevokesOldSession(t *testing.T) {
	e := newEnv(t)
	old, _ := e.loggedIn()
	require.Equal(t, 200, e.login(goodBody(), "10.0.0.2", old).Code)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/api/me", nil)
	r.AddCookie(old)
	e.a.Me(w, r)
	require.Equal(t, 401, w.Code)
}

func TestLogsNeverContainSecrets(t *testing.T) {
	e := newEnv(t)
	cookie, csrf := e.loggedIn()
	e.login(`{"name":"alice","password":"wrong wrong wrong"}`, "10.0.0.3")
	e.login(`{"name":"alice","password":"`+alicePW+`","x":1}`, "10.0.0.4")
	r := httptest.NewRequest("POST", "/x", nil)
	r.AddCookie(cookie)
	r.Header.Set(CSRFHeader, "wrong-csrf-guess")
	e.a.Require(func(http.ResponseWriter, *http.Request, store.Session) {}).ServeHTTP(httptest.NewRecorder(), r)
	for i := 0; i < 6; i++ {
		e.login(`{"name":"alice","password":"wrong wrong wrong"}`, "10.0.0.5")
	}
	mr := httptest.NewRequest("GET", "/api/me", nil)
	mr.AddCookie(cookie)
	e.a.Me(httptest.NewRecorder(), mr)
	lr := httptest.NewRequest("POST", "/api/logout", nil)
	lr.AddCookie(cookie)
	lr.Header.Set(CSRFHeader, csrf)
	e.a.Logout(httptest.NewRecorder(), lr)
	e.a.Logout(httptest.NewRecorder(), lr)
	require.NotEmpty(t, e.logs.String())
	for _, secret := range []string{alicePW, cookie.Value, csrf, "wrong wrong wrong", "wrong-csrf-guess", aliceHash} {
		require.NotContains(t, e.logs.String(), secret)
	}
}
