package console

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/SaiPisey2/cutover/internal/store"
)

const (
	SessionCookie = "cutover_session"
	CSRFHeader    = "X-Cutover-CSRF"

	sessionTTL     = 12 * time.Hour
	loginBodyLimit = 4 << 10
	loginAttempts  = 5
	loginWindow    = time.Minute
	// limiterHosts bounds the limiter's memory: past it a new address is
	// refused rather than tracked.
	limiterHosts = 4096

	invalidName = "(invalid)"
)

// AuthStore is the slice of *store.Store that authentication needs.
type AuthStore interface {
	ApproverByName(ctx context.Context, name string) (store.Approver, error)
	CreateSession(ctx context.Context, s store.Session, idHash []byte) error
	SessionByHash(ctx context.Context, idHash []byte, now time.Time) (store.Session, error)
	RevokeSession(ctx context.Context, idHash []byte, at time.Time) error
	AppendAudit(ctx context.Context, r store.AuditRow) (int64, error)
}

type Auth struct {
	Store      AuthStore
	Now        func() time.Time
	Log        *slog.Logger
	SessionTTL time.Duration

	limiter *limiter
	// check and dummyHash exist so an unknown name still costs one hash
	// check; check is replaceable so a test can count the calls.
	check     func(phc, pw string) bool
	dummyOnce sync.Once
	dummyHash string
}

func NewAuth(st AuthStore, log *slog.Logger) *Auth {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Auth{Store: st, Now: time.Now, Log: log, SessionTTL: sessionTTL,
		limiter: newLimiter(), check: CheckPassword}
}

// ApproverID is the identity recorded in the workflow and audit log.
func ApproverID(name, id string) string { return name + "#" + id }

func hashOf(s string) []byte {
	h := sha256.Sum256([]byte(s))
	return h[:]
}

func randomString() string {
	b := make([]byte, 32)
	rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

const (
	errUnauthenticated = `{"error":"unauthenticated"}`
	errCSRF            = `{"error":"missing or wrong csrf header"}`
	errLimited         = `{"error":"too many login attempts"}`
	errInternal        = `{"error":"internal error"}`
	errBadRequest      = `{"error":"bad request"}`
	errNotJSON         = `{"error":"content type must be application/json"}`
)

func writeJSON(w http.ResponseWriter, code int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write([]byte(body))
}

// remoteHost is the limiter's key: the connection's own address, never a
// forwarded header the client writes.
func remoteHost(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

type loginBody struct{ name, password string }

var errLoginBody = errors.New("login body must be exactly {\"name\": \"...\", \"password\": \"...\"}")

// readLoginBody accepts exactly one JSON object with the keys name and
// password (strings, each once, case-sensitive) and nothing after it. It
// walks tokens because encoding/json would take the last duplicate key and
// match keys case-insensitively.
func readLoginBody(r io.Reader) (loginBody, error) {
	dec := json.NewDecoder(r)
	if t, err := dec.Token(); err != nil || t != json.Delim('{') {
		return loginBody{}, errLoginBody
	}
	var b loginBody
	seen := map[string]bool{}
	for dec.More() {
		k, err := dec.Token()
		key, ok := k.(string)
		if err != nil || !ok || seen[key] || (key != "name" && key != "password") {
			return loginBody{}, errLoginBody
		}
		v, err := dec.Token()
		s, ok := v.(string)
		if err != nil || !ok {
			return loginBody{}, errLoginBody
		}
		seen[key] = true
		if key == "name" {
			b.name = s
		} else {
			b.password = s
		}
	}
	if t, err := dec.Token(); err != nil || t != json.Delim('}') {
		return loginBody{}, errLoginBody
	}
	if _, err := dec.Token(); err != io.EOF {
		return loginBody{}, errLoginBody
	}
	if len(seen) != 2 {
		return loginBody{}, errLoginBody
	}
	return b, nil
}

// Login is POST /api/login {"name","password"}. Success sets the session
// cookie and returns {"name","id","csrf"}.
func (a *Auth) Login(w http.ResponseWriter, r *http.Request) {
	host := remoteHost(r)
	// Before reading the body: a limited address costs nothing.
	if !a.limiter.allow(host, a.Now()) {
		a.Log.Warn("login refused", "remote", host, "reason", "rate limited")
		writeJSON(w, http.StatusTooManyRequests, errLimited)
		return
	}
	// Only application/json: a cross-site text/plain form could otherwise
	// log the browser in as the attacker.
	if mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type")); err != nil || mt != "application/json" {
		writeJSON(w, http.StatusUnsupportedMediaType, errNotJSON)
		return
	}
	body, err := readLoginBody(http.MaxBytesReader(w, r.Body, loginBodyLimit))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errBadRequest)
		return
	}
	ap, err := a.Store.ApproverByName(r.Context(), body.name)
	found := err == nil && ap.Disabled == nil
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		a.internal(w, "approver lookup", err)
		return
	}
	phc := ap.PasswordHash
	if !found {
		// Same work either way, so timing does not reveal which names exist.
		phc = a.dummy()
	}
	ok := a.check(phc, body.password)
	if !found || !ok {
		a.failed(r.Context(), host, body.name)
		writeJSON(w, http.StatusUnauthorized, errUnauthenticated)
		return
	}
	// A login ends the session an old cookie names, so a copied cookie
	// does not outlive the approver's belief that it is gone.
	if c, err := r.Cookie(SessionCookie); err == nil && c.Value != "" {
		err := a.Store.RevokeSession(r.Context(), hashOf(c.Value), a.Now())
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			a.internal(w, "revoke previous session", err)
			return
		}
	}
	id, csrf := randomString(), randomString()
	now := a.Now()
	s := store.Session{ApproverID: ap.ID, ApproverName: ap.Name, CSRF: csrf, Created: now, Expires: now.Add(a.SessionTTL)}
	if err := a.Store.CreateSession(r.Context(), s, hashOf(id)); err != nil {
		a.internal(w, "create session", err)
		return
	}
	row := store.AuditRow{At: now, ActorID: ApproverID(ap.Name, ap.ID), ActorName: ap.Name, Kind: "login"}
	if _, err := a.Store.AppendAudit(r.Context(), row); err != nil {
		// No audit trail, no session.
		a.Store.RevokeSession(r.Context(), hashOf(id), now)
		a.internal(w, "audit login", err)
		return
	}
	http.SetCookie(w, sessionCookie(id, int(a.SessionTTL/time.Second)))
	out, _ := json.Marshal(map[string]string{"name": ap.Name, "id": ApproverID(ap.Name, ap.ID), "csrf": csrf})
	a.Log.Info("login", "remote", host, "approver", ap.Name)
	writeJSON(w, http.StatusOK, string(out))
}

func (a *Auth) dummy() string {
	a.dummyOnce.Do(func() { a.dummyHash, _ = HashPassword("cutover dummy password") })
	return a.dummyHash
}

func (a *Auth) failed(ctx context.Context, host, name string) {
	// Only a well-formed name is stored: the log must not become a place
	// to park arbitrary attacker text, or a mistyped password.
	if !NamePattern.MatchString(name) {
		name = invalidName
	}
	a.Log.Warn("login refused", "remote", host, "reason", "bad credentials")
	row := store.AuditRow{At: a.Now(), ActorName: name, Kind: "login-failed"}
	if _, err := a.Store.AppendAudit(ctx, row); err != nil {
		a.Log.Error("console auth: audit login-failed", "err", err)
	}
}

func (a *Auth) internal(w http.ResponseWriter, what string, err error) {
	a.Log.Error("console auth: "+what, "err", err)
	writeJSON(w, http.StatusInternalServerError, errInternal)
}

// sessionCookie builds the one cookie shape used to both set and clear.
func sessionCookie(value string, maxAge int) *http.Cookie {
	return &http.Cookie{
		Name:     SessionCookie,
		Value:    value,
		Path:     "/",
		MaxAge:   maxAge,
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteStrictMode,
	}
}

func clearCookie(w http.ResponseWriter) { http.SetCookie(w, sessionCookie("", -1)) }

// Logout revokes the session the cookie names and clears the cookie. It
// is mounted behind Require, so the session is known to be live.
func (a *Auth) Logout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(SessionCookie); err == nil && c.Value != "" {
		err := a.Store.RevokeSession(r.Context(), hashOf(c.Value), a.Now())
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			a.internal(w, "revoke session", err)
			return
		}
	}
	clearCookie(w)
	w.WriteHeader(http.StatusNoContent)
}

// Me is GET /api/me, behind Require: it returns {name,id,csrf,session_created}.
func (a *Auth) Me(w http.ResponseWriter, r *http.Request) {
	a.Require(func(w http.ResponseWriter, _ *http.Request, s store.Session) {
		out, _ := json.Marshal(map[string]string{
			"name":            s.ApproverName,
			"id":              ApproverID(s.ApproverName, s.ApproverID),
			"csrf":            s.CSRF,
			"session_created": s.Created.UTC().Format(time.RFC3339),
		})
		writeJSON(w, http.StatusOK, string(out))
	}).ServeHTTP(w, r)
}

// Require passes a request on only with a live session. A presented cookie
// that fails is cleared. Every method but GET must also carry the session's
// CSRF value; the session check comes first so a logged-out UI sees 401.
func (a *Auth) Require(next func(http.ResponseWriter, *http.Request, store.Session)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s, ok := a.Session(w, r)
		if !ok {
			return
		}
		next(w, r, s)
	})
}

// Session authenticates r exactly as Require does, writing the refusal
// itself when it returns false. Streaming handlers use it to keep one rule.
func (a *Auth) Session(w http.ResponseWriter, r *http.Request) (store.Session, bool) {
	c, err := r.Cookie(SessionCookie)
	if err != nil || c.Value == "" {
		writeJSON(w, http.StatusUnauthorized, errUnauthenticated)
		return store.Session{}, false
	}
	s, err := a.Store.SessionByHash(r.Context(), hashOf(c.Value), a.Now())
	if errors.Is(err, store.ErrNotFound) {
		clearCookie(w)
		writeJSON(w, http.StatusUnauthorized, errUnauthenticated)
		return store.Session{}, false
	}
	if err != nil {
		a.internal(w, "session lookup", err)
		return store.Session{}, false
	}
	if r.Method != http.MethodGet {
		got := r.Header.Get(CSRFHeader)
		if got == "" || subtle.ConstantTimeCompare([]byte(got), []byte(s.CSRF)) != 1 {
			writeJSON(w, http.StatusForbidden, errCSRF)
			return store.Session{}, false
		}
	}
	return s, true
}

// limiter is a fixed window per host: at most loginAttempts per
// loginWindow from the host's first attempt in the window. Every attempt
// counts, a success included.
type limiter struct {
	mu    sync.Mutex
	hosts map[string]*window
}

type window struct {
	start time.Time
	n     int
}

func newLimiter() *limiter { return &limiter{hosts: map[string]*window{}} }

func (l *limiter) allow(host string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for h, w := range l.hosts {
		if !now.Before(w.start.Add(loginWindow)) {
			delete(l.hosts, h)
		}
	}
	w, ok := l.hosts[host]
	if !ok {
		if len(l.hosts) >= limiterHosts {
			return false
		}
		w = &window{start: now}
		l.hosts[host] = w
	}
	if w.n >= loginAttempts {
		return false
	}
	w.n++
	return true
}

func (l *limiter) size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.hosts)
}
