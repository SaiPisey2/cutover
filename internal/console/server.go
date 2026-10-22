package console

// This file is the console listener's whole handler: the /api surface and
// the built UI behind one set of response headers.

import (
	"io"
	"io/fs"
	"net/http"
	"path"
	"strings"
	"time"
)

// ContentSecurityPolicy lets the UI load only its own bundle. No inline
// script or style is allowed: a name or note that somehow reached the
// page as markup still could not run. frame-ancestors keeps the approve
// button out of another site's frame.
const ContentSecurityPolicy = "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"

const errNotFound = `{"error":"not found"}`

// NewServer is the console listener's handler: /api/* goes to api (which
// owns its routes and its own authentication) and everything else is the
// UI from uiFS. a is accepted so the listener is wired to the same Auth
// as the API; the shell itself needs no session.
//
// The headers are set on the ResponseWriter before any handler runs, so
// every response carries them -- a 401, a 404, a static file -- without
// wrapping the writer. A wrapper would hide the connection's Flush and
// SetWriteDeadline from the event stream (unless it unwrapped), and the
// stream would then buffer events or lose its defence against a client
// that stops reading.
func NewServer(a *Auth, api http.Handler, uiFS fs.FS) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/api/", api)
	mux.Handle("/api", api)
	mux.Handle("/", static(uiFS))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", ContentSecurityPolicy)
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("X-Frame-Options", "DENY")
		// On every /api answer whatever its status: a cached 401 or error
		// page is as stale as a cached list. /api itself redirects to
		// /api/, and so do //api/me and /./api/me, whose redirects are
		// /api answers too.
		if isAPIPath(r.URL.Path) {
			h.Set("Cache-Control", "no-store")
		}
		// No Access-Control-* header is ever set: the UI is same-origin,
		// and another origin's script must not read a response.
		mux.ServeHTTP(w, r)
	})
}

// static serves the built UI. A path naming a file serves it; a path with
// no dot in its last segment is a client-side route (/migrations/<id>) and
// gets index.html so a reload or a pasted link lands in the app; any
// other miss is a 404, so a stale bundle name does not come back as HTML
// the browser then refuses as a script. Directories are never listed.
func static(uiFS fs.FS) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		// The mux matches on the escaped path, so /api%2fme is not an
		// /api route and lands here, decoded to /api/me. It is not a UI
		// route either: answering it with index.html would hand a script
		// probing the API the app, cached, where it expected a refusal.
		if isAPIPath(r.URL.Path) {
			writeJSON(w, http.StatusNotFound, errNotFound)
			return
		}
		name := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
		if name == "" {
			name = "index.html"
		}
		if serveFile(w, r, uiFS, name) {
			return
		}
		if strings.Contains(path.Base(name), ".") {
			http.NotFound(w, r)
			return
		}
		if !serveFile(w, r, uiFS, "index.html") {
			http.NotFound(w, r)
		}
	})
}

// isAPIPath reports a path under /api, as written or once cleaned: the
// decoded /api/../index.html is an /api path as sent, and //api/me is one
// once cleaned. Either way it gets the /api treatment, never the UI's.
func isAPIPath(p string) bool {
	for _, q := range []string{p, path.Clean("/" + p)} {
		if q == "/api" || strings.HasPrefix(q, "/api/") {
			return true
		}
	}
	return false
}

// serveFile serves name from fsys if it is a regular file, and reports
// whether it did. ServeContent picks the type from the extension and
// answers HEAD itself.
func serveFile(w http.ResponseWriter, r *http.Request, fsys fs.FS, name string) bool {
	if !fs.ValidPath(name) {
		return false
	}
	f, err := fsys.Open(name)
	if err != nil {
		return false
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() {
		return false
	}
	rs, ok := f.(io.ReadSeeker)
	if !ok {
		// embed.FS files seek; a filesystem whose files do not is a
		// wiring mistake, reported rather than served half-way.
		http.Error(w, "ui file does not seek", http.StatusInternalServerError)
		return true
	}
	if name == "index.html" {
		// index.html names the hashed bundle; a cached copy after an
		// upgrade would point at files this binary no longer has.
		w.Header().Set("Cache-Control", "no-cache")
	}
	// Ranges are not offered: the files are small, and ServeContent's
	// 416 for a bad range strips Cache-Control, so index.html would go out
	// cacheable. Without the header every request is a plain 200.
	r.Header.Del("Range")
	r.Header.Del("If-Range")
	// The modification time is left zero: embedded files have none, and a
	// fake one would make Last-Modified lie.
	http.ServeContent(w, r, name, time.Time{}, rs)
	return true
}
