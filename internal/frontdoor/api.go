package frontdoor

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/SaiPisey2/cutover/internal/traffic"
)

// API is the server's internal HTTP API: the worker's activities and the
// CLI use it to change and read routes. It listens on loopback only and
// requires a bearer token; Plan C replaces the token with accounts.
type API struct {
	XDS   *XDS
	Admin *Admin
	Token string
}

type Health struct {
	Source string `json:"source"`
	Target string `json:"target"`
}

type Stats struct {
	Source *traffic.Counters `json:"source,omitempty"`
	Target *traffic.Counters `json:"target,omitempty"`
}

const maxBody = 64 << 10

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /v1/routes/{owner}/{service}", a.put)
	mux.HandleFunc("GET /v1/routes", a.list)
	mux.HandleFunc("GET /v1/routes/{owner}/{service}/health", a.health)
	mux.HandleFunc("GET /v1/routes/{owner}/{service}/stats", a.stats)
	return a.auth(mux)
}

func (a *API) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || a.Token == "" || subtle.ConstantTimeCompare([]byte(got), []byte(a.Token)) != 1 {
			writeErr(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func (a *API) put(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBody)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var rt Route
	if err := dec.Decode(&rt); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeErr(w, http.StatusRequestEntityTooLarge, "request body over 64 KiB")
			return
		}
		writeErr(w, http.StatusBadRequest, "body is not a route: "+err.Error())
		return
	}
	if rt.Owner != r.PathValue("owner") || rt.Service != r.PathValue("service") {
		writeErr(w, http.StatusBadRequest, "owner and service in the body must match the path")
		return
	}
	switch err := a.XDS.Apply(r.Context(), rt); {
	case errors.Is(err, ErrInvalid):
		writeErr(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, ErrConflict):
		writeErr(w, http.StatusConflict, err.Error())
	case err != nil:
		writeErr(w, http.StatusBadGateway, err.Error())
	default:
		writeJSON(w, http.StatusOK, struct{}{})
	}
}

func (a *API) list(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string][]Route{"routes": a.XDS.Routes()})
}

func (a *API) route(w http.ResponseWriter, r *http.Request) (Route, bool) {
	rt, ok := a.XDS.Route(r.PathValue("owner") + "/" + r.PathValue("service"))
	if !ok {
		writeErr(w, http.StatusNotFound, "no such route")
	}
	return rt, ok
}

func (a *API) health(w http.ResponseWriter, r *http.Request) {
	rt, ok := a.route(w, r)
	if !ok {
		return
	}
	var h Health
	sides := []struct {
		side string
		has  bool
		dst  *string
	}{{SideSource, rt.Source != nil, &h.Source}, {SideTarget, rt.Target != nil, &h.Target}}
	for _, s := range sides {
		if !s.has {
			*s.dst = Absent
			continue
		}
		st, err := a.Admin.Health(r.Context(), ClusterName(rt.Owner, rt.Service, s.side))
		if err != nil {
			writeErr(w, http.StatusBadGateway, err.Error())
			return
		}
		*s.dst = st
	}
	writeJSON(w, http.StatusOK, h)
}

func (a *API) stats(w http.ResponseWriter, r *http.Request) {
	rt, ok := a.route(w, r)
	if !ok {
		return
	}
	all, err := a.Admin.Stats(r.Context())
	if err != nil {
		writeErr(w, http.StatusBadGateway, err.Error())
		return
	}
	var st Stats
	pick := func(side string) *traffic.Counters {
		c := all[ClusterName(rt.Owner, rt.Service, side)]
		return &c
	}
	if rt.Source != nil {
		st.Source = pick(SideSource)
	}
	if rt.Target != nil {
		st.Target = pick(SideTarget)
	}
	writeJSON(w, http.StatusOK, st)
}

// ListenAPI listens on a loopback host:port only (see RequireLoopback).
func ListenAPI(addr string) (net.Listener, error) {
	if err := RequireLoopback(addr); err != nil {
		return nil, fmt.Errorf("api: %w", err)
	}
	return net.Listen("tcp", addr)
}
