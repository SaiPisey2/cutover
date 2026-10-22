package console

// This file is the console's JSON API: migrations, gate decisions,
// rollback, front-door failures and the audit log. Every route but login
// sits behind Require. Every error body is a fixed string; the real error
// goes to the log.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/SaiPisey2/cutover/internal/flow"
	"github.com/SaiPisey2/cutover/internal/frontdoor"
	"github.com/SaiPisey2/cutover/internal/plan"
	"github.com/SaiPisey2/cutover/internal/store"
)

// APIStore is the slice of *store.Store the API needs.
type APIStore interface {
	AppendAudit(ctx context.Context, r store.AuditRow) (int64, error)
	AuditPage(ctx context.Context, migration string, before int64, limit int) ([]store.AuditRow, error)
	ListApprovers(ctx context.Context) ([]store.Approver, error)
}

// FailureSource is the front door's recent failed requests per routed
// service; *frontdoor.Failures implements it.
type FailureSource interface {
	Failures(owner, service string) (source, target []frontdoor.Failure)
}

// Deps is what the API runs on. Failures, Log and Context may be nil. The
// clock is the Auth's, so sessions and the switch's freshness rule share
// one.
type Deps struct {
	Temporal Temporal
	Store    APIStore
	Failures FailureSource
	Log      *slog.Logger
	// Context bounds the live stream: when it ends, the shared poller
	// stops and every open stream closes, so a server cancels it before
	// Shutdown (which would otherwise wait on streams that never idle).
	// nil means never.
	Context context.Context
}

const (
	apiBodyLimit = 64 << 10
	// temporalTimeout bounds each Temporal query a request makes.
	temporalTimeout = 10 * time.Second
	// switchFreshness is how old a session may be to approve the switch.
	switchFreshness = 15 * time.Minute
	gateSwitch      = "switch"
	auditLimit      = 100
	auditLimitMax   = 500
)

// actionTimeout bounds a signal or start, on its own clock;
// failedAuditTimeout bounds the row recording that one failed, on another.
// Vars so a test can shrink them.
var (
	actionTimeout      = 10 * time.Second
	failedAuditTimeout = 5 * time.Second
)

// gatePattern is the shape of a gate name, retry gates included.
var gatePattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)

const (
	errNoMigration    = `{"error":"no such migration"}`
	errTemporalDown   = `{"error":"temporal unavailable"}`
	errExists         = `{"error":"migration already exists"}`
	errBadID          = `{"error":"invalid migration id"}`
	errBadGate        = `{"error":"invalid gate name"}`
	errBadDecision    = `{"error":"decision must be approve or reject"}`
	errNeedReason     = `{"error":"a reason is required"}`
	errBadQuery       = `{"error":"bad query parameter"}`
	errGateNotOpen    = `{"error":"gate is not open"}`
	errStaleEvidence  = `{"error":"evidence changed; review the gate again"}`
	errSignInAgain    = `{"error":"sign in again to decide the switch"}`
	errDifferentHuman = `{"error":"the second approval must come from a different person"}`
	errAlreadyDecided = `{"error":"you already approved this gate"}`
	errNotYAML        = "plan is not valid YAML"
)

type api struct {
	auth  *Auth
	d     Deps
	log   *slog.Logger
	locks *keyedLocks
	hub   *hub
	slots *streamSlots
}

// Routes mounts the whole /api surface on mux.
func Routes(mux *http.ServeMux, a *Auth, d Deps) { routes(mux, a, d) }

// routes is Routes, returning the handlers so a test can reach the hub.
func routes(mux *http.ServeMux, a *Auth, d Deps) *api {
	log := d.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	h := &api{auth: a, d: d, log: log, locks: &keyedLocks{m: map[string]*keyedLock{}},
		slots: &streamSlots{per: map[string]int{}}}
	h.hub = newHub(d.Context, h, log)
	mux.HandleFunc("POST /api/login", a.Login)
	mux.HandleFunc("POST /api/logout", a.Logout) // Require inside
	mux.HandleFunc("GET /api/me", a.Me)          // Require inside
	mux.Handle("GET /api/migrations", a.Require(h.list))
	mux.Handle("POST /api/migrations", a.Require(h.start))
	mux.Handle("GET /api/migrations/{id}", a.Require(h.status))
	mux.Handle("POST /api/migrations/{id}/gates/{gate}", a.Require(h.decide))
	mux.Handle("POST /api/migrations/{id}/rollback", a.Require(h.rollback))
	mux.Handle("GET /api/migrations/{id}/failures", a.Require(h.failures))
	mux.Handle("GET /api/audit", a.Require(h.audit))
	// The stream authenticates itself (with Session), so it can key its
	// slots on the cookie it was given.
	mux.HandleFunc("GET /api/stream", h.stream)

	// Anything else under /api, a known path with the wrong method
	// included, is a 401 without a session, so probing learns nothing
	// before signing in.
	mux.Handle("/api/", a.Require(func(w http.ResponseWriter, _ *http.Request, _ store.Session) {
		writeJSON(w, http.StatusNotFound, errNotFound)
	}))
	return h
}

// ---- helpers ----

func reply(w http.ResponseWriter, code int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		code, b = http.StatusInternalServerError, []byte(errInternal)
	}
	writeJSON(w, code, string(b))
}

func (h *api) internal(w http.ResponseWriter, what string, err error) {
	h.log.Error("console api: "+what, "err", err)
	writeJSON(w, http.StatusInternalServerError, errInternal)
}

// temporalFailed answers a Temporal error with its fixed status and text.
func (h *api) temporalFailed(w http.ResponseWriter, what string, err error) {
	switch {
	case errors.Is(err, ErrNoMigration):
		writeJSON(w, http.StatusNotFound, errNoMigration)
	case errors.Is(err, ErrExists):
		writeJSON(w, http.StatusConflict, errExists)
	case errors.Is(err, ErrTemporal):
		h.log.Warn("console api: "+what, "err", err)
		writeJSON(w, http.StatusServiceUnavailable, errTemporalDown)
	default:
		h.internal(w, what, err)
	}
}

func tctx(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), temporalTimeout)
}

// actionCtx is the context for what follows an action's audit row: the
// signal or start, and the row that records it failed. Once the row is
// written the action must be carried through or marked failed, so a
// client that disconnects cannot cancel it half way.
func actionCtx(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(r.Context()), actionTimeout)
}

// readBody decodes exactly one JSON object into v: application/json only,
// at most apiBodyLimit bytes, no unknown fields, nothing after it.
func readBody(w http.ResponseWriter, r *http.Request, v any) bool {
	if mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type")); err != nil || mt != "application/json" {
		writeJSON(w, http.StatusUnsupportedMediaType, errNotJSON)
		return false
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, apiBodyLimit))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeJSON(w, http.StatusBadRequest, errBadRequest)
		return false
	}
	if _, err := dec.Token(); err != io.EOF {
		writeJSON(w, http.StatusBadRequest, errBadRequest)
		return false
	}
	return true
}

// migrationID reads and checks the {id} path value.
func migrationID(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.PathValue("id")
	if !plan.NamePattern.MatchString(id) {
		writeJSON(w, http.StatusBadRequest, errBadID)
		return "", false
	}
	return id, true
}

// actor is the session's identity as the workflow and audit log record it.
func actor(s store.Session) string { return ApproverID(s.ApproverName, s.ApproverID) }

// keyedLocks serializes work per migration: query, checks, audit and
// signal run as one step, so two decisions cannot both pass the checks on
// the same evidence. Entries are dropped when unused.
type keyedLocks struct {
	mu sync.Mutex
	m  map[string]*keyedLock
}

type keyedLock struct {
	mu   sync.Mutex
	refs int
}

func (k *keyedLocks) lock(key string) func() {
	k.mu.Lock()
	l := k.m[key]
	if l == nil {
		l = &keyedLock{}
		k.m[key] = l
	}
	l.refs++
	k.mu.Unlock()
	l.mu.Lock()
	return func() {
		l.mu.Unlock()
		k.mu.Lock()
		if l.refs--; l.refs == 0 {
			delete(k.m, key)
		}
		k.mu.Unlock()
	}
}

// ---- migrations ----

// migrationRow is a MigrationSummary as JSON.
type migrationRow struct {
	ID      string    `json:"id"`
	Phase   string    `json:"phase"`
	State   string    `json:"state"`
	Gate    string    `json:"gate"`
	Started time.Time `json:"started"`
}

func migrationRows(list []MigrationSummary) []migrationRow {
	out := make([]migrationRow, 0, len(list))
	for _, m := range list {
		out = append(out, migrationRow{ID: m.ID, Phase: m.Phase, State: m.State, Gate: m.Gate, Started: m.Started.UTC()})
	}
	return out
}

func (h *api) list(w http.ResponseWriter, r *http.Request, _ store.Session) {
	ctx, cancel := tctx(r)
	defer cancel()
	list, err := h.d.Temporal.List(ctx)
	if err != nil {
		h.temporalFailed(w, "list", err)
		return
	}
	reply(w, http.StatusOK, migrationRows(list))
}

// StatusReply is GET /api/migrations/{id}.
type StatusReply struct {
	Status         flow.Status           `json:"status"`
	EvidenceDigest string                `json:"evidence_digest"`
	Replicate      *flow.ReplicateStatus `json:"replicate,omitempty"`
	// Names maps each "name#id" in the gate's approvals and the events to
	// the account's name, disabled accounts included.
	Names map[string]string `json:"names"`
}

func (h *api) status(w http.ResponseWriter, r *http.Request, _ store.Session) {
	id, ok := migrationID(w, r)
	if !ok {
		return
	}
	out, err := h.statusReply(r.Context(), id)
	if err != nil {
		h.temporalFailed(w, "status", err)
		return
	}
	reply(w, http.StatusOK, out)
}

// statusReply builds the status answer. Its errors are Temporal's mapped
// errors, or a store error, which temporalFailed answers as a 500.
func (h *api) statusReply(ctx context.Context, id string) (StatusReply, error) {
	tc, cancel := context.WithTimeout(ctx, temporalTimeout)
	defer cancel()
	st, err := h.d.Temporal.Status(tc, id)
	if err != nil {
		return StatusReply{}, err
	}
	out := StatusReply{Status: st, EvidenceDigest: flow.EvidenceDigest(st)}
	if st.ReplicateWorkflowID != "" {
		// The child's progress is extra detail: without it the status is
		// still true, so a failed query leaves it out.
		rs, err := h.d.Temporal.Replicate(tc, st.ReplicateWorkflowID)
		if err == nil {
			out.Replicate = &rs
		} else {
			h.log.Warn("console api: replicate status", "migration", id, "err", err)
		}
	}
	out.Names, err = h.names(ctx, st)
	if err != nil {
		return StatusReply{}, fmt.Errorf("names: %w", err)
	}
	return out, nil
}

func (h *api) names(ctx context.Context, st flow.Status) (map[string]string, error) {
	var seen []string
	if st.Gate != nil {
		seen = append(seen, st.Gate.Approvals...)
	}
	for _, e := range st.Events {
		seen = append(seen, e.Actor)
	}
	out := map[string]string{}
	if !slices.ContainsFunc(seen, func(s string) bool { return strings.Contains(s, "#") }) {
		return out, nil
	}
	accounts, err := h.d.Store.ListApprovers(ctx)
	if err != nil {
		return nil, err
	}
	byID := make(map[string]string, len(accounts))
	for _, a := range accounts {
		byID[a.ID] = a.Name
	}
	for _, s := range seen {
		if _, aid, ok := strings.Cut(s, "#"); ok {
			if name, ok := byID[aid]; ok {
				out[s] = name
			}
		}
	}
	return out, nil
}

type startBody struct {
	ID   string `json:"id"`
	Plan string `json:"plan"`
}

func (h *api) start(w http.ResponseWriter, r *http.Request, s store.Session) {
	var body startBody
	if !readBody(w, r, &body) {
		return
	}
	p, msg := parsePlan([]byte(body.Plan))
	if msg != "" {
		reply(w, http.StatusBadRequest, map[string]string{"error": msg})
		return
	}
	id := body.ID
	if id == "" {
		id = p.Name
	}
	if !plan.NamePattern.MatchString(id) {
		writeJSON(w, http.StatusBadRequest, errBadID)
		return
	}
	unlock := h.locks.lock(id)
	defer unlock()
	sum := sha256.Sum256([]byte(body.Plan))
	row := store.AuditRow{At: h.auth.Now(), ActorID: actor(s), ActorName: s.ApproverName,
		Migration: id, Kind: "migration-started", Evidence: hex.EncodeToString(sum[:])}
	// The log is written first: a start that is not in it does not happen.
	if _, err := h.d.Store.AppendAudit(r.Context(), row); err != nil {
		h.internal(w, "audit migration-started", err)
		return
	}
	ctx, cancel := actionCtx(r)
	defer cancel()
	if err := h.d.Temporal.Start(ctx, id, p); err != nil {
		h.auditFailed(r, row, "migration-start-failed")
		h.temporalFailed(w, "start", err)
		return
	}
	h.log.Info("migration started", "migration", id, "by", s.ApproverName)
	reply(w, http.StatusCreated, map[string]string{"id": id})
}

// auditFailed follows an action's row with one saying it was not
// confirmed. A "*-failed" kind means exactly that, not "did not happen": a
// signal that timed out may still have reached the workflow, whose own
// history is the final word.
//
// It runs on its own context, cut loose from the client and from the
// action's deadline: the likeliest failure is that deadline running out,
// and a row written on the dead context would be refused.
func (h *api) auditFailed(r *http.Request, row store.AuditRow, kind string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), failedAuditTimeout)
	defer cancel()
	row.Kind, row.At = kind, h.auth.Now()
	if row.Note == "" {
		row.Note = notConfirmed
	} else {
		row.Note = notConfirmed + "; note: " + row.Note
	}
	if _, err := h.d.Store.AppendAudit(ctx, row); err != nil {
		h.log.Error("console api: audit "+kind, "migration", row.Migration, "err", err)
	}
}

const notConfirmed = "not confirmed (it may still have reached the workflow)"

var yamlLine = regexp.MustCompile(`line (\d+)`)

// parsePlan decodes, defaults and validates a plan the way plan.Parse
// does, but returns the client's message apart from the cause: a YAML
// problem is a fixed text with at most a line number, since the decoder's
// message quotes the input; a validation problem is the first one Validate
// reports, which names fields and never holds a secret (plans hold refs).
func parsePlan(data []byte) (plan.Plan, string) {
	var p plan.Plan
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&p); err != nil {
		if m := yamlLine.FindStringSubmatch(err.Error()); m != nil {
			return plan.Plan{}, errNotYAML + " (line " + m[1] + ")"
		}
		return plan.Plan{}, errNotYAML
	}
	var extra yaml.Node
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return plan.Plan{}, errNotYAML
	}
	plan.ApplyDefaults(&p)
	if err := p.Validate(); err != nil {
		first := err
		if j, ok := err.(interface{ Unwrap() []error }); ok && len(j.Unwrap()) > 0 {
			first = j.Unwrap()[0]
		}
		return plan.Plan{}, "invalid plan: " + first.Error()
	}
	return p, ""
}

// ---- decisions ----

type decideBody struct {
	Decision       flow.Decision `json:"decision"`
	Note           string        `json:"note"`
	EvidenceDigest string        `json:"evidence_digest"`
}

func (h *api) decide(w http.ResponseWriter, r *http.Request, s store.Session) {
	id, ok := migrationID(w, r)
	if !ok {
		return
	}
	gate := r.PathValue("gate")
	if !gatePattern.MatchString(gate) {
		writeJSON(w, http.StatusBadRequest, errBadGate)
		return
	}
	var body decideBody
	if !readBody(w, r, &body) {
		return
	}
	if body.Decision != flow.Approve && body.Decision != flow.Reject {
		writeJSON(w, http.StatusBadRequest, errBadDecision)
		return
	}

	unlock := h.locks.lock(id)
	defer unlock()
	ctx, cancel := tctx(r)
	defer cancel()
	st, err := h.d.Temporal.Status(ctx, id)
	if err != nil {
		h.temporalFailed(w, "status for decision", err)
		return
	}
	g := st.Gate
	if g == nil || g.Name != gate {
		writeJSON(w, http.StatusConflict, errGateNotOpen)
		return
	}
	digest := flow.EvidenceDigest(st)
	if body.EvidenceDigest != digest {
		writeJSON(w, http.StatusConflict, errStaleEvidence)
		return
	}
	me := actor(s)
	twoPerson := gate == gateSwitch || g.Needs > 1
	if gate == gateSwitch && body.Decision == flow.Approve && h.auth.Now().Sub(s.Created) > switchFreshness {
		writeJSON(w, http.StatusForbidden, errSignInAgain)
		return
	}
	// An account decides a gate once. On a two-person gate the second
	// approver must also be another person by name: a disabled account's
	// name, reused, is still the same person.
	if slices.Contains(g.Approvals, me) {
		writeJSON(w, http.StatusConflict, errAlreadyDecided)
		return
	}
	if twoPerson && body.Decision == flow.Approve && sameName(g.Approvals, s.ApproverName) {
		writeJSON(w, http.StatusConflict, errDifferentHuman)
		return
	}

	row := store.AuditRow{At: h.auth.Now(), ActorID: me, ActorName: s.ApproverName, Migration: id,
		Kind: "gate-decision", Gate: gate, Decision: string(body.Decision), Note: body.Note, Evidence: digest}
	if _, err := h.d.Store.AppendAudit(r.Context(), row); err != nil {
		h.internal(w, "audit gate-decision", err)
		return
	}
	sig := flow.GateSignal{Gate: gate, Decision: body.Decision, ApproverID: me, Note: body.Note}
	actx, acancel := actionCtx(r)
	defer acancel()
	if err := h.d.Temporal.Gate(actx, id, sig); err != nil {
		h.auditFailed(r, row, "gate-decision-failed")
		h.temporalFailed(w, "gate signal", err)
		return
	}
	h.log.Info("gate decided", "migration", id, "gate", gate, "decision", body.Decision, "by", s.ApproverName)
	reply(w, http.StatusAccepted, map[string]bool{"accepted": true})
}

// sameName reports whether any approval ("name#id") names this person,
// case-insensitively.
func sameName(approvals []string, name string) bool {
	for _, a := range approvals {
		n, _, _ := strings.Cut(a, "#")
		if strings.EqualFold(n, name) {
			return true
		}
	}
	return false
}

// ---- rollback ----

type rollbackBody struct {
	Reason string `json:"reason"`
}

func (h *api) rollback(w http.ResponseWriter, r *http.Request, s store.Session) {
	id, ok := migrationID(w, r)
	if !ok {
		return
	}
	var body rollbackBody
	if !readBody(w, r, &body) {
		return
	}
	if strings.TrimSpace(body.Reason) == "" {
		writeJSON(w, http.StatusBadRequest, errNeedReason)
		return
	}
	unlock := h.locks.lock(id)
	defer unlock()
	ctx, cancel := tctx(r)
	defer cancel()
	st, err := h.d.Temporal.Status(ctx, id)
	if err != nil {
		h.temporalFailed(w, "status for rollback", err)
		return
	}
	me := actor(s)
	row := store.AuditRow{At: h.auth.Now(), ActorID: me, ActorName: s.ApproverName, Migration: id,
		Kind: "rollback", Note: body.Reason, Evidence: flow.EvidenceDigest(st)}
	if _, err := h.d.Store.AppendAudit(r.Context(), row); err != nil {
		h.internal(w, "audit rollback", err)
		return
	}
	actx, acancel := actionCtx(r)
	defer acancel()
	if err := h.d.Temporal.Rollback(actx, id, flow.RollbackSignal{ApproverID: me, Reason: body.Reason}); err != nil {
		h.auditFailed(r, row, "rollback-failed")
		h.temporalFailed(w, "rollback signal", err)
		return
	}
	h.log.Info("rollback requested", "migration", id, "by", s.ApproverName)
	reply(w, http.StatusAccepted, map[string]bool{"accepted": true})
}

// ---- failures ----

type serviceFailures struct {
	Service string              `json:"service"`
	Source  []frontdoor.Failure `json:"source"`
	Target  []frontdoor.Failure `json:"target"`
}

func (h *api) failures(w http.ResponseWriter, r *http.Request, _ store.Session) {
	id, ok := migrationID(w, r)
	if !ok {
		return
	}
	ctx, cancel := tctx(r)
	defer cancel()
	st, err := h.d.Temporal.Status(ctx, id)
	if err != nil {
		h.temporalFailed(w, "status for failures", err)
		return
	}
	// The front door keys routes by the plan's name, not the migration's.
	out := make([]serviceFailures, 0, len(st.Routed))
	for _, svc := range st.Routed {
		f := serviceFailures{Service: svc, Source: []frontdoor.Failure{}, Target: []frontdoor.Failure{}}
		if h.d.Failures != nil {
			src, tgt := h.d.Failures.Failures(st.PlanName, svc)
			if src != nil {
				f.Source = src
			}
			if tgt != nil {
				f.Target = tgt
			}
		}
		out = append(out, f)
	}
	reply(w, http.StatusOK, map[string][]serviceFailures{"services": out})
}

// ---- audit ----

// AuditEntry is one audit row as JSON.
type AuditEntry struct {
	ID        int64     `json:"id"`
	At        time.Time `json:"at"`
	ActorID   string    `json:"actor_id"`
	ActorName string    `json:"actor_name"`
	Migration string    `json:"migration"`
	Kind      string    `json:"kind"`
	Gate      string    `json:"gate"`
	Decision  string    `json:"decision"`
	Note      string    `json:"note"`
	Evidence  string    `json:"evidence"`
}

// queryInt reads an optional positive integer: absent is def, anything
// else that is not an integer >= 1 is an error.
func queryInt(r *http.Request, name string, def int64) (int64, bool) {
	v := r.URL.Query().Get(name)
	if v == "" {
		return def, true
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 1 {
		return 0, false
	}
	return n, true
}

func (h *api) audit(w http.ResponseWriter, r *http.Request, _ store.Session) {
	mig := r.URL.Query().Get("migration")
	before, ok1 := queryInt(r, "before", 0)
	limit, ok2 := queryInt(r, "limit", auditLimit)
	if !ok1 || !ok2 || (mig != "" && !plan.NamePattern.MatchString(mig)) {
		writeJSON(w, http.StatusBadRequest, errBadQuery)
		return
	}
	rows, err := h.d.Store.AuditPage(r.Context(), mig, before, int(min(limit, auditLimitMax)))
	if err != nil {
		h.internal(w, "audit page", err)
		return
	}
	out := make([]AuditEntry, 0, len(rows))
	for _, x := range rows {
		out = append(out, AuditEntry{ID: x.ID, At: x.At.UTC(), ActorID: x.ActorID, ActorName: x.ActorName,
			Migration: x.Migration, Kind: x.Kind, Gate: x.Gate, Decision: x.Decision, Note: x.Note, Evidence: x.Evidence})
	}
	reply(w, http.StatusOK, out)
}
