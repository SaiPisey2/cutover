package console

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SaiPisey2/cutover/internal/flow"
	"github.com/SaiPisey2/cutover/internal/frontdoor"
	"github.com/SaiPisey2/cutover/internal/plan"
	"github.com/SaiPisey2/cutover/internal/store"
	"github.com/stretchr/testify/require"
)

// fakeTemporal records every call. Accepted gate approvals are applied to
// the status, so the evidence digest moves the way the real workflow's does.
type fakeTemporal struct {
	mu        sync.Mutex
	statuses  map[string]*flow.Status
	replicate map[string]flow.ReplicateStatus
	list      []MigrationSummary
	err       error // returned by every call when set
	// meet, when set, makes Status wait (up to a second) until that many
	// Status calls are in flight, so a race that is possible is certain.
	meet    int
	arrived int
	gateErr error
	opened  int  // gates opened so far, for distinct opening times
	lag     bool // accepted signals are not yet visible in the status
	// action, when set, runs inside Start, Gate and Rollback with their
	// context; its error is the call's.
	action    func(ctx context.Context) error
	gates     []flow.GateSignal
	rollbacks []flow.RollbackSignal
	starts    []string
}

func newFake() *fakeTemporal {
	return &fakeTemporal{statuses: map[string]*flow.Status{}, replicate: map[string]flow.ReplicateStatus{}}
}

func (f *fakeTemporal) open(id, gate string, needs int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.opened++
	f.statuses[id] = &flow.Status{MigrationID: id, PlanName: "shop", Routed: []string{"api", "web"},
		Phase: gate, State: flow.StateWaiting,
		Gate:   &flow.Gate{Name: gate, Phase: gate, Needs: needs, Approvals: []string{}, Summary: "s"},
		Events: []flow.Event{{At: time.Unix(int64(f.opened), 0), Phase: gate, Kind: "gate-opened"}}}
}

func (f *fakeTemporal) List(context.Context) ([]MigrationSummary, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.list, f.err
}

func (f *fakeTemporal) Status(_ context.Context, id string) (flow.Status, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp, err := f.snapshot(id)
	// The answer is read on arrival, then held until the others arrive.
	f.arrived++
	for deadline := time.Now().Add(time.Second); f.arrived < f.meet && time.Now().Before(deadline); {
		f.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
		f.mu.Lock()
	}
	return cp, err
}

func (f *fakeTemporal) snapshot(id string) (flow.Status, error) {
	if f.err != nil {
		return flow.Status{}, f.err
	}
	st, ok := f.statuses[id]
	if !ok {
		return flow.Status{}, ErrNoMigration
	}
	cp := *st
	if st.Gate != nil {
		g := *st.Gate
		g.Approvals = slices.Clone(g.Approvals)
		cp.Gate = &g
	}
	return cp, nil
}

func (f *fakeTemporal) Replicate(_ context.Context, wid string) (flow.ReplicateStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.replicate[wid]
	if !ok {
		return r, ErrNoMigration
	}
	return r, nil
}

func (f *fakeTemporal) Start(ctx context.Context, id string, _ plan.Plan) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	if f.action != nil {
		if err := f.action(ctx); err != nil {
			return err
		}
	}
	if _, ok := f.statuses[id]; ok {
		return ErrExists
	}
	f.starts = append(f.starts, id)
	f.statuses[id] = &flow.Status{MigrationID: id, State: flow.StateRunning}
	return nil
}

func (f *fakeTemporal) Gate(ctx context.Context, id string, s flow.GateSignal) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	if f.action != nil {
		if err := f.action(ctx); err != nil {
			return err
		}
	}
	if f.gateErr != nil {
		return f.gateErr
	}
	f.gates = append(f.gates, s)
	if st := f.statuses[id]; !f.lag && st != nil && st.Gate != nil && s.Decision == flow.Approve {
		st.Gate.Approvals = append(st.Gate.Approvals, s.ApproverID)
		st.Events = append(st.Events, flow.Event{Kind: "approved", Actor: s.ApproverID})
	}
	return nil
}

func (f *fakeTemporal) Rollback(ctx context.Context, _ string, s flow.RollbackSignal) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	if f.action != nil {
		if err := f.action(ctx); err != nil {
			return err
		}
	}
	f.rollbacks = append(f.rollbacks, s)
	return nil
}

func (f *fakeTemporal) gateCalls() []flow.GateSignal {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.gates)
}

type fakeFailures map[string][2][]frontdoor.Failure

func (f fakeFailures) Failures(owner, service string) (source, target []frontdoor.Failure) {
	v := f[owner+"/"+service]
	return v[0], v[1]
}

// failingAudit is a store whose audit writes fail.
type failingAudit struct{ APIStore }

func (failingAudit) AppendAudit(context.Context, store.AuditRow) (int64, error) {
	return 0, errors.New("disk full at /secret/path")
}

type apiEnv struct {
	*env
	fake *fakeTemporal
	mux  *http.ServeMux
	api  *api
	n    int
}

func newAPIEnv(t *testing.T, mod ...func(*Deps)) *apiEnv {
	t.Helper()
	e := newEnv(t)
	fake := newFake()
	d := Deps{Temporal: fake, Store: e.st, Log: e.a.Log}
	for _, m := range mod {
		m(&d)
	}
	mux := http.NewServeMux()
	h := routes(mux, e.a, d)
	return &apiEnv{env: e, fake: fake, mux: mux, api: h}
}

// user is a session made directly in the store, so tests do not pay for
// an argon2id check per login.
type user struct{ cookie, csrf string }

func (e *apiEnv) account(id, name string) {
	e.t.Helper()
	require.NoError(e.t, e.st.CreateApprover(context.Background(), store.Approver{
		ID: id, Name: name, PasswordHash: "x", Created: e.now}))
}

func (e *apiEnv) sessionAged(approverID string, age time.Duration) user {
	e.t.Helper()
	e.n++
	u := user{cookie: fmt.Sprintf("cookie-%d", e.n), csrf: fmt.Sprintf("csrf-%d", e.n)}
	now := e.a.Now()
	require.NoError(e.t, e.st.CreateSession(context.Background(), store.Session{
		ApproverID: approverID, CSRF: u.csrf, Created: now.Add(-age), Expires: now.Add(time.Hour)}, hashOf(u.cookie)))
	return u
}

func (e *apiEnv) session(approverID string) user { return e.sessionAged(approverID, time.Minute) }

func (e *apiEnv) do(u *user, method, path, body string) *httptest.ResponseRecorder {
	e.t.Helper()
	return e.doCtx(context.Background(), u, method, path, body)
}

func (e *apiEnv) doCtx(ctx context.Context, u *user, method, path, body string) *httptest.ResponseRecorder {
	e.t.Helper()
	r := httptest.NewRequestWithContext(ctx, method, path, strings.NewReader(body))
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	if u != nil {
		r.AddCookie(&http.Cookie{Name: SessionCookie, Value: u.cookie})
		r.Header.Set(CSRFHeader, u.csrf)
	}
	w := httptest.NewRecorder()
	e.mux.ServeHTTP(w, r)
	return w
}

// digest re-fetches the evidence digest the way the UI would.
func (e *apiEnv) digest(u user, id string) string {
	e.t.Helper()
	w := e.do(&u, "GET", "/api/migrations/"+id, "")
	require.Equal(e.t, 200, w.Code, w.Body.String())
	var out struct {
		Digest string `json:"evidence_digest"`
	}
	require.NoError(e.t, json.Unmarshal(w.Body.Bytes(), &out))
	return out.Digest
}

func decision(dec, note, digest string) string {
	b, _ := json.Marshal(map[string]string{"decision": dec, "note": note, "evidence_digest": digest})
	return string(b)
}

func (e *apiEnv) decide(u user, id, gate, dec string) *httptest.ResponseRecorder {
	e.t.Helper()
	return e.do(&u, "POST", "/api/migrations/"+id+"/gates/"+gate, decision(dec, "ok", e.digest(u, id)))
}

func errBody(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	var out map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out), w.Body.String())
	return out["error"]
}

func TestDecideSignalsWithTheSessionAccount(t *testing.T) {
	e := newAPIEnv(t)
	e.fake.open("m1", "prepare", 1)
	alice := e.session("a1")

	w := e.decide(alice, "m1", "prepare", "approve")
	require.Equal(t, 202, w.Code, w.Body.String())
	require.Equal(t, []flow.GateSignal{{Gate: "prepare", Decision: flow.Approve, ApproverID: "alice#a1", Note: "ok"}}, e.fake.gateCalls())

	// The approver is never the client's to name.
	e.fake.open("m2", "prepare", 1)
	body := `{"decision":"approve","note":"","evidence_digest":"` + e.digest(alice, "m2") + `","approver_id":"bob#b1"}`
	w = e.do(&alice, "POST", "/api/migrations/m2/gates/prepare", body)
	require.Equal(t, 400, w.Code)
	require.Len(t, e.fake.gateCalls(), 1)
}

func TestDecideValidatesInputBeforeTemporal(t *testing.T) {
	e := newAPIEnv(t)
	e.fake.err = fmt.Errorf("%w: should not be called", ErrTemporal)
	alice := e.session("a1")
	for _, path := range []string{
		"/api/migrations/Bad_ID/gates/prepare",
		"/api/migrations/m1/gates/Prepare",
		"/api/migrations/m1/gates/" + strings.Repeat("a", 65),
	} {
		w := e.do(&alice, "POST", path, decision("approve", "", "x"))
		require.Equal(t, 400, w.Code, path)
	}
	e.fake.err = nil
	e.fake.open("m1", "prepare", 1)
	for _, body := range []string{
		decision("maybe", "", e.digest(alice, "m1")),
		`not json`,
		`{"decision":"approve"} {}`,
	} {
		w := e.do(&alice, "POST", "/api/migrations/m1/gates/prepare", body)
		require.Equal(t, 400, w.Code, body)
	}
	r := httptest.NewRequest("POST", "/api/migrations/m1/gates/prepare", strings.NewReader(decision("approve", "", "")))
	r.Header.Set("Content-Type", "text/plain")
	r.AddCookie(&http.Cookie{Name: SessionCookie, Value: alice.cookie})
	r.Header.Set(CSRFHeader, alice.csrf)
	w := httptest.NewRecorder()
	e.mux.ServeHTTP(w, r)
	require.Equal(t, http.StatusUnsupportedMediaType, w.Code)
	big := `{"decision":"approve","note":"` + strings.Repeat("x", 70<<10) + `","evidence_digest":""}`
	require.Equal(t, 400, e.do(&alice, "POST", "/api/migrations/m1/gates/prepare", big).Code)
	require.Empty(t, e.fake.gateCalls())
}

func TestSwitchNeedsADifferentPerson(t *testing.T) {
	e := newAPIEnv(t)
	e.account("b1", "bob")
	e.fake.open("m1", "switch", 2)

	require.Equal(t, 202, e.decide(e.session("a1"), "m1", "switch", "approve").Code)

	// The same account in a second session.
	w := e.decide(e.session("a1"), "m1", "switch", "approve")
	require.Equal(t, 409, w.Code)
	require.Equal(t, "you already approved this gate", errBody(t, w))

	// A new account that reuses the name: names are lowercase only, so the
	// case-insensitive rule is met by disabling alice and creating alice again.
	require.NoError(t, e.st.DisableApprover(context.Background(), "alice", e.now))
	e.account("a2", "alice")
	w = e.decide(e.session("a2"), "m1", "switch", "approve")
	require.Equal(t, 409, w.Code)
	require.Equal(t, "the second approval must come from a different person", errBody(t, w))

	require.Equal(t, 202, e.decide(e.session("b1"), "m1", "switch", "approve").Code)
	require.Len(t, e.fake.gateCalls(), 2)
}

func TestSwitchNameRuleIsCaseInsensitive(t *testing.T) {
	// An approval signalled outside the console (the CLI) can carry any
	// case; the rule still holds.
	e := newAPIEnv(t)
	e.fake.open("m1", "switch", 2)
	e.fake.statuses["m1"].Gate.Approvals = []string{"ALICE#cli"}
	w := e.decide(e.session("a1"), "m1", "switch", "approve")
	require.Equal(t, 409, w.Code)
	require.Equal(t, "the second approval must come from a different person", errBody(t, w))
	require.Empty(t, e.fake.gateCalls())
}

func TestAnyGateRefusesARepeatedAccount(t *testing.T) {
	e := newAPIEnv(t)
	e.fake.open("m1", "rehearse", 2)
	e.fake.statuses["m1"].Gate.Approvals = []string{"alice#a1"}
	w := e.decide(e.session("a1"), "m1", "rehearse", "reject")
	require.Equal(t, 409, w.Code)
	require.Equal(t, "you already approved this gate", errBody(t, w))
	require.Empty(t, e.fake.gateCalls())
}

func TestSwitchNeedsAFreshSession(t *testing.T) {
	e := newAPIEnv(t)
	e.fake.open("m1", "switch", 2)
	old := e.sessionAged("a1", 16*time.Minute)
	w := e.decide(old, "m1", "switch", "approve")
	require.Equal(t, 403, w.Code)
	require.Equal(t, "sign in again to decide the switch", errBody(t, w))
	require.Empty(t, e.fake.gateCalls())

	require.Equal(t, 202, e.decide(e.sessionAged("a1", 14*time.Minute), "m1", "switch", "approve").Code)

	// Other gates do not ask for a fresh session.
	e.fake.open("m2", "prepare", 1)
	require.Equal(t, 202, e.decide(old, "m2", "prepare", "approve").Code)
}

func TestStaleEvidenceIsRefused(t *testing.T) {
	e := newAPIEnv(t)
	e.account("b1", "bob")
	e.fake.open("m1", "switch", 2)
	alice, bob := e.session("a1"), e.session("b1")
	before := e.digest(bob, "m1")

	require.Equal(t, 202, e.decide(alice, "m1", "switch", "approve").Code)
	w := e.do(&bob, "POST", "/api/migrations/m1/gates/switch", decision("approve", "", before))
	require.Equal(t, 409, w.Code)
	require.Equal(t, "evidence changed; review the gate again", errBody(t, w))
	require.Len(t, e.fake.gateCalls(), 1)
	require.Len(t, e.audit("gate-decision"), 1)
}

func TestGateMustBeOpen(t *testing.T) {
	e := newAPIEnv(t)
	e.fake.open("m1", "prepare", 1)
	alice := e.session("a1")
	w := e.do(&alice, "POST", "/api/migrations/m1/gates/switch", decision("approve", "", e.digest(alice, "m1")))
	require.Equal(t, 409, w.Code)
	require.Equal(t, "gate is not open", errBody(t, w))

	e.fake.statuses["m1"].Gate = nil
	w = e.do(&alice, "POST", "/api/migrations/m1/gates/prepare", decision("approve", "", ""))
	require.Equal(t, 409, w.Code)
	require.Empty(t, e.fake.gateCalls())
}

func TestDecisionIsAudited(t *testing.T) {
	e := newAPIEnv(t)
	e.fake.open("m1", "prepare", 1)
	alice := e.session("a1")
	d := e.digest(alice, "m1")
	w := e.do(&alice, "POST", "/api/migrations/m1/gates/prepare", decision("reject", "not today", d))
	require.Equal(t, 202, w.Code)
	rows := e.audit("gate-decision")
	require.Len(t, rows, 1)
	r := rows[0]
	require.Equal(t, "alice#a1", r.ActorID)
	require.Equal(t, "alice", r.ActorName)
	require.Equal(t, "m1", r.Migration)
	require.Equal(t, "prepare", r.Gate)
	require.Equal(t, "reject", r.Decision)
	require.Equal(t, "not today", r.Note)
	require.Equal(t, d, r.Evidence)
	require.NotEmpty(t, d)
}

func TestDecisionIsAuditedBeforeTheSignal(t *testing.T) {
	// No audit row, no signal.
	e := newAPIEnv(t, func(d *Deps) { d.Store = failingAudit{d.Store} })
	e.fake.open("m1", "prepare", 1)
	alice := e.session("a1")
	w := e.decide(alice, "m1", "prepare", "approve")
	require.Equal(t, 500, w.Code)
	require.Equal(t, "internal error", errBody(t, w))
	require.NotContains(t, w.Body.String(), "disk")
	require.Empty(t, e.fake.gateCalls())

	w = e.do(&alice, "POST", "/api/migrations/m1/rollback", `{"reason":"r"}`)
	require.Equal(t, 500, w.Code)
	require.Empty(t, e.fake.rollbacks)
}

func TestFailedSignalIsAuditedAsFailed(t *testing.T) {
	e := newAPIEnv(t)
	e.fake.open("m1", "prepare", 1)
	alice := e.session("a1")
	e.fake.gateErr = fmt.Errorf("%w: rpc error: connection refused", ErrTemporal)
	w := e.decide(alice, "m1", "prepare", "approve")
	require.Equal(t, 503, w.Code)
	require.Equal(t, `{"error":"temporal unavailable"}`, w.Body.String())
	require.Len(t, e.audit("gate-decision"), 1)
	failed := e.audit("gate-decision-failed")
	require.Len(t, failed, 1)
	require.Equal(t, "prepare", failed[0].Gate)
	require.Equal(t, "alice#a1", failed[0].ActorID)
}

func TestConcurrentDecisionsAreSerialized(t *testing.T) {
	// Two sessions of one account race the same digest: only one may pass.
	e := newAPIEnv(t)
	e.fake.open("m1", "switch", 2)
	s1, s2 := e.session("a1"), e.session("a1")
	d := e.digest(s1, "m1")
	// Unserialized, both requests would read the status before either
	// signals; serialized, the first waits out the rendezvous alone.
	e.fake.meet, e.fake.arrived = 2, 0
	codes := make(chan int, 2)
	var wg sync.WaitGroup
	for _, u := range []user{s1, s2} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			codes <- e.do(&u, "POST", "/api/migrations/m1/gates/switch", decision("approve", "", d)).Code
		}()
	}
	wg.Wait()
	close(codes)
	var got []int
	for c := range codes {
		got = append(got, c)
	}
	slices.Sort(got)
	require.Equal(t, []int{202, 409}, got)
	require.Len(t, e.fake.gateCalls(), 1)
}

func examplePlan(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("../../examples/plan.yaml")
	require.NoError(t, err)
	_, err = plan.Parse(b)
	require.NoError(t, err)
	return string(b)
}

func startJSON(id, yaml string) string {
	m := map[string]string{"plan": yaml}
	if id != "" {
		m["id"] = id
	}
	b, _ := json.Marshal(m)
	return string(b)
}

func TestStartMigration(t *testing.T) {
	e := newAPIEnv(t)
	alice := e.session("a1")
	good := examplePlan(t)

	w := e.do(&alice, "POST", "/api/migrations", startJSON("", good))
	require.Equal(t, 201, w.Code, w.Body.String())
	require.JSONEq(t, `{"id":"shop"}`, w.Body.String())
	w = e.do(&alice, "POST", "/api/migrations", startJSON("shop-2", good))
	require.Equal(t, 201, w.Code, w.Body.String())
	require.JSONEq(t, `{"id":"shop-2"}`, w.Body.String())
	require.Equal(t, []string{"shop", "shop-2"}, e.fake.starts)
	rows := e.audit("migration-started")
	require.Len(t, rows, 2)
	require.Equal(t, "alice#a1", rows[1].ActorID)
	require.Equal(t, "shop", rows[1].Migration)

	// Invalid: the first validation error, in a fixed frame.
	w = e.do(&alice, "POST", "/api/migrations", startJSON("x1", strings.Replace(good, "version: 1", "version: 2", 1)))
	require.Equal(t, 400, w.Code)
	require.Equal(t, "invalid plan: version: must be 1", errBody(t, w))

	// Not YAML, or an unknown key: a fixed text and a line, never the input.
	w = e.do(&alice, "POST", "/api/migrations", startJSON("x1", "version: 1\nname: [unclosed\n"))
	require.Equal(t, 400, w.Code)
	require.Regexp(t, `^plan is not valid YAML( \(line \d+\))?$`, errBody(t, w))
	w = e.do(&alice, "POST", "/api/migrations", startJSON("x1", good+"\nsecret_sauce_key: 1\n"))
	require.Equal(t, 400, w.Code)
	require.Regexp(t, `^plan is not valid YAML \(line \d+\)$`, errBody(t, w))
	require.NotContains(t, w.Body.String(), "secret_sauce")
	w = e.do(&alice, "POST", "/api/migrations", startJSON("x1", ""))
	require.Equal(t, 400, w.Code)
	require.Equal(t, "plan is not valid YAML", errBody(t, w))
	w = e.do(&alice, "POST", "/api/migrations", startJSON("x1", good+"\n---\nversion: 1\n"))
	require.Equal(t, 400, w.Code)
	require.Equal(t, "plan is not valid YAML", errBody(t, w))

	// A bad ID is refused before Temporal is asked.
	w = e.do(&alice, "POST", "/api/migrations", startJSON("Bad ID", good))
	require.Equal(t, 400, w.Code)

	// A duplicate.
	w = e.do(&alice, "POST", "/api/migrations", startJSON("", good))
	require.Equal(t, 409, w.Code)
	require.Equal(t, "migration already exists", errBody(t, w))
	require.Len(t, e.fake.starts, 2)
	require.Len(t, e.audit("migration-started"), 3)
	require.Len(t, e.audit("migration-start-failed"), 1)
}

func TestTemporalErrorsMapToClearStatuses(t *testing.T) {
	e := newAPIEnv(t)
	alice := e.session("a1")
	e.fake.err = fmt.Errorf("%w: dial tcp 10.9.9.9:7233: connection refused", ErrTemporal)
	for _, c := range []struct{ method, path, body string }{
		{"GET", "/api/migrations", ""},
		{"GET", "/api/migrations/m1", ""},
		{"GET", "/api/migrations/m1/failures", ""},
		{"POST", "/api/migrations/m1/gates/prepare", decision("approve", "", "x")},
		{"POST", "/api/migrations/m1/rollback", `{"reason":"r"}`},
		{"POST", "/api/migrations", startJSON("m1", examplePlan(t))},
	} {
		w := e.do(&alice, c.method, c.path, c.body)
		require.Equal(t, 503, w.Code, c.path)
		require.Equal(t, `{"error":"temporal unavailable"}`, w.Body.String(), c.path)
	}
	e.fake.err = nil
	for _, c := range []struct{ method, path, body string }{
		{"GET", "/api/migrations/m1", ""},
		{"GET", "/api/migrations/m1/failures", ""},
		{"POST", "/api/migrations/m1/gates/prepare", decision("approve", "", "x")},
		{"POST", "/api/migrations/m1/rollback", `{"reason":"r"}`},
	} {
		w := e.do(&alice, c.method, c.path, c.body)
		require.Equal(t, 404, w.Code, c.path)
		require.Equal(t, `{"error":"no such migration"}`, w.Body.String(), c.path)
	}
	require.Equal(t, 400, e.do(&alice, "GET", "/api/migrations/NOPE", "").Code)
	require.NotContains(t, e.logs.String(), "csrf-")
}

func TestRollbackIsAuditedAndSignalled(t *testing.T) {
	e := newAPIEnv(t)
	e.fake.open("m1", "switch", 2)
	alice := e.session("a1")
	d := e.digest(alice, "m1")
	w := e.do(&alice, "POST", "/api/migrations/m1/rollback", `{"reason":"latency"}`)
	require.Equal(t, 202, w.Code, w.Body.String())
	require.Equal(t, []flow.RollbackSignal{{ApproverID: "alice#a1", Reason: "latency"}}, e.fake.rollbacks)
	rows := e.audit("rollback")
	require.Len(t, rows, 1)
	require.Equal(t, "alice", rows[0].ActorName)
	require.Equal(t, "m1", rows[0].Migration)
	require.Equal(t, "latency", rows[0].Note)
	require.Equal(t, d, rows[0].Evidence)

	require.Equal(t, 400, e.do(&alice, "POST", "/api/migrations/m1/rollback", `{"reason":"  "}`).Code)
	require.Equal(t, 400, e.do(&alice, "POST", "/api/migrations/m1/rollback", `{"reason":"x","approver_id":"b"}`).Code)
	require.Len(t, e.fake.rollbacks, 1)
}

func TestRollbackSignalFailureIsAudited(t *testing.T) {
	e := newAPIEnv(t)
	e.fake.open("m1", "switch", 2)
	alice := e.session("a1")
	// Status answers, the signal does not.
	e2 := &rollbackFails{fakeTemporal: e.fake}
	mux := http.NewServeMux()
	Routes(mux, e.a, Deps{Temporal: e2, Store: e.st})
	e.mux = mux
	w := e.do(&alice, "POST", "/api/migrations/m1/rollback", `{"reason":"r"}`)
	require.Equal(t, 503, w.Code)
	require.Len(t, e.audit("rollback"), 1)
	require.Len(t, e.audit("rollback-failed"), 1)
}

type rollbackFails struct{ *fakeTemporal }

func (*rollbackFails) Rollback(context.Context, string, flow.RollbackSignal) error {
	return fmt.Errorf("%w: boom", ErrTemporal)
}

func TestStatusNamesAndReplicate(t *testing.T) {
	e := newAPIEnv(t)
	e.fake.open("m1", "switch", 2)
	e.fake.statuses["m1"].Gate.Approvals = []string{"alice#a1", "ghost#zz"}
	e.fake.statuses["m1"].Events = []flow.Event{{Kind: "approved", Actor: "alice#a1"}, {Kind: "x", Actor: "cli"}}
	e.fake.statuses["m1"].ReplicateWorkflowID = "migration-m1-replicate"
	e.fake.replicate["migration-m1-replicate"] = flow.ReplicateStatus{CopyDone: true, Samples: 3}
	require.NoError(t, e.st.DisableApprover(context.Background(), "alice", e.now))
	e.account("b1", "bob")
	bob := e.session("b1")

	w := e.do(&bob, "GET", "/api/migrations/m1", "")
	require.Equal(t, 200, w.Code, w.Body.String())
	var out struct {
		Status    flow.Status           `json:"status"`
		Digest    string                `json:"evidence_digest"`
		Replicate *flow.ReplicateStatus `json:"replicate"`
		Names     map[string]string     `json:"names"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Equal(t, "m1", out.Status.MigrationID)
	st, _ := e.fake.Status(context.Background(), "m1")
	require.Equal(t, flow.EvidenceDigest(st), out.Digest)
	require.NotNil(t, out.Replicate)
	require.Equal(t, 3, out.Replicate.Samples)
	// A disabled account still resolves; an unknown ID does not.
	require.Equal(t, map[string]string{"alice#a1": "alice"}, out.Names)

	// No child: no replicate key at all.
	e.fake.statuses["m1"].ReplicateWorkflowID = ""
	w = e.do(&bob, "GET", "/api/migrations/m1", "")
	require.NotContains(t, w.Body.String(), `"replicate"`)
}

func TestListMigrations(t *testing.T) {
	e := newAPIEnv(t)
	at := time.Date(2026, 10, 22, 9, 0, 0, 0, time.UTC)
	e.fake.list = []MigrationSummary{{ID: "m1", Phase: "switch", State: "waiting", Gate: "switch", Started: at}}
	alice := e.session("a1")
	w := e.do(&alice, "GET", "/api/migrations", "")
	require.Equal(t, 200, w.Code)
	require.JSONEq(t, `[{"id":"m1","phase":"switch","state":"waiting","gate":"switch","started":"2026-10-22T09:00:00Z"}]`, w.Body.String())
	e.fake.list = nil
	w = e.do(&alice, "GET", "/api/migrations", "")
	require.JSONEq(t, `[]`, w.Body.String())
}

func TestFailuresUseThePlanNameAndRoutedServices(t *testing.T) {
	f := fakeFailures{"shop/api": {{{Path: "/a", Status: 503}}, nil}}
	e := newAPIEnv(t, func(d *Deps) { d.Failures = f })
	e.fake.open("m1", "switch", 2)
	alice := e.session("a1")
	w := e.do(&alice, "GET", "/api/migrations/m1/failures", "")
	require.Equal(t, 200, w.Code, w.Body.String())
	var out struct {
		Services []struct {
			Service string              `json:"service"`
			Source  []frontdoor.Failure `json:"source"`
			Target  []frontdoor.Failure `json:"target"`
		} `json:"services"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Len(t, out.Services, 2)
	require.Equal(t, "api", out.Services[0].Service)
	require.Len(t, out.Services[0].Source, 1)
	require.Equal(t, "web", out.Services[1].Service)
	require.Contains(t, w.Body.String(), `"target":[]`)

	// No source configured: empty lists, not null.
	e2 := newAPIEnv(t)
	e2.fake.open("m1", "switch", 2)
	a2 := e2.session("a1")
	w = e2.do(&a2, "GET", "/api/migrations/m1/failures", "")
	require.Equal(t, 200, w.Code)
	require.JSONEq(t, `{"services":[{"service":"api","source":[],"target":[]},{"service":"web","source":[],"target":[]}]}`, w.Body.String())
}

func TestAuditPageAPI(t *testing.T) {
	e := newAPIEnv(t)
	ctx := context.Background()
	for i := range 5 {
		mig := "m1"
		if i%2 == 1 {
			mig = "m2"
		}
		_, err := e.st.AppendAudit(ctx, store.AuditRow{At: e.now, ActorID: "alice#a1", ActorName: "alice", Migration: mig, Kind: "gate-decision", Gate: "prepare", Decision: "approve", Note: fmt.Sprint(i)})
		require.NoError(t, err)
	}
	alice := e.session("a1")
	type row struct {
		ID        int64  `json:"id"`
		At        string `json:"at"`
		ActorID   string `json:"actor_id"`
		ActorName string `json:"actor_name"`
		Migration string `json:"migration"`
		Kind      string `json:"kind"`
		Note      string `json:"note"`
	}
	get := func(q string) []row {
		w := e.do(&alice, "GET", "/api/audit"+q, "")
		require.Equal(t, 200, w.Code, w.Body.String())
		var out []row
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
		return out
	}
	all := get("")
	require.Len(t, all, 5)
	require.Greater(t, all[0].ID, all[1].ID)
	require.Equal(t, "alice#a1", all[0].ActorID)
	require.NotEmpty(t, all[0].At)
	m1 := get("?migration=m1")
	require.Len(t, m1, 3)
	page := get("?migration=m1&limit=2")
	require.Len(t, page, 2)
	rest := get(fmt.Sprintf("?migration=m1&before=%d", page[1].ID))
	require.Len(t, rest, 1)
	require.Len(t, get("?limit=100000"), 5)
	for _, q := range []string{"?migration=Bad", "?before=x", "?before=0", "?limit=0", "?limit=-1"} {
		require.Equal(t, 400, e.do(&alice, "GET", "/api/audit"+q, "").Code, q)
	}
	require.Equal(t, 401, e.do(nil, "GET", "/api/audit", "").Code)
}

func TestUnknownAPIPathNeedsLogin(t *testing.T) {
	e := newAPIEnv(t)
	require.Equal(t, 401, e.do(nil, "GET", "/api/nope", "").Code)
	require.Equal(t, 401, e.do(nil, "GET", "/api/login", "").Code)
	require.Equal(t, 401, e.do(nil, "GET", "/api/migrations", "").Code)
	require.Equal(t, 401, e.do(nil, "POST", "/api/migrations/m1/rollback", `{"reason":"r"}`).Code)
	alice := e.session("a1")
	w := e.do(&alice, "GET", "/api/nope", "")
	require.Equal(t, 404, w.Code)
	require.Equal(t, `{"error":"not found"}`, w.Body.String())

	// Login, me and logout are mounted too.
	w = e.login(goodBody(), "10.0.0.2")
	require.Equal(t, 200, w.Code)
	r := httptest.NewRequest("POST", "/api/login", strings.NewReader(goodBody()))
	r.Header.Set("Content-Type", "application/json")
	r.RemoteAddr = "10.0.0.3:1"
	rec := httptest.NewRecorder()
	e.mux.ServeHTTP(rec, r)
	require.Equal(t, 200, rec.Code)
	require.Equal(t, 200, e.do(&alice, "GET", "/api/me", "").Code)
	require.Equal(t, 204, e.do(&alice, "POST", "/api/logout", "").Code)
}

func TestActionsNeedTheCSRFHeader(t *testing.T) {
	e := newAPIEnv(t)
	e.fake.open("m1", "prepare", 1)
	alice := e.session("a1")
	noCSRF := user{cookie: alice.cookie}
	for _, c := range []struct{ path, body string }{
		{"/api/migrations/m1/gates/prepare", decision("approve", "", e.digest(alice, "m1"))},
		{"/api/migrations/m1/rollback", `{"reason":"r"}`},
		{"/api/migrations", startJSON("m9", examplePlan(t))},
	} {
		for _, u := range []user{noCSRF, {cookie: alice.cookie, csrf: "wrong"}} {
			w := e.do(&u, "POST", c.path, c.body)
			require.Equal(t, 403, w.Code, c.path)
		}
	}
	require.Empty(t, e.fake.gateCalls())
	require.Empty(t, e.fake.rollbacks)
	require.Empty(t, e.fake.starts)
	rows, err := e.st.AuditPage(context.Background(), "", 0, 100)
	require.NoError(t, err)
	require.Empty(t, rows)
}

// TestDisconnectDoesNotStrandTheAuditRow: once the action's row is
// written, a client that goes away cancels neither the signal nor the row
// that records it failed.
func TestDisconnectDoesNotStrandTheAuditRow(t *testing.T) {
	cases := []struct {
		name, path, kind, failed string
		body                     func(e *apiEnv, u user) string
	}{
		{"decide", "/api/migrations/m1/gates/prepare", "gate-decision", "gate-decision-failed",
			func(e *apiEnv, u user) string { return decision("approve", "ok", e.digest(u, "m1")) }},
		{"rollback", "/api/migrations/m1/rollback", "rollback", "rollback-failed",
			func(*apiEnv, user) string { return `{"reason":"r"}` }},
		{"start", "/api/migrations", "migration-started", "migration-start-failed",
			func(_ *apiEnv, _ user) string { return startJSON("m2", examplePlan(t)) }},
	}
	for _, c := range cases {
		t.Run(c.name+"/signal-succeeds", func(t *testing.T) {
			e := newAPIEnv(t)
			e.fake.open("m1", "prepare", 1)
			alice := e.session("a1")
			body := c.body(e, alice)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var seen error
			e.fake.action = func(actx context.Context) error {
				cancel() // the client goes away mid-call
				seen = actx.Err()
				return seen
			}
			w := e.doCtx(ctx, &alice, "POST", c.path, body)
			require.NoError(t, seen)
			require.Contains(t, []int{201, 202}, w.Code, w.Body.String())
			require.Len(t, e.audit(c.kind), 1)
			require.Empty(t, e.audit(c.failed))
		})
		t.Run(c.name+"/signal-fails", func(t *testing.T) {
			e := newAPIEnv(t)
			e.fake.open("m1", "prepare", 1)
			alice := e.session("a1")
			body := c.body(e, alice)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			e.fake.action = func(context.Context) error {
				cancel()
				return fmt.Errorf("%w: deadline exceeded", ErrTemporal)
			}
			w := e.doCtx(ctx, &alice, "POST", c.path, body)
			require.Equal(t, 503, w.Code)
			require.Len(t, e.audit(c.kind), 1)
			failed := e.audit(c.failed)
			require.Len(t, failed, 1)
			require.True(t, strings.HasPrefix(failed[0].Note, "not confirmed"), failed[0].Note)
		})
	}
}

// TestActionDeadlineDoesNotLoseTheFailedRow: the call fails because the
// action's own deadline ran out; the row recording that still lands.
func TestActionDeadlineDoesNotLoseTheFailedRow(t *testing.T) {
	old := actionTimeout
	actionTimeout = 50 * time.Millisecond
	t.Cleanup(func() { actionTimeout = old })
	for _, c := range []struct{ name, path, failed string }{
		{"decide", "/api/migrations/m1/gates/prepare", "gate-decision-failed"},
		{"rollback", "/api/migrations/m1/rollback", "rollback-failed"},
		{"start", "/api/migrations", "migration-start-failed"},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := newAPIEnv(t)
			e.fake.open("m1", "prepare", 1)
			alice := e.session("a1")
			body := map[string]string{
				"decide":   decision("approve", "", e.digest(alice, "m1")),
				"rollback": `{"reason":"r"}`,
				"start":    startJSON("m2", examplePlan(t)),
			}[c.name]
			e.fake.action = func(ctx context.Context) error {
				<-ctx.Done() // a Temporal that never answers
				return fmt.Errorf("%w: %w", ErrTemporal, ctx.Err())
			}
			w := e.do(&alice, "POST", c.path, body)
			require.Equal(t, 503, w.Code)
			failed := e.audit(c.failed)
			require.Len(t, failed, 1)
			require.True(t, strings.HasPrefix(failed[0].Note, "not confirmed"), failed[0].Note)
		})
	}
}

// TestRepeatDecisionBeforeTheStatusShowsIt: a 202 does not mean the
// workflow's status already shows the approval. A second tab's repeat is
// refused by what this server remembers, until the gate changes.
func TestRepeatDecisionBeforeTheStatusShowsIt(t *testing.T) {
	e := newAPIEnv(t)
	e.account("b1", "bob")
	e.fake.open("m1", "switch", 2)
	e.fake.lag = true
	alice, alice2 := e.session("a1"), e.session("a1")

	require.Equal(t, 202, e.decide(alice, "m1", "switch", "approve").Code)
	for _, dec := range []string{"approve", "reject"} {
		w := e.decide(alice2, "m1", "switch", dec)
		require.Equal(t, 409, w.Code, dec)
		require.Equal(t, "you already decided this gate", errBody(t, w))
	}
	require.Len(t, e.fake.gateCalls(), 1)
	require.Len(t, e.audit("gate-decision"), 1)

	// Another account is not affected.
	require.Equal(t, 202, e.decide(e.session("b1"), "m1", "switch", "approve").Code)

	// A different gate: alice decides again.
	e.fake.open("m1", "finish", 1)
	require.Equal(t, 202, e.decide(alice, "m1", "finish", "approve").Code)
	// The same gate name reopened (a retry): a new gate, so alice may decide.
	e.fake.open("m1", "finish", 1)
	require.Equal(t, 202, e.decide(alice, "m1", "finish", "approve").Code)
	require.Len(t, e.fake.gateCalls(), 4)

	// No gate open: the memory is dropped.
	e.fake.statuses["m1"].Gate = nil
	require.Equal(t, 409, e.do(&alice, "POST", "/api/migrations/m1/gates/finish", decision("approve", "", "")).Code)
	e.api.decided.mu.Lock()
	require.Empty(t, e.api.decided.m)
	e.api.decided.mu.Unlock()
}

func TestFailedSignalIsNotRemembered(t *testing.T) {
	e := newAPIEnv(t)
	e.fake.open("m1", "prepare", 1)
	e.fake.lag = true
	alice := e.session("a1")
	e.fake.gateErr = fmt.Errorf("%w: down", ErrTemporal)
	require.Equal(t, 503, e.decide(alice, "m1", "prepare", "approve").Code)
	e.fake.gateErr = nil
	require.Equal(t, 202, e.decide(alice, "m1", "prepare", "approve").Code)
}

// TestSameNameBeforeTheStatusShowsIt: on the switch, a second account with
// the first approver's name (an account disabled and made again) is the
// same person, even before the status shows the first approval.
func TestSameNameBeforeTheStatusShowsIt(t *testing.T) {
	e := newAPIEnv(t)
	e.account("b1", "bob")
	e.fake.open("m1", "switch", 2)
	e.fake.lag = true
	require.Equal(t, 202, e.decide(e.session("a1"), "m1", "switch", "approve").Code)

	require.NoError(t, e.st.DisableApprover(context.Background(), "alice", e.now))
	e.account("a2", "Alice")
	w := e.decide(e.session("a2"), "m1", "switch", "approve")
	require.Equal(t, 409, w.Code, w.Body.String())
	require.Equal(t, "the second approval must come from a different person", errBody(t, w))
	require.Len(t, e.fake.gateCalls(), 1)
	require.Len(t, e.audit("gate-decision"), 1)

	require.Equal(t, 202, e.decide(e.session("b1"), "m1", "switch", "approve").Code)
}
