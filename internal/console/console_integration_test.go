//go:build integration

package console

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/worker"
	"go.yaml.in/yaml/v3"

	"github.com/SaiPisey2/cutover/internal/flow"
	"github.com/SaiPisey2/cutover/internal/itest"
	"github.com/SaiPisey2/cutover/internal/store"
	"github.com/SaiPisey2/cutover/internal/tlsutil"
	"github.com/SaiPisey2/cutover/ui"
)

// itRig is the console as `cutover server --data` runs it, over TLS,
// against a real Temporal dev server, worker and database harness.
type itRig struct {
	t    *testing.T
	st   *store.Store
	base string
	ca   []byte
	// pw holds each account's password, by name.
	pw map[string]string
}

func newITRig(t *testing.T) *itRig {
	t.Helper()
	tc, _ := itest.StartTemporal(t)
	w := worker.New(tc, flow.TaskQueue, flow.WorkerOptions())
	flow.Register(w)
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)

	data := t.TempDir()
	st, err := store.Open(filepath.Join(data, "cutover.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	cert, caPEM, err := tlsutil.LoadOrCreate(filepath.Join(data, "tls"), []string{"127.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	hubCtx, hubCancel := context.WithCancel(context.Background())
	auth := NewAuth(st, log)
	mux := http.NewServeMux()
	Routes(mux, auth, Deps{Temporal: NewTemporal(tc), Store: st, Log: log, Context: hubCtx})
	srv := httptest.NewUnstartedServer(NewServer(auth, mux, ui.FS()))
	srv.EnableHTTP2 = true
	srv.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}}
	srv.StartTLS()
	t.Cleanup(func() { hubCancel(); srv.Close() })
	return &itRig{t: t, st: st, base: srv.URL, ca: caPEM, pw: map[string]string{}}
}

// approver creates an account the way `cutover approver new` does.
func (r *itRig) approver(name string) string {
	r.t.Helper()
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	pw := hex.EncodeToString(b)
	hash, err := HashPassword(pw)
	if err != nil {
		r.t.Fatal(err)
	}
	id := hex.EncodeToString(b[:8])
	if err := r.st.CreateApprover(context.Background(), store.Approver{ID: id, Name: name, PasswordHash: hash, Created: time.Now().UTC()}); err != nil {
		r.t.Fatal(err)
	}
	r.pw[name] = pw
	return id
}

// login signs name in on a client of its own: a session.
func (r *itRig) login(name string) *Client {
	r.t.Helper()
	c, err := NewClient(r.base, r.ca)
	if err != nil {
		r.t.Fatal(err)
	}
	if err := c.Login(context.Background(), name, r.pw[name]); err != nil {
		r.t.Fatalf("login %s: %v", name, err)
	}
	return c
}

// decideWith posts a decision with the digest given, not a fresh one, and
// returns the status code and error text. It never fails the test itself,
// so it is safe from goroutines.
func decideWith(c *Client, id, gate string, d flow.Decision, digest string) (int, string, error) {
	err := c.do(context.Background(), http.MethodPost, "/api/migrations/"+id+"/gates/"+gate,
		decideBody{Decision: d, Note: "it", EvidenceDigest: digest}, nil, http.StatusAccepted)
	var ae *APIError
	switch {
	case err == nil:
		return http.StatusAccepted, "", nil
	case errors.As(err, &ae):
		return ae.Status, ae.Message, nil
	default:
		return 0, "", err
	}
}

// itStream reads /api/stream?migration=id and hands each status event to
// the test, in order.
type itStream struct {
	t      *testing.T
	events chan StatusReply
	done   chan error
	seen   []string // gate changes, in order, as gateChange describes them
}

func openStream(t *testing.T, c *Client, id string) *itStream {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base.String()+"/api/stream?migration="+id, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", "text/event-stream")
	// The client's own timeout would cut a stream that outlives it.
	hc := &http.Client{Transport: c.hc.Transport, Jar: c.hc.Jar}
	resp, err := hc.Do(req)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || resp.ProtoMajor != 2 {
		cancel()
		t.Fatalf("stream: %s over %s", resp.Status, resp.Proto)
	}
	s := &itStream{t: t, events: make(chan StatusReply, 1024), done: make(chan error, 1)}
	go func() {
		defer resp.Body.Close()
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 0, 64<<10), 8<<20)
		var name string
		for sc.Scan() {
			line := sc.Text()
			switch {
			case strings.HasPrefix(line, "event: "):
				name = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: ") && name == "status":
				var st StatusReply
				if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &st); err != nil {
					s.done <- err
					return
				}
				s.events <- st
			}
		}
		s.done <- sc.Err()
	}()
	t.Cleanup(cancel)
	return s
}

// gateChange names a status by its open gate and approvals, or its state
// when no gate is open.
func gateChange(st flow.Status) string {
	if st.Gate == nil {
		return "state:" + st.State
	}
	return fmt.Sprintf("%s/%d", st.Gate.Name, len(st.Gate.Approvals))
}

// until reads status events until ok holds and returns that one. Gates
// other than the one expected are logged, so a test stuck at a retry gate
// says why.
func (s *itStream) until(what string, d time.Duration, ok func(StatusReply) bool) StatusReply {
	s.t.Helper()
	timeout := time.After(d)
	for {
		select {
		case st := <-s.events:
			if c := gateChange(st.Status); len(s.seen) == 0 || s.seen[len(s.seen)-1] != c {
				s.seen = append(s.seen, c)
				if g := st.Status.Gate; g != nil && !strings.HasPrefix(c, what) {
					s.t.Logf("stream: gate %s open (waiting for %s): %s", c, what, g.Summary)
				}
			}
			if ok(st) {
				return st
			}
		case err := <-s.done:
			s.t.Fatalf("stream ended waiting for %s: %v", what, err)
		case <-timeout:
			s.t.Fatalf("waited %s for %s; seen %v", d, what, s.seen)
		}
	}
}

// gateOpen waits for gate to be open with n approvals.
func (s *itStream) gateOpen(gate string, n int) StatusReply {
	s.t.Helper()
	return s.until(gate, 3*time.Minute, func(st StatusReply) bool {
		g := st.Status.Gate
		return g != nil && g.Name == gate && len(g.Approvals) == n
	})
}

func TestApprovalsThroughTheServer(t *testing.T) {
	itest.Reset(t)
	itest.Seed(t, itest.Conn(t, itest.SourceDSN))
	r := newITRig(t)
	ctx := context.Background()

	aliceID := r.approver("alice")
	bobID := r.approver("bob")
	alice, alice2, bob := r.login("alice"), r.login("alice"), r.login("bob")

	// Start a database-only migration through the API.
	p := itest.Plan(t)
	y, err := yaml.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	const id = "console-it"
	got, err := alice.Start(ctx, id, y)
	if err != nil || got != id {
		t.Fatalf("start: %q %v", got, err)
	}
	// The same ID again while it runs: Temporal refuses, the API says 409.
	_, err = bob.Start(ctx, id, y)
	var ae *APIError
	if !errors.As(err, &ae) || ae.Status != http.StatusConflict || ae.Message != "migration already exists" {
		t.Fatalf("second start: %v", err)
	}

	// Bob's stream watches the whole run; alice's account is disabled
	// part way, which would end a stream of hers.
	stream := openStream(t, bob, id)
	accepted := 0
	approve := func(c *Client, gate string) {
		t.Helper()
		if err := c.Decide(ctx, id, gate, flow.Approve, "it"); err != nil {
			t.Fatalf("approve %s: %v", gate, err)
		}
		accepted++
	}

	// Every gate up to the switch, as alice.
	for _, gate := range []string{"discover", "prepare", "rehearse"} {
		stream.gateOpen(gate, 0)
		approve(alice, gate)
	}

	// The switch.
	open := stream.gateOpen("switch", 0)
	d0 := open.EvidenceDigest
	// Two of alice's sessions at once: exactly one is accepted.
	var wg sync.WaitGroup
	codes := make([]int, 2)
	errs := make([]error, 2)
	for i, c := range []*Client{alice, alice2} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			codes[i], _, errs[i] = decideWith(c, id, "switch", flow.Approve, d0)
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	slices.Sort(codes)
	if codes[0] != http.StatusAccepted || codes[1] != http.StatusConflict {
		t.Fatalf("two of alice's sessions at once: %v, want one 202 and one 409", codes)
	}
	accepted++
	// Once the status shows alice's approval, the digest she saw is stale.
	// Alice again, with the current digest, is refused for deciding twice.
	d1 := stream.gateOpen("switch", 1).EvidenceDigest
	code, msg, derr := decideWith(alice, id, "switch", flow.Approve, d1)
	if derr != nil {
		t.Fatal(derr)
	}
	if code != http.StatusConflict || msg != "you already approved this gate" {
		t.Fatalf("alice again: %d %s", code, msg)
	}
	// Another account with alice's name, at once: the same person.
	if err := r.st.DisableApprover(ctx, "alice", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	newAliceID := r.approver("alice")
	newAlice := r.login("alice")
	err = newAlice.Decide(ctx, id, "switch", flow.Approve, "it")
	if !errors.As(err, &ae) || ae.Status != http.StatusConflict || ae.Message != "the second approval must come from a different person" {
		t.Fatalf("new alice on the switch: %v", err)
	}
	code, msg, derr = decideWith(bob, id, "switch", flow.Approve, d0)
	if derr != nil {
		t.Fatal(derr)
	}
	if code != http.StatusConflict || msg != "evidence changed; review the gate again" {
		t.Fatalf("bob with the old digest: %d %s", code, msg)
	}
	approve(bob, "switch")

	// Finish, as alice (her new account).
	stream.gateOpen("finish", 0)
	approve(newAlice, "finish")
	final := stream.until("state:finished", 3*time.Minute, func(st StatusReply) bool {
		return st.Status.State == flow.StateFinished
	})

	// The workflow's events name the accounts that decided.
	actors := map[string]bool{}
	for _, e := range final.Status.Events {
		if e.Actor != "" {
			actors[e.Actor] = true
		}
	}
	for _, want := range []string{ApproverID("alice", aliceID), ApproverID("bob", bobID), ApproverID("alice", newAliceID)} {
		if !actors[want] {
			t.Errorf("no event by %s; actors %v", want, actors)
		}
	}
	for a := range actors {
		if !strings.HasPrefix(a, "alice#") && !strings.HasPrefix(a, "bob#") {
			t.Errorf("unexpected actor %q", a)
		}
	}

	// One gate-decision row per accepted decision, none for the refused
	// ones, and no failed signals.
	resp, err := bob.hc.Get(r.base + "/api/audit?migration=" + id + "&limit=500")
	if err != nil {
		t.Fatal(err)
	}
	var rows []AuditEntry
	err = json.NewDecoder(resp.Body).Decode(&rows)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || err != nil {
		t.Fatalf("audit: %s %v", resp.Status, err)
	}
	var decisions []string
	for _, row := range rows {
		switch row.Kind {
		case "gate-decision":
			decisions = append(decisions, row.Gate+":"+row.ActorID)
		case "gate-decision-failed":
			t.Errorf("audit row %s: %+v", row.Kind, row)
		}
	}
	slices.Sort(decisions)
	want := []string{
		"discover:" + ApproverID("alice", aliceID), "finish:" + ApproverID("alice", newAliceID),
		"prepare:" + ApproverID("alice", aliceID), "rehearse:" + ApproverID("alice", aliceID),
		"switch:" + ApproverID("alice", aliceID), "switch:" + ApproverID("bob", bobID),
	}
	if !slices.Equal(decisions, want) || len(decisions) != accepted {
		t.Errorf("gate-decision rows %v, want %v (%d accepted)", decisions, want, accepted)
	}

	// The stream sent a status for every gate change.
	for _, c := range []string{"discover/0", "prepare/0", "rehearse/0", "switch/0", "switch/1", "finish/0", "state:finished"} {
		if !slices.Contains(stream.seen, c) {
			t.Errorf("stream never showed %s; saw %v", c, stream.seen)
		}
	}
	t.Logf("stream saw %v", stream.seen)
}
