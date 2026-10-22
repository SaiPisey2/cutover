package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)

func open(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "cutover.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestOpenCreatesPrivateFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cutover.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.CreateApprover(context.Background(), Approver{ID: "a1", Name: "alice", PasswordHash: "h", Created: t0}); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{path, path + "-wal", path + "-shm"} {
		fi, err := os.Stat(p)
		if errors.Is(err, os.ErrNotExist) && p != path {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if m := fi.Mode().Perm(); m != 0o600 {
			t.Errorf("%s mode %o, want 600", filepath.Base(p), m)
		}
	}
}

func TestOpenRefusesURIChars(t *testing.T) {
	for _, name := range []string{"a?b.db", "a#b.db"} {
		if s, err := Open(filepath.Join(t.TempDir(), name)); err == nil {
			s.Close()
			t.Errorf("%q opened, want error", name)
		}
	}
}

func TestMigrationsAreIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cutover.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var v, n int
	if err := s.DB().QueryRow(`SELECT MAX(version), COUNT(*) FROM schema_version`).Scan(&v, &n); err != nil {
		t.Fatal(err)
	}
	if v != len(migrations) || n != len(migrations) {
		t.Fatalf("version %d rows %d, want %d", v, n, len(migrations))
	}
}

func TestApproverNameIsUniqueWhileLive(t *testing.T) {
	s, ctx := open(t), context.Background()
	if err := s.CreateApprover(ctx, Approver{ID: "a1", Name: "alice", PasswordHash: "h", Created: t0}); err != nil {
		t.Fatal(err)
	}
	err := s.CreateApprover(ctx, Approver{ID: "a2", Name: "alice", PasswordHash: "h", Created: t0})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("got %v, want ErrConflict", err)
	}
	if err := s.DisableApprover(ctx, "alice", t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateApprover(ctx, Approver{ID: "a3", Name: "alice", PasswordHash: "h2", Created: t0}); err != nil {
		t.Fatal(err)
	}
	a, err := s.ApproverByName(ctx, "alice")
	if err != nil || a.ID != "a3" || a.Disabled != nil {
		t.Fatalf("got %+v, %v", a, err)
	}
	old, err := s.ApproverByID(ctx, "a1")
	if err != nil || old.Disabled == nil || !old.Disabled.Equal(t0.Add(time.Minute)) {
		t.Fatalf("got %+v, %v", old, err)
	}
	list, err := s.ListApprovers(ctx)
	if err != nil || len(list) != 2 {
		t.Fatalf("got %d, %v", len(list), err)
	}
	if err := s.DisableApprover(ctx, "nobody", t0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
	if _, err := s.ApproverByName(ctx, "nobody"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

func newSession(s *Store, t *testing.T, expires time.Time) []byte {
	t.Helper()
	ctx := context.Background()
	if _, err := s.ApproverByID(ctx, "a1"); errors.Is(err, ErrNotFound) {
		if err := s.CreateApprover(ctx, Approver{ID: "a1", Name: "alice", PasswordHash: "h", Created: t0}); err != nil {
			t.Fatal(err)
		}
	}
	h := []byte("hash-of-session-id")
	if err := s.CreateSession(ctx, Session{ApproverID: "a1", CSRF: "csrf", Created: t0, Expires: expires}, h); err != nil {
		t.Fatal(err)
	}
	return h
}

func TestSessionLifecycle(t *testing.T) {
	s, ctx := open(t), context.Background()
	h := newSession(s, t, t0.Add(time.Hour))
	got, err := s.SessionByHash(ctx, h, t0)
	if err != nil {
		t.Fatal(err)
	}
	if got.ApproverID != "a1" || got.ApproverName != "alice" || got.CSRF != "csrf" || !got.Expires.Equal(t0.Add(time.Hour)) {
		t.Fatalf("got %+v", got)
	}
	if err := s.RevokeSession(ctx, h, t0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SessionByHash(ctx, h, t0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("revoked: got %v, want ErrNotFound", err)
	}
	if _, err := s.SessionByHash(ctx, []byte("unknown"), t0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown: got %v", err)
	}
}

func TestDisableRevokesSessions(t *testing.T) {
	s, ctx := open(t), context.Background()
	h := newSession(s, t, t0.Add(time.Hour))
	if _, err := s.SessionByHash(ctx, h, t0); err != nil {
		t.Fatal(err)
	}
	if err := s.DisableApprover(ctx, "alice", t0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SessionByHash(ctx, h, t0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
	var revoked *int64
	if err := s.DB().QueryRow(`SELECT revoked_at FROM sessions`).Scan(&revoked); err != nil || revoked == nil {
		t.Fatalf("session not revoked: %v %v", revoked, err)
	}
}

func TestSessionExpiry(t *testing.T) {
	s, ctx := open(t), context.Background()
	h := newSession(s, t, t0.Add(time.Hour))
	if _, err := s.SessionByHash(ctx, h, t0.Add(time.Hour-time.Millisecond)); err != nil {
		t.Fatalf("before expiry: %v", err)
	}
	for _, now := range []time.Time{t0.Add(time.Hour), t0.Add(2 * time.Hour)} {
		if _, err := s.SessionByHash(ctx, h, now); !errors.Is(err, ErrNotFound) {
			t.Fatalf("now=%v: got %v, want ErrNotFound", now, err)
		}
	}
}

func TestAuditIsAppendOnly(t *testing.T) {
	s, ctx := open(t), context.Background()
	if _, err := s.AppendAudit(ctx, AuditRow{At: t0, Kind: "login", ActorID: "a1", ActorName: "alice"}); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{`UPDATE audit SET note = 'x'`, `DELETE FROM audit`} {
		_, err := s.DB().Exec(q)
		if err == nil || !strings.Contains(err.Error(), "append-only") {
			t.Errorf("%s: got %v, want append-only error", q, err)
		}
	}
}

func TestAuditPage(t *testing.T) {
	s, ctx := open(t), context.Background()
	var ids []int64
	for i := 0; i < 7; i++ {
		mig := "m1"
		if i%2 == 1 {
			mig = "m2"
		}
		id, err := s.AppendAudit(ctx, AuditRow{At: t0.Add(time.Duration(i) * time.Second), Kind: "gate-decision",
			ActorID: "a1", ActorName: "alice", Migration: mig, Gate: "switch", Decision: "approve",
			Note: fmt.Sprint("n", i), Evidence: "sha256:abc"})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	all, err := s.AuditPage(ctx, "", 0, 100)
	if err != nil || len(all) != 7 {
		t.Fatalf("all: %d, %v", len(all), err)
	}
	if all[0].ID != ids[6] || all[6].ID != ids[0] {
		t.Fatalf("not newest first: %v", all)
	}
	if r := all[0]; r.Note != "n6" || r.Evidence != "sha256:abc" || r.ActorName != "alice" || !r.At.Equal(t0.Add(6*time.Second)) {
		t.Fatalf("row not round-tripped: %+v", r)
	}
	m1, err := s.AuditPage(ctx, "m1", 0, 100)
	if err != nil || len(m1) != 4 {
		t.Fatalf("m1: %d, %v", len(m1), err)
	}
	for _, r := range m1 {
		if r.Migration != "m1" {
			t.Fatalf("leaked %+v", r)
		}
	}
	before, err := s.AuditPage(ctx, "", ids[3], 100)
	if err != nil || len(before) != 3 || before[0].ID != ids[2] {
		t.Fatalf("before: %v, %v", before, err)
	}
	one, err := s.AuditPage(ctx, "", 0, 0)
	if err != nil || len(one) != 1 {
		t.Fatalf("limit 0: %d, %v", len(one), err)
	}
	two, err := s.AuditPage(ctx, "", 0, 2)
	if err != nil || len(two) != 2 {
		t.Fatalf("limit 2: %d, %v", len(two), err)
	}
	for i := 0; i < 600; i++ {
		if _, err := s.AppendAudit(ctx, AuditRow{At: t0, Kind: "login"}); err != nil {
			t.Fatal(err)
		}
	}
	big, err := s.AuditPage(ctx, "", 0, 9999)
	if err != nil || len(big) != 500 {
		t.Fatalf("limit 9999: %d, %v", len(big), err)
	}
}
