package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeStore struct {
	mu       sync.Mutex
	orders   map[int64]order
	next     int64
	ticks    [][2]any
	tickFail int
	onTick   func(pod string, seq int64)
}

func newFakeStore() *fakeStore { return &fakeStore{orders: map[int64]order{}} }

func (f *fakeStore) InsertOrder(_ context.Context, note string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.next++
	f.orders[f.next] = order{ID: f.next, Note: note}
	return f.next, nil
}

func (f *fakeStore) GetOrder(_ context.Context, id int64) (order, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.orders[id]
	if !ok {
		return order{}, errNotFound
	}
	return o, nil
}

func (f *fakeStore) InsertTick(_ context.Context, pod string, seq int64) error {
	f.mu.Lock()
	f.ticks = append(f.ticks, [2]any{pod, seq})
	fail := f.tickFail > 0
	if fail {
		f.tickFail--
	}
	cb := f.onTick
	f.mu.Unlock()
	if cb != nil {
		cb(pod, seq)
	}
	if fail {
		return errors.New("boom")
	}
	return nil
}

func useFailFile(t *testing.T) string {
	t.Helper()
	old := failFile
	failFile = filepath.Join(t.TempDir(), "fail")
	t.Cleanup(func() { failFile = old })
	return failFile
}

func do(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestHealthzAlwaysOK(t *testing.T) {
	path := useFailFile(t)
	h := newMux(newFakeStore())
	if rec := do(t, h, "GET", "/healthz", ""); rec.Code != 200 || rec.Body.String() != "ok" {
		t.Fatalf("healthz = %d %q", rec.Code, rec.Body.String())
	}
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if rec := do(t, h, "GET", "/healthz", ""); rec.Code != 200 || rec.Body.String() != "ok" {
		t.Fatalf("healthz with fail file = %d %q", rec.Code, rec.Body.String())
	}
}

func TestOrdersRoundTrip(t *testing.T) {
	useFailFile(t)
	h := newMux(newFakeStore())
	host, _ := os.Hostname()

	rec := do(t, h, "POST", "/orders", "hello")
	if rec.Code != 201 {
		t.Fatalf("post = %d", rec.Code)
	}
	var created struct{ ID int64 }
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil || created.ID == 0 {
		t.Fatalf("post body %q: %v", rec.Body.String(), err)
	}
	if got := rec.Header().Get("X-Demo-Host"); got != host {
		t.Fatalf("X-Demo-Host = %q, want %q", got, host)
	}

	rec = do(t, h, "GET", "/orders/1", "")
	var got order
	if rec.Code != 200 || json.Unmarshal(rec.Body.Bytes(), &got) != nil || got.ID != created.ID || got.Note != "hello" {
		t.Fatalf("get = %d %q", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("X-Demo-Host") == "" {
		t.Fatal("get response lacks X-Demo-Host")
	}

	rec = do(t, h, "GET", "/orders/99", "")
	if rec.Code != 404 || rec.Header().Get("X-Demo-Host") == "" {
		t.Fatalf("unknown id = %d", rec.Code)
	}
	if rec = do(t, h, "GET", "/orders/abc", ""); rec.Code != 400 {
		t.Fatalf("non-numeric id = %d", rec.Code)
	}
}

func TestFailFileBreaksEverythingButHealth(t *testing.T) {
	path := useFailFile(t)
	h := newMux(newFakeStore())
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	for _, c := range [][3]string{{"POST", "/orders", "x"}, {"GET", "/orders/1", ""}} {
		rec := do(t, h, c[0], c[1], c[2])
		body, _ := io.ReadAll(rec.Body)
		if rec.Code != 503 || strings.TrimSpace(string(body)) != "injected fault" {
			t.Fatalf("%s %s = %d %q", c[0], c[1], rec.Code, body)
		}
		if rec.Header().Get("X-Demo-Host") == "" {
			t.Fatalf("%s %s lacks X-Demo-Host", c[0], c[1])
		}
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if rec := do(t, h, "POST", "/orders", "x"); rec.Code != 201 {
		t.Fatalf("after removing fail file post = %d", rec.Code)
	}
	if rec := do(t, h, "GET", "/orders/1", ""); rec.Code != 200 {
		t.Fatalf("after removing fail file get = %d", rec.Code)
	}
}

func TestWorkerRetriesSameSeq(t *testing.T) {
	f := newFakeStore()
	f.tickFail = 2
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.onTick = func(_ string, seq int64) {
		if seq == 2 {
			cancel()
		}
	}
	done := make(chan struct{})
	go func() {
		runWorker(ctx, f, "pod-a", time.Millisecond, time.Millisecond, 4*time.Millisecond, func(string, ...any) {})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop")
	}
	want := [][2]any{{"pod-a", int64(1)}, {"pod-a", int64(1)}, {"pod-a", int64(1)}, {"pod-a", int64(2)}}
	if len(f.ticks) != len(want) {
		t.Fatalf("ticks = %v, want %v", f.ticks, want)
	}
	for i := range want {
		if f.ticks[i] != want[i] {
			t.Fatalf("ticks = %v, want %v", f.ticks, want)
		}
	}
}
