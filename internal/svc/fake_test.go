package svc

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
)

// fakeRunner answers commands from a table of exact command strings.
// Root(cmd) is rendered "ROOT(cmd)" so tests read what runs as root.
type fakeRunner struct {
	t     *testing.T
	mu    sync.Mutex
	resp  map[string]fakeResp
	calls []string
	files map[string][]byte
	modes map[string]os.FileMode
	stdin map[string][]byte // last stdin per command
}

type fakeResp struct {
	out string
	err error
}

func newFake(t *testing.T) *fakeRunner {
	return &fakeRunner{t: t, resp: map[string]fakeResp{}, files: map[string][]byte{}, modes: map[string]os.FileMode{}, stdin: map[string][]byte{}}
}

func (f *fakeRunner) on(cmd, out string, err error) { f.resp[cmd] = fakeResp{out, err} }

func (f *fakeRunner) Root(cmd string) string { return "ROOT(" + cmd + ")" }

func (f *fakeRunner) Run(_ context.Context, cmd string, stdin []byte) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, cmd)
	f.stdin[cmd] = stdin
	r, ok := f.resp[cmd]
	if !ok {
		f.t.Errorf("unexpected command: %s", cmd)
		return nil, errors.New("unexpected command")
	}
	return []byte(r.out), r.err
}

func (f *fakeRunner) WriteFile(_ context.Context, p string, mode os.FileMode, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "WRITE "+p)
	f.files[p], f.modes[p] = append([]byte{}, data...), mode
	return nil
}

func (f *fakeRunner) ReadFile(_ context.Context, p string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "READ "+p)
	b, ok := f.files[p]
	if !ok {
		return nil, errors.New("no such file: " + p)
	}
	return b, nil
}

func (f *fakeRunner) Exists(_ context.Context, p string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "EXISTS "+p)
	if _, ok := f.files[p]; ok {
		return true, nil
	}
	for k := range f.files { // a directory exists when a file lives under it
		if strings.HasPrefix(k, p+"/") {
			return true, nil
		}
	}
	return false, nil
}

func (f *fakeRunner) ran(prefix string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.calls {
		if strings.HasPrefix(c, prefix) {
			return true
		}
	}
	return false
}

func sideOf(r Runner) Side {
	return Side{Host: func(context.Context, string) (Runner, error) { return r, nil }}
}

func codes(fs []Finding) map[string]bool {
	m := map[string]bool{}
	for _, f := range fs {
		m[f.Code] = true
	}
	return m
}
