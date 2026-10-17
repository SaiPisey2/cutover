package kube

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/SaiPisey2/cutover/internal/plan"
)

// client-go sends credentials only over TLS, so the fake servers speak TLS.
func kubeconfig(server, token string) string {
	return `apiVersion: v1
kind: Config
clusters:
- name: c
  cluster: {server: ` + server + `, insecure-skip-tls-verify: true}
users:
- name: u
  user: {token: ` + token + `}
contexts:
- name: target
  context: {cluster: c, user: u}
current-context: target
`
}

func TestClientUsesOnlyTheExplicitFile(t *testing.T) {
	hit := false
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit = true
		if r.Header.Get("Authorization") != "Bearer right-token" {
			t.Errorf("wrong credentials reached the server: %q %s", r.Header.Get("Authorization"), r.URL)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"major":"1","minor":"37","gitVersion":"v1.37.0"}`))
	}))
	defer srv.Close()
	decoy := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("the default kubeconfig was used: %s", r.URL)
	}))
	defer decoy.Close()

	home := t.TempDir()
	os.MkdirAll(filepath.Join(home, ".kube"), 0o700)
	os.WriteFile(filepath.Join(home, ".kube", "config"), []byte(kubeconfig(decoy.URL, "wrong-token")), 0o600)
	t.Setenv("HOME", home)
	envFile := filepath.Join(t.TempDir(), "env-kubeconfig")
	os.WriteFile(envFile, []byte(kubeconfig(decoy.URL, "wrong-token")), 0o600)
	t.Setenv("KUBECONFIG", envFile)

	path := filepath.Join(t.TempDir(), "target.kubeconfig")
	os.WriteFile(path, []byte(kubeconfig(srv.URL, "right-token")), 0o600)
	c, err := Client(plan.Kube{Kubeconfig: path, Context: "target"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Discovery().ServerVersion(); err != nil {
		t.Fatal(err)
	}
	if !hit {
		t.Fatal("the explicit kubeconfig's server was not used")
	}
}

func TestClientErrorsDoNotQuoteTheFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.kubeconfig")
	os.WriteFile(path, []byte("users:\n- user: {token: SECRET-TOKEN-123}\n  {{{ not yaml"), 0o600)
	_, err := Client(plan.Kube{Kubeconfig: path, Context: "x"})
	if err == nil || strings.Contains(err.Error(), "SECRET-TOKEN") {
		t.Fatalf("got %v", err)
	}
	good := filepath.Join(t.TempDir(), "good.kubeconfig")
	os.WriteFile(good, []byte(kubeconfig("https://127.0.0.1:1", "t")), 0o600)
	if _, err := Client(plan.Kube{Kubeconfig: good, Context: "nope"}); err == nil || !strings.Contains(err.Error(), `context "nope"`) {
		t.Fatalf("missing context: %v", err)
	}
	if _, err := Client(plan.Kube{Kubeconfig: filepath.Join(t.TempDir(), "absent"), Context: "x"}); err == nil {
		t.Fatal("a missing file is an error")
	}
}
