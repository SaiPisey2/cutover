package console

import (
	"context"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/SaiPisey2/cutover/internal/flow"
	"github.com/SaiPisey2/cutover/internal/tlsutil"
	"github.com/stretchr/testify/require"
)

// clientEnv is the real API behind a TLS test server, with a client that
// trusts the test server's certificate.
type clientEnv struct {
	*apiEnv
	srv *httptest.Server
	c   *Client
}

func newClientEnv(t *testing.T) *clientEnv {
	t.Helper()
	e := newAPIEnv(t)
	srv := httptest.NewTLSServer(NewServer(e.a, e.mux, testUI))
	t.Cleanup(srv.Close)
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	c, err := NewClient(srv.URL, ca)
	require.NoError(t, err)
	return &clientEnv{apiEnv: e, srv: srv, c: c}
}

func TestClientRefusesUntrustedCert(t *testing.T) {
	e := newClientEnv(t)
	_, other, err := tlsutil.LoadOrCreate(filepath.Join(t.TempDir(), "tls"), []string{"127.0.0.1"})
	require.NoError(t, err)
	c, err := NewClient(e.srv.URL, other)
	require.NoError(t, err)
	err = c.Login(context.Background(), "alice", alicePW)
	require.Error(t, err)
	require.Contains(t, err.Error(), "certificate")
	require.Empty(t, e.audit("login"), "no request may reach the server")
}

func TestNewClientChecksItsInputs(t *testing.T) {
	ca := []byte("not a pem")
	for _, base := range []string{"http://127.0.0.1:7700", "127.0.0.1:7700", "https://", "https://u:p@127.0.0.1:7700"} {
		_, err := NewClient(base, ca)
		require.Error(t, err, base)
	}
	_, err := NewClient("https://127.0.0.1:7700", ca)
	require.ErrorContains(t, err, "no PEM certificate")
}

func TestClientLoginAndDecide(t *testing.T) {
	e := newClientEnv(t)
	ctx := context.Background()
	err := e.c.Login(ctx, "alice", "wrong password here")
	require.EqualError(t, err, errBadLoginText)
	require.NoError(t, e.c.Login(ctx, "alice", alicePW))

	e.fake.open("m1", "prepare", 1)
	require.EqualError(t, e.c.Decide(ctx, "m1", "switch", flow.Approve, ""), "gate is not open (open: prepare)")
	require.Empty(t, e.fake.gateCalls())

	require.NoError(t, e.c.Decide(ctx, "m1", "prepare", flow.Approve, "looks good"))
	g := e.fake.gateCalls()
	require.Len(t, g, 1)
	require.Equal(t, flow.GateSignal{Gate: "prepare", Decision: flow.Approve, ApproverID: "alice#a1", Note: "looks good"}, g[0])
	rows := e.audit("gate-decision")
	require.Len(t, rows, 1)
	require.NotEmpty(t, rows[0].Evidence)

	// The server's own refusal comes back as its fixed text.
	err = e.c.Decide(ctx, "m1", "prepare", flow.Approve, "")
	require.EqualError(t, err, "you already approved this gate")
	err = e.c.Decide(ctx, "nope", "prepare", flow.Approve, "")
	require.EqualError(t, err, "no such migration")
}

func TestClientStartStatusRollback(t *testing.T) {
	e := newClientEnv(t)
	ctx := context.Background()
	_, err := e.c.Status(ctx, "m1")
	var ae *APIError
	require.ErrorAs(t, err, &ae)
	require.Equal(t, http.StatusUnauthorized, ae.Status)

	require.NoError(t, e.c.Login(ctx, "alice", alicePW))
	id, err := e.c.Start(ctx, "", []byte(examplePlan(t)))
	require.NoError(t, err)
	require.NotEmpty(t, id)
	require.Equal(t, []string{id}, e.fake.starts)
	_, err = e.c.Start(ctx, id, []byte(examplePlan(t)))
	require.EqualError(t, err, "migration already exists")

	st, err := e.c.Status(ctx, id)
	require.NoError(t, err)
	require.Equal(t, id, st.Status.MigrationID)
	require.Equal(t, flow.EvidenceDigest(st.Status), st.EvidenceDigest)

	require.NoError(t, e.c.Rollback(ctx, id, "pager"))
	require.Len(t, e.fake.rollbacks, 1)
	require.Equal(t, "alice#a1", e.fake.rollbacks[0].ApproverID)
	require.EqualError(t, e.c.Rollback(ctx, id, " "), "a reason is required")
}

func TestClientFollowsNoRedirect(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://example.com/api/login", http.StatusFound)
	}))
	defer srv.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	c, err := NewClient(srv.URL, ca)
	require.NoError(t, err)
	err = c.Login(context.Background(), "alice", alicePW)
	require.EqualError(t, err, "the server answered 302 Found")
}

func TestClientQuotesAnUnprintableRefusal(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusConflict, `{"error":"bad\u001b[2Jthing"}`)
	}))
	defer srv.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	c, err := NewClient(srv.URL, ca)
	require.NoError(t, err)
	err = c.Rollback(context.Background(), "m1", "r")
	require.EqualError(t, err, `"bad\x1b[2Jthing"`)
}
