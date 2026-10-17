package frontdoor

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discoverygrpc "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// fakeEnvoy subscribes to listeners, routes and clusters over ADS and
// ACKs (or NACKs) every response, recording what it received.
type fakeEnvoy struct {
	t      *testing.T
	stream discoverygrpc.AggregatedDiscoveryService_StreamAggregatedResourcesClient
	mu     sync.Mutex
	got    map[string][]string // type -> versions received
	nack   func(typ, version string) bool
}

func dialEnvoy(t *testing.T, addr string, nack func(typ, version string) bool) *fakeEnvoy {
	t.Helper()
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	st, err := discoverygrpc.NewAggregatedDiscoveryServiceClient(cc).StreamAggregatedResources(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	e := &fakeEnvoy{t: t, stream: st, got: map[string][]string{}, nack: nack}
	node := &corev3.Node{Id: NodeID, Cluster: "cutover"}
	for i, typ := range []string{resource.ClusterType, resource.ListenerType, resource.RouteType} {
		req := &discoverygrpc.DiscoveryRequest{TypeUrl: typ}
		if i == 0 {
			req.Node = node
		}
		if typ == resource.RouteType {
			req.ResourceNames = []string{RouteConfigName}
		}
		if err := st.Send(req); err != nil {
			t.Fatal(err)
		}
	}
	go e.loop()
	return e
}

func (e *fakeEnvoy) loop() {
	acked := map[string]string{}
	for {
		resp, err := e.stream.Recv()
		if err != nil {
			return
		}
		e.mu.Lock()
		e.got[resp.TypeUrl] = append(e.got[resp.TypeUrl], resp.VersionInfo)
		e.mu.Unlock()
		req := &discoverygrpc.DiscoveryRequest{TypeUrl: resp.TypeUrl, ResponseNonce: resp.Nonce, VersionInfo: resp.VersionInfo}
		if resp.TypeUrl == resource.RouteType {
			req.ResourceNames = []string{RouteConfigName}
		}
		if e.nack != nil && e.nack(resp.TypeUrl, resp.VersionInfo) {
			req.VersionInfo = acked[resp.TypeUrl]
			req.ErrorDetail = &status.Status{Code: 3, Message: "fake rejection of " + resp.VersionInfo}
		} else {
			acked[resp.TypeUrl] = resp.VersionInfo
		}
		if err := e.stream.Send(req); err != nil {
			return
		}
	}
}

func (e *fakeEnvoy) versions(typ string) []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string{}, e.got[typ]...)
}

func startXDS(t *testing.T, store *Store, drain time.Duration) (*XDS, string) {
	t.Helper()
	x, err := NewXDS(XDSConfig{Listener: Listener{Address: "127.0.0.1", Port: 18080}, Store: store, Drain: drain, AckTimeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	l, err := ListenXDS("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go x.Serve(l)
	t.Cleanup(x.Stop)
	return x, l.Addr().String()
}

func newStore(t *testing.T) *Store { return &Store{Path: filepath.Join(t.TempDir(), "fd.json")} }

func TestApplyWaitsForAckAndPersists(t *testing.T) {
	store := newStore(t)
	x, addr := startXDS(t, store, 0)
	dialEnvoy(t, addr, nil)
	if err := x.Apply(context.Background(), base()); err != nil {
		t.Fatal(err)
	}
	saved, err := store.Load()
	if err != nil || len(saved) != 1 || saved[0].Key() != "shop/api" {
		t.Fatalf("saved %+v %v", saved, err)
	}
	if r, ok := x.Route("shop/api"); !ok || r.Source.Port != 8080 {
		t.Fatalf("route %+v", r)
	}
}

func TestApplyWithoutEnvoyFails(t *testing.T) {
	store := newStore(t)
	x, _ := startXDS(t, store, 0)
	err := x.Apply(context.Background(), base())
	if err == nil || !strings.Contains(err.Error(), "no Envoy is connected") {
		t.Fatalf("got %v", err)
	}
	if saved, _ := store.Load(); len(saved) != 0 {
		t.Fatal("an unconfirmed change must not be saved")
	}
}

func TestApplyStagesAddsAndRemovals(t *testing.T) {
	x, addr := startXDS(t, newStore(t), 50*time.Millisecond)
	e := dialEnvoy(t, addr, nil)
	ctx := context.Background()
	if err := x.Apply(ctx, base()); err != nil {
		t.Fatal(err)
	}
	n0 := len(e.versions(resource.ClusterType))
	// Adding a target and weighting it in one change: clusters first
	// (S1), then routes (S2); nothing is removed, so no S3.
	r := base()
	r.Target, r.TargetWeight = &Upstream{Address: "10.8.0.20", Port: 8080}, 10
	if err := x.Apply(ctx, r); err != nil {
		t.Fatal(err)
	}
	if got := len(e.versions(resource.ClusterType)) - n0; got != 2 {
		t.Fatalf("adding a cluster and routing to it takes 2 snapshots, took %d", got)
	}
	// Dropping the source: routes first (S2), clusters after the drain (S3).
	n1 := len(e.versions(resource.ClusterType))
	r.Source, r.TargetWeight = nil, 100
	began := time.Now()
	if err := x.Apply(ctx, r); err != nil {
		t.Fatal(err)
	}
	if got := len(e.versions(resource.ClusterType)) - n1; got != 2 {
		t.Fatalf("removing a cluster takes 2 snapshots, took %d", got)
	}
	if time.Since(began) < 50*time.Millisecond {
		t.Fatal("the drain must pass between unrouting a cluster and removing it")
	}
	// The same route again changes nothing and sends nothing.
	n2 := len(e.versions(resource.ClusterType))
	if err := x.Apply(ctx, r); err != nil {
		t.Fatal(err)
	}
	if len(e.versions(resource.ClusterType)) != n2 {
		t.Fatal("an unchanged route must not send a snapshot")
	}
}

func TestApplyNackRestoresAndFails(t *testing.T) {
	store := newStore(t)
	x, addr := startXDS(t, store, 0)
	var reject string
	var mu sync.Mutex
	dialEnvoy(t, addr, func(typ, v string) bool {
		mu.Lock()
		defer mu.Unlock()
		return typ == resource.ClusterType && v == reject
	})
	ctx := context.Background()
	if err := x.Apply(ctx, base()); err != nil {
		t.Fatal(err)
	}
	// Reject whatever version comes next.
	mu.Lock()
	reject = x.nextVersion()
	mu.Unlock()
	r := base()
	r.Target, r.TargetWeight = &Upstream{Address: "10.8.0.20", Port: 8080}, 10
	err := x.Apply(ctx, r)
	if err == nil || !strings.Contains(err.Error(), "fake rejection") {
		t.Fatalf("want Envoy's rejection in the error, got %v", err)
	}
	if got, _ := x.Route("shop/api"); got.Target != nil {
		t.Fatal("a rejected change must not become the current route")
	}
	if saved, _ := store.Load(); len(saved) != 1 || saved[0].Target != nil {
		t.Fatalf("a rejected change must not be saved: %+v", saved)
	}
}

func TestApplyRefusesConflictAndInvalid(t *testing.T) {
	x, addr := startXDS(t, newStore(t), 0)
	dialEnvoy(t, addr, nil)
	ctx := context.Background()
	if err := x.Apply(ctx, base()); err != nil {
		t.Fatal(err)
	}
	other := base()
	other.Service = "web"
	if err := x.Apply(ctx, other); !errorsIs(err, ErrConflict) {
		t.Fatalf("want ErrConflict, got %v", err)
	}
	bad := base()
	bad.TargetWeight = 7
	if err := x.Apply(ctx, bad); !errorsIs(err, ErrInvalid) {
		t.Fatalf("want ErrInvalid, got %v", err)
	}
}

func TestRestartServesStoredRoutes(t *testing.T) {
	store := newStore(t)
	x, addr := startXDS(t, store, 0)
	dialEnvoy(t, addr, nil)
	if err := x.Apply(context.Background(), base()); err != nil {
		t.Fatal(err)
	}
	x.Stop()
	// A new server on the same store has the route before any Envoy asks.
	x2, err := NewXDS(XDSConfig{Listener: Listener{Address: "127.0.0.1", Port: 18080}, Store: store, AckTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := x2.Route("shop/api"); !ok {
		t.Fatal("restarted server lost the route")
	}
}

func TestListenXDSRefusesNonLoopback(t *testing.T) {
	for _, a := range []string{"0.0.0.0:18000", ":18000", "192.0.2.1:18000"} {
		if l, err := ListenXDS(a); err == nil {
			l.Close()
			t.Fatalf("%s must be refused", a)
		}
	}
	sock := filepath.Join(t.TempDir(), "xds.sock")
	l, err := ListenXDS("unix:" + sock)
	if err != nil {
		t.Fatal(err)
	}
	l.Close()
	var _ net.Listener = l
}

func errorsIs(err, target error) bool { return err != nil && errors.Is(err, target) }

func TestStopBeforeServeMakesServeReturn(t *testing.T) {
	x, err := NewXDS(XDSConfig{Listener: Listener{Address: "127.0.0.1", Port: 18080}, Store: newStore(t), AckTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	x.Stop()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	done := make(chan error, 1)
	go func() { done <- x.Serve(l) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Serve after Stop must return an error")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve after Stop must return promptly")
	}
}
