package frontdoor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discoverygrpc "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/envoyproxy/go-control-plane/pkg/log"
	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	serverv3 "github.com/envoyproxy/go-control-plane/pkg/server/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// NodeID is the node ID every front-door Envoy uses (see bootstrap.go):
// they all receive the same snapshot.
const NodeID = "cutover-frontdoor"

var (
	ErrConflict = errors.New("route conflict")
	ErrInvalid  = errors.New("invalid route")
)

var errNoEnvoy = errors.New("no Envoy is connected to the xDS server; start the front door's Envoy (cutover envoy-bootstrap)")

// ackTypes are the types every Envoy must ACK before a stage counts as applied.
var ackTypes = []string{resource.ListenerType, resource.RouteType, resource.ClusterType}

type XDSConfig struct {
	Listener   Listener
	Store      *Store
	Drain      time.Duration // between unrouting a cluster and removing it
	AckTimeout time.Duration // per stage; default 30s
}

type XDS struct {
	cfg    XDSConfig
	cache  cache.SnapshotCache
	acks   *ackTracker
	grpc   *grpc.Server
	boot   string
	applyM sync.Mutex // serializes Apply
	mu     sync.Mutex // guards routes, seq and cur
	routes []Route
	seq    int
	cur    string // version of the snapshot last set
}

// NewXDS loads the stored routes and sets them as the first snapshot, so
// the first Envoy to connect (or reconnect after a restart) gets them.
func NewXDS(cfg XDSConfig) (*XDS, error) {
	if cfg.AckTimeout <= 0 {
		cfg.AckTimeout = 30 * time.Second
	}
	routes, err := cfg.Store.Load()
	if err != nil {
		return nil, err
	}
	x := &XDS{
		cfg:    cfg,
		cache:  cache.NewSnapshotCache(true, cache.IDHash{}, log.LoggerFuncs{}),
		acks:   newAckTracker(),
		boot:   strconv.FormatInt(time.Now().UnixNano(), 36),
		routes: routes,
	}
	if _, err := x.set(routes, routes); err != nil {
		return nil, err
	}
	x.grpc = grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: 30 * time.Second, Timeout: 5 * time.Second}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 30 * time.Second, PermitWithoutStream: true}),
	)
	discoverygrpc.RegisterAggregatedDiscoveryServiceServer(x.grpc, serverv3.NewServer(context.Background(), x.cache, x.acks.callbacks()))
	return x, nil
}

// nextVersion is the version the next snapshot will carry (tests use it).
func (x *XDS) nextVersion() string {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.boot + "." + strconv.Itoa(x.seq+1)
}

// set builds and installs one snapshot and returns its version.
func (x *XDS) set(routes, clusters []Route) (string, error) {
	res, err := Build(routes, clusters, x.cfg.Listener)
	if err != nil {
		return "", err
	}
	x.mu.Lock()
	x.seq++
	v := x.boot + "." + strconv.Itoa(x.seq)
	x.mu.Unlock()
	snap, err := cache.NewSnapshot(v, res)
	if err != nil {
		return "", err
	}
	if err := snap.Consistent(); err != nil {
		return "", err
	}
	x.acks.expect(v)
	if err := x.cache.SetSnapshot(context.Background(), NodeID, snap); err != nil {
		return "", err
	}
	x.mu.Lock()
	x.cur = v
	x.mu.Unlock()
	return v, nil
}

func (x *XDS) Routes() []Route {
	x.mu.Lock()
	defer x.mu.Unlock()
	return slices.Clone(x.routes)
}

func (x *XDS) Route(key string) (Route, bool) {
	for _, r := range x.Routes() {
		if r.Key() == key {
			return r, true
		}
	}
	return Route{}, false
}

// Apply makes r the route for its key (replacing a finished migration's route
// on the same host and prefix, see Route.Finished): stages
// that add clusters before routing to them and unroute clusters before
// removing them, each confirmed by every connected Envoy.
func (x *XDS) Apply(ctx context.Context, r Route) error {
	if err := r.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	x.applyM.Lock()
	defer x.applyM.Unlock()
	old := x.Routes()
	// A finished migration's route on the same host and prefix (even one
	// owned by another plan) is replaced, not a conflict.
	others := slices.DeleteFunc(slices.Clone(old), func(e Route) bool {
		return e.Key() == r.Key() || (e.Finished() && e.Host == r.Host && trimPrefix(e.Prefix) == trimPrefix(r.Prefix))
	})
	if err := CheckConflicts(others, r); err != nil {
		return fmt.Errorf("%w: %v", ErrConflict, err)
	}
	next := append(slices.Clone(others), r)
	slices.SortFunc(next, func(a, b Route) int { return strings.Compare(a.Key(), b.Key()) })

	if prev, ok := x.Route(r.Key()); ok && signature([]Route{prev}, []Route{prev}) == signature([]Route{r}, []Route{r}) {
		x.mu.Lock()
		v := x.cur
		x.mu.Unlock()
		return x.acks.wait(ctx, v, x.cfg.AckTimeout)
	}

	union := unionClusters(old, next)
	type stage struct{ routes, clusters []Route }
	stages := []stage{{old, union}, {next, union}, {next, next}}
	prevSig := signature(old, old)
	removes := len(clusterSet(union)) != len(clusterSet(next))
	for i, s := range stages {
		sig := signature(s.routes, s.clusters)
		if sig == prevSig {
			continue
		}
		prevSig = sig
		if i == 2 && removes && x.cfg.Drain > 0 {
			select {
			case <-time.After(x.cfg.Drain):
			case <-ctx.Done():
				x.restore(old)
				return ctx.Err()
			}
		}
		v, err := x.set(s.routes, s.clusters)
		if err == nil {
			err = x.acks.wait(ctx, v, x.cfg.AckTimeout)
		}
		if err != nil {
			x.restore(old)
			return err
		}
	}
	if err := x.cfg.Store.Save(next); err != nil {
		x.restore(old)
		return fmt.Errorf("save front-door state: %w", err)
	}
	x.mu.Lock()
	x.routes = next
	x.mu.Unlock()
	return nil
}

// restore puts the old routes back after a failed change. It does not
// wait for ACKs: the caller is already returning an error, and the
// activity's retry will confirm the state it needs.
func (x *XDS) restore(old []Route) {
	_, _ = x.set(old, old)
}

// unionClusters: every route of b, plus routes of a whose key b lacks,
// with the sides of a's version of a shared key that b dropped kept too.
func unionClusters(a, b []Route) []Route {
	out := slices.Clone(b)
	for _, ar := range a {
		i := slices.IndexFunc(out, func(br Route) bool { return br.Key() == ar.Key() })
		if i < 0 {
			out = append(out, ar)
			continue
		}
		merged := out[i]
		if merged.Source == nil && ar.Source != nil {
			merged.Source = ar.Source
		}
		if merged.Target == nil && ar.Target != nil {
			merged.Target = ar.Target
		}
		out[i] = merged
	}
	return out
}

func clusterSet(rs []Route) map[string]bool {
	m := map[string]bool{}
	for _, r := range rs {
		for _, n := range r.ClusterNames() {
			m[n] = true
		}
	}
	return m
}

// signature identifies a stage by its routes and cluster definitions.
// Routes are compared by value (JSON), never by pointer.
func signature(routes, clusters []Route) string {
	rb, _ := json.Marshal(routes)
	var b strings.Builder
	b.Write(rb)
	b.WriteString("#")
	defs := map[string]string{}
	for _, r := range clusters {
		if r.Source != nil {
			defs[ClusterName(r.Owner, r.Service, SideSource)] = r.Source.String() + r.Host + r.HealthPath
		}
		if r.Target != nil {
			defs[ClusterName(r.Owner, r.Service, SideTarget)] = r.Target.String() + r.Host + r.HealthPath
		}
	}
	keys := make([]string, 0, len(defs))
	for n := range defs {
		keys = append(keys, n)
	}
	slices.Sort(keys)
	for _, k := range keys {
		b.WriteString(k + "=" + defs[k] + ";")
	}
	return b.String()
}

// Serve runs the ADS gRPC server on l until Stop. After Stop it returns
// grpc.ErrServerStopped, even if Stop came first.
func (x *XDS) Serve(l net.Listener) error { return x.grpc.Serve(l) }

// Stop stops the gRPC server; it is safe before Serve.
func (x *XDS) Stop() { x.grpc.Stop() }

// ListenXDS listens on "unix:/path" or a loopback host:port. The xDS
// server has no authentication, so it never listens anywhere else.
func ListenXDS(addr string) (net.Listener, error) {
	if path, ok := strings.CutPrefix(addr, "unix:"); ok {
		return net.Listen("unix", path)
	}
	if err := RequireLoopback(addr); err != nil {
		return nil, fmt.Errorf("xds: %w", err)
	}
	return net.Listen("tcp", addr)
}

// RequireLoopback refuses any address that is not 127.0.0.0/8, ::1 or "localhost".
func RequireLoopback(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("%s is not a loopback address; this listener has no authentication of its own", addr)
	}
	return nil
}

// ackTracker records, per ADS stream, the last version each type ACKed
// and any NACK received after the latest expected version was set.
type ackTracker struct {
	mu      sync.Mutex
	streams map[int64]*streamState
	want    string
	nack    string // Envoy's error for the expected version, if any
}

type streamState struct {
	node  string
	acked map[string]string
}

func newAckTracker() *ackTracker { return &ackTracker{streams: map[int64]*streamState{}} }

func (a *ackTracker) expect(v string) {
	a.mu.Lock()
	a.want, a.nack = v, ""
	a.mu.Unlock()
}

func (a *ackTracker) callbacks() serverv3.Callbacks {
	return serverv3.CallbackFuncs{
		StreamOpenFunc: func(_ context.Context, id int64, _ string) error {
			a.mu.Lock()
			a.streams[id] = &streamState{acked: map[string]string{}}
			a.mu.Unlock()
			return nil
		},
		StreamClosedFunc: func(id int64, _ *corev3Node) {
			a.mu.Lock()
			delete(a.streams, id)
			a.mu.Unlock()
		},
		StreamRequestFunc: func(id int64, req *discoveryRequest) error {
			a.mu.Lock()
			defer a.mu.Unlock()
			st := a.streams[id]
			if st == nil {
				return nil
			}
			if n := req.GetNode(); n != nil {
				st.node = n.GetId()
			}
			if e := req.GetErrorDetail(); e != nil {
				a.nack = fmt.Sprintf("envoy %s rejected the configuration: %s", st.node, e.GetMessage())
				return nil
			}
			if req.GetVersionInfo() != "" {
				st.acked[req.GetTypeUrl()] = req.GetVersionInfo()
			}
			return nil
		},
	}
}

// wait returns when every connected stream has ACKed v for every type in
// ackTypes, or with an error on NACK, timeout or no connected Envoy.
func (a *ackTracker) wait(ctx context.Context, v string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	t := time.NewTicker(20 * time.Millisecond)
	defer t.Stop()
	for {
		a.mu.Lock()
		nack, n, done := a.nack, len(a.streams), true
		if a.want != v {
			nack = ""
		}
		for _, st := range a.streams {
			for _, typ := range ackTypes {
				if st.acked[typ] != v {
					done = false
				}
			}
		}
		a.mu.Unlock()
		switch {
		case nack != "":
			return errors.New(nack)
		case n == 0:
			return errNoEnvoy
		case done:
			return nil
		case time.Now().After(deadline):
			return fmt.Errorf("envoy did not confirm configuration %s within %s", v, timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

type corev3Node = corev3.Node
type discoveryRequest = discoverygrpc.DiscoveryRequest
