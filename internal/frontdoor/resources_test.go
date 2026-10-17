package frontdoor

import (
	"testing"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"
)

type typesResource = types.Resource

var lst = Listener{Address: "127.0.0.1", Port: 18080}

func routeConfigOf(t *testing.T, res map[resource.Type][]typesResource) *routev3.RouteConfiguration {
	t.Helper()
	rs := res[resource.RouteType]
	if len(rs) != 1 {
		t.Fatalf("want one route configuration, got %d", len(rs))
	}
	return rs[0].(*routev3.RouteConfiguration)
}

func clusters(res map[resource.Type][]typesResource) map[string]*clusterv3.Cluster {
	out := map[string]*clusterv3.Cluster{}
	for _, c := range res[resource.ClusterType] {
		cl := c.(*clusterv3.Cluster)
		out[cl.Name] = cl
	}
	return out
}

func build(t *testing.T, routes, cls []Route) map[resource.Type][]typesResource {
	t.Helper()
	res, err := Build(routes, cls, lst)
	if err != nil {
		t.Fatal(err)
	}
	snap, err := cache.NewSnapshot("1", res)
	if err != nil {
		t.Fatal(err)
	}
	if err := snap.Consistent(); err != nil {
		t.Fatalf("snapshot not consistent: %v", err)
	}
	return res
}

func TestBuildSourceOnly(t *testing.T) {
	r := base()
	res := build(t, []Route{r}, []Route{r})
	if l := res[resource.ListenerType]; len(l) != 1 || l[0].(*listenerv3.Listener).Name != ListenerName {
		t.Fatalf("listener %v", l)
	}
	cl := clusters(res)
	src := cl["c_shop_api_source"]
	if len(cl) != 1 || src == nil {
		t.Fatalf("clusters %v", cl)
	}
	hc := src.HealthChecks[0].GetHttpHealthCheck()
	if hc.Path != "/healthz" || hc.Host != "shop.example.com" {
		t.Fatalf("health check %+v", hc)
	}
	if src.HealthChecks[0].NoTrafficInterval.AsDuration() > 2_000_000_000 {
		t.Fatal("a cluster without traffic must still be checked often (no_traffic_interval)")
	}
	vh := routeConfigOf(t, res).VirtualHosts
	if len(vh) != 1 || vh[0].Domains[0] != "shop.example.com" || vh[0].Domains[1] != "shop.example.com:*" {
		t.Fatalf("vhosts %+v", vh)
	}
	act := vh[0].Routes[0].GetRoute()
	if act.GetCluster() != "c_shop_api_source" || act.RetryPolicy.RetryOn != "connect-failure,refused-stream,reset-before-request" {
		t.Fatalf("action %+v", act)
	}
}

func TestBuildWeights(t *testing.T) {
	tgt := &Upstream{Address: "10.8.0.20", Port: 8080}
	for _, tc := range []struct {
		w      int
		direct string
	}{{0, "c_shop_api_source"}, {100, "c_shop_api_target"}} {
		r := base()
		r.Target, r.TargetWeight = tgt, tc.w
		act := routeConfigOf(t, build(t, []Route{r}, []Route{r})).VirtualHosts[0].Routes[0].GetRoute()
		if act.GetCluster() != tc.direct {
			t.Fatalf("weight %d: want direct %s, got %+v", tc.w, tc.direct, act)
		}
	}
	r := base()
	r.Target, r.TargetWeight = tgt, 10
	res := build(t, []Route{r}, []Route{r})
	wc := routeConfigOf(t, res).VirtualHosts[0].Routes[0].GetRoute().GetWeightedClusters()
	if len(wc.Clusters) != 2 || wc.Clusters[0].Weight.Value != 90 || wc.Clusters[1].Weight.Value != 10 || wc.Clusters[1].Name != "c_shop_api_target" {
		t.Fatalf("weighted %+v", wc)
	}
	if len(clusters(res)) != 2 {
		t.Fatal("both clusters exist")
	}
}

func TestBuildMirrorsSafeMethodsOnly(t *testing.T) {
	r := base()
	r.Target, r.Mirror, r.MirrorMethods = &Upstream{Address: "10.8.0.20", Port: 8080}, true, []string{"HEAD", "GET"}
	routes := routeConfigOf(t, build(t, []Route{r}, []Route{r})).VirtualHosts[0].Routes
	if len(routes) != 2 {
		t.Fatalf("want a mirroring route and a plain route, got %d", len(routes))
	}
	m := routes[0]
	h := m.Match.Headers[0]
	if h.Name != ":method" || h.GetStringMatch().GetSafeRegex().Regex != "^(GET|HEAD)$" {
		t.Fatalf("method match %+v", h)
	}
	mp := m.GetRoute().RequestMirrorPolicies[0]
	if mp.Cluster != "c_shop_api_target" || !mp.DisableShadowHostSuffixAppend || mp.RuntimeFraction.DefaultValue.Numerator != 100 {
		t.Fatalf("mirror policy %+v", mp)
	}
	if len(routes[1].Match.Headers) != 0 || len(routes[1].GetRoute().RequestMirrorPolicies) != 0 {
		t.Fatal("the plain route neither matches methods nor mirrors")
	}
}

func TestBuildOrdersPrefixesAndUnionsClusters(t *testing.T) {
	a := base()
	b := base()
	b.Service, b.Prefix = "billing", "/billing"
	b.Source = &Upstream{Address: "10.0.1.20", Port: 9090}
	old := base()
	old.Service, old.Host = "web", "www.example.com"
	res := build(t, []Route{a, b}, []Route{a, b, old})
	vh := routeConfigOf(t, res).VirtualHosts
	if len(vh) != 1 || vh[0].Routes[0].Match.GetPrefix() != "/billing" || vh[0].Routes[1].Match.GetPrefix() != "/" {
		t.Fatalf("longest prefix first: %+v", vh)
	}
	if cl := clusters(res); len(cl) != 3 || cl["c_shop_web_source"] == nil {
		t.Fatalf("clusters come from the cluster set: %v", cl)
	}
}

func TestBuildRejectsRouteWithoutItsCluster(t *testing.T) {
	r := base()
	r.Target, r.TargetWeight = &Upstream{Address: "10.8.0.20", Port: 8080}, 10
	onlySource := base()
	if _, err := Build([]Route{r}, []Route{onlySource}, lst); err == nil {
		t.Fatal("a route must never reference a cluster the snapshot lacks")
	}
}

func TestBuildEmpty(t *testing.T) {
	res := build(t, nil, nil)
	if len(routeConfigOf(t, res).VirtualHosts) != 0 || len(res[resource.ListenerType]) != 1 {
		t.Fatal("no routes still means a listener and an empty route configuration")
	}
}
