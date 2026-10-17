package frontdoor

import (
	"strings"
	"testing"
)

func base() Route {
	return Route{Owner: "shop", Service: "api", Host: "shop.example.com", Prefix: "/", HealthPath: "/healthz",
		Source: &Upstream{Address: "10.0.1.20", Port: 8080}}
}

func TestClusterName(t *testing.T) {
	if got := ClusterName("shop-eu", "api", SideTarget); got != "c_shop-eu_api_target" {
		t.Fatal(got)
	}
}

func TestRouteValidate(t *testing.T) {
	tgt := &Upstream{Address: "10.8.0.20", Port: 8080}
	for _, tc := range []struct {
		name string
		edit func(*Route)
		want string // "" = valid
	}{
		{"source only", func(r *Route) {}, ""},
		{"both at 10", func(r *Route) { r.Target, r.TargetWeight = tgt, 10 }, ""},
		{"target only", func(r *Route) { r.Source, r.Target, r.TargetWeight = nil, tgt, 100 }, ""},
		{"mirror", func(r *Route) { r.Target, r.Mirror, r.MirrorMethods = tgt, true, []string{"GET"} }, ""},
		{"neither side", func(r *Route) { r.Source = nil }, "a route needs a source or a target"},
		{"weight range", func(r *Route) { r.Target, r.TargetWeight = tgt, 101 }, "target_weight must be between 0 and 100"},
		{"weight without target", func(r *Route) { r.TargetWeight = 5 }, "target_weight must be 0 without a target"},
		{"target only below 100", func(r *Route) { r.Source, r.Target, r.TargetWeight = nil, tgt, 50 }, "target_weight must be 100 without a source"},
		{"mirror without target", func(r *Route) { r.Mirror, r.MirrorMethods = true, []string{"GET"} }, "mirroring needs both a source and a target"},
		{"mirror with weight", func(r *Route) { r.Target, r.TargetWeight, r.Mirror, r.MirrorMethods = tgt, 10, true, []string{"GET"} }, "mirroring needs target_weight 0"},
		{"mirror without methods", func(r *Route) { r.Target, r.Mirror = tgt, true }, "mirroring needs at least one method"},
		{"unsafe method", func(r *Route) { r.Target, r.Mirror, r.MirrorMethods = tgt, true, []string{"DELETE"} }, `"DELETE" is not a safe method`},
		{"owner", func(r *Route) { r.Owner = "Shop" }, "owner"},
		{"service", func(r *Route) { r.Service = "a_b" }, "service"},
		{"host", func(r *Route) { r.Host = "x y" }, "host"},
		{"prefix", func(r *Route) { r.Prefix = "x" }, "prefix"},
		{"health", func(r *Route) { r.HealthPath = "" }, "health_path"},
		{"port", func(r *Route) { r.Source.Port = 0 }, "source: port must be between 1 and 65535"},
		{"address", func(r *Route) { r.Source.Address = "a b" }, "source: address"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := base()
			r.Source = &Upstream{Address: "10.0.1.20", Port: 8080}
			tc.edit(&r)
			err := r.Validate()
			switch {
			case tc.want == "" && err != nil:
				t.Fatalf("want valid, got %v", err)
			case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
				t.Fatalf("want %q, got %v", tc.want, err)
			}
		})
	}
}

func TestCheckConflicts(t *testing.T) {
	a := base()
	b := base()
	b.Service = "web"
	if err := CheckConflicts([]Route{a}, b); err == nil || !strings.Contains(err.Error(), "shop.example.com/ is already routed by shop/api") {
		t.Fatalf("got %v", err)
	}
	b.Prefix = "/web"
	if err := CheckConflicts([]Route{a}, b); err != nil {
		t.Fatal(err)
	}
	c := base()
	c.Owner, c.Prefix = "other", "/billing/"
	d := base()
	d.Prefix = "/billing"
	if err := CheckConflicts([]Route{d}, c); err == nil {
		t.Fatal("a trailing slash must not hide a conflict")
	}
	// The same key replaces itself; never a conflict.
	if err := CheckConflicts([]Route{a}, a); err != nil {
		t.Fatal(err)
	}
}

func TestParseUpstream(t *testing.T) {
	u, err := ParseUpstream("10.8.0.20:8080")
	if err != nil || u != (Upstream{Address: "10.8.0.20", Port: 8080}) || u.String() != "10.8.0.20:8080" {
		t.Fatalf("%+v %v", u, err)
	}
	if _, err := ParseUpstream("10.8.0.20"); err == nil {
		t.Fatal("want an error without a port")
	}
}
