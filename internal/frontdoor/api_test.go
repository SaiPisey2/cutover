package frontdoor

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const clustersJSON = `{"cluster_statuses":[
 {"name":"c_shop_api_source","host_statuses":[{"health_status":{"eds_health_status":"HEALTHY"}}]},
 {"name":"c_shop_api_target","host_statuses":[{"health_status":{"eds_health_status":"HEALTHY","failed_active_health_check":true}}]},
 {"name":"c_shop_web_source","host_statuses":[{"health_status":{"eds_health_status":"HEALTHY","pending_active_hc":true}}]}
]}`

func fakeAdmin(t *testing.T) *Admin {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/clusters":
			if r.URL.Query().Get("format") != "json" {
				http.Error(w, "want format=json", 400)
				return
			}
			w.Write([]byte(clustersJSON))
		case "/stats/prometheus":
			w.Write([]byte(promText))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return &Admin{Base: srv.URL, HTTP: srv.Client()}
}

const promText = `envoy_cluster_upstream_rq_total{envoy_cluster_name="c_shop_api_source"} 120
envoy_cluster_upstream_rq_xx{envoy_response_code_class="5",envoy_cluster_name="c_shop_api_source"} 3
`

func apiServer(t *testing.T) (*Client, *XDS) {
	t.Helper()
	x, addr := startXDS(t, newStore(t), 0)
	dialEnvoy(t, addr, nil)
	api := &API{XDS: x, Admin: fakeAdmin(t), Token: "s3cret-token"}
	srv := httptest.NewServer(api.Handler())
	t.Cleanup(srv.Close)
	return NewClient(srv.URL, "s3cret-token"), x
}

func TestAPIRequiresToken(t *testing.T) {
	c, _ := apiServer(t)
	c.Token = "wrong"
	_, err := c.Routes(context.Background())
	var ae *APIError
	if !errors.As(err, &ae) || ae.Status != 401 || strings.Contains(ae.Message, "s3cret") {
		t.Fatalf("got %v", err)
	}
}

func TestAPIApplyAndList(t *testing.T) {
	c, x := apiServer(t)
	ctx := context.Background()
	if err := c.Apply(ctx, base()); err != nil {
		t.Fatal(err)
	}
	if _, ok := x.Route("shop/api"); !ok {
		t.Fatal("not applied")
	}
	rs, err := c.Routes(ctx)
	if err != nil || len(rs) != 1 || rs[0].Key() != "shop/api" {
		t.Fatalf("%+v %v", rs, err)
	}
	other := base()
	other.Service = "web"
	err = c.Apply(ctx, other)
	var ae *APIError
	if !errors.As(err, &ae) || ae.Status != 409 || !ae.Permanent() {
		t.Fatalf("conflict: %v", err)
	}
	bad := base()
	bad.TargetWeight = 3
	if err := c.Apply(ctx, bad); !errors.As(err, &ae) || ae.Status != 400 {
		t.Fatalf("invalid: %v", err)
	}
}

func TestAPIPathMustMatchBody(t *testing.T) {
	_, x := apiServer(t)
	h := (&API{XDS: x, Token: "t"}).Handler()
	req := httptest.NewRequest("PUT", "/v1/routes/shop/web", strings.NewReader(`{"owner":"shop","service":"api"}`))
	req.Header.Set("Authorization", "Bearer t")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 400 {
		t.Fatalf("code %d body %s", rec.Code, rec.Body)
	}
	req = httptest.NewRequest("PUT", "/v1/routes/shop/api", strings.NewReader(`{"owner":"shop","service":"api","nope":1}`))
	req.Header.Set("Authorization", "Bearer t")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 400 {
		t.Fatalf("unknown fields must be refused: %d", rec.Code)
	}
	req = httptest.NewRequest("PUT", "/v1/routes/shop/api", strings.NewReader(`{"owner":"`+strings.Repeat("a", 70000)+`"}`))
	req.Header.Set("Authorization", "Bearer t")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 413 {
		t.Fatalf("oversized body: %d", rec.Code)
	}
}

func TestAPIHealthAndStats(t *testing.T) {
	c, _ := apiServer(t)
	ctx := context.Background()
	r := base()
	r.Target = &Upstream{Address: "10.8.0.20", Port: 8080}
	if err := c.Apply(ctx, r); err != nil {
		t.Fatal(err)
	}
	h, err := c.Health(ctx, "shop", "api")
	if err != nil || h.Source != Healthy || h.Target != Unhealthy {
		t.Fatalf("%+v %v", h, err)
	}
	st, err := c.Stats(ctx, "shop", "api")
	if err != nil || st.Source == nil || st.Source.Requests != 120 || st.Source.Errors != 3 {
		t.Fatalf("source stats %+v %v", st.Source, err)
	}
	if st.Target == nil || st.Target.Requests != 0 {
		t.Fatalf("a side with no requests yet is zeros, not missing: %+v", st.Target)
	}
	var ae *APIError
	if _, err := c.Health(ctx, "shop", "nope"); !errors.As(err, &ae) || ae.Status != 404 {
		t.Fatalf("unknown route: %v", err)
	}
}

func TestAdminHealthStates(t *testing.T) {
	a := fakeAdmin(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for name, want := range map[string]string{
		"c_shop_api_source": Healthy, "c_shop_api_target": Unhealthy, "c_shop_web_source": Pending, "c_nope_x_source": Absent,
	} {
		if got, err := a.Health(ctx, name); err != nil || got != want {
			t.Errorf("%s: got %q %v, want %q", name, got, err, want)
		}
	}
}

func TestListenAPIRefusesNonLoopback(t *testing.T) {
	if l, err := ListenAPI("0.0.0.0:7600"); err == nil {
		l.Close()
		t.Fatal("refused")
	}
}
