package plan

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

const withServices = minimal + `
frontdoor:
  api: http://127.0.0.1:7600
  token: file:/etc/cutover/frontdoor-token
  addresses: [203.0.113.10]
services:
  - name: api
    kind: compose
    route: {host: shop.example.com, health_path: /healthz}
    compose: {project: shop, service: api}
    source: {host: app1, endpoint: 10.0.1.20:8080}
    target: {host: app1, endpoint: 10.8.0.20:8080}
  - name: billing
    kind: systemd
    route: {host: shop.example.com, prefix: /billing, health_path: /billing/health}
    mirror_methods: []
    systemd: {unit: billing.service, files: [/usr/local/bin/billing, /etc/billing/env]}
    source: {host: app1, endpoint: 10.0.1.20:9090}
    target: {host: app1, endpoint: 10.8.0.20:9090}
  - name: worker
    kind: deployment
    deployment: {namespace: shop, name: worker}
  - name: web
    kind: external
    route: {host: www.example.com, health_path: /}
    source: {endpoint: 10.0.1.30:80}
    target: {endpoint: 10.8.0.30:80}
`

// The base plan's environments gain hosts and kube in every test.
func servicesPlan(t *testing.T) Plan {
	t.Helper()
	y := strings.Replace(withServices, "source:\n  postgres:", `source:
  hosts:
    app1: {address: 10.0.1.20, user: deploy, key: file:/etc/cutover/ssh-key, known_hosts: /etc/cutover/known_hosts}
  kube: {kubeconfig: /etc/cutover/a.kubeconfig, context: a}
  postgres:`, 1)
	y = strings.Replace(y, "target:\n  postgres:", `target:
  hosts:
    app1: {address: 10.8.0.20, user: deploy, key: file:/etc/cutover/ssh-key, known_hosts: /etc/cutover/known_hosts, sudo: true}
  kube: {kubeconfig: /etc/cutover/b.kubeconfig, context: b}
  postgres:`, 1)
	p, err := Parse([]byte(y))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestServicesParseAndDefault(t *testing.T) {
	p := servicesPlan(t)
	if got := p.Source.Hosts["app1"].Port; got != 22 {
		t.Errorf("ssh port default = %d, want 22", got)
	}
	api, ok := p.Service("api")
	if !ok || api.Route.Prefix != "/" || !reflect.DeepEqual(api.MirrorMethods, []string{"GET", "HEAD"}) {
		t.Errorf("api defaults: %+v", api)
	}
	billing, _ := p.Service("billing")
	if billing.MirrorMethods == nil || len(billing.MirrorMethods) != 0 {
		t.Errorf("mirror_methods: [] must stay empty (no mirroring), got %#v", billing.MirrorMethods)
	}
	if n := len(p.RoutedServices()); n != 3 {
		t.Errorf("routed services = %d, want 3", n)
	}
	sh := p.Thresholds.Shift
	if !reflect.DeepEqual(sh.Steps, []int{1, 10, 50, 100}) || sh.Hold.D() != 5*time.Minute ||
		sh.SampleEvery.D() != 10*time.Second || sh.MaxErrorDelta != 0.5 || sh.MaxP99 != 2 || sh.MinRequests != 50 || sh.Preapprove {
		t.Errorf("shift defaults: %+v", sh)
	}
	if p.Thresholds.Rehearse.Window.D() != 5*time.Minute {
		t.Errorf("rehearse window default = %s", p.Thresholds.Rehearse.Window.D())
	}
	// The plan travels through workflow history as JSON; an empty mirror
	// list must survive the round trip as empty, not become the default.
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var back Plan
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if bb, _ := back.Service("billing"); bb.MirrorMethods == nil || len(bb.MirrorMethods) != 0 {
		t.Errorf("after JSON: %#v", bb.MirrorMethods)
	}
}

func TestPlanWithoutServicesNeedsNoFrontDoor(t *testing.T) {
	if _, err := Parse([]byte(minimal)); err != nil {
		t.Fatal(err)
	}
}

func TestServiceValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*Plan)
		want string
	}{
		{"unknown kind", func(p *Plan) { p.Services[0].Kind = "lambda" }, `services[0] (api): kind must be one of compose, systemd, deployment, external`},
		{"two blocks", func(p *Plan) { p.Services[0].Systemd = &Systemd{Unit: "x.service"} }, `services[0] (api): kind compose takes only the compose block`},
		{"missing block", func(p *Plan) { p.Services[0].Compose = nil }, `services[0] (api): kind compose needs a compose block`},
		{"bad name", func(p *Plan) { p.Services[0].Name = "API_1" }, `services[0]: name must match`},
		{"duplicate name", func(p *Plan) { p.Services[1].Name = "api" }, `services[1] (api): duplicate service name`},
		{"host missing", func(p *Plan) { p.Services[0].Source.Host = "" }, `services[0] (api): source.host: required for kind compose`},
		{"host unknown", func(p *Plan) { p.Services[0].Target.Host = "app9" }, `services[0] (api): target.host: "app9" is not in target.hosts`},
		{"host on deployment", func(p *Plan) { p.Services[2].Source.Host = "app1" }, `services[2] (worker): source.host: not used by kind deployment`},
		{"deployment without kube", func(p *Plan) { p.Target.Kube = nil }, `services[2] (worker): kind deployment needs target.kube`},
		{"external without route", func(p *Plan) { p.Services[3].Route = nil }, `services[3] (web): kind external needs a route`},
		{"duplicate route", func(p *Plan) { p.Services[1].Route.Prefix = "/" }, `services[1] (billing): route shop.example.com/ is already used by api`},
		{"bad route host", func(p *Plan) { p.Services[0].Route.Host = "Shop.Example.com" }, `services[0] (api): route.host: "Shop.Example.com" is not a lowercase DNS name`},
		{"bad prefix", func(p *Plan) { p.Services[0].Route.Prefix = "api" }, `services[0] (api): route.prefix: must start with /`},
		{"no health path", func(p *Plan) { p.Services[0].Route.HealthPath = "" }, `services[0] (api): route.health_path: required`},
		{"no endpoint", func(p *Plan) { p.Services[0].Target.Endpoint = "" }, `services[0] (api): target.endpoint: required when the service has a route`},
		{"bad endpoint", func(p *Plan) { p.Services[0].Target.Endpoint = "10.8.0.20" }, `services[0] (api): target.endpoint: "10.8.0.20" is not host:port`},
		{"same endpoints", func(p *Plan) { p.Services[0].Target.Endpoint = "10.0.1.20:8080" }, `services[0] (api): source and target endpoints are the same`},
		{"endpoint without route", func(p *Plan) { p.Services[2].Source.Endpoint = "10.0.0.1:80" }, `services[2] (worker): source.endpoint: only used with a route`},
		{"unsafe mirror", func(p *Plan) { p.Services[0].MirrorMethods = []string{"GET", "POST"} }, `services[0] (api): mirror_methods: "POST" is not a safe method (GET, HEAD, OPTIONS)`},
		{"compose project", func(p *Plan) { p.Services[0].Compose.Project = "Shop!" }, `services[0] (api): compose.project: "Shop!" is not a compose project name`},
		{"unit name", func(p *Plan) { p.Services[1].Systemd.Unit = "billing" }, `services[1] (billing): systemd.unit: "billing" must be a .service unit`},
		{"relative file", func(p *Plan) { p.Services[1].Systemd.Files = []string{"bin/billing"} }, `services[1] (billing): systemd.files: "bin/billing" must be a clean absolute path`},
		{"k8s name", func(p *Plan) { p.Services[2].Deployment.Namespace = "Shop" }, `services[2] (worker): deployment.namespace: "Shop" is not a Kubernetes name`},
		{"no front door", func(p *Plan) { p.FrontDoor = nil }, `frontdoor: required when a service has a route`},
		{"front door address", func(p *Plan) { p.FrontDoor.Addresses = []string{"frontdoor.example.com"} }, `frontdoor.addresses: "frontdoor.example.com" is not an IP address`},
		{"front door api", func(p *Plan) { p.FrontDoor.API = "127.0.0.1:7600" }, `frontdoor.api: must be an http:// or https:// URL with a host and port`},
		{"front door token", func(p *Plan) { p.FrontDoor.Token = "hunter2" }, `frontdoor.token:`},
		{"ssh key", func(p *Plan) { h := p.Source.Hosts["app1"]; h.Key = "xyz"; p.Source.Hosts["app1"] = h }, `source.hosts.app1.key:`},
		{"known hosts", func(p *Plan) { h := p.Source.Hosts["app1"]; h.KnownHosts = "known_hosts"; p.Source.Hosts["app1"] = h }, `source.hosts.app1.known_hosts: must be an absolute path`},
		{"ssh user", func(p *Plan) { h := p.Source.Hosts["app1"]; h.User = "root; rm"; p.Source.Hosts["app1"] = h }, `source.hosts.app1.user: "root; rm" is not a user name`},
		{"kubeconfig", func(p *Plan) { p.Source.Kube.Kubeconfig = "a.kubeconfig" }, `source.kube.kubeconfig: must be an absolute path`},
		{"kube context", func(p *Plan) { p.Source.Kube.Context = "" }, `source.kube.context: required`},
		{"steps end", func(p *Plan) { p.Thresholds.Shift.Steps = []int{1, 10, 50} }, `thresholds.shift.steps: must end at 100`},
		{"steps order", func(p *Plan) { p.Thresholds.Shift.Steps = []int{10, 5, 100} }, `thresholds.shift.steps: must increase, each between 1 and 100`},
		{"sample > hold", func(p *Plan) { p.Thresholds.Shift.SampleEvery = Duration(10 * time.Minute) }, `thresholds.shift.sample_every: must be positive and at most hold`},
		{"error delta", func(p *Plan) { p.Thresholds.Shift.MaxErrorDelta = 101 }, `thresholds.shift.max_error_delta: must be between 0 and 100`},
		{"p99 factor", func(p *Plan) { p.Thresholds.Shift.MaxP99 = 0.5 }, `thresholds.shift.max_p99: must be at least 1`},
		{"min requests", func(p *Plan) { p.Thresholds.Shift.MinRequests = -1 }, `thresholds.shift.min_requests: must be at least 1`},
		{"pgbouncer host", func(p *Plan) {
			p.Database.PgBouncer.Host = &Host{Address: "10.0.1.9", Port: 22, User: "deploy", Key: "bad", KnownHosts: "/k"}
		}, `database.pgbouncer.host.key:`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := servicesPlan(t)
			tc.edit(&p)
			err := p.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestEnvFor(t *testing.T) {
	p := servicesPlan(t)
	if p.EnvFor(false).Kube.Context != "a" || p.EnvFor(true).Kube.Context != "b" {
		t.Fatal("EnvFor picks the wrong side")
	}
}
