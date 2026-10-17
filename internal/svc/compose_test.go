package svc

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/SaiPisey2/cutover/internal/plan"
)

func composePlan() (plan.Plan, plan.Service) {
	s := plan.Service{Name: "api", Kind: plan.KindCompose, Compose: &plan.Compose{Project: "shop", Service: "api"},
		Source: plan.Placement{Host: "app1"}, Target: plan.Placement{Host: "app1"}}
	return plan.Plan{Name: "shop", Services: []plan.Service{s}}, s
}

const lsJSON = `[{"Name":"shop","Status":"running(2)","ConfigFiles":"/srv/shop/compose.yaml,/srv/shop/compose.prod.yaml"},{"Name":"other","Status":"running(1)","ConfigFiles":"/srv/other/compose.yaml"}]`

const lsCmd = "ROOT(docker compose ls --all --format json)"
const configCmd = "ROOT(docker compose -p 'shop' --project-directory '/srv/shop' -f '/srv/shop/compose.yaml' -f '/srv/shop/compose.prod.yaml' config --format json)"
const versionCmd = "ROOT(docker compose version --short)"
const srcPsCmd = "ROOT(docker compose -p 'shop' ps --format json)"
const dir = "/var/lib/cutover/shop/api"

// Real-shaped `docker compose ps --format json` of the source (one object
// per line, as compose 2.29 prints it): IPv4 and IPv6 publish the same port.
const srcPsJSON = `{"ID":"a1","Name":"shop-api-1","Service":"api","State":"running","Health":"healthy","Publishers":[{"URL":"0.0.0.0","TargetPort":8080,"PublishedPort":8080,"Protocol":"tcp"},{"URL":"::","TargetPort":8080,"PublishedPort":8080,"Protocol":"tcp"},{"URL":"","TargetPort":9090,"PublishedPort":0,"Protocol":"tcp"}]}
{"ID":"b2","Name":"shop-api-2","Service":"api","State":"running","Health":"","Publishers":[{"URL":"0.0.0.0","TargetPort":53,"PublishedPort":5353,"Protocol":"udp"}]}
{"ID":"c3","Name":"shop-cache-1","Service":"cache","State":"running","Health":"","Publishers":[{"URL":"0.0.0.0","TargetPort":6379,"PublishedPort":6379,"Protocol":"tcp"}]}
`

// A resolved config as docker compose 2.29 prints it (trimmed).
const configJSON = `{
  "name": "shop",
  "services": {
    "api": {
      "container_name": "shop-api",
      "depends_on": {"cache": {"condition": "service_started", "required": true}},
      "environment": {"DATABASE_URL": "postgres://app:S3CRET-PASSWORD@pgbouncer:6432/shop", "MODE": "prod"},
      "image": "registry.example.com/shop/api:1.4",
      "networks": {"default": null},
      "ports": [{"mode": "ingress", "target": 8080, "published": "8080", "protocol": "tcp"}],
      "labels": {"team": "shop"},
      "volumes": [{"type": "tmpfs", "target": "/tmp"}]
    },
    "cache": {"image": "redis:7", "volumes": [{"type": "volume", "source": "cache-data", "target": "/data"}]}
  },
  "networks": {"default": {"name": "shop_default"}}
}`

func cleanSource(t *testing.T) *fakeRunner {
	f := newFake(t)
	f.on(lsCmd, lsJSON, nil)
	f.on(configCmd, configJSON, nil)
	f.on(srcPsCmd, srcPsJSON, nil)
	return f
}

func cleanTarget(t *testing.T) *fakeRunner {
	f := newFake(t)
	f.on(versionCmd, "2.29.7\n", nil)
	f.on(lsCmd, `[]`, nil)
	return f
}

func TestComposeInspectClean(t *testing.T) {
	p, s := composePlan()
	d, err := For(p, s)
	if err != nil {
		t.Fatal(err)
	}
	facts, err := d.Inspect(context.Background(), sideOf(cleanSource(t)), sideOf(cleanTarget(t)))
	if err != nil {
		t.Fatal(err)
	}
	if facts.Image != "registry.example.com/shop/api:1.4" || facts.Kind != plan.KindCompose || facts.Service != "api" {
		t.Fatalf("facts %+v", facts)
	}
	got := codes(facts.Findings)
	if len(facts.Findings) != 1 || !got["compose-depends"] || facts.Findings[0].Blocker {
		t.Fatalf("want only the depends_on warning, got %+v", facts.Findings)
	}
}

func TestComposeFactsCarryNoEnvironment(t *testing.T) {
	p, s := composePlan()
	d, _ := For(p, s)
	facts, err := d.Inspect(context.Background(), sideOf(cleanSource(t)), sideOf(cleanTarget(t)))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(facts)
	if strings.Contains(string(b), "S3CRET") || strings.Contains(string(b), "DATABASE_URL") {
		t.Fatalf("facts leak the environment: %s", b)
	}
}

func TestComposeInspectBlockers(t *testing.T) {
	p, s := composePlan()
	d, _ := For(p, s)
	for _, tc := range []struct {
		name   string
		config string
		code   string
	}{
		{"build", `{"services":{"api":{"build":{"context":"/srv/shop"},"image":"x"}}}`, "compose-build"},
		{"no image", `{"services":{"api":{}}}`, "compose-image"},
		{"bind", `{"services":{"api":{"image":"x","volumes":[{"type":"bind","source":"/srv/data","target":"/data"}]}}}`, "compose-state"},
		{"volume", `{"services":{"api":{"image":"x","volumes":[{"type":"volume","source":"data","target":"/data"}]}}}`, "compose-state"},
		{"network", `{"services":{"api":{"image":"x","network_mode":"service:vpn"}}}`, "compose-network"},
		{"service missing", `{"services":{"web":{"image":"x"}}}`, "compose-missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := newFake(t)
			src.on(lsCmd, lsJSON, nil)
			src.on(configCmd, tc.config, nil)
			src.on(srcPsCmd, `[]`, nil)
			facts, err := d.Inspect(context.Background(), sideOf(src), sideOf(cleanTarget(t)))
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, f := range facts.Findings {
				if f.Code == tc.code && f.Blocker {
					found = true
				}
			}
			if !found {
				t.Fatalf("want blocker %s, got %+v", tc.code, facts.Findings)
			}
		})
	}
}

func TestComposeInspectProjectAndDocker(t *testing.T) {
	p, s := composePlan()
	d, _ := For(p, s)
	src := newFake(t)
	src.on(lsCmd, `[]`, nil)
	tgt := newFake(t)
	tgt.on(versionCmd, "", errors.New("exit 127: docker: not found"))
	facts, err := d.Inspect(context.Background(), sideOf(src), sideOf(tgt))
	if err != nil {
		t.Fatal(err)
	}
	got := codes(facts.Findings)
	if !got["compose-missing"] || !got["compose-target"] {
		t.Fatalf("%+v", facts.Findings)
	}
	tgt = newFake(t)
	tgt.on(versionCmd, "2.12.2\n", nil)
	tgt.on(lsCmd, `[]`, nil)
	facts, _ = d.Inspect(context.Background(), sideOf(cleanSource(t)), sideOf(tgt))
	if !codes(facts.Findings)["compose-target"] {
		t.Fatalf("compose older than 2.20 must block: %+v", facts.Findings)
	}
}

const upCmd = "ROOT(docker compose -p 'cutover-shop-api' -f '/var/lib/cutover/shop/api/compose.json' up -d --wait --wait-timeout 300 --quiet-pull)"

func TestComposePrepare(t *testing.T) {
	p, s := composePlan()
	d, _ := For(p, s)
	tgt := newFake(t)
	tgt.on(lsCmd, `[]`, nil)
	tgt.on(upCmd, "", nil)
	if err := d.Prepare(context.Background(), sideOf(cleanSource(t)), sideOf(tgt)); err != nil {
		t.Fatal(err)
	}
	path := "/var/lib/cutover/shop/api/compose.json"
	if tgt.modes[path] != 0o600 {
		t.Fatalf("compose file mode %o (it holds the environment)", tgt.modes[path])
	}
	var doc struct {
		Name     string                    `json:"name"`
		Services map[string]map[string]any `json:"services"`
	}
	if err := json.Unmarshal(tgt.files[path], &doc); err != nil {
		t.Fatal(err)
	}
	api := doc.Services["api"]
	if doc.Name != "cutover-shop-api" || len(doc.Services) != 1 {
		t.Fatalf("doc %+v", doc)
	}
	for _, k := range []string{"depends_on", "container_name", "networks"} {
		if _, ok := api[k]; ok {
			t.Errorf("%s must be dropped", k)
		}
	}
	labels := api["labels"].(map[string]any)
	if labels[OwnerLabel] != "shop" || labels["team"] != "shop" {
		t.Errorf("labels %v", labels)
	}
	if api["environment"] == nil || api["image"] != "registry.example.com/shop/api:1.4" {
		t.Error("the copy keeps the image and environment")
	}
	if !tgt.ran(upCmd) {
		t.Fatal("compose up did not run")
	}
	if string(tgt.files[dir+"/.cutover-owner"]) != "shop\n" {
		t.Fatalf("owner marker %q", tgt.files[dir+"/.cutover-owner"])
	}
	// The marker lands before the compose file and before up.
	if i, j := slices.Index(tgt.calls, "WRITE "+dir+"/.cutover-owner"), slices.Index(tgt.calls, "WRITE "+path); i < 0 || i > j {
		t.Fatalf("marker must be written first: %v", tgt.calls)
	}
}

func TestComposePrepareRetryIsIdempotent(t *testing.T) {
	p, s := composePlan()
	d, _ := For(p, s)
	tgt := newFake(t)
	tgt.files[dir+"/.cutover-owner"] = []byte("shop\n")
	tgt.files[dir+"/compose.json"] = []byte("{}")
	tgt.on(upCmd, "", nil)
	if err := d.Prepare(context.Background(), sideOf(cleanSource(t)), sideOf(tgt)); err != nil {
		t.Fatalf("retry over our own copy: %v", err)
	}
}

func TestComposePrepareRefusesForeignTarget(t *testing.T) {
	p, s := composePlan()
	d, _ := For(p, s)
	tgt := newFake(t)
	tgt.files[dir+"/compose.json"] = []byte("{}") // no marker: not ours
	if err := d.Prepare(context.Background(), sideOf(cleanSource(t)), sideOf(tgt)); !errors.Is(err, ErrBlocked) {
		t.Fatalf("want ErrBlocked, got %v", err)
	}
	if tgt.ran("WRITE") || tgt.ran(upCmd) {
		t.Fatalf("touched a foreign target: %v", tgt.calls)
	}
	tgt = newFake(t)
	tgt.on(lsCmd, `[{"Name":"cutover-shop-api","Status":"running(1)","ConfigFiles":"/home/x/compose.yaml"}]`, nil)
	if err := d.Prepare(context.Background(), sideOf(cleanSource(t)), sideOf(tgt)); !errors.Is(err, ErrBlocked) {
		t.Fatalf("foreign project: want ErrBlocked, got %v", err)
	}
}

func TestComposeInspectTargetExists(t *testing.T) {
	p, s := composePlan()
	d, _ := For(p, s)
	has := func(t *testing.T, tgt *fakeRunner) bool {
		facts, err := d.Inspect(context.Background(), sideOf(cleanSource(t)), sideOf(tgt))
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range facts.Findings {
			if f.Code == "compose-target-exists" && f.Blocker {
				return true
			}
		}
		return false
	}
	foreignProject := `[{"Name":"cutover-shop-api","Status":"running(1)","ConfigFiles":"/home/x/compose.yaml"}]`
	t.Run("foreign project", func(t *testing.T) {
		tgt := newFake(t)
		tgt.on(versionCmd, "2.29.7\n", nil)
		tgt.on(lsCmd, foreignProject, nil)
		if !has(t, tgt) {
			t.Fatal("a project of that name we did not make must block")
		}
	})
	t.Run("foreign directory", func(t *testing.T) {
		tgt := cleanTarget(t)
		tgt.files[dir+"/compose.json"] = []byte("{}")
		if !has(t, tgt) {
			t.Fatal("a directory without our marker must block")
		}
	})
	t.Run("directory marked for another plan", func(t *testing.T) {
		tgt := cleanTarget(t)
		tgt.files[dir+"/.cutover-owner"] = []byte("other\n")
		if !has(t, tgt) {
			t.Fatal("another plan's marker is not ours")
		}
	})
	t.Run("our own prepared copy", func(t *testing.T) {
		tgt := newFake(t)
		tgt.on(versionCmd, "2.29.7\n", nil)
		tgt.on(lsCmd, foreignProject, nil)
		tgt.files[dir+"/.cutover-owner"] = []byte("shop\n")
		tgt.files[dir+"/compose.json"] = []byte("{}")
		if has(t, tgt) {
			t.Fatal("an idempotent retry over our own copy must not block")
		}
	})
	t.Run("nothing there", func(t *testing.T) {
		if has(t, cleanTarget(t)) {
			t.Fatal("clean target blocked")
		}
	})
}

func TestComposeFactsPorts(t *testing.T) {
	p, s := composePlan()
	d, _ := For(p, s)
	facts, err := d.Inspect(context.Background(), sideOf(cleanSource(t)), sideOf(cleanTarget(t)))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"5353->53/udp", "8080->8080/tcp"}
	if !slices.Equal(facts.Ports, want) {
		t.Fatalf("ports %v, want %v (own service only, sorted, deduplicated, unpublished skipped)", facts.Ports, want)
	}
	// Array shape, as older compose prints it.
	src := cleanSource(t)
	src.on(srcPsCmd, `[{"Service":"api","State":"running","Publishers":[{"URL":"0.0.0.0","TargetPort":80,"PublishedPort":8080,"Protocol":"tcp"}]}]`, nil)
	facts, _ = d.Inspect(context.Background(), sideOf(src), sideOf(cleanTarget(t)))
	if !slices.Equal(facts.Ports, []string{"8080->80/tcp"}) {
		t.Fatalf("ports %v", facts.Ports)
	}
}

func TestComposePrepareRefusesBlockers(t *testing.T) {
	p, s := composePlan()
	d, _ := For(p, s)
	src := newFake(t)
	src.on(lsCmd, lsJSON, nil)
	src.on(configCmd, `{"services":{"api":{"image":"x","volumes":[{"type":"bind","source":"/srv/data","target":"/data"}]}}}`, nil)
	err := d.Prepare(context.Background(), sideOf(src), sideOf(newFake(t)))
	if !errors.Is(err, ErrBlocked) {
		t.Fatalf("want ErrBlocked, got %v", err)
	}
}

const psCmd = "ROOT(docker compose -p 'cutover-shop-api' ps --all --format json)"

func TestComposeHealthy(t *testing.T) {
	p, s := composePlan()
	d, _ := For(p, s)
	for _, tc := range []struct {
		out  string
		want bool
	}{
		{`{"Service":"api","State":"running","Health":"healthy"}` + "\n", true},
		{`[{"Service":"api","State":"running","Health":""}]`, true},
		{`{"Service":"api","State":"running","Health":"starting"}` + "\n", false},
		{`{"Service":"api","State":"exited","Health":""}` + "\n", false},
		{``, false},
	} {
		tgt := newFake(t)
		tgt.on(psCmd, tc.out, nil)
		ok, detail, err := d.Healthy(context.Background(), sideOf(tgt))
		if err != nil || ok != tc.want || detail == "" {
			t.Errorf("%q: ok=%v detail=%q err=%v", tc.out, ok, detail, err)
		}
	}
}

func TestComposeUndo(t *testing.T) {
	p, s := composePlan()
	d, _ := For(p, s)
	tgt := newFake(t)
	tgt.files[dir+"/.cutover-owner"] = []byte("shop\n")
	tgt.files[dir+"/compose.json"] = []byte("{}")
	tgt.on("ROOT(docker compose -p 'cutover-shop-api' -f '/var/lib/cutover/shop/api/compose.json' down --remove-orphans)", "", nil)
	tgt.on("ROOT(rm -rf '/var/lib/cutover/shop/api')", "", nil)
	if err := d.Undo(context.Background(), sideOf(tgt)); err != nil {
		t.Fatal(err)
	}
	if !tgt.ran("ROOT(rm -rf") {
		t.Fatal("our directory was not removed")
	}
	// Nothing there: Undo still succeeds and runs nothing.
	tgt = newFake(t)
	if err := d.Undo(context.Background(), sideOf(tgt)); err != nil {
		t.Fatal(err)
	}
	if tgt.ran("ROOT(") {
		t.Fatalf("undo ran commands on an empty target: %v", tgt.calls)
	}
	// A directory without our marker is somebody else's: left alone.
	tgt = newFake(t)
	tgt.files[dir+"/compose.json"] = []byte("{}")
	if err := d.Undo(context.Background(), sideOf(tgt)); err != nil {
		t.Fatal(err)
	}
	if tgt.ran("ROOT(") {
		t.Fatalf("undo touched a foreign directory: %v", tgt.calls)
	}
}

func TestExternalDoesNothing(t *testing.T) {
	s := plan.Service{Name: "web", Kind: plan.KindExternal, Route: &plan.Route{Host: "www.example.com", Prefix: "/", HealthPath: "/"}}
	d, err := For(plan.Plan{Name: "shop"}, s)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	none := Side{}
	if facts, err := d.Inspect(ctx, none, none); err != nil || len(facts.Findings) != 0 {
		t.Fatalf("%+v %v", facts, err)
	}
	if err := d.Prepare(ctx, none, none); err != nil {
		t.Fatal(err)
	}
	if ok, _, err := d.Healthy(ctx, none); !ok || err != nil {
		t.Fatal("an external copy is the operator's; the front door checks its health")
	}
	if err := d.Undo(ctx, none); err != nil {
		t.Fatal(err)
	}
}
