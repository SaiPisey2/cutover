package svc

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/SaiPisey2/cutover/internal/plan"
	"github.com/SaiPisey2/cutover/internal/sshx"
)

func systemdPlan() (plan.Plan, plan.Service) {
	s := plan.Service{Name: "billing", Kind: plan.KindSystemd,
		Systemd: &plan.Systemd{Unit: "billing.service", Files: []string{"/usr/local/bin/billing", "/etc/billing/env"}},
		Source:  plan.Placement{Host: "app1"}, Target: plan.Placement{Host: "app1"}}
	return plan.Plan{Name: "shop", Services: []plan.Service{s}}, s
}

const showCmd = "ROOT(systemctl show -p LoadState -p FragmentPath -p DropInPaths -p ExecStart -p EnvironmentFiles --no-pager 'billing.service')"

// As systemd 252 prints it: properties in systemd's own order, one
// EnvironmentFiles line per file.
const showOut = `ExecStart={ path=/usr/local/bin/billing ; argv[]=/usr/local/bin/billing --port 9090 ; ignore_errors=no ; start_time=[n/a] ; stop_time=[n/a] ; pid=0 ; code=(null) ; status=0/0 }
EnvironmentFiles=/etc/billing/env (ignore_errors=no)
EnvironmentFiles=/etc/billing/optional (ignore_errors=yes)
FragmentPath=/etc/systemd/system/billing.service
DropInPaths=/etc/systemd/system/billing.service.d/override.conf
LoadState=loaded
`

const tgtShowCmd = "ROOT(systemctl show -p LoadState --no-pager 'billing.service')"
const marker = "/etc/systemd/system/billing.service.d/zz-cutover.conf"
const manifest = "/var/lib/cutover/shop/systemd/billing.service.manifest"

func systemdSource(t *testing.T) *fakeRunner {
	f := newFake(t)
	f.on(showCmd, showOut, nil)
	f.files["/usr/local/bin/billing"] = []byte("\x7fELF binary")
	f.files["/etc/billing/env"] = []byte("STRIPE_KEY=sk_live_SECRET\n")
	f.files["/etc/systemd/system/billing.service"] = []byte("[Service]\nExecStart=/usr/local/bin/billing --port 9090\nEnvironmentFile=/etc/billing/env\n")
	f.files["/etc/systemd/system/billing.service.d/override.conf"] = []byte("[Service]\nRestart=always\n")
	f.on("ROOT(stat -c %a '/usr/local/bin/billing')", "755\n", nil)
	f.on("ROOT(stat -c %a '/etc/billing/env')", "600\n", nil)
	return f
}

func systemdTarget(t *testing.T) *fakeRunner {
	f := newFake(t)
	f.on(tgtShowCmd, "LoadState=not-found\n", nil)
	return f
}

func TestSystemdInspectClean(t *testing.T) {
	p, s := systemdPlan()
	d, _ := For(p, s)
	facts, err := d.Inspect(context.Background(), sideOf(systemdSource(t)), sideOf(systemdTarget(t)))
	if err != nil {
		t.Fatal(err)
	}
	if len(facts.Findings) != 1 || facts.Findings[0].Code != "systemd-state" || facts.Findings[0].Blocker {
		t.Fatalf("want only the state warning, got %+v", facts.Findings)
	}
}

func TestSystemdInspectBlockers(t *testing.T) {
	p, s := systemdPlan()
	ctx := context.Background()

	// A needed file neither listed nor on the target.
	s2 := s
	s2.Systemd = &plan.Systemd{Unit: "billing.service", Files: []string{"/usr/local/bin/billing"}}
	d, _ := For(p, s2)
	facts, _ := d.Inspect(ctx, sideOf(systemdSource(t)), sideOf(systemdTarget(t)))
	if !codes(facts.Findings)["systemd-file-missing"] {
		t.Fatalf("env file not listed: %+v", facts.Findings)
	}

	// A listed file that differs on the target.
	d, _ = For(p, s)
	src, tgt := systemdSource(t), systemdTarget(t)
	tgt.files["/usr/local/bin/billing"] = []byte("older build")
	src.on("ROOT(sha256sum '/usr/local/bin/billing')", "aaaa  /usr/local/bin/billing\n", nil)
	tgt.on("ROOT(sha256sum '/usr/local/bin/billing')", "bbbb  /usr/local/bin/billing\n", nil)
	facts, _ = d.Inspect(ctx, sideOf(src), sideOf(tgt))
	if !codes(facts.Findings)["systemd-file-differs"] {
		t.Fatalf("%+v", facts.Findings)
	}

	// Unit not loaded on the source.
	src = newFake(t)
	src.on(showCmd, "LoadState=not-found\nFragmentPath=\n", nil)
	facts, _ = d.Inspect(ctx, sideOf(src), sideOf(systemdTarget(t)))
	if !codes(facts.Findings)["systemd-missing"] {
		t.Fatalf("%+v", facts.Findings)
	}

	// systemctl missing on the target.
	tgt = newFake(t)
	tgt.on(tgtShowCmd, "", errors.New("exit 127: systemctl: not found"))
	facts, _ = d.Inspect(ctx, sideOf(systemdSource(t)), sideOf(tgt))
	if !codes(facts.Findings)["systemd-target"] {
		t.Fatalf("%+v", facts.Findings)
	}
}

func TestSystemdRefusesForeignUnit(t *testing.T) {
	p, s := systemdPlan()
	d, _ := For(p, s)
	tgt := newFake(t)
	tgt.on(tgtShowCmd, "LoadState=loaded\n", nil) // loaded, and no marker
	facts, _ := d.Inspect(context.Background(), sideOf(systemdSource(t)), sideOf(tgt))
	if !codes(facts.Findings)["systemd-target-unit"] {
		t.Fatalf("%+v", facts.Findings)
	}
	if err := d.Prepare(context.Background(), sideOf(systemdSource(t)), sideOf(tgt)); !errors.Is(err, ErrBlocked) {
		t.Fatalf("Prepare must refuse a unit cutover did not create: %v", err)
	}
	if len(tgt.files) != 0 {
		t.Fatalf("Prepare wrote to the target anyway: %v", tgt.files)
	}
	// Undo with no manifest touches nothing, foreign unit or not.
	if err := d.Undo(context.Background(), sideOf(tgt)); err != nil {
		t.Fatal(err)
	}
}

const enableCmd = "ROOT(systemctl daemon-reload && systemctl enable --now 'billing.service')"

func TestSystemdPrepare(t *testing.T) {
	p, s := systemdPlan()
	d, _ := For(p, s)
	src, tgt := systemdSource(t), systemdTarget(t)
	tgt.on(enableCmd, "", nil)
	if err := d.Prepare(context.Background(), sideOf(src), sideOf(tgt)); err != nil {
		t.Fatal(err)
	}
	if string(tgt.files["/etc/billing/env"]) != "STRIPE_KEY=sk_live_SECRET\n" || tgt.modes["/etc/billing/env"] != 0o600 {
		t.Fatalf("env file copied wrong: mode %o", tgt.modes["/etc/billing/env"])
	}
	if tgt.modes["/usr/local/bin/billing"] != 0o755 {
		t.Fatal("binary mode not kept")
	}
	if !strings.Contains(string(tgt.files[marker]), "X-Cutover-Owner=shop") {
		t.Fatal("marker drop-in missing")
	}
	if tgt.files["/etc/systemd/system/billing.service.d/override.conf"] == nil || tgt.files["/etc/systemd/system/billing.service"] == nil {
		t.Fatal("unit and drop-in not copied")
	}
	m := string(tgt.files[manifest])
	for _, want := range []string{"/usr/local/bin/billing", "/etc/billing/env", "/etc/systemd/system/billing.service", marker} {
		if !strings.Contains(m, want+"\n") {
			t.Errorf("manifest lacks %s:\n%s", want, m)
		}
	}
	// The manifest is written before anything it lists.
	first := -1
	for i, c := range tgt.calls {
		if strings.HasPrefix(c, "WRITE ") {
			first = i
			break
		}
	}
	if first < 0 {
		t.Fatal("Prepare wrote nothing")
	}
	if tgt.calls[first] != "WRITE "+manifest {
		t.Fatalf("first write was %s, want the manifest", tgt.calls[first])
	}
	if !tgt.ran(enableCmd) {
		t.Fatal("unit not enabled")
	}
}

func TestSystemdPrepareKeepsExistingIdenticalFile(t *testing.T) {
	p, s := systemdPlan()
	d, _ := For(p, s)
	src, tgt := systemdSource(t), systemdTarget(t)
	tgt.files["/usr/local/bin/billing"] = []byte("\x7fELF binary")
	src.on("ROOT(sha256sum '/usr/local/bin/billing')", "aaaa  /usr/local/bin/billing\n", nil)
	tgt.on("ROOT(sha256sum '/usr/local/bin/billing')", "aaaa  /usr/local/bin/billing\n", nil)
	tgt.on(enableCmd, "", nil)
	if err := d.Prepare(context.Background(), sideOf(src), sideOf(tgt)); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(tgt.files[manifest]), "/usr/local/bin/billing\n") {
		t.Fatal("a file that was already on the target must not be in the manifest (undo would delete it)")
	}
}

func TestSystemdHealthy(t *testing.T) {
	p, s := systemdPlan()
	d, _ := For(p, s)
	for out, want := range map[string]bool{"active\n": true, "activating\n": false, "failed\n": false} {
		tgt := newFake(t)
		tgt.on("ROOT(systemctl is-active 'billing.service' || true)", out, nil)
		if ok, _, err := d.Healthy(context.Background(), sideOf(tgt)); err != nil || ok != want {
			t.Errorf("%q: %v %v", out, ok, err)
		}
	}
}

func TestSystemdUndo(t *testing.T) {
	p, s := systemdPlan()
	d, _ := For(p, s)
	tgt := newFake(t)
	tgt.files[manifest] = []byte("/etc/billing/env\n/etc/systemd/system/billing.service\n" + marker + "\n")
	tgt.files[marker] = []byte("[Unit]\nX-Cutover-Owner=shop\n")
	undo := "ROOT(systemctl disable --now 'billing.service' || true; rm -f '/etc/billing/env' '/etc/systemd/system/billing.service' '" + marker +
		"'; rmdir '/etc/systemd/system/billing.service.d' 2>/dev/null || true; systemctl daemon-reload; rm -f '" + manifest + "')"
	tgt.on(undo, "", nil)
	if err := d.Undo(context.Background(), sideOf(tgt)); err != nil {
		t.Fatal(err)
	}
	if !tgt.ran(undo) {
		t.Fatalf("undo ran %v", tgt.calls)
	}
}

// P6: any LoadState but not-found is an existing unit.
func TestSystemdRefusesMaskedAndBrokenUnits(t *testing.T) {
	p, s := systemdPlan()
	d, _ := For(p, s)
	ctx := context.Background()
	for _, state := range []string{"masked", "bad-setting", "error"} {
		tgt := newFake(t)
		tgt.on(tgtShowCmd, "LoadState="+state+"\n", nil)
		facts, err := d.Inspect(ctx, sideOf(systemdSource(t)), sideOf(tgt))
		if err != nil || !codes(facts.Findings)["systemd-target-unit"] {
			t.Fatalf("%s: %v %+v", state, err, facts.Findings)
		}
		if err := d.Prepare(ctx, sideOf(systemdSource(t)), sideOf(tgt)); !errors.Is(err, ErrBlocked) {
			t.Fatalf("%s: Prepare must refuse: %v", state, err)
		}
		if len(tgt.files) != 0 {
			t.Fatalf("%s: Prepare wrote %v", state, tgt.files)
		}
		if err := d.Undo(ctx, sideOf(tgt)); err != nil || tgt.ran("WRITE") {
			t.Fatalf("%s: Undo must leave it: %v", state, err)
		}
	}
}

// A retry: the unit is loaded with this plan's marker, so it is not foreign.
func TestSystemdRetryPassesWithMarker(t *testing.T) {
	p, s := systemdPlan()
	d, _ := For(p, s)
	tgt := newFake(t)
	tgt.on(tgtShowCmd, "LoadState=loaded\n", nil)
	tgt.files[marker] = []byte("[Unit]\nX-Cutover-Owner=shop\n")
	facts, err := d.Inspect(context.Background(), sideOf(systemdSource(t)), sideOf(tgt))
	if err != nil || codes(facts.Findings)["systemd-target-unit"] {
		t.Fatalf("%v %+v", err, facts.Findings)
	}
}

type readFails struct {
	*fakeRunner
	path string
	err  error
}

func (r readFails) ReadFile(ctx context.Context, p string) ([]byte, error) {
	if p == r.path {
		return nil, r.err
	}
	return r.fakeRunner.ReadFile(ctx, p)
}

// P7: an unreadable manifest is an error, not "no old manifest".
func TestSystemdManifestReadErrorStopsPrepare(t *testing.T) {
	p, s := systemdPlan()
	d, _ := For(p, s)
	tgt := systemdTarget(t)
	tgt.files[manifest] = []byte("/etc/billing/env\n")
	tgt.on(enableCmd, "", nil)
	r := readFails{tgt, manifest, errors.New("permission denied")}
	err := d.Prepare(context.Background(), sideOf(systemdSource(t)), sideOf(r))
	if err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("want the read error, got %v", err)
	}
	if tgt.ran("WRITE") || tgt.ran("ROOT(systemctl daemon-reload") {
		t.Fatalf("Prepare went on after a manifest read failure: %v", tgt.calls)
	}
}

func TestSystemdPrepareMergesOldManifest(t *testing.T) {
	p, s := systemdPlan()
	d, _ := For(p, s)
	src, tgt := systemdSource(t), systemdTarget(t)
	tgt.files[manifest] = []byte("/opt/earlier\n")
	tgt.on(enableCmd, "", nil)
	if err := d.Prepare(context.Background(), sideOf(src), sideOf(tgt)); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(tgt.files[manifest]), "/opt/earlier\n") {
		t.Fatalf("old manifest entry lost:\n%s", tgt.files[manifest])
	}
}

func sysLeaky() error {
	return &sshx.ExitError{Command: "ssh h: systemctl", Code: 3, Stderr: "Environment=KEY=" + leak}
}

type writeFails struct {
	*fakeRunner
}

func (writeFails) WriteFile(context.Context, string, os.FileMode, []byte) error { return sysLeaky() }

func TestSystemdErrorsCarryNoStderr(t *testing.T) {
	p, s := systemdPlan()
	d, _ := For(p, s)
	ctx := context.Background()
	check := func(name string, err error) {
		t.Helper()
		if err == nil || strings.Contains(err.Error(), leak) {
			t.Errorf("%s: want an error without the stderr, got %v", name, err)
		}
	}
	src := newFake(t)
	src.on(showCmd, "", sysLeaky())
	_, err := d.Inspect(ctx, sideOf(src), sideOf(systemdTarget(t)))
	check("inspect show", err)
	if err != nil && !strings.Contains(err.Error(), "exit 3") {
		t.Errorf("the exit code should survive: %v", err)
	}

	tgt := newFake(t)
	tgt.on(tgtShowCmd, "", sysLeaky())
	facts, err := d.Inspect(ctx, sideOf(systemdSource(t)), sideOf(tgt))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range facts.Findings {
		if strings.Contains(f.Message, leak) {
			t.Errorf("finding carries stderr: %+v", f)
		}
	}

	tgt = systemdTarget(t)
	tgt.on(enableCmd, "", sysLeaky())
	check("prepare enable", d.Prepare(ctx, sideOf(systemdSource(t)), sideOf(tgt)))

	check("prepare write", d.Prepare(ctx, sideOf(systemdSource(t)), sideOf(writeFails{systemdTarget(t)})))

	tgt = newFake(t)
	tgt.on("ROOT(systemctl is-active 'billing.service' || true)", "", sysLeaky())
	_, _, err = d.Healthy(ctx, sideOf(tgt))
	check("healthy", err)

	tgt = newFake(t)
	tgt.files[manifest] = []byte("/etc/billing/env\n")
	undo := "ROOT(rm -f '/etc/billing/env'; rmdir '/etc/systemd/system/billing.service.d' 2>/dev/null || true; systemctl daemon-reload; rm -f '" + manifest + "')"
	tgt.on(undo, "", sysLeaky())
	check("undo", d.Undo(ctx, sideOf(tgt)))
}

func TestSystemdFactsCarryNoSecrets(t *testing.T) {
	p, s := systemdPlan()
	d, _ := For(p, s)
	src := newFake(t)
	src.on(showCmd, strings.Replace(showOut, "--port 9090", "--token sk_live_ARGV", 1), nil)
	for k, v := range systemdSource(t).files {
		src.files[k] = v
	}
	facts, err := d.Inspect(context.Background(), sideOf(src), sideOf(systemdTarget(t)))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(facts)
	if strings.Contains(string(b), "sk_live") {
		t.Fatalf("facts leak: %s", b)
	}
}

func TestSystemdRefusesPreexistingTargetFiles(t *testing.T) {
	p, s := systemdPlan()
	d, _ := For(p, s)
	ctx := context.Background()
	for _, existing := range []string{
		"/etc/systemd/system/billing.service.d/override.conf",
		"/etc/systemd/system/billing.service",
		marker, // another plan's marker
	} {
		tgt := systemdTarget(t)
		tgt.files[existing] = []byte("not ours")
		facts, err := d.Inspect(ctx, sideOf(systemdSource(t)), sideOf(tgt))
		if err != nil {
			t.Fatal(err)
		}
		var found bool
		for _, f := range facts.Findings {
			if f.Code == "systemd-target-file" && f.Subject == existing && f.Blocker {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s: %+v", existing, facts.Findings)
		}
		if err := d.Prepare(ctx, sideOf(systemdSource(t)), sideOf(tgt)); !errors.Is(err, ErrBlocked) {
			t.Fatalf("%s: %v", existing, err)
		}
		if tgt.ran("WRITE") || string(tgt.files[existing]) != "not ours" {
			t.Fatalf("%s: touched: %v", existing, tgt.calls)
		}
	}
}

// Retry after the source binary changed: cutover's own copy is refreshed.
func TestSystemdRetryRecopiesOwnFile(t *testing.T) {
	p, s := systemdPlan()
	d, _ := For(p, s)
	src, tgt := systemdSource(t), systemdTarget(t)
	tgt.on(tgtShowCmd, "LoadState=loaded\n", nil)
	tgt.files[marker] = []byte("[Unit]\nX-Cutover-Owner=shop\n")
	tgt.files[manifest] = []byte("/usr/local/bin/billing\n/etc/systemd/system/billing.service\n" + marker + "\n/etc/systemd/system/billing.service.d/override.conf\n")
	tgt.files["/usr/local/bin/billing"] = []byte("old build")
	tgt.files["/etc/systemd/system/billing.service"] = []byte("old unit")
	tgt.files["/etc/systemd/system/billing.service.d/override.conf"] = []byte("old")
	tgt.on(enableCmd, "", nil)
	if err := d.Prepare(context.Background(), sideOf(src), sideOf(tgt)); err != nil {
		t.Fatal(err)
	}
	if string(tgt.files["/usr/local/bin/billing"]) != "\x7fELF binary" {
		t.Fatal("binary not re-copied")
	}
	if !strings.Contains(string(tgt.files[manifest]), "/usr/local/bin/billing\n") {
		t.Fatal("manifest lost the binary")
	}
}

func TestSystemdExecStartTakesOnlyOwnPath(t *testing.T) {
	p, s := systemdPlan()
	d, _ := For(p, s)
	src := systemdSource(t)
	evil := "ExecStart={ path=/usr/local/bin/billing ; argv[]=/bin/sh -c x; path=/opt/tok_SECRET ; ignore_errors=no }\n" +
		"ExecStart={ path=/opt/../tok_SECRET2 ; argv[]=x }\n" +
		"EnvironmentFiles=tok_SECRET3 (ignore_errors=no)\n" +
		"FragmentPath=/etc/systemd/system/billing.service\nLoadState=loaded\n"
	src.on(showCmd, evil, nil)
	facts, err := d.Inspect(context.Background(), sideOf(src), sideOf(systemdTarget(t)))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(facts)
	if strings.Contains(string(b), "SECRET") {
		t.Fatalf("facts leak: %s", b)
	}
	if codes(facts.Findings)["systemd-file-missing"] {
		t.Fatalf("a dropped value became a need: %+v", facts.Findings)
	}
}

func TestSystemdDuplicateDropIns(t *testing.T) {
	p, s := systemdPlan()
	d, _ := For(p, s)
	src := systemdSource(t)
	src.on(showCmd, strings.Replace(showOut, "override.conf\n", "override.conf /run/systemd/system/billing.service.d/override.conf\n", 1), nil)
	facts, _ := d.Inspect(context.Background(), sideOf(src), sideOf(systemdTarget(t)))
	if !codes(facts.Findings)["systemd-dropin-duplicate"] {
		t.Fatalf("%+v", facts.Findings)
	}
}

func TestSystemdSkipsSourceMarkerDropIn(t *testing.T) {
	p, s := systemdPlan()
	d, _ := For(p, s)
	src, tgt := systemdSource(t), systemdTarget(t)
	src.on(showCmd, strings.Replace(showOut, "override.conf\n", "override.conf /etc/systemd/system/billing.service.d/zz-cutover.conf\n", 1), nil)
	src.files["/etc/systemd/system/billing.service.d/zz-cutover.conf"] = []byte("[Unit]\nX-Cutover-Owner=other\n")
	tgt.on(enableCmd, "", nil)
	if err := d.Prepare(context.Background(), sideOf(src), sideOf(tgt)); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(tgt.files[marker]), "X-Cutover-Owner=shop") {
		t.Fatalf("marker replaced: %s", tgt.files[marker])
	}
}

func TestSystemdUndoIgnoresRelativeLinesAndSkipsDisableWithoutMarker(t *testing.T) {
	p, s := systemdPlan()
	d, _ := For(p, s)
	tgt := newFake(t)
	tgt.files[manifest] = []byte("../../etc/passwd\nrelative\n/etc/billing/env\n")
	undo := "ROOT(rm -f '/etc/billing/env'; rmdir '/etc/systemd/system/billing.service.d' 2>/dev/null || true; systemctl daemon-reload; rm -f '" + manifest + "')"
	tgt.on(undo, "", nil)
	if err := d.Undo(context.Background(), sideOf(tgt)); err != nil {
		t.Fatal(err)
	}
	if !tgt.ran(undo) {
		t.Fatalf("undo ran %v", tgt.calls)
	}
}

type existsFails struct {
	*fakeRunner
	path string
}

func (r existsFails) Exists(ctx context.Context, p string) (bool, error) {
	if p == r.path {
		return false, errors.New("ssh dropped")
	}
	return r.fakeRunner.Exists(ctx, p)
}

func TestSystemdManifestExistsErrorStopsPrepare(t *testing.T) {
	p, s := systemdPlan()
	d, _ := For(p, s)
	tgt := systemdTarget(t)
	err := d.Prepare(context.Background(), sideOf(systemdSource(t)), sideOf(existsFails{tgt, manifest}))
	if err == nil || !strings.Contains(err.Error(), "ssh dropped") {
		t.Fatalf("got %v", err)
	}
	if tgt.ran("WRITE") {
		t.Fatal("wrote after an Exists error")
	}
}
