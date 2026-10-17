package svc

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/SaiPisey2/cutover/internal/plan"
	"github.com/SaiPisey2/cutover/internal/sshx"
)

// SystemdMarker is the drop-in that marks a unit as cutover's on a target.
const SystemdMarker = "zz-cutover.conf"

// Unit files, environment files and ExecStart argv can hold secrets. Errors
// from commands and file copies go through runErr, which drops stderr, and
// nothing read from those files reaches a finding.
type systemd struct {
	plan string
	svc  plan.Service
}

func osMode(m uint64) os.FileMode { return os.FileMode(m) & os.ModePerm }

func (d systemd) unit() string       { return d.svc.Systemd.Unit }
func (d systemd) unitPath() string   { return "/etc/systemd/system/" + d.unit() }
func (d systemd) dropDir() string    { return d.unitPath() + ".d" }
func (d systemd) markerPath() string { return d.dropDir() + "/" + SystemdMarker }
func (d systemd) manifestPath() string {
	return "/var/lib/cutover/" + d.plan + "/systemd/" + d.unit() + ".manifest"
}

// unitInfo is what systemctl show says about the unit on the source.
type unitInfo struct {
	loaded   bool
	fragment string
	dropIns  []string
	needs    []string // ExecStart binaries and required environment files
}

func (d systemd) show(ctx context.Context, r Runner) (unitInfo, error) {
	out, err := r.Run(ctx, r.Root("systemctl show -p LoadState -p FragmentPath -p DropInPaths -p ExecStart -p EnvironmentFiles --no-pager "+sshx.Quote(d.unit())), nil)
	if err != nil {
		return unitInfo{}, runErr("systemctl show "+d.unit(), err)
	}
	var u unitInfo
	for _, line := range strings.Split(string(out), "\n") {
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		switch k {
		case "LoadState":
			u.loaded = v == "loaded"
		case "FragmentPath":
			u.fragment = v
		case "DropInPaths":
			for _, dp := range strings.Fields(v) {
				if path.Base(dp) != SystemdMarker { // never let a source drop-in take the marker's place
					u.dropIns = append(u.dropIns, dp)
				}
			}
		case "ExecStart":
			// Only systemd's own leading path= field is kept; argv[] may
			// hold inline secrets, and a value that is not a clean
			// absolute path is dropped without being echoed.
			if rest, ok := strings.CutPrefix(strings.TrimSpace(v), "{"); ok {
				if p, ok := strings.CutPrefix(strings.TrimSpace(rest), "path="); ok {
					p, _, _ = strings.Cut(p, " ;")
					if p = strings.TrimSpace(p); cleanAbs(p) {
						u.needs = append(u.needs, p)
					}
				}
			}
		case "EnvironmentFiles":
			f := strings.Fields(v)
			if len(f) > 0 && !strings.Contains(v, "ignore_errors=yes") {
				if cleanAbs(f[0]) {
					u.needs = append(u.needs, f[0])
				}
			}
		}
	}
	return u, nil
}

func cleanAbs(p string) bool { return strings.HasPrefix(p, "/") && filepath.Clean(p) == p }

// manifestPaths returns what an earlier Prepare recorded as created by
// cutover. An unreadable manifest is an error, not an empty one.
func (d systemd) manifestPaths(ctx context.Context, r Runner) ([]string, error) {
	has, err := r.Exists(ctx, d.manifestPath())
	if err != nil || !has {
		return nil, err
	}
	b, err := r.ReadFile(ctx, d.manifestPath())
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", runErr("read "+d.manifestPath(), err))
	}
	var out []string
	for _, l := range strings.Split(string(b), "\n") {
		if l != "" {
			out = append(out, l)
		}
	}
	return out, nil
}

func sha(ctx context.Context, r Runner, p string) (string, error) {
	out, err := r.Run(ctx, r.Root("sha256sum "+sshx.Quote(p)), nil)
	if err != nil {
		return "", runErr("sha256sum "+p, err)
	}
	f := strings.Fields(string(out))
	if len(f) == 0 {
		return "", fmt.Errorf("sha256sum %s: no output", p)
	}
	return f[0], nil
}

// ours reports whether the target's unit carries this plan's marker.
func (d systemd) ours(ctx context.Context, r Runner) (bool, error) {
	has, err := r.Exists(ctx, d.markerPath())
	if err != nil || !has {
		return false, err
	}
	b, err := r.ReadFile(ctx, d.markerPath())
	if err != nil {
		return false, runErr("read "+d.markerPath(), err)
	}
	return strings.Contains(string(b), "X-Cutover-Owner="+d.plan+"\n"), nil
}

// inspect works out the findings, the source unit, and the listed files
// the target lacks (what Prepare will copy).
func (d systemd) inspect(ctx context.Context, src, tgt Side) ([]Finding, unitInfo, []string, error) {
	unit := d.unit()
	var fs []Finding
	add := func(blocker bool, code, subject, format string, a ...any) {
		fs = append(fs, Finding{Blocker: blocker, Code: code, Subject: subject, Message: fmt.Sprintf(format, a...)})
	}
	sr, err := src.Host(ctx, d.svc.Source.Host)
	if err != nil {
		return nil, unitInfo{}, nil, fmt.Errorf("source: %w", err)
	}
	tr, err := tgt.Host(ctx, d.svc.Target.Host)
	if err != nil {
		return nil, unitInfo{}, nil, fmt.Errorf("target: %w", err)
	}
	u, err := d.show(ctx, sr)
	if err != nil {
		return nil, unitInfo{}, nil, fmt.Errorf("source: %w", err)
	}
	if !u.loaded || u.fragment == "" {
		add(true, "systemd-missing", unit, "unit %s is not loaded on source host %s", unit, d.svc.Source.Host)
		return fs, u, nil, nil
	}
	out, err := tr.Run(ctx, tr.Root("systemctl show -p LoadState --no-pager "+sshx.Quote(unit)), nil)
	if err != nil {
		add(true, "systemd-target", d.svc.Target.Host, "systemctl does not run on target host %s: %v", d.svc.Target.Host, runErr("systemctl show "+unit, err))
		return fs, u, nil, nil
	}
	// Anything but not-found (loaded, masked, bad-setting, error, ...) is
	// an existing unit.
	state := ""
	for _, line := range strings.Split(string(out), "\n") {
		if v, ok := strings.CutPrefix(line, "LoadState="); ok {
			state = strings.TrimSpace(v)
		}
	}
	if state != "not-found" {
		mine, err := d.ours(ctx, tr)
		if err != nil {
			return nil, u, nil, fmt.Errorf("target: %w", err)
		}
		if !mine {
			add(true, "systemd-target-unit", unit, "target host %s already has unit %s (LoadState=%s); cutover never replaces a unit it did not create", d.svc.Target.Host, unit, state)
		}
	}
	mine, err := d.manifestPaths(ctx, tr)
	if err != nil {
		return nil, u, nil, fmt.Errorf("target: %w", err)
	}
	seen := map[string]bool{}
	targets := []string{d.unitPath(), d.markerPath()}
	for _, dp := range u.dropIns {
		to := d.dropDir() + "/" + path.Base(dp)
		if seen[to] {
			add(true, "systemd-dropin-duplicate", to, "unit %s has two drop-ins named %s on source host %s; cutover cannot copy both to one path", unit, path.Base(dp), d.svc.Source.Host)
			continue
		}
		seen[to] = true
		targets = append(targets, to)
	}
	for _, to := range targets {
		there, err := tr.Exists(ctx, to)
		if err != nil {
			return nil, u, nil, fmt.Errorf("target: %w", err)
		}
		if there && !slices.Contains(mine, to) {
			add(true, "systemd-target-file", to, "%s already exists on target host %s and cutover did not create it; cutover never overwrites such a file", to, d.svc.Target.Host)
		}
	}
	var toCopy []string
	for _, f := range d.svc.Systemd.Files {
		ok, err := sr.Exists(ctx, f)
		if err != nil {
			return nil, u, nil, fmt.Errorf("source: %w", err)
		}
		if !ok {
			add(true, "systemd-file-source", f, "%s is listed in systemd.files but is not on source host %s", f, d.svc.Source.Host)
			continue
		}
		there, err := tr.Exists(ctx, f)
		if err != nil {
			return nil, u, nil, fmt.Errorf("target: %w", err)
		}
		if !there || slices.Contains(mine, f) { // cutover's own copy is refreshed, not compared
			toCopy = append(toCopy, f)
			continue
		}
		a, err := sha(ctx, sr, f)
		if err != nil {
			return nil, u, nil, fmt.Errorf("source: %w", err)
		}
		b, err := sha(ctx, tr, f)
		if err != nil {
			return nil, u, nil, fmt.Errorf("target: %w", err)
		}
		if a != b {
			add(true, "systemd-file-differs", f, "%s on target host %s differs from the source's; cutover never overwrites a file it did not create", f, d.svc.Target.Host)
		}
	}
	for _, n := range u.needs {
		if slices.Contains(d.svc.Systemd.Files, n) {
			continue
		}
		ok, err := tr.Exists(ctx, n)
		if err != nil {
			return nil, u, nil, fmt.Errorf("target: %w", err)
		}
		if !ok {
			add(true, "systemd-file-missing", n, "unit %s needs %s, which is neither in systemd.files nor on target host %s", unit, n, d.svc.Target.Host)
		}
	}
	add(false, "systemd-state", unit, "cutover cannot tell whether %s keeps state on its host's disk; make sure it does not", unit)
	return fs, u, toCopy, nil
}

func (d systemd) Inspect(ctx context.Context, src, tgt Side) (Facts, error) {
	fs, _, _, err := d.inspect(ctx, src, tgt)
	return Facts{Service: d.svc.Name, Kind: plan.KindSystemd, Findings: fs}, err
}

func (d systemd) Prepare(ctx context.Context, src, tgt Side) error {
	fs, u, toCopy, err := d.inspect(ctx, src, tgt)
	if err != nil {
		return err
	}
	if err := blocked(fs); err != nil {
		return err
	}
	sr, err := src.Host(ctx, d.svc.Source.Host)
	if err != nil {
		return fmt.Errorf("source: %w", err)
	}
	tr, err := tgt.Host(ctx, d.svc.Target.Host)
	if err != nil {
		return fmt.Errorf("target: %w", err)
	}

	dropIns := u.dropIns

	created := slices.Clone(toCopy)
	created = append(created, d.unitPath(), d.markerPath())
	for _, dp := range dropIns {
		created = append(created, d.dropDir()+"/"+path.Base(dp))
	}
	old, err := d.manifestPaths(ctx, tr)
	if err != nil {
		return fmt.Errorf("target: %w", err)
	}
	for _, l := range old {
		if !slices.Contains(created, l) {
			created = append(created, l)
		}
	}
	if err := tr.WriteFile(ctx, d.manifestPath(), 0o600, []byte(strings.Join(created, "\n")+"\n")); err != nil {
		return fmt.Errorf("target: manifest: %w", runErr("write "+d.manifestPath(), err))
	}
	if err := tr.WriteFile(ctx, d.markerPath(), 0o644, []byte("[Unit]\nX-Cutover-Owner="+d.plan+"\n")); err != nil {
		return fmt.Errorf("target: marker: %w", runErr("write "+d.markerPath(), err))
	}
	copyFile := func(from, to string, mode uint64) error {
		b, err := sr.ReadFile(ctx, from)
		if err != nil {
			return fmt.Errorf("source: %w", runErr("read "+from, err))
		}
		if err := tr.WriteFile(ctx, to, osMode(mode), b); err != nil {
			return fmt.Errorf("target: %w", runErr("write "+to, err))
		}
		return nil
	}
	for _, f := range toCopy {
		out, err := sr.Run(ctx, sr.Root("stat -c %a "+sshx.Quote(f)), nil)
		if err != nil {
			return fmt.Errorf("source: %w", runErr("stat "+f, err))
		}
		mode, err := strconv.ParseUint(strings.TrimSpace(string(out)), 8, 32)
		if err != nil {
			return fmt.Errorf("source: mode of %s is not octal", f)
		}
		if err := copyFile(f, f, mode); err != nil {
			return err
		}
	}
	if err := copyFile(u.fragment, d.unitPath(), 0o644); err != nil {
		return err
	}
	for _, dp := range dropIns {
		if err := copyFile(dp, d.dropDir()+"/"+path.Base(dp), 0o644); err != nil {
			return err
		}
	}
	if _, err := tr.Run(ctx, tr.Root("systemctl daemon-reload && systemctl enable --now "+sshx.Quote(d.unit())), nil); err != nil {
		return fmt.Errorf("target: %w", runErr("enable "+d.unit(), err))
	}
	return nil
}

func (d systemd) Healthy(ctx context.Context, tgt Side) (bool, string, error) {
	r, err := tgt.Host(ctx, d.svc.Target.Host)
	if err != nil {
		return false, "", err
	}
	out, err := r.Run(ctx, r.Root("systemctl is-active "+sshx.Quote(d.unit())+" || true"), nil)
	if err != nil {
		return false, "", runErr("systemctl is-active "+d.unit(), err)
	}
	s := strings.TrimSpace(string(out))
	return s == "active", d.unit() + ": " + s, nil
}

func (d systemd) Undo(ctx context.Context, tgt Side) error {
	r, err := tgt.Host(ctx, d.svc.Target.Host)
	if err != nil {
		return err
	}
	has, err := r.Exists(ctx, d.manifestPath())
	if err != nil || !has {
		return err
	}
	b, err := r.ReadFile(ctx, d.manifestPath())
	if err != nil {
		return runErr("read "+d.manifestPath(), err)
	}
	var quoted []string
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "/") {
			quoted = append(quoted, sshx.Quote(l))
		}
	}
	script := ""
	mine, err := d.ours(ctx, r)
	if err != nil {
		return err
	}
	if mine {
		script = "systemctl disable --now " + sshx.Quote(d.unit()) + " || true; "
	}
	if len(quoted) > 0 {
		script += "rm -f " + strings.Join(quoted, " ") + "; "
	}
	script += "rmdir " + sshx.Quote(d.dropDir()) + " 2>/dev/null || true; systemctl daemon-reload; rm -f " + sshx.Quote(d.manifestPath())
	if _, err := r.Run(ctx, r.Root(script), nil); err != nil {
		return runErr("undo "+d.unit(), err)
	}
	return nil
}
