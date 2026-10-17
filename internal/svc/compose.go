package svc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/SaiPisey2/cutover/internal/plan"
	"github.com/SaiPisey2/cutover/internal/sshx"
)

const OwnerLabel = "dev.cutover.owner"

var _ Runner = (*sshx.Client)(nil)

type compose struct {
	plan string
	svc  plan.Service
}

func (d compose) project() string { return "cutover-" + d.plan + "-" + d.svc.Name }
func (d compose) dir() string     { return "/var/lib/cutover/" + d.plan + "/" + d.svc.Name }
func (d compose) file() string    { return d.dir() + "/compose.json" }

// marker is written before anything else on the target. Its content is the
// plan name: without it, a project or directory of this name is not ours.
func (d compose) marker() string { return d.dir() + "/.cutover-owner" }

// ours reports whether this plan prepared the target directory.
func (d compose) ours(ctx context.Context, r Runner) (bool, error) {
	has, err := r.Exists(ctx, d.marker())
	if err != nil || !has {
		return false, err
	}
	b, err := r.ReadFile(ctx, d.marker())
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(string(b)) == d.plan, nil
}

// foreign reports what already occupies this plan's target names and was
// not created by cutover for this plan: a compose project or the directory.
func (d compose) foreign(ctx context.Context, r Runner, checkProject bool) (string, error) {
	ours, err := d.ours(ctx, r)
	if err != nil || ours {
		return "", err
	}
	if has, err := r.Exists(ctx, d.dir()); err != nil {
		return "", err
	} else if has {
		return "directory " + d.dir(), nil
	}
	if checkProject {
		ls, err := composeLs(ctx, r)
		if err != nil {
			return "", err
		}
		if slices.ContainsFunc(ls, func(e lsEntry) bool { return e.Name == d.project() }) {
			return "compose project " + d.project(), nil
		}
	}
	return "", nil
}

// runErr describes a failed docker command without its stderr: compose
// quotes environment values in interpolation errors and warnings, and an
// error ends up in workflow history. Non-exit errors (connection, context)
// carry no command output and keep their cause.
func runErr(what string, err error) error {
	var ee *sshx.ExitError
	if errors.As(err, &ee) {
		return fmt.Errorf("%s failed (exit %d)", what, ee.Code)
	}
	return fmt.Errorf("%s: %w", what, err)
}

type lsEntry struct {
	Name        string `json:"Name"`
	ConfigFiles string `json:"ConfigFiles"`
}

func composeLs(ctx context.Context, r Runner) ([]lsEntry, error) {
	out, err := r.Run(ctx, r.Root("docker compose ls --all --format json"), nil)
	if err != nil {
		return nil, runErr("docker compose ls", err)
	}
	var ls []lsEntry
	if err := json.Unmarshal(bytes.TrimSpace(out), &ls); err != nil {
		return nil, fmt.Errorf("docker compose ls: %w", err)
	}
	return ls, nil
}

// definition returns the source's resolved definition of the service, or
// findings saying why there is none. The definition holds the service's
// environment: it stays inside this package.
func (d compose) definition(ctx context.Context, src Side) (map[string]any, []Finding, error) {
	subj := d.svc.Compose.Project + "/" + d.svc.Compose.Service
	r, err := src.Host(ctx, d.svc.Source.Host)
	if err != nil {
		return nil, nil, err
	}
	ls, err := composeLs(ctx, r)
	if err != nil {
		return nil, nil, err
	}
	i := slices.IndexFunc(ls, func(e lsEntry) bool { return e.Name == d.svc.Compose.Project })
	if i < 0 {
		return nil, []Finding{{Blocker: true, Code: "compose-missing", Subject: subj,
			Message: fmt.Sprintf("compose project %s is not on source host %s", d.svc.Compose.Project, d.svc.Source.Host)}}, nil
	}
	if strings.TrimSpace(ls[i].ConfigFiles) == "" {
		return nil, nil, fmt.Errorf("docker compose ls lists no config files for project %s on %s", d.svc.Compose.Project, d.svc.Source.Host)
	}
	files := strings.Split(ls[i].ConfigFiles, ",")
	cmd := "docker compose -p " + sshx.Quote(d.svc.Compose.Project) + " --project-directory " + sshx.Quote(path.Dir(files[0]))
	for _, f := range files {
		cmd += " -f " + sshx.Quote(f)
	}
	out, err := r.Run(ctx, r.Root(cmd+" config --format json"), nil)
	if err != nil {
		return nil, nil, runErr("docker compose config for "+d.svc.Compose.Project, err)
	}
	var doc struct {
		Services map[string]map[string]any `json:"services"`
	}
	if err := json.Unmarshal(out, &doc); err != nil {
		return nil, nil, fmt.Errorf("docker compose config for %s: output is not JSON", d.svc.Compose.Project)
	}
	def, ok := doc.Services[d.svc.Compose.Service]
	if !ok {
		return nil, []Finding{{Blocker: true, Code: "compose-missing", Subject: subj,
			Message: fmt.Sprintf("compose project %s has no service %s", d.svc.Compose.Project, d.svc.Compose.Service)}}, nil
	}
	return def, check(subj, def), nil
}

// check reports what in a definition cannot move with zero downtime.
func check(subj string, def map[string]any) []Finding {
	var fs []Finding
	add := func(blocker bool, code, format string, a ...any) {
		fs = append(fs, Finding{Blocker: blocker, Code: code, Subject: subj, Message: fmt.Sprintf(format, a...)})
	}
	if def["build"] != nil {
		add(true, "compose-build", "%s is built from source on its host; push an image and use image: instead", subj)
	}
	if img, _ := def["image"].(string); img == "" {
		add(true, "compose-image", "%s has no image", subj)
	}
	vols, _ := def["volumes"].([]any)
	for _, v := range vols {
		m, _ := v.(map[string]any)
		switch m["type"] {
		case "bind":
			add(true, "compose-state", "%s bind-mounts %v at %v: that data is on the source host's disk", subj, m["source"], m["target"])
		case "volume":
			add(true, "compose-state", "%s keeps data in volume %v at %v, which stays on the source host", subj, m["source"], m["target"])
		}
	}
	if nm, _ := def["network_mode"].(string); strings.HasPrefix(nm, "service:") || strings.HasPrefix(nm, "container:") {
		add(true, "compose-network", "%s shares the network of %s, which does not move", subj, nm)
	}
	if deps, ok := def["depends_on"].(map[string]any); ok && len(deps) > 0 {
		names := slices.Sorted(maps.Keys(deps))
		add(false, "compose-depends", "%s depends on %s; only %s moves, so the copy must reach them some other way", subj, strings.Join(names, ", "), subj)
	}
	return fs
}

func (d compose) Inspect(ctx context.Context, src, tgt Side) (Facts, error) {
	facts := Facts{Service: d.svc.Name, Kind: plan.KindCompose}
	def, fs, err := d.definition(ctx, src)
	if err != nil {
		return facts, fmt.Errorf("source: %w", err)
	}
	facts.Findings = fs
	if def != nil {
		facts.Image, _ = def["image"].(string)
		if facts.Ports, err = d.ports(ctx, src); err != nil {
			facts.Findings = append(facts.Findings, Finding{Code: "compose-ports-unknown", Subject: d.svc.Compose.Project + "/" + d.svc.Compose.Service,
				Message: fmt.Sprintf("could not list the published ports on source host %s: %v", d.svc.Source.Host, err)})
		}
	}
	r, err := tgt.Host(ctx, d.svc.Target.Host)
	if err != nil {
		return facts, fmt.Errorf("target: %w", err)
	}
	out, err := r.Run(ctx, r.Root("docker compose version --short"), nil)
	dockerOK := err == nil
	if err != nil {
		facts.Findings = append(facts.Findings, Finding{Blocker: true, Code: "compose-target", Subject: d.svc.Target.Host,
			Message: fmt.Sprintf("Docker Compose v2 does not run on target host %s: %v", d.svc.Target.Host, runErr("docker compose version", err))})
	} else if !atLeast(strings.TrimSpace(string(out)), 2, 20) {
		facts.Findings = append(facts.Findings, Finding{Blocker: true, Code: "compose-target", Subject: d.svc.Target.Host,
			Message: fmt.Sprintf("target host %s has Docker Compose %s; cutover needs 2.20 or later (up --wait)", d.svc.Target.Host, strings.TrimSpace(string(out)))})
	}
	// docker answers only when it runs; the directory check needs no docker.
	what, err := d.foreign(ctx, r, dockerOK)
	if err != nil {
		return facts, fmt.Errorf("target: %w", err)
	}
	if what != "" {
		facts.Findings = append(facts.Findings, Finding{Blocker: true, Code: "compose-target-exists", Subject: d.svc.Target.Host,
			Message: fmt.Sprintf("target host %s already has %s that cutover did not create; cutover never overwrites it: rename or remove it", d.svc.Target.Host, what)})
	}
	return facts, nil
}

// atLeast compares "2.29.7" (an optional leading "v") with major.minor.
func atLeast(v string, major, minor int) bool {
	parts := strings.SplitN(strings.TrimPrefix(v, "v"), ".", 3)
	if len(parts) < 2 {
		return false
	}
	ma, err1 := strconv.Atoi(parts[0])
	mi, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil {
		return false
	}
	return ma > major || (ma == major && mi >= minor)
}

func (d compose) Prepare(ctx context.Context, src, tgt Side) error {
	def, fs, err := d.definition(ctx, src)
	if err != nil {
		return fmt.Errorf("source: %w", err)
	}
	if err := blocked(fs); err != nil {
		return err
	}
	cp := maps.Clone(def)
	for _, k := range []string{"depends_on", "container_name", "networks"} {
		delete(cp, k)
	}
	labels, _ := cp["labels"].(map[string]any)
	labels = maps.Clone(labels)
	if labels == nil {
		labels = map[string]any{}
	}
	labels[OwnerLabel] = d.plan
	cp["labels"] = labels
	doc, err := json.MarshalIndent(map[string]any{"name": d.project(), "services": map[string]any{d.svc.Compose.Service: cp}}, "", "  ")
	if err != nil {
		return err
	}
	r, err := tgt.Host(ctx, d.svc.Target.Host)
	if err != nil {
		return fmt.Errorf("target: %w", err)
	}
	what, err := d.foreign(ctx, r, true)
	if err != nil {
		return fmt.Errorf("target: %w", err)
	}
	if what != "" {
		return fmt.Errorf("%w: %s: target already has %s that cutover did not create", ErrBlocked, d.svc.Target.Host, what)
	}
	// The marker first: a crash between the two writes leaves a directory
	// Undo recognises as ours.
	if err := r.WriteFile(ctx, d.marker(), 0o644, []byte(d.plan+"\n")); err != nil {
		return fmt.Errorf("target: write %s: %w", d.marker(), err)
	}
	if err := r.WriteFile(ctx, d.file(), 0o600, doc); err != nil {
		return fmt.Errorf("target: write %s: %w", d.file(), err)
	}
	_, err = r.Run(ctx, r.Root("docker compose -p "+sshx.Quote(d.project())+" -f "+sshx.Quote(d.file())+
		" up -d --wait --wait-timeout 300 --quiet-pull"), nil)
	if err != nil {
		return fmt.Errorf("target: %w", runErr("compose up", err))
	}
	return nil
}

type psEntry struct {
	Service    string `json:"Service"`
	State      string `json:"State"`
	Health     string `json:"Health"`
	Publishers []struct {
		TargetPort    int    `json:"TargetPort"`
		PublishedPort int    `json:"PublishedPort"`
		Protocol      string `json:"Protocol"`
	} `json:"Publishers"`
}

// parsePs reads `docker compose ps --format json`: a JSON array or one
// object per line, depending on the compose version.
func parsePs(out []byte) ([]psEntry, error) {
	var ps []psEntry
	out = bytes.TrimSpace(out)
	if bytes.HasPrefix(out, []byte("[")) {
		if err := json.Unmarshal(out, &ps); err != nil {
			return nil, fmt.Errorf("docker compose ps: %w", err)
		}
		return ps, nil
	}
	for _, line := range bytes.Split(out, []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var e psEntry
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, fmt.Errorf("docker compose ps: %w", err)
		}
		ps = append(ps, e)
	}
	return ps, nil
}

// ports lists what the source service publishes, from its running containers.
func (d compose) ports(ctx context.Context, src Side) ([]string, error) {
	r, err := src.Host(ctx, d.svc.Source.Host)
	if err != nil {
		return nil, err
	}
	out, err := r.Run(ctx, r.Root("docker compose -p "+sshx.Quote(d.svc.Compose.Project)+" ps --format json"), nil)
	if err != nil {
		return nil, runErr("docker compose ps", err)
	}
	ps, err := parsePs(out)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{} // IPv4 and IPv6 publish the same port twice
	var ports []string
	for _, e := range ps {
		if e.Service != d.svc.Compose.Service {
			continue
		}
		for _, p := range e.Publishers {
			if p.PublishedPort == 0 {
				continue
			}
			s := fmt.Sprintf("%d->%d/%s", p.PublishedPort, p.TargetPort, p.Protocol)
			if !seen[s] {
				seen[s] = true
				ports = append(ports, s)
			}
		}
	}
	slices.Sort(ports)
	return ports, nil
}

func (d compose) Healthy(ctx context.Context, tgt Side) (bool, string, error) {
	r, err := tgt.Host(ctx, d.svc.Target.Host)
	if err != nil {
		return false, "", err
	}
	out, err := r.Run(ctx, r.Root("docker compose -p "+sshx.Quote(d.project())+" ps --all --format json"), nil)
	if err != nil {
		return false, "", runErr("docker compose ps", err)
	}
	ps, err := parsePs(out)
	if err != nil {
		return false, "", err
	}
	if len(ps) == 0 {
		return false, "no containers in " + d.project(), nil
	}
	ok := true
	var states []string
	for _, e := range ps {
		s := e.Service + ": " + e.State
		if e.Health != "" {
			s += " (" + e.Health + ")"
		}
		states = append(states, s)
		if e.State != "running" || (e.Health != "" && e.Health != "healthy") {
			ok = false
		}
	}
	return ok, strings.Join(states, ", "), nil
}

// Undo removes the project and directory Prepare made, recognised by the
// owner marker. A project or directory without it is not ours: left alone.
func (d compose) Undo(ctx context.Context, tgt Side) error {
	r, err := tgt.Host(ctx, d.svc.Target.Host)
	if err != nil {
		return err
	}
	ours, err := d.ours(ctx, r)
	if err != nil || !ours {
		return err
	}
	if has, err := r.Exists(ctx, d.file()); err != nil {
		return err
	} else if has {
		if _, err := r.Run(ctx, r.Root("docker compose -p "+sshx.Quote(d.project())+" -f "+sshx.Quote(d.file())+" down --remove-orphans"), nil); err != nil {
			return runErr("compose down", err)
		}
	} else if _, err := r.Run(ctx, r.Root("docker compose -p "+sshx.Quote(d.project())+" down --remove-orphans"), nil); err != nil {
		return runErr("compose down", err)
	}
	_, err = r.Run(ctx, r.Root("rm -rf "+sshx.Quote(d.dir())), nil)
	return err
}
