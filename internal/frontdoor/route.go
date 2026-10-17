// Package frontdoor is cutover's HTTP front door: the routes it serves,
// the Envoy configuration they become, the xDS server that pushes it, the
// Envoy admin reader, and the API activities and the CLI call.
package frontdoor

import (
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"

	"github.com/SaiPisey2/cutover/internal/plan"
)

const (
	SideSource = "source"
	SideTarget = "target"
)

// Upstream is one copy of a service: host:port Envoy connects to.
type Upstream struct {
	Address string `json:"address"`
	Port    int    `json:"port"`
}

func (u Upstream) String() string { return net.JoinHostPort(u.Address, strconv.Itoa(u.Port)) }

func ParseUpstream(hostport string) (Upstream, error) {
	h, p, err := net.SplitHostPort(hostport)
	if err != nil {
		return Upstream{}, fmt.Errorf("%q is not host:port", hostport)
	}
	n, err := strconv.Atoi(p)
	if err != nil {
		return Upstream{}, fmt.Errorf("%q: port is not a number", hostport)
	}
	u := Upstream{Address: h, Port: n}
	return u, u.validate()
}

func (u Upstream) validate() error {
	if u.Address == "" || (net.ParseIP(u.Address) == nil && !plan.DNSName.MatchString(u.Address)) {
		return fmt.Errorf("address %q is not an IP or DNS name", u.Address)
	}
	if u.Port < 1 || u.Port > 65535 {
		return errors.New("port must be between 1 and 65535")
	}
	return nil
}

// Route is everything the front door knows about one service: where users
// reach it, where its two copies are, and how traffic is split. Owner is
// the plan name; Owner and Service together are the route's key.
type Route struct {
	Owner      string    `json:"owner"`
	Service    string    `json:"service"`
	Host       string    `json:"host"`
	Prefix     string    `json:"prefix"`
	HealthPath string    `json:"health_path"`
	Source     *Upstream `json:"source,omitempty"`
	Target     *Upstream `json:"target,omitempty"`
	// TargetWeight is the percentage of requests sent to the target.
	TargetWeight int `json:"target_weight"`
	// Mirror copies requests whose method is in MirrorMethods to the
	// target, discarding the target's responses.
	Mirror        bool     `json:"mirror"`
	MirrorMethods []string `json:"mirror_methods,omitempty"`
}

// Finished reports whether r is the shape Finish leaves: all traffic on
// the target, no source, no mirroring. Nothing more changes such a route,
// so a plan may take it over or re-register it.
func (r Route) Finished() bool {
	return r.Source == nil && r.Target != nil && r.TargetWeight == 100 && !r.Mirror
}

func (r Route) Key() string { return r.Owner + "/" + r.Service }

// ClusterName is the Envoy cluster for one side of a service. Owner names
// and service names never contain "_", so the name splits back unambiguously.
func ClusterName(owner, service, side string) string {
	return "c_" + owner + "_" + service + "_" + side
}

// ClusterNames lists the clusters this route needs, source first.
func (r Route) ClusterNames() []string {
	var out []string
	if r.Source != nil {
		out = append(out, ClusterName(r.Owner, r.Service, SideSource))
	}
	if r.Target != nil {
		out = append(out, ClusterName(r.Owner, r.Service, SideTarget))
	}
	return out
}

func (r Route) Validate() error {
	var errs []string
	bad := func(format string, a ...any) { errs = append(errs, fmt.Sprintf(format, a...)) }
	if !plan.NamePattern.MatchString(r.Owner) {
		bad("owner %q must match %s", r.Owner, plan.NamePattern)
	}
	if !plan.ServiceName.MatchString(r.Service) {
		bad("service %q must match %s", r.Service, plan.ServiceName)
	}
	if !plan.DNSName.MatchString(r.Host) {
		bad("host %q is not a lowercase DNS name", r.Host)
	}
	if !plan.URLPath.MatchString(r.Prefix) {
		bad("prefix %q must start with /", r.Prefix)
	}
	if r.HealthPath == "" || !plan.URLPath.MatchString(r.HealthPath) {
		bad("health_path %q must start with /", r.HealthPath)
	}
	for side, u := range map[string]*Upstream{SideSource: r.Source, SideTarget: r.Target} {
		if u != nil {
			if err := u.validate(); err != nil {
				bad("%s: %v", side, err)
			}
		}
	}
	switch {
	case r.Source == nil && r.Target == nil:
		bad("a route needs a source or a target")
	case r.TargetWeight < 0 || r.TargetWeight > 100:
		bad("target_weight must be between 0 and 100")
	case r.Target == nil && r.TargetWeight != 0:
		bad("target_weight must be 0 without a target")
	case r.Source == nil && r.TargetWeight != 100:
		bad("target_weight must be 100 without a source")
	}
	if r.Mirror {
		switch {
		case r.Source == nil || r.Target == nil:
			bad("mirroring needs both a source and a target")
		case len(r.MirrorMethods) == 0:
			bad("mirroring needs at least one method")
		case r.TargetWeight != 0:
			bad("mirroring needs target_weight 0")
		}
	}
	for _, m := range r.MirrorMethods {
		if !slices.Contains(plan.SafeMethods, m) {
			bad("mirror method %q is not a safe method (GET, HEAD, OPTIONS)", m)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	slices.Sort(errs)
	return errors.New("route " + r.Key() + ": " + strings.Join(errs, "; "))
}

// CheckConflicts refuses r if another route already serves its host and prefix.
func CheckConflicts(existing []Route, r Route) error {
	for _, e := range existing {
		if e.Key() != r.Key() && e.Host == r.Host && trimPrefix(e.Prefix) == trimPrefix(r.Prefix) {
			return fmt.Errorf("%s%s is already routed by %s", r.Host, r.Prefix, e.Key())
		}
	}
	return nil
}

// trimPrefix drops one trailing "/" so "/billing" and "/billing/" compare
// equal; "/" itself stays "/".
func trimPrefix(p string) string {
	if p != "/" {
		return strings.TrimSuffix(p, "/")
	}
	return p
}
