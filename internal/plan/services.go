package plan

import (
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/SaiPisey2/cutover/internal/secret"
)

const (
	KindCompose    = "compose"
	KindSystemd    = "systemd"
	KindDeployment = "deployment"
	KindExternal   = "external"
)

var (
	// ServiceName is the shape of service and host names. It has no
	// underscore, so "c_<plan>_<service>_<side>" splits unambiguously.
	ServiceName  = regexp.MustCompile(`^[a-z][a-z0-9]*(-[a-z0-9]+)*$`)
	DNSName      = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)*$`)
	composeName  = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)
	unitName     = regexp.MustCompile(`^[A-Za-z0-9@._-]{1,200}\.service$`)
	k8sName      = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)
	k8sSubdomain = regexp.MustCompile(`^[a-z0-9]([a-z0-9.-]{0,251}[a-z0-9])?$`)
	userName     = regexp.MustCompile(`^[a-z_][a-z0-9_-]{0,31}$`)
	filePath     = regexp.MustCompile(`^/[A-Za-z0-9._@+/-]*$`)
	URLPath      = regexp.MustCompile(`^/[A-Za-z0-9._~/-]*$`)
)

// SafeMethods are the only methods Rehearse may mirror: a mirrored write
// would run twice against the one database both copies share.
var SafeMethods = []string{"GET", "HEAD", "OPTIONS"}

// Host is an SSH host. Key is a reference to a private key (no passphrase);
// KnownHosts is the path of a known_hosts file that must list the host.
type Host struct {
	Address    string     `yaml:"address" json:"address"`
	Port       int        `yaml:"port" json:"port"`
	User       string     `yaml:"user" json:"user"`
	Key        secret.Ref `yaml:"key" json:"key"`
	KnownHosts string     `yaml:"known_hosts" json:"known_hosts"`
	// Sudo prefixes commands that need root with "sudo -n".
	Sudo bool `yaml:"sudo" json:"sudo"`
}

// Kube names a kubeconfig file and context. The path is used as given:
// cutover never falls back to KUBECONFIG or ~/.kube/config.
type Kube struct {
	Kubeconfig string `yaml:"kubeconfig" json:"kubeconfig"`
	Context    string `yaml:"context" json:"context"`
}

// FrontDoor is how the worker reaches cutover server, and the addresses
// every route's DNS name must resolve to.
type FrontDoor struct {
	API       string     `yaml:"api" json:"api"`
	Token     secret.Ref `yaml:"token" json:"token"`
	Addresses []string   `yaml:"addresses" json:"addresses"`
}

type Service struct {
	Name string `yaml:"name" json:"name"`
	Kind string `yaml:"kind" json:"kind"`
	// Route is how users reach the service through the front door; a
	// service without one (a background worker) is copied but not routed.
	Route *Route `yaml:"route" json:"route,omitempty"`
	// MirrorMethods are mirrored to the target in Rehearse. Omitted means
	// GET and HEAD; an empty list turns mirroring off for this service.
	MirrorMethods []string    `yaml:"mirror_methods" json:"mirror_methods"`
	Compose       *Compose    `yaml:"compose" json:"compose,omitempty"`
	Systemd       *Systemd    `yaml:"systemd" json:"systemd,omitempty"`
	Deployment    *Deployment `yaml:"deployment" json:"deployment,omitempty"`
	Source        Placement   `yaml:"source" json:"source"`
	Target        Placement   `yaml:"target" json:"target"`
}

type Route struct {
	Host       string `yaml:"host" json:"host"`
	Prefix     string `yaml:"prefix" json:"prefix"`
	HealthPath string `yaml:"health_path" json:"health_path"`
}

// Placement is where one copy of a service runs: Host names an entry in
// that side's hosts (compose, systemd); Endpoint is host:port the front
// door connects to (routed services).
type Placement struct {
	Host     string `yaml:"host" json:"host,omitempty"`
	Endpoint string `yaml:"endpoint" json:"endpoint,omitempty"`
}

type Compose struct {
	Project string `yaml:"project" json:"project"`
	Service string `yaml:"service" json:"service"`
}

// Systemd names a unit and the files it needs on the target that are not
// already there (its binary, environment files, config).
type Systemd struct {
	Unit  string   `yaml:"unit" json:"unit"`
	Files []string `yaml:"files" json:"files,omitempty"`
}

type Deployment struct {
	Namespace string `yaml:"namespace" json:"namespace"`
	Name      string `yaml:"name" json:"name"`
}

type Shift struct {
	// Steps are target weights in percent; the last is 100.
	Steps []int    `yaml:"steps" json:"steps"`
	Hold  Duration `yaml:"hold" json:"hold"`
	// Preapprove approves the whole ramp at its first gate.
	Preapprove bool `yaml:"preapprove" json:"preapprove"`
	// MaxErrorDelta is in percentage points of error rate.
	MaxErrorDelta float64 `yaml:"max_error_delta" json:"max_error_delta"`
	// MaxP99 is a factor of the reference p99 (see judge in internal/flow).
	MaxP99      float64  `yaml:"max_p99" json:"max_p99"`
	MinRequests int      `yaml:"min_requests" json:"min_requests"`
	SampleEvery Duration `yaml:"sample_every" json:"sample_every"`
}

func (s Service) Routed() bool { return s.Route != nil }

// RoutedServices returns the services that have a route, in plan order.
func (p Plan) RoutedServices() []Service {
	var out []Service
	for _, s := range p.Services {
		if s.Routed() {
			out = append(out, s)
		}
	}
	return out
}

func (p Plan) Service(name string) (Service, bool) {
	for _, s := range p.Services {
		if s.Name == name {
			return s, true
		}
	}
	return Service{}, false
}

// EnvFor returns the target environment when target is true, else the source.
func (p Plan) EnvFor(target bool) Env {
	if target {
		return p.Target
	}
	return p.Source
}

func applyServiceDefaults(p *Plan) {
	for _, env := range []*Env{&p.Source, &p.Target} {
		for name, h := range env.Hosts {
			if h.Port == 0 {
				h.Port = 22
				env.Hosts[name] = h
			}
		}
	}
	if h := p.Database.PgBouncer.Host; h != nil && h.Port == 0 {
		h.Port = 22
	}
	for i := range p.Services {
		s := &p.Services[i]
		if s.Route != nil && s.Route.Prefix == "" {
			s.Route.Prefix = "/"
		}
		if s.MirrorMethods == nil {
			s.MirrorMethods = []string{"GET", "HEAD"}
		}
	}
	sh := &p.Thresholds.Shift
	if len(sh.Steps) == 0 {
		sh.Steps = []int{1, 10, 50, 100}
	}
	setDur(&sh.Hold, 5*time.Minute)
	setDur(&sh.SampleEvery, 10*time.Second)
	if sh.MaxErrorDelta == 0 {
		sh.MaxErrorDelta = 0.5
	}
	if sh.MaxP99 == 0 {
		sh.MaxP99 = 2
	}
	if sh.MinRequests == 0 {
		sh.MinRequests = 50
	}
	setDur(&p.Thresholds.Rehearse.Window, 5*time.Minute)
}

func validateHost(at string, h Host, bad func(string, ...any)) {
	switch {
	case h.Address == "" || strings.ContainsAny(h.Address, " \t\r\n/"):
		bad("%s.address: required, a host name or IP", at)
	case net.ParseIP(h.Address) == nil && !DNSName.MatchString(h.Address):
		bad("%s.address: %q is not a host name or IP", at, h.Address)
	}
	if h.Port < 1 || h.Port > 65535 {
		bad("%s.port: must be between 1 and 65535", at)
	}
	if !userName.MatchString(h.User) {
		bad("%s.user: %q is not a user name", at, h.User)
	}
	if err := h.Key.Validate(); err != nil {
		bad("%s.key: %v", at, err)
	}
	if !filepath.IsAbs(h.KnownHosts) {
		bad("%s.known_hosts: must be an absolute path", at)
	}
}

// validEndpoint checks host:port; the host is an IP or a DNS name.
func validEndpoint(s string) bool {
	host, port, err := net.SplitHostPort(s)
	if err != nil || host == "" {
		return false
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return false
	}
	return net.ParseIP(host) != nil || DNSName.MatchString(host)
}

func (p Plan) validateServices(bad func(string, ...any)) {
	for _, side := range []string{"source", "target"} {
		env := p.EnvFor(side == "target")
		for name, h := range env.Hosts {
			if !ServiceName.MatchString(name) {
				bad("%s.hosts: name %q must match %s", side, name, ServiceName)
			}
			validateHost(side+".hosts."+name, h, bad)
		}
		if k := env.Kube; k != nil {
			if !filepath.IsAbs(k.Kubeconfig) {
				bad("%s.kube.kubeconfig: must be an absolute path", side)
			}
			if k.Context == "" {
				bad("%s.kube.context: required", side)
			}
		}
	}
	if h := p.Database.PgBouncer.Host; h != nil {
		validateHost("database.pgbouncer.host", *h, bad)
	}

	names := map[string]bool{}
	routes := map[string]string{} // host+prefix -> service
	for i, s := range p.Services {
		at := fmt.Sprintf("services[%d]", i)
		if !ServiceName.MatchString(s.Name) || len(s.Name) > 30 {
			bad("%s: name must match %s (at most 30 characters)", at, ServiceName)
		} else {
			at += " (" + s.Name + ")"
			if names[s.Name] {
				bad("%s: duplicate service name", at)
			}
			names[s.Name] = true
			if s.Name == "systemd" {
				bad("%s: the name \"systemd\" is reserved (systemd services keep their files under <plan>/systemd/)", at)
			}
		}
		blocks := 0
		for _, set := range []bool{s.Compose != nil, s.Systemd != nil, s.Deployment != nil} {
			if set {
				blocks++
			}
		}
		needHost, needKube := false, false
		switch s.Kind {
		case KindCompose:
			switch {
			case s.Compose == nil:
				bad("%s: kind compose needs a compose block", at)
			case blocks > 1:
				bad("%s: kind compose takes only the compose block", at)
			default:
				if !composeName.MatchString(s.Compose.Project) {
					bad("%s: compose.project: %q is not a compose project name", at, s.Compose.Project)
				}
				if !composeName.MatchString(s.Compose.Service) {
					bad("%s: compose.service: %q is not a compose service name", at, s.Compose.Service)
				}
			}
			needHost = true
		case KindSystemd:
			switch {
			case s.Systemd == nil:
				bad("%s: kind systemd needs a systemd block", at)
			case blocks > 1:
				bad("%s: kind systemd takes only the systemd block", at)
			default:
				if !unitName.MatchString(s.Systemd.Unit) {
					bad("%s: systemd.unit: %q must be a .service unit", at, s.Systemd.Unit)
				}
				seen := map[string]bool{}
				for _, f := range s.Systemd.Files {
					if !filePath.MatchString(f) || filepath.Clean(f) != f || f == "/" {
						bad("%s: systemd.files: %q must be a clean absolute path", at, f)
					} else if seen[f] {
						bad("%s: systemd.files: duplicate %q", at, f)
					}
					seen[f] = true
				}
			}
			needHost = true
		case KindDeployment:
			switch {
			case s.Deployment == nil:
				bad("%s: kind deployment needs a deployment block", at)
			case blocks > 1:
				bad("%s: kind deployment takes only the deployment block", at)
			default:
				if !k8sName.MatchString(s.Deployment.Namespace) {
					bad("%s: deployment.namespace: %q is not a Kubernetes name", at, s.Deployment.Namespace)
				}
				if !k8sSubdomain.MatchString(s.Deployment.Name) {
					bad("%s: deployment.name: %q is not a Kubernetes name", at, s.Deployment.Name)
				}
			}
			needKube = true
		case KindExternal:
			if blocks > 0 {
				bad("%s: kind external takes no compose, systemd or deployment block", at)
			}
			if s.Route == nil {
				bad("%s: kind external needs a route", at)
			}
		default:
			bad("%s: kind must be one of compose, systemd, deployment, external", at)
		}
		for _, side := range []string{"source", "target"} {
			pl, env := s.Source, p.Source
			if side == "target" {
				pl, env = s.Target, p.Target
			}
			switch {
			case needHost && pl.Host == "":
				bad("%s: %s.host: required for kind %s", at, side, s.Kind)
			case needHost:
				if _, ok := env.Hosts[pl.Host]; !ok {
					bad("%s: %s.host: %q is not in %s.hosts", at, side, pl.Host, side)
				}
			case pl.Host != "":
				bad("%s: %s.host: not used by kind %s", at, side, s.Kind)
			}
			if needKube && env.Kube == nil {
				bad("%s: kind deployment needs %s.kube", at, side)
			}
			switch {
			case s.Route == nil && pl.Endpoint != "":
				bad("%s: %s.endpoint: only used with a route", at, side)
			case s.Route != nil && pl.Endpoint == "":
				bad("%s: %s.endpoint: required when the service has a route", at, side)
			case s.Route != nil && !validEndpoint(pl.Endpoint):
				bad("%s: %s.endpoint: %q is not host:port", at, side, pl.Endpoint)
			}
		}
		if r := s.Route; r != nil {
			if !DNSName.MatchString(r.Host) {
				bad("%s: route.host: %q is not a lowercase DNS name", at, r.Host)
			}
			if !URLPath.MatchString(r.Prefix) {
				bad("%s: route.prefix: must start with / and hold only URL path characters", at)
			}
			switch {
			case r.HealthPath == "":
				bad("%s: route.health_path: required", at)
			case !URLPath.MatchString(r.HealthPath):
				bad("%s: route.health_path: must start with / and hold only URL path characters", at)
			}
			key := r.Host + r.Prefix
			if other, ok := routes[key]; ok {
				bad("%s: route %s is already used by %s", at, key, other)
			} else {
				routes[key] = s.Name
			}
			if s.Source.Endpoint != "" && s.Source.Endpoint == s.Target.Endpoint {
				bad("%s: source and target endpoints are the same", at)
			}
		}
		seenM := map[string]bool{}
		for _, m := range s.MirrorMethods {
			if !slices.Contains(SafeMethods, m) {
				bad("%s: mirror_methods: %q is not a safe method (GET, HEAD, OPTIONS)", at, m)
			} else if seenM[m] {
				bad("%s: mirror_methods: duplicate %q", at, m)
			}
			seenM[m] = true
		}
	}

	if len(p.RoutedServices()) > 0 {
		fd := p.FrontDoor
		if fd == nil {
			bad("frontdoor: required when a service has a route")
		} else {
			u, err := url.Parse(fd.API)
			if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Port() == "" || u.Hostname() == "" ||
				(u.Path != "" && u.Path != "/") || u.User != nil || u.RawQuery != "" {
				bad("frontdoor.api: must be an http:// or https:// URL with a host and port")
			}
			if err := fd.Token.Validate(); err != nil {
				bad("frontdoor.token: %v", err)
			}
			if len(fd.Addresses) == 0 {
				bad("frontdoor.addresses: list the IP addresses route hosts resolve to")
			}
			for _, a := range fd.Addresses {
				if _, err := netip.ParseAddr(a); err != nil {
					bad("frontdoor.addresses: %q is not an IP address", a)
				}
			}
		}
	}

	sh := p.Thresholds.Shift
	ordered := true
	for i, w := range sh.Steps {
		if w < 1 || w > 100 || (i > 0 && w <= sh.Steps[i-1]) {
			ordered = false
		}
	}
	if !ordered {
		bad("thresholds.shift.steps: must increase, each between 1 and 100")
	} else if len(sh.Steps) > 0 && sh.Steps[len(sh.Steps)-1] != 100 {
		bad("thresholds.shift.steps: must end at 100")
	}
	if sh.Hold <= 0 {
		bad("thresholds.shift.hold: must be positive")
	}
	if sh.SampleEvery <= 0 || sh.SampleEvery > sh.Hold {
		bad("thresholds.shift.sample_every: must be positive and at most hold")
	}
	if sh.MaxErrorDelta <= 0 || sh.MaxErrorDelta > 100 {
		bad("thresholds.shift.max_error_delta: must be between 0 and 100")
	}
	if sh.MaxP99 < 1 {
		bad("thresholds.shift.max_p99: must be at least 1")
	}
	if sh.MinRequests < 1 {
		bad("thresholds.shift.min_requests: must be at least 1")
	}
	if p.Thresholds.Rehearse.Window <= 0 {
		bad("thresholds.rehearse.window: must be positive")
	}
}
