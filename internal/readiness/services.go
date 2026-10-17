package readiness

import (
	"net/netip"
	"slices"
	"strings"

	"github.com/SaiPisey2/cutover/internal/frontdoor"
)

type addFunc func(s Severity, code, subject, format string, a ...any)

func assessServices(in Input, add addFunc) {
	p := in.Plan
	for _, s := range p.Services {
		if e, ok := in.ServiceErrs[s.Name]; ok {
			add(Blocker, "service-unreachable", s.Name, "could not inspect %s: %s", s.Name, e)
		}
		for _, f := range in.Services {
			if f.Service != s.Name {
				continue
			}
			for _, sf := range f.Findings {
				sev := Warning
				if sf.Blocker {
					sev = Blocker
				}
				add(sev, sf.Code, sf.Subject, "%s", sf.Message)
			}
		}
		if !s.Routed() {
			add(Warning, "service-unrouted", s.Name, "%s has no route: from Prepare on, its target copy runs alongside the source copy until you stop the source copy", s.Name)
		}
	}

	routed := p.RoutedServices()
	if len(routed) == 0 {
		return
	}
	if in.RoutesErr != "" {
		add(Blocker, "frontdoor-unreachable", "frontdoor", "cannot read the front door's routes: %s", in.RoutesErr)
	} else {
		for _, s := range routed {
			key := p.Name + "/" + s.Name
			i := slices.IndexFunc(in.Routes, func(r frontdoor.Route) bool { return r.Key() == key })
			if i < 0 {
				add(Blocker, "route-not-registered", s.Name, "the front door has no route for %s; register the plan's routes first: cutover route apply --plan <file>", s.Name)
				continue
			}
			r := in.Routes[i]
			want, err := frontdoor.ParseUpstream(s.Source.Endpoint)
			switch {
			case r.Host != s.Route.Host || prefixKey(r.Prefix) != prefixKey(s.Route.Prefix) || r.HealthPath != s.Route.HealthPath:
				add(Blocker, "route-mismatch", s.Name, "the front door routes %s%s (health %s) for %s, the plan says %s%s (health %s); run cutover route apply --plan <file>",
					r.Host, r.Prefix, r.HealthPath, s.Name, s.Route.Host, s.Route.Prefix, s.Route.HealthPath)
			case err != nil || r.Source == nil || *r.Source != want:
				add(Blocker, "route-mismatch", s.Name, "the front door sends %s to %s, the plan's source endpoint is %s", s.Name, upstreamString(r.Source), s.Source.Endpoint)
			}
			if r.Target != nil || r.TargetWeight != 0 || r.Mirror {
				add(Blocker, "route-busy", s.Name, "the front door already sends %s traffic to a target (a migration in progress, or one left over); finish or roll it back first", s.Name)
			}
		}
	}
	for _, s := range routed {
		if len(s.MirrorMethods) == 0 {
			add(Info, "mirror-off", s.Name, "mirroring is off for %s (mirror_methods: []); Rehearse shows no target traffic for it", s.Name)
		}
	}

	var front, listed []string
	if p.FrontDoor != nil {
		listed = p.FrontDoor.Addresses
	}
	var frontIPs []netip.Addr
	for _, a := range listed {
		if ip, err := netip.ParseAddr(a); err == nil {
			frontIPs = append(frontIPs, ip.Unmap())
		}
	}
	front = listed
	seen := map[string]bool{}
	for _, s := range routed {
		h := s.Route.Host
		if seen[h] {
			continue
		}
		seen[h] = true
		if e, ok := in.DNSErr[h]; ok {
			add(Blocker, "dns-lookup", h, "%s does not resolve: %s", h, e)
			continue
		}
		addrs := in.DNS[h]
		var stray bool
		for _, a := range addrs {
			ip, err := netip.ParseAddr(a)
			if err != nil || !slices.Contains(frontIPs, ip.Unmap()) {
				stray = true
			}
		}
		if len(addrs) == 0 || stray {
			add(Blocker, "dns-not-front-door", h, "%s resolves to %s, not only to the front door (%s); point its DNS at the front door first",
				h, orNone(addrs), strings.Join(front, ", "))
		}
	}
}

// prefixKey folds "/x/" into "/x": the front door treats them as one prefix.
func prefixKey(p string) string {
	if p == "/" {
		return p
	}
	return strings.TrimRight(p, "/")
}

func upstreamString(u *frontdoor.Upstream) string {
	if u == nil {
		return "nothing"
	}
	return u.String()
}

func orNone(a []string) string {
	if len(a) == 0 {
		return "no addresses"
	}
	return strings.Join(a, ", ")
}
