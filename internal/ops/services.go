package ops

import (
	"context"
	"errors"
	"slices"

	"github.com/SaiPisey2/cutover/internal/frontdoor"
	"github.com/SaiPisey2/cutover/internal/plan"
	"github.com/SaiPisey2/cutover/internal/readiness"
	"github.com/SaiPisey2/cutover/internal/redact"
	"github.com/SaiPisey2/cutover/internal/svc"
)

type ServiceIn struct {
	Plan    plan.Plan `json:"plan"`
	Service string    `json:"service"`
}

type ServicesIn struct {
	Plan plan.Plan `json:"plan"`
}

type Health struct {
	Healthy bool   `json:"healthy"`
	Detail  string `json:"detail"`
}

func service(p plan.Plan, name string) (plan.Service, error) {
	s, ok := p.Service(name)
	if !ok {
		return plan.Service{}, permanentf("the plan has no service %q", name)
	}
	return s, nil
}

// svcErr marks a refused Prepare permanent; the rest retry.
func svcErr(err error) error {
	if errors.Is(err, svc.ErrBlocked) {
		return permanentf("%s", err.Error())
	}
	return err
}

// servicesInput fills the readiness inputs for services: what each
// driver found, the front door's routes, and what route hosts resolve to.
// Failures become findings, never a failed Discover.
func (a *Activities) servicesInput(ctx context.Context, p plan.Plan, ri *readiness.Input) {
	src, closeSrc := side(p, false)
	defer closeSrc()
	tgt, closeTgt := side(p, true)
	defer closeTgt()
	ri.ServiceErrs = map[string]string{}
	for _, s := range p.Services {
		d, err := a.driverFor(p, s)
		if err == nil {
			var f svc.Facts
			if f, err = d.Inspect(ctx, src, tgt); err == nil {
				ri.Services = append(ri.Services, f)
				continue
			}
		}
		ri.ServiceErrs[s.Name] = redact.DSN(err.Error())
	}
	if len(p.RoutedServices()) == 0 {
		return
	}
	if c, err := FrontDoorClient(p); err != nil {
		ri.RoutesErr = redact.DSN(err.Error())
	} else if ri.Routes, err = c.Routes(ctx); err != nil {
		ri.RoutesErr = redact.DSN(err.Error())
	}
	ri.DNS, ri.DNSErr = map[string][]string{}, map[string]string{}
	for _, s := range p.RoutedServices() {
		h := s.Route.Host
		if _, done := ri.DNS[h]; done {
			continue
		}
		if _, done := ri.DNSErr[h]; done {
			continue
		}
		addrs, err := a.lookup(ctx, h)
		if err != nil {
			ri.DNSErr[h] = err.Error()
			continue
		}
		slices.Sort(addrs)
		ri.DNS[h] = addrs
	}
}

// PrepareService creates the service's target copy (idempotent).
func (a *Activities) PrepareService(ctx context.Context, in ServiceIn) (err error) {
	defer func() { err = fail(err) }()
	s, err := service(in.Plan, in.Service)
	if err != nil {
		return err
	}
	d, err := a.driverFor(in.Plan, s)
	if err != nil {
		return permanentf("%s", err.Error())
	}
	src, closeSrc := side(in.Plan, false)
	defer closeSrc()
	tgt, closeTgt := side(in.Plan, true)
	defer closeTgt()
	return heartbeat(ctx, func() error { return svcErr(d.Prepare(ctx, src, tgt)) })
}

// UndoService removes what PrepareService created on the target.
func (a *Activities) UndoService(ctx context.Context, in ServiceIn) (err error) {
	defer func() { err = fail(err) }()
	s, err := service(in.Plan, in.Service)
	if err != nil {
		return err
	}
	d, err := a.driverFor(in.Plan, s)
	if err != nil {
		return permanentf("%s", err.Error())
	}
	tgt, closeTgt := side(in.Plan, true)
	defer closeTgt()
	return heartbeat(ctx, func() error { return d.Undo(ctx, tgt) })
}

// CheckHealth is one look at the target copy: its driver's view, and for
// a routed service the front door's active health check too.
func (a *Activities) CheckHealth(ctx context.Context, in ServiceIn) (_ Health, err error) {
	defer func() { err = fail(err) }()
	s, err := service(in.Plan, in.Service)
	if err != nil {
		return Health{}, err
	}
	d, err := a.driverFor(in.Plan, s)
	if err != nil {
		return Health{}, permanentf("%s", err.Error())
	}
	tgt, closeTgt := side(in.Plan, true)
	defer closeTgt()
	ok, detail, err := d.Healthy(ctx, tgt)
	if err != nil {
		return Health{}, err
	}
	if !ok || !s.Routed() {
		return Health{Healthy: ok, Detail: detail}, nil
	}
	c, err := FrontDoorClient(in.Plan)
	if err != nil {
		return Health{}, err
	}
	h, err := c.Health(ctx, in.Plan.Name, s.Name)
	if err != nil {
		return Health{}, frontDoorErr(err)
	}
	detail = "front door: target " + h.Target
	return Health{Healthy: h.Target == frontdoor.Healthy, Detail: detail}, nil
}
