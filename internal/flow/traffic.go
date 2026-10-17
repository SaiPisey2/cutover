package flow

import (
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/SaiPisey2/cutover/internal/frontdoor"
	"github.com/SaiPisey2/cutover/internal/ops"
	"github.com/SaiPisey2/cutover/internal/plan"
	"github.com/SaiPisey2/cutover/internal/traffic"
)

const (
	healthEvery   = 5 * time.Second
	healthTimeout = 10 * time.Minute
)

func (r *runner) setTraffic(service string, d frontdoor.Desired) error {
	return r.execWith(5*time.Minute, 30*time.Second, acts.SetTraffic, ops.TrafficIn{Plan: r.in.Plan, Service: service, Desired: d}, nil)
}

// setAll applies d to every service in ss, in plan order.
func (r *runner) setAll(ss []plan.Service, d frontdoor.Desired) error {
	for _, s := range ss {
		if err := r.setTraffic(s.Name, d); err != nil {
			return err
		}
	}
	return nil
}

func (r *runner) setWeight(w int) error {
	if err := r.setAll(r.in.Plan.RoutedServices(), frontdoor.Desired{Source: true, Target: true, Weight: w}); err != nil {
		return err
	}
	r.st.Weight = w
	return nil
}

func mirrored(p plan.Plan) []plan.Service {
	var out []plan.Service
	for _, s := range p.RoutedServices() {
		if len(s.MirrorMethods) > 0 {
			out = append(out, s)
		}
	}
	return out
}

func (r *runner) prepareSummary() string {
	if n := len(r.in.Plan.Services); n > 0 {
		return fmt.Sprintf("Target schema created and %d services running on the target, receiving no traffic. Approve to start replication.", n)
	}
	return "Target schema created. Approve to start replication."
}

// prepareServices creates every service's target copy, adds each routed
// one to the front door at weight 0, and waits for all of them to be
// healthy. Its undo removes them from the front door, then deletes them.
func (r *runner) prepareServices() error {
	p := r.in.Plan
	if len(p.Services) == 0 {
		return nil
	}
	r.pushUndo(func() error {
		routed := p.RoutedServices()
		for i := len(routed) - 1; i >= 0; i-- {
			if err := r.setTraffic(routed[i].Name, frontdoor.Desired{Source: true}); err != nil {
				return err
			}
		}
		for i := len(p.Services) - 1; i >= 0; i-- {
			if err := r.exec(30*time.Minute, acts.UndoService, ops.ServiceIn{Plan: p, Service: p.Services[i].Name}, nil); err != nil {
				return err
			}
		}
		return nil
	})
	for _, s := range p.Services {
		in := ops.ServiceIn{Plan: p, Service: s.Name}
		if err := r.step("prepare-"+s.Name, func() error { return r.execWith(30*time.Minute, time.Minute, acts.PrepareService, in, nil) }); err != nil {
			return err
		}
	}
	for _, s := range p.RoutedServices() {
		if err := r.step("route-"+s.Name, func() error {
			return r.setTraffic(s.Name, frontdoor.Desired{Source: true, Target: true})
		}); err != nil {
			return err
		}
	}
	for _, s := range p.Services {
		if err := r.step("health-"+s.Name, func() error { return r.waitHealthy(s.Name) }); err != nil {
			return err
		}
	}
	return nil
}

func (r *runner) waitHealthy(service string) error {
	deadline := workflow.Now(r.ctx).Add(healthTimeout)
	in := ops.ServiceIn{Plan: r.in.Plan, Service: service}
	for {
		var h ops.Health
		if err := r.exec(time.Minute, acts.CheckHealth, in, &h); err != nil {
			return err
		}
		if h.Healthy {
			r.event("healthy", "", service+": "+h.Detail)
			return nil
		}
		if !workflow.Now(r.ctx).Before(deadline) {
			return fmt.Errorf("the target copy of %s is not healthy after %s: %s", service, healthTimeout, h.Detail)
		}
		if err := r.sleep(healthEvery); err != nil {
			return err
		}
	}
}

func (r *runner) sample(out *[]ops.ServiceCounters) error {
	return r.exec(time.Minute, acts.SampleTraffic, ops.ServicesIn{Plan: r.in.Plan}, out)
}

// window summarizes each service between two samples.
func window(before, after []ops.ServiceCounters) []ServiceTraffic {
	prev := map[string]ops.ServiceCounters{}
	for _, c := range before {
		prev[c.Service] = c
	}
	since := func(cur, old *traffic.Counters) traffic.Summary {
		if cur == nil {
			return traffic.Summary{}
		}
		if old == nil {
			return cur.Summary()
		}
		return cur.Since(*old).Summary()
	}
	var out []ServiceTraffic
	for _, c := range after {
		p := prev[c.Service]
		out = append(out, ServiceTraffic{Service: c.Service, Source: since(c.Source, p.Source), Target: since(c.Target, p.Target)})
	}
	return out
}

// Describe is one window's traffic for one copy, in a sentence fragment.
func Describe(s traffic.Summary) string {
	p99 := "no latency data"
	if s.P99Known {
		p99 = fmt.Sprintf("p99 %gms", s.P99Ms)
		if s.P99Over {
			p99 += "+"
		}
	}
	return fmt.Sprintf("%d requests, %.2f%% errors, %s", s.Requests, s.ErrorRate, p99)
}

func trafficSummary(ts []ServiceTraffic) string {
	var b strings.Builder
	for _, t := range ts {
		fmt.Fprintf(&b, " %s: source %s; target %s.", t.Service, Describe(t.Source), Describe(t.Target))
	}
	return b.String()
}

// rehearseTraffic mirrors safe requests for rehearse.window and returns
// the evidence for the Rehearse gate.
func (r *runner) rehearseTraffic() (string, error) {
	p := r.in.Plan
	if len(p.RoutedServices()) == 0 {
		return "", nil
	}
	if m := mirrored(p); len(m) > 0 {
		r.pushUndo(func() error { return r.setAll(m, frontdoor.Desired{Source: true, Target: true}) })
		if err := r.step("mirror", func() error { return r.setAll(m, frontdoor.Desired{Source: true, Target: true, Mirror: true}) }); err != nil {
			return "", err
		}
	}
	var before, after []ops.ServiceCounters
	if err := r.step("sample", func() error { return r.sample(&before) }); err != nil {
		return "", err
	}
	if err := r.sleep(p.Thresholds.Rehearse.Window.D()); err != nil {
		return "", err
	}
	if err := r.step("sample", func() error { return r.sample(&after) }); err != nil {
		return "", err
	}
	r.st.Traffic = window(before, after)
	r.baseline = map[string]ServiceTraffic{}
	for _, t := range r.st.Traffic {
		r.baseline[t.Service] = t
	}
	s := trafficSummary(r.st.Traffic)
	r.event("traffic", "", strings.TrimSpace(s))
	return s, nil
}

func (r *runner) stopMirror() error {
	m := mirrored(r.in.Plan)
	if len(m) == 0 {
		return nil
	}
	return r.step("stop-mirror", func() error { return r.setAll(m, frontdoor.Desired{Source: true, Target: true}) })
}

// shift runs the weighted ramp: each step moves traffic, holds it for
// shift.hold, and either trips back to the source, asks again (too little
// traffic to judge), or moves on.
func (r *runner) shift() error {
	p := r.in.Plan
	if len(p.RoutedServices()) == 0 {
		r.event("skipped", "", "no routed services in this plan")
		return nil
	}
	sh := p.Thresholds.Shift
	r.pushUndo(func() error { return r.setWeight(0) })
	ask := true
	for i := 0; i < len(sh.Steps); i++ {
		w := sh.Steps[i]
		if ask {
			msg := fmt.Sprintf("Move %d%% of traffic to the target, then watch it for %s.", w, sh.Hold.D())
			if sh.Preapprove {
				msg += fmt.Sprintf(" Approving runs the rest of the ramp %v without asking, unless a step lacks the traffic to judge.", sh.Steps[i:])
			}
			if err := r.gate(fmt.Sprintf("shift-%d", w), msg+trafficSummary(r.st.Traffic), 1); err != nil {
				return err
			}
		}
		ask = !sh.Preapprove
		if err := r.step(fmt.Sprintf("weight-%d", w), func() error { return r.setWeight(w) }); err != nil {
			return err
		}
		v, err := r.hold()
		if err != nil {
			return err
		}
		switch {
		case v.Judged && !v.Tripped:
			r.st.ShiftVerdict = "judged ok"
		case !v.Judged:
			r.st.ShiftVerdict = "unjudged - " + v.Reason
		}
		switch {
		case v.Tripped:
			if err := r.must(func() error { return r.setWeight(0) }); err != nil {
				return err
			}
			r.event("shift-tripped", "", v.Reason)
			if err := r.gate("shift-tripped", fmt.Sprintf("%s. Traffic is back on the source. Approve to start the ramp again at %d%%; reject to roll back.",
				v.Reason, sh.Steps[0]), 1); err != nil {
				return err
			}
			i, ask = -1, false
		case !v.Judged:
			r.event("shift-unjudged", "", v.Reason)
			ask = true
		}
	}
	return nil
}

// hold watches the current step for shift.hold and returns the verdict
// of the last window judged, or the first trip.
func (r *runner) hold() (traffic.Verdict, error) {
	sh := r.in.Plan.Thresholds.Shift
	blind := func(err error) traffic.Verdict {
		return traffic.Verdict{Judged: true, Tripped: true, Reason: "cannot read front-door statistics: " + message(err)}
	}
	var start []ops.ServiceCounters
	if err := r.sample(&start); err != nil {
		if r.ctx.Err() != nil {
			return traffic.Verdict{}, err
		}
		return blind(err), nil
	}
	end := workflow.Now(r.ctx).Add(sh.Hold.D())
	last := traffic.Verdict{Reason: "no sample yet"}
	for {
		left := end.Sub(workflow.Now(r.ctx))
		if left <= 0 {
			return last, nil
		}
		if err := r.sleep(min(sh.SampleEvery.D(), left)); err != nil {
			return traffic.Verdict{}, err
		}
		var cur []ops.ServiceCounters
		if err := r.sample(&cur); err != nil {
			if r.ctx.Err() != nil {
				return traffic.Verdict{}, err
			}
			return blind(err), nil
		}
		ts, v := r.judge(window(start, cur))
		r.st.Traffic = ts
		if v.Tripped {
			return v, nil
		}
		last = v
	}
}

// judge applies the tripwire to every service in a window . The reference p99 is the highest of this window's source p99 (only if
// the source served min_requests), the baseline source p99 and the baseline
// target p99 (only if it served min_requests); a window whose reference
// has no p99 cannot judge latency and is unjudged, as is any service with
// fewer than min_requests.
func (r *runner) judge(ts []ServiceTraffic) ([]ServiceTraffic, traffic.Verdict) {
	sh := r.in.Plan.Thresholds.Shift
	lim := traffic.Limits{MaxErrorDelta: sh.MaxErrorDelta, MaxP99: sh.MaxP99, MinRequests: uint64(sh.MinRequests)}
	all := traffic.Verdict{Judged: true}
	var unjudged []string
	for i, t := range ts {
		base := r.baseline[t.Service]
		refRate := base.Source.ErrorRate
		if t.Source.Requests >= lim.MinRequests {
			refRate = t.Source.ErrorRate
		}
		refP99 := 0.0
		if t.Source.Requests >= lim.MinRequests && t.Source.P99Known {
			refP99 = t.Source.P99Ms
		}
		if base.Source.P99Known {
			refP99 = max(refP99, base.Source.P99Ms)
		}
		if base.Target.P99Known && base.Target.Requests >= lim.MinRequests {
			refP99 = max(refP99, base.Target.P99Ms)
		}
		v := traffic.Judge(refRate, refP99, t.Target, lim)
		if v.Judged && !v.Tripped && refP99 == 0 && t.Target.P99Known {
			// No reference to hold the target's latency against: passing
			// on error rate alone is not a judgement.
			v = traffic.Verdict{Reason: "no reference p99 (the source has no latency data), so the target's latency cannot be judged"}
		}
		v.Reason = t.Service + ": " + v.Reason
		ts[i].Verdict = &v
		if v.Tripped && !all.Tripped {
			all = v
		}
		if !v.Judged {
			unjudged = append(unjudged, v.Reason)
		}
	}
	if !all.Tripped && len(unjudged) > 0 {
		all = traffic.Verdict{Reason: strings.Join(unjudged, "; ")}
	}
	return ts, all
}

// finishRoutes leaves only the target in every routed service's route.
func (r *runner) finishRoutes() error {
	return r.setAll(r.in.Plan.RoutedServices(), frontdoor.Desired{Target: true, Weight: 100})
}
