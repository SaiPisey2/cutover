// Package traffic is the arithmetic behind Rehearse and Shift: counter
// deltas, error rates, p99 from histogram buckets, and the tripwire. It is
// pure (no I/O, no clock), so workflows may call it.
package traffic

import (
	"fmt"
	"math"
)

// Bucket is one cumulative latency bucket: Count requests took at most LE ms.
type Bucket struct {
	LE    float64 `json:"le"`
	Count uint64  `json:"count"`
}

// Counters are one Envoy cluster's cumulative counters. Buckets hold only
// finite bounds, ascending; requests above the last bound are
// LatencyCount minus the last bucket's Count.
type Counters struct {
	Requests     uint64   `json:"requests"`
	Errors       uint64   `json:"errors"`
	LatencyCount uint64   `json:"latency_count"`
	Buckets      []Bucket `json:"buckets,omitempty"`
}

type Summary struct {
	Requests  uint64  `json:"requests"`
	Errors    uint64  `json:"errors"`
	ErrorRate float64 `json:"error_rate"` // percent
	P99Ms     float64 `json:"p99_ms"`
	// P99Known is false when no request was timed. P99Over means p99 lies
	// above the last bucket; P99Ms is then that bucket's bound.
	P99Known bool `json:"p99_known"`
	P99Over  bool `json:"p99_over,omitempty"`
}

type Limits struct {
	MaxErrorDelta float64 // percentage points
	MaxP99        float64 // factor of the reference p99
	MinRequests   uint64
}

type Verdict struct {
	Judged  bool   `json:"judged"`
	Tripped bool   `json:"tripped"`
	Reason  string `json:"reason,omitempty"`
}

// Since returns the counts accumulated after prev. If any counter went
// backwards Envoy restarted, and c itself is the count since then.
func (c Counters) Since(prev Counters) Counters {
	if c.Requests < prev.Requests || c.Errors < prev.Errors || c.LatencyCount < prev.LatencyCount || len(c.Buckets) != len(prev.Buckets) {
		return c
	}
	out := Counters{Requests: c.Requests - prev.Requests, Errors: c.Errors - prev.Errors, LatencyCount: c.LatencyCount - prev.LatencyCount}
	for i, b := range c.Buckets {
		if b.LE != prev.Buckets[i].LE || b.Count < prev.Buckets[i].Count {
			return c
		}
		out.Buckets = append(out.Buckets, Bucket{LE: b.LE, Count: b.Count - prev.Buckets[i].Count})
	}
	return out
}

func (c Counters) Summary() Summary {
	s := Summary{Requests: c.Requests, Errors: c.Errors}
	if c.Requests > 0 {
		s.ErrorRate = math.Round(float64(c.Errors)*100/float64(c.Requests)*10000) / 10000
	}
	if c.LatencyCount == 0 || len(c.Buckets) == 0 {
		return s
	}
	s.P99Known = true
	want := uint64(math.Ceil(0.99 * float64(c.LatencyCount)))
	for _, b := range c.Buckets {
		if b.Count >= want {
			s.P99Ms = b.LE
			return s
		}
	}
	s.P99Ms, s.P99Over = c.Buckets[len(c.Buckets)-1].LE, true
	return s
}

// Judge compares a target window against the reference: refRate is the
// source error rate (percent), refP99 the reference p99 in ms (0 = none,
// latency then goes unjudged). A target with fewer than MinRequests
// requests is not judged at all.
func Judge(refRate, refP99 float64, t Summary, l Limits) Verdict {
	if t.Requests < l.MinRequests {
		return Verdict{Reason: fmt.Sprintf("%d target requests, fewer than the %d needed to judge", t.Requests, l.MinRequests)}
	}
	if t.ErrorRate-refRate > l.MaxErrorDelta {
		return Verdict{Judged: true, Tripped: true, Reason: fmt.Sprintf(
			"target error rate %.2f%% vs %.2f%% on the source, more than %.2f points worse", t.ErrorRate, refRate, l.MaxErrorDelta)}
	}
	if refP99 > 0 && t.P99Known && (t.P99Over || t.P99Ms > l.MaxP99*refP99) {
		over := ""
		if t.P99Over {
			over = " or more"
		}
		return Verdict{Judged: true, Tripped: true, Reason: fmt.Sprintf(
			"target p99 %gms%s vs reference %gms, more than %gx", t.P99Ms, over, refP99, l.MaxP99)}
	}
	return Verdict{Judged: true}
}
