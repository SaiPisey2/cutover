package traffic

import (
	"math"
	"strings"
	"testing"
)

func buckets(counts ...uint64) []Bucket {
	les := []float64{5, 10, 25, 50, 100, 250, 500, 1000}
	out := make([]Bucket, len(counts))
	for i, c := range counts {
		out[i] = Bucket{LE: les[i], Count: c}
	}
	return out
}

func TestSinceAndReset(t *testing.T) {
	prev := Counters{Requests: 100, Errors: 2, LatencyCount: 100, Buckets: buckets(10, 50, 90, 100, 100, 100, 100, 100)}
	cur := Counters{Requests: 300, Errors: 5, LatencyCount: 300, Buckets: buckets(20, 100, 250, 290, 300, 300, 300, 300)}
	d := cur.Since(prev)
	if d.Requests != 200 || d.Errors != 3 || d.LatencyCount != 200 || d.Buckets[2].Count != 160 {
		t.Fatalf("delta %+v", d)
	}
	// A counter that went backwards means Envoy restarted: the current
	// value is everything since the restart.
	reset := Counters{Requests: 40, Errors: 1, LatencyCount: 40, Buckets: buckets(5, 20, 40, 40, 40, 40, 40, 40)}
	if got := reset.Since(cur); got.Requests != 40 || got.Errors != 1 {
		t.Fatalf("after a reset: %+v", got)
	}
}

func TestSummary(t *testing.T) {
	c := Counters{Requests: 1000, Errors: 7, LatencyCount: 1000, Buckets: buckets(100, 400, 800, 950, 985, 991, 1000, 1000)}
	s := c.Summary()
	if s.ErrorRate != 0.7 {
		t.Errorf("error rate %v", s.ErrorRate)
	}
	// 99% of 1000 = 990; the first bucket holding 990 is le=250.
	if !s.P99Known || s.P99Ms != 250 || s.P99Over {
		t.Errorf("p99 %+v", s)
	}
	over := Counters{Requests: 100, LatencyCount: 100, Buckets: buckets(10, 20, 30, 40, 50, 60, 70, 80)}
	if s := over.Summary(); !s.P99Known || !s.P99Over || s.P99Ms != 1000 {
		t.Errorf("above every bucket: %+v", s)
	}
	if s := (Counters{}).Summary(); s.P99Known || s.ErrorRate != 0 {
		t.Errorf("empty: %+v", s)
	}
}

func lim() Limits { return Limits{MaxErrorDelta: 0.5, MaxP99: 2, MinRequests: 50} }

func TestJudgeNeedsMinRequests(t *testing.T) {
	// One failure out of one request is not evidence.
	v := Judge(0, 20, Summary{Requests: 1, Errors: 1, ErrorRate: 100}, lim())
	if v.Judged || v.Tripped {
		t.Fatalf("%+v", v)
	}
}

func TestJudgeErrorRate(t *testing.T) {
	v := Judge(0.2, 20, Summary{Requests: 200, Errors: 2, ErrorRate: 1.0, P99Known: true, P99Ms: 25}, lim())
	if !v.Judged || !v.Tripped || !strings.Contains(v.Reason, "error rate 1.00% vs 0.20%") {
		t.Fatalf("%+v", v)
	}
	v = Judge(0.2, 20, Summary{Requests: 200, Errors: 1, ErrorRate: 0.5, P99Known: true, P99Ms: 25}, lim())
	if !v.Judged || v.Tripped {
		t.Fatalf("0.3 points over is within 0.5: %+v", v)
	}
}

func TestJudgeP99(t *testing.T) {
	v := Judge(0, 20, Summary{Requests: 200, P99Known: true, P99Ms: 50}, lim())
	if !v.Tripped || !strings.Contains(v.Reason, "p99 50ms vs reference 20ms") {
		t.Fatalf("%+v", v)
	}
	if v := Judge(0, 20, Summary{Requests: 200, P99Known: true, P99Ms: 40}, lim()); v.Tripped {
		t.Fatalf("exactly 2x does not exceed 2x: %+v", v)
	}
	if v := Judge(0, 20, Summary{Requests: 200, P99Known: true, P99Over: true, P99Ms: 1000}, lim()); !v.Tripped {
		t.Fatalf("above every bucket trips: %+v", v)
	}
	// No reference p99: latency is not judged, errors still are.
	if v := Judge(0, 0, Summary{Requests: 200, P99Known: true, P99Ms: 900}, lim()); !v.Judged || v.Tripped {
		t.Fatalf("%+v", v)
	}
}

const promText = `# TYPE envoy_cluster_upstream_rq_total counter
envoy_cluster_upstream_rq_total{envoy_cluster_name="c_shop_api_source"} 120
envoy_cluster_upstream_rq_total{envoy_cluster_name="c_shop_api_target"} 30
envoy_cluster_upstream_rq_xx{envoy_response_code_class="2",envoy_cluster_name="c_shop_api_target"} 26
envoy_cluster_upstream_rq_xx{envoy_response_code_class="5",envoy_cluster_name="c_shop_api_target"} 4
envoy_cluster_upstream_rq_timeout{envoy_cluster_name="c_shop_api_target"} 1
envoy_cluster_upstream_rq_time_bucket{envoy_cluster_name="c_shop_api_target",le="5"} 10
envoy_cluster_upstream_rq_time_bucket{envoy_cluster_name="c_shop_api_target",le="10"} 25
envoy_cluster_upstream_rq_time_bucket{envoy_cluster_name="c_shop_api_target",le="+Inf"} 30
envoy_cluster_upstream_rq_time_count{envoy_cluster_name="c_shop_api_target"} 30
envoy_cluster_upstream_rq_time_sum{envoy_cluster_name="c_shop_api_target"} 211.5
envoy_cluster_upstream_cx_active{envoy_cluster_name="xds"} 1
envoy_server_live 1
`

func TestParsePrometheus(t *testing.T) {
	got, err := ParsePrometheus(strings.NewReader(promText))
	if err != nil {
		t.Fatal(err)
	}
	tg := got["c_shop_api_target"]
	if tg.Requests != 30 || tg.Errors != 4 || tg.LatencyCount != 30 || len(tg.Buckets) != 2 || tg.Buckets[1] != (Bucket{LE: 10, Count: 25}) {
		t.Fatalf("target %+v", tg)
	}
	if got["c_shop_api_source"].Requests != 120 {
		t.Fatalf("source %+v", got["c_shop_api_source"])
	}
	if _, ok := got["xds"]; !ok {
		t.Fatal("every cluster is reported, even with no request stats")
	}
}

func TestParsePrometheusRejectsGarbage(t *testing.T) {
	if _, err := ParsePrometheus(strings.NewReader("envoy_cluster_upstream_rq_total{envoy_cluster_name=\"x\"} notanumber\n")); err == nil {
		t.Fatal("want an error")
	}
	if _, err := ParsePrometheus(strings.NewReader("envoy_cluster_upstream_rq_total{envoy_cluster_name=\"x} 1\n")); err == nil {
		t.Fatal("want an error for an unterminated label")
	}
}

func TestSummaryJSONSafe(t *testing.T) {
	// Summaries travel through workflow history as JSON: no NaN or Inf.
	s := Counters{Requests: 10, LatencyCount: 10, Buckets: buckets(0, 0, 0, 0, 0, 0, 0, 0)}.Summary()
	if math.IsInf(s.P99Ms, 0) || math.IsNaN(s.P99Ms) || math.IsNaN(s.ErrorRate) {
		t.Fatalf("%+v", s)
	}
}
