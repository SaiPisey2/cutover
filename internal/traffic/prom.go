package traffic

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
)

// ParsePrometheus reads Envoy's /stats/prometheus output and returns the
// counters of every cluster it mentions, keyed by cluster name.
//
// Requests is upstream_rq_completed: every request Envoy finished for the
// cluster, once each however many attempts it took. upstream_rq_total is
// not used, as it leaves out requests that never got a connection: on
// Envoy 1.39 a target refusing every connection moved it by zero, which
// would leave a dead target unjudged.
// Errors counts each failed client-visible request once: 5xx responses,
// which include Envoy's own 503 for a failed connection (once per
// request, not per attempt) and 504 for an upstream timeout.
// upstream_rq_timeout is not added on top, as that would count a timeout
// twice.
// TestStatsCountEachFailureOnce (internal/frontdoor, integration) pins
// this against a real Envoy; if Envoy disagrees, change this formula,
// not that test.
func ParsePrometheus(r io.Reader) (map[string]Counters, error) {
	type acc struct {
		completed, xx5, count float64
		buckets               []Bucket
	}
	all := map[string]*acc{}
	get := func(c string) *acc {
		if all[c] == nil {
			all[c] = &acc{}
		}
		return all[c]
	}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, labels, value, err := parseLine(line)
		if err != nil {
			return nil, fmt.Errorf("stats line %d: %w", n, err)
		}
		cluster, ok := labels["envoy_cluster_name"]
		if !ok || !strings.HasPrefix(name, "envoy_cluster_") {
			continue
		}
		a := get(cluster)
		switch name {
		case "envoy_cluster_upstream_rq_completed":
			a.completed = value
		case "envoy_cluster_upstream_rq_xx":
			if labels["envoy_response_code_class"] == "5" {
				a.xx5 = value
			}
		case "envoy_cluster_upstream_rq_time_count":
			a.count = value
		case "envoy_cluster_upstream_rq_time_bucket":
			le := labels["le"]
			if le == "+Inf" {
				continue
			}
			f, err := strconv.ParseFloat(le, 64)
			if err != nil {
				return nil, fmt.Errorf("stats line %d: le %q: %w", n, le, err)
			}
			a.buckets = append(a.buckets, Bucket{LE: f, Count: uint64(value)})
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	out := map[string]Counters{}
	for c, a := range all {
		slices.SortFunc(a.buckets, func(x, y Bucket) int {
			switch {
			case x.LE < y.LE:
				return -1
			case x.LE > y.LE:
				return 1
			}
			return 0
		})
		out[c] = Counters{Requests: uint64(a.completed), Errors: uint64(a.xx5), LatencyCount: uint64(a.count), Buckets: a.buckets}
	}
	return out, nil
}

// parseLine splits `name{k="v",...} value` (labels optional).
func parseLine(line string) (string, map[string]string, float64, error) {
	labels := map[string]string{}
	var name, rest string
	if i := strings.IndexByte(line, '{'); i >= 0 {
		name = line[:i]
		j := i + 1
		for {
			for j < len(line) && (line[j] == ',' || line[j] == ' ') {
				j++
			}
			if j >= len(line) {
				return "", nil, 0, fmt.Errorf("unterminated labels")
			}
			if line[j] == '}' {
				rest = line[j+1:]
				break
			}
			eq := strings.IndexByte(line[j:], '=')
			if eq < 0 || j+eq+1 >= len(line) || line[j+eq+1] != '"' {
				return "", nil, 0, fmt.Errorf("malformed label")
			}
			key := line[j : j+eq]
			k := j + eq + 2
			var val strings.Builder
			for ; k < len(line) && line[k] != '"'; k++ {
				if line[k] == '\\' && k+1 < len(line) {
					k++
				}
				val.WriteByte(line[k])
			}
			if k >= len(line) {
				return "", nil, 0, fmt.Errorf("unterminated label value")
			}
			labels[key] = val.String()
			j = k + 1
		}
	} else {
		sp := strings.IndexByte(line, ' ')
		if sp < 0 {
			return "", nil, 0, fmt.Errorf("no value")
		}
		name, rest = line[:sp], line[sp:]
	}
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return "", nil, 0, fmt.Errorf("no value")
	}
	v, err := strconv.ParseFloat(fields[0], 64)
	if err != nil || math.IsNaN(v) {
		return "", nil, 0, fmt.Errorf("value %q is not a number", fields[0])
	}
	return name, labels, v, nil
}
