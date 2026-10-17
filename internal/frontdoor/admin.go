package frontdoor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/SaiPisey2/cutover/internal/traffic"
)

const (
	Healthy   = "healthy"
	Unhealthy = "unhealthy"
	Pending   = "pending"
	Absent    = "absent"
)

// Admin reads Envoy's admin interface (bound to 127.0.0.1 by the
// bootstrap): per-cluster counters and active health-check results.
type Admin struct {
	Base string
	HTTP *http.Client
}

func (a *Admin) get(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.Base+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := a.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("envoy admin: %w", err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, fmt.Errorf("envoy admin: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("envoy admin %s: HTTP %d", path, resp.StatusCode)
	}
	return b, nil
}

// Stats returns the counters of cutover's clusters (names starting c_).
func (a *Admin) Stats(ctx context.Context) (map[string]traffic.Counters, error) {
	b, err := a.get(ctx, "/stats/prometheus?filter="+url.QueryEscape(`^cluster\.c_`))
	if err != nil {
		return nil, err
	}
	return traffic.ParsePrometheus(bytesReader(b))
}

type clustersDoc struct {
	ClusterStatuses []struct {
		Name         string `json:"name"`
		HostStatuses []struct {
			HealthStatus struct {
				EdsHealthStatus         string `json:"eds_health_status"`
				FailedActiveHealthCheck bool   `json:"failed_active_health_check"`
				PendingActiveHC         bool   `json:"pending_active_hc"`
			} `json:"health_status"`
		} `json:"host_statuses"`
	} `json:"cluster_statuses"`
}

// Health is healthy when any host of the cluster passes its active
// health check, pending while the first check has not finished, absent
// when Envoy has no such cluster, and unhealthy otherwise.
func (a *Admin) Health(ctx context.Context, cluster string) (string, error) {
	b, err := a.get(ctx, "/clusters?format=json")
	if err != nil {
		return "", err
	}
	var doc clustersDoc
	if err := json.Unmarshal(b, &doc); err != nil {
		return "", fmt.Errorf("envoy admin /clusters: %w", err)
	}
	for _, c := range doc.ClusterStatuses {
		if c.Name != cluster {
			continue
		}
		state := Unhealthy
		for _, h := range c.HostStatuses {
			hs := h.HealthStatus
			eds := hs.EdsHealthStatus == "" || hs.EdsHealthStatus == "HEALTHY"
			switch {
			case eds && !hs.FailedActiveHealthCheck && !hs.PendingActiveHC:
				return Healthy, nil
			case hs.PendingActiveHC:
				state = Pending
			}
		}
		return state, nil
	}
	return Absent, nil
}

func bytesReader(b []byte) io.Reader { return bytes.NewReader(b) }
