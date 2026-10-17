package ops

import (
	"context"
	"fmt"
	"sync"

	"k8s.io/client-go/kubernetes"

	"github.com/SaiPisey2/cutover/internal/kube"
	"github.com/SaiPisey2/cutover/internal/plan"
	"github.com/SaiPisey2/cutover/internal/sshx"
	"github.com/SaiPisey2/cutover/internal/svc"
)

// side opens one environment's SSH and Kubernetes connections on first
// use and closes them when the returned func runs (end of the activity).
func side(p plan.Plan, target bool) (svc.Side, func()) {
	env := p.EnvFor(target)
	name := "source"
	if target {
		name = "target"
	}
	var mu sync.Mutex
	clients := map[string]*sshx.Client{}
	var kc kubernetes.Interface
	s := svc.Side{
		Host: func(ctx context.Context, host string) (svc.Runner, error) {
			mu.Lock()
			defer mu.Unlock()
			if c, ok := clients[host]; ok {
				return c, nil
			}
			h, ok := env.Hosts[host]
			if !ok {
				return nil, permanentf("%s.hosts has no %q", name, host)
			}
			c, err := sshx.Dial(ctx, h)
			if err != nil {
				return nil, fmt.Errorf("%s host %s: %w", name, host, err)
			}
			clients[host] = c
			return c, nil
		},
		Kube: func(ctx context.Context) (kubernetes.Interface, error) {
			mu.Lock()
			defer mu.Unlock()
			if kc != nil {
				return kc, nil
			}
			if env.Kube == nil {
				return nil, permanentf("%s.kube is not set", name)
			}
			c, err := kube.Client(*env.Kube)
			if err != nil {
				return nil, fmt.Errorf("%s cluster: %w", name, err)
			}
			kc = c
			return c, nil
		},
	}
	return s, func() {
		mu.Lock()
		defer mu.Unlock()
		for _, c := range clients {
			c.Close()
		}
	}
}
