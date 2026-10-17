// Package kube builds Kubernetes clients from the plan's explicit
// kubeconfig path. It never reads KUBECONFIG or ~/.kube/config: on the
// machines cutover runs on those can point at production.
package kube

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/SaiPisey2/cutover/internal/plan"
)

func Client(k plan.Kube) (kubernetes.Interface, error) {
	b, err := os.ReadFile(k.Kubeconfig)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("kubeconfig %s does not exist", k.Kubeconfig)
		}
		return nil, fmt.Errorf("kubeconfig %s cannot be read", k.Kubeconfig)
	}
	// Load errors can quote the file, which holds credentials: say only
	// that it does not parse.
	cfg, err := clientcmd.Load(b)
	if err != nil {
		return nil, fmt.Errorf("kubeconfig %s does not parse as a kubeconfig", k.Kubeconfig)
	}
	if _, ok := cfg.Contexts[k.Context]; !ok {
		return nil, fmt.Errorf("kubeconfig %s has no context %q", k.Kubeconfig, k.Context)
	}
	rc, err := clientcmd.NewNonInteractiveClientConfig(*cfg, k.Context, &clientcmd.ConfigOverrides{}, nil).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("kubeconfig %s, context %q: incomplete (%T)", k.Kubeconfig, k.Context, err)
	}
	rc.Timeout = 30 * time.Second
	return kubernetes.NewForConfig(rc)
}
