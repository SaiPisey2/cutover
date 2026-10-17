// Package svc copies a service from the source environment to the target
// and removes the copy again. One driver per kind: compose and systemd
// over SSH, deployment over the Kubernetes API, external for copies the
// operator runs. Drivers move service definitions (which can hold
// secrets) host to host inside one call and never return them.
package svc

import (
	"context"
	"errors"
	"fmt"
	"os"

	"k8s.io/client-go/kubernetes"

	"github.com/SaiPisey2/cutover/internal/plan"
)

// Runner runs commands and moves files on one host (*sshx.Client).
type Runner interface {
	Run(ctx context.Context, cmd string, stdin []byte) ([]byte, error)
	Root(cmd string) string
	WriteFile(ctx context.Context, path string, mode os.FileMode, data []byte) error
	ReadFile(ctx context.Context, path string) ([]byte, error)
	Exists(ctx context.Context, path string) (bool, error)
}

// Side is one environment's connections. The caller opens them lazily
// and closes them after the call.
type Side struct {
	Host func(ctx context.Context, name string) (Runner, error)
	Kube func(ctx context.Context) (kubernetes.Interface, error)
}

type Finding struct {
	Blocker bool   `json:"blocker"`
	Code    string `json:"code"`
	Subject string `json:"subject"`
	Message string `json:"message"`
}

// Facts are what Inspect learned: never the definition itself.
type Facts struct {
	Service string `json:"service"`
	Kind    string `json:"kind"`
	Image   string `json:"image,omitempty"`
	// Ports are the source service's published ports, "8080->80/tcp", sorted.
	Ports    []string  `json:"ports,omitempty"`
	Findings []Finding `json:"findings,omitempty"`
}

type Driver interface {
	// Inspect reads both sides and changes nothing.
	Inspect(ctx context.Context, src, tgt Side) (Facts, error)
	// Prepare creates the target copy. A retry after success is a no-op.
	Prepare(ctx context.Context, src, tgt Side) error
	// Healthy reports whether the target copy runs. Routed services are
	// also checked through the front door; this is for the rest.
	Healthy(ctx context.Context, tgt Side) (bool, string, error)
	// Undo removes what Prepare created on the target and nothing else.
	Undo(ctx context.Context, tgt Side) error
}

// ErrBlocked wraps a Prepare refused because the source has a blocker.
var ErrBlocked = errors.New("blocked")

func For(p plan.Plan, s plan.Service) (Driver, error) {
	switch s.Kind {
	case plan.KindExternal:
		return external{svc: s}, nil
	case plan.KindCompose:
		return compose{plan: p.Name, svc: s}, nil
	case plan.KindSystemd:
		return systemd{plan: p.Name, svc: s}, nil
	case plan.KindDeployment:
		return deployment{plan: p.Name, svc: s}, nil
	}
	return nil, fmt.Errorf("service %s: unknown kind %q", s.Name, s.Kind)
}

func blocked(fs []Finding) error {
	for _, f := range fs {
		if f.Blocker {
			return fmt.Errorf("%w: %s: %s", ErrBlocked, f.Subject, f.Message)
		}
	}
	return nil
}

type systemd struct {
	plan string
	svc  plan.Service
}
type deployment struct {
	plan string
	svc  plan.Service
}

func (systemd) Inspect(context.Context, Side, Side) (Facts, error) {
	return Facts{}, errors.New("systemd: not built yet")
}
func (systemd) Prepare(context.Context, Side, Side) error {
	return errors.New("systemd: not built yet")
}
func (systemd) Healthy(context.Context, Side) (bool, string, error) {
	return false, "", errors.New("systemd: not built yet")
}
func (systemd) Undo(context.Context, Side) error { return errors.New("systemd: not built yet") }
func (deployment) Inspect(context.Context, Side, Side) (Facts, error) {
	return Facts{}, errors.New("deployment: not built yet")
}
func (deployment) Prepare(context.Context, Side, Side) error {
	return errors.New("deployment: not built yet")
}
func (deployment) Healthy(context.Context, Side) (bool, string, error) {
	return false, "", errors.New("deployment: not built yet")
}
func (deployment) Undo(context.Context, Side) error { return errors.New("deployment: not built yet") }
