package ops

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.temporal.io/sdk/temporal"

	"github.com/SaiPisey2/cutover/internal/pgbouncer"
)

func TestLinks(t *testing.T) {
	f, r := Forward("shop"), Reverse("shop")
	if f.From != Source || f.To != Target || r.From != Target || r.To != Source || f.Name == r.Name {
		t.Fatalf("%+v %+v", f, r)
	}
	if Source.Other() != Target || Target.Other() != Source {
		t.Fatal("Other")
	}
}

func TestFailRedactsAndClassifies(t *testing.T) {
	err := fail(errors.New("dial postgres://u:hunter2@h/db: refused"))
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || app.NonRetryable() || app.Type() != ErrTypeTransient {
		t.Fatalf("%#v", err)
	}
	if strings.Contains(err.Error(), "hunter2") {
		t.Fatalf("password leaked: %v", err)
	}
	if errors.Unwrap(app) != nil {
		t.Fatal("cause chain would carry the password into history")
	}
	err = fail(permanentf("target has %d of %d tables", 1, 2))
	if !errors.As(err, &app) || !app.NonRetryable() || app.Type() != ErrTypePermanent {
		t.Fatalf("%#v", err)
	}
	if fail(nil) != nil {
		t.Fatal("fail(nil)")
	}
}

// An application error built further down must not bypass redaction, nor
// keep its cause chain.
func TestFailRedactsApplicationErrors(t *testing.T) {
	in := temporal.NewNonRetryableApplicationError("x password=hunter2 y", "custom", errors.New("cause password=hunter2"))
	err := fail(in)
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || !app.NonRetryable() || app.Type() != "custom" {
		t.Fatalf("%#v", err)
	}
	if strings.Contains(err.Error(), "hunter2") || errors.Unwrap(app) != nil {
		t.Fatalf("leaked or chained: %v", err)
	}
}

// A cancelled activity must still look cancelled to Temporal.
func TestFailKeepsCancellation(t *testing.T) {
	err := fail(errors.Join(errors.New("password=hunter2"), context.Canceled))
	if !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "hunter2") {
		t.Fatalf("%v", err)
	}
}

func TestPauseErrors(t *testing.T) {
	var app *temporal.ApplicationError
	err := fail(pauseErr(pgbouncer.ErrGlobalPause))
	if !errors.As(err, &app) || !app.NonRetryable() || app.Type() != ErrTypePermanent || !strings.Contains(err.Error(), "globally paused") {
		t.Fatalf("global pause: %#v", err)
	}
	err = fail(pauseErr(pgbouncer.ErrPauseTimeout))
	if !errors.As(err, &app) || app.NonRetryable() || app.Type() != ErrTypeTransient {
		t.Fatalf("pause timeout: %#v", err)
	}
	if pauseErr(nil) != nil {
		t.Fatal("pauseErr(nil)")
	}
}
