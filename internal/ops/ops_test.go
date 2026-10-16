package ops

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"go.temporal.io/sdk/temporal"

	"github.com/SaiPisey2/cutover/internal/pg"
	"github.com/SaiPisey2/cutover/internal/pgbouncer"
	"github.com/SaiPisey2/cutover/internal/secret"
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
	err := fail(pauseErr(context.Background(), pgbouncer.ErrGlobalPause))
	if !errors.As(err, &app) || !app.NonRetryable() || app.Type() != ErrTypePermanent || app.Message() != "pgbouncer is globally paused; cutover will not pause a pooler someone else paused — resume it first" {
		t.Fatalf("global pause: %#v", err)
	}
	err = fail(pauseErr(context.Background(), pgbouncer.ErrPauseTimeout))
	if !errors.As(err, &app) || app.NonRetryable() || app.Type() != ErrTypeTransient {
		t.Fatalf("pause timeout: %#v", err)
	}
	if pauseErr(context.Background(), nil) != nil {
		t.Fatal("pauseErr(nil)")
	}
}

// Pause reports ErrPauseTimeout whenever its context ends; when the activity
// itself was cancelled that must surface as a cancellation.
func TestPauseErrCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := fail(pauseErr(ctx, pgbouncer.ErrPauseTimeout))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("%#v", err)
	}
	dctx, dcancel := context.WithTimeout(context.Background(), 0)
	defer dcancel()
	var app *temporal.ApplicationError
	if err := fail(pauseErr(dctx, pgbouncer.ErrPauseTimeout)); !errors.As(err, &app) || app.NonRetryable() {
		t.Fatalf("deadline is a retryable timeout: %#v", err)
	}
}

func TestFailPermanentPgErrors(t *testing.T) {
	for code, want := range map[string]string{
		"42P01": ErrTypePermanent, "42501": ErrTypePermanent, "42704": ErrTypePermanent,
		"40001": ErrTypeTransient, "57P01": ErrTypeTransient,
	} {
		pe := &pgconn.PgError{Code: code, Message: "boom password=hunter2"}
		err := fail(fmt.Errorf("estimate public.x: %w", pe))
		var app *temporal.ApplicationError
		if !errors.As(err, &app) || app.Type() != want || app.NonRetryable() != (want == ErrTypePermanent) {
			t.Fatalf("%s: %#v", code, err)
		}
		if strings.Contains(err.Error(), "hunter2") || errors.Unwrap(app) != nil {
			t.Fatalf("%s: leaked or chained: %v", code, err)
		}
	}
}

func TestFailVersionMismatchIsPermanent(t *testing.T) {
	for _, msg := range []string{
		"pg_dump: exit status 1: pg_dump: error: aborting because of server version mismatch",
		"psql: exit status 1: server version mismatch password=hunter2",
	} {
		err := fail(errors.New(msg))
		var app *temporal.ApplicationError
		if !errors.As(err, &app) || !app.NonRetryable() || app.Type() != ErrTypePermanent || strings.Contains(err.Error(), "hunter2") {
			t.Fatalf("%q: %#v", msg, err)
		}
	}
}

// mistyped are DSNs pgx cannot parse, with a fragment of their password that
// pgx's own error would echo back past redact.DSN.
var mistyped = []struct{ dsn, fragment string }{
	{"host=h user=u password=hun ter2 dbname=app", "ter2"},
	{"host=h user=u password='abc def dbname=app", "def"},
	{"postgres://u:p@ss w@h/db", "ss w"},
}

// A DSN that does not parse must not reach the error at all: pgx echoes the
// string with best-effort masking that stops at the first space.
func TestMistypedDSNLeaksNothing(t *testing.T) {
	for _, m := range mistyped {
		t.Setenv("CUTOVER_TEST_MISTYPED", m.dsn)
		ref := secret.Ref("env:CUTOVER_TEST_MISTYPED")
		_, err := connect(context.Background(), ref)
		if err == nil {
			t.Fatalf("%q: want a parse error", m.dsn)
		}
		if msg := fail(err).Error(); strings.Contains(msg, m.fragment) || !strings.Contains(msg, "env:CUTOVER_TEST_MISTYPED") {
			t.Fatalf("connect %q: %s", m.dsn, msg)
		}
		var f pg.Facts
		err = replRole(context.Background(), nil, ref, &f)
		if err == nil {
			t.Fatalf("%q: want a parse error", m.dsn)
		}
		if msg := fail(err).Error(); strings.Contains(msg, m.fragment) || !strings.Contains(msg, "env:CUTOVER_TEST_MISTYPED") {
			t.Fatalf("replRole %q: %s", m.dsn, msg)
		}
	}
}

// StartLink hands the replication DSN to CREATE SUBSCRIPTION, whose error
// would echo it; a mistyped one is refused before any connection.
func TestStartLinkRejectsMistypedReplicationDSN(t *testing.T) {
	for _, m := range mistyped {
		t.Setenv("CUTOVER_TEST_MISTYPED", m.dsn)
		err := checkDSN(secret.Ref("env:CUTOVER_TEST_MISTYPED"))
		if err == nil || strings.Contains(err.Error(), m.fragment) {
			t.Fatalf("%q: %v", m.dsn, err)
		}
	}
	t.Setenv("CUTOVER_TEST_MISTYPED", "host=h user=u password=x dbname=app")
	if err := checkDSN(secret.Ref("env:CUTOVER_TEST_MISTYPED")); err != nil {
		t.Fatal(err)
	}
}
