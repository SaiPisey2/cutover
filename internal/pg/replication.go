package pg

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/SaiPisey2/cutover/internal/redact"
)

// ObjectName names a publication, subscription and slot for a migration.
// Migration IDs match plan.NamePattern, so the result is a valid identifier
// of at most 52 characters.
func ObjectName(migrationID, suffix string) string {
	return "cutover_" + strings.ReplaceAll(migrationID, "-", "_") + "_" + suffix
}

func literal(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func tableList(tables []string) string {
	idents := make([]string, len(tables))
	for i, t := range tables {
		s, n, _ := strings.Cut(t, ".")
		idents[i] = pgx.Identifier{s, n}.Sanitize()
	}
	return strings.Join(idents, ", ")
}

// EnsurePublication creates the publication, or sets its table list if it exists.
func EnsurePublication(ctx context.Context, c *pgx.Conn, name string, tables []string) error {
	var exists bool
	if err := c.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", name).Scan(&exists); err != nil {
		return fmt.Errorf("publication %s: %w", name, err)
	}
	verb := "CREATE PUBLICATION %s FOR TABLE %s"
	if exists {
		verb = "ALTER PUBLICATION %s SET TABLE %s"
	}
	if _, err := c.Exec(ctx, fmt.Sprintf(verb, pgx.Identifier{name}.Sanitize(), tableList(tables))); err != nil {
		return fmt.Errorf("publication %s: %w", name, err)
	}
	return nil
}

func DropPublication(ctx context.Context, c *pgx.Conn, name string) error {
	if _, err := c.Exec(ctx, "DROP PUBLICATION IF EXISTS "+pgx.Identifier{name}.Sanitize()); err != nil {
		return fmt.Errorf("drop publication %s: %w", name, err)
	}
	return nil
}

// EnsureSubscription creates the subscription (which creates its slot on the
// publisher), or enables it if it exists. conninfo is how this database
// reaches the publisher; Postgres stores it in pg_subscription, readable by
// superusers only. It carries a password, so every returned error is passed
// through redact.Err.
func EnsureSubscription(ctx context.Context, c *pgx.Conn, name, conninfo, publication string, copyData bool) error {
	var enabled bool
	err := c.QueryRow(ctx, `SELECT subenabled FROM pg_subscription
		WHERE subname = $1 AND subdbid = (SELECT oid FROM pg_database WHERE datname = current_database())`, name).Scan(&enabled)
	switch {
	case err == nil && enabled:
		return nil
	case err == nil:
		_, err = c.Exec(ctx, "ALTER SUBSCRIPTION "+pgx.Identifier{name}.Sanitize()+" ENABLE")
	case errors.Is(err, pgx.ErrNoRows):
		_, err = c.Exec(ctx, fmt.Sprintf("CREATE SUBSCRIPTION %s CONNECTION %s PUBLICATION %s WITH (copy_data = %t)",
			pgx.Identifier{name}.Sanitize(), literal(conninfo), pgx.Identifier{publication}.Sanitize(), copyData))
	}
	if err != nil {
		return redact.Err(fmt.Errorf("subscription %s: %w", name, err))
	}
	return nil
}

// DropSubscription drops the subscription and its slot on the publisher.
// Missing is fine. It fails if the publisher is unreachable, rather than
// leave an orphaned slot holding WAL there.
func DropSubscription(ctx context.Context, c *pgx.Conn, name string) error {
	if _, err := c.Exec(ctx, "DROP SUBSCRIPTION IF EXISTS "+pgx.Identifier{name}.Sanitize()); err != nil {
		return redact.Err(fmt.Errorf("drop subscription %s: %w", name, err))
	}
	return nil
}

// CopyProgress counts the subscription's tables that finished initial copy.
func CopyProgress(ctx context.Context, c *pgx.Conn, subscription string) (ready, total int, err error) {
	err = c.QueryRow(ctx, `SELECT count(*) FILTER (WHERE r.srsubstate = 'r'), count(*)
		FROM pg_subscription_rel r JOIN pg_subscription s ON s.oid = r.srsubid WHERE s.subname = $1`,
		subscription).Scan(&ready, &total)
	if err != nil {
		return 0, 0, fmt.Errorf("copy progress %s: %w", subscription, err)
	}
	return ready, total, nil
}

// WriteSentinel inserts a fresh token on the publisher and prunes old ones.
func WriteSentinel(ctx context.Context, c *pgx.Conn) (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	tok := hex.EncodeToString(b)
	if _, err := c.Exec(ctx, "INSERT INTO "+SentinelTable+" (token) VALUES ($1)", tok); err != nil {
		return "", fmt.Errorf("write sentinel: %w", err)
	}
	if _, err := c.Exec(ctx, "DELETE FROM "+SentinelTable+" WHERE written_at < now() - interval '1 hour'"); err != nil {
		return "", fmt.Errorf("prune sentinel: %w", err)
	}
	return tok, nil
}

// WaitSentinel polls the subscriber until token is visible. When ctx ends
// first it returns false and no error: "not drained in time" is an answer.
// After a context timeout pgx closes the connection, so callers must not
// reuse c once this returns false; open a fresh connection per call.
func WaitSentinel(ctx context.Context, c *pgx.Conn, token string, poll time.Duration) (bool, error) {
	for {
		var seen bool
		err := c.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM "+SentinelTable+" WHERE token = $1)", token).Scan(&seen)
		if ctx.Err() != nil {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("read sentinel: %w", err)
		}
		if seen {
			return true, nil
		}
		select {
		case <-ctx.Done():
			return false, nil
		case <-time.After(poll):
		}
	}
}
