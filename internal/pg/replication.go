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
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/SaiPisey2/cutover/internal/redact"
)

// ObjectName names a publication, subscription and slot for a migration.
// Migration IDs match plan.NamePattern, so the result is a valid identifier
// of at most 52 characters.
func ObjectName(migrationID, suffix string) string {
	return "cutover_" + strings.ReplaceAll(migrationID, "-", "_") + "_" + suffix
}

// literal quotes s as an escape string, so backslashes survive whatever
// standard_conforming_strings is set to.
func literal(s string) string {
	return "E'" + strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), "'", "''") + "'"
}

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
// publisher). If it exists it is brought in line: connection string and
// publication list are altered when they differ, it is enabled, and the
// publication is refreshed. copyData only affects tables added to the
// publication since the subscription was created. conninfo is how this
// database reaches the publisher; Postgres stores it in pg_subscription,
// readable by superusers only. It carries a password, so every returned
// error is passed through redact.Err.
func EnsureSubscription(ctx context.Context, c *pgx.Conn, name, conninfo, publication string, copyData bool) error {
	if err := ensureSubscription(ctx, c, name, conninfo, publication, copyData); err != nil {
		return redact.Err(fmt.Errorf("subscription %s: %w", name, err))
	}
	return nil
}

func ensureSubscription(ctx context.Context, c *pgx.Conn, name, conninfo, publication string, copyData bool) error {
	var enabled bool
	var haveConn string
	var havePubs []string
	err := c.QueryRow(ctx, `SELECT subenabled, subconninfo, subpublications FROM pg_subscription
		WHERE subname = $1 AND subdbid = (SELECT oid FROM pg_database WHERE datname = current_database())`, name).
		Scan(&enabled, &haveConn, &havePubs)
	if errors.Is(err, pgx.ErrNoRows) {
		_, err = c.Exec(ctx, fmt.Sprintf("CREATE SUBSCRIPTION %s CONNECTION %s PUBLICATION %s WITH (copy_data = %t)",
			pgx.Identifier{name}.Sanitize(), literal(conninfo), pgx.Identifier{publication}.Sanitize(), copyData))
		return err
	}
	if err != nil {
		return err
	}
	q := pgx.Identifier{name}.Sanitize()
	if haveConn != conninfo {
		if _, err := c.Exec(ctx, "ALTER SUBSCRIPTION "+q+" CONNECTION "+literal(conninfo)); err != nil {
			return err
		}
	}
	if len(havePubs) != 1 || havePubs[0] != publication {
		if _, err := c.Exec(ctx, "ALTER SUBSCRIPTION "+q+" SET PUBLICATION "+pgx.Identifier{publication}.Sanitize()+" WITH (refresh = false)"); err != nil {
			return err
		}
	}
	if !enabled {
		if _, err := c.Exec(ctx, "ALTER SUBSCRIPTION "+q+" ENABLE"); err != nil {
			return err
		}
	}
	_, err = c.Exec(ctx, fmt.Sprintf("ALTER SUBSCRIPTION %s REFRESH PUBLICATION WITH (copy_data = %t)", q, copyData))
	return err
}

// DropSubscription drops the subscription and its slot on the publisher.
// Missing is fine. It retries briefly while the slot is still active (the
// walsender can outlive the apply worker). If the publisher is unreachable
// or the remote slot is already gone, it detaches the subscription from its
// slot and drops it anyway; in the unreachable case a slot on the publisher
// is orphaned and the caller must remove it with DropSlot.
func DropSubscription(ctx context.Context, c *pgx.Conn, name string) error {
	q := pgx.Identifier{name}.Sanitize()
	deadline := time.Now().Add(slotRetryFor)
	var err error
	for {
		_, err = c.Exec(ctx, "DROP SUBSCRIPTION IF EXISTS "+q)
		if err == nil {
			return nil
		}
		if !isSlotActive(err) || time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	msg := err.Error()
	if !strings.Contains(msg, "could not drop replication slot") && !strings.Contains(msg, "could not connect") {
		return redact.Err(fmt.Errorf("drop subscription %s: %w", name, err))
	}
	for _, stmt := range []string{
		"ALTER SUBSCRIPTION " + q + " DISABLE",
		"ALTER SUBSCRIPTION " + q + " SET (slot_name = NONE)",
		"DROP SUBSCRIPTION " + q,
	} {
		if _, err := c.Exec(ctx, stmt); err != nil {
			return redact.Err(fmt.Errorf("drop subscription %s: %w", name, err))
		}
	}
	return nil
}

// CopyProgress counts the subscription's tables that finished initial copy.
func CopyProgress(ctx context.Context, c *pgx.Conn, subscription string) (ready, total int, err error) {
	var oid uint32
	err = c.QueryRow(ctx, `SELECT oid FROM pg_subscription
		WHERE subname = $1 AND subdbid = (SELECT oid FROM pg_database WHERE datname = current_database())`, subscription).Scan(&oid)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, fmt.Errorf("copy progress: subscription %s does not exist", subscription)
	}
	if err != nil {
		return 0, 0, fmt.Errorf("copy progress %s: %w", subscription, err)
	}
	err = c.QueryRow(ctx, `SELECT count(*) FILTER (WHERE r.srsubstate = 'r'), count(*)
		FROM pg_subscription_rel r WHERE r.srsubid = $1`, oid).Scan(&ready, &total)
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

// waitEnd maps the end of ctx to WaitSentinel's result: a deadline is an
// answer (not drained in time), an explicit cancel is the caller's error.
func waitEnd(ctx context.Context) error {
	if errors.Is(ctx.Err(), context.Canceled) {
		return ctx.Err()
	}
	return nil
}

// WaitSentinel polls the subscriber until token is visible. When ctx's deadline
// passes first it returns false and no error: "not drained in time" is an
// answer. If ctx is cancelled it returns false and ctx.Err().
// After a context timeout pgx closes the connection, so callers must not
// reuse c once this returns false; open a fresh connection per call.
func WaitSentinel(ctx context.Context, c *pgx.Conn, token string, poll time.Duration) (bool, error) {
	for {
		var seen bool
		err := c.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM "+SentinelTable+" WHERE token = $1)", token).Scan(&seen)
		if ctx.Err() != nil {
			return false, waitEnd(ctx)
		}
		if err != nil {
			return false, fmt.Errorf("read sentinel: %w", err)
		}
		if seen {
			return true, nil
		}
		select {
		case <-ctx.Done():
			return false, waitEnd(ctx)
		case <-time.After(poll):
		}
	}
}

const slotRetryFor = 5 * time.Second

func isSlotActive(err error) bool {
	return err != nil && strings.Contains(err.Error(), "is active for PID")
}

func isMissing(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == "42704"
}

// DropSlot drops a logical replication slot on the publisher. Absent is
// fine. An active slot has its walsender terminated and is retried for up
// to five seconds.
func DropSlot(ctx context.Context, c *pgx.Conn, name string) error {
	deadline := time.Now().Add(slotRetryFor)
	for {
		var pid *int32
		err := c.QueryRow(ctx, "SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1", name).Scan(&pid)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("drop slot %s: %w", name, err)
		}
		if pid != nil {
			if _, err := c.Exec(ctx, "SELECT pg_terminate_backend($1)", *pid); err != nil {
				return fmt.Errorf("drop slot %s: %w", name, err)
			}
		}
		_, err = c.Exec(ctx, "SELECT pg_drop_replication_slot($1)", name)
		if err == nil || isMissing(err) {
			return nil
		}
		if !isSlotActive(err) || time.Now().After(deadline) {
			return fmt.Errorf("drop slot %s: %w", name, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}
