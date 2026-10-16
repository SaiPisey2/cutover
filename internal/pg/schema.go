package pg

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/SaiPisey2/cutover/internal/plan"
	"github.com/SaiPisey2/cutover/internal/redact"
)

// SentinelTable is cutover's own table on both sides. It is in every
// publication; a token inserted on the publisher and seen on the subscriber
// proves every earlier commit has been applied.
const SentinelTable = plan.ReservedSchema + ".sentinel"

// CopySchema pipes pg_dump --schema-only --table (one per plan table) from
// src into psql on dst in a single transaction, so a failed apply leaves dst
// unchanged. It copies only the plan's tables and the sequences they own,
// which is exactly what UndoPrepare drops, so Prepare after a rollback starts
// clean. The schemas are created first if missing (pg_dump --table does not
// emit CREATE SCHEMA). Types, functions and standalone sequences the tables
// depend on are not copied: the apply then fails naming the missing object,
// and the operator creates it on the target first. Passwords go through
// PGPASSWORD, never argv.
func CopySchema(ctx context.Context, tools plan.Tools, srcDSN, dstDSN string, schemas, tables []string) error {
	if len(tables) == 0 {
		return fmt.Errorf("copy schema: no tables")
	}
	src, srcPW := redact.Split(srcDSN)
	dst, dstPW := redact.Split(dstDSN)
	args := []string{"--schema-only", "--no-owner", "--no-privileges",
		"--no-publications", "--no-subscriptions", "--dbname", src}
	for _, t := range tables {
		args = append(args, "--table", t) // plan.Ident names: no pattern characters
	}
	dump := exec.CommandContext(ctx, tools.PgDump, args...)
	dump.Env = withPassword(srcPW)
	psqlArgs := []string{"--no-psqlrc", "--quiet", "--single-transaction",
		"--set", "ON_ERROR_STOP=1", "--dbname", dst}
	for _, s := range schemas {
		psqlArgs = append(psqlArgs, "--command", "CREATE SCHEMA IF NOT EXISTS "+pgx.Identifier{s}.Sanitize())
	}
	psqlArgs = append(psqlArgs, "--file", "-")
	apply := exec.CommandContext(ctx, tools.Psql, psqlArgs...)
	apply.Env = withPassword(dstPW)

	r, w, err := os.Pipe()
	if err != nil {
		return err
	}
	var dumpErr, applyErr strings.Builder
	dump.Stdout, dump.Stderr = w, &dumpErr
	apply.Stdin, apply.Stderr, apply.Stdout = r, &applyErr, &applyErr
	if err := apply.Start(); err != nil {
		r.Close()
		w.Close()
		return fmt.Errorf("psql: %w", err)
	}
	r.Close()
	if err := dump.Start(); err != nil {
		w.Close()
		_ = apply.Wait()
		return fmt.Errorf("pg_dump: %w", err)
	}
	w.Close()
	derr, aerr := dump.Wait(), apply.Wait()
	if derr != nil {
		return fmt.Errorf("pg_dump: %v: %s", derr, tail(dumpErr.String()))
	}
	if aerr != nil {
		return fmt.Errorf("psql: %v: %s", aerr, tail(applyErr.String()))
	}
	return nil
}

func withPassword(pw string) []string {
	env := os.Environ()
	if pw != "" {
		env = append(env, "PGPASSWORD="+pw)
	}
	return env
}

func tail(s string) string {
	s = strings.TrimSpace(redact.DSN(s))
	if len(s) > 2000 {
		s = "..." + s[len(s)-2000:]
	}
	return s
}

type Column struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	NotNull bool   `json:"not_null"`
}

// Schema maps a qualified table name to its columns, sorted by name.
// Logical replication matches columns by name, so order does not matter.
type Schema map[string][]Column

// ReadSchema reads the columns of the named tables. Tables that do not
// exist are absent from the result.
func ReadSchema(ctx context.Context, c *pgx.Conn, tables []string) (Schema, error) {
	rows, err := c.Query(ctx, `
		SELECT n.nspname || '.' || c.relname, a.attname::text, format_type(a.atttypid, a.atttypmod), a.attnotnull
		FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE a.attnum > 0 AND NOT a.attisdropped AND n.nspname || '.' || c.relname = ANY($1)
		ORDER BY 1, 2`, tables)
	if err != nil {
		return nil, fmt.Errorf("read schema: %w", err)
	}
	defer rows.Close()
	s := Schema{}
	for rows.Next() {
		var table string
		var col Column
		if err := rows.Scan(&table, &col.Name, &col.Type, &col.NotNull); err != nil {
			return nil, err
		}
		s[table] = append(s[table], col)
	}
	return s, rows.Err()
}

// Diff describes how dst differs from src, one sorted line per difference.
func Diff(src, dst Schema) []string {
	var out []string
	for table, scols := range src {
		dcols, ok := dst[table]
		if !ok {
			out = append(out, table+": missing on target")
			continue
		}
		dm := map[string]Column{}
		for _, c := range dcols {
			dm[c.Name] = c
		}
		for _, sc := range scols {
			dc, ok := dm[sc.Name]
			switch {
			case !ok:
				out = append(out, fmt.Sprintf("%s.%s: only on source", table, sc.Name))
			case dc.Type != sc.Type:
				out = append(out, fmt.Sprintf("%s.%s: %s on source, %s on target", table, sc.Name, sc.Type, dc.Type))
			case dc.NotNull != sc.NotNull:
				out = append(out, fmt.Sprintf("%s.%s: NOT NULL differs", table, sc.Name))
			}
			delete(dm, sc.Name)
		}
		for name := range dm {
			out = append(out, fmt.Sprintf("%s.%s: only on target", table, name))
		}
	}
	for table := range dst {
		if _, ok := src[table]; !ok {
			out = append(out, table+": only on target")
		}
	}
	slices.Sort(out)
	return out
}

// EnsureSentinel creates cutover's sentinel table if it does not exist.
func EnsureSentinel(ctx context.Context, c *pgx.Conn) error {
	_, err := c.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+plan.ReservedSchema+`;
		CREATE TABLE IF NOT EXISTS `+SentinelTable+` (token text PRIMARY KEY, written_at timestamptz NOT NULL DEFAULT now())`)
	if err != nil {
		return fmt.Errorf("sentinel: %w", err)
	}
	return nil
}

// DropSentinel removes cutover's schema. Missing is fine.
func DropSentinel(ctx context.Context, c *pgx.Conn) error {
	_, err := c.Exec(ctx, "DROP SCHEMA IF EXISTS "+plan.ReservedSchema+" CASCADE")
	return err
}

// DropTables drops the named tables and the sequences they own. Missing is fine.
func DropTables(ctx context.Context, c *pgx.Conn, tables []string) error {
	if len(tables) == 0 {
		return nil
	}
	idents := make([]string, len(tables))
	for i, t := range tables {
		s, n, _ := strings.Cut(t, ".")
		idents[i] = pgx.Identifier{s, n}.Sanitize()
	}
	_, err := c.Exec(ctx, "DROP TABLE IF EXISTS "+strings.Join(idents, ", ")+" CASCADE")
	return err
}
