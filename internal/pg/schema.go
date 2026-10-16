package pg

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/SaiPisey2/cutover/internal/plan"
	"github.com/SaiPisey2/cutover/internal/redact"
)

// SentinelTable is cutover's own table on both sides. It is in every
// publication; a token inserted on the publisher and seen on the subscriber
// proves every earlier commit has been applied.
const SentinelTable = plan.ReservedSchema + ".sentinel"

// CopySchema runs pg_dump --schema-only --table (one per plan table) against
// src, and only if that succeeds applies the dump with psql on dst in a
// single transaction, so a failed dump applies nothing and a failed apply
// leaves dst unchanged. It copies only the plan's tables and the sequences
// they own, which is exactly what UndoPrepare drops, so Prepare after a
// rollback starts clean. The schemas are created first if missing (pg_dump
// --table does not emit CREATE SCHEMA). Types, functions and standalone
// sequences the tables depend on are not copied: the apply then fails naming
// the missing object, and the operator creates it on the target first. The
// dump is held in memory, capped at 256 MiB. Passwords go through
// PGPASSWORD, never argv.
func CopySchema(ctx context.Context, tools plan.Tools, srcDSN, dstDSN string, schemas, tables []string) error {
	if len(tables) == 0 {
		return fmt.Errorf("copy schema: no tables")
	}
	src, srcPW := redact.Split(srcDSN)
	dst, dstPW := redact.Split(dstDSN)
	args := []string{"--schema-only", "--no-owner", "--no-privileges", "--no-tablespaces",
		"--no-publications", "--no-subscriptions", "--strict-names", "--dbname", src}
	for _, t := range tables {
		args = append(args, "--table", t) // plan.Ident names: no pattern characters
	}
	dump := exec.CommandContext(ctx, tools.PgDump, args...)
	dump.Env = childEnv(srcPW)
	dump.WaitDelay = 5 * time.Second
	out := &cappedBuffer{max: maxDump}
	var dumpErr strings.Builder
	dump.Stdout, dump.Stderr = out, &dumpErr
	if err := dump.Run(); err != nil {
		if errors.Is(err, errDumpTooBig) {
			return errDumpTooBig
		}
		return fmt.Errorf("pg_dump: %v: %s", redact.DSN(err.Error()), tail(dumpErr.String()))
	}

	psqlArgs := []string{"--no-psqlrc", "--quiet", "--single-transaction",
		"--set", "ON_ERROR_STOP=1", "--dbname", dst}
	for _, s := range schemas {
		psqlArgs = append(psqlArgs, "--command", "CREATE SCHEMA IF NOT EXISTS "+pgx.Identifier{s}.Sanitize())
	}
	psqlArgs = append(psqlArgs, "--file", "-")
	apply := exec.CommandContext(ctx, tools.Psql, psqlArgs...)
	apply.Env = childEnv(dstPW)
	apply.WaitDelay = 5 * time.Second
	var applyErr strings.Builder
	apply.Stdin, apply.Stderr, apply.Stdout = bytes.NewReader(out.Bytes()), &applyErr, &applyErr
	if err := apply.Run(); err != nil {
		return fmt.Errorf("psql: %v: %s", redact.DSN(err.Error()), tail(applyErr.String()))
	}
	return nil
}

// childEnv is the parent environment minus every PG* variable (a stray
// PGSERVICE, PGOPTIONS or PGHOST must not redirect pg_dump or psql), plus
// PGPASSWORD when there is one.
func childEnv(pw string) []string {
	var env []string
	for _, e := range os.Environ() {
		if !strings.HasPrefix(e, "PG") {
			env = append(env, e)
		}
	}
	if pw != "" {
		env = append(env, "PGPASSWORD="+pw)
	}
	return env
}

const maxDump = 256 << 20

var errDumpTooBig = errors.New("schema dump exceeds 256 MiB")

// cappedBuffer is a bytes.Buffer that refuses to grow past max.
type cappedBuffer struct {
	bytes.Buffer
	max int
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	if b.Len()+len(p) > b.max {
		return 0, errDumpTooBig
	}
	return b.Buffer.Write(p)
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

// DropTables drops the named tables and the sequences they own. Missing is
// fine. It does not cascade: a view or foreign key outside the list that
// depends on a table makes the drop fail instead of being dropped with it.
func DropTables(ctx context.Context, c *pgx.Conn, tables []string) error {
	if len(tables) == 0 {
		return nil
	}
	idents := make([]string, len(tables))
	for i, t := range tables {
		s, n, _ := strings.Cut(t, ".")
		idents[i] = pgx.Identifier{s, n}.Sanitize()
	}
	_, err := c.Exec(ctx, "DROP TABLE IF EXISTS "+strings.Join(idents, ", "))
	return err
}
