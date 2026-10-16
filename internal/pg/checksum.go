package pg

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// ErrNoKey means a table has no primary key to order a sample by.
var ErrNoKey = errors.New("no primary key")

type TableCheck struct {
	Table          string `json:"table"`
	SourceEstimate int64  `json:"source_estimate"`
	TargetEstimate int64  `json:"target_estimate"`
	Sampled        int    `json:"sampled"`
	Match          bool   `json:"match"`
	Note           string `json:"note,omitempty"`
}

// SampleDigest hashes a head and tail sample of t: the first ceil(n/2) rows in
// primary key order and the last floor(n/2) rows. Rows are hashed one by one
// and the hashes aggregated, so memory stays bounded. Collatable key columns
// are ordered with COLLATE "C" and the session's output settings are pinned,
// so two servers with different locales or defaults agree on the digest.
//
// The digest is meaningful only when the source is quiesced or replication
// lag is zero; otherwise a mismatch may just be a row in flight. rows counts
// sampled rows; on a table smaller than n it is the table's row count.
func SampleDigest(ctx context.Context, c *pgx.Conn, t Table, n int) (string, int, error) {
	if n <= 0 {
		return "", 0, fmt.Errorf("sample %s: n must be positive, got %d", t.Qualified(), n)
	}
	if len(t.PKCols) == 0 {
		return "", 0, ErrNoKey
	}
	tx, err := c.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return "", 0, fmt.Errorf("sample %s: %w", t.Qualified(), err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, err := tx.Exec(ctx, `SELECT set_config('DateStyle', 'ISO, MDY', true),
		set_config('IntervalStyle', 'postgres', true), set_config('TimeZone', 'UTC', true),
		set_config('extra_float_digits', '1', true), set_config('bytea_output', 'hex', true)`); err != nil {
		return "", 0, fmt.Errorf("sample %s: pin settings: %w", t.Qualified(), err)
	}
	rows, err := tx.Query(ctx, `SELECT a.attcollation <> 0 FROM unnest($2::text[]) WITH ORDINALITY k(name, ord)
		JOIN pg_attribute a ON a.attrelid = $1::regclass AND a.attname = k.name ORDER BY k.ord`, t.Ident(), t.PKCols)
	if err != nil {
		return "", 0, fmt.Errorf("sample %s: collations: %w", t.Qualified(), err)
	}
	collatable, err := pgx.CollectRows(rows, pgx.RowTo[bool])
	if err != nil || len(collatable) != len(t.PKCols) {
		return "", 0, fmt.Errorf("sample %s: key columns not found (%v)", t.Qualified(), err)
	}
	head, tail := (n+1)/2, n/2
	asc, desc := orderBy(t.PKCols, collatable, false), orderBy(t.PKCols, collatable, true)
	q := fmt.Sprintf(`SELECT
		coalesce((SELECT md5(string_agg(h, ',' ORDER BY rn)) FROM (SELECT md5(x::text) h, row_number() OVER (ORDER BY %[1]s) rn
			FROM (SELECT * FROM %[3]s ORDER BY %[1]s LIMIT %[4]d) x) hh), ''),
		coalesce((SELECT md5(string_agg(h, ',' ORDER BY rn)) FROM (SELECT md5(x::text) h, row_number() OVER (ORDER BY %[2]s) rn
			FROM (SELECT * FROM %[3]s ORDER BY %[2]s LIMIT %[5]d) x) tt), ''),
		(SELECT count(*) FROM (SELECT 1 FROM %[3]s LIMIT %[4]d) x),
		(SELECT count(*) FROM (SELECT 1 FROM %[3]s LIMIT %[5]d) x)`, asc, desc, t.Ident(), head, tail)
	var hd, tl string
	var hc, tc int
	if err := tx.QueryRow(ctx, q).Scan(&hd, &tl, &hc, &tc); err != nil {
		return "", 0, fmt.Errorf("sample %s: %w", t.Qualified(), err)
	}
	if hc < head {
		tl, tc = "", 0 // whole table already in the head
	}
	if hd == "" && tl == "" {
		return "", hc, nil
	}
	sum := md5.Sum([]byte(hd + "|" + tl))
	return hex.EncodeToString(sum[:]), hc + tc, nil
}

// orderBy renders a key ordering, forcing COLLATE "C" on collatable columns
// so the sample does not depend on each server's locale.
func orderBy(cols []string, collatable []bool, desc bool) string {
	parts := make([]string, len(cols))
	for i, col := range cols {
		parts[i] = pgx.Identifier{col}.Sanitize()
		if collatable[i] {
			parts[i] += ` COLLATE "C"`
		}
		if desc {
			parts[i] += " DESC"
		}
	}
	return strings.Join(parts, ", ")
}

// Estimate returns the planner's row estimate. 0 or -1 means unknown: newer
// servers report -1 for a never-analyzed table, PG12 and 13 report 0.
// Exact counts on large tables are too slow for a rehearsal gate.
func Estimate(ctx context.Context, c *pgx.Conn, qualified string) (int64, error) {
	s, n, _ := strings.Cut(qualified, ".")
	var est int64
	if err := c.QueryRow(ctx, "SELECT reltuples::bigint FROM pg_class WHERE oid = $1::regclass",
		pgx.Identifier{s, n}.Sanitize()).Scan(&est); err != nil {
		return 0, fmt.Errorf("estimate %s: %w", qualified, err)
	}
	return est, nil
}
