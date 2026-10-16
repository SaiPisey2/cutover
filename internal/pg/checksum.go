package pg

import (
	"context"
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

// SampleDigest hashes the first n rows of t in primary key order.
func SampleDigest(ctx context.Context, c *pgx.Conn, t Table, n int) (string, int, error) {
	if len(t.PKCols) == 0 {
		return "", 0, ErrNoKey
	}
	inner := make([]string, len(t.PKCols))
	outer := make([]string, len(t.PKCols))
	for i, col := range t.PKCols {
		inner[i] = pgx.Identifier{col}.Sanitize()
		outer[i] = "x." + inner[i]
	}
	q := fmt.Sprintf(`SELECT coalesce(md5(string_agg(x::text, E'\n' ORDER BY %s)), ''), count(*)
		FROM (SELECT * FROM %s ORDER BY %s LIMIT %d) x`,
		strings.Join(outer, ", "), t.Ident(), strings.Join(inner, ", "), n)
	var digest string
	var rows int
	if err := c.QueryRow(ctx, q).Scan(&digest, &rows); err != nil {
		return "", 0, fmt.Errorf("sample %s: %w", t.Qualified(), err)
	}
	return digest, rows, nil
}

// Estimate returns the planner's row estimate (-1 if never analyzed).
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
