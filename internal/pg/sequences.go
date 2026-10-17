package pg

import (
	"context"
	"fmt"
	"math"

	"github.com/jackc/pgx/v5"
)

type SequenceSync struct {
	Name    string `json:"name"`
	Source  int64  `json:"source"`
	Set     int64  `json:"set"`
	Changed bool   `json:"changed"`
}

// SyncSequences moves each sequence on `to` to at least its value on `from`
// plus margin. It never lowers a sequence, so a retry is harmless.
// Logical replication does not carry sequences; without this, the first
// insert on the new primary would collide with a replicated key.
func SyncSequences(ctx context.Context, from, to *pgx.Conn, schemas []string, margin int64) ([]SequenceSync, error) {
	if margin < 0 {
		return nil, fmt.Errorf("margin must not be negative, got %d", margin)
	}
	src, err := Sequences(ctx, from, schemas)
	if err != nil {
		return nil, err
	}
	dst, err := Sequences(ctx, to, schemas)
	if err != nil {
		return nil, err
	}
	have := map[string]Sequence{}
	for _, s := range dst {
		have[s.Qualified()] = s
	}
	var out []SequenceSync
	batch := &pgx.Batch{}
	var moved []int
	for _, s := range src {
		if s.Increment < 0 {
			return nil, fmt.Errorf("sequence %s: descending sequences are not supported", s.Qualified())
		}
		// Never used: nothing to carry, even if the other side lacks it. A
		// switch-back runs this from the target, which may hold sequences
		// the source never had.
		if s.LastValue == nil {
			continue
		}
		d, ok := have[s.Qualified()]
		if !ok {
			return nil, fmt.Errorf("sequence %s: missing on the new primary", s.Qualified())
		}
		if *s.LastValue > math.MaxInt64-margin {
			return nil, fmt.Errorf("sequence %s: last value %d plus margin %d overflows", s.Qualified(), *s.LastValue, margin)
		}
		// An is_called=false source hands out LastValue next, so it counts
		// as used: the target is set past it either way.
		want := *s.LastValue + margin
		// An is_called=false target hands out its LastValue next, so it has
		// effectively used one increment less.
		if d.LastValue != nil {
			have := *d.LastValue
			if !d.IsCalled {
				have -= d.Increment
			}
			if have >= want {
				out = append(out, SequenceSync{Name: s.Qualified(), Source: *s.LastValue, Set: have})
				continue
			}
		}
		batch.Queue("SELECT setval($1::regclass, $2, true)", pgx.Identifier{s.Schema, s.Name}.Sanitize(), want)
		moved = append(moved, len(out))
		out = append(out, SequenceSync{Name: s.Qualified(), Source: *s.LastValue, Set: want, Changed: true})
	}
	if batch.Len() == 0 {
		return out, nil
	}
	// One round trip for every setval: this runs inside the switch's pause budget.
	br := to.SendBatch(ctx, batch)
	for _, i := range moved {
		if _, err := br.Exec(); err != nil {
			br.Close()
			return nil, fmt.Errorf("setval %s: %w", out[i].Name, err)
		}
	}
	if err := br.Close(); err != nil {
		return nil, fmt.Errorf("setval: %w", err)
	}
	return out, nil
}
