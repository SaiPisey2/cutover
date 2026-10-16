//go:build integration

package itest

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SaiPisey2/cutover/internal/redact"
)

const (
	SeedOrders = 1000
	seedItems  = 50
)

// Seed creates the demo schema on c: orders (inserts) and items (updates).
// orders.origin defaults to the cluster that accepted the insert; logical
// replication carries the value, so it records which side took each write.
func Seed(t testing.TB, c *pgx.Conn) {
	t.Helper()
	mustExec(t, c, fmt.Sprintf(`
		CREATE TABLE public.orders (id bigserial PRIMARY KEY, writer int NOT NULL, n int NOT NULL,
			origin text NOT NULL DEFAULT current_setting('cluster_name'), UNIQUE (writer, n));
		CREATE TABLE public.items (sku text PRIMARY KEY, qty bigint NOT NULL);
		INSERT INTO public.orders (writer, n) SELECT 0, i FROM generate_series(1, %d) i;
		INSERT INTO public.items SELECT 'sku-' || lpad(i::text, 2, '0'), 0 FROM generate_series(0, %d) i;`,
		SeedOrders, seedItems-1))
}

// LoadResult is what the load did. Every failed statement counts in
// Failed; nothing is retried. The unexported fields record exactly which
// writes were acknowledged, for Verify.
type LoadResult struct {
	Inserted, Updated, Failed int64
	Errors                    []string

	orders  map[int][]int    // writer → acknowledged n, in order
	skus    map[string]int64 // sku → acknowledged updates
	slowest time.Duration    // longest acknowledged statement
}

type Load struct {
	cancel                    context.CancelFunc
	wg                        sync.WaitGroup
	pool                      *pgxpool.Pool
	start                     time.Time
	inserted, updated, failed atomic.Int64
	mu                        sync.Mutex
	errs                      []string
	orders                    map[int][]int
	skus                      map[string]int64
	slowest                   atomic.Int64
}

// StartLoad runs writers through PgBouncer until Stop. Each statement uses a
// background context, so Stop never turns an in-flight query into a failure;
// a held query during a pause just waits.
func StartLoad(t testing.TB, writers int) *Load {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), PoolDSN+"&pool_max_conns="+fmt.Sprint(writers))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	l := &Load{cancel: cancel, pool: pool, start: time.Now(), orders: map[int][]int{}, skus: map[string]int64{}}
	for w := 1; w <= writers; w++ {
		l.wg.Add(1)
		go func() {
			defer l.wg.Done()
			var acked []int
			skus := map[string]int64{}
			defer func() {
				l.mu.Lock()
				defer l.mu.Unlock()
				l.orders[w] = acked
				for k, v := range skus {
					l.skus[k] += v
				}
			}()
			for i := 1; ctx.Err() == nil; i++ {
				began := time.Now()
				_, err := pool.Exec(context.Background(), "INSERT INTO public.orders (writer, n) VALUES ($1, $2)", w, i)
				l.took(began)
				if err != nil {
					l.fail(fmt.Sprintf("insert (%d,%d)", w, i), err)
				} else {
					l.inserted.Add(1)
					acked = append(acked, i)
				}
				if i%3 == 0 {
					sku := fmt.Sprintf("sku-%02d", i%seedItems)
					began := time.Now()
					_, err := pool.Exec(context.Background(), "UPDATE public.items SET qty = qty + 1 WHERE sku = $1", sku)
					l.took(began)
					if err != nil {
						l.fail("update "+sku, err)
					} else {
						l.updated.Add(1)
						skus[sku]++
					}
				}
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}
	return l
}

func (l *Load) took(began time.Time) {
	d := int64(time.Since(began))
	for {
		cur := l.slowest.Load()
		if d <= cur || l.slowest.CompareAndSwap(cur, d) {
			return
		}
	}
}

func (l *Load) fail(what string, err error) {
	l.failed.Add(1)
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.errs) < 5 {
		l.errs = append(l.errs, fmt.Sprintf("+%s %s: %s", time.Since(l.start).Round(time.Millisecond), what, redact.DSN(err.Error())))
	}
}

func (l *Load) Stop() LoadResult {
	l.cancel()
	l.wg.Wait()
	l.pool.Close()
	l.mu.Lock()
	defer l.mu.Unlock()
	return LoadResult{Inserted: l.inserted.Load(), Updated: l.updated.Load(), Failed: l.failed.Load(), Errors: l.errs,
		orders: l.orders, skus: l.skus, slowest: time.Duration(l.slowest.Load())}
}

// Verify checks that c holds every write the load made, exactly once: no
// failed statement, every acknowledged insert present once and nothing
// else, and every item's qty equal to its acknowledged updates.
func Verify(t testing.TB, c *pgx.Conn, r LoadResult) {
	t.Helper()
	ctx := context.Background()
	if r.Failed != 0 {
		t.Errorf("%d writes failed: %q", r.Failed, r.Errors)
	}
	if r.Inserted == 0 {
		t.Fatal("the load made no writes")
	}
	t.Logf("load: %d inserts, %d updates, %d failed; slowest statement %s", r.Inserted, r.Updated, r.Failed, r.slowest)
	var orders, qty int64
	if err := c.QueryRow(ctx, "SELECT (SELECT count(*) FROM public.orders), (SELECT sum(qty) FROM public.items)").Scan(&orders, &qty); err != nil {
		t.Fatal(err)
	}
	if orders != SeedOrders+r.Inserted {
		t.Errorf("orders: %d rows, want %d (seed %d + inserted %d)", orders, SeedOrders+r.Inserted, SeedOrders, r.Inserted)
	}
	if qty != r.Updated {
		t.Errorf("items: qty sum %d, want %d updates", qty, r.Updated)
	}

	// Row by row: the counts above could hide a lost write and a stray one.
	rows, err := c.Query(ctx, "SELECT writer, n, count(*) FROM public.orders GROUP BY writer, n ORDER BY writer, n")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	have := map[int][]int{}
	var dup []string
	for rows.Next() {
		var w, n, k int
		if err := rows.Scan(&w, &n, &k); err != nil {
			t.Fatal(err)
		}
		if k != 1 {
			dup = append(dup, fmt.Sprintf("(%d,%d)×%d", w, n, k))
		}
		have[w] = append(have[w], n)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(dup) > 0 {
		t.Errorf("orders written more than once: %v", dup)
	}
	if got := len(have[0]); got != SeedOrders {
		t.Errorf("seed orders: %d rows, want %d", got, SeedOrders)
	}
	for w, want := range r.orders {
		got := have[w]
		if !slices.Equal(got, want) {
			t.Errorf("writer %d: %d rows on the final primary, %d acknowledged; first difference %s", w, len(got), len(want), firstDiff(got, want))
		}
	}
	for w := range have {
		if _, ok := r.orders[w]; !ok && w != 0 {
			t.Errorf("orders from unknown writer %d", w)
		}
	}

	skus, err := c.Query(ctx, "SELECT sku, qty FROM public.items ORDER BY sku")
	if err != nil {
		t.Fatal(err)
	}
	defer skus.Close()
	n := 0
	for skus.Next() {
		var sku string
		var q int64
		if err := skus.Scan(&sku, &q); err != nil {
			t.Fatal(err)
		}
		n++
		if q != r.skus[sku] {
			t.Errorf("%s: qty %d, want %d acknowledged updates", sku, q, r.skus[sku])
		}
	}
	if err := skus.Err(); err != nil {
		t.Fatal(err)
	}
	if n != seedItems {
		t.Errorf("items: %d rows, want %d", n, seedItems)
	}
}

func firstDiff(got, want []int) string {
	for i := range min(len(got), len(want)) {
		if got[i] != want[i] {
			return fmt.Sprintf("at index %d: have n=%d, acknowledged n=%d", i, got[i], want[i])
		}
	}
	switch {
	case len(got) > len(want):
		return fmt.Sprintf("extra n=%d", got[len(want)])
	case len(want) > len(got):
		return fmt.Sprintf("missing n=%d", want[len(got)])
	}
	return "none"
}

// Origins counts the load's orders on c by the cluster that accepted them.
func Origins(t testing.TB, c *pgx.Conn) map[string]int64 {
	t.Helper()
	rows, err := c.Query(context.Background(), "SELECT origin, count(*) FROM public.orders WHERE writer > 0 GROUP BY origin")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	m := map[string]int64{}
	for rows.Next() {
		var k string
		var v int64
		if err := rows.Scan(&k, &v); err != nil {
			t.Fatal(err)
		}
		m[k] = v
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return m
}
