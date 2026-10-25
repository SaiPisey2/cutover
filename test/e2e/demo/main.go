// Command demo is the small application the e2e run migrates: an HTTP API
// and a worker that both write to Postgres.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// failFile is a var so tests can point it at a temp dir.
var failFile = "/run/demo/fail"

var errNotFound = errors.New("not found")

type order struct {
	ID   int64  `json:"id"`
	Note string `json:"note"`
}

type store interface {
	InsertOrder(ctx context.Context, note string) (int64, error)
	GetOrder(ctx context.Context, id int64) (order, error)
	InsertTick(ctx context.Context, pod string, seq int64) error
}

func main() {
	if len(os.Args) != 2 || (os.Args[1] != "api" && os.Args[1] != "worker") {
		fmt.Fprintln(os.Stderr, "usage: demo api|worker")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	s, err := openStore(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("demo: %v", err)
	}
	host, _ := os.Hostname()

	if os.Args[1] == "worker" {
		pod := os.Getenv("POD_NAME")
		if pod == "" {
			pod = host
		}
		runWorker(ctx, s, pod, 200*time.Millisecond, 100*time.Millisecond, time.Second, log.Printf)
		return
	}

	srv := &http.Server{Addr: ":8080", Handler: newMux(s)}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("demo: %v", err)
	}
}

func newMux(s store) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, "ok")
	})
	mux.HandleFunc("POST /orders", func(w http.ResponseWriter, r *http.Request) {
		note, err := io.ReadAll(io.LimitReader(r.Body, 1<<16))
		if err != nil {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		id, err := s.InsertOrder(r.Context(), string(note))
		if err != nil {
			http.Error(w, "insert failed", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(struct {
			ID int64 `json:"id"`
		}{id})
	})
	mux.HandleFunc("GET /orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil {
			http.Error(w, "bad id", http.StatusBadRequest)
			return
		}
		o, err := s.GetOrder(r.Context(), id)
		if errors.Is(err, errNotFound) {
			http.NotFound(w, r)
			return
		}
		if err != nil {
			http.Error(w, "lookup failed", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(o)
	})

	host, _ := os.Hostname()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Demo-Host", host)
		// The fault is injected by touching a file in the pod, so /healthz
		// must stay up or the kubelet would restart the pod and clear it.
		if r.URL.Path != "/healthz" {
			if _, err := os.Stat(failFile); err == nil {
				http.Error(w, "injected fault", http.StatusServiceUnavailable)
				return
			}
		}
		mux.ServeHTTP(w, r)
	})
}

// runWorker inserts one tick per interval until ctx ends. A failed insert is
// retried with the same seq so the table never has a gap.
func runWorker(ctx context.Context, s store, pod string, interval, minBackoff, maxBackoff time.Duration, logf func(string, ...any)) {
	for seq := int64(1); ; seq++ {
		backoff := minBackoff
		for {
			err := s.InsertTick(ctx, pod, seq)
			if err == nil {
				break
			}
			logf("worker: insert tick %s/%d: %v", pod, seq, err)
			if !sleep(ctx, backoff) {
				return
			}
			backoff = min(backoff*2, maxBackoff)
		}
		if !sleep(ctx, interval) {
			return
		}
	}
}

// sleep reports false if ctx ended first.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

type pgStore struct{ pool *pgxpool.Pool }

func openStore(ctx context.Context, url string) (*pgStore, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, errors.New("invalid DATABASE_URL")
	}
	// PgBouncer runs in transaction mode, which cannot hold prepared statements.
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	for _, ddl := range []string{
		`CREATE TABLE IF NOT EXISTS orders (id bigserial PRIMARY KEY, note text, at timestamptz DEFAULT now())`,
		`CREATE TABLE IF NOT EXISTS ticks (pod text, seq bigint, at timestamptz DEFAULT now(), PRIMARY KEY (pod, seq))`,
	} {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			pool.Close()
			return nil, fmt.Errorf("create table: %w", err)
		}
	}
	return &pgStore{pool}, nil
}

func (p *pgStore) InsertOrder(ctx context.Context, note string) (int64, error) {
	var id int64
	err := p.pool.QueryRow(ctx, `INSERT INTO orders (note) VALUES ($1) RETURNING id`, note).Scan(&id)
	return id, err
}

func (p *pgStore) GetOrder(ctx context.Context, id int64) (order, error) {
	o := order{ID: id}
	err := p.pool.QueryRow(ctx, `SELECT note FROM orders WHERE id = $1`, id).Scan(&o.Note)
	if errors.Is(err, pgx.ErrNoRows) {
		return order{}, errNotFound
	}
	return o, err
}

// InsertTick is idempotent: a retry after a lost reply, or a restarted pod
// reusing its name, must not wedge the worker on the primary key.
func (p *pgStore) InsertTick(ctx context.Context, pod string, seq int64) error {
	_, err := p.pool.Exec(ctx, `INSERT INTO ticks (pod, seq) VALUES ($1, $2) ON CONFLICT DO NOTHING`, pod, seq)
	return err
}
