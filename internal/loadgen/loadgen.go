// Package loadgen drives steady HTTP load against the demo API and records
// every acknowledged order id and every failed request.
package loadgen

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const maxSamples = 50

type Config struct {
	Target   string // base URL, e.g. http://127.0.0.1:18080
	Host     string // Host header, empty keeps the target's
	Writers  int
	Readers  int
	Interval time.Duration // pause between requests per worker
	Timeout  time.Duration // per request
}

// Failure describes one failed request: a transport error, a timeout or a
// non-2xx status.
type Failure struct {
	// At is when the request was started, not when it failed. Callers compare
	// it against when a migration gate was observed.
	At     time.Time
	Method string
	Path   string
	Status int // 0 when there was no response
	Err    string
}

type Report struct {
	Requests       int
	Failures       int
	IDs            []int64 // ids the server acknowledged with a 2xx
	FailureSamples []Failure
	Started        time.Time
	Stopped        time.Time
}

type Run struct {
	cfg    Config
	client *http.Client
	cancel context.CancelFunc
	ctx    context.Context
	wg     sync.WaitGroup

	seq     atomic.Int64
	mu      sync.Mutex
	rep     Report
	stopped bool
	final   Report
}

// Start launches the workers. They run until Stop or until ctx is done.
func Start(ctx context.Context, cfg Config) *Run {
	ctx, cancel := context.WithCancel(ctx)
	conns := cfg.Writers + cfg.Readers
	// A pooled client: a fresh connection per request would measure the dialer.
	tr := &http.Transport{MaxIdleConns: conns, MaxIdleConnsPerHost: conns, IdleConnTimeout: 30 * time.Second}
	r := &Run{
		cfg:    cfg,
		client: &http.Client{Transport: tr, Timeout: cfg.Timeout},
		cancel: cancel,
		ctx:    ctx,
	}
	r.rep.Started = time.Now()
	for i := 0; i < cfg.Writers; i++ {
		r.wg.Add(1)
		go r.loop(r.write)
	}
	for i := 0; i < cfg.Readers; i++ {
		r.wg.Add(1)
		go r.loop(r.read)
	}
	return r
}

// Stop ends the load, waits for the workers and returns the report. Requests
// cut short by Stop itself are not counted. Calling it again returns the same
// report.
func (r *Run) Stop() Report {
	r.cancel()
	r.wg.Wait()
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.stopped {
		r.stopped = true
		r.rep.Stopped = time.Now()
		r.final = r.rep
		r.final.IDs = append([]int64(nil), r.rep.IDs...)
		r.final.FailureSamples = append([]Failure(nil), r.rep.FailureSamples...)
	}
	return r.final
}

func (r *Run) loop(op func()) {
	defer r.wg.Done()
	for {
		if r.ctx.Err() != nil {
			return
		}
		op()
		select {
		case <-r.ctx.Done():
			return
		case <-time.After(r.cfg.Interval):
		}
	}
}

func (r *Run) write() {
	body := fmt.Sprintf("lg-%d", r.seq.Add(1))
	status, resp, at, err := r.do(http.MethodPost, "/orders", body)
	var id int64
	if err == nil && status/100 == 2 {
		var out struct {
			ID int64 `json:"id"`
		}
		if jerr := json.Unmarshal(resp, &out); jerr != nil || out.ID == 0 {
			err = fmt.Errorf("unreadable order id in %q", resp)
		} else {
			id = out.ID
		}
	}
	r.record(http.MethodPost, "/orders", status, at, err, id)
}

func (r *Run) read() {
	path := "/healthz"
	r.mu.Lock()
	if n := len(r.rep.IDs); n > 0 {
		path = fmt.Sprintf("/orders/%d", r.rep.IDs[rand.IntN(n)])
	}
	r.mu.Unlock()
	status, _, at, err := r.do(http.MethodGet, path, "")
	r.record(http.MethodGet, path, status, at, err, 0)
}

func (r *Run) do(method, path, body string) (status int, resp []byte, at time.Time, err error) {
	req, err := http.NewRequestWithContext(r.ctx, method, strings.TrimRight(r.cfg.Target, "/")+path, strings.NewReader(body))
	if err != nil {
		return 0, nil, time.Now(), err
	}
	if r.cfg.Host != "" {
		req.Host = r.cfg.Host
	}
	at = time.Now()
	res, err := r.client.Do(req)
	if err != nil {
		return 0, nil, at, err
	}
	defer res.Body.Close()
	resp, err = io.ReadAll(io.LimitReader(res.Body, 1<<16))
	return res.StatusCode, resp, at, err
}

func (r *Run) record(method, path string, status int, at time.Time, err error, id int64) {
	if err != nil && r.ctx.Err() != nil {
		return // aborted by Stop; a completed response is counted even so
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rep.Requests++
	if err == nil && status/100 == 2 {
		if id != 0 {
			r.rep.IDs = append(r.rep.IDs, id)
		}
		return
	}
	r.rep.Failures++
	if len(r.rep.FailureSamples) < maxSamples {
		f := Failure{At: at, Method: method, Path: path, Status: status}
		if err != nil {
			f.Err = err.Error()
		} else {
			f.Err = fmt.Sprintf("status %d", status)
		}
		r.rep.FailureSamples = append(r.rep.FailureSamples, f)
	}
}
