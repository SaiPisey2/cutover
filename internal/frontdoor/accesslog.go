package frontdoor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"math"
	"os"
	"strconv"
	"sync"
	"time"
)

// Failure is one failed request Envoy wrote to the access log. The
// listener logs no headers or bodies, and the path carries no query.
type Failure struct {
	At         time.Time `json:"at"`
	Method     string    `json:"method"`
	Path       string    `json:"path"`
	Status     int       `json:"status"`
	Flags      string    `json:"flags"`
	DurationMs int       `json:"duration_ms"`
	Cluster    string    `json:"cluster"`
	RequestID  string    `json:"request_id"`
}

// maxLine bounds one access-log line. Envoy's lines are a few hundred
// bytes (the path is cut at 200); anything longer is not ours.
const maxLine = 64 << 10

// Failures follows Envoy's access log and keeps the newest failures of
// each upstream cluster.
type Failures struct {
	path       string
	perCluster int
	poll       time.Duration
	onOpen     func() // tests: called after each open

	mu        sync.Mutex
	byCluster map[string][]Failure // oldest first
}

// NewFailures returns a Failures that keeps at most perCluster entries
// per cluster (at least 1) from the access log at path. Run starts it.
func NewFailures(path string, perCluster int) *Failures {
	perCluster = max(perCluster, 1)
	return &Failures{path: path, perCluster: perCluster, poll: 250 * time.Millisecond, byCluster: map[string][]Failure{}}
}

// Recent returns cluster's failures, newest first.
func (f *Failures) Recent(cluster string) []Failure {
	f.mu.Lock()
	defer f.mu.Unlock()
	kept := f.byCluster[cluster]
	out := make([]Failure, len(kept))
	for i, x := range kept {
		out[len(kept)-1-i] = x
	}
	return out
}

// Failures returns the recent failures of a route's source and target
// clusters, newest first.
func (f *Failures) Failures(owner, service string) (source, target []Failure) {
	return f.Recent(ClusterName(owner, service, SideSource)), f.Recent(ClusterName(owner, service, SideTarget))
}

func (f *Failures) add(x Failure) {
	f.mu.Lock()
	defer f.mu.Unlock()
	kept := append(f.byCluster[x.Cluster], x)
	if len(kept) > f.perCluster {
		kept = append(kept[:0:0], kept[len(kept)-f.perCluster:]...)
	}
	f.byCluster[x.Cluster] = kept
}

// Run follows the access log until ctx ends. It waits for the file to
// appear. A file that exists when Run starts is read from its end, since
// what it holds came before this run; a file that appears later, or
// replaces a rotated one, is read whole. It survives truncation (the
// file shrinks: read again from its start) and rotation (another file
// takes the path: drain the old one, then open the new). Lines
// that are not JSON objects are skipped.
func (f *Failures) Run(ctx context.Context) error {
	_, err := os.Stat(f.path)
	fromStart := errors.Is(err, fs.ErrNotExist)
	for {
		fh, err := f.open(ctx)
		if err != nil {
			return err
		}
		if !fromStart {
			if _, err := fh.Seek(0, io.SeekEnd); err != nil {
				fh.Close()
				return err
			}
		}
		fromStart = true
		if f.onOpen != nil {
			f.onOpen()
		}
		err = f.follow(ctx, fh)
		fh.Close()
		if err != nil {
			return err
		}
	}
}

// open waits until the access log can be opened.
func (f *Failures) open(ctx context.Context) (*os.File, error) {
	for {
		fh, err := os.Open(f.path)
		if err == nil {
			return fh, nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
		if err := f.sleep(ctx); err != nil {
			return nil, err
		}
	}
}

func (f *Failures) sleep(ctx context.Context) error {
	t := time.NewTimer(f.poll)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// follow reads fh until ctx ends (returning its error) or another file
// takes the path (returning nil once fh is drained).
func (f *Failures) follow(ctx context.Context, fh *os.File) error {
	offset, err := fh.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	buf := make([]byte, 32<<10)
	var pending []byte
	discarding := false // inside an over-long line
	rotated := false
	for {
		n, err := fh.Read(buf)
		if n > 0 {
			offset += int64(n)
			data := buf[:n]
			for {
				i := bytes.IndexByte(data, '\n')
				if i < 0 {
					if !discarding {
						pending = append(pending, data...)
						if len(pending) > maxLine {
							pending, discarding = pending[:0], true
						}
					}
					break
				}
				if !discarding {
					pending = append(pending, data[:i]...)
					if x, ok := decodeFailure(pending); ok {
						f.add(x)
					}
				}
				pending, discarding = pending[:0], false
				data = data[i+1:]
			}
			continue
		}
		if err != nil && err != io.EOF {
			return err
		}
		// At the end of fh.
		if rotated {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := f.sleep(ctx); err != nil {
			return err
		}
		cur, err := fh.Stat()
		if err != nil {
			return err
		}
		// While the path names no file (mid-rotation, or removed), the
		// writer still holds the old one: keep reading it.
		if named, err := os.Stat(f.path); err == nil && !os.SameFile(named, cur) {
			// Another file took the path: read what the old one
			// still holds, then reopen.
			rotated = true
			continue
		}
		if cur.Size() < offset {
			if _, err := fh.Seek(0, io.SeekStart); err != nil {
				return err
			}
			offset, pending, discarding = 0, pending[:0], false
		}
	}
}

// decodeFailure reads one access-log line. Envoy writes "-" for a
// missing value; numbers that are "-", or not numbers at all, read as 0.
func decodeFailure(line []byte) (Failure, bool) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(line, &m); err != nil || m == nil {
		return Failure{}, false
	}
	x := Failure{
		Method:     jsonString(m["method"]),
		Path:       jsonString(m["path"]),
		Status:     jsonInt(m["status"]),
		Flags:      jsonString(m["flags"]),
		DurationMs: jsonInt(m["duration_ms"]),
		Cluster:    jsonString(m["cluster"]),
		RequestID:  jsonString(m["request_id"]),
	}
	if t, err := time.Parse(time.RFC3339Nano, jsonString(m["at"])); err == nil {
		x.At = t
	}
	return x, true
}

func jsonString(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) != nil {
		return ""
	}
	return s
}

func jsonInt(raw json.RawMessage) int {
	var n float64
	if json.Unmarshal(raw, &n) == nil {
		return clampInt(n)
	}
	if n, err := strconv.ParseFloat(jsonString(raw), 64); err == nil {
		return clampInt(n)
	}
	return 0
}

func clampInt(n float64) int {
	if math.IsNaN(n) || n < 0 || n > math.MaxInt32 {
		return 0
	}
	return int(n)
}
