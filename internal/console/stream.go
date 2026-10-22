package console

// This file is GET /api/stream[?migration=ID]: Server-Sent Events that
// keep the console live. hello {} comes first. Then migrations (the list,
// as GET /api/migrations answers it) whenever it changes, and with
// ?migration status (as GET /api/migrations/{id} answers it) whenever its
// JSON changes; unavailable {} once when Temporal starts failing; a
// ": ping" comment as a heartbeat; and expired {} when the session ends,
// after which the stream closes.
//
// Temporal is polled by one hub per server, not per stream: the list
// while anyone listens, and each migration's status while anyone watches
// it. Streams only compare the hub's latest answers with what they last
// sent, so ten tabs on one migration cost Temporal what one does.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/SaiPisey2/cutover/internal/plan"
	"github.com/SaiPisey2/cutover/internal/store"
)

// Vars, not consts, so a test can poll in milliseconds instead of waiting
// on the real clock.
var (
	// streamPoll is how often the hub asks for each watched migration's
	// status, and how often each stream re-reads its session.
	streamPoll = time.Second
	// streamListPoll is how often the hub asks for the migration list.
	streamListPoll = 2 * time.Second
	// streamHeartbeat keeps idle proxies from closing the connection and
	// surfaces a dead client as a write error.
	streamHeartbeat = 15 * time.Second
	// streamWriteTimeout bounds each write. A browser that stops reading
	// fills the socket buffers and then blocks the write forever; the
	// deadline turns that into an error that ends the stream and frees
	// its slot. It is set before each write and cleared after it, and it
	// is longer than the heartbeat, so even a deadline left behind is
	// renewed by the next ping before it can fire on an idle stream
	// (TestHeartbeatIsShorterThanTheWriteTimeout).
	streamWriteTimeout = 20 * time.Second
)

const (
	// A UI opens one stream per tab; four covers a few tabs, and the
	// total bounds what a handful of approvers (or one stolen cookie) can
	// hold open.
	maxStreamsPerSession = 4
	maxStreams           = 32

	errStreams      = `{"error":"too many live streams"}`
	errShuttingDown = `{"error":"server is shutting down"}`
)

var emptyObject = []byte("{}")

// streamSlots counts live streams per session (keyed by the cookie's
// hash, never the cookie) and in total. The lock is only ever held to
// count, never across a write to a client.
type streamSlots struct {
	mu    sync.Mutex
	total int
	per   map[string]int
}

func (s *streamSlots) acquire(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.total >= maxStreams || s.per[key] >= maxStreamsPerSession {
		return false
	}
	s.total++
	s.per[key]++
	return true
}

func (s *streamSlots) release(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.total--
	// Deleted at zero, so sessions that come and go do not pile up keys.
	if s.per[key]--; s.per[key] <= 0 {
		delete(s.per, key)
	}
}

// ---- the hub ----

// snapshot is the hub's latest answer to one question.
type snapshot struct {
	data []byte // the last good answer as JSON; nil before the first
	down bool   // the last poll failed
}

// hub is the one poller behind every stream. It runs only while someone
// listens: the first subscriber starts its goroutines and the last one
// out stops them. Its context (Deps.Context) ends it for good.
type hub struct {
	ctx context.Context
	api *api
	log *slog.Logger
	wg  sync.WaitGroup // every goroutine the hub starts

	mu    sync.Mutex
	subs  int
	watch map[string]int // migration -> streams watching it
	// gen names the running poller; a poll from a stopped one is
	// dropped. loopCtx and stop are nil while nothing runs.
	gen     uint64
	loopCtx context.Context
	stop    context.CancelFunc
	list    snapshot
	status  map[string]snapshot
	// changed is closed, and replaced, whenever a snapshot changes.
	changed chan struct{}
}

func newHub(ctx context.Context, a *api, log *slog.Logger) *hub {
	if ctx == nil {
		ctx = context.Background()
	}
	return &hub{ctx: ctx, api: a, log: log, watch: map[string]int{},
		status: map[string]snapshot{}, changed: make(chan struct{})}
}

// subscribe registers a stream, watching migration id unless id is "".
// It is false once the hub's context has ended.
func (b *hub) subscribe(id string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ctx.Err() != nil {
		return false
	}
	b.subs++
	if b.stop == nil {
		b.gen++
		b.loopCtx, b.stop = context.WithCancel(b.ctx)
		b.wg.Add(2)
		go b.listLoop(b.loopCtx, b.gen)
		go b.statusLoop(b.loopCtx, b.gen)
	}
	if id != "" {
		b.watch[id]++
		if b.watch[id] == 1 {
			// A new migration is asked at once, not at the next tick.
			ctx, gen := b.loopCtx, b.gen
			b.wg.Add(1)
			go func() {
				defer b.wg.Done()
				b.pollStatus(ctx, gen, id)
			}()
		}
	}
	return true
}

func (b *hub) unsubscribe(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subs--
	if id != "" {
		if b.watch[id]--; b.watch[id] <= 0 {
			delete(b.watch, id)
			delete(b.status, id)
		}
	}
	if b.subs == 0 && b.stop != nil {
		// Nobody listens: stop polling, and forget the answers so the next
		// stream does not start from stale ones.
		b.stop()
		b.stop, b.loopCtx = nil, nil
		b.gen++
		b.list = snapshot{}
		clear(b.status)
	}
}

// view is the latest list and, unless id is "", status, with the channel
// that closes when either may have changed.
func (b *hub) view(id string) (list, status snapshot, changed <-chan struct{}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if id != "" {
		status = b.status[id]
	}
	return b.list, status, b.changed
}

func (b *hub) listLoop(ctx context.Context, gen uint64) {
	defer b.wg.Done()
	t := time.NewTicker(streamListPoll)
	defer t.Stop()
	for {
		b.pollList(ctx, gen)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (b *hub) statusLoop(ctx context.Context, gen uint64) {
	defer b.wg.Done()
	t := time.NewTicker(streamPoll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		b.mu.Lock()
		ids := make([]string, 0, len(b.watch))
		for id := range b.watch {
			ids = append(ids, id)
		}
		b.mu.Unlock()
		var wg sync.WaitGroup
		for _, id := range ids {
			wg.Add(1)
			go func() {
				defer wg.Done()
				b.pollStatus(ctx, gen, id)
			}()
		}
		wg.Wait()
	}
}

func (b *hub) pollList(ctx context.Context, gen uint64) {
	tc, cancel := context.WithTimeout(ctx, temporalTimeout)
	list, err := b.api.d.Temporal.List(tc)
	cancel()
	if ctx.Err() != nil {
		return // stopped: neither an answer nor an outage
	}
	var data []byte
	if err == nil {
		data, err = json.Marshal(migrationRows(list))
	}
	b.set(gen, "", data, err)
}

func (b *hub) pollStatus(ctx context.Context, gen uint64, id string) {
	out, err := b.api.statusReply(ctx, id)
	if ctx.Err() != nil {
		return
	}
	var data []byte
	if err == nil {
		data, err = json.Marshal(out)
	}
	b.set(gen, id, data, err)
}

// set records one poll's answer and wakes the streams if it changed. A
// missing migration is not an outage; it leaves the last answer as it was.
func (b *hub) set(gen uint64, id string, data []byte, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if gen != b.gen || (id != "" && b.watch[id] == 0) {
		return
	}
	cur := b.list
	if id != "" {
		cur = b.status[id]
	}
	next := cur
	switch {
	case err == nil:
		next.down, next.data = false, data
	case errors.Is(err, ErrNoMigration):
		next.down = false
	default:
		// Logged once per outage, not once per poll.
		if !cur.down {
			lvl := slog.LevelError
			if errors.Is(err, ErrTemporal) {
				lvl = slog.LevelWarn
			}
			b.log.Log(context.Background(), lvl, "console stream: poll failed", "migration", id, "err", err)
		}
		next.down = true
	}
	if next.down == cur.down && bytes.Equal(next.data, cur.data) {
		return
	}
	if id == "" {
		b.list = next
	} else {
		b.status[id] = next
	}
	close(b.changed)
	b.changed = make(chan struct{})
}

// ---- one stream ----

// sse writes events to one client. Every write gets a fresh deadline and
// is flushed at once: an event sitting in a buffer is a feed that looks
// live and is not.
type sse struct {
	w  http.ResponseWriter
	rc *http.ResponseController
}

func (s *sse) write(b []byte) error {
	// A writer without deadlines (a wrapper that does not Unwrap) only
	// loses the protection against a client that never reads.
	if err := s.setDeadline(time.Now().Add(streamWriteTimeout)); err != nil {
		return err
	}
	if _, err := s.w.Write(b); err != nil {
		return err
	}
	if err := s.rc.Flush(); err != nil {
		return err
	}
	// The deadline covers the write and nothing after it. Over HTTP/2 it
	// is a timer on the whole stream that fires whether or not anything is
	// being written: left set, it resets an idle stream with
	// INTERNAL_ERROR one write timeout after the last event.
	return s.setDeadline(time.Time{})
}

func (s *sse) setDeadline(t time.Time) error {
	if err := s.rc.SetWriteDeadline(t); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	return nil
}

// event sends one named event. data is always json.Marshal output (or
// "{}"), which never holds a raw newline, so a value cannot split into a
// second, forged event.
func (s *sse) event(name string, data []byte) error {
	msg := make([]byte, 0, len(data)+len(name)+16)
	msg = append(msg, "event: "+name+"\ndata: "...)
	msg = append(msg, data...)
	msg = append(msg, "\n\n"...)
	return s.write(msg)
}

func (h *api) stream(w http.ResponseWriter, r *http.Request) {
	if _, ok := h.auth.Session(w, r); !ok {
		return
	}
	c, _ := r.Cookie(SessionCookie) // Session found it
	mig := r.URL.Query().Get("migration")
	if mig != "" && !plan.NamePattern.MatchString(mig) {
		writeJSON(w, http.StatusBadRequest, errBadQuery)
		return
	}
	idHash := hashOf(c.Value)
	key := string(idHash)
	// Refused before any SSE header is written, so the browser sees a
	// plain JSON 429 rather than a stream that ends at once and retries.
	if !h.slots.acquire(key) {
		writeJSON(w, http.StatusTooManyRequests, errStreams)
		return
	}
	defer h.slots.release(key)
	if !h.hub.subscribe(mig) {
		writeJSON(w, http.StatusServiceUnavailable, errShuttingDown)
		return
	}
	defer h.hub.unsubscribe(mig)

	hdr := w.Header()
	hdr.Set("Content-Type", "text/event-stream")
	hdr.Set("Cache-Control", "no-store")
	// A reverse proxy (nginx) that buffers responses would hold events
	// until its buffer filled.
	hdr.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	s := &sse{w: w, rc: http.NewResponseController(w)}
	if s.event("hello", emptyObject) != nil {
		return
	}

	ctx := r.Context()
	st := &streamState{s: s, mig: mig}
	pt := time.NewTicker(streamPoll)
	defer pt.Stop()
	ht := time.NewTicker(streamHeartbeat)
	defer ht.Stop()
	for {
		list, status, changed := h.hub.view(mig)
		if st.send(list, status) != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-h.hub.ctx.Done():
			return
		case <-changed:
		case <-pt.C:
			// The session Session saw may since have been revoked or
			// expired: re-read it by hash.
			if !h.sessionLive(ctx, s, idHash) {
				return
			}
		case <-ht.C:
			// A comment: EventSource ignores it.
			if s.write([]byte(": ping\n\n")) != nil {
				return
			}
		}
	}
}

// sessionLive re-reads the stream's session. When it is gone it sends
// expired and is false; a store error also ends the stream (the browser
// reconnects on its own).
func (h *api) sessionLive(ctx context.Context, s *sse, idHash []byte) bool {
	_, err := h.auth.Store.SessionByHash(ctx, idHash, h.auth.Now())
	if errors.Is(err, store.ErrNotFound) {
		// data is required: EventSource drops an event with no data line
		// without dispatching it, and the UI would never sign out.
		s.event("expired", emptyObject)
		return false
	}
	if err != nil {
		if ctx.Err() == nil {
			h.log.Error("console stream: session lookup", "err", err)
		}
		return false
	}
	return true
}

// streamState is what one stream last sent.
type streamState struct {
	s      *sse
	mig    string
	list   []byte
	status []byte
	down   bool // in an outage already reported
}

// send writes what changed since the last call: unavailable once when an
// outage begins, then any answer whose JSON differs from the last sent.
// Recovery itself sends nothing beyond the answers that changed.
func (st *streamState) send(list, status snapshot) error {
	down := list.down || (st.mig != "" && status.down)
	if down && !st.down {
		if err := st.s.event("unavailable", emptyObject); err != nil {
			return err
		}
	}
	st.down = down
	if list.data != nil && !bytes.Equal(list.data, st.list) {
		if err := st.s.event("migrations", list.data); err != nil {
			return err
		}
		st.list = list.data
	}
	if st.mig != "" && status.data != nil && !bytes.Equal(status.data, st.status) {
		if err := st.s.event("status", status.data); err != nil {
			return err
		}
		st.status = status.data
	}
	return nil
}
