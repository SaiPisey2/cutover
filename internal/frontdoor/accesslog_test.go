package frontdoor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	accesslogv3 "github.com/envoyproxy/go-control-plane/envoy/config/accesslog/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	filev3 "github.com/envoyproxy/go-control-plane/envoy/extensions/access_loggers/file/v3"
	hcmv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"
)

// tail starts f on path with a fast poll and returns once f has opened
// the file the first time.
func tail(t *testing.T, path string, perCluster int) *Failures {
	t.Helper()
	f := NewFailures(path, perCluster)
	f.poll = 5 * time.Millisecond
	opened := make(chan struct{}, 16)
	f.onOpen = func() { opened <- struct{}{} }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil && err != context.Canceled {
			t.Errorf("Run: %v", err)
		}
	})
	select {
	case <-opened:
	case <-time.After(5 * time.Second):
		t.Fatal("Run never opened the file")
	}
	return f
}

func appendLines(t *testing.T, path string, lines ...string) {
	t.Helper()
	fh, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()
	for _, l := range lines {
		if _, err := fh.WriteString(l + "\n"); err != nil {
			t.Fatal(err)
		}
	}
}

func line(cluster, reqID string, status int) string {
	return fmt.Sprintf(`{"at":"2026-10-22T10:00:00.123Z","method":"GET","path":"/a","status":%d,"flags":"-","duration_ms":7,"cluster":%q,"request_id":%q}`,
		status, cluster, reqID)
}

// waitRecent polls Recent(cluster) until it has n entries.
func waitRecent(t *testing.T, f *Failures, cluster string, n int) []Failure {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := f.Recent(cluster)
		if len(got) == n {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("Recent(%s) has %d entries, want %d: %+v", cluster, len(got), n, got)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func ids(fs []Failure) []string {
	out := make([]string, len(fs))
	for i, f := range fs {
		out[i] = f.RequestID
	}
	return out
}

func newLog(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "access.log")
	if err := os.WriteFile(p, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestFailuresKeepsTheNewest(t *testing.T) {
	p := newLog(t)
	f := tail(t, p, 50)
	var lines []string
	for i := 0; i < 60; i++ {
		lines = append(lines, line("c_shop_api_target", fmt.Sprint(i), 503))
	}
	appendLines(t, p, lines...)
	waitRecent(t, f, "c_shop_api_target", 50)
	time.Sleep(50 * time.Millisecond) // nothing more arrives
	got := f.Recent("c_shop_api_target")
	if len(got) != 50 || got[0].RequestID != "59" || got[49].RequestID != "10" {
		t.Fatalf("want 59..10 newest first, got %v", ids(got))
	}
	want := Failure{
		At:     time.Date(2026, 10, 22, 10, 0, 0, 123e6, time.UTC),
		Method: "GET", Path: "/a", Status: 503, Flags: "-", DurationMs: 7,
		Cluster: "c_shop_api_target", RequestID: "59",
	}
	if !got[0].At.Equal(want.At) {
		t.Fatalf("at %v, want %v", got[0].At, want.At)
	}
	got[0].At = want.At
	if got[0] != want {
		t.Fatalf("got %+v, want %+v", got[0], want)
	}
	if other := f.Recent("c_shop_api_source"); len(other) != 0 {
		t.Fatalf("another cluster has %v", other)
	}
}

func TestFailuresMapsOwnerAndService(t *testing.T) {
	p := newLog(t)
	f := tail(t, p, 50)
	appendLines(t, p, line("c_shop_api_source", "s", 500), line("c_shop_api_target", "t", 503), line("c_shop_web_target", "w", 503))
	waitRecent(t, f, "c_shop_web_target", 1)
	src, tgt := f.Failures("shop", "api")
	if !slices.Equal(ids(src), []string{"s"}) || !slices.Equal(ids(tgt), []string{"t"}) {
		t.Fatalf("source %v target %v", ids(src), ids(tgt))
	}
}

func TestFailuresStartsAtTheEnd(t *testing.T) {
	p := newLog(t)
	appendLines(t, p, line("c", "old", 503))
	f := tail(t, p, 50)
	appendLines(t, p, line("c", "new", 503))
	if got := waitRecent(t, f, "c", 1); got[0].RequestID != "new" {
		t.Fatalf("got %v; lines from before Run must be skipped", ids(got))
	}
}

func TestFailuresWaitsForTheFile(t *testing.T) {
	p := filepath.Join(t.TempDir(), "access.log")
	f := NewFailures(p, 50)
	f.poll = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil && err != context.Canceled {
			t.Errorf("Run: %v", err)
		}
	}()
	time.Sleep(30 * time.Millisecond)
	// The file did not exist when Run started, so all of it is new.
	appendLines(t, p, line("c", "first", 503))
	if got := waitRecent(t, f, "c", 1); got[0].RequestID != "first" {
		t.Fatalf("got %v", ids(got))
	}
}

func TestFailuresSurvivesTruncation(t *testing.T) {
	p := newLog(t)
	f := tail(t, p, 50)
	appendLines(t, p, line("c", "1", 503), line("c", "2", 503), line("c", "3", 503))
	waitRecent(t, f, "c", 3)
	if err := os.Truncate(p, 0); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // let the tailer see the shrink
	appendLines(t, p, line("c", "4", 503))
	if got := waitRecent(t, f, "c", 4); got[0].RequestID != "4" {
		t.Fatalf("got %v", ids(got))
	}
}

func TestFailuresFollowsRotation(t *testing.T) {
	p := newLog(t)
	f := tail(t, p, 50)
	appendLines(t, p, line("c", "1", 503))
	waitRecent(t, f, "c", 1)
	if err := os.Rename(p, p+".1"); err != nil {
		t.Fatal(err)
	}
	appendLines(t, p+".1", line("c", "2", 503)) // written before the writer reopens
	appendLines(t, p, line("c", "3", 503))      // the new file
	if got := waitRecent(t, f, "c", 3); !slices.Equal(ids(got), []string{"3", "2", "1"}) {
		t.Fatalf("got %v", ids(got))
	}
}

func TestFailuresKeepsAtLeastOne(t *testing.T) {
	for _, n := range []int{0, -3} {
		p := newLog(t)
		f := tail(t, p, n)
		appendLines(t, p, line("c", "1", 503), line("c", "2", 503))
		deadline := time.Now().Add(5 * time.Second)
		for {
			got := f.Recent("c")
			if len(got) == 1 && got[0].RequestID == "2" {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("perCluster %d: got %v, want only the newest", n, ids(got))
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func TestFailuresIgnoresGarbage(t *testing.T) {
	p := newLog(t)
	f := tail(t, p, 50)
	appendLines(t, p,
		"not json",
		`[1,2,3]`,
		`{"cluster":"c","request_id":"a","status":"-","duration_ms":"-","at":"-"}`,
		"",
		`{"cluster":"c","request_id":"b","status":"503","duration_ms":12.0}`,
		`{"cluster":"c","request_id":"c","status":{"x":1},"method":42}`,
		`{"cluster":"c","request_id":"d","status":504`, // cut short
		line("c", "e", 500),
	)
	got := waitRecent(t, f, "c", 4)
	if !slices.Equal(ids(got), []string{"e", "c", "b", "a"}) {
		t.Fatalf("got %v", ids(got))
	}
	if got[3].Status != 0 || got[3].DurationMs != 0 || !got[3].At.IsZero() {
		t.Fatalf(`"-" must read as zero: %+v`, got[3])
	}
	if got[2].Status != 503 || got[2].DurationMs != 12 {
		t.Fatalf("numeric strings: %+v", got[2])
	}
	if got[1].Status != 0 || got[1].Method != "" {
		t.Fatalf("wrong types read as zero: %+v", got[1])
	}
}

func fileAccessLogs(t *testing.T, l Listener) []*accesslogv3.AccessLog {
	t.Helper()
	res, err := Build(nil, nil, l)
	if err != nil {
		t.Fatal(err)
	}
	lis := res[resource.ListenerType][0].(*listenerv3.Listener)
	var hcm hcmv3.HttpConnectionManager
	if err := lis.FilterChains[0].Filters[0].GetTypedConfig().UnmarshalTo(&hcm); err != nil {
		t.Fatal(err)
	}
	return hcm.AccessLog
}

func TestListenerHasAccessLogOnlyWhenSet(t *testing.T) {
	if als := fileAccessLogs(t, lst); len(als) != 0 {
		t.Fatalf("no AccessLog given, yet the listener logs: %v", als)
	}
	l := lst
	l.AccessLog = "/var/lib/cutover/access.log"
	als := fileAccessLogs(t, l)
	if len(als) != 1 {
		t.Fatalf("want one access log, got %d", len(als))
	}
	var file filev3.FileAccessLog
	if err := als[0].GetTypedConfig().UnmarshalTo(&file); err != nil {
		t.Fatal(err)
	}
	if file.Path != l.AccessLog {
		t.Fatalf("path %q", file.Path)
	}
	format := file.GetLogFormat()
	got := map[string]string{}
	for k, v := range format.GetJsonFormat().GetFields() {
		got[k] = v.GetStringValue()
	}
	want := map[string]string{
		"at":          "%START_TIME%",
		"method":      "%REQ(:METHOD)%",
		"path":        "%REQ_WITHOUT_QUERY(:PATH):200%",
		"status":      "%RESPONSE_CODE%",
		"flags":       "%RESPONSE_FLAGS%",
		"duration_ms": "%DURATION%",
		"cluster":     "%UPSTREAM_CLUSTER%",
		"request_id":  "%REQ(X-REQUEST-ID):64%",
	}
	if len(got) != len(want) {
		t.Fatalf("format has keys %v, want exactly %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("format[%q] = %q, want %q (whole format %v)", k, got[k], v, got)
		}
	}
	if fs := format.GetFormatters(); len(fs) != 1 || fs[0].Name != "envoy.formatter.req_without_query" {
		t.Fatalf("formatters %v", fs)
	}
	or := als[0].GetFilter().GetOrFilter()
	if or == nil || len(or.Filters) != 2 {
		t.Fatalf("filter %v, want or(status >= 500, any response flag)", als[0].GetFilter())
	}
	cmp := or.Filters[0].GetStatusCodeFilter().GetComparison()
	if cmp.GetOp() != accesslogv3.ComparisonFilter_GE || cmp.GetValue().GetDefaultValue() != 500 ||
		cmp.GetValue().GetRuntimeKey() != "cutover.access_log.min_status" {
		t.Fatalf("status filter %v", cmp)
	}
	if rf := or.Filters[1].GetResponseFlagFilter(); rf == nil || len(rf.Flags) != 0 {
		t.Fatalf("response flag filter %v, want one matching any flag", or.Filters[1])
	}
}
