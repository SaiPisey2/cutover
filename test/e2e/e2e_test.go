//go:build e2e

package e2e

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/SaiPisey2/cutover/internal/console"
	"github.com/SaiPisey2/cutover/internal/flow"
	"github.com/SaiPisey2/cutover/internal/loadgen"
	"github.com/SaiPisey2/cutover/internal/ops"
	"github.com/SaiPisey2/cutover/internal/pgbouncer"
	"github.com/SaiPisey2/cutover/internal/sshx"
	"github.com/SaiPisey2/cutover/internal/svc"
)

// TestE2E shares one setup: a migration that rolled back is closed, so
// migration-shop can start again in the next subtest.
func TestE2E(t *testing.T) {
	e := setup(t)
	t.Run("rollback-on-target-fault", func(t *testing.T) { rollbackOnTargetFault(t, e) })
	t.Run("cutover-under-load", func(t *testing.T) { cutoverUnderLoad(t, e) })
}

// rollbackOnTargetFault breaks the target API once the ramp is about to
// start: the error tripwire must move traffic back to the source by itself,
// and rejecting the trip undoes Prepare. It leaves the fixture clean for the
// cutover.
func rollbackOnTargetFault(t *testing.T, e *env) {
	requireClean(t, e)
	planPath := writePlan(t, e)
	routeApply(t, e, planPath)
	ctx := context.Background()

	var mu sync.Mutex
	var failures []loadgen.Failure
	lg := loadgen.Start(ctx, loadgen.Config{Target: "http://" + listenAddr, Host: "localhost",
		Writers: 4, Readers: 2, Interval: 50 * time.Millisecond, Timeout: 15 * time.Second,
		OnFailure: func(f loadgen.Failure) {
			mu.Lock()
			failures = append(failures, f)
			mu.Unlock()
		}})
	defer lg.Stop()

	auditFrom := lastAuditID(t, e)
	y, err := os.ReadFile(planPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.alice.Start(ctx, migrationID, y); err != nil {
		t.Fatalf("alice starts %s: %v", migrationID, err)
	}
	before := []string{"discover", "prepare", "rehearse"}
	drive(t, e, "shift-10", func(g flow.Gate) (*console.Client, bool) {
		if !slices.Contains(before, g.Name) {
			t.Fatalf("unexpected gate %s before the ramp: %s", g.Name, g.Summary)
		}
		return e.alice, true
	})

	injectFault(t, ctx)
	approved := time.Now()
	if err := e.alice.Decide(ctx, migrationID, "shift-10", flow.Approve, "e2e"); err != nil {
		t.Fatalf("alice approves shift-10: %v", err)
	}
	st := waitStatus(t, e, "the tripwire", 2*time.Minute, 50*time.Millisecond, func(st flow.Status) bool {
		return (st.Gate != nil && st.Gate.Name != "shift-10") || st.State == flow.StateRolledBack ||
			st.State == flow.StateFailed || st.State == flow.StateFinished
	})
	tripped := time.Now()
	if st.Gate == nil || st.Gate.Name != "shift-tripped" {
		t.Fatalf("after shift-10: state %s, gate %s, want the shift-tripped gate:%s", st.State, gateString(st.Gate), phaseEvents(t, e, flow.PhaseShift))
	}
	tripTime := tripped.Sub(approved)
	t.Logf("tripped %s after the approval:%s", tripTime.Round(time.Millisecond), phaseEvents(t, e, flow.PhaseShift))

	if err := e.alice.Decide(ctx, migrationID, "shift-tripped", flow.Reject, "e2e"); err != nil {
		t.Fatalf("alice rejects shift-tripped: %v", err)
	}
	st = drive(t, e, flow.StateRolledBack, func(g flow.Gate) (*console.Client, bool) {
		t.Fatalf("unexpected gate %s during the rollback: %s", g.Name, g.Summary)
		return nil, false
	})
	rep := lg.Stop()
	mu.Lock()
	defer mu.Unlock()

	t.Logf("load: %d requests, %d failures, %d acknowledged orders", rep.Requests, rep.Failures, len(rep.IDs))
	if limit := 8*time.Second + 2*time.Second; tripTime > limit {
		t.Errorf("trip: the shift-tripped gate opened %s after the approval, want at most %s", tripTime, limit)
	}
	if len(failures) != rep.Failures {
		t.Errorf("load: saw %d failures, the report counts %d", len(failures), rep.Failures)
	}
	other, late := 0, 0
	for _, f := range failures {
		if f.Status != http.StatusServiceUnavailable || f.Body != "injected fault" {
			if other++; other <= 10 {
				t.Errorf("load: failure other than the injected fault: %s %s status %d %q (%s) at %s",
					f.Method, f.Path, f.Status, f.Body, f.Err, f.At.Format("15:04:05.000"))
			}
		}
		if !f.At.Before(tripped) {
			if late++; late <= 10 {
				t.Errorf("load: %s %s started at %s, after the trip was seen at %s, failed: %s %q",
					f.Method, f.Path, f.At.Format("15:04:05.000"), tripped.Format("15:04:05.000"), f.Err, f.Body)
			}
		}
	}
	if other > 0 || late > 0 {
		t.Errorf("load: %d of %d failures were not the injected fault, %d started after the trip", other, len(failures), late)
	}
	if st.Primary != ops.Source {
		t.Errorf("primary: %s, want source", st.Primary)
	}
	checkRouteLine(t, e, planPath, "source "+apiA+"  target -  weight 0%")
	checkUndone(t)

	db, err := pgConn(ctx, pgAPort, e.dbPassword)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(context.Background())
	checkOrders(t, ctx, db, "pg-a", rep.IDs)

	rows, _ := audit(t, e)
	rejected := false
	for _, r := range rows {
		if r.ID <= auditFrom {
			continue
		}
		if strings.HasSuffix(r.Kind, "-failed") {
			t.Errorf("audit: %s by %s at gate %q: %s", r.Kind, r.ActorName, r.Gate, r.Note)
		}
		if r.Kind == "gate-decision" && r.Gate == "shift-tripped" && r.Decision == string(flow.Reject) && r.ActorName == "alice" {
			rejected = true
		}
	}
	if !rejected {
		t.Error("audit: no reject of shift-tripped by alice")
	}
	requireClean(t, e)
}

// injectFault makes the target API fail every request but /healthz (D5),
// and checks that it does. The image is FROM scratch, with no shell or
// touch to exec, so the fail file goes in with docker cp.
func injectFault(t *testing.T, ctx context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	h, err := sshx.Dial(ctx, sshHost(t, sshBPort))
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	ids, err := h.Run(ctx, "docker ps -q --filter label=com.docker.compose.project=cutover-shop-api", nil)
	if err != nil {
		t.Fatal(err)
	}
	f := strings.Fields(string(ids))
	if len(f) != 1 {
		t.Fatalf("host b: containers of cutover-shop-api %q, want one", f)
	}
	var archive bytes.Buffer
	tw := tar.NewWriter(&archive)
	if err := tw.WriteHeader(&tar.Header{Name: "fail", Mode: 0o644, Typeflag: tar.TypeReg, ModTime: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Run(ctx, "docker cp - "+f[0]+":/run/demo", archive.Bytes()); err != nil {
		t.Fatalf("host b: inject the fault: %v", err)
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+apiB+"/orders/1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || strings.TrimSpace(string(body)) != "injected fault" {
		t.Fatalf("target api after the injection: %s %q, want 503 injected fault", resp.Status, body)
	}
	t.Logf("injected the fault into %s on host b at %s", f[0], time.Now().Format("15:04:05.000"))
}

// checkUndone: Prepare's undo removed every copy it made in cloud B and
// left PgBouncer on pg-a.
func checkUndone(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := composeProjects(ctx, t, sshBPort, ""); err != nil {
		t.Errorf("host b: %v", err)
	}

	// Kubernetes deletes in the background: the undo must have asked for
	// every deletion, and they must finish before the next subtest.
	c, err := kubeClient(t, "b")
	if err != nil {
		t.Fatal(err)
	}
	left := func() (pending, kept []string) {
		ds, err := c.AppsV1().Deployments("").List(ctx, metav1.ListOptions{FieldSelector: "metadata.name=shop-worker"})
		if err != nil {
			t.Fatal(err)
		}
		ns, err := c.CoreV1().Namespaces().List(ctx, metav1.ListOptions{LabelSelector: svc.ManagedByLabel + "=" + svc.ManagedBy})
		if err != nil {
			t.Fatal(err)
		}
		var objs []metav1.ObjectMeta
		for _, d := range ds.Items {
			objs = append(objs, d.ObjectMeta)
		}
		for _, n := range ns.Items {
			objs = append(objs, n.ObjectMeta)
		}
		for _, m := range objs {
			name := strings.TrimPrefix(m.Namespace+"/"+m.Name, "/")
			if m.DeletionTimestamp == nil {
				kept = append(kept, name)
			} else {
				pending = append(pending, name)
			}
		}
		return pending, kept
	}
	start := time.Now()
	pending, kept := left()
	if len(kept) > 0 {
		t.Errorf("kind b: the undo left %v (namespaces created by cutover, or shop-worker)", kept)
	}
	for len(pending) > 0 && len(kept) == 0 {
		if time.Since(start) > 2*time.Minute {
			t.Errorf("kind b: %v still being deleted after %s", pending, time.Since(start).Round(time.Second))
			break
		}
		time.Sleep(500 * time.Millisecond)
		pending, kept = left()
	}
	t.Logf("kind b: the undo's deletions finished %s after the rollback", time.Since(start).Round(100*time.Millisecond))

	db, err := pgConn(ctx, pgBPort, dbPassword(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(context.Background())
	var tables []string
	rows, err := db.Query(ctx, `SELECT schemaname || '.' || tablename FROM pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema')`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, n)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(tables) > 0 {
		t.Errorf("pg-b: tables %v, want none", tables)
	}

	a, err := pgbouncer.Dial(ctx, pgbAdmin)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(context.Background())
	d, err := a.Database(ctx, "shop")
	if err != nil {
		t.Fatal(err)
	}
	if d.Host != pgAAddress {
		t.Errorf("pgbouncer: shop routes to %s, want pg-a (%s)", d.Host, pgAAddress)
	}
}

func cutoverUnderLoad(t *testing.T, e *env) {
	requireClean(t, e)
	planPath := writePlan(t, e)
	routeApply(t, e, planPath)
	ctx := context.Background()
	podsA := workerPods(t, "a")

	// The switch may hold writes for max_pause (10s); a shorter client
	// timeout would fail by design. 15s is max_pause plus a 5s margin.
	lg := loadgen.Start(ctx, loadgen.Config{Target: "http://" + listenAddr, Host: "localhost",
		Writers: 4, Readers: 2, Interval: 50 * time.Millisecond, Timeout: 15 * time.Second})
	defer func() {
		if r := lg.Stop(); t.Failed() {
			t.Logf("load at the end: %d requests, %d failures", r.Requests, r.Failures)
			for _, f := range r.FailureSamples {
				t.Logf("  failed: %s %s %s at %s", f.Method, f.Path, f.Err, f.At.Format("15:04:05.000"))
			}
		}
	}()

	auditFrom := lastAuditID(t, e)
	y, err := os.ReadFile(planPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.alice.Start(ctx, migrationID, y); err != nil {
		t.Fatalf("alice starts %s: %v", migrationID, err)
	}

	before := []string{"discover", "prepare", "rehearse", "shift-10", "shift-100"}
	drive(t, e, flow.PhaseSwitch, func(g flow.Gate) (*console.Client, bool) {
		if !slices.Contains(before, g.Name) {
			t.Fatalf("unexpected gate %s before the switch: %s", g.Name, g.Summary)
		}
		return e.alice, true
	})

	waitStatus(t, e, "the switch gate", 2*time.Minute, 200*time.Millisecond, func(st flow.Status) bool {
		return st.Gate != nil && st.Gate.Name == "switch"
	})
	if err := e.alice.Decide(ctx, migrationID, "switch", flow.Approve, "e2e"); err != nil {
		t.Fatalf("alice approves the switch: %v", err)
	}
	// bob decides on what he sees: alice's approval is part of the evidence.
	waitStatus(t, e, "alice's switch approval", 30*time.Second, 50*time.Millisecond, func(st flow.Status) bool {
		return st.Gate != nil && st.Gate.Name == "switch" && len(st.Gate.Approvals) == 1
	})
	if err := e.bob.Decide(ctx, migrationID, "switch", flow.Approve, "e2e"); err != nil {
		t.Fatalf("bob approves the switch: %v", err)
	}
	st := waitStatus(t, e, "the switch to run", 30*time.Second, 20*time.Millisecond, func(st flow.Status) bool {
		return st.Phase == flow.PhaseSwitch && st.Gate == nil
	})
	e.killWorker()
	t.Logf("killed the worker mid-switch at %s; last events: %s", time.Now().Format("15:04:05.000"), lastEvents(st))
	time.Sleep(2 * time.Second)
	e.startWorker(t)

	// A switch that could not finish inside its pause budget aborts back to
	// the source and asks again; the operators approve it again.
	reopened := 0
	st = drive(t, e, flow.StateFinished, func(g flow.Gate) (*console.Client, bool) {
		switch {
		case g.Name == "finish":
			return e.bob, true
		case g.Name == "switch" && len(g.Approvals) == 0:
			reopened++
			t.Logf("the switch gate reopened after the kill (%d); approving again:%s", reopened, switchEvents(t, e))
			return e.alice, true
		case g.Name == "switch":
			return e.bob, true
		}
		t.Fatalf("unexpected gate %s after the switch: %s%s", g.Name, g.Summary, switchEvents(t, e))
		return nil, false
	})
	t.Logf("the switch gate reopened %d times after the kill", reopened)
	rep := lg.Stop()
	t.Logf("the switch:%s", switchEvents(t, e))

	t.Logf("load: %d requests, %d failures, %d acknowledged orders", rep.Requests, rep.Failures, len(rep.IDs))
	if rep.Failures != 0 {
		t.Errorf("load: %d failed requests, want 0", rep.Failures)
	}
	if rep.Requests < 500 {
		t.Errorf("load: %d requests, want at least 500", rep.Requests)
	}
	if st.State != flow.StateFinished || st.Primary != ops.Target {
		t.Errorf("final: state %s, primary %s; want finished, target", st.State, st.Primary)
	}

	db, err := pgConn(ctx, pgBPort, e.dbPassword)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(context.Background())
	checkOrders(t, ctx, db, "pg-b", rep.IDs)
	checkTicks(t, ctx, db, podsA, workerPods(t, "b"), switchedAt(t, st))
	checkMoved(t, e, planPath)
	checkAudit(t, e, auditFrom)
	checkNoLeak(t, e, st)
}

// waitStatus polls alice's status every `every` until ok holds.
func waitStatus(t *testing.T, e *env, what string, d, every time.Duration, ok func(flow.Status) bool) flow.Status {
	t.Helper()
	deadline := time.Now().Add(d)
	var last flow.Status
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		r, err := e.alice.Status(ctx, migrationID)
		cancel()
		if err != nil {
			t.Fatalf("waiting for %s: %v", what, err)
		}
		last = r.Status
		if ok(last) {
			return last
		}
		if time.Now().After(deadline) {
			t.Fatalf("waited %s for %s; phase %s, state %s, gate %s, events %s",
				d, what, last.Phase, last.State, gateString(last.Gate), lastEvents(last))
		}
		time.Sleep(every)
	}
}

// switchEvents is every event of the switch phase, timed, for the log.
func switchEvents(t *testing.T, e *env) string { return phaseEvents(t, e, flow.PhaseSwitch) }

// phaseEvents is every event of one phase, timed, for the log.
func phaseEvents(t *testing.T, e *env, phase string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err := e.alice.Status(ctx, migrationID)
	if err != nil {
		return " (status: " + err.Error() + ")"
	}
	var b strings.Builder
	for _, x := range r.Status.Events {
		if x.Phase == phase {
			b.WriteString("\n  " + x.At.Format("15:04:05.000") + " " + x.Kind + " " + x.Actor + " " + x.Message)
		}
	}
	return b.String()
}

// workerPods lists the shop-worker pods running in one cloud.
func workerPods(t *testing.T, cloud string) []string {
	t.Helper()
	c, err := kubeClient(t, cloud)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pods, err := c.CoreV1().Pods("shop").List(ctx, metav1.ListOptions{LabelSelector: "app=shop-worker"})
	if err != nil {
		t.Fatalf("kind %s: list worker pods: %v", cloud, err)
	}
	var names []string
	for _, p := range pods.Items {
		names = append(names, p.Name)
	}
	if len(names) == 0 {
		t.Fatalf("kind %s runs no shop-worker pod", cloud)
	}
	return names
}

// checkOrders: every acknowledged order id was acknowledged once and is on
// the primary (named name) once.
func checkOrders(t *testing.T, ctx context.Context, db *pgx.Conn, name string, ids []int64) {
	t.Helper()
	seen := map[int64]bool{}
	for _, id := range ids {
		if seen[id] {
			t.Errorf("orders: id %d acknowledged twice", id)
		}
		seen[id] = true
	}
	rows, err := db.Query(ctx, `SELECT id, count(*) FROM orders WHERE id = ANY($1) GROUP BY id`, ids)
	if err != nil {
		t.Fatal(err)
	}
	found := map[int64]int64{}
	for rows.Next() {
		var id, n int64
		if err := rows.Scan(&id, &n); err != nil {
			t.Fatal(err)
		}
		found[id] = n
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	missing := 0
	for id := range seen {
		switch n := found[id]; {
		case n == 0:
			if missing++; missing <= 10 {
				t.Errorf("orders: acknowledged id %d is not on %s", id, name)
			}
		case n > 1:
			t.Errorf("orders: id %d is on %s %d times", id, name, n)
		}
	}
	if missing > 0 {
		t.Errorf("orders: %d of %d acknowledged ids missing on %s", missing, len(seen), name)
	}
}

// switchedAt is when the switch event says writes moved to the target.
func switchedAt(t *testing.T, st flow.Status) time.Time {
	t.Helper()
	for _, x := range st.Events {
		if x.Kind == "switched" {
			return x.At
		}
	}
	t.Fatalf("no switched event in%s", lastEvents(st))
	return time.Time{}
}

// checkTicks: each pod that ever wrote has seq 1..n with no gap, and the
// worker pods of both clouds wrote. A worker retries a failed tick with the
// same seq, so a stalled worker leaves no gap: cloud B's must also have
// written after the switch.
func checkTicks(t *testing.T, ctx context.Context, db *pgx.Conn, podsA, podsB []string, switched time.Time) {
	t.Helper()
	rows, err := db.Query(ctx, `SELECT pod, seq, at FROM ticks ORDER BY pod, seq`)
	if err != nil {
		t.Fatal(err)
	}
	seqs := map[string][]int64{}
	last := map[string]time.Time{}
	for rows.Next() {
		var pod string
		var seq int64
		var at time.Time
		if err := rows.Scan(&pod, &seq, &at); err != nil {
			t.Fatal(err)
		}
		seqs[pod] = append(seqs[pod], seq)
		last[pod] = at
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for pod, s := range seqs {
		for i, n := range s {
			if n != int64(i+1) {
				t.Errorf("ticks: pod %s has seq %d at position %d, want %d (a gap or a duplicate)", pod, n, i+1, i+1)
				break
			}
		}
		t.Logf("ticks: pod %s wrote seq 1..%d, the last at %s", pod, len(s), last[pod].Format("15:04:05.000"))
	}
	// Ticks are stamped by pg-b's clock; a second covers skew.
	if !slices.ContainsFunc(podsB, func(p string) bool { return last[p].After(switched.Add(time.Second)) }) {
		t.Errorf("ticks: no worker pod of cloud b (%v) wrote more than 1s after the switch at %s", podsB, switched.Local().Format("15:04:05.000"))
	}
	for cloud, pods := range map[string][]string{"a": podsA, "b": podsB} {
		if !slices.ContainsFunc(pods, func(p string) bool { return len(seqs[p]) > 0 }) {
			t.Errorf("ticks: no worker pod of cloud %s (%v) wrote", cloud, pods)
		}
	}
}

// checkMoved: the API runs on host B and answers through the front door,
// and the worker runs on kind B.
func checkMoved(t *testing.T, e *env, planPath string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := composeProjects(ctx, t, sshBPort, "cutover-shop-api"); err != nil {
		t.Errorf("host b: %v", err)
	}

	c, err := kubeClient(t, "b")
	if err != nil {
		t.Fatal(err)
	}
	d, err := c.AppsV1().Deployments("shop").Get(ctx, "shop-worker", metav1.GetOptions{})
	if err != nil {
		t.Errorf("kind b: %v", err)
	} else if d.Status.ReadyReplicas < 1 {
		t.Errorf("kind b: shop-worker has %d ready replicas, want at least 1", d.Status.ReadyReplicas)
	}

	checkRouteLine(t, e, planPath, "source -  target "+apiB+"  weight 100%")

	h, err := sshx.Dial(ctx, sshHost(t, sshBPort))
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	ids, err := h.Run(ctx, "docker ps -q --filter label=com.docker.compose.project=cutover-shop-api", nil)
	if err != nil {
		t.Fatal(err)
	}
	if f := strings.Fields(string(ids)); len(f) != 1 {
		t.Fatalf("host b: containers of cutover-shop-api %q, want one", f)
	}
	hostname, err := h.Run(ctx, "docker inspect -f '{{.Config.Hostname}}' "+strings.TrimSpace(string(ids)), nil)
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+listenAddr+"/healthz", nil)
	req.Host = "localhost"
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if got, want := resp.Header.Get("X-Demo-Host"), strings.TrimSpace(string(hostname)); resp.StatusCode != http.StatusOK || got != want {
		t.Errorf("front door: %s from X-Demo-Host %q, want 200 from host b's %q", resp.Status, got, want)
	}
}

// checkRouteLine: route list shows the api route with want in its line.
func checkRouteLine(t *testing.T, e *env, planPath, want string) {
	t.Helper()
	cmd := exec.Command(e.bin, "route", "list", "--plan", planPath)
	cmd.Env = e.baseEnv()
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("route list: %v", err)
	}
	var line string
	for _, l := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(l, migrationID+"/api ") {
			line = l
		}
	}
	if !strings.Contains(line, want) {
		t.Errorf("route list: api is %q, want %q in:\n%s", line, want, out)
	}
}

// audit reads migration shop's audit page as alice.
func audit(t *testing.T, e *env) ([]console.AuditEntry, []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	hc, err := e.session(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	defer hc.CloseIdleConnections()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, consoleURL+"/api/audit?migration="+migrationID+"&limit=500", nil)
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("audit: %s: %s", resp.Status, body)
	}
	var rows []console.AuditEntry
	if err := json.Unmarshal(body, &rows); err != nil {
		t.Fatalf("audit: %v", err)
	}
	return rows, body
}

func lastAuditID(t *testing.T, e *env) int64 {
	rows, _ := audit(t, e)
	var id int64
	for _, r := range rows {
		id = max(id, r.ID)
	}
	return id
}

// checkAudit: the cutover's gate decisions came from alice and bob, and none
// of its actions failed. Rows up to after belong to earlier subtests.
func checkAudit(t *testing.T, e *env, after int64) {
	t.Helper()
	rows, _ := audit(t, e)
	by := map[string]bool{}
	for _, r := range rows {
		if r.ID <= after {
			continue
		}
		if strings.HasSuffix(r.Kind, "-failed") {
			t.Errorf("audit: %s by %s at gate %q: %s", r.Kind, r.ActorName, r.Gate, r.Note)
		}
		if r.Kind == "gate-decision" {
			by[r.ActorName] = true
		}
	}
	for _, who := range []string{"alice", "bob"} {
		if !by[who] {
			t.Errorf("audit: no gate decision by %s", who)
		}
	}
}

// checkNoLeak: the database password is in no log, the workflow history,
// the console's status or the audit page.
func checkNoLeak(t *testing.T, e *env, st flow.Status) {
	t.Helper()
	pw := []byte(e.dbPassword)
	logs, err := filepath.Glob(runFile(t, "logs", "*.log"))
	if err != nil || len(logs) == 0 {
		t.Fatalf("no logs to search (%v)", err)
	}
	for _, f := range logs {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(b, pw) {
			t.Errorf("leak: the database password is in %s", f)
		}
	}

	ids := []string{flow.WorkflowID(migrationID)}
	if st.ReplicateWorkflowID != "" {
		ids = append(ids, st.ReplicateWorkflowID)
	}
	for _, id := range ids {
		cmd := exec.Command(temporalBin(), "workflow", "show", "--workflow-id", id, "--output", "json", "--address", e.temporal)
		cmd.Env = e.baseEnv()
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("temporal workflow show %s: %v", id, err)
		}
		if containsSecret(t, out, pw) {
			t.Errorf("leak: the database password is in the history of %s", id)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	reply, err := e.alice.Status(ctx, migrationID)
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(reply)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(b, pw) {
		t.Error("leak: the database password is in the console status")
	}
	if _, page := audit(t, e); bytes.Contains(page, pw) {
		t.Error("leak: the database password is in the audit page")
	}
}

// containsSecret searches JSON for pw, in its text and in every string
// that decodes as base64: history payloads are base64 in JSON output.
func containsSecret(t *testing.T, doc, pw []byte) bool {
	t.Helper()
	if bytes.Contains(doc, pw) {
		return true
	}
	var v any
	if err := json.Unmarshal(doc, &v); err != nil {
		t.Fatalf("history is not JSON: %v", err)
	}
	var walk func(any) bool
	walk = func(v any) bool {
		switch x := v.(type) {
		case string:
			b, err := base64.StdEncoding.DecodeString(x)
			return err == nil && bytes.Contains(b, pw)
		case []any:
			return slices.ContainsFunc(x, walk)
		case map[string]any:
			for _, y := range x {
				if walk(y) {
					return true
				}
			}
		}
		return false
	}
	return walk(v)
}
