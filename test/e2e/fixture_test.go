//go:build e2e

package e2e

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/SaiPisey2/cutover/internal/kube"
	"github.com/SaiPisey2/cutover/internal/pgbouncer"
	"github.com/SaiPisey2/cutover/internal/plan"
	"github.com/SaiPisey2/cutover/internal/secret"
	"github.com/SaiPisey2/cutover/internal/sshx"
)

const (
	pgAPort    = 55442
	pgBPort    = 55443
	pgbAdmin   = "postgres://postgres@127.0.0.1:56442/pgbouncer?sslmode=disable"
	sshAPort   = 52241
	sshBPort   = 52242
	apiA       = "127.0.0.1:28080"
	apiB       = "127.0.0.1:38080"
	pgAAddress = "172.30.0.11"
)

func TestFixture(t *testing.T) {
	if problems := fixtureProblems(t); len(problems) > 0 {
		t.Fatalf("fixture is not ready:\n  %s\n%s", strings.Join(problems, "\n  "), hint)
	}
}

// fixtureProblems checks the state make e2e-up leaves behind: everything
// runs in cloud A and nothing in cloud B. It returns one line per problem.
func fixtureProblems(t testing.TB) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var problems []string
	check := func(what string, err error) {
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s: %v", what, err))
		}
	}
	pw := dbPassword(t)

	check("kind a", func() error {
		c, err := kubeClient(t, "a")
		if err != nil {
			return err
		}
		d, err := c.AppsV1().Deployments("shop").Get(ctx, "shop-worker", metav1.GetOptions{})
		if err != nil {
			return err
		}
		if d.Status.ReadyReplicas != 1 {
			return fmt.Errorf("shop-worker has %d ready replicas, want 1", d.Status.ReadyReplicas)
		}
		return nil
	}())
	check("kind b", func() error {
		c, err := kubeClient(t, "b")
		if err != nil {
			return err
		}
		_, err = c.CoreV1().Namespaces().Get(ctx, "shop", metav1.GetOptions{})
		if err == nil {
			return errors.New("namespace shop exists, want none")
		}
		if !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}())

	check("host a", composeProjects(ctx, t, sshAPort, "shop"))
	check("host b", composeProjects(ctx, t, sshBPort, ""))

	check("pg-a", func() error {
		c, err := pgConn(ctx, pgAPort, pw)
		if err != nil {
			return err
		}
		defer c.Close(context.Background())
		var ok bool
		if err := c.QueryRow(ctx, `SELECT to_regclass('public.orders') IS NOT NULL`).Scan(&ok); err != nil {
			return err
		}
		if !ok {
			return errors.New("table orders does not exist")
		}
		return nil
	}())
	check("pg-b", func() error {
		c, err := pgConn(ctx, pgBPort, pw)
		if err != nil {
			return err
		}
		defer c.Close(context.Background())
		var n int
		err = c.QueryRow(ctx, `SELECT count(*) FROM pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema')`).Scan(&n)
		if err != nil {
			return err
		}
		if n != 0 {
			return fmt.Errorf("%d tables in database shop, want none", n)
		}
		return nil
	}())

	for _, side := range []struct {
		name string
		port int
	}{{"pg-a", pgAPort}, {"pg-b", pgBPort}} {
		left, err := replicationLeft(ctx, side.port, pw)
		check(side.name, err)
		for _, l := range left {
			problems = append(problems, side.name+": "+l+" left over, want none")
		}
	}

	check("pgbouncer", func() error {
		a, err := pgbouncer.Dial(ctx, pgbAdmin)
		if err != nil {
			return err
		}
		defer a.Close(context.Background())
		d, err := a.Database(ctx, "shop")
		if err != nil {
			return err
		}
		if d.Host != pgAAddress {
			return fmt.Errorf("shop routes to %s, want %s", d.Host, pgAAddress)
		}
		return nil
	}())

	check("api a", func() error {
		body, err := get(ctx, "http://"+apiA+"/healthz")
		if err != nil {
			return err
		}
		if body != "ok" {
			return fmt.Errorf("healthz says %q, want ok", body)
		}
		return nil
	}())
	// Docker publishes 38080 for host B whether or not anything runs
	// behind it, so the connect may succeed; the request must not.
	check("api b", func() error {
		if _, err := get(ctx, "http://"+apiB+"/healthz"); err == nil {
			return fmt.Errorf("%s answers, want nothing running there", apiB)
		}
		return nil
	}())
	return problems
}

// replicationLeft lists the replication slots, publications and
// subscriptions on one Postgres: a migration that ended leaves none.
func replicationLeft(ctx context.Context, port int, pw string) ([]string, error) {
	c, err := pgConn(ctx, port, pw)
	if err != nil {
		return nil, err
	}
	defer c.Close(context.Background())
	var left []string
	for _, q := range []struct{ kind, sql string }{
		{"replication slot", `SELECT slot_name FROM pg_replication_slots`},
		{"publication", `SELECT pubname FROM pg_publication`},
		{"subscription", `SELECT subname FROM pg_subscription`},
	} {
		rows, err := c.Query(ctx, q.sql)
		if err != nil {
			return nil, err
		}
		names, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return nil, err
		}
		for _, n := range names {
			left = append(left, q.kind+" "+n)
		}
	}
	return left, nil
}

func kubeClient(t testing.TB, cloud string) (kubernetes.Interface, error) {
	return kube.Client(plan.Kube{
		Kubeconfig: runFile(t, cloud+".kubeconfig"),
		Context:    "kind-cutover-e2e-" + cloud,
	})
}

// sshHost is how cutover reaches a fixture Docker host.
func sshHost(t testing.TB, port int) plan.Host {
	return plan.Host{
		Address:    "127.0.0.1",
		Port:       port,
		User:       "deploy",
		Key:        secret.Ref("file:" + runFile(t, "ssh", "id_ed25519")),
		KnownHosts: runFile(t, "ssh", "known_hosts"),
	}
}

// composeProjects checks that the host runs exactly the compose project
// want ("" for none).
func composeProjects(ctx context.Context, t testing.TB, port int, want string) error {
	c, err := sshx.Dial(ctx, sshHost(t, port))
	if err != nil {
		return err
	}
	defer c.Close()
	out, err := c.Run(ctx, "docker compose ls -q", nil)
	if err != nil {
		return err
	}
	if got := strings.TrimSpace(string(out)); got != want {
		return fmt.Errorf("compose projects %q, want %q", got, want)
	}
	return nil
}

func pgConn(ctx context.Context, port int, pw string) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(fmt.Sprintf("postgres://postgres@127.0.0.1:%d/shop?sslmode=disable", port))
	if err != nil {
		return nil, err
	}
	cfg.Password = pw
	return pgx.ConnectConfig(ctx, cfg)
}

func get(ctx context.Context, url string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<10))
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("%s: %s", url, resp.Status)
	}
	return string(b), nil
}
