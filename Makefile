.PHONY: test itest itest-up itest-down e2e-up e2e-down e2e-check e2e-test vet ui ui-test

test:
	go test -race -count=1 ./...

vet:
	go vet ./...
	go vet -tags integration ./...
	go vet -tags e2e ./...

itest:
	go test -race -count=1 -p 1 -tags integration ./...

itest-up:
	mkdir -p test/run/pgbouncer
	# Same 1024-byte layout as pgbouncer.WriteRoute (see RoutesFileSize).
	printf '[databases]\napp = host=pg-source port=5432 dbname=app\n%-969s\n' '; managed by cutover; do not edit while a migration runs' > test/run/pgbouncer/routes.ini
	docker compose -f test/compose.yaml up -d --build --wait

itest-down:
	docker compose -f test/compose.yaml down -v

# The kind fixture for test/e2e. up.sh refuses to start over an existing one.
e2e-up:
	test/e2e/fixture/up.sh

e2e-down:
	test/e2e/fixture/down.sh

# e2e-check runs only up.sh's refusals (disk, existing clusters, ports).
e2e-check:
	test/e2e/fixture/up.sh --check-only

# TestE2E and the unit test TestSourceOnly, not TestFixture: TestE2E leaves
# cloud B populated, so the fixture check would fail after it. A run takes a
# few minutes; the timeout stays inside CI's job limit so a hang dumps stacks.
e2e-test:
	KUBECONFIG=/nonexistent go test -tags e2e -p 1 -count=1 -timeout 20m -run '^(TestE2E|TestSourceOnly)$$' ./test/e2e/

# ui rebuilds ui/dist, which is committed and embedded into the binary.
ui:
	cd ui && npm ci && npm run build

ui-test:
	cd ui && npm test -- --run
