.PHONY: test itest itest-up itest-down vet

test:
	go test -race -count=1 ./...

vet:
	go vet ./...
	go vet -tags integration ./...

itest:
	go test -race -count=1 -p 1 -tags integration ./...

itest-up:
	mkdir -p test/run/pgbouncer
	# Same 1024-byte layout as pgbouncer.WriteRoute (see RoutesFileSize).
	printf '[databases]\napp = host=pg-source port=5432 dbname=app\n%-969s\n' '; managed by cutover; do not edit while a migration runs' > test/run/pgbouncer/routes.ini
	docker compose -f test/compose.yaml up -d --build --wait

itest-down:
	docker compose -f test/compose.yaml down -v
