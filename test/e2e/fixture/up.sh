#!/usr/bin/env bash
# Builds the e2e fixture: cloud A (pg-a, Docker host A, kind A) runs the
# demo; cloud B (pg-b, Docker host B, kind B) is empty. State goes to
# test/run/e2e. With --check-only it runs the refusals and stops.
#
# ~/.kube/config may point at production: KUBECONFIG is pinned to a path
# that does not exist, and every kind and kubectl call names its own file.
set -euo pipefail
export KUBECONFIG=/nonexistent
cd "$(dirname "$0")/../../.."

run=test/run/e2e
fixture=test/e2e/fixture
compose=(docker compose -p cutover-e2e -f "$fixture/compose.yaml")
ports=(55442 55443 56442 52241 52242 28080 38080 18080 7600 7700 18000)

step=""
die() { echo "e2e-up: $*" >&2; exit 1; }
on_exit() {
	local rc=$?
	if [ "$rc" -ne 0 ] && [ -n "$step" ]; then echo "e2e-up: failed at: $step" >&2; fi
}
trap on_exit EXIT
say() { step="$*"; echo "==> $*"; }

ssh_opts() {
	echo -F /dev/null -i "$run/ssh/id_ed25519" -o IdentitiesOnly=yes -o BatchMode=yes \
		-o StrictHostKeyChecking=yes -o UserKnownHostsFile="$run/ssh/known_hosts"
}
on_host() { # port, command...
	local port=$1; shift
	# shellcheck disable=SC2046
	ssh $(ssh_opts) -p "$port" deploy@127.0.0.1 "$@"
}

say "checking disk, clusters and ports"
free_kib=$(df -k / | awk 'NR == 2 { print $4 }')
[ "$free_kib" -ge $((20 * 1024 * 1024)) ] ||
	die "only $((free_kib / 1024 / 1024)) GiB free on /, need 20 GiB"
clusters=$(kind get clusters 2>/dev/null) || die "kind get clusters failed; is Docker running?"
for c in cutover-e2e-a cutover-e2e-b; do
	if grep -qx "$c" <<<"$clusters"; then
		die "kind cluster $c already exists; run: make e2e-down"
	fi
done
for p in "${ports[@]}"; do
	if lsof -nP -iTCP:"$p" -sTCP:LISTEN >/dev/null 2>&1; then
		die "port 127.0.0.1:$p is in use"
	fi
done
if [ "${1:-}" = "--check-only" ]; then
	step=""
	echo "e2e-up: clear to start"
	exit 0
fi

say "writing run state to $run"
rm -rf "$run"
mkdir -p "$run/ssh" "$run/pgbouncer" "$run/logs"
ssh-keygen -q -t ed25519 -N '' -C cutover-e2e -f "$run/ssh/id_ed25519"
chmod 600 "$run/ssh/id_ed25519"
pw=$(openssl rand -hex 16)
printf '%s\n' "$pw" >"$run/pg-password"
chmod 644 "$run/pg-password"
printf '"postgres" "%s"\n"shop" "%s"\n' "$pw" "$pw" >"$run/pgbouncer/userlist.txt"
chmod 644 "$run/pgbouncer/userlist.txt"
# Same 1024-byte layout as pgbouncer.WriteRoute (see RoutesFileSize).
printf '[databases]\nshop = host=172.30.0.11 port=5432 dbname=shop\n%-965s\n' \
	'; managed by cutover; do not edit while a migration runs' >"$run/pgbouncer/routes.ini"
[ "$(wc -c <"$run/pgbouncer/routes.ini")" -eq 1024 ] || die "routes.ini is not 1024 bytes"
printf 'DATABASE_URL=postgres://shop:%s@172.30.0.10:6432/shop?sslmode=disable\n' "$pw" >"$run/api.env"
chmod 600 "$run/api.env"

say "starting databases, pgbouncer and docker hosts"
"${compose[@]}" up -d --build --wait

say "recording docker host keys"
# Both hosts share a host key baked into the image, which changes on rebuild.
# The published ports can lag the healthchecks briefly, so retry.
for _ in $(seq 30); do
	for p in 52241 52242; do
		# No -q (Linux's OpenSSH lacks it), so drop the "#" banner lines,
		# which some builds print to stdout. A refused port exits non-zero;
		# the line count below decides.
		ssh-keyscan -t ed25519 -p "$p" 127.0.0.1 2>/dev/null | grep -v '^#' || true
	done >"$run/ssh/known_hosts"
	[ "$(wc -l <"$run/ssh/known_hosts")" -eq 2 ] && break
	sleep 1
done
[ "$(wc -l <"$run/ssh/known_hosts")" -eq 2 ] || die "ssh-keyscan did not return both host keys"

say "building the demo image"
docker build -q -f test/e2e/demo/Dockerfile -t cutover-demo:e2e .
for p in 52241 52242; do
	say "loading the demo image into the docker host on port $p"
	docker save cutover-demo:e2e | on_host "$p" docker load
done

for c in a b; do
	say "creating kind cluster cutover-e2e-$c"
	KIND_EXPERIMENTAL_DOCKER_NETWORK=cutover-e2e kind create cluster --name "cutover-e2e-$c" \
		--config "$fixture/kind-$c.yaml" --kubeconfig "$run/$c.kubeconfig" --wait 120s
done
for c in a b; do
	say "loading the demo image into cutover-e2e-$c"
	kind load docker-image cutover-demo:e2e --name "cutover-e2e-$c"
done

say "creating the demo tables on pg-a"
# Role and database come from pg-init on both; only cloud A gets tables.
# Same schema the demo creates for itself, owned by shop.
"${compose[@]}" exec -T pg-a psql -q -v ON_ERROR_STOP=1 -U shop -d shop <<'SQL'
CREATE TABLE orders (id bigserial PRIMARY KEY, note text, at timestamptz DEFAULT now());
CREATE TABLE ticks (pod text, seq bigint, at timestamptz DEFAULT now(), PRIMARY KEY (pod, seq));
SQL

say "deploying the demo api on docker host a"
on_host 52241 'sudo mkdir -p /srv/shop && sudo chown deploy /srv/shop'
# The hosts' sshd has no sftp subsystem, so scp uses the original protocol (-O).
# shellcheck disable=SC2046
scp -q -O $(ssh_opts) -P 52241 "$fixture/source/compose.yaml" "$run/api.env" deploy@127.0.0.1:/srv/shop/
on_host 52241 'chmod 600 /srv/shop/api.env && cd /srv/shop && docker compose -p shop up -d'

say "deploying the demo worker on kind a"
kubectl=(kubectl --kubeconfig "$run/a.kubeconfig" --context kind-cutover-e2e-a)
"${kubectl[@]}" apply -f "$fixture/manifests/shop.yaml"
"${kubectl[@]}" -n shop create secret generic shop-db --from-env-file="$run/api.env" \
	--dry-run=client -o yaml | "${kubectl[@]}" apply -f -
"${kubectl[@]}" -n shop rollout status deployment/shop-worker --timeout=180s

say "waiting for the demo api on 127.0.0.1:28080"
for _ in $(seq 60); do
	if [ "$(curl -fsS 127.0.0.1:28080/healthz 2>/dev/null)" = ok ]; then
		step=""
		echo "e2e-up: fixture is up"
		exit 0
	fi
	sleep 1
done
die "the demo api on 127.0.0.1:28080 did not become healthy"
