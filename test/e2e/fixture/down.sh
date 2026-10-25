#!/usr/bin/env bash
# Removes everything up.sh made, and nothing else. Safe to run when the
# fixture is partly up or already gone.
set -uo pipefail
export KUBECONFIG=/nonexistent
cd "$(dirname "$0")/../../.." || exit 1

run=test/run/e2e
mkdir -p "$run"
for c in a b; do
	kind delete cluster --name "cutover-e2e-$c" --kubeconfig "$run/$c.kubeconfig"
done
docker compose -p cutover-e2e -f test/e2e/fixture/compose.yaml down -v
for img in cutover-demo:e2e cutover-e2e-dockerhost; do
	if docker image inspect "$img" >/dev/null 2>&1; then
		docker image rm "$img"
	fi
done
rm -rf "$run"
