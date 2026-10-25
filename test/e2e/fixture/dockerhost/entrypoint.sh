#!/bin/sh
# Starts dockerd (unix socket only, no TLS), waits for it, then runs sshd in the foreground.
set -eu
export DOCKER_TLS_CERTDIR=""
dockerd-entrypoint.sh dockerd -G docker >/var/log/dockerd.log 2>&1 &
i=0
until docker info >/dev/null 2>&1; do
  i=$((i + 1))
  if [ "$i" -gt 60 ]; then cat /var/log/dockerd.log >&2; echo "dockerd did not start" >&2; exit 1; fi
  sleep 1
done
exec /usr/sbin/sshd -D -e
