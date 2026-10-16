FROM alpine:3.22
RUN apk add --no-cache pgbouncer && (id pgbouncer >/dev/null 2>&1 || adduser -D -H pgbouncer)
USER pgbouncer
ENTRYPOINT ["pgbouncer", "/etc/pgbouncer/pgbouncer.ini"]
