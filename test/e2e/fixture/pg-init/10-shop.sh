#!/bin/sh
# Creates the demo role and database at first init. Runs as postgres over the local socket.
set -eu
pw=$(cat "$SHOP_PASSWORD_FILE")
psql -v ON_ERROR_STOP=1 -U postgres -v pw="$pw" <<'SQL'
CREATE ROLE shop LOGIN PASSWORD :'pw';
CREATE DATABASE shop OWNER shop;
SQL
