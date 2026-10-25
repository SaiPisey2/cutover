#!/bin/sh
docker info >/dev/null 2>&1 && nc -z 127.0.0.1 22
