#!/bin/sh
set -eu
PORT="${LISTEN_PORT:-20516}"
HOST="127.0.0.1"
SECRET="${ORIGIN_SECRET:-}"
if [ -z "${SECRET}" ]; then
  echo "ORIGIN_SECRET not set" >&2
  exit 1
fi
curl --fail --silent --show-error --output /dev/null \
  --connect-timeout 3 --max-time 5 \
  -H "X-Origin-Secret: ${SECRET}" \
  "http://${HOST}:${PORT}/healthz"