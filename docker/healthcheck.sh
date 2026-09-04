#!/bin/sh
set -eu
LISTEN_ADDR="${LISTEN_ADDR:-0.0.0.0:20516}"
HOST="127.0.0.1"
# Default 20516 also covers IPv6 forms like "[::]:20516" (last colon wins).
PORT="${LISTEN_ADDR##*:}"
SECRET="${ORIGIN_SECRET:-}"
if [ -z "${SECRET}" ]; then
  echo "ORIGIN_SECRET not set" >&2
  exit 1
fi
curl --fail --silent --show-error --output /dev/null \
  --connect-timeout 3 --max-time 5 \
  -H "X-Origin-Secret: ${SECRET}" \
  "http://${HOST}:${PORT}/healthz"