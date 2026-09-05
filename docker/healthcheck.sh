#!/bin/sh
set -eu
LISTEN_ADDR="${LISTEN_ADDR:-0.0.0.0:20516}"
HOST="127.0.0.1"
# Default 20516 also covers IPv6 forms like "[::]:20516" (last colon wins).
PORT="${LISTEN_ADDR##*:}"
SCHEME="http"
# Self-signed certs are the norm for LAN deployments; -k only in TLS mode.
CURL_TLS_ARGS=""
if [ -n "${TLS_CERT_PATH:-}" ] && [ -n "${TLS_KEY_PATH:-}" ]; then
  SCHEME="https"
  CURL_TLS_ARGS="-k"
fi
curl --fail --silent --show-error --output /dev/null \
  --connect-timeout 3 --max-time 5 \
  $CURL_TLS_ARGS \
  "${SCHEME}://${HOST}:${PORT}/healthz"
