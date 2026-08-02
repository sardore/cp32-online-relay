#!/usr/bin/env sh
set -eu
exec node "$(dirname "$0")/server.js" "${CP32_RELAY_PORT:-8787}"
