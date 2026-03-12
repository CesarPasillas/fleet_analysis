#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BACKEND_HOST="${BACKEND_HOST:-127.0.0.1}"
BACKEND_PORT="${BACKEND_PORT:-8000}"
FRONTEND_HOST="${FRONTEND_HOST:-127.0.0.1}"
FRONTEND_PORT="${FRONTEND_PORT:-5173}"

"$ROOT_DIR/scripts/run_backend.sh" &
BACK_PID=$!

cleanup() {
  kill "$BACK_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

npm --prefix "$ROOT_DIR/frontend" run dev -- --host "$FRONTEND_HOST" --port "$FRONTEND_PORT"
