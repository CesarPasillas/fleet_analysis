#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
BACKEND_HOST="${BACKEND_HOST:-127.0.0.1}"
BACKEND_PORT="${BACKEND_PORT:-8000}"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "No se encontró $PYTHON_BIN"
  echo "Crea el entorno con: python3 -m venv .venv"
  exit 1
fi

exec "$PYTHON_BIN" -m uvicorn app.main:app \
  --app-dir "$ROOT_DIR/backend" \
  --host "$BACKEND_HOST" \
  --port "$BACKEND_PORT" \
  --reload
