#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="$ROOT_DIR/.venv/bin/python"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "No se encontró $PYTHON_BIN"
  echo "Crea el entorno con: python3 -m venv .venv"
  exit 1
fi

exec "$PYTHON_BIN" "$ROOT_DIR/scripts/build_snapshot.py" "$@"
