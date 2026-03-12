#!/usr/bin/env bash
set -euo pipefail

BACKEND_PORT="${BACKEND_PORT:-8000}"
FRONTEND_PORT="${FRONTEND_PORT:-5173}"

kill_port() {
  local port="$1"
  local pids
  pids=$(lsof -t -iTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)
  if [[ -n "$pids" ]]; then
    echo "Deteniendo procesos en puerto $port: $pids"
    kill $pids 2>/dev/null || true
  else
    echo "No hay procesos escuchando en puerto $port"
  fi
}

kill_port "$BACKEND_PORT"
kill_port "$FRONTEND_PORT"

echo "Listo. Servicios detenidos (si estaban activos)."
