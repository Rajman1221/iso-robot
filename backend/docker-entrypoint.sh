#!/usr/bin/env bash
set -euo pipefail

cd /app/backend
export PYTHONPATH=src

mkdir -p "$(dirname "${DATABASE_PATH:-/app/backend/data/db.sqlite}")"

if [[ "${RUN_SEED_DEMO:-false}" == "true" ]]; then
  echo "Running seed_demo.py (RUN_SEED_DEMO=true)..."
  python seed_demo.py
fi

if [[ $# -gt 0 ]]; then
  exec "$@"
else
  exec uvicorn iso_robot.main:app \
    --host 0.0.0.0 \
    --port "${PORT:-8000}"
fi
