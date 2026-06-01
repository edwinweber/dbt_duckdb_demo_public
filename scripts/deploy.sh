#!/usr/bin/env bash
# Deploy main branch to the Hetzner server.
# Run from your laptop: ./scripts/deploy.sh
#
# Config (set these once in your shell profile or a local .env.deploy):
#   DEPLOY_HOST   — server IP or hostname
#   DEPLOY_USER   — SSH user
#   DEPLOY_PATH   — absolute path to the repo on the server
#   DEPLOY_KEY    — path to SSH private key (defaults to ~/.ssh/id_ed25519)
#
# Usage:
#   ./scripts/deploy.sh
#   DEPLOY_HOST=1.2.3.4 DEPLOY_USER=root DEPLOY_PATH=/opt/dbt_duckdb_demo ./scripts/deploy.sh

set -euo pipefail

# ── Load local config if present ─────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/../.env.deploy"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck source=/dev/null
  set -a; source "$ENV_FILE"; set +a
fi

# ── Required variables ────────────────────────────────────────────────────────
: "${DEPLOY_HOST:?Set DEPLOY_HOST (server IP or hostname)}"
: "${DEPLOY_USER:?Set DEPLOY_USER (SSH username)}"
: "${DEPLOY_PATH:?Set DEPLOY_PATH (repo path on server, e.g. /opt/dbt_duckdb_demo)}"
DEPLOY_KEY="${DEPLOY_KEY:-$HOME/.ssh/id_ed25519}"
DEPLOY_PORT="${DEPLOY_PORT:-22}"

echo "==> Deploying to ${DEPLOY_USER}@${DEPLOY_HOST}:${DEPLOY_PATH}"

ssh -i "$DEPLOY_KEY" -p "$DEPLOY_PORT" \
    -o StrictHostKeyChecking=accept-new \
    "${DEPLOY_USER}@${DEPLOY_HOST}" \
    DEPLOY_PATH="$DEPLOY_PATH" \
    'bash -euo pipefail -s' << 'REMOTE'

echo "==> Pulling latest main"
cd "$DEPLOY_PATH"
git fetch origin main
git checkout main
git reset --hard origin/main

echo "==> Stopping containers"
docker compose down --remove-orphans

echo "==> Building images"
docker compose build

echo "==> Starting persistent services"
docker compose up -d dagster metabase

echo "==> Done"
docker compose ps

REMOTE
