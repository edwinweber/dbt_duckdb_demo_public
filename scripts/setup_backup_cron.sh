#!/usr/bin/env bash
# =============================================================================
# DDD Platform — install backup cron entry
#
# Adds the nightly backup job to the current user's crontab.  The path to the
# repository is derived from this file's own location, so the entry is always
# correct regardless of where the repository is cloned.
#
# Existing crontab entries are preserved.  Running the script twice is safe —
# the entry is not added a second time.
#
# Usage:
#   scripts/setup_backup_cron.sh            # preview (no changes made)
#   scripts/setup_backup_cron.sh --install  # write to crontab
# =============================================================================

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CRON_LOG="/data_backup/logs/cron.log"

# DOCKER_HOST is set inline so it does not affect other cron jobs.
# cd to the repo dir first so docker compose finds docker-compose.yml.
CRON_LINE="0 2 * * * DOCKER_HOST=unix:///var/run/docker.sock cd \"${REPO_DIR}\" && docker compose run --rm backup >> ${CRON_LOG} 2>&1"

# ── Guard: docker must be available ──────────────────────────────────────────

if ! command -v docker &>/dev/null; then
    printf 'ERROR: docker not found in PATH\n' >&2
    exit 1
fi

# ── Check for existing entry ──────────────────────────────────────────────────

existing="$(crontab -l 2>/dev/null || true)"

if echo "${existing}" | grep -qF "docker compose run --rm backup"; then
    printf 'Backup cron entry already present — nothing to do.\n\n'
    printf 'Current entry:\n  %s\n' "$(echo "${existing}" | grep -F "docker compose run --rm backup")"
    exit 0
fi

# ── Preview ───────────────────────────────────────────────────────────────────

printf 'Entry to be added to crontab:\n\n  %s\n\n' "${CRON_LINE}"

if [[ "${1:-}" != "--install" ]]; then
    printf 'Dry run — no changes made.  Run with --install to apply.\n'
    exit 0
fi

# ── Install ───────────────────────────────────────────────────────────────────

if [[ -z "${existing}" ]]; then
    printf '%s\n' "${CRON_LINE}" | crontab -
else
    { printf '%s\n' "${existing}"; printf '%s\n' "${CRON_LINE}"; } | crontab -
fi

printf 'Done.  Verify with:\n  crontab -l\n'
