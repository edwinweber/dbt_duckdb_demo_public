#!/usr/bin/env bash
# Sync the latest changes from this (private) repo to the public repo.
#
# Usage:
#   ./sync_to_public.sh
#   ./sync_to_public.sh "Custom commit message"
#
# How it works:
#   1. Reads the last-synced commit hash stored in the public repo's SYNC_REF file.
#   2. Diffs the private repo from that commit to HEAD to find changed files.
#   3. Copies those files into the public repo.
#   4. Commits with an auto-generated (or user-supplied) message and pushes.

set -euo pipefail

PRIVATE_REPO="$(cd "$(dirname "$0")" && pwd)"
PUBLIC_REPO="/home/edwin/source/repos/dbt_duckdb_demo_public"
SYNC_REF_FILE="$PUBLIC_REPO/SYNC_REF"

# ── Resolve the base commit (last sync point) ────────────────────────────────
if [[ -f "$SYNC_REF_FILE" ]]; then
    BASE_COMMIT=$(cat "$SYNC_REF_FILE")
else
    # Fallback: use the public repo's latest commit message to find the match
    echo "SYNC_REF not found — please set it manually (echo <commit-hash> > $SYNC_REF_FILE)"
    exit 1
fi

HEAD_COMMIT=$(git -C "$PRIVATE_REPO" rev-parse HEAD)

if [[ "$BASE_COMMIT" == "$HEAD_COMMIT" ]]; then
    echo "Public repo is already up to date with $HEAD_COMMIT"
    exit 0
fi

# ── Find changed files ────────────────────────────────────────────────────────
CHANGED=$(git -C "$PRIVATE_REPO" diff --name-only "$BASE_COMMIT" "$HEAD_COMMIT")

if [[ -z "$CHANGED" ]]; then
    echo "No file changes detected between $BASE_COMMIT and $HEAD_COMMIT"
    exit 0
fi

echo "Syncing $(echo "$CHANGED" | wc -l | tr -d ' ') changed file(s) to public repo..."

# ── Copy files ────────────────────────────────────────────────────────────────
while IFS= read -r file; do
    src="$PRIVATE_REPO/$file"
    dst="$PUBLIC_REPO/$file"

    if [[ -f "$src" ]]; then
        mkdir -p "$(dirname "$dst")"
        cp "$src" "$dst"
        echo "  copied: $file"
    else
        # File was deleted in private repo — remove from public too
        if [[ -f "$dst" ]]; then
            rm "$dst"
            echo "  deleted: $file"
        fi
    fi
done <<< "$CHANGED"

# ── Update SYNC_REF ───────────────────────────────────────────────────────────
echo "$HEAD_COMMIT" > "$SYNC_REF_FILE"

# ── Commit and push ───────────────────────────────────────────────────────────
PRIVATE_MSG=$(git -C "$PRIVATE_REPO" log --format="%s" "$BASE_COMMIT..$HEAD_COMMIT" | head -1)
COMMIT_MSG="${1:-Sync with private repo: $PRIVATE_MSG}"

git -C "$PUBLIC_REPO" add -A
git -C "$PUBLIC_REPO" commit -m "$COMMIT_MSG"
git -C "$PUBLIC_REPO" push origin main

echo ""
echo "Done — public repo synced to $HEAD_COMMIT"
