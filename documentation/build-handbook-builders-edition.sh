#!/usr/bin/env bash
set -euo pipefail

DOC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

HANDBOOK_BASENAME="modern-data-platform-handbook-builders-edition" \
HANDBOOK_BODY_FILE="$DOC_DIR/modern-data-platform-handbook-builders-edition.md" \
HANDBOOK_FRONT_FILE="$DOC_DIR/modern-data-platform-handbook-builders-frontmatter.md" \
HANDBOOK_THEME_CSS_FILE="$DOC_DIR/handbook-builders-theme.css" \
HANDBOOK_PRINT_CSS_FILE="$DOC_DIR/handbook-builders-print.css" \
"$DOC_DIR/build-handbook.sh"