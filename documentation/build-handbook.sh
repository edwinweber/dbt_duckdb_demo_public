#!/usr/bin/env bash
set -euo pipefail

DOC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="$DOC_DIR/dist"

mkdir -p "$OUT_DIR"

BASENAME="${HANDBOOK_BASENAME:-modern-data-platform-handbook}"
BODY_FILE="${HANDBOOK_BODY_FILE:-$DOC_DIR/modern-data-platform-handbook-complete.md}"
FRONT_FILE="${HANDBOOK_FRONT_FILE:-$DOC_DIR/modern-data-platform-handbook-frontmatter.md}"
CSS_FILE="${HANDBOOK_THEME_CSS_FILE:-$DOC_DIR/handbook-theme.css}"
PRINT_CSS_FILE="${HANDBOOK_PRINT_CSS_FILE:-$DOC_DIR/handbook-print.css}"

if [[ ! -f "$BODY_FILE" || ! -f "$FRONT_FILE" || ! -f "$CSS_FILE" || ! -f "$PRINT_CSS_FILE" ]]; then
    echo "Expected handbook source files were not found." >&2
    exit 1
fi

if ! command -v pandoc >/dev/null 2>&1; then
    echo "pandoc is required but was not found in PATH." >&2
    exit 1
fi

if ! command -v weasyprint >/dev/null 2>&1; then
    echo "weasyprint is required but was not found in PATH." >&2
    exit 1
fi

echo "Building EPUB edition..."
(
    cd "$DOC_DIR"
    pandoc \
        "$(basename "$FRONT_FILE")" \
        "$(basename "$BODY_FILE")" \
        --from markdown+fenced_code_blocks+backtick_code_blocks+yaml_metadata_block \
        --to epub \
        --standalone \
        --toc \
        --toc-depth=3 \
        --highlight-style=tango \
        --css "$(basename "$CSS_FILE")" \
        --epub-cover-image assets/cover.svg \
        --resource-path ".:assets" \
        --output "$OUT_DIR/${BASENAME}.epub"
)

echo "Building HTML edition..."
(
    cd "$DOC_DIR"
    pandoc \
        "$(basename "$FRONT_FILE")" \
        "$(basename "$BODY_FILE")" \
        --from markdown+fenced_code_blocks+backtick_code_blocks+yaml_metadata_block \
        --standalone \
        --embed-resources \
        --toc \
        --toc-depth=3 \
        --highlight-style=tango \
        --css "$(basename "$CSS_FILE")" \
        --css "$(basename "$PRINT_CSS_FILE")" \
        --resource-path ".:assets" \
        --output "$OUT_DIR/${BASENAME}.html"
)

echo "Building PDF edition..."
(
    cd "$DOC_DIR"
    weasyprint \
        "dist/${BASENAME}.html" \
        "$OUT_DIR/${BASENAME}.pdf"
)

echo "Build complete. Outputs written to $OUT_DIR"