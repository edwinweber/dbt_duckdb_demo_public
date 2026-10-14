#!/usr/bin/env bash
# Install the dbt-oss standalone binary (no Python package).
#
# Usage:
#   ./scripts/install_dbt_oss.sh                # installs to ~/.local/bin/dbt
#   PREFIX=/usr/local ./scripts/install_dbt_oss.sh  # installs to /usr/local/bin/dbt
#
# After installation, ensure the target directory is on PATH and optionally set
# DBT_EXECUTABLE in .env if the binary is not named 'dbt' or not on PATH.
#
# Supports: x86_64 Linux, arm64 Linux, x86_64 macOS, arm64 macOS.

set -euo pipefail

DBT_VERSION="2.0.5"
PREFIX="${PREFIX:-${HOME}/.local}"
BIN_DIR="${PREFIX}/bin"

# Detect platform
OS="$(uname -s)"
ARCH="$(uname -m)"

case "${OS}-${ARCH}" in
    Linux-x86_64)
        TARBALL="dbt-core-${DBT_VERSION}-x86_64-unknown-linux-gnu.tar.gz"
        SHA256="da45bb51a200617b6390ae6804158b1de4a64070d1bd8caeb4f71a282bea899a"
        ;;
    Linux-aarch64)
        TARBALL="dbt-core-${DBT_VERSION}-aarch64-unknown-linux-gnu.tar.gz"
        SHA256="0afb18df3de93398516e1c3870487609be28a7619e0fb9139798b26537ce11c3"
        ;;
    Darwin-x86_64)
        TARBALL="dbt-core-${DBT_VERSION}-x86_64-apple-darwin.tar.gz"
        SHA256="e74e50198755db2388c2a42f9deb129f0b96f61072263cfb44e20b65bf451e83"
        ;;
    Darwin-arm64)
        TARBALL="dbt-core-${DBT_VERSION}-aarch64-apple-darwin.tar.gz"
        SHA256="b598e91e8c7a237173973ae84607657f0b41060d4e71db54067ab92912e23bb1"
        ;;
    *)
        echo "Unsupported platform: ${OS}-${ARCH}" >&2
        exit 1
        ;;
esac

BASE_URL="https://github.com/dbt-labs/dbt-core/releases/download/v${DBT_VERSION}"
TMPDIR="$(mktemp -d)"
trap 'rm -rf "${TMPDIR}"' EXIT

echo "Downloading dbt-oss ${DBT_VERSION} for ${OS}-${ARCH}..."
curl -fsSL "${BASE_URL}/${TARBALL}" -o "${TMPDIR}/${TARBALL}"

echo "Verifying checksum..."
echo "${SHA256}  ${TMPDIR}/${TARBALL}" | sha256sum --check --quiet

echo "Installing dbt to ${BIN_DIR}/dbt..."
mkdir -p "${BIN_DIR}"
tar xzf "${TMPDIR}/${TARBALL}" -C "${TMPDIR}"
# Find the dbt binary regardless of the extracted directory name
DBT_BIN="$(find "${TMPDIR}" -maxdepth 2 -name dbt -type f | head -1)"
if [ -z "${DBT_BIN}" ]; then
    echo "Could not find dbt binary in extracted archive" >&2
    exit 1
fi
install -m 0755 "${DBT_BIN}" "${BIN_DIR}/dbt"

echo ""
echo "dbt-oss ${DBT_VERSION} installed to ${BIN_DIR}/dbt"
"${BIN_DIR}/dbt" --version
echo ""
echo "Add ${BIN_DIR} to your PATH if not already present:"
echo "  export PATH=\"${BIN_DIR}:\${PATH}\""
