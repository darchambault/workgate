#!/usr/bin/env bash
# Builds workgate and installs it to ~/.local/bin, printing a PATH hint if
# that directory is not on the PATH.
#
#   ./install.sh                # build, test, install
#   ./install.sh --skip-tests   # build, install
#
# Idempotent: safe to re-run after every source change.

set -euo pipefail

repo="$(cd "$(dirname "$0")" && pwd)"

skip_tests=0
for arg in "$@"; do
    case "$arg" in
        --skip-tests) skip_tests=1 ;;
        *) echo "usage: $0 [--skip-tests]" >&2; exit 2 ;;
    esac
done

if ! command -v go >/dev/null 2>&1; then
    echo "Go toolchain not found. Install Go 1.25+ (e.g. 'brew install go' or https://go.dev/dl)." >&2
    exit 1
fi

if [ "$skip_tests" -eq 0 ]; then
    echo "Running tests..."
    (cd "$repo" && go test ./...)
fi

echo "Building workgate..."
(cd "$repo" && go build -o "$repo/workgate" ./cmd/workgate)

dest="$HOME/.local/bin"
mkdir -p "$dest"
install -m 0755 "$repo/workgate" "$dest/workgate"

case ":$PATH:" in
    *":$dest:"*) ;;
    *)
        echo "Note: $dest is not on your PATH. Add it in your shell profile"
        echo "(~/.zshrc, ~/.bashrc, ...):  export PATH=\"\$HOME/.local/bin:\$PATH\""
        ;;
esac

echo "Installed $dest/workgate"
"$dest/workgate" help | head -n 1
