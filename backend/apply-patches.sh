#!/usr/bin/env bash
set -euo pipefail

TARGET="${1:?usage: apply-patches.sh <convex-checkout-dir>}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PATCHES="${PATCHES_DIR:-$HERE/patches/backend}"
PIN_FILE="${UPSTREAM_PIN:-$(dirname "$PATCHES")/UPSTREAM}"
if [ -f "$PIN_FILE" ]; then
  pin="$(head -1 "$PIN_FILE")"
  head_sha="$(git -C "$TARGET" rev-parse HEAD)"
  if [ "$head_sha" != "$pin" ]; then
    echo "FATAL: checkout $head_sha != patch-series base $pin (backend/UPSTREAM)"
    echo "  the patches were generated against $pin; fix backend/UPSTREAM or rebase the patches"
    exit 1
  fi
fi

for patch in "$PATCHES"/*.patch; do
  git -C "$TARGET" apply --whitespace=nowarn "$patch"
  echo "applied $(basename "$patch")"
done
