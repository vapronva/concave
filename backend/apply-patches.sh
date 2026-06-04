#!/usr/bin/env bash
set -euo pipefail

TARGET="${1:?usage: apply-patches.sh <convex-checkout-dir>}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PATCHES="${PATCHES_DIR:-$HERE/patches/backend}"

for patch in "$PATCHES"/*.patch; do
  git -C "$TARGET" apply --whitespace=nowarn "$patch"
  echo "applied $(basename "$patch")"
done
