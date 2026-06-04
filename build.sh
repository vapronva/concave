#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET="${1:?usage: build.sh <backend|dashboard|usher|bigbrain>}"
BUILDER="${BUILDER:-buildah}"

case "$TARGET" in
  backend)
    IMAGE="${IMAGE:-localhost/concave/backend:dev}"
    CONTEXT="$HERE/backend"
    DOCKERFILE="$HERE/backend/Dockerfile.backend"
    ;;
  dashboard)
    IMAGE="${IMAGE:-localhost/concave/dashboard:dev}"
    CONTEXT="$HERE/backend"
    DOCKERFILE="$HERE/backend/Dockerfile.dashboard"
    ;;
  usher)
    IMAGE="${IMAGE:-localhost/concave/usher:dev}"
    CONTEXT="$HERE/router/usher"
    DOCKERFILE="$HERE/router/usher/Dockerfile"
    ;;
  bigbrain)
    IMAGE="${IMAGE:-localhost/concave/bigbrain:dev}"
    CONTEXT="$HERE/router/bigbrain"
    DOCKERFILE="$HERE/router/bigbrain/Dockerfile"
    ;;
  *)
    echo "FATAL: unknown target '$TARGET' (expected: backend|dashboard|usher|bigbrain)"
    exit 2
    ;;
esac

"$BUILDER" build -f "$DOCKERFILE" -t "$IMAGE" "$CONTEXT"
echo "built $IMAGE"
