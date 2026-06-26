#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME="${IMAGE_NAME:-mcp-proxy}"
IMAGE_TAG="${IMAGE_TAG:-local}"
PUSH_IMAGE="${PUSH_IMAGE:-false}"
PLATFORM="${PLATFORM:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
IMAGE="${IMAGE_NAME}:${IMAGE_TAG}"

cd "$PROJECT_ROOT"

DOCKER_BUILD_ARGS=(-f docker/Dockerfile.mcp-proxy -t "$IMAGE")
if [[ -n "$PLATFORM" ]]; then
  DOCKER_BUILD_ARGS+=(--platform "$PLATFORM")
fi
DOCKER_BUILD_ARGS+=(.)

docker build "${DOCKER_BUILD_ARGS[@]}"

if [[ "$PUSH_IMAGE" == "true" ]]; then
  docker push "$IMAGE"
fi

printf 'Built image: %s\n' "$IMAGE"
