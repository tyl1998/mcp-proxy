#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME="${IMAGE_NAME:-mcp-proxy}"
IMAGE_TAG="${IMAGE_TAG:-local}"
CONTAINER_NAME="${CONTAINER_NAME:-nuwax-mcp-proxy}"
HOST_PORT="${HOST_PORT:-8020}"
APP_PORT="${MCP_PROXY_PORT:-8089}"

# DEPLOY_DIR 为 nuwax_deploy/docker 目录的宿主机绝对路径
# 本地 macOS 示例：/Users/atan/Desktop/work/vs_code_nuwax/nuwax_deploy/docker
# Linux 服务器示例：/opt/nuwax/nuwax_deploy/docker
DEPLOY_DIR="${DEPLOY_DIR:?请设置 DEPLOY_DIR 环境变量，指向 nuwax_deploy/docker 目录}"

CONFIG_FILE="${CONFIG_FILE:-${DEPLOY_DIR}/config/mcp_config.yml}"
LOG_DIR="${LOG_DIR:-${DEPLOY_DIR}/logs/mcp_proxy}"
UV_CACHE_DIR="${UV_CACHE_DIR:-${DEPLOY_DIR}/data/uv_cache/uv}"
NPM_CACHE_DIR="${NPM_CACHE_DIR:-${DEPLOY_DIR}/data/npx_cache/.npm}"
WAIT_TIMEOUT_SECONDS="${WAIT_TIMEOUT_SECONDS:-120}"
PULL_IMAGE="${PULL_IMAGE:-false}"
IMAGE="${IMAGE_NAME}:${IMAGE_TAG}"

if [[ "$PULL_IMAGE" == "true" ]]; then
  docker pull "$IMAGE"
fi

mkdir -p "$LOG_DIR" "$UV_CACHE_DIR" "$NPM_CACHE_DIR"

docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true

docker run -d \
  --name "$CONTAINER_NAME" \
  --restart=always \
  --add-host=host.docker.internal:host-gateway \
  -p "${HOST_PORT}:${APP_PORT}" \
  -e MCP_PROXY_PORT="$APP_PORT" \
  -e MCP_PROXY_LOG_DIR=/app/logs \
  -e MCP_PROXY_LOG_LEVEL="${MCP_PROXY_LOG_LEVEL:-info}" \
  -e RUST_LOG="${RUST_LOG:-info}" \
  -v "$CONFIG_FILE:/app/config.yml:ro" \
  -v "$LOG_DIR:/app/logs" \
  -v "$UV_CACHE_DIR:/root/.cache/uv" \
  -v "$NPM_CACHE_DIR:/root/.npm" \
  "$IMAGE"

HEALTH_URL="${HEALTH_URL:-http://localhost:${HOST_PORT}/health}"
deadline=$((SECONDS + WAIT_TIMEOUT_SECONDS))
until curl -fsS "$HEALTH_URL" >/dev/null; do
  if (( SECONDS >= deadline )); then
    docker logs --tail=200 "$CONTAINER_NAME" || true
    printf 'Container %s failed health check: %s\n' "$CONTAINER_NAME" "$HEALTH_URL" >&2
    exit 1
  fi
  sleep 2
done

printf 'Deployed %s as %s\n' "$IMAGE" "$CONTAINER_NAME"
