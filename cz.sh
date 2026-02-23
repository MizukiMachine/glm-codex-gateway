#!/usr/bin/env bash
set -euo pipefail

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GATEWAY_DIR="${GLM_CODEX_GATEWAY_DIR:-$SCRIPT_DIR}"
GATEWAY_HOST="${GLM_CODEX_GATEWAY_HOST:-127.0.0.1}"
GATEWAY_PORT="${GLM_CODEX_GATEWAY_PORT:-8787}"
HEALTH_URL="http://${GATEWAY_HOST}:${GATEWAY_PORT}/healthz"
LOG_DIR="${HOME}/.cache"
LOG_FILE="${LOG_DIR}/glm-codex-gateway.log"

mkdir -p "${LOG_DIR}"

# Load local env file for persistent settings (includes OPENAI_BASE_URL, CODEX_MODEL, etc.)
if [ -f "${GATEWAY_DIR}/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  . "${GATEWAY_DIR}/.env"
  set +a
fi

if [ -z "${ZAI_API_KEY:-}" ] && [ -z "${Z_AI_API_KEY:-}" ]; then
  echo "[glm-codex-gateway] ZAI_API_KEY が未設定です。"
  echo "[glm-codex-gateway] 例: export ZAI_API_KEY='<your-zai-key>'"
  echo "[glm-codex-gateway] または ${GATEWAY_DIR}/.env に ZAI_API_KEY=... を記載してください。"
  exit 1
fi

# Set defaults if not already set by .env
export OPENAI_BASE_URL="${OPENAI_BASE_URL:-http://${GATEWAY_HOST}:${GATEWAY_PORT}/v1}"
export OPENAI_API_KEY="${OPENAI_API_KEY:-dummy-openai-key-for-proxy}"
export CODEX_MODEL="${CODEX_MODEL:-glm-5}"

if ! curl -fsS "${HEALTH_URL}" >/dev/null 2>&1; then
  nohup node "${GATEWAY_DIR}/glm-codex-gateway.mjs" >"${LOG_FILE}" 2>&1 &
  # Wait up to ~8s for startup.
  for _ in $(seq 1 80); do
    if curl -fsS "${HEALTH_URL}" >/dev/null 2>&1; then
      break
    fi
    sleep 0.1
  done
fi

if ! curl -fsS "${HEALTH_URL}" >/dev/null 2>&1; then
  echo "[glm-codex-gateway] ゲートウェイ起動に失敗しました: ${HEALTH_URL}"
  echo "[glm-codex-gateway] ログ: ${LOG_FILE}"
  tail -n 40 "${LOG_FILE}" 2>/dev/null || true
  exit 1
fi

exec codex "$@"
