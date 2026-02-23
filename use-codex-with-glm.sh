#!/usr/bin/env bash

# Source this file in your current shell:
#   source ./use-codex-with-glm.sh

GLM_CODEX_GATEWAY_HOST="${GLM_CODEX_GATEWAY_HOST:-127.0.0.1}"
GLM_CODEX_GATEWAY_PORT="${GLM_CODEX_GATEWAY_PORT:-8787}"

export OPENAI_BASE_URL="http://${GLM_CODEX_GATEWAY_HOST}:${GLM_CODEX_GATEWAY_PORT}/v1"

# Codex CLI usually requires OPENAI_API_KEY even when a proxy rewrites auth.
# Any non-empty value works if gateway has ZAI_API_KEY configured.
export OPENAI_API_KEY="${OPENAI_API_KEY:-dummy-openai-key-for-proxy}"

# Optional default model hint in your shell profile
export CODEX_MODEL="${CODEX_MODEL:-glm-5}"

echo "[glm-codex-gateway] OPENAI_BASE_URL=$OPENAI_BASE_URL"
echo "[glm-codex-gateway] OPENAI_API_KEY is set (${#OPENAI_API_KEY} chars)"
echo "[glm-codex-gateway] CODEX_MODEL=$CODEX_MODEL"
