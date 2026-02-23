# GLM Codex Gateway

Z.AIのGLMモデルを、OpenAI互換のローカルプロキシ経由で`Codex CLI`から使うための最小ゲートウェイです。

This is a local OpenAI-compatible proxy that lets you run Codex CLI against Z.AI GLM models.

## 1. 起動 / Start

```bash
cd /home/mizuki/glm-codex-gateway

export ZAI_API_KEY="<your-zai-key>"
# optional:
# export ZAI_BASE_URL="https://api.z.ai/api/paas/v4"
# export ZAI_CODING_BASE_URL="https://api.z.ai/api/coding/paas/v4" (default preferred)
# export GLM_CODEX_GATEWAY_PORT="8787"
# export GLM_DEFAULT_MODEL="glm-5"
# export GLM_RESPONSES_FALLBACK_MODE="auto"  # auto|always|never
# export GLM_ZAI_ENDPOINT_MODE="coding-only"   # auto|base-only|coding-only (default: coding-only)

node ./glm-codex-gateway.mjs
```

ヘルスチェック / Health check:

```bash
curl -s http://127.0.0.1:8787/healthz | jq .
```

## 2. Codex CLIをこのゲートウェイに向ける / Point Codex CLI to gateway

同じターミナルで:

```bash
source ./use-codex-with-glm.sh
```

これで以下が設定されます:

- `OPENAI_BASE_URL=http://127.0.0.1:8787/v1`
- `OPENAI_API_KEY=dummy-openai-key-for-proxy` (未設定時)
- `CODEX_MODEL=glm-5` (未設定時)

## 3. 使い方 / Usage

通常どおりCodex CLIを実行してください。

Run Codex CLI as usual from terminal/VS Code terminal.

```bash
codex
# or
codex exec "summarize this repository" --model glm-5
```

`gpt-5.3-codex`などのOpenAI系モデル指定は、ゲートウェイ内部でGLMにマップされます。

OpenAI-like model names are mapped to GLM internally.

## モデルマッピング / Model mapping

デフォルトマッピング例:

- `gpt-5.3-codex -> glm-4.7`
- `gpt-5.2-codex -> glm-4.7`
- `gpt-5.1-codex -> glm-4.7`
- `codex-mini-latest -> glm-4.7-flash`

カスタムマッピング:

```bash
export GLM_MODEL_MAP_JSON='{"gpt-5.3-codex":"glm-4.7","gpt-5-mini":"glm-4.7-flash"}'
```

## 実装エンドポイント / Implemented endpoints

- `GET /healthz`
- `GET /v1/models`
- `POST /v1/chat/completions` (pass-through + model mapping)
- `POST /v1/responses`
  - まず upstream `/responses` を試行
  - 非対応時は非ストリームに限り `/chat/completions` へフォールバック変換

## 注意事項 / Notes

- `Codex CLI`の機能追加で`/v1/responses`のストリーミング互換要件が厳しくなると、追加対応が必要になる場合があります。
- まずは`codex exec --json ...`のような非ストリーム中心の使い方から検証してください。
- VS Code統合ターミナルで使う場合も、同じ環境変数を読み込むようにしてください。

## トラブルシュート / Troubleshooting

1. 401が出る: `ZAI_API_KEY`をゲートウェイ起動プロセスに渡してください。
2. モデル不一致: `GLM_MODEL_MAP_JSON` を調整してください。
3. 接続できない: `OPENAI_BASE_URL` が `http://127.0.0.1:8787/v1` になっているか確認してください。
4. `zai_1113` / `Insufficient balance`:
   - デフォルトは `/api/coding/paas/v4`（無料枠）のみを使用します。
   - 有料枠の `/api/paas/v4` も試すには `GLM_ZAI_ENDPOINT_MODE=auto` を設定してください。
   - 既存の別系統エンドポイント（例: Anthropic互換）で使えていても、PaaS/Coding枠は別管理の可能性があります。
