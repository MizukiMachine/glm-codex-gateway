#!/usr/bin/env node

import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import {
  brotliDecompressSync,
  gunzipSync,
  inflateRawSync,
  inflateSync,
  zstdDecompressSync,
} from "node:zlib";
import { readFile, writeFile, readdir } from "node:fs/promises";
import { spawn } from "node:child_process";
import path from "node:path";

const HOST = process.env.GLM_CODEX_GATEWAY_HOST || "127.0.0.1";
const PORT = Number(process.env.GLM_CODEX_GATEWAY_PORT || "8787");
const ZAI_GLOBAL_BASE_URL = "https://api.z.ai/api/paas/v4";
const ZAI_CODING_GLOBAL_BASE_URL = "https://api.z.ai/api/coding/paas/v4";
const ZAI_BASE_URL = (process.env.ZAI_BASE_URL || ZAI_GLOBAL_BASE_URL).replace(/\/+$/, "");
const ZAI_CODING_BASE_URL = (process.env.ZAI_CODING_BASE_URL || ZAI_CODING_GLOBAL_BASE_URL).replace(/\/+$/, "");
const ZAI_ENDPOINT_MODE = (process.env.GLM_ZAI_ENDPOINT_MODE || "coding-only").trim().toLowerCase();
const ZAI_API_KEY = (process.env.ZAI_API_KEY || process.env.Z_AI_API_KEY || "").trim();
const REQUEST_TIMEOUT_MS = Number(process.env.GLM_CODEX_GATEWAY_TIMEOUT_MS || "180000");
const MODEL_DEFAULT = (process.env.GLM_DEFAULT_MODEL || "glm-5").trim();
const LOG_LEVEL = (process.env.GLM_CODEX_GATEWAY_LOG_LEVEL || "info").trim().toLowerCase();
const RESPONSES_FALLBACK_MODE = (process.env.GLM_RESPONSES_FALLBACK_MODE || "auto").trim().toLowerCase();
const RESPONSES_STREAM_FALLBACK_MODE = (process.env.GLM_RESPONSES_STREAM_FALLBACK_MODE || "sse")
  .trim()
  .toLowerCase();
let ACTIVE_ZAI_BASE_URL = ZAI_CODING_BASE_URL;
const DEFAULT_RESPONSES_USAGE = Object.freeze({
  input_tokens: 0,
  output_tokens: 0,
  total_tokens: 0,
});

const DEFAULT_MODEL_MAP = {
  "gpt-5.3-codex": "glm-4.7",
  "gpt-5.2-codex": "glm-4.7",
  "gpt-5.1-codex": "glm-4.7",
  "gpt-5-codex": "glm-4.7",
  "codex-mini-latest": "glm-4.7-flash",
  "gpt-5": "glm-4.7",
  "gpt-5-mini": "glm-4.7-flash",
  "gpt-4.1": "glm-4.7",
  "gpt-4.1-mini": "glm-4.7-flash",
};

function loadModelMap() {
  const raw = (process.env.GLM_MODEL_MAP_JSON || "").trim();
  if (!raw) {
    return DEFAULT_MODEL_MAP;
  }
  try {
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return DEFAULT_MODEL_MAP;
    }
    const out = { ...DEFAULT_MODEL_MAP };
    for (const [k, v] of Object.entries(parsed)) {
      const key = String(k || "").trim().toLowerCase();
      const value = String(v || "").trim();
      if (key && value) {
        out[key] = value;
      }
    }
    return out;
  } catch {
    return DEFAULT_MODEL_MAP;
  }
}

const MODEL_MAP = loadModelMap();

function logInfo(msg, meta) {
  if (LOG_LEVEL === "silent") {
    return;
  }
  const suffix = meta ? ` ${JSON.stringify(meta)}` : "";
  console.log(`[glm-codex-gateway] ${msg}${suffix}`);
}

function logDebug(msg, meta) {
  if (LOG_LEVEL !== "debug") {
    return;
  }
  const suffix = meta ? ` ${JSON.stringify(meta)}` : "";
  console.log(`[glm-codex-gateway:debug] ${msg}${suffix}`);
}

function toPreviewText(value, maxLength = 360) {
  const text = String(value || "").replace(/\s+/g, " ").trim();
  if (text.length <= maxLength) {
    return text;
  }
  return `${text.slice(0, maxLength)}...`;
}

function getUpstreamRequestId(headers) {
  return (
    String(headers.get("x-request-id") || "").trim() ||
    String(headers.get("request-id") || "").trim() ||
    String(headers.get("x-zai-request-id") || "").trim() ||
    ""
  );
}

function shouldFallbackResponses(status, failText) {
  if (RESPONSES_FALLBACK_MODE === "never") {
    return false;
  }
  if (status === 401 || status === 403) {
    return false;
  }
  if (RESPONSES_FALLBACK_MODE === "always") {
    return true;
  }
  if (status === 404 || status === 405 || status === 415 || status === 422 || status === 429 || status >= 500) {
    return true;
  }
  const lowerText = String(failText || "").toLowerCase();
  return (
    lowerText.includes("not found") ||
    lowerText.includes("unknown") ||
    lowerText.includes("unsupported") ||
    lowerText.includes("not support") ||
    lowerText.includes("does not support") ||
    lowerText.includes("/responses")
  );
}

function parseUpstreamError(text) {
  try {
    const parsed = JSON.parse(String(text || ""));
    const err = parsed && typeof parsed === "object" ? parsed.error : undefined;
    if (!err || typeof err !== "object") {
      return { message: "", code: "" };
    }
    return {
      message: String(err.message || "").trim(),
      code: String(err.code || "").trim(),
    };
  } catch {
    return { message: "", code: "" };
  }
}

function buildEndpointCandidates() {
  const unique = new Set();
  const out = [];
  const add = (value) => {
    const normalized = String(value || "").trim().replace(/\/+$/, "");
    if (!normalized || unique.has(normalized)) {
      return;
    }
    unique.add(normalized);
    out.push(normalized);
  };

  if (ZAI_ENDPOINT_MODE === "base-only") {
    add(ZAI_BASE_URL);
    return out;
  }
  if (ZAI_ENDPOINT_MODE === "coding-only") {
    add(ZAI_CODING_BASE_URL);
    return out;
  }

  // auto: prefer last successful endpoint, then primary, then coding fallback.
  add(ACTIVE_ZAI_BASE_URL);
  add(ZAI_BASE_URL);
  add(ZAI_CODING_BASE_URL);
  return out;
}

// Build endpoint candidates with preference for coding endpoint when responses failed
function buildEndpointCandidatesForChatFallback() {
  const unique = new Set();
  const out = [];
  const add = (value) => {
    const normalized = String(value || "").trim().replace(/\/+$/, "");
    if (!normalized || unique.has(normalized)) {
      return;
    }
    unique.add(normalized);
    out.push(normalized);
  };

  if (ZAI_ENDPOINT_MODE === "base-only") {
    add(ZAI_BASE_URL);
    return out;
  }
  if (ZAI_ENDPOINT_MODE === "coding-only") {
    // For chat fallback, still prefer coding but allow retry
    add(ZAI_CODING_BASE_URL);
    // Also add base as fallback in case coding fails
    add(ZAI_BASE_URL);
    return out;
  }

  // For chat fallback after /responses failure, prefer coding endpoint first
  add(ZAI_CODING_BASE_URL);
  add(ZAI_BASE_URL);
  add(ACTIVE_ZAI_BASE_URL);
  return out;
}

function shouldTryNextEndpoint(status, failText) {
  if (ZAI_ENDPOINT_MODE === "base-only" || ZAI_ENDPOINT_MODE === "coding-only") {
    // For fixed modes, only retry on specific errors
    if (status === 429) {
      const parsed = parseUpstreamError(failText);
      if (parsed.code === "1113" || parsed.code === "insufficient_quota") {
        return true; // Allow retry to alternative endpoint
      }
    }
    return false;
  }
  // auto mode: more flexible retry logic
  if (status === 404 || status === 405 || status === 401 || status === 403) {
    return true;
  }
  if (status === 429) {
    const parsed = parseUpstreamError(failText);
    if (parsed.code === "1113" || parsed.code === "insufficient_quota") {
      return true;
    }
  }
  return false;
}

async function fetchWithEndpointFailover(params) {
  const { req, apiKey, path, payload, endpointCandidates } = params;
  const candidates = endpointCandidates || buildEndpointCandidates();
  let lastFailure = null;

  for (let i = 0; i < candidates.length; i += 1) {
    const baseUrl = candidates[i];
    const upstream = await fetch(`${baseUrl}${path}`, {
      method: "POST",
      headers: sanitizeHeadersForUpstream(req, apiKey),
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    });

    if (upstream.ok) {
      ACTIVE_ZAI_BASE_URL = baseUrl;
      return { ok: true, upstream, baseUrl };
    }

    const failText = await upstream.text();
    const upstreamRequestId = getUpstreamRequestId(upstream.headers);
    const shouldRetry = shouldTryNextEndpoint(upstream.status, failText) && i < candidates.length - 1;

    logInfo("upstream attempt failed", {
      path,
      baseUrl,
      status: upstream.status,
      upstreamRequestId,
      shouldRetry,
      body: toPreviewText(failText),
    });

    lastFailure = {
      ok: false,
      baseUrl,
      status: upstream.status,
      text: failText,
      contentType: upstream.headers.get("content-type") || "application/json; charset=utf-8",
      upstreamRequestId,
    };

    if (!shouldRetry) {
      break;
    }
  }

  return (
    lastFailure || {
      ok: false,
      baseUrl: ACTIVE_ZAI_BASE_URL,
      status: 502,
      text: JSON.stringify({
        error: { message: "No upstream endpoint candidate available.", type: "bad_gateway" },
      }),
      contentType: "application/json; charset=utf-8",
      upstreamRequestId: "",
    }
  );
}

function maybeSendNormalizedQuotaError(res, status, text, extraHeaders) {
  const parsed = parseUpstreamError(text);
  if (status === 429 && parsed.code === "1113") {
    sendJson(
      res,
      402,
      {
        error: {
          message:
            "Z.AI upstream code=1113: Insufficient balance or no resource package for this endpoint. Check your plan/package for this API route (paas or coding/paas). / Z.AI上流 code=1113: このAPIルート向けの残高またはリソースパッケージが不足しています。",
          type: "insufficient_quota",
          code: "zai_1113",
        },
      },
      extraHeaders,
    );
    return true;
  }
  return false;
}

function sendJson(res, statusCode, payload, extraHeaders) {
  const body = JSON.stringify(payload);
  res.writeHead(statusCode, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(body),
    ...extraHeaders,
  });
  res.end(body);
}

function getBearerToken(req) {
  const auth = req.headers.authorization;
  if (!auth || typeof auth !== "string") {
    return "";
  }
  const match = auth.match(/^Bearer\s+(.+)$/i);
  return match ? match[1].trim() : "";
}

function resolveUpstreamApiKey(req) {
  return ZAI_API_KEY || getBearerToken(req);
}

function normalizeRequestedModel(raw) {
  const value = String(raw || "").trim();
  if (!value) {
    return "";
  }
  if (value.includes("/")) {
    const last = value.split("/").pop();
    return (last || "").trim();
  }
  return value;
}

function mapModel(raw) {
  const requested = normalizeRequestedModel(raw);
  const lower = requested.toLowerCase();
  if (!requested) {
    return MODEL_DEFAULT;
  }
  if (lower.startsWith("glm-")) {
    return requested;
  }
  if (MODEL_MAP[lower]) {
    return MODEL_MAP[lower];
  }
  if (lower.startsWith("gpt-5")) {
    return "glm-4.7";
  }
  if (lower.includes("mini")) {
    return "glm-4.7-flash";
  }
  return MODEL_DEFAULT;
}

function sanitizeHeadersForUpstream(req, apiKey) {
  const headers = {
    "content-type": "application/json",
    "user-agent": "glm-codex-gateway/1.0",
    accept: String(req.headers.accept || "application/json"),
    authorization: `Bearer ${apiKey}`,
  };
  if (req.headers["x-request-id"]) {
    headers["x-request-id"] = String(req.headers["x-request-id"]);
  }
  return headers;
}

function firstHeaderValue(value) {
  if (Array.isArray(value)) {
    return String(value[0] || "");
  }
  return String(value || "");
}

function decodeBodyBuffer(rawBuffer, contentEncoding) {
  const encoding = contentEncoding.trim().toLowerCase();
  if (!encoding || encoding === "identity") {
    return rawBuffer;
  }
  if (encoding === "gzip" || encoding === "x-gzip") {
    return gunzipSync(rawBuffer);
  }
  if (encoding === "br") {
    return brotliDecompressSync(rawBuffer);
  }
  if (encoding === "deflate") {
    try {
      return inflateSync(rawBuffer);
    } catch {
      return inflateRawSync(rawBuffer);
    }
  }
  if (encoding === "zstd" || encoding === "x-zstd") {
    return zstdDecompressSync(rawBuffer);
  }
  const err = new Error("UNSUPPORTED_CONTENT_ENCODING");
  err.meta = { contentEncoding: encoding };
  throw err;
}

function tryParseJsonSequence(rawText) {
  const text = rawText.trim();
  if (!text) {
    return {};
  }

  // First, try to parse as regular JSON (handles pretty-printed JSON)
  try {
    return JSON.parse(text);
  } catch {
    // Not regular JSON, try other formats
  }

  // Some clients can send JSON text sequences (RFC 7464) or newline-delimited JSON.
  // Only split if it looks like SSE format (contains "data:" prefix)
  if (text.includes("data:")) {
    const seq = text
      .split(/\r?\n|\u001e/)
      .map((line) => line.trim())
      .filter(Boolean)
      .map((line) => (line.startsWith("data:") ? line.slice(5).trim() : line))
      .filter(Boolean)
      .filter((line) => line !== "[DONE]");

    if (seq.length >= 1) {
      try {
        return JSON.parse(seq[0]);
      } catch {
        // Fall through
      }
    }
  }

  // Last resort: try to parse anyway
  return JSON.parse(text);
}

async function readJsonBody(req) {
  const chunks = [];
  for await (const chunk of req) {
    chunks.push(chunk);
  }
  const rawBuffer = Buffer.concat(chunks);
  if (rawBuffer.length === 0) {
    return {};
  }

  const contentEncoding = firstHeaderValue(req.headers["content-encoding"]);
  const decodedBuffer = decodeBodyBuffer(rawBuffer, contentEncoding);
  const raw = decodedBuffer.toString("utf8");
  if (!raw.trim()) {
    return {};
  }

  try {
    return tryParseJsonSequence(raw);
  } catch (cause) {
    const err = new Error("INVALID_JSON");
    err.meta = {
      contentType: firstHeaderValue(req.headers["content-type"]),
      contentEncoding,
      contentLength: firstHeaderValue(req.headers["content-length"]),
      rawBytes: rawBuffer.length,
      decodedBytes: decodedBuffer.length,
      previewBase64: decodedBuffer.subarray(0, 96).toString("base64"),
      parseError: cause instanceof Error ? cause.message : String(cause),
    };
    throw err;
  }
}

function pickAssistantTextFromChatResponse(chatResponse) {
  const choices = Array.isArray(chatResponse?.choices) ? chatResponse.choices : [];
  const first = choices[0] || {};
  const message = first.message || {};
  const content = message.content;
  if (typeof content === "string") {
    return content;
  }
  if (Array.isArray(content)) {
    return content
      .map((part) => {
        if (!part || typeof part !== "object") {
          return "";
        }
        if (typeof part.text === "string") {
          return part.text;
        }
        return "";
      })
      .filter(Boolean)
      .join("\n");
  }
  return "";
}

function normalizeResponsesUsage(rawUsage) {
  if (!rawUsage || typeof rawUsage !== "object") {
    return { ...DEFAULT_RESPONSES_USAGE };
  }
  const usage = rawUsage;

  const inputTokens = Number.isFinite(usage.input_tokens)
    ? Number(usage.input_tokens)
    : Number.isFinite(usage.prompt_tokens)
      ? Number(usage.prompt_tokens)
      : 0;
  const outputTokens = Number.isFinite(usage.output_tokens)
    ? Number(usage.output_tokens)
    : Number.isFinite(usage.completion_tokens)
      ? Number(usage.completion_tokens)
      : 0;
  const totalTokens = Number.isFinite(usage.total_tokens)
    ? Number(usage.total_tokens)
    : inputTokens + outputTokens;

  return {
    input_tokens: inputTokens,
    output_tokens: outputTokens,
    total_tokens: totalTokens,
  };
}

function toResponsesObjectFromChat(chatResponse, model) {
  const responseId = `resp_${String(chatResponse?.id || randomUUID()).replace(/[^a-zA-Z0-9_\-]/g, "")}`;
  const outputText = pickAssistantTextFromChatResponse(chatResponse);
  return {
    id: responseId,
    object: "response",
    created_at: new Date().toISOString(),
    status: "completed",
    model,
    output: [
      {
        type: "message",
        id: `msg_${randomUUID().replace(/-/g, "")}`,
        status: "completed",
        role: "assistant",
        content: [
          {
            type: "output_text",
            text: outputText,
            annotations: [],
          },
        ],
      },
    ],
    output_text: outputText,
    usage: normalizeResponsesUsage(chatResponse?.usage),
  };
}

function normalizeChatRole(rawRole) {
  const role = String(rawRole || "").trim().toLowerCase();
  if (role === "system") {
    return "system";
  }
  if (role === "developer") {
    // Z.AI chat/completions expects system/user/assistant.
    return "system";
  }
  if (role === "assistant") {
    return "assistant";
  }
  if (role === "tool") {
    // Tool role is maintained for proper tool result handling.
    return "tool";
  }
  return "user";
}

// Convert Responses API tools to Chat Completions tools format
function convertResponsesToolsToChatTools(responsesTools) {
  if (!Array.isArray(responsesTools)) {
    return [];
  }
  return responsesTools
    .map((tool) => {
      // Support both formats:
      // 1. OpenAI format: {type:"function", function:{name:"...", parameters:{...}}}
      // 2. Codex CLI format: {type:"function", name:"...", parameters:{...}}
      if (tool.type === "function") {
        const funcData = tool.function || tool;
        if (funcData && funcData.name) {
          return {
            type: "function",
            function: {
              name: funcData.name,
              description: funcData.description || "",
              parameters: funcData.parameters || { type: "object", properties: {} },
            },
          };
        }
      }
      return null;
    })
    .filter(Boolean);
}

// Convert tool result message from Responses API to Chat Completions format
function convertToolResultToChatMessage(item) {
  const toolCallId = item.tool_call_id || item.toolCallId || "";
  let content = "";
  if (typeof item.content === "string") {
    content = item.content;
  } else if (Array.isArray(item.content)) {
    content = item.content
      .map((c) => {
        if (!c || typeof c !== "object") return "";
        if (typeof c.text === "string") return c.text;
        if (c.type === "input_text" && typeof c.text === "string") return c.text;
        if (c.type === "output_text" && typeof c.text === "string") return c.text;
        return "";
      })
      .filter(Boolean)
      .join("\n");
  }
  return {
    role: "tool",
    tool_call_id: toolCallId,
    content: content,
  };
}

function toChatCompletionsPayloadFromResponses(payload) {
  const mappedModel = mapModel(payload?.model);
  const base = {
    model: mappedModel,
    stream: Boolean(payload?.stream),
  };

  if (typeof payload?.temperature === "number") {
    base.temperature = payload.temperature;
  }
  if (typeof payload?.max_output_tokens === "number") {
    base.max_tokens = payload.max_output_tokens;
  }

  // ★Added: Tool definition conversion and propagation
  if (Array.isArray(payload?.tools) && payload.tools.length > 0) {
    base.tools = convertResponsesToolsToChatTools(payload.tools);
    if (payload.tool_choice) {
      base.tool_choice = payload.tool_choice;
    }
  }

  const input = payload?.input;
  if (typeof input === "string") {
    return {
      ...base,
      messages: [{ role: "user", content: input }],
    };
  }

  if (Array.isArray(input)) {
    const messages = [];
    for (const item of input) {
      if (!item || typeof item !== "object") {
        continue;
      }
      if (item.type === "message") {
        const role = normalizeChatRole(item.role);
        const content = item.content;

        // Handle tool role messages specially
        if (role === "tool") {
          messages.push(convertToolResultToChatMessage(item));
          continue;
        }

        if (typeof content === "string") {
          messages.push({ role, content });
          continue;
        }
        if (Array.isArray(content)) {
          const text = content
            .map((part) => {
              if (!part || typeof part !== "object") {
                return "";
              }
              if (part.type === "input_text" && typeof part.text === "string") {
                return part.text;
              }
              if (part.type === "output_text" && typeof part.text === "string") {
                return part.text;
              }
              return "";
            })
            .filter(Boolean)
            .join("\n");
          if (text) {
            messages.push({ role, content: text });
          }
        }
      }
      // Handle function_call_output (tool results in Responses API format)
      if (item.type === "function_call_output") {
        messages.push({
          role: "tool",
          tool_call_id: item.call_id || "",
          content: typeof item.output === "string" ? item.output : JSON.stringify(item.output || ""),
        });
      }
    }
    if (messages.length > 0) {
      return {
        ...base,
        messages,
      };
    }
  }

  return {
    ...base,
    messages: [{ role: "user", content: "" }],
  };
}

function writeSse(res, data) {
  res.write(`data: ${JSON.stringify(data)}\n\n`);
}

function sendResponsesSseFromObject(res, responseObject, extraHeaders) {
  const responseId = String(responseObject?.id || `resp_${randomUUID().replace(/-/g, "")}`);
  const createdAt = String(responseObject?.created_at || new Date().toISOString());
  const model = String(responseObject?.model || MODEL_DEFAULT);
  const outputText = String(responseObject?.output_text || "");
  const messageId = String(responseObject?.output?.[0]?.id || `msg_${randomUUID().replace(/-/g, "")}`);
  const usage = normalizeResponsesUsage(responseObject?.usage);

  const responseInProgress = {
    id: responseId,
    object: "response",
    created_at: createdAt,
    status: "in_progress",
    model,
    output: [],
    usage,
  };

  // Build the final completed output item with proper structure
  const completedOutputItem = {
    type: "message",
    id: messageId,
    status: "completed",
    role: "assistant",
    content: [
      {
        type: "output_text",
        text: outputText,
        annotations: [],
      },
    ],
  };

  const responseCompleted = {
    id: responseId,
    object: "response",
    created_at: createdAt,
    status: "completed",
    model,
    output: [completedOutputItem],
    output_text: outputText,
    usage: {
      input_tokens: usage.input_tokens,
      output_tokens: usage.output_tokens,
      total_tokens: usage.total_tokens,
    },
  };

  res.writeHead(200, {
    "content-type": "text/event-stream; charset=utf-8",
    "cache-control": "no-cache",
    connection: "keep-alive",
    ...extraHeaders,
  });

  writeSse(res, { type: "response.created", response: responseInProgress });
  writeSse(res, { type: "response.in_progress", response: responseInProgress });
  writeSse(res, {
    type: "response.output_item.added",
    response_id: responseId,
    output_index: 0,
    item: {
      type: "message",
      id: messageId,
      status: "in_progress",
      role: "assistant",
      content: [],
    },
  });
  writeSse(res, {
    type: "response.content_part.added",
    response_id: responseId,
    output_index: 0,
    item_id: messageId,
    content_index: 0,
    part: { type: "output_text", text: "" },
  });

  // Send text in chunks for proper streaming experience
  const chunkSize = 20; // Small chunks for better streaming feel
  for (let i = 0; i < outputText.length; i += chunkSize) {
    const chunk = outputText.slice(i, i + chunkSize);
    writeSse(res, {
      type: "response.output_text.delta",
      response_id: responseId,
      output_index: 0,
      item_id: messageId,
      content_index: 0,
      delta: { type: "text", text: chunk },
    });
  }

  writeSse(res, {
    type: "response.output_text.done",
    response_id: responseId,
    output_index: 0,
    item_id: messageId,
    content_index: 0,
    text: outputText,
  });
  writeSse(res, {
    type: "response.content_part.done",
    response_id: responseId,
    output_index: 0,
    item_id: messageId,
    content_index: 0,
    part: {
      type: "output_text",
      text: outputText,
      annotations: [],
    },
  });
  writeSse(res, {
    type: "response.output_item.done",
    response_id: responseId,
    output_index: 0,
    item: {
      type: "message",
      id: messageId,
      status: "completed",
      role: "assistant",
      content: [{ type: "output_text", text: outputText, annotations: [] }],
    },
  });
  writeSse(res, { type: "response.completed", response: responseCompleted });
  res.write("data: [DONE]\n\n");
  res.end();
}

async function streamUpstreamBodyToHttpResponse(upstream, res) {
  const reader = upstream.body?.getReader?.();
  if (!reader) {
    res.end();
    return;
  }
  while (true) {
    const { value, done } = await reader.read();
    if (done) {
      break;
    }
    if (value) {
      res.write(Buffer.from(value));
    }
  }
  res.end();
}

async function streamChatCompletionsToResponsesSse(upstream, res, meta) {
  const { requestedModel, mappedModel, baseUrl } = meta;
  const responseId = `resp_${randomUUID().replace(/-/g, "")}`;
  const outputItemId = `msg_${randomUUID().replace(/-/g, "")}`;

  // Responses API SSE ヘッダー設定（OpenClow 方式）
  res.statusCode = 200;
  res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("x-glm-codex-conversion", "chat->responses-streaming");
  res.setHeader("x-glm-codex-requested-model", String(requestedModel));
  res.setHeader("x-glm-codex-mapped-model", mappedModel);
  res.setHeader("x-glm-codex-upstream", baseUrl);
  res.flushHeaders?.();

  // 初期イベント送信（OpenResponses 仕様）
  function writeSseEvent(res, event) {
    res.write(`event: ${event.type}\n`);
    res.write(`data: ${JSON.stringify(event)}\n\n`);
  }

  const initialResponse = {
    id: responseId,
    object: "response",
    created_at: Math.floor(Date.now() / 1000),
    status: "in_progress",
    model: mappedModel,
    output: [],
    usage: { input_tokens: 0, output_tokens: 0, total_tokens: 0 },
  };

  writeSseEvent(res, { type: "response.created", response_id: responseId, response: initialResponse });
  writeSseEvent(res, { type: "response.in_progress", response_id: responseId, response: initialResponse });
  writeSseEvent(res, {
    type: "response.output_item.added",
    response_id: responseId,
    output_index: 0,
    item: {
      type: "message",
      id: outputItemId,
      role: "assistant",
      content: [],
      status: "in_progress",
    },
  });
  writeSseEvent(res, {
    type: "response.content_part.added",
    response_id: responseId,
    item_id: outputItemId,
    output_index: 0,
    content_index: 0,
    part: { type: "output_text", text: "" },
  });

  // Chat Completions SSE を読み取り、Responses API 形式に変換
  const reader = upstream.body?.getReader?.();
  if (!reader) {
    // エラー処理
    writeSseEvent(res, {
      type: "response.failed",
      response_id: responseId,
      response: {
        ...initialResponse,
        status: "failed",
        error: { code: "stream_error", message: "Failed to read upstream stream" },
      },
    });
    res.write("data: [DONE]\n\n");
    res.end();
    return;
  }

  const decoder = new TextDecoder("utf-8");
  let buffer = "";
  let accumulatedText = ""; // デルタテキストを収集
  let finishReasonSeen = false; // finish_reason を検出したか
  let finalUsage = { input_tokens: 0, output_tokens: 0, total_tokens: 0 }; // usage 情報を収集

  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split("\n");
      buffer = lines.pop() || "";

      for (const line of lines) {
        if (!line.trim() || !line.startsWith("data: ")) continue;
        const data = line.slice(6).trim();
        if (data === "[DONE]") continue;

        try {
          const parsed = JSON.parse(data);
          const delta = parsed.choices?.[0]?.delta;
          const content = delta?.content;

          if (typeof content === "string" && content) {
            accumulatedText += content; // テキストを収集
            // Responses API 形式でテキストデルタを送信
            writeSseEvent(res, {
              type: "response.output_text.delta",
              response_id: responseId,
              item_id: outputItemId,
              output_index: 0,
              content_index: 0,
              delta: content,
            });
          }

          // usage 情報を収集（ストリーミング中に来る場合がある）
          if (parsed.usage) {
            finalUsage = {
              input_tokens: parsed.usage.prompt_tokens ?? parsed.usage.input_tokens ?? finalUsage.input_tokens,
              output_tokens: parsed.usage.completion_tokens ?? parsed.usage.output_tokens ?? finalUsage.output_tokens,
              total_tokens: parsed.usage.total_tokens ?? finalUsage.total_tokens,
            };
          }

          const finishReason = parsed.choices?.[0]?.finish_reason;
          if (finishReason === "stop" || finishReason === "length") {
            finishReasonSeen = true;
            // finish_reason を検出したが、すぐには完了イベントを送信しない
            // ストリームが完全に終了するのを待つ
          }
        } catch (e) {
          // パースエラーを無視
        }
      }
    }

    // ストリームが完全に終了した後、完了イベントを送信
    // これにより、すべてのデータが確実に読み取られてからファイナライズされる
    if (finishReasonSeen || accumulatedText.length > 0) {
      // ストリーム完了 - 収集したテキストを含めて送信
      writeSseEvent(res, {
        type: "response.output_text.done",
        response_id: responseId,
        item_id: outputItemId,
        output_index: 0,
        content_index: 0,
        text: accumulatedText,
      });
      writeSseEvent(res, {
        type: "response.content_part.done",
        response_id: responseId,
        item_id: outputItemId,
        output_index: 0,
        content_index: 0,
        part: { type: "output_text", text: accumulatedText },
      });
      writeSseEvent(res, {
        type: "response.output_item.done",
        response_id: responseId,
        output_index: 0,
        item: {
          type: "message",
          id: outputItemId,
          role: "assistant",
          content: [{ type: "output_text", text: accumulatedText }],
          status: "completed",
        },
      });
    }

    // 完了イベント送信 - usage 情報を含める（OpenClaw 方式）
    const finalResponse = {
      ...initialResponse,
      status: "completed",
      output: [{
        type: "message",
        id: outputItemId,
        role: "assistant",
        content: [{ type: "output_text", text: accumulatedText }],
        status: "completed",
      }],
      usage: finalUsage,
    };

    writeSseEvent(res, { type: "response.completed", response_id: responseId, response: finalResponse });
    res.write("data: [DONE]\n\n");
    res.flush?.(); // データを即座にフラッシュ
  } finally {
    res.end();
  }
}

async function proxyChatCompletions(req, res, payload, apiKey) {
  const requestedModel = payload?.model;
  const mappedModel = mapModel(requestedModel);
  const upstreamPayload = {
    ...payload,
    model: mappedModel,
  };

  logInfo("proxy chat.completions", {
    requestedModel: requestedModel || "",
    mappedModel,
    stream: Boolean(upstreamPayload.stream),
  });

  const result = await fetchWithEndpointFailover({
    req,
    apiKey,
    path: "/chat/completions",
    payload: upstreamPayload,
  });
  if (!result.ok) {
    const passthroughHeaders = {
      "x-glm-codex-requested-model": String(requestedModel || ""),
      "x-glm-codex-mapped-model": mappedModel,
      "x-glm-codex-upstream": result.baseUrl,
      ...(result.upstreamRequestId ? { "x-upstream-request-id": result.upstreamRequestId } : {}),
    };
    if (maybeSendNormalizedQuotaError(res, result.status, result.text, passthroughHeaders)) {
      return;
    }
    res.writeHead(result.status, {
      "content-type": result.contentType,
      ...passthroughHeaders,
    });
    res.end(result.text);
    return;
  }

  const { upstream, baseUrl } = result;

  const isSse = String(upstream.headers.get("content-type") || "").includes("text/event-stream");
  const upstreamRequestId = getUpstreamRequestId(upstream.headers);
  const passthroughHeaders = {
    "x-glm-codex-requested-model": String(requestedModel || ""),
    "x-glm-codex-mapped-model": mappedModel,
    "x-glm-codex-upstream": baseUrl,
    ...(upstreamRequestId ? { "x-upstream-request-id": upstreamRequestId } : {}),
  };

  if (isSse) {
    res.writeHead(upstream.status, {
      "content-type": "text/event-stream; charset=utf-8",
      "cache-control": "no-cache",
      connection: "keep-alive",
      ...passthroughHeaders,
    });
    await streamUpstreamBodyToHttpResponse(upstream, res);
    return;
  }

  const text = await upstream.text();
  if (!upstream.ok) {
    logInfo("upstream chat.completions error", {
      status: upstream.status,
      upstreamRequestId,
      body: toPreviewText(text),
    });
    if (maybeSendNormalizedQuotaError(res, upstream.status, text, passthroughHeaders)) {
      return;
    }
  }
  res.writeHead(upstream.status, {
    "content-type": upstream.headers.get("content-type") || "application/json; charset=utf-8",
    ...passthroughHeaders,
  });
  res.end(text);
}

async function proxyResponses(req, res, payload, apiKey) {
  const requestedModel = payload?.model;
  const mappedModel = mapModel(requestedModel);

  // Debug: log raw tools from request
  logDebug("proxyResponses: raw payload tools", {
    hasTools: Array.isArray(payload?.tools),
    toolsCount: Array.isArray(payload?.tools) ? payload.tools.length : 0,
    toolTypes: Array.isArray(payload?.tools) ? payload.tools.slice(0, 3).map((t) => t?.type) : [],
    firstToolPreview: Array.isArray(payload?.tools) && payload.tools.length > 0
      ? JSON.stringify(payload.tools[0]).slice(0, 300)
      : null,
  });

  // Check if tools are present and agent loop is enabled
  const hasTools = Array.isArray(payload?.tools) && payload.tools.length > 0;
  const agentLoopEnabled = process.env.GLM_AGENT_LOOP_ENABLED !== "false";

  logInfo("proxy responses", {
    requestedModel: requestedModel || "",
    mappedModel,
    stream: Boolean(payload?.stream),
    hasTools,
    agentLoopEnabled,
  });

  // ★If tools are present and agent loop is enabled, use agent loop for server-side tool execution
  if (hasTools && agentLoopEnabled) {
    logInfo("Using agent loop for server-side tool execution", {
      toolsCount: payload.tools.length,
    });
    return runAgentLoop({
      req,
      res,
      payload,
      apiKey,
      requestedModel,
      mappedModel,
    });
  }

  // Legacy path: No tools or agent loop disabled - use direct conversion
  const upstreamPayload = {
    ...payload,
    model: mappedModel,
  };

  // Responses API ペイロードを Chat Completions ペイロードに変換
  const chatPayload = toChatCompletionsPayloadFromResponses(upstreamPayload);
  chatPayload.stream = Boolean(upstreamPayload.stream); // ストリーミング設定を維持

  // Chat Completions エンドポイントを直接呼び出し
  logInfo("proxy responses: fetching chat.completions (legacy path)", {
    mappedModel,
    stream: chatPayload.stream,
  });

  const chatResult = await fetchWithEndpointFailover({
    req,
    apiKey,
    path: "/chat/completions",
    payload: chatPayload,
    endpointCandidates: buildEndpointCandidatesForChatFallback(),
  });

  // エラーハンドリング
  if (!chatResult.ok) {
    const chatText = chatResult.text;
    const chatUpstreamRequestId = chatResult.upstreamRequestId;
    logInfo("chat.completions error", {
      status: chatResult.status,
      upstreamRequestId: chatUpstreamRequestId,
      baseUrl: chatResult.baseUrl,
      body: toPreviewText(chatText),
    });

    if (maybeSendNormalizedQuotaError(res, chatResult.status, chatText, {
      "x-glm-codex-conversion": "chat->responses-error",
      "x-glm-codex-upstream": chatResult.baseUrl,
      ...(chatUpstreamRequestId ? { "x-upstream-request-id": chatUpstreamRequestId } : {}),
    })) {
      return;
    }

    res.writeHead(chatResult.status, {
      "content-type": chatResult.contentType,
      "x-glm-codex-conversion": "chat->responses-error",
      "x-glm-codex-upstream": chatResult.baseUrl,
      ...(chatUpstreamRequestId ? { "x-upstream-request-id": chatUpstreamRequestId } : {}),
    });
    res.end(chatText);
    return;
  }

  // 成功時のレスポンス変換
  const isSse = String(chatResult.upstream.headers.get("content-type") || "").includes("text/event-stream");

  if (isSse) {
    // ストリーミング: Chat Completions SSE を Responses API SSE にリアルタイム変換
    await streamChatCompletionsToResponsesSse(chatResult.upstream, res, {
      requestedModel: requestedModel || "",
      mappedModel,
      baseUrl: chatResult.baseUrl,
    });
    return;
  }

  // 非ストリーミング: レスポンス変換
  let chatText;
  try {
    chatText = await chatResult.upstream.text();
  } catch (readError) {
    logInfo("failed to read chat.completions response", {
      message: readError instanceof Error ? readError.message : String(readError),
    });
    sendJson(
      res,
      502,
      {
        error: {
          message: "Failed to read /chat/completions response: " + (readError instanceof Error ? readError.message : String(readError)),
          type: "bad_gateway",
        },
      },
      {
        "x-glm-codex-conversion": "chat->responses-read-error",
      },
    );
    return;
  }

  let chatJson;
  try {
    chatJson = JSON.parse(chatText);
  } catch (parseError) {
    logInfo("failed to parse chat.completions response", {
      error: parseError instanceof Error ? parseError.message : String(parseError),
    });
    sendJson(
      res,
      502,
      {
        error: {
          message: "Failed to parse /chat/completions response: " + (parseError instanceof Error ? parseError.message : String(parseError)),
          type: "bad_gateway",
        },
      },
      {
        "x-glm-codex-conversion": "chat->responses-parse-error",
      },
    );
    return;
  }

  // 既存関数 toResponsesObjectFromChat を使用（524-551行目）
  const converted = toResponsesObjectFromChat(chatJson, mappedModel);

  if (upstreamPayload.stream && RESPONSES_STREAM_FALLBACK_MODE === "sse") {
    // 既存関数 sendResponsesSseFromObject を使用（647-774行目）
    sendResponsesSseFromObject(res, converted, {
      "x-glm-codex-conversion": "chat->responses-stream-simulated",
      "x-glm-codex-requested-model": String(requestedModel || ""),
      "x-glm-codex-mapped-model": mappedModel,
      "x-glm-codex-upstream": chatResult.baseUrl,
    });
  } else {
    sendJson(res, 200, converted, {
      "x-glm-codex-conversion": "chat->responses",
      "x-glm-codex-requested-model": String(requestedModel || ""),
      "x-glm-codex-mapped-model": mappedModel,
      "x-glm-codex-upstream": chatResult.baseUrl,
    });
  }
}

function handleModels(res) {
  // Define model metadata for Codex CLI compatibility
  const modelMetadata = {
    "gpt-5.3-codex": {
      id: "gpt-5.3-codex",
      object: "model",
      created: 1677610602,
      owned_by: "openai",
      type: "chat",
      max_tokens: 128000,
      context_length: 128000,
    },
    "gpt-5.2-codex": {
      id: "gpt-5.2-codex",
      object: "model",
      created: 1677610602,
      owned_by: "openai",
      type: "chat",
      max_tokens: 128000,
      context_length: 128000,
    },
    "gpt-5-codex": {
      id: "gpt-5-codex",
      object: "model",
      created: 1677610602,
      owned_by: "openai",
      type: "chat",
      max_tokens: 128000,
      context_length: 128000,
    },
    "gpt-5-mini": {
      id: "gpt-5-mini",
      object: "model",
      created: 1677610602,
      owned_by: "openai",
      type: "chat",
      max_tokens: 128000,
      context_length: 128000,
    },
    "codex-mini-latest": {
      id: "codex-mini-latest",
      object: "model",
      created: 1677610602,
      owned_by: "openai",
      type: "chat",
      max_tokens: 128000,
      context_length: 128000,
    },
    // GLM models for direct usage
    "glm-4.7": {
      id: "glm-4.7",
      object: "model",
      created: 1677610602,
      owned_by: "zai",
      type: "chat",
      max_tokens: 128000,
      context_length: 128000,
    },
    "glm-4.7-flash": {
      id: "glm-4.7-flash",
      object: "model",
      created: 1677610602,
      owned_by: "zai",
      type: "chat",
      max_tokens: 128000,
      context_length: 128000,
    },
  };

  const models = Object.values(modelMetadata);

  sendJson(res, 200, { object: "list", data: models });
}

function handleHealth(res) {
  sendJson(res, 200, {
    ok: true,
    service: "glm-codex-gateway",
    host: HOST,
    port: PORT,
    zaiBaseUrl: ZAI_BASE_URL,
    zaiCodingBaseUrl: ZAI_CODING_BASE_URL,
    zaiEndpointMode: ZAI_ENDPOINT_MODE,
    activeZaiBaseUrl: ACTIVE_ZAI_BASE_URL,
    responsesStreamFallbackMode: RESPONSES_STREAM_FALLBACK_MODE,
    hasZaiApiKey: Boolean(ZAI_API_KEY),
    modelDefault: MODEL_DEFAULT,
  });
}

const server = createServer(async (req, res) => {
  const reqId = randomUUID().slice(0, 8);
  const method = String(req.method || "GET").toUpperCase();
  const url = new URL(String(req.url || "/"), `http://${HOST}:${PORT}`);

  res.setHeader("x-glm-codex-gateway", "1");
  res.setHeader("x-request-id", reqId);

  try {
    if (method === "GET" && url.pathname === "/healthz") {
      handleHealth(res);
      return;
    }

    if (method === "GET" && url.pathname === "/v1/models") {
      handleModels(res);
      return;
    }

    if (method !== "POST") {
      sendJson(res, 405, {
        error: {
          message: `Method not allowed: ${method}`,
          type: "invalid_request_error",
        },
      });
      return;
    }

    const apiKey = resolveUpstreamApiKey(req);
    if (!apiKey) {
      sendJson(res, 401, {
        error: {
          message:
            "No API key available. Set ZAI_API_KEY on gateway or pass Authorization Bearer from client. / APIキーがありません。ゲートウェイにZAI_API_KEYを設定するか、クライアントからBearerを渡してください。",
          type: "authentication_error",
        },
      });
      return;
    }

    const payload = await readJsonBody(req);

    if (url.pathname === "/v1/chat/completions") {
      await proxyChatCompletions(req, res, payload, apiKey);
      return;
    }

    if (url.pathname === "/v1/responses") {
      await proxyResponses(req, res, payload, apiKey);
      return;
    }

    sendJson(res, 404, {
      error: {
        message: `Route not found: ${url.pathname}`,
        type: "invalid_request_error",
      },
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const status = message === "INVALID_JSON" ? 400 : 500;
    const errMeta = err && typeof err === "object" && "meta" in err ? err.meta : undefined;
    logInfo("request error", { reqId, path: url.pathname, message, ...(errMeta ? { meta: errMeta } : {}) });
    sendJson(res, status, {
      error: {
        message,
        type: status === 400 ? "invalid_request_error" : "server_error",
      },
    });
  } finally {
    logDebug("request finished", { reqId, method, path: url.pathname });
  }
});

server.listen(PORT, HOST, () => {
  logInfo("started", {
    host: HOST,
    port: PORT,
    zaiBaseUrl: ZAI_BASE_URL,
    zaiCodingBaseUrl: ZAI_CODING_BASE_URL,
    zaiEndpointMode: ZAI_ENDPOINT_MODE,
    activeZaiBaseUrl: ACTIVE_ZAI_BASE_URL,
    responsesStreamFallbackMode: RESPONSES_STREAM_FALLBACK_MODE,
    modelDefault: MODEL_DEFAULT,
    hasZaiApiKey: Boolean(ZAI_API_KEY),
  });
  logInfo("usage", {
    health: `http://${HOST}:${PORT}/healthz`,
    baseUrlForCodex: `http://${HOST}:${PORT}/v1`,
  });
});

server.on("error", (err) => {
  const message = err instanceof Error ? err.message : String(err);
  logInfo("server error", { message, host: HOST, port: PORT });
  process.exit(1);
});

// ============================================================
// Tool Executor Implementation
// ============================================================

const TOOL_EXECUTION_TIMEOUT_MS = Number(process.env.GLM_TOOL_TIMEOUT_MS || "60000");
const MAX_FILE_SIZE = Number(process.env.GLM_MAX_FILE_SIZE || "1048576");
const MAX_COMMAND_OUTPUT = 10000;
const MAX_GLOB_RESULTS = 100;

class ToolExecutor {
  constructor(options = {}) {
    this.workspaceRoot = path.resolve(options.workspaceRoot || process.cwd());
    this.allowedPaths = options.allowedPaths
      ? options.allowedPaths.map((p) => path.resolve(p))
      : [this.workspaceRoot];
    this.timeout = options.timeout || TOOL_EXECUTION_TIMEOUT_MS;
    this.maxFileSize = options.maxFileSize || MAX_FILE_SIZE;
  }

  async execute(name, args) {
    logDebug("ToolExecutor.execute", { name, args: JSON.stringify(args).slice(0, 200) });

    switch (name) {
      case "read_file":
      case "readFile":
        return this.readFile(args);
      case "write_file":
      case "writeFile":
        return this.writeFile(args);
      case "edit_file":
      case "editFile":
      case "apply_patch":
      case "applyPatch":
        return this.editFile(args);
      case "list_directory":
      case "listDirectory":
      case "list_files":
      case "listFiles":
        return this.listDirectory(args);
      case "glob":
      case "search_files":
      case "searchFiles":
        return this.glob(args);
      case "exec":
      case "execute":
      case "shell":
      case "run_command":
      case "runCommand":
      case "exec_command":
        return this.exec(args);
      default:
        throw new Error(`Unknown tool: ${name}`);
    }
  }

  validatePath(filepath) {
    const resolved = path.resolve(this.workspaceRoot, filepath);
    for (const allowed of this.allowedPaths) {
      if (resolved.startsWith(allowed)) {
        return resolved;
      }
    }
    throw new Error(`Path not allowed: ${filepath}. Allowed paths: ${this.allowedPaths.join(", ")}`);
  }

  async readFile(args) {
    const filepath = this.validatePath(args.file_path || args.path || args.filePath || "");
    const content = await readFile(filepath, "utf-8");

    if (content.length > this.maxFileSize) {
      return (
        content.slice(0, this.maxFileSize) +
        `\n... (truncated, file is ${content.length} bytes, showing first ${this.maxFileSize} bytes)`
      );
    }
    return content;
  }

  async writeFile(args) {
    const filepath = this.validatePath(args.file_path || args.path || args.filePath || "");
    const content = args.content ?? args.text ?? "";
    await writeFile(filepath, content, "utf-8");
    return JSON.stringify({ success: true, message: `File written: ${filepath}` });
  }

  async editFile(args) {
    const filepath = this.validatePath(args.file_path || args.path || args.filePath || "");
    let content = await readFile(filepath, "utf-8");

    const oldText = args.old_text ?? args.oldText ?? args.old_string ?? args.oldString;
    const newText = args.new_text ?? args.newText ?? args.new_string ?? args.newString ?? "";

    if (oldText && content.includes(oldText)) {
      if (args.replace_all ?? args.replaceAll) {
        content = content.split(oldText).join(newText);
      } else {
        content = content.replace(oldText, newText);
      }
      await writeFile(filepath, content, "utf-8");
      return JSON.stringify({ success: true, message: `File edited: ${filepath}` });
    }
    throw new Error(`old_text not found in file: ${filepath}`);
  }

  async listDirectory(args) {
    const dirpath = this.validatePath(args.path || args.directory || ".");
    const entries = await readdir(dirpath, { withFileTypes: true });
    const lines = entries.map((e) => `${e.isDirectory() ? "d" : e.isSymbolicLink() ? "l" : "f"} ${e.name}`);
    return lines.join("\n");
  }

  async glob(args) {
    const pattern = args.pattern || "**/*";
    const cwd = this.validatePath(args.path || args.cwd || ".");

    // Use Node.js built-in glob or fallback to simple implementation
    let matches = [];
    try {
      // Try dynamic import for glob package
      const globModule = await import("glob").catch(() => null);
      if (globModule && globModule.glob) {
        matches = await globModule.glob(pattern, { cwd, nodir: true });
      } else {
        // Fallback: use readdir recursively for simple patterns
        matches = await this.simpleGlob(pattern, cwd);
      }
    } catch (e) {
      logDebug("glob error, using fallback", { error: e.message });
      matches = await this.simpleGlob(pattern, cwd);
    }

    const limited = matches.slice(0, MAX_GLOB_RESULTS);
    if (matches.length > MAX_GLOB_RESULTS) {
      limited.push(`... (${matches.length - MAX_GLOB_RESULTS} more results truncated)`);
    }
    return limited.join("\n");
  }

  async simpleGlob(_pattern, cwd) {
    // Simple recursive file listing as fallback (pattern matching not fully implemented)
    const results = [];
    const seen = new Set();

    const walk = async (dir, depth = 0) => {
      if (depth > 10) return; // Prevent infinite recursion
      try {
        const entries = await readdir(dir, { withFileTypes: true });
        for (const entry of entries) {
          if (entry.name.startsWith(".")) continue;
          const fullPath = path.join(dir, entry.name);
          if (entry.isDirectory()) {
            await walk(fullPath, depth + 1);
          } else if (entry.isFile()) {
            const relative = path.relative(cwd, fullPath);
            if (!seen.has(relative)) {
              seen.add(relative);
              results.push(relative);
            }
          }
        }
      } catch (e) {
        // Ignore permission errors
      }
    };

    await walk(cwd);
    return results;
  }

  async exec(args) {
    const command = args.command || args.cmd || "";
    const cmdArgs = args.args || args.arguments || [];

    if (!command) {
      throw new Error("No command specified");
    }

    // Security: Block dangerous commands
    const blockedPatterns = ["rm -rf /", "mkfs", "dd if=/dev/zero", ">: /dev/sd"];
    for (const blocked of blockedPatterns) {
      if (command.includes(blocked)) {
        throw new Error(`Blocked command pattern detected`);
      }
    }

    return new Promise((resolve) => {
      let resolved = false;
      let proc = null;

      const timeout = setTimeout(() => {
        if (!resolved && proc) {
          resolved = true;
          proc.kill("SIGKILL");
          resolve(
            JSON.stringify({
              error: "Command timeout",
              exitCode: -1,
              stdout: "",
              stderr: `Command timed out after ${this.timeout}ms`,
            })
          );
        }
      }, this.timeout);

      proc = spawn(command, Array.isArray(cmdArgs) ? cmdArgs : [], {
        cwd: this.workspaceRoot,
        shell: true,
        timeout: this.timeout,
      });

      let stdout = "";
      let stderr = "";

      proc.stdout.on("data", (data) => {
        stdout += String(data);
      });

      proc.stderr.on("data", (data) => {
        stderr += String(data);
      });

      proc.on("close", (code) => {
        if (resolved) return;
        resolved = true;
        clearTimeout(timeout);
        resolve(
          JSON.stringify({
            exitCode: code ?? 0,
            stdout: stdout.slice(0, MAX_COMMAND_OUTPUT),
            stderr: stderr.slice(0, MAX_COMMAND_OUTPUT / 2),
            truncated: stdout.length > MAX_COMMAND_OUTPUT || stderr.length > MAX_COMMAND_OUTPUT / 2,
          })
        );
      });

      proc.on("error", (err) => {
        if (resolved) return;
        resolved = true;
        clearTimeout(timeout);
        resolve(
          JSON.stringify({
            error: err.message,
            exitCode: -1,
            stdout: "",
            stderr: err.message,
          })
        );
      });
    });
  }
}

// ============================================================
// Agent Loop Implementation
// ============================================================

const MAX_AGENT_LOOP_ITERATIONS = Number(process.env.GLM_MAX_LOOP_ITERATIONS || "50");

function writeSseEvent(res, event) {
  res.write(`event: ${event.type}\n`);
  res.write(`data: ${JSON.stringify(event)}\n\n`);
}

function writeToolResultEvent(res, responseId, toolCall, toolResult) {
  writeSseEvent(res, {
    type: "response.function_call_output",
    response_id: responseId,
    tool_call_id: toolCall.id,
    output: toolResult,
  });
}

async function sendFinalResponse(res, responseId, loopResult, totalUsage, mappedModel) {
  const finalResponse = {
    id: responseId,
    object: "response",
    created_at: Math.floor(Date.now() / 1000),
    status: "completed",
    model: mappedModel,
    output: [
      {
        type: "message",
        id: `msg_${randomUUID().replace(/-/g, "")}`,
        role: "assistant",
        content: [{ type: "output_text", text: loopResult.assistantContent || "" }],
        status: "completed",
      },
    ],
    output_text: loopResult.assistantContent || "",
    usage: {
      input_tokens: totalUsage.input_tokens + loopResult.usage.input_tokens,
      output_tokens: totalUsage.output_tokens + loopResult.usage.output_tokens,
      total_tokens: totalUsage.input_tokens + totalUsage.output_tokens + loopResult.usage.input_tokens + loopResult.usage.output_tokens,
    },
  };

  writeSseEvent(res, { type: "response.completed", response_id: responseId, response: finalResponse });
  res.write("data: [DONE]\n\n");
  res.end();
}

async function sendMaxIterationResponse(res, responseId, iteration) {
  writeSseEvent(res, {
    type: "response.failed",
    response_id: responseId,
    response: {
      id: responseId,
      status: "failed",
      error: {
        type: "max_iterations_reached",
        message: `Agent loop reached maximum iterations (${iteration})`,
      },
    },
  });
  res.write("data: [DONE]\n\n");
  res.end();
}

async function handleLoopError(res, result, responseId) {
  const errorMessage = result.error?.message || result.text || "Unknown error";
  writeSseEvent(res, {
    type: "response.failed",
    response_id: responseId,
    response: {
      id: responseId,
      status: "failed",
      error: {
        type: "server_error",
        message: errorMessage,
      },
    },
  });
  res.write("data: [DONE]\n\n");
  res.end();
}

function buildInitialMessages(payload) {
  const input = payload?.input;
  const messages = [];

  // Add system instruction if present
  if (payload?.instructions) {
    messages.push({ role: "system", content: payload.instructions });
  }

  if (typeof input === "string") {
    messages.push({ role: "user", content: input });
  } else if (Array.isArray(input)) {
    for (const item of input) {
      if (!item || typeof item !== "object") continue;

      if (item.type === "message") {
        const role = normalizeChatRole(item.role);
        if (role === "tool") {
          messages.push(convertToolResultToChatMessage(item));
        } else {
          let content = "";
          if (typeof item.content === "string") {
            content = item.content;
          } else if (Array.isArray(item.content)) {
            content = item.content
              .map((c) => {
                if (!c || typeof c !== "object") return "";
                if (typeof c.text === "string") return c.text;
                return "";
              })
              .filter(Boolean)
              .join("\n");
          }
          if (content) {
            messages.push({ role, content });
          }
        }
      }

      // Handle function_call_output (tool results in Responses API format)
      if (item.type === "function_call_output") {
        messages.push({
          role: "tool",
          tool_call_id: item.call_id || "",
          content: typeof item.output === "string" ? item.output : JSON.stringify(item.output || {}),
        });
      }

      // Handle previous assistant messages with tool_calls
      if (item.type === "function_call" || item.type === "tool_use") {
        // These are typically in the input history, convert to assistant tool_calls
        const assistantIndex = messages.findLastIndex((m) => m.role === "assistant");
        if (assistantIndex !== -1) {
          const existingMsg = messages[assistantIndex];
          const existingToolCalls = existingMsg.tool_calls || [];
          messages[assistantIndex] = {
            ...existingMsg,
            tool_calls: [
              ...existingToolCalls,
              {
                id: item.id || `call_${randomUUID().replace(/-/g, "").slice(0, 24)}`,
                type: "function",
                function: {
                  name: item.name || item.function?.name || "",
                  arguments: typeof item.arguments === "string" ? item.arguments : JSON.stringify(item.arguments || {}),
                },
              },
            ],
          };
        }
      }
    }
  }

  return messages;
}

function extractToolDefinitions(payload) {
  if (!Array.isArray(payload?.tools)) {
    logDebug("extractToolDefinitions: no tools array", { toolsType: typeof payload?.tools });
    return [];
  }
  logDebug("extractToolDefinitions: raw tools", {
    toolsCount: payload.tools.length,
    toolTypes: payload.tools.map((t) => t?.type).slice(0, 10),
    firstTool: JSON.stringify(payload.tools[0]).slice(0, 200),
  });
  const converted = convertResponsesToolsToChatTools(payload.tools);
  logDebug("extractToolDefinitions: converted", { convertedCount: converted.length });
  return converted;
}

function parseAllowedPaths(envValue) {
  if (!envValue) return null;
  return envValue
    .split(":")
    .map((p) => p.trim())
    .filter(Boolean);
}

async function processAgentLoopStream(params) {
  const { upstream, res, responseId, isFirstIteration } = params;
  const reader = upstream.body?.getReader?.();

  if (!reader) {
    return {
      assistantContent: "",
      toolCalls: [],
      usage: { input_tokens: 0, output_tokens: 0 },
      error: "No stream reader available",
    };
  }

  const decoder = new TextDecoder("utf-8");
  let buffer = "";
  let assistantContent = "";
  let toolCalls = [];
  let usage = { input_tokens: 0, output_tokens: 0 };
  let outputItemId = `msg_${randomUUID().replace(/-/g, "")}`;

  // Send initial events on first iteration
  if (isFirstIteration) {
    const initialResponse = {
      id: responseId,
      object: "response",
      created_at: Math.floor(Date.now() / 1000),
      status: "in_progress",
      model: params.mappedModel || MODEL_DEFAULT,
      output: [],
      usage: { input_tokens: 0, output_tokens: 0, total_tokens: 0 },
    };

    writeSseEvent(res, { type: "response.created", response_id: responseId, response: initialResponse });
    writeSseEvent(res, { type: "response.in_progress", response_id: responseId, response: initialResponse });
    writeSseEvent(res, {
      type: "response.output_item.added",
      response_id: responseId,
      output_index: 0,
      item: {
        type: "message",
        id: outputItemId,
        role: "assistant",
        content: [],
        status: "in_progress",
      },
    });
    writeSseEvent(res, {
      type: "response.content_part.added",
      response_id: responseId,
      item_id: outputItemId,
      output_index: 0,
      content_index: 0,
      part: { type: "output_text", text: "" },
    });
  }

  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split("\n");
      buffer = lines.pop() || "";

      for (const line of lines) {
        if (!line.trim() || !line.startsWith("data: ")) continue;
        const data = line.slice(6).trim();
        if (data === "[DONE]") continue;

        try {
          const parsed = JSON.parse(data);
          const delta = parsed.choices?.[0]?.delta;
          const finishReason = parsed.choices?.[0]?.finish_reason;

          // Text content
          if (typeof delta?.content === "string" && delta.content) {
            assistantContent += delta.content;
            writeSseEvent(res, {
              type: "response.output_text.delta",
              response_id: responseId,
              item_id: outputItemId,
              output_index: 0,
              content_index: 0,
              delta: delta.content,
            });
          }

          // Tool calls streaming
          if (delta?.tool_calls) {
            for (const tc of delta.tool_calls) {
              const idx = tc.index ?? toolCalls.length;
              if (!toolCalls[idx]) {
                toolCalls[idx] = {
                  id: tc.id || "",
                  type: "function",
                  function: { name: "", arguments: "" },
                };
              }
              if (tc.id && !toolCalls[idx].id) {
                toolCalls[idx].id = tc.id;
              }
              if (tc.function?.name) {
                toolCalls[idx].function.name = tc.function.name;
              }
              if (tc.function?.arguments) {
                toolCalls[idx].function.arguments += tc.function.arguments;
              }
            }
          }

          // Usage info
          if (parsed.usage) {
            usage.input_tokens = parsed.usage.prompt_tokens ?? parsed.usage.input_tokens ?? usage.input_tokens;
            usage.output_tokens = parsed.usage.completion_tokens ?? parsed.usage.output_tokens ?? usage.output_tokens;
          }

          // Handle finish_reason for tool_calls
          if (finishReason === "tool_calls" || finishReason === "tool_use") {
            logDebug("Tool calls detected", { toolCallsCount: toolCalls.length });
          }
        } catch (e) {
          // Parse errors are ignored
        }
      }
    }
  } catch (e) {
    logInfo("Stream processing error", { error: e.message });
  }

  return { assistantContent, toolCalls, usage, outputItemId };
}

async function executeToolCall(toolExecutor, toolCall) {
  const name = toolCall.function?.name || toolCall.name || "";
  let args = {};

  try {
    args = JSON.parse(toolCall.function?.arguments || toolCall.arguments || "{}");
  } catch (e) {
    return JSON.stringify({ error: "Invalid JSON arguments", raw: toolCall.function?.arguments });
  }

  try {
    logInfo("Executing tool", { name, argsPreview: JSON.stringify(args).slice(0, 100) });
    const result = await toolExecutor.execute(name, args);
    logInfo("Tool execution completed", { name, resultLength: String(result).length });
    return typeof result === "string" ? result : JSON.stringify(result);
  } catch (error) {
    logInfo("Tool execution failed", { name, error: error.message });
    return JSON.stringify({
      error: error.message || "Tool execution failed",
      tool: name,
    });
  }
}

async function runAgentLoop(params) {
  const { req, res, payload, apiKey, requestedModel, mappedModel } = params;
  const responseId = `resp_${randomUUID().replace(/-/g, "")}`;
  const messages = buildInitialMessages(payload);
  const tools = extractToolDefinitions(payload);

  const toolExecutor = new ToolExecutor({
    workspaceRoot: process.env.GLM_WORKSPACE_ROOT || process.cwd(),
    allowedPaths: parseAllowedPaths(process.env.GLM_ALLOWED_PATHS),
    timeout: TOOL_EXECUTION_TIMEOUT_MS,
  });

  // Set up streaming headers
  res.writeHead(200, {
    "Content-Type": "text/event-stream; charset=utf-8",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "x-glm-codex-agent-loop": "enabled",
    "x-glm-codex-requested-model": String(requestedModel || ""),
    "x-glm-codex-mapped-model": mappedModel,
  });
  res.flushHeaders?.();

  let iteration = 0;
  let totalUsage = { input_tokens: 0, output_tokens: 0 };
  let currentOutputItemId = null;

  logInfo("Agent loop started", {
    responseId,
    mappedModel,
    messagesCount: messages.length,
    toolsCount: tools.length,
  });

  try {
    while (iteration < MAX_AGENT_LOOP_ITERATIONS) {
      iteration++;
      logDebug(`Agent loop iteration ${iteration}/${MAX_AGENT_LOOP_ITERATIONS}`);

      // Build chat payload for this iteration
      const chatPayload = {
        model: mappedModel,
        messages: messages,
        stream: true,
        ...(tools.length > 0 && { tools, tool_choice: "auto" }),
      };

      const result = await fetchWithEndpointFailover({
        req,
        apiKey,
        path: "/chat/completions",
        payload: chatPayload,
        endpointCandidates: buildEndpointCandidatesForChatFallback(),
      });

      if (!result.ok) {
        logInfo("Agent loop fetch failed", {
          iteration,
          status: result.status,
          body: toPreviewText(result.text),
        });
        await handleLoopError(res, result, responseId);
        return;
      }

      // Process the stream
      const loopResult = await processAgentLoopStream({
        upstream: result.upstream,
        res,
        responseId,
        iteration,
        isFirstIteration: iteration === 1,
        mappedModel,
      });

      currentOutputItemId = loopResult.outputItemId;
      totalUsage.input_tokens += loopResult.usage.input_tokens;
      totalUsage.output_tokens += loopResult.usage.output_tokens;

      // Check for tool calls
      const validToolCalls = loopResult.toolCalls.filter((tc) => tc?.id && tc?.function?.name);

      if (validToolCalls.length > 0) {
        logInfo("Tool calls detected in response", {
          iteration,
          toolCallsCount: validToolCalls.length,
          tools: validToolCalls.map((tc) => tc.function.name),
        });

        // Send tool call events
        for (const tc of validToolCalls) {
          writeSseEvent(res, {
            type: "response.function_call_arguments.delta",
            response_id: responseId,
            tool_call_id: tc.id,
            delta: tc.function.arguments,
          });
          writeSseEvent(res, {
            type: "response.function_call.completed",
            response_id: responseId,
            tool_call_id: tc.id,
            name: tc.function.name,
            arguments: tc.function.arguments,
          });
        }

        // Add assistant message with tool_calls
        messages.push({
          role: "assistant",
          content: loopResult.assistantContent || null,
          tool_calls: validToolCalls.map((tc) => ({
            id: tc.id,
            type: "function",
            function: {
              name: tc.function.name,
              arguments: tc.function.arguments,
            },
          })),
        });

        // Execute tools and add results
        for (const tc of validToolCalls) {
          const toolResult = await executeToolCall(toolExecutor, tc);

          // Send tool result event
          writeToolResultEvent(res, responseId, tc, toolResult);

          // Add tool result to messages
          messages.push({
            role: "tool",
            tool_call_id: tc.id,
            content: toolResult,
          });
        }

        // Continue loop to get next response
        continue;
      }

      // No tool calls - this is the final response
      logInfo("Agent loop completed", {
        iteration,
        totalInputTokens: totalUsage.input_tokens + loopResult.usage.input_tokens,
        totalOutputTokens: totalUsage.output_tokens + loopResult.usage.output_tokens,
        responseLength: loopResult.assistantContent.length,
      });

      // Send final events
      if (loopResult.assistantContent) {
        writeSseEvent(res, {
          type: "response.output_text.done",
          response_id: responseId,
          item_id: currentOutputItemId,
          output_index: 0,
          content_index: 0,
          text: loopResult.assistantContent,
        });
      }

      await sendFinalResponse(res, responseId, loopResult, totalUsage, mappedModel);
      return;
    }

    // Max iterations reached
    logInfo("Agent loop reached max iterations", { iteration: MAX_AGENT_LOOP_ITERATIONS });
    await sendMaxIterationResponse(res, responseId, iteration);
  } catch (error) {
    logInfo("Agent loop error", {
      error: error.message,
      stack: error.stack?.split("\n").slice(0, 3),
    });
    await handleLoopError(res, { error }, responseId);
  }
}
