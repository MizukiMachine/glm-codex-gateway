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

const HOST = process.env.GLM_CODEX_GATEWAY_HOST || "127.0.0.1";
const PORT = Number(process.env.GLM_CODEX_GATEWAY_PORT || "8787");
const ZAI_GLOBAL_BASE_URL = "https://api.z.ai/api/paas/v4";
const ZAI_CODING_GLOBAL_BASE_URL = "https://api.z.ai/api/coding/paas/v4";
const ZAI_BASE_URL = (process.env.ZAI_BASE_URL || ZAI_GLOBAL_BASE_URL).replace(/\/+$/, "");
const ZAI_CODING_BASE_URL = (process.env.ZAI_CODING_BASE_URL || ZAI_CODING_GLOBAL_BASE_URL).replace(/\/+$/, "");
const ZAI_ENDPOINT_MODE = (process.env.GLM_ZAI_ENDPOINT_MODE || "auto").trim().toLowerCase();
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
    add(ZAI_CODING_BASE_URL);
    return out;
  }

  // For chat fallback after /responses failure, prefer coding endpoint first
  add(ZAI_CODING_BASE_URL);
  add(ZAI_BASE_URL);
  add(ACTIVE_ZAI_BASE_URL);
  return out;
}

function shouldTryNextEndpoint(status, failText) {
  if (ZAI_ENDPOINT_MODE !== "auto") {
    return false;
  }
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

  // Some clients can send JSON text sequences (RFC 7464) or newline-delimited JSON.
  const seq = text
    .split(/\r?\n|\u001e/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => (line.startsWith("data:") ? line.slice(5).trim() : line))
    .filter(Boolean)
    .filter((line) => line !== "[DONE]");

  if (seq.length === 1) {
    return JSON.parse(seq[0]);
  }
  if (seq.length > 1) {
    return JSON.parse(seq[0]);
  }

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
    // Tool results are best-effort represented as assistant text in fallback.
    return "assistant";
  }
  return "user";
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
  const upstreamPayload = {
    ...payload,
    model: mappedModel,
  };

  logInfo("proxy responses", {
    requestedModel: requestedModel || "",
    mappedModel,
    stream: Boolean(upstreamPayload.stream),
  });

  const upstreamResult = await fetchWithEndpointFailover({
    req,
    apiKey,
    path: "/responses",
    payload: upstreamPayload,
  });

  if (upstreamResult.ok) {
    const upstream = upstreamResult.upstream;
    const upstreamBaseUrl = upstreamResult.baseUrl;
    const isSse = String(upstream.headers.get("content-type") || "").includes("text/event-stream");
    if (isSse) {
      res.writeHead(200, {
        "content-type": "text/event-stream; charset=utf-8",
        "cache-control": "no-cache",
        connection: "keep-alive",
        "x-glm-codex-requested-model": String(requestedModel || ""),
        "x-glm-codex-mapped-model": mappedModel,
        "x-glm-codex-upstream": upstreamBaseUrl,
        ...(getUpstreamRequestId(upstream.headers)
          ? { "x-upstream-request-id": getUpstreamRequestId(upstream.headers) }
          : {}),
      });
      await streamUpstreamBodyToHttpResponse(upstream, res);
      return;
    }

    const text = await upstream.text();
    res.writeHead(200, {
      "content-type": upstream.headers.get("content-type") || "application/json; charset=utf-8",
      "x-glm-codex-requested-model": String(requestedModel || ""),
      "x-glm-codex-mapped-model": mappedModel,
      "x-glm-codex-upstream": upstreamBaseUrl,
      ...(getUpstreamRequestId(upstream.headers)
        ? { "x-upstream-request-id": getUpstreamRequestId(upstream.headers) }
        : {}),
    });
    res.end(text);
    return;
  }

  const failText = upstreamResult.text;
  const upstreamRequestId = upstreamResult.upstreamRequestId;
  const canFallback = shouldFallbackResponses(upstreamResult.status, failText);
  logInfo("upstream responses error", {
    status: upstreamResult.status,
    upstreamRequestId,
    baseUrl: upstreamResult.baseUrl,
    canFallback,
    fallbackMode: RESPONSES_FALLBACK_MODE,
    body: toPreviewText(failText),
  });

  if (!canFallback) {
    res.writeHead(upstreamResult.status, {
      "content-type": upstreamResult.contentType,
      "x-glm-codex-upstream": upstreamResult.baseUrl,
      ...(upstreamRequestId ? { "x-upstream-request-id": upstreamRequestId } : {}),
    });
    res.end(failText);
    return;
  }

  const chatPayload = toChatCompletionsPayloadFromResponses(upstreamPayload);
  chatPayload.stream = false;

  let chatResult;
  try {
    chatResult = await fetchWithEndpointFailover({
      req,
      apiKey,
      path: "/chat/completions",
      payload: chatPayload,
      endpointCandidates: buildEndpointCandidatesForChatFallback(),
    });
  } catch (fetchError) {
    logInfo("fallback chat.completions fetch error", {
      message: fetchError instanceof Error ? fetchError.message : String(fetchError),
    });
    sendJson(
      res,
      502,
      {
        error: {
          message: "Fallback to /chat/completions failed: " + (fetchError instanceof Error ? fetchError.message : String(fetchError)),
          type: "bad_gateway",
        },
      },
      {
        "x-glm-codex-fallback": "responses->chat-fetch-error",
      },
    );
    return;
  }

  if (!chatResult.ok) {
    const chatText = chatResult.text;
    const chatUpstreamRequestId = chatResult.upstreamRequestId;
    logInfo("fallback chat.completions error", {
      status: chatResult.status,
      upstreamRequestId: chatUpstreamRequestId,
      baseUrl: chatResult.baseUrl,
      body: toPreviewText(chatText),
    });
    if (
      maybeSendNormalizedQuotaError(res, chatResult.status, chatText, {
        "x-glm-codex-fallback": "responses->chat",
        "x-glm-codex-upstream": chatResult.baseUrl,
        ...(chatUpstreamRequestId ? { "x-upstream-request-id": chatUpstreamRequestId } : {}),
      })
    ) {
      return;
    }
    res.writeHead(chatResult.status, {
      "content-type": chatResult.contentType,
      "x-glm-codex-fallback": "responses->chat",
      "x-glm-codex-upstream": chatResult.baseUrl,
      ...(chatUpstreamRequestId ? { "x-upstream-request-id": chatUpstreamRequestId } : {}),
    });
    res.end(chatText);
    return;
  }

  let chatText;
  try {
    chatText = await chatResult.upstream.text();
  } catch (readError) {
    logInfo("fallback chat.completions read error", {
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
        "x-glm-codex-fallback": "responses->chat-read-error",
      },
    );
    return;
  }

  let chatJson;
  try {
    chatJson = JSON.parse(chatText);
  } catch (parseError) {
    sendJson(
      res,
      502,
      {
        error: {
          message: "Fallback conversion failed: upstream chat response is not JSON.",
          type: "bad_gateway",
        },
      },
      {
        "x-glm-codex-fallback": "responses->chat-parse-error",
      },
    );
    return;
  }

  const converted = toResponsesObjectFromChat(chatJson, mappedModel);
  if (upstreamPayload.stream && RESPONSES_STREAM_FALLBACK_MODE === "sse") {
    sendResponsesSseFromObject(res, converted, {
      "x-glm-codex-fallback": "responses->chat-stream-simulated",
      "x-glm-codex-requested-model": String(requestedModel || ""),
      "x-glm-codex-mapped-model": mappedModel,
      "x-glm-codex-upstream": chatResult.baseUrl,
    });
    return;
  }
  sendJson(res, 200, converted, {
    "x-glm-codex-fallback":
      upstreamPayload.stream && RESPONSES_STREAM_FALLBACK_MODE !== "sse"
        ? "responses->chat-nonstream-fallback"
        : "responses->chat",
    "x-glm-codex-requested-model": String(requestedModel || ""),
    "x-glm-codex-mapped-model": mappedModel,
    "x-glm-codex-upstream": chatResult.baseUrl,
  });
}

function handleModels(req, res) {
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
      handleModels(req, res);
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
