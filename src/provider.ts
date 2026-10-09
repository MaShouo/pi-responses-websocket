import {
  clampThinkingLevel, createAssistantMessageEventStream,
  getDeclaredTools, normalizeContext, resolveTranscript, resolveTranscriptTools,
  type Api, type AssistantMessage, type Model, type OpenAIResponsesOptions,
  type ProviderStreams, type SimpleStreamOptions, type TranscriptContext,
} from "@earendil-works/pi-ai";
import {
  convertResponsesMessages, convertResponsesTools, processResponsesStream,
  createGrammarToolInputProperties, buildBaseOptions, resolveSamplingParams,
  clampOpenAIPromptCacheKey, type ConvertResponsesMessagesOptions,
} from "./pi-internals.ts";
import { ResponsesSocketPool, responsesUrl, WebSocketTransportError, type ResponseBody, type SocketLease, type WireEvent } from "./transport.ts";
import { sseEvents } from "./sse.ts";

export const PROVIDER_ID = "responses-websocket";
type ResponsesModel = Model<"openai-responses">;
type ParsedEvent = Parameters<typeof processResponsesStream>[0] extends AsyncIterable<infer T> ? T : never;

function headersFor(model: ResponsesModel, options: OpenAIResponsesOptions): Record<string, string> {
  const headers = new Headers();
  if (options.apiKey) headers.set("authorization", `Bearer ${options.apiKey}`);
  if (options.sessionId && options.cacheRetention !== "none") {
    headers.set("x-client-request-id", options.sessionId);
    if (model.compat?.sessionAffinityFormat === "openrouter") headers.set("x-session-id", options.sessionId);
    else if (model.compat?.sessionAffinityFormat !== "openai-nosession") headers.set("session_id", options.sessionId);
  }
  for (const source of [model.headers, options.headers]) {
    for (const [key, value] of Object.entries(source ?? {})) {
      if (value === null) headers.delete(key);
      else headers.set(key, value);
    }
  }
  // ws owns handshake/connection headers. Never forward HTTP body framing.
  for (const key of [...headers.keys()]) {
    if (["host", "connection", "upgrade", "content-length", "transfer-encoding"].includes(key) || key.startsWith("sec-websocket-")) {
      headers.delete(key);
    }
  }
  return Object.fromEntries(headers);
}

function serialization(model: ResponsesModel, context: TranscriptContext): ConvertResponsesMessagesOptions {
  const compat = model.compat;
  return {
    grammarToolInputProperties: createGrammarToolInputProperties(getDeclaredTools(context.messages), compat?.supportsOpenAIGrammarTools ?? false),
    supportsMidConvoSystemMessages: compat?.supportsMidConvoSystemMessages ?? false,
    supportsAdditionalTools: compat?.supportsAdditionalTools ?? false,
    supportsToolSearch: compat?.supportsToolSearch ?? false,
    toolOptions: {
      supportsStrictMode: compat?.supportsStrictMode ?? false,
      supportsOpenAIGrammarTools: compat?.supportsOpenAIGrammarTools ?? false,
    },
  };
}

function requestBody(model: ResponsesModel, context: TranscriptContext, options: OpenAIResponsesOptions, conversion: ConvertResponsesMessagesOptions): ResponseBody {
  const body: ResponseBody = {
    model: model.id,
    input: convertResponsesMessages(model, context, new Set([model.provider, "openai", "openai-codex", "opencode"]), conversion),
    store: false,
  };
  if (options.sessionId && options.cacheRetention !== "none") body.prompt_cache_key = clampOpenAIPromptCacheKey(options.sessionId);
  if (options.maxTokens !== undefined && model.compat?.supportsMaxOutputTokens !== false) body.max_output_tokens = Math.max(16, options.maxTokens);
  if (options.temperature !== undefined) body.temperature = options.temperature;
  if (options.toolChoice !== undefined) body.tool_choice = options.toolChoice;
  if (options.serviceTier !== undefined) body.service_tier = options.serviceTier;
  const tools = resolveTranscriptTools(context.messages, !!(conversion.supportsAdditionalTools || conversion.supportsToolSearch)).requestTools;
  if (tools.length) body.tools = convertResponsesTools(tools, conversion.toolOptions);
  if (model.reasoning) {
    const effort = options.reasoningEffort ?? (options.reasoningSummary ? "medium" : undefined);
    if (effort) {
      body.reasoning = { effort: model.thinkingLevelMap?.[effort] ?? effort, summary: options.reasoningSummary ?? "auto" };
      body.include = ["reasoning.encrypted_content"];
    } else if (model.thinkingLevelMap?.off !== null) {
      body.reasoning = { effort: model.thinkingLevelMap?.off ?? "none" };
    }
  }
  Object.assign(body, resolveSamplingParams(model, options.reasoningEffort ?? "off", options.samplingParams));
  return body;
}

function validateBody(payload: unknown): ResponseBody {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) throw new Error("Responses payload must be an object");
  const body = payload as ResponseBody;
  if (!Array.isArray(body.input) || typeof body.model !== "string" || !body.model) {
    throw new Error("Responses WebSocket needs a model and input array");
  }
  if (body.background === true || body.generate === false) throw new Error("This provider supports foreground generation only");
  // WS uses response.create instead of the HTTP stream flag. Snapshot the
  // payload so external hook mutation cannot corrupt continuation comparisons.
  const { stream: _stream, background: _background, ...rest } = body;
  return JSON.parse(JSON.stringify({ ...rest, type: "response.create" })) as ResponseBody;
}

class ContinuationRejected extends Error {}
class WebSocketUnsupported extends Error {}

async function* parsedEvents(events: AsyncIterable<WireEvent>, model: ResponsesModel, options: OpenAIResponsesOptions, recoverContinuation = false): AsyncGenerator<ParsedEvent> {
  for await (const raw of events) {
    await options.onProviderStreamEvent?.(raw, model);
    if (raw.type === "error" || raw.type === "response.failed") {
      const response = raw.response as { error?: { code?: string } } | undefined;
      const error = (raw.error ?? response?.error ?? raw) as { code?: string };
      if (recoverContinuation && ["previous_response_not_found", "previous_response_id_not_found"].includes(error.code ?? "")) {
        throw new ContinuationRejected("WebSocket previous_response_id was not found");
      }
      if (recoverContinuation && ["websocket_not_supported", "unsupported_websocket_protocol"].includes(error.code ?? "")) {
        throw new WebSocketUnsupported("Endpoint does not support Responses WebSocket");
      }
    }
    if (raw.type === "response.done") {
      yield { ...raw, type: "response.completed" } as unknown as ParsedEvent;
    } else if (raw.type === "error" && raw.error && typeof raw.error === "object") {
      yield { ...raw, ...raw.error, type: "error" } as unknown as ParsedEvent;
    } else {
      yield raw as unknown as ParsedEvent;
    }
  }
}

export function createResponsesWebSocketApi(pool = new ResponsesSocketPool()): ProviderStreams {
  const stream = (rawModel: Model<Api>, rawContext: TranscriptContext, options: OpenAIResponsesOptions = {}) => {
    const events = createAssistantMessageEventStream();
    const request = pool.beginRequest(options.signal);
    options = { ...options, signal: request.signal };
    const output: AssistantMessage = {
      role: "assistant", api: rawModel.api, provider: rawModel.provider, model: rawModel.id,
      timestamp: Date.now(), content: [], stopReason: "pending",
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
    };
    void (async () => {
      let lease: SocketLease | undefined;
      try {
        if (rawModel.api !== "openai-responses") throw new Error('responses-websocket requires api: "openai-responses"');
        const model = rawModel as ResponsesModel;
        const context = resolveTranscript(rawContext, model.compat?.supportsMidConvoSystemMessages ?? false);
        const conversion = serialization(model, context);
        const initialBody = requestBody(model, context, options, conversion);
        const replacement = await options.onPayload?.(initialBody, model);
        const body = validateBody(replacement ?? initialBody);
        const headers = headersFor(model, options);
        const transport = options.transport ?? "auto";
        let started = false;
        const start = () => { if (!started) { started = true; events.push({ type: "start", partial: output }); } };
        const http = async () => {
          await processResponsesStream(parsedEvents(sseEvents(model.baseUrl, body, headers, options,
            async response => { await options.onResponse?.(response, model); }, start), model, options), output, events, model, {
            grammarToolInputProperties: conversion.grammarToolInputProperties,
          });
        };
        if (transport === "sse") {
          await http();
        } else {
          try {
            lease = await pool.acquire({
              url: responsesUrl(model.baseUrl), headers,
              sessionId: options.cacheRetention === "none" ? undefined : options.sessionId,
              signal: options.signal, connectTimeoutMs: options.websocketConnectTimeoutMs, idleTimeoutMs: options.timeoutMs,
              onHandshake: async response => { await options.onResponse?.(response, model); },
            });
            start();
            const cached = transport !== "websocket" && options.cacheRetention !== "none";
            await processResponsesStream(parsedEvents(lease.events(body, cached), model, options, true), output, events, model, {
              grammarToolInputProperties: conversion.grammarToolInputProperties,
            });
          } catch (error) {
            const recoverable = error instanceof WebSocketTransportError && error.fallbackAllowed ||
              error instanceof WebSocketUnsupported || error instanceof ContinuationRejected && lease?.usedContinuation;
            if (transport !== "auto" || options.signal?.aborted || lease?.receivedOutput || output.content.length ||
                body.previous_response_id || !recoverable) throw error;
            lease?.release();
            lease = undefined;
            // The saved body is full history, never the WS-only delta. Hooks
            // ran once; replaying them would compound payload transformations.
            delete output.responseId;
            delete output.rawStopReason;
            delete output.errorMessage;
            output.stopReason = "pending";
            output.diagnostics = [{ type: "responses_ws_fallback", timestamp: Date.now(),
              details: { from: "websocket", to: "sse", reason: error instanceof ContinuationRejected ? "continuation_not_found" : "transport_unavailable" } }];
            await http();
          }
        }
        if (options.signal?.aborted) throw new Error("Request was aborted");
        if (output.stopReason === "pending" || output.stopReason === "error" || output.stopReason === "aborted") {
          throw new Error(output.errorMessage ?? "Responses stream did not complete successfully");
        }
        // Compare the next full request against exactly what Pi can replay,
        // not against raw server items containing extra status/annotation fields.
        const projected = convertResponsesMessages(model, normalizeContext({ messages: [output] }), new Set([model.provider]), conversion)
          // Pi repairs orphaned tool calls with synthetic results. Here we are
          // projecting just the assistant output; real results arrive next turn.
          .filter(item => item.type !== "function_call_output" && item.type !== "custom_tool_call_output");
        if (output.stopReason !== "length") lease?.finish(output.responseId, projected);
        events.push({ type: "done", reason: output.stopReason, message: output });
      } catch (error) {
        for (const block of output.content) {
          const scratch = block as unknown as Record<string, unknown>;
          delete scratch.partialJson;
          delete scratch.customInput;
          delete scratch.index;
        }
        output.stopReason = options.signal?.aborted ? "aborted" : "error";
        output.errorMessage = error instanceof Error ? error.message : String(error);
        events.push({ type: "error", reason: output.stopReason, error: output });
      } finally {
        lease?.release();
        request.dispose();
        events.end();
      }
    })();
    return events;
  };
  const streamSimple = (model: Model<Api>, context: TranscriptContext, options: SimpleStreamOptions = {}) => {
    // Deferred/background jobs do not fit a foreground WebSocket turn.
    if (options.deferred) {
      return stream(model, context, { ...options, onPayload: () => ({ model: model.id, input: [], background: true }) });
    }
    const level = options.reasoning ? clampThinkingLevel(model, options.reasoning) : undefined;
    return stream(model, context, {
      ...buildBaseOptions(model, context, options, options.apiKey),
      reasoningEffort: level === "off" ? undefined : level,
      toolChoice: options.toolChoice,
    });
  };
  return { stream, streamSimple };
}
