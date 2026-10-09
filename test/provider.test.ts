import assert from "node:assert/strict";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { test, type TestContext } from "node:test";
import { WebSocketServer, type WebSocket } from "ws";
import { normalizeContext, Type, type AssistantMessage, type Context, type Model } from "@earendil-works/pi-ai";
import { createResponsesWebSocketApi, PROVIDER_ID } from "../src/provider.ts";
import { ResponsesSocketPool, responsesUrl, type JsonObject } from "../src/transport.ts";

const model: Model<"openai-responses"> = {
  id: "test-model", name: "Test", provider: PROVIDER_ID, api: "openai-responses",
  baseUrl: "", reasoning: false, input: ["text", "image"], contextWindow: 128000,
  maxTokens: 4096, cost: { input: 1, output: 2, cacheRead: 0.1, cacheWrite: 0 },
};
const context = (messages: Context["messages"] = [{ role: "user", content: "hello", timestamp: 1 }]) => normalizeContext({ systemPrompt: "Be helpful.", messages });
const usage = { input_tokens: 20, output_tokens: 5, total_tokens: 25, input_tokens_details: { cached_tokens: 10 } };
function send(socket: WebSocket, event: JsonObject) { socket.send(JSON.stringify(event)); }
function textReply(socket: WebSocket, id = "resp_1", text = "你好 🌏") {
  const item = { type: "message", id: `msg_${id}`, role: "assistant", status: "completed", content: [{ type: "output_text", text, annotations: [] }] };
  send(socket, { type: "response.created", response: { id } });
  send(socket, { type: "response.output_item.added", output_index: 0, item: { ...item, content: [] } });
  send(socket, { type: "response.output_text.delta", output_index: 0, delta: text });
  send(socket, { type: "response.output_item.done", output_index: 0, item });
  send(socket, { type: "response.completed", response: { id, status: "completed", usage, output: [item] } });
}
function toolReply(socket: WebSocket) {
  const item = { type: "function_call", id: "fc_1", call_id: "call_1", name: "read_test", arguments: '{"path":"a.txt"}', status: "completed" };
  send(socket, { type: "response.output_item.added", output_index: 0, item: { ...item, arguments: "" } });
  send(socket, { type: "response.function_call_arguments.delta", output_index: 0, delta: '{"path":' });
  send(socket, { type: "response.function_call_arguments.delta", output_index: 0, delta: '"a.txt"}' });
  send(socket, { type: "response.output_item.done", output_index: 0, item });
  send(socket, { type: "response.completed", response: { id: "resp_tool", status: "completed", usage, output: [item] } });
}
async function fixture(t: TestContext, reply: (socket: WebSocket, body: JsonObject, n: number) => void) {
  const server = createServer();
  const wss = new WebSocketServer({ server });
  const requests: JsonObject[] = [];
  const paths: string[] = [];
  const auth: (string | undefined)[] = [];
  let connections = 0;
  wss.on("connection", (socket, req) => {
    connections++;
    paths.push(req.url!);
    auth.push(req.headers.authorization);
    socket.on("message", raw => {
      const body = JSON.parse(raw.toString()) as JsonObject;
      requests.push(body);
      reply(socket, body, requests.length);
    });
  });
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  const baseUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}/codex/v1`;
  const pool = new ResponsesSocketPool();
  const api = createResponsesWebSocketApi(pool);
  const selected = { ...model, baseUrl };
  t.after(async () => {
    pool.close();
    for (const client of wss.clients) client.terminate();
    await new Promise<void>(resolve => wss.close(() => resolve()));
    await new Promise<void>(resolve => server.close(() => resolve()));
  });
  return { api, selected, pool, requests, auth, paths, connections: () => connections };
}
const options = { apiKey: "test-key", sessionId: "session-1", timeoutMs: 1000, transport: "websocket-cached" as const };

// These tests never use real credentials, production endpoints, or user config.
test("URL conversion preserves gateway prefixes, query and full endpoint", () => {
  assert.equal(responsesUrl("https://example.com/codex/v1/"), "wss://example.com/codex/v1/responses");
  assert.equal(responsesUrl("ws://localhost:123/v1/responses?route=a"), "ws://localhost:123/v1/responses?route=a");
  assert.throws(() => responsesUrl("https://key:secret@example.com/v1"), /userinfo/);
  assert.throws(() => responsesUrl("ftp://example.com"), /http/);
});

test("streaming Unicode, usage, headers and instrumentation", async t => {
  const f = await fixture(t, socket => textReply(socket));
  const seen: string[] = [];
  const stream = f.api.streamSimple(f.selected, context(), {
    ...options,
    onPayload: payload => { (payload as JsonObject).temperature = 0.25; },
    onResponse: response => { assert.equal(response.status, 101); seen.push("handshake"); },
    onProviderStreamEvent: event => { seen.push((event as JsonObject).type as string); },
  });
  const eventTypes: string[] = [];
  for await (const event of stream) eventTypes.push(event.type);
  const result = await stream.result();
  assert.equal(result.stopReason, "stop");
  assert.deepEqual(eventTypes, ["start", "text_start", "text_delta", "text_end", "done"]);
  assert.equal(result.content[0].type === "text" && result.content[0].text, "你好 🌏");
  assert.equal(result.provider, PROVIDER_ID);
  assert.equal(result.responseId, "resp_1");
  assert.equal(result.usage.input, 10);
  assert.equal(result.usage.cacheRead, 10);
  assert.equal(result.usage.totalTokens, 25);
  assert.equal(result.usage.cost.total, 0.000021);
  assert.equal(f.auth[0], "Bearer test-key");
  assert.equal(f.paths[0], "/codex/v1/responses");
  assert.equal(f.requests[0].type, "response.create");
  assert.equal(f.requests[0].stream, undefined);
  assert.equal(f.requests[0].store, false);
  assert.equal(f.requests[0].temperature, 0.25);
  assert.equal(seen[0], "handshake");
  assert.equal(seen.at(-1), "response.completed");
});

test("same-session request reuses socket and sends only new input with response id", async t => {
  const f = await fixture(t, (socket, _body, n) => textReply(socket, `resp_${n}`));
  const first = await f.api.streamSimple(f.selected, context(), options).result();
  const next = context([...context().messages.filter(m => m.role !== "system"), first, { role: "user", content: "next", timestamp: 2 }]);
  const result = await f.api.streamSimple(f.selected, next, options).result();
  assert.equal(result.stopReason, "stop");
  assert.equal(f.connections(), 1);
  assert.equal(f.requests[1].previous_response_id, "resp_1");
  assert.equal((f.requests[1].input as unknown[]).length, 1);
});

test("function call IDs and tool-result delta remain paired", async t => {
  const f = await fixture(t, (socket, _body, n) => n === 1 ? toolReply(socket) : textReply(socket));
  const tools = [{ name: "read_test", description: "Read", parameters: Type.Object({ path: Type.String() }) }];
  const initial: Context = { systemPrompt: "Be helpful.", tools, messages: [{ role: "user", content: "read", timestamp: 1 }] };
  const first = await f.api.streamSimple(f.selected, normalizeContext(initial), options).result();
  assert.equal(first.stopReason, "toolUse");
  const call = first.content[0];
  assert.equal(call.type, "toolCall");
  if (call.type !== "toolCall") throw new Error("Expected tool");
  assert.equal(call.id, "call_1|fc_1");
  assert.deepEqual(call.arguments, { path: "a.txt" });
  const messages: Context["messages"] = [...initial.messages, first, { role: "toolResult", toolCallId: call.id, toolName: call.name, content: [{ type: "text", text: "file" }], isError: false, timestamp: 3 }];
  await f.api.streamSimple(f.selected, normalizeContext({ ...initial, messages }), options).result();
  assert.equal(f.requests[1].previous_response_id, "resp_tool");
  assert.deepEqual(f.requests[1].input, [{ type: "function_call_output", call_id: "call_1", output: "file" }]);
  // Full replay after reconnect must preserve call_id, not flatten call_1|fc_1.
  f.pool.close();
  await f.api.streamSimple(f.selected, normalizeContext({ ...initial, messages }), options).result();
  assert.equal(f.requests[2].previous_response_id, undefined);
  const input = f.requests[2].input as JsonObject[];
  assert.equal(input.find(item => item.type === "function_call")?.call_id, "call_1");
  assert.equal(input.find(item => item.type === "function_call_output")?.call_id, "call_1");
});

test("prompt changes, forks and sampling changes do not reuse stale response ids", async t => {
  const f = await fixture(t, (socket, _body, n) => textReply(socket, `resp_${n}`));
  const first = await f.api.streamSimple(f.selected, context(), options).result();
  const messages: Context["messages"] = [{ role: "user", content: "hello", timestamp: 1 }, first, { role: "user", content: "next", timestamp: 2 }];
  await f.api.streamSimple(f.selected, normalizeContext({ systemPrompt: "Different", messages }), options).result();
  await f.api.streamSimple(f.selected, context(), options).result();
  await f.api.streamSimple(f.selected, context(messages), { ...options, temperature: 0.5 }).result();
  for (const body of f.requests.slice(1)) assert.equal(body.previous_response_id, undefined);
});

test("changed credentials and session ids cannot reuse a connection", async t => {
  const f = await fixture(t, socket => textReply(socket));
  await f.api.streamSimple(f.selected, context(), options).result();
  await f.api.streamSimple(f.selected, context(), { ...options, apiKey: "different-key" }).result();
  await f.api.streamSimple(f.selected, context(), { ...options, sessionId: "different-session" }).result();
  assert.equal(f.connections(), 3);
  assert.deepEqual(f.auth, ["Bearer test-key", "Bearer different-key", "Bearer test-key"]);
});

test("empty successful response and legacy response.done terminate", async t => {
  const f = await fixture(t, socket => send(socket, { type: "response.done", response: { id: "resp_empty", status: "completed", output: [], usage } }));
  const result = await f.api.streamSimple(f.selected, context(), options).result();
  assert.equal(result.stopReason, "stop");
  assert.deepEqual(result.content, []);
});

test("reasoning signatures, summaries and model input images", async t => {
  const item = { type: "reasoning", id: "rs_1", encrypted_content: "opaque", summary: [{ type: "summary_text", text: "think" }] };
  const f = await fixture(t, socket => {
    send(socket, { type: "response.output_item.added", output_index: 0, item: { ...item, summary: [] } });
    send(socket, { type: "response.reasoning_summary_text.delta", output_index: 0, delta: "think" });
    send(socket, { type: "response.output_item.done", output_index: 0, item });
    send(socket, { type: "response.completed", response: { id: "resp_reason", status: "completed", output: [item], usage } });
  });
  const result = await f.api.streamSimple({ ...f.selected, reasoning: true }, context([{ role: "user", timestamp: 1, content: [{ type: "image", mimeType: "image/png", data: "aGVsbG8=" }] }]), { ...options, reasoning: "high" }).result();
  assert.equal(result.stopReason, "stop");
  assert.equal(result.content[0].type, "thinking");
  assert.equal(result.content[0].type === "thinking" && result.content[0].thinkingSignature, JSON.stringify(item));
  assert.deepEqual(f.requests[0].reasoning, { effort: "high", summary: "auto" });
  assert.match(JSON.stringify(f.requests[0].input), /data:image\/png;base64,aGVsbG8=/);
});

test("nested protocol errors keep context-overflow code and do not retry", async t => {
  const f = await fixture(t, socket => send(socket, { type: "error", error: { code: "context_length_exceeded", message: "Too much input" } }));
  const result = await f.api.streamSimple(f.selected, context(), options).result();
  assert.equal(result.stopReason, "error");
  assert.match(result.errorMessage!, /context_length_exceeded/);
  assert.equal(f.requests.length, 1);
});

test("failed and incomplete terminals preserve errors / output-token limit", async t => {
  const f = await fixture(t, (socket, _body, n) => send(socket, n === 1
    ? { type: "response.failed", response: { status: "failed", error: { code: "invalid_prompt", message: "Bad prompt" } } }
    : { type: "response.incomplete", response: { id: "resp_limit", status: "incomplete", output: [], incomplete_details: { reason: "max_output_tokens" }, usage } }));
  const failed = await f.api.streamSimple(f.selected, context(), options).result();
  assert.equal(failed.stopReason, "error");
  assert.match(failed.errorMessage!, /invalid_prompt/);
  const incomplete = await f.api.streamSimple(f.selected, context(), options).result();
  assert.equal(incomplete.stopReason, "length");
  assert.equal(incomplete.usage.totalTokens, 25);
});

test("malformed JSON and nonterminal EOF fail without hanging", async t => {
  const f = await fixture(t, (socket, _body, n) => {
    if (n === 1) socket.send("not-json");
    else socket.close();
  });
  const invalid = await f.api.streamSimple(f.selected, context(), options).result();
  assert.equal(invalid.stopReason, "error");
  assert.match(invalid.errorMessage!, /Invalid JSON/);
  const closed = await f.api.streamSimple(f.selected, context(), options).result();
  assert.equal(closed.stopReason, "error");
  assert.match(closed.errorMessage!, /closed before/);
});

test("abort active and already-aborted requests; no replay", async t => {
  const abort = new AbortController();
  const f = await fixture(t, () => abort.abort());
  const result = await f.api.streamSimple(f.selected, context(), { ...options, signal: abort.signal }).result();
  assert.equal(result.stopReason, "aborted");
  const again = await f.api.streamSimple(f.selected, context(), { ...options, signal: abort.signal }).result();
  assert.equal(again.stopReason, "aborted");
  assert.equal(f.requests.length, 1);
});

test("idle timeout and throwing observer release sockets", async t => {
  const f = await fixture(t, (socket, _body, n) => { if (n > 1) textReply(socket); });
  const timeout = await f.api.streamSimple(f.selected, context(), { ...options, timeoutMs: 25 }).result();
  assert.equal(timeout.stopReason, "error");
  assert.match(timeout.errorMessage!, /idle timeout/);
  const callback = await f.api.streamSimple(f.selected, context(), { ...options, onProviderStreamEvent: () => { throw new Error("observer failed"); } }).result();
  assert.equal(callback.stopReason, "error");
  assert.match(callback.errorMessage!, /observer failed/);
  const valid = await f.api.streamSimple(f.selected, context(), options).result();
  assert.equal(valid.stopReason, "stop");
  assert.equal(f.connections(), 3);
});

test("concurrent calls are isolated even with the same session id", async t => {
  const f = await fixture(t, (socket, _body, n) => setTimeout(() => textReply(socket, `resp_${n}`), 10));
  const [a, b] = await Promise.all([
    f.api.streamSimple(f.selected, context(), options).result(),
    f.api.streamSimple(f.selected, context(), options).result(),
  ]);
  assert.equal(a.stopReason, "stop");
  assert.equal(b.stopReason, "stop");
  assert.notEqual(a.responseId, b.responseId);
  assert.equal(f.connections(), 2);
});

test("cacheRetention none and full-input websocket mode", async t => {
  const f = await fixture(t, socket => textReply(socket));
  const first = await f.api.streamSimple(f.selected, context(), options).result();
  const next = context([{ role: "user", content: "hello", timestamp: 1 }, first, { role: "user", content: "next", timestamp: 2 }]);
  await f.api.streamSimple(f.selected, next, { ...options, transport: "websocket" }).result();
  assert.equal(f.requests[1].previous_response_id, undefined);
  assert.equal(f.connections(), 1);
  await f.api.streamSimple(f.selected, context(), { ...options, cacheRetention: "none" }).result();
  await f.api.streamSimple(f.selected, context(), { ...options, cacheRetention: "none" }).result();
  assert.equal(f.requests[2].prompt_cache_key, undefined);
  assert.equal(f.connections(), 3);
});

test("request / handshake hooks fail before sending a generation", async t => {
  const f = await fixture(t, socket => textReply(socket));
  const payload = await f.api.streamSimple(f.selected, context(), { ...options, onPayload: () => { throw new Error("payload failed"); } }).result();
  assert.match(payload.errorMessage!, /payload failed/);
  assert.equal(f.connections(), 0);
  const handshake = await f.api.streamSimple(f.selected, context(), { ...options, onResponse: () => { throw new Error("handshake failed"); } }).result();
  assert.match(handshake.errorMessage!, /handshake failed/);
  assert.equal(f.requests.length, 0);
});

test("custom authorization header overrides key; null header removes it", async t => {
  const f = await fixture(t, socket => textReply(socket));
  await f.api.streamSimple(f.selected, context(), { ...options, headers: { Authorization: "Bearer custom" } }).result();
  await f.api.streamSimple(f.selected, context(), { ...options, headers: { authorization: null, "x-api-key": "custom" } }).result();
  assert.deepEqual(f.auth, ["Bearer custom", undefined]);
});

test("shutdown cancels an active request and does not leave an iterator pending", async t => {
  const f = await fixture(t, () => f.pool.close());
  const result = await f.api.streamSimple(f.selected, context(), options).result();
  assert.equal(result.stopReason, "aborted");
  assert.match(result.errorMessage!, /aborted/);
});


test("partial tool call is never returned as executable toolUse", async t => {
  const f = await fixture(t, socket => {
    send(socket, { type: "response.output_item.added", output_index: 0, item: { type: "function_call", id: "fc_partial", call_id: "call_partial", name: "dangerous", arguments: "" } });
    send(socket, { type: "response.function_call_arguments.delta", output_index: 0, delta: '{"path":' });
    send(socket, { type: "response.completed", response: { id: "resp_partial", status: "completed", output: [], usage } });
  });
  const result = await f.api.streamSimple(f.selected, context(), options).result();
  assert.equal(result.stopReason, "error");
  assert.match(result.errorMessage!, /unfinished tool call/);
  assert.equal(f.requests.length, 1);
});

test("tool image result and foreign-provider history are serialized", async t => {
  const f = await fixture(t, socket => textReply(socket));
  const foreign: AssistantMessage = {
    role: "assistant", provider: "foreign", api: "anthropic-messages", model: "foreign-model", timestamp: 1,
    stopReason: "toolUse", content: [{ type: "toolCall", id: "call:foreign", name: "read_test", arguments: { path: "img.png" } }],
    usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
  };
  const result = await f.api.streamSimple(f.selected, context([
    { role: "user", content: "read", timestamp: 1 }, foreign,
    { role: "toolResult", toolCallId: "call:foreign", toolName: "read_test", timestamp: 2, isError: false, content: [{ type: "image", mimeType: "image/png", data: "aGVsbG8=" }] },
  ]), options).result();
  assert.equal(result.stopReason, "stop");
  const input = f.requests[0].input as JsonObject[];
  assert.equal(input.find(item => item.type === "function_call")?.call_id, "call_foreign");
  const tool = input.find(item => item.type === "function_call_output");
  assert.equal(tool?.call_id, "call_foreign");
  assert.match(JSON.stringify(tool?.output), /data:image\/png;base64,aGVsbG8=/);
});

test("handshake rejection returns an error, not a stuck stream or HTTP fallback", async t => {
  const http = createServer((_req, res) => { res.writeHead(401); res.end(); });
  await new Promise<void>(done => http.listen(0, "127.0.0.1", done));
  const pool = new ResponsesSocketPool();
  t.after(async () => {
    pool.close();
    await new Promise<void>(done => http.close(() => done()));
  });
  const api = createResponsesWebSocketApi(pool);
  const result = await api.streamSimple({ ...model, baseUrl: `http://127.0.0.1:${(http.address() as AddressInfo).port}/v1` }, context(), options).result();
  assert.equal(result.stopReason, "error");
  assert.match(result.errorMessage!, /401/);
});
