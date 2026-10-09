import assert from "node:assert/strict";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { test, type TestContext } from "node:test";
import { WebSocketServer, type WebSocket } from "ws";
import { normalizeContext, type Model, type Context, type OpenAIResponsesOptions } from "@earendil-works/pi-ai";
import { createResponsesWebSocketApi } from "../src/provider.ts";
import { ResponsesSocketPool, responsesHttpUrl, type JsonObject } from "../src/transport.ts";

const opts = { apiKey: "synthetic-key", sessionId: "session-1", timeoutMs: 1000 };
const context = (messages: Context["messages"] = [{ role: "user", timestamp: 1, content: "你好" }]) => normalizeContext({ systemPrompt: "System", messages });
const model: Model<"openai-responses"> = {
  id: "test", name: "Test", provider: "responses-websocket", api: "openai-responses", baseUrl: "",
  reasoning: false, input: ["text"], contextWindow: 128000, maxTokens: 4096,
  cost: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0 },
};
function events(id = "resp_sse") {
  const item = { type: "message", role: "assistant", id: `msg_${id}`, content: [{ type: "output_text", text: "SSE 你好 🌏", annotations: [] }], status: "completed" };
  return [
    { type: "response.created", response: { id } },
    { type: "response.output_item.added", output_index: 0, item: { ...item, content: [] } },
    { type: "response.output_text.delta", output_index: 0, delta: "SSE 你好 🌏" },
    { type: "response.output_item.done", output_index: 0, item },
    { type: "response.completed", response: { id, output: [item], status: "completed", usage: { input_tokens: 3, output_tokens: 2, total_tokens: 5 } } },
  ];
}
function sse(res: ServerResponse, data = events()) {
  res.writeHead(200, { "content-type": "text/event-stream" });
  res.end(data.map(event => `event: ${event.type}\r\ndata: ${JSON.stringify(event)}\r\n\r\n`).join("") + "data: [DONE]\r\n\r\n");
}
async function setup(t: TestContext, config: {
  reject?: number | ((attempt: number) => number | undefined);
  ws?: (socket: WebSocket, body: JsonObject, n: number) => void;
  http?: (res: ServerResponse, body: JsonObject, req: IncomingMessage) => void;
} = {}) {
  const posts: JsonObject[] = [], wsBodies: JsonObject[] = [];
  const httpRequests: IncomingMessage[] = [];
  let upgrades = 0;
  const server = createServer(async (req, res) => {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const body = JSON.parse(Buffer.concat(chunks).toString());
    posts.push(body);
    httpRequests.push(req);
    if (config.http) config.http(res, body, req);
    else sse(res);
  });
  const wss = new WebSocketServer({ noServer: true });
  server.on("upgrade", (req, socket, head) => {
    upgrades++;
    const rejection = typeof config.reject === "function" ? config.reject(upgrades) : config.reject;
    if (rejection) {
      socket.end(`HTTP/1.1 ${rejection} Test\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`);
    } else {
      wss.handleUpgrade(req, socket, head, client => wss.emit("connection", client, req));
    }
  });
  wss.on("connection", socket => socket.on("message", raw => {
    const body = JSON.parse(raw.toString());
    wsBodies.push(body);
    if (config.ws) config.ws(socket, body, wsBodies.length);
    else for (const event of events("resp_ws")) socket.send(JSON.stringify(event));
  }));
  await new Promise<void>(done => server.listen(0, "127.0.0.1", done));
  const selected = { ...model, baseUrl: `http://127.0.0.1:${(server.address() as AddressInfo).port}/codex/v1/responses?route=one` };
  const pool = new ResponsesSocketPool();
  const api = createResponsesWebSocketApi(pool);
  t.after(async () => {
    pool.close();
    for (const socket of wss.clients) socket.terminate();
    await new Promise<void>(done => wss.close(() => done()));
    server.closeAllConnections();
    await new Promise<void>(done => server.close(() => done()));
  });
  return { selected, pool, api, posts, wsBodies, httpRequests, upgrades: () => upgrades };
}

test("HTTP URL conversion preserves encryption, path, query", () => {
  assert.equal(responsesHttpUrl("wss://example.com/codex/v1?x=1"), "https://example.com/codex/v1/responses?x=1");
  assert.equal(responsesHttpUrl("ws://127.0.0.1/v1/responses"), "http://127.0.0.1/v1/responses");
});

for (const status of [404, 405, 426, 502, 503]) {
  test(`auto falls back after upgrade ${status}, preserving payload, auth, hooks and usage`, async t => {
    const f = await setup(t, { reject: status });
    let hooks = 0;
    const received: string[] = [];
    const responses: number[] = [];
    const call = f.api.streamSimple(f.selected, context(), {
      ...opts,
      onPayload: payload => { hooks++; (payload as JsonObject).temperature = 0.25; },
      onResponse: response => { responses.push(response.status); },
      onProviderStreamEvent: event => { received.push((event as JsonObject).type as string); },
    });
    const lifecycle: string[] = [];
    for await (const event of call) lifecycle.push(event.type);
    const result = await call.result();
    assert.equal(result.stopReason, "stop", result.errorMessage);
    assert.equal(result.responseId, "resp_sse");
    assert.equal(result.usage.totalTokens, 5);
    assert.equal(result.content[0].type === "text" && result.content[0].text, "SSE 你好 🌏");
    assert.equal(result.diagnostics?.[0].type, "responses_ws_fallback");
    assert.deepEqual(lifecycle, ["start", "text_start", "text_delta", "text_end", "done"]);
    assert.equal(hooks, 1);
    assert.equal(f.upgrades(), 4);
    assert.equal(f.posts.length, 1);
    assert.equal(f.posts[0].stream, true);
    assert.equal(f.posts[0].type, undefined);
    assert.equal(f.posts[0].previous_response_id, undefined);
    assert.equal(f.posts[0].temperature, 0.25);
    assert.equal(f.posts[0].prompt_cache_key, opts.sessionId);
    assert.equal((f.posts[0].input as unknown[]).length, 2);
    assert.equal(f.httpRequests[0].url, "/codex/v1/responses?route=one");
    assert.equal(f.httpRequests[0].headers.authorization, "Bearer synthetic-key");
    assert.deepEqual(responses, [200]);
    assert.equal(received.at(-1), "response.completed");
  });
}

for (const status of [400, 401, 403, 402, 429, 302]) {
  test(`upgrade ${status} is not retried using HTTP`, async t => {
    const f = await setup(t, { reject: status });
    const result = await f.api.streamSimple(f.selected, context(), opts).result();
    assert.equal(result.stopReason, "error");
    assert.match(result.errorMessage!, new RegExp(String(status)));
    assert.equal(f.upgrades(), 1);
    assert.equal(f.posts.length, 0);
  });
}

for (const transport of ["websocket", "websocket-cached"] as const) {
  test(`${transport} is strict: no SSE fallback`, async t => {
    const f = await setup(t, { reject: 426 });
    const result = await f.api.streamSimple(f.selected, context(), { ...opts, transport }).result();
    assert.equal(result.stopReason, "error");
    assert.equal(f.upgrades(), 1);
    assert.equal(f.posts.length, 0);
  });
}

test("sse skips WS, honors custom fetch and header override", async t => {
  const f = await setup(t);
  let fetches = 0;
  const result = await f.api.streamSimple(f.selected, context(), { ...opts, transport: "sse",
    headers: { Authorization: "Bearer alternate" },
    fetch: async (...args) => { fetches++; return fetch(...args); },
  }).result();
  assert.equal(result.stopReason, "stop");
  assert.equal(f.upgrades(), 0);
  assert.equal(fetches, 1);
  assert.equal(f.httpRequests[0].headers.authorization, "Bearer alternate");
});

test("output-free WS disconnect falls back with one start event", async t => {
  const f = await setup(t, { ws: socket => {
    socket.send(JSON.stringify({ type: "response.created", response: { id: "resp_abandoned" } }));
    socket.close();
  } });
  const call = f.api.streamSimple(f.selected, context(), opts);
  const types = [];
  for await (const event of call) types.push(event.type);
  const result = await call.result();
  assert.equal(result.stopReason, "stop");
  assert.equal(result.responseId, "resp_sse");
  assert.equal(types.filter(type => type === "start").length, 1);
  assert.equal(f.posts.length, 1);
});

for (const kind of ["text", "tool", "thinking"] as const) {
  test(`${kind} output followed by disconnect must not trigger HTTP replay`, async t => {
    const f = await setup(t, { ws: socket => {
      const item = kind === "tool" ? { type: "function_call", id: "fc_1", call_id: "call_1", name: "write", arguments: "" }
        : kind === "thinking" ? { type: "reasoning", id: "rs_1", summary: [] }
        : { type: "message", id: "msg_1", role: "assistant", content: [] };
      socket.send(JSON.stringify({ type: "response.output_item.added", output_index: 0, item }));
      // Close immediately: receipt of buffered output must also block retry.
      socket.close();
    } });
    const result = await f.api.streamSimple(f.selected, context(), opts).result();
    assert.equal(result.stopReason, "error");
    assert.equal(f.upgrades(), 1);
    assert.equal(f.posts.length, 0);
  });
}

for (const code of ["invalid_prompt", "insufficient_quota", "context_length_exceeded", "invalid_api_key"]) {
  test(`WS protocol business error ${code} does not fallback`, async t => {
    const f = await setup(t, { ws: socket => {
      socket.send(JSON.stringify({ type: "error", error: { code, message: "Test error" } }));
      socket.close();
    } });
    const result = await f.api.streamSimple(f.selected, context(), opts).result();
    assert.equal(result.stopReason, "error");
    assert.match(result.errorMessage!, new RegExp(code));
    assert.equal(f.upgrades(), 1);
    assert.equal(f.posts.length, 0);
  });
}

test("explicit unsupported WS protocol falls back", async t => {
  const f = await setup(t, { ws: socket => socket.send(JSON.stringify({ type: "error", code: "websocket_not_supported", message: "Use HTTP" })) });
  const result = await f.api.streamSimple(f.selected, context(), opts).result();
  assert.equal(result.stopReason, "stop");
  assert.equal(f.posts.length, 1);
});

test("missing cached continuation uses full transformed history once over SSE", async t => {
  const f = await setup(t, { ws: (socket, _body, n) => {
    if (n === 1) for (const event of events("resp_first")) socket.send(JSON.stringify(event));
    else socket.send(JSON.stringify({ type: "response.failed", response: { status: "failed", error: { code: "previous_response_not_found", message: "Expired" } } }));
  } });
  const first = await f.api.streamSimple(f.selected, context(), opts).result();
  const next = context([{ role: "user", timestamp: 1, content: "你好" }, first, { role: "user", timestamp: 2, content: "continue" }]);
  const result = await f.api.streamSimple(f.selected, next, opts).result();
  assert.equal(result.stopReason, "stop", result.errorMessage);
  assert.equal(f.wsBodies[1].previous_response_id, "resp_first");
  assert.equal((f.wsBodies[1].input as unknown[]).length, 1);
  assert.equal(f.posts[0].previous_response_id, undefined);
  assert.equal((f.posts[0].input as unknown[]).length, 4);
  assert.match(JSON.stringify(f.posts[0].input), /SSE 你好/);
  assert.equal(f.posts.length, 1);
  assert.equal(result.diagnostics?.[0].details?.reason, "continuation_not_found");
});

test("caller-provided previous_response_id is never silently removed or replayed", async t => {
  const f = await setup(t, { reject: 426 });
  const result = await f.api.streamSimple(f.selected, context(), { ...opts,
    onPayload: payload => ({ ...payload as JsonObject, previous_response_id: "external", input: [] }),
  }).result();
  assert.equal(result.stopReason, "error");
  assert.equal(f.posts.length, 0);
});

test("SSE error is final, no retry; retains provider failure details", async t => {
  const f = await setup(t, { reject: 426, http: res => {
    res.writeHead(429, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: { code: "insufficient_quota", message: "No quota" } }));
  } });
  const result = await f.api.streamSimple(f.selected, context(), opts).result();
  assert.equal(result.stopReason, "error");
  assert.match(result.errorMessage!, /429.*insufficient_quota/);
  assert.equal(f.posts.length, 1);
  assert.equal(f.upgrades(), 4);
});

test("hooks throwing on WS or HTTP do not cause retries", async t => {
  const f = await setup(t);
  const handshake = await f.api.streamSimple(f.selected, context(), { ...opts, onResponse: () => { throw new Error("hook"); } }).result();
  assert.match(handshake.errorMessage!, /hook/);
  assert.equal(f.upgrades(), 1);
  const observer = await f.api.streamSimple(f.selected, context(), { ...opts, onProviderStreamEvent: () => { throw new Error("observer"); } }).result();
  assert.match(observer.errorMessage!, /observer/);
  assert.equal(f.upgrades(), 2);
  assert.equal(f.posts.length, 0);
  const http = await f.api.streamSimple(f.selected, context(), { ...opts, transport: "sse", onResponse: () => { throw new Error("http hook"); } }).result();
  assert.match(http.errorMessage!, /http hook/);
  assert.equal(f.posts.length, 1);
});

test("cancel during WS must not fall back", async t => {
  const controller = new AbortController();
  const f = await setup(t, { ws: () => controller.abort() });
  const result = await f.api.streamSimple(f.selected, context(), { ...opts, signal: controller.signal }).result();
  assert.equal(result.stopReason, "aborted");
  assert.equal(f.upgrades(), 1);
  assert.equal(f.posts.length, 0);
});

test("cancel and shutdown interrupt HTTP body waits", async t => {
  const controller = new AbortController();
  const f = await setup(t, { reject: 426, http: res => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.flushHeaders();
    if (f.posts.length === 1) setTimeout(() => controller.abort(), 10);
    else setTimeout(() => f.pool.close(), 10);
  } });
  const cancel = await f.api.streamSimple(f.selected, context(), { ...opts, signal: controller.signal }).result();
  assert.equal(cancel.stopReason, "aborted");
  const shutdown = await f.api.streamSimple(f.selected, context(), opts).result();
  assert.equal(shutdown.stopReason, "aborted");
  assert.equal(f.posts.length, 2);
});

test("HTTP headers and body timeout is bounded", async t => {
  const f = await setup(t, { reject: 426, http: () => {} });
  const result = await f.api.streamSimple(f.selected, context(), { ...opts, timeoutMs: 30 }).result();
  assert.equal(result.stopReason, "error");
  assert.match(result.errorMessage!, /HTTP idle timeout/);
  assert.equal(f.posts.length, 1);
});

test("SSE truncation is not successful completion", async t => {
  const f = await setup(t, { reject: 426, http: res => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.end('data: {"type":"response.created","response":{"id":"resp_short"}}\n\n');
  } });
  const result = await f.api.streamSimple(f.selected, context(), opts).result();
  assert.equal(result.stopReason, "error");
  assert.match(result.errorMessage!, /terminal response/);
  assert.equal(f.posts.length, 1);
});

test("tool continuation fallback preserves full call/result pairs", async t => {
  const tool = { type: "function_call", id: "fc_1", call_id: "call_1", name: "read", arguments: '{"path":"a"}' };
  const f = await setup(t, { ws: (socket, _body, n) => {
    if (n === 1) {
      socket.send(JSON.stringify({ type: "response.output_item.added", output_index: 0, item: { ...tool, arguments: "" } }));
      socket.send(JSON.stringify({ type: "response.output_item.done", output_index: 0, item: tool }));
      socket.send(JSON.stringify({ type: "response.completed", response: { id: "resp_tool", status: "completed", output: [tool] } }));
    } else socket.send(JSON.stringify({ type: "error", code: "previous_response_not_found", message: "Expired" }));
  } });
  const first = await f.api.streamSimple(f.selected, context(), opts).result();
  assert.equal(first.stopReason, "toolUse");
  const result = await f.api.streamSimple(f.selected, context([
    { role: "user", content: "你好", timestamp: 1 }, first,
    { role: "toolResult", toolCallId: "call_1|fc_1", toolName: "read", timestamp: 2, isError: false, content: [{ type: "text", text: "file content" }] },
  ]), opts).result();
  assert.equal(result.stopReason, "stop", result.errorMessage);
  assert.equal(f.wsBodies[1].previous_response_id, "resp_tool");
  assert.deepEqual(f.wsBodies[1].input, [{ type: "function_call_output", call_id: "call_1", output: "file content" }]);
  const input = f.posts[0].input as JsonObject[];
  assert.equal(input.find(item => item.type === "function_call")?.call_id, "call_1");
  assert.equal(input.find(item => item.type === "function_call_output")?.call_id, "call_1");
  assert.equal(f.posts.length, 1);
});

test("SSE tool call is parsed once after handshake fallback", async t => {
  const item = { type: "function_call", id: "fc_http", call_id: "call_http", name: "write", arguments: '{"text":"abc"}' };
  const f = await setup(t, { reject: 426, http: res => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.end([
      { type: "response.output_item.added", output_index: 0, item: { ...item, arguments: "" } },
      { type: "response.function_call_arguments.delta", output_index: 0, delta: item.arguments },
      { type: "response.output_item.done", output_index: 0, item },
      { type: "response.completed", response: { id: "resp_http_tool", status: "completed", output: [item] } },
    ].map(event => `data: ${JSON.stringify(event)}\n\n`).join(""));
  } });
  const result = await f.api.streamSimple(f.selected, context(), opts).result();
  assert.equal(result.stopReason, "toolUse");
  assert.equal(result.content.length, 1);
  const call = result.content[0];
  assert.equal(call.type, "toolCall");
  if (call.type === "toolCall") assert.deepEqual(call.arguments, { text: "abc" });
  assert.equal(f.posts.length, 1);
});

test("malformed SSE JSON fails without echoing raw input", async t => {
  const f = await setup(t, { reject: 426, http: res => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.end('data: secret-payload-not-json\n\n');
  } });
  const result = await f.api.streamSimple(f.selected, context(), opts).result();
  assert.equal(result.stopReason, "error");
  assert.doesNotMatch(result.errorMessage!, /secret-payload/);
  assert.equal(f.posts.length, 1);
});

test("terminal SSE without HTTP EOF finishes promptly", async t => {
  const f = await setup(t, { reject: 426, http: res => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    for (const event of events()) res.write(`data: ${JSON.stringify(event)}\n\n`);
  } });
  const result = await f.api.streamSimple(f.selected, context(), opts).result();
  assert.equal(result.stopReason, "stop", result.errorMessage);
});

test("SSE timeout after headers remains a timeout, not a truncated-stream error", async t => {
  const f = await setup(t, { reject: 426, http: res => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.write('data: {"type":"response.created","response":{"id":"resp_stall"}}\n\n');
  } });
  const result = await f.api.streamSimple(f.selected, context(), { ...opts, timeoutMs: 35 }).result();
  assert.equal(result.stopReason, "error");
  assert.match(result.errorMessage!, /HTTP idle timeout/);
  assert.doesNotMatch(result.errorMessage!, /terminal response/);
});

test("timeout during HTTP response hook emits no start", async t => {
  const f = await setup(t);
  const call = f.api.streamSimple(f.selected, context(), { ...opts, transport: "sse", timeoutMs: 25,
    onResponse: async () => { await new Promise(done => setTimeout(done, 50)); },
  });
  const types = [];
  for await (const event of call) types.push(event.type);
  assert.deepEqual(types, ["error"]);
  assert.match((await call.result()).errorMessage!, /HTTP idle timeout/);
});

test("SSE WS-specific code retains server message instead of reclassifying transport", async t => {
  const f = await setup(t, { reject: 426, http: res => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.end('data: {"type":"error","error":{"code":"websocket_not_supported","message":"Original endpoint error"}}\n\n');
  } });
  const result = await f.api.streamSimple(f.selected, context(), opts).result();
  assert.equal(result.stopReason, "error");
  assert.match(result.errorMessage!, /websocket_not_supported.*Original endpoint error/);
  assert.equal(f.posts.length, 1);
});

for (const payload of [
  'event: error\ndata: {"error":{"code":"insufficient_quota","message":"No quota"}}\n\n',
  'event: error\ndata: {"code":"insufficient_quota","message":"No quota"}\n\n',
  'data: {"error":{"code":"insufficient_quota","message":"No quota"}}\n\n',
]) test(`SSE error envelope preserves quota code (${payload.slice(0, 30)})`, async t => {
  const f = await setup(t, { reject: 426, http: res => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.end(payload);
  } });
  const result = await f.api.streamSimple(f.selected, context(), opts).result();
  assert.equal(result.stopReason, "error");
  assert.match(result.errorMessage!, /insufficient_quota.*No quota/);
  assert.equal(f.posts.length, 1);
});

for (const failures of [1, 2, 3]) test(`WS succeeds after ${failures} retries without SSE`, async t => {
  const f = await setup(t, { reject: n => n <= failures ? 503 : undefined });
  let payloadCalls = 0;
  const result = await f.api.streamSimple(f.selected, context(), {
    ...opts, onPayload: () => { payloadCalls++; },
  }).result();
  assert.equal(result.stopReason, "stop", result.errorMessage);
  assert.equal(result.responseId, "resp_ws");
  assert.equal(f.upgrades(), failures + 1);
  assert.equal(f.posts.length, 0);
  assert.equal(payloadCalls, 1);
  assert.equal(f.pool.getUserTurn(opts.sessionId).useSse, false);
});

test("four failures select SSE through tool continuations; next user retries WS", async t => {
  const tool = { type: "function_call", id: "fc_http", call_id: "call_http", name: "read", arguments: '{"path":"a"}' };
  const f = await setup(t, {
    reject: n => n <= 4 ? 503 : undefined,
    http: res => {
      if (f.posts.length !== 1) { sse(res); return; }
      sse(res, [
        { type: "response.output_item.added", output_index: 0, item: { ...tool, arguments: "" } },
        { type: "response.output_item.done", output_index: 0, item: tool },
        { type: "response.completed", response: { id: "resp_tool", status: "completed", output: [tool] } },
      ] as unknown as ReturnType<typeof events>);
    },
  });
  f.pool.beginUserTurn(opts.sessionId);
  const first = await f.api.streamSimple(f.selected, context(), opts).result();
  assert.equal(first.stopReason, "toolUse", first.errorMessage);
  assert.equal(first.diagnostics?.[0].details?.attempts, 4);
  const messages: Context["messages"] = [
    { role: "user", timestamp: 1, content: "你好" }, first,
    { role: "toolResult", toolCallId: "call_http|fc_http", toolName: "read", timestamp: 2, isError: false, content: [{ type: "text", text: "file" }] },
  ];
  const second = await f.api.streamSimple(f.selected, context(messages), { ...opts, cacheRetention: "none" }).result();
  assert.equal(second.stopReason, "stop", second.errorMessage);
  assert.equal(f.upgrades(), 4, "tool continuation must not retry WS");
  assert.equal(f.posts.length, 2);
  const input = f.posts[1].input as JsonObject[];
  assert.equal(input.find(item => item.type === "function_call")?.call_id, "call_http");
  assert.equal(input.find(item => item.type === "function_call_output")?.call_id, "call_http");
  // Repeating the same user text still starts a fresh user turn.
  f.pool.beginUserTurn(opts.sessionId);
  const third = await f.api.streamSimple(f.selected, context([
    ...messages, second, { role: "user", timestamp: 3, content: "你好" },
  ]), opts).result();
  assert.equal(third.stopReason, "stop", third.errorMessage);
  assert.equal(third.responseId, "resp_ws");
  assert.equal(f.upgrades(), 5);
  assert.equal(f.posts.length, 2);
  assert.equal(f.wsBodies[0].previous_response_id, undefined);
  assert.match(JSON.stringify(f.wsBodies[0].input), /file/);
});

test("SSE failure remains sticky for this user turn but not other sessions", async t => {
  const f = await setup(t, {
    reject: n => n <= 4 ? 503 : undefined,
    http: res => { res.writeHead(503); res.end("HTTP unavailable"); },
  });
  for (let i = 0; i < 2; i++) {
    const result = await f.api.streamSimple(f.selected, context(), opts).result();
    assert.equal(result.stopReason, "error");
    assert.match(result.errorMessage!, /HTTP error/);
  }
  assert.equal(f.upgrades(), 4);
  assert.equal(f.posts.length, 2);
  const other = await f.api.streamSimple(f.selected, context(), { ...opts, sessionId: "session-other" }).result();
  assert.equal(other.responseId, "resp_ws");
  assert.equal(f.upgrades(), 5);
});

test("retry after a rejected cached continuation sends full input on a fresh WS", async t => {
  const f = await setup(t, { ws: (socket, _body, n) => {
    if (n === 2) socket.send(JSON.stringify({ type: "error", code: "previous_response_not_found" }));
    else for (const event of events(`resp_${n}`)) socket.send(JSON.stringify(event));
  } });
  const first = await f.api.streamSimple(f.selected, context(), opts).result();
  f.pool.beginUserTurn(opts.sessionId);
  const result = await f.api.streamSimple(f.selected, context([
    { role: "user", timestamp: 1, content: "你好" }, first, { role: "user", timestamp: 2, content: "continue" },
  ]), opts).result();
  assert.equal(result.stopReason, "stop", result.errorMessage);
  assert.equal(f.wsBodies[1].previous_response_id, "resp_1");
  assert.equal(f.wsBodies[2].previous_response_id, undefined);
  assert.equal((f.wsBodies[2].input as unknown[]).length, 4);
  assert.equal(f.upgrades(), 2);
  assert.equal(f.posts.length, 0);
});

for (const shutdown of [false, true]) test(`${shutdown ? "shutdown" : "abort"} stops retry backoff without SSE`, async t => {
  const controller = new AbortController();
  const f = await setup(t, { reject: () => {
    setTimeout(() => shutdown ? f.pool.close() : controller.abort(), 25);
    return 503;
  } });
  const result = await f.api.streamSimple(f.selected, context(), { ...opts, signal: controller.signal }).result();
  assert.equal(result.stopReason, "aborted");
  assert.equal(f.upgrades(), 1);
  assert.equal(f.posts.length, 0);
  assert.equal(f.pool.getUserTurn(opts.sessionId).useSse, false);
});

for (const maxRetries of [0, 1, 2, 10]) test(`explicit maxRetries=${maxRetries} caps the default three retries`, async t => {
  const f = await setup(t, { reject: 503 });
  const result = await f.api.streamSimple(f.selected, context(), { ...opts, maxRetries }).result();
  assert.equal(result.stopReason, "stop", result.errorMessage);
  assert.equal(f.upgrades(), Math.min(maxRetries, 3) + 1);
  assert.equal(f.posts.length, 1);
  assert.equal(f.pool.getUserTurn(opts.sessionId).useSse, maxRetries !== 0);
});

test("zero-retry best-effort calls follow existing SSE affinity without WS probes", async t => {
  const f = await setup(t);
  f.pool.getUserTurn(opts.sessionId).useSse = true;
  const result = await f.api.streamSimple(f.selected, context(), { ...opts, maxRetries: 0 }).result();
  assert.equal(result.stopReason, "stop", result.errorMessage);
  assert.equal(f.posts.length, 1);
  assert.equal(f.upgrades(), 0);
});

const connectionLimitError = {
  type: "invalid_request_error",
  code: "websocket_connection_limit_reached",
  message: "Responses websocket connection limit reached (60 minutes). Create a new websocket connection to continue.",
};
const connectionLimitEvent = { type: "error", status: 400, error: connectionLimitError };

for (const envelope of [
  connectionLimitEvent,
  { ...connectionLimitError, type: "error" },
  { type: "response.failed", response: { status: "failed", error: connectionLimitError } },
]) test(`connection lifetime limit (${envelope.type}${"status" in envelope ? "/400" : ""}) reconnects with full history then resumes deltas`, async t => {
  const sockets: WebSocket[] = [];
  const f = await setup(t, { ws: (socket, _body, n) => {
    sockets.push(socket);
    if (n === 2) {
      // Leave the old connection OPEN: the client must explicitly discard it.
      socket.send(JSON.stringify(envelope));
    } else for (const event of events(`resp_${n}`)) socket.send(JSON.stringify(event));
  } });
  const first = await f.api.streamSimple(f.selected, context(), opts).result();
  const history: Context["messages"] = [
    { role: "user", timestamp: 1, content: "你好" }, first,
    { role: "user", timestamp: 2, content: "continue" },
  ];
  f.pool.beginUserTurn(opts.sessionId);
  let hooks = 0;
  const call = f.api.streamSimple(f.selected, context(history), { ...opts, onPayload: () => { hooks++; } });
  const types: string[] = [];
  for await (const event of call) types.push(event.type);
  const result = await call.result();
  assert.equal(result.stopReason, "stop", result.errorMessage);
  assert.equal(result.responseId, "resp_3");
  assert.equal(types.filter(type => type === "start").length, 1);
  assert.equal(hooks, 1);
  assert.equal(sockets[0], sockets[1]);
  assert.notEqual(sockets[1], sockets[2]);
  assert.equal(sockets[1].readyState, 3, "expired connection must be closed");
  assert.equal(f.wsBodies[1].previous_response_id, "resp_1");
  assert.equal(f.wsBodies[2].previous_response_id, undefined);
  assert.equal((f.wsBodies[2].input as unknown[]).length, 4);
  assert.match(JSON.stringify(f.wsBodies[2].input), /SSE 你好/);
  assert.equal(f.upgrades(), 2);
  assert.equal(f.posts.length, 0);
  assert.equal(f.pool.getUserTurn(opts.sessionId).useSse, false);
  const next = await f.api.streamSimple(f.selected, context([
    ...history, result, { role: "user", timestamp: 3, content: "next" },
  ]), opts).result();
  assert.equal(next.stopReason, "stop", next.errorMessage);
  assert.equal(sockets[2], sockets[3]);
  assert.equal(f.wsBodies[3].previous_response_id, "resp_3");
  assert.equal((f.wsBodies[3].input as unknown[]).length, 1);
});

test("repeated connection lifetime errors exhaust four attempts and keep SSE until next user", async t => {
  const f = await setup(t, { ws: (socket, _body, n) => {
    if (n <= 4) socket.send(JSON.stringify(connectionLimitEvent));
    else for (const event of events("resp_recovered")) socket.send(JSON.stringify(event));
  } });
  const first = await f.api.streamSimple(f.selected, context(), opts).result();
  assert.equal(first.stopReason, "stop", first.errorMessage);
  assert.equal(first.diagnostics?.[0].details?.attempts, 4);
  assert.equal(f.upgrades(), 4);
  assert.equal(f.posts.length, 1);
  const second = await f.api.streamSimple(f.selected, context(), opts).result();
  assert.equal(second.stopReason, "stop");
  assert.equal(f.upgrades(), 4);
  assert.equal(f.posts.length, 2);
  f.pool.beginUserTurn(opts.sessionId);
  const third = await f.api.streamSimple(f.selected, context(), opts).result();
  assert.equal(third.responseId, "resp_recovered");
  assert.equal(f.upgrades(), 5);
});

for (const kind of ["text", "tool", "thinking"]) test(`connection lifetime error after ${kind} output must not replay`, async t => {
  const f = await setup(t, { ws: socket => {
    const item = kind === "tool" ? { type: "function_call", id: "fc_1", call_id: "call_1", name: "write", arguments: "" }
      : kind === "thinking" ? { type: "reasoning", id: "rs_1", summary: [] }
      : { type: "message", id: "msg_1", role: "assistant", content: [] };
    socket.send(JSON.stringify({ type: "response.output_item.added", output_index: 0, item }));
    socket.send(JSON.stringify(connectionLimitEvent));
  } });
  const result = await f.api.streamSimple(f.selected, context(), opts).result();
  assert.equal(result.stopReason, "error");
  assert.match(result.errorMessage!, /websocket_connection_limit_reached/);
  assert.equal(f.upgrades(), 1);
  assert.equal(f.posts.length, 0);
});

for (const transport of ["websocket", "websocket-cached"] as const) test(`connection lifetime errors preserve strict ${transport} semantics`, async t => {
  const f = await setup(t, { ws: socket => socket.send(JSON.stringify(connectionLimitEvent)) });
  const result = await f.api.streamSimple(f.selected, context(), { ...opts, transport }).result();
  assert.equal(result.stopReason, "error");
  assert.match(result.errorMessage!, /websocket_connection_limit_reached/);
  assert.equal(f.upgrades(), 1);
  assert.equal(f.posts.length, 0);
});

test("connection lifetime error does not replay a caller-owned previous_response_id", async t => {
  const f = await setup(t, { ws: socket => socket.send(JSON.stringify(connectionLimitEvent)) });
  const result = await f.api.streamSimple(f.selected, context(), {
    ...opts, onPayload: body => ({ ...body as JsonObject, previous_response_id: "external" }),
  }).result();
  assert.equal(result.stopReason, "error");
  assert.equal(f.upgrades(), 1);
  assert.equal(f.posts.length, 0);
});

test("connection lifetime error over SSE retains its original message and does not retry", async t => {
  const f = await setup(t, { http: res => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.end(`data: ${JSON.stringify(connectionLimitEvent)}\n\n`);
  } });
  const result = await f.api.streamSimple(f.selected, context(), { ...opts, transport: "sse" }).result();
  assert.equal(result.stopReason, "error");
  assert.match(result.errorMessage!, /websocket_connection_limit_reached/);
  assert.ok(result.errorMessage!.includes(connectionLimitError.message));
  assert.equal(f.upgrades(), 0);
  assert.equal(f.posts.length, 1);
});
