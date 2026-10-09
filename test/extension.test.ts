import assert from "node:assert/strict";
import { test } from "node:test";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { ProviderStreams } from "@earendil-works/pi-ai";
import extension, { isResponsesWebSocketProvider } from "../src/index.ts";
import { ResponsesSocketPool } from "../src/transport.ts";

type Registration = { name?: string; api: string; streamSimple: ProviderStreams["streamSimple"] };
type Handler = (event: unknown, ctx: unknown) => unknown;
function fixture() {
  const registrations = new Map<string, Registration>();
  const handlers = new Map<string, Handler>();
  let calls = 0;
  extension({
    registerProvider: (id: string, config: Registration) => {
      calls++;
      registrations.set(id, config);
    },
    on: (event: string, handler: Handler) => { handlers.set(event, handler); },
  } as unknown as ExtensionAPI);
  const emit = (event: string, payload: unknown = {}, providers: string[] = []) => {
    const handler = handlers.get(event);
    assert.ok(handler, `Missing ${event} handler`);
    return handler(payload, { modelRegistry: { getAll: () => providers.map(provider => ({ provider })) } });
  };
  return { registrations, emit, calls: () => calls };
}

test("WS provider matching uses a case-insensitive suffix and preserves the legacy id", () => {
  for (const id of ["OpenAI-ws", "OpenAI-WS", "GPT-WS", "custom-Ws", "custom-wS", "OpenAI_ws", "OpenAI_WS", "GPT_WS", "custom_Ws", "responses-websocket"]) {
    assert.equal(isResponsesWebSocketProvider(id), true, id);
  }
  for (const id of ["", "openai", "gpt", "ws", "ws-openai", "openai-ws-extra", "OpenAI-WS ", "OpenAI_ws_extra", "OpenAI_WS ", "responses-websocket-other"]) {
    assert.equal(isResponsesWebSocketProvider(id), false, id);
  }
});

test("session startup registers all matching providers once without replacing user configuration", () => {
  const f = fixture();
  assert.deepEqual([...f.registrations.keys()], ["responses-websocket"]);
  const providers = ["OpenAI-ws", "OpenAI-WS", "GPT-WS", "OpenAI-ws", "openai", "not-ws-extra"];
  f.emit("session_start", {}, providers);
  f.emit("session_start", {}, providers);
  assert.equal(f.calls(), 4);
  assert.deepEqual([...f.registrations.keys()], ["responses-websocket", "OpenAI-ws", "OpenAI-WS", "GPT-WS"]);
  for (const id of ["OpenAI-ws", "OpenAI-WS", "GPT-WS"]) {
    const config = f.registrations.get(id)!;
    assert.deepEqual(Object.keys(config).sort(), ["api", "streamSimple"]);
    assert.equal(config.api, "openai-responses");
    assert.equal(typeof config.streamSimple, "function");
  }
  assert.notEqual(f.registrations.get("OpenAI-ws")!.streamSimple, f.registrations.get("OpenAI-WS")!.streamSimple);
});

test("model selection discovers new suffix providers without duplicate registration", () => {
  const f = fixture();
  f.emit("model_select", { model: { provider: "new-WS" } });
  f.emit("model_select", { model: { provider: "new-WS" } });
  f.emit("model_select", { model: { provider: "openai" } });
  assert.deepEqual([...f.registrations.keys()], ["responses-websocket", "new-WS"]);
  assert.equal(f.calls(), 2);
});

test("shutdown closes every provider pool", t => {
  const closed = new Set<ResponsesSocketPool>();
  t.mock.method(ResponsesSocketPool.prototype, "close", function (this: ResponsesSocketPool) { closed.add(this); });
  const f = fixture();
  f.emit("session_start", {}, ["OpenAI-ws", "GPT-WS"]);
  f.emit("session_shutdown");
  assert.equal(closed.size, 3);
});
