import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "node:test";
import { WebSocketServer } from "ws";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const cli = process.env.PI_RESPONSES_WS_TEST_CLI ?? join(root, "node_modules/@earendil-works/pi-coding-agent/dist/cli.js");
const extension = process.env.PI_RESPONSES_WS_TEST_EXTENSION ?? join(root, "src/index.ts");
const instructionsExtension = process.env.PI_RESPONSES_WS_TEST_INSTRUCTIONS;
const instructionsMarker = "INSTRUCTIONS_COMPAT_TEST_MARKER";
function run(args: string[], cwd: string, agentDir: string): Promise<{ code: number | null; out: string; err: string }> {
  return new Promise((resolveRun, reject) => {
    const child = spawn(process.execPath, [cli, ...args], {
      cwd, env: { ...process.env, PI_CODING_AGENT_DIR: agentDir, PI_OFFLINE: "1", NO_COLOR: "1" },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let out = "", err = "";
    child.stdout.on("data", chunk => { out += chunk; });
    child.stderr.on("data", chunk => { err += chunk; });
    const timeout = setTimeout(() => { child.kill(); reject(new Error(`Pi CLI timeout (${args.includes("--print") ? "generation" : "catalog"}): ${err}\n${out}`)); }, 45_000);
    child.on("error", error => { clearTimeout(timeout); reject(error); });
    child.on("close", code => { clearTimeout(timeout); resolveRun({ code, out, err }); });
  });
}

const scenarios = [
  ...["responses-websocket", "OpenAI-ws", "OpenAI-WS", "GPT-WS", "custom-Ws", "OpenAI_ws", "OpenAI_WS", "GPT_WS", "custom_Ws"].flatMap(provider =>
    [false, true].map(fallback => ({ provider, fallback, websocket: true, withInstructions: false }))),
  ...["OpenAI", "OpenAI-ws-extra", "OpenAI_ws_extra"].map(provider => ({ provider, fallback: false, websocket: false, withInstructions: false })),
  ...(instructionsExtension ? [false, true].map(fallback => ({ provider: "responses-websocket", fallback, websocket: true, withInstructions: true })) : []),
];
for (const { provider, fallback, websocket, withInstructions } of scenarios)
test(`actual Pi loader + models.json: ${provider} ${fallback ? "falls back to SSE" : websocket ? "sends a WS request" : "keeps HTTP"}${withInstructions ? " + instructions extension" : ""}`, { timeout: 60000 }, async t => {
  const useHttp = fallback || !websocket;
  const instructionBodies: Record<string, unknown>[] = [];
  const checkInstructions = (body: Record<string, unknown>) => {
    if (withInstructions) instructionBodies.push(body);
  };
  const dir = await mkdtemp(join(tmpdir(), "pi-responses-ws-test-"));
  const agentDir = join(dir, "agent");
  const work = join(dir, "work");
  await mkdir(agentDir);
  await mkdir(work);
  let posts = 0;
  const http = createServer(async (req, res) => {
    if (useHttp && req.method === "POST") {
      const chunks = [];
      for await (const chunk of req) chunks.push(chunk);
      const body = JSON.parse(Buffer.concat(chunks).toString());
      checkInstructions(body);
      assert.equal(body.stream, true);
      assert.equal(body.type, undefined);
      assert.equal(req.headers.authorization, "Bearer mock-key");
      assert.equal(req.headers["x-test-provider"], provider);
      posts++;
      const item = { type: "message", id: "msg_http", role: "assistant", status: "completed", content: [{ type: "output_text", text: "HTTP_CLI_OK", annotations: [] }] };
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.end([
        { type: "response.output_item.added", output_index: 0, item: { ...item, content: [] } },
        { type: "response.output_item.done", output_index: 0, item },
        { type: "response.completed", response: { id: "resp_http", status: "completed", output: [item] } },
      ].map(event => `data: ${JSON.stringify(event)}\n\n`).join(""));
      return;
    }
    res.writeHead(400, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: { message: `Expected WebSocket, received HTTP ${req.method} ${req.url}` } }));
  });
  let upgrades = 0;
  const wss = new WebSocketServer({ server: http, verifyClient: (_info, done) => {
    upgrades++;
    done(!fallback, 426, "Upgrade unavailable");
  } });
  let requests = 0;
  wss.on("connection", (socket, req) => {
    assert.equal(req.headers.authorization, "Bearer mock-key");
    assert.equal(req.headers["x-test-provider"], provider);
    assert.equal(req.url, "/v1/responses");
    socket.on("message", raw => {
      const body = JSON.parse(raw.toString());
      checkInstructions(body);
      assert.equal(body.type, "response.create");
      assert.equal(body.model, "mock-model");
      requests++;
      const item = { type: "message", id: "msg_cli", role: "assistant", status: "completed", content: [{ type: "output_text", text: "WS_CLI_OK", annotations: [] }] };
      for (const event of [
        { type: "response.output_item.added", output_index: 0, item: { ...item, content: [] } },
        { type: "response.output_text.delta", output_index: 0, delta: "WS_CLI_OK" },
        { type: "response.output_item.done", output_index: 0, item },
        { type: "response.completed", response: { id: "resp_cli", status: "completed", output: [item], usage: { input_tokens: 1, output_tokens: 1, total_tokens: 2 } } },
      ]) socket.send(JSON.stringify(event));
    });
  });
  await new Promise<void>(done => http.listen(0, "127.0.0.1", done));
  t.after(async () => {
    for (const client of wss.clients) client.terminate();
    await new Promise<void>(done => wss.close(() => done()));
    await new Promise<void>(done => http.close(() => done()));
    await rm(dir, { recursive: true, force: true });
  });
  await writeFile(join(agentDir, "models.json"), JSON.stringify({ providers: {
    [provider]: {
      name: "My configured provider",
      api: "openai-responses", apiKey: "mock-key",
      headers: { "x-test-provider": provider },
      baseUrl: `http://127.0.0.1:${(http.address() as AddressInfo).port}/v1`,
      models: [{ id: "mock-model", name: "Mock WS", reasoning: false, input: ["text"], contextWindow: 128000, maxTokens: 2048, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }],
    },
  } }));
  await writeFile(join(agentDir, "settings.json"), JSON.stringify({ enableInstallTelemetry: false, enableAnalytics: false }));
  const flags = ["--offline", "--no-extensions", "--no-mcp", "--no-skills", "--no-prompt-templates", "--no-context-files", "--no-themes", "--no-approve", "-e", extension];
  if (withInstructions) flags.push("-e", instructionsExtension!, "--system-prompt", instructionsMarker);
  const catalog = await run([...flags, "--list-models", "mock-model"], work, agentDir);
  assert.equal(catalog.code, 0, catalog.err + catalog.out);
  assert.ok(catalog.out.includes(provider), catalog.out);
  assert.doesNotMatch(catalog.err + catalog.out, /Failed to load extension|Extension Error/i);
  const generation = await run([...flags, "--model", `${provider}/mock-model`, "--no-tools", "--no-session", "--print", "hello"], work, agentDir);
  assert.equal(generation.code, 0, generation.err + generation.out);
  assert.match(generation.out, useHttp ? /HTTP_CLI_OK/ : /WS_CLI_OK/);
  assert.equal(upgrades, websocket ? 1 : 0);
  assert.equal(requests, useHttp ? 0 : 1);
  assert.equal(posts, useHttp ? 1 : 0);
  if (withInstructions) {
    assert.equal(instructionBodies.length, 1);
    const body = instructionBodies[0];
    assert.equal(body.instructions, `${instructionsMarker}\n\n<cwd>\n${work.replaceAll("\\", "/")}\n</cwd>`);
    assert.ok(Array.isArray(body.input));
    assert.ok(!(body.input as { role?: string }[]).some(item => item.role === "system" || item.role === "developer"));
    assert.ok((body.input as { role?: string }[]).some(item => item.role === "user"));
  }
});
