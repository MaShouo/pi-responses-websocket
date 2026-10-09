import assert from "node:assert/strict";
import { test } from "node:test";
import { pathToFileURL } from "node:url";

// Opt-in: exercise the user's actual companion extension, not a copied fixture.
const path = process.env.PI_RESPONSES_WS_TEST_INSTRUCTIONS;
if (path) {
  const { promoteOpenAIResponsesInstructions: promote } = await import(pathToFileURL(path).href);
  const model = { provider: "responses-websocket", api: "openai-responses" };
  const user = { role: "user", content: "hello" };
  for (const role of ["system", "developer"]) {
    test(`instructions: promotes ${role} for WS provider without mutating input`, () => {
      const payload = { model: "mock", input: [{ role, content: [{ type: "input_text", text: "First" }, { type: "text", text: "Second" }] }, user], store: false };
      const snapshot = structuredClone(payload);
      const result = promote(payload, model);
      assert.deepEqual(result, { ...payload, instructions: "First\n\nSecond", input: [user] });
      assert.deepEqual(payload, snapshot);
      assert.equal(promote(result, model), undefined);
    });
  }
  test("instructions: preserves explicit instructions", () => {
    assert.equal(promote({ instructions: "Explicit", input: [{ role: "system", content: "System" }, user] }, model), undefined);
  });
  test("instructions: retains existing OpenAI matching and excludes unrelated providers/APIs", () => {
    const payload = { input: [{ role: "system", content: "System" }, user] };
    for (const provider of ["openai", "openai-custom", "openai_custom"]) {
      assert.equal(promote(payload, { ...model, provider }).instructions, "System");
    }
    for (const identity of [undefined, { ...model, provider: "unrelated" }, { ...model, api: "openai-completions" }, { ...model, provider: "responses-websocket-other" }]) {
      assert.equal(promote(payload, identity), undefined);
    }
  });
  test("instructions: ignores user-first, blank and nontext inputs", () => {
    for (const payload of [null, {}, { input: [user] }, { input: [{ role: "system", content: "  " }] }, { input: [{ role: "system", content: [{ type: "input_image", image_url: "x" }] }] }]) {
      assert.equal(promote(payload, model), undefined);
    }
  });
}
