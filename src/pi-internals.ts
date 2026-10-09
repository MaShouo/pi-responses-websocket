import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import { join } from "node:path";
import { getPackageDir } from "@earendil-works/pi-coding-agent";

// Pi 1.1's jiti alias rewrites pi-ai subpath imports as compat.js/api/... .
// Resolve exported helpers from the running Node host, not from the extension's
// devDependencies. Native require bypasses that prefix alias without copying Pi.
const hostRequire = createRequire(join(getPackageDir(), "package.json"));
// pi-ai declares import-only exports. Locate the host's installed package, then
// use Node >=22.19 require(ESM) on its physical modules (no jiti alias involved).
const aiRoot = (hostRequire.resolve.paths("@earendil-works/pi-ai") ?? [])
  .map(path => join(path, "@earendil-works/pi-ai"))
  .find(path => existsSync(join(path, "dist/api/openai-responses-shared.js")));
if (!aiRoot) throw new Error("responses-websocket requires a Node/npm Pi 1.1.x installation with pi-ai modules on disk");
const shared = hostRequire(join(aiRoot, "dist/api/openai-responses-shared.js")) as typeof import("@earendil-works/pi-ai/api/openai-responses-shared");
const constrained = hostRequire(join(aiRoot, "dist/api/constrained-sampling.js")) as typeof import("@earendil-works/pi-ai/api/constrained-sampling");
const simple = hostRequire(join(aiRoot, "dist/api/simple-options.js")) as typeof import("@earendil-works/pi-ai/api/simple-options");
const cache = hostRequire(join(aiRoot, "dist/api/openai-prompt-cache.js")) as typeof import("@earendil-works/pi-ai/api/openai-prompt-cache");
const aiRequire = createRequire(join(aiRoot, "package.json"));
const sse = aiRequire("openai/core/streaming") as typeof import("openai/core/streaming");
export const { _iterSSEMessages: iterateSseMessages } = sse;

export const { convertResponsesMessages, convertResponsesTools, processResponsesStream } = shared;
export const { createGrammarToolInputProperties } = constrained;
export const { buildBaseOptions, resolveSamplingParams } = simple;
export const { clampOpenAIPromptCacheKey } = cache;
export type { ConvertResponsesMessagesOptions } from "@earendil-works/pi-ai/api/openai-responses-shared";
