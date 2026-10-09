import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { createResponsesWebSocketApi, PROVIDER_ID } from "./provider.ts";
import { ResponsesSocketPool } from "./transport.ts";

export function isResponsesWebSocketProvider(provider: string): boolean {
  return provider === PROVIDER_ID || /[-_]ws$/i.test(provider);
}

export default function responsesWebSocketExtension(pi: ExtensionAPI): void {
  // Separate pools also isolate providers that use identical URLs and headers.
  const pools = new Map<string, ResponsesSocketPool>();
  const register = (provider: string) => {
    if (!isResponsesWebSocketProvider(provider) || pools.has(provider)) return;
    const pool = new ResponsesSocketPool(); // No sockets until the first request.
    // Only replace Responses streaming; preserve the configured name, models,
    // baseUrl, credentials and headers. Other APIs on the provider are untouched.
    pi.registerProvider(provider, {
      ...(provider === PROVIDER_ID ? { name: "Responses WebSocket (API Key)" } : {}),
      api: "openai-responses",
      streamSimple: createResponsesWebSocketApi(pool).streamSimple,
    });
    pools.set(provider, pool);
  };

  register(PROVIDER_ID); // Backwards compatibility with existing configurations.
  // The registry is available at session_start, before the first request in
  // interactive, print and RPC modes. Pi owns loading/validating models.json.
  pi.on("session_start", (_event, ctx) => {
    for (const model of ctx.modelRegistry.getAll()) register(model.provider);
  });
  // /model can reload models.json and introduce providers after session_start.
  pi.on("model_select", event => register(event.model.provider));
  pi.on("session_shutdown", () => {
    for (const pool of pools.values()) pool.close();
  });
}
