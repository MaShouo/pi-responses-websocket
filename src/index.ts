import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { createResponsesWebSocketApi, PROVIDER_ID } from "./provider.ts";
import { ResponsesSocketPool } from "./transport.ts";

export default function responsesWebSocketExtension(pi: ExtensionAPI): void {
  const pool = new ResponsesSocketPool(); // No sockets until the first request.
  // Explicit streaming registration is intentional: in Pi 1.1 a native
  // provider with an empty catalog lets models.json fall through to HTTP.
  // Do not supply apiKey/baseUrl/models here: preserve all user configuration.
  pi.registerProvider(PROVIDER_ID, {
    name: "Responses WebSocket (API Key)",
    api: "openai-responses",
    streamSimple: createResponsesWebSocketApi(pool).streamSimple,
  });
  pi.on("session_shutdown", () => pool.close());
}
