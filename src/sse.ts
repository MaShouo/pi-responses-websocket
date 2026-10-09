import type { OpenAIResponsesOptions } from "@earendil-works/pi-ai";
import { iterateSseMessages } from "./pi-internals.ts";
import { responsesHttpUrl, type ResponseBody, type WireEvent } from "./transport.ts";

/** One HTTP attempt, same endpoint and already-transformed full request. */
export async function* sseEvents(
  baseUrl: string,
  body: ResponseBody,
  headers: Record<string, string>,
  options: OpenAIResponsesOptions,
  onResponse: (response: { status: number; headers: Record<string, string> }) => Promise<void>,
  onStart: () => void,
): AsyncGenerator<WireEvent> {
  const controller = new AbortController();
  let timeout: ReturnType<typeof setTimeout> | undefined;
  let timedOut = false;
  let response: Response | undefined;
  const abort = () => controller.abort();
  const checkStopped = () => {
    if (options.signal?.aborted) throw new Error("Request was aborted");
    if (timedOut) throw new Error(`Responses HTTP idle timeout after ${options.timeoutMs ?? 300_000}ms`);
  };
  const resetTimer = () => {
    clearTimeout(timeout);
    const ms = options.timeoutMs ?? 300_000;
    if (ms > 0) timeout = setTimeout(() => { timedOut = true; controller.abort(); }, ms);
  };
  options.signal?.addEventListener("abort", abort, { once: true });
  try {
    if (options.signal?.aborted) throw new Error("Request was aborted");
    const { type: _type, stream: _stream, ...input } = body;
    resetTimer();
    response = await (options.fetch ?? globalThis.fetch)(responsesHttpUrl(baseUrl), {
      method: "POST",
      headers: { ...headers, "content-type": "application/json", accept: "text/event-stream" },
      body: JSON.stringify({ ...input, stream: true }),
      signal: controller.signal,
      redirect: "error", // Never forward credentials to a redirected endpoint.
    });
    await onResponse({ status: response.status, headers: Object.fromEntries(response.headers) });
    checkStopped();
    if (!response.ok) {
      // Preserve provider codes (including quota/context overflow) without
      // logging payloads or issuing a second HTTP attempt.
      const details = await response.text();
      throw new Error(`Responses HTTP error (${response.status}): ${details.slice(0, 8192)}`);
    }
    if (!response.headers.get("content-type")?.toLowerCase().includes("text/event-stream")) {
      throw new Error("Expected text/event-stream from Responses HTTP endpoint");
    }
    if (options.signal?.aborted) throw new Error("Request was aborted");
    onStart();
    const messages = iterateSseMessages(response, controller);
    const closeMessages = messages.return.bind(messages);
    messages.return = value => {
      // Abort before the decoder cancels its reader (which can otherwise wait
      // on a server that leaves a terminal SSE response open).
      controller.abort();
      return closeMessages(value);
    };
    for await (const message of messages) {
      if (options.signal?.aborted) throw new Error("Request was aborted");
      if (message.data === "[DONE]") break;
      if (!message.data) continue;
      let value: unknown;
      try { value = JSON.parse(message.data); }
      catch { throw new Error("Invalid JSON Responses SSE event"); }
      if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("Invalid Responses SSE event");
      const data = value as Record<string, unknown>;
      const isError = message.event === "error" || data.error && typeof data.error === "object";
      const event = (isError ? { ...data, type: "error" } : data) as WireEvent;
      if (typeof event.type !== "string") throw new Error("Invalid Responses SSE event");
      resetTimer();
      const terminal = ["response.completed", "response.done", "response.failed", "response.incomplete", "error"].includes(event.type);
      if (terminal) clearTimeout(timeout);
      yield event;
      if (terminal) return;
    }
    // The host SSE decoder may finish normally when its signal aborts.
    // Distinguish cancellation/timeouts from an actual truncated response.
    checkStopped();
  } catch (error) {
    checkStopped();
    throw error;
  } finally {
    clearTimeout(timeout);
    options.signal?.removeEventListener("abort", abort);
    controller.abort();
    if (response?.body && !response.body.locked) await response.body.cancel().catch(() => {});
  }
}
