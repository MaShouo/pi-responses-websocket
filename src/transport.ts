import { createHash } from "node:crypto";
import type { IncomingHttpHeaders } from "node:http";
import WebSocket from "ws";

export type JsonObject = Record<string, unknown>;
export interface ResponseBody extends JsonObject { input: unknown[] }
export interface WireEvent extends JsonObject { type: string }

export class WebSocketTransportError extends Error {
  constructor(message: string, readonly fallbackAllowed = true, options?: ErrorOptions) {
    super(message, options);
    this.name = "WebSocketTransportError";
  }
}
export class WebSocketHandshakeError extends WebSocketTransportError {
  constructor(readonly status: number) {
    // Authentication, quota, malformed requests and redirects must not be
    // retried using another transport. A rejected upgrade has not generated.
    super(`WebSocket upgrade rejected (HTTP ${status})`, [404, 405, 426].includes(status) || status >= 500 && status <= 599);
    this.name = "WebSocketHandshakeError";
  }
}

export function responsesHttpUrl(baseUrl: string): string {
  const url = new URL(responsesUrl(baseUrl));
  url.protocol = url.protocol === "wss:" ? "https:" : "http:";
  return url.toString();
}

export function responsesUrl(baseUrl: string): string {
  const url = new URL(baseUrl);
  if (url.username || url.password) throw new Error("Use headers for WebSocket credentials, not URL userinfo");
  if (!["https:", "http:", "wss:", "ws:"].includes(url.protocol)) {
    throw new Error("Responses baseUrl must use http(s) or ws(s)");
  }
  url.protocol = url.protocol === "https:" || url.protocol === "wss:" ? "wss:" : "ws:";
  url.pathname = url.pathname.replace(/\/+$/, "");
  if (!url.pathname.endsWith("/responses")) url.pathname += "/responses";
  url.hash = "";
  return url.toString();
}

// Object key order is not protocol state; array order and all values are.
export function canonical(value: unknown): string {
  return JSON.stringify(value, (_key, item: unknown) => {
    if (item && typeof item === "object" && !Array.isArray(item)) {
      return Object.fromEntries(Object.entries(item).sort(([a], [b]) => a.localeCompare(b)));
    }
    return item;
  });
}

function template(body: ResponseBody): string {
  const { input: _input, previous_response_id: _previous, ...rest } = body;
  return canonical(rest);
}

interface Continuation { template: string; baseline: unknown[]; responseId: string }
interface Entry {
  socket: WebSocket;
  key?: string;
  busy: boolean;
  disposed?: boolean;
  continuation?: Continuation;
  idleTimer?: ReturnType<typeof setTimeout>;
}
export interface ConnectOptions {
  url: string;
  headers: Record<string, string>;
  sessionId?: string;
  signal?: AbortSignal;
  connectTimeoutMs?: number;
  idleTimeoutMs?: number;
  onHandshake?: (response: { status: number; headers: Record<string, string> }) => Promise<void>;
}

function abortError(): Error { return new Error("Request was aborted"); }
function checkAbort(signal?: AbortSignal): void { if (signal?.aborted) throw abortError(); }
function responseHeaders(headers: IncomingHttpHeaders): Record<string, string> {
  return Object.fromEntries(Object.entries(headers).flatMap(([key, value]) =>
    value === undefined ? [] : [[key, Array.isArray(value) ? value.join(", ") : String(value)]],
  ));
}

export class ResponsesSocketPool {
  private entries = new Map<string, Entry>();
  private all = new Set<Entry>();
  private requests = new Set<AbortController>();

  beginRequest(signal?: AbortSignal): { signal: AbortSignal; dispose: () => void } {
    const controller = new AbortController();
    this.requests.add(controller);
    return {
      signal: signal ? AbortSignal.any([signal, controller.signal]) : controller.signal,
      dispose: () => { this.requests.delete(controller); },
    };
  }

  close(): void {
    for (const controller of this.requests) controller.abort();
    this.requests.clear();
    for (const entry of this.all) this.discard(entry);
  }

  private discard(entry: Entry): void {
    clearTimeout(entry.idleTimer);
    if (entry.key && this.entries.get(entry.key) === entry) this.entries.delete(entry.key);
    this.all.delete(entry);
    entry.disposed = true;
    entry.continuation = undefined;
    entry.socket.terminate();
  }

  async acquire(options: ConnectOptions): Promise<SocketLease> {
    checkAbort(options.signal);
    const key = options.sessionId ? createHash("sha256")
      .update(canonical([options.url, options.headers, options.sessionId])).digest("hex") : undefined;
    let entry = key ? this.entries.get(key) : undefined;
    if (entry && !entry.busy && entry.socket.readyState === WebSocket.OPEN) {
      clearTimeout(entry.idleTimer);
      entry.busy = true;
    } else {
      // Concurrent requests never share a socket. The temporary connection is
      // closed on release rather than overwriting an active session's cache.
      const pooledKey = entry?.busy ? undefined : key;
      const socket = new WebSocket(options.url, {
        headers: options.headers,
        followRedirects: false,
        perMessageDeflate: false,
        handshakeTimeout: options.connectTimeoutMs ?? 15_000,
      });
      entry = { socket, key: pooledKey, busy: true };
      this.all.add(entry);
      const created = entry;
      socket.on("error", () => {}); // also covers idle/terminate errors
      socket.once("close", () => {
        clearTimeout(created.idleTimer);
        this.all.delete(created);
        if (created.key && this.entries.get(created.key) === created) this.entries.delete(created.key);
      });
      let handshake: { status: number; headers: Record<string, string> } | undefined;
      socket.once("upgrade", response => {
        handshake = { status: response.statusCode ?? 101, headers: responseHeaders(response.headers) };
      });
      try {
        await new Promise<void>((resolve, reject) => {
          const cleanup = () => {
            socket.off("open", onOpen);
            socket.off("error", onError);
            socket.off("close", onClose);
            socket.off("unexpected-response", onRejected);
            options.signal?.removeEventListener("abort", onAbort);
          };
          const onOpen = () => { cleanup(); resolve(); };
          const rejectSetup = (error: Error) => { cleanup(); reject(error); };
          const onError = (error: Error) => rejectSetup(new WebSocketTransportError(error.message, !created.disposed, { cause: error }));
          const onClose = () => onError(new Error("WebSocket closed during handshake"));
          const onRejected = (_request: unknown, response: import("node:http").IncomingMessage) => {
            rejectSetup(new WebSocketHandshakeError(response.statusCode ?? 0));
            response.destroy();
          };
          const onAbort = () => rejectSetup(abortError());
          socket.once("unexpected-response", onRejected);
          socket.once("open", onOpen);
          socket.once("error", onError);
          socket.once("close", onClose);
          options.signal?.addEventListener("abort", onAbort, { once: true });
          if (options.signal?.aborted) onAbort();
        });
        if (handshake) await options.onHandshake?.(handshake);
        checkAbort(options.signal);
        if (socket.readyState !== WebSocket.OPEN) throw new WebSocketTransportError("WebSocket closed before request", !created.disposed);
        if (pooledKey) this.entries.set(pooledKey, entry);
      } catch (error) {
        this.discard(entry);
        throw error;
      }
    }
    const leased = entry;
    return new SocketLease(leased, options, (keep) => {
      if (!keep || !leased.key || leased.socket.readyState !== WebSocket.OPEN) {
        this.discard(leased);
      } else {
        leased.busy = false;
        // Bound idle resources without background work at extension load time.
        leased.idleTimer = setTimeout(() => this.discard(leased), 300_000);
        leased.idleTimer.unref();
      }
    });
  }
}

export class SocketLease {
  private released = false;
  private completed = false;
  private requestBody?: ResponseBody;
  usedContinuation = false;
  // Set at frame receipt, before async hooks/parsing: buffered content must
  // also prevent replay if an error/EOF arrives in the same event-loop tick.
  receivedOutput = false;
  constructor(private entry: Entry, private options: ConnectOptions, private releaseEntry: (keep: boolean) => void) {}

  async *events(body: ResponseBody, cached: boolean): AsyncGenerator<WireEvent> {
    this.requestBody = body;
    const previous = this.entry.continuation;
    let frame = body;
    if (cached && !body.previous_response_id && previous && template(body) === previous.template &&
        body.input.length >= previous.baseline.length &&
        canonical(body.input.slice(0, previous.baseline.length)) === canonical(previous.baseline)) {
      frame = { ...body, previous_response_id: previous.responseId, input: body.input.slice(previous.baseline.length) };
    }
    this.usedContinuation = !!frame.previous_response_id;
    this.entry.continuation = undefined;
    const socket = this.entry.socket;
    const queue: WireEvent[] = [];
    let failure: Error | undefined;
    let wake: (() => void) | undefined;
    let terminal = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const fail = (error: Error) => { failure ??= error; wake?.(); };
    const resetTimer = () => {
      clearTimeout(timer);
      const ms = this.options.idleTimeoutMs ?? 300_000;
      if (ms > 0) timer = setTimeout(() => fail(new WebSocketTransportError(`WebSocket response idle timeout after ${ms}ms`, !this.entry.disposed)), ms);
    };
    const onMessage = (data: WebSocket.RawData) => {
      if (terminal || failure) return;
      try {
        const parsed: unknown = JSON.parse(data.toString());
        if (!parsed || typeof parsed !== "object" || typeof (parsed as WireEvent).type !== "string") {
          throw new Error("WebSocket event must be a JSON object with a type");
        }
        const event = parsed as WireEvent;
        const responseOutput = (event.response as { output?: unknown[] } | undefined)?.output;
        if (responseOutput?.length || !["response.created", "response.in_progress", "response.metadata", "codex.response.metadata", "codex.rate_limits", "error", "response.failed"].includes(event.type)) {
          this.receivedOutput = true;
        }
        queue.push(event);
        terminal = ["response.completed", "response.done", "response.failed", "response.incomplete", "error"].includes(event.type);
        if (terminal) clearTimeout(timer);
        else resetTimer();
        wake?.();
      } catch {
        fail(new Error("Invalid JSON Responses WebSocket event"));
      }
    };
    const onError = (error: Error) => { if (!terminal) fail(new WebSocketTransportError(error.message, !this.entry.disposed, { cause: error })); };
    const onClose = (code: number) => {
      if (!terminal) fail(new WebSocketTransportError(`WebSocket closed before response completed (code ${code})`, !this.entry.disposed && code !== 1008));
    };
    const onAbort = () => fail(abortError());
    socket.on("message", onMessage);
    socket.on("error", onError);
    socket.on("close", onClose);
    this.options.signal?.addEventListener("abort", onAbort, { once: true });
    try {
      checkAbort(this.options.signal);
      resetTimer();
      socket.send(JSON.stringify({ ...frame, type: "response.create" }), error => { if (error) onError(error); });
      while (true) {
        checkAbort(this.options.signal);
        const event = queue.shift();
        if (event) { yield event; continue; }
        if (failure) throw failure;
        if (terminal) break;
        await new Promise<void>(resolve => { wake = resolve; });
        wake = undefined;
      }
      this.completed = true;
    } finally {
      clearTimeout(timer);
      socket.off("message", onMessage);
      socket.off("error", onError);
      socket.off("close", onClose);
      this.options.signal?.removeEventListener("abort", onAbort);
    }
  }

  finish(responseId: string | undefined, projectedOutput: unknown[]): void {
    const body = this.requestBody;
    if (this.completed && body && !body.previous_response_id && responseId) {
      this.entry.continuation = {
        template: template(body), baseline: [...body.input, ...projectedOutput], responseId,
      };
    }
    this.release(true);
  }

  release(keep = false): void {
    if (this.released) return;
    this.released = true;
    this.releaseEntry(keep && this.completed);
  }
}
