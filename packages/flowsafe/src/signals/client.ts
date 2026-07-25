// SPDX-License-Identifier: Apache-2.0
// Track C (M-004), CI-M-004-005 — a DOM-free SignalClient in the
// ApprovalApiClient mold: fetch injected behind a minimal structural type, so it
// typechecks in the workers-typed pass, runs under plain node in tests, and gets
// the global fetch by default in a browser. Subpath-only
// (`@proofoftech/flowsafe/signals/client`) — never the host-side signals barrel.
//
// The SEND surface targets the createSignalRouter routes
// (`POST /api/threads/:threadId/{message,queue,signal,state,notification}`), so
// every call crosses the P6 ingestion gate (auth → role → thread-prefix
// ownership → allowlist/size/rate → audit) before it reaches a thread DO.
//
// SUBSCRIBE / hub bridge (DL-016 — CORRECTED): a thread's live chunks are a
// per-END-USER stream (one conversation's tokens), NOT tenant-wide fan-out. The
// Part B hub is per-TENANT, so forwarding subscribeToThread chunks onto it and
// filtering CLIENT-side by threadId would put one end-user's thread bytes on
// EVERY same-tenant operator's hub socket — a within-tenant confidentiality leak
// (and wasted fan-out). So phase-2 live subscribe MUST be scoped SERVER-side per
// thread / per resourceId: a thread-addressed channel (a per-thread WS on the
// thread DO, or a hub subscription the server filters by (tenantId, threadId)
// before it emits), never client-side filtering of a tenant firehose. It is
// deferred to phase 2 and shares Track A's real-loop boundary (a loop must be
// producing chunks to forward). Nothing here mints capability — sendToolApproval
// is not an approval surface (P8).

export interface SignalResponseLike {
  ok: boolean;
  status: number;
  json(): Promise<unknown>;
}

export type SignalFetchLike = (
  url: string,
  init?: {
    method?: string;
    headers?: Record<string, string>;
    body?: string;
  },
) => Promise<SignalResponseLike>;

export class SignalApiError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = 'SignalApiError';
    this.status = status;
  }
}

/** Idle-thread behavior a send may request (mirrors the router/thread-DO contract). */
export type SignalIdleBehavior = 'wake' | 'persist' | 'discard';
/** Active-thread behavior a signal may request. */
export type SignalActiveBehavior = 'deliver' | 'persist' | 'discard';

export interface SendMessageBody {
  contents: string;
  attributes?: Record<string, string | number | boolean | null>;
  ifIdle?: SignalIdleBehavior;
}

export interface SendSignalBody {
  contents: string;
  tagName?: string;
  attributes?: Record<string, string | number | boolean | null>;
  ifActive?: SignalActiveBehavior;
  ifIdle?: SignalIdleBehavior;
}

export interface SendStateBody {
  id: string;
  cacheKey: string;
  contents: string;
  mode?: 'snapshot' | 'delta';
  value?: unknown;
  delta?: unknown;
  attributes?: Record<string, string | number | boolean | null>;
}

export interface SendNotificationBody {
  source: string;
  kind: string;
  summary: string;
  priority?: 'low' | 'medium' | 'high' | 'urgent';
  payload?: unknown;
  dedupeKey?: string;
  coalesceKey?: string;
  attributes?: Record<string, string | number | boolean | null>;
}

export interface SignalClientOptions {
  /** Default: '/api/threads' (same-origin deployment). Matches createSignalRouter's basePath. */
  baseUrl?: string;
  /** Default: globalThis.fetch. Injected in tests and non-browser hosts. */
  fetch?: SignalFetchLike;
  /** Sent on every request — the deployment's auth (bearer token, etc.). */
  headers?: Record<string, string>;
}

export class SignalClient {
  readonly #baseUrl: string;
  readonly #fetch: SignalFetchLike;
  readonly #headers: Record<string, string>;

  constructor(options: SignalClientOptions = {}) {
    this.#baseUrl = options.baseUrl ?? '/api/threads';
    const fetchFn =
      options.fetch ??
      ((globalThis as { fetch?: unknown }).fetch as
        | SignalFetchLike
        | undefined);
    if (!fetchFn) {
      throw new Error('SignalClient: no fetch available; pass one');
    }
    this.#fetch = (url, init) => fetchFn(url, init);
    this.#headers = { ...options.headers };
  }

  /** An immediate user message — joins the active loop, or (idle) wakes/persists. */
  async sendMessage(threadId: string, body: SendMessageBody): Promise<unknown> {
    return this.#post(threadId, 'message', body);
  }

  /** A message delivered on the NEXT turn (never wakes an idle thread). */
  async queueMessage(
    threadId: string,
    body: SendMessageBody,
  ): Promise<unknown> {
    return this.#post(threadId, 'queue', body);
  }

  /** A system signal (ifActive/ifIdle deliver/persist/discard/wake). */
  async sendSignal(threadId: string, body: SendSignalBody): Promise<unknown> {
    return this.#post(threadId, 'signal', body);
  }

  /** A durable thread-state lane (snapshot/delta, cacheKey dedupe). */
  async sendStateSignal(
    threadId: string,
    body: SendStateBody,
  ): Promise<unknown> {
    return this.#post(threadId, 'state', body);
  }

  /** A durable AGENT inbox notification (surfaces on the next turn). */
  async sendNotificationSignal(
    threadId: string,
    body: SendNotificationBody,
  ): Promise<unknown> {
    return this.#post(threadId, 'notification', body);
  }

  async #post(
    threadId: string,
    channel: string,
    body: unknown,
  ): Promise<unknown> {
    const url = `${this.#baseUrl}/${encodeURIComponent(threadId)}/${channel}`;
    const response = await this.#fetch(url, {
      method: 'POST',
      headers: { ...this.#headers, 'content-type': 'application/json' },
      body: JSON.stringify(body),
    });
    const payload = await response.json().catch(() => undefined);
    if (!response.ok) {
      const message =
        payload !== null &&
        typeof payload === 'object' &&
        'error' in payload &&
        typeof (payload as { error: unknown }).error === 'string'
          ? (payload as { error: string }).error
          : `signal request failed with status ${response.status}`;
      throw new SignalApiError(response.status, message);
    }
    return payload;
  }
}
