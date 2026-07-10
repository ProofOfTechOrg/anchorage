// REST client for the approval API. Deliberately DOM-free: fetch is injected
// behind a minimal structural type, so this file typechecks in the main
// (workers-types) pass, tests run it in plain node, and the browser gets the
// global fetch by default. Types come straight from approval-api — the wire
// format IS those types serialized.

import type {
  ApprovalDecision,
  ApprovalListFilter,
  ApprovalMetrics,
  ApprovalRecord,
  DecideResult,
} from '../approval-api/types.js';

export interface ResponseLike {
  ok: boolean;
  status: number;
  json(): Promise<unknown>;
}

export type FetchLike = (
  url: string,
  init?: {
    method?: string;
    headers?: Record<string, string>;
    body?: string;
  },
) => Promise<ResponseLike>;

export class ApprovalApiError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = 'ApprovalApiError';
    this.status = status;
  }
}

export interface ApprovalApiClientOptions {
  /** Default: '/api/approvals' (same-origin deployment). */
  baseUrl?: string;
  /** Default: globalThis.fetch. Injected in tests and non-browser hosts. */
  fetch?: FetchLike;
  /** Sent on every request — the deployment's auth (bearer token, etc.). */
  headers?: Record<string, string>;
}

export class ApprovalApiClient {
  readonly #baseUrl: string;
  readonly #fetch: FetchLike;
  readonly #headers: Record<string, string>;

  constructor(options: ApprovalApiClientOptions = {}) {
    this.#baseUrl = options.baseUrl ?? '/api/approvals';
    const fetchFn =
      options.fetch ??
      ((globalThis as { fetch?: unknown }).fetch as FetchLike | undefined);
    if (!fetchFn) {
      throw new Error('ApprovalApiClient: no fetch available; pass one');
    }
    // Global fetch must be invoked without a bound `this` complaint.
    this.#fetch = (url, init) => fetchFn(url, init);
    this.#headers = { ...options.headers };
  }

  async list(filter: ApprovalListFilter = {}): Promise<ApprovalRecord[]> {
    const params = new URLSearchParams();
    if (filter.status !== undefined) {
      const statuses = Array.isArray(filter.status)
        ? filter.status
        : [filter.status];
      params.set('status', statuses.join(','));
    }
    if (filter.workflowId !== undefined)
      params.set('workflowId', filter.workflowId);
    if (filter.runId !== undefined) params.set('runId', filter.runId);
    if (filter.claimedBy !== undefined)
      params.set('claimedBy', filter.claimedBy);
    const query = params.size > 0 ? `?${params.toString()}` : '';
    return (await this.#request(query)) as ApprovalRecord[];
  }

  async get(id: string): Promise<ApprovalRecord> {
    return (await this.#request(
      `/${encodeURIComponent(id)}`,
    )) as ApprovalRecord;
  }

  async metrics(): Promise<ApprovalMetrics> {
    return (await this.#request('/metrics')) as ApprovalMetrics;
  }

  async claim(id: string): Promise<ApprovalRecord> {
    return (await this.#post(
      `/${encodeURIComponent(id)}/claim`,
    )) as ApprovalRecord;
  }

  async decide(
    id: string,
    decision: ApprovalDecision,
    comment?: string,
  ): Promise<DecideResult> {
    const body: Record<string, unknown> = { decision };
    if (comment !== undefined && comment !== '') body.comment = comment;
    return (await this.#post(
      `/${encodeURIComponent(id)}/decide`,
      body,
    )) as DecideResult;
  }

  async delegate(id: string, to: string): Promise<ApprovalRecord> {
    return (await this.#post(`/${encodeURIComponent(id)}/delegate`, {
      to,
    })) as ApprovalRecord;
  }

  // There is deliberately no sweep() — POST /sla/sweep no longer exists. The
  // SLA sweep is cron-owned TCB code (approval-api sweepSLA over a
  // SystemApprovalStore): an HTTP-reachable sweep was an unfiltered
  // cross-tenant read+write behind a role check.

  async #post(path: string, body?: Record<string, unknown>): Promise<unknown> {
    return this.#request(path, {
      method: 'POST',
      ...(body !== undefined && {
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body),
      }),
    });
  }

  async #request(
    path: string,
    init: {
      method?: string;
      headers?: Record<string, string>;
      body?: string;
    } = {},
  ): Promise<unknown> {
    const response = await this.#fetch(`${this.#baseUrl}${path}`, {
      ...init,
      headers: { ...this.#headers, ...init.headers },
    });
    const payload = await response.json().catch(() => undefined);
    if (!response.ok) {
      const message =
        payload !== null &&
        typeof payload === 'object' &&
        'error' in payload &&
        typeof (payload as { error: unknown }).error === 'string'
          ? (payload as { error: string }).error
          : `request failed with status ${response.status}`;
      throw new ApprovalApiError(response.status, message);
    }
    return payload;
  }
}
