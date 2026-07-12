// A tiny client for the showcase run surface (GET /workflows, POST /runs,
// GET /runs/:wf/:run), mirroring ApprovalApiClient's shape: injectable
// baseUrl/fetch/headers, a private #request that prefixes the base and merges
// #headers on every call, and a #post wrapper. The actor switcher rebuilds this
// with a new Authorization header to act as a different identity.

// Structural types matching the JSON the run API returns. Kept local (not
// imported from the server-only do-runner/host-kit modules) so the browser
// bundle stays decoupled from Workers-typed server code — the client only needs
// this subset of fields.

/** A workflow module's public metadata (GET /workflows). */
export interface WorkflowMeta {
  id: string;
  title: string;
  description: string;
  sampleInput: unknown;
  allowedRoles?: readonly string[];
}

/** The server's view of the authenticated caller (GET /workflows). */
export interface CatalogActor {
  id: string;
  role: string;
  tenantId?: string;
  /**
   * Server's verdict on whether this identity may decide its OWN request under
   * the deployment's separation-of-duties policy. Display hint only (the server
   * enforces SoD on every decision regardless); the SPA uses it to suppress the
   * "you advanced this run, so the server will refuse" banner for an exempt role.
   */
  canSelfDecide?: boolean;
}

/**
 * GET /workflows: the workflow catalog plus the SERVER-derived identity of
 * the presented token. The UI renders role gates from `actor` — it never
 * infers a role client-side (an unknown token must render as nothing, not
 * default to some local actor table's first entry).
 */
export interface WorkflowCatalog {
  workflows: WorkflowMeta[];
  actor: CatalogActor;
}

/**
 * The run projection (POST /runs, GET /runs/:wf/:run) — a subset of the server's
 * RunSummary carrying the fields the UI may render. The suspension fields are
 * keyed by the dot-joined step path (e.g. 'reviewAndApprove'): `suspendPayload`
 * is what the gate suspended with, and `(suspendedAt, resumeCount)` is the
 * fingerprint approvals bind to (resumeCount is the runtime's per-step resume
 * ordinal — absent on a first suspension, 1,2,… on re-suspensions).
 */
export interface RunSummary {
  runId: string;
  status: string;
  result?: unknown;
  error?: string;
  suspended?: string[][];
  suspendPayload?: Record<
    string,
    { reason?: string; connectors?: string[] } & Record<string, unknown>
  >;
  /** Epoch ms per step key. */
  suspendedAt?: Record<string, number>;
  /** Resume ordinal per step key. */
  resumeCount?: Record<string, number>;
  createdAt?: string;
  updatedAt?: string;
}

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

/**
 * Statuses a run can never leave. ONE definition — the poller decides when to
 * stop polling and the narration layer decides when a flip is terminal from
 * the same set, so they can never disagree on "done".
 */
export const TERMINAL_RUN_STATUSES: ReadonlySet<string> = new Set([
  'success',
  'failed',
  'tripwire',
  'canceled',
  'bailed',
  'skipped',
]);

export class RunApiError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = 'RunApiError';
    this.status = status;
  }
}

export interface RunClientOptions {
  /** Default: '' — the run routes (/workflows, /runs) live at the origin root. */
  baseUrl?: string;
  /** Default: globalThis.fetch. Injected in tests and non-browser hosts. */
  fetch?: FetchLike;
  /** Sent on every request — the deployment's auth (bearer token, etc.). */
  headers?: Record<string, string>;
}

/** A run start response: the RunSummary plus, on a suspension, the queued approval. */
export interface StartRunResponse extends RunSummary {
  approval?: { id: string } & Record<string, unknown>;
}

/**
 * POST /demo/reset: the server-side sandbox wipe. `purged` carries the exact
 * delete counts, so the UI narrates verified numbers only. Admin role + demo
 * tenant enforced server-side (401/403 surface as RunApiError).
 */
export interface DemoResetResponse {
  ok: boolean;
  tenantId: string;
  purged: { snapshots: number; approvals: number; artifacts: number };
}

export class RunClient {
  readonly #baseUrl: string;
  readonly #fetch: FetchLike;
  readonly #headers: Record<string, string>;

  constructor(options: RunClientOptions = {}) {
    this.#baseUrl = options.baseUrl ?? '';
    const fetchFn =
      options.fetch ??
      ((globalThis as { fetch?: unknown }).fetch as FetchLike | undefined);
    if (!fetchFn) {
      throw new Error('RunClient: no fetch available; pass one');
    }
    this.#fetch = (url, init) => fetchFn(url, init);
    this.#headers = { ...options.headers };
  }

  async catalog(): Promise<WorkflowCatalog> {
    return (await this.#request('/workflows')) as WorkflowCatalog;
  }

  async start(
    workflowId: string,
    inputData: unknown,
  ): Promise<StartRunResponse> {
    return (await this.#post('/runs', {
      workflowId,
      inputData,
    })) as StartRunResponse;
  }

  async status(workflowId: string, runId: string): Promise<RunSummary> {
    return (await this.#request(
      `/runs/${encodeURIComponent(workflowId)}/${encodeURIComponent(runId)}`,
    )) as RunSummary;
  }

  async reset(): Promise<DemoResetResponse> {
    return (await this.#post('/demo/reset', {})) as DemoResetResponse;
  }

  async #post(path: string, body: Record<string, unknown>): Promise<unknown> {
    return this.#request(path, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body),
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
      throw new RunApiError(response.status, message);
    }
    return payload;
  }
}
