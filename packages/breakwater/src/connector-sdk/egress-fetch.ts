// SPDX-License-Identifier: Apache-2.0
// Structural fetch types keep the ES2022 build independent of DOM declarations.
// Manual redirects let the guard check Location before the transport follows it.

import {
  CONNECTOR_DECISIONS,
  type ConnectorDenialCode,
  type ConnectorDenialMetadata,
  captureConnectorDenialMetadata,
} from '../connector-decision.js';
import {
  assertEgressHostList,
  domainAllowed,
  normalizeDomain,
} from '../policy-engine/tool-policy.js';

/** Response headers subset the guard and its callers read. */
export interface EgressResponseHeaders {
  /** Return a header value by case-insensitive name, or `null` when absent. */
  get(name: string): string | null;
}

/**
 * Response subset a guarded fetch resolves to — members every fetch
 * implementation provides. The guard returns the base fetch's response
 * OBJECT untouched (it only reads status + the location header off
 * intermediate 3xx hops), so a connector needing more (streaming body,
 * clone) can safely cast back to its own runtime's Response type — the
 * underlying value IS that Response, nothing is wrapped or consumed.
 */
export interface EgressResponse {
  /** Numeric HTTP status code. */
  readonly status: number;
  /** Whether the status is in the successful 200–299 range. */
  readonly ok: boolean;
  /** HTTP status text supplied by the transport. */
  readonly statusText: string;
  /** Final response URL reported by the transport. */
  readonly url: string;
  /** Response headers. */
  readonly headers: EgressResponseHeaders;
  /** Parse the response body as JSON. */
  json(): Promise<unknown>;
  /** Read the response body as text. */
  text(): Promise<string>;
  /** Read the response body as an `ArrayBuffer`. */
  arrayBuffer(): Promise<ArrayBuffer>;
}

/**
 * RequestInit subset the guarded fetch understands. Members it does not
 * model (cache, credentials, cf, duplex, …) pass through to the base fetch
 * untouched via the index signature.
 */
export interface EgressRequestInit {
  /** HTTP method. Defaults to the base fetch implementation's default. */
  method?: string;
  /**
   * 'follow' (default) follows redirects with a per-hop allowlist check.
   * 'manual' and 'error' pass straight through to the base fetch — no hop
   * happens here, so nothing escapes the initial check.
   */
  redirect?: 'follow' | 'error' | 'manual';
  /** Header initializer forwarded to the base fetch. */
  headers?: unknown;
  /** Request body forwarded to the base fetch. */
  body?: unknown;
  /** Cancellation signal forwarded to the base fetch. */
  signal?: unknown;
  [key: string]: unknown;
}

/**
 * The guarded fetch. Takes a URL string or URL object — never a Request
 * object, so the guard sees every request whole (a Request smuggles url,
 * body, and redirect state the wrapper would have to re-derive).
 */
export type EgressGuardedFetch = (
  input: string | { readonly href: string },
  init?: EgressRequestInit,
) => Promise<EgressResponse>;

/**
 * Any fetch-shaped function — the runtime global, a vendor mock, an
 * instrumented wrapper. The `never[]` parameters make every fetch signature
 * assignable (parameters are contravariant); the runtime contract is
 * `(url: string, init?: object) => Promise<Response-shaped>`.
 */
export type EgressFetchBase = (...args: never[]) => Promise<unknown>;

/**
 * One denied request. `host` is null when the URL never parsed; `hop` is 0
 * for the initial request, n for the nth redirect. Paths and query strings
 * can contain secrets, so diagnostic fields use the hostname.
 */
export interface EgressDenial {
  /** Stable request refusal code; omitted by legacy manual construction. */
  readonly code?: Exclude<
    Extract<ConnectorDenialCode, `EGRESS_${string}`>,
    'EGRESS_HOST_NOT_ALLOWED_BY_ORG'
  >;
  /** Denied hostname, or `null` when the URL was invalid. */
  readonly host: string | null;
  /** Safe explanation that excludes the path and query string. */
  readonly reason: string;
  /** Zero for the initial request, or the one-based redirect hop number. */
  readonly hop: number;
}

type AuthoredEgressDenial = EgressDenial & {
  readonly code: NonNullable<EgressDenial['code']>;
};

type EgressDecisionMetadata = Extract<
  ConnectorDenialMetadata,
  { code: NonNullable<EgressDenial['code']> }
>;

/** Code-specific metadata for an operational redirect refusal. */
export type EgressGuardMetadata = Extract<
  ConnectorDenialMetadata,
  {
    code:
      | 'EGRESS_REDIRECT_UNVERIFIABLE'
      | 'EGRESS_REDIRECT_LIMIT_EXCEEDED'
      | 'EGRESS_REDIRECT_BODY_UNREPLAYABLE';
  }
>;

/** Error thrown when {@link egressFetch} refuses a request. */
export class EgressDeniedError extends Error {
  /** Structural discriminator for a request denial. */
  readonly kind = 'egress-denied';
  /** Stable refusal code. */
  readonly code: EgressDecisionMetadata['code'];
  /** Canonical policy category independent of diagnostic names. */
  readonly policyKind: 'egress-fetch';
  /** Whether the unchanged logical operation may be retried. */
  readonly retryable: false;
  /** Copied safe decision fields, excluding request contents. */
  readonly details: EgressDecisionMetadata['details'];
  /** Denied hostname, or `null` when the URL was invalid. */
  readonly host: string | null;
  /** Zero for the initial request, or the one-based redirect hop number. */
  readonly hop: number;
  /** Safe explanation that excludes the path and query string. */
  readonly reason: string;

  constructor(denial: EgressDenial) {
    super(`egress denied: ${denial.reason}`);
    const metadata = captureConnectorDenialMetadata({
      code: denial.code ?? 'EGRESS_DENIED',
      details: {
        host: denial.host === null ? null : normalizeDomain(denial.host),
        hop: denial.hop,
      },
    });
    this.name = 'EgressDeniedError';
    this.code = metadata.code;
    this.policyKind = CONNECTOR_DECISIONS[this.code].policyKind;
    this.retryable = CONNECTOR_DECISIONS[this.code].retryable;
    this.details = metadata.details;
    this.host = denial.host;
    this.hop = denial.hop;
    this.reason = denial.reason;
  }
}

/** A redirect refusal that retains the guard's TypeError boundary. */
export class EgressGuardError extends TypeError {
  /** Structural discriminator for an operational redirect refusal. */
  readonly kind = 'egress-guard';
  /** Stable refusal code. */
  readonly code: EgressGuardMetadata['code'];
  /** Canonical policy category independent of diagnostic names. */
  readonly policyKind: 'egress-fetch';
  /** Whether the unchanged logical operation may be retried. */
  readonly retryable: false;
  /** Copied safe decision fields, excluding request contents. */
  readonly details: EgressGuardMetadata['details'];

  constructor(message: string, metadata: EgressGuardMetadata) {
    super(message);
    const captured = captureConnectorDenialMetadata(metadata);
    this.name = 'EgressGuardError';
    this.code = captured.code;
    this.policyKind = CONNECTOR_DECISIONS[this.code].policyKind;
    this.retryable = CONNECTOR_DECISIONS[this.code].retryable;
    this.details = captured.details;
  }
}

/** Configuration for {@link egressFetch}. */
export interface EgressFetchOptions {
  /**
   * Base fetch the guard wraps. Defaults to the runtime's global fetch,
   * resolved per call.
   */
  fetch?: EgressFetchBase;
  /**
   * Map a request denial to a caller-owned error.
   * Default: `new EgressDeniedError(denial)`.
   */
  denied?: (denial: EgressDenial) => Error;
  /** Redirect hops followed before throwing a TypeError (default 20, the
   * fetch spec's cap). */
  maxRedirects?: number;
}

// Internal view of the base fetch: what the guard actually sends and the
// members it actually reads (status + headers.get on redirect statuses).
// The seam cast from EgressFetchBase is confined to construction.
type InternalFetch = (
  input: string,
  init?: Record<string, unknown>,
) => Promise<EgressResponse>;

interface UrlLike {
  readonly protocol: string;
  readonly hostname: string;
  readonly origin: string;
  readonly href: string;
}

type UrlConstructor = new (input: string, base?: string) => UrlLike;

interface HeadersLike {
  get(name: string): string | null;
  delete(name: string): void;
}

type HeadersConstructor = new (init?: unknown) => HeadersLike;

function requireGlobal<T>(name: 'URL' | 'Headers'): T {
  const ctor = (globalThis as Record<string, unknown>)[name];
  if (typeof ctor !== 'function') {
    throw new Error(
      `egressFetch requires the ${name} global (Workers, Node >= 18, or a browser)`,
    );
  }
  return ctor as T;
}

const REDIRECT_STATUSES = new Set([301, 302, 303, 307, 308]);

// Fetch spec: 303 rewrites every non-HEAD method to GET; 301/302 rewrite
// only POST. 307/308 always preserve the method.
function redirectMethod(status: number, method: string): string {
  if (status === 303) return method === 'HEAD' ? 'HEAD' : 'GET';
  if ((status === 301 || status === 302) && method === 'POST') return 'GET';
  return method;
}

// Headers that describe a body, stripped when a redirect drops it.
const BODY_HEADER_NAMES = [
  'content-encoding',
  'content-language',
  'content-length',
  'content-location',
  'content-type',
  'transfer-encoding',
] as const;

// The fetch spec strips credentials on cross-ORIGIN hops; a manual follower
// that forgot this would leak Authorization to whatever host the allowed one
// redirected to.
const CREDENTIAL_HEADER_NAMES = [
  'authorization',
  'cookie',
  'proxy-authorization',
] as const;

// A one-shot body — a ReadableStream (getReader) or a Node Readable /
// async-iterable (Symbol.asyncIterator) — can be sent exactly once; re-sending
// it across a 307/308 hop would silently transmit an empty body. Buffered
// bodies (string, ArrayBuffer, TypedArray, URLSearchParams, FormData, Blob)
// carry neither and are not misclassified.
function isOneShotBody(body: unknown): boolean {
  if (typeof body !== 'object' || body === null) return false;
  const candidate = body as {
    getReader?: unknown;
    [Symbol.asyncIterator]?: unknown;
  };
  return (
    typeof candidate.getReader === 'function' ||
    typeof candidate[Symbol.asyncIterator] === 'function'
  );
}

// Release a redirect response the loop is discarding. Manual following reads
// only status + the Location header off each 3xx, then follows it or throws —
// either way the caller never sees that response again, but its body is a live
// stream over a connection (Node/Undici, workerd). Left unconsumed it keeps the
// connection checked out until GC finalizes the stream, so sustained redirected
// traffic can exhaust the pool and an unbounded 3xx body can stay live
// indefinitely. cancel() releases the connection WITHOUT draining the body.
// EgressResponse omits `body` (its consumers read only status + headers), so
// the stream is reached structurally — the same discipline as isOneShotBody.
// cancel() is best-effort and guarded on both axes: a conformant stream
// cancel() rejects when locked/errored (swallowed via .catch), and an injected
// vendor body (the policies.fetch transport seam) could supply a cancel() that
// throws synchronously (swallowed via try/catch) — so disposal never masks the
// real result or a propagating throw, nor surfaces as an unhandled rejection.
function releaseResponse(response: EgressResponse): void {
  const body = (response as { readonly body?: unknown }).body;
  if (
    typeof body !== 'object' ||
    body === null ||
    typeof (body as { cancel?: unknown }).cancel !== 'function'
  ) {
    return;
  }
  try {
    Promise.resolve((body as { cancel(): unknown }).cancel()).catch(() => {});
  } catch {
    // cancel() threw synchronously; disposal is best-effort — nothing to do.
  }
}

/**
 * Wrap a fetch so every request — redirect hops included — must resolve to
 * an allowed host (exact or `*.wildcard`, the same matcher as the
 * networkEgress policy) over http(s), or the call throws before the base
 * fetch is invoked. An empty allowlist denies everything: no declared
 * egress means no network.
 */
export function egressFetch(
  allowedHosts: readonly string[],
  options: EgressFetchOptions = {},
): EgressGuardedFetch {
  assertEgressHostList(
    allowedHosts,
    (entry) =>
      `egressFetch: allowed host '${entry}' must be a bare hostname ('api.example.com') or wildcard ('*.example.com')`,
  );
  // Normalize the allowlist ONCE per guard; only the incoming host is
  // normalized per hop (checkUrl), via the same matcher the declaration gate
  // uses.
  const normalizedHosts = allowedHosts.map(normalizeDomain);
  const UrlCtor = requireGlobal<UrlConstructor>('URL');
  const denied: (denial: AuthoredEgressDenial) => Error =
    options.denied ?? ((denial: EgressDenial) => new EgressDeniedError(denial));
  // A non-negative integer, symmetric with the allowlist validation above:
  // NaN/negative/fractional would make `hop > maxRedirects` never fire and an
  // allowed self-redirect loop unbounded. 0 stays valid (refuse all redirects).
  if (
    options.maxRedirects !== undefined &&
    !(Number.isInteger(options.maxRedirects) && options.maxRedirects >= 0)
  ) {
    throw new TypeError(
      `egressFetch: maxRedirects must be a non-negative integer (got ${options.maxRedirects})`,
    );
  }
  const maxRedirects = options.maxRedirects ?? 20;
  const base: InternalFetch = options.fetch
    ? (options.fetch as InternalFetch)
    : (input, init) => {
        const globalFetch = (globalThis as { fetch?: InternalFetch }).fetch;
        if (typeof globalFetch !== 'function') {
          throw new Error(
            'egressFetch: the runtime has no global fetch and none was injected',
          );
        }
        return globalFetch(input, init);
      };

  function checkUrl(raw: string, hop: number, from?: UrlLike): UrlLike {
    let url: UrlLike;
    try {
      url = from ? new UrlCtor(raw, from.href) : new UrlCtor(raw);
    } catch {
      throw denied({
        code: hop === 0 ? 'EGRESS_URL_INVALID' : 'EGRESS_REDIRECT_URL_INVALID',
        host: null,
        reason:
          hop === 0
            ? 'request URL is not an absolute, parseable URL'
            : 'redirect Location is not a parseable URL',
        hop,
      });
    }
    if (url.protocol !== 'http:' && url.protocol !== 'https:') {
      throw denied({
        code:
          hop === 0
            ? 'EGRESS_SCHEME_NOT_ALLOWED'
            : 'EGRESS_REDIRECT_SCHEME_NOT_ALLOWED',
        host: url.hostname,
        reason: `scheme '${url.protocol}' is not http(s)`,
        hop,
      });
    }
    if (!domainAllowed(normalizeDomain(url.hostname), normalizedHosts)) {
      throw denied({
        code:
          hop === 0
            ? 'EGRESS_HOST_NOT_DECLARED'
            : 'EGRESS_REDIRECT_HOST_DENIED',
        host: url.hostname,
        reason: `host '${url.hostname}' is not in the allowed egress hosts`,
        hop,
      });
    }
    return url;
  }

  return async function guardedFetch(input, init) {
    const raw =
      typeof input === 'string'
        ? input
        : typeof input?.href === 'string'
          ? input.href
          : undefined;
    if (raw === undefined) {
      throw denied({
        code: 'EGRESS_INPUT_INVALID',
        host: null,
        reason:
          'input must be a URL string or URL object — pass (url, init), not a Request',
        hop: 0,
      });
    }
    let url = checkUrl(raw, 0);
    if (init?.redirect !== undefined && init.redirect !== 'follow') {
      // 'manual' hands the 3xx back to the caller (any follow-up fetch goes
      // through this guard again); 'error' fails on it at the base.
      return base(url.href, init);
    }

    let method = (init?.method ?? 'GET').toUpperCase();
    let body = init?.body ?? null;
    let headers: HeadersLike | undefined; // built on the first hop only
    let response = await base(url.href, {
      ...(init ?? {}),
      redirect: 'manual',
    });

    for (let hop = 1; ; hop++) {
      if (response.status === 0) {
        releaseResponse(response);
        throw new EgressGuardError(
          'egressFetch: received an opaque redirect (status 0) whose Location cannot be read — this guard cannot verify the hop, so it fails closed; on a browser use redirect: "manual" and handle the 3xx yourself',
          {
            code: 'EGRESS_REDIRECT_UNVERIFIABLE',
            details: { host: normalizeDomain(url.hostname), hop },
          },
        );
      }
      if (!REDIRECT_STATUSES.has(response.status)) return response;
      const location = response.headers.get('location');
      if (location === null) return response;
      const status = response.status;
      releaseResponse(response);
      if (hop > maxRedirects) {
        throw new EgressGuardError(
          `egressFetch: exceeded ${maxRedirects} redirects`,
          {
            code: 'EGRESS_REDIRECT_LIMIT_EXCEEDED',
            details: { host: normalizeDomain(url.hostname), hop },
          },
        );
      }
      const nextUrl = checkUrl(location, hop, url);
      headers ??= new (requireGlobal<HeadersConstructor>('Headers'))(
        init?.headers,
      );
      const nextMethod = redirectMethod(status, method);
      if (nextMethod !== method) {
        body = null;
        for (const name of BODY_HEADER_NAMES) headers.delete(name);
      } else if (body !== null && isOneShotBody(body)) {
        throw new EgressGuardError(
          'egressFetch: cannot follow a redirect that re-sends a one-shot (stream) body — buffer the body or handle the 3xx with redirect: "manual"',
          {
            code: 'EGRESS_REDIRECT_BODY_UNREPLAYABLE',
            details: { host: normalizeDomain(nextUrl.hostname), hop },
          },
        );
      }
      if (nextUrl.origin !== url.origin) {
        for (const name of CREDENTIAL_HEADER_NAMES) headers.delete(name);
      }
      method = nextMethod;
      url = nextUrl;
      response = await base(url.href, {
        ...(init ?? {}),
        method,
        headers,
        body,
        redirect: 'manual',
      });
    }
  };
}
