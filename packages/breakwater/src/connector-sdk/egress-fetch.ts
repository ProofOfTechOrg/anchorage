// SPDX-License-Identifier: Apache-2.0
// Fetch-level egress enforcement — the runtime half of the egress posture.
// The networkEgress POLICY gates what a manifest DECLARES; this guard gates
// what the connector actually REACHES: egressFetch() wraps a base fetch so
// every request — redirect hops included — must resolve to an allowed host
// before any bytes leave. createConnector() hands each execution a guard
// bound to the manifest's declared egress (ConnectorRuntime.fetch), closing
// the actual ⊆ declared ⊆ org-allowed chain.
//
// Everything here is structural on purpose: breakwater's build tsconfig is
// lib-ES2022-only (runtime-agnostic — no DOM, no @types/node, no
// workers-types), so the web fetch surface is modeled as minimal structural
// subsets, the same discipline as the D1 store seams. URL and Headers are
// runtime globals everywhere fetch exists (Workers, Node >= 18, browsers).
//
// Redirects are followed MANUALLY with a per-hop allowlist check — the whole
// point: with the platform's redirect: 'follow', an allowed host could 302
// to an arbitrary one and the response would come back as if nothing left
// the allowlist. Divergences from platform 'follow' (accepted): the final
// response's `redirected` flag stays false (each hop was a 'manual' fetch);
// a one-shot stream body cannot be re-sent across a 307/308 hop (throws
// instead of silently truncating — buffer the body or handle the 3xx
// yourself with redirect: 'manual'); and a browser's opaque redirect
// response (status 0, all a browser exposes for the internal 'manual' hop) is
// refused fail-closed rather than returned unfollowed — inert on Workers/Node,
// which return a real 3xx status the per-hop check can inspect.

import {
  assertEgressHostList,
  domainAllowed,
  normalizeDomain,
} from '../policy-engine/tool-policy.js';

/** Response headers subset the guard and its callers read. */
export interface EgressResponseHeaders {
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
  readonly status: number;
  readonly ok: boolean;
  readonly statusText: string;
  readonly url: string;
  readonly headers: EgressResponseHeaders;
  json(): Promise<unknown>;
  text(): Promise<string>;
  arrayBuffer(): Promise<ArrayBuffer>;
}

/**
 * RequestInit subset the guarded fetch understands. Members it does not
 * model (cache, credentials, cf, duplex, …) pass through to the base fetch
 * untouched via the index signature.
 */
export interface EgressRequestInit {
  method?: string;
  /**
   * 'follow' (default) follows redirects with a per-hop allowlist check.
   * 'manual' and 'error' pass straight through to the base fetch — no hop
   * happens here, so nothing escapes the initial check.
   */
  redirect?: 'follow' | 'error' | 'manual';
  headers?: unknown;
  body?: unknown;
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
 * for the initial request, n for the nth redirect. Deliberately never
 * carries the full URL — denials get audited, and query strings/paths can
 * embed secrets that must not reach a log sink.
 */
export interface EgressDenial {
  readonly host: string | null;
  readonly reason: string;
  readonly hop: number;
}

export class EgressDeniedError extends Error {
  readonly host: string | null;
  readonly hop: number;
  readonly reason: string;

  constructor(denial: EgressDenial) {
    super(`egress denied: ${denial.reason}`);
    this.name = 'EgressDeniedError';
    this.host = denial.host;
    this.hop = denial.hop;
    this.reason = denial.reason;
  }
}

export interface EgressFetchOptions {
  /** Base fetch the guard wraps (tests inject the vendor mock here).
   * Defaults to the runtime's global fetch, resolved per call. */
  fetch?: EgressFetchBase;
  /**
   * Map a denial to the error thrown to the caller — the audit seam
   * (createConnector records the denial and returns a ConnectorPolicyError
   * here). Default: `new EgressDeniedError(denial)`.
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
  const denied =
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
        host: url.hostname,
        reason: `scheme '${url.protocol}' is not http(s)`,
        hop,
      });
    }
    if (!domainAllowed(normalizeDomain(url.hostname), normalizedHosts)) {
      throw denied({
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
        // A browser's redirect: 'manual' yields an opaque status-0 response
        // (Workers/Node return a real 3xx); its Location is unreadable, so the
        // per-hop check cannot run. Fail closed rather than return an
        // unfollowed redirect. Covers the initial response and every hop.
        // Release the discarded response's body first, like the sites below.
        releaseResponse(response);
        throw new TypeError(
          'egressFetch: received an opaque redirect (status 0) whose Location cannot be read — this guard cannot verify the hop, so it fails closed; on a browser use redirect: "manual" and handle the 3xx yourself',
        );
      }
      if (!REDIRECT_STATUSES.has(response.status)) return response;
      const location = response.headers.get('location');
      if (location === null) return response;
      // Past here `response` is a redirect this loop consumes: every path below
      // either follows the hop (reassigning `response`) or throws, so the caller
      // never sees it again. Capture the one field the follow-up still needs,
      // then release its body — the hop cap, a denied Location, the one-shot
      // refusal, and the reassignment would each otherwise leak the discarded
      // 3xx's connection (the opaque status-0 refusal above is the fifth discard
      // site, released the same way at the top of the loop before its throw).
      const status = response.status;
      releaseResponse(response);
      if (hop > maxRedirects) {
        throw new TypeError(`egressFetch: exceeded ${maxRedirects} redirects`);
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
        throw new TypeError(
          'egressFetch: cannot follow a redirect that re-sends a one-shot (stream) body — buffer the body or handle the 3xx with redirect: "manual"',
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
