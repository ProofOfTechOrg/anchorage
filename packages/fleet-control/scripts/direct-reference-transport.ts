// SPDX-License-Identifier: Apache-2.0

import { readBoundedBody } from '@proofoftech/flowsafe/host-kit';
import type { DirectConformanceConfig } from './direct-credentialed-conformance-config.mjs';
import { DirectReferenceExecutionError } from './direct-reference-http.js';

export const DIRECT_REFERENCE_LEASE = Object.freeze({
  leaseTtlMs: 900_000,
  leaseRenewalIntervalMs: 300_000,
});

export interface DirectReferenceTransportOptions {
  readonly runtime: Pick<
    DirectConformanceConfig['referenceWorker'],
    'requestTimeoutMs' | 'invocationTimeoutMs' | 'maxProviderRequests'
  >;
  readonly startedAt: number;
  readonly signal: AbortSignal;
  readonly fetch?: typeof fetch;
}

export interface DirectReferenceTransportSnapshot {
  readonly providerAttempts: number;
  readonly maintenanceAttempts: number;
  readonly applicationAttempts: number;
  readonly effectiveRequestTimeoutMs: number;
  readonly failure: 'deadline' | 'attempts' | 'aborted' | null;
}

export class DirectReferenceTransport {
  readonly #nativeFetch: typeof fetch;
  readonly #signal: AbortSignal;
  readonly #deadlineAt: number;
  readonly #maxAttempts: number;
  readonly #abort = new AbortController();
  readonly effectiveRequestTimeoutMs: number;
  #providerAttempts = 0;
  #maintenanceAttempts = 0;
  #applicationAttempts = 0;
  #failure: DirectReferenceTransportSnapshot['failure'] = null;

  constructor(options: DirectReferenceTransportOptions) {
    const { requestTimeoutMs, invocationTimeoutMs, maxProviderRequests } =
      options.runtime;
    if (
      !Number.isFinite(options.startedAt) ||
      options.startedAt < 0 ||
      !Number.isSafeInteger(requestTimeoutMs) ||
      requestTimeoutMs < 1 ||
      requestTimeoutMs > 2_147_483_647 ||
      !Number.isSafeInteger(invocationTimeoutMs) ||
      invocationTimeoutMs < 1 ||
      invocationTimeoutMs > 2_147_483_647 ||
      !Number.isSafeInteger(maxProviderRequests) ||
      maxProviderRequests < 9 ||
      maxProviderRequests > 1000
    )
      throw new DirectReferenceExecutionError();
    const fetchFn = options.fetch ?? globalThis.fetch;
    this.#nativeFetch = (input, init) => fetchFn(input, init);
    this.#signal = options.signal;
    this.#deadlineAt = options.startedAt + invocationTimeoutMs;
    this.#maxAttempts = maxProviderRequests;
    this.effectiveRequestTimeoutMs = Math.min(
      requestTimeoutMs,
      invocationTimeoutMs,
      DIRECT_REFERENCE_LEASE.leaseTtlMs - 1,
    );
  }

  readonly providerFetch: typeof fetch = (input, init) =>
    this.#send('provider', input, init);

  readonly maintenanceFetch: typeof fetch = (input, init) =>
    this.#send('maintenance', input, init);

  readonly applicationFetch: typeof fetch = (input, init) =>
    this.#send('application', input, init);

  #fail(
    reason: NonNullable<DirectReferenceTransportSnapshot['failure']>,
  ): void {
    this.#failure ??= reason;
    this.#abort.abort();
  }

  #observeFailure(): void {
    if (performance.now() >= this.#deadlineAt) this.#fail('deadline');
    else if (this.#signal.aborted) this.#fail('aborted');
  }

  assertWithinBudget(): void {
    this.#observeFailure();
    if (this.#failure !== null)
      throw new DirectReferenceExecutionError('budget-exhausted');
  }

  snapshot(): DirectReferenceTransportSnapshot {
    this.#observeFailure();
    return Object.freeze({
      providerAttempts: this.#providerAttempts,
      maintenanceAttempts: this.#maintenanceAttempts,
      applicationAttempts: this.#applicationAttempts,
      effectiveRequestTimeoutMs: this.effectiveRequestTimeoutMs,
      failure: this.#failure,
    });
  }

  async #send(
    kind: 'provider' | 'maintenance' | 'application',
    input: Parameters<typeof fetch>[0],
    init: Parameters<typeof fetch>[1],
  ): Promise<Response> {
    this.assertWithinBudget();
    const requestInit = {
      ...init,
      redirect: 'manual' as const,
      duplex: 'half' as const,
    };
    const request = new Request(input, requestInit);
    request.signal.throwIfAborted();
    if (request.url === 'data:,') return this.#nativeFetch(request);
    const protocol = new URL(request.url).protocol;
    if (protocol !== 'https:' && protocol !== 'http:')
      throw new DirectReferenceExecutionError();
    this.assertWithinBudget();
    if (
      this.#providerAttempts +
        this.#maintenanceAttempts +
        this.#applicationAttempts >=
      this.#maxAttempts
    ) {
      this.#fail('attempts');
      this.assertWithinBudget();
    }
    const timeoutMs = Math.max(
      1,
      Math.ceil(
        Math.min(
          this.effectiveRequestTimeoutMs,
          this.#deadlineAt - performance.now(),
        ),
      ),
    );
    const signal = AbortSignal.any([
      request.signal,
      this.#signal,
      this.#abort.signal,
      AbortSignal.timeout(timeoutMs),
    ]);
    this.assertWithinBudget();
    signal.throwIfAborted();
    if (kind === 'provider') this.#providerAttempts++;
    else if (kind === 'maintenance') this.#maintenanceAttempts++;
    else this.#applicationAttempts++;
    const response = await this.#nativeFetch(request, {
      signal,
      redirect: 'manual',
    });
    if ([301, 302, 303, 307, 308].includes(response.status)) {
      void response.body?.cancel().catch(() => undefined);
      throw new DirectReferenceExecutionError();
    }
    return response;
  }
}

export interface DirectBoundedRequestOptions {
  readonly fetch: typeof fetch;
  readonly url: URL;
  readonly method: string;
  readonly token: string;
  readonly body?: unknown;
  /** Statuses the caller treats as an answer; anything else is refused. */
  readonly acceptStatuses: readonly number[];
  /** Required response media type, or `undefined` for an empty-body answer. */
  readonly mediaType?: string;
  readonly byteLimit: number;
  readonly invocationSignal: AbortSignal;
  readonly requestTimeoutMs: number;
}

/**
 * Issues one bearer-authorized request against a tenant route and reads its
 * body under `byteLimit`. A `mediaType` of `undefined` accepts an empty body,
 * which is what the 204 answers to object writes and deletes carry.
 */
export async function readBoundedDirectResponse(
  options: DirectBoundedRequestOptions,
): Promise<{ status: number; text: string }> {
  const cleanup = new AbortController();
  const signal = AbortSignal.any([
    options.invocationSignal,
    cleanup.signal,
    AbortSignal.timeout(options.requestTimeoutMs),
  ]);
  let response: Response | undefined;
  let bodySettled: Promise<void> | undefined;
  try {
    response = await options.fetch(options.url, {
      method: options.method,
      headers: {
        authorization: `Bearer ${options.token}`,
        ...(options.body === undefined
          ? {}
          : { 'content-type': 'application/json' }),
      },
      ...(options.body === undefined
        ? {}
        : { body: JSON.stringify(options.body) }),
      signal,
    });
    const media = response.headers
      .get('content-type')
      ?.split(';')[0]
      ?.trim()
      .toLowerCase();
    if (
      !options.acceptStatuses.includes(response.status) ||
      (options.mediaType !== undefined && media !== options.mediaType)
    )
      throw new DirectReferenceExecutionError();
    const stream = new TransformStream<Uint8Array, Uint8Array>();
    bodySettled = response.body
      ?.pipeTo(stream.writable, { signal })
      .catch(() => undefined);
    const bodyInit = {
      method: 'POST',
      headers: response.headers,
      body: response.body ? stream.readable : undefined,
      signal,
      duplex: 'half' as const,
    };
    const bounded = await readBoundedBody(
      new Request(options.url, bodyInit),
      options.byteLimit,
    );
    if (!bounded.ok) throw new DirectReferenceExecutionError();
    return { status: response.status, text: bounded.text };
  } finally {
    cleanup.abort();
    await bodySettled;
    if (response && !response.bodyUsed && !response.body?.locked)
      await response.body?.cancel().catch(() => undefined);
  }
}

/** Decodes a bounded body as a JSON object, refusing anything else. */
export function decodeDirectJsonObject(text: string): Record<string, unknown> {
  let decoded: unknown;
  try {
    decoded = JSON.parse(text);
  } catch {
    throw new DirectReferenceExecutionError();
  }
  if (!decoded || typeof decoded !== 'object' || Array.isArray(decoded))
    throw new DirectReferenceExecutionError();
  return decoded as Record<string, unknown>;
}
