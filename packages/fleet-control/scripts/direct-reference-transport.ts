// SPDX-License-Identifier: Apache-2.0

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
      effectiveRequestTimeoutMs: this.effectiveRequestTimeoutMs,
      failure: this.#failure,
    });
  }

  async #send(
    kind: 'provider' | 'maintenance',
    input: Parameters<typeof fetch>[0],
    init: Parameters<typeof fetch>[1],
  ): Promise<Response> {
    this.assertWithinBudget();
    const requestInit = { ...init, duplex: 'half' as const };
    const request = new Request(input, requestInit);
    request.signal.throwIfAborted();
    if (request.url === 'data:,') return this.#nativeFetch(request);
    const protocol = new URL(request.url).protocol;
    if (protocol !== 'https:' && protocol !== 'http:')
      throw new DirectReferenceExecutionError();
    this.assertWithinBudget();
    if (
      this.#providerAttempts + this.#maintenanceAttempts >=
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
    else this.#maintenanceAttempts++;
    return this.#nativeFetch(request, { signal, redirect: 'error' });
  }
}
