// SPDX-License-Identifier: Apache-2.0

import {
  bearerActorAuthenticator,
  staticTokenVerifier,
} from '@proofoftech/flowsafe/host-kit';
import { APIConnectionTimeoutError } from 'cloudflare';
import {
  DIRECT_REFERENCE_PATH,
  type DirectReferenceAction,
  type DirectReferenceErrorCode,
  type DirectReferenceRequest,
  DirectReferenceRequestError,
  isDirectReferenceReadOnlyAction,
  readDirectReferenceRequest,
} from './direct-reference-contract.mjs';
import {
  type DirectInvocationLedgerState,
  type DirectJournalErrorCode,
  type DirectReferenceJournal,
  DirectReferenceJournalError,
} from './direct-reference-journal.js';

export type DirectReferenceExecutionErrorCode =
  | 'operation-refused'
  | 'wrong-operation'
  | 'missing-continuation'
  | 'injected-response-loss'
  | 'budget-exhausted';

export class DirectReferenceExecutionError extends Error {
  readonly code: DirectReferenceExecutionErrorCode;
  constructor(code: DirectReferenceExecutionErrorCode = 'operation-refused') {
    super(code);
    this.name = 'DirectReferenceExecutionError';
    this.code = code;
  }
}

export interface DirectReferenceHttpOptions {
  readonly invokeSecret: string | undefined;
  readonly configSha256: string;
  readonly invocationTimeoutMs: number;
  readonly startedAt?: number;
  readonly dispatch: (
    action: DirectReferenceAction,
    signal: AbortSignal,
  ) => Promise<unknown>;
  readonly invocationJournal: (
    signal: AbortSignal,
  ) => Promise<
    Pick<
      DirectReferenceJournal,
      'receiveInvocation' | 'settleReceivedInvocation' | 'reconcileInvocation'
    >
  >;
}

const requestStatus = {
  'invalid-request': 400,
  'payload-too-large': 413,
  'invalid-utf8': 400,
  'run-binding-mismatch': 409,
  'request-hash-mismatch': 409,
} satisfies Record<DirectReferenceErrorCode, number>;
const journalStatus = {
  'journal-state': 500,
  'run-binding-mismatch': 409,
  'duplicate-ordinal': 409,
  'operation-mismatch': 409,
  'prerequisite-unavailable': 409,
  'missing-start': 409,
} satisfies Record<DirectJournalErrorCode, number>;
const executionStatus = {
  'operation-refused': 409,
  'wrong-operation': 409,
  'missing-continuation': 409,
  'injected-response-loss': 503,
  'budget-exhausted': 503,
} satisfies Record<DirectReferenceExecutionErrorCode, number>;

function response(
  value: unknown,
  status: number,
  headers?: ConstructorParameters<typeof Headers>[0],
) {
  const outputHeaders = new Headers(headers);
  outputHeaders.set('Cache-Control', 'no-store');
  return Response.json(value, { status, headers: outputHeaders });
}

function failure(
  code: string,
  status: number,
  headers?: ConstructorParameters<typeof Headers>[0],
) {
  return response(
    { contractVersion: 2, ok: false, error: { code } },
    status,
    headers,
  );
}

function failureFromException(error: unknown): Response {
  try {
    let statuses: Readonly<Record<string, number>> | undefined;
    let code: unknown;
    if (error instanceof DirectReferenceRequestError) {
      statuses = requestStatus;
      code = error.code;
    } else if (error instanceof DirectReferenceJournalError) {
      statuses = journalStatus;
      code = error.code;
    } else if (error instanceof DirectReferenceExecutionError) {
      statuses = executionStatus;
      code = error.code;
    }
    if (statuses && typeof code === 'string' && Object.hasOwn(statuses, code)) {
      const status = statuses[code];
      if (status !== undefined) return failure(code, status);
    }
    if (error instanceof Error || error instanceof DOMException) {
      const name = error.name;
      const message = error.message;
      if (
        error instanceof APIConnectionTimeoutError ||
        (error instanceof DOMException && name === 'TimeoutError') ||
        (name === 'CloudflareAttachmentScanDriftError' &&
          message ===
            'Cloudflare attachment inventory changed during a resumable scan') ||
        (name === 'CloudflareAttachmentScanProgressError' &&
          message === 'Cloudflare attachment scan progress is malformed') ||
        (name === 'CloudflareProviderRequestNotDispatchedError' &&
          message === 'Cloudflare provider request was not dispatched') ||
        (name === 'Error' &&
          typeof message === 'string' &&
          (/^R2 returned incomplete metadata for '[a-z0-9][a-z0-9-]{1,61}[a-z0-9]'$(?![\s\S])/u.test(
            message,
          ) ||
            /^R2 bucket '[a-z0-9][a-z0-9-]{1,61}[a-z0-9]' has no valid creation date$(?![\s\S])/u.test(
              message,
            )))
      )
        return failure('operation-refused', 409);
    }
  } catch {
    // Error inspection can invoke traps on a foreign rejection.
  }
  return failure('operation-refused', 500);
}

export async function handleDirectReferenceHttpRequest(
  request: Request,
  options: DirectReferenceHttpOptions,
): Promise<Response> {
  const deadline = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;
  let deadlineAt: number | undefined;
  try {
    if (new URL(request.url).pathname !== DIRECT_REFERENCE_PATH)
      return failure('not-found', 404);
    if (request.method !== 'POST')
      return failure('method-not-allowed', 405, { Allow: 'POST' });
    const secret = options.invokeSecret;
    const authenticate = bearerActorAuthenticator(
      staticTokenVerifier(
        new Map(
          typeof secret === 'string' && secret.trim().length > 0
            ? [[secret, { id: 'direct-conformance-operator', role: 'admin' }]]
            : [],
        ),
      ),
    );
    if (!(await authenticate(request)))
      return failure('unauthorized', 401, { 'WWW-Authenticate': 'Bearer' });
    if (
      !Number.isSafeInteger(options.invocationTimeoutMs) ||
      options.invocationTimeoutMs < 1 ||
      options.invocationTimeoutMs > 2_147_483_647
    )
      return failure('operation-refused', 500);
    const startedAt = options.startedAt ?? performance.now();
    if (
      !Number.isFinite(startedAt) ||
      startedAt < 0 ||
      startedAt > performance.now()
    )
      return failure('operation-refused', 500);
    const expiresAt = startedAt + options.invocationTimeoutMs;
    deadlineAt = expiresAt;
    timer = setTimeout(
      () => deadline.abort(),
      Math.min(
        options.invocationTimeoutMs,
        Math.max(1, Math.ceil(expiresAt - performance.now())),
      ),
    );
    const signal = AbortSignal.any([request.signal, deadline.signal]);
    const assertActive = () => {
      if (performance.now() >= expiresAt) deadline.abort();
      signal.throwIfAborted();
    };
    assertActive();
    const body = request.body?.pipeThrough(new TransformStream(), { signal });
    const init = {
      method: request.method,
      headers: request.headers,
      body,
      signal,
      duplex: 'half' as const,
    };
    let abortRead!: () => void;
    const aborted = new Promise<never>((_resolve, reject) => {
      abortRead = () => reject(signal.reason);
      signal.addEventListener('abort', abortRead, { once: true });
    });
    let input: DirectReferenceRequest;
    try {
      // workerd can await source cancellation after the pipe aborts.
      input = await Promise.race([
        readDirectReferenceRequest(
          new Request(request.url, init),
          options.configSha256,
        ),
        aborted,
      ]);
    } finally {
      signal.removeEventListener('abort', abortRead);
    }
    assertActive();
    const journal = await options.invocationJournal(signal);
    assertActive();
    let result: unknown;
    if (input.action.kind === 'reconcile-invocation') {
      const state: DirectInvocationLedgerState =
        await journal.reconcileInvocation(input.action);
      result = { state };
    } else {
      const reservation = input.reservation;
      if (reservation === null)
        throw new DirectReferenceRequestError('invalid-request');
      await journal.receiveInvocation(
        reservation,
        isDirectReferenceReadOnlyAction(input.action),
      );
      assertActive();
      try {
        result = await options.dispatch(input.action, signal);
      } catch (error) {
        await journal.settleReceivedInvocation(reservation, 'failed');
        throw error;
      }
      await journal.settleReceivedInvocation(reservation, 'executed');
      assertActive();
    }
    const output = response(
      {
        contractVersion: 2,
        configSha256: options.configSha256,
        action: input.action.kind,
        ok: true,
        result,
      },
      200,
    );
    assertActive();
    return output;
  } catch (error) {
    if (deadlineAt !== undefined && performance.now() >= deadlineAt)
      deadline.abort();
    if (deadline.signal.aborted) return failure('invocation-timeout', 504);
    if (request.signal.aborted) return failure('request-aborted', 499);
    return failureFromException(error);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
    deadline.abort();
  }
}
