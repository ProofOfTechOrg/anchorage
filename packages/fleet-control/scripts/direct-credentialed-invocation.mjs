// SPDX-License-Identifier: Apache-2.0

import { validateHeaderValue } from 'node:http';
import https from 'node:https';
import { Readable } from 'node:stream';
import { readBoundedBody } from '@proofoftech/flowsafe/host-kit';
import { cancelBodyWithoutAwait } from './direct-credentialed-body-cancel.mjs';
import {
  deriveDirectConformanceNames,
  validateDirectConformanceConfig,
} from './direct-credentialed-conformance-config.mjs';
import { DirectRunStateError } from './direct-credentialed-run-state.mjs';
import {
  DIRECT_REFERENCE_PATH,
  isDirectReferenceReadOnlyAction,
  serializeDirectReferenceCore,
} from './direct-reference-contract.mjs';

const RESPONSE_BYTE_LIMIT = 4 * 1024 * 1024;
const JSON_CONTENT_TYPE = /^application\/json(?:;\s*charset=utf-8)?$/iu;
// workers.dev enablement propagates asynchronously.
const INGRESS_DEADLINE_MS = 120_000;
const INGRESS_INTERVAL_MS = 2_000;
/**
 * Measured route flaps span at most 6 s after enablement and 3 s after the
 * first contract answer; consecutive probes span the latter window.
 */
const INGRESS_STABLE_PROBES = 3;
// The Worker aborts each request at invocationTimeoutMs. This margin lets its
// terminal ledger write and response reach the client before polling expires.
export const DIRECT_RECONCILIATION_MARGIN_MS = 5_000;
export const DIRECT_RECONCILIATION_INTERVAL_MS = 250;
export const DIRECT_RECONCILIATION_MAX_INTERVAL_MS = 32_000;
// The cap guards a shorter interval override. At the production interval the
// time bound ends polling first.
export const DIRECT_RECONCILIATION_MAX_REQUESTS = 32;
export const DIRECT_INVOCATION_FAILURE_DETAILS = Object.freeze([
  'platform-page',
  'transport-failure',
  'non-contract-answer',
  'delivery-window-expired',
]);
const ERROR_CODES = new Set([
  'invalid-input',
  'invocation-busy',
  'invocation-budget-exhausted',
  'outcome-unknown',
  'injected-response-loss',
  'reference-refused',
]);
const REFERENCE_REFUSAL_STATUS = {
  'run-binding-mismatch': 409,
  'operation-mismatch': 409,
  'prerequisite-unavailable': 409,
  'missing-start': 409,
  'operation-refused': 409,
  'wrong-operation': 409,
  'missing-continuation': 409,
  'duplicate-ordinal': 409,
  'budget-exhausted': 503,
};

export class DirectInvocationError extends Error {
  constructor(code = 'invalid-input', attempts, referenceCode, detail) {
    const accepted = ERROR_CODES.has(code) ? code : 'invalid-input';
    super(accepted);
    this.name = 'DirectInvocationError';
    this.code = accepted;
    this.attempts = attempts;
    this.referenceCode = referenceCode;
    this.detail = DIRECT_INVOCATION_FAILURE_DETAILS.includes(detail)
      ? detail
      : undefined;
  }
}

function invalid() {
  throw new DirectInvocationError();
}

function unknown(detail) {
  throw new DirectInvocationError(
    'outcome-unknown',
    undefined,
    undefined,
    detail,
  );
}

function exactKeys(value, keys) {
  return (
    value !== null &&
    typeof value === 'object' &&
    !Array.isArray(value) &&
    Object.keys(value).length === keys.length &&
    keys.every((key) => Object.hasOwn(value, key))
  );
}

function reservationError(error) {
  try {
    if (error instanceof DirectRunStateError) {
      if (error.code === 'outcome-unknown')
        return new DirectInvocationError('outcome-unknown');
      if (error.code === 'invocation-budget-exhausted')
        return new DirectInvocationError('invocation-budget-exhausted');
    }
  } catch {
    // Foreign rejection inspection can invoke traps.
  }
  return new DirectInvocationError();
}

function readAttempts(headers, maxAttempts) {
  const attempts = {};
  for (const kind of ['provider', 'maintenance', 'application']) {
    const value = headers.get(`X-Direct-${kind}-Attempts`);
    if (value === null || !/^(?:0|[1-9][0-9]*)$/u.test(value))
      unknown('non-contract-answer');
    const count = Number(value);
    if (!Number.isSafeInteger(count) || count > maxAttempts)
      unknown('non-contract-answer');
    attempts[kind] = count;
  }
  if (
    attempts.provider + attempts.maintenance + attempts.application >
    maxAttempts
  )
    unknown('non-contract-answer');
  return Object.freeze(attempts);
}

export function resolveDirectReferenceEndpoint(prepared, subdomain) {
  const config = validateDirectConformanceConfig(prepared.config);
  const names = deriveDirectConformanceNames(config);
  if (
    typeof subdomain !== 'string' ||
    !/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/u.test(subdomain) ||
    prepared.names.referenceWorker !== names.referenceWorker
  )
    invalid();
  return {
    config,
    endpoint: `https://${names.referenceWorker}.${subdomain}.workers.dev${DIRECT_REFERENCE_PATH}`,
  };
}

export async function awaitReferenceIngress(input) {
  let endpoint;
  let fetchRequest;
  let deadlineMs;
  let intervalMs;
  let sleep;
  try {
    ({ endpoint } = resolveDirectReferenceEndpoint(
      input.prepared,
      input.accountWorkersDevSubdomain,
    ));
    fetchRequest = input.fetch ?? globalThis.fetch;
    deadlineMs = input.deadlineMs ?? INGRESS_DEADLINE_MS;
    intervalMs = input.intervalMs ?? INGRESS_INTERVAL_MS;
    sleep =
      input.sleep ??
      ((ms) => new Promise((resolve) => setTimeout(resolve, ms)));
    if (
      typeof fetchRequest !== 'function' ||
      typeof sleep !== 'function' ||
      !Number.isSafeInteger(deadlineMs) ||
      deadlineMs < 1 ||
      deadlineMs > 2_147_483_647 ||
      !Number.isSafeInteger(intervalMs) ||
      intervalMs < 1 ||
      intervalMs > 2_147_483_647
    )
      invalid();
  } catch {
    invalid();
  }
  const expiresAt = performance.now() + deadlineMs;
  const deadline = new AbortController();
  const signal = deadline.signal;
  let expire;
  const expired = new Promise((resolve) => {
    expire = () => {
      deadline.abort();
      resolve(false);
    };
  });
  const timer = setTimeout(expire, deadlineMs);
  const active = () => !signal.aborted && performance.now() < expiresAt;
  let consecutive = 0;
  try {
    while (active()) {
      const exchange = (async () => {
        let response;
        try {
          response = await fetchRequest(endpoint, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              Accept: 'application/json',
              'Cache-Control': 'no-store',
            },
            body: '{}',
            cache: 'no-store',
            redirect: 'manual',
            signal,
          });
          if (
            !active() ||
            response.redirected ||
            response.status !== 401 ||
            response.headers.get('cache-control') !== 'no-store' ||
            response.headers.get('www-authenticate') !== 'Bearer' ||
            !JSON_CONTENT_TYPE.test(response.headers.get('content-type') ?? '')
          )
            return false;
          const body = response.body?.pipeThrough(new TransformStream(), {
            signal,
          });
          const bounded = await readBoundedBody(
            new Request(endpoint, {
              method: 'POST',
              headers: response.headers,
              body,
              signal,
              duplex: 'half',
            }),
            RESPONSE_BYTE_LIMIT,
          );
          return (
            active() &&
            bounded.ok &&
            bounded.text ===
              '{"contractVersion":2,"ok":false,"error":{"code":"unauthorized"}}'
          );
        } catch {
          return false;
        } finally {
          cancelBodyWithoutAwait(response?.body);
        }
      })();
      consecutive = (await Promise.race([exchange, expired]))
        ? consecutive + 1
        : 0;
      if (active() && consecutive >= INGRESS_STABLE_PROBES) return true;
      if (!active()) return false;
      if (performance.now() + intervalMs >= expiresAt) {
        await expired;
        return false;
      }
      await Promise.race([sleep(intervalMs), expired]);
    }
    return false;
  } finally {
    clearTimeout(timer);
    deadline.abort();
  }
}

function requestReference(endpoint, { method, headers, body, signal }) {
  return new Promise((resolve, reject) => {
    const request = https.request(
      endpoint,
      { method, headers, signal, timeout: 0, agent: false },
      (incoming) => {
        try {
          const responseHeaders = new Headers();
          for (let index = 0; index < incoming.rawHeaders.length; index += 2)
            responseHeaders.append(
              incoming.rawHeaders[index],
              incoming.rawHeaders[index + 1],
            );
          resolve(
            new Response(Readable.toWeb(incoming), {
              status: incoming.statusCode,
              headers: responseHeaders,
            }),
          );
        } catch (error) {
          incoming.destroy();
          reject(error);
        }
      },
    );
    request.on('error', reject);
    request.end(body);
  });
}

export async function reconcileDirectInvocation(input) {
  let endpoint;
  let authorization;
  let configSha256;
  let ordinal;
  let requestSha256;
  let workerDeadlineMs;
  let deadlineMs;
  let intervalMs;
  let fetchRequest;
  let sleep;
  try {
    endpoint = new URL(input.endpoint);
    authorization = `Bearer ${input.secret}`;
    configSha256 = input.configSha256;
    ordinal = input.ordinal;
    requestSha256 = input.requestSha256;
    workerDeadlineMs = input.workerDeadlineMs;
    deadlineMs =
      input.deadlineMs ?? workerDeadlineMs + DIRECT_RECONCILIATION_MARGIN_MS;
    intervalMs = input.intervalMs ?? DIRECT_RECONCILIATION_INTERVAL_MS;
    fetchRequest = input.fetch ?? requestReference;
    sleep =
      input.sleep ??
      ((ms) => new Promise((resolve) => setTimeout(resolve, ms)));
    if (
      endpoint.protocol !== 'https:' ||
      endpoint.username ||
      endpoint.password ||
      endpoint.pathname !== DIRECT_REFERENCE_PATH ||
      endpoint.search ||
      endpoint.hash ||
      typeof input.secret !== 'string' ||
      !input.secret ||
      input.secret !== input.secret.trim() ||
      typeof configSha256 !== 'string' ||
      !/^[a-f0-9]{64}$/u.test(configSha256) ||
      !Number.isSafeInteger(ordinal) ||
      ordinal < 1 ||
      typeof requestSha256 !== 'string' ||
      !/^[a-f0-9]{64}$/u.test(requestSha256) ||
      !Number.isSafeInteger(workerDeadlineMs) ||
      workerDeadlineMs < 1 ||
      !Number.isSafeInteger(deadlineMs) ||
      deadlineMs < workerDeadlineMs ||
      deadlineMs > 2_147_483_647 ||
      !Number.isSafeInteger(intervalMs) ||
      intervalMs < 1 ||
      intervalMs > DIRECT_RECONCILIATION_MAX_INTERVAL_MS ||
      intervalMs >= deadlineMs ||
      typeof fetchRequest !== 'function' ||
      typeof sleep !== 'function'
    )
      invalid();
    validateHeaderValue('Authorization', authorization);
  } catch {
    invalid();
  }
  const core = {
    contractVersion: 2,
    configSha256,
    action: { kind: 'reconcile-invocation', ordinal, requestSha256 },
  };
  const serialized = JSON.stringify({ ...core, reservation: null });
  const deadline = new AbortController();
  const signal = deadline.signal;
  const expiresAt = performance.now() + deadlineMs;
  const pollBy = expiresAt - (deadlineMs - workerDeadlineMs);
  const timer = setTimeout(() => deadline.abort(), deadlineMs);
  let requestsSent = 0;
  let waitMs = intervalMs;
  try {
    for (;;) {
      if (signal.aborted || performance.now() >= expiresAt)
        return 'unreachable';
      let response;
      try {
        requestsSent += 1;
        response = await fetchRequest(endpoint, {
          method: 'POST',
          headers: {
            Authorization: authorization,
            'Content-Type': 'application/json',
            Accept: 'application/json',
            'Cache-Control': 'no-store',
          },
          body: serialized,
          cache: 'no-store',
          redirect: 'manual',
          signal,
        });
        if (
          response.redirected ||
          response.status !== 200 ||
          response.headers.get('cache-control') !== 'no-store' ||
          !JSON_CONTENT_TYPE.test(response.headers.get('content-type') ?? '')
        )
          return 'unreachable';
        const body = response.body?.pipeThrough(new TransformStream(), {
          signal,
        });
        const bounded = await readBoundedBody(
          new Request(endpoint, {
            method: 'POST',
            headers: response.headers,
            body,
            signal,
            duplex: 'half',
          }),
          RESPONSE_BYTE_LIMIT,
        );
        if (!bounded.ok) return 'unreachable';
        const value = JSON.parse(bounded.text);
        if (
          !exactKeys(value, [
            'contractVersion',
            'configSha256',
            'action',
            'ok',
            'result',
          ]) ||
          value.contractVersion !== 2 ||
          value.configSha256 !== configSha256 ||
          value.action !== 'reconcile-invocation' ||
          value.ok !== true ||
          !exactKeys(value.result, ['state']) ||
          !['received', 'executed', 'failed', 'cancelled'].includes(
            value.result.state,
          )
        )
          return 'unreachable';
        if (value.result.state !== 'received') return value.result.state;
      } catch {
        return 'unreachable';
      } finally {
        cancelBodyWithoutAwait(response?.body);
      }
      if (requestsSent >= DIRECT_RECONCILIATION_MAX_REQUESTS)
        return 'unreachable';
      const remaining = pollBy - performance.now();
      if (remaining <= 0) return 'unreachable';
      const sleepMs = Math.min(waitMs, remaining);
      try {
        await new Promise((resolve, reject) => {
          const abort = () => reject(signal.reason);
          signal.addEventListener('abort', abort, { once: true });
          Promise.resolve(sleep(sleepMs)).then(
            (value) => {
              signal.removeEventListener('abort', abort);
              resolve(value);
            },
            (error) => {
              signal.removeEventListener('abort', abort);
              reject(error);
            },
          );
        });
      } catch {
        return 'unreachable';
      }
      waitMs = Math.min(waitMs * 2, DIRECT_RECONCILIATION_MAX_INTERVAL_MS);
    }
  } finally {
    clearTimeout(timer);
    deadline.abort();
  }
}

export function createDirectInvocationClient(input) {
  let endpoint;
  let configSha256;
  let invocationTimeoutMs;
  let maxAttempts;
  let journal;
  let fetchRequest;
  let authorization;
  try {
    const prepared = input.prepared;
    const resolved = resolveDirectReferenceEndpoint(
      prepared,
      input.accountWorkersDevSubdomain,
    );
    const config = resolved.config;
    endpoint = resolved.endpoint;
    const secret = input.invokeSecret;
    journal = input.journal;
    fetchRequest = input.fetch ?? requestReference;
    configSha256 = prepared.configSha256;
    invocationTimeoutMs = config.referenceWorker.invocationTimeoutMs;
    maxAttempts = config.referenceWorker.maxProviderRequests;
    if (
      typeof secret !== 'string' ||
      !secret ||
      secret !== secret.trim() ||
      typeof fetchRequest !== 'function' ||
      typeof journal.reserveInvocation !== 'function' ||
      typeof journal.settleInvocation !== 'function' ||
      typeof configSha256 !== 'string' ||
      !/^[a-f0-9]{64}$/u.test(configSha256) ||
      typeof prepared.referenceModuleSetSha256 !== 'string' ||
      !/^[a-f0-9]{64}$/u.test(prepared.referenceModuleSetSha256)
    )
      invalid();
    const binding = journal.snapshot().binding;
    if (
      binding.configSha256 !== configSha256 ||
      binding.referenceModuleSetSha256 !== prepared.referenceModuleSetSha256 ||
      binding.resourcePrefix !== config.resourcePrefix ||
      binding.maxInvocations !== config.referenceWorker.maxInvocations
    )
      invalid();
    authorization = `Bearer ${secret}`;
    validateHeaderValue('Authorization', authorization);
    if (
      new Headers({ Authorization: authorization }).get('authorization') !==
      authorization
    )
      invalid();
  } catch {
    invalid();
  }

  let busy = false;
  let uncertain = false;
  return Object.freeze({
    async invoke(action) {
      if (busy) throw new DirectInvocationError('invocation-busy');
      if (uncertain) unknown();
      busy = true;
      let timer;
      let deliveryTimer;
      let retryTimer;
      let abortRead;
      let response;
      const deadline = new AbortController();
      const signal = deadline.signal;
      try {
        let serialized;
        let reservation;
        try {
          const core = {
            contractVersion: 2,
            configSha256,
            action,
          };
          serialized = serializeDirectReferenceCore(core);
          reservation = await journal.reserveInvocation(serialized);
        } catch (error) {
          throw reservationError(error);
        }
        uncertain = true;
        const serializedAction = JSON.parse(serialized).action;
        const readOnly = isDirectReferenceReadOnlyAction(serializedAction);
        const transmitted = JSON.stringify({
          ...JSON.parse(serialized),
          reservation,
        });
        let deliveryPending = true;
        let redelivering = false;
        let answerDetail = 'transport-failure';
        const startedAt = performance.now();
        const expiresAt = startedAt + invocationTimeoutMs;
        const deliveryExpiresAt = Math.min(
          expiresAt,
          startedAt + INGRESS_DEADLINE_MS,
        );
        const assertActive = () => {
          const now = performance.now();
          if (now >= expiresAt || (redelivering && now >= deliveryExpiresAt))
            deadline.abort();
          signal.throwIfAborted();
        };
        const abortDetail = () =>
          deliveryPending
            ? readOnly &&
              redelivering &&
              (signal.aborted || performance.now() >= deliveryExpiresAt)
              ? 'delivery-window-expired'
              : answerDetail
            : undefined;
        const aborted = new Promise((_, reject) => {
          abortRead = () =>
            reject(
              new DirectInvocationError(
                'outcome-unknown',
                undefined,
                undefined,
                abortDetail(),
              ),
            );
          signal.addEventListener('abort', abortRead, { once: true });
        });
        timer = setTimeout(() => deadline.abort(), invocationTimeoutMs);
        let outcome;
        try {
          const actionKind = serializedAction.kind;
          const exchange = (async () => {
            try {
              assertActive();
              const request = {
                method: 'POST',
                headers: {
                  Authorization: authorization,
                  'Content-Type': 'application/json',
                  Accept: 'application/json',
                  'Cache-Control': 'no-store',
                },
                body: transmitted,
                cache: 'no-store',
                redirect: 'manual',
                signal,
              };
              for (;;) {
                let platformPage = false;
                try {
                  assertActive();
                  answerDetail = 'transport-failure';
                  response = await fetchRequest(endpoint, request);
                  assertActive();
                  answerDetail = 'non-contract-answer';
                  const mediaType = response.headers
                    .get('content-type')
                    ?.split(';')[0]
                    ?.trim()
                    .toLowerCase();
                  platformPage =
                    !response.redirected &&
                    (mediaType === 'text/plain' || mediaType === 'text/html') &&
                    response.headers.get('cache-control') !== 'no-store' &&
                    !response.headers.has('www-authenticate');
                  answerDetail = platformPage
                    ? 'platform-page'
                    : 'non-contract-answer';
                  if (
                    response.redirected ||
                    ![200, 409, 503].includes(response.status) ||
                    response.headers.get('cache-control') !== 'no-store' ||
                    !JSON_CONTENT_TYPE.test(
                      response.headers.get('content-type') ?? '',
                    )
                  )
                    unknown(answerDetail);
                  redelivering = false;
                  if (deliveryTimer !== undefined) {
                    clearTimeout(deliveryTimer);
                    deliveryTimer = undefined;
                  }
                  const attempts = readAttempts(response.headers, maxAttempts);
                  const body = response.body?.pipeThrough(
                    new TransformStream(),
                    {
                      signal,
                    },
                  );
                  answerDetail = 'transport-failure';
                  const bounded = await readBoundedBody(
                    new Request(endpoint, {
                      method: 'POST',
                      headers: response.headers,
                      body,
                      signal,
                      duplex: 'half',
                    }),
                    RESPONSE_BYTE_LIMIT,
                  );
                  assertActive();
                  answerDetail = 'non-contract-answer';
                  if (!bounded.ok) unknown('non-contract-answer');
                  const value = JSON.parse(bounded.text);
                  if (response.status === 200) {
                    if (
                      !exactKeys(value, [
                        'contractVersion',
                        'configSha256',
                        'action',
                        'ok',
                        'result',
                      ]) ||
                      value.contractVersion !== 2 ||
                      value.configSha256 !== configSha256 ||
                      value.action !== actionKind ||
                      value.ok !== true
                    )
                      unknown('non-contract-answer');
                  } else {
                    if (
                      !exactKeys(value, ['contractVersion', 'ok', 'error']) ||
                      value.contractVersion !== 2 ||
                      value.ok !== false ||
                      !exactKeys(value.error, ['code'])
                    )
                      unknown('non-contract-answer');
                    const code = value.error.code;
                    if (code === 'injected-response-loss') {
                      if (
                        response.status !== 503 ||
                        actionKind !== 'migration-continue'
                      )
                        unknown('non-contract-answer');
                    } else if (
                      typeof code !== 'string' ||
                      !Object.hasOwn(REFERENCE_REFUSAL_STATUS, code) ||
                      REFERENCE_REFUSAL_STATUS[code] !== response.status
                    )
                      unknown('non-contract-answer');
                  }
                  assertActive();
                  deliveryPending = false;
                  return { value, attempts };
                } catch {
                  assertActive();
                  const redeliver =
                    readOnly || (platformPage && response.status === 404);
                  if (!redeliver) unknown(answerDetail);
                  cancelBodyWithoutAwait(response?.body);
                  response = undefined;
                  const remaining = deliveryExpiresAt - performance.now();
                  if (remaining <= 0)
                    unknown(
                      readOnly && redelivering
                        ? 'delivery-window-expired'
                        : answerDetail,
                    );
                  redelivering = true;
                  if (deliveryExpiresAt < expiresAt)
                    deliveryTimer ??= setTimeout(
                      () => deadline.abort(),
                      remaining,
                    );
                  if (remaining <= INGRESS_INTERVAL_MS) await aborted;
                  await Promise.race([
                    new Promise((resolve) => {
                      retryTimer = setTimeout(resolve, INGRESS_INTERVAL_MS);
                    }),
                    aborted,
                  ]);
                  if (performance.now() >= deliveryExpiresAt)
                    unknown(
                      readOnly && redelivering
                        ? 'delivery-window-expired'
                        : answerDetail,
                    );
                }
              }
            } finally {
              if (signal.aborted) cancelBodyWithoutAwait(response?.body);
            }
          })();
          outcome = await Promise.race([exchange, aborted]);
          assertActive();
          await journal.settleInvocation(reservation);
        } catch {
          unknown(abortDetail());
        }
        uncertain = false;
        if (!outcome.value.ok) {
          if (outcome.value.error.code === 'injected-response-loss')
            throw new DirectInvocationError(
              'injected-response-loss',
              outcome.attempts,
            );
          throw new DirectInvocationError(
            'reference-refused',
            outcome.attempts,
            outcome.value.error.code,
          );
        }
        return { result: outcome.value.result, attempts: outcome.attempts };
      } finally {
        if (timer !== undefined) clearTimeout(timer);
        if (deliveryTimer !== undefined) clearTimeout(deliveryTimer);
        if (retryTimer !== undefined) clearTimeout(retryTimer);
        if (abortRead) signal.removeEventListener('abort', abortRead);
        deadline.abort();
        cancelBodyWithoutAwait(response?.body);
        busy = false;
      }
    },
  });
}
