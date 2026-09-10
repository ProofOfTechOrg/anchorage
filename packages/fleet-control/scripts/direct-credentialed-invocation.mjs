// SPDX-License-Identifier: Apache-2.0

import { validateHeaderValue } from 'node:http';
import { readBoundedBody } from '@proofoftech/flowsafe/host-kit';
import {
  deriveDirectConformanceNames,
  validateDirectConformanceConfig,
} from './direct-credentialed-conformance-config.mjs';
import { DirectRunStateError } from './direct-credentialed-run-state.mjs';
import { DIRECT_REFERENCE_PATH } from './direct-reference-contract.mjs';

const RESPONSE_BYTE_LIMIT = 4 * 1024 * 1024;
const ERROR_CODES = new Set([
  'invalid-input',
  'invocation-busy',
  'invocation-budget-exhausted',
  'outcome-unknown',
  'injected-response-loss',
  'reference-refused',
]);
const REFERENCE_REFUSAL_STATUS = {
  'operation-refused': 409,
  'wrong-operation': 409,
  'missing-continuation': 409,
  'budget-exhausted': 503,
};

export class DirectInvocationError extends Error {
  constructor(code = 'invalid-input', attempts, referenceCode) {
    const accepted = ERROR_CODES.has(code) ? code : 'invalid-input';
    super(accepted);
    this.name = 'DirectInvocationError';
    this.code = accepted;
    this.attempts = attempts;
    this.referenceCode = referenceCode;
  }
}

function invalid() {
  throw new DirectInvocationError();
}

function unknown() {
  throw new DirectInvocationError('outcome-unknown');
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

function cancelResponse(response) {
  try {
    void response?.body?.cancel().catch(() => {});
  } catch {
    // The abortable pipe owns cancellation while its source is locked.
  }
}

function readAttempts(headers, maxAttempts) {
  const attempts = {};
  for (const kind of ['provider', 'maintenance', 'application']) {
    const value = headers.get(`X-Direct-${kind}-Attempts`);
    if (value === null || !/^(?:0|[1-9][0-9]*)$/u.test(value)) unknown();
    const count = Number(value);
    if (!Number.isSafeInteger(count) || count > maxAttempts) unknown();
    attempts[kind] = count;
  }
  if (
    attempts.provider + attempts.maintenance + attempts.application >
    maxAttempts
  )
    unknown();
  return Object.freeze(attempts);
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
    const config = validateDirectConformanceConfig(prepared.config);
    const names = deriveDirectConformanceNames(config);
    const subdomain = input.accountWorkersDevSubdomain;
    const secret = input.invokeSecret;
    journal = input.journal;
    fetchRequest = input.fetch ?? globalThis.fetch;
    configSha256 = prepared.configSha256;
    invocationTimeoutMs = config.referenceWorker.invocationTimeoutMs;
    maxAttempts = config.referenceWorker.maxProviderRequests;
    if (
      typeof subdomain !== 'string' ||
      !/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/u.test(subdomain) ||
      typeof secret !== 'string' ||
      !secret ||
      secret !== secret.trim() ||
      typeof fetchRequest !== 'function' ||
      typeof journal.reserveInvocation !== 'function' ||
      typeof journal.settleInvocation !== 'function' ||
      typeof configSha256 !== 'string' ||
      !/^[a-f0-9]{64}$/u.test(configSha256) ||
      typeof prepared.referenceModuleSetSha256 !== 'string' ||
      !/^[a-f0-9]{64}$/u.test(prepared.referenceModuleSetSha256) ||
      prepared.names.referenceWorker !== names.referenceWorker
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
    endpoint = `https://${names.referenceWorker}.${subdomain}.workers.dev${DIRECT_REFERENCE_PATH}`;
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
      let abortRead;
      let response;
      const deadline = new AbortController();
      const signal = deadline.signal;
      try {
        let serialized;
        let reservation;
        try {
          serialized = JSON.stringify({
            contractVersion: 1,
            configSha256,
            action,
          });
          reservation = await journal.reserveInvocation(serialized);
        } catch (error) {
          throw reservationError(error);
        }
        uncertain = true;
        const expiresAt = performance.now() + invocationTimeoutMs;
        const assertActive = () => {
          if (performance.now() >= expiresAt) deadline.abort();
          signal.throwIfAborted();
        };
        const aborted = new Promise((_, reject) => {
          abortRead = () =>
            reject(new DirectInvocationError('outcome-unknown'));
          signal.addEventListener('abort', abortRead, { once: true });
        });
        timer = setTimeout(() => deadline.abort(), invocationTimeoutMs);
        let outcome;
        try {
          const actionKind = JSON.parse(serialized).action.kind;
          const exchange = (async () => {
            try {
              assertActive();
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
              assertActive();
              if (
                response.redirected ||
                ![200, 409, 503].includes(response.status) ||
                response.headers.get('cache-control') !== 'no-store' ||
                !/^application\/json(?:;\s*charset=utf-8)?$/iu.test(
                  response.headers.get('content-type') ?? '',
                )
              )
                unknown();
              const attempts = readAttempts(response.headers, maxAttempts);
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
              assertActive();
              if (!bounded.ok) unknown();
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
                  value.contractVersion !== 1 ||
                  value.configSha256 !== configSha256 ||
                  value.action !== actionKind ||
                  value.ok !== true
                )
                  unknown();
              } else {
                if (
                  !exactKeys(value, ['contractVersion', 'ok', 'error']) ||
                  value.contractVersion !== 1 ||
                  value.ok !== false ||
                  !exactKeys(value.error, ['code'])
                )
                  unknown();
                const code = value.error.code;
                if (code === 'injected-response-loss') {
                  if (
                    response.status !== 503 ||
                    actionKind !== 'migration-continue'
                  )
                    unknown();
                } else if (
                  typeof code !== 'string' ||
                  !Object.hasOwn(REFERENCE_REFUSAL_STATUS, code) ||
                  REFERENCE_REFUSAL_STATUS[code] !== response.status
                )
                  unknown();
              }
              assertActive();
              return { value, attempts };
            } finally {
              if (signal.aborted) cancelResponse(response);
            }
          })();
          outcome = await Promise.race([exchange, aborted]);
          assertActive();
          await journal.settleInvocation(reservation);
        } catch {
          unknown();
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
        if (abortRead) signal.removeEventListener('abort', abortRead);
        deadline.abort();
        cancelResponse(response);
        busy = false;
      }
    },
  });
}
