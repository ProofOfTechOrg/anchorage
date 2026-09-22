// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { readBoundedBody } from '@proofoftech/flowsafe/host-kit';

// The path identifies the authenticated transport; the envelope version
// identifies the request and response body contract carried over it.
export const DIRECT_REFERENCE_PATH =
  '/.well-known/anchorage/direct-conformance/v1/actions';
export const DIRECT_REFERENCE_BODY_LIMIT = 16 * 1024;

const READ_ONLY_ACTIONS = new Map([
  ['control-read', null],
  ['inventory-read', null],
  ['audit-page', null],
  ['migration-page', null],
  ['cleanup-receipt', null],
  ['decommission-export', null],
  ['tenant-probe', new Set(['health', 'object-read'])],
  ['tenant-fence', new Set(['read', 'inventory'])],
  ['tenant-continuation', new Set(['status'])],
]);

export class DirectReferenceRequestError extends Error {
  constructor(code = 'invalid-request') {
    super(code);
    this.name = 'DirectReferenceRequestError';
    this.code = code;
  }
}

function invalid() {
  throw new DirectReferenceRequestError();
}

function keys(value, required, optional = []) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) invalid();
  if (required.some((key) => !Object.hasOwn(value, key))) invalid();
  const accepted = new Set([...required, ...optional]);
  if (Object.keys(value).some((key) => !accepted.has(key))) invalid();
}

function member(value, values) {
  if (!values.includes(value)) invalid();
}

function ordinal(value) {
  if (!Number.isSafeInteger(value) || value < 1) invalid();
  return value;
}

function sha256(value) {
  if (typeof value !== 'string' || !/^[a-f0-9]{64}$/u.test(value)) invalid();
  return value;
}

function canonicalJson(value) {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value))
    return `[${value.map((entry) => canonicalJson(entry)).join(',')}]`;
  return `{${Object.keys(value)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`)
    .join(',')}}`;
}

export function serializeDirectReferenceCore(value) {
  return canonicalJson(JSON.parse(JSON.stringify(value)));
}

export function directReferenceRequestSha256(value) {
  return createHash('sha256')
    .update(serializeDirectReferenceCore(value))
    .digest('hex');
}

export function isDirectReferenceReadOnlyAction(action) {
  const operations = READ_ONLY_ACTIONS.get(action.kind);
  return operations === null || operations?.has(action.operation) === true;
}

function page(action) {
  if (
    !Number.isSafeInteger(action.limit) ||
    action.limit < 1 ||
    action.limit > 1_000
  )
    invalid();
  if (
    Object.hasOwn(action, 'afterOrdinal') &&
    (!Number.isSafeInteger(action.afterOrdinal) ||
      action.afterOrdinal < 0 ||
      action.afterOrdinal >= Number.MAX_SAFE_INTEGER)
  )
    invalid();
}

function counters(value) {
  for (const counter of [value.expectedMutationEpoch, value.expectedRevision])
    if (
      !Number.isSafeInteger(counter) ||
      counter < 0 ||
      counter >= Number.MAX_SAFE_INTEGER
    )
      invalid();
}

function actionFromParsed(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) invalid();
  const kind = value.kind;
  switch (kind) {
    case 'control-read':
    case 'migration-start':
    case 'migration-reprovision-a':
    case 'migration-abandon':
    case 'force-recovery':
    case 'force-observe':
    case 'recover-force-residual':
      keys(value, ['kind']);
      break;
    case 'reconcile-invocation':
      keys(value, ['kind', 'ordinal', 'requestSha256']);
      ordinal(value.ordinal);
      sha256(value.requestSha256);
      break;
    case 'force-terminal':
      keys(value, ['kind', 'role']);
      member(value.role, ['a']);
      break;
    case 'tenant-probe':
      keys(value, ['kind', 'role', 'operation']);
      member(value.role, ['a', 'b', 'recovery']);
      member(value.operation, [
        'health',
        'object-put',
        'object-read',
        'object-delete',
      ]);
      break;
    case 'tenant-continuation':
      member(value.operation, ['start', 'status', 'resume-locked', 'resume']);
      if (value.operation === 'start') {
        keys(value, ['kind', 'operation', 'challenge']);
        if (
          typeof value.challenge !== 'string' ||
          !/^[a-f0-9]{64}$/u.test(value.challenge)
        )
          invalid();
      } else if (value.operation === 'resume') {
        keys(value, ['kind', 'operation', 'runId', 'approvalId']);
        if (
          typeof value.runId !== 'string' ||
          !/^[A-Za-z0-9_-]{1,128}$/u.test(value.runId) ||
          typeof value.approvalId !== 'string' ||
          !/^[A-Za-z0-9_-]{1,128}$/u.test(value.approvalId)
        )
          invalid();
      } else {
        keys(value, ['kind', 'operation', 'runId']);
        if (
          typeof value.runId !== 'string' ||
          !/^[A-Za-z0-9_-]{1,128}$/u.test(value.runId)
        )
          invalid();
      }
      break;
    case 'tenant-fence': {
      member(value.operation, [
        'read',
        'drain',
        'reopen',
        'lock',
        'unlock',
        'inventory',
        'mutate-current',
        'probe-missing',
        'probe-stale',
        'probe-future',
      ]);
      const versioned =
        value.operation === 'drain' ||
        value.operation === 'reopen' ||
        value.operation === 'lock' ||
        value.operation === 'unlock';
      keys(
        value,
        versioned
          ? [
              'kind',
              'role',
              'operation',
              'expectedMutationEpoch',
              'expectedRevision',
            ]
          : ['kind', 'role', 'operation'],
      );
      member(value.role, ['a', 'b']);
      if (versioned) counters(value);
      break;
    }
    case 'provision':
      keys(value, ['kind', 'role', 'release'], ['cycle']);
      member(value.role, ['a', 'b', 'recovery']);
      member(value.release, ['initial', 'failed-recovery']);
      if (value.release === 'failed-recovery' && value.role !== 'recovery')
        invalid();
      if (
        Object.hasOwn(value, 'cycle') &&
        (value.cycle !== 'reprovision' ||
          value.role !== 'a' ||
          value.release !== 'initial')
      )
        invalid();
      break;
    case 'inventory-start':
    case 'inventory-read':
    case 'inventory-continue':
      keys(
        value,
        ['kind', 'slot'],
        kind === 'inventory-continue' ? ['token'] : [],
      );
      member(value.slot, ['inventory-before', 'inventory-after']);
      break;
    case 'audit-start':
    case 'audit-abandon':
    case 'audit-continue':
      keys(value, ['kind', 'slot'], kind === 'audit-continue' ? ['token'] : []);
      member(value.slot, ['audit-before', 'audit-after']);
      break;
    case 'audit-page':
      keys(value, ['kind', 'slot', 'limit'], ['afterOrdinal']);
      member(value.slot, ['audit-before', 'audit-after']);
      page(value);
      break;
    case 'migration-page':
      keys(value, ['kind', 'limit'], ['afterOrdinal']);
      page(value);
      break;
    case 'migration-continue':
      keys(value, ['kind'], ['token']);
      break;
    case 'cleanup-start':
    case 'cleanup-receipt':
    case 'decommission-start':
    case 'decommission-export':
      keys(value, ['kind', 'role'], ['cycle']);
      member(value.role, ['a', 'b', 'recovery']);
      if (
        Object.hasOwn(value, 'cycle') &&
        (value.cycle !== 'reprovision' || value.role !== 'a')
      )
        invalid();
      break;
    case 'cleanup-continue':
    case 'decommission-continue':
      keys(value, ['kind', 'role'], ['token', 'cycle']);
      member(value.role, ['a', 'b', 'recovery']);
      if (
        Object.hasOwn(value, 'cycle') &&
        (value.cycle !== 'reprovision' || value.role !== 'a')
      )
        invalid();
      break;
    case 'cleanup-restart-blocked':
    case 'decommission-restart-blocked':
      keys(value, ['kind', 'role', 'token'], ['cycle']);
      member(value.role, ['a', 'b', 'recovery']);
      if (
        Object.hasOwn(value, 'cycle') &&
        (value.cycle !== 'reprovision' || value.role !== 'a')
      )
        invalid();
      break;
    default:
      invalid();
  }
  return Object.freeze(value);
}

export async function readDirectReferenceRequest(
  request,
  expectedConfigSha256,
) {
  let body;
  try {
    body = await readBoundedBody(request, DIRECT_REFERENCE_BODY_LIMIT);
  } catch {
    invalid();
  }
  if (!body.ok) throw new DirectReferenceRequestError(body.reason);
  let value;
  try {
    value = JSON.parse(body.text);
  } catch {
    invalid();
  }
  keys(value, ['contractVersion', 'configSha256', 'action', 'reservation']);
  if (
    value.contractVersion !== 2 ||
    typeof value.configSha256 !== 'string' ||
    !/^[a-f0-9]{64}$/u.test(value.configSha256)
  )
    invalid();
  if (value.configSha256 !== expectedConfigSha256)
    throw new DirectReferenceRequestError('run-binding-mismatch');
  const action = actionFromParsed(value.action);
  let reservation = null;
  if (action.kind === 'reconcile-invocation') {
    if (value.reservation !== null) invalid();
  } else {
    keys(value.reservation, ['ordinal', 'requestSha256']);
    const candidate = value.reservation;
    reservation = Object.freeze({
      ordinal: ordinal(candidate.ordinal),
      requestSha256: sha256(candidate.requestSha256),
    });
    const core = {
      contractVersion: 2,
      configSha256: value.configSha256,
      action,
    };
    if (directReferenceRequestSha256(core) !== reservation.requestSha256)
      throw new DirectReferenceRequestError('request-hash-mismatch');
  }
  return Object.freeze({
    contractVersion: 2,
    configSha256: value.configSha256,
    action,
    reservation,
  });
}
