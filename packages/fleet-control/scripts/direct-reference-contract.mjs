// SPDX-License-Identifier: Apache-2.0

import { readBoundedBody } from '@proofoftech/flowsafe/host-kit';

export const DIRECT_REFERENCE_PATH =
  '/.well-known/anchorage/direct-conformance/v1/actions';
export const DIRECT_REFERENCE_BODY_LIMIT = 16 * 1024;

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

function actionFromParsed(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) invalid();
  const kind = value.kind;
  switch (kind) {
    case 'control-read':
    case 'migration-start':
    case 'migration-abandon':
    case 'force-recovery':
    case 'force-observe':
      keys(value, ['kind']);
      break;
    case 'provision':
      keys(value, ['kind', 'role', 'release']);
      member(value.role, ['a', 'b', 'recovery']);
      member(value.release, ['initial', 'failed-recovery']);
      if (value.release === 'failed-recovery' && value.role !== 'recovery')
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
      keys(value, ['kind', 'role']);
      member(value.role, ['a', 'b', 'recovery']);
      break;
    case 'cleanup-continue':
    case 'decommission-continue':
      keys(value, ['kind', 'role'], ['token']);
      member(value.role, ['a', 'b', 'recovery']);
      break;
    case 'cleanup-restart-blocked':
    case 'decommission-restart-blocked':
      keys(value, ['kind', 'role', 'token']);
      member(value.role, ['a', 'b', 'recovery']);
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
  keys(value, ['contractVersion', 'configSha256', 'action']);
  if (
    value.contractVersion !== 1 ||
    typeof value.configSha256 !== 'string' ||
    !/^[a-f0-9]{64}$/u.test(value.configSha256)
  )
    invalid();
  if (value.configSha256 !== expectedConfigSha256)
    throw new DirectReferenceRequestError('run-binding-mismatch');
  return Object.freeze({
    contractVersion: 1,
    configSha256: value.configSha256,
    action: actionFromParsed(value.action),
  });
}
