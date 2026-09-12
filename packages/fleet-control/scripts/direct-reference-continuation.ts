// SPDX-License-Identifier: Apache-2.0

import type { DirectReferenceAction } from './direct-reference-contract.mjs';
import { DirectReferenceExecutionError } from './direct-reference-http.js';
import type { DirectStoredOperation } from './direct-reference-journal.js';

export function directContinuation(
  stored: DirectStoredOperation,
  action: DirectReferenceAction,
): unknown {
  const token: unknown = Object.hasOwn(action, 'token')
    ? Reflect.get(action, 'token')
    : stored.tokenJson === null
      ? undefined
      : JSON.parse(stored.tokenJson);
  if (token === undefined)
    throw new DirectReferenceExecutionError('missing-continuation');
  if (
    !token ||
    typeof token !== 'object' ||
    Array.isArray(token) ||
    !Object.hasOwn(token, 'operationId') ||
    (token as { operationId: unknown }).operationId !== stored.operationId
  )
    throw new DirectReferenceExecutionError('wrong-operation');
  return token;
}
