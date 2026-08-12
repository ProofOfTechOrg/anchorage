// SPDX-License-Identifier: Apache-2.0

function encodeUtf16(value: string): string {
  let encoded = '';
  for (let index = 0; index < value.length; index += 1) {
    encoded += value.charCodeAt(index).toString(16).padStart(4, '0');
  }
  return encoded;
}

export interface IdempotencyIdentity {
  connectorId: string;
  idempotencyKey: string;
  isolationScope?: string;
}

export function legacyIdempotencyStorageKey(
  identity: IdempotencyIdentity,
): string {
  const { connectorId, idempotencyKey, isolationScope } = identity;
  return isolationScope === undefined
    ? `${connectorId}:${idempotencyKey}`
    : `${isolationScope}:${connectorId}:${idempotencyKey}`;
}

// Version, key kind, and tuple arity are explicit. Each component is encoded
// as fixed-width UTF-16 code units, whose alphabet cannot contain the `_`
// separator. The result contains no colon and is therefore disjoint from all
// v1 `[scope:]connector:key` records, including lone-surrogate JS strings.
export function idempotencyStorageKey(identity: IdempotencyIdentity): string {
  const { connectorId, idempotencyKey, isolationScope } = identity;
  return isolationScope === undefined
    ? `bw2_i_u_${encodeUtf16(connectorId)}_${encodeUtf16(idempotencyKey)}`
    : `bw2_i_s_${encodeUtf16(isolationScope)}_${encodeUtf16(connectorId)}_${encodeUtf16(idempotencyKey)}`;
}

export function isAmbiguousLegacyIdempotencyIdentity(
  identity: Pick<IdempotencyIdentity, 'idempotencyKey' | 'isolationScope'>,
): boolean {
  return (
    identity.isolationScope !== undefined ||
    identity.idempotencyKey.includes(':')
  );
}
