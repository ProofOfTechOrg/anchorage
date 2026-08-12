// SPDX-License-Identifier: Apache-2.0

import type { IdempotencyInspection, IdempotencyRecord } from './index.js';

export interface AtomicLegacyIdempotencyMigrationRequest {
  sourceKey: string;
  targetKey: string;
  expectedRecord: IdempotencyRecord;
  targetRecord: IdempotencyRecord;
}

export type AtomicLegacyIdempotencyMigrationResult =
  | { state: 'migrated'; record: IdempotencyRecord }
  | { state: 'already-migrated'; record: IdempotencyRecord }
  | { state: 'source-absent' }
  | { state: 'source-pending' }
  | { state: 'source-mismatch'; record: IdempotencyRecord }
  | { state: 'target-conflict'; target: IdempotencyInspection };

export const ATOMIC_LEGACY_IDEMPOTENCY_MIGRATION = Symbol(
  'breakwater.atomic-legacy-idempotency-migration',
);

export interface AtomicLegacyIdempotencyMigrator {
  [ATOMIC_LEGACY_IDEMPOTENCY_MIGRATION](
    request: AtomicLegacyIdempotencyMigrationRequest,
  ): Promise<AtomicLegacyIdempotencyMigrationResult>;
}

export function isAtomicLegacyIdempotencyMigrator(
  value: unknown,
): value is AtomicLegacyIdempotencyMigrator {
  return (
    typeof value === 'object' &&
    value !== null &&
    typeof (value as Partial<AtomicLegacyIdempotencyMigrator>)[
      ATOMIC_LEGACY_IDEMPOTENCY_MIGRATION
    ] === 'function'
  );
}
