// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import type { DriftFinding } from '../src/fleet.js';
import {
  driftFindingRowFromUnknown,
  FLEET_AUDIT_STAGE_ORDER,
  type FleetAuditFindingKind,
  fleetAuditFactRowFromUnknown,
  fleetAuditOperationRecordFromUnknown,
  fleetAuditStageFromUnknown,
  nextAuditStage,
  withheldAuditDetail,
} from '../src/fleet-audit-state.js';
import {
  FLEET_MIGRATION_STEPS,
  fleetMigrationItemFromUnknown,
  fleetMigrationOperationRecordFromUnknown,
  fleetMigrationPlanEntryFromUnknown,
} from '../src/fleet-migration-state.js';
import {
  assertFleetOperationId,
  canonicalFleetOperationBytes,
  classifyFleetOperationToken,
  FLEET_MIGRATION_PLAN_BOUND,
  FLEET_OPERATION_INTAKE_BYTE_BOUND,
  FLEET_OPERATION_RECORD_BYTE_BOUND,
  FLEET_OPERATION_RECORD_ROW_BYTE_BOUND,
  FLEET_OPERATION_ROW_PAYLOAD_BYTE_BOUND,
  FLEET_OPERATION_STRING_BYTE_BOUND,
  FLEET_OPERATION_TOKEN_BYTE_BOUND,
  FleetOperationStateError,
  FleetOperationTokenError,
  FleetOperationTokenFutureError,
  FleetOperationTokenKindError,
  FleetOperationTokenOperationError,
  fleetOperationIntakeDigest,
  fleetOperationItemsIntake,
  fleetOperationRunRecordFromUnknown,
  fleetOperationStagedRowFromUnknown,
  isDurableAuditDetailSafe,
  parseFleetOperationToken,
} from '../src/fleet-operation-state.js';

const OPERATION_ID = '123e4567-e89b-42d3-a456-426614174000';
const NOW = '2026-09-01T00:00:00.000Z';

function auditProgress(revision = 0) {
  return {
    kind: 'audit' as const,
    revision,
    stage: { step: 'provider-findings' as const, rowOrdinal: 0 },
    generation: 1,
    auditTimeMs: 1_700_000_000_000,
    staleAfterMs: 60_000,
    recordCount: 1,
    findingCount: 0,
    factCount: 0,
  };
}

function auditRecord(revision = 0) {
  return {
    version: 1 as const,
    operationId: OPERATION_ID,
    kind: 'audit' as const,
    state: 'running' as const,
    progress: auditProgress(revision),
    updatedAt: NOW,
  };
}

function migrationItem(
  status: 'pending' | 'active' | 'complete' | 'failed' = 'pending',
) {
  const common = {
    ordinal: 0,
    tenantTag: 'tenant',
    environment: 'production',
    entryRecordDigest: 'a'.repeat(64),
    status,
  };
  return status === 'pending'
    ? common
    : {
        ...common,
        targetSpecDigest: 'b'.repeat(64),
        plan: [{ step: 'apply-migrations', targetSchemaVersion: 2 }],
        planCursor: status === 'complete' ? 1 : 0,
      };
}

function migrationRecord() {
  return {
    version: 1 as const,
    operationId: OPERATION_ID,
    kind: 'migration' as const,
    state: 'running' as const,
    progress: {
      kind: 'migration' as const,
      revision: 0,
      itemCount: 1,
      activeItemOrdinal: 0,
      completedItemCount: 0,
    },
    updatedAt: NOW,
  };
}

function nested(depth: number): unknown {
  let value: unknown = 'leaf';
  for (let index = 0; index < depth; index += 1) value = { value };
  return value;
}

describe('fleet operation state', () => {
  it('token round-trip + exact-key refusal', () => {
    const token = { version: 1, operationId: OPERATION_ID, revision: 7 };
    expect(parseFleetOperationToken(token)).toEqual(token);
    expect(() => parseFleetOperationToken({ ...token, cursor: 1 })).toThrow(
      FleetOperationTokenError,
    );
  });

  it('token classification order generic → operation → kind → future → stale/current (with expectedKind)', () => {
    expect(() => parseFleetOperationToken(null)).toThrow(
      FleetOperationTokenError,
    );
    const token = parseFleetOperationToken({
      version: 1,
      operationId: OPERATION_ID,
      revision: 1,
    });
    expect(() =>
      classifyFleetOperationToken(token, undefined, 'audit'),
    ).toThrow(FleetOperationTokenOperationError);
    expect(() =>
      classifyFleetOperationToken(token, migrationRecord(), 'audit'),
    ).toThrow(FleetOperationTokenKindError);
    expect(() =>
      classifyFleetOperationToken(token, auditRecord(0), 'audit'),
    ).toThrow(FleetOperationTokenFutureError);
    expect(classifyFleetOperationToken(token, auditRecord(2), 'audit')).toBe(
      'stale',
    );
    expect(classifyFleetOperationToken(token, auditRecord(1), 'audit')).toBe(
      'current',
    );
  });

  it('envelope record codec round-trip + bound refusals', () => {
    expect(fleetOperationRunRecordFromUnknown(auditRecord())).toEqual(
      auditRecord(),
    );
    expect(() =>
      fleetOperationRunRecordFromUnknown({
        ...auditRecord(),
        progress: { ...auditProgress(), extra: 'x'.repeat(100_000) },
      }),
    ).toThrow(FleetOperationStateError);
    expect(() =>
      fleetOperationRunRecordFromUnknown({
        ...auditRecord(),
        state: 'failed',
        progress: { ...auditProgress(), failure: { reason: 'unknown' } },
      }),
    ).toThrow(FleetOperationStateError);
    expect(() =>
      fleetOperationRunRecordFromUnknown({
        ...auditRecord(),
        state: 'failed',
        progress: {
          ...auditProgress(),
          failure: { reason: 'item-failed', itemOrdinal: 0.5 },
        },
      }),
    ).toThrow(FleetOperationStateError);
  });

  it('audit refinement codec round-trip + vocabulary refusal', () => {
    expect(fleetAuditOperationRecordFromUnknown(auditRecord())).toEqual(
      auditRecord(),
    );
    expect(() =>
      fleetAuditOperationRecordFromUnknown({
        ...auditRecord(),
        progress: { ...auditProgress(), auditTimeMs: 9e15 },
      }),
    ).toThrow(FleetOperationStateError);
    expect(() =>
      fleetAuditOperationRecordFromUnknown({
        ...auditRecord(),
        progress: { ...auditProgress(), staleAfterMs: 0 },
      }),
    ).toThrow(FleetOperationStateError);
    expect(() =>
      driftFindingRowFromUnknown({
        tenantTag: 'tenant',
        environment: 'production',
        kind: 'not-a-finding',
        detail: 'safe',
      }),
    ).toThrow(FleetOperationStateError);
    expect(() =>
      fleetAuditFactRowFromUnknown({
        factKind: 'database-owner',
        key: 'database-name',
        tenantTag: 'Prod-1',
        environment: 'production',
      }),
    ).toThrow(FleetOperationStateError);
  });

  it('migration refinement codec round-trip + item/status vocabulary refusal (incl. targetSpecDigest/plan/planCursor optionality by status)', () => {
    expect(fleetMigrationOperationRecordFromUnknown(migrationRecord())).toEqual(
      migrationRecord(),
    );
    for (const status of ['pending', 'active', 'complete', 'failed'] as const) {
      expect(fleetMigrationItemFromUnknown(migrationItem(status)).status).toBe(
        status,
      );
    }
    expect(() =>
      fleetMigrationItemFromUnknown({
        ...migrationItem('pending'),
        status: 'waiting',
      }),
    ).toThrow(FleetOperationStateError);
    expect(() =>
      fleetMigrationItemFromUnknown({
        ...migrationItem('pending'),
        targetSpecDigest: 'b'.repeat(64),
      }),
    ).toThrow(FleetOperationStateError);
    expect(() =>
      fleetMigrationItemFromUnknown({
        ...migrationItem('active'),
        targetSpecDigest: undefined,
      }),
    ).toThrow(FleetOperationStateError);
    expect(() =>
      fleetMigrationItemFromUnknown({
        ...migrationItem('active'),
        plan: undefined,
      }),
    ).toThrow(FleetOperationStateError);
    expect(() =>
      fleetMigrationItemFromUnknown({
        ...migrationItem('complete'),
        planCursor: 0,
      }),
    ).toThrow(FleetOperationStateError);
    expect(fleetMigrationItemFromUnknown(migrationItem('complete'))).toEqual(
      migrationItem('complete'),
    );
    expect(() =>
      fleetMigrationItemFromUnknown({
        ...migrationItem('pending'),
        tenantTag: 'Prod-1',
      }),
    ).toThrow(FleetOperationStateError);
  });

  it('staged-row codec exact keys + row-kind vocabulary refusal', () => {
    const row = { rowKind: 'record', ordinal: 0, payload: { value: true } };
    expect(fleetOperationStagedRowFromUnknown(row)).toEqual(row);
    expect(() =>
      fleetOperationStagedRowFromUnknown({ ...row, extra: true }),
    ).toThrow(FleetOperationStateError);
    expect(() =>
      fleetOperationStagedRowFromUnknown({ ...row, rowKind: 'cursor' }),
    ).toThrow(FleetOperationStateError);
  });

  it('record/token byte bounds fail closed', () => {
    expect(() =>
      parseFleetOperationToken({
        version: 1,
        operationId: OPERATION_ID,
        revision: 0,
        padding: 'x'.repeat(FLEET_OPERATION_TOKEN_BYTE_BOUND),
      }),
    ).toThrow(FleetOperationTokenError);
    const padding = Object.fromEntries(
      Array.from(
        { length: Math.floor(FLEET_OPERATION_RECORD_BYTE_BOUND / 4000) + 1 },
        (_, index) => [`padding${index}`, 'x'.repeat(4000)],
      ),
    );
    expect(() =>
      fleetOperationRunRecordFromUnknown({
        ...auditRecord(),
        progress: { ...auditProgress(), padding },
      }),
    ).toThrow(FleetOperationStateError);
  });

  it('string/depth/node bounds fail closed', () => {
    expect(() =>
      fleetOperationRunRecordFromUnknown({
        ...auditRecord(),
        progress: {
          ...auditProgress(),
          text: 'x'.repeat(FLEET_OPERATION_STRING_BYTE_BOUND + 1),
        },
      }),
    ).toThrow(FleetOperationStateError);
    expect(() =>
      fleetOperationRunRecordFromUnknown({
        ...auditRecord(),
        progress: { ...auditProgress(), nested: nested(65) },
      }),
    ).toThrow(FleetOperationStateError);
    expect(() =>
      fleetOperationRunRecordFromUnknown({
        ...auditRecord(),
        progress: { ...auditProgress(), nodes: Array(8193).fill(0) },
      }),
    ).toThrow(FleetOperationStateError);
  });

  it('row payload 16 KiB bound + 96 KiB record-row allowance', () => {
    const valueBytes = 3000;
    const rowPayloadOverflowCount =
      Math.floor(FLEET_OPERATION_ROW_PAYLOAD_BYTE_BOUND / valueBytes) + 1;
    const recordRowOverflowCount =
      Math.floor(FLEET_OPERATION_RECORD_ROW_BYTE_BOUND / valueBytes) + 1;
    const payload = Object.fromEntries(
      Array.from({ length: rowPayloadOverflowCount }, (_, index) => [
        `value${index}`,
        'x'.repeat(valueBytes),
      ]),
    );
    expect(() =>
      fleetOperationStagedRowFromUnknown({
        rowKind: 'finding',
        ordinal: 0,
        payload,
      }),
    ).toThrow(FleetOperationStateError);
    expect(
      fleetOperationStagedRowFromUnknown({
        rowKind: 'record',
        ordinal: 0,
        payload,
      }).payload,
    ).toEqual(payload);
    expect(() =>
      fleetOperationStagedRowFromUnknown({
        rowKind: 'record',
        ordinal: 0,
        payload: Object.fromEntries(
          Array.from({ length: recordRowOverflowCount }, (_, index) => [
            `value${index}`,
            'x'.repeat(valueBytes),
          ]),
        ),
      }),
    ).toThrow(FleetOperationStateError);
  });

  it('intake byte bound refusal above and acceptance at the bound', () => {
    const atBound = Array.from({ length: 4096 }, (_, index) =>
      'x'.repeat(index === 0 ? 4092 : 4093),
    );
    expect(canonicalFleetOperationBytes(atBound).length).toBe(
      FLEET_OPERATION_INTAKE_BYTE_BOUND,
    );
    expect(() => fleetOperationIntakeDigest(atBound)).not.toThrow();
    const aboveBound = [...atBound];
    aboveBound[0] = `${aboveBound[0]}x`;
    expect(() => fleetOperationIntakeDigest(aboveBound)).toThrow(
      FleetOperationStateError,
    );
  });

  it('structured-field validation accepts any bounded provider-claimed finding tag — empty and control-byte values included — and rejects an over-bound one', () => {
    const base = {
      environment: 'production',
      kind: 'audit-error',
      detail: 'safe detail',
    } as const;
    for (const tenantTag of ['bearer', 'Prod-1', 'Bad\nTag', '']) {
      expect(driftFindingRowFromUnknown({ ...base, tenantTag }).tenantTag).toBe(
        tenantTag,
      );
    }
    expect(() =>
      driftFindingRowFromUnknown({
        ...base,
        tenantTag: 'x'.repeat(FLEET_OPERATION_STRING_BYTE_BOUND + 1),
      }),
    ).toThrow(FleetOperationStateError);
  });

  it('isDurableAuditDetailSafe rejects control bytes AND credential substrings; accepts a long spaced benign detail', () => {
    expect(isDurableAuditDetailSafe('line\nsecret')).toBe(false);
    for (const marker of ['Authorization', 'BEARER', 'x-auth', 'API_TOKEN']) {
      expect(isDurableAuditDetailSafe(`provider said ${marker}`)).toBe(false);
    }
    expect(isDurableAuditDetailSafe('safe words '.repeat(300))).toBe(true);
    expect(
      driftFindingRowFromUnknown({
        tenantTag: 'tenant',
        environment: 'production',
        kind: 'audit-error',
        detail: '',
      }).detail,
    ).toBe('');
    expect(() =>
      driftFindingRowFromUnknown({
        tenantTag: 'tenant',
        environment: 'production',
        kind: 'audit-error',
        detail: 'unsafe\u0000detail',
      }),
    ).toThrow(FleetOperationStateError);
    for (const key of ['', 'database\nname']) {
      expect(
        fleetAuditFactRowFromUnknown({
          factKind: 'duplicate-namespace',
          key,
        }).key,
      ).toBe(key);
    }
  });

  it('the withheld-detail fallback shape (a bearer-service detail is withheld, never thrown)', () => {
    const unsafe = 'maintenance failed for bearer-service';
    expect(isDurableAuditDetailSafe(unsafe)).toBe(false);
    expect(withheldAuditDetail('maintenance-stale')).toBe(
      "finding detail withheld: unsafe bytes (kind 'maintenance-stale')",
    );
    for (const detail of [
      'x'.repeat(FLEET_OPERATION_STRING_BYTE_BOUND + 1),
      'unsafe\u0000detail',
    ]) {
      expect(isDurableAuditDetailSafe(detail)).toBe(false);
      expect(() =>
        driftFindingRowFromUnknown({
          tenantTag: 'tenant',
          environment: 'production',
          kind: 'maintenance-stale',
          detail,
        }),
      ).toThrow(FleetOperationStateError);
      expect(withheldAuditDetail('maintenance-stale')).toBe(
        "finding detail withheld: unsafe bytes (kind 'maintenance-stale')",
      );
    }
  });

  it('assertFleetOperationId accepts lowercase UUIDv4; rejects uppercase/short/non-v4', () => {
    expect(() => assertFleetOperationId(OPERATION_ID)).not.toThrow();
    for (const value of [
      OPERATION_ID.toUpperCase(),
      'short',
      OPERATION_ID.replace('-4', '-3'),
    ]) {
      expect(() => assertFleetOperationId(value)).toThrow(
        FleetOperationStateError,
      );
    }
  });

  it('canonicalFleetOperationBytes/fleetOperationIntakeDigest stable under nested key reorder; digest differs on any value change', () => {
    const first = { z: [{ b: 2, a: 1 }], a: { d: 4, c: 3 } };
    const reordered = { a: { c: 3, d: 4 }, z: [{ a: 1, b: 2 }] };
    expect(canonicalFleetOperationBytes(first)).toBe(
      canonicalFleetOperationBytes(reordered),
    );
    expect(fleetOperationIntakeDigest(first)).toBe(
      fleetOperationIntakeDigest(reordered),
    );
    expect(fleetOperationIntakeDigest(first)).not.toBe(
      fleetOperationIntakeDigest({ ...reordered, z: [{ a: 1, b: 3 }] }),
    );
  });

  it('audit stage codec round-trip + unknown-step refusal', () => {
    const stage = { step: 'r2-missing-identity', expectedOrdinal: 4 } as const;
    expect(fleetAuditStageFromUnknown(stage)).toEqual(stage);
    expect(() => fleetAuditStageFromUnknown({ step: 'unknown' })).toThrow(
      FleetOperationStateError,
    );
  });

  it('nextAuditStage successor chain over all 13 stages (same-step on exhausted: false)', () => {
    const initial = {
      step: 'provider-findings',
      rowOrdinal: 0,
    } as const;
    let stage = fleetAuditStageFromUnknown(initial);
    const seen = [stage.step];
    expect(nextAuditStage({ ...initial, rowOrdinal: 7 }, false)).toEqual({
      step: 'provider-findings',
      rowOrdinal: 7,
    });
    while (stage.step !== 'finalize') {
      stage = nextAuditStage(stage, true);
      seen.push(stage.step);
    }
    expect(seen).toEqual(FLEET_AUDIT_STAGE_ORDER);
    expect(nextAuditStage(stage, true)).toEqual({ step: 'finalize' });
  });

  it('FleetMigrationStep 24-member vocabulary refusal + fleetMigrationPlanEntryFromUnknown per-entry scope', () => {
    expect(FLEET_MIGRATION_STEPS).toHaveLength(24);
    for (const step of FLEET_MIGRATION_STEPS) {
      expect(fleetMigrationPlanEntryFromUnknown({ step }).step).toBe(step);
    }
    expect(() =>
      fleetMigrationPlanEntryFromUnknown({ step: 'unknown' }),
    ).toThrow(FleetOperationStateError);
    expect(() =>
      fleetMigrationPlanEntryFromUnknown({
        step: 'promote',
        targetSchemaVersion: 2,
      }),
    ).toThrow(FleetOperationStateError);
    expect(
      fleetMigrationPlanEntryFromUnknown({
        step: 'apply-migrations',
        targetSchemaVersion: 2,
      }),
    ).toEqual({ step: 'apply-migrations', targetSchemaVersion: 2 });
    expect(() =>
      fleetMigrationPlanEntryFromUnknown({
        step: 'apply-migrations',
        targetSchemaVersion: 0,
      }),
    ).toThrow(FleetOperationStateError);
    expect(() =>
      fleetMigrationPlanEntryFromUnknown({
        step: 'apply-migrations',
        targetSchemaVersion: Number.MAX_SAFE_INTEGER + 1,
      }),
    ).toThrow(FleetOperationStateError);
  });

  it('FLEET_MIGRATION_PLAN_BOUND refusal', () => {
    expect(() =>
      fleetMigrationItemFromUnknown({
        ...migrationItem('active'),
        plan: Array.from({ length: FLEET_MIGRATION_PLAN_BOUND + 1 }, () => ({
          step: 'promote',
        })),
      }),
    ).toThrow(FleetOperationStateError);
  });

  it("the audit kind vocabulary is set-equal to DriftFinding['kind']", () => {
    type DriftKind = DriftFinding['kind'];
    // The set-equality proof is compile-time; pnpm typecheck is the gate
    // that enforces it.
    const driftIsSubsetOfAudit: Exclude<
      DriftKind,
      FleetAuditFindingKind
    > extends never
      ? true
      : false = true;
    const auditIsSubsetOfDrift: Exclude<
      FleetAuditFindingKind,
      DriftKind
    > extends never
      ? true
      : false = true;
    expect(driftIsSubsetOfAudit).toBe(true);
    expect(auditIsSubsetOfDrift).toBe(true);
  });

  it('fleetOperationItemsIntake digests are order-sensitive across items, key-order-stable within an item, and framed so two items never collide with one concatenated item', () => {
    const envelope = { generation: 1 };
    const a = 1;
    const b = 2;
    const ab = 12;
    const digestFor = (
      items: readonly unknown[],
      candidateEnvelope: Record<string, unknown> = envelope,
    ): string => {
      const result = fleetOperationItemsIntake({
        envelope: candidateEnvelope,
        items,
        itemByteBound: FLEET_OPERATION_RECORD_ROW_BYTE_BOUND,
      });
      expect('digest' in result).toBe(true);
      if (!('digest' in result)) throw new Error('unreachable');
      return result.digest;
    };

    expect(digestFor([a, b])).not.toBe(digestFor([b, a]));
    expect(digestFor([{ b: 2, a: 1 }])).toBe(digestFor([{ a: 1, b: 2 }]));
    expect(canonicalFleetOperationBytes(ab)).toBe(
      canonicalFleetOperationBytes(a) + canonicalFleetOperationBytes(b),
    );
    expect(digestFor([a, b])).not.toBe(digestFor([ab]));
    expect(digestFor([a, b])).not.toBe(digestFor([a]));
    expect(digestFor([a, b], { generation: 2 })).not.toBe(digestFor([a, b]));

    const oracle = createHash('sha256').update(
      canonicalFleetOperationBytes(envelope),
    );
    const encoder = new TextEncoder();
    for (const item of [a, b]) {
      const canonical = canonicalFleetOperationBytes(item);
      oracle
        .update(String(encoder.encode(canonical).byteLength))
        .update(':')
        .update(canonical);
    }
    expect(digestFor([a, b])).toBe(oracle.digest('hex'));
  });
});
