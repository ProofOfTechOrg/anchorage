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
  FLEET_OPERATION_SINGLE_UPDATE_ROW_MESSAGE,
  FLEET_OPERATION_STRING_BYTE_BOUND,
  FLEET_OPERATION_TOKEN_BYTE_BOUND,
  type FleetOperationRunRecord,
  type FleetOperationStagedRow,
  FleetOperationStateError,
  FleetOperationTokenError,
  FleetOperationTokenFutureError,
  FleetOperationTokenKindError,
  FleetOperationTokenOperationError,
  fleetOperationIntakeDigest,
  fleetOperationItemsIntake,
  fleetOperationOtherKindMessage,
  fleetOperationRunRecordFromUnknown,
  fleetOperationStagedRowFromUnknown,
  fleetOperationWatermarkRunMessage,
  isDurableAuditDetailSafe,
  parseFleetOperationToken,
} from '../src/fleet-operation-state.js';
import type { FleetRecord } from '../src/types.js';
import {
  FakeOperationStore,
  uuidFor,
} from './fixtures/fleet-operation-fakes.js';

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
      for (let presence = 0; presence < 8; presence += 1) {
        const admitted = migrationItem(
          status === 'complete' ? 'complete' : 'active',
        );
        if (!('plan' in admitted))
          throw new Error('missing admitted fixture fields');
        const candidate = {
          ...migrationItem('pending'),
          status,
          ...(presence & 1
            ? { targetSpecDigest: admitted.targetSpecDigest }
            : {}),
          ...(presence & 2 ? { plan: admitted.plan } : {}),
          ...(presence & 4 ? { planCursor: admitted.planCursor } : {}),
        };
        const accepted =
          status === 'pending'
            ? presence === 0
            : status === 'failed'
              ? presence === 0 || presence === 7
              : presence === 7;
        if (accepted) {
          expect(fleetMigrationItemFromUnknown(candidate)).toEqual(candidate);
        } else {
          expect(() => fleetMigrationItemFromUnknown(candidate)).toThrow(
            FleetOperationStateError,
          );
        }
      }
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

  it('record byte bound fails closed; the token codec refuses oversized and unknown-key input', () => {
    expect(() =>
      parseFleetOperationToken({
        version: 1,
        operationId:
          OPERATION_ID + 'x'.repeat(FLEET_OPERATION_TOKEN_BYTE_BOUND),
        revision: 0,
      }),
    ).toThrow(FleetOperationTokenError);
    // A tiny forbidden value, so the exact-key check is what refuses it.
    expect(() =>
      parseFleetOperationToken({
        version: 1,
        operationId: OPERATION_ID,
        revision: 0,
        padding: 'x',
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
  });

  it('the row codecs accept an empty finding detail and an empty or control-byte fact key on every fact kind, and reject a control-byte detail', () => {
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
      // The owner kinds admit the same keys through the same bounded
      // provider-text guard, but their arm asserts FOUR exact keys and
      // validates `tenantTag`/`environment` against the deployment grammar,
      // so the fixture has to carry grammar-valid values for those two or
      // the very first iteration refuses for the wrong reason.
      for (const factKind of ['database-owner', 'namespace-owner'] as const) {
        expect(
          fleetAuditFactRowFromUnknown({
            factKind,
            key,
            tenantTag: 'tenant',
            environment: 'production',
          }).key,
        ).toBe(key);
      }
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
        'operationId must be a lowercase UUIDv4',
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

  it('audit stage codec refuses a persisted cursor that is not a safe non-negative integer', () => {
    // The guard the forced-generation reader rests on: a cursor read back out
    // of D1 indexes the audited collection, so a fractional, negative or
    // out-of-range value has to fail the decode rather than the array read.
    for (const expectedOrdinal of [
      4.5,
      -1,
      Number.MAX_SAFE_INTEGER + 1,
      Number.NaN,
      Number.POSITIVE_INFINITY,
      '4',
      null,
    ]) {
      expect(() =>
        fleetAuditStageFromUnknown({
          step: 'r2-missing-identity',
          expectedOrdinal,
        }),
      ).toThrow(FleetOperationStateError);
    }
    expect(
      fleetAuditStageFromUnknown({
        step: 'r2-missing-identity',
        expectedOrdinal: 0,
      }),
    ).toEqual({ step: 'r2-missing-identity', expectedOrdinal: 0 });
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
    // Capped by the very array the next assertion compares against. This loop
    // is SYNCHRONOUS, so a non-progressing successor chain would never reach
    // vitest's test timeout — it would block the worker and grow `seen` until
    // the process died. The cap is the stage count, which is one slot of slack
    // over the 12 successors the chain actually needs.
    for (let step = 0; stage.step !== 'finalize'; step += 1) {
      if (step >= FLEET_AUDIT_STAGE_ORDER.length) {
        throw new Error(
          `nextAuditStage did not reach 'finalize' within ${FLEET_AUDIT_STAGE_ORDER.length} successors`,
        );
      }
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

    // A FRAMING oracle, not a canonicalization oracle: it reuses
    // `canonicalFleetOperationBytes` from the module under test, so it pins
    // the netstring framing and the hash composition and nothing about the
    // canonicalizer itself. That is pinned independently by the key-reorder
    // and concatenation assertions above.
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

// ---------------------------------------------------------------------------
// The shared FakeOperationStore's own contract, beside the port it implements
// rather than inside one of its consumers: a suite that drives the fake reads
// its guarded-progress behaviour from here.
// ---------------------------------------------------------------------------

describe('operation fake guarded progress contract', () => {
  const operationId = uuidFor(990);
  const initial = {
    version: 1,
    operationId,
    kind: 'migration',
    state: 'running',
    progress: {
      kind: 'migration',
      revision: 0,
      itemCount: 1,
      activeItemOrdinal: 0,
      completedItemCount: 0,
    },
    updatedAt: '2026-09-05T00:00:00.000Z',
  } as const;
  const intended: FleetOperationRunRecord = {
    ...initial,
    progress: { ...initial.progress, revision: 1 },
  };
  const row: FleetOperationStagedRow = {
    rowKind: 'item',
    ordinal: 0,
    payload: {
      ordinal: 0,
      tenantTag: 'fake',
      environment: 'production',
      entryRecordDigest: 'a'.repeat(64),
      status: 'pending',
    },
  };
  const different = {
    ...row,
    payload: { ...row.payload, tenantTag: 'other' },
  };

  it('loses the next successful progress response after a refused commit and accepts its retry', async () => {
    const store = new FakeOperationStore();
    store.operations.set(operationId, initial);
    store.heads.set('migration', operationId);
    const lostResponse = new Error('progress response lost');
    store.loseNextSuccessfulCommitProgressResponse = lostResponse;
    await store.withAccountOperationLease('migration', async (lease) => {
      const input = {
        operationId,
        expectedRevision: 0,
        runRecord: intended,
        rows: [row],
        expectedRowWatermarks: { item: 1 },
      };
      const refused = lease.commitProgress({ ...input, expectedRevision: 2 });
      await expect(refused).rejects.toBeInstanceOf(Error);
      await expect(refused).rejects.toHaveProperty(
        'message',
        `fleet operation '${operationId}' is no longer at the expected revision`,
      );
      expect(store.loseNextSuccessfulCommitProgressResponse).toBe(lostResponse);
      expect(store.operations.get(operationId)).toEqual(initial);
      expect(store.rows.size).toBe(0);
      expect(store.heads.get('migration')).toBe(operationId);

      await expect(lease.commitProgress(input)).rejects.toBe(lostResponse);
      expect(store.loseNextSuccessfulCommitProgressResponse).toBeUndefined();
      expect(store.operations.get(operationId)).toEqual(intended);
      expect(store.rows.get(`${operationId}:item`)).toEqual([row]);
      const operationsBeforeRetry = structuredClone([...store.operations]);
      const rowsBeforeRetry = structuredClone([...store.rows]);
      const headsBeforeRetry = [...store.heads];
      await expect(lease.commitProgress(input)).resolves.toEqual(intended);
      expect([...store.operations]).toEqual(operationsBeforeRetry);
      expect([...store.rows]).toEqual(rowsBeforeRetry);
      expect([...store.heads]).toEqual(headsBeforeRetry);
    });
  });

  it.each([
    { method: 'finalizeOperation', state: 'running' },
    { method: 'finalizeOperation', state: 'finalized' },
    { method: 'finalizeOperation', state: 'failed' },
    { method: 'failOperation', state: 'running' },
    { method: 'failOperation', state: 'finalized' },
    { method: 'failOperation', state: 'failed' },
  ] as const)('$method refuses a foreign-kind $state record using the captured lease', async ({
    method,
    state,
  }) => {
    const store = new FakeOperationStore();
    const source = state === 'running' ? initial : intended;
    store.rows.set(`${operationId}:item`, [row]);
    store.heads.set('migration', operationId);
    store.heads.set('audit', uuidFor(991));
    store.operations.set(
      operationId,
      fleetOperationRunRecordFromUnknown({
        ...source,
        state,
        progress: {
          ...source.progress,
          ...(state === 'failed'
            ? { failure: { reason: 'operator-abandoned' } }
            : {}),
          ...(state === 'finalized' ? { completedItemCount: 1 } : {}),
        },
      }),
    );
    const operationsBefore = structuredClone([...store.operations]);
    const rowsBefore = structuredClone([...store.rows]);
    const headsBefore = [...store.heads];
    await store.withAccountOperationLease('audit', async (lease) => {
      const runRecord = fleetOperationRunRecordFromUnknown({
        ...source,
        kind: 'audit',
        state: method === 'finalizeOperation' ? 'finalized' : 'failed',
        progress: {
          kind: 'audit',
          revision: 1,
          stage: { step: 'finalize' },
          generation: 1,
          auditTimeMs: 0,
          staleAfterMs: 60_000,
          recordCount: 0,
          findingCount: 0,
          factCount: 0,
          ...(method === 'failOperation'
            ? { failure: { reason: 'operator-abandoned' } }
            : {}),
        },
      });
      const input = { operationId, expectedRevision: 0, runRecord };
      const result =
        method === 'finalizeOperation'
          ? lease.finalizeOperation({ ...input, expectedRowCounts: {} })
          : lease.failOperation(input);
      await expect(result).rejects.toBeInstanceOf(Error);
      await expect(result).rejects.toHaveProperty(
        'message',
        fleetOperationOtherKindMessage(operationId),
      );
      expect([...store.operations]).toEqual(operationsBefore);
      expect([...store.rows]).toEqual(rowsBefore);
      expect([...store.heads]).toEqual(headsBefore);
    });
  });

  it.each([
    'missing-operation',
    'watermark',
    'other-record',
    'different-row',
    'missing-row',
    'converged',
  ] as const)('orders the %s convergence identity', async (variant) => {
    const store = new FakeOperationStore();
    if (variant !== 'missing-operation') {
      store.operations.set(
        operationId,
        variant === 'other-record'
          ? { ...intended, state: 'failed' }
          : intended,
      );
    }
    store.rows.set(
      `${operationId}:item`,
      variant === 'missing-row'
        ? []
        : [variant === 'converged' ? row : different],
    );
    const operationsBefore = structuredClone([...store.operations]);
    const rowsBefore = structuredClone([...store.rows]);
    await store.withAccountOperationLease('migration', async (lease) => {
      const commit = lease.commitProgress({
        operationId,
        expectedRevision: 0,
        runRecord: intended,
        updateRows:
          variant === 'different-row'
            ? [
                { ...row, ordinal: 1, payload: { ...row.payload, ordinal: 1 } },
                row,
              ]
            : [row],
        expectedRowWatermarks: {
          item: variant === 'watermark' ? 2 : variant === 'missing-row' ? 0 : 1,
        },
      });
      if (variant === 'converged') {
        await expect(commit).resolves.toEqual(intended);
      } else {
        const message =
          variant === 'missing-operation'
            ? `no fleet operation '${operationId}'`
            : variant === 'different-row'
              ? `fleet operation '${operationId}' staged rows diverge from the persisted operation`
              : `fleet operation '${operationId}' is no longer at the expected revision`;
        await expect(commit).rejects.toBeInstanceOf(Error);
        await expect(commit).rejects.toHaveProperty('message', message);
      }
    });
    expect([...store.operations]).toEqual(operationsBefore);
    expect([...store.rows]).toEqual(rowsBefore);
  });

  it('enforces watermark writer obligations', async () => {
    for (const insert of [false, true]) {
      const store = new FakeOperationStore();
      store.operations.set(operationId, initial);
      store.rows.set(`${operationId}:item`, [row]);
      await store.withAccountOperationLease('migration', async (lease) => {
        await expect(
          lease.commitProgress({
            operationId,
            expectedRevision: 0,
            runRecord: intended,
            ...(insert ? { rows: [different] } : {}),
            expectedRowWatermarks: { item: 2 },
          }),
        ).rejects.toThrow(
          insert
            ? fleetOperationWatermarkRunMessage('item')
            : `fleet operation '${operationId}' is no longer at the expected revision`,
        );
      });
      expect(store.operations.get(operationId)).toEqual(initial);
      expect(store.rows.get(`${operationId}:item`)).toEqual([row]);
    }
  });

  it('refuses noncontiguous rows and malformed mutations on stale-revision replay', async () => {
    const replay = new FakeOperationStore();
    const secondRow: FleetOperationStagedRow = {
      ...row,
      ordinal: 1,
      payload: { ...row.payload, ordinal: 1 },
    };
    const twoItems = {
      ...intended,
      progress: {
        ...initial.progress,
        revision: 1,
        itemCount: 2,
      },
    };
    replay.operations.set(operationId, twoItems);
    replay.rows.set(`${operationId}:item`, [row, secondRow]);
    await replay.withAccountOperationLease('migration', async (lease) => {
      await expect(
        lease.commitProgress({
          operationId,
          expectedRevision: 0,
          runRecord: twoItems,
          rows: [row],
          expectedRowWatermarks: { item: 2 },
        }),
      ).rejects.toThrow(fleetOperationWatermarkRunMessage('item'));
    });
    expect(replay.operations.get(operationId)).toEqual(twoItems);
    expect(replay.rows.get(`${operationId}:item`)).toEqual([row, secondRow]);
    for (const mutations of [
      { rows: [secondRow, secondRow] },
      { rows: [secondRow], updateRows: [secondRow] },
      { updateRows: [secondRow, secondRow] },
      { updateRows: [{ ...row, rowKind: 'record' as const }] },
    ]) {
      await replay.withAccountOperationLease('migration', async (lease) => {
        await expect(
          lease.commitProgress({
            operationId,
            expectedRevision: 0,
            runRecord: twoItems,
            expectedRowWatermarks: { item: 2 },
            ...mutations,
          }),
        ).rejects.toBeInstanceOf(FleetOperationStateError);
      });
      expect(replay.operations.get(operationId)).toEqual(twoItems);
      expect(replay.rows.get(`${operationId}:item`)).toEqual([row, secondRow]);
    }
  });

  it('refuses missing updates and different immutable bytes before sibling writes, and accepts exact retries', async () => {
    const secondRow = {
      ...row,
      ordinal: 1,
      payload: { ...row.payload, ordinal: 1 },
    };
    const updated = {
      ...secondRow,
      payload: { ...secondRow.payload, tenantTag: 'updated' },
    };
    const sibling = {
      ...row,
      ordinal: 2,
      payload: { ...row.payload, ordinal: 2 },
    };
    const missing = {
      ...row,
      ordinal: 3,
      payload: { ...row.payload, ordinal: 3 },
    };
    for (const variant of ['missing-update', 'different-insert', 'exact']) {
      const store = new FakeOperationStore();
      store.operations.set(operationId, initial);
      store.heads.set('migration', operationId);
      store.rows.set(`${operationId}:item`, [row, secondRow]);
      const input = {
        operationId,
        expectedRevision: 0,
        runRecord: intended,
        rows: [sibling, variant === 'different-insert' ? different : row],
        updateRows:
          variant === 'missing-update' ? [updated, missing] : [updated],
        expectedRowWatermarks: { item: 1 },
      };
      await store.withAccountOperationLease('migration', async (lease) => {
        const result = await lease
          .commitProgress(input)
          .catch((error: unknown) => error);
        if (variant === 'exact') {
          expect(store.operations.get(operationId)).toEqual(intended);
          expect(store.rows.get(`${operationId}:item`)).toEqual([
            row,
            updated,
            sibling,
          ]);
          expect(result).toEqual(intended);
          expect(await lease.commitProgress(input)).toEqual(intended);
          expect(store.operations.get(operationId)).toEqual(intended);
          expect(store.rows.get(`${operationId}:item`)).toEqual([
            row,
            updated,
            sibling,
          ]);
        } else {
          expect(store.operations.get(operationId)).toEqual(initial);
          expect(store.rows.get(`${operationId}:item`)).toEqual([
            row,
            secondRow,
          ]);
          expect(store.heads.get('migration')).toBe(operationId);
          expect(result).toBeInstanceOf(Error);
          if (variant === 'missing-update') {
            expect(result).toHaveProperty(
              'message',
              `fleet operation '${operationId}' is no longer at the expected revision`,
            );
          }
        }
      });
    }
  });

  it('compares record payloads canonically for fresh commits and convergence', async () => {
    const record: FleetRecord = {
      tenantTag: 'canonical',
      backend: 'plain-worker',
      environment: 'production',
      scriptName: 'canonical-worker',
      databaseId: 'db-canonical',
      databaseName: 'database-canonical',
      schemaVersion: 1,
      artifactVersion: 'v1',
      desiredSpecDigest: 'a'.repeat(64),
      durableObjectBindings: [
        { name: 'RUNNER', className: 'Runner', namespaceId: 'ns-canonical' },
      ],
      routeHostname: 'canonical.example.test',
      phase: 'ready',
      updatedAt: NOW,
    };
    const stored: FleetOperationStagedRow = {
      rowKind: 'record',
      ordinal: 0,
      payload: { ...record },
    };
    const reordered = {
      ...stored,
      payload: Object.fromEntries(Object.entries(record).reverse()),
    };
    expect(JSON.stringify(stored.payload)).not.toBe(
      JSON.stringify(reordered.payload),
    );
    const auditInitial = {
      ...initial,
      kind: 'audit' as const,
      progress: { kind: 'audit' as const, revision: 0 },
    };
    const auditIntended = {
      ...auditInitial,
      progress: { ...auditInitial.progress, revision: 1 },
    };
    const store = new FakeOperationStore();
    store.operations.set(operationId, auditInitial);
    store.rows.set(`${operationId}:record`, [stored]);
    await store.withAccountOperationLease('audit', async (lease) => {
      const input = {
        operationId,
        expectedRevision: 0,
        runRecord: auditIntended,
        rows: [reordered],
        expectedRowWatermarks: { record: 1 },
      };
      const error = await lease
        .commitProgress({
          ...input,
          rows: [
            { ...stored, ordinal: 1 },
            { ...stored, payload: { ...stored.payload, tenantTag: 'other' } },
          ],
        })
        .catch((error: unknown) => error);
      expect(store.operations.get(operationId)).toEqual(auditInitial);
      expect(store.rows.get(`${operationId}:record`)).toEqual([stored]);
      expect(error).toBeInstanceOf(Error);
      for (let retry = 0; retry < 2; retry += 1) {
        expect(await lease.commitProgress(input)).toEqual(auditIntended);
        expect(store.operations.get(operationId)).toEqual(auditIntended);
        expect(store.rows.get(`${operationId}:record`)).toEqual([stored]);
        expect(JSON.stringify(store.rows.get(`${operationId}:record`))).toBe(
          JSON.stringify([stored]),
        );
      }
    });
  });

  it('refuses multiple failure updates before changing rows or releasing the head', async () => {
    const secondRow = {
      ...row,
      ordinal: 1,
      payload: { ...row.payload, ordinal: 1 },
    };
    const store = new FakeOperationStore();
    store.operations.set(operationId, initial);
    store.heads.set('migration', operationId);
    store.rows.set(`${operationId}:item`, [row, secondRow]);
    await store.withAccountOperationLease('migration', async (lease) => {
      let error: unknown;
      try {
        await lease.failOperation({
          operationId,
          expectedRevision: 0,
          runRecord: { ...intended, state: 'failed' },
          updateRows: [row, secondRow].map((item) => ({
            ...item,
            payload: { ...item.payload, status: 'failed' },
          })),
        });
      } catch (caught) {
        error = caught;
      }
      expect(store.operations.get(operationId)).toEqual(initial);
      expect(store.rows.get(`${operationId}:item`)).toEqual([row, secondRow]);
      expect(store.heads.get('migration')).toBe(operationId);
      expect(error).toBeInstanceOf(Error);
      expect(error).toHaveProperty(
        'message',
        FLEET_OPERATION_SINGLE_UPDATE_ROW_MESSAGE,
      );
    });
  });
});
