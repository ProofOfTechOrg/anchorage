// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  assertInventoryFindingValue,
  assertNoCredentialInInventoryText,
  canonicalFleetInventoryRunOptions,
  classifyFleetInventoryRunToken,
  emptyFleetInventoryRowCounts,
  type FleetInventoryRunOptions,
  type FleetInventoryRunRecord,
  FleetInventoryRunTokenError,
  FleetInventoryRunTokenFutureError,
  FleetInventoryRunTokenOperationError,
  type FleetInventoryStage,
  FleetInventoryStateError,
  fleetInventoryGenerationRefFromUnknown,
  fleetInventoryOptionsDigest,
  fleetInventoryRunRecordFromUnknown,
  fleetInventoryStagedFactFromUnknown,
  fleetInventoryStagedRowFromUnknown,
  fleetInventoryStageFromUnknown,
  InventoryFindingValueError,
  initialFleetInventoryStage,
  isInventoryKeyNameShape,
  nextStage,
  parseFleetInventoryRunToken,
} from '../src/fleet-inventory-state.js';

const OPERATION_ID = '123e4567-e89b-42d3-a456-426614174000';

const CALLER_OPTIONS = {
  hostRoutingKvId: 'kv-host-routing',
  databaseNamePrefix: 'anchorage-db',
  scriptNamePrefix: 'anchorage',
} as const;

function options(
  overrides: Partial<FleetInventoryRunOptions> = {},
): FleetInventoryRunOptions {
  return { ...canonicalFleetInventoryRunOptions(CALLER_OPTIONS), ...overrides };
}

function runRecord(
  overrides: Partial<FleetInventoryRunRecord> = {},
): FleetInventoryRunRecord {
  const runOptions = overrides.options ?? options();
  return {
    version: 1,
    operationId: OPERATION_ID,
    optionsDigest: fleetInventoryOptionsDigest(runOptions),
    options: runOptions,
    state: 'staging',
    progress: {
      stage: { step: 'host-kv-keys' },
      generation: 3,
      revision: 4,
      stagedCounts: emptyFleetInventoryRowCounts(),
      factCount: 0,
      providerRequests: 7,
    },
    updatedAt: '2026-08-29T00:00:00.000Z',
    ...overrides,
  };
}

function roundTrips(stages: readonly FleetInventoryStage[]): void {
  for (const stage of stages) {
    expect(fleetInventoryStageFromUnknown(structuredClone(stage))).toEqual(
      stage,
    );
  }
}

describe('fleet inventory state', () => {
  it('round-trips the host KV, dispatch, and registration stage variants', () => {
    roundTrips([
      { step: 'host-kv-keys' },
      { step: 'host-kv-keys', cursor: 'kv-cursor' },
      { step: 'host-kv-values', keyOrdinal: 12 },
      { step: 'dispatch-pages', pageOrdinal: 0 },
      { step: 'dispatch-pages', cursor: 'page-2', pageOrdinal: 1 },
      { step: 'registration-checks', registrationOrdinal: 4 },
      { step: 'registration-postprocess' },
    ]);
    expect(() =>
      fleetInventoryStageFromUnknown({ step: 'host-kv-values' }),
    ).toThrow(FleetInventoryStateError);
  });

  it('round-trips the custom-domain and zone stage variants', () => {
    roundTrips([
      { step: 'custom-domains' },
      { step: 'zone-authority' },
      { step: 'zone-routes', zoneOrdinal: 0 },
      { step: 'zone-routes', zoneOrdinal: 2 },
    ]);
    expect(() =>
      fleetInventoryStageFromUnknown({ step: 'zone-routes', zoneOrdinal: -1 }),
    ).toThrow(FleetInventoryStateError);
  });

  it('round-trips the ordinary-script and route-claim stage variants', () => {
    roundTrips([
      { step: 'ordinary-scripts' },
      { step: 'ordinary-scripts', cursor: 'script-cursor' },
      { step: 'ordinary-script-detail', scriptOrdinal: 9 },
      { step: 'route-claims' },
    ]);
    expect(() =>
      fleetInventoryStageFromUnknown({
        step: 'ordinary-scripts',
        cursor: 'authorization-token',
      }),
    ).toThrow(InventoryFindingValueError);
  });

  it('round-trips the D1, Durable Object, R2, and finalize stage variants', () => {
    roundTrips([
      { step: 'd1-databases' },
      { step: 'do-namespaces' },
      { step: 'r2-buckets', jurisdictionOrdinal: 0 },
      { step: 'r2-buckets', jurisdictionOrdinal: 2, startAfter: 'bucket-42' },
      { step: 'finalize' },
    ]);
    expect(() =>
      fleetInventoryStageFromUnknown({
        step: 'r2-buckets',
        jurisdictionOrdinal: 3,
      }),
    ).toThrow(FleetInventoryStateError);
  });

  it('round-trips a persisted run record', () => {
    const record = runRecord({
      progress: {
        stage: { step: 'r2-buckets', jurisdictionOrdinal: 1 },
        generation: 2,
        revision: 11,
        stagedCounts: { ...emptyFleetInventoryRowCounts(), finding: 3 },
        factCount: 5,
        lastPageDigest: 'a'.repeat(64),
        providerRequests: 41,
      },
    });
    expect(fleetInventoryRunRecordFromUnknown(structuredClone(record))).toEqual(
      record,
    );
  });

  it('refuses a run record with an unexpected or missing key', () => {
    expect(() =>
      fleetInventoryRunRecordFromUnknown({ ...runRecord(), extra: 1 }),
    ).toThrow(FleetInventoryStateError);
    const { updatedAt: _updatedAt, ...missing } = runRecord();
    expect(() => fleetInventoryRunRecordFromUnknown(missing)).toThrow(
      FleetInventoryStateError,
    );
    expect(() =>
      fleetInventoryRunRecordFromUnknown({
        ...runRecord(),
        optionsDigest: 'b'.repeat(64),
      }),
    ).toThrow(FleetInventoryStateError);
  });

  it('round-trips a finalized generation reference', () => {
    const ref = {
      generation: 7,
      operationId: OPERATION_ID,
      finalizedAtMs: 1_788_000_000_000,
      rowManifest: { ...emptyFleetInventoryRowCounts(), deployment: 2 },
      factCount: 6,
    };
    expect(
      fleetInventoryGenerationRefFromUnknown(structuredClone(ref)),
    ).toEqual(ref);
    expect(() =>
      fleetInventoryGenerationRefFromUnknown({ ...ref, generation: 0 }),
    ).toThrow(FleetInventoryStateError);
  });

  it('round-trips staged rows and facts with exact keys', () => {
    const row = {
      kind: 'finding' as const,
      ordinal: 3,
      payload: { detail: "registered script 'edge' could not be inspected" },
    };
    const fact = {
      deploymentOrdinal: 1,
      factKind: 'secret-name' as const,
      factOrdinal: 0,
      payload: { name: 'ANCHORAGE_TOKEN_NAME' },
    };
    expect(fleetInventoryStagedRowFromUnknown(structuredClone(row))).toEqual(
      row,
    );
    expect(fleetInventoryStagedFactFromUnknown(structuredClone(fact))).toEqual(
      fact,
    );
    expect(() =>
      fleetInventoryStagedRowFromUnknown({ ...row, extra: true }),
    ).toThrow(FleetInventoryStateError);
    expect(() =>
      fleetInventoryStagedFactFromUnknown({
        deploymentOrdinal: 1,
        factKind: 'secret-name',
        payload: {},
      }),
    ).toThrow(FleetInventoryStateError);
  });

  it('round-trips a bounded run token', () => {
    const token = {
      version: 1 as const,
      operationId: OPERATION_ID,
      revision: 2,
    };
    expect(parseFleetInventoryRunToken(structuredClone(token))).toEqual(token);
    expect(() =>
      parseFleetInventoryRunToken({ ...token, accountId: 'account' }),
    ).toThrow(FleetInventoryRunTokenError);
    expect(() =>
      parseFleetInventoryRunToken({ ...token, operationId: 'not-a-uuid' }),
    ).toThrow(FleetInventoryRunTokenError);
  });

  it('classifies a token generic, operation, future, then stale', () => {
    const record = runRecord();
    expect(() =>
      classifyFleetInventoryRunToken(
        { version: 2, operationId: OPERATION_ID, revision: 4 } as never,
        record,
      ),
    ).toThrow(FleetInventoryRunTokenError);
    expect(() =>
      classifyFleetInventoryRunToken(
        { version: 1, operationId: OPERATION_ID, revision: 4 },
        undefined,
      ),
    ).toThrow(FleetInventoryRunTokenOperationError);
    expect(() =>
      classifyFleetInventoryRunToken(
        { version: 1, operationId: OPERATION_ID, revision: 5 },
        record,
      ),
    ).toThrow(FleetInventoryRunTokenFutureError);
    expect(
      classifyFleetInventoryRunToken(
        { version: 1, operationId: OPERATION_ID, revision: 4 },
        record,
      ),
    ).toBe('current');
    expect(
      classifyFleetInventoryRunToken(
        { version: 1, operationId: OPERATION_ID, revision: 3 },
        record,
      ),
    ).toBe('stale');
  });

  it('refuses a run record above the record byte bound', () => {
    expect(() =>
      fleetInventoryRunRecordFromUnknown({
        ...runRecord(),
        filler: 'x'.repeat(200_000),
      }),
    ).toThrow(FleetInventoryStateError);
  });

  it('refuses an over-long string and an over-deep or over-wide payload', () => {
    expect(() =>
      fleetInventoryStageFromUnknown({
        step: 'host-kv-keys',
        cursor: 'c'.repeat(5_000),
      }),
    ).toThrow(FleetInventoryStateError);
    let deep: Record<string, unknown> = { leaf: 'value' };
    for (let depth = 0; depth < 70; depth += 1) deep = { nested: deep };
    expect(() =>
      fleetInventoryStagedRowFromUnknown({
        kind: 'meta',
        ordinal: 0,
        payload: deep,
      }),
    ).toThrow(FleetInventoryStateError);
    const wide: Record<string, number> = {};
    for (let index = 0; index < 9_000; index += 1) wide[`k${index}`] = index;
    expect(() =>
      fleetInventoryStagedRowFromUnknown({
        kind: 'meta',
        ordinal: 0,
        payload: wide,
      }),
    ).toThrow(FleetInventoryStateError);
  });

  it('refuses a staged row payload above the payload byte bound', () => {
    expect(() =>
      fleetInventoryStagedRowFromUnknown({
        kind: 'meta',
        ordinal: 0,
        payload: { blob: 'y'.repeat(20_000) },
      }),
    ).toThrow(FleetInventoryStateError);
  });

  it('canonicalizes options and resolves both defaults at start', () => {
    expect(canonicalFleetInventoryRunOptions(CALLER_OPTIONS)).toEqual({
      hostRoutingKvId: 'kv-host-routing',
      databaseNamePrefix: 'anchorage-db',
      scriptNamePrefix: 'anchorage',
      includeDispatchNamespace: true,
      includeR2Buckets: false,
    });
    expect(
      canonicalFleetInventoryRunOptions({
        databaseNamePrefix: 'anchorage-db',
        scriptNamePrefix: 'anchorage',
      }),
    ).toEqual({
      databaseNamePrefix: 'anchorage-db',
      scriptNamePrefix: 'anchorage',
      includeDispatchNamespace: false,
      includeR2Buckets: false,
    });
    expect(() =>
      canonicalFleetInventoryRunOptions({
        databaseNamePrefix: '',
        scriptNamePrefix: 'anchorage',
      }),
    ).toThrow(
      'databaseNamePrefix and scriptNamePrefix are required for fleet inventory',
    );
  });

  it('canonicalizes an empty host-routing KV id to absent', () => {
    const canonical = canonicalFleetInventoryRunOptions({
      hostRoutingKvId: '',
      databaseNamePrefix: 'anchorage-db',
      scriptNamePrefix: 'anchorage',
    });
    expect(canonical).toEqual({
      databaseNamePrefix: 'anchorage-db',
      scriptNamePrefix: 'anchorage',
      includeDispatchNamespace: false,
      includeR2Buckets: false,
    });
    expect(Object.hasOwn(canonical, 'hostRoutingKvId')).toBe(false);
    expect(initialFleetInventoryStage(canonical)).toEqual({
      step: 'custom-domains',
    });
  });

  it('keeps the options digest stable under caller key reorder', () => {
    const forward = canonicalFleetInventoryRunOptions({
      hostRoutingKvId: 'kv-host-routing',
      databaseNamePrefix: 'anchorage-db',
      scriptNamePrefix: 'anchorage',
      includeR2Buckets: true,
    });
    const reordered = canonicalFleetInventoryRunOptions({
      includeR2Buckets: true,
      scriptNamePrefix: 'anchorage',
      databaseNamePrefix: 'anchorage-db',
      hostRoutingKvId: 'kv-host-routing',
    });
    expect(fleetInventoryOptionsDigest(reordered)).toBe(
      fleetInventoryOptionsDigest(forward),
    );
  });

  it('changes the options digest on any option change', () => {
    const base = fleetInventoryOptionsDigest(options());
    const digests = [
      base,
      fleetInventoryOptionsDigest(options({ hostRoutingKvId: 'kv-other' })),
      fleetInventoryOptionsDigest(options({ databaseNamePrefix: 'other-db' })),
      fleetInventoryOptionsDigest(options({ scriptNamePrefix: 'other' })),
      fleetInventoryOptionsDigest(options({ includeDispatchNamespace: false })),
      fleetInventoryOptionsDigest(options({ includeR2Buckets: true })),
    ];
    expect(new Set(digests).size).toBe(digests.length);
  });

  it('refuses a row kind outside the staged-row vocabulary', () => {
    expect(() =>
      fleetInventoryStagedRowFromUnknown({
        kind: 'secret-value',
        ordinal: 0,
        payload: {},
      }),
    ).toThrow(FleetInventoryStateError);
  });

  it('refuses a fact kind outside the deployment-fact vocabulary', () => {
    expect(() =>
      fleetInventoryStagedFactFromUnknown({
        deploymentOrdinal: 0,
        factKind: 'secret-value',
        factOrdinal: 0,
        payload: {},
      }),
    ).toThrow(FleetInventoryStateError);
  });

  it('skips every host-routing stage when no KV id is configured', () => {
    const withoutKv = canonicalFleetInventoryRunOptions({
      databaseNamePrefix: 'anchorage-db',
      scriptNamePrefix: 'anchorage',
    });
    const counts = { ...emptyFleetInventoryRowCounts(), registration: 4 };
    expect(initialFleetInventoryStage(withoutKv)).toEqual({
      step: 'custom-domains',
    });
    const visited: string[] = [];
    let stage = initialFleetInventoryStage(withoutKv);
    while (stage.step !== 'finalize') {
      visited.push(stage.step);
      stage = nextStage(stage, withoutKv, counts);
    }
    expect(visited).not.toContain('host-kv-keys');
    expect(visited).not.toContain('host-kv-values');
    expect(visited).not.toContain('dispatch-pages');
    expect(visited).not.toContain('registration-checks');
    expect(visited).not.toContain('registration-postprocess');
  });

  it('keeps the registration-postprocess stage when includeDispatchNamespace is false', () => {
    const attestationOff = options({ includeDispatchNamespace: false });
    const counts = { ...emptyFleetInventoryRowCounts(), registration: 1 };
    expect(
      nextStage(
        { step: 'registration-checks', registrationOrdinal: 0 },
        attestationOff,
        counts,
      ),
    ).toEqual({ step: 'registration-postprocess' });
    expect(attestationOff.includeDispatchNamespace).toBe(false);
  });

  it('skips the R2 stage when includeR2Buckets is false', () => {
    const counts = emptyFleetInventoryRowCounts();
    expect(nextStage({ step: 'do-namespaces' }, options(), counts)).toEqual({
      step: 'finalize',
    });
    expect(
      nextStage(
        { step: 'do-namespaces' },
        options({ includeR2Buckets: true }),
        counts,
      ),
    ).toEqual({ step: 'r2-buckets', jurisdictionOrdinal: 0 });
  });

  it('rejects over-length, whitespace, and credential-shaped finding values', () => {
    for (const value of [
      'a'.repeat(513),
      'edge worker\nAuthorization: Bearer',
      'route pattern with space',
      'Authorization',
      'x-auth-key',
      'API_TOKEN=abc',
      'bearer-of-secrets',
    ]) {
      expect(() => assertInventoryFindingValue(value, 'detail')).toThrow(
        InventoryFindingValueError,
      );
    }
    expect(() =>
      assertInventoryFindingValue('a'.repeat(513), 'detail'),
    ).toThrow("inventory finding value for 'detail' is not durable-safe");
  });

  it('accepts printable inventory values and falls back for a hostile KV key', () => {
    const idnHostname = new URL('https://xn--bcher-kva.example').hostname;
    expect(() =>
      assertInventoryFindingValue(idnHostname, 'routeHostname'),
    ).not.toThrow();
    for (const key of ['tenant!prod%v+1=~x', 'tenant.example.test']) {
      expect(() => assertInventoryFindingValue(key, 'keyName')).not.toThrow();
      expect(isInventoryKeyNameShape(key)).toBe(true);
    }
    const hostile = 'tenant\u0000prod';
    expect(() =>
      assertNoCredentialInInventoryText(hostile, 'keyName'),
    ).not.toThrow();
    expect(isInventoryKeyNameShape(hostile)).toBe(false);
  });

  it('routes base64-shaped KV keys to the fallback and still refuses unsafe ones', () => {
    expect(isInventoryKeyNameShape('QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo')).toBe(
      false,
    );
    expect(isInventoryKeyNameShape('tenant-production.example.test')).toBe(
      true,
    );
    expect(
      isInventoryKeyNameShape('__anchorage_script__:anchorage-tenant-prod'),
    ).toBe(true);
    expect(() =>
      assertNoCredentialInInventoryText('k'.repeat(513), 'keyName'),
    ).toThrow(InventoryFindingValueError);
    expect(() =>
      assertNoCredentialInInventoryText('bearer-eyJhbGciOi', 'keyName'),
    ).toThrow(InventoryFindingValueError);
  });

  it('accepts long dotless script names and dispatch namespaces', () => {
    for (const value of [
      'my-long-script-name-12345678901234',
      'anchorage-dispatch-namespace-production-primary',
    ]) {
      expect(() =>
        assertInventoryFindingValue(value, 'scriptName'),
      ).not.toThrow();
    }
  });
});
