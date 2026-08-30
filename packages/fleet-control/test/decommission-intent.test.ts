// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import { initialWorkerAttachmentScan } from '../src/cloudflare-worker-attachment-scan-state.js';
import type {
  AdvanceDecommissionDeploymentOptions,
  DecommissionAdvanceAction,
  DecommissionAdvanceCapability,
  DecommissionAdvanceResult,
} from '../src/decommission-advance.js';
import {
  classifyDecommissionAdvanceToken,
  DecommissionAdvanceIntentError,
  DecommissionAdvanceTokenDeploymentError,
  DecommissionAdvanceTokenError,
  DecommissionAdvanceTokenFutureError,
  DecommissionAdvanceTokenOperationError,
  decommissionAdvanceIntentFromUnknown,
  parseDecommissionAdvanceToken,
} from '../src/decommission-intent.js';
import {
  type ApplicationR2Resource,
  assertNoActiveDecommission,
  type DecommissionAdvanceIntent,
  type DecommissionAttachmentPurpose,
  effectiveLifecyclePhase,
  type FleetRecord,
  type NormalDecommissionLifecyclePhase,
  PROVISIONING_PHASES,
} from '../src/types.js';

const OPERATION_ID = '12345678-1234-4abc-8def-1234567890ab';
const DATABASE_ID = '00000000-0000-0000-0000-000000000001';
const RECEIPT_AUTHORITY = 'memory://fleet-exports/receipts/v1';
const NOW = '2026-08-29T12:00:00.000Z';
const DIGEST = 'a'.repeat(64);
const EVIDENCE = 'b'.repeat(64);
const INITIAL_PHASES = [
  'publishing',
  'ready',
  'migrating',
  'rolling-back',
] as const;
const TEARDOWN_PHASES = [
  'decommissioning',
  'traffic-removed',
  'credentials-revoked',
  'worker-deleted',
  'platform-credentials-revoked',
  'platform-resources-deleted',
  'application-resources-deleting',
  'application-resources-deleted',
  'database-exported',
  'database-deleting',
] as const;

function fleetRecord(overrides: Partial<FleetRecord> = {}): FleetRecord {
  return {
    tenantTag: 'acme',
    environment: 'production',
    backend: 'plain-worker',
    scriptName: 'acme-production',
    databaseId: DATABASE_ID,
    databaseName: 'acme-production',
    schemaVersion: 1,
    artifactVersion: 'version-1',
    desiredSpecDigest: DIGEST,
    durableObjectBindings: [],
    applicationResources: [
      {
        name: 'FILES',
        bucketName: 'acme-production-files',
        jurisdiction: 'default',
        state: 'detach-authorized',
        reservationNonce: 'x'.repeat(32),
        creationDate: '2026-08-29T00:00:00.000Z',
      },
    ],
    applicationBindings: { vars: [], secrets: [], r2Buckets: [] },
    routeHostname: 'acme.example.test',
    phase: 'decommission-advancing',
    databaseExportLocation: 'r2://exports/database.sqlite',
    databaseExportSha256: 'c'.repeat(64),
    databaseExportSize: 128,
    updatedAt: '2026-08-29T00:00:00.000Z',
    ...overrides,
  };
}

function identity(
  entryLifecyclePhase: NormalDecommissionLifecyclePhase = 'ready',
  source: FleetRecord = fleetRecord(),
  requestedSpecDigest = source.desiredSpecDigest,
) {
  return {
    record: {
      tenantTag: source.tenantTag,
      environment: source.environment,
      backend: source.backend,
      scriptName: source.scriptName,
      databaseId: source.databaseId,
      databaseName: source.databaseName,
      routeHostname: source.routeHostname,
    },
    mode: {
      kind: 'normal' as const,
      requestedSpecDigest,
      entryLifecyclePhase,
    },
  };
}

function common(
  lifecyclePhase: NormalDecommissionLifecyclePhase,
  overrides: Partial<{
    revision: number;
    generation: number;
    databaseExportReceiptAuthority: string;
  }> = {},
) {
  return {
    version: 1 as const,
    operationId: OPERATION_ID,
    revision: overrides.revision ?? 0,
    generation: overrides.generation ?? 0,
    updatedAt: NOW,
    identity: identity(),
    ...(overrides.databaseExportReceiptAuthority === undefined
      ? {}
      : {
          databaseExportReceiptAuthority:
            overrides.databaseExportReceiptAuthority,
        }),
    lifecyclePhase,
  };
}

function applicationResource(
  state: ApplicationR2Resource['state'] = 'detach-authorized',
): ApplicationR2Resource {
  return {
    name: 'FILES',
    bucketName: 'acme-production-files',
    jurisdiction: 'default',
    state,
    reservationNonce: 'x'.repeat(32),
    creationDate: '2026-08-29T00:00:00.000Z',
  };
}

function r2Purpose(): Extract<
  DecommissionAttachmentPurpose,
  { kind: 'application-r2-detach' }
> {
  return {
    kind: 'application-r2-detach',
    resourceIndex: 0,
    name: 'FILES',
    bucketName: 'acme-production-files',
    jurisdiction: 'default',
    reservationNonce: 'x'.repeat(32),
    creationDate: '2026-08-29T00:00:00.000Z',
  };
}

function parse(value: unknown, source: FleetRecord = fleetRecord()) {
  return decommissionAdvanceIntentFromUnknown(value, source);
}

describe('decommission advance intent', () => {
  it('round-trips every exact state arm', () => {
    const purpose = r2Purpose();
    const progress = initialWorkerAttachmentScan({
      kind: 'r2',
      bucketName: purpose.bucketName,
    });
    const source = fleetRecord();
    const arms: DecommissionAdvanceIntent[] = [
      { ...common('ready'), state: 'transitioning' },
      {
        ...common('application-resources-deleting'),
        state: 'discover',
        purpose,
        progress,
      },
      {
        ...common('application-resources-deleting'),
        state: 'verify',
        purpose,
        progress,
        discoverEvidence: { evidenceSha256: EVIDENCE, evidenceCount: 2 },
      },
      {
        ...common('application-resources-deleting'),
        state: 'blocked',
        purpose,
        attachment: {
          plane: 'dispatch',
          scriptName: 'foreign',
          dispatchNamespace: 'fleet',
        },
      },
    ];
    for (const arm of arms) {
      expect(parse(JSON.parse(JSON.stringify(arm)), source)).toEqual(arm);
    }
    const completeSource = fleetRecord({
      phase: 'decommissioned',
      applicationResources: [applicationResource('deleted')],
    });
    const complete = {
      ...common('ready'),
      databaseExportReceiptAuthority: RECEIPT_AUTHORITY,
      lifecyclePhase: 'decommissioned' as const,
      state: 'complete' as const,
    };
    expect(parse(complete, completeSource)).toEqual(complete);
  });

  it('rejects future, cross-arm, extra, missing, and non-plain shapes', () => {
    const valid = { ...common('ready'), state: 'transitioning' as const };
    const accessor = vi.fn(() => 'transitioning');
    const cyclic: { self?: unknown } = {};
    cyclic.self = cyclic;
    const transparent = new Proxy(valid, {});
    const revoked = Proxy.revocable(valid, {});
    revoked.revoke();
    for (const malformed of [
      { ...valid, version: 2 },
      { ...valid, extra: true },
      { ...valid, operationId: undefined },
      { ...valid, purpose: r2Purpose() },
      Object.assign(Object.create({ inherited: true }), valid),
      Object.defineProperty({ ...valid }, 'state', {
        enumerable: true,
        get: accessor,
      }),
      Object.assign({ ...valid }, { [Symbol('extra')]: true }),
      transparent,
      revoked.proxy,
      cyclic,
      { ...valid, databaseExportReceiptAuthority: RECEIPT_AUTHORITY },
    ]) {
      expect(() => parse(malformed)).toThrow(DecommissionAdvanceIntentError);
    }
    expect(accessor).not.toHaveBeenCalled();
  });

  it('rejects malformed identifiers, counters, timestamps, and wrapper bounds', () => {
    const valid = { ...common('ready'), state: 'transitioning' as const };
    for (const malformed of [
      { ...valid, operationId: 'not-a-uuid' },
      { ...valid, operationId: OPERATION_ID.toUpperCase() },
      { ...valid, operationId: '12345678-1234-5abc-8def-1234567890ab' },
      { ...valid, operationId: '12345678-1234-4abc-7def-1234567890ab' },
      { ...valid, revision: -1 },
      { ...valid, generation: Number.MAX_SAFE_INTEGER + 1 },
      { ...valid, updatedAt: 'yesterday' },
      { ...valid, updatedAt: '2026-08-29T12:00:00Z' },
    ]) {
      expect(() => parse(malformed)).toThrow(DecommissionAdvanceIntentError);
    }
    for (const boundary of ['x'.repeat(4096), 'é'.repeat(2048)]) {
      const source = fleetRecord({ routeHostname: boundary });
      const exact = {
        ...valid,
        identity: identity('ready', source),
      };
      expect(new TextEncoder().encode(boundary).byteLength).toBe(4096);
      expect(parse(exact, source)).toEqual(exact);
    }
    const overlong = 'x'.repeat(4097);
    expect(() =>
      parse(
        {
          ...valid,
          identity: {
            ...valid.identity,
            record: { ...valid.identity.record, routeHostname: overlong },
          },
        },
        fleetRecord({ routeHostname: overlong }),
      ),
    ).toThrow(DecommissionAdvanceIntentError);
    const overlongUtf8 = 'é'.repeat(2049);
    expect(new TextEncoder().encode(overlongUtf8).byteLength).toBe(4098);
    expect(() =>
      parse(
        {
          ...valid,
          identity: identity(
            'ready',
            fleetRecord({ routeHostname: overlongUtf8 }),
          ),
        },
        fleetRecord({ routeHostname: overlongUtf8 }),
      ),
    ).toThrow(DecommissionAdvanceIntentError);
    let exactError: unknown;
    try {
      parse({ ...valid, version: 2 });
    } catch (error) {
      exactError = error;
    }
    expect(exactError).toMatchObject({
      name: 'DecommissionAdvanceIntentError',
      message: 'decommission advance intent is malformed',
    });
    expect(exactError).toBeInstanceOf(DecommissionAdvanceIntentError);
  });

  it('rejects every immutable record identity mismatch', () => {
    const valid = { ...common('ready'), state: 'transitioning' as const };
    for (const [key, value] of Object.entries({
      tenantTag: 'other',
      environment: 'other',
      backend: 'workers-for-platforms',
      scriptName: 'other',
      databaseId: 'other',
      databaseName: 'other',
      routeHostname: 'other.example.test',
    })) {
      expect(() =>
        parse({
          ...valid,
          identity: {
            ...valid.identity,
            record: { ...valid.identity.record, [key]: value },
          },
        }),
      ).toThrow(DecommissionAdvanceIntentError);
    }
  });

  it('requires exact detach-authorized R2 purpose identity', () => {
    const purpose = r2Purpose();
    const valid = {
      ...common('application-resources-deleting'),
      state: 'discover' as const,
      purpose,
      progress: initialWorkerAttachmentScan({
        kind: 'r2' as const,
        bucketName: purpose.bucketName,
      }),
    };
    expect(parse(valid)).toEqual(valid);
    for (const malformed of [
      { ...valid, purpose: { ...purpose, resourceIndex: 1 } },
      { ...valid, purpose: { ...purpose, name: 'OTHER' } },
      { ...valid, purpose: { ...purpose, bucketName: 'other' } },
      { ...valid, purpose: { ...purpose, jurisdiction: 'eu' } },
      { ...valid, purpose: { ...purpose, reservationNonce: 'y'.repeat(32) } },
      {
        ...valid,
        purpose: { ...purpose, creationDate: '2026-08-28T00:00:00.000Z' },
      },
      {
        ...valid,
        progress: initialWorkerAttachmentScan({
          kind: 'r2',
          bucketName: 'other',
        }),
      },
    ]) {
      expect(() => parse(malformed)).toThrow(DecommissionAdvanceIntentError);
    }
    const reversedPurpose = {
      creationDate: purpose.creationDate,
      reservationNonce: purpose.reservationNonce,
      jurisdiction: purpose.jurisdiction,
      bucketName: purpose.bucketName,
      name: purpose.name,
      resourceIndex: purpose.resourceIndex,
      kind: purpose.kind,
    };
    expect(
      JSON.stringify(
        (parse({ ...valid, purpose: reversedPurpose }) as { purpose: unknown })
          .purpose,
      ),
    ).toBe(JSON.stringify(purpose));
    let hostileJurisdiction: unknown;
    try {
      parse({ ...valid, purpose: { ...purpose, jurisdiction: {} } });
    } catch (error) {
      hostileJurisdiction = error;
    }
    expect(hostileJurisdiction).toMatchObject({
      name: 'DecommissionAdvanceIntentError',
      message: 'decommission advance intent is malformed',
    });
    expect(hostileJurisdiction).toBeInstanceOf(DecommissionAdvanceIntentError);
    expect(() =>
      parse(
        valid,
        fleetRecord({
          applicationResources: [applicationResource('created')],
        }),
      ),
    ).toThrow(DecommissionAdvanceIntentError);
  });

  it('enforces the D1 purpose, lifecycle, and durable-export table', () => {
    const source = fleetRecord({
      applicationResources: [applicationResource('deleted')],
    });
    const exportPurpose = {
      kind: 'database-pre-export' as const,
      databaseId: DATABASE_ID,
    };
    const exportIntent = {
      ...common('application-resources-deleted', {
        databaseExportReceiptAuthority: RECEIPT_AUTHORITY,
      }),
      state: 'discover' as const,
      purpose: exportPurpose,
      progress: initialWorkerAttachmentScan({
        kind: 'd1',
        databaseId: DATABASE_ID,
      }),
    };
    expect(parse(exportIntent, source)).toEqual(exportIntent);
    const deletePurpose = {
      kind: 'database-pre-delete' as const,
      databaseId: DATABASE_ID,
      exportLocation: 'r2://exports/database.sqlite',
      exportSha256: 'c'.repeat(64),
      exportSize: 128,
    };
    const deleteIntent = {
      ...common('database-exported', {
        databaseExportReceiptAuthority: RECEIPT_AUTHORITY,
      }),
      state: 'discover' as const,
      purpose: deletePurpose,
      progress: initialWorkerAttachmentScan({
        kind: 'd1',
        databaseId: DATABASE_ID,
      }),
    };
    expect(parse(deleteIntent, source)).toEqual(deleteIntent);
    expect(
      parse({ ...deleteIntent, lifecyclePhase: 'database-deleting' }, source),
    ).toMatchObject({ lifecyclePhase: 'database-deleting' });
    for (const malformed of [
      {
        ...exportIntent,
        purpose: { ...exportPurpose, databaseId: 'other' },
      },
      { ...exportIntent, lifecyclePhase: 'ready' },
      {
        ...exportIntent,
        progress: initialWorkerAttachmentScan({
          kind: 'd1' as const,
          databaseId: 'other',
        }),
      },
      {
        ...deleteIntent,
        purpose: { ...deletePurpose, databaseId: 'other' },
      },
      {
        ...deleteIntent,
        purpose: { ...deletePurpose, exportLocation: 'r2://other' },
      },
      {
        ...deleteIntent,
        purpose: { ...deletePurpose, exportSha256: 'd'.repeat(64) },
      },
      {
        ...deleteIntent,
        purpose: { ...deletePurpose, exportSize: 129 },
      },
      { ...deleteIntent, lifecyclePhase: 'application-resources-deleted' },
      {
        ...deleteIntent,
        progress: initialWorkerAttachmentScan({
          kind: 'd1' as const,
          databaseId: 'other',
        }),
      },
    ]) {
      expect(() => parse(malformed, source)).toThrow(
        DecommissionAdvanceIntentError,
      );
    }
    for (const malformed of [
      {
        ...exportIntent,
        databaseExportReceiptAuthority: undefined,
      },
      (() => {
        const {
          databaseExportReceiptAuthority: _databaseExportReceiptAuthority,
          ...withoutAuthority
        } = exportIntent;
        return withoutAuthority;
      })(),
      {
        ...deleteIntent,
        databaseExportReceiptAuthority: '',
      },
      {
        ...deleteIntent,
        databaseExportReceiptAuthority: 'x'.repeat(4097),
      },
    ]) {
      expect(() => parse(malformed, source)).toThrow(
        DecommissionAdvanceIntentError,
      );
    }
    const exactAuthority = 'é'.repeat(2048);
    expect(new TextEncoder().encode(exactAuthority)).toHaveLength(4096);
    expect(
      parse(
        { ...exportIntent, databaseExportReceiptAuthority: exactAuthority },
        source,
      ),
    ).toMatchObject({ databaseExportReceiptAuthority: exactAuthority });
    const activeD1Shapes = [
      exportIntent,
      deleteIntent,
      {
        ...common('database-exported', {
          databaseExportReceiptAuthority: RECEIPT_AUTHORITY,
        }),
        state: 'transitioning' as const,
      },
      {
        ...common('database-deleting', {
          databaseExportReceiptAuthority: RECEIPT_AUTHORITY,
        }),
        state: 'transitioning' as const,
      },
      { ...deleteIntent, lifecyclePhase: 'database-deleting' as const },
      {
        ...deleteIntent,
        lifecyclePhase: 'database-deleting' as const,
        state: 'verify' as const,
        discoverEvidence: { evidenceSha256: EVIDENCE, evidenceCount: 2 },
      },
    ];
    const completeIntent = {
      ...common('ready'),
      databaseExportReceiptAuthority: RECEIPT_AUTHORITY,
      lifecyclePhase: 'decommissioned' as const,
      state: 'complete' as const,
    };
    for (const state of [
      'reserved',
      'create-authorized',
      'created',
      'detach-authorized',
      'detached',
      'empty-authorized',
      'empty',
      'delete-authorized',
    ] as const) {
      const malformedResources = [
        { ...applicationResource(), state } as ApplicationR2Resource,
      ];
      for (const shape of activeD1Shapes) {
        expect(() =>
          parse(
            shape,
            fleetRecord({ applicationResources: malformedResources }),
          ),
        ).toThrow(DecommissionAdvanceIntentError);
      }
      expect(() =>
        parse(
          completeIntent,
          fleetRecord({
            phase: 'decommissioned',
            applicationResources: malformedResources,
          }),
        ),
      ).toThrow(DecommissionAdvanceIntentError);
    }
  });

  it('rejects malformed, future, and target-mismatched R1 progress', () => {
    const purpose = r2Purpose();
    const valid = {
      ...common('application-resources-deleting'),
      state: 'discover' as const,
      purpose,
      progress: initialWorkerAttachmentScan({
        kind: 'r2',
        bucketName: purpose.bucketName,
      }),
    };
    for (const progress of [
      { ...valid.progress, version: 2 },
      { ...valid.progress, unexpected: true },
      initialWorkerAttachmentScan({ kind: 'r2', bucketName: 'other' }),
    ]) {
      expect(() => parse({ ...valid, progress })).toThrow(
        DecommissionAdvanceIntentError,
      );
    }
  });

  it('requires terminal-shaped discover evidence and purpose lifecycle', () => {
    const purpose = r2Purpose();
    const valid = {
      ...common('application-resources-deleting'),
      state: 'verify' as const,
      purpose,
      progress: initialWorkerAttachmentScan({
        kind: 'r2' as const,
        bucketName: purpose.bucketName,
      }),
      discoverEvidence: { evidenceSha256: EVIDENCE, evidenceCount: 2 },
    };
    expect(parse(valid)).toEqual(valid);
    expect(
      parse({
        ...valid,
        discoverEvidence: {
          ...valid.discoverEvidence,
          evidenceCount: 1_000_000,
        },
      }),
    ).toMatchObject({ discoverEvidence: { evidenceCount: 1_000_000 } });
    for (const malformed of [
      {
        ...valid,
        discoverEvidence: { ...valid.discoverEvidence, evidenceCount: 1 },
      },
      {
        ...valid,
        discoverEvidence: { ...valid.discoverEvidence, evidenceSha256: 'bad' },
      },
      {
        ...valid,
        discoverEvidence: {
          ...valid.discoverEvidence,
          evidenceCount: 1_000_001,
        },
      },
      { ...valid, lifecyclePhase: 'application-resources-deleted' },
    ]) {
      expect(() => parse(malformed)).toThrow(DecommissionAdvanceIntentError);
    }
  });

  it('enforces lifecycle reachability and outer phase relationships', () => {
    const valid = { ...common('ready'), state: 'transitioning' as const };
    expect(parse(valid)).toEqual(valid);
    const considered = [...INITIAL_PHASES, ...TEARDOWN_PHASES];
    for (const entry of INITIAL_PHASES) {
      for (const current of considered) {
        const initialSource =
          entry === 'migrating' && current === 'migrating'
            ? fleetRecord({
                pendingSpecDigest: 'd'.repeat(64),
                pendingArtifactVersion: 'candidate-v1',
              })
            : fleetRecord();
        const source = [
          'application-resources-deleted',
          'database-exported',
          'database-deleting',
        ].includes(current)
          ? {
              ...initialSource,
              applicationResources: [applicationResource('deleted')],
            }
          : initialSource;
        const requestedSpecDigest =
          entry === 'migrating' && current === 'migrating'
            ? 'd'.repeat(64)
            : source.desiredSpecDigest;
        const candidate = {
          ...common(current, {
            ...(['database-exported', 'database-deleting'].includes(current)
              ? { databaseExportReceiptAuthority: RECEIPT_AUTHORITY }
              : {}),
          }),
          identity: identity(entry, source, requestedSpecDigest),
          state: 'transitioning' as const,
        };
        if (current === entry || TEARDOWN_PHASES.includes(current as never)) {
          expect(parse(candidate, source)).toEqual(candidate);
        } else {
          expect(() => parse(candidate, source)).toThrow(
            DecommissionAdvanceIntentError,
          );
        }
      }
    }
    for (
      let entryIndex = 0;
      entryIndex < TEARDOWN_PHASES.length;
      entryIndex += 1
    ) {
      for (const current of considered) {
        const entry = TEARDOWN_PHASES[
          entryIndex
        ] as NormalDecommissionLifecyclePhase;
        const currentIndex = TEARDOWN_PHASES.indexOf(current as never);
        const source = [
          'application-resources-deleted',
          'database-exported',
          'database-deleting',
        ].includes(current)
          ? fleetRecord({
              applicationResources: [applicationResource('deleted')],
            })
          : fleetRecord();
        const candidate = {
          ...common(current, {
            ...(['database-exported', 'database-deleting'].includes(current)
              ? { databaseExportReceiptAuthority: RECEIPT_AUTHORITY }
              : {}),
          }),
          identity: identity(entry, source),
          state: 'transitioning' as const,
        };
        if (currentIndex >= entryIndex)
          expect(parse(candidate, source)).toEqual(candidate);
        else
          expect(() => parse(candidate, source)).toThrow(
            DecommissionAdvanceIntentError,
          );
      }
    }
    for (const lifecyclePhase of PROVISIONING_PHASES.filter(
      (phase) =>
        ![
          'publishing',
          'ready',
          'migrating',
          'rolling-back',
          ...[
            'decommissioning',
            'traffic-removed',
            'credentials-revoked',
            'worker-deleted',
            'platform-credentials-revoked',
            'platform-resources-deleted',
            'application-resources-deleting',
            'application-resources-deleted',
            'database-exported',
            'database-deleting',
          ],
        ].includes(phase),
    )) {
      expect(() => parse({ ...valid, lifecyclePhase })).toThrow(
        DecommissionAdvanceIntentError,
      );
    }
    expect(() => parse(valid, fleetRecord({ phase: 'ready' }))).toThrow(
      DecommissionAdvanceIntentError,
    );
    expect(() => parse({ ...valid, identity: identity('publishing') })).toThrow(
      DecommissionAdvanceIntentError,
    );
    const missingShell = fleetRecord({
      phase: 'decommission-advancing',
      decommissionIntent: undefined,
    });
    expect(() => effectiveLifecyclePhase(missingShell)).toThrow(
      'decommission-advancing record has no active decommission intent',
    );
    const malformedComplete = fleetRecord({
      phase: 'ready',
      decommissionIntent: {
        ...common('ready'),
        databaseExportReceiptAuthority: RECEIPT_AUTHORITY,
        lifecyclePhase: 'decommissioned',
        state: 'complete',
      },
    });
    expect(() => assertNoActiveDecommission(malformedComplete, 'test')).toThrow(
      'test cannot run during an active decommission',
    );
  });

  it('validates migration carriers and rejects reserved backend-switch mode', () => {
    const pendingSpecDigest = 'd'.repeat(64);
    const migrating = fleetRecord({
      backend: 'plain-worker',
      pendingSpecDigest,
      pendingArtifactVersion: 'pending-version',
    });
    const valid = {
      ...common('migrating'),
      identity: {
        ...identity('migrating'),
        mode: {
          kind: 'normal' as const,
          requestedSpecDigest: pendingSpecDigest,
          entryLifecyclePhase: 'migrating' as const,
        },
      },
      state: 'transitioning' as const,
    };
    expect(parse(valid, migrating)).toEqual(valid);
    expect(parse(valid, fleetRecord({ pendingSpecDigest }))).toEqual(valid);
    for (const pendingArtifactVersion of ['', 'pending', 42]) {
      expect(() =>
        parse(valid, {
          ...migrating,
          pendingArtifactVersion: pendingArtifactVersion as never,
        }),
      ).toThrow(DecommissionAdvanceIntentError);
    }
    expect(() =>
      parse({ ...valid, lifecyclePhase: 'decommissioning' }, migrating),
    ).toThrow(DecommissionAdvanceIntentError);
    expect(() =>
      parse({ ...valid, identity: identity('migrating') }, migrating),
    ).toThrow(DecommissionAdvanceIntentError);
    expect(() =>
      parse({
        ...valid,
        identity: {
          ...valid.identity,
          mode: {
            kind: 'backend-switch',
            priorSpecDigest: DIGEST,
            targetSpecDigest: DIGEST,
            decommissionSnapshotSha256: DIGEST,
            backendSwitchSubphase: 'decommission-application-r2-authorized',
          },
        },
      }),
    ).toThrow(DecommissionAdvanceIntentError);
    expect(() =>
      parse(
        { ...common('ready'), state: 'transitioning' },
        fleetRecord({ pendingSpecDigest: 'd'.repeat(64) }),
      ),
    ).toThrow(DecommissionAdvanceIntentError);
    expect(() =>
      parse(valid, fleetRecord({ migrationIntent: {} as never })),
    ).toThrow(DecommissionAdvanceIntentError);

    const wfpMigration = fleetRecord({
      backend: 'workers-for-platforms',
      pendingSpecDigest,
      migrationIntent: { targetSpecDigest: pendingSpecDigest } as never,
    });
    const wfpValid = {
      ...valid,
      identity: identity('migrating', wfpMigration, pendingSpecDigest),
    };
    expect(parse(wfpValid, wfpMigration)).toEqual(wfpValid);
    expect(() =>
      parse(wfpValid, { ...wfpMigration, migrationIntent: undefined }),
    ).toThrow(DecommissionAdvanceIntentError);
    expect(() =>
      parse(wfpValid, {
        ...wfpMigration,
        pendingSpecDigest: 'e'.repeat(64),
      }),
    ).toThrow(DecommissionAdvanceIntentError);

    const advanced = {
      ...common('decommissioning'),
      identity: {
        ...identity('migrating'),
        mode: {
          kind: 'normal' as const,
          requestedSpecDigest: DIGEST,
          entryLifecyclePhase: 'migrating' as const,
        },
      },
      state: 'transitioning' as const,
    };
    expect(parse(advanced)).toEqual(advanced);
    expect(() =>
      parse({
        ...advanced,
        identity: {
          ...advanced.identity,
          mode: {
            ...advanced.identity.mode,
            requestedSpecDigest: 'f'.repeat(64),
          },
        },
      }),
    ).toThrow(DecommissionAdvanceIntentError);
  });

  it('requires an exact safe blocked attachment union', () => {
    const base = {
      ...common('application-resources-deleting'),
      state: 'blocked' as const,
      purpose: r2Purpose(),
    };
    expect(
      parse({ ...base, attachment: { plane: 'ordinary', scriptName: 'one' } }),
    ).toMatchObject({
      attachment: { plane: 'ordinary', scriptName: 'one' },
    });
    for (const attachment of [
      { plane: 'ordinary', scriptName: 'one', dispatchNamespace: 'fleet' },
      { plane: 'dispatch', scriptName: 'one' },
      { plane: 'dispatch', scriptName: '', dispatchNamespace: 'fleet' },
      {
        plane: 'dispatch',
        scriptName: 'one',
        dispatchNamespace: 'fleet',
        token: 'secret',
      },
    ]) {
      expect(() => parse({ ...base, attachment })).toThrow(
        DecommissionAdvanceIntentError,
      );
    }
  });

  it('requires evidence-free terminal state and complete record invariants', () => {
    const source = fleetRecord({
      backend: 'workers-for-platforms',
      phase: 'decommissioned',
      applicationResources: [applicationResource('deleted')],
      activeRelease: {
        physicalScriptName: 'acme-production-aaaaaaaaaaaaaaaaaaaa',
        specDigest: DIGEST,
        artifactVersion: 'version-1',
        releaseSchemaVersion: 1,
      },
    });
    const valid = {
      ...common('ready'),
      identity: identity('ready', source),
      databaseExportReceiptAuthority: RECEIPT_AUTHORITY,
      lifecyclePhase: 'decommissioned' as const,
      state: 'complete' as const,
    };
    expect(parse(valid, source)).toEqual(valid);
    const {
      databaseExportReceiptAuthority: _databaseExportReceiptAuthority,
      ...missingReceiptAuthority
    } = valid;
    expect(() => parse(missingReceiptAuthority, source)).toThrow(
      DecommissionAdvanceIntentError,
    );
    expect(() =>
      parse({ ...valid, databaseExportReceiptAuthority: undefined }, source),
    ).toThrow(DecommissionAdvanceIntentError);
    expect(() =>
      parse(
        {
          ...valid,
          discoverEvidence: { evidenceSha256: EVIDENCE, evidenceCount: 2 },
        },
        source,
      ),
    ).toThrow(DecommissionAdvanceIntentError);
    expect(() =>
      parse(valid, { ...source, databaseExportSize: undefined }),
    ).toThrow(DecommissionAdvanceIntentError);
    for (const malformedSource of [
      { ...source, phase: 'ready' as const },
      { ...source, applicationResources: [applicationResource('created')] },
      { ...source, databaseExportLocation: undefined },
      { ...source, databaseExportSha256: undefined },
      { ...source, pendingSpecDigest: 'd'.repeat(64) },
      { ...source, pendingArtifactVersion: 'pending-version' },
      { ...source, pendingRelease: {} as never },
      { ...source, migrationPriorRelease: {} as never },
      { ...source, rollbackRelease: {} as never },
      { ...source, retiringRelease: {} as never },
      { ...source, migrationIntent: {} as never },
      { ...source, backendSwitchIntent: {} as never },
    ]) {
      expect(() => parse(valid, malformedSource)).toThrow(
        DecommissionAdvanceIntentError,
      );
    }
    expect(() =>
      parse(
        {
          ...valid,
          identity: {
            ...valid.identity,
            mode: {
              kind: 'normal',
              requestedSpecDigest: 'f'.repeat(64),
              entryLifecyclePhase: 'migrating',
            },
          },
        },
        source,
      ),
    ).toThrow(DecommissionAdvanceIntentError);
  });

  it('parses exact secret-negative continuation tokens', () => {
    const valid = {
      version: 1 as const,
      tenantTag: 'acme',
      environment: 'production',
      operationId: OPERATION_ID,
      revision: 3,
    };
    expect(parseDecommissionAdvanceToken(valid)).toEqual(valid);
    const transparent = new Proxy(valid, {});
    const revoked = Proxy.revocable(valid, {});
    revoked.revoke();
    expect(() => parseDecommissionAdvanceToken(transparent)).toThrow(
      DecommissionAdvanceTokenError,
    );
    expect(() => parseDecommissionAdvanceToken(revoked.proxy)).toThrow(
      DecommissionAdvanceTokenError,
    );
    const actions: readonly DecommissionAdvanceAction[] = [
      { kind: 'start' },
      { kind: 'continue', token: valid },
      { kind: 'restart-blocked', token: valid },
    ];
    const capabilities: readonly DecommissionAdvanceCapability[] = [
      'attachment-scan',
      'database-residuals',
      'application-r2-inspection',
      'application-r2-empty',
      'application-r2-delete',
      'database-export-receipt',
    ];
    const result: DecommissionAdvanceResult = {
      status: 'pending',
      token: valid,
    };
    type AdvanceAction = AdvanceDecommissionDeploymentOptions['action'];
    const action: AdvanceAction = actions[0] as DecommissionAdvanceAction;
    expect({ actions, capabilities, result, action }).toMatchObject({
      result: { status: 'pending', token: valid },
    });
    const tokenAtSerializedBytes = (byteLength: number) => {
      const current = new TextEncoder().encode(
        JSON.stringify(valid),
      ).byteLength;
      const token = {
        ...valid,
        tenantTag: `${valid.tenantTag}${'x'.repeat(byteLength - current)}`,
      };
      expect(new TextEncoder().encode(JSON.stringify(token)).byteLength).toBe(
        byteLength,
      );
      return token;
    };
    const exactBound = tokenAtSerializedBytes(1024);
    expect(parseDecommissionAdvanceToken(exactBound)).toEqual(exactBound);
    expect(() =>
      parseDecommissionAdvanceToken(tokenAtSerializedBytes(1025)),
    ).toThrow(DecommissionAdvanceTokenError);
    expect(JSON.stringify(valid)).not.toMatch(
      /cursor|evidence|database|token/iu,
    );
    for (const malformed of [
      { ...valid, version: 2 },
      { ...valid, cursor: 'secret' },
      { ...valid, revision: -1 },
      { ...valid, operationId: 'bad' },
      { ...valid, operationId: OPERATION_ID.toUpperCase() },
      { ...valid, operationId: '12345678-1234-5abc-8def-1234567890ab' },
      { ...valid, tenantTag: 'x'.repeat(600), environment: 'y'.repeat(600) },
    ]) {
      expect(() => parseDecommissionAdvanceToken(malformed)).toThrow(
        DecommissionAdvanceTokenError,
      );
    }
    let exactError: unknown;
    try {
      parseDecommissionAdvanceToken({ ...valid, version: 2 });
    } catch (error) {
      exactError = error;
    }
    expect(exactError).toMatchObject({
      name: 'DecommissionAdvanceTokenError',
      message: 'decommission advance token is malformed',
    });
    expect(exactError).toBeInstanceOf(DecommissionAdvanceTokenError);
  });

  it('classifies current, stale, future, deployment, operation, and complete tokens', () => {
    const intent = {
      ...common('ready', { revision: 3 }),
      state: 'transitioning' as const,
    };
    const source = fleetRecord({ decommissionIntent: intent });
    const token = {
      version: 1 as const,
      tenantTag: 'acme',
      environment: 'production',
      operationId: OPERATION_ID,
      revision: 3,
    };
    expect(classifyDecommissionAdvanceToken(token, source)).toBe('current');
    expect(
      classifyDecommissionAdvanceToken({ ...token, revision: 2 }, source),
    ).toBe('stale');
    expect(() =>
      classifyDecommissionAdvanceToken({ ...token, revision: 4 }, source),
    ).toThrow(DecommissionAdvanceTokenFutureError);
    expect(() =>
      classifyDecommissionAdvanceToken(
        { ...token, tenantTag: 'other' },
        source,
      ),
    ).toThrow(DecommissionAdvanceTokenDeploymentError);
    expect(() =>
      classifyDecommissionAdvanceToken(
        { ...token, environment: 'other' },
        source,
      ),
    ).toThrow(DecommissionAdvanceTokenDeploymentError);
    expect(() =>
      classifyDecommissionAdvanceToken(
        { ...token, operationId: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa' },
        source,
      ),
    ).toThrow(DecommissionAdvanceTokenOperationError);
    expect(() =>
      classifyDecommissionAdvanceToken(token, fleetRecord()),
    ).toThrow(DecommissionAdvanceTokenOperationError);
    for (const [operation, Expected, message] of [
      [
        () =>
          classifyDecommissionAdvanceToken({ ...token, revision: 4 }, source),
        DecommissionAdvanceTokenFutureError,
        'decommission advance token is from the future',
      ],
      [
        () =>
          classifyDecommissionAdvanceToken(
            { ...token, environment: 'other' },
            source,
          ),
        DecommissionAdvanceTokenDeploymentError,
        'decommission advance token targets another deployment',
      ],
      [
        () => classifyDecommissionAdvanceToken(token, fleetRecord()),
        DecommissionAdvanceTokenOperationError,
        'decommission advance token targets another operation',
      ],
    ] as const) {
      let refusal: unknown;
      try {
        operation();
      } catch (error) {
        refusal = error;
      }
      expect(refusal).toBeInstanceOf(Expected);
      expect((refusal as Error).message).toBe(message);
    }
    const completeSource = fleetRecord({
      phase: 'decommissioned',
      applicationResources: [applicationResource('deleted')],
      decommissionIntent: {
        ...intent,
        databaseExportReceiptAuthority: RECEIPT_AUTHORITY,
        lifecyclePhase: 'decommissioned',
        state: 'complete',
      },
    });
    expect(
      classifyDecommissionAdvanceToken(
        { ...token, revision: 2 },
        completeSource,
      ),
    ).toBe('stale');
    expect(classifyDecommissionAdvanceToken(token, completeSource)).toBe(
      'current',
    );
    expect(() =>
      classifyDecommissionAdvanceToken(
        { ...token, revision: 4 },
        completeSource,
      ),
    ).toThrow(DecommissionAdvanceTokenFutureError);
    expect(() =>
      classifyDecommissionAdvanceToken(
        { ...token, tenantTag: 'other' },
        fleetRecord(),
      ),
    ).toThrow(DecommissionAdvanceTokenDeploymentError);
  });
});
