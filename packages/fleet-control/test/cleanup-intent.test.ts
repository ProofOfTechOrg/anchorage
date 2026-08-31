// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  CLEANUP_INTENT_BYTE_BOUND,
  CleanupAdvanceIntentError,
  CleanupAdvanceTokenDeploymentError,
  CleanupAdvanceTokenError,
  CleanupAdvanceTokenFutureError,
  CleanupAdvanceTokenOperationError,
  CleanupTerminalReceiptError,
  canonicalCleanupEvidenceBytes,
  classifyCleanupAdvanceToken,
  classifyCleanupDatabaseEligibility,
  cleanupAdvanceIntentFromUnknown,
  cleanupTerminalReceiptFromUnknown,
  InvocationAuthorityCarrierError,
  invocationAuthorityCarrierFromUnknown,
  parseCleanupAdvanceToken,
} from '../src/cleanup-intent.js';
import { initialWorkerAttachmentScan } from '../src/cloudflare-worker-attachment-scan-state.js';
import {
  DecommissionAdvanceIntentError,
  decommissionAdvanceIntentFromUnknown,
} from '../src/decommission-intent.js';
import type {
  CleanupAdvanceIntent,
  CleanupAdvanceState,
  CleanupAttachmentPurpose,
  CleanupAuthority,
  CleanupReceiptEvidence,
  CleanupTerminalReceipt,
  DecommissionAdvanceIntent,
  ExternalMigrationIntent,
  ExternalPlatformTargetDescription,
  ExternalReleaseSnapshot,
  FleetRecord,
  ProvisioningPhase,
} from '../src/types.js';
import { PROVISIONING_PHASES } from '../src/types.js';

const OPERATION_ID = '12345678-1234-4abc-8def-1234567890ab';
const OTHER_OPERATION_ID = '87654321-4321-4abc-8def-ba0987654321';
const DATABASE_ID = '00000000-0000-0000-0000-000000000001';
const RECEIPT_AUTHORITY = 'memory://fleet-exports/receipts/v1';
const NOW = '2026-08-29T12:00:00.000Z';
const DIGEST = 'a'.repeat(64);
const EVIDENCE = 'b'.repeat(64);
const ADMITTED_PHASES = [
  'database-reserved',
  'database-create-authorized',
  'database-created',
  'identity-seeded',
  'migrated',
  'application-resources-create-authorized',
  'application-resources-deployed',
  'platform-resources-deployed',
  'worker-deployed',
  'maintenance-armed',
] as const;
const RESERVATION_PHASES = [
  'database-reserved',
  'database-create-authorized',
] as const;
const LEGACY_IMPOSSIBLE_PHASES = [
  'database-created',
  'identity-seeded',
  'migrated',
  'application-resources-create-authorized',
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
    applicationResources: [],
    applicationBindings: { vars: [], secrets: [], r2Buckets: [] },
    routeHostname: 'acme.example.test',
    phase: 'cleanup-advancing',
    updatedAt: '2026-08-29T00:00:00.000Z',
    ...overrides,
  };
}

function identity(
  overrides: Partial<CleanupAdvanceIntent['identity']> = {},
  source: FleetRecord = fleetRecord(),
): CleanupAdvanceIntent['identity'] {
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
    admittedPhase: 'worker-deployed',
    externalArtifact: false,
    ...overrides,
  };
}

function intent(
  state: CleanupAdvanceState,
  overrides: Partial<CleanupAdvanceIntent> = {},
): CleanupAdvanceIntent {
  return {
    version: 1,
    operationId: OPERATION_ID,
    revision: 0,
    generation: 0,
    updatedAt: NOW,
    authority: { kind: 'manual-cleanup' },
    identity: identity(),
    state,
    ...overrides,
  };
}

function purpose(): CleanupAttachmentPurpose {
  return {
    kind: 'cleanup-database-pre-delete',
    databaseId: DATABASE_ID,
    operationId: OPERATION_ID,
  };
}

function scanProgress() {
  return initialWorkerAttachmentScan({ kind: 'd1', databaseId: DATABASE_ID });
}

function parse(value: unknown, source: FleetRecord = fleetRecord()) {
  return cleanupAdvanceIntentFromUnknown(value, source);
}

function rollbackAuthority(): CleanupAuthority {
  return {
    kind: 'provisioning-rollback',
    reservationOwned: true,
    databaseOwned: true,
    workerCreatedByAttempt: false,
    workerResourceState: 'absent',
    requestedSpecDigest: DIGEST,
  };
}

function decommissionRecord(): FleetRecord {
  return fleetRecord({ phase: 'decommission-advancing' });
}

function decommissionDiscoverIntent(): DecommissionAdvanceIntent {
  const source = decommissionRecord();
  return {
    version: 1,
    operationId: OPERATION_ID,
    revision: 0,
    generation: 0,
    updatedAt: NOW,
    identity: {
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
        kind: 'normal',
        requestedSpecDigest: source.desiredSpecDigest,
        entryLifecyclePhase: 'ready',
      },
    },
    databaseExportReceiptAuthority: RECEIPT_AUTHORITY,
    lifecyclePhase: 'application-resources-deleted',
    state: 'discover',
    purpose: { kind: 'database-pre-export', databaseId: DATABASE_ID },
    progress: scanProgress(),
  };
}

function receiptEvidence(): CleanupReceiptEvidence {
  return {
    eligibility: 'carrier-null',
    ingressRemoved: true,
    workerAbsent: true,
    platformResourcesAbsent: true,
    applicationR2Settled: true,
    databaseAbsentReadback: true,
    scan: {
      discover: { evidenceSha256: EVIDENCE, evidenceCount: 2 },
      verify: { evidenceSha256: EVIDENCE, evidenceCount: 2 },
    },
  };
}

function terminalReceipt(
  overrides: Partial<CleanupTerminalReceipt> = {},
): CleanupTerminalReceipt {
  const source = fleetRecord();
  return {
    version: 1,
    operationId: OPERATION_ID,
    tenantTag: source.tenantTag,
    environment: source.environment,
    backend: source.backend,
    scriptName: source.scriptName,
    databaseId: source.databaseId,
    databaseName: source.databaseName,
    authority: 'manual-cleanup',
    admittedPhase: 'worker-deployed',
    disposition: 'prepublication-owned-no-export',
    evidence: receiptEvidence(),
    ...overrides,
  };
}

describe('cleanup advance intent codec', () => {
  it('round-trips the teardown-traffic state', () => {
    const value = intent({ step: 'teardown-traffic' });
    expect(parse(value)).toEqual(value);
    expect(() =>
      parse({ ...value, state: { step: 'teardown-traffic', extra: 1 } }),
    ).toThrow(CleanupAdvanceIntentError);
  });

  it('round-trips the teardown-worker state', () => {
    const value = intent(
      { step: 'teardown-worker' },
      { authority: rollbackAuthority() },
    );
    expect(parse(value)).toEqual(value);
    expect(() =>
      parse({
        ...value,
        state: { step: 'teardown-worker', startResourceIndex: 0 },
      }),
    ).toThrow(CleanupAdvanceIntentError);
  });

  it('round-trips the teardown-platform state', () => {
    const value = intent({ step: 'teardown-platform' }, { revision: 3 });
    expect(parse(value)).toEqual(value);
    expect(() =>
      parse({ ...value, state: { step: 'teardown-platform', scan: {} } }),
    ).toThrow(CleanupAdvanceIntentError);
  });

  it('round-trips the r2-deletion state with and without a verified detachment index', () => {
    const bare = intent({ step: 'r2-deletion', startResourceIndex: 1 });
    expect(parse(bare)).toEqual(bare);
    const verified = intent({
      step: 'r2-deletion',
      startResourceIndex: 1,
      verifiedDetachmentResourceIndex: 1,
    });
    expect(parse(verified)).toEqual(verified);
    expect(() =>
      parse(intent({ step: 'r2-deletion', startResourceIndex: -1 })),
    ).toThrow(CleanupAdvanceIntentError);
    expect(() =>
      parse(
        intent({
          step: 'r2-deletion',
          startResourceIndex: 0,
          verifiedDetachmentResourceIndex: 1.5,
        }),
      ),
    ).toThrow(CleanupAdvanceIntentError);
    expect(() =>
      parse({
        ...bare,
        state: { step: 'r2-deletion', startResourceIndex: 0, extra: true },
      }),
    ).toThrow(CleanupAdvanceIntentError);
  });

  it('round-trips the attachment-scan discover and verify passes', () => {
    const discover = intent({
      step: 'attachment-scan',
      scan: { purpose: purpose(), pass: 'discover', progress: scanProgress() },
    });
    expect(parse(discover)).toEqual(discover);
    const verify = intent({
      step: 'attachment-scan',
      scan: {
        purpose: purpose(),
        pass: 'verify',
        progress: scanProgress(),
        discoverEvidence: { evidenceSha256: EVIDENCE, evidenceCount: 2 },
      },
    });
    expect(parse(verify)).toEqual(verify);
    expect(() =>
      parse(
        intent({
          step: 'attachment-scan',
          scan: {
            purpose: purpose(),
            pass: 'discover',
            progress: scanProgress(),
            discoverEvidence: { evidenceSha256: EVIDENCE, evidenceCount: 2 },
          },
        }),
      ),
    ).toThrow(CleanupAdvanceIntentError);
    expect(() =>
      parse(
        intent({
          step: 'attachment-scan',
          scan: {
            purpose: purpose(),
            pass: 'verify',
            progress: scanProgress(),
          } as never,
        }),
      ),
    ).toThrow(CleanupAdvanceIntentError);
    expect(() =>
      parse(
        intent({
          step: 'attachment-scan',
          scan: {
            purpose: purpose(),
            pass: 'verify',
            progress: scanProgress(),
            discoverEvidence: { evidenceSha256: EVIDENCE, evidenceCount: 1 },
          },
        }),
      ),
    ).toThrow(CleanupAdvanceIntentError);
    expect(() =>
      parse(
        intent({
          step: 'attachment-scan',
          scan: {
            purpose: purpose(),
            pass: 'discover',
            progress: { ...scanProgress(), stage: 'unknown-stage' } as never,
          },
        }),
      ),
    ).toThrow(CleanupAdvanceIntentError);
  });

  it('round-trips the blocked state with a safe attachment identity', () => {
    const ordinary = intent({
      step: 'blocked',
      purpose: purpose(),
      attachment: { plane: 'ordinary', scriptName: 'foreign-worker' },
    });
    expect(parse(ordinary)).toEqual(ordinary);
    const dispatch = intent({
      step: 'blocked',
      purpose: purpose(),
      attachment: {
        plane: 'dispatch',
        scriptName: 'foreign-worker',
        dispatchNamespace: 'foreign-namespace',
      },
    });
    expect(parse(dispatch)).toEqual(dispatch);
    expect(() =>
      parse({
        ...ordinary,
        state: {
          step: 'blocked',
          purpose: purpose(),
          attachment: { plane: 'ordinary', scriptName: '' },
        },
      }),
    ).toThrow(CleanupAdvanceIntentError);
  });

  it('round-trips the database-deletion state', () => {
    const value = intent({ step: 'database-deletion' });
    expect(parse(value)).toEqual(value);
    expect(() =>
      parse({ ...value, state: { step: 'database-deleting' } }),
    ).toThrow(CleanupAdvanceIntentError);
  });

  it('rejects missing, extra, and malformed top-level keys', () => {
    const value = intent({ step: 'teardown-traffic' });
    const { state: _state, ...missing } = value;
    expect(() => parse(missing)).toThrow(CleanupAdvanceIntentError);
    expect(() => parse({ ...value, extra: 1 })).toThrow(
      CleanupAdvanceIntentError,
    );
    expect(() => parse({ ...value, version: 2 })).toThrow(
      CleanupAdvanceIntentError,
    );
    expect(() => parse({ ...value, operationId: 'not-a-uuid' })).toThrow(
      CleanupAdvanceIntentError,
    );
    expect(() => parse({ ...value, revision: -1 })).toThrow(
      CleanupAdvanceIntentError,
    );
    expect(() => parse({ ...value, generation: 1.5 })).toThrow(
      CleanupAdvanceIntentError,
    );
    expect(() =>
      parse({ ...value, updatedAt: '2026-08-29T12:00:00Z' }),
    ).toThrow(CleanupAdvanceIntentError);
    expect(() =>
      parse(value, fleetRecord({ phase: 'worker-deployed' })),
    ).toThrow(CleanupAdvanceIntentError);
  });

  it('rejects identity mismatches and malformed identity shapes', () => {
    const value = intent({ step: 'teardown-traffic' });
    expect(() =>
      parse({
        ...value,
        identity: identity({
          record: { ...identity().record, scriptName: 'other-worker' },
        }),
      }),
    ).toThrow(CleanupAdvanceIntentError);
    expect(() =>
      parse({
        ...value,
        identity: identity({ admittedPhase: 'ready' as ProvisioningPhase }),
      }),
    ).toThrow(CleanupAdvanceIntentError);
    expect(() =>
      parse({
        ...value,
        identity: identity({ externalArtifact: 'no' as never }),
      }),
    ).toThrow(CleanupAdvanceIntentError);
    const { externalArtifact: _externalArtifact, ...withoutArtifact } =
      identity();
    expect(() => parse({ ...value, identity: withoutArtifact })).toThrow(
      CleanupAdvanceIntentError,
    );
    expect(() =>
      parse({
        ...value,
        identity: {
          ...identity(),
          record: { ...identity().record, extra: 1 },
        },
      }),
    ).toThrow(CleanupAdvanceIntentError);
  });

  it('round-trips manual and rollback authority and rejects malformed variants', () => {
    const manual = intent({ step: 'database-deletion' });
    expect(parse(manual)).toEqual(manual);
    const rollback = intent(
      { step: 'database-deletion' },
      { authority: rollbackAuthority() },
    );
    expect(parse(rollback)).toEqual(rollback);
    expect(() =>
      parse({
        ...manual,
        authority: { kind: 'manual-cleanup', reservationOwned: true },
      }),
    ).toThrow(CleanupAdvanceIntentError);
    const { requestedSpecDigest: _digest, ...withoutDigest } =
      rollbackAuthority() as Extract<
        CleanupAuthority,
        { kind: 'provisioning-rollback' }
      >;
    expect(() => parse({ ...rollback, authority: withoutDigest })).toThrow(
      CleanupAdvanceIntentError,
    );
    expect(() =>
      parse({
        ...rollback,
        authority: { ...rollbackAuthority(), workerResourceState: 'maybe' },
      }),
    ).toThrow(CleanupAdvanceIntentError);
    expect(() =>
      parse({
        ...rollback,
        authority: {
          ...rollbackAuthority(),
          requestedSpecDigest: 'A'.repeat(64),
        },
      }),
    ).toThrow(CleanupAdvanceIntentError);
  });

  it('rejects a foreign purpose database, operation, or kind', () => {
    const blocked = (
      value: CleanupAttachmentPurpose | Record<string, unknown>,
    ) =>
      intent({
        step: 'blocked',
        purpose: value as CleanupAttachmentPurpose,
        attachment: { plane: 'ordinary', scriptName: 'foreign-worker' },
      });
    expect(() =>
      parse(blocked({ ...purpose(), databaseId: 'other-database' })),
    ).toThrow(CleanupAdvanceIntentError);
    expect(() =>
      parse(blocked({ ...purpose(), operationId: OTHER_OPERATION_ID })),
    ).toThrow(CleanupAdvanceIntentError);
    expect(() =>
      parse(
        blocked({
          kind: 'database-pre-delete',
          databaseId: DATABASE_ID,
          operationId: OPERATION_ID,
        }),
      ),
    ).toThrow(CleanupAdvanceIntentError);
  });

  it('fails closed on byte, depth, and node bounds', () => {
    const value = intent({ step: 'teardown-traffic' });
    expect(CLEANUP_INTENT_BYTE_BOUND).toBe(98_304);
    expect(() =>
      parse({ ...value, state: { step: 'x'.repeat(98_305) } }),
    ).toThrow(CleanupAdvanceIntentError);
    let deep: unknown = 'leaf';
    for (let index = 0; index < 70; index += 1) deep = [deep];
    expect(() => parse({ ...value, state: deep })).toThrow(
      CleanupAdvanceIntentError,
    );
    expect(() =>
      parse({ ...value, state: Array.from({ length: 9_000 }, () => 0) }),
    ).toThrow(CleanupAdvanceIntentError);
  });
});

describe('cleanup advance token', () => {
  const token = {
    version: 1,
    tenantTag: 'acme',
    environment: 'production',
    operationId: OPERATION_ID,
    revision: 2,
  } as const;

  it('parses a canonical token and throws the generic token error first', () => {
    expect(parseCleanupAdvanceToken(token)).toEqual(token);
    for (const malformed of [
      undefined,
      null,
      42,
      'token',
      [],
      { ...token, extra: 1 },
      { ...token, version: 2 },
      { ...token, operationId: 'not-a-uuid' },
      { ...token, revision: -1 },
      { ...token, tenantTag: '' },
      { ...token, environment: 'x'.repeat(5_000) },
    ]) {
      expect(() => parseCleanupAdvanceToken(malformed)).toThrow(
        CleanupAdvanceTokenError,
      );
    }
  });

  it('classifies current and stale tokens against the active intent', () => {
    const source = fleetRecord({
      cleanupIntent: intent({ step: 'database-deletion' }, { revision: 2 }),
    });
    expect(classifyCleanupAdvanceToken(token, source)).toBe('current');
    expect(classifyCleanupAdvanceToken({ ...token, revision: 1 }, source)).toBe(
      'stale',
    );
  });

  it('throws deployment, operation, and future token errors in order', () => {
    const source = fleetRecord({
      cleanupIntent: intent({ step: 'database-deletion' }, { revision: 2 }),
    });
    expect(() =>
      classifyCleanupAdvanceToken({ ...token, tenantTag: 'other' }, source),
    ).toThrow(CleanupAdvanceTokenDeploymentError);
    expect(() =>
      classifyCleanupAdvanceToken(
        { ...token, operationId: OTHER_OPERATION_ID },
        source,
      ),
    ).toThrow(CleanupAdvanceTokenOperationError);
    expect(() =>
      classifyCleanupAdvanceToken(
        token,
        fleetRecord({ phase: 'ready', applicationResources: [] }),
      ),
    ).toThrow(CleanupAdvanceTokenOperationError);
    expect(() =>
      classifyCleanupAdvanceToken({ ...token, revision: 3 }, source),
    ).toThrow(CleanupAdvanceTokenFutureError);
  });
});

describe('invocation authority carrier codec', () => {
  it('round-trips null and timestamp carriers', () => {
    expect(
      invocationAuthorityCarrierFromUnknown({ version: 1, authorizedAt: null }),
    ).toEqual({ version: 1, authorizedAt: null });
    expect(
      invocationAuthorityCarrierFromUnknown({ version: 1, authorizedAt: NOW }),
    ).toEqual({ version: 1, authorizedAt: NOW });
  });

  it('rejects malformed carrier keys, versions, and timestamps', () => {
    for (const malformed of [
      undefined,
      null,
      42,
      {},
      { version: 1 },
      { authorizedAt: null },
      { version: 1, authorizedAt: null, extra: 1 },
      { version: 2, authorizedAt: null },
      { version: 1, authorizedAt: 42 },
      { version: 1, authorizedAt: '2026-08-29T12:00:00Z' },
      { version: 1, authorizedAt: 'not-a-timestamp' },
    ]) {
      expect(() => invocationAuthorityCarrierFromUnknown(malformed)).toThrow(
        InvocationAuthorityCarrierError,
      );
    }
  });
});

describe('cleanup and decommission structural cross-rejection', () => {
  it('rejects cleanup intents in the decommission codec and vice versa', () => {
    const cleanup = intent({ step: 'database-deletion' });
    expect(() =>
      decommissionAdvanceIntentFromUnknown(cleanup, decommissionRecord()),
    ).toThrow(DecommissionAdvanceIntentError);
    const decommission = decommissionDiscoverIntent();
    expect(
      decommissionAdvanceIntentFromUnknown(decommission, decommissionRecord()),
    ).toEqual(decommission);
    expect(() => parse(decommission)).toThrow(CleanupAdvanceIntentError);
  });

  it('rejects the cleanup purpose kind inside decommission scan states', () => {
    const decommission = decommissionDiscoverIntent();
    expect(() =>
      decommissionAdvanceIntentFromUnknown(
        { ...decommission, purpose: purpose() },
        decommissionRecord(),
      ),
    ).toThrow(DecommissionAdvanceIntentError);
  });
});

describe('canonical cleanup evidence serialization', () => {
  it('serializes receipt evidence independent of key order', () => {
    const ordered = receiptEvidence();
    const reordered = JSON.parse(
      JSON.stringify({
        scan: {
          verify: { evidenceCount: 2, evidenceSha256: EVIDENCE },
          discover: { evidenceCount: 2, evidenceSha256: EVIDENCE },
        },
        databaseAbsentReadback: true,
        applicationR2Settled: true,
        platformResourcesAbsent: true,
        workerAbsent: true,
        ingressRemoved: true,
        eligibility: 'carrier-null',
      }),
    ) as CleanupReceiptEvidence;
    expect(canonicalCleanupEvidenceBytes(reordered)).toBe(
      canonicalCleanupEvidenceBytes(ordered),
    );
    expect(
      canonicalCleanupEvidenceBytes({ ...ordered, workerAbsent: false }),
    ).not.toBe(canonicalCleanupEvidenceBytes(ordered));
  });
});

describe('cleanup terminal receipt codec', () => {
  it('round-trips a terminal receipt and rejects malformed receipts', () => {
    const receipt = terminalReceipt();
    expect(cleanupTerminalReceiptFromUnknown(receipt)).toEqual(receipt);
    const completed = terminalReceipt({ completedAtMs: 1_760_000_000_000 });
    expect(cleanupTerminalReceiptFromUnknown(completed)).toEqual(completed);
    const { scan: _scan, ...scanless } = receiptEvidence();
    const reservation = terminalReceipt({
      authority: 'provisioning-rollback',
      admittedPhase: 'database-reserved',
      disposition: 'reservation-cleared',
      evidence: { ...scanless, eligibility: 'reservation-only' },
    });
    expect(cleanupTerminalReceiptFromUnknown(reservation)).toEqual(reservation);
    for (const malformed of [
      undefined,
      { ...receipt, extra: 1 },
      { ...receipt, version: 2 },
      { ...receipt, operationId: 'not-a-uuid' },
      { ...receipt, authority: 'forced' },
      { ...receipt, admittedPhase: 'publishing' },
      { ...receipt, disposition: 'exported' },
      { ...receipt, completedAtMs: -1 },
      { ...receipt, evidence: { ...receiptEvidence(), eligibility: 'always' } },
      { ...receipt, evidence: { ...receiptEvidence(), extra: true } },
      {
        ...receipt,
        evidence: {
          ...receiptEvidence(),
          scan: { discover: { evidenceSha256: EVIDENCE, evidenceCount: 2 } },
        },
      },
    ]) {
      expect(() => cleanupTerminalReceiptFromUnknown(malformed)).toThrow(
        CleanupTerminalReceiptError,
      );
    }
  });
});

describe('classifyCleanupDatabaseEligibility', () => {
  function release(): ExternalReleaseSnapshot {
    return {
      physicalScriptName: 'acme-production',
      specDigest: DIGEST,
      artifactVersion: 'version-1',
      releaseSchemaVersion: 1,
    };
  }

  function classify(record: FleetRecord, externalArtifact = false) {
    return classifyCleanupDatabaseEligibility({ record, externalArtifact });
  }

  it('refuses phases outside the admitted set toward export-backed decommissioning', () => {
    for (const phase of PROVISIONING_PHASES.filter(
      (candidate) => !ADMITTED_PHASES.includes(candidate as never),
    )) {
      expect(classify(fleetRecord({ phase }))).toEqual({
        eligible: false,
        reason: 'phase-requires-decommission',
      });
    }
  });

  it('refuses Workers for Platforms and external artifacts as untrusted data bindings', () => {
    expect(
      classify(
        fleetRecord({
          backend: 'workers-for-platforms',
          phase: 'worker-deployed',
        }),
      ),
    ).toEqual({ eligible: false, reason: 'untrusted-data-binding' });
    expect(classify(fleetRecord({ phase: 'worker-deployed' }), true)).toEqual({
      eligible: false,
      reason: 'untrusted-data-binding',
    });
  });

  it('refuses records carrying external staging evidence', () => {
    const staged: readonly Partial<FleetRecord>[] = [
      { activeRelease: release() },
      { pendingRelease: release() },
      { migrationPriorRelease: release() },
      { rollbackRelease: release() },
      { retiringRelease: release() },
      { platformTarget: {} as ExternalPlatformTargetDescription },
      { migrationIntent: {} as ExternalMigrationIntent },
      { pendingArtifactVersion: 'candidate-v1' },
      { pendingSpecDigest: DIGEST },
    ];
    for (const overrides of staged) {
      expect(
        classify(fleetRecord({ phase: 'worker-deployed', ...overrides })),
      ).toEqual({ eligible: false, reason: 'external-staging-evidence' });
    }
  });

  it('fails closed on a malformed invocation authority carrier', () => {
    for (const carrier of [
      undefined,
      42,
      { version: 2, authorizedAt: null },
      { version: 1, authorizedAt: 42 },
      { version: 1, authorizedAt: null, extra: 1 },
    ]) {
      expect(
        classify({
          ...fleetRecord({ phase: 'worker-deployed' }),
          invocationAuthority: carrier as never,
        }),
      ).toEqual({ eligible: false, reason: 'malformed-carrier' });
    }
  });

  it('refuses a null carrier at maintenance-armed as carrier-phase-inconsistent', () => {
    expect(
      classify(
        fleetRecord({
          phase: 'maintenance-armed',
          invocationAuthority: { version: 1, authorizedAt: null },
        }),
      ),
    ).toEqual({ eligible: false, reason: 'carrier-phase-inconsistent' });
  });

  it('refuses an authorized carrier at every admitted phase', () => {
    for (const phase of ADMITTED_PHASES) {
      expect(
        classify(
          fleetRecord({
            phase,
            invocationAuthority: { version: 1, authorizedAt: NOW },
          }),
        ),
      ).toEqual({ eligible: false, reason: 'invocation-authorized' });
    }
  });

  it('classifies absent and null carriers across the admitted phases', () => {
    for (const phase of RESERVATION_PHASES) {
      expect(classify(fleetRecord({ phase }))).toEqual({
        eligible: true,
        eligibility: 'reservation-only',
      });
      expect(
        classify(
          fleetRecord({
            phase,
            invocationAuthority: { version: 1, authorizedAt: null },
          }),
        ),
      ).toEqual({ eligible: true, eligibility: 'reservation-only' });
    }
    for (const phase of LEGACY_IMPOSSIBLE_PHASES) {
      expect(classify(fleetRecord({ phase }))).toEqual({
        eligible: true,
        eligibility: 'legacy-phase-impossible',
      });
    }
    for (const phase of [
      'application-resources-deployed',
      'platform-resources-deployed',
      'worker-deployed',
      'maintenance-armed',
    ] as const) {
      expect(classify(fleetRecord({ phase }))).toEqual({
        eligible: false,
        reason: 'legacy-phase-ambiguous',
      });
    }
    for (const phase of ADMITTED_PHASES.filter(
      (candidate) =>
        !RESERVATION_PHASES.includes(candidate as never) &&
        candidate !== 'maintenance-armed',
    )) {
      expect(
        classify(
          fleetRecord({
            phase,
            invocationAuthority: { version: 1, authorizedAt: null },
          }),
        ),
      ).toEqual({ eligible: true, eligibility: 'carrier-null' });
    }
  });
});
