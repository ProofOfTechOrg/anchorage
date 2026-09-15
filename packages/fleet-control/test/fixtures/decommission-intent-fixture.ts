// SPDX-License-Identifier: Apache-2.0

import {
  backendSwitchDecommissionShell,
  backendSwitchDecommissionSnapshotDigest,
  backendSwitchIntentFromUnknown,
  normalizeSwitchDecommissionEntry,
} from '../../src/backend-switch.js';
import { normalizeDecommissionAdvanceIntent } from '../../src/decommission-intent.js';
import type {
  BackendSwitchIntent,
  BackendSwitchSubphase,
  DecommissionAdvanceIntent,
  DecommissionIntentCommon,
  FleetRecord,
  NormalDecommissionLifecyclePhase,
} from '../../src/types.js';

export interface NormalDecommissionIntentFixtureOptions {
  readonly operationId?: string;
  readonly revision?: number;
  readonly generation?: number;
  readonly updatedAt?: string;
  readonly requestedSpecDigest?: string;
  readonly entryLifecyclePhase?: NormalDecommissionLifecyclePhase;
  readonly databaseExportReceiptAuthority?: string;
}

export function normalDecommissionIntentFixture(
  record: FleetRecord,
  lifecyclePhase: NormalDecommissionLifecyclePhase,
  options: NormalDecommissionIntentFixtureOptions = {},
): DecommissionIntentCommon {
  return {
    version: 1,
    operationId: options.operationId ?? '00000000-0000-4000-8000-000000000001',
    revision: options.revision ?? 1,
    generation: options.generation ?? 0,
    updatedAt: options.updatedAt ?? '2026-08-11T00:00:00.000Z',
    identity: {
      record: {
        tenantTag: record.tenantTag,
        environment: record.environment,
        backend: record.backend,
        scriptName: record.scriptName,
        databaseId: record.databaseId,
        databaseName: record.databaseName,
        routeHostname: record.routeHostname,
      },
      mode: {
        kind: 'normal',
        requestedSpecDigest:
          options.requestedSpecDigest ?? record.desiredSpecDigest,
        entryLifecyclePhase: options.entryLifecyclePhase ?? lifecyclePhase,
      },
    },
    ...(options.databaseExportReceiptAuthority === undefined
      ? {}
      : {
          databaseExportReceiptAuthority:
            options.databaseExportReceiptAuthority,
        }),
    lifecyclePhase,
  };
}

export function decommissionAdvancingRecordFixture(
  record: FleetRecord,
  lifecyclePhase: NormalDecommissionLifecyclePhase,
  options: NormalDecommissionIntentFixtureOptions = {},
): FleetRecord {
  return {
    ...record,
    phase: 'decommission-advancing',
    decommissionIntent: {
      ...normalDecommissionIntentFixture(record, lifecyclePhase, options),
      state: 'transitioning',
    },
  };
}

export interface BackendSwitchDecommissionRecordFixtureOptions {
  readonly operationId?: string;
  readonly revision?: number;
  readonly generation?: number;
  readonly entrySubphase?: BackendSwitchSubphase;
  readonly subphase?: BackendSwitchSubphase;
  readonly updatedAt?: string;
}

export function backendSwitchDecommissionRecordFixture(
  record: FleetRecord,
  intent: BackendSwitchIntent,
  options: BackendSwitchDecommissionRecordFixtureOptions = {},
): FleetRecord {
  const canonicalIntent = backendSwitchIntentFromUnknown({
    ...intent,
    subphase: options.subphase ?? 'decommission-traffic-authorized',
  });
  const snapshot = canonicalIntent.decommissionSnapshot;
  if (!snapshot) {
    throw new Error('backend-switch fixture requires a decommission snapshot');
  }
  const entrySubphase = options.entrySubphase ?? 'finalized';
  const snapshotSha256 = backendSwitchDecommissionSnapshotDigest(snapshot);
  const switchIntent = backendSwitchIntentFromUnknown({
    ...canonicalIntent,
    decommissionSnapshotSha256: snapshotSha256,
    decommissionEntrySubphase: entrySubphase,
  });
  const updatedAt = options.updatedAt ?? record.updatedAt;
  if (switchIntent.subphase === 'decommissioned') {
    const databaseExport = switchIntent.databaseExport;
    if (!databaseExport) {
      throw new Error(
        'terminal backend-switch fixture requires a database export',
      );
    }
    const shell: Extract<
      DecommissionAdvanceIntent,
      { readonly state: 'complete' }
    > = {
      version: 1,
      operationId:
        options.operationId ?? '00000000-0000-4000-8000-000000000002',
      revision: options.revision ?? 0,
      generation: options.generation ?? 0,
      updatedAt,
      identity: {
        record: {
          tenantTag: record.tenantTag,
          environment: record.environment,
          backend: record.backend,
          scriptName: record.scriptName,
          databaseId: record.databaseId,
          databaseName: record.databaseName,
          routeHostname: record.routeHostname,
        },
        mode: {
          kind: 'backend-switch' as const,
          priorSpecDigest: switchIntent.prior.specDigest,
          targetSpecDigest: switchIntent.targetSpecDigest,
          decommissionSnapshotSha256: snapshotSha256,
          backendSwitchSubphase: entrySubphase,
        },
      },
      databaseExportReceiptAuthority:
        'memory://fleet-exports/backend-switch/receipts/v1',
      lifecyclePhase: 'decommissioned' as const,
      state: 'complete' as const,
    };
    const normalized = normalizeSwitchDecommissionEntry(
      record,
      switchIntent,
      shell,
    );
    const complete: FleetRecord = {
      ...normalized,
      phase: 'decommissioned',
      databaseExportLocation: databaseExport.location,
      databaseExportSha256: databaseExport.sha256,
      databaseExportSize: databaseExport.size,
    };
    return {
      ...complete,
      decommissionIntent: normalizeDecommissionAdvanceIntent(shell, complete),
    };
  }
  const shell = backendSwitchDecommissionShell({
    record,
    intent: switchIntent,
    operationId: options.operationId ?? '00000000-0000-4000-8000-000000000002',
    snapshotSha256,
    entrySubphase,
    now: updatedAt,
  });
  const normalized = normalizeSwitchDecommissionEntry(
    record,
    switchIntent,
    shell,
  );
  return {
    ...normalized,
    decommissionIntent: normalizeDecommissionAdvanceIntent(shell, normalized),
  };
}
