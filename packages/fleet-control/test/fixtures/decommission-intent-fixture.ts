// SPDX-License-Identifier: Apache-2.0

import type {
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
