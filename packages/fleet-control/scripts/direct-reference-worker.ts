// SPDX-License-Identifier: Apache-2.0

import type { DirectRunManifest } from './direct-credentialed-conformance-preflight.mjs';
import {
  createDirectReferenceContext,
  type DirectReferenceContext,
  type DirectReferenceEnvironment,
} from './direct-reference-context.js';
import type { DirectReferenceAction } from './direct-reference-contract.mjs';
import { handleDirectReferenceHttpRequest } from './direct-reference-http.js';
import { dispatchDirectInventory } from './direct-reference-inventory.js';
import type { DirectOperationSlot } from './direct-reference-journal.js';
import { dispatchDirectLifecycle } from './direct-reference-lifecycle.js';
import { dispatchDirectR4 } from './direct-reference-r4.js';
import type { DirectReferenceTransportSnapshot } from './direct-reference-transport.js';

const operationSlots: readonly DirectOperationSlot[] = [
  'inventory-before',
  'inventory-after',
  'audit-before',
  'audit-after',
  'migration-next',
  'cleanup-a',
  'cleanup-b',
  'cleanup-recovery',
  'cleanup-recovery-initial',
  'decommission-a',
  'decommission-b',
  'decommission-recovery',
];

async function dispatch(
  context: DirectReferenceContext,
  manifest: DirectRunManifest,
  action: DirectReferenceAction,
  signal: AbortSignal,
): Promise<unknown> {
  if (action.kind === 'control-read') {
    const operations = await Promise.all(
      operationSlots.map((slot) => context.journal.readOperation(slot)),
    );
    const records = await Promise.all(
      (['a', 'b', 'recovery'] as const).map(async (role) => {
        const record = await context.control.getDeployment(
          manifest.names.roles[role].tenantTag,
          manifest.environment,
        );
        if (!record) return { role, present: false };
        context.roleFor(record);
        return {
          role,
          present: true,
          phase: record.phase,
          desiredSpecDigest: record.desiredSpecDigest,
          pendingSpecDigest: record.pendingSpecDigest,
          artifactVersion: record.artifactVersion,
          pendingArtifactVersion: record.pendingArtifactVersion,
          databaseId: record.databaseId,
        };
      }),
    );
    return {
      binding: context.binding,
      operations: operations.filter((value) => value !== undefined),
      records,
      interruption: await context.journal.readInterruption(),
    };
  }
  if ('role' in action)
    return dispatchDirectLifecycle(context, manifest, action, signal);
  if (
    'slot' in action &&
    (action.slot === 'inventory-before' || action.slot === 'inventory-after')
  )
    return dispatchDirectInventory(context, manifest, action, signal);
  return dispatchDirectR4(context, manifest, action, signal);
}

export function createDirectReferenceWorker(
  manifest: DirectRunManifest,
  runtime: Readonly<{ fetch?: typeof fetch }> = {},
) {
  return {
    async fetch(
      request: Request,
      environment: DirectReferenceEnvironment,
    ): Promise<Response> {
      const startedAt = performance.now();
      let metrics: DirectReferenceTransportSnapshot | undefined;
      const response = await handleDirectReferenceHttpRequest(request, {
        invokeSecret: environment.FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET,
        configSha256: manifest.configSha256,
        invocationTimeoutMs: manifest.referenceRuntime.invocationTimeoutMs,
        startedAt,
        dispatch: async (action, signal) => {
          const context = await createDirectReferenceContext(
            manifest,
            environment,
            { startedAt, signal, fetch: runtime.fetch },
          );
          let result: unknown;
          try {
            result = await dispatch(context, manifest, action, signal);
          } finally {
            metrics = context.transport.snapshot();
          }
          context.transport.assertWithinBudget();
          return result;
        },
      });
      if (metrics) {
        response.headers.set(
          'X-Direct-Provider-Attempts',
          String(metrics.providerAttempts),
        );
        response.headers.set(
          'X-Direct-Maintenance-Attempts',
          String(metrics.maintenanceAttempts),
        );
      }
      return response;
    },
  };
}
