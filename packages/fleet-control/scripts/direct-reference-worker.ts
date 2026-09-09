// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import type { CloudflareFleetInventoryAdvanceAction } from '@proofoftech/fleet-control/cloudflare-control-plane';
import type { DirectRunManifest } from './direct-credentialed-conformance-preflight.mjs';
import {
  createDirectReferenceContext,
  type DirectReferenceContext,
  type DirectReferenceEnvironment,
} from './direct-reference-context.js';
import type {
  DirectInventorySlot,
  DirectReferenceAction,
} from './direct-reference-contract.mjs';
import {
  DirectReferenceExecutionError,
  handleDirectReferenceHttpRequest,
} from './direct-reference-http.js';
import {
  type DirectOperationSlot,
  DirectReferenceJournalError,
  type DirectStoredOperation,
} from './direct-reference-journal.js';
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
  'decommission-a',
  'decommission-b',
  'decommission-recovery',
];

function inventoryOptions(manifest: DirectRunManifest) {
  const prefix = `${manifest.resourcePrefix}-tenant-`;
  return {
    databaseNamePrefix: prefix,
    scriptNamePrefix: prefix,
    includeR2Buckets: true,
  };
}

function inventoryStart(
  manifest: DirectRunManifest,
  stored: DirectStoredOperation,
) {
  if (stored.kind !== 'inventory' || stored.operationId === null)
    throw new DirectReferenceJournalError();
  const action = {
    kind: 'start' as const,
    operationId: stored.operationId,
    options: inventoryOptions(manifest),
  };
  if (stored.inputJson !== JSON.stringify(action))
    throw new DirectReferenceJournalError();
  return action;
}

function continuation(
  stored: DirectStoredOperation,
  action: DirectReferenceAction,
): unknown {
  const token: unknown = Object.hasOwn(action, 'token')
    ? Reflect.get(action, 'token')
    : stored.tokenJson === null
      ? undefined
      : JSON.parse(stored.tokenJson);
  if (token === undefined)
    throw new DirectReferenceExecutionError('missing-continuation');
  if (
    !token ||
    typeof token !== 'object' ||
    Array.isArray(token) ||
    !Object.hasOwn(token, 'operationId') ||
    (token as { operationId: unknown }).operationId !== stored.operationId
  )
    throw new DirectReferenceExecutionError('wrong-operation');
  return token;
}

function pinOwner(
  manifest: DirectRunManifest,
  slot: DirectInventorySlot,
): string {
  return `direct-reference:${manifest.resourcePrefix}:${slot}`;
}

async function selectedGeneration(
  context: DirectReferenceContext,
  manifest: DirectRunManifest,
  slot: DirectInventorySlot,
) {
  const stored = await context.journal.readOperation(slot);
  if (!stored)
    throw new DirectReferenceJournalError('prerequisite-unavailable');
  const start = inventoryStart(manifest, stored);
  const run = await context.inventoryStore.readRunByOperation(
    start.operationId,
  );
  if (run?.state !== 'finalized')
    throw new DirectReferenceJournalError('prerequisite-unavailable');
  if (
    run.operationId !== start.operationId ||
    run.options.databaseNamePrefix !== start.options.databaseNamePrefix ||
    run.options.scriptNamePrefix !== start.options.scriptNamePrefix ||
    run.options.includeR2Buckets !== true ||
    run.options.includeDispatchNamespace !== false ||
    run.options.hostRoutingKvId !== undefined
  )
    throw new DirectReferenceJournalError();
  return { operationId: run.operationId, generation: run.progress.generation };
}

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
  if (action.kind === 'inventory-read') {
    const selected = await selectedGeneration(context, manifest, action.slot);
    return {
      ...selected,
      inventory: await context.control.readFleetInventoryGeneration(
        selected.generation,
      ),
    };
  }
  if (action.kind !== 'inventory-start' && action.kind !== 'inventory-continue')
    throw new DirectReferenceExecutionError();
  let advance: CloudflareFleetInventoryAdvanceAction;
  if (action.kind === 'inventory-start') {
    const stored = await context.journal.freezeStart(action.slot, async () => {
      if (action.slot === 'inventory-after') {
        const before = await selectedGeneration(
          context,
          manifest,
          'inventory-before',
        );
        await context.inventoryStore.pinGeneration({
          generation: before.generation,
          pinnedBy: pinOwner(manifest, 'inventory-before'),
        });
      }
      const operationId = randomUUID();
      return {
        operationId,
        inputJson: JSON.stringify({
          kind: 'start',
          operationId,
          options: inventoryOptions(manifest),
        }),
      };
    });
    const start = inventoryStart(manifest, stored);
    advance =
      stored.tokenJson === null
        ? start
        : { kind: 'continue', token: continuation(stored, action) };
  } else {
    const stored = await context.journal.readOperation(action.slot);
    if (!stored)
      throw new DirectReferenceExecutionError('missing-continuation');
    inventoryStart(manifest, stored);
    advance = { kind: 'continue', token: continuation(stored, action) };
  }
  const result = await context.control.advanceFleetInventory({
    action: advance,
    maxProviderRequests: manifest.referenceRuntime.maxProviderRequests,
    signal,
  });
  await context.journal.rememberToken(
    action.slot,
    JSON.stringify(result.token),
  );
  if (result.status === 'complete') {
    await context.inventoryStore.pinGeneration({
      generation: result.generation.generation,
      pinnedBy: pinOwner(manifest, action.slot),
    });
  }
  return result;
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
