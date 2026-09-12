// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import type { CloudflareFleetInventoryAdvanceAction } from '@proofoftech/fleet-control/cloudflare-control-plane';
import type { DirectRunManifest } from './direct-credentialed-conformance-preflight.mjs';
import type { DirectReferenceContext } from './direct-reference-context.js';
import { directContinuation } from './direct-reference-continuation.js';
import type {
  DirectInventorySlot,
  DirectReferenceAction,
} from './direct-reference-contract.mjs';
import { DirectReferenceExecutionError } from './direct-reference-http.js';
import {
  DirectReferenceJournalError,
  type DirectStoredOperation,
} from './direct-reference-journal.js';

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

function pinOwner(
  manifest: DirectRunManifest,
  slot: DirectInventorySlot,
): string {
  return `direct-reference:${manifest.resourcePrefix}:${slot}`;
}

export async function selectedDirectGeneration(
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

export async function dispatchDirectInventory(
  context: DirectReferenceContext,
  manifest: DirectRunManifest,
  action: Extract<DirectReferenceAction, { slot: DirectInventorySlot }>,
  signal: AbortSignal,
): Promise<unknown> {
  if (action.kind === 'inventory-read') {
    const selected = await selectedDirectGeneration(
      context,
      manifest,
      action.slot,
    );
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
        const before = await selectedDirectGeneration(
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
        : { kind: 'continue', token: directContinuation(stored, action) };
  } else {
    const stored = await context.journal.readOperation(action.slot);
    if (!stored)
      throw new DirectReferenceExecutionError('missing-continuation');
    inventoryStart(manifest, stored);
    advance = { kind: 'continue', token: directContinuation(stored, action) };
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
