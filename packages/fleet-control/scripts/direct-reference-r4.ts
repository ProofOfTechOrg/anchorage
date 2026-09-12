// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import {
  deploymentSpecDigest,
  type FleetAuditAdvanceAction,
  type FleetMigrationAdvanceAction,
  type FleetMigrationItem,
  type FleetRecord,
} from '@proofoftech/fleet-control/cloudflare-control-plane';
import type { DirectRunManifest } from './direct-credentialed-conformance-preflight.mjs';
import type { DirectReferenceContext } from './direct-reference-context.js';
import { directContinuation } from './direct-reference-continuation.js';
import type {
  DirectAuditSlot,
  DirectReferenceAction,
} from './direct-reference-contract.mjs';
import { DirectReferenceExecutionError } from './direct-reference-http.js';
import { selectedDirectGeneration } from './direct-reference-inventory.js';
import {
  DirectReferenceJournalError,
  type DirectStoredOperation,
} from './direct-reference-journal.js';
import {
  directSettlementHost,
  recordDirectResource,
} from './direct-reference-observations.js';

type FleetSlot = DirectAuditSlot | 'migration-next';
type AuditStart = Extract<FleetAuditAdvanceAction, { kind: 'start' }>;
type MigrationStart = Extract<FleetMigrationAdvanceAction, { kind: 'start' }>;
const staleAfterMs = 60 * 60 * 1_000;

function normalRole(
  context: DirectReferenceContext,
  record: FleetRecord,
): 'a' | 'b' {
  const role = context.roleFor(record);
  if (role === 'recovery') throw new DirectReferenceExecutionError();
  return role;
}

function normalRecords(
  context: DirectReferenceContext,
  value: unknown,
): readonly FleetRecord[] {
  if (!Array.isArray(value) || value.length !== 2)
    throw new DirectReferenceJournalError();
  for (const [index, role] of ['a', 'b'].entries()) {
    const record = value[index];
    if (!record || typeof record !== 'object' || Array.isArray(record))
      throw new DirectReferenceJournalError();
    if (normalRole(context, record) !== role)
      throw new DirectReferenceJournalError();
    context.specFor(record);
  }
  return value;
}

async function createStart(
  context: DirectReferenceContext,
  manifest: DirectRunManifest,
  slot: FleetSlot,
) {
  const records = await Promise.all(
    (['a', 'b'] as const).map((role) =>
      context.control.getDeployment(
        manifest.names.roles[role].tenantTag,
        manifest.environment,
      ),
    ),
  );
  if (records.some((record) => record === undefined))
    throw new DirectReferenceJournalError('prerequisite-unavailable');
  const selected = normalRecords(context, records);
  const operationId = randomUUID();
  const action =
    slot === 'migration-next'
      ? {
          kind: 'start' as const,
          operationId,
          records: selected,
          canaryTenantTags: [manifest.names.roles.a.tenantTag],
        }
      : {
          kind: 'start' as const,
          operationId,
          records: selected,
          staleAfterMs,
          generation: (
            await selectedDirectGeneration(
              context,
              manifest,
              slot === 'audit-before' ? 'inventory-before' : 'inventory-after',
            )
          ).generation,
        };
  return { operationId, inputJson: JSON.stringify(action) };
}

function readStart(
  context: DirectReferenceContext,
  manifest: DirectRunManifest,
  stored: DirectStoredOperation,
):
  | { kind: 'audit'; action: AuditStart }
  | { kind: 'migration'; action: MigrationStart } {
  try {
    if (stored.operationId === null) throw new DirectReferenceJournalError();
    const input = JSON.parse(stored.inputJson) as Record<string, unknown>;
    const records = normalRecords(context, input.records);
    if (stored.slot === 'migration-next' && stored.kind === 'migration') {
      const action: MigrationStart = {
        kind: 'start',
        operationId: stored.operationId,
        records,
        canaryTenantTags: [manifest.names.roles.a.tenantTag],
      };
      if (stored.inputJson !== JSON.stringify(action))
        throw new DirectReferenceJournalError();
      return { kind: 'migration', action };
    }
    if (
      (stored.slot !== 'audit-before' && stored.slot !== 'audit-after') ||
      stored.kind !== 'audit'
    )
      throw new DirectReferenceJournalError();
    if (
      typeof input.generation !== 'number' ||
      !Number.isSafeInteger(input.generation) ||
      input.generation < 1
    )
      throw new DirectReferenceJournalError();
    const action: AuditStart = {
      kind: 'start',
      operationId: stored.operationId,
      records,
      staleAfterMs,
      generation: input.generation,
    };
    if (stored.inputJson !== JSON.stringify(action))
      throw new DirectReferenceJournalError();
    return { kind: 'audit', action };
  } catch {
    throw new DirectReferenceJournalError();
  }
}

function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new DirectReferenceJournalError();
  return value as Record<string, unknown>;
}

function claimRevision(json: unknown, operationId: string): number {
  if (typeof json !== 'string') throw new DirectReferenceJournalError();
  const claim = object(JSON.parse(json));
  if (
    claim.operationId !== operationId ||
    typeof claim.revision !== 'number' ||
    !Number.isSafeInteger(claim.revision) ||
    claim.revision < 0
  )
    throw new DirectReferenceJournalError();
  return claim.revision;
}

async function hasInterruption(
  context: DirectReferenceContext,
  manifest: DirectRunManifest,
  operationId: string,
): Promise<boolean> {
  const raw = await context.journal.readInterruption();
  if (raw === null) return false;
  try {
    const value = object(JSON.parse(raw)),
      item = object(value.item);
    const before = claimRevision(value.claimJson, operationId),
      after = claimRevision(value.returnedTokenJson, operationId);
    if (
      after <= before ||
      typeof item.entryRecordDigest !== 'string' ||
      !/^[a-f0-9]{64}$/u.test(item.entryRecordDigest)
    )
      throw new DirectReferenceJournalError();
    const expected = {
      version: 1,
      boundary: 'after-migration-admission',
      slot: 'migration-next',
      operationId,
      claimJson: value.claimJson,
      returnedTokenJson: value.returnedTokenJson,
      item: {
        ordinal: 0,
        tenantTag: manifest.names.roles.a.tenantTag,
        environment: manifest.environment,
        entryRecordDigest: item.entryRecordDigest,
        targetSpecDigest: deploymentSpecDigest(context.spec('a', 'next')),
        beforeStatus: 'pending',
        afterStatus: 'active',
        planCursor: 0,
      },
    };
    if (raw !== JSON.stringify(expected))
      throw new DirectReferenceJournalError();
    return true;
  } catch {
    throw new DirectReferenceJournalError();
  }
}

function firstCanary(
  item: FleetMigrationItem | undefined,
  manifest: DirectRunManifest,
): item is FleetMigrationItem {
  return (
    item?.ordinal === 0 &&
    item.tenantTag === manifest.names.roles.a.tenantTag &&
    item.environment === manifest.environment &&
    item.canaryRank === 0
  );
}

async function recordNormalResources(
  context: DirectReferenceContext,
  manifest: DirectRunManifest,
): Promise<void> {
  for (const role of ['a', 'b'] as const) {
    const record = await context.control.getDeployment(
      manifest.names.roles[role].tenantTag,
      manifest.environment,
    );
    if (record) {
      if (normalRole(context, record) !== role)
        throw new DirectReferenceExecutionError();
      await recordDirectResource(context, record, 'migration-read');
    }
  }
}

export async function dispatchDirectR4(
  context: DirectReferenceContext,
  manifest: DirectRunManifest,
  action: DirectReferenceAction,
  signal: AbortSignal,
): Promise<unknown> {
  if (
    !action.kind.startsWith('audit-') &&
    !action.kind.startsWith('migration-')
  )
    throw new DirectReferenceExecutionError();
  const slot: FleetSlot =
    'slot' in action &&
    (action.slot === 'audit-before' || action.slot === 'audit-after')
      ? action.slot
      : 'migration-next';
  if ((slot === 'migration-next') !== action.kind.startsWith('migration-'))
    throw new DirectReferenceExecutionError();
  const starting =
    action.kind === 'audit-start' || action.kind === 'migration-start';
  const stored = starting
    ? await context.journal.freezeStart(slot, () =>
        createStart(context, manifest, slot),
      )
    : await context.journal.readOperation(slot);
  if (!stored) throw new DirectReferenceJournalError('missing-start');
  const frozen = readStart(context, manifest, stored);
  const operationId = frozen.action.operationId;
  if (action.kind === 'audit-page')
    return context.control.readFleetAuditFindingsPage({
      operationId,
      limit: action.limit,
      afterOrdinal: action.afterOrdinal,
    });
  if (action.kind === 'migration-page')
    return context.control.readFleetMigrationItemsPage({
      operationId,
      limit: action.limit,
      afterOrdinal: action.afterOrdinal,
    });
  if (action.kind === 'audit-abandon') {
    await context.control.abandonFleetAuditOperation(operationId);
    return { operationId };
  }
  if (action.kind === 'migration-abandon') {
    await context.control.abandonFleetMigrationOperation(operationId);
    return { operationId };
  }
  const claim =
    starting && stored.tokenJson === null
      ? undefined
      : directContinuation(stored, action);
  if (frozen.kind === 'audit') {
    const result = await context.control.advanceFleetAudit({
      action:
        claim === undefined
          ? frozen.action
          : { kind: 'continue', token: claim },
      specFor: (record) => {
        normalRole(context, record);
        return context.specFor(record);
      },
      maintenanceSecretFor: (record) =>
        context.secrets(normalRole(context, record)).maintenanceAdmin,
      maxItemsPerCall: 1,
      signal,
    });
    await context.journal.rememberToken(slot, JSON.stringify(result.token));
    return result;
  }
  const observed = await hasInterruption(context, manifest, operationId);
  const claimJson = claim === undefined ? undefined : JSON.stringify(claim);
  const before =
    !observed && claim !== undefined
      ? (
          await context.control.readFleetMigrationItemsPage({
            operationId,
            limit: 1,
          })
        ).items[0]
      : undefined;
  const result = await context.control.advanceFleetMigration({
    action:
      claim === undefined ? frozen.action : { kind: 'continue', token: claim },
    specFor: (record) => context.spec(normalRole(context, record), 'next'),
    secretsFor: (record) => context.secrets(normalRole(context, record)),
    settlementFor: (record) => {
      normalRole(context, record);
      return directSettlementHost(context, record);
    },
  });
  await context.journal.rememberToken(slot, JSON.stringify(result.token));
  if (
    !observed &&
    claimJson !== undefined &&
    result.status === 'pending' &&
    result.itemOrdinal === 0 &&
    result.planCursor === 0 &&
    result.token.operationId === operationId &&
    result.token.revision > claimRevision(claimJson, operationId) &&
    firstCanary(before, manifest) &&
    before.status === 'pending'
  ) {
    const after = (
      await context.control.readFleetMigrationItemsPage({
        operationId,
        limit: 1,
      })
    ).items[0];
    if (
      firstCanary(after, manifest) &&
      after.status === 'active' &&
      after.planCursor === 0 &&
      after.entryRecordDigest === before.entryRecordDigest &&
      after.targetSpecDigest === deploymentSpecDigest(context.spec('a', 'next'))
    ) {
      const injected = await context.journal.recordInterruption(
        JSON.stringify({
          version: 1,
          boundary: 'after-migration-admission',
          slot: 'migration-next',
          operationId,
          claimJson,
          returnedTokenJson: JSON.stringify(result.token),
          item: {
            ordinal: 0,
            tenantTag: after.tenantTag,
            environment: after.environment,
            entryRecordDigest: after.entryRecordDigest,
            targetSpecDigest: after.targetSpecDigest,
            beforeStatus: 'pending',
            afterStatus: 'active',
            planCursor: 0,
          },
        }),
      );
      if (injected)
        throw new DirectReferenceExecutionError('injected-response-loss');
    }
  }
  await recordNormalResources(context, manifest);
  return result;
}
