// SPDX-License-Identifier: Apache-2.0

import {
  type CleanupAdvanceAction,
  type CleanupAdvanceResult,
  type CleanupTerminalReceipt,
  type CloudflareDeploymentSpec,
  deploymentSpecDigest,
  type FleetRecord,
  ProvisioningError,
  type ProvisioningResult,
} from '@proofoftech/fleet-control/cloudflare-control-plane';
import type { DirectRunManifest } from './direct-credentialed-conformance-preflight.mjs';
import type {
  DirectFixtureRelease,
  DirectFixtureRole,
} from './direct-credentialed-spec.js';
import type { DirectReferenceContext } from './direct-reference-context.js';
import { directContinuation } from './direct-reference-continuation.js';
import type { DirectReferenceAction } from './direct-reference-contract.mjs';
import { DirectReferenceExecutionError } from './direct-reference-http.js';
import {
  type DirectOperationSlot,
  DirectReferenceJournalError,
  type DirectStoredOperation,
} from './direct-reference-journal.js';
import { recordDirectResource } from './direct-reference-observations.js';

type LifecycleAction = Exclude<
  Extract<DirectReferenceAction, { role: DirectFixtureRole }>,
  { kind: 'tenant-probe' }
>;
type CleanupSlot = `cleanup-${DirectFixtureRole}` | 'cleanup-recovery-initial';

function cleanupSlot(
  role: DirectFixtureRole,
  release: DirectFixtureRelease,
): CleanupSlot {
  return role === 'recovery' && release === 'initial'
    ? 'cleanup-recovery-initial'
    : `cleanup-${role}`;
}

function candidate(
  context: DirectReferenceContext,
  role: DirectFixtureRole,
  release: DirectFixtureRelease,
) {
  return {
    operationId: null,
    inputJson: JSON.stringify({
      version: 1,
      role,
      release,
      specDigest: deploymentSpecDigest(context.spec(role, release)),
    }),
  };
}

function recipeForRecord(context: DirectReferenceContext, record: FleetRecord) {
  const role = context.roleFor(record);
  const spec = context.specFor(record);
  const releases: readonly DirectFixtureRelease[] =
    role === 'recovery'
      ? ['initial', 'next', 'failed-recovery']
      : ['initial', 'next'];
  const release = releases.find(
    (release) => context.spec(role, release) === spec,
  );
  if (!release) throw new DirectReferenceJournalError();
  return { role, release, spec };
}

export function readFrozenLifecycleSpec(
  context: DirectReferenceContext,
  stored: DirectStoredOperation,
  role: DirectFixtureRole,
): CloudflareDeploymentSpec {
  const input = JSON.parse(stored.inputJson) as Record<string, unknown>;
  const release = input.release;
  if (
    input.role !== role ||
    (release !== 'initial' &&
      release !== 'next' &&
      !(role === 'recovery' && release === 'failed-recovery')) ||
    (stored.kind !== 'cleanup' && stored.kind !== 'decommission')
  )
    throw new DirectReferenceJournalError();
  if (stored.inputJson !== candidate(context, role, release).inputJson)
    throw new DirectReferenceJournalError();
  return context.spec(role, release);
}

function validateReceipt(
  receipt: CleanupTerminalReceipt,
  operationId: string,
  spec: CloudflareDeploymentSpec,
) {
  if (
    receipt.operationId !== operationId ||
    receipt.tenantTag !== spec.tenantTag ||
    receipt.environment !== spec.environment ||
    receipt.backend !== 'plain-worker' ||
    receipt.scriptName !== spec.scriptName ||
    receipt.databaseName !== spec.databaseName
  )
    throw new DirectReferenceJournalError();
  return receipt;
}

export async function readHistoricalRecoveryReceipt(
  context: DirectReferenceContext,
) {
  const stored = await context.journal.readOperation('cleanup-recovery');
  if (!stored?.operationId)
    throw new DirectReferenceJournalError('prerequisite-unavailable');
  const spec = readFrozenLifecycleSpec(context, stored, 'recovery');
  if (spec !== context.spec('recovery', 'failed-recovery'))
    throw new DirectReferenceJournalError();
  const receipt = await context.control.readCleanupReceipt(stored.operationId);
  if (!receipt)
    throw new DirectReferenceJournalError('prerequisite-unavailable');
  validateReceipt(receipt, stored.operationId, spec);
  if (receipt.authority !== 'provisioning-rollback')
    throw new DirectReferenceJournalError();
  return { slot: stored.slot, receipt };
}

async function provision(
  context: DirectReferenceContext,
  manifest: DirectRunManifest,
  action: Extract<LifecycleAction, { kind: 'provision' }>,
) {
  const { role, release } = action;
  if (role === 'recovery' && (await context.journal.readForceBefore()))
    throw new DirectReferenceExecutionError();
  const names = manifest.names.roles[role];
  const slot = cleanupSlot(role, release);
  const stored = await context.journal.freezeStart(slot, async () => {
    if (role === 'recovery' && release === 'initial') {
      await readHistoricalRecoveryReceipt(context);
      if (
        await context.control.getDeployment(
          names.tenantTag,
          manifest.environment,
        )
      )
        throw new DirectReferenceJournalError('prerequisite-unavailable');
    }
    return candidate(context, role, release);
  });
  const spec = readFrozenLifecycleSpec(context, stored, role);
  if (spec !== context.spec(role, release) || stored.tokenJson !== null)
    throw new DirectReferenceExecutionError();
  const current = await context.control.getDeployment(
    names.tenantTag,
    manifest.environment,
  );
  if (current) {
    context.roleFor(current);
    if (
      current.cleanupIntent ||
      current.decommissionIntent ||
      context.specFor(current) !== spec
    )
      throw new DirectReferenceExecutionError();
  } else if (!stored.inserted) throw new DirectReferenceExecutionError();
  let result: ProvisioningResult;
  try {
    result = await context.control.provisionDeployment({
      spec,
      secrets: context.secrets(role),
      initialExecutionFenceState: 'open',
    });
  } catch (error) {
    let cleanup: CleanupAdvanceResult | undefined;
    try {
      if (error instanceof ProvisioningError) cleanup = error.cleanup;
    } catch {
      // Foreign rejections can trap class or property inspection.
    }
    if (!cleanup) throw error;
    if (
      cleanup.token.tenantTag !== spec.tenantTag ||
      cleanup.token.environment !== spec.environment
    )
      throw new DirectReferenceExecutionError('wrong-operation');
    await context.journal.rememberToken(slot, JSON.stringify(cleanup.token));
    const record = await context.control.getDeployment(
      names.tenantTag,
      manifest.environment,
    );
    const resource = record
      ? await recordDirectResource(context, record, 'provision-read')
      : undefined;
    return { status: 'failed-provision', role, slot, cleanup, resource };
  }
  const resource = await recordDirectResource(
    context,
    result.record,
    'provision-read',
  );
  return { status: 'ready', role, resource };
}

async function selectCleanup(
  context: DirectReferenceContext,
  manifest: DirectRunManifest,
  action: LifecycleAction,
): Promise<DirectStoredOperation> {
  const slots: readonly CleanupSlot[] =
    action.role === 'recovery'
      ? ['cleanup-recovery-initial', 'cleanup-recovery']
      : [`cleanup-${action.role}`];
  const stored = await Promise.all(
    slots.map((slot) => context.journal.readOperation(slot)),
  );
  if (Object.hasOwn(action, 'token')) {
    const token: unknown = Reflect.get(action, 'token');
    if (
      !token ||
      typeof token !== 'object' ||
      Array.isArray(token) ||
      !Object.hasOwn(token, 'operationId')
    )
      throw new DirectReferenceExecutionError('wrong-operation');
    const matches = stored.filter(
      (entry) =>
        entry !== undefined &&
        entry.operationId !== null &&
        entry.operationId === Reflect.get(token, 'operationId'),
    );
    if (matches.length !== 1 || !matches[0])
      throw new DirectReferenceExecutionError('wrong-operation');
    return matches[0];
  }
  const selected = stored.find((value) => value !== undefined);
  if (selected) return selected;
  if (action.kind !== 'cleanup-start')
    throw new DirectReferenceExecutionError('missing-continuation');
  const record = await context.control.getDeployment(
    manifest.names.roles[action.role].tenantTag,
    manifest.environment,
  );
  if (!record)
    throw new DirectReferenceJournalError('prerequisite-unavailable');
  const recipe = recipeForRecord(context, record);
  return context.journal.freezeStart(
    cleanupSlot(action.role, recipe.release),
    async () => candidate(context, action.role, recipe.release),
  );
}

export async function dispatchDirectLifecycle(
  context: DirectReferenceContext,
  manifest: DirectRunManifest,
  action: LifecycleAction,
  signal: AbortSignal,
): Promise<unknown> {
  if (action.kind === 'provision') return provision(context, manifest, action);
  const decommission = action.kind.startsWith('decommission-');
  let stored: DirectStoredOperation;
  if (decommission) {
    const slot: DirectOperationSlot = `decommission-${action.role}`;
    if (action.kind === 'decommission-start') {
      stored = await context.journal.freezeStart(slot, async () => {
        const record = await context.control.getDeployment(
          manifest.names.roles[action.role].tenantTag,
          manifest.environment,
        );
        if (!record)
          throw new DirectReferenceJournalError('prerequisite-unavailable');
        const recipe = recipeForRecord(context, record);
        return candidate(context, action.role, recipe.release);
      });
    } else {
      const existing = await context.journal.readOperation(slot);
      if (!existing)
        throw new DirectReferenceExecutionError('missing-continuation');
      stored = existing;
    }
  } else stored = await selectCleanup(context, manifest, action);
  const spec = readFrozenLifecycleSpec(context, stored, action.role);
  if (action.kind === 'cleanup-receipt') {
    if (!stored.operationId)
      throw new DirectReferenceExecutionError('missing-continuation');
    const receipt = await context.control.readCleanupReceipt(
      stored.operationId,
    );
    return {
      slot: stored.slot,
      receipt: receipt
        ? validateReceipt(receipt, stored.operationId, spec)
        : null,
    };
  }
  let advance: CleanupAdvanceAction;
  if (
    (action.kind === 'cleanup-start' || action.kind === 'decommission-start') &&
    stored.tokenJson === null
  )
    advance = { kind: 'start' };
  else
    advance = {
      kind: action.kind.endsWith('-restart-blocked')
        ? 'restart-blocked'
        : 'continue',
      token: directContinuation(stored, action),
    };
  const current = await context.control.getDeployment(
    spec.tenantTag,
    spec.environment,
  );
  if (
    current &&
    (stored.operationId === null ||
      current.cleanupIntent?.operationId === stored.operationId ||
      current.decommissionIntent?.operationId === stored.operationId)
  )
    await recordDirectResource(context, current, 'teardown-read');
  const result = decommission
    ? await context.control.advanceDecommissionDeployment({
        spec,
        action: advance,
        maxProviderRequests: manifest.referenceRuntime.maxProviderRequests,
        signal,
      })
    : await context.control.advanceCleanupDeployment({
        spec,
        action: advance,
        maxProviderRequests: manifest.referenceRuntime.maxProviderRequests,
        signal,
      });
  await context.journal.rememberToken(
    stored.slot,
    JSON.stringify(result.token),
  );
  return { slot: stored.slot, ...result };
}
