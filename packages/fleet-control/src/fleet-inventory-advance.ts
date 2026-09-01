// SPDX-License-Identifier: Apache-2.0

// The 9..1,000 provider-request contract and its refusal bytes are shared with
// the attachment scanner; duplicating the message would let the two drift.
import { assertWorkerAttachmentProviderRequestBudget } from './cloudflare-worker-attachment-scan-state.js';
import {
  advanceFleetInventoryProgress,
  type CollectFleetInventoryOptions,
  canonicalFleetInventoryRunOptions,
  classifyFleetInventoryRunToken,
  type FleetInventoryGenerationRef,
  type FleetInventoryLease,
  type FleetInventoryProviderContext,
  type FleetInventoryRunRecord,
  type FleetInventoryRunStore,
  type FleetInventoryRunToken,
  FleetInventoryRunTokenOperationError,
  fleetInventoryOptionsDigest,
  materializeFleetInventoryGeneration,
  parseFleetInventoryRunToken,
} from './fleet-inventory-state.js';
import type { FleetResourceInventory } from './types.js';

/** Default staged rows plus facts one bounded chunk may commit. */
export const DEFAULT_FLEET_INVENTORY_STAGED_ROWS_PER_CHUNK = 500;
const MIN_STAGED_ROWS_PER_CHUNK = 1;
const MAX_STAGED_ROWS_PER_CHUNK = 2_000;

/** One bounded inventory step: begin a run, or continue a persisted one. */
export type FleetInventoryAdvanceAction =
  | Readonly<{
      kind: 'start';
      operationId: string;
      options: CollectFleetInventoryOptions;
    }>
  | Readonly<{ kind: 'continue'; token: unknown }>;

/** Inputs for one bounded account inventory step. */
export interface AdvanceFleetInventoryOptions {
  readonly context: FleetInventoryProviderContext;
  readonly store: FleetInventoryRunStore;
  readonly action: FleetInventoryAdvanceAction;
  readonly maxProviderRequests: number;
  readonly maxStagedRowsPerChunk?: number;
  /** Call-local cancellation; it is never persisted. */
  readonly signal?: AbortSignal;
}

/** Authoritative durable outcome after at most one bounded stage chunk. */
export type FleetInventoryAdvanceResult =
  | Readonly<{
      /** More work remains; the token, not this status, is continuation input. */
      status: 'pending';
      token: FleetInventoryRunToken;
    }>
  | Readonly<{
      /** The generation is finalized and readable; rows are not returned here. */
      status: 'complete';
      token: FleetInventoryRunToken;
      generation: FleetInventoryGenerationRef;
    }>;

/** Named capability whose absence makes bounded inventory work fail closed. */
export type FleetInventoryAdvanceCapability =
  | 'inventory-run-store'
  | 'generation-read'
  | 'generation-pin';

const CAPABILITY_MESSAGES: Readonly<
  Record<FleetInventoryAdvanceCapability, string>
> = Object.freeze({
  'inventory-run-store':
    'fleet inventory requires a run store with bounded staging support',
  'generation-read':
    'fleet inventory requires a run store that can read finalized generations',
  'generation-pin':
    'fleet inventory requires a run store that can pin finalized generations',
});

const CAPABILITY_MEMBERS: Readonly<
  Record<FleetInventoryAdvanceCapability, readonly string[]>
> = Object.freeze({
  'inventory-run-store': Object.freeze(['withAccountInventoryLease']),
  'generation-read': Object.freeze([
    'readFinalizedGeneration',
    'readRunByOperation',
  ]),
  'generation-pin': Object.freeze(['pinGeneration']),
});

/** Fixed configuration refusal for one missing bounded capability. */
export class FleetInventoryAdvanceCapabilityError extends Error {
  constructor(readonly capability: FleetInventoryAdvanceCapability) {
    super(CAPABILITY_MESSAGES[capability]);
    this.name = 'FleetInventoryAdvanceCapabilityError';
  }
}

function assertStoreCapability(
  store: FleetInventoryRunStore,
  capability: FleetInventoryAdvanceCapability,
): void {
  for (const member of CAPABILITY_MEMBERS[capability]) {
    if (
      !Reflect.has(store, member) ||
      typeof (store as unknown as Record<string, unknown>)[member] !==
        'function'
    ) {
      throw new FleetInventoryAdvanceCapabilityError(capability);
    }
  }
}

function assertStoreCapabilities(store: FleetInventoryRunStore): void {
  for (const capability of Object.keys(
    CAPABILITY_MEMBERS,
  ) as FleetInventoryAdvanceCapability[]) {
    assertStoreCapability(store, capability);
  }
}

function assertStagedRowsBudget(value: number): void {
  if (
    !Number.isSafeInteger(value) ||
    value < MIN_STAGED_ROWS_PER_CHUNK ||
    value > MAX_STAGED_ROWS_PER_CHUNK
  ) {
    throw new Error('maxStagedRowsPerChunk must be an integer from 1 to 2000');
  }
}

function runToken(run: FleetInventoryRunRecord): FleetInventoryRunToken {
  return {
    version: 1,
    operationId: run.operationId,
    revision: run.progress.revision,
  };
}

async function completeFromRun(
  store: FleetInventoryRunStore,
  run: FleetInventoryRunRecord,
): Promise<FleetInventoryAdvanceResult> {
  const generation = await store.readFinalizedGeneration(
    run.progress.generation,
  );
  return {
    status: 'complete',
    token: runToken(run),
    generation: generation.ref,
  };
}

async function advanceChunk(
  options: AdvanceFleetInventoryOptions,
  lease: FleetInventoryLease,
  run: FleetInventoryRunRecord,
  maxStagedRowsPerChunk: number,
): Promise<FleetInventoryAdvanceResult> {
  if (run.state === 'finalized') {
    return completeFromRun(options.store, run);
  }
  if (run.state === 'failed') {
    throw new Error(
      `fleet inventory run '${run.operationId}' failed and cannot be continued`,
    );
  }
  // Lease loss is detected at the dispatch boundary, before any provider work.
  await lease.assertOwned();
  const executed = run.progress.stage;
  if (executed.step === 'finalize') {
    const generation = await lease.finalizeRun({
      operationId: run.operationId,
      expectedRevision: run.progress.revision,
      manifest: run.progress.stagedCounts,
      factCount: run.progress.factCount,
    });
    return { status: 'complete', token: runToken(run), generation };
  }
  const result = await options.context.advanceStage({
    stage: executed,
    options: run.options,
    progress: run.progress,
    maxProviderRequests: options.maxProviderRequests,
    ...(options.signal ? { signal: options.signal } : {}),
  });
  if (result.rows.length + result.facts.length > maxStagedRowsPerChunk) {
    throw new Error(
      'fleet inventory chunk staged more rows and facts than maxStagedRowsPerChunk allows',
    );
  }
  const progress = advanceFleetInventoryProgress(
    run.progress,
    executed,
    result,
  );
  const committed = await lease.commitChunk({
    operationId: run.operationId,
    expectedRevision: run.progress.revision,
    runRecord: {
      ...run,
      progress,
      updatedAt: new Date().toISOString(),
    },
    rows: result.rows,
    facts: result.facts,
  });
  return { status: 'pending', token: runToken(committed) };
}

/**
 * Performs at most ONE bounded provider stage chunk against the durable run
 * store, then returns the authoritative token. Provider work is reached only
 * through the injected context, so this coordinator stays transport-neutral.
 *
 * A stale token returns the authoritative current result with ZERO provider
 * calls; a token ahead of the persisted run, an unknown operation, and every
 * missing store capability all fail closed before any provider call.
 */
export async function advanceFleetInventory(
  options: AdvanceFleetInventoryOptions,
): Promise<FleetInventoryAdvanceResult> {
  assertWorkerAttachmentProviderRequestBudget(options.maxProviderRequests);
  const maxStagedRowsPerChunk =
    options.maxStagedRowsPerChunk ??
    DEFAULT_FLEET_INVENTORY_STAGED_ROWS_PER_CHUNK;
  assertStagedRowsBudget(maxStagedRowsPerChunk);
  assertStoreCapabilities(options.store);
  const action = options.action;
  if (action.kind === 'start') {
    const canonical = canonicalFleetInventoryRunOptions(action.options);
    const optionsDigest = fleetInventoryOptionsDigest(canonical);
    return options.store.withAccountInventoryLease(async (lease) => {
      const run = await lease.startRun({
        operationId: action.operationId,
        options: canonical,
        optionsDigest,
      });
      return advanceChunk(options, lease, run, maxStagedRowsPerChunk);
    });
  }
  const token = parseFleetInventoryRunToken(action.token);
  return options.store.withAccountInventoryLease(async (lease) => {
    const run = await lease.readRun(token.operationId);
    if (!run) {
      const persisted = await options.store.readRunByOperation(
        token.operationId,
      );
      if (persisted?.state === 'finalized') {
        return completeFromRun(options.store, persisted);
      }
      throw new FleetInventoryRunTokenOperationError(token.operationId);
    }
    if (classifyFleetInventoryRunToken(token, run) === 'stale') {
      // The caller is behind the persisted run, so the authoritative current
      // result is returned without touching the provider.
      return run.state === 'finalized'
        ? completeFromRun(options.store, run)
        : { status: 'pending', token: runToken(run) };
    }
    return advanceChunk(options, lease, run, maxStagedRowsPerChunk);
  });
}

/**
 * Materializes one finalized, readable generation as today's
 * `FleetResourceInventory`. Staging, failed, pruned, and unpinned historical
 * generations are refused by the run store before any row is read.
 */
export async function readFleetInventoryGeneration(
  store: FleetInventoryRunStore,
  generation: number,
): Promise<FleetResourceInventory> {
  // Materialization is a read: a store that can only read finalized
  // generations must not be refused for missing staging or pinning members.
  assertStoreCapability(store, 'generation-read');
  const finalized = await store.readFinalizedGeneration(generation);
  const run = await store.readRunByOperation(finalized.ref.operationId);
  if (run?.state !== 'finalized') {
    throw new FleetInventoryRunTokenOperationError(finalized.ref.operationId);
  }
  return materializeFleetInventoryGeneration({
    rows: finalized.rows,
    facts: finalized.facts,
    options: run.options,
  });
}
