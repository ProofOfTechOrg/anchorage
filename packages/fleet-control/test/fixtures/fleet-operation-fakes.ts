// SPDX-License-Identifier: Apache-2.0

import type {
  FleetInventoryGeneration,
  FleetInventoryGenerationRef,
  FleetInventoryLease,
  FleetInventoryRowKind,
  FleetInventoryRunOptions,
  FleetInventoryRunRecord,
  FleetInventoryRunStore,
  FleetInventoryStagedFact,
  FleetInventoryStagedRow,
} from '../../src/fleet-inventory-state.js';
import { emptyFleetInventoryRowCounts } from '../../src/fleet-inventory-state.js';
import {
  canonicalFleetOperationBytes,
  FLEET_OPERATION_SINGLE_UPDATE_ROW_MESSAGE,
  FLEET_OPERATION_STAGE_BATCH_STATEMENTS,
  type FleetOperationKind,
  type FleetOperationLease,
  type FleetOperationRowKind,
  type FleetOperationRunRecord,
  type FleetOperationStagedRow,
  FleetOperationStateError,
  type FleetOperationStore,
  fleetOperationOtherKindMessage,
  fleetOperationPageLimit,
  fleetOperationStagedRowFromUnknown,
  fleetOperationWatermarkRunMessage,
} from '../../src/fleet-operation-state.js';
import type {
  FleetInventoryDeployment,
  FleetResourceInventory,
} from '../../src/types.js';

// ---------------------------------------------------------------------------
// In-memory FleetOperationStore and FleetInventoryRunStore fakes for the
// bounded audit coordinator, the frozen clock their generation refs are
// stamped with, and the seeded operation-id helper their rows are keyed by —
// more than the operation fakes the file is named for. Suites that pin audit
// behaviour against a fabricated world and suites that pin it against a real
// ProviderWorld share these, so a change to the durable contract lands in one
// place.
// ---------------------------------------------------------------------------

/**
 * The frozen instant this fixture stamps finalized generation refs and run
 * records with. It governs what this file writes, not what a consumer audits
 * against: a consumer pins its own audit and authority clocks, which need not
 * equal this instant. A further clock reaches the port through this fake's
 * terminal writes, which stamp `terminalAtMs` from `Date.now()`, so a
 * `finalizedAtMs` assertion reads the wall clock rather than a pinned one.
 */
export const AUDIT_NOW = Date.parse('2026-06-01T00:00:00.000Z');

export function uuidFor(seed: number): string {
  return `${seed.toString(16).padStart(8, '0')}-0000-4000-8000-000000000000`;
}

// ---------------------------------------------------------------------------
// The FleetInventoryRunStore fake: registers a finalized generation directly
// from a FleetResourceInventory, going through the real materialization codec.
// Named for what it carries because `fleet-inventory-advance.test.ts` declares
// its own local fake of the same interface for a different purpose.
// ---------------------------------------------------------------------------

function stageInventoryFixture(inventory: FleetResourceInventory): {
  rows: FleetInventoryStagedRow[];
  facts: FleetInventoryStagedFact[];
  options: FleetInventoryRunOptions;
} {
  const rows: FleetInventoryStagedRow[] = [];
  const facts: FleetInventoryStagedFact[] = [];
  let ordinal = 0;
  for (const finding of inventory.findings) {
    rows.push({
      kind: 'finding',
      ordinal: ordinal++,
      payload: { record: 'finding', ...finding },
    });
  }
  ordinal = 0;
  for (const registration of inventory.scriptRegistrations) {
    rows.push({
      kind: 'registration',
      ordinal: ordinal++,
      payload: { record: 'registration', ...registration },
    });
  }
  ordinal = 0;
  for (const deployment of inventory.deployments) {
    const deploymentOrdinal = ordinal++;
    const {
      databaseIds,
      durableObjectBindings,
      serviceBindings,
      queueProducerBindings,
      kvNamespaceBindings,
      r2BucketBindings,
      secretNames,
      plainTextBindings,
      routeHostnames,
      zoneRoutes,
      ...identity
    } = deployment as FleetInventoryDeployment & {
      kvNamespaceBindings?: readonly Readonly<{
        name: string;
        namespaceId: string;
      }>[];
      zoneRoutes?: readonly Readonly<{ zoneId: string; routeId: string }>[];
    };
    rows.push({
      kind: 'deployment',
      ordinal: deploymentOrdinal,
      payload: { record: 'deployment', ...identity },
    });
    let factOrdinal = 0;
    for (const databaseId of databaseIds ?? []) {
      facts.push({
        deploymentOrdinal,
        factKind: 'database-id',
        factOrdinal: factOrdinal++,
        payload: { databaseId },
      });
    }
    for (const binding of durableObjectBindings ?? []) {
      facts.push({
        deploymentOrdinal,
        factKind: 'durable-object-binding',
        factOrdinal: factOrdinal++,
        payload: { ...binding },
      });
    }
    for (const binding of serviceBindings ?? []) {
      facts.push({
        deploymentOrdinal,
        factKind: 'service-binding',
        factOrdinal: factOrdinal++,
        payload: { ...binding },
      });
    }
    for (const binding of queueProducerBindings ?? []) {
      facts.push({
        deploymentOrdinal,
        factKind: 'queue-producer-binding',
        factOrdinal: factOrdinal++,
        payload: { ...binding },
      });
    }
    for (const binding of kvNamespaceBindings ?? []) {
      facts.push({
        deploymentOrdinal,
        factKind: 'kv-binding',
        factOrdinal: factOrdinal++,
        payload: { ...binding },
      });
    }
    for (const binding of r2BucketBindings ?? []) {
      facts.push({
        deploymentOrdinal,
        factKind: 'r2-binding',
        factOrdinal: factOrdinal++,
        payload: { ...binding },
      });
    }
    for (const secretName of secretNames ?? []) {
      facts.push({
        deploymentOrdinal,
        factKind: 'secret-name',
        factOrdinal: factOrdinal++,
        payload: { secretName },
      });
    }
    for (const [name, text] of Object.entries(plainTextBindings ?? {})) {
      facts.push({
        deploymentOrdinal,
        factKind: 'plain-text-binding',
        factOrdinal: factOrdinal++,
        payload: { name, text },
      });
    }
    for (const hostname of routeHostnames ?? []) {
      facts.push({
        deploymentOrdinal,
        factKind: 'route-hostname',
        factOrdinal: factOrdinal++,
        payload: { hostname },
      });
    }
    for (const zoneRoute of zoneRoutes ?? []) {
      facts.push({
        deploymentOrdinal,
        factKind: 'zone-route',
        factOrdinal: factOrdinal++,
        payload: { ...zoneRoute },
      });
    }
  }
  ordinal = 0;
  for (const databaseId of inventory.databaseIds) {
    rows.push({
      kind: 'database-id',
      ordinal: ordinal++,
      payload: { record: 'database-id', databaseId },
    });
  }
  ordinal = 0;
  for (const namespaceId of inventory.namespaceIds) {
    rows.push({
      kind: 'namespace-id',
      ordinal: ordinal++,
      payload: { record: 'namespace-id', namespaceId },
    });
  }
  ordinal = 0;
  for (const bucket of inventory.r2Buckets ?? []) {
    rows.push({
      kind: 'r2-bucket',
      ordinal: ordinal++,
      payload: { record: 'r2-bucket', ...bucket },
    });
  }
  ordinal = 0;
  for (const route of inventory.routes) {
    rows.push({
      kind: 'route',
      ordinal: ordinal++,
      payload: { record: 'route', ...route },
    });
  }
  const options: FleetInventoryRunOptions = {
    databaseNamePrefix: 'fleet-',
    scriptNamePrefix: 'fleet-',
    includeDispatchNamespace: false,
    includeR2Buckets: true,
    ...(inventory.hostRoutingKvId === undefined
      ? {}
      : { hostRoutingKvId: inventory.hostRoutingKvId }),
  };
  return { rows, facts, options };
}

export class RegisteredGenerationInventoryRunStore
  implements FleetInventoryRunStore
{
  readonly refs = new Map<number, FleetInventoryGenerationRef>();
  readonly generations = new Map<
    number,
    { rows: FleetInventoryStagedRow[]; facts: FleetInventoryStagedFact[] }
  >();
  readonly runs = new Map<string, FleetInventoryRunRecord>();
  readonly pins: { generation: number; pinnedBy: string }[] = [];
  readonly releasedPins: { generation: number; pinnedBy: string }[] = [];
  latestGeneration: number | undefined;
  latestFinalizedGenerationCalls = 0;
  readFinalizedGenerationCalls = 0;
  readRunByOperationCalls = 0;
  unreadableGenerations = new Set<number>();
  pinFailsForGeneration: number | undefined;
  /**
   * When set, `latestFinalizedGeneration` throws it instead of answering, so
   * an unwanted call fails its title outright rather than being counted after
   * the fact: a title that must prove the method is never reached instruments
   * it to fail rather than asserting a call count afterwards.
   *
   * Arming is the caller's job because this fake has no notion of "the replay
   * path" and cannot detect one; a title arms it once its own legitimate call
   * has returned. Arming it for every title would break the suite instead:
   * `buildHarness` hands each title its own store, and EVERY implicit-generation
   * start calls this method.
   */
  latestFinalizedGenerationError: Error | undefined;

  registerFinalizedGeneration(
    generation: number,
    inventory: FleetResourceInventory,
  ): void {
    const { rows, facts, options } = stageInventoryFixture(inventory);
    const rowManifest: Record<FleetInventoryRowKind, number> = {
      ...emptyFleetInventoryRowCounts(),
    };
    for (const row of rows) {
      rowManifest[row.kind] = (rowManifest[row.kind] ?? 0) + 1;
    }
    const operationId = uuidFor(900_000 + generation);
    const ref: FleetInventoryGenerationRef = {
      generation,
      operationId,
      finalizedAtMs: AUDIT_NOW,
      rowManifest,
      factCount: facts.length,
    };
    this.refs.set(generation, ref);
    this.generations.set(generation, { rows, facts });
    this.runs.set(operationId, {
      version: 1,
      operationId,
      optionsDigest: `digest-${generation}`,
      options,
      state: 'finalized',
      progress: {
        stage: { step: 'finalize' },
        generation,
        revision: 1,
        stagedCounts: rowManifest,
        factCount: facts.length,
        providerRequests: 0,
      },
      updatedAt: new Date(AUDIT_NOW).toISOString(),
    });
    this.latestGeneration = generation;
  }

  async withAccountInventoryLease<T>(
    operation: (lease: FleetInventoryLease) => Promise<T>,
  ): Promise<T> {
    // The lease handed to the callback answers nothing: `assertOwned`
    // rejects and no other member exists, so a caller that reaches for the
    // lease path fails its title instead of reading fabricated state.
    // `pinGeneration` and `releasePin` are store-level and take no lease.
    return operation({
      assertOwned: () => Promise.reject(new Error('unused')),
    } as unknown as FleetInventoryLease);
  }

  async readFinalizedGeneration(
    generation: number,
  ): Promise<FleetInventoryGeneration> {
    this.readFinalizedGenerationCalls += 1;
    if (this.unreadableGenerations.has(generation)) {
      throw new Error(
        `fleet inventory generation ${generation} is not finalized`,
      );
    }
    const ref = this.refs.get(generation);
    const stored = this.generations.get(generation);
    if (!ref || !stored) {
      throw new Error(
        `fleet inventory generation ${generation} is not finalized`,
      );
    }
    return { ref, rows: stored.rows, facts: stored.facts };
  }

  async latestFinalizedGeneration(): Promise<
    FleetInventoryGenerationRef | undefined
  > {
    this.latestFinalizedGenerationCalls += 1;
    if (this.latestFinalizedGenerationError !== undefined) {
      throw this.latestFinalizedGenerationError;
    }
    return this.latestGeneration === undefined
      ? undefined
      : this.refs.get(this.latestGeneration);
  }

  async readRunByOperation(
    operationId: string,
  ): Promise<FleetInventoryRunRecord | undefined> {
    this.readRunByOperationCalls += 1;
    return this.runs.get(operationId);
  }

  async pinGeneration(
    input: Readonly<{ generation: number; pinnedBy: string }>,
  ): Promise<void> {
    if (this.pinFailsForGeneration === input.generation) {
      throw new Error(
        `fleet inventory generation ${input.generation} cannot be pinned`,
      );
    }
    this.pins.push({ ...input });
  }

  async releasePin(
    input: Readonly<{ generation: number; pinnedBy: string }>,
  ): Promise<void> {
    this.releasedPins.push({ ...input });
  }

  async pruneInventoryGenerations(): Promise<Readonly<{ deleted: number }>> {
    return { deleted: 0 };
  }
}

function payloadBytes(row: FleetOperationStagedRow): string {
  return row.rowKind === 'record'
    ? canonicalFleetOperationBytes(row.payload)
    : JSON.stringify(row.payload);
}

// Head-scoped lease reads exercise the coordinator's head-independent fallback.
export class FakeOperationStore implements FleetOperationStore {
  readonly heads = new Map<FleetOperationKind, string>();
  readonly operations = new Map<string, FleetOperationRunRecord>();
  readonly intakeDigests = new Map<string, string>();
  readonly rows = new Map<string, FleetOperationStagedRow[]>();
  readonly locked = new Set<FleetOperationKind>();
  readonly probeMiss = new Set<string>();
  loseLeaseKind: FleetOperationKind | undefined;
  leaseCount = 0;
  readOperationByIdCalls = 0;
  readonly rowPageReadCounts = new Map<FleetOperationRowKind, number>();
  stagedRowCodecCalls = 0;
  /**
   * Runs on the NEXT `readOperationById` and disarms itself. One-shot on
   * purpose: `readFleetAuditFindingsPage` and the start path's catch-all call
   * the same method, so a hook armed for one coordinator call must not leak
   * into either.
   */
  onNextReadOperationById: (() => void) | undefined;
  loseNextSuccessfulCommitProgressResponse: Error | undefined;

  #rowsKey(operationId: string, rowKind: FleetOperationRowKind): string {
    return `${operationId}:${rowKind}`;
  }

  async withAccountOperationLease<T>(
    kind: FleetOperationKind,
    operation: (lease: FleetOperationLease) => Promise<T>,
  ): Promise<T> {
    if (this.locked.has(kind)) {
      throw new Error(
        `fleet ${kind} operations for account 'test' are already being modified`,
      );
    }
    this.locked.add(kind);
    this.leaseCount += 1;
    const lost = this.loseLeaseKind === kind;
    const lease: FleetOperationLease = {
      assertOwned: async () => {
        if (lost) {
          throw new Error(
            `fleet ${kind} operation lease for account 'test' is no longer owned by this operation`,
          );
        }
      },
      startOperation: async (input) => this.#startOperation(input),
      // Head-scoped, where `D1FleetOperationStore` delegates this member to
      // the head-independent `readOperationById` and so reaches a terminal
      // row too. The coordinators fall back to `readOperationById` for a row
      // the head no longer names — `abandonFleetAuditOperation` at the
      // `run ?? readOperationById` read — so this answer exercises that
      // fallback rather than hiding it.
      readOperation: async (operationId) => {
        const op = this.operations.get(operationId);
        if (!op) return undefined;
        return this.heads.get(op.kind) === operationId ? op : undefined;
      },
      stageRows: async (input) => this.#stageRows(input),
      commitProgress: async (input) => {
        const committed = await this.#commitProgress(input);
        const lost = this.loseNextSuccessfulCommitProgressResponse;
        if (lost === undefined) return committed;
        this.loseNextSuccessfulCommitProgressResponse = undefined;
        throw lost;
      },
      finalizeOperation: async (input) => this.#finalizeOperation(kind, input),
      failOperation: async (input) => this.#failOperation(kind, input),
    };
    try {
      return await operation(lease);
    } finally {
      this.locked.delete(kind);
    }
  }

  async readOperationById(
    operationId: string,
  ): Promise<FleetOperationRunRecord | undefined> {
    this.readOperationByIdCalls += 1;
    const hook = this.onNextReadOperationById;
    if (hook !== undefined) {
      this.onNextReadOperationById = undefined;
      hook();
    }
    if (this.probeMiss.has(operationId)) {
      this.probeMiss.delete(operationId);
      return undefined;
    }
    return this.operations.get(operationId);
  }

  async readOperationRowsPage(
    input: Readonly<{
      operationId: string;
      rowKind: FleetOperationRowKind;
      afterOrdinal?: number;
      limit: number;
    }>,
  ): Promise<
    Readonly<{ rows: readonly FleetOperationStagedRow[]; done: boolean }>
  > {
    this.rowPageReadCounts.set(
      input.rowKind,
      (this.rowPageReadCounts.get(input.rowKind) ?? 0) + 1,
    );
    const limit = fleetOperationPageLimit(input.limit);
    const key = this.#rowsKey(input.operationId, input.rowKind);
    const all = [...(this.rows.get(key) ?? [])].sort(
      (left, right) => left.ordinal - right.ordinal,
    );
    const after = input.afterOrdinal ?? -1;
    const filtered = all.filter((row) => row.ordinal > after);
    const page = filtered.slice(0, limit);
    return { rows: page, done: filtered.length <= limit };
  }

  async pruneFleetOperations(): Promise<
    Readonly<{ deleted: number; releasedPins: number }>
  > {
    return { deleted: 0, releasedPins: 0 };
  }

  #startOperation(
    input: Parameters<FleetOperationLease['startOperation']>[0],
  ): ReturnType<FleetOperationLease['startOperation']> {
    const { operationId, kind, runRecord, intakeDigest } = input;
    const existing = this.operations.get(operationId);
    if (existing) {
      if (existing.kind !== kind) {
        throw new Error(
          `fleet operation '${operationId}' belongs to the other operation kind`,
        );
      }
      if (this.intakeDigests.get(operationId) !== intakeDigest) {
        throw new Error(
          `fleet operation '${operationId}' already exists with a different intake`,
        );
      }
      return Promise.resolve({
        outcome:
          existing.state === 'running'
            ? ('adopted-running' as const)
            : ('adopted-terminal' as const),
        record: existing,
      });
    }
    if (this.heads.has(kind)) {
      throw new Error(
        `another fleet ${kind} operation is active for this account`,
      );
    }
    this.operations.set(operationId, runRecord);
    this.intakeDigests.set(operationId, intakeDigest);
    this.heads.set(kind, operationId);
    return Promise.resolve({ outcome: 'created' as const, record: runRecord });
  }

  #stageRows(input: Parameters<FleetOperationLease['stageRows']>[0]): void {
    const { operationId } = input;
    const rows = this.#validatedRows(input.rows);
    for (
      let offset = 0;
      offset < rows.length;
      offset += FLEET_OPERATION_STAGE_BATCH_STATEMENTS
    ) {
      const staged = new Map<string, FleetOperationStagedRow[]>();
      for (const row of rows.slice(
        offset,
        offset + FLEET_OPERATION_STAGE_BATCH_STATEMENTS,
      )) {
        const key = this.#rowsKey(operationId, row.rowKind);
        const list = staged.get(key) ?? [...(this.rows.get(key) ?? [])];
        const existing = list.find((prior) => prior.ordinal === row.ordinal);
        if (existing && payloadBytes(existing) !== payloadBytes(row)) {
          throw new Error('immutable operation row payload differs');
        }
        if (!existing) list.push(row);
        staged.set(key, list);
      }
      for (const [key, list] of staged) this.rows.set(key, list);
    }
  }

  #commitProgress(
    input: Parameters<FleetOperationLease['commitProgress']>[0],
  ): ReturnType<FleetOperationLease['commitProgress']> {
    const {
      operationId,
      expectedRevision,
      runRecord,
      rows: inputRows = [],
      updateRows: inputUpdateRows = [],
      expectedRowWatermarks = {},
    } = input;
    const rows = this.#validatedRows(inputRows);
    const updateRows = this.#validatedRows(inputUpdateRows);
    const mutationKeys = [...rows, ...updateRows].map(
      (row) => `${row.rowKind}:${row.ordinal}`,
    );
    if (
      updateRows.some((row) => row.rowKind !== 'item') ||
      new Set(mutationKeys).size !== mutationKeys.length
    ) {
      throw new FleetOperationStateError();
    }
    if (
      rows.length + updateRows.length + 1 >
      FLEET_OPERATION_STAGE_BATCH_STATEMENTS
    ) {
      throw new Error(
        `commitProgress exceeds the operation batch budget of ${FLEET_OPERATION_STAGE_BATCH_STATEMENTS} statements`,
      );
    }
    for (const [rowKind, watermark] of Object.entries(expectedRowWatermarks)) {
      const below = rows.filter(
        (row) => row.rowKind === rowKind && row.ordinal < (watermark as number),
      );
      const prefix = (watermark as number) - below.length;
      if (below.some((row) => row.ordinal < prefix)) {
        throw new Error(
          fleetOperationWatermarkRunMessage(rowKind as FleetOperationRowKind),
        );
      }
    }
    const current = this.operations.get(operationId);
    const matches =
      current !== undefined &&
      current.state === 'running' &&
      current.progress.revision === expectedRevision;
    if (matches) {
      for (const [rowKind, watermark] of Object.entries(
        expectedRowWatermarks,
      )) {
        const key = this.#rowsKey(
          operationId,
          rowKind as FleetOperationRowKind,
        );
        const ordinals = new Set(
          (this.rows.get(key) ?? []).map((existing) => existing.ordinal),
        );
        for (const row of rows) {
          if (row.rowKind === rowKind) ordinals.add(row.ordinal);
        }
        const count = [...ordinals].filter(
          (ordinal) => ordinal < (watermark as number),
        ).length;
        if (count !== watermark) {
          throw new Error(
            `fleet operation '${operationId}' is no longer at the expected revision`,
          );
        }
      }
      for (const row of updateRows) {
        const list = this.rows.get(this.#rowsKey(operationId, row.rowKind));
        if (!list?.some((existing) => existing.ordinal === row.ordinal)) {
          throw new Error(
            `fleet operation '${operationId}' is no longer at the expected revision`,
          );
        }
      }
      for (const row of rows) {
        const stored = this.rows
          .get(this.#rowsKey(operationId, row.rowKind))
          ?.find((existing) => existing.ordinal === row.ordinal);
        if (stored && payloadBytes(stored) !== payloadBytes(row)) {
          throw new Error('immutable operation row payload differs');
        }
      }
      for (const row of rows) {
        const key = this.#rowsKey(operationId, row.rowKind);
        const list = this.rows.get(key) ?? [];
        if (!list.some((existing) => existing.ordinal === row.ordinal)) {
          list.push(row);
          this.rows.set(key, list);
        }
      }
      for (const row of updateRows) {
        const key = this.#rowsKey(operationId, row.rowKind);
        const list = this.rows.get(key) ?? [];
        const index = list.findIndex(
          (existing) => existing.ordinal === row.ordinal,
        );
        if (index >= 0) list[index] = row;
      }
      this.operations.set(operationId, runRecord);
      return Promise.resolve(runRecord);
    }
    const persisted = this.operations.get(operationId);
    if (!persisted) throw new Error(`no fleet operation '${operationId}'`);
    for (const [rowKind, watermark] of Object.entries(expectedRowWatermarks)) {
      const list =
        this.rows.get(
          this.#rowsKey(operationId, rowKind as FleetOperationRowKind),
        ) ?? [];
      const count = list.filter(
        (row) => row.ordinal < (watermark as number),
      ).length;
      if (count !== watermark) {
        throw new Error(
          `fleet operation '${operationId}' is no longer at the expected revision`,
        );
      }
    }
    if (JSON.stringify(persisted) !== JSON.stringify(runRecord)) {
      throw new Error(
        `fleet operation '${operationId}' is no longer at the expected revision`,
      );
    }
    let complete = true;
    for (const row of [...rows, ...updateRows]) {
      const key = this.#rowsKey(operationId, row.rowKind);
      const list = this.rows.get(key) ?? [];
      const stored = list.find((existing) => existing.ordinal === row.ordinal);
      if (!stored) complete = false;
      else if (payloadBytes(stored) !== payloadBytes(row)) {
        throw new Error(
          `fleet operation '${operationId}' staged rows diverge from the persisted operation`,
        );
      }
    }
    if (!complete) {
      throw new Error(
        `fleet operation '${operationId}' is no longer at the expected revision`,
      );
    }
    return Promise.resolve(persisted);
  }

  #validatedRows(
    rows: readonly FleetOperationStagedRow[],
  ): FleetOperationStagedRow[] {
    return rows.map((row) => {
      this.stagedRowCodecCalls += 1;
      return fleetOperationStagedRowFromUnknown(row);
    });
  }

  #finalizeOperation(
    kind: FleetOperationKind,
    input: Parameters<FleetOperationLease['finalizeOperation']>[0],
  ): ReturnType<FleetOperationLease['finalizeOperation']> {
    const {
      operationId,
      expectedRevision,
      runRecord,
      expectedRowCounts,
      requireAllItemsComplete,
    } = input;
    const current = this.operations.get(operationId);
    if (current && current.kind !== kind) {
      throw new Error(fleetOperationOtherKindMessage(operationId));
    }
    if (
      current?.state !== 'running' ||
      current.progress.revision !== expectedRevision
    ) {
      throw new Error(
        `fleet operation '${operationId}' is no longer at the expected revision`,
      );
    }
    for (const [rowKind, count] of Object.entries(expectedRowCounts)) {
      const list =
        this.rows.get(
          this.#rowsKey(operationId, rowKind as FleetOperationRowKind),
        ) ?? [];
      if (list.length !== count) {
        throw new Error(
          `fleet operation '${operationId}' does not match its finalize counts`,
        );
      }
    }
    if (requireAllItemsComplete) {
      const items = this.rows.get(this.#rowsKey(operationId, 'item')) ?? [];
      const complete = items.filter(
        (row) =>
          (row.payload as Readonly<{ status?: string }>).status === 'complete',
      ).length;
      const itemCount = (runRecord.progress as Readonly<{ itemCount?: number }>)
        .itemCount;
      if (complete !== itemCount) {
        throw new Error(
          `fleet operation '${operationId}' does not match its finalize counts`,
        );
      }
    }
    const finalized: FleetOperationRunRecord = {
      ...runRecord,
      // The wall clock, not a pinned audit clock (see AUDIT_NOW).
      terminalAtMs: Date.now(),
    };
    this.operations.set(operationId, finalized);
    if (this.heads.get(current.kind) === operationId) {
      this.heads.delete(current.kind);
    }
    return Promise.resolve(finalized);
  }

  #failOperation(
    kind: FleetOperationKind,
    input: Parameters<FleetOperationLease['failOperation']>[0],
  ): Promise<void> {
    const { operationId, expectedRevision, runRecord, updateRows = [] } = input;
    if (updateRows.length > 1) {
      throw new Error(FLEET_OPERATION_SINGLE_UPDATE_ROW_MESSAGE);
    }
    const current = this.operations.get(operationId);
    if (current && current.kind !== kind) {
      throw new Error(fleetOperationOtherKindMessage(operationId));
    }
    if (
      current?.state !== 'running' ||
      current.progress.revision !== expectedRevision
    ) {
      throw new Error(
        `fleet operation '${operationId}' is no longer at the expected revision`,
      );
    }
    for (const row of updateRows) {
      const key = this.#rowsKey(operationId, row.rowKind);
      const list = this.rows.get(key) ?? [];
      const index = list.findIndex(
        (existing) => existing.ordinal === row.ordinal,
      );
      if (index >= 0) list[index] = row;
    }
    const failed: FleetOperationRunRecord = {
      ...runRecord,
      // The wall clock, not a pinned audit clock (see AUDIT_NOW).
      terminalAtMs: Date.now(),
    };
    this.operations.set(operationId, failed);
    if (this.heads.get(current.kind) === operationId) {
      this.heads.delete(current.kind);
    }
    return Promise.resolve();
  }
}
