// The per-run resume ledger: runKey (`${workflowId}:${runId}`, composed by
// RunnerRuntime.#runKey) -> (stepKey -> times that step has been resumed).
// The count is the grant-binding tie-breaker (RunLeg.resumeCount /
// RunSummary.resumeCount): approvals capture it at suspension time and the
// provider must see the same value on the resuming leg.
//
// Durability contract: a lost ledger is fail-closed for grants — the leg
// reads resumeCount undefined against an approval's captured ordinal, the
// pair mismatch denies, and the APPROVED action silently no-ops. That is an
// availability bug, not a leak, and it is exactly what a Durable Object
// eviction (~70-140s idle), hibernation, or code deploy does to in-memory
// state. ctx.storage survives all three, so the DO shell adopts
// DurableStorageResumeLedger (see durable-object.ts); InMemoryResumeLedger
// remains the default for node tests and non-DO hosts, preserving the
// pre-seam behavior exactly.

export interface ResumeLedger {
  /**
   * stepKey -> prior resume count for the run; undefined when the run has no
   * recorded resumes. Read BEFORE an increment, the count is the ordinal of
   * the suspension currently being resumed (undefined = first suspension).
   */
  counts(runKey: string): Promise<ReadonlyMap<string, number> | undefined>;
  /** Record one more resume of stepKey; resolves to the run's updated counts. */
  increment(
    runKey: string,
    stepKey: string,
  ): Promise<ReadonlyMap<string, number>>;
  /** Drop the run's ledger — call only once the run is terminal. */
  delete(runKey: string): Promise<void>;
}

export class InMemoryResumeLedger implements ResumeLedger {
  readonly #counts = new Map<string, Map<string, number>>();

  async counts(
    runKey: string,
  ): Promise<ReadonlyMap<string, number> | undefined> {
    return this.#counts.get(runKey);
  }

  async increment(
    runKey: string,
    stepKey: string,
  ): Promise<ReadonlyMap<string, number>> {
    const counts = this.#counts.get(runKey) ?? new Map<string, number>();
    counts.set(stepKey, (counts.get(stepKey) ?? 0) + 1);
    this.#counts.set(runKey, counts);
    return counts;
  }

  async delete(runKey: string): Promise<void> {
    this.#counts.delete(runKey);
  }
}

/**
 * Structural subset of DurableObjectStorage the ledger needs — a seam, so
 * node tests can back it with a plain Map and this module needs no
 * workers-only import.
 */
export interface ResumeLedgerStorage {
  get<T = unknown>(key: string): Promise<T | undefined>;
  put<T>(key: string, value: T): Promise<void>;
  delete(key: string): Promise<boolean>;
}

const KEY_PREFIX = 'flowsafe:resume-ledger:';

// Counts are stored as [stepKey, count] pairs, not a Record: step keys are
// author-chosen strings, and object-key storage would silently drop a step
// named '__proto__' (assignment routes to the prototype setter). Pairs have
// no reserved names.
type StoredCounts = readonly (readonly [string, number])[];

/**
 * ctx.storage-backed ledger. One storage key per run; reads and writes ride
 * the DO's own serialization (the runtime's per-run FIFO lock already orders
 * the increment after the fingerprint read within one resume).
 */
export class DurableStorageResumeLedger implements ResumeLedger {
  readonly #storage: ResumeLedgerStorage;

  constructor(storage: ResumeLedgerStorage) {
    this.#storage = storage;
  }

  async counts(
    runKey: string,
  ): Promise<ReadonlyMap<string, number> | undefined> {
    const stored = await this.#storage.get<StoredCounts>(KEY_PREFIX + runKey);
    return stored ? new Map(stored) : undefined;
  }

  async increment(
    runKey: string,
    stepKey: string,
  ): Promise<ReadonlyMap<string, number>> {
    const key = KEY_PREFIX + runKey;
    const counts = new Map((await this.#storage.get<StoredCounts>(key)) ?? []);
    counts.set(stepKey, (counts.get(stepKey) ?? 0) + 1);
    await this.#storage.put(key, [...counts.entries()]);
    return counts;
  }

  async delete(runKey: string): Promise<void> {
    await this.#storage.delete(KEY_PREFIX + runKey);
  }
}
