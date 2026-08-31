// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import {
  emptyFleetInventoryRowCounts,
  FLEET_INVENTORY_FAILURE_REASONS,
  FLEET_INVENTORY_ROW_KINDS,
  type FleetInventoryFailureReason,
  type FleetInventoryGeneration,
  type FleetInventoryGenerationRef,
  type FleetInventoryLease,
  type FleetInventoryRowKind,
  type FleetInventoryRunOptions,
  type FleetInventoryRunRecord,
  type FleetInventoryRunStore,
  type FleetInventoryStagedFact,
  type FleetInventoryStagedRow,
  fleetInventoryRunRecordFromUnknown,
  fleetInventoryStagedFactFromUnknown,
  fleetInventoryStagedRowFromUnknown,
  initialFleetInventoryStage,
} from './fleet-inventory-state.js';
import type { FleetStateDatabase } from './state-store.js';

const HEAD_TABLE = 'anchorage_fleet_inventory_heads';
const RUN_TABLE = 'anchorage_fleet_inventory_runs';
const ROW_TABLE = 'anchorage_fleet_inventory_rows';
const FACT_TABLE = 'anchorage_fleet_inventory_deployment_facts';
const LEASE_TABLE = 'anchorage_fleet_inventory_leases';
const PIN_TABLE = 'anchorage_fleet_inventory_pins';
// Duplicated from state-store.ts:132-133 on purpose: that module does not
// export the two integers, and widening its surface for them would couple the
// inventory store to the deployment store for nothing.
const LEASE_TTL_MS = 15 * 60_000;
const LEASE_RENEWAL_INTERVAL_MS = 5 * 60_000;
// Byte-identical to state-store.ts:134. The Wrangler harness lease clock
// rewrites exactly this substring, so every SQL string in this module must
// express database time with this token and no other time expression.
const DB_NOW_MS = "CAST(unixepoch('subsec') * 1000 AS INTEGER)";
const PRUNE_LIMIT_MAX = 1_000;
const ROW_KIND_CHECK = FLEET_INVENTORY_ROW_KINDS.map(
  (kind) => `'${kind}'`,
).join(',');

type Row = Readonly<Record<string, unknown>>;

const EXPECTED_COLUMNS: Readonly<
  Record<string, Readonly<Record<string, string>>>
> = Object.freeze({
  [HEAD_TABLE]: {
    account_id: 'TEXT',
    active_operation_id: 'TEXT',
    latest_finalized_generation: 'INTEGER',
    next_generation: 'INTEGER',
  },
  [RUN_TABLE]: {
    operation_id: 'TEXT',
    account_id: 'TEXT',
    generation: 'INTEGER',
    options_digest: 'TEXT',
    run_record: 'TEXT',
    created_at_ms: 'INTEGER',
    finalized_at_ms: 'INTEGER',
  },
  [ROW_TABLE]: {
    account_id: 'TEXT',
    generation: 'INTEGER',
    kind: 'TEXT',
    ordinal: 'INTEGER',
    payload: 'TEXT',
  },
  [FACT_TABLE]: {
    account_id: 'TEXT',
    generation: 'INTEGER',
    deployment_ordinal: 'INTEGER',
    fact_kind: 'TEXT',
    fact_ordinal: 'INTEGER',
    payload: 'TEXT',
  },
  [LEASE_TABLE]: {
    account_id: 'TEXT',
    owner_token: 'TEXT',
    expires_at: 'INTEGER',
  },
  [PIN_TABLE]: {
    account_id: 'TEXT',
    generation: 'INTEGER',
    pinned_by: 'TEXT',
    pinned_at_ms: 'INTEGER',
  },
});

export interface D1FleetInventoryRunStoreOptions {
  readonly accountId: string;
  readonly leaseTtlMs?: number;
  readonly leaseRenewalIntervalMs?: number;
}

function rowText(row: Row | undefined, key: string): string {
  const value = row?.[key];
  if (typeof value !== 'string') {
    throw new Error(`fleet inventory row has invalid ${key}`);
  }
  return value;
}

function rowInteger(row: Row | undefined, key: string): number {
  const value = Number(row?.[key]);
  if (!Number.isSafeInteger(value)) {
    throw new Error(`fleet inventory row has invalid ${key}`);
  }
  return value;
}

function optionalInteger(
  row: Row | undefined,
  key: string,
): number | undefined {
  const value = row?.[key];
  if (value === null || value === undefined) return undefined;
  return rowInteger(row, key);
}

function assertGeneration(generation: number): void {
  if (!Number.isSafeInteger(generation) || generation < 1) {
    throw new Error('generation must be a positive integer');
  }
}

function assertPinnedBy(pinnedBy: string): void {
  if (typeof pinnedBy !== 'string' || pinnedBy.length === 0) {
    throw new Error('pinnedBy is required');
  }
}

function notFinalized(generation: number): Error {
  return new Error(`fleet inventory generation ${generation} is not finalized`);
}

function requiresPin(generation: number): Error {
  return new Error(
    `fleet inventory generation ${generation} requires a pin before it can be read`,
  );
}

function corruptGeneration(generation: number): Error {
  return new Error(`fleet inventory generation ${generation} is corrupt`);
}

function unknownRun(operationId: string): Error {
  return new Error(`no fleet inventory run for operation '${operationId}'`);
}

function runConflict(operationId: string): Error {
  return new Error(
    `fleet inventory run '${operationId}' is no longer at the expected revision`,
  );
}

function runOptionsConflict(operationId: string): Error {
  return new Error(
    `fleet inventory run '${operationId}' was started with different options`,
  );
}

function stagedDivergence(operationId: string): Error {
  return new Error(
    `fleet inventory run '${operationId}' staged rows diverge from the persisted generation`,
  );
}

function manifestDisagreement(operationId: string): Error {
  return new Error(
    `fleet inventory run '${operationId}' finalize manifest disagrees with the persisted run record`,
  );
}

function manifestMismatch(operationId: string): Error {
  return new Error(
    `fleet inventory run '${operationId}' does not match its finalize manifest`,
  );
}

function sameCounts(
  left: Readonly<Record<FleetInventoryRowKind, number>>,
  right: Readonly<Record<FleetInventoryRowKind, number>>,
): boolean {
  return FLEET_INVENTORY_ROW_KINDS.every((kind) => left[kind] === right[kind]);
}

/**
 * Durable account inventory run store over the fleet state database port. The
 * account is trusted configuration, never a per-call argument, and every
 * multi-statement mutation is one guarded batch whose guards make a partial
 * application impossible.
 */
export class D1FleetInventoryRunStore implements FleetInventoryRunStore {
  readonly #db: FleetStateDatabase;
  readonly #accountId: string;
  readonly #leaseTtlMs: number;
  readonly #leaseRenewalIntervalMs: number;
  #schemaReady: Promise<void> | undefined;

  constructor(
    db: FleetStateDatabase,
    options: D1FleetInventoryRunStoreOptions,
  ) {
    this.#db = db;
    if (!options.accountId) throw new Error('accountId is required');
    this.#accountId = options.accountId;
    this.#leaseTtlMs = options.leaseTtlMs ?? LEASE_TTL_MS;
    this.#leaseRenewalIntervalMs =
      options.leaseRenewalIntervalMs ?? LEASE_RENEWAL_INTERVAL_MS;
    if (!Number.isSafeInteger(this.#leaseTtlMs) || this.#leaseTtlMs < 1) {
      throw new Error('leaseTtlMs must be a positive integer');
    }
    if (
      !Number.isSafeInteger(this.#leaseRenewalIntervalMs) ||
      this.#leaseRenewalIntervalMs < 1 ||
      this.#leaseRenewalIntervalMs >= this.#leaseTtlMs
    ) {
      throw new Error(
        'leaseRenewalIntervalMs must be a positive integer below leaseTtlMs',
      );
    }
  }

  async #ensureSchema(): Promise<void> {
    const pending = this.#schemaReady ?? this.#initializeSchema();
    this.#schemaReady = pending;
    try {
      await pending;
    } catch (error) {
      if (this.#schemaReady === pending) this.#schemaReady = undefined;
      throw error;
    }
  }

  async #initializeSchema(): Promise<void> {
    await this.#db.execute(`CREATE TABLE IF NOT EXISTS ${HEAD_TABLE} (
      account_id TEXT PRIMARY KEY,
      active_operation_id TEXT,
      latest_finalized_generation INTEGER,
      next_generation INTEGER NOT NULL
    )`);
    await this.#db.execute(`CREATE TABLE IF NOT EXISTS ${RUN_TABLE} (
      operation_id TEXT PRIMARY KEY,
      account_id TEXT NOT NULL,
      generation INTEGER NOT NULL,
      options_digest TEXT NOT NULL,
      run_record TEXT NOT NULL,
      created_at_ms INTEGER NOT NULL,
      finalized_at_ms INTEGER,
      UNIQUE (account_id, generation)
    )`);
    await this.#db.execute(`CREATE TABLE IF NOT EXISTS ${ROW_TABLE} (
      account_id TEXT NOT NULL,
      generation INTEGER NOT NULL,
      kind TEXT NOT NULL CHECK (kind IN (${ROW_KIND_CHECK})),
      ordinal INTEGER NOT NULL,
      payload TEXT NOT NULL,
      PRIMARY KEY (account_id, generation, kind, ordinal)
    )`);
    await this.#db.execute(`CREATE TABLE IF NOT EXISTS ${FACT_TABLE} (
      account_id TEXT NOT NULL,
      generation INTEGER NOT NULL,
      deployment_ordinal INTEGER NOT NULL,
      fact_kind TEXT NOT NULL,
      fact_ordinal INTEGER NOT NULL,
      payload TEXT NOT NULL,
      PRIMARY KEY (account_id, generation, deployment_ordinal, fact_kind, fact_ordinal)
    )`);
    await this.#db.execute(`CREATE TABLE IF NOT EXISTS ${LEASE_TABLE} (
      account_id TEXT PRIMARY KEY,
      owner_token TEXT NOT NULL,
      expires_at INTEGER NOT NULL
    )`);
    await this.#db.execute(`CREATE TABLE IF NOT EXISTS ${PIN_TABLE} (
      account_id TEXT NOT NULL,
      generation INTEGER NOT NULL,
      pinned_by TEXT NOT NULL,
      pinned_at_ms INTEGER NOT NULL,
      PRIMARY KEY (account_id, generation, pinned_by)
    )`);
    // These tables are new, so there is no ALTER path: a column that is absent
    // or of the wrong type means someone else owns the name, and a write would
    // silently drop values rather than fail.
    for (const [table, columns] of Object.entries(EXPECTED_COLUMNS)) {
      const present = await this.#db.query(`PRAGMA table_info(${table})`);
      for (const [name, type] of Object.entries(columns)) {
        const column = present.find((candidate) => candidate.name === name);
        if (!column || String(column.type).toUpperCase() !== type) {
          throw new Error(
            `fleet inventory table '${table}' column '${name}' is absent or incompatible`,
          );
        }
      }
    }
  }

  #leaseExists(): string {
    return `EXISTS (SELECT 1 FROM ${LEASE_TABLE}
        WHERE account_id = ? AND owner_token = ? AND expires_at > ${DB_NOW_MS})`;
  }

  #leaseBindings(token: string): readonly unknown[] {
    return [this.#accountId, token];
  }

  #contention(): Error {
    return new Error(
      `fleet inventory for account '${this.#accountId}' is already being modified`,
    );
  }

  #leaseLost(): Error {
    return new Error(
      `fleet inventory for account '${this.#accountId}' lease is no longer owned by this operation`,
    );
  }

  #headContention(operationId: string): Error {
    return new Error(
      `fleet inventory for account '${this.#accountId}' has an active operation other than '${operationId}'`,
    );
  }

  /**
   * Acquires the account inventory lease, runs the operation with a renewing
   * heartbeat, and releases it. Every run mutation, pin, release, and prune
   * happens through the lease this passes to the operation.
   */
  async withAccountInventoryLease<T>(
    operation: (lease: FleetInventoryLease) => Promise<T>,
  ): Promise<T> {
    await this.#ensureSchema();
    const token = randomUUID();
    const claimed = await this.#db.query(
      `INSERT INTO ${LEASE_TABLE} (
        account_id, owner_token, expires_at
      ) VALUES (?, ?, ${DB_NOW_MS} + ?)
      ON CONFLICT (account_id) DO UPDATE SET
        owner_token = excluded.owner_token,
        expires_at = excluded.expires_at
      WHERE ${LEASE_TABLE}.expires_at <= ${DB_NOW_MS}
      RETURNING owner_token, expires_at`,
      [this.#accountId, token, this.#leaseTtlMs],
    );
    if (claimed.length !== 1 || claimed[0]?.owner_token !== token) {
      throw this.#contention();
    }
    return this.#runRenewingLease({
      label: `fleet inventory for account '${this.#accountId}'`,
      renew: () => this.#renewLease(token),
      release: async () => {
        const released = await this.#db.query(
          `DELETE FROM ${LEASE_TABLE}
        WHERE account_id = ? AND owner_token = ?
        RETURNING owner_token`,
          [this.#accountId, token],
        );
        if (released.length !== 1 || released[0]?.owner_token !== token) {
          throw this.#leaseLost();
        }
      },
      createLease: (assertOwned) => ({
        assertOwned,
        startRun: (input) => this.#startRun(token, input),
        readRun: (operationId) => this.readRunByOperation(operationId),
        commitChunk: (input) => this.#commitChunk(token, input),
        finalizeRun: (input) => this.#finalizeRun(token, input),
        failRun: (input) => this.#failRun(token, input),
        pinGeneration: (input) => this.#pinGeneration(token, input),
        releasePin: (input) => this.#releasePin(token, input),
        pruneInventoryGenerations: (input) =>
          this.#pruneGenerations(token, input),
      }),
      operation,
    });
  }

  async #runRenewingLease<T>(options: {
    readonly label: string;
    readonly renew: () => Promise<void>;
    readonly release: () => Promise<void>;
    readonly createLease: (
      assertOwned: () => Promise<void>,
    ) => FleetInventoryLease;
    readonly operation: (lease: FleetInventoryLease) => Promise<T>;
  }): Promise<T> {
    const heartbeatAbort = new AbortController();
    const renewalErrors: unknown[] = [];
    const assertOwned = async () => {
      const heartbeatError = renewalErrors[0];
      if (heartbeatError !== undefined) {
        throw new Error(`${options.label} heartbeat failed`, {
          cause: heartbeatError,
        });
      }
      await options.renew();
    };
    const lease = options.createLease(assertOwned);
    const heartbeat = this.#renewUntilAborted(
      options.renew,
      heartbeatAbort.signal,
    ).catch((error: unknown) => {
      renewalErrors.push(error);
    });

    let operationFailed = false;
    let operationError: unknown;
    let outcome: { readonly value: T } | undefined;
    try {
      outcome = { value: await options.operation(lease) };
    } catch (error) {
      operationFailed = true;
      operationError = error;
    }

    heartbeatAbort.abort();
    await heartbeat;
    if (!operationFailed && renewalErrors.length === 0) {
      try {
        await assertOwned();
      } catch (error) {
        renewalErrors.push(error);
      }
    }

    let releaseFailed = false;
    let releaseError: unknown;
    try {
      await options.release();
    } catch (error) {
      releaseFailed = true;
      releaseError = error;
    }

    const errors: unknown[] = [];
    if (operationFailed) errors.push(operationError);
    errors.push(...renewalErrors);
    if (releaseFailed) errors.push(releaseError);
    if (errors.length === 1) throw errors[0];
    if (errors.length > 1) {
      throw new AggregateError(
        errors,
        `${options.label} operation and lease cleanup failed`,
      );
    }
    if (!outcome) throw new Error(`${options.label} operation had no outcome`);
    return outcome.value;
  }

  async #renewUntilAborted(
    renew: () => Promise<void>,
    signal: AbortSignal,
  ): Promise<void> {
    while (await this.#waitForRenewal(signal)) {
      await renew();
    }
  }

  #waitForRenewal(signal: AbortSignal): Promise<boolean> {
    if (signal.aborted) return Promise.resolve(false);
    return new Promise((resolve) => {
      const timeout = setTimeout(
        () => finish(true),
        this.#leaseRenewalIntervalMs,
      );
      const aborted = () => finish(false);
      const finish = (renew: boolean) => {
        clearTimeout(timeout);
        signal.removeEventListener('abort', aborted);
        resolve(renew);
      };
      signal.addEventListener('abort', aborted, { once: true });
    });
  }

  async #renewLease(token: string): Promise<void> {
    const renewed = await this.#db.query(
      `UPDATE ${LEASE_TABLE}
      SET expires_at = ${DB_NOW_MS} + ?
      WHERE account_id = ? AND owner_token = ?
        AND expires_at > ${DB_NOW_MS}
      RETURNING owner_token, expires_at`,
      [this.#leaseTtlMs, this.#accountId, token],
    );
    if (renewed.length !== 1 || renewed[0]?.owner_token !== token) {
      throw this.#leaseLost();
    }
  }

  async #startRun(
    token: string,
    input: Readonly<{
      operationId: string;
      options: FleetInventoryRunOptions;
      optionsDigest: string;
    }>,
  ): Promise<FleetInventoryRunRecord> {
    const { operationId, options, optionsDigest } = input;
    // progress.generation is only known inside the batch, so it is seeded here
    // and set from SQL by statement 3.
    const seeded: FleetInventoryRunRecord = {
      version: 1,
      operationId,
      optionsDigest,
      options,
      state: 'staging',
      progress: {
        stage: initialFleetInventoryStage(options),
        generation: 1,
        revision: 0,
        stagedCounts: emptyFleetInventoryRowCounts(),
        factCount: 0,
        providerRequests: 0,
      },
      // The store invents no wall-clock time: database time is the only clock
      // it may read, and the epoch stamp is replaced by the first commit whose
      // record the coordinator supplies.
      updatedAt: new Date(0).toISOString(),
    };
    const claimed = await this.#db.batch([
      {
        sql: `INSERT INTO ${HEAD_TABLE} (
          account_id, active_operation_id, latest_finalized_generation, next_generation
        ) VALUES (?, NULL, NULL, 1)
        ON CONFLICT (account_id) DO NOTHING`,
        bindings: [this.#accountId],
      },
      {
        sql: `UPDATE ${HEAD_TABLE}
           SET active_operation_id = ?,
               next_generation = next_generation
                 + CASE WHEN active_operation_id = ? THEN 0 ELSE 1 END
         WHERE account_id = ?
           AND (active_operation_id = ?
                OR (active_operation_id IS NULL
                    AND NOT EXISTS (SELECT 1 FROM ${RUN_TABLE}
                          WHERE account_id = ? AND operation_id = ?)))
           AND ${this.#leaseExists()}
        RETURNING next_generation, active_operation_id`,
        bindings: [
          operationId,
          operationId,
          this.#accountId,
          operationId,
          this.#accountId,
          operationId,
          ...this.#leaseBindings(token),
        ],
      },
      {
        sql: `INSERT INTO ${RUN_TABLE} (
          operation_id, account_id, generation, options_digest, run_record, created_at_ms
        )
        SELECT ?, ?, h.next_generation - 1, ?,
               json_set(?, '$.progress.generation', h.next_generation - 1),
               ${DB_NOW_MS}
          FROM ${HEAD_TABLE} h
         WHERE h.account_id = ? AND h.active_operation_id = ?
        ON CONFLICT (operation_id) DO NOTHING
        RETURNING operation_id, generation`,
        bindings: [
          operationId,
          this.#accountId,
          optionsDigest,
          JSON.stringify(seeded),
          this.#accountId,
          operationId,
        ],
      },
    ]);
    // Statement 2's zero-row result IS asserted. Statement 3's is NOT, because a
    // replayed start legitimately returns no rows from its DO NOTHING; the
    // readback below adjudicates instead.
    const head = claimed[1] ?? [];
    const claimedHead =
      head.length === 1 && head[0]?.active_operation_id === operationId;
    const persisted = await this.#db.query(
      `SELECT operation_id, options_digest, run_record
         FROM ${RUN_TABLE}
        WHERE operation_id = ? AND account_id = ?`,
      [operationId, this.#accountId],
    );
    const row = persisted[0];
    if (!claimedHead && !row) throw this.#headContention(operationId);
    if (!row) throw unknownRun(operationId);
    if (rowText(row, 'options_digest') !== optionsDigest) {
      throw runOptionsConflict(operationId);
    }
    const record = fleetInventoryRunRecordFromUnknown(
      JSON.parse(rowText(row, 'run_record')),
    );
    // Statement 2 wrote nothing while this operation's run exists: either the
    // run already completed, in which case the replay is idempotent and must not
    // re-reserve the head or burn a generation, or a foreign operation owns the
    // head while this run is still unfinished, which is contention.
    if (!claimedHead && record.state === 'staging') {
      throw this.#headContention(operationId);
    }
    return record;
  }

  async readRunByOperation(
    operationId: string,
  ): Promise<FleetInventoryRunRecord | undefined> {
    await this.#ensureSchema();
    const rows = await this.#db.query(
      `SELECT run_record FROM ${RUN_TABLE}
        WHERE operation_id = ? AND account_id = ?`,
      [operationId, this.#accountId],
    );
    const row = rows[0];
    if (!row) return undefined;
    return fleetInventoryRunRecordFromUnknown(
      JSON.parse(rowText(row, 'run_record')),
    );
  }

  async #commitChunk(
    token: string,
    input: Readonly<{
      operationId: string;
      expectedRevision: number;
      runRecord: FleetInventoryRunRecord;
      rows: readonly FleetInventoryStagedRow[];
      facts: readonly FleetInventoryStagedFact[];
    }>,
  ): Promise<FleetInventoryRunRecord> {
    const { operationId, expectedRevision } = input;
    const runRecord = fleetInventoryRunRecordFromUnknown(input.runRecord);
    const rows = input.rows.map(fleetInventoryStagedRowFromUnknown);
    const facts = input.facts.map(fleetInventoryStagedFactFromUnknown);
    if (
      runRecord.operationId !== operationId ||
      runRecord.state !== 'staging' ||
      runRecord.progress.revision !== expectedRevision + 1
    ) {
      throw runConflict(operationId);
    }
    const generation = runRecord.progress.generation;
    const rowPayloads = new Map(
      rows.map((row) => [
        `${row.kind}:${row.ordinal}`,
        JSON.stringify(row.payload),
      ]),
    );
    const factPayloads = new Map(
      facts.map((fact) => [
        `${fact.deploymentOrdinal}:${fact.factKind}:${fact.factOrdinal}`,
        JSON.stringify(fact.payload),
      ]),
    );
    // Every staging insert carries the SAME lease, state, and PRE-update
    // revision guard as the run update, so all statements stand or fall
    // together. An unguarded insert would let a stale-lease or losing writer
    // land bytes that a later legitimate commit cannot overwrite (DO NOTHING),
    // poisoning payloads while the per-kind counts still match the manifest.
    const stagingGuard = `FROM ${RUN_TABLE} r
         WHERE r.operation_id = ?
           AND json_extract(r.run_record, '$.state') = 'staging'
           AND json_extract(r.run_record, '$.progress.revision') = ?
           AND ${this.#leaseExists()}`;
    const stagingGuardBindings = [
      operationId,
      expectedRevision,
      ...this.#leaseBindings(token),
    ];
    const updated = await this.#db.batch([
      ...rows.map((row) => ({
        sql: `INSERT INTO ${ROW_TABLE} (
          account_id, generation, kind, ordinal, payload
        )
        SELECT ?, ?, ?, ?, ?
        ${stagingGuard}
        ON CONFLICT (account_id, generation, kind, ordinal) DO NOTHING
        RETURNING kind, ordinal`,
        bindings: [
          this.#accountId,
          generation,
          row.kind,
          row.ordinal,
          rowPayloads.get(`${row.kind}:${row.ordinal}`),
          ...stagingGuardBindings,
        ],
      })),
      ...facts.map((fact) => ({
        sql: `INSERT INTO ${FACT_TABLE} (
          account_id, generation, deployment_ordinal, fact_kind, fact_ordinal, payload
        )
        SELECT ?, ?, ?, ?, ?, ?
        ${stagingGuard}
        ON CONFLICT (
          account_id, generation, deployment_ordinal, fact_kind, fact_ordinal
        ) DO NOTHING
        RETURNING fact_kind, fact_ordinal`,
        bindings: [
          this.#accountId,
          generation,
          fact.deploymentOrdinal,
          fact.factKind,
          fact.factOrdinal,
          factPayloads.get(
            `${fact.deploymentOrdinal}:${fact.factKind}:${fact.factOrdinal}`,
          ),
          ...stagingGuardBindings,
        ],
      })),
      {
        sql: `UPDATE ${RUN_TABLE}
           SET run_record = ?
         WHERE operation_id = ?
           AND json_extract(run_record, '$.state') = 'staging'
           AND json_extract(run_record, '$.progress.revision') = ?
           AND ${this.#leaseExists()}
        RETURNING operation_id`,
        bindings: [
          JSON.stringify(runRecord),
          operationId,
          expectedRevision,
          ...this.#leaseBindings(token),
        ],
      },
    ]);
    const written = updated.at(-1) ?? [];
    if (written.length === 1 && written[0]?.operation_id === operationId) {
      return runRecord;
    }
    // Convergence must re-query the persisted record and the stored bytes. The
    // inserts' RETURNING output proves nothing either way: a DO NOTHING insert
    // whose row already exists returns no rows, and a guard miss returns no rows
    // without failing the batch.
    return this.#commitConverged({
      operationId,
      generation,
      revision: runRecord.progress.revision,
      rowPayloads,
      factPayloads,
    });
  }

  async #commitConverged(
    input: Readonly<{
      operationId: string;
      generation: number;
      revision: number;
      rowPayloads: ReadonlyMap<string, string | undefined>;
      factPayloads: ReadonlyMap<string, string | undefined>;
    }>,
  ): Promise<FleetInventoryRunRecord> {
    const { operationId, generation } = input;
    const storedRows = await this.#db.query(
      `SELECT kind, ordinal, payload FROM ${ROW_TABLE}
        WHERE account_id = ? AND generation = ?`,
      [this.#accountId, generation],
    );
    const storedFacts = await this.#db.query(
      `SELECT deployment_ordinal, fact_kind, fact_ordinal, payload
         FROM ${FACT_TABLE}
        WHERE account_id = ? AND generation = ?`,
      [this.#accountId, generation],
    );
    const rowBytes = new Map(
      storedRows.map((row) => [
        `${rowText(row, 'kind')}:${rowInteger(row, 'ordinal')}`,
        rowText(row, 'payload'),
      ]),
    );
    const factBytes = new Map(
      storedFacts.map((row) => [
        `${rowInteger(row, 'deployment_ordinal')}:${rowText(row, 'fact_kind')}:${rowInteger(row, 'fact_ordinal')}`,
        rowText(row, 'payload'),
      ]),
    );
    let complete = true;
    for (const [key, payload] of input.rowPayloads) {
      const stored = rowBytes.get(key);
      if (stored === undefined) complete = false;
      else if (stored !== payload) throw stagedDivergence(operationId);
    }
    for (const [key, payload] of input.factPayloads) {
      const stored = factBytes.get(key);
      if (stored === undefined) complete = false;
      else if (stored !== payload) throw stagedDivergence(operationId);
    }
    const persisted = await this.readRunByOperation(operationId);
    if (!persisted) throw unknownRun(operationId);
    if (complete && persisted.progress.revision === input.revision) {
      return persisted;
    }
    throw runConflict(operationId);
  }

  async #finalizeRun(
    token: string,
    input: Readonly<{
      operationId: string;
      expectedRevision: number;
      manifest: Readonly<Record<FleetInventoryRowKind, number>>;
      factCount: number;
    }>,
  ): Promise<FleetInventoryGenerationRef> {
    const { operationId, expectedRevision, manifest, factCount } = input;
    const persisted = await this.readRunByOperation(operationId);
    if (!persisted) throw unknownRun(operationId);
    const generation = persisted.progress.generation;
    if (persisted.state === 'failed') throw runConflict(operationId);
    if (persisted.state === 'staging') {
      // The counts the guard compares are the PERSISTED record's own, so a
      // caller cannot finalize a generation whose run record describes different
      // counts than its rows; the caller's arguments only have to agree.
      const stagedCounts = persisted.progress.stagedCounts;
      if (
        !sameCounts(manifest, stagedCounts) ||
        factCount !== persisted.progress.factCount
      ) {
        throw manifestDisagreement(operationId);
      }
      const finalized: FleetInventoryRunRecord = {
        ...persisted,
        state: 'finalized',
      };
      const total = FLEET_INVENTORY_ROW_KINDS.reduce(
        (sum, kind) => sum + stagedCounts[kind],
        0,
      );
      await this.#db.batch([
        {
          sql: `UPDATE ${RUN_TABLE}
             SET run_record = ?, finalized_at_ms = ${DB_NOW_MS}
           WHERE operation_id = ?
             AND json_extract(run_record, '$.progress.revision') = ?
             AND json_extract(run_record, '$.state') = 'staging'
             AND ${this.#leaseExists()}
             AND (SELECT COUNT(*) FROM ${FACT_TABLE}
                   WHERE account_id = ? AND generation = ?) = ?
             AND (SELECT COUNT(*) FROM ${ROW_TABLE}
                   WHERE account_id = ? AND generation = ?) = ?
             ${FLEET_INVENTORY_ROW_KINDS.map(
               (kind) => `AND (SELECT COUNT(*) FROM ${ROW_TABLE}
                   WHERE account_id = ? AND generation = ? AND kind = '${kind}') = ?`,
             ).join('\n             ')}
          RETURNING generation, finalized_at_ms`,
          bindings: [
            JSON.stringify(finalized),
            operationId,
            expectedRevision,
            ...this.#leaseBindings(token),
            this.#accountId,
            generation,
            persisted.progress.factCount,
            this.#accountId,
            generation,
            total,
            ...FLEET_INVENTORY_ROW_KINDS.flatMap((kind) => [
              this.#accountId,
              generation,
              stagedCounts[kind],
            ]),
          ],
        },
        {
          sql: `UPDATE ${HEAD_TABLE}
             SET latest_finalized_generation = ?, active_operation_id = NULL
           WHERE account_id = ? AND active_operation_id = ?
             AND ${this.#leaseExists()}
             AND EXISTS (SELECT 1 FROM ${RUN_TABLE}
                   WHERE operation_id = ? AND finalized_at_ms IS NOT NULL
                     AND json_extract(run_record, '$.state') = 'finalized')
          RETURNING latest_finalized_generation`,
          bindings: [
            generation,
            this.#accountId,
            operationId,
            ...this.#leaseBindings(token),
            operationId,
          ],
        },
      ]);
    }
    // The batch is a PROBE: a lost-response replay returns zero rows from both
    // statements, so the run row and the head are the only authority.
    const run = await this.#db.query(
      `SELECT run_record, finalized_at_ms FROM ${RUN_TABLE}
        WHERE operation_id = ? AND account_id = ?`,
      [operationId, this.#accountId],
    );
    const runRow = run[0];
    if (!runRow) throw unknownRun(operationId);
    const record = fleetInventoryRunRecordFromUnknown(
      JSON.parse(rowText(runRow, 'run_record')),
    );
    const finalizedAtMs = optionalInteger(runRow, 'finalized_at_ms');
    if (record.state !== 'finalized' || finalizedAtMs === undefined) {
      if (record.progress.revision !== expectedRevision) {
        throw runConflict(operationId);
      }
      throw manifestMismatch(operationId);
    }
    const head = await this.#headRow();
    if (optionalInteger(head, 'latest_finalized_generation') !== generation) {
      // The only legal repair: statement 2 alone is idempotent and writes no
      // generation data.
      await this.#db.query(
        `UPDATE ${HEAD_TABLE}
           SET latest_finalized_generation = ?, active_operation_id = NULL
         WHERE account_id = ?
           AND ${this.#leaseExists()}
           AND EXISTS (SELECT 1 FROM ${RUN_TABLE}
                 WHERE operation_id = ? AND finalized_at_ms IS NOT NULL
                   AND json_extract(run_record, '$.state') = 'finalized')
        RETURNING latest_finalized_generation`,
        [
          generation,
          this.#accountId,
          ...this.#leaseBindings(token),
          operationId,
        ],
      );
    }
    return {
      generation,
      operationId,
      finalizedAtMs,
      rowManifest: record.progress.stagedCounts,
      factCount: record.progress.factCount,
    };
  }

  async #failRun(
    token: string,
    input: Readonly<{
      operationId: string;
      expectedRevision: number;
      reason: FleetInventoryFailureReason;
    }>,
  ): Promise<void> {
    const { operationId, expectedRevision, reason } = input;
    if (!FLEET_INVENTORY_FAILURE_REASONS.includes(reason)) {
      throw new Error('fleet inventory failure reason is not recognized');
    }
    const persisted = await this.readRunByOperation(operationId);
    if (!persisted) throw unknownRun(operationId);
    if (persisted.state === 'staging') {
      const failed: FleetInventoryRunRecord = {
        ...persisted,
        state: 'failed',
      };
      await this.#db.batch([
        {
          sql: `UPDATE ${RUN_TABLE}
             SET run_record = ?
           WHERE operation_id = ?
             AND json_extract(run_record, '$.state') = 'staging'
             AND json_extract(run_record, '$.progress.revision') = ?
             AND ${this.#leaseExists()}
          RETURNING operation_id`,
          bindings: [
            JSON.stringify(failed),
            operationId,
            expectedRevision,
            ...this.#leaseBindings(token),
          ],
        },
        {
          sql: `UPDATE ${HEAD_TABLE}
             SET active_operation_id = NULL
           WHERE account_id = ? AND active_operation_id = ?
             AND ${this.#leaseExists()}
             AND EXISTS (SELECT 1 FROM ${RUN_TABLE}
                   WHERE operation_id = ?
                     AND json_extract(run_record, '$.state') = 'failed')
          RETURNING account_id`,
          bindings: [
            this.#accountId,
            operationId,
            ...this.#leaseBindings(token),
            operationId,
          ],
        },
      ]);
    }
    const readback = await this.readRunByOperation(operationId);
    if (readback?.state !== 'failed') throw runConflict(operationId);
  }

  async #headRow(): Promise<Row | undefined> {
    const rows = await this.#db.query(
      `SELECT active_operation_id, latest_finalized_generation, next_generation
         FROM ${HEAD_TABLE} WHERE account_id = ?`,
      [this.#accountId],
    );
    return rows[0];
  }

  async #pinGeneration(
    token: string,
    input: Readonly<{ generation: number; pinnedBy: string }>,
  ): Promise<void> {
    const { generation, pinnedBy } = input;
    assertGeneration(generation);
    assertPinnedBy(pinnedBy);
    await this.#assertFinalized(generation);
    await this.#db.batch([
      {
        sql: `INSERT INTO ${PIN_TABLE} (
          account_id, generation, pinned_by, pinned_at_ms
        )
        SELECT ?, ?, ?, ${DB_NOW_MS}
         WHERE ${this.#leaseExists()}
        ON CONFLICT (account_id, generation, pinned_by) DO NOTHING
        RETURNING generation`,
        bindings: [
          this.#accountId,
          generation,
          pinnedBy,
          ...this.#leaseBindings(token),
        ],
      },
    ]);
    if (!(await this.#pinned(generation, pinnedBy))) throw this.#leaseLost();
  }

  async #releasePin(
    token: string,
    input: Readonly<{ generation: number; pinnedBy: string }>,
  ): Promise<void> {
    const { generation, pinnedBy } = input;
    assertGeneration(generation);
    assertPinnedBy(pinnedBy);
    await this.#db.batch([
      {
        sql: `DELETE FROM ${PIN_TABLE}
         WHERE account_id = ? AND generation = ? AND pinned_by = ?
           AND ${this.#leaseExists()}
        RETURNING generation`,
        bindings: [
          this.#accountId,
          generation,
          pinnedBy,
          ...this.#leaseBindings(token),
        ],
      },
    ]);
    if (await this.#pinned(generation, pinnedBy)) throw this.#leaseLost();
  }

  async #pinned(generation: number, pinnedBy: string): Promise<boolean> {
    const rows = await this.#db.query(
      `SELECT generation FROM ${PIN_TABLE}
        WHERE account_id = ? AND generation = ? AND pinned_by = ?`,
      [this.#accountId, generation, pinnedBy],
    );
    return rows.length > 0;
  }

  async #assertFinalized(generation: number): Promise<void> {
    const rows = await this.#db.query(
      `SELECT operation_id FROM ${RUN_TABLE}
        WHERE account_id = ? AND generation = ?
          AND finalized_at_ms IS NOT NULL
          AND json_extract(run_record, '$.state') = 'finalized'`,
      [this.#accountId, generation],
    );
    if (rows.length !== 1) throw notFinalized(generation);
  }

  async #pruneGenerations(
    token: string,
    input: Readonly<{ limit: number }>,
  ): Promise<Readonly<{ deleted: number }>> {
    const { limit } = input;
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > PRUNE_LIMIT_MAX) {
      throw new Error(`limit must be an integer from 1 to ${PRUNE_LIMIT_MAX}`);
    }
    const candidates = await this.#db.query(
      `SELECT generation FROM ${RUN_TABLE} r
        WHERE r.account_id = ?
          AND (r.finalized_at_ms IS NOT NULL
               OR json_extract(r.run_record, '$.state') = 'failed')
          AND NOT EXISTS (SELECT 1 FROM ${HEAD_TABLE}
                WHERE account_id = r.account_id
                  AND latest_finalized_generation = r.generation)
          AND NOT EXISTS (SELECT 1 FROM ${PIN_TABLE}
                WHERE account_id = r.account_id AND generation = r.generation)
        ORDER BY r.generation ASC
        LIMIT ?`,
      [this.#accountId, limit],
    );
    let deleted = 0;
    for (const candidate of candidates) {
      const generation = rowInteger(candidate, 'generation');
      // The pin and latest re-checks live inside the delete batch, so a pin
      // committed after candidate selection still wins.
      const guard = `AND NOT EXISTS (SELECT 1 FROM ${PIN_TABLE}
             WHERE account_id = ? AND generation = ?)
           AND NOT EXISTS (SELECT 1 FROM ${HEAD_TABLE}
             WHERE account_id = ? AND latest_finalized_generation = ?)
           AND ${this.#leaseExists()}`;
      const guardBindings = [
        this.#accountId,
        generation,
        this.#accountId,
        generation,
        ...this.#leaseBindings(token),
      ];
      const results = await this.#db.batch([
        {
          sql: `DELETE FROM ${ROW_TABLE}
           WHERE account_id = ? AND generation = ?
             ${guard}
          RETURNING ordinal`,
          bindings: [this.#accountId, generation, ...guardBindings],
        },
        {
          sql: `DELETE FROM ${FACT_TABLE}
           WHERE account_id = ? AND generation = ?
             ${guard}
          RETURNING fact_ordinal`,
          bindings: [this.#accountId, generation, ...guardBindings],
        },
        {
          sql: `DELETE FROM ${RUN_TABLE}
           WHERE account_id = ? AND generation = ?
             ${guard}
          RETURNING generation`,
          bindings: [this.#accountId, generation, ...guardBindings],
        },
      ]);
      deleted += (results.at(-1) ?? []).length;
    }
    return { deleted };
  }

  async latestFinalizedGeneration(): Promise<
    FleetInventoryGenerationRef | undefined
  > {
    await this.#ensureSchema();
    const latest = optionalInteger(
      await this.#headRow(),
      'latest_finalized_generation',
    );
    if (latest === undefined) return undefined;
    return (await this.#finalizedRef(latest)).ref;
  }

  /**
   * Reads one finalized generation. Only the latest finalized generation, or a
   * generation an operator pinned first, is readable; a partial, failed, or
   * corrupt generation is structurally unreadable.
   */
  async readFinalizedGeneration(
    generation: number,
  ): Promise<FleetInventoryGeneration> {
    assertGeneration(generation);
    await this.#ensureSchema();
    const latest = optionalInteger(
      await this.#headRow(),
      'latest_finalized_generation',
    );
    if (latest !== generation) {
      const pins = await this.#db.query(
        `SELECT generation FROM ${PIN_TABLE}
          WHERE account_id = ? AND generation = ?`,
        [this.#accountId, generation],
      );
      if (pins.length === 0) throw requiresPin(generation);
    }
    const { ref, record } = await this.#finalizedRef(generation);
    const storedRows = await this.#db.query(
      `SELECT kind, ordinal, payload FROM ${ROW_TABLE}
        WHERE account_id = ? AND generation = ?
        ORDER BY kind ASC, ordinal ASC`,
      [this.#accountId, generation],
    );
    const storedFacts = await this.#db.query(
      `SELECT deployment_ordinal, fact_kind, fact_ordinal, payload
         FROM ${FACT_TABLE}
        WHERE account_id = ? AND generation = ?
        ORDER BY deployment_ordinal ASC, fact_kind ASC, fact_ordinal ASC`,
      [this.#accountId, generation],
    );
    const rows = storedRows.map((row) =>
      fleetInventoryStagedRowFromUnknown({
        kind: rowText(row, 'kind'),
        ordinal: rowInteger(row, 'ordinal'),
        payload: JSON.parse(rowText(row, 'payload')),
      }),
    );
    const facts = storedFacts.map((row) =>
      fleetInventoryStagedFactFromUnknown({
        deploymentOrdinal: rowInteger(row, 'deployment_ordinal'),
        factKind: rowText(row, 'fact_kind'),
        factOrdinal: rowInteger(row, 'fact_ordinal'),
        payload: JSON.parse(rowText(row, 'payload')),
      }),
    );
    // Defense in depth behind the in-SQL finalize guard: the live per-kind
    // counts, their ordinal contiguity, and the fact count must still match the
    // manifest the finalized run persisted.
    const live = emptyFleetInventoryRowCounts() as Record<
      FleetInventoryRowKind,
      number
    >;
    for (const row of rows) live[row.kind] += 1;
    if (
      !sameCounts(live, record.progress.stagedCounts) ||
      facts.length !== record.progress.factCount
    ) {
      throw corruptGeneration(generation);
    }
    for (const kind of FLEET_INVENTORY_ROW_KINDS) {
      // Sorted locally so contiguity never depends on the SELECT's ORDER BY.
      const ordinals = rows
        .filter((row) => row.kind === kind)
        .map((row) => row.ordinal)
        .sort((left, right) => left - right);
      if (ordinals.some((ordinal, index) => ordinal !== index)) {
        throw corruptGeneration(generation);
      }
    }
    return { ref, rows, facts };
  }

  async #finalizedRef(generation: number): Promise<
    Readonly<{
      ref: FleetInventoryGenerationRef;
      record: FleetInventoryRunRecord;
    }>
  > {
    const rows = await this.#db.query(
      `SELECT operation_id, run_record, finalized_at_ms FROM ${RUN_TABLE}
        WHERE account_id = ? AND generation = ?`,
      [this.#accountId, generation],
    );
    const row = rows[0];
    if (!row) throw notFinalized(generation);
    const finalizedAtMs = optionalInteger(row, 'finalized_at_ms');
    const record = fleetInventoryRunRecordFromUnknown(
      JSON.parse(rowText(row, 'run_record')),
    );
    if (record.state !== 'finalized' || finalizedAtMs === undefined) {
      throw notFinalized(generation);
    }
    return {
      ref: {
        generation,
        operationId: rowText(row, 'operation_id'),
        finalizedAtMs,
        rowManifest: record.progress.stagedCounts,
        factCount: record.progress.factCount,
      },
      record,
    };
  }

  /**
   * Acquires the account lease and pins a finalized generation. It is ONLY for
   * callers that do not already hold the lease: inside a
   * `withAccountInventoryLease` callback use `lease.pinGeneration`, because
   * this wrapper would attempt a second acquisition of the same account lease
   * and fail with the contention error.
   */
  pinGeneration(
    input: Readonly<{ generation: number; pinnedBy: string }>,
  ): Promise<void> {
    return this.withAccountInventoryLease((lease) =>
      lease.pinGeneration(input),
    );
  }

  /**
   * Acquires the account lease and releases a pin. It is ONLY for callers that
   * do not already hold the lease: inside a `withAccountInventoryLease`
   * callback use `lease.releasePin`, because this wrapper would attempt a
   * second acquisition of the same account lease and fail with the contention
   * error.
   */
  releasePin(
    input: Readonly<{ generation: number; pinnedBy: string }>,
  ): Promise<void> {
    return this.withAccountInventoryLease((lease) => lease.releasePin(input));
  }

  /**
   * Acquires the account lease and prunes unpinned, non-latest generations. It
   * is ONLY for callers that do not already hold the lease: inside a
   * `withAccountInventoryLease` callback use
   * `lease.pruneInventoryGenerations`, because this wrapper would attempt a
   * second acquisition of the same account lease and fail with the contention
   * error.
   */
  pruneInventoryGenerations(
    input: Readonly<{ limit: number }>,
  ): Promise<Readonly<{ deleted: number }>> {
    return this.withAccountInventoryLease((lease) =>
      lease.pruneInventoryGenerations(input),
    );
  }
}
