// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import {
  driftFindingRowFromUnknown,
  fleetAuditFactRowFromUnknown,
  fleetAuditOperationRecordFromUnknown,
} from './fleet-audit-state.js';
import type { FleetInventoryRunStore } from './fleet-inventory-state.js';
import { fleetMigrationItemFromUnknown } from './fleet-migration-state.js';
import {
  canonicalFleetOperationBytes,
  FLEET_OPERATION_KINDS,
  FLEET_OPERATION_ROW_KINDS,
  FLEET_OPERATION_STAGE_BATCH_STATEMENTS,
  type FleetOperationKind,
  type FleetOperationLease,
  type FleetOperationRowKind,
  type FleetOperationRunRecord,
  type FleetOperationStagedRow,
  FleetOperationStateError,
  type FleetOperationStore,
  FleetOperationStoreCapabilityError,
  fleetOperationRunRecordFromUnknown,
  fleetOperationSafeInteger,
  fleetOperationSha256,
  fleetOperationStagedRowFromUnknown,
} from './fleet-operation-state.js';
import type { FleetStateDatabase } from './state-store.js';

const LEASE_TABLE = 'anchorage_fleet_operation_leases';
const HEAD_TABLE = 'anchorage_fleet_operation_heads';
const OPERATION_TABLE = 'anchorage_fleet_operations';
const ROW_TABLE = 'anchorage_fleet_operation_rows';
const LEASE_TTL_MS = 15 * 60_000;
const LEASE_RENEWAL_INTERVAL_MS = 5 * 60_000;
// Byte-identical to state-store.ts:134. The Wrangler harness lease clock
// rewrites exactly this substring, so every SQL string in this module must
// express database time with this token and no other time expression.
const DB_NOW_MS = "CAST(unixepoch('subsec') * 1000 AS INTEGER)";
const LIMIT_MAX = 1_000;
const KIND_CHECK = FLEET_OPERATION_KINDS.map((kind) => `'${kind}'`).join(',');
const ROW_KIND_CHECK = FLEET_OPERATION_ROW_KINDS.map(
  (kind) => `'${kind}'`,
).join(',');

type Row = Readonly<Record<string, unknown>>;

const EXPECTED_COLUMNS: Readonly<
  Record<string, Readonly<Record<string, string>>>
> = Object.freeze({
  [LEASE_TABLE]: {
    account_id: 'TEXT',
    operation_kind: 'TEXT',
    owner_token: 'TEXT',
    expires_at: 'INTEGER',
  },
  [HEAD_TABLE]: {
    account_id: 'TEXT',
    operation_kind: 'TEXT',
    active_operation_id: 'TEXT',
  },
  [OPERATION_TABLE]: {
    account_id: 'TEXT',
    operation_id: 'TEXT',
    operation_kind: 'TEXT',
    intake_digest: 'TEXT',
    op_record: 'TEXT',
    created_at_ms: 'INTEGER',
    terminal_at_ms: 'INTEGER',
  },
  [ROW_TABLE]: {
    account_id: 'TEXT',
    operation_id: 'TEXT',
    row_kind: 'TEXT',
    ordinal: 'INTEGER',
    payload: 'TEXT',
  },
});

export interface D1FleetOperationStoreOptions {
  readonly accountId: string;
  readonly leaseTtlMs?: number;
  readonly leaseRenewalIntervalMs?: number;
  readonly inventoryStore?: FleetInventoryRunStore;
}

function rowString(row: Row | undefined, key: string): string {
  const value = row?.[key];
  if (typeof value !== 'string') {
    throw new Error(`fleet operation row has invalid ${key}`);
  }
  return value;
}

function rowNumber(row: Row | undefined, key: string): number {
  const value = Number(row?.[key]);
  if (!Number.isSafeInteger(value)) {
    throw new Error(`fleet operation row has invalid ${key}`);
  }
  return value;
}

function assertLimit(limit: number): void {
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > LIMIT_MAX) {
    throw new Error(`limit must be an integer from 1 to ${LIMIT_MAX}`);
  }
}

function assertKind(kind: FleetOperationKind): void {
  if (!FLEET_OPERATION_KINDS.includes(kind)) {
    throw new FleetOperationStateError();
  }
}

function operationConflict(operationId: string): Error {
  return new Error(
    `fleet operation '${operationId}' is no longer at the expected revision`,
  );
}

function operationDivergence(operationId: string): Error {
  return new Error(
    `fleet operation '${operationId}' staged rows diverge from the persisted operation`,
  );
}

function finalizeMismatch(operationId: string): Error {
  return new Error(
    `fleet operation '${operationId}' does not match its finalize counts`,
  );
}

function unknownOperation(operationId: string): Error {
  return new Error(`no fleet operation '${operationId}'`);
}

function parseJson(value: string): unknown {
  try {
    return JSON.parse(value) as unknown;
  } catch {
    throw new FleetOperationStateError();
  }
}

function rowPayloadFromUnknown(
  kind: FleetOperationKind,
  row: unknown,
): FleetOperationStagedRow {
  const parsed = fleetOperationStagedRowFromUnknown(row);
  if (kind === 'audit') {
    if (parsed.rowKind === 'item') throw new FleetOperationStateError();
    return {
      ...parsed,
      payload:
        parsed.rowKind === 'finding'
          ? driftFindingRowFromUnknown(parsed.payload)
          : parsed.rowKind === 'fact'
            ? fleetAuditFactRowFromUnknown(parsed.payload)
            : parsed.payload,
    };
  }
  if (parsed.rowKind !== 'item') throw new FleetOperationStateError();
  const payload = fleetMigrationItemFromUnknown(parsed.payload);
  if (payload.ordinal !== parsed.ordinal) {
    throw new FleetOperationStateError();
  }
  return {
    ...parsed,
    payload: { ...payload },
  };
}

function serializedPayload(row: FleetOperationStagedRow): string {
  return row.rowKind === 'record'
    ? canonicalFleetOperationBytes(row.payload)
    : JSON.stringify(row.payload);
}

/** Provider-neutral D1 operation store over the fleet state database port. */
export class D1FleetOperationStore implements FleetOperationStore {
  readonly #db: FleetStateDatabase;
  readonly #accountId: string;
  readonly #leaseTtlMs: number;
  readonly #leaseRenewalIntervalMs: number;
  readonly #inventoryStore: FleetInventoryRunStore | undefined;
  #schemaReady: Promise<void> | undefined;

  constructor(db: FleetStateDatabase, options: D1FleetOperationStoreOptions) {
    this.#db = db;
    if (!options.accountId) throw new Error('accountId is required');
    this.#accountId = options.accountId;
    this.#leaseTtlMs = options.leaseTtlMs ?? LEASE_TTL_MS;
    this.#leaseRenewalIntervalMs =
      options.leaseRenewalIntervalMs ?? LEASE_RENEWAL_INTERVAL_MS;
    this.#inventoryStore = options.inventoryStore;
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
    await this.#db.execute(`CREATE TABLE IF NOT EXISTS ${LEASE_TABLE} (
      account_id TEXT NOT NULL,
      operation_kind TEXT NOT NULL CHECK (operation_kind IN (${KIND_CHECK})),
      owner_token TEXT NOT NULL,
      expires_at INTEGER NOT NULL,
      PRIMARY KEY (account_id, operation_kind)
    )`);
    await this.#db.execute(`CREATE TABLE IF NOT EXISTS ${HEAD_TABLE} (
      account_id TEXT NOT NULL,
      operation_kind TEXT NOT NULL CHECK (operation_kind IN (${KIND_CHECK})),
      active_operation_id TEXT,
      PRIMARY KEY (account_id, operation_kind)
    )`);
    await this.#db.execute(`CREATE TABLE IF NOT EXISTS ${OPERATION_TABLE} (
      account_id TEXT NOT NULL,
      operation_id TEXT NOT NULL,
      operation_kind TEXT NOT NULL CHECK (operation_kind IN (${KIND_CHECK})),
      intake_digest TEXT NOT NULL,
      op_record TEXT NOT NULL,
      created_at_ms INTEGER NOT NULL,
      terminal_at_ms INTEGER,
      PRIMARY KEY (account_id, operation_id)
    )`);
    await this.#db.execute(`CREATE TABLE IF NOT EXISTS ${ROW_TABLE} (
      account_id TEXT NOT NULL,
      operation_id TEXT NOT NULL,
      row_kind TEXT NOT NULL CHECK (row_kind IN (${ROW_KIND_CHECK})),
      ordinal INTEGER NOT NULL,
      payload TEXT NOT NULL,
      PRIMARY KEY (account_id, operation_id, row_kind, ordinal)
    )`);
    for (const [table, columns] of Object.entries(EXPECTED_COLUMNS)) {
      const present = await this.#db.query(`PRAGMA table_info(${table})`);
      for (const [name, type] of Object.entries(columns)) {
        const column = present.find((candidate) => candidate.name === name);
        if (!column || String(column.type).toUpperCase() !== type) {
          throw new Error(
            `fleet operation table '${table}' column '${name}' is absent or incompatible`,
          );
        }
      }
    }
  }

  #leaseExists(_kind: FleetOperationKind): string {
    return `EXISTS (SELECT 1 FROM ${LEASE_TABLE}
        WHERE account_id = ? AND operation_kind = ? AND owner_token = ?
          AND expires_at > ${DB_NOW_MS})`;
  }

  #leaseBindings(kind: FleetOperationKind, token: string): readonly unknown[] {
    return [this.#accountId, kind, token];
  }

  #contention(kind: FleetOperationKind): Error {
    return new Error(
      `fleet ${kind} operations for account '${this.#accountId}' are already being modified`,
    );
  }

  #leaseLost(kind: FleetOperationKind): Error {
    return new Error(
      `fleet ${kind} operation lease for account '${this.#accountId}' is no longer owned by this operation`,
    );
  }

  async withAccountOperationLease<T>(
    kind: FleetOperationKind,
    operation: (lease: FleetOperationLease) => Promise<T>,
  ): Promise<T> {
    return this.#withAccountOperationLeaseInternal(kind, (lease) =>
      operation(lease),
    );
  }

  async #withAccountOperationLeaseInternal<T>(
    kind: FleetOperationKind,
    operation: (lease: FleetOperationLease, token: string) => Promise<T>,
  ): Promise<T> {
    assertKind(kind);
    await this.#ensureSchema();
    const token = randomUUID();
    const claimed = await this.#db.query(
      `INSERT INTO ${LEASE_TABLE} (
        account_id, operation_kind, owner_token, expires_at
      ) VALUES (?, ?, ?, ${DB_NOW_MS} + ?)
      ON CONFLICT (account_id, operation_kind) DO UPDATE SET
        owner_token = excluded.owner_token,
        expires_at = excluded.expires_at
      WHERE ${LEASE_TABLE}.expires_at <= ${DB_NOW_MS}
      RETURNING owner_token, expires_at`,
      [this.#accountId, kind, token, this.#leaseTtlMs],
    );
    if (claimed.length !== 1 || claimed[0]?.owner_token !== token) {
      throw this.#contention(kind);
    }
    return this.#runRenewingLease({
      kind,
      token,
      operation: (lease) => operation(lease, token),
    });
  }

  async #runRenewingLease<T>(
    input: Readonly<{
      kind: FleetOperationKind;
      token: string;
      operation: (lease: FleetOperationLease) => Promise<T>;
    }>,
  ): Promise<T> {
    const { kind, token, operation } = input;
    const heartbeatAbort = new AbortController();
    const renewalErrors: unknown[] = [];
    const assertOwned = async () => {
      const heartbeatError = renewalErrors[0];
      if (heartbeatError !== undefined) {
        throw new Error(`fleet ${kind} operation heartbeat failed`, {
          cause: heartbeatError,
        });
      }
      await this.#renewLease(kind, token);
    };
    const heartbeat = this.#renewUntilAborted(
      () => this.#renewLease(kind, token),
      heartbeatAbort.signal,
    ).catch((error: unknown) => renewalErrors.push(error));
    const lease: FleetOperationLease = {
      assertOwned,
      startOperation: (value) => this.#startOperation(kind, token, value),
      readOperation: (operationId) => this.readOperationById(operationId),
      stageRows: (value) => this.#stageRows(kind, token, value),
      commitProgress: (value) => this.#commitProgress(kind, token, value),
      finalizeOperation: (value) => this.#finalizeOperation(kind, token, value),
      failOperation: (value) => this.#failOperation(kind, token, value),
    };

    let operationFailed = false;
    let operationError: unknown;
    let outcome: { readonly value: T } | undefined;
    try {
      outcome = { value: await operation(lease) };
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
    let releaseError: unknown;
    try {
      const released = await this.#db.query(
        `DELETE FROM ${LEASE_TABLE}
          WHERE account_id = ? AND operation_kind = ? AND owner_token = ?
          RETURNING owner_token`,
        [this.#accountId, kind, token],
      );
      if (released.length !== 1 || released[0]?.owner_token !== token) {
        throw this.#leaseLost(kind);
      }
    } catch (error) {
      releaseError = error;
    }
    const errors: unknown[] = [];
    if (operationFailed) errors.push(operationError);
    errors.push(...renewalErrors);
    if (releaseError !== undefined) errors.push(releaseError);
    if (errors.length === 1) throw errors[0];
    if (errors.length > 1) {
      throw new AggregateError(
        errors,
        `fleet ${kind} operation and lease cleanup failed`,
      );
    }
    if (!outcome) throw new Error(`fleet ${kind} operation had no outcome`);
    return outcome.value;
  }

  async #renewUntilAborted(
    renew: () => Promise<void>,
    signal: AbortSignal,
  ): Promise<void> {
    while (await this.#waitForRenewal(signal)) await renew();
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

  async #renewLease(kind: FleetOperationKind, token: string): Promise<void> {
    const renewed = await this.#db.query(
      `UPDATE ${LEASE_TABLE}
        SET expires_at = ${DB_NOW_MS} + ?
        WHERE account_id = ? AND operation_kind = ? AND owner_token = ?
          AND expires_at > ${DB_NOW_MS}
        RETURNING owner_token, expires_at`,
      [this.#leaseTtlMs, this.#accountId, kind, token],
    );
    if (renewed.length !== 1 || renewed[0]?.owner_token !== token) {
      throw this.#leaseLost(kind);
    }
  }

  async #startOperation(
    leaseKind: FleetOperationKind,
    token: string,
    input: Parameters<FleetOperationLease['startOperation']>[0],
  ): ReturnType<FleetOperationLease['startOperation']> {
    const { operationId, kind, intakeDigest } = input;
    const runRecord = fleetOperationRunRecordFromUnknown(input.runRecord);
    if (
      kind !== leaseKind ||
      runRecord.operationId !== operationId ||
      runRecord.kind !== kind ||
      runRecord.state !== 'running' ||
      runRecord.progress.revision !== 0 ||
      !fleetOperationSha256(intakeDigest)
    ) {
      throw new FleetOperationStateError();
    }
    const result = await this.#db.batch([
      {
        sql: `INSERT INTO ${HEAD_TABLE} (
          account_id, operation_kind, active_operation_id
        ) VALUES (?, ?, NULL)
        ON CONFLICT (account_id, operation_kind) DO NOTHING`,
        bindings: [this.#accountId, kind],
      },
      {
        sql: `UPDATE ${HEAD_TABLE}
          SET active_operation_id = ?
          WHERE account_id = ? AND operation_kind = ?
            AND (active_operation_id = ?
              OR (active_operation_id IS NULL
                AND NOT EXISTS (SELECT 1 FROM ${OPERATION_TABLE}
                  WHERE account_id = ? AND operation_id = ?)))
            AND ${this.#leaseExists(kind)}
          RETURNING active_operation_id`,
        bindings: [
          operationId,
          this.#accountId,
          kind,
          operationId,
          this.#accountId,
          operationId,
          ...this.#leaseBindings(kind, token),
        ],
      },
      {
        sql: `INSERT INTO ${OPERATION_TABLE} (
          account_id, operation_id, operation_kind, intake_digest,
          op_record, created_at_ms, terminal_at_ms
        )
        SELECT ?, ?, ?, ?, ?, ${DB_NOW_MS}, NULL
          FROM ${HEAD_TABLE}
          WHERE account_id = ? AND operation_kind = ?
            AND active_operation_id = ?
        ON CONFLICT (account_id, operation_id) DO NOTHING
        RETURNING operation_id`,
        bindings: [
          this.#accountId,
          operationId,
          kind,
          intakeDigest,
          JSON.stringify(runRecord),
          this.#accountId,
          kind,
          operationId,
        ],
      },
    ]);
    const persisted = await this.#operationRow(operationId);
    if (!persisted) {
      throw new Error(
        `another fleet ${kind} operation is active for this account`,
      );
    }
    if (rowString(persisted, 'operation_kind') !== kind) {
      throw new Error(
        `fleet operation '${operationId}' belongs to the other operation kind`,
      );
    }
    if (rowString(persisted, 'intake_digest') !== intakeDigest) {
      throw new Error(
        `fleet operation '${operationId}' already exists with a different intake`,
      );
    }
    const record = fleetOperationRunRecordFromUnknown(
      parseJson(rowString(persisted, 'op_record')),
    );
    const created = (result[2] ?? []).some(
      (row) => row.operation_id === operationId,
    );
    return {
      outcome: created
        ? 'created'
        : record.state === 'running'
          ? 'adopted-running'
          : 'adopted-terminal',
      record,
    };
  }

  async #operationRow(operationId: string): Promise<Row | undefined> {
    const rows = await this.#db.query(
      `SELECT operation_kind, intake_digest, op_record, terminal_at_ms
        FROM ${OPERATION_TABLE}
        WHERE account_id = ? AND operation_id = ?`,
      [this.#accountId, operationId],
    );
    return rows[0];
  }

  async readOperationById(
    operationId: string,
  ): Promise<FleetOperationRunRecord | undefined> {
    await this.#ensureSchema();
    const row = await this.#operationRow(operationId);
    return row
      ? fleetOperationRunRecordFromUnknown(
          parseJson(rowString(row, 'op_record')),
        )
      : undefined;
  }

  #operationGuard(kind: FleetOperationKind): string {
    return `FROM ${OPERATION_TABLE} r
      WHERE r.account_id = ? AND r.operation_id = ?
        AND r.operation_kind = ?
        AND json_extract(r.op_record, '$.state') = 'running'
        AND json_extract(r.op_record, '$.progress.revision') = ?
        AND ${this.#leaseExists(kind)}`;
  }

  #operationGuardBindings(
    kind: FleetOperationKind,
    token: string,
    operationId: string,
    expectedRevision: number,
  ): readonly unknown[] {
    return [
      this.#accountId,
      operationId,
      kind,
      expectedRevision,
      ...this.#leaseBindings(kind, token),
    ];
  }

  async #stageRows(
    kind: FleetOperationKind,
    token: string,
    input: Parameters<FleetOperationLease['stageRows']>[0],
  ): Promise<void> {
    const { operationId, expectedRevision } = input;
    if (!fleetOperationSafeInteger(expectedRevision)) {
      throw new FleetOperationStateError();
    }
    const rows = input.rows.map((row) => rowPayloadFromUnknown(kind, row));
    for (
      let offset = 0;
      offset < rows.length;
      offset += FLEET_OPERATION_STAGE_BATCH_STATEMENTS
    ) {
      const batch = rows.slice(
        offset,
        offset + FLEET_OPERATION_STAGE_BATCH_STATEMENTS,
      );
      await this.#db.batch(
        batch.map((row) => ({
          sql: `INSERT INTO ${ROW_TABLE} (
            account_id, operation_id, row_kind, ordinal, payload
          )
          SELECT ?, ?, ?, ?, ?
          ${this.#operationGuard(kind)}
          ON CONFLICT (account_id, operation_id, row_kind, ordinal) DO NOTHING
          RETURNING row_kind, ordinal`,
          bindings: [
            this.#accountId,
            operationId,
            row.rowKind,
            row.ordinal,
            serializedPayload(row),
            ...this.#operationGuardBindings(
              kind,
              token,
              operationId,
              expectedRevision,
            ),
          ],
        })),
      );
    }
  }

  async #commitProgress(
    kind: FleetOperationKind,
    token: string,
    input: Parameters<FleetOperationLease['commitProgress']>[0],
  ): ReturnType<FleetOperationLease['commitProgress']> {
    const { operationId, expectedRevision } = input;
    const runRecord = fleetOperationRunRecordFromUnknown(input.runRecord);
    const rows = (input.rows ?? []).map((row) =>
      rowPayloadFromUnknown(kind, row),
    );
    const updateRows = (input.updateRows ?? []).map((row) =>
      rowPayloadFromUnknown(kind, row),
    );
    if (updateRows.some((row) => row.rowKind !== 'item')) {
      throw new FleetOperationStateError();
    }
    const mutationKeys = [...rows, ...updateRows].map(
      (row) => `${row.rowKind}:${row.ordinal}`,
    );
    if (new Set(mutationKeys).size !== mutationKeys.length) {
      throw new FleetOperationStateError();
    }
    if (rows.length + updateRows.length + 1 > 100) {
      throw new Error(
        'commitProgress exceeds the operation batch budget of 100 statements',
      );
    }
    if (
      runRecord.operationId !== operationId ||
      runRecord.kind !== kind ||
      runRecord.state !== 'running' ||
      !fleetOperationSafeInteger(expectedRevision) ||
      runRecord.progress.revision !== expectedRevision + 1
    ) {
      throw operationConflict(operationId);
    }
    const watermarks = Object.entries(input.expectedRowWatermarks ?? {}) as [
      FleetOperationRowKind,
      number,
    ][];
    for (const [rowKind, watermark] of watermarks) {
      if (
        !FLEET_OPERATION_ROW_KINDS.includes(rowKind) ||
        !fleetOperationSafeInteger(watermark)
      ) {
        throw new FleetOperationStateError();
      }
    }
    const payloads = [...rows, ...updateRows].map((row) => ({
      row,
      bytes: serializedPayload(row),
    }));
    const insertPayloads = payloads.slice(0, rows.length);
    const updatePayloads = payloads.slice(rows.length);
    const guardBindings = this.#operationGuardBindings(
      kind,
      token,
      operationId,
      expectedRevision,
    );
    // A retry may stage byte-identical later members before this transition;
    // later watermarks and finalize's totals cover those surplus ordinals.
    const watermarkSql = watermarks
      .map(
        () => `AND (SELECT COUNT(*) FROM ${ROW_TABLE}
          WHERE account_id = ? AND operation_id = ?
            AND row_kind = ? AND ordinal < ?) = ?`,
      )
      .join('\n');
    // Every row mutation carries the SAME lease, kind, state, and PRE-update
    // revision guard as the operation update, so stale or losing writers land
    // no bytes that a later legitimate commit cannot replace.
    const result = await this.#db.batch([
      ...insertPayloads.map(({ row, bytes }) => ({
        sql: `INSERT INTO ${ROW_TABLE} (
          account_id, operation_id, row_kind, ordinal, payload
        )
        SELECT ?, ?, ?, ?, ?
        ${this.#operationGuard(kind)}
        ON CONFLICT (account_id, operation_id, row_kind, ordinal) DO NOTHING
        RETURNING row_kind, ordinal`,
        bindings: [
          this.#accountId,
          operationId,
          row.rowKind,
          row.ordinal,
          bytes,
          ...guardBindings,
        ],
      })),
      ...updatePayloads.map(({ row, bytes }) => ({
        sql: `UPDATE ${ROW_TABLE}
          SET payload = ?
          WHERE account_id = ? AND operation_id = ?
            AND row_kind = ? AND ordinal = ?
            AND EXISTS (SELECT 1 ${this.#operationGuard(kind)})
          RETURNING row_kind, ordinal`,
        bindings: [
          bytes,
          this.#accountId,
          operationId,
          row.rowKind,
          row.ordinal,
          ...guardBindings,
        ],
      })),
      {
        sql: `UPDATE ${OPERATION_TABLE}
          SET op_record = ?
          WHERE account_id = ? AND operation_id = ?
            AND operation_kind = ?
            AND json_extract(op_record, '$.state') = 'running'
            AND json_extract(op_record, '$.progress.revision') = ?
            AND ${this.#leaseExists(kind)}
            ${watermarkSql}
          RETURNING operation_id`,
        bindings: [
          JSON.stringify(runRecord),
          this.#accountId,
          operationId,
          kind,
          expectedRevision,
          ...this.#leaseBindings(kind, token),
          ...watermarks.flatMap(([rowKind, watermark]) => [
            this.#accountId,
            operationId,
            rowKind,
            watermark,
            watermark,
          ]),
        ],
      },
    ]);
    const written = result.at(-1) ?? [];
    if (written.length === 1 && written[0]?.operation_id === operationId) {
      return runRecord;
    }
    // Insert RETURNING proves nothing either way: DO NOTHING and guard misses
    // both return no rows, so convergence must re-query every authority.
    return this.#commitConverged(operationId, runRecord, payloads, watermarks);
  }

  async #commitConverged(
    operationId: string,
    intended: FleetOperationRunRecord,
    payloads: readonly Readonly<{
      row: FleetOperationStagedRow;
      bytes: string;
    }>[],
    watermarks: readonly [FleetOperationRowKind, number][],
  ): Promise<FleetOperationRunRecord> {
    let complete = true;
    for (const { row, bytes } of payloads) {
      const stored = await this.#db.query(
        `SELECT payload FROM ${ROW_TABLE}
          WHERE account_id = ? AND operation_id = ?
            AND row_kind = ? AND ordinal = ?`,
        [this.#accountId, operationId, row.rowKind, row.ordinal],
      );
      if (!stored[0]) complete = false;
      else if (rowString(stored[0], 'payload') !== bytes) {
        throw operationDivergence(operationId);
      }
    }
    for (const [rowKind, watermark] of watermarks) {
      const stored = await this.#db.query(
        `SELECT COUNT(*) AS count FROM ${ROW_TABLE}
          WHERE account_id = ? AND operation_id = ?
            AND row_kind = ? AND ordinal < ?`,
        [this.#accountId, operationId, rowKind, watermark],
      );
      if (rowNumber(stored[0], 'count') !== watermark) {
        throw operationConflict(operationId);
      }
    }
    const persisted = await this.readOperationById(operationId);
    if (!persisted) throw unknownOperation(operationId);
    // Progress uses plain JSON equality; coordinators must build it in stable
    // key order so a byte-identical replay can converge.
    if (
      complete &&
      persisted.progress.revision === intended.progress.revision &&
      JSON.stringify(persisted) === JSON.stringify(intended)
    ) {
      return persisted;
    }
    throw operationConflict(operationId);
  }

  async #finalizeOperation(
    kind: FleetOperationKind,
    token: string,
    input: Parameters<FleetOperationLease['finalizeOperation']>[0],
  ): ReturnType<FleetOperationLease['finalizeOperation']> {
    const { operationId, expectedRevision } = input;
    const runRecord = fleetOperationRunRecordFromUnknown(input.runRecord);
    if (
      runRecord.operationId !== operationId ||
      runRecord.kind !== kind ||
      runRecord.state !== 'finalized' ||
      !fleetOperationSafeInteger(expectedRevision) ||
      runRecord.progress.revision !== expectedRevision + 1
    ) {
      throw operationConflict(operationId);
    }
    const counts = Object.entries(input.expectedRowCounts) as [
      FleetOperationRowKind,
      number,
    ][];
    for (const [rowKind, count] of counts) {
      if (
        !FLEET_OPERATION_ROW_KINDS.includes(rowKind) ||
        !fleetOperationSafeInteger(count)
      ) {
        throw new FleetOperationStateError();
      }
    }
    const countSql = counts
      .map(
        () => `AND (SELECT COUNT(*) FROM ${ROW_TABLE}
          WHERE account_id = ? AND operation_id = ? AND row_kind = ?) = ?`,
      )
      .join('\n');
    const completeSql = input.requireAllItemsComplete
      ? `AND (SELECT COUNT(*) FROM ${ROW_TABLE}
          WHERE account_id = ? AND operation_id = ? AND row_kind = 'item'
            AND json_extract(payload, '$.status') = 'complete')
          = json_extract(?, '$.progress.itemCount')`
      : '';
    await this.#db.batch([
      {
        sql: `UPDATE ${OPERATION_TABLE}
          SET op_record = json_set(?, '$.terminalAtMs', ${DB_NOW_MS}),
              terminal_at_ms = ${DB_NOW_MS}
          WHERE account_id = ? AND operation_id = ?
            AND operation_kind = ?
            AND json_extract(op_record, '$.state') = 'running'
            AND json_extract(op_record, '$.progress.revision') = ?
            AND ${this.#leaseExists(kind)}
            ${countSql}
            ${completeSql}
          RETURNING operation_id`,
        bindings: [
          JSON.stringify(runRecord),
          this.#accountId,
          operationId,
          kind,
          expectedRevision,
          ...this.#leaseBindings(kind, token),
          ...counts.flatMap(([rowKind, count]) => [
            this.#accountId,
            operationId,
            rowKind,
            count,
          ]),
          ...(input.requireAllItemsComplete
            ? [this.#accountId, operationId, JSON.stringify(runRecord)]
            : []),
        ],
      },
      {
        sql: `UPDATE ${HEAD_TABLE}
          SET active_operation_id = NULL
          WHERE account_id = ? AND operation_kind = ?
            AND active_operation_id = ?
            AND ${this.#leaseExists(kind)}
            AND EXISTS (SELECT 1 FROM ${OPERATION_TABLE}
              WHERE account_id = ? AND operation_id = ?
                AND operation_kind = ?
                AND json_extract(op_record, '$.state') = 'finalized'
                AND json_extract(op_record, '$.progress.revision') = ?)
          RETURNING account_id`,
        bindings: [
          this.#accountId,
          kind,
          operationId,
          ...this.#leaseBindings(kind, token),
          this.#accountId,
          operationId,
          kind,
          runRecord.progress.revision,
        ],
      },
    ]);
    // This batch is a probe: a lost response makes RETURNING inconclusive, so
    // the operation row and head are the only authority.
    const persisted = await this.readOperationById(operationId);
    if (!persisted) throw unknownOperation(operationId);
    if (
      persisted.state !== 'finalized' ||
      persisted.progress.revision !== runRecord.progress.revision
    ) {
      if (persisted.progress.revision === expectedRevision) {
        throw finalizeMismatch(operationId);
      }
      throw operationConflict(operationId);
    }
    const head = await this.#db.query(
      `SELECT active_operation_id FROM ${HEAD_TABLE}
        WHERE account_id = ? AND operation_kind = ?`,
      [this.#accountId, kind],
    );
    if (head[0]?.active_operation_id === operationId) {
      // The only legal repair is the head release; operation data is immutable.
      await this.#db.query(
        `UPDATE ${HEAD_TABLE}
          SET active_operation_id = NULL
          WHERE account_id = ? AND operation_kind = ?
            AND active_operation_id = ?
            AND ${this.#leaseExists(kind)}
          RETURNING account_id`,
        [
          this.#accountId,
          kind,
          operationId,
          ...this.#leaseBindings(kind, token),
        ],
      );
    }
    return persisted;
  }

  async #failOperation(
    kind: FleetOperationKind,
    token: string,
    input: Parameters<FleetOperationLease['failOperation']>[0],
  ): Promise<void> {
    const { operationId, expectedRevision } = input;
    const runRecord = fleetOperationRunRecordFromUnknown(input.runRecord);
    // The run update binds 8 + 5n parameters; D1 caps a statement at 100.
    if ((input.updateRows?.length ?? 0) > 18) {
      throw new Error(
        'failOperation exceeds the operation update budget of 18 rows',
      );
    }
    const updateRows = (input.updateRows ?? []).map((row) =>
      rowPayloadFromUnknown(kind, row),
    );
    if (updateRows.some((row) => row.rowKind !== 'item')) {
      throw new FleetOperationStateError();
    }
    const payloads = updateRows.map((row) => ({
      row,
      bytes: serializedPayload(row),
    }));
    const updateKeys = updateRows.map((row) => `${row.rowKind}:${row.ordinal}`);
    if (new Set(updateKeys).size !== updateKeys.length) {
      throw new FleetOperationStateError();
    }
    if (
      runRecord.operationId !== operationId ||
      runRecord.kind !== kind ||
      runRecord.state !== 'failed' ||
      runRecord.progress.failure === undefined ||
      !fleetOperationSafeInteger(expectedRevision) ||
      runRecord.progress.revision !== expectedRevision + 1
    ) {
      throw operationConflict(operationId);
    }
    const guardBindings = this.#operationGuardBindings(
      kind,
      token,
      operationId,
      expectedRevision,
    );
    const exactRows = updateRows
      .map(
        () => `AND EXISTS (SELECT 1 FROM ${ROW_TABLE}
          WHERE account_id = ? AND operation_id = ?
            AND row_kind = ? AND ordinal = ? AND payload = ?)`,
      )
      .join('\n');
    await this.#db.batch([
      ...payloads.map(({ row, bytes }) => ({
        sql: `UPDATE ${ROW_TABLE}
          SET payload = ?
          WHERE account_id = ? AND operation_id = ?
            AND row_kind = ? AND ordinal = ?
            AND EXISTS (SELECT 1 ${this.#operationGuard(kind)})
          RETURNING row_kind, ordinal`,
        bindings: [
          bytes,
          this.#accountId,
          operationId,
          row.rowKind,
          row.ordinal,
          ...guardBindings,
        ],
      })),
      {
        sql: `UPDATE ${OPERATION_TABLE}
          SET op_record = json_set(?, '$.terminalAtMs', ${DB_NOW_MS}),
              terminal_at_ms = ${DB_NOW_MS}
          WHERE account_id = ? AND operation_id = ?
            AND operation_kind = ?
            AND json_extract(op_record, '$.state') = 'running'
            AND json_extract(op_record, '$.progress.revision') = ?
            AND ${this.#leaseExists(kind)}
            ${exactRows}
          RETURNING operation_id`,
        bindings: [
          JSON.stringify(runRecord),
          this.#accountId,
          operationId,
          kind,
          expectedRevision,
          ...this.#leaseBindings(kind, token),
          ...payloads.flatMap(({ row, bytes }) => [
            this.#accountId,
            operationId,
            row.rowKind,
            row.ordinal,
            bytes,
          ]),
        ],
      },
      {
        sql: `UPDATE ${HEAD_TABLE}
          SET active_operation_id = NULL
          WHERE account_id = ? AND operation_kind = ?
            AND active_operation_id = ?
            AND ${this.#leaseExists(kind)}
            AND EXISTS (SELECT 1 FROM ${OPERATION_TABLE}
              WHERE account_id = ? AND operation_id = ?
                AND operation_kind = ?
                AND json_extract(op_record, '$.state') = 'failed'
                AND json_extract(op_record, '$.progress.revision') = ?)
          RETURNING account_id`,
        bindings: [
          this.#accountId,
          kind,
          operationId,
          ...this.#leaseBindings(kind, token),
          this.#accountId,
          operationId,
          kind,
          runRecord.progress.revision,
        ],
      },
    ]);
    const persisted = await this.readOperationById(operationId);
    if (
      persisted?.state !== 'failed' ||
      persisted.progress.revision !== runRecord.progress.revision
    ) {
      throw operationConflict(operationId);
    }
    for (const { row, bytes } of payloads) {
      const stored = await this.#db.query(
        `SELECT payload FROM ${ROW_TABLE}
          WHERE account_id = ? AND operation_id = ?
            AND row_kind = ? AND ordinal = ?`,
        [this.#accountId, operationId, row.rowKind, row.ordinal],
      );
      if (!stored[0] || rowString(stored[0], 'payload') !== bytes) {
        throw operationDivergence(operationId);
      }
    }
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
    await this.#ensureSchema();
    assertLimit(input.limit);
    if (
      !FLEET_OPERATION_ROW_KINDS.includes(input.rowKind) ||
      (input.afterOrdinal !== undefined &&
        (!fleetOperationSafeInteger(input.afterOrdinal) ||
          input.afterOrdinal >= Number.MAX_SAFE_INTEGER))
    ) {
      throw new FleetOperationStateError();
    }
    const operation = await this.#operationRow(input.operationId);
    if (!operation) throw unknownOperation(input.operationId);
    const kind = rowString(operation, 'operation_kind') as FleetOperationKind;
    const stored = await this.#db.query(
      `SELECT row_kind, ordinal, payload FROM ${ROW_TABLE}
        WHERE account_id = ? AND operation_id = ? AND row_kind = ?
          AND ordinal > ?
        ORDER BY ordinal ASC
        LIMIT ?`,
      [
        this.#accountId,
        input.operationId,
        input.rowKind,
        input.afterOrdinal ?? -1,
        input.limit + 1,
      ],
    );
    const rows = stored.slice(0, input.limit).map((row) =>
      rowPayloadFromUnknown(kind, {
        rowKind: rowString(row, 'row_kind'),
        ordinal: rowNumber(row, 'ordinal'),
        payload: parseJson(rowString(row, 'payload')),
      }),
    );
    return { rows, done: stored.length <= input.limit };
  }

  async pruneFleetOperations(
    input: Readonly<{
      kind: FleetOperationKind;
      limit: number;
    }>,
  ): Promise<Readonly<{ deleted: number; releasedPins: number }>> {
    assertKind(input.kind);
    assertLimit(input.limit);
    if (input.kind === 'audit' && !this.#inventoryStore) {
      throw new FleetOperationStoreCapabilityError();
    }
    return this.#withAccountOperationLeaseInternal(
      input.kind,
      async (lease, token) => {
        const candidates = await this.#db.query(
          `SELECT operation_id, op_record, terminal_at_ms
          FROM ${OPERATION_TABLE} o
          WHERE o.account_id = ? AND o.operation_kind = ?
            AND o.terminal_at_ms IS NOT NULL
            AND json_extract(o.op_record, '$.state') IN ('finalized','failed')
            AND NOT EXISTS (SELECT 1 FROM ${HEAD_TABLE}
              WHERE account_id = o.account_id
                AND operation_kind = o.operation_kind
                AND active_operation_id = o.operation_id)
            AND NOT (
              json_extract(o.op_record, '$.state') = 'finalized'
              AND o.terminal_at_ms = (SELECT MAX(latest.terminal_at_ms)
                FROM ${OPERATION_TABLE} latest
                WHERE latest.account_id = o.account_id
                  AND latest.operation_kind = o.operation_kind
                  AND json_extract(latest.op_record, '$.state') = 'finalized')
            )
          ORDER BY o.terminal_at_ms ASC, o.operation_id ASC
          LIMIT ?`,
          [this.#accountId, input.kind, input.limit],
        );
        let deleted = 0;
        let releasedPins = 0;
        for (const candidate of candidates) {
          const operationId = rowString(candidate, 'operation_id');
          if (input.kind === 'audit') {
            const record = fleetAuditOperationRecordFromUnknown(
              parseJson(rowString(candidate, 'op_record')),
            );
            // Lock order is operation KIND lease outer, inventory ACCOUNT lease
            // inner, and is never acquired in reverse by production callers.
            await this.#inventoryStore?.releasePin({
              generation: record.progress.generation,
              pinnedBy: `fleet-audit:${operationId}`,
            });
            releasedPins += 1;
          }
          await lease.assertOwned();
          deleted += await this.#deletePruneCandidate(
            input.kind,
            token,
            operationId,
          );
        }
        return { deleted, releasedPins };
      },
    );
  }

  async #deletePruneCandidate(
    kind: FleetOperationKind,
    token: string,
    operationId: string,
  ): Promise<number> {
    // Protection is re-checked inside the delete batch, so a candidate that
    // becomes active or latest-finalized after selection still survives.
    const guard = `AND EXISTS (SELECT 1 FROM ${OPERATION_TABLE} o
      WHERE o.account_id = ? AND o.operation_id = ?
        AND o.operation_kind = ? AND o.terminal_at_ms IS NOT NULL
        AND json_extract(o.op_record, '$.state') IN ('finalized','failed')
        AND NOT EXISTS (SELECT 1 FROM ${HEAD_TABLE}
          WHERE account_id = o.account_id
            AND operation_kind = o.operation_kind
            AND active_operation_id = o.operation_id)
        AND NOT (
          json_extract(o.op_record, '$.state') = 'finalized'
          AND o.terminal_at_ms = (SELECT MAX(latest.terminal_at_ms)
            FROM ${OPERATION_TABLE} latest
            WHERE latest.account_id = o.account_id
              AND latest.operation_kind = o.operation_kind
              AND json_extract(latest.op_record, '$.state') = 'finalized')
        ))
      AND ${this.#leaseExists(kind)}`;
    const guardBindings = [
      this.#accountId,
      operationId,
      kind,
      ...this.#leaseBindings(kind, token),
    ];
    const result = await this.#db.batch([
      {
        sql: `DELETE FROM ${ROW_TABLE}
          WHERE account_id = ? AND operation_id = ?
          ${guard}
          RETURNING ordinal`,
        bindings: [this.#accountId, operationId, ...guardBindings],
      },
      {
        sql: `DELETE FROM ${OPERATION_TABLE}
          WHERE account_id = ? AND operation_id = ?
          ${guard}
          RETURNING operation_id`,
        bindings: [this.#accountId, operationId, ...guardBindings],
      },
    ]);
    return (result.at(-1) ?? []).length;
  }
}
