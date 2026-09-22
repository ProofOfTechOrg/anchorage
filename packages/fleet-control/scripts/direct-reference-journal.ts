// SPDX-License-Identifier: Apache-2.0

import { Buffer } from 'node:buffer';
import { createHash } from 'node:crypto';
import type { D1Database } from '@cloudflare/workers-types';
import { D1FleetStateDatabase } from '@proofoftech/fleet-control/cloudflare-control-plane';
import type { DirectFixtureRole } from './direct-credentialed-spec.js';
import type {
  DirectAuditSlot,
  DirectInventorySlot,
} from './direct-reference-contract.mjs';

export type DirectOperationSlot =
  | DirectInventorySlot
  | DirectAuditSlot
  | 'migration-next'
  | 'cleanup-recovery-initial'
  | 'cleanup-a-reprovision'
  | 'decommission-a-reprovision'
  | `cleanup-${DirectFixtureRole}`
  | `decommission-${DirectFixtureRole}`;
export type DirectOperationKind =
  | 'inventory'
  | 'audit'
  | 'migration'
  | 'cleanup'
  | 'decommission';
export type DirectJournalErrorCode =
  | 'journal-state'
  | 'run-binding-mismatch'
  | 'duplicate-ordinal'
  | 'operation-mismatch'
  | 'prerequisite-unavailable'
  | 'missing-start';

export class DirectReferenceJournalError extends Error {
  readonly code: DirectJournalErrorCode;
  constructor(code: DirectJournalErrorCode = 'journal-state') {
    super(code);
    this.name = 'DirectReferenceJournalError';
    this.code = code;
  }
}

export interface DirectStartCandidate {
  readonly operationId: string | null;
  readonly inputJson: string;
}

export interface DirectStoredOperation extends DirectStartCandidate {
  readonly slot: DirectOperationSlot;
  readonly kind: DirectOperationKind;
  readonly tokenJson: string | null;
  readonly tokenRevision: number | null;
}

export interface DirectFrozenStart extends DirectStoredOperation {
  readonly inserted: boolean;
}

export interface DirectStoredObservation {
  readonly identityJson: string;
  readonly provenanceJson: string;
}

export interface DirectStoredResource extends DirectStoredObservation {
  readonly identitySha256: string;
}

export type DirectInvocationLedgerState =
  | 'received'
  | 'executed'
  | 'failed'
  | 'cancelled';

export interface DirectStoredInvocation {
  readonly ordinal: number;
  readonly requestSha256: string;
  readonly state: DirectInvocationLedgerState;
}

type ObservationKind =
  | 'resource'
  | 'settlement'
  | 'force-before'
  | 'force-after';

const MAX_JSON_BYTES = 256 * 1024;
const schema = [
  `CREATE TABLE IF NOT EXISTS direct_reference_run (
    run_key TEXT PRIMARY KEY,
    binding_json TEXT NOT NULL,
    binding_sha256 TEXT NOT NULL,
    interruption_json TEXT,
    interruption_sha256 TEXT,
    CHECK ((interruption_json IS NULL) = (interruption_sha256 IS NULL))
  )`,
  `CREATE TABLE IF NOT EXISTS direct_reference_operations (
    run_key TEXT NOT NULL REFERENCES direct_reference_run(run_key),
    slot TEXT NOT NULL,
    operation_kind TEXT NOT NULL,
    operation_id TEXT,
    start_json TEXT NOT NULL,
    start_sha256 TEXT NOT NULL,
    token_json TEXT,
    token_sha256 TEXT,
    token_revision INTEGER,
    PRIMARY KEY (run_key, slot),
    CHECK ((token_json IS NULL AND token_sha256 IS NULL AND token_revision IS NULL)
      OR (token_json IS NOT NULL AND token_sha256 IS NOT NULL AND token_revision IS NOT NULL))
  )`,
  `CREATE TABLE IF NOT EXISTS direct_reference_observations (
    run_key TEXT NOT NULL REFERENCES direct_reference_run(run_key),
    observation_kind TEXT NOT NULL,
    observation_key TEXT NOT NULL,
    identity_json TEXT NOT NULL,
    identity_sha256 TEXT NOT NULL,
    provenance_json TEXT NOT NULL,
    provenance_sha256 TEXT NOT NULL,
    PRIMARY KEY (run_key, observation_kind, observation_key)
  )`,
  `CREATE TABLE IF NOT EXISTS direct_reference_invocations (
    run_key TEXT NOT NULL REFERENCES direct_reference_run(run_key),
    ordinal INTEGER NOT NULL,
    request_sha256 TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('received','executed','failed','cancelled')),
    PRIMARY KEY (run_key, ordinal)
  )`,
];

function stateError(): never {
  throw new DirectReferenceJournalError();
}

function text(value: unknown): string {
  if (typeof value !== 'string' || !value || value.length > 128) stateError();
  return value;
}

function role(value: DirectFixtureRole): DirectFixtureRole {
  if (value !== 'a' && value !== 'b' && value !== 'recovery') stateError();
  return value;
}

function sha256(value: string): string {
  if (typeof value !== 'string' || !/^[a-f0-9]{64}$/u.test(value)) stateError();
  return value;
}

function kindFor(slot: string): DirectOperationKind {
  if (slot === 'inventory-before' || slot === 'inventory-after')
    return 'inventory';
  if (slot === 'audit-before' || slot === 'audit-after') return 'audit';
  if (slot === 'migration-next') return 'migration';
  if (slot === 'cleanup-recovery-initial') return 'cleanup';
  if (slot === 'cleanup-a-reprovision') return 'cleanup';
  if (slot === 'decommission-a-reprovision') return 'decommission';
  for (const kind of ['cleanup', 'decommission'] as const)
    if (
      ['a', 'b', 'recovery'].some(
        (fixtureRole) => slot === `${kind}-${fixtureRole}`,
      )
    )
      return kind;
  return stateError();
}

function jsonObject(value: unknown): {
  text: string;
  value: Record<string, unknown>;
} {
  if (typeof value !== 'string' || Buffer.byteLength(value) > MAX_JSON_BYTES)
    stateError();
  let parsed: unknown;
  try {
    parsed = JSON.parse(value);
  } catch {
    stateError();
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed))
    stateError();
  return {
    text: value,
    value: parsed as Record<string, unknown>,
  };
}

function boundHash(context: readonly unknown[], value: string): string {
  return createHash('sha256')
    .update(JSON.stringify([...context, value]))
    .digest('hex');
}

function storedJson(
  value: unknown,
  hash: unknown,
  context: readonly unknown[],
) {
  const parsed = jsonObject(value);
  if (boundHash(context, parsed.text) !== hash) stateError();
  return parsed;
}

function tokenFields(token: Record<string, unknown>) {
  const operationId = text(token.operationId);
  const revision = token.revision;
  if (
    typeof revision !== 'number' ||
    !Number.isSafeInteger(revision) ||
    revision < 0
  )
    stateError();
  return { operationId, revision };
}

export class DirectReferenceJournal {
  readonly #database: D1FleetStateDatabase;
  readonly #runKey: string;
  readonly #binding: ReturnType<typeof jsonObject>;
  readonly #bindingHash: string;
  readonly #maxInvocations: number;
  #ready?: Promise<void>;

  constructor(
    database: D1Database,
    runKey: string,
    bindingJson: string,
    maxInvocations = 1_000,
  ) {
    this.#database = new D1FleetStateDatabase(database);
    this.#runKey = text(runKey);
    this.#binding = jsonObject(bindingJson);
    this.#bindingHash = boundHash(
      ['binding', this.#runKey],
      this.#binding.text,
    );
    if (!Number.isSafeInteger(maxInvocations) || maxInvocations < 1)
      stateError();
    this.#maxInvocations = maxInvocations;
  }

  async #initialize(): Promise<void> {
    await this.#database.batch(schema.map((sql) => ({ sql })));
    await this.#database.execute(
      'INSERT INTO direct_reference_run (run_key,binding_json,binding_sha256) VALUES (?,?,?) ON CONFLICT DO NOTHING',
      [this.#runKey, this.#binding.text, this.#bindingHash],
    );
    const rows = await this.#database.query(
      'SELECT * FROM direct_reference_run WHERE run_key=?',
      [this.#runKey],
    );
    const row = rows[0];
    if (rows.length !== 1 || !row) stateError();
    storedJson(row.binding_json, row.binding_sha256, ['binding', this.#runKey]);
    if (
      row.binding_json !== this.#binding.text ||
      row.binding_sha256 !== this.#bindingHash
    )
      throw new DirectReferenceJournalError('run-binding-mismatch');
  }

  async #withState<T>(operation: () => Promise<T>): Promise<T> {
    try {
      this.#ready ??= this.#initialize().catch((error) => {
        this.#ready = undefined;
        throw error;
      });
      await this.#ready;
      return await operation();
    } catch (error) {
      let code: DirectJournalErrorCode = 'journal-state';
      try {
        if (error instanceof DirectReferenceJournalError) {
          const candidate = error.code;
          if (
            candidate === 'journal-state' ||
            candidate === 'run-binding-mismatch' ||
            candidate === 'duplicate-ordinal' ||
            candidate === 'operation-mismatch' ||
            candidate === 'prerequisite-unavailable' ||
            candidate === 'missing-start'
          )
            code = candidate;
        }
      } catch {
        // Class and code inspection can invoke traps on a foreign rejection.
      }
      throw new DirectReferenceJournalError(code);
    }
  }

  #invocationReservation(
    reservation: Readonly<{
      ordinal: number;
      requestSha256: string;
    }>,
  ) {
    if (
      !Number.isSafeInteger(reservation.ordinal) ||
      reservation.ordinal < 1 ||
      reservation.ordinal > this.#maxInvocations
    )
      stateError();
    return {
      ordinal: reservation.ordinal,
      requestSha256: sha256(reservation.requestSha256),
    };
  }

  #storedInvocation(row: Readonly<Record<string, unknown>> | undefined) {
    if (!row) stateError();
    const ordinal = row.ordinal;
    const state = row.state;
    if (
      typeof ordinal !== 'number' ||
      !Number.isSafeInteger(ordinal) ||
      ordinal < 1 ||
      ordinal > this.#maxInvocations ||
      !['received', 'executed', 'failed', 'cancelled'].includes(state as string)
    )
      stateError();
    return Object.freeze({
      ordinal,
      requestSha256: sha256(row.request_sha256 as string),
      state: state as DirectInvocationLedgerState,
    });
  }

  receiveInvocation(
    reservation: Readonly<{ ordinal: number; requestSha256: string }>,
    readOnly: boolean,
  ): Promise<void> {
    return this.#withState(async () => {
      const checked = this.#invocationReservation(reservation);
      if (typeof readOnly !== 'boolean') stateError();
      // `received` precedes dispatch, which is not a D1 statement. The state
      // therefore covers never-started, in-flight and completed-unrecorded work.
      const results = await this.#database.batch([
        {
          sql: "INSERT INTO direct_reference_invocations (run_key,ordinal,request_sha256,state) VALUES (?,?,?,'received') ON CONFLICT DO NOTHING RETURNING ordinal",
          bindings: [this.#runKey, checked.ordinal, checked.requestSha256],
        },
        ...(readOnly
          ? [
              {
                sql: "UPDATE direct_reference_invocations SET state='received' WHERE run_key=? AND ordinal=? AND request_sha256=? AND state!='cancelled' RETURNING ordinal",
                bindings: [
                  this.#runKey,
                  checked.ordinal,
                  checked.requestSha256,
                ],
              },
            ]
          : []),
        {
          sql: 'SELECT ordinal,request_sha256,state FROM direct_reference_invocations WHERE run_key=? AND ordinal=?',
          bindings: [this.#runKey, checked.ordinal],
        },
      ]);
      const inserted = results[0] ?? [];
      const updated = readOnly ? (results[1] ?? []) : inserted;
      const selected = results[readOnly ? 2 : 1] ?? [];
      if (inserted.length > 1 || updated.length > 1 || selected.length !== 1)
        stateError();
      const stored = this.#storedInvocation(selected[0]);
      if (
        stored.requestSha256 !== checked.requestSha256 ||
        stored.state === 'cancelled' ||
        (inserted.length === 0 && !readOnly) ||
        (readOnly && updated.length !== 1)
      )
        throw new DirectReferenceJournalError('duplicate-ordinal');
    });
  }

  settleReceivedInvocation(
    reservation: Readonly<{ ordinal: number; requestSha256: string }>,
    state: 'executed' | 'failed',
  ): Promise<void> {
    return this.#withState(async () => {
      const checked = this.#invocationReservation(reservation);
      if (state !== 'executed' && state !== 'failed') stateError();
      const rows = await this.#database.query(
        "UPDATE direct_reference_invocations SET state=? WHERE run_key=? AND ordinal=? AND request_sha256=? AND state!='cancelled' RETURNING ordinal,request_sha256,state",
        [state, this.#runKey, checked.ordinal, checked.requestSha256],
      );
      if (rows.length !== 1) stateError();
      const stored = this.#storedInvocation(rows[0]);
      if (
        stored.ordinal !== checked.ordinal ||
        stored.requestSha256 !== checked.requestSha256 ||
        stored.state !== state
      )
        stateError();
    });
  }

  reconcileInvocation(
    reservation: Readonly<{ ordinal: number; requestSha256: string }>,
  ): Promise<DirectInvocationLedgerState> {
    return this.#withState(async () => {
      const checked = this.#invocationReservation(reservation);
      const results = await this.#database.batch([
        {
          sql: "INSERT INTO direct_reference_invocations (run_key,ordinal,request_sha256,state) VALUES (?,?,?,'cancelled') ON CONFLICT DO NOTHING RETURNING ordinal",
          bindings: [this.#runKey, checked.ordinal, checked.requestSha256],
        },
        {
          sql: 'SELECT ordinal,request_sha256,state FROM direct_reference_invocations WHERE run_key=? AND ordinal=?',
          bindings: [this.#runKey, checked.ordinal],
        },
      ]);
      if ((results[0]?.length ?? 0) > 1 || results[1]?.length !== 1)
        stateError();
      const stored = this.#storedInvocation(results[1]?.[0]);
      if (stored.requestSha256 !== checked.requestSha256)
        throw new DirectReferenceJournalError('duplicate-ordinal');
      return stored.state;
    });
  }

  async #operation(
    slot: DirectOperationSlot,
  ): Promise<DirectStoredOperation | undefined> {
    const kind = kindFor(slot);
    const rows = await this.#database.query(
      'SELECT * FROM direct_reference_operations WHERE run_key=? AND slot=?',
      [this.#runKey, slot],
    );
    if (rows.length > 1) stateError();
    const row = rows[0];
    if (!row) return undefined;
    if (row.operation_kind !== kind) stateError();
    const operationId =
      row.operation_id === null ? null : text(row.operation_id);
    const assignedByFleet = kind === 'cleanup' || kind === 'decommission';
    if (operationId === null && !assignedByFleet) stateError();
    if (assignedByFleet && operationId !== null && row.token_json === null)
      stateError();
    const start = storedJson(row.start_json, row.start_sha256, [
      'start',
      this.#runKey,
      slot,
      kind,
      assignedByFleet ? null : operationId,
    ]);
    let tokenJson: string | null = null;
    let tokenRevision: number | null = null;
    if (row.token_json !== null) {
      const token = storedJson(row.token_json, row.token_sha256, [
        'token',
        this.#runKey,
        slot,
      ]);
      const fields = tokenFields(token.value);
      if (
        fields.operationId !== operationId ||
        fields.revision !== row.token_revision
      )
        stateError();
      tokenJson = token.text;
      tokenRevision = fields.revision;
    } else if (row.token_sha256 !== null || row.token_revision !== null)
      stateError();
    return Object.freeze({
      slot,
      kind,
      operationId,
      inputJson: start.text,
      tokenJson,
      tokenRevision,
    });
  }

  readOperation(
    slot: DirectOperationSlot,
  ): Promise<DirectStoredOperation | undefined> {
    return this.#withState(() => this.#operation(slot));
  }

  freezeStart(
    slot: DirectOperationSlot,
    create: () => Promise<DirectStartCandidate>,
  ): Promise<DirectFrozenStart> {
    return this.#withState(async () => {
      const prior = await this.#operation(slot);
      if (prior) return Object.freeze({ ...prior, inserted: false });
      const candidate = await create();
      const kind = kindFor(slot);
      const operationId =
        candidate.operationId === null ? null : text(candidate.operationId);
      const assignedByFleet = kind === 'cleanup' || kind === 'decommission';
      if ((operationId === null) !== assignedByFleet) stateError();
      const input = jsonObject(candidate.inputJson);
      const inserted = await this.#database.query(
        'INSERT INTO direct_reference_operations (run_key,slot,operation_kind,operation_id,start_json,start_sha256) VALUES (?,?,?,?,?,?) ON CONFLICT DO NOTHING RETURNING slot',
        [
          this.#runKey,
          slot,
          kind,
          operationId,
          input.text,
          boundHash(
            ['start', this.#runKey, slot, kind, operationId],
            input.text,
          ),
        ],
      );
      if (
        inserted.length > 1 ||
        (inserted.length === 1 && inserted[0]?.slot !== slot)
      )
        stateError();
      const winner = await this.#operation(slot);
      if (!winner) stateError();
      if (inserted.length === 1 && winner.inputJson !== input.text)
        stateError();
      return Object.freeze({ ...winner, inserted: inserted.length === 1 });
    });
  }

  rememberToken(slot: DirectOperationSlot, tokenJson: string): Promise<void> {
    return this.#withState(async () => {
      const before = await this.#operation(slot);
      if (!before) throw new DirectReferenceJournalError('missing-start');
      const token = jsonObject(tokenJson);
      const { operationId, revision } = tokenFields(token.value);
      const updated = await this.#database.query(
        `UPDATE direct_reference_operations
         SET operation_id=COALESCE(operation_id,?),token_json=?,token_sha256=?,token_revision=?
         WHERE run_key=? AND slot=? AND (operation_id IS NULL OR operation_id=?)
           AND (token_revision IS NULL OR token_revision<?) RETURNING slot`,
        [
          operationId,
          token.text,
          boundHash(['token', this.#runKey, slot], token.text),
          revision,
          this.#runKey,
          slot,
          operationId,
          revision,
        ],
      );
      if (updated.length > 1) stateError();
      const after = await this.#operation(slot);
      if (!after) stateError();
      if (after.operationId !== operationId)
        throw new DirectReferenceJournalError('operation-mismatch');
      if (after.tokenRevision === null || after.tokenRevision < revision)
        stateError();
      if (
        updated.length === 1 &&
        after.tokenRevision === revision &&
        after.tokenJson !== token.text
      )
        stateError();
    });
  }

  recordInterruption(witnessJson: string): Promise<boolean> {
    return this.#withState(async () => {
      const witness = jsonObject(witnessJson);
      const rows = await this.#database.query(
        'UPDATE direct_reference_run SET interruption_json=?,interruption_sha256=? WHERE run_key=? AND interruption_json IS NULL RETURNING run_key',
        [
          witness.text,
          boundHash(['interruption', this.#runKey], witness.text),
          this.#runKey,
        ],
      );
      if (rows.length > 1) stateError();
      const recorded = await this.#interruption();
      if (recorded === null || (rows.length === 1 && recorded !== witness.text))
        stateError();
      return rows.length === 1;
    });
  }

  async #interruption(): Promise<string | null> {
    const rows = await this.#database.query(
      'SELECT interruption_json,interruption_sha256 FROM direct_reference_run WHERE run_key=?',
      [this.#runKey],
    );
    const row = rows[0];
    if (rows.length !== 1 || !row) stateError();
    if (row.interruption_json === null) {
      if (row.interruption_sha256 !== null) stateError();
      return null;
    }
    return storedJson(row.interruption_json, row.interruption_sha256, [
      'interruption',
      this.#runKey,
    ]).text;
  }

  readInterruption(): Promise<string | null> {
    return this.#withState(() => this.#interruption());
  }

  async #observation(
    kind: ObservationKind,
    key: string,
  ): Promise<DirectStoredObservation | undefined> {
    const rows = await this.#database.query(
      'SELECT * FROM direct_reference_observations WHERE run_key=? AND observation_kind=? AND observation_key=?',
      [this.#runKey, kind, key],
    );
    if (rows.length > 1) stateError();
    const row = rows[0];
    if (!row) return undefined;
    const context = ['observation', this.#runKey, kind, key];
    return Object.freeze({
      identityJson: storedJson(row.identity_json, row.identity_sha256, [
        ...context,
        'identity',
      ]).text,
      provenanceJson: storedJson(row.provenance_json, row.provenance_sha256, [
        ...context,
        'provenance',
      ]).text,
    });
  }

  async #recordObservation(
    kind: ObservationKind,
    key: string,
    identityJson: string,
    provenanceJson: string,
  ): Promise<DirectStoredObservation> {
    const identity = jsonObject(identityJson);
    const provenance = jsonObject(provenanceJson);
    const context = ['observation', this.#runKey, kind, key];
    await this.#database.execute(
      'INSERT INTO direct_reference_observations (run_key,observation_kind,observation_key,identity_json,identity_sha256,provenance_json,provenance_sha256) VALUES (?,?,?,?,?,?,?) ON CONFLICT DO NOTHING',
      [
        this.#runKey,
        kind,
        key,
        identity.text,
        boundHash([...context, 'identity'], identity.text),
        provenance.text,
        boundHash([...context, 'provenance'], provenance.text),
      ],
    );
    const stored = await this.#observation(kind, key);
    if (!stored || stored.identityJson !== identity.text) stateError();
    return stored;
  }

  recordResource(
    fixtureRole: DirectFixtureRole,
    identityJson: string,
    provenanceJson: string,
  ): Promise<DirectStoredResource> {
    return this.#withState(async () => {
      const identity = jsonObject(identityJson);
      const identitySha256 = boundHash(
        ['resource', this.#runKey, role(fixtureRole)],
        identity.text,
      );
      const stored = await this.#recordObservation(
        'resource',
        `${fixtureRole}:${identitySha256}`,
        identity.text,
        provenanceJson,
      );
      return Object.freeze({ ...stored, identitySha256 });
    });
  }

  readResource(
    fixtureRole: DirectFixtureRole,
    identitySha256: string,
  ): Promise<DirectStoredResource | undefined> {
    return this.#withState(async () => {
      const stored = await this.#observation(
        'resource',
        `${role(fixtureRole)}:${sha256(identitySha256)}`,
      );
      if (!stored) return undefined;
      if (
        boundHash(
          ['resource', this.#runKey, fixtureRole],
          stored.identityJson,
        ) !== identitySha256
      )
        stateError();
      return Object.freeze({ ...stored, identitySha256 });
    });
  }

  recordSettlement(
    settlementKey: string,
    identityJson: string,
    provenanceJson: string,
  ): Promise<DirectStoredObservation> {
    return this.#withState(() =>
      this.#recordObservation(
        'settlement',
        sha256(settlementKey),
        identityJson,
        provenanceJson,
      ),
    );
  }

  readSettlement(
    settlementKey: string,
  ): Promise<DirectStoredObservation | undefined> {
    return this.#withState(() =>
      this.#observation('settlement', sha256(settlementKey)),
    );
  }

  recordForceBefore(
    identityJson: string,
    provenanceJson: string,
  ): Promise<DirectStoredObservation> {
    return this.#withState(() =>
      this.#recordObservation(
        'force-before',
        'recovery',
        identityJson,
        provenanceJson,
      ),
    );
  }

  readForceBefore(): Promise<DirectStoredObservation | undefined> {
    return this.#withState(() => this.#observation('force-before', 'recovery'));
  }

  recordForceAfter(
    identityJson: string,
    provenanceJson: string,
  ): Promise<DirectStoredObservation> {
    return this.#withState(() =>
      this.#recordObservation(
        'force-after',
        'recovery',
        identityJson,
        provenanceJson,
      ),
    );
  }

  readForceAfter(): Promise<DirectStoredObservation | undefined> {
    return this.#withState(() => this.#observation('force-after', 'recovery'));
  }
}
