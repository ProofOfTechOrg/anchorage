// SPDX-License-Identifier: Apache-2.0

import { EXECUTION_FENCE_TABLE } from '#deployment-identity-protocol';
import {
  assertExecutionPrincipal,
  type ExecutionPrincipal,
} from '../approval-api/principal.js';
import { credentialsMatch } from './deployment-identity.js';
import { DoStatusError } from './do-status-error.js';
import {
  admitsExistingRun,
  captureExecutionFenceAdmissionSchema,
  type ExecutionFenceDatabase,
  type ExecutionFenceStore,
  executionFenceAdmissionSql,
  executionFenceAdmissionValues,
} from './execution-fence.js';
import { isPathSafeId } from './path-safe-id.js';
import {
  lifecycleFromRequestContext,
  RUN_LIFECYCLE_CONTEXT_KEY,
} from './run-lifecycle.js';
import type { RunnerRuntime, RunSummary } from './runtime.js';
import { validateTablePrefix } from './table-prefix.js';

export const REMOTE_STEP_TABLE = 'flowsafe_remote_step_invocations';
export const REMOTE_STEP_SCHEMA_TABLE = 'flowsafe_remote_step_schema';
const REMOTE_STEP_RESULT_COLUMNS = [
  'result_json',
  'result_digest',
  'quiescence_digest',
  'executor_quiescent_at',
] as const;
const REMOTE_STEP_COLUMNS = [
  'invocation_id',
  'principal_json',
  'start_token',
  'deployment_id',
  'table_prefix',
  'workflow_id',
  'agent_id',
  'thread_id',
  'resource_id',
  'run_id',
  'step_path',
  'suspended_at',
  'resume_count',
  'request_json',
  'deadline_at',
  'status',
  'permit_generation',
  'permit_epoch',
  'permit_revision',
  'permit_expires_at',
  'permit_consumed_at',
  'permit_digest',
  ...REMOTE_STEP_RESULT_COLUMNS,
] as const;
const REMOTE_STEP_INTEGER_COLUMNS = new Set<string>([
  'suspended_at',
  'resume_count',
  'deadline_at',
  'permit_generation',
  'permit_epoch',
  'permit_revision',
  'permit_expires_at',
  'permit_consumed_at',
  'executor_quiescent_at',
]);
export const REMOTE_STEP_MARKER = 'flowsafe.remote-step/v1';
export const MAX_REMOTE_STEP_REQUEST_BYTES = 16_384;
export const MAX_REMOTE_STEP_RESULT_BYTES = 16_384;

const SHA256_HEX = /^[a-f0-9]{64}$/;
const RAW_PERMIT = /^[A-Za-z0-9_-]{43}$/;

function boundedJsonBytes(value: string): void {
  if (
    typeof value !== 'string' ||
    new TextEncoder().encode(value).length > MAX_REMOTE_STEP_RESULT_BYTES
  )
    throw new RemoteStepRefusal(400, 'remote step JSON exceeds its limit');
  try {
    JSON.parse(value);
  } catch {
    throw new RemoteStepRefusal(400, 'remote step JSON is malformed');
  }
}

function rawDispatchPermit(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(32));
  let binary = '';
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary)
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/, '');
}

async function sha256Hex(value: string): Promise<string> {
  const digest = await crypto.subtle.digest(
    'SHA-256',
    new TextEncoder().encode(value),
  );
  return Array.from(new Uint8Array(digest), (byte) =>
    byte.toString(16).padStart(2, '0'),
  ).join('');
}

const SNAPSHOT_EXECUTION_OPEN_SQL = `json_valid(snapshot)
  AND json_type(snapshot, '$.requestContext."${RUN_LIFECYCLE_CONTEXT_KEY}".transitionIntent') IS NULL
  AND json_type(snapshot, '$.requestContext."${RUN_LIFECYCLE_CONTEXT_KEY}".terminal') IS NULL`;

export interface RemoteStepAddress {
  deploymentId: string;
  tablePrefix: string;
  workflowId: string;
  agentId: string;
  threadId: string;
  resourceId: string;
  runId: string;
}

export interface RemoteStepInvocation extends RemoteStepAddress {
  invocationId: string;
  principal: ExecutionPrincipal;
  startToken: string;
  stepPath: readonly string[];
  suspendedAt: number;
  resumeCount?: number;
  request: unknown;
  deadlineAt: number;
  status: 'pending' | 'dispatched' | 'result-retained' | 'failed';
  permitGeneration: number;
  permitEpoch?: number;
  permitRevision?: number;
  permitExpiresAt?: number;
  permitConsumedAt?: number;
}

export interface RemoteStepPermit {
  invocationId: string;
  generation: number;
  mutationEpoch: number;
  transitionRevision: number;
  expiresAt: number;
}

export interface RecordedRemoteStepResult {
  invocationId: string;
  resultJson: string;
  resultDigest: string;
  quiescenceDigest: string;
  executorQuiescentAt: number;
}

class RemoteStepRefusal extends DoStatusError {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = 'RemoteStepRefusal';
    this.status = status;
  }
}

type JournalRow = {
  invocation_id: string;
  principal_json: string;
  start_token: string;
  deployment_id: string;
  table_prefix: string;
  workflow_id: string;
  agent_id: string;
  thread_id: string;
  resource_id: string;
  run_id: string;
  step_path: string;
  suspended_at: number;
  resume_count: number;
  request_json: string;
  deadline_at: number;
  status: string;
  permit_generation: number;
  permit_epoch: number | null;
  permit_revision: number | null;
  permit_expires_at: number | null;
  permit_consumed_at: number | null;
  permit_digest: string | null;
  result_json: string | null;
  result_digest: string | null;
  quiescence_digest: string | null;
  executor_quiescent_at: number | null;
};

function record(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function positiveTime(value: unknown): value is number {
  return Number.isSafeInteger(value) && (value as number) > 0;
}

function rowResult(value: unknown): JournalRow | undefined {
  const rows = record(value)?.results;
  return Array.isArray(rows) && rows.length === 1
    ? (rows[0] as JournalRow)
    : undefined;
}

function decode(row: JournalRow): RemoteStepInvocation {
  if (
    typeof row.step_path !== 'string' ||
    row.step_path.length > 1_024 ||
    typeof row.request_json !== 'string' ||
    typeof row.principal_json !== 'string' ||
    new TextEncoder().encode(row.request_json).length >
      MAX_REMOTE_STEP_REQUEST_BYTES ||
    !isPathSafeId(row.deployment_id) ||
    !isPathSafeId(row.workflow_id) ||
    !isPathSafeId(row.agent_id) ||
    !isPathSafeId(row.thread_id) ||
    !isPathSafeId(row.resource_id) ||
    !isPathSafeId(row.run_id) ||
    !isPathSafeId(row.start_token)
  )
    throw new RemoteStepRefusal(503, 'remote step journal is unreadable');
  try {
    validateTablePrefix(row.table_prefix);
  } catch {
    throw new RemoteStepRefusal(503, 'remote step journal is unreadable');
  }
  let stepPath: unknown;
  let request: unknown;
  let principal: ExecutionPrincipal;
  try {
    stepPath = JSON.parse(row.step_path);
    request = JSON.parse(row.request_json);
    principal = assertExecutionPrincipal(
      JSON.parse(row.principal_json),
      'stored remote step principal',
    );
  } catch {
    throw new RemoteStepRefusal(503, 'remote step journal is unreadable');
  }
  if (
    !Array.isArray(stepPath) ||
    stepPath.length === 0 ||
    stepPath.some((part) => typeof part !== 'string' || part === '') ||
    !isPathSafeId(row.invocation_id) ||
    !positiveTime(row.suspended_at) ||
    !positiveTime(row.deadline_at) ||
    !Number.isSafeInteger(row.resume_count) ||
    (row.resume_count !== -1 && row.resume_count < 1) ||
    !Number.isSafeInteger(row.permit_generation) ||
    row.permit_generation < 0 ||
    !['pending', 'dispatched', 'result-retained', 'failed'].includes(row.status)
  )
    throw new RemoteStepRefusal(503, 'remote step journal is unreadable');
  return {
    invocationId: row.invocation_id,
    principal,
    startToken: row.start_token,
    deploymentId: row.deployment_id,
    tablePrefix: row.table_prefix,
    workflowId: row.workflow_id,
    agentId: row.agent_id,
    threadId: row.thread_id,
    resourceId: row.resource_id,
    runId: row.run_id,
    stepPath,
    suspendedAt: row.suspended_at,
    ...(row.resume_count < 0 ? {} : { resumeCount: row.resume_count }),
    request,
    deadlineAt: row.deadline_at,
    status: row.status as RemoteStepInvocation['status'],
    permitGeneration: row.permit_generation,
    ...(row.permit_epoch === null ? {} : { permitEpoch: row.permit_epoch }),
    ...(row.permit_revision === null
      ? {}
      : { permitRevision: row.permit_revision }),
    ...(row.permit_expires_at === null
      ? {}
      : { permitExpiresAt: row.permit_expires_at }),
    ...(row.permit_consumed_at === null
      ? {}
      : { permitConsumedAt: row.permit_consumed_at }),
  };
}

function selectedSuspension(
  summary: RunSummary,
  allowedToolIds: ReadonlySet<string>,
): {
  stepPath: string[];
  suspendedAt: number;
  resumeCount?: number;
  requestJson: string;
} {
  if (summary.status !== 'suspended')
    throw new RemoteStepRefusal(409, 'run is not suspended');
  const payloads = record(summary.suspendPayload);
  const matches = (summary.suspended ?? []).flatMap((path) => {
    const key = path.join('.');
    const suspended = record(payloads?.[key]);
    const tool = record(suspended?.toolCallSuspended);
    return suspended?.type === 'suspension' &&
      tool?.remoteStep === REMOTE_STEP_MARKER
      ? [{ stepPath: path, key, tool, suspended }]
      : [];
  });
  if (matches.length !== 1)
    throw new RemoteStepRefusal(409, 'remote step suspension is ambiguous');
  const selected = matches[0];
  if (!selected)
    throw new RemoteStepRefusal(409, 'remote step suspension is ambiguous');
  const { stepPath, key, tool, suspended } = selected;
  const suspendedAt = summary.suspendedAt?.[key];
  const resumeCount = summary.resumeCount?.[key];
  if (
    !positiveTime(suspendedAt) ||
    typeof suspended.toolName !== 'string' ||
    !allowedToolIds.has(suspended.toolName) ||
    typeof suspended.toolCallId !== 'string' ||
    suspended.toolCallId === '' ||
    (resumeCount !== undefined &&
      (!Number.isSafeInteger(resumeCount) || resumeCount < 1)) ||
    !Object.hasOwn(tool, 'request')
  )
    throw new RemoteStepRefusal(503, 'remote step suspension is unreadable');
  const requestJson = JSON.stringify(tool.request);
  if (
    typeof requestJson !== 'string' ||
    new TextEncoder().encode(requestJson).length > MAX_REMOTE_STEP_REQUEST_BYTES
  )
    throw new RemoteStepRefusal(400, 'remote step request exceeds its limit');
  return {
    stepPath,
    suspendedAt,
    ...(resumeCount === undefined ? {} : { resumeCount }),
    requestJson,
  };
}

export class RemoteStepJournal {
  readonly #db: ExecutionFenceDatabase;
  readonly #fence: ExecutionFenceStore;
  readonly #runtime: Pick<
    RunnerRuntime,
    'authoritativeStatus' | 'authoritativeStartState'
  >;
  readonly #now: () => number;
  readonly #deploymentId: string;
  readonly #tablePrefix: string;
  readonly #allowedToolIds: ReadonlySet<string>;
  readonly #verifyCompletion: (input: {
    invocation: RemoteStepInvocation;
    rawPermit: string;
    resultJson: string;
    evidenceJson: string;
  }) => Promise<void>;

  constructor(input: {
    db: ExecutionFenceDatabase;
    fence: ExecutionFenceStore;
    runtime: Pick<
      RunnerRuntime,
      'authoritativeStatus' | 'authoritativeStartState'
    >;
    deploymentId: string;
    tablePrefix: string;
    allowedToolIds: readonly string[];
    verifyCompletion: (input: {
      invocation: RemoteStepInvocation;
      rawPermit: string;
      resultJson: string;
      evidenceJson: string;
    }) => Promise<void>;
    now?: () => number;
  }) {
    if (!input.fence.usesDatabase(input.db))
      throw new Error('remote step fence uses another database');
    if (!isPathSafeId(input.deploymentId))
      throw new Error('remote step deployment id is malformed');
    validateTablePrefix(input.tablePrefix);
    if (
      !Array.isArray(input.allowedToolIds) ||
      input.allowedToolIds.length === 0 ||
      input.allowedToolIds.some((id) => !isPathSafeId(id)) ||
      new Set(input.allowedToolIds).size !== input.allowedToolIds.length
    )
      throw new Error('remote step tool ids are malformed');
    if (typeof input.verifyCompletion !== 'function')
      throw new Error('remote step completion verifier is required');
    this.#db = input.db;
    this.#fence = input.fence;
    this.#runtime = input.runtime;
    this.#deploymentId = input.deploymentId;
    this.#tablePrefix = input.tablePrefix;
    this.#allowedToolIds = new Set(input.allowedToolIds);
    this.#verifyCompletion = input.verifyCompletion;
    this.#now = input.now ?? Date.now;
  }

  async initialize(): Promise<void> {
    await this.#db
      .prepare(`CREATE TABLE IF NOT EXISTS ${REMOTE_STEP_TABLE} (
        invocation_id TEXT PRIMARY KEY,
        principal_json TEXT NOT NULL,
        start_token TEXT NOT NULL,
        deployment_id TEXT NOT NULL,
        table_prefix TEXT NOT NULL,
        workflow_id TEXT NOT NULL,
        agent_id TEXT NOT NULL,
        thread_id TEXT NOT NULL,
        resource_id TEXT NOT NULL,
        run_id TEXT NOT NULL,
        step_path TEXT NOT NULL,
        suspended_at INTEGER NOT NULL,
        resume_count INTEGER NOT NULL,
        request_json TEXT NOT NULL,
        deadline_at INTEGER NOT NULL,
        status TEXT NOT NULL,
        permit_generation INTEGER NOT NULL DEFAULT 0,
        permit_epoch INTEGER,
        permit_revision INTEGER,
        permit_expires_at INTEGER,
        permit_consumed_at INTEGER,
        permit_digest TEXT,
        result_json TEXT,
        result_digest TEXT,
        quiescence_digest TEXT,
        executor_quiescent_at INTEGER,
        UNIQUE (deployment_id, table_prefix, workflow_id, agent_id,
          thread_id, resource_id, run_id, step_path, suspended_at, resume_count)
      )`)
      .run();
    await this.#db
      .prepare(`CREATE TABLE IF NOT EXISTS ${REMOTE_STEP_SCHEMA_TABLE} (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        version INTEGER NOT NULL
      )`)
      .run();
    const readSchema = async () => {
      const schema = await this.#db
        .prepare(`PRAGMA table_xinfo(${REMOTE_STEP_TABLE})`)
        .all();
      const columns = record(schema)?.results;
      if (!Array.isArray(columns))
        throw new RemoteStepRefusal(503, 'remote step schema is unreadable');
      const samePrefix = (expected: readonly string[]) =>
        columns.length === expected.length &&
        columns.every((value, index) => {
          const column = record(value);
          const name = column?.name;
          return (
            name === expected[index] &&
            column?.type ===
              (REMOTE_STEP_INTEGER_COLUMNS.has(name as string)
                ? 'INTEGER'
                : 'TEXT') &&
            column?.notnull === (index > 0 && index <= 16 ? 1 : 0) &&
            column?.pk === (index === 0 ? 1 : 0) &&
            column?.hidden === 0 &&
            column?.dflt_value === (name === 'permit_generation' ? '0' : null)
          );
        });
      const indexes = record(
        await this.#db.prepare(`PRAGMA index_list(${REMOTE_STEP_TABLE})`).all(),
      )?.results;
      if (!Array.isArray(indexes))
        throw new RemoteStepRefusal(503, 'remote step schema is unreadable');
      const suspensionKey = [
        'deployment_id',
        'table_prefix',
        'workflow_id',
        'agent_id',
        'thread_id',
        'resource_id',
        'run_id',
        'step_path',
        'suspended_at',
        'resume_count',
      ];
      let uniqueSuspension = false;
      for (const value of indexes) {
        const indexRow = record(value);
        const name = indexRow?.name;
        if (
          indexRow?.unique !== 1 ||
          indexRow?.origin !== 'u' ||
          indexRow?.partial !== 0 ||
          typeof name !== 'string' ||
          !/^sqlite_autoindex_flowsafe_remote_step_invocations_[1-9][0-9]*$/.test(
            name,
          )
        )
          continue;
        const details = record(
          await this.#db.prepare(`PRAGMA index_info('${name}')`).all(),
        )?.results;
        if (
          Array.isArray(details) &&
          details.length === suspensionKey.length &&
          details.every(
            (part, index) =>
              record(part)?.seqno === index &&
              record(part)?.name === suspensionKey[index],
          )
        )
          uniqueSuspension = true;
      }
      if (!uniqueSuspension)
        throw new RemoteStepRefusal(503, 'remote step schema is unreadable');
      const versions = await this.#db
        .prepare(`SELECT id, version FROM ${REMOTE_STEP_SCHEMA_TABLE} LIMIT 2`)
        .all();
      const rows = record(versions)?.results;
      if (!Array.isArray(rows) || rows.length > 1)
        throw new RemoteStepRefusal(503, 'remote step schema is unreadable');
      if (rows.length === 1) {
        if (
          record(rows[0])?.id !== 1 ||
          record(rows[0])?.version !== 2 ||
          !samePrefix(REMOTE_STEP_COLUMNS)
        )
          throw new RemoteStepRefusal(503, 'remote step schema is unreadable');
        return 'current';
      }
      if (samePrefix(REMOTE_STEP_COLUMNS.slice(0, -4))) return 'prior';
      if (samePrefix(REMOTE_STEP_COLUMNS)) return 'unversioned-current';
      throw new RemoteStepRefusal(503, 'remote step schema is unreadable');
    };
    const stage = await readSchema();
    if (stage === 'current') return;
    if (stage === 'prior') {
      const transactional = this.#db as ExecutionFenceDatabase & {
        batch?: (
          statements: ReturnType<ExecutionFenceDatabase['prepare']>[],
        ) => Promise<unknown>;
      };
      if (typeof transactional.batch !== 'function')
        throw new RemoteStepRefusal(
          503,
          'remote step schema upgrade is unavailable',
        );
      try {
        await transactional.batch([
          ...REMOTE_STEP_RESULT_COLUMNS.map((column) =>
            this.#db.prepare(
              `ALTER TABLE ${REMOTE_STEP_TABLE} ADD COLUMN ${column} ${column === 'executor_quiescent_at' ? 'INTEGER' : 'TEXT'}`,
            ),
          ),
          this.#db.prepare(
            `INSERT INTO ${REMOTE_STEP_SCHEMA_TABLE} (id, version) VALUES (1, 2)`,
          ),
        ]);
      } catch (error) {
        if ((await readSchema()) !== 'current') throw error;
      }
      return;
    }
    try {
      await this.#db
        .prepare(
          `INSERT INTO ${REMOTE_STEP_SCHEMA_TABLE} (id, version) VALUES (1, 2)`,
        )
        .run();
    } catch (error) {
      if ((await readSchema()) !== 'current') throw error;
    }
  }

  async #snapshot(address: RemoteStepAddress): Promise<string> {
    const result = await this.#db
      .prepare(
        `SELECT snapshot FROM "${address.tablePrefix}mastra_workflow_snapshot"
         WHERE workflow_name = ? AND run_id = ? LIMIT 2`,
      )
      .bind(address.workflowId, address.runId)
      .all();
    const rows = record(result)?.results;
    if (
      !Array.isArray(rows) ||
      rows.length !== 1 ||
      typeof record(rows[0])?.snapshot !== 'string'
    )
      throw new RemoteStepRefusal(503, 'remote step snapshot is unreadable');
    return record(rows[0])?.snapshot as string;
  }

  async #current(address: RemoteStepAddress) {
    const summary = await this.#runtime.authoritativeStatus(
      address.workflowId,
      address.runId,
    );
    if (!summary) throw new RemoteStepRefusal(404, 'run not found');
    return selectedSuspension(summary, this.#allowedToolIds);
  }

  async #stableSnapshot(address: RemoteStepAddress) {
    const before = await this.#current(address);
    const snapshot = await this.#snapshot(address);
    const state = await this.#execution(address);
    const lifecycle = lifecycleFromRequestContext(
      state.snapshot.requestContext,
    );
    if (lifecycle?.transitionIntent || lifecycle?.terminal)
      throw new RemoteStepRefusal(409, 'remote step run is ending');
    const after = await this.#current(address);
    if (
      before.suspendedAt !== after.suspendedAt ||
      before.resumeCount !== after.resumeCount ||
      JSON.stringify(before.stepPath) !== JSON.stringify(after.stepPath) ||
      before.requestJson !== after.requestJson
    )
      throw new RemoteStepRefusal(409, 'remote step suspension changed');
    return { suspension: after, snapshot };
  }

  async register(
    address: RemoteStepAddress,
    hostDeadlineAt: number,
    originalPrincipal: ExecutionPrincipal,
  ): Promise<RemoteStepInvocation> {
    if (
      address.deploymentId !== this.#deploymentId ||
      address.tablePrefix !== this.#tablePrefix ||
      !isPathSafeId(address.workflowId) ||
      !isPathSafeId(address.agentId) ||
      !isPathSafeId(address.threadId) ||
      !isPathSafeId(address.resourceId) ||
      !isPathSafeId(address.runId) ||
      !positiveTime(hostDeadlineAt)
    )
      throw new RemoteStepRefusal(400, 'remote step address is malformed');
    validateTablePrefix(address.tablePrefix);
    let principal: ExecutionPrincipal;
    try {
      principal = assertExecutionPrincipal(
        originalPrincipal,
        'remote step principal',
      );
    } catch {
      throw new RemoteStepRefusal(400, 'remote step principal is malformed');
    }
    const execution = await this.#execution(address, principal);
    const { suspension, snapshot } = await this.#stableSnapshot(address);
    const summary = await this.#runtime.authoritativeStatus(
      address.workflowId,
      address.runId,
    );
    const deadlineAt = Math.min(
      hostDeadlineAt,
      summary?.deadlineAt ?? hostDeadlineAt,
    );
    if (deadlineAt <= this.#now())
      throw new RemoteStepRefusal(409, 'remote step deadline elapsed');
    const invocationId = crypto.randomUUID();
    const stepPath = JSON.stringify(suspension.stepPath);
    await this.#db
      .prepare(`INSERT INTO ${REMOTE_STEP_TABLE} (
        invocation_id, principal_json, start_token, deployment_id, table_prefix, workflow_id, agent_id,
        thread_id, resource_id, run_id, step_path, suspended_at, resume_count,
        request_json, deadline_at, status)
        SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending'
        WHERE EXISTS (SELECT 1 FROM "${address.tablePrefix}mastra_workflow_snapshot"
          WHERE workflow_name = ? AND run_id = ? AND snapshot = ?
            AND ${SNAPSHOT_EXECUTION_OPEN_SQL})
        ON CONFLICT DO NOTHING`)
      .bind(
        invocationId,
        JSON.stringify(principal),
        execution.execution.startToken,
        address.deploymentId,
        address.tablePrefix,
        address.workflowId,
        address.agentId,
        address.threadId,
        address.resourceId,
        address.runId,
        stepPath,
        suspension.suspendedAt,
        suspension.resumeCount ?? -1,
        suspension.requestJson,
        deadlineAt,
        address.workflowId,
        address.runId,
        snapshot,
      )
      .run();
    const result = await this.#db
      .prepare(`UPDATE ${REMOTE_STEP_TABLE}
        SET deadline_at = MIN(deadline_at, ?)
        WHERE deployment_id = ? AND table_prefix = ? AND workflow_id = ?
          AND agent_id = ? AND thread_id = ? AND resource_id = ? AND run_id = ?
          AND step_path = ? AND suspended_at = ? AND resume_count = ?
          AND request_json = ? AND principal_json = ? AND start_token = ?
          AND EXISTS (SELECT 1 FROM "${address.tablePrefix}mastra_workflow_snapshot"
            WHERE workflow_name = ? AND run_id = ? AND snapshot = ?
              AND ${SNAPSHOT_EXECUTION_OPEN_SQL})
        RETURNING *`)
      .bind(
        deadlineAt,
        address.deploymentId,
        address.tablePrefix,
        address.workflowId,
        address.agentId,
        address.threadId,
        address.resourceId,
        address.runId,
        stepPath,
        suspension.suspendedAt,
        suspension.resumeCount ?? -1,
        suspension.requestJson,
        JSON.stringify(principal),
        execution.execution.startToken,
        address.workflowId,
        address.runId,
        snapshot,
      )
      .all();
    const row = rowResult(result);
    if (!row || row.request_json !== suspension.requestJson)
      throw new RemoteStepRefusal(
        409,
        'remote step registration did not settle',
      );
    return decode(row);
  }

  async get(invocationId: string): Promise<RemoteStepInvocation | undefined> {
    if (!isPathSafeId(invocationId))
      throw new RemoteStepRefusal(400, 'invocation id is malformed');
    const row = rowResult(
      await this.#db
        .prepare(`SELECT * FROM ${REMOTE_STEP_TABLE} WHERE invocation_id = ?`)
        .bind(invocationId)
        .all(),
    );
    return row ? decode(row) : undefined;
  }

  async #execution(address: RemoteStepAddress, principal?: ExecutionPrincipal) {
    const state = await this.#runtime.authoritativeStartState(
      address.workflowId,
      address.runId,
    );
    const target = state?.provenance.startIdentity?.target;
    if (
      state?.storage !== 'd1' ||
      target?.kind !== 'agent' ||
      !isPathSafeId(target.resourceId)
    )
      throw new RemoteStepRefusal(
        503,
        'remote step run identity is unreadable',
      );
    if (
      target.id !== address.agentId ||
      target.threadId !== address.threadId ||
      (principal !== undefined &&
        (state.provenance.startIdentity?.owner.kind !== principal.kind ||
          state.provenance.startIdentity.owner.id !== principal.id)) ||
      state.execution.tablePrefix !== address.tablePrefix ||
      address.resourceId !== target.resourceId
    )
      throw new RemoteStepRefusal(409, 'remote step run identity changed');
    return state;
  }

  async #admission(invocation: RemoteStepInvocation, priorBindings: number) {
    const observed = await this.#fence.readForAdmission();
    const proof = (await this.#execution(invocation, invocation.principal))
      .execution;
    if (proof.startToken !== invocation.startToken)
      throw new RemoteStepRefusal(409, 'remote step run generation changed');
    if (!admitsExistingRun(observed.reading, proof))
      throw new RemoteStepRefusal(503, 'remote step execution is fenced');
    const schema = await captureExecutionFenceAdmissionSchema(
      await this.#db
        .prepare(`PRAGMA table_xinfo(${EXECUTION_FENCE_TABLE})`)
        .all(),
    );
    const values: unknown[] = [];
    const bind = (value: unknown) => {
      values.push(value);
      return `?${priorBindings + values.length}`;
    };
    const predicate = executionFenceAdmissionSql({
      callerEpoch: bind(observed.reading.mutationEpoch ?? null),
      semantic: executionFenceAdmissionValues(observed).map(bind),
      schema: bind(schema),
      statePredicate: `f.state IN ('open', 'draining') OR
        (f.state = 'proof-only' AND f.proof_table_prefix IS ${bind(proof.tablePrefix)}
          AND f.proof_workflow_id IS ${bind(proof.workflowId)}
          AND f.proof_run_id IS ${bind(proof.runId)}
          AND f.proof_start_token IS ${bind(proof.startToken)})`,
    });
    return { predicate, values, bind, observed: observed.reading };
  }

  async issuePermit(
    invocationId: string,
    lifetimeMs: number,
  ): Promise<RemoteStepPermit> {
    const invocation = await this.get(invocationId);
    if (!invocation) throw new RemoteStepRefusal(404, 'invocation not found');
    if (!positiveTime(lifetimeMs))
      throw new RemoteStepRefusal(400, 'permit lifetime is malformed');
    const now = this.#now();
    const expiresAt = Math.min(invocation.deadlineAt, now + lifetimeMs);
    if (!positiveTime(expiresAt) || expiresAt <= now)
      throw new RemoteStepRefusal(409, 'remote step deadline elapsed');
    const address = invocation;
    const { suspension, snapshot } = await this.#stableSnapshot(address);
    if (
      suspension.suspendedAt !== invocation.suspendedAt ||
      suspension.resumeCount !== invocation.resumeCount ||
      suspension.requestJson !== JSON.stringify(invocation.request) ||
      JSON.stringify(suspension.stepPath) !==
        JSON.stringify(invocation.stepPath)
    )
      throw new RemoteStepRefusal(409, 'remote step suspension changed');
    const gate = await this.#admission(invocation, 10);
    if (
      invocation.status === 'dispatched' &&
      invocation.permitConsumedAt === undefined &&
      invocation.permitExpiresAt !== undefined &&
      invocation.permitExpiresAt > now &&
      invocation.permitEpoch === gate.observed.mutationEpoch &&
      invocation.permitRevision === gate.observed.transitionRevision
    )
      return {
        invocationId,
        generation: invocation.permitGeneration,
        mutationEpoch: invocation.permitEpoch,
        transitionRevision: invocation.permitRevision,
        expiresAt: invocation.permitExpiresAt,
      };
    if (
      invocation.status === 'failed' ||
      invocation.permitConsumedAt !== undefined
    )
      throw new RemoteStepRefusal(409, 'remote step dispatch is active');
    const epoch = gate.observed.mutationEpoch;
    const revision = gate.observed.transitionRevision;
    if (epoch === undefined || revision === undefined)
      throw new RemoteStepRefusal(503, 'remote step fence is unversioned');
    const result = await this.#db
      .prepare(`UPDATE ${REMOTE_STEP_TABLE} SET
        status = 'dispatched', permit_generation = permit_generation + 1,
        permit_epoch = ?, permit_revision = ?,
        permit_expires_at = MIN(?, deadline_at), permit_consumed_at = NULL,
        permit_digest = NULL
        WHERE invocation_id = ? AND permit_generation = ? AND deadline_at > ?
          AND status IN ('pending', 'dispatched')
          AND permit_consumed_at IS NULL AND request_json = ?
          AND EXISTS (SELECT 1 FROM "${address.tablePrefix}mastra_workflow_snapshot"
            WHERE workflow_name = ? AND run_id = ? AND snapshot = ?
              AND ${SNAPSHOT_EXECUTION_OPEN_SQL})
          AND ${gate.predicate} RETURNING *`)
      .bind(
        epoch,
        revision,
        expiresAt,
        invocationId,
        invocation.permitGeneration,
        now,
        suspension.requestJson,
        invocation.workflowId,
        invocation.runId,
        snapshot,
        ...gate.values,
      )
      .all();
    const row = rowResult(result);
    if (!row) throw new RemoteStepRefusal(409, 'remote step dispatch changed');
    return {
      invocationId,
      generation: row.permit_generation,
      mutationEpoch: epoch,
      transitionRevision: revision,
      expiresAt: row.permit_expires_at as number,
    };
  }

  async consumePermit(
    permit: RemoteStepPermit,
  ): Promise<
    { kind: 'accepted'; rawPermit: string } | { kind: 'already-consumed' }
  > {
    const invocation = await this.get(permit.invocationId);
    if (!invocation) throw new RemoteStepRefusal(404, 'invocation not found');
    if (
      invocation.permitGeneration !== permit.generation ||
      invocation.permitEpoch !== permit.mutationEpoch ||
      invocation.permitRevision !== permit.transitionRevision ||
      invocation.permitExpiresAt !== permit.expiresAt
    )
      throw new RemoteStepRefusal(409, 'remote step permit changed');
    if (invocation.permitConsumedAt !== undefined)
      return { kind: 'already-consumed' };
    const now = this.#now();
    const { suspension, snapshot } = await this.#stableSnapshot(invocation);
    if (
      suspension.suspendedAt !== invocation.suspendedAt ||
      suspension.resumeCount !== invocation.resumeCount ||
      suspension.requestJson !== JSON.stringify(invocation.request) ||
      JSON.stringify(suspension.stepPath) !==
        JSON.stringify(invocation.stepPath)
    )
      throw new RemoteStepRefusal(409, 'remote step suspension changed');
    const rawPermit = rawDispatchPermit();
    const permitDigest = await sha256Hex(rawPermit);
    const gate = await this.#admission(invocation, 13);
    const result = await this.#db
      .prepare(`UPDATE ${REMOTE_STEP_TABLE}
        SET permit_consumed_at = ?, permit_digest = ?
        WHERE invocation_id = ? AND permit_generation = ?
          AND permit_epoch = ? AND permit_revision = ?
          AND permit_expires_at = ? AND permit_expires_at > ?
          AND deadline_at > ? AND permit_consumed_at IS NULL
          AND status = 'dispatched' AND request_json = ?
          AND EXISTS (SELECT 1 FROM "${invocation.tablePrefix}mastra_workflow_snapshot"
            WHERE workflow_name = ? AND run_id = ? AND snapshot = ?
              AND ${SNAPSHOT_EXECUTION_OPEN_SQL})
          AND ${gate.predicate} RETURNING invocation_id`)
      .bind(
        now,
        permitDigest,
        permit.invocationId,
        permit.generation,
        permit.mutationEpoch,
        permit.transitionRevision,
        permit.expiresAt,
        now,
        now,
        suspension.requestJson,
        invocation.workflowId,
        invocation.runId,
        snapshot,
        ...gate.values,
      )
      .all();
    if (rowResult(result)) return { kind: 'accepted', rawPermit };
    const after = await this.get(permit.invocationId);
    if (
      after?.permitGeneration === permit.generation &&
      after.permitConsumedAt !== undefined
    )
      return { kind: 'already-consumed' };
    throw new RemoteStepRefusal(409, 'remote step permit is unavailable');
  }

  async recordResult(
    invocationId: string,
    rawPermit: string,
    resultJson: string,
    evidenceJson: string,
  ): Promise<
    | { kind: 'recorded'; resultJson: string; resultDigest: string }
    | { kind: 'replay'; resultJson: string; resultDigest: string }
  > {
    if (!isPathSafeId(invocationId) || !RAW_PERMIT.test(rawPermit))
      throw new RemoteStepRefusal(
        400,
        'remote step result identity is malformed',
      );
    boundedJsonBytes(resultJson);
    boundedJsonBytes(evidenceJson);
    const stored = rowResult(
      await this.#db
        .prepare(`SELECT * FROM ${REMOTE_STEP_TABLE} WHERE invocation_id = ?`)
        .bind(invocationId)
        .all(),
    );
    if (!stored) throw new RemoteStepRefusal(404, 'invocation not found');
    const invocation = decode(stored);
    const permitDigest = stored.permit_digest;
    if (
      typeof permitDigest !== 'string' ||
      !SHA256_HEX.test(permitDigest) ||
      invocation.permitConsumedAt === undefined
    )
      throw new RemoteStepRefusal(409, 'remote step permit was not consumed');
    const presentedDigest = await sha256Hex(rawPermit);
    if (!(await credentialsMatch(presentedDigest, permitDigest)))
      throw new RemoteStepRefusal(403, 'remote step permit is invalid');
    const resultDigest = await sha256Hex(resultJson);
    const quiescenceDigest = await sha256Hex(evidenceJson);
    const replay = (row: JournalRow) => {
      if (
        row.result_json !== resultJson ||
        row.result_digest !== resultDigest ||
        row.quiescence_digest !== quiescenceDigest ||
        !positiveTime(row.executor_quiescent_at)
      )
        throw new RemoteStepRefusal(409, 'remote step result conflicts');
      return { kind: 'replay' as const, resultJson, resultDigest };
    };
    if (stored.result_json !== null) return replay(stored);
    await this.#verifyCompletion({
      invocation,
      rawPermit,
      resultJson,
      evidenceJson,
    });
    const result = await this.#db
      .prepare(`UPDATE ${REMOTE_STEP_TABLE}
        SET result_json = ?, result_digest = ?, quiescence_digest = ?,
            executor_quiescent_at = ?, status = 'result-retained'
        WHERE invocation_id = ? AND permit_digest = ?
          AND permit_consumed_at IS NOT NULL AND result_json IS NULL
          AND status = 'dispatched'
        RETURNING *`)
      .bind(
        resultJson,
        resultDigest,
        quiescenceDigest,
        this.#now(),
        invocationId,
        permitDigest,
      )
      .all();
    if (rowResult(result))
      return { kind: 'recorded', resultJson, resultDigest };
    const after = rowResult(
      await this.#db
        .prepare(`SELECT * FROM ${REMOTE_STEP_TABLE} WHERE invocation_id = ?`)
        .bind(invocationId)
        .all(),
    );
    if (!after)
      throw new RemoteStepRefusal(503, 'remote step result is unreadable');
    return replay(after);
  }

  async readRetainedResult(
    invocationId: string,
  ): Promise<RecordedRemoteStepResult | undefined> {
    if (!isPathSafeId(invocationId))
      throw new RemoteStepRefusal(400, 'invocation id is malformed');
    const row = rowResult(
      await this.#db
        .prepare(`SELECT * FROM ${REMOTE_STEP_TABLE} WHERE invocation_id = ?`)
        .bind(invocationId)
        .all(),
    );
    if (!row) return undefined;
    decode(row);
    const resultJson = row.result_json;
    const resultDigest = row.result_digest;
    const quiescenceDigest = row.quiescence_digest;
    const executorQuiescentAt = row.executor_quiescent_at;
    if (resultJson === null) return undefined;
    if (
      row.status !== 'result-retained' ||
      row.permit_consumed_at === null ||
      typeof resultDigest !== 'string' ||
      !SHA256_HEX.test(resultDigest) ||
      typeof quiescenceDigest !== 'string' ||
      !SHA256_HEX.test(quiescenceDigest) ||
      !positiveTime(executorQuiescentAt)
    )
      throw new RemoteStepRefusal(503, 'remote step result is unreadable');
    try {
      boundedJsonBytes(resultJson);
    } catch {
      throw new RemoteStepRefusal(503, 'remote step result is unreadable');
    }
    if ((await sha256Hex(resultJson)) !== resultDigest)
      throw new RemoteStepRefusal(503, 'remote step result is unreadable');
    return {
      invocationId,
      resultJson,
      resultDigest,
      quiescenceDigest,
      executorQuiescentAt,
    };
  }
}
