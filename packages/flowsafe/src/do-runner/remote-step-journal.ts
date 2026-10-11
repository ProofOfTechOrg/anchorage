// SPDX-License-Identifier: Apache-2.0

import { EXECUTION_FENCE_TABLE } from '#deployment-identity-protocol';
import { DoStatusError } from './do-status-error.js';
import {
  admitsExistingRun,
  captureExecutionFenceAdmissionSchema,
  type ExecutionFenceDatabase,
  type ExecutionFenceStore,
  executionFenceAdmissionSql,
  executionFenceAdmissionValues,
} from './execution-fence.js';
import { resourceIdFromKey } from './memory-id.js';
import { isPathSafeId } from './path-safe-id.js';
import type { RunnerRuntime, RunSummary } from './runtime.js';
import { validateTablePrefix } from './table-prefix.js';

export const REMOTE_STEP_TABLE = 'flowsafe_remote_step_invocations';
export const REMOTE_STEP_MARKER = 'flowsafe.remote-step/v1';
export const MAX_REMOTE_STEP_REQUEST_BYTES = 16_384;

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
  stepPath: readonly string[];
  suspendedAt: number;
  resumeCount?: number;
  request: unknown;
  deadlineAt: number;
  status: 'pending' | 'dispatched' | 'failed';
  permitGeneration: number;
  permitEpoch?: number;
  permitRevision?: number;
  permitExpiresAt?: number;
  permitConsumedAt?: number;
  permitClosedAt?: number;
}

export interface RemoteStepPermit {
  invocationId: string;
  generation: number;
  mutationEpoch: number;
  transitionRevision: number;
  expiresAt: number;
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
  permit_closed_at: number | null;
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
    new TextEncoder().encode(row.request_json).length >
      MAX_REMOTE_STEP_REQUEST_BYTES ||
    !isPathSafeId(row.deployment_id) ||
    !isPathSafeId(row.workflow_id) ||
    !isPathSafeId(row.agent_id) ||
    !isPathSafeId(row.thread_id) ||
    !isPathSafeId(row.resource_id) ||
    !isPathSafeId(row.run_id)
  )
    throw new RemoteStepRefusal(503, 'remote step journal is unreadable');
  try {
    validateTablePrefix(row.table_prefix);
  } catch {
    throw new RemoteStepRefusal(503, 'remote step journal is unreadable');
  }
  let stepPath: unknown;
  let request: unknown;
  try {
    stepPath = JSON.parse(row.step_path);
    request = JSON.parse(row.request_json);
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
    !['pending', 'dispatched', 'failed'].includes(row.status)
  )
    throw new RemoteStepRefusal(503, 'remote step journal is unreadable');
  return {
    invocationId: row.invocation_id,
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
    ...(row.permit_closed_at === null
      ? {}
      : { permitClosedAt: row.permit_closed_at }),
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
    this.#db = input.db;
    this.#fence = input.fence;
    this.#runtime = input.runtime;
    this.#deploymentId = input.deploymentId;
    this.#tablePrefix = input.tablePrefix;
    this.#allowedToolIds = new Set(input.allowedToolIds);
    this.#now = input.now ?? Date.now;
  }

  async initialize(): Promise<void> {
    await this.#db
      .prepare(`CREATE TABLE IF NOT EXISTS ${REMOTE_STEP_TABLE} (
        invocation_id TEXT PRIMARY KEY,
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
        permit_closed_at INTEGER,
        UNIQUE (deployment_id, table_prefix, workflow_id, agent_id,
          thread_id, resource_id, run_id, step_path, suspended_at, resume_count)
      )`)
      .run();
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
    await this.#execution(address);
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
        invocation_id, deployment_id, table_prefix, workflow_id, agent_id,
        thread_id, resource_id, run_id, step_path, suspended_at, resume_count,
        request_json, deadline_at, status)
        SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending'
        WHERE EXISTS (SELECT 1 FROM "${address.tablePrefix}mastra_workflow_snapshot"
          WHERE workflow_name = ? AND run_id = ? AND snapshot = ?)
        ON CONFLICT DO NOTHING`)
      .bind(
        invocationId,
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
      .prepare(`SELECT * FROM ${REMOTE_STEP_TABLE}
        WHERE deployment_id = ? AND table_prefix = ? AND workflow_id = ?
          AND agent_id = ? AND thread_id = ? AND resource_id = ? AND run_id = ?
          AND step_path = ? AND suspended_at = ? AND resume_count = ?`)
      .bind(
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

  async #execution(address: RemoteStepAddress) {
    const state = await this.#runtime.authoritativeStartState(
      address.workflowId,
      address.runId,
    );
    const target = state?.provenance.startIdentity?.target;
    if (state?.storage !== 'd1' || target?.kind !== 'agent')
      throw new RemoteStepRefusal(
        503,
        'remote step run identity is unreadable',
      );
    if (
      target.id !== address.agentId ||
      target.threadId !== address.threadId ||
      state.execution.tablePrefix !== address.tablePrefix ||
      address.resourceId !== resourceIdFromKey(address.threadId)
    )
      throw new RemoteStepRefusal(409, 'remote step run identity changed');
    return state.execution;
  }

  async #admission(invocation: RemoteStepInvocation, priorBindings: number) {
    const observed = await this.#fence.readForAdmission();
    const proof = await this.#execution(invocation);
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
      JSON.stringify(suspension.stepPath) !==
        JSON.stringify(invocation.stepPath)
    )
      throw new RemoteStepRefusal(409, 'remote step suspension changed');
    const gate = await this.#admission(invocation, 9);
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
      (invocation.permitConsumedAt !== undefined &&
        invocation.permitClosedAt === undefined)
    )
      throw new RemoteStepRefusal(409, 'remote step dispatch is active');
    const epoch = gate.observed.mutationEpoch;
    const revision = gate.observed.transitionRevision;
    if (epoch === undefined || revision === undefined)
      throw new RemoteStepRefusal(503, 'remote step fence is unversioned');
    const result = await this.#db
      .prepare(`UPDATE ${REMOTE_STEP_TABLE} SET
        status = 'dispatched', permit_generation = permit_generation + 1,
        permit_epoch = ?, permit_revision = ?, permit_expires_at = ?,
        permit_consumed_at = NULL, permit_closed_at = NULL
        WHERE invocation_id = ? AND permit_generation = ? AND deadline_at > ?
          AND status IN ('pending', 'dispatched')
          AND (permit_consumed_at IS NULL OR permit_closed_at IS NOT NULL)
          AND EXISTS (SELECT 1 FROM "${address.tablePrefix}mastra_workflow_snapshot"
            WHERE workflow_name = ? AND run_id = ? AND snapshot = ?)
          AND ${gate.predicate} RETURNING *`)
      .bind(
        epoch,
        revision,
        expiresAt,
        invocationId,
        invocation.permitGeneration,
        now,
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
      expiresAt,
    };
  }

  async consumePermit(
    permit: RemoteStepPermit,
  ): Promise<'accepted' | 'already-consumed'> {
    const invocation = await this.get(permit.invocationId);
    if (!invocation) throw new RemoteStepRefusal(404, 'invocation not found');
    if (
      invocation.permitGeneration !== permit.generation ||
      invocation.permitEpoch !== permit.mutationEpoch ||
      invocation.permitRevision !== permit.transitionRevision ||
      invocation.permitExpiresAt !== permit.expiresAt
    )
      throw new RemoteStepRefusal(409, 'remote step permit changed');
    if (invocation.permitConsumedAt !== undefined) return 'already-consumed';
    const now = this.#now();
    const { suspension, snapshot } = await this.#stableSnapshot(invocation);
    if (
      suspension.suspendedAt !== invocation.suspendedAt ||
      suspension.resumeCount !== invocation.resumeCount ||
      JSON.stringify(suspension.stepPath) !==
        JSON.stringify(invocation.stepPath)
    )
      throw new RemoteStepRefusal(409, 'remote step suspension changed');
    const gate = await this.#admission(invocation, 11);
    const result = await this.#db
      .prepare(`UPDATE ${REMOTE_STEP_TABLE} SET permit_consumed_at = ?
        WHERE invocation_id = ? AND permit_generation = ?
          AND permit_epoch = ? AND permit_revision = ?
          AND permit_expires_at = ? AND permit_expires_at > ?
          AND deadline_at > ? AND permit_consumed_at IS NULL
          AND status = 'dispatched'
          AND EXISTS (SELECT 1 FROM "${invocation.tablePrefix}mastra_workflow_snapshot"
            WHERE workflow_name = ? AND run_id = ? AND snapshot = ?)
          AND ${gate.predicate} RETURNING invocation_id`)
      .bind(
        now,
        permit.invocationId,
        permit.generation,
        permit.mutationEpoch,
        permit.transitionRevision,
        permit.expiresAt,
        now,
        now,
        invocation.workflowId,
        invocation.runId,
        snapshot,
        ...gate.values,
      )
      .all();
    if (rowResult(result)) return 'accepted';
    const after = await this.get(permit.invocationId);
    if (
      after?.permitGeneration === permit.generation &&
      after.permitConsumedAt !== undefined
    )
      return 'already-consumed';
    throw new RemoteStepRefusal(409, 'remote step permit is unavailable');
  }

  async closePermit(permit: RemoteStepPermit): Promise<void> {
    const result = await this.#db
      .prepare(`UPDATE ${REMOTE_STEP_TABLE} SET permit_closed_at = ?
        WHERE invocation_id = ? AND permit_generation = ?
          AND permit_consumed_at IS NOT NULL AND permit_closed_at IS NULL
        RETURNING invocation_id`)
      .bind(this.#now(), permit.invocationId, permit.generation)
      .all();
    if (rowResult(result)) return;
    const current = await this.get(permit.invocationId);
    if (
      current?.permitGeneration !== permit.generation ||
      current.permitClosedAt === undefined
    )
      throw new RemoteStepRefusal(409, 'remote step permit close conflicts');
  }
}
