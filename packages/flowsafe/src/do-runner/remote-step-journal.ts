// SPDX-License-Identifier: Apache-2.0

import { EXECUTION_FENCE_TABLE } from '#deployment-identity-protocol';
import {
  assertExecutionPrincipal,
  type ExecutionPrincipal,
} from '../approval-api/principal.js';
import type { SignalDatabase } from '../signals/d1-shared.js';
import {
  D1NotificationsStorage,
  DEFAULT_NOTIFICATION_OPERATION_RECEIPT_LIMIT,
  NOTIFICATION_OPERATION_PROJECTION_VERSION,
  NOTIFICATION_OPERATION_RECEIPTS_TABLE,
  type NotificationOperationInput,
  projectNotificationOperation,
} from '../signals/notifications-d1.js';
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
export const REMOTE_STEP_NOTIFICATION_CURSOR_TABLE =
  'flowsafe_remote_step_notification_cursor';
const REMOTE_STEP_NOTIFICATION_CURSOR_SQL = `CREATE TABLE ${REMOTE_STEP_NOTIFICATION_CURSOR_TABLE} (
  deployment_id TEXT NOT NULL,
  table_prefix TEXT NOT NULL,
  version INTEGER NOT NULL,
  after_invocation_id TEXT NOT NULL,
  PRIMARY KEY (deployment_id, table_prefix)
)`;
const REMOTE_STEP_RECOVERY_INDEX = 'flowsafe_remote_step_recovery';
const REMOTE_STEP_RECOVERY_INDEX_SQL = `CREATE INDEX ${REMOTE_STEP_RECOVERY_INDEX} ON ${REMOTE_STEP_TABLE} (invocation_id) WHERE status IN ('result-retained', 'resuming')`;
const REMOTE_STEP_AGENT_INDEX = 'flowsafe_remote_step_agent_suspension';
const REMOTE_STEP_WORKFLOW_INDEX = 'flowsafe_remote_step_workflow_suspension';
const REMOTE_STEP_AGENT_INDEX_SQL = `CREATE UNIQUE INDEX ${REMOTE_STEP_AGENT_INDEX} ON ${REMOTE_STEP_TABLE} (deployment_id, table_prefix, workflow_id, agent_id, thread_id, resource_id, run_id, step_path, suspended_at, resume_count) WHERE target_kind = 'agent'`;
const REMOTE_STEP_WORKFLOW_INDEX_SQL = `CREATE UNIQUE INDEX ${REMOTE_STEP_WORKFLOW_INDEX} ON ${REMOTE_STEP_TABLE} (deployment_id, table_prefix, workflow_id, run_id, step_path, suspended_at, resume_count) WHERE target_kind = 'workflow'`;
const REMOTE_STEP_WRITE_TRIGGER = 'flowsafe_remote_step_write_version';
const REMOTE_STEP_WRITE_TRIGGER_SQL = `CREATE TRIGGER ${REMOTE_STEP_WRITE_TRIGGER} BEFORE UPDATE ON ${REMOTE_STEP_TABLE} WHEN NEW.write_revision <= OLD.write_revision BEGIN SELECT RAISE(ABORT, 'remote step writer version changed'); END`;
const REMOTE_STEP_V5_WRITE_TRIGGER_SQL = `CREATE TRIGGER ${REMOTE_STEP_WRITE_TRIGGER} BEFORE UPDATE ON ${REMOTE_STEP_TABLE} WHEN NEW.write_generation <= OLD.write_generation OR NEW.write_revision <= OLD.write_revision BEGIN SELECT RAISE(ABORT, 'remote step writer version changed'); END`;
const REMOTE_STEP_OPERATION_INDEX =
  'flowsafe_remote_step_notification_recovery';
const REMOTE_STEP_OPERATION_INDEX_SQL = `CREATE INDEX ${REMOTE_STEP_OPERATION_INDEX} ON ${REMOTE_STEP_TABLE} (invocation_id) WHERE operation_state IN ('prepared', 'effect-admitted', 'receipt-recorded', 'resume-admitted', 'unresolved')`;
const REMOTE_STEP_RESULT_COLUMNS = [
  'result_json',
  'result_digest',
  'quiescence_digest',
  'executor_quiescent_at',
] as const;
const REMOTE_STEP_SETTLEMENT_COLUMNS = [
  'resume_lease_owner',
  'resume_lease_expires_at',
  'settled_at',
  'settlement_outcome',
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
  ...REMOTE_STEP_SETTLEMENT_COLUMNS,
  'target_kind',
  'suspension_marker',
  'request_digest',
  'write_revision',
] as const;
const REMOTE_STEP_V5_COLUMNS = [
  ...REMOTE_STEP_COLUMNS,
  'write_generation',
  'operation_key',
  'operation_id',
  'notification_id',
  'notification_request_json',
  'notification_projection_version',
  'notification_projection_json',
  'notification_request_digest',
  'operation_state',
  'effect_admitted_at',
  'receipt_recorded_at',
  'resume_admitted_at',
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
  'resume_lease_expires_at',
  'settled_at',
  'write_revision',
  'write_generation',
  'effect_admitted_at',
  'receipt_recorded_at',
  'resume_admitted_at',
]);
export const REMOTE_STEP_MARKER = 'flowsafe.remote-step/v1';
export const WORKFLOW_REMOTE_STEP_MARKER = 'flowsafe.remote-step.workflow/v1';
export const MAX_REMOTE_STEP_REQUEST_BYTES = 16_384;
export const MAX_REMOTE_STEP_RESULT_BYTES = 16_384;

const SHA256_HEX = /^[a-f0-9]{64}$/;
const RAW_PERMIT = /^[A-Za-z0-9_-]{43}$/;

function remoteStepTableSql(name: string, version: 4 | 5 = 5): string {
  return `CREATE TABLE IF NOT EXISTS ${name} (
        invocation_id TEXT PRIMARY KEY,
        principal_json TEXT NOT NULL,
        start_token TEXT NOT NULL,
        deployment_id TEXT NOT NULL,
        table_prefix TEXT NOT NULL,
        workflow_id TEXT NOT NULL,
        agent_id TEXT,
        thread_id TEXT,
        resource_id TEXT,
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
        resume_lease_owner TEXT,
        resume_lease_expires_at INTEGER,
        settled_at INTEGER,
        settlement_outcome TEXT,
        target_kind TEXT NOT NULL CHECK (target_kind IN ('agent', 'workflow')),
        suspension_marker TEXT NOT NULL,
        request_digest TEXT,
        write_revision INTEGER NOT NULL DEFAULT 0${
          version === 5
            ? `,
        write_generation INTEGER NOT NULL CHECK (write_generation > 0),
        operation_key TEXT UNIQUE,
        operation_id TEXT,
        notification_id TEXT,
        notification_request_json TEXT,
        notification_projection_version TEXT,
        notification_projection_json TEXT,
        notification_request_digest TEXT,
        operation_state TEXT CHECK (operation_state IN ('prepared', 'effect-admitted', 'receipt-recorded', 'resume-admitted', 'unresolved')),
        effect_admitted_at INTEGER,
        receipt_recorded_at INTEGER,
        resume_admitted_at INTEGER,
        CHECK ((operation_state IS NULL AND operation_key IS NULL AND operation_id IS NULL
          AND notification_id IS NULL AND notification_request_json IS NULL
          AND notification_projection_version IS NULL AND notification_projection_json IS NULL
          AND notification_request_digest IS NULL AND effect_admitted_at IS NULL
          AND receipt_recorded_at IS NULL AND resume_admitted_at IS NULL) OR
          (target_kind = 'agent' AND operation_key IS NOT NULL AND operation_id IS NOT NULL
          AND notification_id IS NOT NULL AND notification_request_json IS NOT NULL
          AND notification_projection_version = 'notification-record-values-v1'
          AND notification_projection_json IS NOT NULL AND notification_request_digest IS NOT NULL))`
            : ''
        },
        CHECK ((target_kind = 'agent' AND agent_id IS NOT NULL
          AND thread_id IS NOT NULL AND resource_id IS NOT NULL
          AND suspension_marker = '${REMOTE_STEP_MARKER}') OR
          (target_kind = 'workflow' AND agent_id IS NULL
          AND thread_id IS NULL AND resource_id IS NULL
          AND suspension_marker = '${WORKFLOW_REMOTE_STEP_MARKER}'
          AND request_digest IS NOT NULL))
      )`;
}

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

export interface WorkflowRemoteStepAddress {
  targetKind: 'workflow';
  deploymentId: string;
  tablePrefix: string;
  workflowId: string;
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
  status:
    | 'pending'
    | 'dispatched'
    | 'result-retained'
    | 'resuming'
    | 'settled'
    | 'failed';
  permitGeneration: number;
  permitEpoch?: number;
  permitRevision?: number;
  permitExpiresAt?: number;
  permitConsumedAt?: number;
  resumeLeaseExpiresAt?: number;
  settlementOutcome?:
    | 'applied'
    | 'terminal-not-success'
    | 'deadline-expired'
    | 'advanced-without-proof';
}

export interface WorkflowRemoteStepInvocation
  extends Omit<RemoteStepInvocation, 'agentId' | 'threadId' | 'resourceId'> {
  targetKind: 'workflow';
  requestDigest: string;
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

export interface RemoteStepNotificationOperation {
  invocationId: string;
  operationId: string;
  operationKey: string;
  notificationId: string;
  requestJson: string;
  projectionVersion: typeof NOTIFICATION_OPERATION_PROJECTION_VERSION;
  projectionJson: string;
  requestDigest: string;
  state:
    | 'prepared'
    | 'effect-admitted'
    | 'receipt-recorded'
    | 'resume-admitted'
    | 'unresolved';
  effectAdmittedAt?: number;
  receiptRecordedAt?: number;
  resumeAdmittedAt?: number;
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
  resume_lease_owner: string | null;
  resume_lease_expires_at: number | null;
  settled_at: number | null;
  settlement_outcome: string | null;
  target_kind: string;
  suspension_marker: string;
  request_digest: string | null;
  write_revision: number;
  write_generation: number;
  operation_key: string | null;
  operation_id: string | null;
  notification_id: string | null;
  notification_request_json: string | null;
  notification_projection_version: string | null;
  notification_projection_json: string | null;
  notification_request_digest: string | null;
  operation_state: string | null;
  effect_admitted_at: number | null;
  receipt_recorded_at: number | null;
  resume_admitted_at: number | null;
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

function validLifecycle(row: JournalRow): boolean {
  return (
    Number.isSafeInteger(row.resume_count) &&
    (row.resume_count === -1 || row.resume_count >= 1) &&
    Number.isSafeInteger(row.permit_generation) &&
    row.permit_generation >= 0 &&
    [
      'pending',
      'dispatched',
      'result-retained',
      'resuming',
      'settled',
      'failed',
    ].includes(row.status) &&
    (row.settlement_outcome === null ||
      [
        'applied',
        'terminal-not-success',
        'deadline-expired',
        'advanced-without-proof',
      ].includes(row.settlement_outcome)) &&
    (row.status !== 'resuming' ||
      (isPathSafeId(row.resume_lease_owner) &&
        positiveTime(row.resume_lease_expires_at) &&
        row.result_json !== null)) &&
    (row.status === 'resuming' ||
      (row.resume_lease_owner === null &&
        row.resume_lease_expires_at === null)) &&
    (row.status !== 'settled' ||
      (row.settlement_outcome === 'applied' &&
        row.result_json !== null &&
        row.result_digest !== null &&
        positiveTime(row.executor_quiescent_at))) &&
    (row.settlement_outcome === null ||
      ((row.status === 'settled' || row.status === 'failed') &&
        positiveTime(row.settled_at)))
  );
}

function validPayloadBounds(row: JournalRow): boolean {
  return (
    typeof row.step_path === 'string' &&
    row.step_path.length <= 1_024 &&
    typeof row.request_json === 'string' &&
    new TextEncoder().encode(row.request_json).length <=
      MAX_REMOTE_STEP_REQUEST_BYTES
  );
}

function validStepPath(value: unknown): value is string[] {
  return (
    Array.isArray(value) &&
    value.length > 0 &&
    value.every((part) => typeof part === 'string' && part !== '')
  );
}

function decode(row: JournalRow): RemoteStepInvocation {
  if (
    row.target_kind !== 'agent' ||
    row.suspension_marker !== REMOTE_STEP_MARKER ||
    (row.request_digest !== null && !SHA256_HEX.test(row.request_digest)) ||
    !Number.isSafeInteger(row.write_revision) ||
    row.write_revision < 0 ||
    !positiveTime(row.write_generation) ||
    !validPayloadBounds(row) ||
    typeof row.principal_json !== 'string' ||
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
    !validStepPath(stepPath) ||
    !isPathSafeId(row.invocation_id) ||
    !positiveTime(row.suspended_at) ||
    !positiveTime(row.deadline_at) ||
    !validLifecycle(row)
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
    ...(row.resume_lease_expires_at === null
      ? {}
      : { resumeLeaseExpiresAt: row.resume_lease_expires_at }),
    ...(row.settlement_outcome === null
      ? {}
      : {
          settlementOutcome: row.settlement_outcome as
            | 'applied'
            | 'terminal-not-success'
            | 'deadline-expired'
            | 'advanced-without-proof',
        }),
  };
}

function decodeWorkflow(row: JournalRow): WorkflowRemoteStepInvocation {
  if (
    row.target_kind !== 'workflow' ||
    row.suspension_marker !== WORKFLOW_REMOTE_STEP_MARKER ||
    row.agent_id !== null ||
    row.thread_id !== null ||
    row.resource_id !== null ||
    !isPathSafeId(row.invocation_id) ||
    !isPathSafeId(row.deployment_id) ||
    !isPathSafeId(row.workflow_id) ||
    !isPathSafeId(row.run_id) ||
    !isPathSafeId(row.start_token) ||
    typeof row.request_digest !== 'string' ||
    !SHA256_HEX.test(row.request_digest) ||
    !Number.isSafeInteger(row.write_revision) ||
    row.write_revision < 0 ||
    !positiveTime(row.write_generation) ||
    !positiveTime(row.suspended_at) ||
    !positiveTime(row.deadline_at) ||
    !validLifecycle(row) ||
    !validPayloadBounds(row)
  )
    throw new RemoteStepRefusal(503, 'remote step journal is unreadable');
  try {
    validateTablePrefix(row.table_prefix);
    const stepPath: unknown = JSON.parse(row.step_path);
    const request: unknown = JSON.parse(row.request_json);
    const principal = assertExecutionPrincipal(
      JSON.parse(row.principal_json),
      'stored remote step principal',
    );
    if (!validStepPath(stepPath)) throw new Error('invalid workflow row');
    return {
      targetKind: 'workflow',
      invocationId: row.invocation_id,
      principal,
      startToken: row.start_token,
      deploymentId: row.deployment_id,
      tablePrefix: row.table_prefix,
      workflowId: row.workflow_id,
      runId: row.run_id,
      stepPath,
      suspendedAt: row.suspended_at,
      ...(row.resume_count < 0 ? {} : { resumeCount: row.resume_count }),
      request,
      requestDigest: row.request_digest,
      deadlineAt: row.deadline_at,
      status: row.status as WorkflowRemoteStepInvocation['status'],
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
      ...(row.resume_lease_expires_at === null
        ? {}
        : { resumeLeaseExpiresAt: row.resume_lease_expires_at }),
      ...(row.settlement_outcome === null
        ? {}
        : {
            settlementOutcome:
              row.settlement_outcome as WorkflowRemoteStepInvocation['settlementOutcome'],
          }),
    };
  } catch {
    throw new RemoteStepRefusal(503, 'remote step journal is unreadable');
  }
}

function decodeAny(
  row: JournalRow,
): RemoteStepInvocation | WorkflowRemoteStepInvocation {
  if (row.target_kind === 'agent') return decode(row);
  if (row.target_kind === 'workflow') return decodeWorkflow(row);
  throw new RemoteStepRefusal(503, 'remote step journal is unreadable');
}

function decodeNotificationOperation(
  row: JournalRow,
): RemoteStepNotificationOperation | undefined {
  if (row.operation_state === null) {
    if (
      row.operation_key !== null ||
      row.operation_id !== null ||
      row.notification_id !== null ||
      row.notification_request_json !== null ||
      row.notification_projection_version !== null ||
      row.notification_projection_json !== null ||
      row.notification_request_digest !== null ||
      row.effect_admitted_at !== null ||
      row.receipt_recorded_at !== null ||
      row.resume_admitted_at !== null
    )
      throw new RemoteStepRefusal(503, 'remote step operation is unreadable');
    return undefined;
  }
  if (
    row.target_kind !== 'agent' ||
    !isPathSafeId(row.operation_key) ||
    !isPathSafeId(row.operation_id) ||
    !isPathSafeId(row.notification_id) ||
    typeof row.notification_request_json !== 'string' ||
    typeof row.notification_projection_json !== 'string' ||
    row.notification_projection_version !==
      NOTIFICATION_OPERATION_PROJECTION_VERSION ||
    typeof row.notification_request_digest !== 'string' ||
    !SHA256_HEX.test(row.notification_request_digest) ||
    ![
      'prepared',
      'effect-admitted',
      'receipt-recorded',
      'resume-admitted',
      'unresolved',
    ].includes(row.operation_state) ||
    new TextEncoder().encode(row.notification_request_json).length >
      MAX_REMOTE_STEP_REQUEST_BYTES ||
    new TextEncoder().encode(row.notification_projection_json).length >
      MAX_REMOTE_STEP_REQUEST_BYTES ||
    (row.operation_state === 'prepared' &&
      (row.effect_admitted_at !== null ||
        row.receipt_recorded_at !== null ||
        row.resume_admitted_at !== null)) ||
    (row.operation_state !== 'prepared' &&
      !positiveTime(row.effect_admitted_at)) ||
    (row.operation_state === 'effect-admitted' &&
      (row.receipt_recorded_at !== null || row.resume_admitted_at !== null)) ||
    (['receipt-recorded', 'resume-admitted'].includes(row.operation_state) &&
      !positiveTime(row.receipt_recorded_at)) ||
    (row.operation_state === 'receipt-recorded' &&
      row.resume_admitted_at !== null) ||
    (row.operation_state === 'resume-admitted' &&
      !positiveTime(row.resume_admitted_at))
  )
    throw new RemoteStepRefusal(503, 'remote step operation is unreadable');
  try {
    JSON.parse(row.notification_request_json);
    JSON.parse(row.notification_projection_json);
  } catch {
    throw new RemoteStepRefusal(503, 'remote step operation is unreadable');
  }
  return {
    invocationId: row.invocation_id,
    operationId: row.operation_id,
    operationKey: row.operation_key,
    notificationId: row.notification_id,
    requestJson: row.notification_request_json,
    projectionVersion: NOTIFICATION_OPERATION_PROJECTION_VERSION,
    projectionJson: row.notification_projection_json,
    requestDigest: row.notification_request_digest,
    state: row.operation_state as RemoteStepNotificationOperation['state'],
    ...(row.effect_admitted_at === null
      ? {}
      : { effectAdmittedAt: row.effect_admitted_at }),
    ...(row.receipt_recorded_at === null
      ? {}
      : { receiptRecordedAt: row.receipt_recorded_at }),
    ...(row.resume_admitted_at === null
      ? {}
      : { resumeAdmittedAt: row.resume_admitted_at }),
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

function selectedWorkflowSuspension(summary: RunSummary): {
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
    const remoteStep = record(suspended?.remoteStep);
    return suspended?.type === 'suspension' &&
      remoteStep?.marker === WORKFLOW_REMOTE_STEP_MARKER
      ? [{ path, key, remoteStep }]
      : [];
  });
  if (matches.length !== 1)
    throw new RemoteStepRefusal(409, 'remote step suspension is ambiguous');
  const selected = matches[0];
  if (!selected || !Object.hasOwn(selected.remoteStep, 'request'))
    throw new RemoteStepRefusal(503, 'remote step suspension is unreadable');
  const suspendedAt = summary.suspendedAt?.[selected.key];
  const resumeCount = summary.resumeCount?.[selected.key];
  const requestJson = JSON.stringify(selected.remoteStep.request);
  if (
    !positiveTime(suspendedAt) ||
    (resumeCount !== undefined &&
      (!Number.isSafeInteger(resumeCount) || resumeCount < 1)) ||
    typeof requestJson !== 'string' ||
    new TextEncoder().encode(requestJson).length > MAX_REMOTE_STEP_REQUEST_BYTES
  )
    throw new RemoteStepRefusal(503, 'remote step suspension is unreadable');
  return {
    stepPath: selected.path,
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
  readonly #verifyWorkflowCompletion?: (input: {
    invocation: WorkflowRemoteStepInvocation;
    rawPermit: string;
    resultJson: string;
    evidenceJson: string;
  }) => Promise<void>;

  get deploymentId(): string {
    return this.#deploymentId;
  }

  get tablePrefix(): string {
    return this.#tablePrefix;
  }

  notificationStorage(
    operationReceiptLimit = DEFAULT_NOTIFICATION_OPERATION_RECEIPT_LIMIT,
  ): D1NotificationsStorage {
    const db = this.#db as ExecutionFenceDatabase & SignalDatabase;
    if (typeof db.batch !== 'function')
      throw new Error('notification operation requires database.batch()');
    return new D1NotificationsStorage(
      db,
      this.#tablePrefix,
      operationReceiptLimit,
    );
  }

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
    verifyWorkflowCompletion?: (input: {
      invocation: WorkflowRemoteStepInvocation;
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
    this.#verifyWorkflowCompletion = input.verifyWorkflowCompletion;
    this.#now = input.now ?? Date.now;
  }

  async initialize(): Promise<void> {
    await this.#db.prepare(remoteStepTableSql(REMOTE_STEP_TABLE)).run();
    await this.#db
      .prepare(`CREATE TABLE IF NOT EXISTS ${REMOTE_STEP_SCHEMA_TABLE} (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        version INTEGER NOT NULL
      )`)
      .run();
    await this.#db
      .prepare(
        REMOTE_STEP_NOTIFICATION_CURSOR_SQL.replace(
          'CREATE TABLE ',
          'CREATE TABLE IF NOT EXISTS ',
        ),
      )
      .run();
    const readSchema = async () => {
      const schema = await this.#db
        .prepare(`PRAGMA table_xinfo(${REMOTE_STEP_TABLE})`)
        .all();
      const columns = record(schema)?.results;
      if (!Array.isArray(columns))
        throw new RemoteStepRefusal(503, 'remote step schema is unreadable');
      const samePrefix = (
        expected: readonly string[],
        v4 = false,
        v5 = false,
      ) =>
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
            column?.notnull ===
              ((index > 0 &&
                index <= 16 &&
                !(v4 && index >= 6 && index <= 8)) ||
              ((v4 || v5) &&
                ['target_kind', 'suspension_marker', 'write_revision'].includes(
                  name as string,
                )) ||
              (v5 && name === 'write_generation')
                ? 1
                : 0) &&
            column?.pk === (index === 0 ? 1 : 0) &&
            column?.hidden === 0 &&
            column?.dflt_value ===
              (name === 'permit_generation' || name === 'write_revision'
                ? '0'
                : null)
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
      const recoveryIndex = indexes.find(
        (value) => record(value)?.name === REMOTE_STEP_RECOVERY_INDEX,
      );
      const recoveryDefinition = record(
        await this.#db
          .prepare('SELECT sql FROM sqlite_schema WHERE type = ? AND name = ?')
          .bind('index', REMOTE_STEP_RECOVERY_INDEX)
          .all(),
      )?.results;
      const validRecoveryIndex =
        record(recoveryIndex)?.unique === 0 &&
        record(recoveryIndex)?.origin === 'c' &&
        record(recoveryIndex)?.partial === 1 &&
        Array.isArray(recoveryDefinition) &&
        recoveryDefinition.length === 1 &&
        record(recoveryDefinition[0])?.sql === REMOTE_STEP_RECOVERY_INDEX_SQL;
      const validV4Index = async (name: string, sql: string) => {
        const entry = indexes.find((value) => record(value)?.name === name);
        const definition = record(
          await this.#db
            .prepare(
              'SELECT sql FROM sqlite_schema WHERE type = ? AND name = ?',
            )
            .bind('index', name)
            .all(),
        )?.results;
        return (
          record(entry)?.unique === 1 &&
          record(entry)?.origin === 'c' &&
          record(entry)?.partial === 1 &&
          Array.isArray(definition) &&
          definition.length === 1 &&
          record(definition[0])?.sql === sql
        );
      };
      const validV4Table = async () => {
        const tableDefinition = record(
          await this.#db
            .prepare(
              'SELECT sql FROM sqlite_schema WHERE type = ? AND name = ?',
            )
            .bind('table', REMOTE_STEP_TABLE)
            .all(),
        )?.results;
        const tableSql =
          Array.isArray(tableDefinition) && tableDefinition.length === 1
            ? record(tableDefinition[0])?.sql
            : undefined;
        return (
          typeof tableSql === 'string' &&
          tableSql.includes(
            "target_kind TEXT NOT NULL CHECK (target_kind IN ('agent', 'workflow'))",
          ) &&
          tableSql.includes(
            "CHECK ((target_kind = 'agent' AND agent_id IS NOT NULL",
          ) &&
          tableSql.includes("(target_kind = 'workflow' AND agent_id IS NULL") &&
          tableSql.includes('write_revision INTEGER NOT NULL DEFAULT 0')
        );
      };
      const validV4 = async () => {
        const trigger = record(
          await this.#db
            .prepare(
              'SELECT sql FROM sqlite_schema WHERE type = ? AND name = ?',
            )
            .bind('trigger', REMOTE_STEP_WRITE_TRIGGER)
            .all(),
        )?.results;
        return (
          (await validV4Table()) &&
          validRecoveryIndex &&
          (await validV4Index(
            REMOTE_STEP_AGENT_INDEX,
            REMOTE_STEP_AGENT_INDEX_SQL,
          )) &&
          (await validV4Index(
            REMOTE_STEP_WORKFLOW_INDEX,
            REMOTE_STEP_WORKFLOW_INDEX_SQL,
          )) &&
          Array.isArray(trigger) &&
          trigger.length === 1 &&
          record(trigger[0])?.sql === REMOTE_STEP_WRITE_TRIGGER_SQL
        );
      };
      const validV5 = async () => {
        const tableDefinition = record(
          await this.#db
            .prepare(
              'SELECT sql FROM sqlite_schema WHERE type = ? AND name = ?',
            )
            .bind('table', REMOTE_STEP_TABLE)
            .all(),
        )?.results;
        const tableSql = Array.isArray(tableDefinition)
          ? record(tableDefinition[0])?.sql
          : undefined;
        const operationIndex = indexes.find(
          (value) => record(value)?.name === REMOTE_STEP_OPERATION_INDEX,
        );
        const operationDefinition = record(
          await this.#db
            .prepare(
              'SELECT sql FROM sqlite_schema WHERE type = ? AND name = ?',
            )
            .bind('index', REMOTE_STEP_OPERATION_INDEX)
            .all(),
        )?.results;
        const trigger = record(
          await this.#db
            .prepare(
              'SELECT sql FROM sqlite_schema WHERE type = ? AND name = ?',
            )
            .bind('trigger', REMOTE_STEP_WRITE_TRIGGER)
            .all(),
        )?.results;
        return (
          typeof tableSql === 'string' &&
          tableSql.includes(
            'write_generation INTEGER NOT NULL CHECK (write_generation > 0)',
          ) &&
          tableSql.includes('operation_key TEXT UNIQUE') &&
          tableSql.includes(
            "CHECK ((target_kind = 'agent' AND agent_id IS NOT NULL",
          ) &&
          tableSql.includes("(target_kind = 'workflow' AND agent_id IS NULL") &&
          tableSql.includes(
            "operation_state TEXT CHECK (operation_state IN ('prepared', 'effect-admitted', 'receipt-recorded', 'resume-admitted', 'unresolved'))",
          ) &&
          tableSql.includes(
            'CHECK ((operation_state IS NULL AND operation_key IS NULL',
          ) &&
          tableSql.includes(
            "(target_kind = 'agent' AND operation_key IS NOT NULL",
          ) &&
          validRecoveryIndex &&
          (await validV4Index(
            REMOTE_STEP_AGENT_INDEX,
            REMOTE_STEP_AGENT_INDEX_SQL,
          )) &&
          (await validV4Index(
            REMOTE_STEP_WORKFLOW_INDEX,
            REMOTE_STEP_WORKFLOW_INDEX_SQL,
          )) &&
          record(operationIndex)?.unique === 0 &&
          record(operationIndex)?.origin === 'c' &&
          record(operationIndex)?.partial === 1 &&
          Array.isArray(operationDefinition) &&
          operationDefinition.length === 1 &&
          record(operationDefinition[0])?.sql ===
            REMOTE_STEP_OPERATION_INDEX_SQL &&
          Array.isArray(trigger) &&
          trigger.length === 1 &&
          record(trigger[0])?.sql === REMOTE_STEP_V5_WRITE_TRIGGER_SQL
        );
      };
      const versions = await this.#db
        .prepare(`SELECT id, version FROM ${REMOTE_STEP_SCHEMA_TABLE} LIMIT 2`)
        .all();
      const rows = record(versions)?.results;
      if (!Array.isArray(rows) || rows.length > 1)
        throw new RemoteStepRefusal(503, 'remote step schema is unreadable');
      if (rows.length === 1) {
        if (record(rows[0])?.id === 1 && record(rows[0])?.version === 5) {
          if (
            !samePrefix(REMOTE_STEP_V5_COLUMNS, true, true) ||
            !(await validV5())
          )
            throw new RemoteStepRefusal(
              503,
              'remote step schema is unreadable',
            );
          return 'current-v5';
        }
        if (record(rows[0])?.id === 1 && record(rows[0])?.version === 4) {
          if (!samePrefix(REMOTE_STEP_COLUMNS, true) || !(await validV4()))
            throw new RemoteStepRefusal(
              503,
              'remote step schema is unreadable',
            );
          return 'current-v4';
        }
        if (
          record(rows[0])?.id !== 1 ||
          (record(rows[0])?.version !== 3 && record(rows[0])?.version !== 2) ||
          !uniqueSuspension ||
          !samePrefix(
            record(rows[0])?.version === 3
              ? REMOTE_STEP_COLUMNS.slice(0, -4)
              : REMOTE_STEP_COLUMNS.slice(0, -8),
          ) ||
          (record(rows[0])?.version === 3 && !validRecoveryIndex)
        )
          throw new RemoteStepRefusal(503, 'remote step schema is unreadable');
        return record(rows[0])?.version === 3 ? 'prior-v3' : 'prior-v2';
      }
      if (uniqueSuspension && samePrefix(REMOTE_STEP_COLUMNS.slice(0, -12)))
        return 'prior-v1';
      if (uniqueSuspension && samePrefix(REMOTE_STEP_COLUMNS.slice(0, -8)))
        return 'unversioned-v2';
      if (uniqueSuspension && samePrefix(REMOTE_STEP_COLUMNS.slice(0, -4)))
        return 'unversioned-v3';
      if (samePrefix(REMOTE_STEP_COLUMNS, true) && (await validV4Table()))
        return 'unversioned-v4';
      if (samePrefix(REMOTE_STEP_V5_COLUMNS, true, true)) {
        const definition = record(
          await this.#db
            .prepare(
              'SELECT sql FROM sqlite_schema WHERE type = ? AND name = ?',
            )
            .bind('table', REMOTE_STEP_TABLE)
            .all(),
        )?.results;
        const sql = Array.isArray(definition)
          ? record(definition[0])?.sql
          : undefined;
        if (
          typeof sql === 'string' &&
          sql.includes(
            'write_generation INTEGER NOT NULL CHECK (write_generation > 0)',
          ) &&
          sql.includes('operation_key TEXT UNIQUE') &&
          sql.includes(
            "operation_state TEXT CHECK (operation_state IN ('prepared', 'effect-admitted', 'receipt-recorded', 'resume-admitted', 'unresolved'))",
          ) &&
          sql.includes(
            'CHECK ((operation_state IS NULL AND operation_key IS NULL',
          ) &&
          sql.includes(
            "(target_kind = 'agent' AND operation_key IS NOT NULL",
          ) &&
          sql.includes(
            "CHECK ((target_kind = 'agent' AND agent_id IS NOT NULL",
          ) &&
          sql.includes("(target_kind = 'workflow' AND agent_id IS NULL")
        )
          return 'unversioned-v5';
      }
      throw new RemoteStepRefusal(503, 'remote step schema is unreadable');
    };
    const stage = await readSchema();
    if (stage === 'current-v5') return;
    if (
      stage === 'prior-v1' ||
      stage === 'prior-v2' ||
      stage === 'unversioned-v2'
    ) {
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
          ...(stage === 'prior-v1' ? REMOTE_STEP_RESULT_COLUMNS : []).map(
            (column) =>
              this.#db.prepare(
                `ALTER TABLE ${REMOTE_STEP_TABLE} ADD COLUMN ${column} ${column === 'executor_quiescent_at' ? 'INTEGER' : 'TEXT'}`,
              ),
          ),
          ...REMOTE_STEP_SETTLEMENT_COLUMNS.map((column) =>
            this.#db.prepare(
              `ALTER TABLE ${REMOTE_STEP_TABLE} ADD COLUMN ${column} ${REMOTE_STEP_INTEGER_COLUMNS.has(column) ? 'INTEGER' : 'TEXT'}`,
            ),
          ),
          this.#db.prepare(REMOTE_STEP_RECOVERY_INDEX_SQL),
          this.#db.prepare(
            stage === 'prior-v2'
              ? `UPDATE ${REMOTE_STEP_SCHEMA_TABLE} SET version = 3 WHERE id = 1 AND version = 2`
              : `INSERT INTO ${REMOTE_STEP_SCHEMA_TABLE} (id, version) VALUES (1, 3)`,
          ),
        ]);
      } catch (error) {
        const current = await readSchema();
        if (
          current !== 'prior-v3' &&
          current !== 'current-v4' &&
          current !== 'current-v5'
        )
          throw error;
      }
      return this.initialize();
    }
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
    if (stage === 'unversioned-v3') {
      try {
        await transactional.batch([
          this.#db.prepare(REMOTE_STEP_RECOVERY_INDEX_SQL),
          this.#db.prepare(
            `INSERT INTO ${REMOTE_STEP_SCHEMA_TABLE} (id, version) VALUES (1, 3)`,
          ),
        ]);
      } catch (error) {
        if ((await readSchema()) !== 'prior-v3') throw error;
      }
      return this.initialize();
    }
    if (stage === 'prior-v3') {
      const replacement = `${REMOTE_STEP_TABLE}_v4`;
      try {
        await transactional.batch([
          this.#db.prepare(remoteStepTableSql(replacement, 4)),
          this.#db.prepare(`INSERT INTO ${replacement} (${REMOTE_STEP_COLUMNS.join(', ')})
            SELECT ${REMOTE_STEP_COLUMNS.slice(0, -4).join(', ')}, 'agent', '${REMOTE_STEP_MARKER}', NULL, 0 FROM ${REMOTE_STEP_TABLE}`),
          this.#db.prepare(`DROP TABLE ${REMOTE_STEP_TABLE}`),
          this.#db.prepare(
            `ALTER TABLE ${replacement} RENAME TO ${REMOTE_STEP_TABLE}`,
          ),
          this.#db.prepare(REMOTE_STEP_RECOVERY_INDEX_SQL),
          this.#db.prepare(REMOTE_STEP_AGENT_INDEX_SQL),
          this.#db.prepare(REMOTE_STEP_WORKFLOW_INDEX_SQL),
          this.#db.prepare(REMOTE_STEP_WRITE_TRIGGER_SQL),
          this.#db.prepare(
            `UPDATE ${REMOTE_STEP_SCHEMA_TABLE} SET version = 4 WHERE id = 1 AND version = 3`,
          ),
        ]);
      } catch (error) {
        if (!['current-v4', 'current-v5'].includes(await readSchema()))
          throw error;
      }
      return this.initialize();
    }
    if (stage === 'unversioned-v4') {
      try {
        await transactional.batch([
          this.#db.prepare(REMOTE_STEP_RECOVERY_INDEX_SQL),
          this.#db.prepare(REMOTE_STEP_AGENT_INDEX_SQL),
          this.#db.prepare(REMOTE_STEP_WORKFLOW_INDEX_SQL),
          this.#db.prepare(REMOTE_STEP_WRITE_TRIGGER_SQL),
          this.#db.prepare(
            `INSERT INTO ${REMOTE_STEP_SCHEMA_TABLE} (id, version) VALUES (1, 4)`,
          ),
        ]);
      } catch (error) {
        if (!['current-v4', 'current-v5'].includes(await readSchema()))
          throw error;
      }
      return this.initialize();
    }
    if (stage === 'current-v4') {
      const replacement = `${REMOTE_STEP_TABLE}_v5`;
      try {
        await transactional.batch([
          this.#db.prepare(remoteStepTableSql(replacement)),
          this.#db.prepare(`INSERT INTO ${replacement} (${REMOTE_STEP_V5_COLUMNS.join(', ')})
            SELECT ${REMOTE_STEP_COLUMNS.join(', ')}, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
            FROM ${REMOTE_STEP_TABLE}`),
          this.#db.prepare(`DROP TABLE ${REMOTE_STEP_TABLE}`),
          this.#db.prepare(
            `ALTER TABLE ${replacement} RENAME TO ${REMOTE_STEP_TABLE}`,
          ),
          this.#db.prepare(REMOTE_STEP_RECOVERY_INDEX_SQL),
          this.#db.prepare(REMOTE_STEP_AGENT_INDEX_SQL),
          this.#db.prepare(REMOTE_STEP_WORKFLOW_INDEX_SQL),
          this.#db.prepare(REMOTE_STEP_OPERATION_INDEX_SQL),
          this.#db.prepare(REMOTE_STEP_V5_WRITE_TRIGGER_SQL),
          this.#db.prepare(
            `UPDATE ${REMOTE_STEP_SCHEMA_TABLE} SET version = 5 WHERE id = 1 AND version = 4`,
          ),
        ]);
      } catch (error) {
        if ((await readSchema()) !== 'current-v5') throw error;
      }
      return;
    }
    if (stage === 'unversioned-v5') {
      try {
        await transactional.batch([
          this.#db.prepare(REMOTE_STEP_RECOVERY_INDEX_SQL),
          this.#db.prepare(REMOTE_STEP_AGENT_INDEX_SQL),
          this.#db.prepare(REMOTE_STEP_WORKFLOW_INDEX_SQL),
          this.#db.prepare(REMOTE_STEP_OPERATION_INDEX_SQL),
          this.#db.prepare(REMOTE_STEP_V5_WRITE_TRIGGER_SQL),
          this.#db.prepare(
            `INSERT INTO ${REMOTE_STEP_SCHEMA_TABLE} (id, version) VALUES (1, 5)`,
          ),
        ]);
      } catch (error) {
        if ((await readSchema()) !== 'current-v5') throw error;
      }
      return;
    }
    throw new RemoteStepRefusal(503, 'remote step schema is unreadable');
  }

  async #snapshot(
    address: RemoteStepAddress | WorkflowRemoteStepAddress,
  ): Promise<string> {
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

  async #current(address: RemoteStepAddress | WorkflowRemoteStepAddress) {
    const summary = await this.#runtime.authoritativeStatus(
      address.workflowId,
      address.runId,
    );
    if (!summary) throw new RemoteStepRefusal(404, 'run not found');
    return 'targetKind' in address
      ? selectedWorkflowSuspension(summary)
      : selectedSuspension(summary, this.#allowedToolIds);
  }

  async #stableSnapshot(
    address: RemoteStepAddress | WorkflowRemoteStepAddress,
  ) {
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
        request_json, deadline_at, status, target_kind, suspension_marker, write_generation)
        SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 'agent', '${REMOTE_STEP_MARKER}', 1
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
        SET deadline_at = MIN(deadline_at, ?), write_revision = write_revision + 1, write_generation = write_generation + 1
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

  async registerWorkflow(
    address: WorkflowRemoteStepAddress,
    hostDeadlineAt: number,
    originalPrincipal: ExecutionPrincipal,
  ): Promise<WorkflowRemoteStepInvocation> {
    if (
      address.targetKind !== 'workflow' ||
      address.deploymentId !== this.#deploymentId ||
      address.tablePrefix !== this.#tablePrefix ||
      !isPathSafeId(address.workflowId) ||
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
    const requestDigest = await sha256Hex(suspension.requestJson);
    const stepPath = JSON.stringify(suspension.stepPath);
    const invocationId = crypto.randomUUID();
    await this.#db
      .prepare(`INSERT INTO ${REMOTE_STEP_TABLE} (
        invocation_id, principal_json, start_token, deployment_id, table_prefix,
        workflow_id, run_id, step_path, suspended_at, resume_count, request_json,
        deadline_at, status, target_kind, suspension_marker, request_digest, write_generation)
        SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 'workflow',
          '${WORKFLOW_REMOTE_STEP_MARKER}', ?, 1
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
        address.runId,
        stepPath,
        suspension.suspendedAt,
        suspension.resumeCount ?? -1,
        suspension.requestJson,
        deadlineAt,
        requestDigest,
        address.workflowId,
        address.runId,
        snapshot,
      )
      .run();
    const row = rowResult(
      await this.#db
        .prepare(`UPDATE ${REMOTE_STEP_TABLE}
          SET deadline_at = MIN(deadline_at, ?), write_revision = write_revision + 1, write_generation = write_generation + 1
          WHERE target_kind = 'workflow' AND deployment_id = ?
            AND table_prefix = ? AND workflow_id = ? AND run_id = ?
            AND step_path = ? AND suspended_at = ? AND resume_count = ?
            AND request_json = ? AND request_digest = ?
            AND principal_json = ? AND start_token = ?
            AND EXISTS (SELECT 1 FROM "${address.tablePrefix}mastra_workflow_snapshot"
              WHERE workflow_name = ? AND run_id = ? AND snapshot = ?
                AND ${SNAPSHOT_EXECUTION_OPEN_SQL})
          RETURNING *`)
        .bind(
          deadlineAt,
          address.deploymentId,
          address.tablePrefix,
          address.workflowId,
          address.runId,
          stepPath,
          suspension.suspendedAt,
          suspension.resumeCount ?? -1,
          suspension.requestJson,
          requestDigest,
          JSON.stringify(principal),
          execution.execution.startToken,
          address.workflowId,
          address.runId,
          snapshot,
        )
        .all(),
    );
    if (!row)
      throw new RemoteStepRefusal(
        409,
        'remote step registration did not settle',
      );
    return decodeWorkflow(row);
  }

  async get(invocationId: string): Promise<RemoteStepInvocation | undefined> {
    const row = await this.#getRow(invocationId);
    return row?.target_kind === 'agent' ? decode(row) : undefined;
  }

  async getWorkflow(
    invocationId: string,
  ): Promise<WorkflowRemoteStepInvocation | undefined> {
    const row = await this.#getRow(invocationId);
    return row?.target_kind === 'workflow' ? decodeWorkflow(row) : undefined;
  }

  async #getRow(invocationId: string): Promise<JournalRow | undefined> {
    if (!isPathSafeId(invocationId))
      throw new RemoteStepRefusal(400, 'invocation id is malformed');
    const row = rowResult(
      await this.#db
        .prepare(`SELECT * FROM ${REMOTE_STEP_TABLE} WHERE invocation_id = ?`)
        .bind(invocationId)
        .all(),
    );
    return row;
  }

  async #getAny(
    invocationId: string,
  ): Promise<RemoteStepInvocation | WorkflowRemoteStepInvocation | undefined> {
    const row = await this.#getRow(invocationId);
    return row ? decodeAny(row) : undefined;
  }

  async readDispatchRequestJson(invocationId: string): Promise<string> {
    return this.#readDispatchRequestJson(invocationId, 'agent');
  }

  async readWorkflowDispatchRequestJson(invocationId: string): Promise<string> {
    return this.#readDispatchRequestJson(invocationId, 'workflow');
  }

  async #readDispatchRequestJson(
    invocationId: string,
    targetKind: 'agent' | 'workflow',
  ): Promise<string> {
    if (!isPathSafeId(invocationId))
      throw new RemoteStepRefusal(400, 'invocation id is malformed');
    const row = rowResult(
      await this.#db
        .prepare(`SELECT * FROM ${REMOTE_STEP_TABLE} WHERE invocation_id = ?`)
        .bind(invocationId)
        .all(),
    );
    if (!row) throw new RemoteStepRefusal(404, 'invocation not found');
    if (row.target_kind !== targetKind)
      throw new RemoteStepRefusal(404, 'invocation not found');
    decodeAny(row);
    return row.request_json;
  }

  async #execution(
    address: RemoteStepAddress | WorkflowRemoteStepAddress,
    principal?: ExecutionPrincipal,
  ) {
    const state = await this.#runtime.authoritativeStartState(
      address.workflowId,
      address.runId,
    );
    const target = state?.provenance.startIdentity?.target;
    if (state?.storage !== 'd1' || !target)
      throw new RemoteStepRefusal(
        503,
        'remote step run identity is unreadable',
      );
    if (
      (principal !== undefined &&
        (state.provenance.startIdentity?.owner.kind !== principal.kind ||
          state.provenance.startIdentity.owner.id !== principal.id)) ||
      state.execution.tablePrefix !== address.tablePrefix ||
      state.execution.workflowId !== address.workflowId ||
      state.execution.runId !== address.runId
    )
      throw new RemoteStepRefusal(409, 'remote step run identity changed');
    if ('targetKind' in address) {
      if (
        target.kind !== 'workflow' ||
        target.id !== address.workflowId ||
        state.provenance.agentStart !== undefined
      )
        throw new RemoteStepRefusal(409, 'remote step run identity changed');
    } else if (target.kind !== 'agent' || !isPathSafeId(target.resourceId)) {
      throw new RemoteStepRefusal(
        503,
        'remote step run identity is unreadable',
      );
    } else if (
      target.id !== address.agentId ||
      target.threadId !== address.threadId ||
      address.resourceId !== target.resourceId
    )
      throw new RemoteStepRefusal(409, 'remote step run identity changed');
    return state;
  }

  async #admission(
    invocation: RemoteStepInvocation | WorkflowRemoteStepInvocation,
    priorBindings: number,
  ) {
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
    const invocation = await this.#getAny(invocationId);
    if (!invocation) throw new RemoteStepRefusal(404, 'invocation not found');
    if ('targetKind' in invocation && !this.#verifyWorkflowCompletion)
      throw new RemoteStepRefusal(
        503,
        'workflow remote step completion is unavailable',
      );
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
        JSON.stringify(invocation.stepPath) ||
      ('targetKind' in invocation &&
        (await sha256Hex(suspension.requestJson)) !== invocation.requestDigest)
    )
      throw new RemoteStepRefusal(409, 'remote step suspension changed');
    const gate = await this.#admission(invocation, 13);
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
        permit_digest = NULL, write_revision = write_revision + 1, write_generation = write_generation + 1
        WHERE invocation_id = ? AND permit_generation = ? AND deadline_at > ?
          AND status IN ('pending', 'dispatched')
          AND permit_consumed_at IS NULL AND request_json = ?
          AND target_kind = ? AND suspension_marker = ?
          AND request_digest IS ?
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
        'targetKind' in invocation ? 'workflow' : 'agent',
        'targetKind' in invocation
          ? WORKFLOW_REMOTE_STEP_MARKER
          : REMOTE_STEP_MARKER,
        'targetKind' in invocation ? invocation.requestDigest : null,
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
    const invocation = await this.#getAny(permit.invocationId);
    if (!invocation) throw new RemoteStepRefusal(404, 'invocation not found');
    if ('targetKind' in invocation && !this.#verifyWorkflowCompletion)
      throw new RemoteStepRefusal(
        503,
        'workflow remote step completion is unavailable',
      );
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
        JSON.stringify(invocation.stepPath) ||
      ('targetKind' in invocation &&
        (await sha256Hex(suspension.requestJson)) !== invocation.requestDigest)
    )
      throw new RemoteStepRefusal(409, 'remote step suspension changed');
    const rawPermit = rawDispatchPermit();
    const permitDigest = await sha256Hex(rawPermit);
    const gate = await this.#admission(invocation, 16);
    const result = await this.#db
      .prepare(`UPDATE ${REMOTE_STEP_TABLE}
        SET permit_consumed_at = ?, permit_digest = ?, write_revision = write_revision + 1, write_generation = write_generation + 1
        WHERE invocation_id = ? AND permit_generation = ?
          AND permit_epoch = ? AND permit_revision = ?
          AND permit_expires_at = ? AND permit_expires_at > ?
          AND deadline_at > ? AND permit_consumed_at IS NULL
          AND status = 'dispatched' AND request_json = ?
          AND target_kind = ? AND suspension_marker = ?
          AND request_digest IS ?
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
        'targetKind' in invocation ? 'workflow' : 'agent',
        'targetKind' in invocation
          ? WORKFLOW_REMOTE_STEP_MARKER
          : REMOTE_STEP_MARKER,
        'targetKind' in invocation ? invocation.requestDigest : null,
        invocation.workflowId,
        invocation.runId,
        snapshot,
        ...gate.values,
      )
      .all();
    if (rowResult(result)) return { kind: 'accepted', rawPermit };
    const after = await this.#getAny(permit.invocationId);
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
    const invocation = decodeAny(stored);
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
    if ('targetKind' in invocation) {
      if (!this.#verifyWorkflowCompletion)
        throw new RemoteStepRefusal(
          503,
          'workflow remote step completion is unavailable',
        );
      await this.#verifyWorkflowCompletion({
        invocation,
        rawPermit,
        resultJson,
        evidenceJson,
      });
    } else {
      await this.#verifyCompletion({
        invocation,
        rawPermit,
        resultJson,
        evidenceJson,
      });
    }
    const result = await this.#db
      .prepare(`UPDATE ${REMOTE_STEP_TABLE}
        SET result_json = ?, result_digest = ?, quiescence_digest = ?,
            executor_quiescent_at = ?, status = 'result-retained',
            write_revision = write_revision + 1, write_generation = write_generation + 1
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
    const row = await this.#getRow(invocationId);
    if (!row) return undefined;
    decodeAny(row);
    return this.#retainedResultFromRow(row);
  }

  async #retainedResultFromRow(
    row: JournalRow,
  ): Promise<RecordedRemoteStepResult | undefined> {
    const resultJson = row.result_json;
    const resultDigest = row.result_digest;
    const quiescenceDigest = row.quiescence_digest;
    const executorQuiescentAt = row.executor_quiescent_at;
    if (resultJson === null) return undefined;
    if (row.status === 'settled' || row.status === 'failed') return undefined;
    if (
      !['result-retained', 'resuming'].includes(row.status) ||
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
      invocationId: row.invocation_id,
      resultJson,
      resultDigest,
      quiescenceDigest,
      executorQuiescentAt,
    };
  }

  async readNotificationOperation(
    invocationId: string,
  ): Promise<RemoteStepNotificationOperation | undefined> {
    const row = await this.#getRow(invocationId);
    if (!row) return undefined;
    decodeAny(row);
    return this.#notificationOperationFromRow(row);
  }

  async #notificationOperationFromRow(
    row: JournalRow,
  ): Promise<RemoteStepNotificationOperation | undefined> {
    const operation = decodeNotificationOperation(row);
    if (
      operation &&
      (await sha256Hex(operation.projectionJson)) !== operation.requestDigest
    )
      throw new RemoteStepRefusal(503, 'remote step operation is unreadable');
    return operation;
  }

  async prepareNotificationOperation(
    invocationId: string,
    operationId: string,
    input: Omit<NotificationOperationInput, 'id'>,
  ): Promise<RemoteStepNotificationOperation> {
    if (!isPathSafeId(operationId))
      throw new RemoteStepRefusal(400, 'remote step operation id is malformed');
    const stored = await this.#getRow(invocationId);
    const invocation =
      stored?.target_kind === 'agent' ? decode(stored) : undefined;
    if (stored && !invocation) decodeAny(stored);
    const retained = stored
      ? await this.#retainedResultFromRow(stored)
      : undefined;
    if (
      !stored ||
      !invocation ||
      invocation.status !== 'result-retained' ||
      !retained
    )
      throw new RemoteStepRefusal(409, 'remote step result is unavailable');
    const operationKey = await sha256Hex(
      JSON.stringify([
        invocation.deploymentId,
        invocation.invocationId,
        invocation.startToken,
        invocation.stepPath,
        invocation.suspendedAt,
        invocation.resumeCount ?? -1,
        operationId,
      ]),
    );
    const notificationId = operationKey;
    const request = { ...input, id: notificationId };
    const projection = await projectNotificationOperation(request);
    const requestJson = JSON.stringify(request);
    if (
      new TextEncoder().encode(requestJson).length >
        MAX_REMOTE_STEP_REQUEST_BYTES ||
      new TextEncoder().encode(projection.requestJson).length >
        MAX_REMOTE_STEP_REQUEST_BYTES
    )
      throw new RemoteStepRefusal(
        400,
        'remote step operation exceeds its limit',
      );
    const existing = await this.#notificationOperationFromRow(stored);
    if (existing) {
      if (
        existing.operationKey !== operationKey ||
        existing.requestJson !== requestJson ||
        existing.projectionJson !== projection.requestJson ||
        existing.requestDigest !== projection.requestDigest
      )
        throw new RemoteStepRefusal(409, 'remote step operation changed');
      return existing;
    }
    const result = rowResult(
      await this.#db
        .prepare(`UPDATE ${REMOTE_STEP_TABLE}
      SET operation_key = ?, operation_id = ?, notification_id = ?,
        notification_request_json = ?, notification_projection_version = ?,
        notification_projection_json = ?, notification_request_digest = ?,
        operation_state = 'prepared',
        write_revision = write_revision + 1, write_generation = write_generation + 1
      WHERE invocation_id = ? AND target_kind = 'agent'
        AND status = 'result-retained' AND operation_state IS NULL
        AND principal_json = ? AND start_token = ? AND result_digest = ?
        AND write_revision = ? AND write_generation = ?
      RETURNING *`)
        .bind(
          operationKey,
          operationId,
          notificationId,
          requestJson,
          projection.version,
          projection.requestJson,
          projection.requestDigest,
          invocationId,
          JSON.stringify(invocation.principal),
          invocation.startToken,
          retained.resultDigest,
          stored.write_revision,
          stored.write_generation,
        )
        .all(),
    );
    if (result)
      return decodeNotificationOperation(
        result,
      ) as RemoteStepNotificationOperation;
    const after = await this.readNotificationOperation(invocationId);
    if (
      after?.operationKey === operationKey &&
      after.requestDigest === projection.requestDigest
    )
      return after;
    throw new RemoteStepRefusal(409, 'remote step operation changed');
  }

  async admitNotificationEffect(
    invocationId: string,
  ): Promise<RemoteStepNotificationOperation> {
    const stored = await this.#getRow(invocationId);
    if (!stored) throw new RemoteStepRefusal(404, 'invocation not found');
    const invocation = decode(stored);
    const operation = await this.#notificationOperationFromRow(stored);
    if (
      !operation ||
      !['prepared', 'effect-admitted'].includes(operation.state)
    )
      throw new RemoteStepRefusal(409, 'remote step operation is unavailable');
    if (operation.state === 'effect-admitted') return operation;
    const retained = await this.#retainedResultFromRow(stored);
    if (!retained || invocation.status !== 'result-retained')
      throw new RemoteStepRefusal(409, 'remote step result is unavailable');
    const { suspension, snapshot } = await this.#stableSnapshot(invocation);
    if (
      suspension.suspendedAt !== invocation.suspendedAt ||
      suspension.resumeCount !== invocation.resumeCount ||
      suspension.requestJson !== JSON.stringify(invocation.request) ||
      JSON.stringify(suspension.stepPath) !==
        JSON.stringify(invocation.stepPath)
    )
      throw new RemoteStepRefusal(409, 'remote step suspension changed');
    const now = this.#now();
    const base = [
      now,
      invocationId,
      invocation.deploymentId,
      invocation.tablePrefix,
      invocation.workflowId,
      invocation.agentId,
      invocation.threadId,
      invocation.resourceId,
      invocation.runId,
      invocation.startToken,
      JSON.stringify(invocation.principal),
      retained.resultDigest,
      operation.operationKey,
      operation.requestDigest,
      stored.write_revision,
      stored.write_generation,
      now,
      invocation.workflowId,
      invocation.runId,
      snapshot,
    ];
    const gate = await this.#admission(invocation, base.length);
    try {
      const row = rowResult(
        await this.#db
          .prepare(`UPDATE ${REMOTE_STEP_TABLE}
        SET operation_state = 'effect-admitted', effect_admitted_at = ?,
          write_revision = write_revision + 1, write_generation = write_generation + 1
        WHERE invocation_id = ? AND deployment_id = ? AND table_prefix = ?
          AND workflow_id = ? AND agent_id = ? AND thread_id = ?
          AND resource_id = ? AND run_id = ? AND start_token = ?
          AND principal_json = ? AND result_digest = ?
          AND operation_key = ? AND notification_request_digest = ?
          AND write_revision = ? AND write_generation = ? AND deadline_at > ?
          AND target_kind = 'agent' AND status = 'result-retained'
          AND operation_state = 'prepared'
          AND EXISTS (SELECT 1 FROM "${invocation.tablePrefix}mastra_workflow_snapshot"
            WHERE workflow_name = ? AND run_id = ? AND snapshot = ?
              AND ${SNAPSHOT_EXECUTION_OPEN_SQL})
          AND ${gate.predicate} RETURNING *`)
          .bind(...base, ...gate.values)
          .all(),
      );
      if (row)
        return decodeNotificationOperation(
          row,
        ) as RemoteStepNotificationOperation;
    } catch (error) {
      const after = await this.readNotificationOperation(invocationId);
      if (
        after?.state === 'effect-admitted' &&
        after.operationKey === operation.operationKey &&
        after.requestDigest === operation.requestDigest
      )
        return after;
      throw error;
    }
    const after = await this.readNotificationOperation(invocationId);
    if (
      after?.state === 'effect-admitted' &&
      after.operationKey === operation.operationKey &&
      after.requestDigest === operation.requestDigest
    )
      return after;
    throw new RemoteStepRefusal(409, 'remote step effect admission changed');
  }

  async recordNotificationReceipt(
    invocationId: string,
    receipt: {
      operationKey: string;
      notificationId: string;
      threadId: string;
      requestDigest: string;
    },
  ): Promise<RemoteStepNotificationOperation> {
    const stored = await this.#getRow(invocationId);
    if (!stored) throw new RemoteStepRefusal(404, 'invocation not found');
    decode(stored);
    const operation = await this.#notificationOperationFromRow(stored);
    if (
      operation?.state !== 'effect-admitted' ||
      receipt.operationKey !== operation.operationKey ||
      receipt.notificationId !== operation.notificationId ||
      receipt.threadId !== stored.thread_id ||
      receipt.requestDigest !== operation.requestDigest
    )
      throw new RemoteStepRefusal(
        409,
        'remote step notification receipt changed',
      );
    const row = rowResult(
      await this.#db
        .prepare(`UPDATE ${REMOTE_STEP_TABLE}
      SET operation_state = 'receipt-recorded', receipt_recorded_at = ?,
        write_revision = write_revision + 1, write_generation = write_generation + 1
      WHERE invocation_id = ? AND status = 'result-retained'
        AND operation_state = 'effect-admitted' AND operation_key = ?
        AND notification_id = ? AND notification_request_digest = ?
        AND result_digest = ? AND write_revision = ? AND write_generation = ?
        AND EXISTS (SELECT 1 FROM "${stored.table_prefix}${NOTIFICATION_OPERATION_RECEIPTS_TABLE}"
          WHERE operation_key = ? AND request_digest = ?
            AND thread_id = ? AND notification_id = ?)
      RETURNING *`)
        .bind(
          this.#now(),
          invocationId,
          operation.operationKey,
          operation.notificationId,
          operation.requestDigest,
          stored.result_digest,
          stored.write_revision,
          stored.write_generation,
          operation.operationKey,
          operation.requestDigest,
          stored.thread_id,
          operation.notificationId,
        )
        .all(),
    );
    if (row)
      return decodeNotificationOperation(
        row,
      ) as RemoteStepNotificationOperation;
    throw new RemoteStepRefusal(
      409,
      'remote step notification receipt changed',
    );
  }

  async admitNotificationResume(invocationId: string): Promise<string> {
    const stored = await this.#getRow(invocationId);
    if (!stored) throw new RemoteStepRefusal(404, 'invocation not found');
    const invocation = decode(stored);
    const operation = await this.#notificationOperationFromRow(stored);
    if (
      operation?.state !== 'receipt-recorded' ||
      invocation.status !== 'result-retained'
    )
      throw new RemoteStepRefusal(
        409,
        'remote step continuation is unavailable',
      );
    const retained = await this.#retainedResultFromRow(stored);
    if (!retained)
      throw new RemoteStepRefusal(409, 'remote step result is unavailable');
    const { suspension, snapshot } = await this.#stableSnapshot(invocation);
    if (
      suspension.suspendedAt !== invocation.suspendedAt ||
      suspension.resumeCount !== invocation.resumeCount ||
      suspension.requestJson !== JSON.stringify(invocation.request) ||
      JSON.stringify(suspension.stepPath) !==
        JSON.stringify(invocation.stepPath)
    )
      throw new RemoteStepRefusal(409, 'remote step suspension changed');
    const now = this.#now();
    const owner = crypto.randomUUID();
    const base = [
      owner,
      now + 60_000,
      now,
      invocationId,
      invocation.deploymentId,
      invocation.tablePrefix,
      invocation.workflowId,
      invocation.agentId,
      invocation.threadId,
      invocation.resourceId,
      invocation.runId,
      invocation.startToken,
      JSON.stringify(invocation.principal),
      retained.resultDigest,
      operation.operationKey,
      operation.requestDigest,
      stored.write_revision,
      stored.write_generation,
      now,
      invocation.workflowId,
      invocation.runId,
      snapshot,
    ];
    const gate = await this.#admission(invocation, base.length);
    const row = rowResult(
      await this.#db
        .prepare(`UPDATE ${REMOTE_STEP_TABLE}
      SET status = 'resuming', resume_lease_owner = ?, resume_lease_expires_at = ?,
        operation_state = 'resume-admitted', resume_admitted_at = ?,
        write_revision = write_revision + 1, write_generation = write_generation + 1
      WHERE invocation_id = ? AND deployment_id = ? AND table_prefix = ?
        AND workflow_id = ? AND agent_id = ? AND thread_id = ?
        AND resource_id = ? AND run_id = ? AND start_token = ?
        AND principal_json = ? AND result_digest = ? AND operation_key = ?
        AND notification_request_digest = ? AND write_revision = ?
        AND write_generation = ? AND deadline_at > ?
        AND operation_state = 'receipt-recorded' AND status = 'result-retained'
        AND receipt_recorded_at IS NOT NULL AND resume_admitted_at IS NULL
        AND EXISTS (SELECT 1 FROM "${invocation.tablePrefix}mastra_workflow_snapshot"
          WHERE workflow_name = ? AND run_id = ? AND snapshot = ?
            AND ${SNAPSHOT_EXECUTION_OPEN_SQL})
        AND ${gate.predicate} RETURNING invocation_id`)
        .bind(...base, ...gate.values)
        .all(),
    );
    if (!row)
      throw new RemoteStepRefusal(409, 'remote step continuation changed');
    return owner;
  }

  async claimResume(invocationId: string, leaseMs: number): Promise<string> {
    if (
      !isPathSafeId(invocationId) ||
      !Number.isSafeInteger(leaseMs) ||
      leaseMs < 1 ||
      leaseMs > 60_000
    )
      throw new RemoteStepRefusal(400, 'remote step resume claim is malformed');
    const invocation = await this.get(invocationId);
    if (!invocation) throw new RemoteStepRefusal(404, 'invocation not found');
    if (invocation.status !== 'result-retained')
      throw new RemoteStepRefusal(409, 'remote step result is unavailable');
    const start = await this.#execution(invocation, invocation.principal);
    if (start.execution.startToken !== invocation.startToken)
      throw new RemoteStepRefusal(409, 'remote step run generation changed');
    if (!(await this.readRetainedResult(invocationId)))
      throw new RemoteStepRefusal(409, 'remote step result is unavailable');
    const now = this.#now();
    const owner = crypto.randomUUID();
    const result = await this.#db
      .prepare(`UPDATE ${REMOTE_STEP_TABLE}
        SET status = 'resuming', resume_lease_owner = ?, resume_lease_expires_at = ?,
            write_revision = write_revision + 1, write_generation = write_generation + 1
        WHERE invocation_id = ? AND operation_state IS NULL
          AND deployment_id = ? AND table_prefix = ?
          AND workflow_id = ? AND agent_id = ? AND thread_id = ?
          AND resource_id = ? AND run_id = ? AND start_token = ?
          AND permit_generation = ? AND permit_epoch IS ?
          AND result_digest IS NOT NULL
          AND status = 'result-retained'
        RETURNING invocation_id`)
      .bind(
        owner,
        now + leaseMs,
        invocationId,
        invocation.deploymentId,
        invocation.tablePrefix,
        invocation.workflowId,
        invocation.agentId,
        invocation.threadId,
        invocation.resourceId,
        invocation.runId,
        invocation.startToken,
        invocation.permitGeneration,
        invocation.permitEpoch ?? null,
      )
      .all();
    if (!rowResult(result))
      throw new RemoteStepRefusal(409, 'remote step resume is claimed');
    return owner;
  }

  async settleAfterResume(
    invocationId: string,
    leaseOwner: string,
    witnessed?: RunSummary,
  ): Promise<
    | 'retry'
    | 'applied'
    | 'terminal-not-success'
    | 'deadline-expired'
    | 'advanced-without-proof'
  > {
    if (!isPathSafeId(invocationId) || !isPathSafeId(leaseOwner))
      throw new RemoteStepRefusal(400, 'remote step resume claim is malformed');
    const invocation = await this.get(invocationId);
    if (invocation?.status !== 'resuming')
      throw new RemoteStepRefusal(409, 'remote step resume claim changed');
    const start = await this.#execution(invocation, invocation.principal);
    if (start.execution.startToken !== invocation.startToken)
      throw new RemoteStepRefusal(409, 'remote step run generation changed');
    const current = await this.#runtime.authoritativeStatus(
      invocation.workflowId,
      invocation.runId,
    );
    if (!current || current.runId !== invocation.runId)
      throw new RemoteStepRefusal(503, 'remote step outcome is unreadable');
    const same = (summary: RunSummary) => {
      if (summary.status !== 'suspended') return false;
      const key = invocation.stepPath.join('.');
      const pathPresent = summary.suspended?.some(
        (path) => JSON.stringify(path) === JSON.stringify(invocation.stepPath),
      );
      const suspended = record(record(summary.suspendPayload)?.[key]);
      const tool = record(suspended?.toolCallSuspended);
      return (
        pathPresent === true &&
        summary.suspendedAt?.[key] === invocation.suspendedAt &&
        summary.resumeCount?.[key] === invocation.resumeCount &&
        suspended?.type === 'suspension' &&
        tool?.remoteStep === REMOTE_STEP_MARKER &&
        JSON.stringify(tool.request) === JSON.stringify(invocation.request)
      );
    };
    const sameObservedOutcome = (left: RunSummary, right: RunSummary) =>
      left.runId === right.runId &&
      left.status === right.status &&
      ((left.status === 'success' &&
        JSON.stringify(left.result) === JSON.stringify(right.result)) ||
        (left.status === 'suspended' &&
          JSON.stringify(left.suspended) === JSON.stringify(right.suspended) &&
          JSON.stringify(left.suspendedAt) ===
            JSON.stringify(right.suspendedAt) &&
          JSON.stringify(left.resumeCount) ===
            JSON.stringify(right.resumeCount) &&
          JSON.stringify(left.suspendPayload) ===
            JSON.stringify(right.suspendPayload)));
    let outcome:
      | 'retry'
      | 'applied'
      | 'terminal-not-success'
      | 'deadline-expired'
      | 'advanced-without-proof';
    if (same(current))
      outcome =
        this.#now() >= invocation.deadlineAt ? 'deadline-expired' : 'retry';
    else if (current.status === 'suspended' || current.status === 'success')
      outcome =
        witnessed && sameObservedOutcome(witnessed, current) && !same(witnessed)
          ? 'applied'
          : 'advanced-without-proof';
    else if (
      [
        'failed',
        'tripwire',
        'canceled',
        'bailed',
        'skipped',
        'cancelled',
        'timed_out',
      ].includes(current.status)
    )
      outcome = 'terminal-not-success';
    else
      throw new RemoteStepRefusal(503, 'remote step outcome is still changing');
    const result = await this.#db
      .prepare(`UPDATE ${REMOTE_STEP_TABLE}
        SET status = ?, resume_lease_owner = NULL, resume_lease_expires_at = NULL,
            settled_at = ?, settlement_outcome = ?, write_revision = write_revision + 1, write_generation = write_generation + 1
        WHERE invocation_id = ? AND status = 'resuming'
          AND resume_lease_owner = ? AND permit_generation = ?
          AND permit_epoch IS ? AND start_token = ?
          AND deployment_id = ? AND table_prefix = ? AND workflow_id = ?
          AND agent_id = ? AND thread_id = ? AND resource_id = ? AND run_id = ?
        RETURNING invocation_id`)
      .bind(
        outcome === 'retry'
          ? 'result-retained'
          : outcome === 'applied'
            ? 'settled'
            : 'failed',
        outcome === 'retry' ? null : this.#now(),
        outcome === 'retry' ? null : outcome,
        invocationId,
        leaseOwner,
        invocation.permitGeneration,
        invocation.permitEpoch ?? null,
        invocation.startToken,
        invocation.deploymentId,
        invocation.tablePrefix,
        invocation.workflowId,
        invocation.agentId,
        invocation.threadId,
        invocation.resourceId,
        invocation.runId,
      )
      .all();
    if (!rowResult(result))
      throw new RemoteStepRefusal(409, 'remote step resume claim changed');
    return outcome;
  }

  async nextNotificationRecoveryBatch(limit: number): Promise<string[]> {
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100)
      throw new RemoteStepRefusal(
        400,
        'notification recovery limit is malformed',
      );
    await this.initialize();
    const definition = record(
      await this.#db
        .prepare('SELECT sql FROM sqlite_schema WHERE type = ? AND name = ?')
        .bind('table', REMOTE_STEP_NOTIFICATION_CURSOR_TABLE)
        .all(),
    )?.results;
    if (
      !Array.isArray(definition) ||
      definition.length !== 1 ||
      record(definition[0])?.sql !== REMOTE_STEP_NOTIFICATION_CURSOR_SQL
    )
      throw new RemoteStepRefusal(
        503,
        'notification recovery cursor is unreadable',
      );
    await this.#db
      .prepare(`INSERT OR IGNORE INTO ${REMOTE_STEP_NOTIFICATION_CURSOR_TABLE}
        (deployment_id, table_prefix, version, after_invocation_id)
        VALUES (?, ?, 1, '')`)
      .bind(this.#deploymentId, this.#tablePrefix)
      .run();
    const cursorRows = record(
      await this.#db
        .prepare(`SELECT version, after_invocation_id
          FROM ${REMOTE_STEP_NOTIFICATION_CURSOR_TABLE}
          WHERE deployment_id = ? AND table_prefix = ?`)
        .bind(this.#deploymentId, this.#tablePrefix)
        .all(),
    )?.results;
    const cursor =
      Array.isArray(cursorRows) && cursorRows.length === 1
        ? record(cursorRows[0])
        : undefined;
    const after = cursor?.after_invocation_id;
    if (
      cursor?.version !== 1 ||
      typeof after !== 'string' ||
      (after !== '' && !isPathSafeId(after))
    )
      throw new RemoteStepRefusal(
        503,
        'notification recovery cursor is unreadable',
      );
    const scan = async (afterInvocationId: string) => {
      const values = record(
        await this.#db
          .prepare(`SELECT invocation_id FROM ${REMOTE_STEP_TABLE}
            INDEXED BY ${REMOTE_STEP_OPERATION_INDEX}
            WHERE invocation_id > ? AND deployment_id = ? AND table_prefix = ?
              AND target_kind = 'agent'
              AND status IN ('result-retained', 'resuming')
              AND operation_state IN ('prepared', 'effect-admitted',
                'receipt-recorded', 'resume-admitted', 'unresolved')
            ORDER BY invocation_id LIMIT ?`)
          .bind(afterInvocationId, this.#deploymentId, this.#tablePrefix, limit)
          .all(),
      )?.results;
      if (
        !Array.isArray(values) ||
        values.some((value) => !isPathSafeId(record(value)?.invocation_id))
      )
        throw new RemoteStepRefusal(
          503,
          'notification recovery scan is unreadable',
        );
      return values.map((value) => record(value)?.invocation_id as string);
    };
    let ids = await scan(after);
    if (ids.length === 0 && after !== '') ids = await scan('');
    const next = ids.at(-1) ?? '';
    const advanced = record(
      await this.#db
        .prepare(`UPDATE ${REMOTE_STEP_NOTIFICATION_CURSOR_TABLE}
          SET after_invocation_id = ?
          WHERE deployment_id = ? AND table_prefix = ? AND version = 1
            AND after_invocation_id = ?
          RETURNING after_invocation_id`)
        .bind(next, this.#deploymentId, this.#tablePrefix, after)
        .all(),
    )?.results;
    if (!Array.isArray(advanced) || advanced.length !== 1)
      throw new RemoteStepRefusal(409, 'notification recovery cursor changed');
    return ids;
  }

  async recoverableResults(
    limit: number,
    afterInvocationId = '',
  ): Promise<{ invocationIds: string[]; nextCursor?: string }> {
    if (
      !Number.isSafeInteger(limit) ||
      limit < 1 ||
      limit > 100 ||
      (afterInvocationId !== '' && !isPathSafeId(afterInvocationId))
    )
      throw new RemoteStepRefusal(
        400,
        'remote step recovery limit is malformed',
      );
    const now = this.#now();
    const rows = record(
      await this.#db
        .prepare(`SELECT invocation_id, status, resume_lease_expires_at
        FROM ${REMOTE_STEP_TABLE} INDEXED BY ${REMOTE_STEP_RECOVERY_INDEX}
        WHERE invocation_id > ?
          AND target_kind = 'agent'
          AND operation_state IS NULL
          AND status IN ('result-retained', 'resuming')
        ORDER BY invocation_id LIMIT ?`)
        .bind(afterInvocationId, limit)
        .all(),
    )?.results;
    if (
      !Array.isArray(rows) ||
      rows.some(
        (row) =>
          !isPathSafeId(record(row)?.invocation_id) ||
          !['result-retained', 'resuming'].includes(
            record(row)?.status as string,
          ) ||
          (record(row)?.status === 'resuming' &&
            !positiveTime(record(row)?.resume_lease_expires_at)),
      )
    )
      throw new RemoteStepRefusal(
        503,
        'remote step recovery scan is unreadable',
      );
    const invocationIds = rows
      .filter((row) => {
        const value = record(row);
        return (
          value?.status === 'result-retained' ||
          (value?.status === 'resuming' &&
            positiveTime(value.resume_lease_expires_at) &&
            value.resume_lease_expires_at <= now)
        );
      })
      .map((row) => record(row)?.invocation_id as string);
    const last = rows.at(-1);
    return {
      invocationIds,
      ...(rows.length === limit && last
        ? { nextCursor: record(last)?.invocation_id as string }
        : {}),
    };
  }

  async reconcileResult(
    invocationId: string,
  ): Promise<
    | 'retry'
    | 'applied'
    | 'terminal-not-success'
    | 'deadline-expired'
    | 'advanced-without-proof'
    | 'pending-quiescence'
  > {
    const invocation = await this.get(invocationId);
    if (!invocation) throw new RemoteStepRefusal(404, 'invocation not found');
    if (invocation.status === 'resuming') return 'pending-quiescence';
    const owner = await this.claimResume(invocationId, 60_000);
    return this.settleAfterResume(invocationId, owner);
  }
}
