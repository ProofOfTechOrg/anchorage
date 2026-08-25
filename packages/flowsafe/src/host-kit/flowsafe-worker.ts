// SPDX-License-Identifier: Apache-2.0
// createFlowsafeWorker — the whole production-Worker skeleton the deploy
// template and the showcase host previously carried as near-byte copies:
// the /healthz → routers → 404 fetch pipeline over one actor resolver, the
// alarm-driven maintenance (deadline, sweep, purge, and optional schedule tick
// never share an invocation). Hosts stay thin shells: they supply workflows,
// their identity seam (buildVerifier), and their deployment-specific hooks —
// preRoutes (extra unauthenticated/authenticated mounts), beforeStart/
// beforeResume (e.g. a budget charge), notify (reviewer-facing transport), and
// extraPurgeDuties (e.g. a host-specific purge). Everything here is structural:
// host-kit never imports @cloudflare/workers-types.

import type {
  ActorContext,
  ActorResolver,
  ApprovalAuditEvent,
  ApprovalDatabase,
  ApprovalNotificationSink,
  ApprovalService,
  ApprovalStore,
  ApprovalStreamSink,
  SelfDecisionPolicy,
} from '../approval-api/index.js';
import {
  APPROVAL_ROLES,
  createActorResolver,
  createApprovalRouter,
  createResourceOwnershipSchema,
  RESOURCE_OWNERSHIP_TABLE,
  trustAutomationPrincipal,
} from '../approval-api/index.js';
import {
  type AuditProxyNamespaceLike,
  type AuditQueue,
  createAuditProxyDurableObjectBinding,
  createAuditProxyQueue,
  type InfrastructureAuditEnvelope,
} from '../audit-export/index.js';
import { credentialsMatch } from '../do-runner/deployment-identity.js';
import type { DurableObjectRunLifecycleHooks } from '../do-runner/durable-object.js';
import type {
  DeploymentIdentityDatabase,
  ExecutionFenceStore,
  PurgeExpiredBackgroundTasksResult,
  RunArtifactPurger,
  RunDeadlineCursor,
  SnapshotDatabase,
  StartIdempotencyStore,
} from '../do-runner/index.js';
import {
  assertExecutionFenceState,
  DEPLOYMENT_IDENTITY_HEADER,
  DeploymentIdentityError,
  DeploymentInventory,
  DoStatusError,
  deploymentIdentityHeaders,
  ensureDeploymentIdentityBindings,
  executionFenceFor,
  executionFenceReadingPayload,
  InvalidInventoryRequestError,
  isInventoryCategory,
  purgeExpiredBackgroundTasks,
  purgeExpiredNotifications,
  purgeExpiredScheduleTriggers,
  purgeExpiredThreadState,
  purgeExpiredThreads,
  purgeExpiredWorkflowRuns,
  START_IDEMPOTENCY_TABLE,
  startIdempotencyFor,
  sweepExpiredRunDeadlines,
  verifyDurableObjectDeploymentIdentity,
  verifyDurableObjectDeploymentRequest,
} from '../do-runner/index.js';
import { validateTablePrefix } from '../do-runner/table-prefix.js';
import { readBoundedBody } from '../http-body.js';
import { abandonApprovalsForRun, type ResumeRunFn } from './approval-bridge.js';
import { bearerActorAuthenticator } from './bearer-auth.js';
import {
  createDoRunTopology,
  type DoRunTopology,
  type RunnerNamespaceLike,
} from './do-run-topology.js';
import {
  numberVar,
  optionalNumberVar,
  selfDecisionPolicyVar,
} from './env-vars.js';
import {
  approvalStoreFactoryFor,
  buildHostApprovalService,
  type MaintenanceOutcome,
  maintenancePrincipal,
  reconcileApprovalsOnStatusDetached,
  runApprovalRetentionPurge,
  runSlaSweepMaintenance,
} from './host-approval-service.js';
import { createHubTopology, type HubNamespaceLike } from './hub-topology.js';
import {
  MAINTENANCE_RECEIPT_HEADER,
  type MaintenanceCapabilityClaims,
  type MaintenanceCapabilityJwk,
  type MaintenanceCapabilityOperation,
  mintMaintenanceReceipt,
  verifyAsymmetricMaintenanceCapability,
  verifyMaintenanceCapability,
} from './maintenance-capability.js';
import { createRunRouter } from './run-router.js';
import {
  type StateEgressBinding,
  type StateEgressEnv,
  validateStateEgressEnv,
} from './state-egress.js';
import { createStreamRouter } from './stream-router.js';
import type { TokenVerifier } from './verifier.js';
import type { WorkflowMeta } from './workflow-meta.js';

/** The ExecutionContext subset the composer needs (structural, like the rest of host-kit). */
export interface FlowsafeWorkerContext {
  waitUntil(promise: Promise<unknown>): void;
}

/** Fixed instance name used by every deployment maintenance namespace. */
export const MAINTENANCE_INSTANCE_NAME = 'deployment-maintenance';

export interface MaintenanceStubLike {
  fetch(
    url: string,
    init?: { method?: string; headers?: Record<string, string> },
  ): Promise<Response>;
}

export interface MaintenanceNamespaceLike<Id = unknown> {
  idFromName(name: string): Id;
  get(id: Id): MaintenanceStubLike;
}

export interface MaintenanceHealth {
  nextDeadlineAt?: number;
  nextSweepAt: number;
  nextPurgeAt: number;
  nextTickAt?: number;
  lastSweepAt?: number;
  lastPurgeAt?: number;
  lastTickAt?: number;
  lastDeadlineAt?: number;
  lastSweepAttemptAt?: number;
  lastPurgeAttemptAt?: number;
  lastTickAttemptAt?: number;
  lastDeadlineAttemptAt?: number;
  lastSweepError?: string;
  lastPurgeError?: string;
  lastTickError?: string;
  lastDeadlineError?: string;
}

type InitializedMaintenanceHealth = MaintenanceHealth & {
  nextDeadlineAt: number;
  nextSweepAt: number;
  nextPurgeAt: number;
};

export interface MaintenanceStorage {
  get<T = unknown>(key: string): Promise<T | undefined>;
  getAlarm(): Promise<number | null>;
  setAlarm(scheduledTime: number | Date): Promise<void>;
  transaction<T>(
    closure: (transaction: MaintenanceStorageTransaction) => Promise<T>,
  ): Promise<T>;
}

export interface MaintenanceStorageTransaction {
  get<T = unknown>(key: string): Promise<T | undefined>;
  put<T>(key: string, value: T): Promise<void>;
  setAlarm(scheduledTime: number | Date): Promise<void>;
}

export interface MaintenanceDurableObjectState {
  readonly id: { readonly name?: string };
  readonly storage: MaintenanceStorage;
}

/**
 * The env bindings the composed Worker reads. A host's own Env extends this
 * (its workers-typed D1Database/DurableObjectNamespace/Queue bindings satisfy
 * these structural subsets) and adds whatever its hooks need.
 */
export interface FlowsafeWorkerEnv {
  /** D1: the approval store AND the Mastra snapshot table live here. */
  DB: ApprovalDatabase & SnapshotDatabase & DeploymentIdentityDatabase;
  /** Provisioning-stamped deployment tag; must match the D1 sentinel. */
  DEPLOYMENT_TENANT: string;
  /** Optional fleet release identity returned only by authenticated maintenance routes. */
  FLEET_SPEC_DIGEST?: string;
  /** Fleet-controlled marker: maintenance routes relay one-shot capabilities. */
  FLEET_MAINTENANCE_CAPABILITIES?: 'required';
  /** Fleet Ed25519 public JWK used before external maintenance execution. */
  FLEET_MAINTENANCE_CAPABILITY_PUBLIC_KEY?: string;
  /** Fleet-controlled marker: audit export must use the trusted state proxy. */
  FLEET_AUDIT_PROXY?: 'required';
  /** Fleet-controlled marker: this private Worker terminates audit proxy calls. */
  FLEET_AUDIT_PROXY_INGRESS?: 'required';
  /** Fleet-controlled environment attribution for trusted state. */
  FLEET_ENVIRONMENT?: string;
  /** Fleet-controlled logical release-family name for trusted audit attribution. */
  FLEET_DEPLOYMENT_SCRIPT?: string;
  /** Fleet resource group stamped onto trusted state. */
  FLEET_RESOURCE_GROUP?: string;
  /** Distinguishes the trusted state runtime from an external candidate. */
  FLEET_RESOURCE_ROLE?: 'platform-state';
  /** Per-deployment Worker-to-Durable-Object credential. */
  DEPLOYMENT_IDENTITY_SECRET: string;
  /** The runner DO namespace createDoRunTopology drives. */
  RUNNER: RunnerNamespaceLike;
  /** Fixed-name singleton that owns deployment maintenance alarms. */
  MAINTENANCE: MaintenanceNamespaceLike;
  /**
   * Optional deployment hub Durable Object namespace for live streaming.
   * Present together with STREAM_TICKET_SECRET => the composer mounts the stream
   * stage and fans approval mutations out to the deployment hub; either absent =>
   * streaming stays unmounted and the client remains poll-only.
   */
  HUB?: HubNamespaceLike;
  /** Dedicated stream-ticket signing secret. Absent means no streaming. */
  STREAM_TICKET_SECRET?: string;
  /** Default SLA seconds for new approvals (var; default 14400 = 4h). */
  APPROVAL_SLA_SECONDS?: string;
  /**
   * Separation-of-duties exemption (var). Unset or a `false` spelling = SoD on
   * (default); `true` = every decider may self-decide; a comma-separated role
   * list (e.g. `admin` or `admin,reviewer`) = only those roles. Any invalid
   * value falls back to OFF (fail closed).
   */
  APPROVAL_ALLOW_SELF_DECISION?: string;
  /** Maintenance purges terminal run snapshots older than this (default 30 days). */
  RUN_RETENTION_DAYS?: string;
  /**
   * How long a spent idempotency key stays answerable after its run settled —
   * the key-validity horizon (var; defaults to RUN_RETENTION_DAYS, and is
   * floored at it).
   *
   * Set this ABOVE run retention when callers may retry a start later than this
   * deployment keeps run summaries: until the horizon elapses such a retry is
   * told ALREADY_SETTLED, and after it the same key reads as brand new and
   * starts a second run.
   */
  START_IDEMPOTENCY_RETENTION_DAYS?: string;
  /** Maintenance purges DECIDED approval records older than this (default 30 days). */
  APPROVAL_RETENTION_DAYS?: string;
  /**
   * Agent-memory thread TTL in days (docs/agent-memory-isolation.md#thread-retention):
   * the purge duty deletes threads untouched for longer than this, with their
   * messages. UNSET, EMPTY, or INVALID => the duty does not run and no thread
   * ever expires — the opt-in default, because a thread is a conversation a host
   * means to keep, not a terminal run snapshot that is finished by definition.
   * There is no safe number to pick on an operator's behalf here, so anything
   * short of a number the operator actually named is answered by NOT deleting
   * (a config-error line marks the invalid case). Unlike RUN_RETENTION_DAYS this
   * var decides whether an irreversible delete happens at all, so it does not
   * take numberVar's fallback — see optionalNumberVar in env-vars.ts.
   */
  THREAD_RETENTION_DAYS?: string;
  /**
   * Agent-inbox TTL in days. The purge duty reaps terminal
   * `mastra_notifications` rows past this age (pending rows are never reaped —
   * one may await a future deliverAt). UNSET/EMPTY/INVALID => the duty does not
   * run and no notification ever expires (opt-in, like THREAD_RETENTION_DAYS —
   * a durable inbox is meant to be readable until the host says otherwise).
   */
  NOTIFICATION_RETENTION_DAYS?: string;
  /**
   * Thread-state TTL in days. The purge duty reaps
   * `mastra_thread_state` rows (state-signal lanes + goals) untouched for longer
   * than this. UNSET/EMPTY/INVALID => the duty does not run (opt-in; an active
   * goal bumps updatedAt so it never ages out).
   */
  THREAD_STATE_RETENTION_DAYS?: string;
  /**
   * Schedule-trigger history TTL in days. The purge duty reaps
   * `mastra_schedule_triggers` rows past this age by their `actualFireAt`.
   * UNSET/EMPTY/INVALID => the duty does not run (opt-in; a schedule's fire
   * history is inspectable until the host sets a window). Schedule rows are
   * standing config with no TTL; an authorized DELETE removes them, and
   * deployment decommissioning removes any that remain.
   */
  SCHEDULE_TRIGGER_RETENTION_DAYS?: string;
  /** Optional audit queue producer; delivery belongs to the control plane. */
  AUDIT_QUEUE?: AuditQueue<ApprovalAuditEvent | InfrastructureAuditEnvelope>;
  /** Remote trusted-state Durable Object namespace used by external releases. */
  AUDIT_PROXY?: AuditProxyNamespaceLike;
  /** Named StateEgress service binding, present only on trusted state. */
  OUTBOUND_PROXY?: StateEgressBinding;
  /** Dedicated trusted-state credential for the shared outbound entrypoint. */
  OUTBOUND_PROXY_CREDENTIAL?: string;
  OUTBOUND_TENANT_ID?: string;
  OUTBOUND_ENVIRONMENT?: string;
  OUTBOUND_RESOURCE_GROUP_ID?: string;
  OUTBOUND_STATE_SCRIPT_NAME?: string;
  OUTBOUND_ROUTE_HOSTNAME?: string;
  OUTBOUND_POLICY_ID?: string;
  /** Dedicated control-plane credential for maintenance administration. */
  MAINTENANCE_ADMIN_SECRET?: string;
}

/**
 * Background-task TTL cleanup config, surfaced through
 * FlowsafeWorkerConfig. Mirrors core BackgroundTaskManager.cleanup's
 * two windows.
 */
export interface BackgroundTasksCleanupConfig {
  /** Completed rows past this age expire. Default 3_600_000 (1h). */
  completedTtlMs?: number;
  /** Failed / cancelled / timed_out rows past this age expire. Default 86_400_000 (24h). */
  failedTtlMs?: number;
}

/** Structurally typed to keep host-kit independent of the agent-host subpath. */
export type AgentRouter = (request: Request) => Promise<Response | null>;

export interface FlowsafeRunnerLifecycleConfig<Env extends FlowsafeWorkerEnv> {
  /**
   * System-principal id for bridge bookkeeping and alarm maintenance
   * attribution. Requester kind is persisted separately, so ids may overlap
   * across principal kinds.
   */
  systemPrincipalId: string;
  /** Compose approval-driven resume handling, including agent-thread targets. */
  buildResumeRun?: (fallback: ResumeRunFn, env: Env) => ResumeRunFn;
  /** Reviewer-facing notification transport used by the same service. */
  notify?: (env: Env) => ApprovalNotificationSink | undefined;
  /** Prefix used by the runner's existing Mastra snapshot table. */
  storageTablePrefix?: string;
}

export interface FlowsafeWorkerConfig<Env extends FlowsafeWorkerEnv>
  extends FlowsafeRunnerLifecycleConfig<Env> {
  /** The catalog createRunRouter serves and gates (hosts pass their metas). */
  workflows: ReadonlyArray<WorkflowMeta>;
  /**
   * The identity seam: env -> TokenVerifier. Called once per fetch, so hosts
   * keep their own per-isolate memoization —
   * re-parsing a token map on every request is pure waste, but that is the
   * host's trade to make.
   */
  buildVerifier: (env: Env) => TokenVerifier;
  /**
   * Recurrence intervals for the singleton maintenance Durable Object. Every
   * duty receives its own alarm invocation; `tickIntervalMs` is required when
   * `scheduleTick` is configured and omitted otherwise.
   */
  maintenance: {
    sweepIntervalMs: number;
    purgeIntervalMs: number;
    tickIntervalMs?: number;
    /** Deadline duty cadence. Defaults to sweepIntervalMs. */
    deadlineIntervalMs?: number;
    /** Maximum runs per deadline pass. Default 100. */
    deadlineLimit?: number;
  };
  /**
   * Deployment-specific routes tried AFTER /healthz and BEFORE the approval
   * and run routers (the showcase mounts its demo sign-in here). `kit` hands
   * over the request's already-built resolver and DO topology so a pre-route
   * can share them. Return null to fall through.
   */
  preRoutes?: (
    request: Request,
    env: Env,
    ctx: FlowsafeWorkerContext,
    kit: { resolve: ActorResolver; topology: DoRunTopology },
  ) => Promise<Response | null>;
  /** Host policy immediately before a validated start reaches the run DO. */
  beforeStart?: (
    context: ActorContext,
    env: Env,
    workflowId: string,
    inputData: unknown,
  ) => Promise<void>;
  /** Host policy immediately before a validated raw resume reaches the run DO. */
  beforeResume?: (
    context: ActorContext,
    env: Env,
    workflowId: string,
    runId: string,
    body: unknown,
  ) => Promise<void>;
  /**
   * Builds the artifact purger from the current invocation's environment.
   * When set, the retention purge deletes each expired run's R2 artifacts WITH
   * its snapshot row. The snapshot row is the only enumerable record of a run's
   * artifact keys, so a retention purge without this pairing strands artifacts
   * until deployment teardown. Return the same R2-backed store used for runtime
   * writes, or undefined for row-only purge. NOT via extraPurgeDuties: that hook
   * runs after the rows are deleted, when the keys are already unenumerable.
   */
  artifactStore?: (env: Env) => RunArtifactPurger | undefined;
  /**
   * Opt-in background-task TTL cleanup. When present, the purge duty
   * reaps terminal `mastra_background_tasks` rows past the TTL as its OWN
   * failure-isolated duty — the storage-layer belt to a hosting DO's manager
   * cleanup (which needs the DO alive). Absent => no duty, byte-identical
   * (background tasks are opt-in). NOT via extraPurgeDuties: this needs its own
   * try/catch and its own log fields, like every sibling purge duty.
   */
  backgroundTasks?: BackgroundTasksCleanupConfig;
  /**
   * Opt-in authenticated agent catalog/run router. Structurally typed so this
   * module does not import the server-only agent-host subpath.
   */
  buildAgentRouter?: (
    resolve: ActorResolver,
    env: Env,
  ) => AgentRouter | undefined;
  /**
   * Opt-in signal-ingestion stage. The host builds its
   * `createSignalRouter` (which needs its per-thread DO namespace via
   * createThreadTopology, plus its audit/rate/allowlist config) and returns it
   * here, closed over the request's ActorResolver; the composer mounts
   * it after preRoutes, ahead of approvals/runs (its `/api/threads/*` routes
   * don't overlap). INJECTED rather than built here because createSignalRouter
   * lives in `signals/`, which imports host-kit — host-kit importing it back
   * would cycle. Absent (or returns undefined) => no signal stage, byte-identical.
   */
  buildSignalRouter?: (
    resolve: ActorResolver,
    env: Env,
  ) => ((request: Request) => Promise<Response | null>) | undefined;
  /**
   * Opt-in goal-objective stage. Mirrors
   * buildSignalRouter: the host builds its `createObjectiveRouter` (which needs
   * the thread-state store from its D1 domains, plus its audit/maxRuns config)
   * and returns it here, closed over the request's ActorResolver; the
   * composer mounts it after the signal stage. Both live under `/api/threads/*`
   * but do not overlap — goals use the `/goal` segment, signals the channel
   * segments. INJECTED rather than built here because createObjectiveRouter lives
   * in `goals/`, which imports host-kit — host-kit importing it back would cycle.
   * Absent (or returns undefined) ⇒ no goal stage, byte-identical.
   */
  buildObjectiveRouter?: (
    resolve: ActorResolver,
    env: Env,
  ) => ((request: Request) => Promise<Response | null>) | undefined;
  /**
   * Opt-in schedule CRUD facade. Mirrors buildSignalRouter/
   * buildObjectiveRouter: the host builds its `createScheduleRouter` (which needs
   * the schedules store from its D1 domains + its audit/cap config) and returns it
   * here, closed over the request's ActorResolver; the composer mounts it
   * after the goal stage. Its `/api/schedules/*` routes don't overlap the others.
   * INJECTED (not built here, typed structurally) because createScheduleRouter
   * lives in `schedules/`, which transitively imports host-kit — importing it back
   * would cycle. Absent (or returns undefined) ⇒ no schedule surface, byte-identical.
   */
  buildScheduleRouter?: (
    resolve: ActorResolver,
    env: Env,
  ) => ((request: Request) => Promise<Response | null>) | undefined;
  /**
   * Opt-in schedule tick. The host builds its `createScheduleTick`
   * (which needs the schedules store, its run-start seam — topology.start — and
   * the run-cap + audit config) and returns the closure here. The composer runs
   * it as its OWN failure-isolated alarm duty (own try/catch,
   * own `schedule-tick` log line). INJECTED (not built here, structurally typed as
   * `() => Promise<unknown>`) because createScheduleTick lives in `schedules/`,
   * which transitively imports host-kit — host-kit importing it back would cycle.
   * Absent (or `tickIntervalMs` unset) ⇒ no tick invocation.
   */
  scheduleTick?: (env: Env) => (() => Promise<unknown>) | undefined;
  /**
   * Extra purge duties. The
   * returned fields fold into the ONE combined `{type:'maintenance'}` log
   * line. A throw reports the purge attempt as failed while the worker still
   * contains it, logs a maintenance-error, and emits the combined log.
   */
  extraPurgeDuties?: (env: Env) => Promise<Record<string, unknown>>;
}

export interface FlowsafeRunnerLifecycleOptions {
  waitUntil?: (promise: Promise<unknown>) => void;
  discardScheduleDispatch?: DurableObjectRunLifecycleHooks['discardScheduleDispatch'];
}

interface ConfiguredApprovalServiceOptions {
  store: ApprovalStore;
  waitUntil?: (promise: Promise<unknown>) => void;
  notify?: ApprovalNotificationSink;
  allowSelfDecision: SelfDecisionPolicy;
  stream?: ApprovalStreamSink;
  /**
   * REQUIRED, unlike its optional counterpart on HostApprovalServiceOptions:
   * this interface is internal to the composer, both of its call sites are in
   * this file, and every service the composer builds sits on a database whose
   * fence it can name. Making it required is what keeps a third call site from
   * being added later that silently builds an unfenced service — which would
   * let a decision commit durably on a migration-locked deployment and then
   * fail to resume.
   */
  executionFence: ExecutionFenceStore;
}

function buildConfiguredApprovalService<Env extends FlowsafeWorkerEnv>(
  config: FlowsafeRunnerLifecycleConfig<Env>,
  env: Env,
  topology: Pick<DoRunTopology, 'resumeRecord'>,
  options: ConfiguredApprovalServiceOptions,
): ApprovalService {
  return buildHostApprovalService(options.store, {
    deploymentTag: env.DEPLOYMENT_TENANT,
    systemPrincipalId: config.systemPrincipalId,
    defaultSlaSeconds: numberVar(
      env.APPROVAL_SLA_SECONDS,
      4 * 60 * 60,
      'APPROVAL_SLA_SECONDS',
    ),
    resumeRun: config.buildResumeRun
      ? config.buildResumeRun(topology.resumeRecord, env)
      : topology.resumeRecord,
    queue: auditQueueFor(env),
    waitUntil: options.waitUntil,
    notify: options.notify,
    allowSelfDecision: options.allowSelfDecision,
    stream: options.stream,
    executionFence: options.executionFence,
  });
}

/**
 * Builds the Runner DO's terminal-cleanup hooks from the same approval-service
 * configuration as createFlowsafeWorker. Hosts supply only the optional DO
 * keepalive and their receipt-protocol-specific dispatch settler.
 */
export function createFlowsafeRunnerLifecycle<Env extends FlowsafeWorkerEnv>(
  config: FlowsafeRunnerLifecycleConfig<Env>,
  env: Env,
  options: FlowsafeRunnerLifecycleOptions = {},
): DurableObjectRunLifecycleHooks {
  const storageTablePrefix = validateTablePrefix(
    config.storageTablePrefix,
    'storageTablePrefix',
  );
  const topology = createDoRunTopology(
    env.RUNNER,
    env.DEPLOYMENT_IDENTITY_SECRET,
  );
  const allowSelfDecision = selfDecisionPolicyVar(
    env.APPROVAL_ALLOW_SELF_DECISION,
    'APPROVAL_ALLOW_SELF_DECISION',
    APPROVAL_ROLES,
  ) as SelfDecisionPolicy;
  const hubTopology = env.HUB
    ? createHubTopology(env.HUB, env.DEPLOYMENT_IDENTITY_SECRET)
    : undefined;
  const service = buildConfiguredApprovalService(config, env, topology, {
    store: approvalStoreFactoryFor(env.DB, storageTablePrefix).store(),
    executionFence: executionFenceForEnv(env),
    waitUntil: options.waitUntil,
    notify: config.notify?.(env),
    allowSelfDecision,
    stream: hubTopology
      ? (event) => {
          const send = hubTopology.publish(event);
          options.waitUntil?.(send);
          return send;
        }
      : undefined,
  });
  return {
    abandonApprovals: (workflowId, runId, status) =>
      abandonApprovalsForRun(
        service,
        workflowId,
        runId,
        status,
        config.systemPrincipalId,
      ).then(() => undefined),
    ...(options.discardScheduleDispatch
      ? { discardScheduleDispatch: options.discardScheduleDispatch }
      : {}),
  };
}

function auditQueueFor<Env extends FlowsafeWorkerEnv>(
  env: Env,
): AuditQueue<ApprovalAuditEvent> | undefined {
  if (env.FLEET_AUDIT_PROXY === 'required') {
    if (!env.AUDIT_PROXY) {
      throw new Error('external fleet release has no trusted audit proxy');
    }
    if (env.AUDIT_QUEUE) {
      throw new Error(
        'external fleet release has a direct audit queue binding',
      );
    }
    return createAuditProxyQueue(
      createAuditProxyDurableObjectBinding(env.AUDIT_PROXY),
      env.DEPLOYMENT_IDENTITY_SECRET,
    );
  }
  return env.AUDIT_QUEUE;
}

const STATE_EGRESS_ENV_KEYS = [
  'OUTBOUND_PROXY',
  'OUTBOUND_PROXY_CREDENTIAL',
  'OUTBOUND_TENANT_ID',
  'OUTBOUND_ENVIRONMENT',
  'OUTBOUND_RESOURCE_GROUP_ID',
  'OUTBOUND_STATE_SCRIPT_NAME',
  'OUTBOUND_ROUTE_HOSTNAME',
  'OUTBOUND_POLICY_ID',
] as const;

async function validateFleetChannelTopology<Env extends FlowsafeWorkerEnv>(
  env: Env,
): Promise<void> {
  const hasStateEgressBinding = STATE_EGRESS_ENV_KEYS.some(
    (key) => env[key] !== undefined,
  );
  if (env.FLEET_RESOURCE_ROLE === 'platform-state') {
    if (
      env.FLEET_AUDIT_PROXY === 'required' ||
      env.AUDIT_PROXY ||
      !env.FLEET_ENVIRONMENT ||
      !env.FLEET_RESOURCE_GROUP ||
      !hasStateEgressBinding
    ) {
      throw new Error(
        'trusted state channel topology is incomplete or ambiguous',
      );
    }
    if (
      env.OUTBOUND_TENANT_ID !== env.DEPLOYMENT_TENANT ||
      env.OUTBOUND_ENVIRONMENT !== env.FLEET_ENVIRONMENT ||
      env.OUTBOUND_RESOURCE_GROUP_ID !== env.FLEET_RESOURCE_GROUP
    ) {
      throw new Error('trusted state outbound attribution is inconsistent');
    }
    if (
      env.FLEET_AUDIT_PROXY_INGRESS === 'required'
        ? !env.AUDIT_QUEUE || !env.FLEET_DEPLOYMENT_SCRIPT
        : env.AUDIT_QUEUE !== undefined
    ) {
      throw new Error(
        'trusted state audit topology is incomplete or ambiguous',
      );
    }
    await validateStateEgressEnv(env as unknown as StateEgressEnv);
    return;
  }
  if (env.FLEET_AUDIT_PROXY_INGRESS === 'required' || hasStateEgressBinding) {
    throw new Error('non-state Worker has trusted state channel bindings');
  }
  if (env.FLEET_AUDIT_PROXY === 'required') {
    if (!env.AUDIT_PROXY || env.AUDIT_QUEUE) {
      throw new Error(
        'external fleet audit topology is incomplete or ambiguous',
      );
    }
    createAuditProxyDurableObjectBinding(env.AUDIT_PROXY);
  } else if (env.AUDIT_PROXY) {
    throw new Error('unmarked Worker has an audit proxy namespace');
  }
  if (env.FLEET_MAINTENANCE_CAPABILITIES === 'required' && env.AUDIT_QUEUE) {
    throw new Error('external fleet release has a direct audit queue binding');
  }
}

export type MaintenanceDuty = 'deadline' | 'sweep' | 'purge' | 'tick';

export interface MaintenanceDutyContext {
  deadlineCursor?: RunDeadlineCursor;
  advanceDeadlineCursor?(cursor: RunDeadlineCursor): Promise<void>;
}

/** The Worker handler plus the maintenance duty seam consumed by its DO. */
export interface FlowsafeWorker<Env extends FlowsafeWorkerEnv> {
  fetch(
    request: Request,
    env: Env,
    ctx: FlowsafeWorkerContext,
  ): Promise<Response>;
  runMaintenanceDuty(
    duty: MaintenanceDuty,
    env: Env,
    context?: MaintenanceDutyContext,
  ): Promise<MaintenanceOutcome>;
}

function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

const MAINTENANCE_ADMIN_SECRET_PATTERN = /^[\x21-\x7e]{32,256}$/;
const FLEET_SPEC_DIGEST_PATTERN = /^[a-f0-9]{64}$/;

/**
 * What the shared admin gate decided. A union rather than `Response | null`
 * because an authorized answer carries two things a caller needs: the
 * credential the request presented, which the maintenance route forwards to its
 * Durable Object on the delegating path below, and WHETHER this was that
 * delegating path. Recovering the credential by re-reading the header would
 * either re-duplicate the Bearer extraction this gate exists to own, or need an
 * unreachable `undefined` branch to satisfy the types.
 *
 * `delegated` is reported rather than re-derived for the stronger reason: the
 * rule that makes `MAINTENANCE_ADMIN_SECRET === undefined` mean "delegating" is
 * enforced HERE — an absent secret refuses outright unless the caller asked to
 * delegate — so a route re-testing the env var is restating a decision it
 * cannot see, and would keep answering `true` if this gate's policy ever
 * changed. One decision, reported once.
 */
type AdminCredentialDecision =
  | {
      readonly authorized: true;
      readonly credential: string;
      /**
       * The credential is a downstream-verified capability token relayed by
       * this Worker, not a match against MAINTENANCE_ADMIN_SECRET.
       */
      readonly delegated: boolean;
    }
  | { readonly authorized: false; readonly response: Response };

/**
 * The credential preamble EVERY /admin surface runs before it does anything.
 *
 * One function rather than a copy per route because this is the trust boundary
 * itself (docs/security-threat-model.md, "The provisioning boundary"): it
 * proves MAINTENANCE_ADMIN_SECRET is configured, proves it is DISTINCT from the
 * deployment identity secret (sharing them would let a Worker-to-DO credential
 * move the fence, and vice versa), and constant-time compares the request's
 * Bearer token against it. A second copy is a second place for one of those
 * three to be dropped in a hurry, and the inventory route lands here next.
 *
 * The surfaces differ in exactly ONE thing, which is why it is a parameter
 * rather than a fork: what an ABSENT secret means. `/admin/execution-fence`
 * always refuses — the fence is the control that stops a deployment executing,
 * so an unauthenticated caller must never reach it. The maintenance routes
 * delegate instead when the fleet requires capability tokens, because there the
 * Durable Object verifies a signed capability and this Worker is only a relay;
 * the longer credential cap applies to that path alone, since a capability
 * token is not a shared secret.
 */
async function authorizeAdminCredential<Env extends FlowsafeWorkerEnv>(
  request: Request,
  env: Env,
  options: {
    /** Names the surface in the config-error log and the 503 body. */
    readonly surface: string;
    /**
     * Whether an absent MAINTENANCE_ADMIN_SECRET delegates authentication
     * downstream rather than refusing. The caller folds its own policy into
     * this boolean so the gate stays about credentials only.
     */
    readonly delegateWhenUnconfigured: boolean;
  },
): Promise<AdminCredentialDecision> {
  const { surface } = options;
  const unavailable = (reason: string): AdminCredentialDecision => {
    console.error(
      JSON.stringify({
        type: 'config-error',
        var: 'MAINTENANCE_ADMIN_SECRET',
        reason,
      }),
    );
    return {
      authorized: false,
      response: json({ error: `${surface} unavailable` }, 503),
    };
  };
  const unauthenticated = (): AdminCredentialDecision => ({
    authorized: false,
    response: json({ error: 'authentication required' }, 401),
  });
  const expected = env.MAINTENANCE_ADMIN_SECRET;
  const delegating = expected === undefined && options.delegateWhenUnconfigured;
  if (!delegating) {
    if (
      expected === undefined ||
      !MAINTENANCE_ADMIN_SECRET_PATTERN.test(expected)
    ) {
      return unavailable(`${surface} is not configured`);
    }
    if (await credentialsMatch(expected, env.DEPLOYMENT_IDENTITY_SECRET)) {
      return unavailable(
        'maintenance and deployment identity credentials must differ',
      );
    }
  }
  const credential = bearerCredential(request);
  const maximumCredentialLength = delegating ? 2_048 : 256;
  if (!credential || credential.length > maximumCredentialLength) {
    return unauthenticated();
  }
  if (
    expected !== undefined &&
    !(await credentialsMatch(credential, expected))
  ) {
    return unauthenticated();
  }
  return { authorized: true, credential, delegated: delegating };
}

/** The Bearer credential a request presents, if it presents a well-formed one. */
function bearerCredential(request: Request): string | undefined {
  return request.headers.get('authorization')?.match(/^Bearer\s+(.+)$/i)?.[1];
}

async function maintenanceAdminResponse<Env extends FlowsafeWorkerEnv>(
  request: Request,
  env: Env,
): Promise<Response | null> {
  const url = new URL(request.url);
  const operation =
    url.pathname === '/admin/ensure-maintenance'
      ? 'ensure'
      : url.pathname === '/admin/maintenance-status'
        ? 'status'
        : undefined;
  if (!operation) return null;
  const expectedMethod = operation === 'ensure' ? 'POST' : 'GET';
  if (request.method !== expectedMethod) {
    return json({ error: 'method not allowed' }, 405);
  }
  const gate = await authorizeAdminCredential(request, env, {
    surface: 'maintenance administration',
    // An unconfigured secret is survivable HERE and only here: a fleet that
    // requires capability tokens authenticates at the maintenance DO, which
    // verifies a signed capability this Worker only relays.
    delegateWhenUnconfigured: env.FLEET_MAINTENANCE_CAPABILITIES === 'required',
  });
  if (!gate.authorized) return gate.response;
  const deploymentSpecDigest = env.FLEET_SPEC_DIGEST;
  if (
    deploymentSpecDigest !== undefined &&
    !FLEET_SPEC_DIGEST_PATTERN.test(deploymentSpecDigest)
  ) {
    console.error(
      JSON.stringify({
        type: 'config-error',
        var: 'FLEET_SPEC_DIGEST',
        reason: 'fleet specification digest is malformed',
      }),
    );
    return json({ error: 'maintenance administration unavailable' }, 503);
  }
  const id = env.MAINTENANCE.idFromName(MAINTENANCE_INSTANCE_NAME);
  const response = await env.MAINTENANCE.get(id).fetch(
    `http://maintenance/${operation}`,
    {
      method: expectedMethod,
      // The gate's own verdict, not a second reading of the env var it decided
      // on: a delegated request carries a capability the maintenance DO
      // verifies, so this Worker relays the credential unchanged; anything else
      // was authenticated HERE and travels on the deployment identity.
      headers: gate.delegated
        ? { authorization: `Bearer ${gate.credential}` }
        : deploymentIdentityHeaders(env.DEPLOYMENT_IDENTITY_SECRET),
    },
  );
  // Nothing to enrich on the delegated path: the digest check below belongs to
  // the deployment this Worker authenticated for, and a relayed response is the
  // maintenance DO's own answer.
  if (gate.delegated) return response;
  if (!response.ok || deploymentSpecDigest === undefined) return response;
  const payload: unknown = await response.json();
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return json({ error: 'maintenance response is malformed' }, 502);
  }
  return json({
    ...(payload as Record<string, unknown>),
    deploymentSpecDigest,
  });
}

/**
 * This composer's env-shaped view of the ONE per-database fence memo
 * (do-runner/execution-fence.ts). Every call site here holds an `env` rather
 * than a binding, so the unwrapping happens once instead of at each of them;
 * the identity guarantee — admin route, approval service, and runner runtime
 * all on the same store for the same database — belongs to the shared memo.
 */
function executionFenceForEnv<Env extends FlowsafeWorkerEnv>(
  env: Env,
): ExecutionFenceStore {
  return executionFenceFor(env.DB);
}

/**
 * The same env-shaped view of the ONE per-database start-reservation memo
 * (do-runner/start-idempotency.ts), for the same reason: the run router
 * reserves here, and the runtime inside the run object settles there, and both
 * must be the store built from THIS env's binding.
 */
function startIdempotencyForEnv<Env extends FlowsafeWorkerEnv>(
  env: Env,
): StartIdempotencyStore {
  return startIdempotencyFor(env.DB);
}

const EXECUTION_FENCE_ADMIN_PATH = '/admin/execution-fence';
const EXECUTION_FENCE_ADMIN_MAX_BODY_BYTES = 4_096;

/**
 * The deployment execution fence's control-plane surface, behind the SAME gate
 * `maintenanceAdminResponse` uses (`authorizeAdminCredential`) — it is the same
 * trust boundary (the provisioning TCB, docs/security-threat-model.md), and
 * splitting the credential would only add a second secret an operator can get
 * wrong.
 *
 * `GET` reports the state; `POST` moves it, compare-and-set on `expected`.
 * Transition POLICY is the HOST's: this package enforces only the state
 * vocabulary, the CAS, and the rule that entering 'proof-only' names a proof
 * key. Which sequence of states a migration walks is the control plane's
 * business, and hard-coding it here would freeze an operational procedure into
 * a published package.
 *
 * Answers on a mis-provisioned deployment are NOT reachable: the identity gate
 * 503s before this dispatch. Accepted — an operator whose bindings are wrong
 * has a bigger problem than the fence, and relaxing the identity gate to serve
 * this route would put an unverified database behind a control-plane write.
 */
async function executionFenceAdminResponse<Env extends FlowsafeWorkerEnv>(
  request: Request,
  env: Env,
): Promise<Response | null> {
  const url = new URL(request.url);
  if (url.pathname !== EXECUTION_FENCE_ADMIN_PATH) return null;
  if (request.method !== 'GET' && request.method !== 'POST') {
    return json({ error: 'method not allowed' }, 405);
  }
  const gate = await authorizeAdminCredential(request, env, {
    surface: 'execution fence administration',
    // Unconfigured is 503, never open: the fence is the control that stops a
    // deployment executing, so an unauthenticated caller must never move it.
    // There is no capability-token relay behind this route to delegate to.
    delegateWhenUnconfigured: false,
  });
  if (!gate.authorized) return gate.response;
  const fence = executionFenceForEnv(env);
  try {
    if (request.method === 'GET') {
      return json(executionFenceReadingPayload(await fence.read()));
    }
    const raw = await readBoundedBody(
      request,
      EXECUTION_FENCE_ADMIN_MAX_BODY_BYTES,
      'execution fence body exceeds limit',
    );
    if (!raw.ok) {
      return json(
        { error: 'a JSON object body is required' },
        raw.reason === 'payload-too-large' ? 413 : 400,
      );
    }
    let parsed: unknown;
    try {
      parsed = raw.text === '' ? undefined : JSON.parse(raw.text);
    } catch {
      parsed = undefined;
    }
    if (
      parsed === null ||
      typeof parsed !== 'object' ||
      Array.isArray(parsed)
    ) {
      return json({ error: 'a JSON object body is required' }, 400);
    }
    const body = parsed as {
      expected?: unknown;
      next?: unknown;
      proofKey?: unknown;
    };
    const reading = await fence.transition({
      expected: assertExecutionFenceState(body.expected, 'expected'),
      next: assertExecutionFenceState(body.next, 'next'),
      ...(body.proofKey === undefined ? {} : { proofKey: body.proofKey }),
    });
    return json({ state: reading.state });
  } catch (error) {
    if (error instanceof DoStatusError) {
      return json(
        {
          error: error.message,
          ...(error.reason === undefined ? {} : { reason: error.reason }),
        },
        error.status,
      );
    }
    console.error(
      JSON.stringify({
        type: 'execution-fence-admin-error',
        reason: error instanceof Error ? error.message : String(error),
      }),
    );
    return json({ error: 'execution fence administration failed' }, 500);
  }
}

const INVENTORY_ADMIN_PATH = '/admin/inventory';

/**
 * The deployment's drain-proof surface uses the same credential helper as the
 * fence and maintenance routes. Their absent-secret policies differ:
 * maintenance routes may delegate to a fleet capability; fence and inventory
 * routes never do.
 *
 * WHAT IT IS FOR. An operator holds the fence in `draining` and needs to prove,
 * with no side effects, that this deployment holds no executable or resumable
 * work before locking it for a migration. Without such a proof the only
 * available answer is a wait-and-hope, and a suspended run that survives it
 * gets resumed by two deployments at once.
 *
 * HOW TO READ THE ANSWER — the contract the index response also states, in
 * full, because this is the part a hurried operator skips:
 *
 *   Each result is a point-in-time observation, not a snapshot. Rows can enter
 *   or leave `work` categories while draining admits work. An empty result
 *   cannot over-count work, and keyset pagination never skips a row that
 *   existed before the sweep started.
 *
 *   The proof is TWO consecutive full sweeps, at least one 60-second
 *   RUN_OWNER_RECOVERY_DELAY_MS cadence apart, in which every `work` category
 *   comes back empty. One sweep cannot cover the window in which a run object
 *   has journalled its recovery key but not yet written its D1 owner row; two,
 *   spaced that far, can.
 *
 *   An empty sweep from `draining` contributes to the two-sweep proof. After
 *   the transition to `migration-locked`, an empty post-lock re-sweep is
 *   conclusive because the lock refuses new work. A non-empty one means work is
 *   still outstanding, either because it entered after the proof or because
 *   the lock parked it before it finished. Return to `draining` and repeat the
 *   proof.
 *
 * `standing` categories (schedules, provider subscriptions) are reported for
 * reconciliation and are never required to be empty: they are configuration a
 * migration carries across.
 *
 * GET only. Every query underneath is a bare SELECT that never creates schema,
 * so running this against a deployment about to be copied does not change what
 * is being copied.
 */
async function inventoryAdminResponse<Env extends FlowsafeWorkerEnv>(
  request: Request,
  env: Env,
  storageTablePrefix: string | undefined,
): Promise<Response | null> {
  const url = new URL(request.url);
  if (url.pathname !== INVENTORY_ADMIN_PATH) return null;
  if (request.method !== 'GET') {
    return json({ error: 'method not allowed' }, 405);
  }
  const gate = await authorizeAdminCredential(request, env, {
    surface: 'deployment inventory',
    // Unconfigured is 503, never open: this read enumerates every outstanding
    // run, approval, and reservation on the deployment. There is no capability
    // relay behind it to delegate to, either.
    delegateWhenUnconfigured: false,
  });
  if (!gate.authorized) return gate.response;
  try {
    const inventory = new DeploymentInventory(env.DB, {
      ...(storageTablePrefix === undefined
        ? {}
        : { tablePrefix: storageTablePrefix }),
    });
    const category = url.searchParams.get('category');
    if (category === null || category === '') {
      return json(inventory.index());
    }
    if (!isInventoryCategory(category)) {
      throw new InvalidInventoryRequestError(
        `unknown inventory category '${category}'`,
      );
    }
    const rawLimit = url.searchParams.get('limit');
    // Parsed here rather than coerced inside the store: `Number('abc')` is NaN
    // and `Number('')` is 0, and both would otherwise reach the clamp as a
    // number that means something the caller never asked for.
    if (rawLimit !== null && !/^[0-9]{1,4}$/.test(rawLimit)) {
      throw new InvalidInventoryRequestError(
        'inventory limit must be a positive integer',
      );
    }
    const cursor = url.searchParams.get('cursor');
    return json(
      await inventory.read(category, {
        ...(cursor === null ? {} : { cursor }),
        ...(rawLimit === null ? {} : { limit: Number(rawLimit) }),
      }),
    );
  } catch (error) {
    if (error instanceof DoStatusError) {
      return json(
        {
          error: error.message,
          ...(error.reason === undefined ? {} : { reason: error.reason }),
        },
        error.status,
      );
    }
    console.error(
      JSON.stringify({
        type: 'inventory-admin-error',
        reason: error instanceof Error ? error.message : String(error),
      }),
    );
    return json({ error: 'deployment inventory failed' }, 500);
  }
}

export function createFlowsafeWorker<Env extends FlowsafeWorkerEnv>(
  config: FlowsafeWorkerConfig<Env>,
): FlowsafeWorker<Env> {
  const storageTablePrefix = validateTablePrefix(
    config.storageTablePrefix,
    'storageTablePrefix',
  );
  // Authenticate before constructing request-scoped services.
  const buildResolve = (
    env: Env,
    topology: DoRunTopology,
    waitUntil: (promise: Promise<unknown>) => void,
    notify: ApprovalNotificationSink | undefined,
    selfDecision: SelfDecisionPolicy,
    stream: ApprovalStreamSink | undefined,
  ): ActorResolver => {
    const base = createActorResolver({
      authenticate: bearerActorAuthenticator(config.buildVerifier(env)),
      storeFactory: approvalStoreFactoryFor(env.DB, storageTablePrefix),
      deploymentTag: env.DEPLOYMENT_TENANT,
      buildService: (store) =>
        buildConfiguredApprovalService(config, env, topology, {
          store,
          executionFence: executionFenceForEnv(env),
          waitUntil,
          notify,
          stream,
          allowSelfDecision: selfDecision,
        }),
      // The resolver's canSelfDecide display hint reads the SAME policy the
      // service enforces (passed to the shared service builder above), so the
      // /workflows echo can never contradict decide().
      allowSelfDecision: selfDecision,
    });
    return base;
  };

  // Parse ONCE per deployment value, memoized by the RAW
  // APPROVAL_ALLOW_SELF_DECISION string — NOT by env identity: a host mutates
  // the same env object across values (unset -> 'admin' -> 'nonsense'), so an
  // env-keyed cache would serve the first policy for all of them. The
  // enforcement (service) and the catalog echo (run-router, via the resolver's
  // canSelfDecide) must read the SAME policy, or the SPA's "you may
  // self-decide" hint could contradict the server's verdict. Roles are
  // validated against APPROVAL_ROLES, so the { roles } branch is an
  // ApprovalRole[] by construction. selfDecisionPolicyVar never throws (garbage
  // -> false + config-error log), so nothing thrown is cached; a policy is
  // never undefined, so a get() miss unambiguously means "not yet computed".
  const selfDecisionCache = new Map<string | undefined, SelfDecisionPolicy>();
  const parseSelfDecision = (env: Env): SelfDecisionPolicy => {
    const raw = env.APPROVAL_ALLOW_SELF_DECISION;
    const cached = selfDecisionCache.get(raw);
    if (cached !== undefined) return cached;
    const policy = selfDecisionPolicyVar(
      raw,
      'APPROVAL_ALLOW_SELF_DECISION',
      APPROVAL_ROLES,
    ) as SelfDecisionPolicy;
    selfDecisionCache.set(raw, policy);
    return policy;
  };

  async function runPurgeMaintenance(
    env: Env,
    trigger: string,
  ): Promise<MaintenanceOutcome> {
    const failures: string[] = [];
    const recordFailure = (surface: string, error: unknown): void => {
      const failure = String(error);
      failures.push(`${surface}: ${failure}`);
      console.error(
        JSON.stringify({
          type: 'maintenance-error',
          surface,
          trigger,
          error: failure,
        }),
      );
    };
    let purged: number | undefined;
    try {
      const artifactStore = config.artifactStore?.(env);
      // The resource registry is lazy like Mastra's snapshot table. Retention
      // may be the first resource-aware operation in a fresh deployment, so
      // initialize it before asking the atomic purge to reference it.
      await createResourceOwnershipSchema(env.DB);
      purged = await purgeExpiredWorkflowRuns(env.DB, {
        // allowZero: RUN_RETENTION_DAYS=0 means "purge terminal runs now".
        ttlMs:
          numberVar(env.RUN_RETENTION_DAYS, 30, 'RUN_RETENTION_DAYS', {
            allowZero: true,
          }) *
          24 *
          60 *
          60 *
          1000,
        // Pairs each expired run's R2 artifacts with its snapshot-row deletion
        // (artifacts BEFORE the row — the row is the only record of their keys).
        // Undefined on hosts that wire no R2, so the purge stays byte-identical.
        artifactStore,
        tablePrefix: storageTablePrefix,
        // Snapshot + owner release share one D1 transaction, so retention
        // cannot leave unbounded run-ownership tombstones or expose a live row
        // without its authorization record.
        resourceOwnerTable: RESOURCE_OWNERSHIP_TABLE,
        // Start reservations join the same transaction, for the same reason:
        // an idempotency key must never be reaped while the run it names is
        // still readable, or the next retry of that key would start a second
        // run beside the live one. The horizon defaults to run retention —
        // START_IDEMPOTENCY_RETENTION_DAYS is what a host sets when its callers
        // retry for longer than it keeps run summaries.
        startIdempotencyTable: START_IDEMPOTENCY_TABLE,
        ...(env.START_IDEMPOTENCY_RETENTION_DAYS === undefined
          ? {}
          : {
              startIdempotencyTtlMs:
                numberVar(
                  env.START_IDEMPOTENCY_RETENTION_DAYS,
                  30,
                  'START_IDEMPOTENCY_RETENTION_DAYS',
                  { allowZero: true },
                ) *
                24 *
                60 *
                60 *
                1000,
            }),
      });
    } catch (error) {
      recordFailure('retention-purge', error);
    }
    // Own containment (inside runApprovalRetentionPurge), same isolation as
    // the snapshot purge above: a failure in any one purge duty must never
    // stop the others.
    const approvalPurge = await runApprovalRetentionPurge({
      store: approvalStoreFactoryFor(env.DB, storageTablePrefix).store(),
      retentionDays: env.APPROVAL_RETENTION_DAYS,
      trigger,
    });
    if (!approvalPurge.ok) {
      failures.push(`approval-retention-purge: ${approvalPurge.error}`);
    }
    const approvalsPurged = approvalPurge.ok ? approvalPurge.value : undefined;
    // Agent-memory thread TTL, opt-in.
    // Its OWN try/catch, like every sibling duty above and below: isolating one
    // loop while a sibling shares its failure is a defect this codebase has
    // already shipped once — a wedged thread purge must cost the run-snapshot
    // purge, the approval purge, and the extra duties nothing.
    let threadsPurged: number | undefined;
    let threadMessagesPurged: number | undefined;
    // optionalNumberVar, not numberVar: this var GATES the duty rather than
    // tuning it, so unset/empty/garbage must all mean "do not delete" — never a
    // silent fallback threshold (see its contract in env-vars.ts).
    // allowZero: THREAD_RETENTION_DAYS=0 means "expire idle threads now", the
    // same operator intent RUN_RETENTION_DAYS=0 states.
    const threadRetentionDays = optionalNumberVar(
      env.THREAD_RETENTION_DAYS,
      'THREAD_RETENTION_DAYS',
      { allowZero: true },
    );
    if (threadRetentionDays !== undefined) {
      try {
        const purgedThreads = await purgeExpiredThreads(env.DB, {
          ttlMs: threadRetentionDays * 24 * 60 * 60 * 1000,
          tablePrefix: storageTablePrefix,
        });
        threadsPurged = purgedThreads.threads;
        threadMessagesPurged = purgedThreads.messages;
      } catch (error) {
        recordFailure('thread-retention-purge', error);
      }
    }
    // Background-task TTL cleanup (opt-in). Its OWN try/catch, like
    // every sibling duty: a wedged background-task purge must cost the
    // run-snapshot, approval, and thread purges nothing.
    let backgroundTasksPurged: PurgeExpiredBackgroundTasksResult | undefined;
    if (config.backgroundTasks) {
      try {
        backgroundTasksPurged = await purgeExpiredBackgroundTasks(env.DB, {
          completedTtlMs: config.backgroundTasks.completedTtlMs,
          failedTtlMs: config.backgroundTasks.failedTtlMs,
          tablePrefix: storageTablePrefix,
        });
      } catch (error) {
        recordFailure('background-task-purge', error);
      }
    }
    // Notification TTL cleanup (opt-in). Its OWN try/catch, like every sibling
    // duty. optionalNumberVar (GATES the duty; unset/garbage => do not delete).
    let notificationsPurged: number | undefined;
    const notificationRetentionDays = optionalNumberVar(
      env.NOTIFICATION_RETENTION_DAYS,
      'NOTIFICATION_RETENTION_DAYS',
      { allowZero: true },
    );
    if (notificationRetentionDays !== undefined) {
      try {
        notificationsPurged = await purgeExpiredNotifications(env.DB, {
          ttlMs: notificationRetentionDays * 24 * 60 * 60 * 1000,
          tablePrefix: storageTablePrefix,
        });
      } catch (error) {
        recordFailure('notification-purge', error);
      }
    }
    // Thread-state TTL cleanup (opt-in). Same isolation + opt-in posture.
    let threadStatePurged: number | undefined;
    const threadStateRetentionDays = optionalNumberVar(
      env.THREAD_STATE_RETENTION_DAYS,
      'THREAD_STATE_RETENTION_DAYS',
      { allowZero: true },
    );
    if (threadStateRetentionDays !== undefined) {
      try {
        threadStatePurged = await purgeExpiredThreadState(env.DB, {
          ttlMs: threadStateRetentionDays * 24 * 60 * 60 * 1000,
          tablePrefix: storageTablePrefix,
        });
      } catch (error) {
        recordFailure('thread-state-purge', error);
      }
    }
    // Schedule-trigger history TTL cleanup (opt-in). Same isolation + opt-in
    // posture. Only the fire HISTORY expires; schedule config rows are reaped
    // only at deployment teardown.
    let scheduleTriggersPurged: number | undefined;
    const scheduleTriggerRetentionDays = optionalNumberVar(
      env.SCHEDULE_TRIGGER_RETENTION_DAYS,
      'SCHEDULE_TRIGGER_RETENTION_DAYS',
      { allowZero: true },
    );
    if (scheduleTriggerRetentionDays !== undefined) {
      try {
        scheduleTriggersPurged = await purgeExpiredScheduleTriggers(env.DB, {
          ttlMs: scheduleTriggerRetentionDays * 24 * 60 * 60 * 1000,
          tablePrefix: storageTablePrefix,
        });
      } catch (error) {
        recordFailure('schedule-trigger-purge', error);
      }
    }
    let extra: Record<string, unknown> = {};
    if (config.extraPurgeDuties) {
      try {
        extra = await config.extraPurgeDuties(env);
      } catch (error) {
        recordFailure('extra-purge-duties', error);
      }
    }
    console.log(
      JSON.stringify({
        type: 'maintenance',
        trigger,
        purged,
        approvalsPurged,
        threadsPurged,
        threadMessagesPurged,
        backgroundTasksCompletedPurged: backgroundTasksPurged?.completed,
        backgroundTasksFailedPurged: backgroundTasksPurged?.failed,
        notificationsPurged,
        threadStatePurged,
        scheduleTriggersPurged,
        ...extra,
      }),
    );
    return failures.length === 0
      ? { ok: true, value: undefined }
      : { ok: false, error: failures.join('; ') };
  }

  // Schedule tick — its OWN failure-isolated duty (own try/catch, own log
  // line). A wedged fire pass must cost the other duties nothing, and vice
  // versa (the same failure-isolation rationale).
  async function runScheduleTickDuty(
    env: Env,
    trigger: string,
  ): Promise<MaintenanceOutcome> {
    const tick = config.scheduleTick?.(env);
    if (!tick) {
      // The tick duty ran but no scheduleTick builder is wired — a misconfig.
      // Do not fall through to another duty; this invocation is the tick's.
      console.error(
        JSON.stringify({
          type: 'config-error',
          var: 'maintenance.tick',
          trigger,
          reason: 'tick duty ran but no scheduleTick builder is configured',
        }),
      );
      return {
        ok: false,
        error: 'tick duty ran but no scheduleTick builder is configured',
      };
    }
    try {
      const result = await tick();
      console.log(JSON.stringify({ type: 'schedule-tick', trigger, result }));
      return { ok: true, value: undefined };
    } catch (error) {
      const failure = String(error);
      console.error(
        JSON.stringify({
          type: 'schedule-tick-error',
          trigger,
          error: failure,
        }),
      );
      return { ok: false, error: failure };
    }
  }

  async function runDeadlineDuty(
    env: Env,
    trigger: string,
    context?: MaintenanceDutyContext,
  ): Promise<MaintenanceOutcome> {
    const topology = createDoRunTopology(
      env.RUNNER,
      env.DEPLOYMENT_IDENTITY_SECRET,
    );
    const principal = trustAutomationPrincipal({
      kind: 'system',
      id: config.systemPrincipalId,
      purpose: 'run-deadline-maintenance',
    });
    try {
      const processed = await sweepExpiredRunDeadlines(env.DB, {
        tablePrefix: storageTablePrefix,
        limit: config.maintenance.deadlineLimit,
        ...(context?.advanceDeadlineCursor
          ? {
              ...(context.deadlineCursor
                ? { cursor: context.deadlineCursor }
                : {}),
              advanceCursor: context.advanceDeadlineCursor,
            }
          : {}),
        transition: async (candidate) => {
          await topology.timeOut(
            candidate.workflowId,
            candidate.runId,
            {
              expectedRevision: candidate.revision,
              expectedDeadlineAt: candidate.deadlineAt,
            },
            principal,
          );
        },
      });
      console.log(
        JSON.stringify({ type: 'deadline-sweep', trigger, processed }),
      );
      return { ok: true, value: undefined };
    } catch (error) {
      const failure = String(error);
      console.error(
        JSON.stringify({
          type: 'deadline-sweep-error',
          trigger,
          error: failure,
        }),
      );
      return { ok: false, error: failure };
    }
  }

  return {
    async fetch(request, env, ctx) {
      try {
        await ensureDeploymentIdentityBindings(env);
        await validateFleetChannelTopology(env);
        const url = new URL(request.url);

        if (request.method === 'GET' && url.pathname === '/healthz') {
          return json({ ok: true });
        }

        const maintenanceAdmin = await maintenanceAdminResponse(request, env);
        if (maintenanceAdmin) return maintenanceAdmin;

        // The execution fence's control plane, beside the maintenance admin
        // routes and ahead of every tenant router: it must answer while the
        // deployment is refusing tenant traffic, which is the whole state it
        // exists to report and clear.
        const fenceAdmin = await executionFenceAdminResponse(request, env);
        if (fenceAdmin) return fenceAdmin;

        // Beside the fence for the same reason: the drain proof is read WHILE
        // the deployment is refusing tenant traffic, so it must not sit behind
        // a router that a drain is trying to empty.
        const inventoryAdmin = await inventoryAdminResponse(
          request,
          env,
          storageTablePrefix,
        );
        if (inventoryAdmin) return inventoryAdmin;

        const waitUntil = (promise: Promise<unknown>): void =>
          ctx.waitUntil(promise);
        const topology = createDoRunTopology(
          env.RUNNER,
          env.DEPLOYMENT_IDENTITY_SECRET,
        );
        const notify = config.notify?.(env);
        const selfDecision = parseSelfDecision(env);
        // Fetch-scope live fan-out sink: present iff a hub is bound (streaming is
        // opt-in). Each publish rides ctx.waitUntil and is
        // contained — a failed fan-out logs and never fails the mutation.
        const hub = env.HUB;
        let streamSink: ApprovalStreamSink | undefined;
        if (hub) {
          const hubTopology = createHubTopology(
            hub,
            env.DEPLOYMENT_IDENTITY_SECRET,
          );
          streamSink = (event) =>
            waitUntil(
              hubTopology.publish(event).catch((error: unknown) =>
                console.error(
                  JSON.stringify({
                    type: 'stream-publish-error',
                    reason:
                      error instanceof Error ? error.message : String(error),
                  }),
                ),
              ),
            );
        }
        const resolve = buildResolve(
          env,
          topology,
          waitUntil,
          notify,
          selfDecision,
          streamSink,
        );

        if (config.preRoutes) {
          const preResponse = await config.preRoutes(request, env, ctx, {
            resolve,
            topology,
          });
          if (preResponse) return preResponse;
        }

        const agentRouter = config.buildAgentRouter?.(resolve, env);
        if (agentRouter) {
          const agentResponse = await agentRouter(request);
          if (agentResponse) return agentResponse;
        }

        // Optional stream stage: mounted only when BOTH the hub
        // binding and the ticket secret are present. Every route is under
        // /api/stream/, so it composes ahead of the approval router without
        // touching the /api/* run_worker_first entry.
        if (env.HUB && env.STREAM_TICKET_SECRET) {
          const streamResponse = await createStreamRouter({
            resolve,
            ticketSecret: env.STREAM_TICKET_SECRET,
            hub: env.HUB,
            runner: env.RUNNER,
            runStatus: topology.status,
            deploymentIdentitySecret: env.DEPLOYMENT_IDENTITY_SECRET,
          })(request);
          if (streamResponse) return streamResponse;
        }

        // Optional signal stage: the host-built createSignalRouter closes over
        // THIS request's resolver. `/api/threads/*` composes ahead of
        // approvals/runs without overlap. Absent seam => unmounted.
        const signalRouter = config.buildSignalRouter?.(resolve, env);
        if (signalRouter) {
          const signalResponse = await signalRouter(request);
          if (signalResponse) return signalResponse;
        }

        // Optional goal stage: the host-built createObjectiveRouter closes over
        // THIS request's resolver. `/api/threads/:threadId/goal` composes after
        // signals and ahead of approvals/runs. Absent seam => unmounted and
        // byte-identical.
        const objectiveRouter = config.buildObjectiveRouter?.(resolve, env);
        if (objectiveRouter) {
          const objectiveResponse = await objectiveRouter(request);
          if (objectiveResponse) return objectiveResponse;
        }

        // Optional schedule CRUD stage: the host-built createScheduleRouter
        // closes over THIS request's resolver. `/api/schedules/*` composes
        // after goals and ahead of approvals/runs. Absent seam => unmounted
        // and byte-identical.
        const scheduleRouter = config.buildScheduleRouter?.(resolve, env);
        if (scheduleRouter) {
          const scheduleResponse = await scheduleRouter(request);
          if (scheduleResponse) return scheduleResponse;
        }

        const approvalResponse = await createApprovalRouter({ resolve })(
          request,
        );
        if (approvalResponse) return approvalResponse;

        const beforeStart = config.beforeStart;
        const beforeResume = config.beforeResume;
        const runResponse = await createRunRouter({
          workflows: config.workflows,
          resolve,
          systemPrincipalId: config.systemPrincipalId,
          start: topology.start,
          status: topology.status,
          resume: topology.resume,
          terminate: topology.terminate,
          // Both halves come from this env's own bindings: the store from the
          // per-database memo (so the runtime inside the run object settles the
          // very rows this router reserved), and the probe from the same DO
          // topology every other run operation travels through (so "is it
          // live?" is asked of the one object that could be running it).
          startIdempotency: {
            store: startIdempotencyForEnv(env),
            live: topology.startLiveness,
            executionFence: executionFenceForEnv(env),
          },
          beforeStart: beforeStart
            ? (context, workflowId, inputData) =>
                beforeStart(context, env, workflowId, inputData)
            : undefined,
          beforeResume: beforeResume
            ? (context, workflowId, runId, body) =>
                beforeResume(context, env, workflowId, runId, body)
            : undefined,
          // Self-healing approval reconciliation is waitUntil-detached; the
          // shared wrapper owns the detach + reconcile-error logging.
          reconcileApprovals: reconcileApprovalsOnStatusDetached(
            config.systemPrincipalId,
            waitUntil,
          ),
        })(request);
        if (runResponse) return runResponse;

        return json({ error: 'not found' }, 404);
      } catch (error) {
        if (error instanceof DeploymentIdentityError) {
          console.error(
            JSON.stringify({
              type: 'deployment-identity-error',
              reason: error.message,
            }),
          );
          return json({ error: 'deployment unavailable' }, 503);
        }
        // Backstop: a mounted router (or any handler fault) that THROWS before
        // returning a Response — e.g. a future unguarded path decode — is
        // contained as a generic 500 here rather than rejecting out of fetch()
        // as an unhandled promise. Never surface error.message to the client.
        console.error(
          JSON.stringify({
            type: 'worker-fetch-error',
            reason: error instanceof Error ? error.message : String(error),
          }),
        );
        return json({ error: 'internal error' }, 500);
      }
    },

    async runMaintenanceDuty(duty, env, context) {
      try {
        await ensureDeploymentIdentityBindings(env);
        await validateFleetChannelTopology(env);
        if (duty === 'deadline') {
          return await runDeadlineDuty(env, duty, context);
        }
        if (duty === 'sweep') {
          const hub = env.HUB;
          const hubTopology = hub
            ? createHubTopology(hub, env.DEPLOYMENT_IDENTITY_SECRET)
            : undefined;
          return await runSlaSweepMaintenance({
            store: approvalStoreFactoryFor(env.DB, storageTablePrefix).store(),
            systemPrincipal: maintenancePrincipal(config.systemPrincipalId),
            deploymentTag: env.DEPLOYMENT_TENANT,
            queue: auditQueueFor(env),
            trigger: duty,
            notify: config.notify?.(env),
            stream: hubTopology
              ? (event) => hubTopology.publish(event)
              : undefined,
          });
        }
        if (duty === 'purge') {
          return await runPurgeMaintenance(env, duty);
        }
        return await runScheduleTickDuty(env, duty);
      } catch (error) {
        const failure = String(error);
        console.error(
          JSON.stringify({
            type: 'maintenance-error',
            surface: `${duty}-duty`,
            trigger: duty,
            error: failure,
          }),
        );
        return { ok: false, error: failure };
      }
    },
  };
}

const MAINTENANCE_HEALTH_KEY = 'flowsafe:maintenance-health:v1';
const MAINTENANCE_NONCES_KEY = 'flowsafe:maintenance-nonces:v1';
const MAINTENANCE_DEADLINE_CURSOR_KEY =
  'flowsafe:maintenance-deadline-cursor:v1';
const DUTY_ORDER = ['deadline', 'sweep', 'purge', 'tick'] as const;

export type MaintenanceDurableObjectConstructor<Env extends FlowsafeWorkerEnv> =
  new (
    state: MaintenanceDurableObjectState,
    env: Env,
  ) => {
    fetch(request: Request): Promise<Response>;
    alarm(): Promise<void>;
  };

/** Build the fixed-name maintenance Durable Object class for one host. */
export function createFlowsafeMaintenanceDurableObject<
  Env extends FlowsafeWorkerEnv,
>(config: FlowsafeWorkerConfig<Env>): MaintenanceDurableObjectConstructor<Env> {
  const intervals: Record<MaintenanceDuty, number | undefined> = {
    deadline:
      config.maintenance.deadlineIntervalMs ??
      config.maintenance.sweepIntervalMs,
    sweep: config.maintenance.sweepIntervalMs,
    purge: config.maintenance.purgeIntervalMs,
    tick: config.maintenance.tickIntervalMs,
  };
  for (const duty of DUTY_ORDER) {
    const interval = intervals[duty];
    if (
      interval !== undefined &&
      (!Number.isSafeInteger(interval) || interval <= 0)
    ) {
      throw new Error(
        `${duty} maintenance interval must be a positive integer`,
      );
    }
  }
  if ((intervals.tick === undefined) !== (config.scheduleTick === undefined)) {
    throw new Error(
      'tickIntervalMs and scheduleTick must either both be configured or both be absent',
    );
  }
  const worker = createFlowsafeWorker(config);

  return class FlowsafeMaintenanceDurableObject {
    readonly #state: MaintenanceDurableObjectState;
    readonly #env: Env;

    constructor(state: MaintenanceDurableObjectState, env: Env) {
      this.#state = state;
      this.#env = env;
    }

    #assertInstanceName(): void {
      if (this.#state.id.name !== MAINTENANCE_INSTANCE_NAME) {
        throw new Error(
          `maintenance must be addressed as '${MAINTENANCE_INSTANCE_NAME}'`,
        );
      }
    }

    async #health(now: number): Promise<InitializedMaintenanceHealth> {
      const stored = await this.#state.storage.get<MaintenanceHealth>(
        MAINTENANCE_HEALTH_KEY,
      );
      const health: InitializedMaintenanceHealth = {
        nextDeadlineAt: validTime(stored?.nextDeadlineAt) ?? now,
        nextSweepAt: validTime(stored?.nextSweepAt) ?? now,
        nextPurgeAt: validTime(stored?.nextPurgeAt) ?? now,
      };
      if (intervals.tick !== undefined) {
        health.nextTickAt = validTime(stored?.nextTickAt) ?? now;
      }
      for (const duty of DUTY_ORDER) {
        if (intervals[duty] === undefined) continue;
        const last = validTime(lastAt(stored, duty));
        if (last !== undefined) setLastAt(health, duty, last);
        const attempted = validTime(lastAttemptAt(stored, duty));
        if (attempted !== undefined) setLastAttemptAt(health, duty, attempted);
        const error = lastError(stored, duty);
        if (error !== undefined) setLastError(health, duty, error);
      }
      return health;
    }

    async #persistAndArm(
      health: InitializedMaintenanceHealth,
      alarmAt: number,
    ): Promise<void> {
      await this.#state.storage.transaction(async (transaction) => {
        await transaction.put(MAINTENANCE_HEALTH_KEY, health);
        await transaction.setAlarm(alarmAt);
      });
    }

    async #recordOutcome(
      duty: MaintenanceDuty,
      completedAt: number,
      outcome: MaintenanceOutcome,
    ): Promise<void> {
      await this.#state.storage.transaction(async (transaction) => {
        const latest = await transaction.get<MaintenanceHealth>(
          MAINTENANCE_HEALTH_KEY,
        );
        if (!latest) throw new Error('maintenance health disappeared');
        setLastAttemptAt(latest, duty, completedAt);
        if (outcome.ok) {
          setLastAt(latest, duty, completedAt);
          clearLastError(latest, duty);
        } else {
          setLastError(latest, duty, outcome.error);
        }
        await transaction.put(MAINTENANCE_HEALTH_KEY, latest);
      });
    }

    async #authorizeCapability(
      request: Request,
      operation: MaintenanceCapabilityOperation,
    ): Promise<MaintenanceCapabilityClaims | undefined> {
      const token = request.headers
        .get('authorization')
        ?.match(/^Bearer\s+(.+)$/i)?.[1];
      if (!token || token.length > 2_048) return undefined;
      const environment = this.#env.FLEET_ENVIRONMENT;
      if (!environment) return undefined;
      let capability: MaintenanceCapabilityClaims | undefined;
      if (this.#env.FLEET_MAINTENANCE_CAPABILITIES === 'required') {
        const encodedPublicKey =
          this.#env.FLEET_MAINTENANCE_CAPABILITY_PUBLIC_KEY;
        if (!encodedPublicKey) return undefined;
        let publicKey: MaintenanceCapabilityJwk;
        try {
          const parsed = JSON.parse(encodedPublicKey) as Readonly<
            Record<string, unknown>
          >;
          if (
            !parsed ||
            typeof parsed !== 'object' ||
            Array.isArray(parsed) ||
            parsed.kty !== 'OKP' ||
            parsed.crv !== 'Ed25519' ||
            parsed.alg !== 'EdDSA' ||
            typeof parsed.kid !== 'string' ||
            !parsed.kid ||
            typeof parsed.x !== 'string' ||
            parsed.d !== undefined
          ) {
            return undefined;
          }
          publicKey = {
            kty: parsed.kty,
            crv: parsed.crv,
            alg: parsed.alg,
            kid: parsed.kid,
            x: parsed.x,
          };
        } catch {
          return undefined;
        }
        capability = await verifyAsymmetricMaintenanceCapability({
          publicKey,
          token,
          operation,
          tenantTag: this.#env.DEPLOYMENT_TENANT,
          environment,
        });
      } else {
        const secret = this.#env.MAINTENANCE_ADMIN_SECRET;
        if (!secret || !MAINTENANCE_ADMIN_SECRET_PATTERN.test(secret)) {
          return undefined;
        }
        capability = await verifyMaintenanceCapability({
          secret,
          token,
          operation,
          tenantTag: this.#env.DEPLOYMENT_TENANT,
          environment,
        });
      }
      if (!capability) return undefined;
      if (operation === 'maintenance-status') return capability;
      const nowSeconds = Math.floor(Date.now() / 1_000);
      const consumed = await this.#state.storage.transaction(
        async (transaction) => {
          const stored =
            (await transaction.get<Record<string, number>>(
              MAINTENANCE_NONCES_KEY,
            )) ?? {};
          if (stored[capability.nonce] !== undefined) return false;
          const live = Object.fromEntries(
            Object.entries(stored).filter(
              ([, expiresAt]) =>
                Number.isSafeInteger(expiresAt) && expiresAt >= nowSeconds,
            ),
          );
          live[capability.nonce] = capability.expiresAt;
          await transaction.put(MAINTENANCE_NONCES_KEY, live);
          return true;
        },
      );
      return consumed ? capability : undefined;
    }

    async #authorizedRequest(
      request: Request,
      operation: MaintenanceCapabilityOperation,
    ): Promise<MaintenanceCapabilityClaims | undefined | false> {
      if (
        this.#env.FLEET_MAINTENANCE_CAPABILITIES !== 'required' &&
        request.headers.has(DEPLOYMENT_IDENTITY_HEADER)
      ) {
        await verifyDurableObjectDeploymentRequest(
          request,
          this.#state,
          this.#env,
        );
        return undefined;
      }
      await verifyDurableObjectDeploymentIdentity(this.#state, this.#env);
      return (await this.#authorizeCapability(request, operation)) ?? false;
    }

    async #response(
      payload: unknown,
      capability: MaintenanceCapabilityClaims | undefined,
    ): Promise<Response> {
      if (!capability) return json(payload);
      const secret = this.#env.MAINTENANCE_ADMIN_SECRET;
      if (!secret) return json({ error: 'maintenance unavailable' }, 503);
      const receipt = await mintMaintenanceReceipt(secret, capability, payload);
      const response = json(payload);
      response.headers.set(MAINTENANCE_RECEIPT_HEADER, receipt);
      return response;
    }

    async fetch(request: Request): Promise<Response> {
      try {
        this.#assertInstanceName();
        const path = new URL(request.url).pathname;
        if (request.method === 'POST' && path === '/ensure') {
          const authorized = await this.#authorizedRequest(
            request,
            'ensure-maintenance',
          );
          if (authorized === false) {
            return json({ error: 'authentication required' }, 401);
          }
          const now = Date.now();
          const health = await this.#health(now);
          const next = nextAlarmAt(health, intervals);
          await this.#persistAndArm(health, Math.max(now, next));
          return this.#response(
            { ...health, alarmAt: Math.max(now, next) },
            authorized,
          );
        }
        if (request.method === 'GET' && path === '/status') {
          const authorized = await this.#authorizedRequest(
            request,
            'maintenance-status',
          );
          if (authorized === false) {
            return json({ error: 'authentication required' }, 401);
          }
          const health = await this.#state.storage.get<MaintenanceHealth>(
            MAINTENANCE_HEALTH_KEY,
          );
          return this.#response(
            {
              ...(health ?? {}),
              alarmAt: await this.#state.storage.getAlarm(),
            },
            authorized,
          );
        }
        return json({ error: 'not found' }, 404);
      } catch (error) {
        if (error instanceof DeploymentIdentityError) {
          return json({ error: 'deployment unavailable' }, 503);
        }
        console.error(
          JSON.stringify({
            type: 'maintenance-do-error',
            reason: error instanceof Error ? error.message : String(error),
          }),
        );
        return json({ error: 'internal error' }, 500);
      }
    }

    async alarm(): Promise<void> {
      this.#assertInstanceName();
      await verifyDurableObjectDeploymentIdentity(this.#state, this.#env);
      const now = Date.now();
      const health = await this.#health(now);
      const duty = DUTY_ORDER.find((candidate) => {
        const dueAt = nextAt(health, candidate);
        return (
          intervals[candidate] !== undefined &&
          dueAt !== undefined &&
          dueAt <= now
        );
      });
      if (!duty) {
        await this.#persistAndArm(health, nextAlarmAt(health, intervals));
        return;
      }

      const interval = intervals[duty];
      const dueAt = nextAt(health, duty);
      if (interval === undefined || dueAt === undefined) {
        throw new Error(`maintenance duty '${duty}' has no schedule`);
      }
      setNextAt(health, duty, advancePast(dueAt, interval, now));
      const followUpAt = nextAlarmAt(health, intervals);
      await this.#persistAndArm(
        health,
        hasDueDuty(health, intervals, now) ? now : followUpAt,
      );

      let context: MaintenanceDutyContext | undefined;
      if (duty === 'deadline') {
        const deadlineCursor = await this.#state.storage.get<RunDeadlineCursor>(
          MAINTENANCE_DEADLINE_CURSOR_KEY,
        );
        context = {
          ...(deadlineCursor ? { deadlineCursor } : {}),
          advanceDeadlineCursor: (cursor) =>
            this.#state.storage.transaction(async (transaction) => {
              await transaction.put(MAINTENANCE_DEADLINE_CURSOR_KEY, cursor);
            }),
        };
      }
      const outcome = await worker.runMaintenanceDuty(duty, this.#env, context);
      await this.#recordOutcome(duty, Date.now(), outcome);
    }
  };
}

function validTime(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0
    ? value
    : undefined;
}

function nextAt(
  health: MaintenanceHealth | undefined,
  duty: MaintenanceDuty,
): number | undefined {
  if (duty === 'deadline') return health?.nextDeadlineAt;
  if (duty === 'sweep') return health?.nextSweepAt;
  if (duty === 'purge') return health?.nextPurgeAt;
  return health?.nextTickAt;
}

function lastAt(
  health: MaintenanceHealth | undefined,
  duty: MaintenanceDuty,
): number | undefined {
  if (duty === 'deadline') return health?.lastDeadlineAt;
  if (duty === 'sweep') return health?.lastSweepAt;
  if (duty === 'purge') return health?.lastPurgeAt;
  return health?.lastTickAt;
}

function lastAttemptAt(
  health: MaintenanceHealth | undefined,
  duty: MaintenanceDuty,
): number | undefined {
  if (duty === 'deadline') return health?.lastDeadlineAttemptAt;
  if (duty === 'sweep') return health?.lastSweepAttemptAt;
  if (duty === 'purge') return health?.lastPurgeAttemptAt;
  return health?.lastTickAttemptAt;
}

function lastError(
  health: MaintenanceHealth | undefined,
  duty: MaintenanceDuty,
): string | undefined {
  const value =
    duty === 'deadline'
      ? health?.lastDeadlineError
      : duty === 'sweep'
        ? health?.lastSweepError
        : duty === 'purge'
          ? health?.lastPurgeError
          : health?.lastTickError;
  return typeof value === 'string' && value.length > 0 ? value : undefined;
}

function setNextAt(
  health: MaintenanceHealth,
  duty: MaintenanceDuty,
  value: number,
): void {
  if (duty === 'deadline') health.nextDeadlineAt = value;
  else if (duty === 'sweep') health.nextSweepAt = value;
  else if (duty === 'purge') health.nextPurgeAt = value;
  else health.nextTickAt = value;
}

function setLastAt(
  health: MaintenanceHealth,
  duty: MaintenanceDuty,
  value: number,
): void {
  if (duty === 'deadline') health.lastDeadlineAt = value;
  else if (duty === 'sweep') health.lastSweepAt = value;
  else if (duty === 'purge') health.lastPurgeAt = value;
  else health.lastTickAt = value;
}

function setLastAttemptAt(
  health: MaintenanceHealth,
  duty: MaintenanceDuty,
  value: number,
): void {
  if (duty === 'deadline') health.lastDeadlineAttemptAt = value;
  else if (duty === 'sweep') health.lastSweepAttemptAt = value;
  else if (duty === 'purge') health.lastPurgeAttemptAt = value;
  else health.lastTickAttemptAt = value;
}

function setLastError(
  health: MaintenanceHealth,
  duty: MaintenanceDuty,
  value: string,
): void {
  const bounded = value.slice(0, 1_024);
  if (duty === 'deadline') health.lastDeadlineError = bounded;
  else if (duty === 'sweep') health.lastSweepError = bounded;
  else if (duty === 'purge') health.lastPurgeError = bounded;
  else health.lastTickError = bounded;
}

function clearLastError(
  health: MaintenanceHealth,
  duty: MaintenanceDuty,
): void {
  if (duty === 'deadline') delete health.lastDeadlineError;
  else if (duty === 'sweep') delete health.lastSweepError;
  else if (duty === 'purge') delete health.lastPurgeError;
  else delete health.lastTickError;
}

function advancePast(dueAt: number, interval: number, now: number): number {
  return dueAt + (Math.floor((now - dueAt) / interval) + 1) * interval;
}

function hasDueDuty(
  health: MaintenanceHealth,
  intervals: Record<MaintenanceDuty, number | undefined>,
  now: number,
): boolean {
  return DUTY_ORDER.some((duty) => {
    const dueAt = nextAt(health, duty);
    return intervals[duty] !== undefined && dueAt !== undefined && dueAt <= now;
  });
}

function nextAlarmAt(
  health: MaintenanceHealth,
  intervals: Record<MaintenanceDuty, number | undefined>,
): number {
  const scheduled = DUTY_ORDER.flatMap((duty) => {
    const dueAt = nextAt(health, duty);
    return intervals[duty] !== undefined && dueAt !== undefined ? [dueAt] : [];
  });
  const next = Math.min(...scheduled);
  if (!Number.isFinite(next)) throw new Error('maintenance has no duties');
  return next;
}
