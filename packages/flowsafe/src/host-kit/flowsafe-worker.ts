// SPDX-License-Identifier: Apache-2.0
// createFlowsafeWorker — the whole production-Worker skeleton the deploy
// template and the showcase host previously carried as near-byte copies:
// the /healthz → routers → 404 fetch pipeline over one actor resolver, the
// failure-isolated scheduled() dispatch (sweep and purge never share an
// invocation, and an optional schedule tick gets its own), and the audit-export
// queue() consumer. Hosts stay thin shells: they supply their workflows,
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
  ApprovalStreamSink,
  SelfDecisionPolicy,
} from '../approval-api/index.js';
import {
  APPROVAL_ROLES,
  createActorResolver,
  createApprovalRouter,
  createResourceOwnershipSchema,
  RESOURCE_OWNERSHIP_TABLE,
} from '../approval-api/index.js';
import type { AuditMessageBatch, AuditQueue } from '../audit-export/index.js';
import { createAuditQueueHandler } from '../audit-export/index.js';
import type {
  DeploymentIdentityDatabase,
  PurgeExpiredBackgroundTasksResult,
  RunArtifactPurger,
  SnapshotDatabase,
} from '../do-runner/index.js';
import {
  DeploymentIdentityError,
  ensureDeploymentIdentityBindings,
  purgeExpiredBackgroundTasks,
  purgeExpiredNotifications,
  purgeExpiredScheduleTriggers,
  purgeExpiredThreadState,
  purgeExpiredThreads,
  purgeExpiredWorkflowRuns,
} from '../do-runner/index.js';
import type { ResumeRunFn } from './approval-bridge.js';
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
  maintenancePrincipal,
  reconcileApprovalsOnStatusDetached,
  runApprovalRetentionPurge,
  runSlaSweepMaintenance,
} from './host-approval-service.js';
import { createHubTopology, type HubNamespaceLike } from './hub-topology.js';
import { createRunRouter } from './run-router.js';
import { createStreamRouter } from './stream-router.js';
import type { TokenVerifier } from './verifier.js';
import type { WorkflowMeta } from './workflow-meta.js';

/** The ExecutionContext subset the composer needs (structural, like the rest of host-kit). */
export interface FlowsafeWorkerContext {
  waitUntil(promise: Promise<unknown>): void;
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
  /** Per-deployment Worker-to-Durable-Object credential. */
  DEPLOYMENT_IDENTITY_SECRET: string;
  /** The runner DO namespace createDoRunTopology drives. */
  RUNNER: RunnerNamespaceLike;
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
  /** Cron purges terminal run snapshots older than this (var; default 30; 0 = immediately). */
  RUN_RETENTION_DAYS?: string;
  /** Cron purges DECIDED approval records older than this (var; default 30; 0 = immediately). */
  APPROVAL_RETENTION_DAYS?: string;
  /**
   * Agent-memory thread TTL in days (docs/agent-memory-isolation.md#thread-retention):
   * the purge cron deletes threads untouched for longer than this, with their
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
   * Agent-inbox TTL in days. The purge cron reaps terminal
   * `mastra_notifications` rows past this age (pending rows are never reaped —
   * one may await a future deliverAt). UNSET/EMPTY/INVALID => the duty does not
   * run and no notification ever expires (opt-in, like THREAD_RETENTION_DAYS —
   * a durable inbox is meant to be readable until the host says otherwise).
   */
  NOTIFICATION_RETENTION_DAYS?: string;
  /**
   * Thread-state TTL in days. The purge cron reaps
   * `mastra_thread_state` rows (state-signal lanes + goals) untouched for longer
   * than this. UNSET/EMPTY/INVALID => the duty does not run (opt-in; an active
   * goal bumps updatedAt so it never ages out).
   */
  THREAD_STATE_RETENTION_DAYS?: string;
  /**
   * Schedule-trigger history TTL in days. The purge cron reaps
   * `mastra_schedule_triggers` rows past this age by their `actualFireAt`.
   * UNSET/EMPTY/INVALID => the duty does not run (opt-in; a schedule's fire
   * history is inspectable until the host sets a window). Schedule rows are
   * standing config with no TTL; an authorized DELETE removes them, and
   * deployment decommissioning removes any that remain.
   */
  SCHEDULE_TRIGGER_RETENTION_DAYS?: string;
  /** Optional audit export: queue producer binding + SIEM collector config. */
  AUDIT_QUEUE?: AuditQueue<ApprovalAuditEvent>;
  SIEM_ENDPOINT?: string;
  SIEM_AUTH_HEADER?: string;
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

export interface FlowsafeWorkerConfig<Env extends FlowsafeWorkerEnv> {
  /** The catalog createRunRouter serves and gates (hosts pass their metas). */
  workflows: ReadonlyArray<WorkflowMeta>;
  /**
   * System-principal id for bridge bookkeeping and cron maintenance
   * attribution. Requester kind is persisted separately, so ids may overlap
   * across principal kinds.
   */
  systemPrincipalId: string;
  /**
   * The identity seam: env -> TokenVerifier. Called once per fetch (and per
   * scheduled notify), so hosts keep their own per-isolate memoization —
   * re-parsing a token map on every request is pure waste, but that is the
   * host's trade to make.
   */
  buildVerifier: (env: Env) => TokenVerifier;
  /**
   * The cron expressions scheduled() dispatches on. `sweep` + `purge` are
   * required and must never share an invocation. `tick` is optional for
   * schedules: when set and `scheduleTick` is provided, the schedule tick runs
   * on it as its OWN failure-isolated invocation (a runaway fire pass gets its
   * own CPU budget, the same rationale that keeps sweep and purge apart). Keep
   * these byte-equal to wrangler.jsonc's `triggers.crons`; an unrecognized
   * expression runs the sweep + purge duties sequentially and logs a
   * config-error (availability beats purity on a misconfig). Absent `tick` ⇒ no
   * schedule-tick invocation, byte-identical.
   */
  crons: { sweep: string; purge: string; tick?: string };
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
   * Compose approval-driven resume handling. Agent hosts use this to handle
   * agent-thread targets and delegate generic workflow targets to fallback.
   */
  buildResumeRun?: (fallback: ResumeRunFn, env: Env) => ResumeRunFn;
  /**
   * Reviewer-facing notification transport (ApprovalNotificationSink) for
   * created records and SLA escalations. Built per invocation from env
   * (transports usually need secrets); undefined = no notifications.
   */
  notify?: (env: Env) => ApprovalNotificationSink | undefined;
  /**
   * When set, the retention purge (runPurgeMaintenance -> the built-in
   * purgeExpiredWorkflowRuns) deletes each expired run's R2 artifacts WITH its
   * snapshot row. The snapshot row is the only enumerable record of a run's
   * artifact keys (R2 keys lead with workflowId — there is no run-level listing
   * without it), so a retention purge without this pairing strands the run's
   * artifacts until deployment teardown. Pass the same RunArtifactPurger
   * (an R2ArtifactStore); undefined keeps the byte-identical
   * row-only purge. NOT via extraPurgeDuties — that hook runs AFTER the rows are
   * deleted, when the keys are already unenumerable.
   */
  artifactStore?: RunArtifactPurger;
  /**
   * Opt-in background-task TTL cleanup. When present, the purge cron
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
   * it on the `crons.tick` cron as its OWN failure-isolated duty (own try/catch,
   * own `schedule-tick` log line). INJECTED (not built here, structurally typed as
   * `() => Promise<unknown>`) because createScheduleTick lives in `schedules/`,
   * which transitively imports host-kit — host-kit importing it back would cycle.
   * Absent (or `crons.tick` unset) ⇒ no tick invocation, byte-identical.
   */
  scheduleTick?: (env: Env) => (() => Promise<unknown>) | undefined;
  /**
   * Extra purge-cron duties. The
   * returned fields fold into the ONE combined `{type:'maintenance'}` log
   * line. Isolated: a throw here logs a maintenance-error and never blocks
   * the combined log — though duties SHOULD own their try/catch so their
   * error surface stays specific.
   */
  extraPurgeDuties?: (
    env: Env,
    cron: string,
  ) => Promise<Record<string, unknown>>;
}

/** The handler triple a host re-exports (behind its own workers-typed casts). */
export interface FlowsafeWorker<Env extends FlowsafeWorkerEnv> {
  fetch(
    request: Request,
    env: Env,
    ctx: FlowsafeWorkerContext,
  ): Promise<Response>;
  scheduled(
    controller: { cron: string },
    env: Env,
    ctx: FlowsafeWorkerContext,
  ): Promise<void>;
  queue(batch: AuditMessageBatch<unknown>, env: Env): Promise<void>;
}

function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

export function createFlowsafeWorker<Env extends FlowsafeWorkerEnv>(
  config: FlowsafeWorkerConfig<Env>,
): FlowsafeWorker<Env> {
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
      storeFactory: approvalStoreFactoryFor(env.DB),
      deploymentTag: env.DEPLOYMENT_TENANT,
      buildService: (store) =>
        buildHostApprovalService(store, {
          deploymentTag: env.DEPLOYMENT_TENANT,
          systemPrincipalId: config.systemPrincipalId,
          defaultSlaSeconds: numberVar(
            env.APPROVAL_SLA_SECONDS,
            4 * 60 * 60,
            'APPROVAL_SLA_SECONDS',
          ),
          // The one topology-specific piece: decisions resume the run
          // through its DO stub.
          resumeRun: config.buildResumeRun
            ? config.buildResumeRun(topology.resumeRecord, env)
            : topology.resumeRecord,
          queue: env.AUDIT_QUEUE,
          waitUntil,
          notify,
          // Fetch-scope hub fan-out: every request-path mutation reaches the
          // deployment hub, kept alive by ctx.waitUntil (DL-020). Undefined when no
          // HUB is bound, so a non-streaming host is byte-identical to before.
          stream,
          allowSelfDecision: selfDecision,
        }),
      // The resolver's canSelfDecide display hint reads the SAME policy the
      // service enforces (passed to buildHostApprovalService above), so the
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

  async function runPurgeMaintenance(env: Env, cron: string): Promise<void> {
    let purged: number | undefined;
    try {
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
        artifactStore: config.artifactStore,
        // Snapshot + owner release share one D1 transaction, so retention
        // cannot leave unbounded run-ownership tombstones or expose a live row
        // without its authorization record.
        resourceOwnerTable: RESOURCE_OWNERSHIP_TABLE,
      });
    } catch (error) {
      console.error(
        JSON.stringify({
          type: 'maintenance-error',
          surface: 'retention-purge',
          cron,
          error: String(error),
        }),
      );
    }
    // Own containment (inside runApprovalRetentionPurge), same isolation as
    // the snapshot purge above: a failure in any one purge duty must never
    // stop the others.
    const approvalsPurged = await runApprovalRetentionPurge({
      store: approvalStoreFactoryFor(env.DB).store(),
      retentionDays: env.APPROVAL_RETENTION_DAYS,
      cron,
    });
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
        });
        threadsPurged = purgedThreads.threads;
        threadMessagesPurged = purgedThreads.messages;
      } catch (error) {
        console.error(
          JSON.stringify({
            type: 'maintenance-error',
            surface: 'thread-retention-purge',
            cron,
            error: String(error),
          }),
        );
      }
    }
    // Background-task TTL cleanup (Track B, opt-in). Its OWN try/catch, like
    // every sibling duty: a wedged background-task purge must cost the
    // run-snapshot, approval, and thread purges nothing.
    let backgroundTasksPurged: PurgeExpiredBackgroundTasksResult | undefined;
    if (config.backgroundTasks) {
      try {
        backgroundTasksPurged = await purgeExpiredBackgroundTasks(env.DB, {
          completedTtlMs: config.backgroundTasks.completedTtlMs,
          failedTtlMs: config.backgroundTasks.failedTtlMs,
        });
      } catch (error) {
        console.error(
          JSON.stringify({
            type: 'maintenance-error',
            surface: 'background-task-purge',
            cron,
            error: String(error),
          }),
        );
      }
    }
    // Track C notification TTL (opt-in). Its OWN try/catch, like every sibling
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
        });
      } catch (error) {
        console.error(
          JSON.stringify({
            type: 'maintenance-error',
            surface: 'notification-purge',
            cron,
            error: String(error),
          }),
        );
      }
    }
    // Track C thread-state TTL (opt-in). Same isolation + opt-in posture.
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
        });
      } catch (error) {
        console.error(
          JSON.stringify({
            type: 'maintenance-error',
            surface: 'thread-state-purge',
            cron,
            error: String(error),
          }),
        );
      }
    }
    // Track D schedule-trigger history TTL (opt-in). Same isolation + opt-in
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
        });
      } catch (error) {
        console.error(
          JSON.stringify({
            type: 'maintenance-error',
            surface: 'schedule-trigger-purge',
            cron,
            error: String(error),
          }),
        );
      }
    }
    let extra: Record<string, unknown> = {};
    if (config.extraPurgeDuties) {
      try {
        extra = await config.extraPurgeDuties(env, cron);
      } catch (error) {
        console.error(
          JSON.stringify({
            type: 'maintenance-error',
            surface: 'extra-purge-duties',
            cron,
            error: String(error),
          }),
        );
      }
    }
    console.log(
      JSON.stringify({
        type: 'maintenance',
        cron,
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
  }

  // Track D schedule tick — its OWN failure-isolated duty (own try/catch, own log
  // line), run on the `crons.tick` invocation. A wedged fire pass must cost the
  // sweep + purge nothing, and vice versa (the same failure-isolation rationale).
  async function runScheduleTickDuty(env: Env, cron: string): Promise<void> {
    const tick = config.scheduleTick?.(env);
    if (!tick) {
      // The tick cron fired but no scheduleTick builder is wired — a misconfig.
      // Do NOT fall through to sweep/purge (this invocation is the tick's).
      console.error(
        JSON.stringify({
          type: 'config-error',
          var: 'crons.tick',
          cron,
          reason: 'tick cron fired but no scheduleTick builder is configured',
        }),
      );
      return;
    }
    try {
      const result = await tick();
      console.log(JSON.stringify({ type: 'schedule-tick', cron, result }));
    } catch (error) {
      console.error(
        JSON.stringify({
          type: 'schedule-tick-error',
          cron,
          error: String(error),
        }),
      );
    }
  }

  return {
    async fetch(request, env, ctx) {
      try {
        await ensureDeploymentIdentityBindings(env);
        const url = new URL(request.url);

        if (request.method === 'GET' && url.pathname === '/healthz') {
          return json({ ok: true });
        }

        const waitUntil = (promise: Promise<unknown>): void =>
          ctx.waitUntil(promise);
        const topology = createDoRunTopology(
          env.RUNNER,
          env.DEPLOYMENT_IDENTITY_SECRET,
        );
        const notify = config.notify?.(env);
        const selfDecision = parseSelfDecision(env);
        // Fetch-scope live fan-out sink: present iff a hub is bound (streaming is
        // opt-in, DL-019). Each publish rides ctx.waitUntil (DL-020) and is
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

        // Optional stream stage (DL-015/DL-019): mounted only when BOTH the hub
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

        // Optional Track C signal stage (P6): the host-built createSignalRouter,
        // closed over THIS request's resolver. `/api/threads/*` — composes ahead
        // of approvals/runs without overlap. Absent seam => unmounted.
        const signalRouter = config.buildSignalRouter?.(resolve, env);
        if (signalRouter) {
          const signalResponse = await signalRouter(request);
          if (signalResponse) return signalResponse;
        }

        // Optional Track F goal stage (P6-lite, DL-018): the host-built
        // createObjectiveRouter, closed over THIS request's resolver.
        // `/api/threads/:threadId/goal` — composes after signals (non-overlapping)
        // and ahead of approvals/runs. Absent seam ⇒ unmounted, byte-identical.
        const objectiveRouter = config.buildObjectiveRouter?.(resolve, env);
        if (objectiveRouter) {
          const objectiveResponse = await objectiveRouter(request);
          if (objectiveResponse) return objectiveResponse;
        }

        // Optional Track D schedule CRUD stage (DL-013): the host-built
        // createScheduleRouter, closed over THIS request's resolver. `/api/schedules/*`
        // — composes after goals (non-overlapping), ahead of approvals/runs. Absent
        // seam ⇒ unmounted, byte-identical.
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
          beforeStart: beforeStart
            ? (context, workflowId, inputData) =>
                beforeStart(context, env, workflowId, inputData)
            : undefined,
          beforeResume: beforeResume
            ? (context, workflowId, runId, body) =>
                beforeResume(context, env, workflowId, runId, body)
            : undefined,
          // D4 self-healing, waitUntil-detached — the shared wrapper owns the
          // detach + reconcile-error logging.
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

    async scheduled(controller, env, ctx) {
      await ensureDeploymentIdentityBindings(env);
      // Dispatch on WHICH cron fired; an unrecognized expression (ops edited
      // wrangler without updating the config) runs both sequentially and
      // logs — availability of both duties beats purity on a misconfig.
      const sweep = (): Promise<void> => {
        const hub = env.HUB;
        const hubTopology = hub
          ? createHubTopology(hub, env.DEPLOYMENT_IDENTITY_SECRET)
          : undefined;
        return runSlaSweepMaintenance({
          store: approvalStoreFactoryFor(env.DB).store(),
          systemPrincipal: maintenancePrincipal(config.systemPrincipalId),
          deploymentTag: env.DEPLOYMENT_TENANT,
          queue: env.AUDIT_QUEUE,
          cron: controller.cron,
          notify: config.notify?.(env),
          // BARE hub-publish thunk, NOT waitUntil-wrapped: a scheduled() handler
          // runs under its own waitUntil, so runSlaSweepMaintenance collects
          // each publish into pendingSends and awaits it via Promise.all (DL-020).
          stream: hubTopology
            ? (event) => hubTopology.publish(event)
            : undefined,
        });
      };
      if (controller.cron === config.crons.sweep) {
        ctx.waitUntil(sweep());
      } else if (controller.cron === config.crons.purge) {
        ctx.waitUntil(runPurgeMaintenance(env, controller.cron));
      } else if (
        config.crons.tick !== undefined &&
        controller.cron === config.crons.tick
      ) {
        // Track D: the schedule tick's OWN invocation (opt-in; unset ⇒ this
        // branch is unreachable, byte-identical).
        ctx.waitUntil(runScheduleTickDuty(env, controller.cron));
      } else {
        console.error(
          JSON.stringify({
            type: 'config-error',
            var: 'triggers.crons',
            raw: controller.cron,
            reason:
              'unknown cron expression; running both maintenance surfaces',
          }),
        );
        ctx.waitUntil(
          sweep()
            // Both duties are self-contained today, but the JOIN must not
            // depend on that: `.then(f)` skips f on rejection, and the whole
            // point of this fallback is availability of BOTH duties.
            .catch(() => {})
            .then(() => runPurgeMaintenance(env, controller.cron)),
        );
      }
    },

    // Audit-export consumer (active only when a queue consumer binding
    // exists): ships each batch to the SIEM collector; a failed export
    // retries the batch, so nothing is acked unconfirmed.
    async queue(batch, env) {
      await ensureDeploymentIdentityBindings(env);
      await createAuditQueueHandler({
        endpoint: env.SIEM_ENDPOINT,
        authHeader: env.SIEM_AUTH_HEADER,
      })(batch);
    },
  };
}
