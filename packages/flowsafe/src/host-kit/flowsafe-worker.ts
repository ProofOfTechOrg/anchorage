// SPDX-License-Identifier: Apache-2.0
// createFlowsafeWorker — the whole production-Worker skeleton the deploy
// template and the showcase host previously carried as near-byte copies:
// the /healthz → routers → 404 fetch pipeline over ONE tenant resolver, the
// two-cron scheduled() dispatch (sweep vs purge never share an invocation —
// a Workers CPU-limit termination is not a catchable JS error, so sharing
// would let a slow sweep permanently starve the purge), and the audit-export
// queue() consumer. Hosts stay thin shells: they supply their workflows,
// their identity seam (buildVerifier), and their deployment-specific hooks —
// preRoutes (extra unauthenticated/authenticated mounts), wrapResolve
// (e.g. the subdomain cross-check), wrapStart/wrapResume (e.g. a budget
// charge), notify (reviewer-facing transport), and extraPurgeDuties (e.g. a
// demo-tenant reaper). Everything here is structural — host-kit never
// imports @cloudflare/workers-types.

import type {
  ApprovalAuditEvent,
  ApprovalDatabase,
  ApprovalNotificationSink,
  ApprovalStreamSink,
  SelfDecisionPolicy,
  TenantResolver,
} from '../approval-api/index.js';
import {
  APPROVAL_ROLES,
  createApprovalRouter,
  createTenantResolver,
} from '../approval-api/index.js';
import type { AuditMessageBatch, AuditQueue } from '../audit-export/index.js';
import { createAuditQueueHandler } from '../audit-export/index.js';
import type {
  PurgeExpiredBackgroundTasksResult,
  SnapshotDatabase,
  TenantArtifactPurger,
} from '../do-runner/index.js';
import {
  purgeExpiredBackgroundTasks,
  purgeExpiredNotifications,
  purgeExpiredThreadState,
  purgeExpiredThreads,
  purgeExpiredWorkflowRuns,
} from '../do-runner/index.js';
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
  maintenanceActor,
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
  DB: ApprovalDatabase & SnapshotDatabase;
  /** The runner DO namespace createDoRunTopology drives. */
  RUNNER: RunnerNamespaceLike;
  /**
   * Optional per-tenant hub DO namespace for live streaming (DL-009/DL-019).
   * Present together with STREAM_TICKET_SECRET => the composer mounts the stream
   * stage and fans approval mutations out to the tenant hub; either absent =>
   * streaming stays unmounted and the client remains poll-only.
   */
  HUB?: HubNamespaceLike;
  /** Dedicated stream-ticket signing secret (DL-019). Absent => no streaming. */
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
   * Agent-memory thread TTL in days (var; docs/agent-memory-tenancy.md item 7):
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
   * Track C agent-inbox TTL in days (var). The purge cron reaps TERMINAL
   * `mastra_notifications` rows past this age (pending rows are never reaped —
   * one may await a future deliverAt). UNSET/EMPTY/INVALID => the duty does not
   * run and no notification ever expires (opt-in, like THREAD_RETENTION_DAYS —
   * a durable inbox is meant to be readable until the host says otherwise).
   */
  NOTIFICATION_RETENTION_DAYS?: string;
  /**
   * Track C thread-state TTL in days (var). The purge cron reaps
   * `mastra_thread_state` rows (state-signal lanes + goals) untouched for longer
   * than this. UNSET/EMPTY/INVALID => the duty does not run (opt-in; an active
   * goal bumps updatedAt so it never ages out).
   */
  THREAD_STATE_RETENTION_DAYS?: string;
  /** Optional audit export: queue producer binding + SIEM collector config. */
  AUDIT_QUEUE?: AuditQueue<ApprovalAuditEvent>;
  SIEM_ENDPOINT?: string;
  SIEM_AUTH_HEADER?: string;
}

/**
 * Track B background-task TTL cleanup config, surfaced through
 * FlowsafeWorkerConfig (DL-003). Mirrors core BackgroundTaskManager.cleanup's
 * two windows.
 */
export interface BackgroundTasksCleanupConfig {
  /** Completed rows past this age expire. Default 3_600_000 (1h). */
  completedTtlMs?: number;
  /** Failed / cancelled / timed_out rows past this age expire. Default 86_400_000 (24h). */
  failedTtlMs?: number;
}

export interface FlowsafeWorkerConfig<Env extends FlowsafeWorkerEnv> {
  /** The catalog createRunRouter serves and gates (hosts pass their metas). */
  workflows: ReadonlyArray<WorkflowMeta>;
  /**
   * Id for system-created records (the bridge's record creator and the cron
   * maintenance attribution). Must differ from human actor ids or the
   * separation-of-duties check can never fire.
   */
  systemActorId: string;
  /**
   * The identity seam: env -> TokenVerifier. Called once per fetch (and per
   * scheduled notify), so hosts keep their own per-isolate memoization —
   * re-parsing a token map on every request is pure waste, but that is the
   * host's trade to make.
   */
  buildVerifier: (env: Env) => TokenVerifier;
  /**
   * The two cron expressions scheduled() dispatches on. Keep byte-equal to
   * wrangler.jsonc's `triggers.crons`; an unrecognized expression runs BOTH
   * duties sequentially and logs a config-error (availability of both beats
   * purity on a misconfig).
   */
  crons: { sweep: string; purge: string };
  /**
   * Deployment-specific routes tried AFTER /healthz and BEFORE the approval
   * and run routers (the showcase mounts its demo sign-in and sandbox reset
   * here). `kit` hands over the request's already-built resolver and DO
   * topology so a pre-route can share them (the reset route authenticates
   * through the same resolve). Return null to fall through.
   */
  preRoutes?: (
    request: Request,
    env: Env,
    ctx: FlowsafeWorkerContext,
    kit: { resolve: TenantResolver; topology: DoRunTopology },
  ) => Promise<Response | null>;
  /** Wrap the tenant resolver (e.g. withSubdomainCrossCheck behind an env var). */
  wrapResolve?: (resolve: TenantResolver, env: Env) => TenantResolver;
  /** Wrap the run-start thunk (e.g. charge a budget BEFORE the DO round-trip). */
  wrapStart?: (
    start: DoRunTopology['start'],
    env: Env,
  ) => DoRunTopology['start'];
  /** Wrap the raw-resume thunk (metered like starts on budgeted hosts). */
  wrapResume?: (
    resume: DoRunTopology['resume'],
    env: Env,
  ) => DoRunTopology['resume'];
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
   * artifacts beyond even purgeTenant's reach. Pass the same TenantArtifactPurger
   * (an R2ArtifactStore) purgeTenant gets; undefined keeps the byte-identical
   * row-only purge. NOT via extraPurgeDuties — that hook runs AFTER the rows are
   * deleted, when the keys are already unenumerable.
   */
  artifactStore?: TenantArtifactPurger;
  /**
   * Opt-in background-task TTL cleanup (Track B). When present, the purge cron
   * reaps terminal `mastra_background_tasks` rows past the TTL as its OWN
   * failure-isolated duty — the storage-layer belt to a hosting DO's manager
   * cleanup (which needs the DO alive). Absent => no duty, byte-identical
   * (background tasks are opt-in). NOT via extraPurgeDuties: this needs its own
   * try/catch and its own log fields, like every sibling purge duty.
   */
  backgroundTasks?: BackgroundTasksCleanupConfig;
  /**
   * Opt-in Track C signal ingestion stage (P6, DL-006). The host builds its
   * `createSignalRouter` (which needs its per-thread DO namespace via
   * createThreadTopology, plus its audit/rate/allowlist config) and returns it
   * here, closed over the request's resolved TenantResolver; the composer mounts
   * it after preRoutes, ahead of approvals/runs (its `/api/threads/*` routes
   * don't overlap). INJECTED rather than built here because createSignalRouter
   * lives in `signals/`, which imports host-kit — host-kit importing it back
   * would cycle. Absent (or returns undefined) => no signal stage, byte-identical.
   */
  buildSignalRouter?: (
    resolve: TenantResolver,
    env: Env,
  ) => ((request: Request) => Promise<Response | null>) | undefined;
  /**
   * Extra purge-cron duties (e.g. the showcase's demo-tenant reaper). The
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
  // AUTHENTICATE FIRST, then construct (INV-2): the resolver binds the
  // approval store to the verified actor's tenant before any service exists —
  // there is no pre-auth store for a rushed fix to reach.
  const buildResolve = (
    env: Env,
    topology: DoRunTopology,
    waitUntil: (promise: Promise<unknown>) => void,
    notify: ApprovalNotificationSink | undefined,
    selfDecision: SelfDecisionPolicy,
    stream: ApprovalStreamSink | undefined,
  ): TenantResolver => {
    const base = createTenantResolver({
      authenticate: bearerActorAuthenticator(config.buildVerifier(env)),
      storeFactory: approvalStoreFactoryFor(env.DB),
      buildService: (store) =>
        buildHostApprovalService(store, {
          systemActorId: config.systemActorId,
          defaultSlaSeconds: numberVar(
            env.APPROVAL_SLA_SECONDS,
            4 * 60 * 60,
            'APPROVAL_SLA_SECONDS',
          ),
          // The one topology-specific piece: decisions resume the run
          // through its DO stub.
          resumeRun: topology.resumeRecord,
          queue: env.AUDIT_QUEUE,
          waitUntil,
          notify,
          // Fetch-scope hub fan-out: every request-path mutation reaches the
          // tenant hub, kept alive by ctx.waitUntil (DL-020). Undefined when no
          // HUB is bound, so a non-streaming host is byte-identical to before.
          stream,
          allowSelfDecision: selfDecision,
        }),
      // The resolver's canSelfDecide display hint reads the SAME policy the
      // service enforces (passed to buildHostApprovalService above), so the
      // /workflows echo can never contradict decide().
      allowSelfDecision: selfDecision,
    });
    return config.wrapResolve ? config.wrapResolve(base, env) : base;
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
      store: approvalStoreFactoryFor(env.DB).system(),
      retentionDays: env.APPROVAL_RETENTION_DAYS,
      cron,
    });
    // Agent-memory thread TTL, opt-in (docs/agent-memory-tenancy.md item 7).
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
        ...extra,
      }),
    );
  }

  return {
    async fetch(request, env, ctx) {
      const url = new URL(request.url);

      if (request.method === 'GET' && url.pathname === '/healthz') {
        return json({ ok: true });
      }

      const waitUntil = (promise: Promise<unknown>): void =>
        ctx.waitUntil(promise);
      const topology = createDoRunTopology(env.RUNNER);
      const notify = config.notify?.(env);
      const selfDecision = parseSelfDecision(env);
      // Fetch-scope live fan-out sink: present iff a hub is bound (streaming is
      // opt-in, DL-019). Each publish rides ctx.waitUntil (DL-020) and is
      // contained — a failed fan-out logs and never fails the mutation.
      const hub = env.HUB;
      let streamSink: ApprovalStreamSink | undefined;
      if (hub) {
        const hubTopology = createHubTopology(hub);
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

      const approvalResponse = await createApprovalRouter({ resolve })(request);
      if (approvalResponse) return approvalResponse;

      const runResponse = await createRunRouter({
        workflows: config.workflows,
        resolve,
        systemActorId: config.systemActorId,
        start: config.wrapStart
          ? config.wrapStart(topology.start, env)
          : topology.start,
        status: topology.status,
        resume: config.wrapResume
          ? config.wrapResume(topology.resume, env)
          : topology.resume,
        // D4 self-healing, waitUntil-detached — the shared wrapper owns the
        // detach + reconcile-error logging.
        reconcileApprovals: reconcileApprovalsOnStatusDetached(
          config.systemActorId,
          waitUntil,
        ),
      })(request);
      if (runResponse) return runResponse;

      return json({ error: 'not found' }, 404);
    },

    async scheduled(controller, env, ctx) {
      // Dispatch on WHICH cron fired; an unrecognized expression (ops edited
      // wrangler without updating the config) runs both sequentially and
      // logs — availability of both duties beats purity on a misconfig.
      const sweep = (): Promise<void> => {
        const hub = env.HUB;
        const hubTopology = hub ? createHubTopology(hub) : undefined;
        return runSlaSweepMaintenance({
          store: approvalStoreFactoryFor(env.DB).system(),
          systemActor: maintenanceActor(config.systemActorId),
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
      await createAuditQueueHandler({
        endpoint: env.SIEM_ENDPOINT,
        authHeader: env.SIEM_AUTH_HEADER,
      })(batch);
    },
  };
}
