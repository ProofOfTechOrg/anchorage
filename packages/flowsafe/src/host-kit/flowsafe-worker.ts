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
import type { SnapshotDatabase } from '../do-runner/index.js';
import { purgeExpiredWorkflowRuns } from '../do-runner/index.js';
import { bearerActorAuthenticator } from './bearer-auth.js';
import {
  createDoRunTopology,
  type DoRunTopology,
  type RunnerNamespaceLike,
} from './do-run-topology.js';
import { numberVar, selfDecisionPolicyVar } from './env-vars.js';
import {
  approvalStoreFactoryFor,
  buildHostApprovalService,
  maintenanceActor,
  reconcileApprovalsOnStatusDetached,
  runApprovalRetentionPurge,
  runSlaSweepMaintenance,
} from './host-approval-service.js';
import { createRunRouter } from './run-router.js';
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
  /** Optional audit export: queue producer binding + SIEM collector config. */
  AUDIT_QUEUE?: AuditQueue<ApprovalAuditEvent>;
  SIEM_ENDPOINT?: string;
  SIEM_AUTH_HEADER?: string;
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
      const resolve = buildResolve(
        env,
        topology,
        waitUntil,
        notify,
        selfDecision,
      );

      if (config.preRoutes) {
        const preResponse = await config.preRoutes(request, env, ctx, {
          resolve,
          topology,
        });
        if (preResponse) return preResponse;
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
      const sweep = (): Promise<void> =>
        runSlaSweepMaintenance({
          store: approvalStoreFactoryFor(env.DB).system(),
          systemActor: maintenanceActor(config.systemActorId),
          queue: env.AUDIT_QUEUE,
          cron: controller.cron,
          notify: config.notify?.(env),
        });
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
