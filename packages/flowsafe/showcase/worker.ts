// Anchorage Showcase — the runnable multi-workflow host.
//
// The five design-sketch workflows grown into an actual deployed service: all
// five (gtm-outbound, content-pipeline, lead-generation, product-launch,
// access-request) running on a real Cloudflare Worker + Durable Object + D1,
// behind the approval queue, bearer auth, and cron maintenance. This file is
// host wiring only: it delegates every workflow to buildShowcaseRuntime(), and
// auth + the run routes + the approval bridge to src/host-kit (shared with
// `deploy/worker.ts`). What stays here is this host's topology — the DO-stub
// start/status/resume thunks — plus cron maintenance and the Queues audit
// export.
//
// Side effects are binding-gated in each connector (see showcase/workflows/):
// with no binding a connector simulates its side effect (envelope/preview
// logged) while still exercising the approval-grant gate. Bind `EMAIL` (and the
// CRM/deploy egress, wired in defineWorkflows) to go live; a forged resume fails
// closed regardless.
//
// Routes (the /workflows + /runs surface is host-kit's createRunRouter; the
// /api/approvals surface is approval-api's createApprovalRouter):
//   GET  /workflows                       -> module catalog (id/title/sampleInput/
//                                            allowedRoles) for the launcher
//   POST /runs { workflowId, inputData }  -> start; a suspension auto-queues
//                                            an approval (response.approval).
//                                            A module's meta.allowedRoles gates
//                                            who may start it (per-workflow RBAC)
//   GET  /runs/:workflowId/:runId         -> status projection
//   POST /runs/:workflowId/:runId/resume  -> raw resume (no grants; the gated
//                                            step fails closed — approve via
//                                            the queue instead)
//   *    /api/approvals[...]              -> approval queue REST surface. The
//                                            create route stays OFF (allowCreate
//                                            defaults false): approval records
//                                            are minted in-process from an
//                                            observed suspension, never from a
//                                            request body.
//   GET  /healthz                         -> liveness (unauthenticated)
//
// All routes except /healthz and /auth/* require `Authorization: Bearer
// <token>`. Two verifiers feed one seam: the APPROVAL_ACTOR_TOKENS static map
// (local dev / operators) and, when DEMO_JWT_SECRET is set, the public demo's
// OAuth-minted HS256 JWTs (see demo-auth.ts: GET /auth/<provider> ->
// per-visitor ephemeral tenant + a four-role token set). Neither is baked in:
// a deploy without secrets 401s everywhere (fail closed by construction).
// DEMO_DISABLED=true is the kill switch — checked in the AUTH middleware, so
// already-issued demo JWTs die with it, and at the mint/refresh routes.
// Local dev reads showcase/.dev.vars.

import type {
  D1Database,
  DurableObjectNamespace,
  ExecutionContext,
  ExportedHandler,
  MessageBatch,
  Queue,
  Request as CfRequest,
  ScheduledController,
} from '@cloudflare/workers-types';
import { AuditLogger } from '@proofoftech/breakwater';

import {
  type ApprovalActor,
  approvalGrantProvider,
  ApprovalService,
  createApprovalRouter,
  createTenantResolver,
  D1ApprovalStoreFactory,
  defaultResumeData,
  sweepSLA,
  type TenantBoundApprovalStore,
  type TenantResolver,
} from '../src/approval-api/index.js';
import {
  createAuditQueueConsumer,
  queueAuditSink,
} from '../src/audit-export/index.js';
import {
  DurableObjectRunner,
  purgeExpiredWorkflowRuns,
  purgeTenant,
  type RunnerRuntime,
  type RunSummary,
  tenantOfRunId,
} from '../src/do-runner/index.js';
import {
  bearerActorAuthenticator,
  createRunRouter,
  doSummary,
  hmacVerifier,
  parseActorTokens,
  resumeRunWithRequeue,
  RunRouteError,
  staticTokenVerifier,
  type TokenVerifier,
} from '../src/host-kit/index.js';
import {
  consumeRunBudget,
  createDemoAuthRouter,
  DEMO_JWT_AUDIENCE,
  DEMO_JWT_ISSUER,
  DEMO_JWT_KID,
  DemoRunLimitError,
  githubProvider,
  googleProvider,
  isDemoTenant,
  type OAuthProvider,
  purgeExpiredDemoTenants,
} from './demo-auth.js';
import {
  buildShowcaseRuntime,
  SHOWCASE_MODULES,
  type EmailServiceBinding,
} from './runtime.js';

interface Env {
  DB: D1Database;
  RUNNER: DurableObjectNamespace;
  /**
   * Optional Cloudflare Email Service binding. Absent (Phase A) => the outreach
   * connector simulates the send (logs the envelope, sends nothing). Bound =>
   * real transactional send from an onboarded domain.
   */
  EMAIL?: EmailServiceBinding;
  /** Envelope sender identity for the outreach connector (vars). */
  OUTREACH_FROM_ADDRESS?: string;
  OUTREACH_FROM_NAME?: string;
  /**
   * Secret (`wrangler secret put APPROVAL_ACTOR_TOKENS`): JSON map of bearer
   * token -> actor, e.g. {"<random-token>": {"id": "ray", "role":
   * "reviewer"}}. Absent => an empty map => every authenticated route 401s.
   * Swap bearerActorAuthenticator for your SSO/JWT verification to replace it —
   * actor mapping stays inside the trusted computing base either way.
   */
  APPROVAL_ACTOR_TOKENS?: string;
  /** Default SLA seconds for new approvals (var; default 14400 = 4h). */
  APPROVAL_SLA_SECONDS?: string;
  /**
   * Public-demo switches. DEMO_JWT_SECRET (secret) turns the OAuth demo on;
   * the OAuth app is named by GOOGLE_CLIENT_ID (var) + GOOGLE_CLIENT_SECRET
   * (secret), or GITHUB_CLIENT_ID/GITHUB_CLIENT_SECRET — a provider counts
   * only with its FULL pair (a half-set pair is a config-error and stays
   * unmounted); when both pairs are configured, Google wins (the launch
   * provider; the router mounts ONE). DEMO_DISABLED=true is the kill switch
   * (auth middleware + mint). Caps are vars so ops can tune without a deploy.
   */
  DEMO_JWT_SECRET?: string;
  GOOGLE_CLIENT_ID?: string;
  GOOGLE_CLIENT_SECRET?: string;
  GITHUB_CLIENT_ID?: string;
  GITHUB_CLIENT_SECRET?: string;
  DEMO_DISABLED?: string;
  /** Max runs per demo tenant lifetime (var; default 20). */
  DEMO_TENANT_RUN_CAP?: string;
  /** Global demo runs per UTC day — the spend backstop (var; default 500). */
  DEMO_DAILY_RUN_CAP?: string;
  /** Demo tenant lifetime hours (var; default 24). */
  DEMO_TENANT_TTL_HOURS?: string;
  /** Demo JWT lifetime seconds (var; default 3600 = 1h, silent refresh). */
  DEMO_JWT_TTL_SECONDS?: string;
  /** Cron purges terminal run snapshots older than this (var; default 30). */
  RUN_RETENTION_DAYS?: string;
  /**
   * Optional audit export to a SIEM: bind a queue producer (wrangler.jsonc
   * `queues` block) and audit events flow producer -> queue -> the `queue`
   * consumer below -> HTTP POST to SIEM_ENDPOINT (auth via the SIEM_AUTH_HEADER
   * secret). Without the binding, audit stays on structured Workers Logs only.
   */
  AUDIT_QUEUE?: Queue;
  SIEM_ENDPOINT?: string;
  SIEM_AUTH_HEADER?: string;
}

/** Id for system-created records; the tenant is bound per request/instance. */
const SYSTEM_ACTOR_ID = 'flowsafe-worker';

/**
 * Attribution identity for the cron SLA sweep (audit only — the sweep is TCB
 * code over the system store; per-record tenants ride in the audit detail).
 */
const MAINTENANCE_ACTOR: ApprovalActor = {
  id: SYSTEM_ACTOR_ID,
  role: 'operator',
  tenantId: 'system',
};

function defineWorkflows(env: Env, tenantId: string): RunnerRuntime {
  return buildShowcaseRuntime({
    initInput: env,
    // The DO topology binds the grant store to THIS instance's tenant,
    // recovered from the DO's own idFromName identity (INV-1 -> INV-2).
    grantProvider: approvalGrantProvider(
      approvalStoreFactory(env.DB).forTenant(tenantId),
    ),
    email: env.EMAIL,
    fromAddress: env.OUTREACH_FROM_ADDRESS,
    fromName: env.OUTREACH_FROM_NAME,
    // Connector audit streams to Workers Logs alongside the approval audit.
    audit: new AuditLogger({
      sink: (event) =>
        console.log(JSON.stringify({ type: 'connector-audit', ...event })),
    }),
    // crm/deploy egress bindings + artifact bucket are left unset: the showcase
    // runs offline (R2 via InMemoryArtifactBucket, CRM/deploy simulated). Bind
    // them in wrangler.jsonc + wire them here to go live per connector.
  });
}

export class ShowcaseRunner extends DurableObjectRunner<Env> {
  protected build(env: Env): RunnerRuntime {
    return defineWorkflows(env, this.tenantId);
  }
}

function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

function runStub(
  env: Env,
  workflowId: string,
  runId: string,
): ReturnType<DurableObjectNamespace['get']> {
  return env.RUNNER.get(env.RUNNER.idFromName(`${workflowId}:${runId}`));
}

function numberVar(
  raw: string | undefined,
  fallback: number,
  name: string,
): number {
  if (raw === undefined || raw === '') return fallback;
  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0) {
    // Fall back rather than fail: maintenance must keep running on a typo'd
    // var, and the log line is the operator's tripwire.
    console.error(
      JSON.stringify({ type: 'config-error', var: name, raw, fallback }),
    );
    return fallback;
  }
  return value;
}

/**
 * The composed verifier: the static operator map, plus (when configured) the
 * demo's HS256 JWTs. DEMO_DISABLED kills the JWT path entirely — issued
 * tokens stop verifying, not just new mints.
 */
export function buildVerifier(env: Env): TokenVerifier {
  const staticVerifier = staticTokenVerifier(
    parseActorTokens(env.APPROVAL_ACTOR_TOKENS),
  );
  const secret = env.DEMO_JWT_SECRET;
  if (!secret || env.DEMO_DISABLED === 'true') return staticVerifier;
  const demoVerifier = hmacVerifier({
    keys: new Map([[DEMO_JWT_KID, secret]]),
    issuer: DEMO_JWT_ISSUER,
    audience: DEMO_JWT_AUDIENCE,
  });
  return {
    async verify(token) {
      return (
        (await staticVerifier.verify(token)) ??
        (await demoVerifier.verify(token))
      );
    },
  };
}

/**
 * A credential pair counts only when BOTH halves are present: an id without
 * its secret mounts a sign-in that always dies at the token exchange — and
 * masks a fully-configured fallback provider. A half-set pair fails closed
 * (skipped) with the missing var named in the log.
 */
function oauthPair(
  idVar: string,
  id: string | undefined,
  secretVar: string,
  secret: string | undefined,
): { clientId: string; clientSecret: string } | undefined {
  if (id && secret) return { clientId: id, clientSecret: secret };
  if (id || secret) {
    const present = id ? idVar : secretVar;
    const missing = id ? secretVar : idVar;
    console.error(
      JSON.stringify({
        type: 'config-error',
        var: missing,
        reason: `${present} is set without ${missing} — sign-in cannot complete, so the provider stays unmounted`,
      }),
    );
  }
  return undefined;
}

/**
 * One OAuth provider mounts per deployment: Google when configured (the
 * launch provider), else GitHub, else none (the /auth/* routes stay
 * unmounted). Configured means the FULL id+secret pair — a half-set pair is
 * a config-error and never mounts (see oauthPair). Identities are
 * provider-scoped (`google:<sub>` vs `github:<id>`), so switching providers
 * mints fresh sandboxes rather than colliding subjects. Both pairs set is
 * tolerated — Google wins — but logged (same tripwire convention as
 * numberVar's config-error), so stale fallback credentials never linger
 * silently. The fetch handler selects per request, but only while the demo
 * is on (DEMO_JWT_SECRET set): a half-set pair on a demo-off deployment is
 * inert, and error-logging it every request would be noise.
 */
export function selectOAuthProvider(env: Env): OAuthProvider | undefined {
  const google = oauthPair(
    'GOOGLE_CLIENT_ID',
    env.GOOGLE_CLIENT_ID,
    'GOOGLE_CLIENT_SECRET',
    env.GOOGLE_CLIENT_SECRET,
  );
  const github = oauthPair(
    'GITHUB_CLIENT_ID',
    env.GITHUB_CLIENT_ID,
    'GITHUB_CLIENT_SECRET',
    env.GITHUB_CLIENT_SECRET,
  );
  if (google) {
    if (github) {
      console.warn(
        JSON.stringify({
          type: 'config-warning',
          var: 'GITHUB_CLIENT_ID',
          reason:
            'the Google and GitHub OAuth pairs are both set — Google mounts; remove the stale GitHub credentials',
        }),
      );
    }
    return googleProvider(google);
  }
  if (github) {
    return githubProvider(github);
  }
  return undefined;
}

// One factory per isolate, not per request: it owns the memoized schema-init
// promise, so rebuilding it inside fetch() would re-run the whole DDL pass
// (CREATE TABLE + DROP/CREATE indexes + PRAGMA + ALTERs) on every request.
// Keyed by the D1 binding, which is stable for an isolate's lifetime.
const approvalFactories = new WeakMap<D1Database, D1ApprovalStoreFactory>();

function approvalStoreFactory(db: D1Database): D1ApprovalStoreFactory {
  let factory = approvalFactories.get(db);
  if (!factory) {
    factory = new D1ApprovalStoreFactory(db);
    approvalFactories.set(db, factory);
  }
  return factory;
}

/** Demo sandboxes pay for runs from two atomic budgets; others start freely. */
async function chargeDemoBudget(env: Env, runId: string): Promise<void> {
  const tenantId = tenantOfRunId(runId);
  // Demo-ness is read from `tenants.kind` (the allocation authority), never
  // guessed from the slug: a commercial tenant slugged 'dmart' would be
  // charged against a demo_tenants row that does not exist and 429 forever,
  // and a change to the demo slug scheme would silently stop budgeting the
  // traffic the ceiling exists to bound.
  if (tenantId === undefined || !(await isDemoTenant(env.DB, tenantId))) {
    return;
  }
  try {
    await consumeRunBudget(env.DB, {
      tenantId,
      tenantRunCap: numberVar(
        env.DEMO_TENANT_RUN_CAP,
        20,
        'DEMO_TENANT_RUN_CAP',
      ),
      dailyRunCap: numberVar(env.DEMO_DAILY_RUN_CAP, 500, 'DEMO_DAILY_RUN_CAP'),
    });
  } catch (error) {
    if (error instanceof DemoRunLimitError) {
      throw new RunRouteError(429, error.message);
    }
    throw error;
  }
}

// The auth seam (parseActorTokens + bearerActorAuthenticator), the run routes
// with their RBAC gate order, and the DO-response reader (doSummary) live in
// src/host-kit, shared with the reference deploy template and the dev backend
// so the security-sensitive pieces — the (suspendedAt, resumeCount) capture,
// the self-decision guard, and the coarse start-role check — have a single
// tested home. This host supplies only its topology: every run leg travels
// through the run's DO stub.

function runRouterFor(env: Env, resolve: TenantResolver) {
  return createRunRouter({
    workflows: SHOWCASE_MODULES.map((entry) => entry.meta),
    resolve,
    systemActorId: SYSTEM_ACTOR_ID,
    start: async (workflowId, runId, inputData) => {
      // Budget BEFORE the DO round-trip: a capped tenant must not consume DO
      // CPU. Charged only on starts (a suspend/resume cycle is one run).
      await chargeDemoBudget(env, runId);
      return doSummary(
        await runStub(env, workflowId, runId).fetch('http://do/runs', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ workflowId, runId, inputData }),
        }),
      );
    },
    // The DO answers 404 for a run it has never seen; the router turns the
    // undefined into its own 404 rather than leaking the DO's body.
    status: async (workflowId, runId) => {
      const response = await runStub(env, workflowId, runId).fetch(
        `http://do/runs/${workflowId}/${runId}`,
      );
      if (response.status === 404) return undefined;
      return doSummary(response);
    },
    resume: async (workflowId, runId, body) =>
      doSummary(
        await runStub(env, workflowId, runId).fetch(
          `http://do/runs/${workflowId}/${runId}/resume`,
          {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify(body),
          },
        ),
      ),
  });
}

// Worker-level approval service sharing the DO's D1 database. Decisions resume
// the run through its DO stub (grants come from the store via the DO-side
// provider, never from this request); if the resumed run suspends again at a
// later gate, the next approval is queued right here, so multi-gate workflows
// keep flowing through the queue.
function buildApprovalService(
  store: TenantBoundApprovalStore,
  env: Env,
  waitUntil?: (promise: Promise<unknown>) => void,
): ApprovalService {
  const queueSink = env.AUDIT_QUEUE
    ? queueAuditSink(env.AUDIT_QUEUE)
    : undefined;
  const systemActor: ApprovalActor = {
    id: SYSTEM_ACTOR_ID,
    role: 'operator',
    tenantId: store.tenantId,
  };
  const service: ApprovalService = new ApprovalService({
    store,
    defaultSlaSeconds: numberVar(
      env.APPROVAL_SLA_SECONDS,
      4 * 60 * 60,
      'APPROVAL_SLA_SECONDS',
    ),
    audit: (event) => {
      console.log(JSON.stringify({ type: 'audit', ...event }));
      if (queueSink) {
        const send = queueSink(event).catch((error: unknown) =>
          console.error(
            JSON.stringify({
              type: 'audit-queue-error',
              reason: String(error),
            }),
          ),
        );
        waitUntil?.(send);
      }
    },
    onEscalation: (record) =>
      console.log(
        JSON.stringify({
          type: 'sla-escalation',
          id: record.id,
          workflowId: record.workflowId,
          runId: record.runId,
          slaDeadlineAt: record.slaDeadlineAt,
        }),
      ),
    // This host's only topology-specific piece: resume the run through its DO
    // stub. resumeRunWithRequeue (host-kit) wraps it with the SoD-guarded
    // re-queue so a run that re-suspends at a later gate keeps flowing through
    // the queue — the same wrapper the dev backend uses over its in-process base.
    resumeRun: resumeRunWithRequeue(
      async (record, decision) =>
        doSummary(
          await runStub(env, record.workflowId, record.runId).fetch(
            `http://do/runs/${record.workflowId}/${record.runId}/resume`,
            {
              method: 'POST',
              headers: { 'content-type': 'application/json' },
              body: JSON.stringify({
                step: record.stepPath,
                resumeData: defaultResumeData(record, decision),
              }),
            },
          ),
        ),
      () => service,
      systemActor,
    ),
  });
  return service;
}

/**
 * Maintenance runs on TWO cron expressions, dispatched on controller.cron so
 * the SLA sweep and the purge NEVER share an invocation: a Workers CPU-limit
 * termination kills the isolate and is NOT a catchable JS error, so a slow
 * sweep sharing an invocation would permanently starve the purge (and vice
 * versa) no matter how many try/catches wrap them. Keep these literals equal
 * to wrangler.jsonc's `triggers.crons`.
 */
export const SWEEP_CRON = '*/15 * * * *';
export const PURGE_CRON = '7 * * * *';

async function runSweepMaintenance(env: Env, cron: string): Promise<void> {
  let escalated: number | undefined;
  const pendingSends: Promise<unknown>[] = [];
  try {
    // Cron-owned TCB sweep over the SYSTEM store: the ONLY legitimate
    // cross-tenant read+write, and it is not reachable over HTTP.
    const queueSink = env.AUDIT_QUEUE
      ? queueAuditSink(env.AUDIT_QUEUE)
      : undefined;
    escalated = (
      await sweepSLA(approvalStoreFactory(env.DB).system(), {
        systemActor: MAINTENANCE_ACTOR,
        audit: (event) => {
          console.log(JSON.stringify({ type: 'audit', ...event }));
          if (queueSink) {
            pendingSends.push(
              queueSink(event).catch((error: unknown) =>
                console.error(
                  JSON.stringify({
                    type: 'audit-queue-error',
                    reason: String(error),
                  }),
                ),
              ),
            );
          }
        },
        onEscalation: (record) =>
          console.log(
            JSON.stringify({
              type: 'sla-escalation',
              id: record.id,
              tenantId: record.tenantId,
              workflowId: record.workflowId,
              runId: record.runId,
              slaDeadlineAt: record.slaDeadlineAt,
            }),
          ),
      })
    ).length;
  } catch (error) {
    console.error(
      JSON.stringify({
        type: 'maintenance-error',
        surface: 'sla-sweep',
        cron,
        error: String(error),
      }),
    );
  }
  await Promise.all(pendingSends);
  console.log(JSON.stringify({ type: 'maintenance', cron, escalated }));
}

async function runPurgeMaintenance(env: Env, cron: string): Promise<void> {
  let purged: number | undefined;
  let demoTenantsPurged: string[] | undefined;
  try {
    purged = await purgeExpiredWorkflowRuns(env.DB, {
      ttlMs:
        numberVar(env.RUN_RETENTION_DAYS, 30, 'RUN_RETENTION_DAYS') *
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
  try {
    // Expired demo sandboxes: complete offboarding per tenant (snapshots of
    // ANY status — a visitor who closed the tab mid-approval left a
    // suspended row the terminal-only retention purge can never reap — plus
    // approvals). No R2 artifactStore here: the showcase's artifact bucket
    // is in-memory. LIMIT-batched; the shrinking table is the durable cursor.
    demoTenantsPurged = await purgeExpiredDemoTenants(env.DB, {
      purgeTenantData: (tenantId) => purgeTenant(env.DB, { tenantId }),
      // Wait out the JWT lifetime past expires_at: a refresh at the last live
      // instant mints a token good until ~expires_at + jwtTtl, and purgeTenant
      // deletes suspended rows. Without the grace, a visitor's still-valid
      // token would find its own runs gone.
      graceMs:
        numberVar(env.DEMO_JWT_TTL_SECONDS, 3600, 'DEMO_JWT_TTL_SECONDS') *
        1000,
      limit: 25,
    });
  } catch (error) {
    console.error(
      JSON.stringify({
        type: 'maintenance-error',
        surface: 'demo-tenant-purge',
        cron,
        error: String(error),
      }),
    );
  }
  console.log(
    JSON.stringify({ type: 'maintenance', cron, purged, demoTenantsPurged }),
  );
}

const handler: ExportedHandler<Env> = {
  async fetch(
    request: CfRequest,
    env: Env,
    ctx: ExecutionContext,
  ): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === 'GET' && url.pathname === '/healthz') {
      return json({ ok: true });
    }

    const waitUntil = (promise: Promise<unknown>) => ctx.waitUntil(promise);
    const routed = request as unknown as Request;

    // Public demo sign-in (no auth — it MINTS identity). Mounted only when
    // configured; the kill switch 503s it. Provider selection runs only with
    // the demo switched on: a half-set OAuth pair on a demo-off deployment is
    // inert, so its config-error tripwire would be per-request noise.
    if (env.DEMO_JWT_SECRET) {
      const oauthProvider = selectOAuthProvider(env);
      if (oauthProvider) {
        const authResponse = await createDemoAuthRouter({
          db: env.DB,
          provider: oauthProvider,
          secret: env.DEMO_JWT_SECRET,
          jwtTtlSeconds: numberVar(
            env.DEMO_JWT_TTL_SECONDS,
            3600,
            'DEMO_JWT_TTL_SECONDS',
          ),
          tenantTtlMs:
            numberVar(env.DEMO_TENANT_TTL_HOURS, 24, 'DEMO_TENANT_TTL_HOURS') *
            60 *
            60 *
            1000,
          purgeTenantData: (tenantId) => purgeTenant(env.DB, { tenantId }),
          disabled: env.DEMO_DISABLED === 'true',
        })(routed);
        if (authResponse) return authResponse;
      }
    }

    // AUTHENTICATE FIRST, then construct (INV-2): the resolver binds the
    // approval store to the verified actor's tenant before any service
    // exists — there is no pre-auth store for a rushed fix to reach.
    const resolve = createTenantResolver({
      authenticate: bearerActorAuthenticator(buildVerifier(env)),
      storeFactory: approvalStoreFactory(env.DB),
      buildService: (store) => buildApprovalService(store, env, waitUntil),
    });

    const approvalResponse = await createApprovalRouter({ resolve })(routed);
    if (approvalResponse) return approvalResponse;

    const runResponse = await runRouterFor(env, resolve)(routed);
    if (runResponse) return runResponse;

    return json({ error: 'not found' }, 404);
  },

  async scheduled(
    controller: ScheduledController,
    env: Env,
    ctx: ExecutionContext,
  ): Promise<void> {
    // Dispatch on WHICH cron fired; an unrecognized expression (ops edited
    // wrangler without updating the constants) runs both sequentially and
    // logs — availability of both duties beats purity on a misconfig.
    if (controller.cron === SWEEP_CRON) {
      ctx.waitUntil(runSweepMaintenance(env, controller.cron));
    } else if (controller.cron === PURGE_CRON) {
      ctx.waitUntil(runPurgeMaintenance(env, controller.cron));
    } else {
      console.error(
        JSON.stringify({
          type: 'config-error',
          var: 'triggers.crons',
          raw: controller.cron,
          reason: 'unknown cron expression — running both maintenance surfaces',
        }),
      );
      ctx.waitUntil(
        runSweepMaintenance(env, controller.cron).then(() =>
          runPurgeMaintenance(env, controller.cron),
        ),
      );
    }
  },

  // Audit-export consumer (active only when the wrangler.jsonc `queues` block is
  // uncommented): ships each batch to the SIEM collector; a failed export
  // retries the batch, so nothing is acked unconfirmed.
  async queue(batch: MessageBatch, env: Env): Promise<void> {
    if (!env.SIEM_ENDPOINT) {
      console.error(
        JSON.stringify({
          type: 'config-error',
          var: 'SIEM_ENDPOINT',
          reason:
            'audit consumer bound without an export endpoint — retrying batch',
        }),
      );
      batch.retryAll();
      return;
    }
    await createAuditQueueConsumer({
      endpoint: env.SIEM_ENDPOINT,
      headers: env.SIEM_AUTH_HEADER
        ? { authorization: env.SIEM_AUTH_HEADER }
        : undefined,
    })(batch);
  },
};

export default handler;
