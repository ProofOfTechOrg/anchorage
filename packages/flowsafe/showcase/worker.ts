// Anchorage Showcase — the runnable multi-workflow host.
//
// The five design-sketch workflows grown into an actual deployed service: all
// five (gtm-outbound, content-pipeline, lead-generation, product-launch,
// access-request) running on a real Cloudflare Worker + Durable Object + D1,
// behind the approval queue, bearer auth, and cron maintenance. This file is
// host wiring only: it delegates every workflow to buildShowcaseRuntime(), and
// auth + the run routes + the approval bridge + the service assembly to
// src/host-kit (shared with `deploy/worker.ts`). What stays here is this
// host's topology (the DO namespace handed to createDoRunTopology), the demo
// budget charge, and cron maintenance.
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
//   POST /demo/reset                      -> self-service sandbox wipe (admin
//                                            role + demo tenant only): purges
//                                            the caller's runs + approvals via
//                                            purgeTenant; the run budget is
//                                            deliberately NOT refilled
//   GET  /healthz                         -> liveness (unauthenticated)
//
// All routes except /healthz and /auth/* require `Authorization: Bearer
// <token>`. Two verifiers feed one seam: the APPROVAL_ACTOR_TOKENS static map
// (local dev / operators) and, when DEMO_JWT_SECRET is set, the public demo's
// OAuth-minted HS256 JWTs (see demo-auth.ts: GET /auth/<provider> ->
// per-visitor ephemeral tenant + a four-role token set). Neither is baked in:
// a deploy without secrets 401s everywhere (fail closed by construction).
// DEMO_DISABLED is the kill switch — checked in the AUTH middleware, so
// already-issued demo JWTs die with it, and at the mint/refresh routes; it
// parses fail-CLOSED (any unrecognized non-empty value disables the demo).
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
import { AuditLogger, D1RateLimitStore } from '@proofoftech/breakwater';

import {
  approvalGrantProvider,
  createApprovalRouter,
  createTenantResolver,
  type TenantResolver,
} from '../src/approval-api/index.js';
import { createAuditQueueHandler } from '../src/audit-export/index.js';
import {
  DurableObjectRunner,
  purgeExpiredWorkflowRuns,
  purgeTenant,
  type RunnerRuntime,
  tenantOfRunId,
} from '../src/do-runner/index.js';
import {
  approvalStoreFactoryFor,
  bearerActorAuthenticator,
  boolVar,
  buildHostApprovalService,
  createDoRunTopology,
  createRunRouter,
  type DoRunTopology,
  hmacVerifier,
  maintenanceActor,
  numberVar,
  parseActorTokens,
  RunRouteError,
  runSlaSweepMaintenance,
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
import { createDemoResetRouter } from './demo-reset.js';
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
   * provider; the router mounts ONE). DEMO_DISABLED is the kill switch (auth
   * middleware + mint): true/1/yes/on disable; any other non-empty value
   * ALSO disables and logs a config-error (an emergency control fed garbage
   * must bite, not no-op). Caps are vars so ops can tune without a deploy;
   * setting a cap to 0 is an intentional freeze, not a typo.
   */
  DEMO_JWT_SECRET?: string;
  GOOGLE_CLIENT_ID?: string;
  GOOGLE_CLIENT_SECRET?: string;
  GITHUB_CLIENT_ID?: string;
  GITHUB_CLIENT_SECRET?: string;
  DEMO_DISABLED?: string;
  /** Max runs per demo tenant lifetime (var; default 20; 0 freezes). */
  DEMO_TENANT_RUN_CAP?: string;
  /** Global demo runs per UTC day — the spend backstop (var; default 500; 0 freezes). */
  DEMO_DAILY_RUN_CAP?: string;
  /** Demo tenant lifetime hours (var; default 24). */
  DEMO_TENANT_TTL_HOURS?: string;
  /** Demo JWT lifetime seconds (var; default 3600 = 1h, silent refresh). */
  DEMO_JWT_TTL_SECONDS?: string;
  /** Cron purges terminal run snapshots older than this (var; default 30; 0 = immediately). */
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

// One durable rate-limit store per isolate: it memoizes its own schema DDL,
// and each DO instance building a fresh store would re-issue CREATE TABLE
// per run. Durable (D1) because the topology is one DO per run — an
// in-memory window would make `rateLimit: '5/min'` mean 5/min PER RUN, and
// ten concurrent runs would multiply the declared budget tenfold.
const rateLimitStores = new WeakMap<D1Database, D1RateLimitStore>();

function rateLimitStoreFor(db: D1Database): D1RateLimitStore {
  let store = rateLimitStores.get(db);
  if (!store) {
    store = new D1RateLimitStore(db);
    rateLimitStores.set(db, store);
  }
  return store;
}

function defineWorkflows(env: Env, tenantId: string): RunnerRuntime {
  return buildShowcaseRuntime({
    initInput: env,
    // The DO topology binds the grant store to THIS instance's tenant,
    // recovered from the DO's own idFromName identity (INV-1 -> INV-2).
    grantProvider: approvalGrantProvider(
      approvalStoreFactoryFor(env.DB).forTenant(tenantId),
    ),
    email: env.EMAIL,
    fromAddress: env.OUTREACH_FROM_ADDRESS,
    fromName: env.OUTREACH_FROM_NAME,
    // Connector audit streams to Workers Logs alongside the approval audit.
    audit: new AuditLogger({
      sink: (event) =>
        console.log(JSON.stringify({ type: 'connector-audit', ...event })),
    }),
    // Rate-limit budgets share one D1 window across every DO instance, so a
    // manifest cap holds per tenant, not per run.
    rateLimitStore: rateLimitStoreFor(env.DB),
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

/**
 * The kill switch, fail-closed via boolVar: an operator mid-incident typing
 * DEMO_DISABLED=1 (or =yes, =on, or a typo) must kill the demo, not silently
 * no-op. Feeds BOTH bites of the switch — the verifier (already-issued JWTs
 * die) and the /auth mint/refresh routes.
 */
function demoDisabledOf(env: Env): boolean {
  return boolVar(env.DEMO_DISABLED, 'DEMO_DISABLED', { onInvalid: true });
}

/**
 * The composed verifier: the static operator map, plus (when configured) the
 * demo's HS256 JWTs. DEMO_DISABLED kills the JWT path entirely — issued
 * tokens stop verifying, not just new mints. Memoized per isolate on its
 * inputs: parseActorTokens re-parses and re-validates the whole secret map,
 * and rebuilding that (plus the HMAC verifier) on every request is pure
 * waste — the env is stable for the isolate's lifetime, so the memo hits on
 * every request after the first (and correctly rebuilds under tests that
 * vary the env).
 */
let verifierMemo: { key: string; verifier: TokenVerifier } | undefined;

export function buildVerifier(env: Env): TokenVerifier {
  const disabled = demoDisabledOf(env);
  // NUL-separated so the concatenation cannot collide (env values never
  // contain NUL). Kept as \u0000 ESCAPES: raw NUL bytes turn this file
  // binary for grep/diff tooling.
  const memoKey = `${env.APPROVAL_ACTOR_TOKENS ?? ''}\u0000${env.DEMO_JWT_SECRET ?? ''}\u0000${disabled}`;
  if (verifierMemo?.key === memoKey) return verifierMemo.verifier;
  const staticVerifier = staticTokenVerifier(
    parseActorTokens(env.APPROVAL_ACTOR_TOKENS),
  );
  const secret = env.DEMO_JWT_SECRET;
  let verifier: TokenVerifier;
  if (!secret || disabled) {
    verifier = staticVerifier;
  } else {
    const demoVerifier = hmacVerifier({
      keys: new Map([[DEMO_JWT_KID, secret]]),
      issuer: DEMO_JWT_ISSUER,
      audience: DEMO_JWT_AUDIENCE,
    });
    verifier = {
      async verify(token) {
        return (
          (await staticVerifier.verify(token)) ??
          (await demoVerifier.verify(token))
        );
      },
    };
  }
  verifierMemo = { key: memoKey, verifier };
  return verifier;
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
      // allowZero: a 0 cap is the incident freeze ("no more demo runs"), and
      // silently reverting it to the fallback would make the freeze a no-op.
      tenantRunCap: numberVar(
        env.DEMO_TENANT_RUN_CAP,
        20,
        'DEMO_TENANT_RUN_CAP',
        { allowZero: true },
      ),
      dailyRunCap: numberVar(
        env.DEMO_DAILY_RUN_CAP,
        500,
        'DEMO_DAILY_RUN_CAP',
        { allowZero: true },
      ),
    });
  } catch (error) {
    if (error instanceof DemoRunLimitError) {
      throw new RunRouteError(429, error.message);
    }
    throw error;
  }
}

// The auth seam (parseActorTokens + bearerActorAuthenticator), the run routes
// with their RBAC gate order, the DO-stub topology, and the approval-service
// assembly live in src/host-kit, shared with the reference deploy template and
// the dev backend so the security-sensitive pieces — the (suspendedAt,
// resumeCount) capture, the self-decision guard, and the role gates — have a
// single tested home. This host supplies only its bindings and the demo
// budget charge.

function runRouterFor(
  env: Env,
  resolve: TenantResolver,
  topology: DoRunTopology,
) {
  return createRunRouter({
    workflows: SHOWCASE_MODULES.map((entry) => entry.meta),
    resolve,
    systemActorId: SYSTEM_ACTOR_ID,
    start: async (workflowId, runId, inputData) => {
      // Budget BEFORE the DO round-trip: a capped tenant must not consume DO
      // CPU.
      await chargeDemoBudget(env, runId);
      return topology.start(workflowId, runId, inputData);
    },
    status: topology.status,
    resume: async (workflowId, runId, body) => {
      // Resumes are metered like starts: every attempt — including one that
      // fails resumeSchema validation and leaves the run suspended — costs a
      // DO round-trip plus a D1 snapshot read, so an uncharged resume would
      // be an unbounded spend loop for an already-capped tenant. Queue
      // DECISIONS stay uncharged, bounded by a different pair of facts:
      // decide()'s one-shot CAS makes each approval record decidable exactly
      // once, and a workflow's gate count is a small server-authored
      // constant — a later gate's record is filed by the (uncharged)
      // decision resume itself, so the uncharged multiplier is gates-per-run
      // (2 at most today), never client-controlled.
      await chargeDemoBudget(env, runId);
      return topology.resume(workflowId, runId, body);
    },
  });
}

/**
 * Maintenance runs on TWO cron expressions, dispatched on controller.cron so
 * the SLA sweep and the purge NEVER share an invocation: a Workers CPU-limit
 * termination kills the isolate and is NOT a catchable JS error, so a slow
 * sweep sharing an invocation would permanently starve the purge (and vice
 * versa) no matter how many try/catches wrap them. Keep these literals equal
 * to wrangler.jsonc's `triggers.crons`. NOT exported: workerd rejects any
 * entry-module export that is not a handler/class/function, so a bare const
 * here fails the whole Worker at startup.
 */
const SWEEP_CRON = '*/15 * * * *';
const PURGE_CRON = '7 * * * *';

async function runPurgeMaintenance(env: Env, cron: string): Promise<void> {
  let purged: number | undefined;
  let demoTenantsPurged: string[] | undefined;
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
          disabled: demoDisabledOf(env),
        })(routed);
        if (authResponse) return authResponse;
      }
    }

    const topology = createDoRunTopology(env.RUNNER);

    // AUTHENTICATE FIRST, then construct (INV-2): the resolver binds the
    // approval store to the verified actor's tenant before any service
    // exists — there is no pre-auth store for a rushed fix to reach.
    const resolve = createTenantResolver({
      authenticate: bearerActorAuthenticator(buildVerifier(env)),
      storeFactory: approvalStoreFactoryFor(env.DB),
      buildService: (store) =>
        buildHostApprovalService(store, {
          systemActorId: SYSTEM_ACTOR_ID,
          defaultSlaSeconds: numberVar(
            env.APPROVAL_SLA_SECONDS,
            4 * 60 * 60,
            'APPROVAL_SLA_SECONDS',
          ),
          // This host's only topology-specific piece: decisions resume the
          // run through its DO stub.
          resumeRun: topology.resumeRecord,
          queue: env.AUDIT_QUEUE,
          waitUntil,
        }),
    });

    // Self-service sandbox reset (admin role + demo tenant only): the same
    // purge primitive as the reaper. Deliberately leaves run_count/demo_daily
    // alone — a reset must never refill the spend budget. (A future
    // budget-refill would extend this purgeTenantData arrow, nothing else.)
    const resetResponse = await createDemoResetRouter({
      resolve,
      isDemoTenant: (tenantId) => isDemoTenant(env.DB, tenantId),
      purgeTenantData: (tenantId) => purgeTenant(env.DB, { tenantId }),
    })(routed);
    if (resetResponse) return resetResponse;

    const approvalResponse = await createApprovalRouter({ resolve })(routed);
    if (approvalResponse) return approvalResponse;

    const runResponse = await runRouterFor(env, resolve, topology)(routed);
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
    const sweep = () =>
      runSlaSweepMaintenance({
        store: approvalStoreFactoryFor(env.DB).system(),
        systemActor: maintenanceActor(SYSTEM_ACTOR_ID),
        queue: env.AUDIT_QUEUE,
        cron: controller.cron,
      });
    if (controller.cron === SWEEP_CRON) {
      ctx.waitUntil(sweep());
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
        sweep().then(() => runPurgeMaintenance(env, controller.cron)),
      );
    }
  },

  // Audit-export consumer (active only when the wrangler.jsonc `queues` block is
  // uncommented): ships each batch to the SIEM collector; a failed export
  // retries the batch, so nothing is acked unconfirmed.
  async queue(batch: MessageBatch, env: Env): Promise<void> {
    await createAuditQueueHandler({
      endpoint: env.SIEM_ENDPOINT,
      authHeader: env.SIEM_AUTH_HEADER,
    })(batch);
  },
};

export default handler;
