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
  Request as CfRequest,
  D1Database,
  DurableObjectNamespace,
  ExecutionContext,
  ExportedHandler,
  MessageBatch,
  Queue,
  ScheduledController,
} from '@cloudflare/workers-types';
import { AuditLogger, D1RateLimitStore } from '@proofoftech/breakwater';

import { approvalGrantProvider } from '@proofoftech/flowsafe/approval-api';
import {
  DurableObjectRunner,
  purgeTenant,
  type RunnerRuntime,
  tenantOfRunId,
} from '@proofoftech/flowsafe/do-runner';
import {
  approvalStoreFactoryFor,
  boolVar,
  createFlowsafeWorker,
  hmacVerifier,
  numberVar,
  parseActorTokens,
  RunRouteError,
  staticTokenVerifier,
  type TokenVerifier,
} from '@proofoftech/flowsafe/host-kit';
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
} from '#worker/demo-auth';
import { createDemoResetRouter } from '#worker/demo-reset';
import {
  buildShowcaseRuntime,
  type EmailServiceBinding,
  SHOWCASE_MODULES,
} from '#worker/runtime';

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
  /** Cron purges DECIDED (approved/rejected) approval records older than this (var; default 30; 0 = immediately). */
  APPROVAL_RETENTION_DAYS?: string;
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
        reason: `${present} is set without ${missing}; sign-in cannot complete, so the provider stays unmounted`,
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
            'the Google and GitHub OAuth pairs are both set. Google mounts; remove the stale GitHub credentials',
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

// The auth seam, the run routes with their RBAC gate order, the DO-stub
// topology, the approval-service assembly, and the whole Worker pipeline
// (createFlowsafeWorker) live in src/host-kit, shared with the reference
// deploy template and the dev backend so the security-sensitive pieces — the
// (suspendedAt, resumeCount) capture, the self-decision guard, and the role
// gates — have a single tested home. This host supplies only its bindings,
// its demo mounts, and the demo budget charge.

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

const worker = createFlowsafeWorker<Env>({
  workflows: SHOWCASE_MODULES.map((entry) => entry.meta),
  systemActorId: SYSTEM_ACTOR_ID,
  buildVerifier,
  crons: { sweep: SWEEP_CRON, purge: PURGE_CRON },
  // The showcase's own mounts, in the order they always ran: the public demo
  // sign-in first (no auth — it MINTS identity; the kill switch 503s it),
  // then the self-service sandbox reset, which authenticates through the
  // SAME resolve the API routers use.
  preRoutes: async (request, env, _ctx, kit) => {
    // Provider selection runs only with the demo switched on: a half-set
    // OAuth pair on a demo-off deployment is inert, so its config-error
    // tripwire would be per-request noise.
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
        })(request);
        if (authResponse) return authResponse;
      }
    }
    // Self-service sandbox reset (admin role + demo tenant only): the same
    // purge primitive as the reaper. Deliberately leaves run_count/demo_daily
    // alone — a reset must never refill the spend budget. (A future
    // budget-refill would extend this purgeTenantData arrow, nothing else.)
    return createDemoResetRouter({
      resolve: kit.resolve,
      isDemoTenant: (tenantId) => isDemoTenant(env.DB, tenantId),
      purgeTenantData: (tenantId) => purgeTenant(env.DB, { tenantId }),
    })(request);
  },
  // Budget BEFORE the DO round-trip: a capped tenant must not consume DO CPU.
  wrapStart: (start, env) => async (workflowId, runId, inputData) => {
    await chargeDemoBudget(env, runId);
    return start(workflowId, runId, inputData);
  },
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
  wrapResume: (resume, env) => async (workflowId, runId, body) => {
    await chargeDemoBudget(env, runId);
    return resume(workflowId, runId, body);
  },
  // Expired demo sandboxes: complete offboarding per tenant (snapshots of
  // ANY status — a visitor who closed the tab mid-approval left a
  // suspended row the terminal-only retention purge can never reap — plus
  // approvals). No R2 artifactStore here: the showcase's artifact bucket
  // is in-memory. LIMIT-batched; the shrinking table is the durable cursor.
  // Own try/catch (D3: a failure in any one purge duty must never stop the
  // others) so the error surface stays 'demo-tenant-purge', not the
  // composer's generic one.
  extraPurgeDuties: async (env, cron) => {
    try {
      const demoTenantsPurged = await purgeExpiredDemoTenants(env.DB, {
        purgeTenantData: (tenantId) => purgeTenant(env.DB, { tenantId }),
        // Wait out the JWT lifetime past expires_at: a refresh at the last
        // live instant mints a token good until ~expires_at + jwtTtl, and
        // purgeTenant deletes suspended rows. Without the grace, a visitor's
        // still-valid token would find its own runs gone.
        graceMs:
          numberVar(env.DEMO_JWT_TTL_SECONDS, 3600, 'DEMO_JWT_TTL_SECONDS') *
          1000,
        limit: 25,
      });
      return { demoTenantsPurged };
    } catch (error) {
      console.error(
        JSON.stringify({
          type: 'maintenance-error',
          surface: 'demo-tenant-purge',
          cron,
          error: String(error),
        }),
      );
      return {};
    }
  },
});

const handler: ExportedHandler<Env> = {
  fetch: (request: CfRequest, env: Env, ctx: ExecutionContext) =>
    worker.fetch(request as unknown as Request, env, ctx),
  scheduled: (
    controller: ScheduledController,
    env: Env,
    ctx: ExecutionContext,
  ) => worker.scheduled(controller, env, ctx),
  queue: (batch: MessageBatch, env: Env) => worker.queue(batch, env),
};

export default handler;
