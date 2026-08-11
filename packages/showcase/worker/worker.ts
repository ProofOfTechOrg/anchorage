// Anchorage Showcase — the runnable single-deployment multi-workflow host.
//
// Six workflows run on the deployed service: the five original design sketches
// plus wire-transfer, which appears in both the launcher and control room. They
// run on a real Cloudflare Worker, Durable Object, and D1 database behind the
// approval queue, bearer auth, and alarm maintenance. This file is
// host wiring only: it delegates every workflow to buildShowcaseRuntime(), and
// auth + the run routes + the approval bridge + the service assembly to
// src/host-kit (shared with `deploy/worker.ts`). What stays here is this
// host's topology (the DO namespace handed to createDoRunTopology), the demo
// budget charge, and maintenance hooks.
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
// OAuth-minted HS256 JWTs (see demo-auth.ts: GET /auth/<provider> -> an
// expiring session in the shared demo organization + four role tokens).
// Neither is baked in:
// a deploy without secrets 401s everywhere (fail closed by construction).
// DEMO_DISABLED is the kill switch — checked in the AUTH middleware, so
// already-issued demo JWTs die with it, and at the mint/refresh routes; it
// parses fail-CLOSED (any unrecognized non-empty value disables the demo).
// Wrangler local development reads packages/showcase/.dev.vars.

import type {
  Request as CfRequest,
  D1Database,
  DurableObjectNamespace,
  ExecutionContext,
  ExportedHandler,
  Queue,
} from '@cloudflare/workers-types';
import {
  AGENT_AUDIT_CONTEXT_KEY,
  AuditLogger,
  D1RateLimitStore,
} from '@proofoftech/breakwater';

import {
  type ActorContext,
  approvalGrantProvider,
} from '@proofoftech/flowsafe/approval-api';
import {
  DurableObjectRunner,
  HubDurableObject,
  type RequestContextProvider,
  type RunnerRuntime,
} from '@proofoftech/flowsafe/do-runner';
import {
  approvalStoreFactoryFor,
  boolVar,
  createFlowsafeMaintenanceDurableObject,
  createFlowsafeWorker,
  type FlowsafeWorkerConfig,
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
  deleteExpiredDemoSessions,
  demoSessionIdOfActor,
  githubProvider,
  googleProvider,
  type OAuthProvider,
} from '#worker/demo-auth';
import {
  buildShowcaseRuntime,
  type EmailServiceBinding,
  SHOWCASE_MODULES,
} from '#worker/runtime';

interface Env {
  DB: D1Database;
  /** Provisioning-stamped tag; must match the sentinel row in DB. */
  DEPLOYMENT_TENANT: string;
  /** Secret credential shared only by this deployment's Worker and DOs. */
  DEPLOYMENT_IDENTITY_SECRET: string;
  RUNNER: DurableObjectNamespace;
  MAINTENANCE: DurableObjectNamespace;
  /**
   * Deployment live-streaming hub DO (ShowcaseHub). Always bound (wrangler v2
   * migration), so it is required, but streaming only mounts when
   * STREAM_TICKET_SECRET is ALSO set (see below), so the binding alone is inert.
   */
  HUB: DurableObjectNamespace;
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
  MAINTENANCE_ADMIN_SECRET?: string;
  /**
   * Secret (`wrangler secret put STREAM_TICKET_SECRET`): the dedicated HS256
   * key that signs short-lived WebSocket stream tickets. Present WITH the HUB
   * binding => the composer mounts live streaming (queue + run channels);
   * absent => streaming stays unmounted and the SPA runs on polling only
   * (graceful degradation, DL-019). A dedicated key so a stream ticket and a
   * session JWT can never be confused under one signing secret.
   */
  STREAM_TICKET_SECRET?: string;
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
  /** Max runs per demo session lifetime (var; default 20; 0 freezes). */
  DEMO_SESSION_RUN_CAP?: string;
  /** Global demo runs per UTC day — the spend backstop (var; default 500; 0 freezes). */
  DEMO_DAILY_RUN_CAP?: string;
  /** Demo session lifetime hours (var; default 24). */
  DEMO_SESSION_TTL_HOURS?: string;
  /** Demo JWT lifetime seconds (var; default 3600 = 1h, silent refresh). */
  DEMO_JWT_TTL_SECONDS?: string;
  /** Alarm maintenance purges terminal run snapshots older than this (var; default 30; 0 = immediately). */
  RUN_RETENTION_DAYS?: string;
  /** Alarm maintenance purges DECIDED approvals older than this (var; default 30; 0 = immediately). */
  APPROVAL_RETENTION_DAYS?: string;
  /**
   * Optional audit export: this data-plane Worker only produces to the shared
   * audit queue. The control plane owns consumption and SIEM delivery.
   */
  AUDIT_QUEUE?: Queue;
}

/** Id for system-created records. */
const SYSTEM_PRINCIPAL_ID = 'flowsafe-worker';

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

function grantProviderFor(env: Env): RequestContextProvider {
  const grants = approvalGrantProvider(approvalStoreFactoryFor(env.DB).store());
  return async (workflowId, runId, leg) => ({
    ...(await grants(workflowId, runId, leg)),
    [AGENT_AUDIT_CONTEXT_KEY]: {
      agentId: workflowId,
      tenantId: env.DEPLOYMENT_TENANT,
      runId,
      entryPath: leg.kind === 'start' ? 'workflow.start' : 'workflow.resume',
    },
  });
}

function defineWorkflows(env: Env): RunnerRuntime {
  return buildShowcaseRuntime({
    initInput: env,
    // Grants are per run. The deployment tag is infrastructure-provided and
    // reaches Breakwater only as trusted audit correlation; connector rate and
    // idempotency keys are deployment-wide by physical isolation.
    grantProvider: grantProviderFor(env),
    email: env.EMAIL,
    fromAddress: env.OUTREACH_FROM_ADDRESS,
    fromName: env.OUTREACH_FROM_NAME,
    // Connector audit streams to Workers Logs alongside the approval audit.
    audit: new AuditLogger({
      sink: (event) =>
        console.log(JSON.stringify({ type: 'connector-audit', ...event })),
    }),
    // Rate-limit budgets share one D1 window across every DO instance, so a
    // manifest cap holds per deployment, not per run.
    rateLimitStore: rateLimitStoreFor(env.DB),
    // crm/deploy egress bindings + artifact bucket are left unset: the showcase
    // runs offline (R2 via InMemoryArtifactBucket, CRM/deploy simulated). Bind
    // them in wrangler.jsonc + wire them here to go live per connector.
  });
}

export class ShowcaseRunner extends DurableObjectRunner<Env> {
  protected build(env: Env): RunnerRuntime {
    return defineWorkflows(env);
  }

  protected runOwnership(env: Env) {
    return approvalStoreFactoryFor(env.DB).resources();
  }
}

/**
 * The deployment live-streaming hub (wrangler HUB binding, v2 migration). The
 * base owns subscriber sockets, approval fan-out, and presence. Exported
 * separately from the handler because workerd resolves the class by name.
 */
export class ShowcaseHub extends HubDurableObject<Env> {}

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
 * mints a fresh session rather than colliding subjects. Both pairs set is
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

/** Public demo sessions pay for runs from two atomic budgets. */
async function chargeDemoBudget(
  env: Env,
  context: ActorContext,
): Promise<void> {
  const sessionId = demoSessionIdOfActor(context.actor);
  if (sessionId === undefined) return;
  try {
    await consumeRunBudget(env.DB, {
      sessionId,
      // allowZero: a 0 cap is the incident freeze ("no more demo runs"), and
      // silently reverting it to the fallback would make the freeze a no-op.
      sessionRunCap: numberVar(
        env.DEMO_SESSION_RUN_CAP,
        20,
        'DEMO_SESSION_RUN_CAP',
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

const workerConfig = {
  workflows: SHOWCASE_MODULES.map((entry) => entry.meta),
  systemPrincipalId: SYSTEM_PRINCIPAL_ID,
  buildVerifier,
  maintenance: {
    sweepIntervalMs: 15 * 60 * 1_000,
    purgeIntervalMs: 60 * 60 * 1_000,
  },
  // The public demo sign-in is unauthenticated because it mints identity; the
  // kill switch 503s it. There is no deployment-wide reset route: one visitor
  // must never erase the shared organization's data.
  preRoutes: async (request, env) => {
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
          sessionTtlMs:
            numberVar(
              env.DEMO_SESSION_TTL_HOURS,
              24,
              'DEMO_SESSION_TTL_HOURS',
            ) *
            60 *
            60 *
            1000,
          disabled: demoDisabledOf(env),
        })(request);
        if (authResponse) return authResponse;
      }
    }
    return null;
  },
  // Budget before the DO round-trip: a capped session must not consume DO CPU.
  beforeStart: (context, env) => chargeDemoBudget(env, context),
  // Resumes are metered like starts: every attempt — including one that
  // fails resumeSchema validation and leaves the run suspended — costs a
  // DO round-trip plus a D1 snapshot read, so an uncharged resume would
  // be an unbounded spend loop for an already-capped session. Queue
  // DECISIONS stay uncharged, bounded by a different pair of facts:
  // decide()'s one-shot CAS makes each approval record decidable exactly
  // once, and a workflow's gate count is a small server-authored
  // constant — a later gate's record is filed by the (uncharged)
  // decision resume itself, so the uncharged multiplier is gates-per-run
  // (2 at most today), never client-controlled.
  beforeResume: (context, env) => chargeDemoBudget(env, context),
  // Expired demo sessions are only auth/budget metadata. Shared run and
  // approval records stay under the normal deployment retention duties.
  extraPurgeDuties: async (env) => {
    const demoSessionsDeleted = await deleteExpiredDemoSessions(env.DB, {
      // Wait out the final JWT lifetime so session metadata remains
      // diagnosable until every issued token has expired.
      graceMs:
        numberVar(env.DEMO_JWT_TTL_SECONDS, 3600, 'DEMO_JWT_TTL_SECONDS') *
        1000,
      limit: 25,
    });
    return { demoSessionsDeleted };
  },
} satisfies FlowsafeWorkerConfig<Env>;

export class ShowcaseMaintenance extends createFlowsafeMaintenanceDurableObject(
  workerConfig,
) {}

const worker = createFlowsafeWorker<Env>(workerConfig);

const handler: ExportedHandler<Env> = {
  fetch: (request: CfRequest, env: Env, ctx: ExecutionContext) =>
    worker.fetch(request as unknown as Request, env, ctx),
};

export default handler;
