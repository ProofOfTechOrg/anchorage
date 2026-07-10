// Public-demo identity: OAuth sign-in -> an EPHEMERAL tenant + a set of
// short-TTL HS256 JWTs (one per demo role, distinct actor ids so
// separation-of-duties stays demonstrable) -> the same TenantResolver seam
// every host uses. Nothing here bypasses the platform's invariants: the
// tenant is provisioned through the tenants registry (insert-or-fail), runIds
// stay server-minted, and the JWTs verify through the standard hmacVerifier.
//
// Abuse posture, honestly: UNIQUE(provider, subject) stops one identity
// holding two live tenants — a speed bump, not a wall (N free accounts mint
// N tenants). The real backstops REACT to load: the per-tenant run cap and
// the GLOBAL daily run ceiling (both enforced as single atomic UPDATEs — a
// SELECT-then-UPDATE is a TOCTOU race a burst of parallel starts walks
// through), the kill switch (checked in the AUTH middleware, so
// already-issued JWTs die with it, not just new mints), and billing alerts.
// Size DEMO_DAILY_RUN_CAP for the spend you can tolerate, not the traffic
// you expect.
//
// What the run budgets DO bound: run STARTS and raw RESUME attempts — the
// worker charges both before the DO round-trip, so a garbage-resume loop
// against a suspended run burns budget instead of free DO CPU (a resume
// that fails resumeSchema still cost a DO fetch + D1 snapshot read). Queue
// DECISIONS are unmetered by design: each approval record is decidable
// once, and records only exist because a charged start/resume suspended.
// What they DON'T bound: status GETs, /auth/refresh, and approval-queue
// reads — cheap per request but unlimited per token; the backstop for
// raw request-rate abuse is platform-level (Cloudflare WAF rate rules),
// not this module.

import type {
  ApprovalActor,
  ApprovalRole,
} from '@proofoftech/flowsafe/approval-api';
import { d1Changes } from '@proofoftech/flowsafe/do-runner';
import {
  base64UrlEncode,
  hmacSign,
  hmacVerifier,
  mintHmacToken,
  provisionTenant,
  TenantCollisionError,
  type TenantRegistryDatabase,
} from '@proofoftech/flowsafe/host-kit';

/** Structural D1 subset (same posture as the other stores). */
export interface DemoDatabase {
  prepare(query: string): DemoStatement;
}

export interface DemoStatement {
  bind(...values: unknown[]): DemoStatement;
  run(): Promise<unknown>;
  first<T = unknown>(): Promise<T | null>;
  all<T = unknown>(): Promise<{ results: T[] }>;
}

const DEMO_SCHEMA: readonly string[] = [
  `CREATE TABLE IF NOT EXISTS demo_tenants (
    tenant_id TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    subject TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    run_count INTEGER NOT NULL DEFAULT 0,
    UNIQUE (provider, subject)
  )`,
  `CREATE TABLE IF NOT EXISTS demo_daily (
    day TEXT PRIMARY KEY,
    runs INTEGER NOT NULL DEFAULT 0
  )`,
];

export async function ensureDemoSchema(db: DemoDatabase): Promise<void> {
  for (const statement of DEMO_SCHEMA) {
    await db.prepare(statement).run();
  }
}

/** The four demo identities minted per visitor — DISTINCT ids, or decide()'s
 * self-approval check (requestedBy === actor.id) would 403 every decision
 * and the demo could never complete an approval. */
export const DEMO_TOKEN_ROLES: ReadonlyArray<{
  id: string;
  role: ApprovalRole;
}> = [
  { id: 'demo-admin', role: 'admin' },
  { id: 'demo-operator', role: 'operator' },
  { id: 'demo-reviewer', role: 'reviewer' },
  { id: 'demo-viewer', role: 'viewer' },
];

export interface DemoTenantRow {
  tenant_id: string;
  provider: string;
  subject: string;
  created_at: string;
  expires_at: string;
  run_count: number;
}

/**
 * Whether a tenant is a DEMO sandbox — read from `tenants.kind`, the
 * allocation authority, never guessed from the slug. A prefix heuristic
 * (`startsWith('dm')`) both mis-fires (a commercial slug 'dmart' would be
 * charged against a demo_tenants row that does not exist -> 429 on every run)
 * and fails open the moment the slug scheme changes. Tenants absent from the
 * registry (e.g. operator identities from the static token map, which are
 * never provisioned) are not demo tenants.
 */
export async function isDemoTenant(
  db: DemoDatabase,
  tenantId: string,
): Promise<boolean> {
  let row: { kind: string } | null;
  try {
    row = await db
      .prepare('SELECT kind FROM tenants WHERE tenant_id = ?')
      .bind(tenantId)
      .first<{ kind: string }>();
  } catch (error) {
    // ONLY "the registry was never created" means "no demo tenants exist".
    // A blanket catch would fail OPEN: a transient read error would make a
    // real demo tenant look commercial, skipping consumeRunBudget entirely
    // and bypassing BOTH the per-tenant cap and the global daily ceiling —
    // the spend backstop this whole path exists to enforce. Everything else
    // propagates and the run start 500s (fail closed). Same narrowing as
    // purgeTenant's missing-approvals-table tolerance.
    if (!/no such table/i.test(errorMessageOf(error))) throw error;
    row = null;
  }
  return row?.kind === 'demo';
}

function errorMessageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/** INV-3-valid ephemeral tenant slug: 'dm' + 18 hex chars. */
export function mintDemoTenantId(): string {
  const bytes = new Uint8Array(9);
  crypto.getRandomValues(bytes);
  let hex = '';
  for (const byte of bytes) hex += byte.toString(16).padStart(2, '0');
  return `dm${hex}`;
}

export interface DemoTenantOptions {
  provider: string;
  /** Provider-scoped stable subject, e.g. `github:<numeric id>`. */
  subject: string;
  /** Tenant lifetime; re-auth after expiry mints a FRESH tenant. */
  tenantTtlMs: number;
  now?: () => number;
  /**
   * Reaps a replaced tenant's data INLINE (purgeTenant). Without this, a
   * re-auth that replaces an expired row would orphan the old tenant's data
   * where the purge cron (which scans demo_tenants) can no longer find it.
   */
  purgeTenantData: (tenantId: string) => Promise<unknown>;
}

/**
 * One live tenant per (provider, subject): a live row is reused (its clock
 * is NOT extended — expiry is from first sign-in); an expired or absent row
 * mints a fresh tenant, purging the expired tenant's data first.
 */
export async function findOrCreateDemoTenant(
  db: DemoDatabase & TenantRegistryDatabase,
  options: DemoTenantOptions,
): Promise<DemoTenantRow> {
  const now = (options.now ?? Date.now)();
  await ensureDemoSchema(db);
  const existing = await db
    .prepare('SELECT * FROM demo_tenants WHERE provider = ? AND subject = ?')
    .bind(options.provider, options.subject)
    .first<DemoTenantRow>();
  if (existing && Date.parse(existing.expires_at) > now) {
    return existing;
  }
  if (existing) {
    // Expired: reap its data BEFORE the row stops referencing it, then drop
    // the row so the fresh insert below cannot violate UNIQUE(provider,subject).
    await options.purgeTenantData(existing.tenant_id);
    await db
      .prepare('DELETE FROM demo_tenants WHERE tenant_id = ?')
      .bind(existing.tenant_id)
      .run();
  }
  // Insert-or-fail into the allocation authority first; a (vanishingly
  // unlikely) random collision retries once with a fresh slug.
  let tenantId = mintDemoTenantId();
  try {
    await provisionTenant(db, { tenantId, kind: 'demo', now: () => now });
  } catch (error) {
    if (!(error instanceof TenantCollisionError)) throw error;
    tenantId = mintDemoTenantId();
    await provisionTenant(db, { tenantId, kind: 'demo', now: () => now });
  }
  const row: DemoTenantRow = {
    tenant_id: tenantId,
    provider: options.provider,
    subject: options.subject,
    created_at: new Date(now).toISOString(),
    expires_at: new Date(now + options.tenantTtlMs).toISOString(),
    run_count: 0,
  };
  // The SELECT..INSERT above is not transactional, so two concurrent first
  // sign-ins for one identity both reach here. `OR IGNORE` lets the loser
  // lose quietly against UNIQUE(provider, subject) — it then reads back the
  // WINNER's row rather than 500ing the visitor. The loser's provisioned slug
  // stays in `tenants` as an inert tombstone (slugs are never reused; the
  // registry is append-only by design).
  const insert = await db
    .prepare(
      `INSERT OR IGNORE INTO demo_tenants
       (tenant_id, provider, subject, created_at, expires_at, run_count)
       VALUES (?, ?, ?, ?, ?, 0)`,
    )
    .bind(
      row.tenant_id,
      row.provider,
      row.subject,
      row.created_at,
      row.expires_at,
    )
    .run();
  if (d1Changes(insert) === 0) {
    const winner = await db
      .prepare('SELECT * FROM demo_tenants WHERE provider = ? AND subject = ?')
      .bind(options.provider, options.subject)
      .first<DemoTenantRow>();
    if (!winner) {
      throw new Error(
        'demo tenant insert lost its race but the winner is unreadable; retry sign-in',
      );
    }
    return winner;
  }
  return row;
}

/**
 * Machine-readable cause of a budget refusal. `scope` says WHICH budget bit
 * ('tenant' caps one sandbox; 'global' is the platform-wide daily ceiling);
 * `reason` says WHY, so consumers branch on it instead of string-matching
 * `message` (whose copy is UX and may change).
 */
export type DemoRunLimitReason = 'not-provisioned' | 'expired' | 'cap-reached';

export class DemoRunLimitError extends Error {
  readonly scope: 'tenant' | 'global';
  readonly reason: DemoRunLimitReason;

  constructor(
    scope: 'tenant' | 'global',
    reason: DemoRunLimitReason,
    message: string,
  ) {
    super(message);
    this.name = 'DemoRunLimitError';
    this.scope = scope;
    this.reason = reason;
  }
}

export interface RunBudgetOptions {
  tenantId: string;
  /** Max runs per demo tenant over its whole lifetime. */
  tenantRunCap: number;
  /** Global ceiling across ALL tenants per UTC day — the spend backstop. */
  dailyRunCap: number;
  now?: () => number;
}

/**
 * Consume one run from BOTH budgets, atomically each: a single conditional
 * UPDATE per budget — `meta.changes === 0` means the cap held (or, for the
 * tenant budget, the tenant is missing/expired; the follow-up read is
 * diagnosability only, on the already-failing path). A SELECT-then-UPDATE
 * would be a TOCTOU race a burst of parallel starts walks straight through.
 *
 * Order: the tenant budget first. A consumed tenant slot on a subsequent
 * global-cap failure only UNDER-counts that tenant's remaining budget — fail
 * safe, never fail open.
 */
export async function consumeRunBudget(
  db: DemoDatabase,
  options: RunBudgetOptions,
): Promise<void> {
  const now = (options.now ?? Date.now)();
  const nowIso = new Date(now).toISOString();
  const tenantUpdate = await db
    .prepare(
      `UPDATE demo_tenants SET run_count = run_count + 1
       WHERE tenant_id = ? AND run_count < ? AND expires_at > ?`,
    )
    .bind(options.tenantId, options.tenantRunCap, nowIso)
    .run();
  if (d1Changes(tenantUpdate) === 0) {
    const row = await db
      .prepare(
        'SELECT run_count, expires_at FROM demo_tenants WHERE tenant_id = ?',
      )
      .bind(options.tenantId)
      .first<{ run_count: number; expires_at: string }>();
    if (!row) {
      throw new DemoRunLimitError(
        'tenant',
        'not-provisioned',
        'demo tenant not found or not provisioned',
      );
    }
    if (Date.parse(row.expires_at) <= now) {
      throw new DemoRunLimitError(
        'tenant',
        'expired',
        'demo tenant expired; sign in again for a fresh sandbox',
      );
    }
    throw new DemoRunLimitError(
      'tenant',
      'cap-reached',
      `demo run limit reached (${options.tenantRunCap} runs per sandbox)`,
    );
  }
  const day = new Date(now).toISOString().slice(0, 10);
  await db
    .prepare('INSERT OR IGNORE INTO demo_daily (day, runs) VALUES (?, 0)')
    .bind(day)
    .run();
  const dailyUpdate = await db
    .prepare('UPDATE demo_daily SET runs = runs + 1 WHERE day = ? AND runs < ?')
    .bind(day, options.dailyRunCap)
    .run();
  if (d1Changes(dailyUpdate) === 0) {
    throw new DemoRunLimitError(
      'global',
      'cap-reached',
      'the demo has reached its global daily run ceiling; try again tomorrow',
    );
  }
}

export interface PurgeDemoTenantsOptions {
  /** Reaps one tenant's data (purgeTenant over snapshots/approvals/artifacts). */
  purgeTenantData: (tenantId: string) => Promise<unknown>;
  /** Expired tenants processed per invocation — the cron's CPU budget guard. */
  limit?: number;
  /**
   * REQUIRED grace after `expires_at` before a tenant is reaped: the JWT
   * lifetime. `purgeTenant` deletes SUSPENDED rows and its safety argument is
   * "only purge tenants whose tokens have already expired, so no live caller
   * can be mid-resume by construction". A refresh at `expires_at - ε` mints a
   * token good until `≈ expires_at + jwtTtl` (refresh deliberately does not
   * extend the sandbox), so reaping at `expires_at` would race a genuinely
   * valid token — the run vanishes under its owner. Waiting out the JWT TTL
   * restores the precondition.
   */
  graceMs: number;
  now?: () => number;
}

/**
 * Reap demo tenants expired for longer than `graceMs`, oldest-expiry first,
 * at most `limit` per call. The "cursor" is the table itself: each reaped
 * tenant's row is deleted, so the next invocation resumes where this one
 * stopped — durable by construction, no separate cursor row to corrupt.
 * Failures are isolated PER TENANT: a failing purge keeps its row (its own
 * retry cursor) while the pass continues — oldest-first ordering would
 * otherwise retry the same wedged tenant first every pass and head-of-line
 * block every later expiry. Failures re-throw AFTER the pass, aggregated,
 * so the cron's error surface still fires. The isolation is bounded by
 * `limit`: a wedged tenant keeps occupying an oldest-first batch slot, so
 * once `limit` many are simultaneously wedged, younger expiries starve
 * until an operator clears them (every pass logs all of them).
 */
export async function purgeExpiredDemoTenants(
  db: DemoDatabase,
  options: PurgeDemoTenantsOptions,
): Promise<string[]> {
  const now = (options.now ?? Date.now)();
  await ensureDemoSchema(db);
  const { results } = await db
    .prepare(
      `SELECT tenant_id FROM demo_tenants WHERE expires_at <= ?
       ORDER BY expires_at ASC LIMIT ?`,
    )
    .bind(new Date(now - options.graceMs).toISOString(), options.limit ?? 25)
    .all<{ tenant_id: string }>();
  const purged: string[] = [];
  const failures: Array<{ tenantId: string; message: string }> = [];
  for (const row of results) {
    try {
      await options.purgeTenantData(row.tenant_id);
    } catch (error) {
      failures.push({
        tenantId: row.tenant_id,
        message: errorMessageOf(error),
      });
      continue;
    }
    await db
      .prepare('DELETE FROM demo_tenants WHERE tenant_id = ?')
      .bind(row.tenant_id)
      .run();
    purged.push(row.tenant_id);
  }
  if (failures.length > 0) {
    throw new Error(
      `purgeExpiredDemoTenants: ${failures.length} of ${results.length} expired tenant purge(s) failed, the rest were reaped (${failures
        .map((failure) => `${failure.tenantId}: ${failure.message}`)
        .join('; ')})`,
    );
  }
  return purged;
}

// ---- OAuth (providers behind a seam; Google launches, GitHub falls back) ---

export interface OAuthProvider {
  readonly name: string;
  authorizeUrl(input: { state: string; redirectUri: string }): string;
  /** Exchange the callback code for a provider-scoped stable subject. */
  exchange(input: {
    code: string;
    redirectUri: string;
  }): Promise<{ subject: string } | undefined>;
}

interface GithubProviderOptions {
  clientId: string;
  clientSecret: string;
  /** Injectable for tests. Default: globalThis.fetch. */
  fetch?: typeof fetch;
}

export function githubProvider(options: GithubProviderOptions): OAuthProvider {
  const fetchFn = options.fetch ?? fetch;
  return {
    name: 'github',
    authorizeUrl({ state, redirectUri }) {
      const url = new URL('https://github.com/login/oauth/authorize');
      url.searchParams.set('client_id', options.clientId);
      url.searchParams.set('redirect_uri', redirectUri);
      url.searchParams.set('state', state);
      // No scopes: public identity is all the demo needs.
      return url.toString();
    },
    async exchange({ code, redirectUri }) {
      const tokenResponse = await fetchFn(
        'https://github.com/login/oauth/access_token',
        {
          method: 'POST',
          headers: {
            accept: 'application/json',
            'content-type': 'application/json',
          },
          body: JSON.stringify({
            client_id: options.clientId,
            client_secret: options.clientSecret,
            code,
            redirect_uri: redirectUri,
          }),
        },
      );
      if (!tokenResponse.ok) return undefined;
      const tokenBody = (await tokenResponse.json()) as {
        access_token?: string;
      };
      if (!tokenBody.access_token) return undefined;
      const userResponse = await fetchFn('https://api.github.com/user', {
        headers: {
          authorization: `Bearer ${tokenBody.access_token}`,
          accept: 'application/vnd.github+json',
          'user-agent': 'anchorage-demo',
        },
      });
      if (!userResponse.ok) return undefined;
      const user = (await userResponse.json()) as { id?: number };
      if (typeof user.id !== 'number') return undefined;
      // The NUMERIC id is the stable subject — logins are renameable.
      return { subject: `github:${user.id}` };
    },
  };
}

interface GoogleProviderOptions {
  clientId: string;
  clientSecret: string;
  /** Injectable for tests. Default: globalThis.fetch. */
  fetch?: typeof fetch;
}

export function googleProvider(options: GoogleProviderOptions): OAuthProvider {
  const fetchFn = options.fetch ?? fetch;
  return {
    name: 'google',
    authorizeUrl({ state, redirectUri }) {
      const url = new URL('https://accounts.google.com/o/oauth2/v2/auth');
      url.searchParams.set('client_id', options.clientId);
      url.searchParams.set('redirect_uri', redirectUri);
      url.searchParams.set('response_type', 'code');
      // 'openid' alone: the demo needs only a stable subject (`sub`), never
      // email or profile — the narrowest consent screen Google offers.
      url.searchParams.set('scope', 'openid');
      url.searchParams.set('state', state);
      return url.toString();
    },
    async exchange({ code, redirectUri }) {
      // Google's token endpoint accepts ONLY form encoding — a JSON body is
      // rejected (unlike GitHub's, which negotiates).
      const tokenResponse = await fetchFn(
        'https://oauth2.googleapis.com/token',
        {
          method: 'POST',
          headers: { 'content-type': 'application/x-www-form-urlencoded' },
          body: new URLSearchParams({
            grant_type: 'authorization_code',
            code,
            client_id: options.clientId,
            client_secret: options.clientSecret,
            redirect_uri: redirectUri,
          }).toString(),
        },
      );
      if (!tokenResponse.ok) return undefined;
      const tokenBody = (await tokenResponse.json()) as {
        access_token?: string;
      };
      if (!tokenBody.access_token) return undefined;
      const userResponse = await fetchFn(
        'https://openidconnect.googleapis.com/v1/userinfo',
        {
          headers: { authorization: `Bearer ${tokenBody.access_token}` },
        },
      );
      if (!userResponse.ok) return undefined;
      const user = (await userResponse.json()) as { sub?: string };
      if (typeof user.sub !== 'string' || user.sub.length === 0) {
        return undefined;
      }
      // `sub` is Google's stable per-account identifier ("unique among all
      // Google accounts and never reused") — emails are renameable.
      return { subject: `google:${user.sub}` };
    },
  };
}

// ---- signed state (CSRF binding for the OAuth round-trip) ------------------

const encoder = new TextEncoder();

/** The cookie carrying the state nonce — binds the round-trip to ONE browser. */
export const STATE_COOKIE = 'demo_oauth_state';

/**
 * Sign an OAuth `state` and return it with its nonce. The signature alone is
 * NOT sufficient CSRF protection: an attacker can obtain a valid state from
 * `GET /auth/<provider>` and walk a victim's browser through the callback
 * with the ATTACKER's code, landing the victim in the attacker's sandbox
 * (login CSRF). The nonce is therefore also set as an HttpOnly cookie and
 * must match at the callback — one browser, one round-trip.
 */
export async function signState(
  secret: string,
  now: number,
  ttlMs = 10 * 60 * 1000,
): Promise<{ state: string; nonce: string }> {
  const nonce = mintDemoTenantId();
  const payload = `${nonce}.${now + ttlMs}`;
  return {
    state: `${payload}.${await hmacSign(secret, payload)}`,
    nonce,
  };
}

/** The nonce a signed state carries (undefined when structurally malformed). */
export function nonceOfState(state: string): string | undefined {
  const parts = state.split('.');
  return parts.length === 3 ? parts[0] : undefined;
}

/** Read one cookie from a request's Cookie header. */
export function cookieValue(
  request: Request,
  name: string,
): string | undefined {
  const header = request.headers.get('cookie');
  if (!header) return undefined;
  for (const pair of header.split(';')) {
    const index = pair.indexOf('=');
    if (index < 0) continue;
    if (pair.slice(0, index).trim() === name) {
      return pair.slice(index + 1).trim();
    }
  }
  return undefined;
}

export async function verifyState(
  secret: string,
  state: string,
  now: number,
): Promise<boolean> {
  const parts = state.split('.');
  if (parts.length !== 3) return false;
  const [nonce, expiry, signature] = parts as [string, string, string];
  const payload = `${nonce}.${expiry}`;
  const expected = await hmacSign(secret, payload);
  // crypto.subtle.verify would be constant-time; for a 10-minute CSRF nonce
  // a length-guarded comparison of two HMACs (attacker knows neither) is
  // sufficient — still, compare via timingSafe-ish accumulation.
  if (signature.length !== expected.length) return false;
  let diff = 0;
  for (let index = 0; index < expected.length; index += 1) {
    diff |= signature.charCodeAt(index) ^ expected.charCodeAt(index);
  }
  if (diff !== 0) return false;
  return Number(expiry) > now;
}

// ---- token minting ---------------------------------------------------------

export const DEMO_JWT_ISSUER = 'anchorage-demo';
export const DEMO_JWT_AUDIENCE = 'anchorage-showcase';
export const DEMO_JWT_KID = 'demo1';

export interface DemoTokenSet {
  tenantId: string;
  /** ISO expiry of the TENANT (tokens refresh silently until then). */
  tenantExpiresAt: string;
  tokens: Array<{ id: string; role: ApprovalRole; token: string }>;
}

export async function mintDemoTokenSet(options: {
  secret: string;
  tenant: DemoTenantRow;
  ttlSeconds: number;
  now?: () => number;
}): Promise<DemoTokenSet> {
  const tokens = [];
  for (const entry of DEMO_TOKEN_ROLES) {
    const actor: ApprovalActor = {
      id: entry.id,
      role: entry.role,
      tenantId: options.tenant.tenant_id,
    };
    tokens.push({
      id: entry.id,
      role: entry.role,
      token: await mintHmacToken({
        secret: options.secret,
        kid: DEMO_JWT_KID,
        issuer: DEMO_JWT_ISSUER,
        audience: DEMO_JWT_AUDIENCE,
        actor,
        ttlSeconds: options.ttlSeconds,
        now: options.now,
      }),
    });
  }
  return {
    tenantId: options.tenant.tenant_id,
    tenantExpiresAt: options.tenant.expires_at,
    tokens,
  };
}

// ---- the auth router --------------------------------------------------------

export interface DemoAuthRouterOptions {
  db: DemoDatabase & TenantRegistryDatabase;
  provider: OAuthProvider;
  /** The HS256 secret demo JWTs are signed with (also signs OAuth state). */
  secret: string;
  /** JWT lifetime (~1h): short enough to make the kill switch bite fast. */
  jwtTtlSeconds: number;
  /** Tenant lifetime (~24h): after this, re-auth mints a FRESH sandbox. */
  tenantTtlMs: number;
  purgeTenantData: (tenantId: string) => Promise<unknown>;
  /**
   * Kill switch. Checked here for mint/refresh; the WORKER must also gate
   * its verifier with it so ALREADY-ISSUED JWTs die too.
   */
  disabled: boolean;
  now?: () => number;
}

function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      'content-type': 'application/json',
      'cache-control': 'no-store',
    },
  });
}

/**
 * Routes (null for paths outside /auth/*, so hosts compose it first; the
 * <provider> segment is the mounted provider's name — google or github):
 *   GET  /auth/<provider>          -> 302 to the provider (signed state)
 *   GET  /auth/<provider>/callback -> verify state, exchange code, mint the
 *                                   tenant + token set, 302 to /#demo-tokens=
 *                                   <base64url(JSON DemoTokenSet)> — a
 *                                   FRAGMENT, so tokens never hit server logs
 *   POST /auth/refresh           -> { token } -> fresh token set while the
 *                                   TENANT row is live (the mid-demo
 *                                   reviewer-switch flow must survive a JWT
 *                                   expiry; after tenant expiry: 401,
 *                                   re-auth mints a fresh sandbox)
 */
export function createDemoAuthRouter(
  options: DemoAuthRouterOptions,
): (request: Request) => Promise<Response | null> {
  const now = options.now ?? Date.now;
  return async (request: Request): Promise<Response | null> => {
    const url = new URL(request.url);
    if (!url.pathname.startsWith('/auth/')) return null;
    // Unauthenticated capability probe for the SPA's sign-in button; carries
    // no secrets and honestly reports the kill switch.
    if (request.method === 'GET' && url.pathname === '/auth/config') {
      return json({
        enabled: !options.disabled,
        provider: options.provider.name,
      });
    }
    if (options.disabled) {
      return json({ error: 'the demo is temporarily disabled' }, 503);
    }
    const redirectUri = `${url.origin}/auth/${options.provider.name}/callback`;

    if (
      request.method === 'GET' &&
      url.pathname === `/auth/${options.provider.name}`
    ) {
      const { state, nonce } = await signState(options.secret, now());
      // SameSite=Lax still sends the cookie on the provider's top-level GET
      // redirect back to us, while blocking cross-site POST/subresource use.
      return new Response(null, {
        status: 302,
        headers: {
          location: options.provider.authorizeUrl({ state, redirectUri }),
          'cache-control': 'no-store',
          'set-cookie': `${STATE_COOKIE}=${nonce}; HttpOnly; Secure; SameSite=Lax; Path=/auth; Max-Age=600`,
        },
      });
    }

    if (
      request.method === 'GET' &&
      url.pathname === `/auth/${options.provider.name}/callback`
    ) {
      const state = url.searchParams.get('state') ?? '';
      const code = url.searchParams.get('code') ?? '';
      // Signature + expiry, THEN the browser binding: a signed state the
      // attacker minted carries a nonce this browser never received, so the
      // login-CSRF walk-the-victim-into-my-sandbox path fails here.
      const nonce = nonceOfState(state);
      if (
        !(await verifyState(options.secret, state, now())) ||
        nonce === undefined ||
        cookieValue(request, STATE_COOKIE) !== nonce
      ) {
        return json({ error: 'invalid or expired state' }, 403);
      }
      const identity = await options.provider.exchange({ code, redirectUri });
      if (!identity) return json({ error: 'sign-in failed' }, 401);
      const tenant = await findOrCreateDemoTenant(options.db, {
        provider: options.provider.name,
        subject: identity.subject,
        tenantTtlMs: options.tenantTtlMs,
        now,
        purgeTenantData: options.purgeTenantData,
      });
      const tokenSet = await mintDemoTokenSet({
        secret: options.secret,
        tenant,
        ttlSeconds: options.jwtTtlSeconds,
        now,
      });
      const fragment = base64UrlEncode(
        encoder.encode(JSON.stringify(tokenSet)),
      );
      return new Response(null, {
        status: 302,
        headers: {
          location: `/#demo-tokens=${fragment}`,
          'cache-control': 'no-store',
          // One round-trip per nonce: expire the binding cookie now.
          'set-cookie': `${STATE_COOKIE}=; HttpOnly; Secure; SameSite=Lax; Path=/auth; Max-Age=0`,
        },
      });
    }

    if (request.method === 'POST' && url.pathname === '/auth/refresh') {
      const body = (await request.json().catch(() => undefined)) as
        | { token?: string }
        | undefined;
      if (typeof body?.token !== 'string') {
        return json({ error: 'token is required' }, 400);
      }
      // Decode WITHOUT full verification of exp: a just-expired JWT may
      // still refresh while its TENANT is live — but the SIGNATURE and the
      // tenant claim must verify, so only a holder of a genuinely-issued
      // token can refresh. Reuse the verifier with a clock pinned to the
      // token's own iat? Simpler and safe: require the presented token to
      // still verify (SPA refreshes BEFORE expiry — 'silent refresh'), and
      // let a fully-expired token fall back to re-auth.
      const verify = hmacVerifier({
        keys: new Map([[DEMO_JWT_KID, options.secret]]),
        issuer: DEMO_JWT_ISSUER,
        audience: DEMO_JWT_AUDIENCE,
        now,
      });
      const actor = await verify.verify(body.token);
      if (!actor) return json({ error: 'invalid token' }, 401);
      await ensureDemoSchema(options.db);
      const tenant = await options.db
        .prepare('SELECT * FROM demo_tenants WHERE tenant_id = ?')
        .bind(actor.tenantId)
        .first<DemoTenantRow>();
      if (!tenant || Date.parse(tenant.expires_at) <= now()) {
        return json(
          { error: 'sandbox expired; sign in again for a fresh one' },
          401,
        );
      }
      return json(
        await mintDemoTokenSet({
          secret: options.secret,
          tenant,
          ttlSeconds: options.jwtTtlSeconds,
          now,
        }),
      );
    }

    return json({ error: 'not found' }, 404);
  };
}
