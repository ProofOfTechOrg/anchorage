// Public-demo identity: OAuth sign-in -> an expiring session in one shared
// demo organization -> short-TTL HS256 JWTs for four distinct role actors.
// The session is authentication and spend-limit metadata, not a physical data
// boundary. Approval records form one role-visible deployment queue; run access
// remains scoped to the session principal, reviewers/viewers, and admins.
//
// UNIQUE(provider, subject) stops one identity holding multiple live sessions.
// The real backstops react to load: a per-session run cap, the global daily run
// ceiling, the kill switch, and billing alerts. Both counters use conditional
// UPDATEs so parallel requests cannot walk through a SELECT-then-UPDATE race.
//
// What the run budgets DO bound: run STARTS and raw RESUME attempts — the
// worker charges both before the DO round-trip, so a garbage-resume loop
// against a suspended run burns budget instead of free DO CPU (a resume
// that fails resumeSchema still cost a DO fetch + D1 snapshot read). Queue
// DECISIONS are unmetered by design: each approval record is decidable
// once, and records only exist because a charged start/resume suspended.
// What they do not bound: status GETs, /auth/refresh, and approval-queue
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
} from '@proofoftech/flowsafe/host-kit';
import { Hono } from 'hono';
import { bodyLimit } from 'hono/body-limit';
import {
  authorizationCodeGrant,
  buildAuthorizationUrl,
  ClientSecretPost,
  Configuration,
  customFetch,
  enableNonRepudiationChecks,
  type ServerMetadata,
} from 'openid-client';
import { z } from 'zod';

const MAX_REFRESH_BODY_BYTES = 16_384;
const REFRESH_BODY_SCHEMA = z.strictObject({ token: z.string() });

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
  `CREATE TABLE IF NOT EXISTS demo_sessions (
    session_id TEXT PRIMARY KEY,
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

/** The four demo roles minted per visitor — distinct actor ids keep the reviewer
 * separation-of-duties lane demonstrable: a reviewer who advanced a run
 * still gets a 403 on that gate, and switching to another decider clears it.
 * admin is exempted from SoD via APPROVAL_ALLOW_SELF_DECISION (so it can drive
 * product-launch's two gates alone); the distinct ids keep that a deliberate,
 * per-role relaxation rather than a blanket one. */
export const DEMO_TOKEN_ROLES: readonly ApprovalRole[] = [
  'admin',
  'operator',
  'reviewer',
  'viewer',
];

export interface DemoSessionRow {
  session_id: string;
  provider: string;
  subject: string;
  created_at: string;
  expires_at: string;
  run_count: number;
}

/** Opaque 72-bit session id. */
export function mintDemoSessionId(): string {
  const bytes = new Uint8Array(9);
  crypto.getRandomValues(bytes);
  let hex = '';
  for (const byte of bytes) hex += byte.toString(16).padStart(2, '0');
  return hex;
}

export function demoActorId(sessionId: string, role: ApprovalRole): string {
  return `demo-${sessionId}-${role}`;
}

export function demoSessionIdOfActor(actor: ApprovalActor): string | undefined {
  const match = /^demo-([0-9a-f]{18})-(admin|operator|reviewer|viewer)$/.exec(
    actor.id,
  );
  if (!match || match[2] !== actor.role) return undefined;
  return match[1];
}

export interface DemoSessionOptions {
  provider: string;
  /** Provider-scoped stable subject, e.g. `github:<numeric id>`. */
  subject: string;
  /** Session lifetime; re-auth after expiry mints a fresh session. */
  sessionTtlMs: number;
  now?: () => number;
}

/**
 * One live session per (provider, subject). Its clock is not extended on
 * re-authentication; expiry always runs from the first sign-in.
 */
export async function findOrCreateDemoSession(
  db: DemoDatabase,
  options: DemoSessionOptions,
): Promise<DemoSessionRow> {
  const now = (options.now ?? Date.now)();
  await ensureDemoSchema(db);
  const existing = await db
    .prepare('SELECT * FROM demo_sessions WHERE provider = ? AND subject = ?')
    .bind(options.provider, options.subject)
    .first<DemoSessionRow>();
  if (existing && Date.parse(existing.expires_at) > now) {
    return existing;
  }
  if (existing) {
    await db
      .prepare(
        'DELETE FROM demo_sessions WHERE session_id = ? AND expires_at <= ?',
      )
      .bind(existing.session_id, new Date(now).toISOString())
      .run();
  }

  for (let attempt = 0; attempt < 2; attempt += 1) {
    const row: DemoSessionRow = {
      session_id: mintDemoSessionId(),
      provider: options.provider,
      subject: options.subject,
      created_at: new Date(now).toISOString(),
      expires_at: new Date(now + options.sessionTtlMs).toISOString(),
      run_count: 0,
    };
    const insert = await db
      .prepare(
        `INSERT OR IGNORE INTO demo_sessions
         (session_id, provider, subject, created_at, expires_at, run_count)
         VALUES (?, ?, ?, ?, ?, 0)`,
      )
      .bind(
        row.session_id,
        row.provider,
        row.subject,
        row.created_at,
        row.expires_at,
      )
      .run();
    if (d1Changes(insert) > 0) return row;

    const winner = await db
      .prepare('SELECT * FROM demo_sessions WHERE provider = ? AND subject = ?')
      .bind(options.provider, options.subject)
      .first<DemoSessionRow>();
    if (winner && Date.parse(winner.expires_at) > now) return winner;
  }
  throw new Error('could not allocate a demo session; retry sign-in');
}

/**
 * Machine-readable cause of a budget refusal. `scope` says WHICH budget bit
 * ('session' caps one sign-in; 'global' is the deployment-wide daily ceiling);
 * `reason` says WHY, so consumers branch on it instead of string-matching
 * `message` (whose copy is UX and may change).
 */
export type DemoRunLimitReason = 'not-provisioned' | 'expired' | 'cap-reached';

export class DemoRunLimitError extends Error {
  readonly scope: 'session' | 'global';
  readonly reason: DemoRunLimitReason;

  constructor(
    scope: 'session' | 'global',
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
  sessionId: string;
  /** Max runs per demo session over its lifetime. */
  sessionRunCap: number;
  /** Global ceiling across all sessions per UTC day. */
  dailyRunCap: number;
  now?: () => number;
}

/**
 * Consume one run from BOTH budgets, atomically each: a single conditional
 * UPDATE per budget — `meta.changes === 0` means the cap held (or, for the
 * session budget, the session is missing/expired; the follow-up read is
 * diagnosability only, on the already-failing path). A SELECT-then-UPDATE
 * would be a TOCTOU race a burst of parallel starts walks straight through.
 *
 * Order: the session budget first. A consumed slot on a subsequent global-cap
 * failure only under-counts that session's remaining budget — fail
 * safe, never fail open.
 */
export async function consumeRunBudget(
  db: DemoDatabase,
  options: RunBudgetOptions,
): Promise<void> {
  const now = (options.now ?? Date.now)();
  const nowIso = new Date(now).toISOString();
  const sessionUpdate = await db
    .prepare(
      `UPDATE demo_sessions SET run_count = run_count + 1
       WHERE session_id = ? AND run_count < ? AND expires_at > ?`,
    )
    .bind(options.sessionId, options.sessionRunCap, nowIso)
    .run();
  if (d1Changes(sessionUpdate) === 0) {
    const row = await db
      .prepare(
        'SELECT run_count, expires_at FROM demo_sessions WHERE session_id = ?',
      )
      .bind(options.sessionId)
      .first<{ run_count: number; expires_at: string }>();
    if (!row) {
      throw new DemoRunLimitError(
        'session',
        'not-provisioned',
        'demo session not found',
      );
    }
    if (Date.parse(row.expires_at) <= now) {
      throw new DemoRunLimitError(
        'session',
        'expired',
        'demo session expired; sign in again',
      );
    }
    throw new DemoRunLimitError(
      'session',
      'cap-reached',
      `demo run limit reached (${options.sessionRunCap} runs per session)`,
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

export interface DeleteExpiredDemoSessionsOptions {
  /** Expired sessions processed per invocation — the cron's CPU budget guard. */
  limit?: number;
  /**
   * Grace after `expires_at`, normally the JWT lifetime. This keeps metadata
   * available while the last token can still reach read-only routes.
   */
  graceMs: number;
  now?: () => number;
}

/**
 * Delete expired demo-session metadata, oldest first and bounded per pass.
 * Run and approval records remain in the shared organization and are handled
 * by the normal retention duties.
 */
export async function deleteExpiredDemoSessions(
  db: DemoDatabase,
  options: DeleteExpiredDemoSessionsOptions,
): Promise<string[]> {
  const now = (options.now ?? Date.now)();
  await ensureDemoSchema(db);
  const { results } = await db
    .prepare(
      `SELECT session_id FROM demo_sessions WHERE expires_at <= ?
       ORDER BY expires_at ASC LIMIT ?`,
    )
    .bind(new Date(now - options.graceMs).toISOString(), options.limit ?? 25)
    .all<{ session_id: string }>();
  const deleted: string[] = [];
  for (const row of results) {
    await db
      .prepare('DELETE FROM demo_sessions WHERE session_id = ?')
      .bind(row.session_id)
      .run();
    deleted.push(row.session_id);
  }
  return deleted;
}

// ---- OAuth (providers behind a seam; Google launches, GitHub falls back) ---

export interface OAuthProvider {
  readonly name: string;
  authorizeUrl(input: { state: string; redirectUri: string }): string;
  /** Exchange the callback code for a provider-scoped stable subject. */
  exchange(input: {
    code: string;
    redirectUri: string;
    /** Full callback URL lets the protocol client validate every parameter. */
    callbackUrl?: URL;
    /** The already signature- and cookie-validated state expectation. */
    state?: string;
  }): Promise<{ subject: string } | undefined>;
}

const GITHUB_SERVER: ServerMetadata = {
  issuer: 'https://github.com',
  authorization_endpoint: 'https://github.com/login/oauth/authorize',
  token_endpoint: 'https://github.com/login/oauth/access_token',
  token_endpoint_auth_methods_supported: ['client_secret_post'],
};

const GOOGLE_SERVER: ServerMetadata = {
  issuer: 'https://accounts.google.com',
  authorization_endpoint: 'https://accounts.google.com/o/oauth2/v2/auth',
  token_endpoint: 'https://oauth2.googleapis.com/token',
  jwks_uri: 'https://www.googleapis.com/oauth2/v3/certs',
  id_token_signing_alg_values_supported: ['RS256'],
  token_endpoint_auth_methods_supported: ['client_secret_post'],
};

function providerConfiguration(
  server: ServerMetadata,
  options: { clientId: string; clientSecret: string },
  fetchFn: typeof fetch,
): Configuration {
  const config = new Configuration(
    server,
    options.clientId,
    undefined,
    ClientSecretPost(options.clientSecret),
  );
  config[customFetch] = (url, init) =>
    fetchFn(url, init as unknown as RequestInit);
  return config;
}

function callbackUrl(input: {
  code: string;
  redirectUri: string;
  callbackUrl?: URL;
  state?: string;
}): URL {
  if (input.callbackUrl) return new URL(input.callbackUrl);
  const url = new URL(input.redirectUri);
  if (input.code) url.searchParams.set('code', input.code);
  if (input.state) url.searchParams.set('state', input.state);
  return url;
}

interface GithubProviderOptions {
  clientId: string;
  clientSecret: string;
  /** Injectable for tests. Default: globalThis.fetch. */
  fetch?: typeof fetch;
}

export function githubProvider(options: GithubProviderOptions): OAuthProvider {
  const fetchFn = options.fetch ?? fetch;
  const protocolFetch = (async (input, init) => {
    const headers = new Headers(init?.headers);
    headers.set('accept', 'application/json');
    return fetchFn(input, { ...init, headers });
  }) as typeof fetch;
  const config = providerConfiguration(GITHUB_SERVER, options, protocolFetch);
  return {
    name: 'github',
    authorizeUrl({ state, redirectUri }) {
      return buildAuthorizationUrl(config, {
        redirect_uri: redirectUri,
        response_type: 'code',
        state,
      }).toString();
    },
    async exchange(input) {
      try {
        const tokens = await authorizationCodeGrant(
          config,
          callbackUrl(input),
          { expectedState: input.state },
        );
        const userResponse = await fetchFn('https://api.github.com/user', {
          headers: {
            authorization: `Bearer ${tokens.access_token}`,
            accept: 'application/vnd.github+json',
            'user-agent': 'anchorage-demo',
          },
        });
        if (!userResponse.ok) return undefined;
        const user = (await userResponse.json()) as { id?: number };
        if (typeof user.id !== 'number') return undefined;
        // The NUMERIC id is the stable subject — logins are renameable.
        return { subject: `github:${user.id}` };
      } catch {
        return undefined;
      }
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
  const config = providerConfiguration(GOOGLE_SERVER, options, fetchFn);
  enableNonRepudiationChecks(config);
  return {
    name: 'google',
    authorizeUrl({ state, redirectUri }) {
      const nonce = nonceOfState(state);
      return buildAuthorizationUrl(config, {
        redirect_uri: redirectUri,
        response_type: 'code',
        // `openid` alone is the narrowest consent: the validated ID Token's
        // stable `sub` is sufficient; no email or profile request is needed.
        scope: 'openid',
        state,
        ...(nonce ? { nonce } : {}),
      }).toString();
    },
    async exchange(input) {
      try {
        const nonce = input.state ? nonceOfState(input.state) : undefined;
        const tokens = await authorizationCodeGrant(
          config,
          callbackUrl(input),
          {
            expectedState: input.state,
            expectedNonce: nonce,
            idTokenExpected: true,
          },
        );
        const subject = tokens.claims()?.sub;
        if (typeof subject !== 'string' || subject.length === 0) {
          return undefined;
        }
        return { subject: `google:${subject}` };
      } catch {
        return undefined;
      }
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
 * with the ATTACKER's code, landing the victim in the attacker's demo session
 * (login CSRF). The nonce is therefore also set as an HttpOnly cookie and
 * must match at the callback — one browser, one round-trip.
 */
export async function signState(
  secret: string,
  now: number,
  ttlMs = 10 * 60 * 1000,
): Promise<{ state: string; nonce: string }> {
  const nonce = mintDemoSessionId();
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
  sessionId: string;
  /** ISO expiry of the session; tokens refresh silently until then. */
  sessionExpiresAt: string;
  tokens: Array<{ id: string; role: ApprovalRole; token: string }>;
}

export async function mintDemoTokenSet(options: {
  secret: string;
  session: DemoSessionRow;
  ttlSeconds: number;
  now?: () => number;
}): Promise<DemoTokenSet> {
  const tokens = [];
  for (const role of DEMO_TOKEN_ROLES) {
    const id = demoActorId(options.session.session_id, role);
    const actor: ApprovalActor = {
      id,
      role,
    };
    tokens.push({
      id,
      role,
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
    sessionId: options.session.session_id,
    sessionExpiresAt: options.session.expires_at,
    tokens,
  };
}

// ---- the auth router --------------------------------------------------------

export interface DemoAuthRouterOptions {
  db: DemoDatabase;
  provider: OAuthProvider;
  /** The HS256 secret demo JWTs are signed with (also signs OAuth state). */
  secret: string;
  /** JWT lifetime (~1h): short enough to make the kill switch bite fast. */
  jwtTtlSeconds: number;
  /** Session lifetime (~24h): after this, re-auth mints a fresh session. */
  sessionTtlMs: number;
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
 *                                   session + token set, 302 to /#demo-tokens=
 *                                   <base64url(JSON DemoTokenSet)> — a
 *                                   FRAGMENT, so tokens never hit server logs
 *   POST /auth/refresh           -> { token } -> fresh token set while the
 *                                   session row is live (the mid-demo
 *                                   reviewer-switch flow must survive a JWT
 *                                   expiry; after session expiry: 401)
 */
export function createDemoAuthRouter(
  options: DemoAuthRouterOptions,
): (request: Request) => Promise<Response | null> {
  const now = options.now ?? Date.now;
  const app = new Hono();

  // Registered before the disabled middleware: this unauthenticated probe
  // must honestly report the kill switch even while every other route 503s.
  app.get('/auth/config', () =>
    json({
      enabled: !options.disabled,
      provider: options.provider.name,
    }),
  );

  app.use('/auth/*', async (_context, next) => {
    if (options.disabled) {
      return json({ error: 'the demo is temporarily disabled' }, 503);
    }
    await next();
  });

  app.get(`/auth/${options.provider.name}`, async (context) => {
    const url = new URL(context.req.url);
    const redirectUri = `${url.origin}/auth/${options.provider.name}/callback`;
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
  });

  app.get(`/auth/${options.provider.name}/callback`, async (context) => {
    const url = new URL(context.req.url);
    const redirectUri = `${url.origin}/auth/${options.provider.name}/callback`;
    const state = url.searchParams.get('state') ?? '';
    const code = url.searchParams.get('code') ?? '';
    // Signature + expiry, THEN the browser binding: a signed state the
    // attacker minted carries a nonce this browser never received, so the
    // login-CSRF walk-the-victim-into-my-session path fails here.
    const nonce = nonceOfState(state);
    if (
      !(await verifyState(options.secret, state, now())) ||
      nonce === undefined ||
      cookieValue(context.req.raw, STATE_COOKIE) !== nonce
    ) {
      return json({ error: 'invalid or expired state' }, 403);
    }
    let identity: { subject: string } | undefined;
    try {
      identity = await options.provider.exchange({
        code,
        redirectUri,
        callbackUrl: url,
        state,
      });
    } catch {
      // Provider errors are intentionally collapsed into the stable public
      // envelope below; credentials and upstream details never reach it.
    }
    if (!identity) return json({ error: 'sign-in failed' }, 401);
    const session = await findOrCreateDemoSession(options.db, {
      provider: options.provider.name,
      subject: identity.subject,
      sessionTtlMs: options.sessionTtlMs,
      now,
    });
    const tokenSet = await mintDemoTokenSet({
      secret: options.secret,
      session,
      ttlSeconds: options.jwtTtlSeconds,
      now,
    });
    const fragment = base64UrlEncode(encoder.encode(JSON.stringify(tokenSet)));
    return new Response(null, {
      status: 302,
      headers: {
        location: `/#demo-tokens=${fragment}`,
        'cache-control': 'no-store',
        // One round-trip per nonce: expire the binding cookie now.
        'set-cookie': `${STATE_COOKIE}=; HttpOnly; Secure; SameSite=Lax; Path=/auth; Max-Age=0`,
      },
    });
  });

  app.post(
    '/auth/refresh',
    bodyLimit({
      maxSize: MAX_REFRESH_BODY_BYTES,
      onError: () => json({ error: 'payload too large' }, 413),
    }),
    async (context) => {
      let value: unknown;
      try {
        const bytes = await context.req.raw.arrayBuffer();
        const text = new TextDecoder('utf-8', {
          fatal: true,
          ignoreBOM: false,
        }).decode(bytes);
        value = JSON.parse(text);
      } catch (error) {
        return error instanceof SyntaxError
          ? json({ error: 'token is required' }, 400)
          : json({ error: 'a JSON object body is required' }, 400);
      }
      const parsed = REFRESH_BODY_SCHEMA.safeParse(value);
      if (!parsed.success) return json({ error: 'token is required' }, 400);
      // Require the presented token to remain valid. The SPA refreshes before
      // expiry; a fully expired token falls back to OAuth sign-in.
      const verify = hmacVerifier({
        keys: new Map([[DEMO_JWT_KID, options.secret]]),
        issuer: DEMO_JWT_ISSUER,
        audience: DEMO_JWT_AUDIENCE,
        now,
      });
      const actor = await verify.verify(parsed.data.token);
      if (!actor) return json({ error: 'invalid token' }, 401);
      const sessionId = demoSessionIdOfActor(actor);
      if (!sessionId) return json({ error: 'invalid token' }, 401);
      await ensureDemoSchema(options.db);
      const session = await options.db
        .prepare('SELECT * FROM demo_sessions WHERE session_id = ?')
        .bind(sessionId)
        .first<DemoSessionRow>();
      if (!session || Date.parse(session.expires_at) <= now()) {
        return json({ error: 'session expired; sign in again' }, 401);
      }
      return json(
        await mintDemoTokenSet({
          secret: options.secret,
          session,
          ttlSeconds: options.jwtTtlSeconds,
          now,
        }),
      );
    },
  );

  app.notFound(() => json({ error: 'not found' }, 404));

  return async (request: Request): Promise<Response | null> => {
    if (!new URL(request.url).pathname.startsWith('/auth/')) return null;
    if (request.method === 'HEAD') {
      return options.disabled
        ? json({ error: 'the demo is temporarily disabled' }, 503)
        : json({ error: 'not found' }, 404);
    }
    return app.fetch(request);
  };
}
