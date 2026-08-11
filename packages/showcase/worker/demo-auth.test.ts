// The public demo's identity + abuse machinery, against real SQLite: session
// lifecycle (mint / reuse / expire-and-replace), the two atomic run budgets,
// LIMIT-batched session cleanup, signed OAuth
// state, the token set, the auth router's full round-trip with a fake
// provider, and the kill switch — including its auth-middleware half (an
// ALREADY-ISSUED JWT must die with the switch, not just new mints).

import { openSqlite, type SqliteDatabase } from '@flowsafe-test/sqlite.js';
import { hmacVerifier } from '@proofoftech/flowsafe/host-kit';
import { beforeAll, describe, expect, it, vi } from 'vitest';
import {
  consumeRunBudget,
  createDemoAuthRouter,
  DEMO_JWT_AUDIENCE,
  DEMO_JWT_ISSUER,
  DEMO_JWT_KID,
  DEMO_TOKEN_ROLES,
  type DemoDatabase,
  DemoRunLimitError,
  type DemoStatement,
  type DemoTokenSet,
  deleteExpiredDemoSessions,
  findOrCreateDemoSession,
  githubProvider,
  googleProvider,
  mintDemoSessionId,
  mintDemoTokenSet,
  nonceOfState,
  type OAuthProvider,
  STATE_COOKIE,
  signState,
  verifyState,
} from '#worker/demo-auth';
import { buildVerifier, selectOAuthProvider } from '#worker/worker';

function demoDb(db: SqliteDatabase): DemoDatabase {
  function statement(sql: string, params: unknown[]): DemoStatement {
    return {
      bind: (...values: unknown[]) => statement(sql, values),
      run: async () => {
        const outcome = db.prepare(sql).run(...params) as {
          changes?: number | bigint;
        };
        return { meta: { changes: Number(outcome?.changes ?? 0) } };
      },
      first: async <T>() =>
        (db.prepare(sql).get(...params) as T | undefined) ?? null,
      all: async <T>() => ({ results: db.prepare(sql).all(...params) as T[] }),
    };
  }
  return { prepare: (sql: string) => statement(sql, []) };
}

const T0 = Date.parse('2026-07-09T12:00:00.000Z');
const HOUR = 3_600_000;
const SECRET = 'demo-test-secret';

function makeSessionOptions() {
  return {
    provider: 'github',
    subject: 'github:1234',
    sessionTtlMs: 24 * HOUR,
    now: () => T0,
  };
}

describe('mintDemoSessionId', () => {
  it('always mints an opaque 72-bit hex id', () => {
    for (let index = 0; index < 20; index += 1) {
      const id = mintDemoSessionId();
      expect(id).toMatch(/^[0-9a-f]{18}$/);
    }
  });
});

describe('findOrCreateDemoSession', () => {
  it('mints a fresh session without creating a logical data boundary', async () => {
    // #given
    const sqlite = openSqlite();
    const db = demoDb(sqlite);

    // #when
    const session = await findOrCreateDemoSession(db, makeSessionOptions());

    // #then — only auth/budget metadata exists; no tenant registry is created
    expect(session.session_id).toMatch(/^[0-9a-f]{18}$/);
    expect(session.run_count).toBe(0);
    expect(
      sqlite
        .prepare("SELECT name FROM sqlite_master WHERE name = 'tenants'")
        .get(),
    ).toBeUndefined();
  });

  it('reuses the live session for the same identity without extending its clock', async () => {
    // #given
    const db = demoDb(openSqlite());
    const first = await findOrCreateDemoSession(db, makeSessionOptions());

    // #when — same subject signs in 1h later
    const second = await findOrCreateDemoSession(db, {
      ...makeSessionOptions(),
      now: () => T0 + HOUR,
    });

    // #then — same session, same expiry (lifetime runs from first sign-in)
    expect(second.session_id).toBe(first.session_id);
    expect(second.expires_at).toBe(first.expires_at);
  });

  it('replaces an expired session with fresh metadata', async () => {
    // #given — a session past its 24h lifetime
    const db = demoDb(openSqlite());
    const first = await findOrCreateDemoSession(db, makeSessionOptions());

    // #when — the same identity returns after expiry
    const second = await findOrCreateDemoSession(db, {
      ...makeSessionOptions(),
      now: () => T0 + 25 * HOUR,
    });

    // #then — the old session row is replaced; shared run data is unrelated
    expect(second.session_id).not.toBe(first.session_id);
    const rows = await db.prepare('SELECT session_id FROM demo_sessions').all();
    expect(rows.results).toEqual([{ session_id: second.session_id }]);
  });
});

describe('consumeRunBudget (atomic caps)', () => {
  async function seededDb(): Promise<{
    db: DemoDatabase;
    sessionId: string;
  }> {
    const db = demoDb(openSqlite());
    const session = await findOrCreateDemoSession(db, makeSessionOptions());
    return { db, sessionId: session.session_id };
  }

  it('enforces the per-session cap as one conditional UPDATE', async () => {
    // #given — cap 2
    const { db, sessionId } = await seededDb();
    const options = {
      sessionId,
      sessionRunCap: 2,
      dailyRunCap: 100,
      now: () => T0 + 1000,
    };

    // #when / #then — two consume, the third refuses; the count NEVER
    // overshoots because increment-and-check is one statement
    await consumeRunBudget(db, options);
    await consumeRunBudget(db, options);
    const refusal = consumeRunBudget(db, options);
    await expect(refusal).rejects.toBeInstanceOf(DemoRunLimitError);
    await expect(refusal).rejects.toMatchObject({
      scope: 'session',
      reason: 'cap-reached',
    });
    const row = (await db
      .prepare('SELECT run_count FROM demo_sessions WHERE session_id = ?')
      .bind(sessionId)
      .first()) as { run_count: number };
    expect(row.run_count).toBe(2);
  });

  it('cap 0 refuses the FIRST run — the incident freeze the worker passes via allowZero', async () => {
    // #given — a live session and DEMO_SESSION_RUN_CAP=0 (the
    // operator's freeze; numberVar's allowZero keeps it from reverting to the
    // fallback). run_count < 0 is never satisfiable, so the very first
    // conditional UPDATE refuses.
    const { db, sessionId } = await seededDb();

    // #when / #then
    await expect(
      consumeRunBudget(db, {
        sessionId,
        sessionRunCap: 0,
        dailyRunCap: 100,
        now: () => T0 + 1000,
      }),
    ).rejects.toMatchObject({ scope: 'session', reason: 'cap-reached' });
  });

  it('daily cap 0 freezes the whole demo even when session budgets remain', async () => {
    // #given
    const { db, sessionId } = await seededDb();

    // #when / #then
    await expect(
      consumeRunBudget(db, {
        sessionId,
        sessionRunCap: 10,
        dailyRunCap: 0,
        now: () => T0 + 1000,
      }),
    ).rejects.toMatchObject({ scope: 'global', reason: 'cap-reached' });
  });

  it('refuses runs for an expired session even under its cap', async () => {
    // #given
    const { db, sessionId } = await seededDb();

    // #when / #then — 25h later the session is dead
    await expect(
      consumeRunBudget(db, {
        sessionId,
        sessionRunCap: 10,
        dailyRunCap: 100,
        now: () => T0 + 25 * HOUR,
      }),
    ).rejects.toMatchObject({ scope: 'session', reason: 'expired' });
  });

  it('refuses runs for an unknown session', async () => {
    // #given
    const { db } = await seededDb();

    // #when / #then
    await expect(
      consumeRunBudget(db, {
        sessionId: '000000000000000000',
        sessionRunCap: 10,
        dailyRunCap: 100,
        now: () => T0 + 1000,
      }),
    ).rejects.toMatchObject({ scope: 'session', reason: 'not-provisioned' });
  });

  it('the global daily ceiling stops runs across sessions', async () => {
    // #given — two sessions, daily cap 3
    const sqlite = openSqlite();
    const db = demoDb(sqlite);
    const a = await findOrCreateDemoSession(db, makeSessionOptions());
    const b = await findOrCreateDemoSession(db, {
      ...makeSessionOptions(),
      subject: 'github:5678',
    });
    const budget = (sessionId: string) => ({
      sessionId,
      sessionRunCap: 100,
      dailyRunCap: 3,
      now: () => T0 + 1000,
    });

    // #when — three runs land (2 from A, 1 from B)
    await consumeRunBudget(db, budget(a.session_id));
    await consumeRunBudget(db, budget(a.session_id));
    await consumeRunBudget(db, budget(b.session_id));

    // #then — the fourth refuses GLOBALLY, whoever asks
    await expect(
      consumeRunBudget(db, budget(b.session_id)),
    ).rejects.toMatchObject({ scope: 'global', reason: 'cap-reached' });

    // ...and the ceiling resets on the next UTC day (T0 is 12:00Z, so +13h
    // crosses midnight while the session is still live)
    await consumeRunBudget(db, {
      ...budget(a.session_id),
      now: () => T0 + 13 * HOUR,
    });
  });
});

describe('deleteExpiredDemoSessions', () => {
  it('deletes at most `limit` expired sessions per call, oldest first', async () => {
    // #given — three expired sessions, one live
    const db = demoDb(openSqlite());
    const expired: string[] = [];
    for (const [index, subject] of ['s1', 's2', 's3'].entries()) {
      const session = await findOrCreateDemoSession(db, {
        ...makeSessionOptions(),
        subject,
        // Stagger expiries so "oldest first" is observable.
        now: () => T0 - (10 - index) * HOUR - 25 * HOUR,
      });
      expired.push(session.session_id);
    }
    const live = await findOrCreateDemoSession(db, {
      ...makeSessionOptions(),
      subject: 'live',
    });
    // #when — limit 2, then the next invocation
    const first = await deleteExpiredDemoSessions(db, {
      graceMs: 0,
      limit: 2,
      now: () => T0,
    });
    const second = await deleteExpiredDemoSessions(db, {
      graceMs: 0,
      limit: 2,
      now: () => T0,
    });

    // #then — batches advance without a cursor row; the live session survives
    expect(first).toEqual([expired[0], expired[1]]);
    expect(second).toEqual([expired[2]]);
    const rows = (await db
      .prepare('SELECT session_id FROM demo_sessions')
      .all()) as { results: Array<{ session_id: string }> };
    expect(rows.results).toEqual([{ session_id: live.session_id }]);
  });
});

describe('signed OAuth state', () => {
  it('round-trips, rejects tampering, and expires', async () => {
    // #given
    const { state, nonce } = await signState(SECRET, T0);

    // #then
    expect(await verifyState(SECRET, state, T0 + 1000)).toBe(true);
    expect(await verifyState(SECRET, `${state}x`, T0 + 1000)).toBe(false);
    expect(await verifyState('other-secret', state, T0 + 1000)).toBe(false);
    expect(await verifyState(SECRET, state, T0 + 11 * 60 * 1000)).toBe(false);
    expect(await verifyState(SECRET, 'garbage', T0)).toBe(false);
    // the nonce is what the browser-binding cookie carries
    expect(nonceOfState(state)).toBe(nonce);
    expect(nonceOfState('garbage')).toBeUndefined();
  });
});

describe('mintDemoTokenSet', () => {
  it('mints four verifiable tokens with DISTINCT actor ids (SoD must stay demonstrable)', async () => {
    // #given
    const db = demoDb(openSqlite());
    const session = await findOrCreateDemoSession(db, makeSessionOptions());

    // #when
    const set = await mintDemoTokenSet({
      secret: SECRET,
      session,
      ttlSeconds: 3600,
      now: () => T0,
    });

    // #then
    expect(set.tokens).toHaveLength(DEMO_TOKEN_ROLES.length);
    expect(new Set(set.tokens.map((entry) => entry.id)).size).toBe(
      set.tokens.length,
    );
    const verify = hmacVerifier({
      keys: new Map([[DEMO_JWT_KID, SECRET]]),
      issuer: DEMO_JWT_ISSUER,
      audience: DEMO_JWT_AUDIENCE,
      now: () => T0 + 1000,
    });
    for (const entry of set.tokens) {
      const actor = await verify.verify(entry.token);
      expect(actor).toEqual({
        id: entry.id,
        role: entry.role,
      });
    }
  });
});

describe('googleProvider', () => {
  const OPTIONS = { clientId: 'client-1', clientSecret: 'secret-1' };
  const REDIRECT_URI = 'https://demo.test/auth/google/callback';
  const STATE = '0123456789abcdef01.1786400000000.state-signature';
  const NONCE = '0123456789abcdef01';
  let signingKey: CryptoKey;
  let publicJwk: JsonWebKey;

  function jsonResponse(payload: unknown, status = 200): Response {
    return new Response(JSON.stringify(payload), {
      status,
      headers: { 'content-type': 'application/json' },
    });
  }

  function base64Url(bytes: Uint8Array): string {
    let binary = '';
    for (const byte of bytes) binary += String.fromCharCode(byte);
    return btoa(binary)
      .replace(/=/g, '')
      .replace(/\+/g, '-')
      .replace(/\//g, '_');
  }

  async function idToken(
    overrides: Record<string, unknown> = {},
  ): Promise<string> {
    const now = Math.floor(Date.now() / 1000);
    const encodeJson = (value: unknown) =>
      base64Url(new TextEncoder().encode(JSON.stringify(value)));
    const protectedHeader = encodeJson({
      alg: 'RS256',
      kid: 'google-test-key',
      typ: 'JWT',
    });
    const claims = encodeJson({
      iss: 'https://accounts.google.com',
      aud: OPTIONS.clientId,
      sub: '1093874',
      nonce: NONCE,
      iat: now,
      exp: now + 300,
      ...overrides,
    });
    const input = `${protectedHeader}.${claims}`;
    const signature = await crypto.subtle.sign(
      'RSASSA-PKCS1-v1_5',
      signingKey,
      new TextEncoder().encode(input),
    );
    return `${input}.${base64Url(new Uint8Array(signature))}`;
  }

  function callback(parameters: string): URL {
    return new URL(`${REDIRECT_URI}?${parameters}`);
  }

  beforeAll(async () => {
    const pair = (await crypto.subtle.generateKey(
      {
        name: 'RSASSA-PKCS1-v1_5',
        modulusLength: 2048,
        publicExponent: new Uint8Array([1, 0, 1]),
        hash: 'SHA-256',
      },
      true,
      ['sign', 'verify'],
    )) as CryptoKeyPair;
    signingKey = pair.privateKey;
    publicJwk = (await crypto.subtle.exportKey(
      'jwk',
      pair.publicKey,
    )) as JsonWebKey;
  });

  it('builds a code-flow authorize URL with minimal scope and a bound OIDC nonce', () => {
    // #given / #when
    const url = new URL(
      googleProvider(OPTIONS).authorizeUrl({
        state: STATE,
        redirectUri: REDIRECT_URI,
      }),
    );

    // #then
    expect(url.origin + url.pathname).toBe(
      'https://accounts.google.com/o/oauth2/v2/auth',
    );
    expect(Object.fromEntries(url.searchParams)).toEqual({
      client_id: 'client-1',
      redirect_uri: REDIRECT_URI,
      response_type: 'code',
      scope: 'openid',
      state: STATE,
      nonce: NONCE,
    });
  });

  it('validates the Google ID Token and scopes its stable subject google:', async () => {
    // #given — a real RS256 ID Token and matching JWKS response
    const calls: Array<{ url: string; init?: RequestInit }> = [];
    const fetchFn = (async (input: unknown, init?: RequestInit) => {
      calls.push({ url: String(input), init });
      if (String(input).includes('oauth2.googleapis.com/token')) {
        return jsonResponse({
          access_token: 'at-123',
          token_type: 'Bearer',
          id_token: await idToken(),
        });
      }
      if (String(input).includes('googleapis.com/oauth2/v3/certs')) {
        return jsonResponse({
          keys: [
            {
              ...publicJwk,
              alg: 'RS256',
              kid: 'google-test-key',
              use: 'sig',
            },
          ],
        });
      }
      throw new Error(`unexpected fetch: ${String(input)}`);
    }) as typeof fetch;

    // #when
    const identity = await googleProvider({
      ...OPTIONS,
      fetch: fetchFn,
    }).exchange({
      code: 'good-code',
      redirectUri: REDIRECT_URI,
      callbackUrl: callback(
        `code=good-code&state=${encodeURIComponent(STATE)}`,
      ),
      state: STATE,
    });

    // #then — provider-scoped stable subject
    expect(identity).toEqual({ subject: 'google:1093874' });
    const [token, jwks] = calls;
    expect(token?.url).toBe('https://oauth2.googleapis.com/token');
    expect(new Headers(token?.init?.headers).get('content-type')).toMatch(
      /^application\/x-www-form-urlencoded(?:;|$)/,
    );
    expect(
      Object.fromEntries(new URLSearchParams(String(token?.init?.body))),
    ).toEqual({
      grant_type: 'authorization_code',
      code: 'good-code',
      client_id: 'client-1',
      client_secret: 'secret-1',
      redirect_uri: REDIRECT_URI,
    });
    expect(jwks?.url).toBe('https://www.googleapis.com/oauth2/v3/certs');
    expect(
      calls.some(({ url }) => url.includes('openidconnect.googleapis.com')),
    ).toBe(false);
  });

  it.each([
    [
      'provider denial',
      `error=access_denied&state=${encodeURIComponent(STATE)}`,
    ],
    ['missing code', `state=${encodeURIComponent(STATE)}`],
    [
      'duplicate code parameter',
      `code=one&code=two&state=${encodeURIComponent(STATE)}`,
    ],
    ['wrong returned state', 'code=good-code&state=another-state'],
  ])('fails closed before token exchange on %s', async (_name, parameters) => {
    const fetchFn = vi.fn<typeof fetch>();

    const identity = await googleProvider({
      ...OPTIONS,
      fetch: fetchFn,
    }).exchange({
      code: 'good-code',
      redirectUri: REDIRECT_URI,
      callbackUrl: callback(parameters),
      state: STATE,
    });

    expect(identity).toBeUndefined();
    expect(fetchFn).not.toHaveBeenCalled();
  });

  it.each([
    [
      'token endpoint error',
      async () => jsonResponse({ error: 'invalid_grant' }, 400),
    ],
    [
      'missing ID Token',
      async () => jsonResponse({ access_token: 'at', token_type: 'Bearer' }),
    ],
    [
      'missing access token',
      async () =>
        jsonResponse({ token_type: 'Bearer', id_token: await idToken() }),
    ],
    [
      'invalid ID Token signature',
      async () => {
        const token = await idToken();
        const [header, claims, signature = ''] = token.split('.');
        const replacement = signature.startsWith('A') ? 'B' : 'A';
        return jsonResponse({
          access_token: 'at',
          token_type: 'Bearer',
          id_token: `${header}.${claims}.${replacement}${signature.slice(1)}`,
        });
      },
    ],
    [
      'wrong ID Token issuer',
      async () =>
        jsonResponse({
          access_token: 'at',
          token_type: 'Bearer',
          id_token: await idToken({ iss: 'https://attacker.example' }),
        }),
    ],
    [
      'wrong ID Token nonce',
      async () =>
        jsonResponse({
          access_token: 'at',
          token_type: 'Bearer',
          id_token: await idToken({ nonce: 'wrong-nonce' }),
        }),
    ],
    [
      'missing ID Token subject',
      async () =>
        jsonResponse({
          access_token: 'at',
          token_type: 'Bearer',
          id_token: await idToken({ sub: undefined }),
        }),
    ],
    [
      'empty ID Token subject',
      async () =>
        jsonResponse({
          access_token: 'at',
          token_type: 'Bearer',
          id_token: await idToken({ sub: '' }),
        }),
    ],
  ])('fails closed on %s', async (_name, tokenResponse) => {
    const fetchFn = (async (input: unknown) => {
      if (String(input).includes('oauth2.googleapis.com/token')) {
        return tokenResponse();
      }
      return jsonResponse({
        keys: [
          {
            ...publicJwk,
            alg: 'RS256',
            kid: 'google-test-key',
            use: 'sig',
          },
        ],
      });
    }) as typeof fetch;

    expect(
      await googleProvider({ ...OPTIONS, fetch: fetchFn }).exchange({
        code: 'good-code',
        redirectUri: REDIRECT_URI,
        callbackUrl: callback(
          `code=good-code&state=${encodeURIComponent(STATE)}`,
        ),
        state: STATE,
      }),
    ).toBeUndefined();
  });
});

describe('githubProvider', () => {
  const OPTIONS = { clientId: 'github-client', clientSecret: 'github-secret' };
  const REDIRECT_URI = 'https://demo.test/auth/github/callback';
  const STATE = '0123456789abcdef01.1786400000000.state-signature';

  function jsonResponse(payload: unknown, status = 200): Response {
    return new Response(JSON.stringify(payload), {
      status,
      headers: { 'content-type': 'application/json' },
    });
  }

  it('uses static server metadata for authorization and validated token exchange, then maps /user numeric id', async () => {
    const calls: Array<{ url: string; init?: RequestInit }> = [];
    const fetchFn = (async (input: unknown, init?: RequestInit) => {
      calls.push({ url: String(input), init });
      if (String(input).includes('/login/oauth/access_token')) {
        return jsonResponse({
          access_token: 'github-at',
          token_type: 'bearer',
        });
      }
      return jsonResponse({ id: 8675309, login: 'renameable' });
    }) as typeof fetch;
    const provider = githubProvider({ ...OPTIONS, fetch: fetchFn });
    const authorize = new URL(
      provider.authorizeUrl({ state: STATE, redirectUri: REDIRECT_URI }),
    );

    expect(authorize.origin + authorize.pathname).toBe(
      'https://github.com/login/oauth/authorize',
    );
    expect(Object.fromEntries(authorize.searchParams)).toEqual({
      client_id: OPTIONS.clientId,
      redirect_uri: REDIRECT_URI,
      response_type: 'code',
      state: STATE,
    });
    await expect(
      provider.exchange({
        code: 'good-code',
        redirectUri: REDIRECT_URI,
        callbackUrl: new URL(
          `${REDIRECT_URI}?code=good-code&state=${encodeURIComponent(STATE)}`,
        ),
        state: STATE,
      }),
    ).resolves.toEqual({ subject: 'github:8675309' });

    const [token, profile] = calls;
    expect(token?.url).toBe('https://github.com/login/oauth/access_token');
    expect(new Headers(token?.init?.headers).get('accept')).toBe(
      'application/json',
    );
    expect(
      Object.fromEntries(new URLSearchParams(String(token?.init?.body))),
    ).toMatchObject({
      grant_type: 'authorization_code',
      code: 'good-code',
      client_id: OPTIONS.clientId,
      client_secret: OPTIONS.clientSecret,
      redirect_uri: REDIRECT_URI,
    });
    expect(profile?.url).toBe('https://api.github.com/user');
    expect(new Headers(profile?.init?.headers).get('authorization')).toBe(
      'Bearer github-at',
    );
  });

  it.each([
    [
      'token endpoint error',
      [jsonResponse({ error: 'bad_verification_code' }, 400)],
    ],
    ['missing access token', [jsonResponse({ token_type: 'bearer' })]],
    [
      'profile endpoint error',
      [
        jsonResponse({ access_token: 'at', token_type: 'bearer' }),
        jsonResponse({}, 401),
      ],
    ],
    [
      'malformed profile',
      [
        jsonResponse({ access_token: 'at', token_type: 'bearer' }),
        jsonResponse({ id: 'not-numeric' }),
      ],
    ],
  ])('fails closed on %s', async (_name, responses) => {
    const queue = [...responses];
    const fetchFn = (async () => {
      const next = queue.shift();
      if (!next) throw new Error('unexpected extra fetch');
      return next;
    }) as unknown as typeof fetch;

    expect(
      await githubProvider({ ...OPTIONS, fetch: fetchFn }).exchange({
        code: 'good-code',
        redirectUri: REDIRECT_URI,
        callbackUrl: new URL(
          `${REDIRECT_URI}?code=good-code&state=${encodeURIComponent(STATE)}`,
        ),
        state: STATE,
      }),
    ).toBeUndefined();
  });
});

describe('createDemoAuthRouter (fake provider round-trip)', () => {
  function fakeProvider(subject = 'github:42', name = 'github'): OAuthProvider {
    return {
      name,
      authorizeUrl: ({ state, redirectUri }) =>
        `https://fake.test/authorize?state=${encodeURIComponent(state)}&redirect_uri=${encodeURIComponent(redirectUri)}`,
      exchange: async ({ code }) =>
        code === 'good-code' ? { subject } : undefined,
    };
  }

  function makeRouter(
    options: {
      disabled?: boolean;
      now?: () => number;
      providerName?: string;
      provider?: OAuthProvider;
    } = {},
  ) {
    const db = demoDb(openSqlite());
    const router = createDemoAuthRouter({
      db,
      provider:
        options.provider ?? fakeProvider(undefined, options.providerName),
      secret: SECRET,
      jwtTtlSeconds: 3600,
      sessionTtlMs: 24 * HOUR,
      disabled: options.disabled ?? false,
      now: options.now ?? (() => T0),
    });
    return { router, db };
  }

  /** The nonce cookie the provider redirect set, as a Cookie header value. */
  function stateCookieOf(response: Response | null): string {
    const setCookie = response?.headers.get('set-cookie') ?? '';
    const value = setCookie.split(';')[0] ?? '';
    return value;
  }

  async function completeSignIn(
    router: (request: Request) => Promise<Response | null>,
  ): Promise<DemoTokenSet> {
    const redirect = await router(new Request('https://demo.test/auth/github'));
    const authorizeUrl = new URL(redirect?.headers.get('location') ?? '');
    const state = authorizeUrl.searchParams.get('state') ?? '';
    const callback = await router(
      new Request(
        `https://demo.test/auth/github/callback?code=good-code&state=${encodeURIComponent(state)}`,
        { headers: { cookie: stateCookieOf(redirect) } },
      ),
    );
    expect(callback?.status).toBe(302);
    const location = callback?.headers.get('location') ?? '';
    expect(location.startsWith('/#demo-tokens=')).toBe(true);
    const encoded = location
      .slice('/#demo-tokens='.length)
      .replace(/-/g, '+')
      .replace(/_/g, '/');
    return JSON.parse(atob(encoded)) as DemoTokenSet;
  }

  it('completes the sign-in round-trip: signed state, code exchange, token set in a FRAGMENT', async () => {
    // #given / #when
    const { router } = makeRouter();
    const tokenSet = await completeSignIn(router);

    // #then — a full four-role set for a freshly minted session
    expect(tokenSet.sessionId).toMatch(/^[0-9a-f]{18}$/);
    expect(tokenSet.tokens).toHaveLength(4);
  });

  it.each([
    [
      'provider denial',
      (state: string) =>
        `error=access_denied&state=${encodeURIComponent(state)}`,
    ],
    [
      'duplicate callback code',
      (state: string) => `code=one&code=two&state=${encodeURIComponent(state)}`,
    ],
  ])('maps %s to the stable sign-in failure envelope', async (_name, query) => {
    const fetchFn = vi.fn<typeof fetch>();
    const { router } = makeRouter({
      provider: githubProvider({
        clientId: 'github-client',
        clientSecret: 'github-secret',
        fetch: fetchFn,
      }),
    });
    const redirect = await router(new Request('https://demo.test/auth/github'));
    const state =
      new URL(redirect?.headers.get('location') ?? '').searchParams.get(
        'state',
      ) ?? '';

    const response = await router(
      new Request(`https://demo.test/auth/github/callback?${query(state)}`, {
        headers: { cookie: stateCookieOf(redirect) },
      }),
    );

    expect(response?.status).toBe(401);
    expect(await response?.json()).toEqual({ error: 'sign-in failed' });
    expect(fetchFn).not.toHaveBeenCalled();
  });

  it('collapses thrown provider details without leaking credentials', async () => {
    const leakedSecret = 'must-not-reach-the-response';
    const { router } = makeRouter({
      provider: {
        name: 'github',
        authorizeUrl: ({ state }) =>
          `https://fake.test/authorize?state=${encodeURIComponent(state)}`,
        exchange: async () => {
          throw new Error(`upstream rejected ${leakedSecret}`);
        },
      },
    });
    const redirect = await router(new Request('https://demo.test/auth/github'));
    const state =
      new URL(redirect?.headers.get('location') ?? '').searchParams.get(
        'state',
      ) ?? '';

    const response = await router(
      new Request(
        `https://demo.test/auth/github/callback?code=bad&state=${encodeURIComponent(state)}`,
        { headers: { cookie: stateCookieOf(redirect) } },
      ),
    );
    const body = await response?.text();

    expect(response?.status).toBe(401);
    expect(body).toBe('{"error":"sign-in failed"}');
    expect(body).not.toContain(leakedSecret);
  });

  it('403s a forged or replay-expired state', async () => {
    // #given
    const { router } = makeRouter();

    // #when / #then
    const forged = await router(
      new Request(
        'https://demo.test/auth/github/callback?code=good-code&state=forged.123.sig',
      ),
    );
    expect(forged?.status).toBe(403);
  });

  it('403s a VALIDLY-SIGNED state presented without the matching browser cookie (login CSRF)', async () => {
    // #given — the attacker obtains a legitimately signed state from the
    // public /auth/github route, then walks a victim's browser through the
    // callback with the attacker's own code. Signature + expiry both pass;
    // only the browser binding stops the victim landing in the attacker's
    // session.
    const { router } = makeRouter();
    const attackerRedirect = await router(
      new Request('https://demo.test/auth/github'),
    );
    const attackerState =
      new URL(attackerRedirect?.headers.get('location') ?? '').searchParams.get(
        'state',
      ) ?? '';
    expect(await verifyState(SECRET, attackerState, T0)).toBe(true);

    // #when — the victim's browser has no such cookie
    const noCookie = await router(
      new Request(
        `https://demo.test/auth/github/callback?code=good-code&state=${encodeURIComponent(attackerState)}`,
      ),
    );
    // ...and a mismatched cookie is no better
    const wrongCookie = await router(
      new Request(
        `https://demo.test/auth/github/callback?code=good-code&state=${encodeURIComponent(attackerState)}`,
        { headers: { cookie: `${STATE_COOKIE}=000000000000000000` } },
      ),
    );

    // #then
    expect(noCookie?.status).toBe(403);
    expect(wrongCookie?.status).toBe(403);
  });

  it('clears the binding cookie on success — one round-trip per nonce', async () => {
    // #given / #when
    const { router } = makeRouter();
    const redirect = await router(new Request('https://demo.test/auth/github'));
    const state =
      new URL(redirect?.headers.get('location') ?? '').searchParams.get(
        'state',
      ) ?? '';
    const callback = await router(
      new Request(
        `https://demo.test/auth/github/callback?code=good-code&state=${encodeURIComponent(state)}`,
        { headers: { cookie: stateCookieOf(redirect) } },
      ),
    );

    // #then — the Set-Cookie expires it, so a replay of the same state finds
    // no cookie to match
    expect(callback?.headers.get('set-cookie')).toContain('Max-Age=0');
  });

  it('refreshes the token set while the session is live, then 401s', async () => {
    // #given — a signed-in visitor
    let nowMs = T0;
    const { router } = makeRouter({ now: () => nowMs });
    const tokenSet = await completeSignIn(router);
    const token = tokenSet.tokens[0]?.token ?? '';

    // #when — 30 minutes later (JWT still valid, silent refresh window)
    nowMs = T0 + 30 * 60 * 1000;
    const refreshed = await router(
      new Request('https://demo.test/auth/refresh', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ token }),
      }),
    );

    // #then — a fresh set for the same session
    expect(refreshed?.status).toBe(200);
    const refreshedSet = (await refreshed?.json()) as DemoTokenSet;
    expect(refreshedSet.sessionId).toBe(tokenSet.sessionId);

    // #when — after the session expired, refresh refuses
    nowMs = T0 + 25 * HOUR;
    const late = await router(
      new Request('https://demo.test/auth/refresh', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ token }),
      }),
    );
    expect(late?.status).toBe(401);
  });

  it('rejects an oversized refresh body before parsing it', async () => {
    const { router } = makeRouter();
    const response = await router(
      new Request('https://demo.test/auth/refresh', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ token: 'x'.repeat(20_000) }),
      }),
    );

    expect(response?.status).toBe(413);
    expect(await response?.json()).toEqual({ error: 'payload too large' });
  });

  it('returns null only outside the exact /auth/ family precheck', async () => {
    const { router } = makeRouter();

    await expect(
      router(new Request('https://demo.test/healthz')),
    ).resolves.toBeNull();
    await expect(
      router(new Request('https://demo.test/auth')),
    ).resolves.toBeNull();
    await expect(
      router(new Request('https://demo.test/authentication')),
    ).resolves.toBeNull();
  });

  it.each([
    ['POST', '/auth/github'],
    ['POST', '/auth/config'],
    ['GET', '/auth/refresh'],
    ['DELETE', '/auth/github/callback'],
    ['HEAD', '/auth/config'],
    ['HEAD', '/auth/github'],
    ['GET', '/auth/config/'],
    ['GET', '/auth/github/'],
  ])('keeps unmatched method/path %s %s in the stable 404 envelope', async (method, path) => {
    const { router } = makeRouter();

    const response = await router(
      new Request(`https://demo.test${path}`, { method }),
    );

    expect(response?.status).toBe(404);
    expect(await response?.json()).toEqual({ error: 'not found' });
    expect(Object.fromEntries(response?.headers ?? [])).toEqual({
      'cache-control': 'no-store',
      'content-type': 'application/json',
    });
  });

  it('maps malformed refresh JSON to the existing validation envelope', async () => {
    const { router } = makeRouter();

    const response = await router(
      new Request('https://demo.test/auth/refresh', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: '{"token":',
      }),
    );

    expect(response?.status).toBe(400);
    expect(await response?.json()).toEqual({ error: 'token is required' });
  });

  it('rejects invalid UTF-8 before refresh token validation', async () => {
    const { router } = makeRouter();
    const prefix = new TextEncoder().encode('{"token":"');
    const suffix = new TextEncoder().encode('"}');
    const body = new Uint8Array(prefix.length + 1 + suffix.length);
    body.set(prefix);
    body[prefix.length] = 0xff;
    body.set(suffix, prefix.length + 1);

    const response = await router(
      new Request('https://demo.test/auth/refresh', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body,
      }),
    );

    expect(response?.status).toBe(400);
    expect(await response?.json()).toEqual({
      error: 'a JSON object body is required',
    });
  });

  it('rejects extra refresh fields through the strict schema before token verification', async () => {
    const { router } = makeRouter();

    const response = await router(
      new Request('https://demo.test/auth/refresh', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          token: 'would-otherwise-be-verified',
          extra: true,
        }),
      }),
    );

    expect(response?.status).toBe(400);
    expect(await response?.json()).toEqual({ error: 'token is required' });
  });

  it('the kill switch 503s every auth route and reports {enabled:false} on /auth/config', async () => {
    // #given
    const { router } = makeRouter({ disabled: true });

    // #when / #then
    expect(
      (await router(new Request('https://demo.test/auth/config')))?.status,
    ).toBe(200);
    expect(
      await (
        await router(new Request('https://demo.test/auth/config'))
      )?.json(),
    ).toMatchObject({ enabled: false });
    expect(
      (await router(new Request('https://demo.test/auth/github')))?.status,
    ).toBe(503);
    expect(
      (
        await router(
          new Request(
            'https://demo.test/auth/github/callback?state=forged.123.sig',
          ),
        )
      )?.status,
    ).toBe(503);
    expect(
      (
        await router(
          new Request('https://demo.test/auth/refresh', {
            method: 'POST',
            body: 'x'.repeat(20_000),
          }),
        )
      )?.status,
    ).toBe(503);
  });

  it('echoes the mounted provider NAME on /auth/config — the SPA derives the sign-in button and href from it', async () => {
    // #given — a live (not disabled) router over the fake 'github' provider
    const { router } = makeRouter();

    // #when
    const response = await router(new Request('https://demo.test/auth/config'));

    // #then — exact echo: dropping/renaming `provider` would silently break
    // the deployed sign-in button while every other test stays green
    expect(response?.status).toBe(200);
    expect(await response?.json()).toEqual({
      enabled: true,
      provider: 'github',
    });
  });

  it("routes derive from provider.name, empirically: a 'google'-named mount serves /auth/google and not /auth/github", async () => {
    // #given — the same router machinery under the LAUNCH provider's name
    const { router } = makeRouter({ providerName: 'google' });

    // #when / #then — the config echo, the entry redirect (with its nonce
    // cookie), and the sibling path all follow the mounted name
    expect(
      await (
        await router(new Request('https://demo.test/auth/config'))
      )?.json(),
    ).toEqual({ enabled: true, provider: 'google' });
    const redirect = await router(new Request('https://demo.test/auth/google'));
    expect(redirect?.status).toBe(302);
    expect(redirect?.headers.get('set-cookie')).toContain(STATE_COOKIE);

    // #then — the OTHER provider's path is not mounted (404, not 302)
    expect(
      (await router(new Request('https://demo.test/auth/github')))?.status,
    ).toBe(404);
  });
});

describe('the kill switch in the AUTH middleware (worker.buildVerifier)', () => {
  it('an ALREADY-ISSUED demo JWT stops verifying when DEMO_DISABLED flips — issued tokens must not sail past the switch', async () => {
    // #given — a valid demo JWT and a static operator token
    const db = demoDb(openSqlite());
    const session = await findOrCreateDemoSession(db, makeSessionOptions());
    const set = await mintDemoTokenSet({
      secret: SECRET,
      session,
      ttlSeconds: 3600,
    });
    const demoJwt = set.tokens[0]?.token ?? '';
    const staticTokens = JSON.stringify({
      'op-token': { id: 'op', role: 'operator' },
    });
    const envBase = {
      DEMO_JWT_SECRET: SECRET,
      APPROVAL_ACTOR_TOKENS: staticTokens,
    };

    // #when — switch off, then on
    const open = buildVerifier(envBase as never);
    const killed = buildVerifier({
      ...envBase,
      DEMO_DISABLED: 'true',
    } as never);

    // #then — the same JWT dies with the switch; operators keep working
    expect(await open.verify(demoJwt)).toMatchObject({ role: 'admin' });
    expect(await killed.verify(demoJwt)).toBeUndefined();
    expect(await killed.verify('op-token')).toMatchObject({ id: 'op' });
  });

  it.each([
    '1',
    'yes',
    'on',
    'TRUE',
    'disable-now-please',
  ])("DEMO_DISABLED='%s' also kills issued JWTs — the emergency switch parses fail-closed, never as a silent no-op", async (raw) => {
    // #given — a valid demo JWT
    const db = demoDb(openSqlite());
    const session = await findOrCreateDemoSession(db, makeSessionOptions());
    const set = await mintDemoTokenSet({
      secret: SECRET,
      session,
      ttlSeconds: 3600,
    });
    const demoJwt = set.tokens[0]?.token ?? '';

    // #when — an operator mid-incident flips the switch with a
    // non-canonical spelling (or a typo)
    const killed = buildVerifier({
      DEMO_JWT_SECRET: SECRET,
      DEMO_DISABLED: raw,
    } as never);

    // #then — the demo verifier is unmounted regardless of spelling
    expect(await killed.verify(demoJwt)).toBeUndefined();
  });
});

describe('provider selection (worker.selectOAuthProvider)', () => {
  const GOOGLE_PAIR = {
    GOOGLE_CLIENT_ID: 'g-id',
    GOOGLE_CLIENT_SECRET: 'g-secret',
  };
  const GITHUB_PAIR = {
    GITHUB_CLIENT_ID: 'h-id',
    GITHUB_CLIENT_SECRET: 'h-secret',
  };

  it.each([
    ['Google pair only', GOOGLE_PAIR, 'google'],
    ['GitHub pair only', GITHUB_PAIR, 'github'],
    [
      'both pairs configured — Google wins',
      { ...GOOGLE_PAIR, ...GITHUB_PAIR },
      'google',
    ],
    ['neither — the demo stays unmounted', {}, undefined],
    [
      'Google id without its secret — a sign-in that dies at the token exchange must not mount',
      { GOOGLE_CLIENT_ID: 'g-id' },
      undefined,
    ],
    [
      'Google secret without its id — same class, same refusal',
      { GOOGLE_CLIENT_SECRET: 'g-secret' },
      undefined,
    ],
    ['GitHub id without its secret', { GITHUB_CLIENT_ID: 'h-id' }, undefined],
    [
      'GitHub secret without its id',
      { GITHUB_CLIENT_SECRET: 'h-secret' },
      undefined,
    ],
    [
      'a half-set Google pair must not mask a fully-configured GitHub fallback',
      { GOOGLE_CLIENT_ID: 'g-id', ...GITHUB_PAIR },
      'github',
    ],
  ])('%s', (_name, envPart, expected) => {
    // #given / #when — the tripwire logs are asserted separately below
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    const provider = selectOAuthProvider(envPart as never);

    // #then
    expect(provider?.name).toBe(expected);
    warnSpy.mockRestore();
    errorSpy.mockRestore();
  });

  it('logs a config-warning only when both FULL pairs are set (stale-credential tripwire)', async () => {
    // #given
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});

    // #when — a full Google pair alone, then alongside a full GitHub pair
    selectOAuthProvider(GOOGLE_PAIR as never);
    expect(warnSpy).not.toHaveBeenCalled();
    selectOAuthProvider({ ...GOOGLE_PAIR, ...GITHUB_PAIR } as never);

    // #then
    expect(warnSpy).toHaveBeenCalledTimes(1);
    expect(String(warnSpy.mock.calls[0]?.[0])).toContain('config-warning');
    warnSpy.mockRestore();
  });

  it('logs a config-error NAMING THE MISSING VAR when a pair is half-set', async () => {
    // #given
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

    // #when — id without secret, then secret without id
    selectOAuthProvider({ GOOGLE_CLIENT_ID: 'g-id' } as never);
    selectOAuthProvider({ GITHUB_CLIENT_SECRET: 'h-secret' } as never);

    // #then — each line names the var the operator must add
    const lines = errorSpy.mock.calls.map(([line]) => String(line));
    expect(lines).toHaveLength(2);
    expect(lines[0]).toContain('config-error');
    expect(lines[0]).toContain('"var":"GOOGLE_CLIENT_SECRET"');
    expect(lines[1]).toContain('"var":"GITHUB_CLIENT_ID"');
    errorSpy.mockRestore();
  });

  it('a half-set GitHub pair beside a full Google pair gets the config-error, NOT the both-pairs warning', async () => {
    // #given — the stale-credential warning keys on the FULL fallback pair;
    // gating it on "any GitHub var present" would double-log this combination
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

    // #when
    const provider = selectOAuthProvider({
      ...GOOGLE_PAIR,
      GITHUB_CLIENT_ID: 'h-id',
    } as never);

    // #then — Google mounts; one config-error names the missing secret
    expect(provider?.name).toBe('google');
    expect(warnSpy).not.toHaveBeenCalled();
    expect(errorSpy).toHaveBeenCalledTimes(1);
    expect(String(errorSpy.mock.calls[0]?.[0])).toContain(
      '"var":"GITHUB_CLIENT_SECRET"',
    );
    warnSpy.mockRestore();
    errorSpy.mockRestore();
  });
});

describe('session cleanup grace window', () => {
  it('retains metadata while a final refreshed token can still be live', async () => {
    // #given — a session that expired 10 minutes ago. A refresh at the last
    // live instant can leave a token valid for another jwtTtl.
    const db = demoDb(openSqlite());
    await findOrCreateDemoSession(db, {
      ...makeSessionOptions(),
      now: () => T0 - 24 * HOUR - 10 * 60 * 1000,
    });
    const jwtTtlMs = 3600 * 1000;

    // #when — the purge duty runs 10 min after expiry with a 1h grace
    const early = await deleteExpiredDemoSessions(db, {
      graceMs: jwtTtlMs,
      now: () => T0,
    });

    // #then — spared: a token minted at expires_at - ε is still valid
    expect(early).toEqual([]);

    // #when — an hour later every possible token has expired
    const late = await deleteExpiredDemoSessions(db, {
      graceMs: jwtTtlMs,
      now: () => T0 + jwtTtlMs,
    });

    // #then — now it reaps
    expect(late).toHaveLength(1);
  });
});

describe('findOrCreateDemoSession concurrency', () => {
  it('two concurrent first sign-ins resolve to one session', async () => {
    // #given — the SELECT..INSERT is not transactional, so both callers take
    // the "absent" branch and race the UNIQUE(provider, subject) constraint
    const db = demoDb(openSqlite());

    // #when
    const [a, b] = await Promise.all([
      findOrCreateDemoSession(db, makeSessionOptions()),
      findOrCreateDemoSession(db, makeSessionOptions()),
    ]);

    // #then — the loser reads back the winner's row instead of throwing
    expect(a.session_id).toBe(b.session_id);
    const rows = (await db
      .prepare('SELECT session_id FROM demo_sessions')
      .all()) as { results: Array<{ session_id: string }> };
    expect(rows.results).toHaveLength(1);
  });
});
