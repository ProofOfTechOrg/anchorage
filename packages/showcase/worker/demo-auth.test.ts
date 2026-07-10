// The public demo's identity + abuse machinery, against real SQLite: tenant
// lifecycle (mint / reuse / expire-and-replace with inline purge), the two
// ATOMIC run budgets, the LIMIT-batched tenant reaper, the signed OAuth
// state, the token set, the auth router's full round-trip with a fake
// provider, and the kill switch — including its auth-middleware half (an
// ALREADY-ISSUED JWT must die with the switch, not just new mints).

import { openSqlite, type SqliteDatabase } from '@flowsafe-test/sqlite.js';
import { hmacVerifier, provisionTenant } from '@proofoftech/flowsafe/host-kit';
import { describe, expect, it, vi } from 'vitest';
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
  findOrCreateDemoTenant,
  googleProvider,
  isDemoTenant,
  mintDemoTenantId,
  mintDemoTokenSet,
  nonceOfState,
  type OAuthProvider,
  purgeExpiredDemoTenants,
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

function makeTenantOptions(purged: string[] = []) {
  return {
    provider: 'github',
    subject: 'github:1234',
    tenantTtlMs: 24 * HOUR,
    now: () => T0,
    purgeTenantData: async (tenantId: string) => {
      purged.push(tenantId);
    },
  };
}

describe('mintDemoTenantId', () => {
  it('always mints an INV-3-valid, dm-prefixed slug', () => {
    for (let index = 0; index < 20; index += 1) {
      const id = mintDemoTenantId();
      expect(id).toMatch(/^dm[0-9a-f]{18}$/);
    }
  });
});

describe('findOrCreateDemoTenant', () => {
  it('mints a fresh tenant through the allocation registry (insert-or-fail)', async () => {
    // #given
    const sqlite = openSqlite();
    const db = demoDb(sqlite);

    // #when
    const tenant = await findOrCreateDemoTenant(db, makeTenantOptions());

    // #then — provisioned in tenants (kind demo) AND registered in demo_tenants
    expect(tenant.tenant_id).toMatch(/^dm/);
    expect(tenant.run_count).toBe(0);
    const registry = sqlite
      .prepare('SELECT tenant_id, kind FROM tenants')
      .all();
    expect(registry).toEqual([{ tenant_id: tenant.tenant_id, kind: 'demo' }]);
  });

  it('reuses the LIVE tenant for the same identity without extending its clock', async () => {
    // #given
    const db = demoDb(openSqlite());
    const first = await findOrCreateDemoTenant(db, makeTenantOptions());

    // #when — same subject signs in 1h later
    const second = await findOrCreateDemoTenant(db, {
      ...makeTenantOptions(),
      now: () => T0 + HOUR,
    });

    // #then — same tenant, same expiry (lifetime runs from FIRST sign-in)
    expect(second.tenant_id).toBe(first.tenant_id);
    expect(second.expires_at).toBe(first.expires_at);
  });

  it('replaces an EXPIRED tenant with a fresh one, purging the old data inline', async () => {
    // #given — a tenant past its 24h lifetime
    const purged: string[] = [];
    const db = demoDb(openSqlite());
    const first = await findOrCreateDemoTenant(db, makeTenantOptions(purged));

    // #when — the same identity returns after expiry
    const second = await findOrCreateDemoTenant(db, {
      ...makeTenantOptions(purged),
      now: () => T0 + 25 * HOUR,
    });

    // #then — fresh sandbox; the old tenant's data was reaped BEFORE its row
    // stopped referencing it (otherwise the purge cron could never find it)
    expect(second.tenant_id).not.toBe(first.tenant_id);
    expect(purged).toEqual([first.tenant_id]);
  });
});

describe('consumeRunBudget (atomic caps)', () => {
  async function seededDb(): Promise<{
    db: DemoDatabase;
    tenantId: string;
  }> {
    const db = demoDb(openSqlite());
    const tenant = await findOrCreateDemoTenant(db, makeTenantOptions());
    return { db, tenantId: tenant.tenant_id };
  }

  it('enforces the per-tenant cap as ONE conditional UPDATE (no TOCTOU window)', async () => {
    // #given — cap 2
    const { db, tenantId } = await seededDb();
    const options = {
      tenantId,
      tenantRunCap: 2,
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
      scope: 'tenant',
      reason: 'cap-reached',
    });
    const row = (await db
      .prepare('SELECT run_count FROM demo_tenants WHERE tenant_id = ?')
      .bind(tenantId)
      .first()) as { run_count: number };
    expect(row.run_count).toBe(2);
  });

  it('cap 0 refuses the FIRST run — the incident freeze the worker passes via allowZero', async () => {
    // #given — a live, provisioned tenant and DEMO_TENANT_RUN_CAP=0 (the
    // operator's freeze; numberVar's allowZero keeps it from reverting to the
    // fallback). run_count < 0 is never satisfiable, so the very first
    // conditional UPDATE refuses.
    const { db, tenantId } = await seededDb();

    // #when / #then
    await expect(
      consumeRunBudget(db, {
        tenantId,
        tenantRunCap: 0,
        dailyRunCap: 100,
        now: () => T0 + 1000,
      }),
    ).rejects.toMatchObject({ scope: 'tenant', reason: 'cap-reached' });
  });

  it('daily cap 0 freezes the whole demo even when tenant budgets remain', async () => {
    // #given
    const { db, tenantId } = await seededDb();

    // #when / #then
    await expect(
      consumeRunBudget(db, {
        tenantId,
        tenantRunCap: 10,
        dailyRunCap: 0,
        now: () => T0 + 1000,
      }),
    ).rejects.toMatchObject({ scope: 'global', reason: 'cap-reached' });
  });

  it('refuses runs for an EXPIRED tenant even under its cap', async () => {
    // #given
    const { db, tenantId } = await seededDb();

    // #when / #then — 25h later the sandbox is dead
    await expect(
      consumeRunBudget(db, {
        tenantId,
        tenantRunCap: 10,
        dailyRunCap: 100,
        now: () => T0 + 25 * HOUR,
      }),
    ).rejects.toMatchObject({ scope: 'tenant', reason: 'expired' });
  });

  it('refuses runs for an unknown tenant (a dm-prefixed forgery)', async () => {
    // #given
    const { db } = await seededDb();

    // #when / #then
    await expect(
      consumeRunBudget(db, {
        tenantId: 'dm000000000000000000',
        tenantRunCap: 10,
        dailyRunCap: 100,
        now: () => T0 + 1000,
      }),
    ).rejects.toMatchObject({ scope: 'tenant', reason: 'not-provisioned' });
  });

  it('the GLOBAL daily ceiling stops runs across tenants — the spend backstop', async () => {
    // #given — two tenants, daily cap 3
    const sqlite = openSqlite();
    const db = demoDb(sqlite);
    const a = await findOrCreateDemoTenant(db, makeTenantOptions());
    const b = await findOrCreateDemoTenant(db, {
      ...makeTenantOptions(),
      subject: 'github:5678',
    });
    const budget = (tenantId: string) => ({
      tenantId,
      tenantRunCap: 100,
      dailyRunCap: 3,
      now: () => T0 + 1000,
    });

    // #when — three runs land (2 from A, 1 from B)
    await consumeRunBudget(db, budget(a.tenant_id));
    await consumeRunBudget(db, budget(a.tenant_id));
    await consumeRunBudget(db, budget(b.tenant_id));

    // #then — the fourth refuses GLOBALLY, whoever asks
    await expect(
      consumeRunBudget(db, budget(b.tenant_id)),
    ).rejects.toMatchObject({ scope: 'global', reason: 'cap-reached' });

    // ...and the ceiling resets on the next UTC day (T0 is 12:00Z, so +13h
    // crosses midnight while the tenant is still live)
    await consumeRunBudget(db, {
      ...budget(a.tenant_id),
      now: () => T0 + 13 * HOUR,
    });
  });
});

describe('purgeExpiredDemoTenants', () => {
  it('reaps at most `limit` expired tenants per call, oldest first; the table is the cursor', async () => {
    // #given — three expired tenants, one live
    const db = demoDb(openSqlite());
    const expired: string[] = [];
    for (const [index, subject] of ['s1', 's2', 's3'].entries()) {
      const tenant = await findOrCreateDemoTenant(db, {
        ...makeTenantOptions(),
        subject,
        // Stagger expiries so "oldest first" is observable.
        now: () => T0 - (10 - index) * HOUR - 25 * HOUR,
      });
      expired.push(tenant.tenant_id);
    }
    const live = await findOrCreateDemoTenant(db, {
      ...makeTenantOptions(),
      subject: 'live',
    });
    const purged: string[] = [];
    const purgeTenantData = async (tenantId: string) => {
      purged.push(tenantId);
    };

    // #when — limit 2, then the next invocation
    const first = await purgeExpiredDemoTenants(db, {
      purgeTenantData,
      graceMs: 0,
      limit: 2,
      now: () => T0,
    });
    const second = await purgeExpiredDemoTenants(db, {
      purgeTenantData,
      graceMs: 0,
      limit: 2,
      now: () => T0,
    });

    // #then — batches advance without a cursor row; the live tenant survives
    expect(first).toEqual([expired[0], expired[1]]);
    expect(second).toEqual([expired[2]]);
    expect(purged).toEqual(expired);
    const rows = (await db
      .prepare('SELECT tenant_id FROM demo_tenants')
      .all()) as { results: Array<{ tenant_id: string }> };
    expect(rows.results).toEqual([{ tenant_id: live.tenant_id }]);
  });

  it("one tenant's failing purge does not head-of-line block the rest — its row survives as its own retry cursor", async () => {
    // #given — three expired tenants; the OLDEST one's purge is wedged.
    // Oldest-first ordering retries it first EVERY pass, so aborting on it
    // would starve the other two (and every later expiry) forever.
    const db = demoDb(openSqlite());
    const expired: string[] = [];
    for (const [index, subject] of ['s1', 's2', 's3'].entries()) {
      const tenant = await findOrCreateDemoTenant(db, {
        ...makeTenantOptions(),
        subject,
        now: () => T0 - (10 - index) * HOUR - 25 * HOUR,
      });
      expired.push(tenant.tenant_id);
    }
    const wedged = expired[0] ?? '';
    const purgeTenantData = async (tenantId: string) => {
      if (tenantId === wedged) throw new Error('snapshot store unavailable');
    };

    // #when / #then — the pass reaps the other two, then reports the
    // failure (naming the tenant) so the cron's error surface still fires
    await expect(
      purgeExpiredDemoTenants(db, {
        purgeTenantData,
        graceMs: 0,
        now: () => T0,
      }),
    ).rejects.toThrow(wedged);
    const rows = (await db
      .prepare('SELECT tenant_id FROM demo_tenants')
      .all()) as { results: Array<{ tenant_id: string }> };
    expect(rows.results).toEqual([{ tenant_id: wedged }]);
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
    const tenant = await findOrCreateDemoTenant(db, makeTenantOptions());

    // #when
    const set = await mintDemoTokenSet({
      secret: SECRET,
      tenant,
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
        tenantId: tenant.tenant_id,
      });
    }
  });
});

describe('googleProvider', () => {
  const OPTIONS = { clientId: 'client-1', clientSecret: 'secret-1' };
  const REDIRECT_URI = 'https://demo.test/auth/google/callback';

  function jsonResponse(payload: unknown, status = 200): Response {
    return new Response(JSON.stringify(payload), { status });
  }

  it('builds a code-flow authorize URL with the minimal openid scope', () => {
    // #given / #when
    const url = new URL(
      googleProvider(OPTIONS).authorizeUrl({
        state: 'the-state',
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
      state: 'the-state',
    });
  });

  it('exchanges the code FORM-ENCODED, reads the subject from userinfo, and scopes it google:', async () => {
    // #given — Google's token endpoint rejects JSON bodies; pin the encoding
    const calls: Array<{ url: string; init?: RequestInit }> = [];
    const fetchFn = (async (input: unknown, init?: RequestInit) => {
      calls.push({ url: String(input), init });
      if (String(input).includes('oauth2.googleapis.com/token')) {
        return jsonResponse({ access_token: 'at-123' });
      }
      return jsonResponse({ sub: '1093874' });
    }) as typeof fetch;

    // #when
    const identity = await googleProvider({
      ...OPTIONS,
      fetch: fetchFn,
    }).exchange({ code: 'good-code', redirectUri: REDIRECT_URI });

    // #then — provider-scoped stable subject
    expect(identity).toEqual({ subject: 'google:1093874' });
    const [token, userinfo] = calls;
    expect(token?.url).toBe('https://oauth2.googleapis.com/token');
    expect(new Headers(token?.init?.headers).get('content-type')).toBe(
      'application/x-www-form-urlencoded',
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
    expect(userinfo?.url).toBe(
      'https://openidconnect.googleapis.com/v1/userinfo',
    );
    expect(new Headers(userinfo?.init?.headers).get('authorization')).toBe(
      'Bearer at-123',
    );
  });

  it.each([
    ['token endpoint error', [jsonResponse({}, 400)]],
    ['missing access_token', [jsonResponse({})]],
    [
      'userinfo error',
      [jsonResponse({ access_token: 'at' }), jsonResponse({}, 401)],
    ],
    [
      'missing sub',
      [jsonResponse({ access_token: 'at' }), jsonResponse({ name: 'x' })],
    ],
    [
      'empty sub',
      [jsonResponse({ access_token: 'at' }), jsonResponse({ sub: '' })],
    ],
  ])('fails closed (undefined identity) on %s', async (_name, responses) => {
    // #given — each step that goes wrong must yield "sign-in failed", never
    // a half-identity
    const queue = [...responses];
    const fetchFn = (async () => {
      const next = queue.shift();
      if (!next) throw new Error('unexpected extra fetch');
      return next;
    }) as unknown as typeof fetch;

    // #when / #then
    expect(
      await googleProvider({ ...OPTIONS, fetch: fetchFn }).exchange({
        code: 'good-code',
        redirectUri: REDIRECT_URI,
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
    } = {},
  ) {
    const db = demoDb(openSqlite());
    const purged: string[] = [];
    const router = createDemoAuthRouter({
      db,
      provider: fakeProvider(undefined, options.providerName),
      secret: SECRET,
      jwtTtlSeconds: 3600,
      tenantTtlMs: 24 * HOUR,
      purgeTenantData: async (tenantId) => {
        purged.push(tenantId);
      },
      disabled: options.disabled ?? false,
      now: options.now ?? (() => T0),
    });
    return { router, db, purged };
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

    // #then — a full four-role set for a freshly-minted dm tenant
    expect(tokenSet.tenantId).toMatch(/^dm/);
    expect(tokenSet.tokens).toHaveLength(4);
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
    // sandbox.
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
        { headers: { cookie: `${STATE_COOKIE}=dm000000000000000000` } },
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

  it('refreshes the token set while the tenant is live, 401s once it expired', async () => {
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

    // #then — a fresh set for the SAME tenant
    expect(refreshed?.status).toBe(200);
    const refreshedSet = (await refreshed?.json()) as DemoTokenSet;
    expect(refreshedSet.tenantId).toBe(tokenSet.tenantId);

    // #when — after the TENANT expired, refresh refuses (re-auth mints fresh)
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
    const tenant = await findOrCreateDemoTenant(db, makeTenantOptions());
    const set = await mintDemoTokenSet({
      secret: SECRET,
      tenant,
      ttlSeconds: 3600,
    });
    const demoJwt = set.tokens[0]?.token ?? '';
    const staticTokens = JSON.stringify({
      'op-token': { id: 'op', role: 'operator', tenantId: 'opsteam' },
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
    expect(await open.verify(demoJwt)).toMatchObject({
      tenantId: tenant.tenant_id,
    });
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
    const tenant = await findOrCreateDemoTenant(db, makeTenantOptions());
    const set = await mintDemoTokenSet({
      secret: SECRET,
      tenant,
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

describe('purge grace window (the purgeTenant safety precondition)', () => {
  it('does NOT reap a tenant whose refreshed token can still be live', async () => {
    // #given — a tenant that expired 10 minutes ago. A refresh at the last
    // live instant mints a token good for another `jwtTtl` (refresh
    // deliberately does not extend the sandbox), so purging now would delete
    // suspended runs out from under a genuinely valid token — the exact race
    // purgeTenant's "no live caller by construction" argument assumes away.
    const db = demoDb(openSqlite());
    await findOrCreateDemoTenant(db, {
      ...makeTenantOptions(),
      now: () => T0 - 24 * HOUR - 10 * 60 * 1000,
    });
    const purged: string[] = [];
    const purgeTenantData = async (tenantId: string) => {
      purged.push(tenantId);
    };
    const jwtTtlMs = 3600 * 1000;

    // #when — the cron runs 10 min after expiry with a 1h grace
    const early = await purgeExpiredDemoTenants(db, {
      purgeTenantData,
      graceMs: jwtTtlMs,
      now: () => T0,
    });

    // #then — spared: a token minted at expires_at - ε is still valid
    expect(early).toEqual([]);
    expect(purged).toEqual([]);

    // #when — an hour later every possible token has expired
    const late = await purgeExpiredDemoTenants(db, {
      purgeTenantData,
      graceMs: jwtTtlMs,
      now: () => T0 + jwtTtlMs,
    });

    // #then — now it reaps
    expect(late).toHaveLength(1);
    expect(purged).toEqual(late);
  });
});

describe('isDemoTenant — the authoritative demo discriminator', () => {
  it("reads tenants.kind, so a COMMERCIAL slug starting with 'dm' is not demo traffic", async () => {
    // #given — the prefix heuristic this replaces would have charged 'dmart'
    // against a demo_tenants row that does not exist -> 429 on every start
    const db = demoDb(openSqlite());
    const sandbox = await findOrCreateDemoTenant(db, makeTenantOptions());
    await provisionTenant(db, { tenantId: 'dmart', kind: 'commercial' });

    // #when / #then
    expect(await isDemoTenant(db, sandbox.tenant_id)).toBe(true);
    expect(await isDemoTenant(db, 'dmart')).toBe(false);
    expect(sandbox.tenant_id.startsWith('dm')).toBe(true); // both look alike
  });

  it('treats an unprovisioned tenant (an operator from the static token map) as non-demo', async () => {
    // #given — static-map identities are never provisioned
    const db = demoDb(openSqlite());
    await findOrCreateDemoTenant(db, makeTenantOptions());

    // #when / #then
    expect(await isDemoTenant(db, 'opsteam')).toBe(false);
  });

  it('reports non-demo when the registry table does not exist at all', async () => {
    // #given — a host without the tenants registry (no demo configured)
    const db = demoDb(openSqlite());

    // #when / #then — never throws on the run-start hot path
    expect(await isDemoTenant(db, 'anything')).toBe(false);
  });

  it('RETHROWS a transient read error rather than reporting non-demo (a blanket catch fails OPEN on the spend cap)', async () => {
    // #given — the registry exists but the read fails transiently. Swallowing
    // it would make a real demo tenant look commercial, skipping
    // consumeRunBudget entirely and bypassing BOTH the per-tenant cap and the
    // global daily ceiling.
    const failing: DemoDatabase = {
      prepare: () => ({
        bind: () => failing.prepare(''),
        run: async () => ({}),
        first: async () => {
          throw new Error('D1_ERROR: network');
        },
        all: async () => ({ results: [] }),
      }),
    };

    // #when / #then — propagates: the run start fails closed
    await expect(isDemoTenant(failing, 'dm00112233')).rejects.toThrow(
      /D1_ERROR/,
    );
  });
});

describe('findOrCreateDemoTenant concurrency', () => {
  it('two concurrent FIRST sign-ins for one identity resolve to ONE tenant, no 500', async () => {
    // #given — the SELECT..INSERT is not transactional, so both callers take
    // the "absent" branch and race the UNIQUE(provider, subject) constraint
    const db = demoDb(openSqlite());

    // #when
    const [a, b] = await Promise.all([
      findOrCreateDemoTenant(db, makeTenantOptions()),
      findOrCreateDemoTenant(db, makeTenantOptions()),
    ]);

    // #then — the loser reads back the winner's row instead of throwing
    expect(a.tenant_id).toBe(b.tenant_id);
    const rows = (await db
      .prepare('SELECT tenant_id FROM demo_tenants')
      .all()) as { results: Array<{ tenant_id: string }> };
    expect(rows.results).toHaveLength(1);
  });
});
