// Drives the showcase worker's EXPORTED fetch() in-process — the route
// composition itself (healthz, the demo-auth mount, the auth fall-through to
// the run/approval surface), which the other showcase tests bypass by calling
// the routers directly. The prepared-statement SQLite adapter is unit-only;
// D1/workerd and Durable Object fidelity live in worker.harness.test.ts.

import { D1ApprovalStoreFactory } from '@proofoftech/flowsafe/approval-api';
import { describe, expect, it, vi } from 'vitest';
import { STATE_COOKIE } from '#worker/demo-auth';
import handler, { ShowcaseRunner } from '#worker/worker';

interface SqliteStatement {
  get(...params: unknown[]): unknown;
  run(...params: unknown[]): unknown;
  all(...params: unknown[]): unknown[];
}

interface SqliteDatabase {
  prepare(sql: string): SqliteStatement;
  exec(sql: string): void;
}

function openSqlite(): SqliteDatabase {
  const getBuiltin = (
    globalThis as {
      process?: { getBuiltinModule?: (id: string) => unknown };
    }
  ).process?.getBuiltinModule;
  if (!getBuiltin) {
    throw new Error('node:sqlite unavailable — tests require node >= 22.13');
  }
  const mod = getBuiltin('node:sqlite') as {
    DatabaseSync: new (path: string) => SqliteDatabase;
  };
  return new mod.DatabaseSync(':memory:');
}

function sqliteUnitDatabase(db: SqliteDatabase): unknown {
  const runSync = Symbol('runSync');

  function statement(sql: string, params: unknown[]): Record<string, unknown> {
    const execute = () => {
      const outcome = db.prepare(sql).run(...params) as {
        changes?: number | bigint;
      };
      return {
        success: true,
        meta: { changes: Number(outcome?.changes ?? 0) },
      };
    };
    return {
      bind: (...values: unknown[]) => statement(sql, values),
      first: async (column?: string) => {
        const row = db.prepare(sql).get(...params) as
          | Record<string, unknown>
          | undefined;
        if (row === undefined) return null;
        return column !== undefined ? (row[column] ?? null) : row;
      },
      run: async () => execute(),
      [runSync]: execute,
      all: async () => ({
        success: true,
        results: db.prepare(sql).all(...params),
        meta: {},
      }),
    };
  }
  return {
    prepare: (sql: string) => statement(sql, []),
    batch: async (
      statements: Array<{
        run: () => Promise<unknown>;
        [runSync]?: () => unknown;
      }>,
    ) => {
      db.exec('BEGIN IMMEDIATE');
      try {
        const results = [];
        for (const prepared of statements) {
          results.push(
            prepared[runSync] ? prepared[runSync]() : await prepared.run(),
          );
        }
        db.exec('COMMIT');
        return results;
      } catch (error) {
        db.exec('ROLLBACK');
        throw error;
      }
    },
  };
}

function required<T>(value: T | undefined): T {
  if (value === undefined) throw new Error('handler method missing');
  return value;
}

const fetchHandler = required(handler.fetch);

type Env = Parameters<typeof fetchHandler>[1];

class TestShowcaseRunner extends ShowcaseRunner {
  lifecycle(env: Env) {
    return this.runLifecycle(env);
  }
}

const TOKENS = {
  'tok-reviewer': { id: 'rev-ray', role: 'reviewer' },
};

const GOOGLE_PAIR = {
  GOOGLE_CLIENT_ID: 'g-id',
  GOOGLE_CLIENT_SECRET: 'g-secret',
};
const GITHUB_PAIR = {
  GITHUB_CLIENT_ID: 'h-id',
  GITHUB_CLIENT_SECRET: 'h-secret',
};

function makeEnv(overrides: Partial<Env> = {}): Env {
  const sqlite = openSqlite();
  seedDeployment(sqlite);
  return {
    DB: sqliteUnitDatabase(sqlite),
    DEPLOYMENT_TENANT: 'showcase',
    DEPLOYMENT_IDENTITY_SECRET: 'test-deployment-identity-secret-0001',
    // None of these routes reaches a run leg; a request that does is a bug.
    RUNNER: {
      idFromName: (name: string) => ({ name }),
      get: () => {
        throw new Error('DO namespace must not be reached by these routes');
      },
    },
    APPROVAL_ACTOR_TOKENS: JSON.stringify(TOKENS),
    ...overrides,
  } as Env;
}

function seedDeployment(db: SqliteDatabase): void {
  db.exec(`
    CREATE TABLE flowsafe_deployment (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      tenant_tag TEXT NOT NULL,
      provisioned_at TEXT NOT NULL
    );
    INSERT INTO flowsafe_deployment (id, tenant_tag, provisioned_at)
    VALUES (1, 'showcase', '2026-08-10T00:00:00.000Z');
  `);
}

async function call(
  env: Env,
  path: string,
  init: RequestInit & { token?: string } = {},
): Promise<Response> {
  const { token, ...requestInit } = init;
  const headers = new Headers(requestInit.headers);
  if (token) headers.set('authorization', `Bearer ${token}`);
  const pending: Promise<unknown>[] = [];
  const ctx = {
    waitUntil: (promise: Promise<unknown>) => {
      pending.push(promise);
    },
    passThroughOnException: () => {},
  } as unknown as Parameters<typeof fetchHandler>[2];
  const response = await fetchHandler(
    new Request(`https://showcase.example${path}`, {
      ...requestInit,
      headers,
    }) as unknown as Parameters<typeof fetchHandler>[0],
    env,
    ctx,
  );
  await Promise.all(pending);
  return response as unknown as Response;
}

describe('showcase worker fetch(): auth composition', () => {
  it('abandons run approvals once and accepts a cleanup replay', async () => {
    const sqlite = openSqlite();
    seedDeployment(sqlite);
    const db = sqliteUnitDatabase(sqlite);
    const approvals = new D1ApprovalStoreFactory(
      db as ConstructorParameters<typeof D1ApprovalStoreFactory>[0],
    );
    const at = new Date(1_751_000_000_000).toISOString();
    await approvals.store().create({
      id: 'apr-terminate-1',
      workflowId: 'gtm-outbound',
      runId: 'run-terminate-1',
      title: 'terminate approval',
      connectors: [],
      priority: 'normal',
      status: 'pending',
      requestedBy: 'demo-operator',
      createdAt: at,
      updatedAt: at,
    });
    const env = makeEnv({ DB: db } as Partial<Env>);
    const hooks = new TestShowcaseRunner(undefined, env).lifecycle(env);

    await hooks.abandonApprovals(
      'gtm-outbound',
      'run-terminate-1',
      'cancelled',
    );
    const abandoned = await approvals.store().get('apr-terminate-1');
    expect(abandoned).toMatchObject({
      status: 'rejected',
      decision: 'reject',
      decidedBy: 'flowsafe-worker',
      comment: 'abandoned: run cancelled',
    });

    await expect(
      hooks.abandonApprovals('gtm-outbound', 'run-terminate-1', 'cancelled'),
    ).resolves.toBeUndefined();
    expect(await approvals.store().get('apr-terminate-1')).toEqual(abandoned);
  });

  it('serves /healthz unauthenticated and 401s the authenticated surfaces without a token', async () => {
    // #given
    const env = makeEnv();

    // #when / #then — liveness is open
    const health = await call(env, '/healthz');
    expect(health.status).toBe(200);
    expect(await health.json()).toEqual({ ok: true });

    // #then — catalog and approvals fail closed
    expect((await call(env, '/workflows')).status).toBe(401);
    expect((await call(env, '/api/approvals')).status).toBe(401);
  });

  it('echoes the authenticated identity and all five module metas on the catalog', async () => {
    // #given
    const env = makeEnv();

    // #when
    const response = await call(env, '/workflows', { token: 'tok-reviewer' });

    // #then — the server's view of the actor, over the REAL showcase metas
    expect(response.status).toBe(200);
    const body = (await response.json()) as {
      actor: unknown;
      workflows: Array<{ id: string }>;
    };
    // canSelfDecide is false here: the test env sets no
    // APPROVAL_ALLOW_SELF_DECISION, so SoD is on and reviewer is not exempt.
    expect(body.actor).toEqual({
      id: 'rev-ray',
      role: 'reviewer',
      canSelfDecide: false,
    });
    expect(body.workflows.map((entry) => entry.id)).toEqual([
      'gtm-outbound',
      'content-pipeline',
      'lead-generation',
      'product-launch',
      'access-request',
      'wire-transfer',
    ]);
  });
});

describe('showcase worker fetch(): the demo-auth mount', () => {
  it('demo OFF: a half-set OAuth pair is inert — no /auth/* mount, no config-error per request', async () => {
    // #given — the committed GOOGLE_CLIENT_ID with no secrets provisioned yet
    // (the partial-provisioning window every fresh deploy passes through)
    const env = makeEnv({ GOOGLE_CLIENT_ID: 'g-id' });
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

    // #when
    const response = await call(env, '/auth/config');

    // #then — unmounted AND silent: selection never runs with the demo off
    expect(response.status).toBe(404);
    expect(errorSpy).not.toHaveBeenCalled();
    errorSpy.mockRestore();
  });

  it('demo ON: a half-set pair stays unmounted and the config-error tripwire fires', async () => {
    // #given
    const env = makeEnv({
      DEMO_JWT_SECRET: 'demo-secret',
      GOOGLE_CLIENT_ID: 'g-id',
    });
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

    // #when
    const response = await call(env, '/auth/config');

    // #then — fail closed, with the missing var named for the operator
    expect(response.status).toBe(404);
    expect(errorSpy).toHaveBeenCalledTimes(1);
    expect(String(errorSpy.mock.calls[0]?.[0])).toContain(
      '"var":"GOOGLE_CLIENT_SECRET"',
    );
    errorSpy.mockRestore();
  });

  it('demo ON + full Google pair: /auth/config advertises google and /auth/google redirects to the real authorize URL', async () => {
    // #given
    const env = makeEnv({ DEMO_JWT_SECRET: 'demo-secret', ...GOOGLE_PAIR });

    // #when / #then — the SPA's provider discovery
    const config = await call(env, '/auth/config');
    expect(config.status).toBe(200);
    expect(await config.json()).toEqual({ enabled: true, provider: 'google' });

    // #then — the entry route 302s to Google with the CSRF-binding cookie
    const entry = await call(env, '/auth/google');
    expect(entry.status).toBe(302);
    expect(entry.headers.get('location')).toContain(
      'https://accounts.google.com/o/oauth2/v2/auth',
    );
    expect(entry.headers.get('set-cookie')).toContain(STATE_COOKIE);
  });

  it('demo ON, half-set Google + full GitHub pair: the fallback mounts — the misconfig cannot mask it', async () => {
    // #given — the exact reviewed failure mode, proven over HTTP
    const env = makeEnv({
      DEMO_JWT_SECRET: 'demo-secret',
      GOOGLE_CLIENT_ID: 'g-id',
      ...GITHUB_PAIR,
    });
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

    // #when
    const config = await call(env, '/auth/config');

    // #then — GitHub serves sign-in; the broken Google pair is still surfaced
    expect(config.status).toBe(200);
    expect(await config.json()).toEqual({ enabled: true, provider: 'github' });
    expect(
      errorSpy.mock.calls.some(([line]) =>
        String(line).includes('"var":"GOOGLE_CLIENT_SECRET"'),
      ),
    ).toBe(true);
    errorSpy.mockRestore();
  });
});

describe('showcase worker fetch(): the live-stream stage', () => {
  // A structural stub HUB namespace: it records every /internal/event POST the
  // composer's fetch-scope stream sink forwards, so the test can assert the
  // fan-out without workerd. Its /subscribe (raw Request) 426s — no WS here.
  function makeHubStub(): { namespace: Env['HUB']; events: unknown[] } {
    const events: unknown[] = [];
    const namespace = {
      idFromName: (name: string) => ({ name }),
      get: () => ({
        fetch: async (
          input: unknown,
          init?: { method?: string; body?: string },
        ): Promise<Response> => {
          if (typeof input === 'string' && init?.method === 'POST') {
            events.push(JSON.parse(init.body ?? 'null'));
            return new Response(JSON.stringify({ ok: true }), { status: 200 });
          }
          return new Response('{}', { status: 426 });
        },
      }),
    };
    return { namespace: namespace as unknown as Env['HUB'], events };
  }

  function streamingEnv(overrides: Partial<Env> = {}): {
    env: Env;
    events: unknown[];
  } {
    const { namespace, events } = makeHubStub();
    const env = makeEnv({
      HUB: namespace,
      STREAM_TICKET_SECRET: 'test-stream-secret',
      ...overrides,
    });
    return { env, events };
  }

  it('mints a hub stream ticket for an authenticated actor', async () => {
    // #given the stream stage is mounted (HUB + secret both present)
    const { env } = streamingEnv();

    // #when the reviewer POSTs for a hub-channel ticket
    const response = await call(env, '/api/stream/ticket', {
      method: 'POST',
      token: 'tok-reviewer',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ channel: 'hub' }),
    });

    // #then a same-origin url + a signed ticket + an expiry come back
    expect(response.status).toBe(200);
    const body = (await response.json()) as {
      url: string;
      ticket: string;
      expiresAt: number;
    };
    expect(body.url).toBe('/api/stream/hub');
    expect(typeof body.ticket).toBe('string');
    expect(body.ticket.length).toBeGreaterThan(0);
    expect(typeof body.expiresAt).toBe('number');
  });

  it('401s the ticket route without a bearer token', async () => {
    // #given / #when / #then — the ticket route authenticates like every mutation
    const { env } = streamingEnv();
    const response = await call(env, '/api/stream/ticket', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ channel: 'hub' }),
    });
    expect(response.status).toBe(401);
  });

  it('404s a run-channel ticket with a malformed opaque id', async () => {
    // #given
    const { env } = streamingEnv();

    // #when the id violates the public path-safe grammar
    const response = await call(env, '/api/stream/ticket', {
      method: 'POST',
      token: 'tok-reviewer',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        channel: 'run',
        runId: 'bad/run',
      }),
    });

    // #then malformed and unknown ids share the not-found posture
    expect(response.status).toBe(404);
  });

  it('mints a run-channel ticket for a registered opaque run id', async () => {
    const runId = '00000000-0000-4000-8000-000000000000';
    const { env } = streamingEnv({
      RUNNER: {
        idFromName: (name: string) => ({ name }),
        get: () => ({
          fetch: async () =>
            new Response(JSON.stringify({ runId, status: 'suspended' }), {
              status: 200,
              headers: { 'content-type': 'application/json' },
            }),
        }),
      } as unknown as Env['RUNNER'],
    });
    await new D1ApprovalStoreFactory(
      env.DB as ConstructorParameters<typeof D1ApprovalStoreFactory>[0],
    )
      .resources()
      .claim('run', runId, { kind: 'human', id: 'rev-ray' });

    // #when they request a run ticket, supplying the workflowId they hold
    const response = await call(env, '/api/stream/ticket', {
      method: 'POST',
      token: 'tok-reviewer',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        channel: 'run',
        runId,
        workflowId: 'gtm-outbound',
      }),
    });

    // #then the url is the complete, workflow-qualified run-channel path
    expect(response.status).toBe(200);
    const body = (await response.json()) as {
      url: string;
      ticket: string;
      expiresAt: number;
    };
    expect(body.url).toBe(`/api/stream/run/gtm-outbound/${runId}`);
    expect(typeof body.ticket).toBe('string');
    expect(body.ticket.length).toBeGreaterThan(0);
  });

  it('forwards an approval mutation to the deployment hub as a stream event', async () => {
    // #given a seeded pending approval + a stub hub
    const sqlite = openSqlite();
    seedDeployment(sqlite);
    const db = sqliteUnitDatabase(sqlite);
    const approvals = new D1ApprovalStoreFactory(
      db as ConstructorParameters<typeof D1ApprovalStoreFactory>[0],
    );
    const at = new Date(1_751_000_000_000).toISOString();
    await approvals.store().create({
      id: 'apr-stream-1',
      workflowId: 'gtm-outbound',
      runId: '00000000-0000-4000-8000-000000000001',
      title: 'stream approval',
      connectors: [],
      priority: 'normal',
      status: 'pending',
      requestedBy: 'demo-operator',
      createdAt: at,
      updatedAt: at,
    });
    const { env, events } = streamingEnv({ DB: db } as Partial<Env>);

    // #when the reviewer claims it (a mutation that fires the stream sink)
    const response = await call(env, '/api/approvals/apr-stream-1/claim', {
      method: 'POST',
      token: 'tok-reviewer',
    });

    // #then the claim succeeds and the hub received exactly one forwarded event
    expect(response.status).toBe(200);
    expect(events).toHaveLength(1);
    const forwarded = events[0] as {
      type: string;
      record: { id: string };
    };
    expect(forwarded.type).toBe('claimed');
    expect(forwarded.record.id).toBe('apr-stream-1');
  });

  it('contains a failing HUB publish: the mutation still succeeds and the failure is only logged (DL-011)', async () => {
    // #given a seeded pending approval, and a HUB stub whose /internal/event
    // THROWS — a hard transport failure, not merely a non-2xx status
    const sqlite = openSqlite();
    seedDeployment(sqlite);
    const db = sqliteUnitDatabase(sqlite);
    const approvals = new D1ApprovalStoreFactory(
      db as ConstructorParameters<typeof D1ApprovalStoreFactory>[0],
    );
    const at = new Date(1_751_000_000_000).toISOString();
    await approvals.store().create({
      id: 'apr-stream-fail',
      workflowId: 'gtm-outbound',
      runId: '00000000-0000-4000-8000-000000000002',
      title: 'stream approval (failing hub)',
      connectors: [],
      priority: 'normal',
      status: 'pending',
      requestedBy: 'demo-operator',
      createdAt: at,
      updatedAt: at,
    });
    const throwingNamespace = {
      idFromName: (name: string) => ({ name }),
      get: () => ({
        fetch: async (): Promise<Response> => {
          throw new Error('hub unreachable');
        },
      }),
    } as unknown as Env['HUB'];
    const env = makeEnv({
      DB: db,
      HUB: throwingNamespace,
      STREAM_TICKET_SECRET: 'test-stream-secret',
    } as Partial<Env>);
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

    // #when the reviewer claims it
    const response = await call(env, '/api/approvals/apr-stream-fail/claim', {
      method: 'POST',
      token: 'tok-reviewer',
    });

    // #then the claim itself is UNAFFECTED — 200, and the record really claimed
    expect(response.status).toBe(200);
    const claimed = (await response.json()) as { status: string };
    expect(claimed.status).toBe('claimed');
    // #then the fan-out failure is contained: only logged, never thrown through
    expect(
      errorSpy.mock.calls.some(([line]) =>
        String(line).includes('"type":"stream-publish-error"'),
      ),
    ).toBe(true);
    errorSpy.mockRestore();
  });

  it('leaves streaming unmounted when the ticket secret is absent (poll-only)', async () => {
    // #given HUB bound but NO STREAM_TICKET_SECRET
    const { namespace } = makeHubStub();
    const env = makeEnv({ HUB: namespace });

    // #when the reviewer tries to mint a ticket
    const response = await call(env, '/api/stream/ticket', {
      method: 'POST',
      token: 'tok-reviewer',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ channel: 'hub' }),
    });

    // #then the stream stage never mounted, so the route falls through to 404
    expect(response.status).toBe(404);
  });
});
