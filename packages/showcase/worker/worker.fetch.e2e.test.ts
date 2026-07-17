// Drives the showcase worker's EXPORTED fetch() in-process — the route
// composition itself (healthz, the demo-auth mount, the auth fall-through to
// the run/approval surface), which the other showcase tests bypass by calling
// the routers directly. Mirrors deploy/worker.e2e.test.ts's scaffolding:
// D1-shaped node:sqlite, a throwing DO namespace (these routes never reach a
// run leg), and the Parameters<typeof fetchHandler> cast for workers types.

import type { ApprovalRecord } from '@proofoftech/flowsafe/approval-api';

import { D1ApprovalStoreFactory } from '@proofoftech/flowsafe/approval-api';
import { provisionTenant } from '@proofoftech/flowsafe/host-kit';
import { describe, expect, it, vi } from 'vitest';
import { STATE_COOKIE } from '#worker/demo-auth';
import handler from '#worker/worker';

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

// Minimal D1 face over node:sqlite — the same envelope deploy's e2e uses
// ({ results }, { meta }); enough for any schema-init the routers run.
function d1DatabaseLike(db: SqliteDatabase): unknown {
  function statement(sql: string, params: unknown[]): Record<string, unknown> {
    return {
      bind: (...values: unknown[]) => statement(sql, values),
      first: async (column?: string) => {
        const row = db.prepare(sql).get(...params) as
          | Record<string, unknown>
          | undefined;
        if (row === undefined) return null;
        return column !== undefined ? (row[column] ?? null) : row;
      },
      run: async () => {
        const outcome = db.prepare(sql).run(...params) as {
          changes?: number | bigint;
        };
        return {
          success: true,
          meta: { changes: Number(outcome?.changes ?? 0) },
        };
      },
      all: async () => ({
        success: true,
        results: db.prepare(sql).all(...params),
        meta: {},
      }),
      raw: async () => {
        const rows = db.prepare(sql).all(...params) as Array<
          Record<string, unknown>
        >;
        return rows.map((row) => Object.values(row));
      },
    };
  }
  return {
    prepare: (sql: string) => statement(sql, []),
    exec: async (sql: string) => {
      db.exec(sql);
      return { count: 1, duration: 0 };
    },
    batch: async (statements: Array<{ run: () => Promise<unknown> }>) => {
      const results = [];
      for (const stmt of statements) results.push(await stmt.run());
      return results;
    },
    dump: async () => new ArrayBuffer(0),
  };
}

function required<T>(value: T | undefined): T {
  if (value === undefined) throw new Error('handler method missing');
  return value;
}

const fetchHandler = required(handler.fetch);

type Env = Parameters<typeof fetchHandler>[1];

const TOKENS = {
  'tok-reviewer': { id: 'rev-ray', role: 'reviewer', tenantId: 'demo' },
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
  return {
    DB: d1DatabaseLike(openSqlite()),
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
      tenantId: 'demo',
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

describe('showcase worker fetch(): the demo-reset mount', () => {
  const DEMO_TENANT = 'dmvisitor01';
  const RESET_TOKENS = {
    'tok-demo-admin': {
      id: 'demo-admin',
      role: 'admin',
      tenantId: DEMO_TENANT,
    },
    'tok-demo-viewer': {
      id: 'demo-viewer',
      role: 'viewer',
      tenantId: DEMO_TENANT,
    },
    // Static operator identity: admin of a tenant NEVER provisioned as demo.
    'tok-ops-admin': { id: 'ops-olive', role: 'admin', tenantId: 'acme' },
    ...TOKENS,
  };

  let approvalSeq = 0;

  function approvalRecord(tenantId: string): ApprovalRecord {
    approvalSeq += 1;
    const at = new Date(1751000000000 + approvalSeq * 1000).toISOString();
    return {
      id: `apr-reset-${approvalSeq}`,
      tenantId,
      workflowId: 'gtm-outbound',
      runId: `${tenantId}_run${approvalSeq}`,
      title: `approval ${approvalSeq}`,
      connectors: [],
      priority: 'normal',
      status: 'pending',
      createdAt: at,
      updatedAt: at,
    };
  }

  // One seeded world: a provisioned demo tenant holding a snapshot row and an
  // approval, plus a foreign tenant's row in each store that must SURVIVE.
  async function seededEnv(): Promise<{
    env: Env;
    sqlite: SqliteDatabase;
    approvals: D1ApprovalStoreFactory;
  }> {
    const sqlite = openSqlite();
    const db = d1DatabaseLike(sqlite);
    const env = makeEnv({
      DB: db,
      APPROVAL_ACTOR_TOKENS: JSON.stringify(RESET_TOKENS),
    } as Partial<Env>);
    await provisionTenant(db as Parameters<typeof provisionTenant>[0], {
      tenantId: DEMO_TENANT,
      kind: 'demo',
    });
    sqlite.exec(
      `CREATE TABLE mastra_workflow_snapshot (
         workflow_name TEXT NOT NULL,
         run_id TEXT NOT NULL,
         snapshot TEXT NOT NULL,
         createdAt TEXT,
         updatedAt TEXT
       )`,
    );
    const insert = sqlite.prepare(
      `INSERT INTO mastra_workflow_snapshot
         (workflow_name, run_id, snapshot) VALUES (?, ?, ?)`,
    );
    insert.run('gtm-outbound', `${DEMO_TENANT}_run1`, '{}');
    insert.run('gtm-outbound', 'acme_run1', '{}');
    const approvals = new D1ApprovalStoreFactory(
      db as ConstructorParameters<typeof D1ApprovalStoreFactory>[0],
    );
    await approvals.forTenant(DEMO_TENANT).create(approvalRecord(DEMO_TENANT));
    await approvals.forTenant('acme').create(approvalRecord('acme'));
    return { env, sqlite, approvals };
  }

  it("an admin of a demo tenant wipes exactly their own rows — the foreign tenant's survive", async () => {
    // #given
    const { env, sqlite, approvals } = await seededEnv();
    const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    // #when
    const response = await call(env, '/demo/reset', {
      method: 'POST',
      token: 'tok-demo-admin',
    });

    // #then — real counts in the envelope
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      ok: true,
      tenantId: DEMO_TENANT,
      purged: {
        snapshots: 1,
        threads: 0,
        messages: 0,
        resources: 0,
        backgroundTasks: 0,
        notifications: 0,
        threadState: 0,
        schedules: 0,
        scheduleTriggers: 0,
        approvals: 1,
        artifacts: 0,
      },
    });
    // #then — the demo tenant's stores are empty, the foreign tenant's intact
    const remaining = sqlite
      .prepare('SELECT run_id FROM mastra_workflow_snapshot')
      .all() as Array<{ run_id: string }>;
    expect(remaining.map((row) => row.run_id)).toEqual(['acme_run1']);
    expect(await approvals.forTenant(DEMO_TENANT).list()).toEqual([]);
    expect(await approvals.forTenant('acme').list()).toHaveLength(1);
    // #then — the structured audit line names the tenant
    expect(
      logSpy.mock.calls.some(([line]) =>
        String(line).includes('"type":"demo-reset"'),
      ),
    ).toBe(true);
    logSpy.mockRestore();
  });

  it('refuses everyone else: 401 without a token, 403 for a non-admin role, 403 for a non-demo tenant', async () => {
    // #given
    const { env, sqlite } = await seededEnv();

    // #when / #then — unauthenticated
    expect((await call(env, '/demo/reset', { method: 'POST' })).status).toBe(
      401,
    );
    // #then — viewer of the demo tenant (RBAC lesson, not a wipe)
    expect(
      (
        await call(env, '/demo/reset', {
          method: 'POST',
          token: 'tok-demo-viewer',
        })
      ).status,
    ).toBe(403);
    // #then — admin of a tenant that was never provisioned as demo
    expect(
      (
        await call(env, '/demo/reset', {
          method: 'POST',
          token: 'tok-ops-admin',
        })
      ).status,
    ).toBe(403);
    // #then — nothing was purged by any refusal
    const rows = sqlite
      .prepare('SELECT run_id FROM mastra_workflow_snapshot')
      .all() as Array<{ run_id: string }>;
    expect(rows).toHaveLength(2);
  });

  it('405s a GET on /demo/reset while the rest of the composition stays intact', async () => {
    // #given
    const { env } = await seededEnv();

    // #when / #then
    expect(
      (await call(env, '/demo/reset', { token: 'tok-demo-admin' })).status,
    ).toBe(405);
    expect((await call(env, '/healthz')).status).toBe(200);
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

  it('404s a run-channel ticket for a run the tenant does not own (no existence oracle)', async () => {
    // #given the reviewer belongs to tenant 'demo'
    const { env } = streamingEnv();

    // #when they request a run ticket for another tenant's runId
    const response = await call(env, '/api/stream/ticket', {
      method: 'POST',
      token: 'tok-reviewer',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        channel: 'run',
        runId: 'acme_00000000-0000-4000-8000-000000000000',
      }),
    });

    // #then the route is not an existence oracle: 404, not 403
    expect(response.status).toBe(404);
  });

  it('mints a run-channel ticket for a run the tenant OWNS, returning the complete workflow-qualified url', async () => {
    // #given the reviewer belongs to tenant 'demo'; ownership is the INV-1
    // prefix check alone (tenantOwnsSaltedId), so no run needs to be seeded
    const { env } = streamingEnv();
    const runId = 'demo_run-stream-success';

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

  it('forwards an approval mutation to the tenant HUB stub as a stream event', async () => {
    // #given a seeded pending approval for the reviewer's tenant + a stub hub
    const sqlite = openSqlite();
    const db = d1DatabaseLike(sqlite);
    const approvals = new D1ApprovalStoreFactory(
      db as ConstructorParameters<typeof D1ApprovalStoreFactory>[0],
    );
    const at = new Date(1_751_000_000_000).toISOString();
    await approvals.forTenant('demo').create({
      id: 'apr-stream-1',
      tenantId: 'demo',
      workflowId: 'gtm-outbound',
      runId: 'demo_stream1',
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
      record: { id: string; tenantId: string };
    };
    expect(forwarded.type).toBe('claimed');
    expect(forwarded.record.id).toBe('apr-stream-1');
    expect(forwarded.record.tenantId).toBe('demo');
  });

  it('contains a failing HUB publish: the mutation still succeeds and the failure is only logged (DL-011)', async () => {
    // #given a seeded pending approval, and a HUB stub whose /internal/event
    // THROWS — a hard transport failure, not merely a non-2xx status
    const sqlite = openSqlite();
    const db = d1DatabaseLike(sqlite);
    const approvals = new D1ApprovalStoreFactory(
      db as ConstructorParameters<typeof D1ApprovalStoreFactory>[0],
    );
    const at = new Date(1_751_000_000_000).toISOString();
    await approvals.forTenant('demo').create({
      id: 'apr-stream-fail',
      tenantId: 'demo',
      workflowId: 'gtm-outbound',
      runId: 'demo_stream2',
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
