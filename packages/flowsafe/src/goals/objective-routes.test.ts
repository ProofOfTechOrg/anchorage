// SPDX-License-Identifier: Apache-2.0
// The goal objective HTTP surface (createObjectiveRouter): the P6-lite gate ORDER
// (401 -> role -> ownership -> size/body/field/cap -> audit -> persist), each
// fail-closed, plus the set/get/update/clear round-trip (byte-identical to core's
// Agent goal methods), the maxRuns host cap, and the GOAL_REQUEST_CONTEXT_KEY
// no-collision reservation — over mock resolve + store seams.

import { describe, expect, it, vi } from 'vitest';

import type { ApprovalActor, TenantContext } from '../approval-api/index.js';
import {
  BREAKWATER_ACTOR_KEY,
  BREAKWATER_APPROVED_CONNECTORS_KEY,
  BREAKWATER_ISOLATION_SCOPE_KEY,
  BREAKWATER_WORKFLOW_SCOPE_KEY,
} from '../do-runner/breakwater-keys.js';
import {
  createObjectiveRouter,
  GOAL_REQUEST_CONTEXT_KEY,
  type ObjectiveAuditEvent,
  type ObjectiveStore,
} from './objective-routes.js';

const OWNED_THREAD = 'acme_t1';

function tenantCtx(
  role: ApprovalActor['role'],
  ownedThread = OWNED_THREAD,
): TenantContext {
  const actor: ApprovalActor = { id: 'opal', role, tenantId: 'acme' };
  return {
    actor,
    tenantId: 'acme',
    ownsMemoryId: (id: string) => id === ownedThread,
  } as unknown as TenantContext;
}

/** A minimal in-memory ObjectiveStore keyed by (threadId, type). */
function memoryStore(): { store: ObjectiveStore; raw: Map<string, unknown> } {
  const raw = new Map<string, unknown>();
  const key = (threadId: string, type: string) => `${threadId}::${type}`;
  const store: ObjectiveStore = {
    getState: async <T = unknown>({
      threadId,
      type,
    }: {
      threadId: string;
      type: string;
    }) => raw.get(key(threadId, type)) as T | undefined,
    setState: async ({ threadId, type, value }) => {
      raw.set(key(threadId, type), value);
    },
    deleteState: async ({ threadId, type }) => {
      raw.delete(key(threadId, type));
    },
  };
  return { store, raw };
}

function req(method: string, threadId: string, body?: unknown): Request {
  return new Request(`http://host/api/threads/${threadId}/goal`, {
    method,
    headers: body === undefined ? {} : { 'content-type': 'application/json' },
    body:
      body === undefined
        ? undefined
        : typeof body === 'string'
          ? body
          : JSON.stringify(body),
  });
}

interface GoalRecord {
  id: string;
  objective: string;
  status: string;
  runsUsed: number;
  startedAt: number;
  updatedAt: number;
  maxRuns?: number;
  judgeModelId?: string;
  prompt?: string;
}

describe('createObjectiveRouter — the P6-lite ingestion gate', () => {
  it.each([
    { maxRunsCap: 0 },
    { maxRunsCap: 1.5 },
    { maxRunsCap: Number.NaN },
    { maxContentBytes: -1 },
    { maxContentBytes: Number.POSITIVE_INFINITY },
  ])('rejects invalid numeric configuration synchronously: %o', (numeric) => {
    const { store } = memoryStore();
    expect(() =>
      createObjectiveRouter({
        resolve: async () => tenantCtx('operator'),
        store,
        ...numeric,
      }),
    ).toThrow(RangeError);
  });

  it('accepts a zero body cap and rejects a non-empty mutation body', async () => {
    const { store } = memoryStore();
    const router = createObjectiveRouter({
      resolve: async () => tenantCtx('operator'),
      store,
      maxContentBytes: 0,
    });
    expect(
      (await router(req('PUT', OWNED_THREAD, { objective: 'ship it' })))
        ?.status,
    ).toBe(413);
  });

  it('returns null for a non-goal path (composes ahead of others)', async () => {
    const { store } = memoryStore();
    const router = createObjectiveRouter({
      resolve: async () => tenantCtx('operator'),
      store,
    });
    expect(await router(new Request('http://host/workflows'))).toBeNull();
    // A signal channel on the same base is NOT ours — leave it for the signal router.
    expect(
      await router(
        new Request(`http://host/api/threads/${OWNED_THREAD}/message`),
      ),
    ).toBeNull();
  });

  it('is route-absent on a malformed percent-encoded threadId (no pre-auth URIError)', async () => {
    // A lone '%' in the threadId segment — bare decodeURIComponent would THROW
    // out of the handler BEFORE auth; safeDecodeSegment makes it route-absent.
    const { store, raw } = memoryStore();
    const router = createObjectiveRouter({
      resolve: async () => tenantCtx('operator'),
      store,
    });
    const res = await router(
      new Request('http://host/api/threads/%/goal', {
        method: 'PUT',
        body: '{}',
      }),
    );
    expect(res).toBeNull();
    expect(raw.size).toBe(0); // never resolved, never written
  });

  it('405 for an unsupported method on the goal path', async () => {
    const { store } = memoryStore();
    const router = createObjectiveRouter({
      resolve: async () => tenantCtx('operator'),
      store,
    });
    const res = await router(req('POST', OWNED_THREAD, { objective: 'x' }));
    expect(res?.status).toBe(405);
  });

  it('401 when unauthenticated, and does NOT audit (no flood amplification)', async () => {
    const { store, raw } = memoryStore();
    const events: ObjectiveAuditEvent[] = [];
    const router = createObjectiveRouter({
      resolve: async () => undefined,
      store,
      audit: (e) => {
        events.push(e);
      },
    });
    const res = await router(req('PUT', OWNED_THREAD, { objective: 'x' }));
    expect(res?.status).toBe(401);
    expect(raw.size).toBe(0);
    expect(events).toHaveLength(0);
  });

  it('403 for a read-only role on a WRITE, and audits the rejection', async () => {
    const { store, raw } = memoryStore();
    const events: ObjectiveAuditEvent[] = [];
    const router = createObjectiveRouter({
      resolve: async () => tenantCtx('viewer'),
      store,
      audit: (e) => {
        events.push(e);
      },
    });
    const res = await router(req('PUT', OWNED_THREAD, { objective: 'x' }));
    expect(res?.status).toBe(403);
    expect(raw.size).toBe(0);
    expect(events).toEqual([
      expect.objectContaining({
        type: 'goal.objective',
        operation: 'set',
        outcome: 'rejected',
        reason: 'forbidden-role',
        tenantId: 'acme',
        actorId: 'opal',
        threadId: OWNED_THREAD,
      }),
    ]);
  });

  it('allows a read-only role to READ its own thread (reads are not role-gated)', async () => {
    const { store } = memoryStore();
    const router = createObjectiveRouter({
      resolve: async () => tenantCtx('viewer'),
      store,
    });
    const res = await router(req('GET', OWNED_THREAD));
    expect(res?.status).toBe(200);
    expect(await res?.json()).toEqual({ objective: null });
  });

  it('404 for a foreign threadId (no existence oracle) and audits the probe', async () => {
    const { store, raw } = memoryStore();
    const events: ObjectiveAuditEvent[] = [];
    const router = createObjectiveRouter({
      resolve: async () => tenantCtx('operator'),
      store,
      audit: (e) => {
        events.push(e);
      },
    });
    const res = await router(req('PUT', 'other_t9', { objective: 'x' }));
    expect(res?.status).toBe(404);
    expect(raw.size).toBe(0);
    expect(events).toEqual([
      expect.objectContaining({
        outcome: 'rejected',
        reason: 'foreign-thread',
        threadId: 'other_t9',
      }),
    ]);
  });

  it('400 + audits when the body smuggles a memory id (assertNoClientMemoryIds)', async () => {
    const { store, raw } = memoryStore();
    const events: ObjectiveAuditEvent[] = [];
    const router = createObjectiveRouter({
      resolve: async () => tenantCtx('operator'),
      store,
      audit: (e) => {
        events.push(e);
      },
    });
    const res = await router(
      req('PUT', OWNED_THREAD, { objective: 'x', resourceId: 'acme_hax' }),
    );
    expect(res?.status).toBe(400);
    expect(raw.size).toBe(0);
    expect(events).toEqual([
      expect.objectContaining({
        outcome: 'rejected',
        reason: 'client-memory-id',
        threadId: OWNED_THREAD,
      }),
    ]);
  });

  it('413 + audits an oversized objective payload', async () => {
    const { store } = memoryStore();
    const events: ObjectiveAuditEvent[] = [];
    const router = createObjectiveRouter({
      resolve: async () => tenantCtx('operator'),
      store,
      maxContentBytes: 64,
      audit: (e) => {
        events.push(e);
      },
    });
    const res = await router(
      req('PUT', OWNED_THREAD, { objective: 'x'.repeat(200) }),
    );
    expect(res?.status).toBe(413);
    expect(events.some((e) => e.reason === 'payload-too-large')).toBe(true);
  });

  it('400 + audits an unknown/reserved body field (allowlist)', async () => {
    const { store } = memoryStore();
    const events: ObjectiveAuditEvent[] = [];
    const router = createObjectiveRouter({
      resolve: async () => tenantCtx('operator'),
      store,
      audit: (e) => {
        events.push(e);
      },
    });
    // runsUsed is runtime-owned — a client resetting it must be refused.
    const res = await router(
      req('PUT', OWNED_THREAD, { objective: 'x', runsUsed: 0 }),
    );
    expect(res?.status).toBe(400);
    expect(events.some((e) => e.reason === 'field-not-allowed:runsUsed')).toBe(
      true,
    );
  });

  it('400 when objective is missing or blank on set', async () => {
    const { store } = memoryStore();
    const router = createObjectiveRouter({
      resolve: async () => tenantCtx('operator'),
      store,
    });
    expect((await router(req('PUT', OWNED_THREAD, {})))?.status).toBe(400);
    expect(
      (await router(req('PUT', OWNED_THREAD, { objective: '   ' })))?.status,
    ).toBe(400);
  });
});

describe('createObjectiveRouter — maxRuns host cap (DL-007)', () => {
  it('rejects a maxRuns above the host cap and audits it (never clamps)', async () => {
    const { store, raw } = memoryStore();
    const events: ObjectiveAuditEvent[] = [];
    const router = createObjectiveRouter({
      resolve: async () => tenantCtx('operator'),
      store,
      maxRunsCap: 10,
      audit: (e) => {
        events.push(e);
      },
    });
    const res = await router(
      req('PUT', OWNED_THREAD, { objective: 'x', maxRuns: 50 }),
    );
    expect(res?.status).toBe(400);
    expect(raw.size).toBe(0); // nothing stored — not a clamped 10
    expect(events.some((e) => e.reason === 'maxruns-over-cap')).toBe(true);
  });

  it('defaults the cap to the core DEFAULT_GOAL_MAX_RUNS (50)', async () => {
    const { store } = memoryStore();
    const router = createObjectiveRouter({
      resolve: async () => tenantCtx('operator'),
      store,
    });
    // 50 is allowed (== core default); 51 is not.
    expect(
      (await router(req('PUT', OWNED_THREAD, { objective: 'x', maxRuns: 50 })))
        ?.status,
    ).toBe(200);
    expect(
      (await router(req('PUT', OWNED_THREAD, { objective: 'x', maxRuns: 51 })))
        ?.status,
    ).toBe(400);
  });

  it('rejects a non-positive-integer maxRuns', async () => {
    const { store } = memoryStore();
    const router = createObjectiveRouter({
      resolve: async () => tenantCtx('operator'),
      store,
    });
    expect(
      (await router(req('PUT', OWNED_THREAD, { objective: 'x', maxRuns: 0 })))
        ?.status,
    ).toBe(400);
    expect(
      (await router(req('PUT', OWNED_THREAD, { objective: 'x', maxRuns: 2.5 })))
        ?.status,
    ).toBe(400);
  });

  it('rejects a whitespace-only judgeModelId but keeps prompt whitespace', async () => {
    const { store, raw } = memoryStore();
    const router = createObjectiveRouter({
      resolve: async () => tenantCtx('operator'),
      store,
    });
    // judgeModelId is an identifier — blank would persist and only fail later
    // at model resolution, so the boundary refuses it.
    const blankJudge = await router(
      req('PUT', OWNED_THREAD, { objective: 'x', judgeModelId: '   ' }),
    );
    expect(blankJudge?.status).toBe(400);
    expect(raw.size).toBe(0);
    // prompt is content — surrounding whitespace is legitimate and preserved.
    const paddedPrompt = await router(
      req('PUT', OWNED_THREAD, { objective: 'x', prompt: '  padded  ' }),
    );
    expect(paddedPrompt?.status).toBe(200);
    expect(
      ((await paddedPrompt?.json()) as { objective: { prompt?: string } })
        .objective.prompt,
    ).toBe('  padded  ');
  });
});

describe('createObjectiveRouter — set / get / update / clear round-trip', () => {
  it('set persists a fresh active record byte-identical to core setObjective', async () => {
    const { store } = memoryStore();
    const audit = vi.fn();
    const router = createObjectiveRouter({
      resolve: async () => tenantCtx('operator'),
      store,
      audit,
    });
    const res = await router(
      req('PUT', OWNED_THREAD, {
        objective: 'ship the launch',
        maxRuns: 5,
        judgeModelId: 'openai/gpt-4o',
        prompt: 'be strict',
      }),
    );
    expect(res?.status).toBe(200);
    const { objective } = (await res?.json()) as { objective: GoalRecord };
    expect(objective).toEqual(
      expect.objectContaining({
        objective: 'ship the launch',
        status: 'active',
        runsUsed: 0,
        maxRuns: 5,
        judgeModelId: 'openai/gpt-4o',
        prompt: 'be strict',
      }),
    );
    expect(typeof objective.id).toBe('string');
    expect(typeof objective.startedAt).toBe('number');
    expect(objective.updatedAt).toBe(objective.startedAt);
    expect(audit).toHaveBeenCalledWith(
      expect.objectContaining({ operation: 'set', outcome: 'accepted' }),
    );
  });

  it('get returns the stored record, then null after clear', async () => {
    const { store } = memoryStore();
    const router = createObjectiveRouter({
      resolve: async () => tenantCtx('operator'),
      store,
    });
    await router(req('PUT', OWNED_THREAD, { objective: 'goal one' }));

    const got = await router(req('GET', OWNED_THREAD));
    expect(got?.status).toBe(200);
    expect(
      ((await got?.json()) as { objective: GoalRecord }).objective.objective,
    ).toBe('goal one');

    const cleared = await router(req('DELETE', OWNED_THREAD));
    expect(cleared?.status).toBe(200);
    expect(await cleared?.json()).toEqual({ ok: true });

    const gone = await router(req('GET', OWNED_THREAD));
    expect(await gone?.json()).toEqual({ objective: null });

    // clear is idempotent.
    expect((await router(req('DELETE', OWNED_THREAD)))?.status).toBe(200);
  });

  it('update merges options, preserves runsUsed/startedAt/id, and bumps updatedAt', async () => {
    const { store } = memoryStore();
    const router = createObjectiveRouter({
      resolve: async () => tenantCtx('operator'),
      store,
    });
    const setRes = await router(
      req('PUT', OWNED_THREAD, { objective: 'original', maxRuns: 3 }),
    );
    const original = ((await setRes?.json()) as { objective: GoalRecord })
      .objective;

    const upRes = await router(
      req('PATCH', OWNED_THREAD, {
        maxRuns: 7,
        prompt: 'refined',
        status: 'paused',
      }),
    );
    expect(upRes?.status).toBe(200);
    const updated = ((await upRes?.json()) as { objective: GoalRecord })
      .objective;
    expect(updated.objective).toBe('original'); // prose preserved
    expect(updated.id).toBe(original.id);
    expect(updated.startedAt).toBe(original.startedAt);
    expect(updated.runsUsed).toBe(0);
    expect(updated.maxRuns).toBe(7);
    expect(updated.prompt).toBe('refined');
    expect(updated.status).toBe('paused');
    expect(updated.updatedAt).toBeGreaterThanOrEqual(original.updatedAt);
  });

  it('update refuses to change the prose (objective is not an update field)', async () => {
    const { store } = memoryStore();
    const router = createObjectiveRouter({
      resolve: async () => tenantCtx('operator'),
      store,
    });
    await router(req('PUT', OWNED_THREAD, { objective: 'original' }));
    const res = await router(
      req('PATCH', OWNED_THREAD, { objective: 'hijacked' }),
    );
    expect(res?.status).toBe(400);
  });

  it('update 404s when no objective is set on the OWN thread', async () => {
    const { store } = memoryStore();
    const events: ObjectiveAuditEvent[] = [];
    const router = createObjectiveRouter({
      resolve: async () => tenantCtx('operator'),
      store,
      audit: (e) => {
        events.push(e);
      },
    });
    const res = await router(req('PATCH', OWNED_THREAD, { maxRuns: 2 }));
    expect(res?.status).toBe(404);
    expect(events.some((e) => e.reason === 'no-objective')).toBe(true);
  });

  it('does not audit a benign successful GET (not a standing-instruction write)', async () => {
    const { store } = memoryStore();
    const events: ObjectiveAuditEvent[] = [];
    const router = createObjectiveRouter({
      resolve: async () => tenantCtx('viewer'),
      store,
      audit: (e) => {
        events.push(e);
      },
    });
    await router(req('GET', OWNED_THREAD));
    expect(events).toHaveLength(0);
  });
});

describe('GOAL_REQUEST_CONTEXT_KEY reservation (DL-018 no-collision pin)', () => {
  // The keys #requestContextFor mints on a leg: the workflow-scope + isolation-
  // scope base (runtime.ts) plus the grant/actor keys the provider merges over
  // them. ALL are the breakwater 'breakwater.*' namespace; the goal key is
  // 'mastra:goal'. Pinning BOTH the exact set AND the namespace means a future
  // base key added under that namespace still cannot collide with this one.
  const RUNTIME_BASE_KEYS = [
    BREAKWATER_WORKFLOW_SCOPE_KEY,
    BREAKWATER_ISOLATION_SCOPE_KEY,
    BREAKWATER_APPROVED_CONNECTORS_KEY,
    BREAKWATER_ACTOR_KEY,
  ];

  it('is the core value mastra:goal', () => {
    expect(GOAL_REQUEST_CONTEXT_KEY).toBe('mastra:goal');
  });

  it('pins the mirrored value to the core dist declaration (drift guard)', () => {
    // GOAL_REQUEST_CONTEXT_KEY is not exports-reachable (R-001), so the mirror
    // cannot be compared via an import — read the pinned core's own .d.ts
    // declaration instead. A core bump that changes the value (or moves the
    // file) fails HERE loudly, per the P9 re-anchor protocol. Builtins load via
    // process.getBuiltinModule (the test-support/sqlite.ts idiom) and
    // import.meta.url is cast, so this workers-typed program never sees a
    // node: import it cannot type.
    const getBuiltin = (
      globalThis as {
        process?: { getBuiltinModule?: (id: string) => unknown };
      }
    ).process?.getBuiltinModule;
    if (!getBuiltin) {
      throw new Error('drift guard requires node >= 22.3');
    }
    const { createRequire } = getBuiltin('node:module') as {
      createRequire: (from: string) => { resolve: (id: string) => string };
    };
    const { readFileSync } = getBuiltin('node:fs') as {
      readFileSync: (path: string, encoding: 'utf8') => string;
    };
    const fromHere = (import.meta as unknown as { url: string }).url;
    const toolsEntry = createRequire(fromHere).resolve('@mastra/core/tools');
    // …/dist/tools/index.cjs -> …/dist/agent/goal/objective.d.ts
    const objectiveDts = toolsEntry.replace(
      /tools[/\\]index\.[cm]?js$/,
      'agent/goal/objective.d.ts',
    );
    if (objectiveDts === toolsEntry) {
      throw new Error(
        `core dist layout changed — re-anchor the drift guard (resolved: ${toolsEntry})`,
      );
    }
    expect(readFileSync(objectiveDts, 'utf8')).toContain(
      `GOAL_REQUEST_CONTEXT_KEY = "${GOAL_REQUEST_CONTEXT_KEY}"`,
    );
  });

  it('does not equal any runtime base context key', () => {
    for (const key of RUNTIME_BASE_KEYS) {
      expect(GOAL_REQUEST_CONTEXT_KEY).not.toBe(key);
    }
  });

  it('is outside the breakwater.* namespace the runtime mints into', () => {
    for (const key of RUNTIME_BASE_KEYS) {
      expect(key.startsWith('breakwater.')).toBe(true);
    }
    expect(GOAL_REQUEST_CONTEXT_KEY.startsWith('breakwater.')).toBe(false);
  });
});

describe('createObjectiveRouter internal errors', () => {
  it('returns a generic 500 and logs the private store detail', async () => {
    const logged: string[] = [];
    const log = vi.spyOn(console, 'error').mockImplementation((value) => {
      logged.push(String(value));
    });
    const router = createObjectiveRouter({
      resolve: async () => tenantCtx('operator'),
      store: {
        getState: async () => {
          throw new Error('private objective store detail');
        },
        setState: async () => {},
        deleteState: async () => {},
      },
    });

    try {
      const response = await router(req('GET', OWNED_THREAD));
      expect(response?.status).toBe(500);
      expect(await response?.json()).toEqual({ error: 'internal error' });
      expect(response?.headers.get('cache-control')).toBe('no-store');
      expect(logged.join('\n')).toContain('private objective store detail');
    } finally {
      log.mockRestore();
    }
  });
});
