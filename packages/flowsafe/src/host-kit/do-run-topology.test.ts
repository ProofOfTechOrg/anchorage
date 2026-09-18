// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it, vi } from 'vitest';

import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';
import {
  beginIdempotentStart,
  EXECUTION_PRINCIPAL_HEADER,
  InvalidMutationEpochError,
  MUTATION_EPOCH_HEADER,
  MutationEpochMismatchError,
  type StartIdempotencyDatabase,
  StartIdempotencyStore,
} from '../do-runner/index.js';
import {
  createDoRunTopology,
  type DoRunLifecycleTopology,
  type DoRunTopology,
  type RunnerNamespaceLike,
} from './do-run-topology.js';
import { RunRouteError } from './run-route-error.js';

const DEPLOYMENT_IDENTITY_SECRET = 'test-deployment-identity-secret-0001';

function harness() {
  const requests: Array<{
    name: string;
    url: string;
    init?: Parameters<
      ReturnType<RunnerNamespaceLike<string>['get']>['fetch']
    >[1];
  }> = [];
  const namespace: RunnerNamespaceLike<string> = {
    idFromName: vi.fn((name) => name),
    get: (name) => ({
      fetch: async (url, init) => {
        requests.push({ name, url, init });
        return new Response(
          JSON.stringify({
            workflowId: 'workflow-1',
            runId: 'run-1',
            status: 'running',
          }),
          { headers: { 'content-type': 'application/json' } },
        );
      },
    }),
  };

  return {
    topology: createDoRunTopology(namespace, DEPLOYMENT_IDENTITY_SECRET),
    namespace,
    requests,
  };
}

describe('C workflow epoch transport', () => {
  it.each([
    undefined,
    0,
    Number.MAX_SAFE_INTEGER,
  ])('C workflow wire carries only canonical epoch headers (%s)', async (mutationEpoch) => {
    const { topology, requests } = harness();
    await topology.start({
      workflowId: 'workflow-1',
      runId: 'run-1',
      inputData: { user: true },
      principal: { kind: 'human', id: 'actor-1', role: 'admin' },
      mutationEpoch,
    });
    expect(
      new Headers(requests[0]?.init?.headers).get(MUTATION_EPOCH_HEADER),
    ).toBe(mutationEpoch === undefined ? null : String(mutationEpoch));
    const body = JSON.parse(requests[0]?.init?.body ?? '');
    expect(body).toEqual({
      workflowId: 'workflow-1',
      runId: 'run-1',
      inputData: { user: true },
    });
    for (const field of [
      'mutationEpoch',
      'startIdentity',
      'agentStart',
      'execution',
      'tablePrefix',
      'startToken',
      'attemptToken',
      'runOwnerGuard',
      'onPreparedStartIdentity',
    ])
      expect(Object.hasOwn(body, field)).toBe(false);
  });

  it.each([
    null,
    '2',
    true,
    -1,
    0.5,
    NaN,
    Infinity,
    Number.MAX_SAFE_INTEGER + 1,
  ])('C invalid workflow epoch refuses before namespace lookup (%s)', async (mutationEpoch) => {
    const { topology, namespace, requests } = harness();
    const get = vi.spyOn(namespace, 'get');
    await expect(
      topology.start({
        workflowId: 'workflow-1',
        runId: 'run-1',
        inputData: {},
        principal: { kind: 'human', id: 'a', role: 'admin' },
        mutationEpoch: mutationEpoch as number,
      }),
    ).rejects.toBeInstanceOf(InvalidMutationEpochError);
    expect(namespace.idFromName).not.toHaveBeenCalled();
    expect(get).not.toHaveBeenCalled();
    expect(requests).toEqual([]);
  });

  it.each([
    'missing',
    'stale',
    'future',
  ] as const)('C workflow transport preserves complete downstream epoch refusal (%s)', async (classification) => {
    const error = new MutationEpochMismatchError(classification, 2);
    const topology = createDoRunTopology(
      {
        idFromName: (name: string) => name,
        get: () => ({
          fetch: async () =>
            Response.json(
              { error: error.message, reason: error.reason },
              { status: 409 },
            ),
        }),
      },
      DEPLOYMENT_IDENTITY_SECRET,
    );
    await expect(
      topology.start({
        workflowId: 'workflow-1',
        runId: 'run-1',
        inputData: {},
        principal: { kind: 'human', id: 'a', role: 'admin' },
      }),
    ).rejects.toMatchObject({
      status: 409,
      message: error.message,
      reason: error.reason,
    });
  });
});

describe('createDoRunTopology', () => {
  it('keeps the legacy topology structurally compatible while returning lifecycle methods', () => {
    const summary = { runId: 'run-1', status: 'running' as const };
    const legacy: DoRunTopology = {
      start: async () => summary,
      status: async () => summary,
      dispatchStatus: async () => summary,
      persistedStart: async () => undefined,
      startLiveness: async () => false,
      resume: async () => summary,
      resumeRecord: async () => summary,
    };
    const lifecycle: DoRunLifecycleTopology = harness().topology;

    expect(legacy).not.toHaveProperty('terminate');
    expect(lifecycle.terminate).toBeTypeOf('function');
    expect(lifecycle.timeOut).toBeTypeOf('function');
  });

  it('carries the full execution principal and ordinary start payload', async () => {
    const { topology, requests } = harness();

    await topology.start({
      workflowId: 'workflow-1',
      runId: 'run-1',
      inputData: { value: 'ordinary-input' },
      initialState: { checkpoint: true },
      requestContext: { 'app.attribution': 'ordinary', nested: { value: 1 } },
      principal: {
        kind: 'agent',
        id: 'agent-1',
        purpose: 'delegated-run',
        delegatedBy: 'operator-1',
      },
    });

    const request = requests[0];
    expect(request?.name).toBe('workflow-1:run-1');
    expect(
      new Headers(request?.init?.headers).get(EXECUTION_PRINCIPAL_HEADER),
    ).toBe(
      '{"kind":"agent","id":"agent-1","purpose":"delegated-run","delegatedBy":"operator-1"}',
    );
    expect(JSON.parse(request?.init?.body ?? '')).toEqual({
      workflowId: 'workflow-1',
      runId: 'run-1',
      inputData: { value: 'ordinary-input' },
      initialState: { checkpoint: true },
      requestContext: { 'app.attribution': 'ordinary', nested: { value: 1 } },
    });
  });

  it('sends only exact trigger addressing for scheduled starts', async () => {
    const { topology, requests } = harness();

    await topology.start({
      workflowId: 'workflow-1',
      runId: 'run-1',
      inputData: { forged: true },
      initialState: { forged: true },
      requestContext: { 'app.attribution': 'forged' },
      principal: {
        kind: 'service',
        id: 'scheduler',
        purpose: 'schedule.fire',
      },
      scheduleId: 'schedule-1',
      dispatchId: 'dispatch-1',
      deadlineMs: 60_000,
    });

    expect(JSON.parse(requests[0]?.init?.body ?? '')).toEqual({
      workflowId: 'workflow-1',
      runId: 'run-1',
      scheduleId: 'schedule-1',
      dispatchId: 'dispatch-1',
      deadlineMs: 60_000,
    });
  });

  it('refuses a partial schedule source before addressing the namespace', async () => {
    const { topology, namespace } = harness();

    await expect(
      topology.start({
        workflowId: 'workflow-1',
        runId: 'run-1',
        inputData: {},
        principal: { kind: 'human', id: 'operator-1', role: 'operator' },
        scheduleId: 'schedule-1',
      }),
    ).rejects.toThrow(
      'scheduled run starts require both scheduleId and dispatchId',
    );
    expect(namespace.idFromName).not.toHaveBeenCalled();
  });
});

describe('FS8 D3 workflow reclaim liveness transport', () => {
  async function retry(
    liveness: () => Promise<Response>,
    state: 'reserved' | 'started' = 'reserved',
  ) {
    const sqlite = openSqlite();
    const store = new StartIdempotencyStore(
      sqliteUnitDatabase(sqlite) as StartIdempotencyDatabase,
      { now: () => 1_000 },
    );
    const request = {
      key: 'retained-key',
      owner: { kind: 'human' as const, id: 'operator-1' },
      targetKind: 'workflow' as const,
      targetId: 'workflow-1',
      mintRunId: () => 'retained-run',
    };
    const { reservation } = await store.reserve(request);
    if (state === 'started') await store.claimReservation(reservation);
    const before = sqlite
      .prepare('SELECT * FROM flowsafe_start_idempotency')
      .all();
    const claim = vi.spyOn(store, 'claimReservation');
    const fetch = vi.fn(async (url: string) => {
      if (url.endsWith('/start-liveness')) return liveness();
      return Response.json({ error: 'run not found' }, { status: 404 });
    });
    const topology = createDoRunTopology(
      { idFromName: (name: string) => name, get: () => ({ fetch }) },
      DEPLOYMENT_IDENTITY_SECRET,
    );
    const outcome = await beginIdempotentStart(store, request, {
      persisted: (row) => topology.persistedStart(row.targetId, row.runId),
      live: (row) => topology.startLiveness(row.targetId, row.runId),
    }).catch((error: unknown) => error);
    const after = sqlite
      .prepare('SELECT * FROM flowsafe_start_idempotency')
      .all();
    return { claim, before, after, outcome, fetch };
  }

  it.each([
    true,
    false,
  ])('uses an explicit boolean %s from the addressed run', async (live) => {
    const h = await retry(async () => Response.json({ live }));
    expect(h.fetch).toHaveBeenCalledWith(
      'http://do/runs/workflow-1/retained-run/start-liveness',
      expect.objectContaining({ headers: expect.any(Object) }),
    );
    if (live) {
      expect(h.outcome).toMatchObject({
        reason: { code: 'IDEMPOTENT_START_PENDING' },
      });
      expect(h.claim).not.toHaveBeenCalled();
      expect(h.after).toEqual(h.before);
    } else {
      expect(h.outcome).toMatchObject({
        kind: 'start',
        reservation: { runId: 'retained-run' },
      });
      expect(h.claim).toHaveBeenCalledOnce();
      expect(h.after[0]).toMatchObject({ state: 'started', updated_at: 1_001 });
    }
  });

  const malformed: Array<[string, unknown]> = [
    ['missing', {}],
    ['null', null],
    ['primitive', false],
    ['array', Object.assign([], { live: false })],
    ['inherited', Object.create({ live: false })],
    ['undefined', { live: undefined }],
    ['null field', { live: null }],
    ['zero', { live: 0 }],
    ['empty string', { live: '' }],
    ['false string', { live: 'false' }],
    ['true string', { live: 'true' }],
  ];
  it.each(
    malformed.flatMap(([name, payload]) =>
      (['reserved', 'started'] as const).map((state) => ({
        name,
        payload,
        state,
      })),
    ),
  )('refuses $name liveness for a $state row without claiming', async ({
    payload,
    state,
  }) => {
    const h = await retry(async () => {
      const response = Response.json({});
      vi.spyOn(response, 'json').mockResolvedValue(payload);
      return response;
    }, state);
    expect(h.outcome).toBeInstanceOf(RunRouteError);
    expect(h.outcome).toMatchObject({
      status: 503,
      message: 'run start liveness is not readable',
    });
    expect(h.claim).not.toHaveBeenCalled();
    expect(h.after).toEqual(h.before);
  });

  it.each([
    201, 400, 404, 503,
  ])('refuses HTTP %s liveness without claiming', async (status) => {
    const h = await retry(async () =>
      Response.json(
        { live: false, error: 'private transport detail' },
        { status },
      ),
    );
    expect(h.outcome).toBeInstanceOf(RunRouteError);
    expect(h.outcome).toMatchObject({
      status: 503,
      message: 'run start liveness is not readable',
    });
    expect(h.claim).not.toHaveBeenCalled();
    expect(h.after).toEqual(h.before);
  });

  it('refuses a liveness accessor without invoking it or claiming', async () => {
    const live = vi.fn(() => false);
    const h = await retry(async () => {
      const response = Response.json({});
      vi.spyOn(response, 'json').mockResolvedValue(
        Object.defineProperty({}, 'live', { get: live }),
      );
      return response;
    });
    expect(live).not.toHaveBeenCalled();
    expect(h.claim).not.toHaveBeenCalled();
    expect(h.after).toEqual(h.before);
    expect(h.outcome).toBeInstanceOf(RunRouteError);
    expect(h.outcome).toMatchObject({
      status: 503,
      message: 'run start liveness is not readable',
    });
  });

  it('refuses invalid liveness JSON without claiming', async () => {
    const h = await retry(async () => new Response('{'));
    expect(h.outcome).toBeInstanceOf(RunRouteError);
    expect(h.outcome).toMatchObject({
      status: 503,
      message: 'run start liveness is not readable',
    });
    expect(h.claim).not.toHaveBeenCalled();
    expect(h.after).toEqual(h.before);
  });

  it('propagates a thrown liveness fetch without claiming', async () => {
    const failure = new Error('liveness fetch failed');
    const h = await retry(async () => {
      throw failure;
    });
    expect(h.outcome).toBe(failure);
    expect(h.claim).not.toHaveBeenCalled();
    expect(h.after).toEqual(h.before);
  });
});

describe('FS8 D3 protected replay workflow transport', () => {
  const execution = {
    tablePrefix: 'selected_',
    workflowId: 'workflow-1',
    runId: 'run-1',
    startToken: 'generation-1',
    owner: { kind: 'human', id: 'actor-1' },
    target: { kind: 'workflow', id: 'workflow-1' },
  };
  const value = {
    runId: 'run-1',
    status: 'success',
    result: { startToken: 'application-value' },
  };
  function topologyFor(payload: unknown, status = 200) {
    const fetch = vi.fn(async () => Response.json(payload, { status }));
    return {
      fetch,
      topology: createDoRunTopology(
        { idFromName: (name: string) => name, get: () => ({ fetch }) },
        DEPLOYMENT_IDENTITY_SECRET,
      ),
    };
  }

  it('uses the private authenticated scalar replay route and keeps initial distinct', async () => {
    const { topology, fetch } = topologyFor({ kind: 'initial', execution });
    await expect(
      topology.persistedStart('workflow-1', 'run-1'),
    ).resolves.toEqual({ kind: 'initial', execution });
    expect(fetch).toHaveBeenCalledWith(
      'http://do/runs/workflow-1/run-1?replay=1',
      expect.objectContaining({ headers: expect.any(Object) }),
    );
  });

  it('projects a nonpending result while preserving application payload', async () => {
    const { topology } = topologyFor({
      kind: 'result',
      execution,
      value: { ...value, unknownExtra: 'omit' },
      raw: 'private wrapper extra',
    });
    await expect(
      topology.persistedStart('workflow-1', 'run-1'),
    ).resolves.toEqual({ kind: 'result', execution, value });
  });

  it.each([
    ['missing discriminator', { execution, value }],
    ['initial with value', { kind: 'initial', execution, value }],
    ['missing result value', { kind: 'result', execution }],
    [
      'pending result',
      { kind: 'result', execution, value: { ...value, status: 'pending' } },
    ],
    [
      'foreign physical workflow',
      {
        kind: 'result',
        execution: { ...execution, workflowId: 'other' },
        value,
      },
    ],
    [
      'foreign logical target',
      {
        kind: 'result',
        execution: { ...execution, target: { kind: 'workflow', id: 'other' } },
        value,
      },
    ],
    [
      'foreign physical run',
      { kind: 'result', execution: { ...execution, runId: 'other' }, value },
    ],
    [
      'noncanonical prefix',
      {
        kind: 'result',
        execution: { ...execution, tablePrefix: 'SELECTED_' },
        value,
      },
    ],
    [
      'partial generation',
      {
        kind: 'result',
        execution: { ...execution, startToken: undefined },
        value,
      },
    ],
    [
      'invalid null prefix arm',
      {
        kind: 'result',
        execution: { ...execution, tablePrefix: undefined },
        value,
      },
    ],
    [
      'foreign result run',
      { kind: 'result', execution, value: { ...value, runId: 'other' } },
    ],
    [
      'unpaired requester',
      {
        kind: 'result',
        execution,
        value: { ...value, requestedBy: 'actor-1' },
      },
    ],
    [
      'invalid timing map',
      {
        kind: 'result',
        execution,
        value: { ...value, resumeCount: { gate: -1 } },
      },
    ],
    [
      'structural authority',
      {
        kind: 'result',
        execution,
        value: { ...value, startReservation: { token: 'private' } },
      },
    ],
  ])('refuses malformed successful private data: %s', async (_name, payload) => {
    const { topology, fetch } = topologyFor(payload);
    const outcome = await topology
      .persistedStart('workflow-1', 'run-1')
      .catch((error: unknown) => error);
    expect(fetch).toHaveBeenCalledOnce();
    expect(outcome).toMatchObject({
      status: 503,
      message: 'persisted start is not readable',
    });
    expect((outcome as Error).message).not.toContain(execution.startToken);
  });

  it('treats only an actual 404 as absence and preserves non-OK reason', async () => {
    await expect(
      topologyFor({ error: 'missing' }, 404).topology.persistedStart(
        'workflow-1',
        'run-1',
      ),
    ).resolves.toBeUndefined();
    await expect(
      topologyFor(
        { error: 'held', reason: { code: 'RUN_START_PENDING' } },
        503,
      ).topology.persistedStart('workflow-1', 'run-1'),
    ).rejects.toMatchObject({
      status: 503,
      message: 'held',
      reason: { code: 'RUN_START_PENDING' },
    });
    const badJson = createDoRunTopology(
      {
        idFromName: (name: string) => name,
        get: () => ({
          fetch: async () => new Response('bad-json', { status: 409 }),
        }),
      },
      DEPLOYMENT_IDENTITY_SECRET,
    );
    await expect(
      badJson.persistedStart('workflow-1', 'run-1'),
    ).rejects.toMatchObject({
      status: 409,
      message: 'run request failed with status 409',
    });
  });

  it('captures and sends the original winning claim before fetch waits', async () => {
    const { topology, requests } = harness();
    const claim = {
      key: 'key-1',
      state: 'started' as const,
      binding: { kind: 'unbound' as const },
      owner: { kind: 'human' as const, id: 'actor-1' },
      targetKind: 'workflow' as const,
      targetId: 'workflow-1',
      runId: 'run-1',
      createdAt: 7,
      updatedAt: 8,
    };
    await topology.start({
      workflowId: 'workflow-1',
      runId: 'run-1',
      inputData: {},
      principal: { kind: 'human', id: 'actor-1', role: 'operator' },
      idempotencyKey: 'key-1',
      startReservation: claim,
    });
    expect(JSON.parse(requests[0]?.init?.body ?? '').startReservation).toEqual(
      claim,
    );
  });
});
