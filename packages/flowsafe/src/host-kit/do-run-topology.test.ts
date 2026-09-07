// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it, vi } from 'vitest';

import {
  EXECUTION_PRINCIPAL_HEADER,
  InvalidMutationEpochError,
  MUTATION_EPOCH_HEADER,
  MutationEpochMismatchError,
} from '../do-runner/index.js';
import {
  createDoRunTopology,
  type DoRunLifecycleTopology,
  type DoRunTopology,
  type RunnerNamespaceLike,
} from './do-run-topology.js';

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
    });
  });

  it('sends only exact trigger addressing for scheduled starts', async () => {
    const { topology, requests } = harness();

    await topology.start({
      workflowId: 'workflow-1',
      runId: 'run-1',
      inputData: { forged: true },
      initialState: { forged: true },
      principal: {
        kind: 'service',
        id: 'scheduler',
        purpose: 'schedule.fire',
      },
      scheduleId: 'schedule-1',
      dispatchId: 'dispatch-1',
    });

    expect(JSON.parse(requests[0]?.init?.body ?? '')).toEqual({
      workflowId: 'workflow-1',
      runId: 'run-1',
      scheduleId: 'schedule-1',
      dispatchId: 'dispatch-1',
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
