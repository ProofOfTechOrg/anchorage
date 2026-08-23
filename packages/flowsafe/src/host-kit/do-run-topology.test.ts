// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it, vi } from 'vitest';

import { EXECUTION_PRINCIPAL_HEADER } from '../do-runner/index.js';
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
