// SPDX-License-Identifier: Apache-2.0
/// <reference types="@cloudflare/vitest-pool-workers/types" />

import { reset } from 'cloudflare:test';
import { env } from 'cloudflare:workers';
import type { WorkflowRunState } from '@mastra/core/workflows';
import { beforeEach, describe, expect, it } from 'vitest';

import { FENCED_WORKFLOW_STORAGE } from './fenced-workflow-capability.js';
import { FencedWorkflowsStorageD1 } from './fenced-workflows-d1.js';
import { RUN_LIFECYCLE_CONTEXT_KEY } from './run-lifecycle.js';

interface TestBindings {
  DB: D1Database;
}

const db = (): D1Database => (env as unknown as TestBindings).DB;

const ADDRESS = { workflowId: 'workflow', runId: 'run' };

const TERMINATED = {
  version: 1,
  revision: 3,
  terminal: {
    status: 'cancelled',
    error: { code: 'CANCELLED', message: 'run was cancelled' },
    transitionedAt: 100,
    replayPrincipals: [{ kind: 'human', id: 'Alice' }],
  },
};

const STORED_FORMS = ['readable'] as const;

async function fencedDomain() {
  const domain = new FencedWorkflowsStorageD1({ binding: db() as never });
  await domain.init();
  const capability = domain[FENCED_WORKFLOW_STORAGE];
  if (!capability) throw new Error('fenced workflow capability missing');
  return { domain, capability };
}

function snapshot(
  status: WorkflowRunState['status'],
  lifecycle?: Record<string, unknown>,
): WorkflowRunState {
  return {
    runId: 'run',
    status,
    value: {},
    context: {},
    serializedStepGraph: [],
    activePaths: [],
    activeStepsPath: {},
    suspendedPaths: {},
    resumeLabels: {},
    waitingPaths: {},
    timestamp: 123,
    ...(lifecycle
      ? { requestContext: { [RUN_LIFECYCLE_CONTEXT_KEY]: lifecycle } }
      : {}),
  };
}

function persist(
  domain: FencedWorkflowsStorageD1,
  value: WorkflowRunState,
): Promise<void> {
  return domain.persistWorkflowSnapshot({
    workflowName: ADDRESS.workflowId,
    runId: ADDRESS.runId,
    snapshot: value,
  });
}

async function rows(): Promise<Record<string, unknown>[]> {
  const { results } = await db()
    .prepare('SELECT * FROM mastra_workflow_snapshot')
    .all<Record<string, unknown>>();
  return results;
}

async function storedLifecycle(): Promise<unknown> {
  const [row] = await rows();
  return JSON.parse(String(row?.snapshot)).requestContext?.[
    RUN_LIFECYCLE_CONTEXT_KEY
  ];
}

describe('FencedWorkflowsStorageD1 on workerd D1', () => {
  beforeEach(reset);

  it.each(
    STORED_FORMS,
  )('refuses a stale write over a terminated row (stored %s) and admits its successor', async () => {
    // #given a run row settled as cancelled
    const { domain } = await fencedDomain();
    await persist(domain, snapshot('failed', TERMINATED));
    const settled = await rows();
    expect(settled).toHaveLength(1);

    // #when a leg that never saw the cancellation writes its running state
    const stale = persist(domain, snapshot('running'));

    // #then the write is refused and the row keeps its bytes
    await expect(stale).rejects.toMatchObject({
      name: 'RunSettledConflictError',
    });
    expect(await rows()).toEqual(settled);

    // #when the settlement's successor advances the revision with the
    // settlement intact
    const successor = {
      ...TERMINATED,
      revision: 4,
      terminal: { ...TERMINATED.terminal, cleanupCompletedAt: 200 },
    };
    await persist(domain, snapshot('failed', successor));

    // #then it is stored
    expect(await storedLifecycle()).toEqual(successor);
  });

  it.each(
    STORED_FORMS,
  )('reports a terminated row settled and an unsettled row live to the liveness touch (stored %s)', async () => {
    const touchedAt = '2030-01-02T03:04:05.006Z';
    const touchedMs = Date.parse(touchedAt);

    // #given an unsettled row
    const { domain, capability } = await fencedDomain();
    await persist(domain, snapshot('running'));
    const [unsettled] = await rows();

    // #when a leg touches it
    const live = await capability.touchRun?.(ADDRESS, touchedMs);

    // #then the row is live and only its updatedAt moves
    expect(live).toBe('live');
    expect(await rows()).toEqual([{ ...unsettled, updatedAt: touchedAt }]);

    // #given the row is then settled as cancelled
    await persist(domain, snapshot('failed', TERMINATED));
    const settled = await rows();

    // #when a later touch arrives
    const outgoing = await capability.touchRun?.(ADDRESS, touchedMs + 1_000);

    // #then it reports the row settled and leaves it as it is
    expect(outgoing).toBe('settled');
    expect(await rows()).toEqual(settled);
  });
});
