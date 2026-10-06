// SPDX-License-Identifier: Apache-2.0
/// <reference types="@cloudflare/vitest-pool-workers/types" />

import { reset } from 'cloudflare:test';
import { env } from 'cloudflare:workers';
import type { WorkflowRunState } from '@mastra/core/workflows';
import { beforeEach, describe, expect, it } from 'vitest';

import { withDeepValue } from '../../test-support/deep-json.js';
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

const STORED_FORMS = [
  'readable',
  'nested past the depth SQLite parses',
] as const;

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

/**
 * Leaves the run row in the form a test needs. The deep form rewrites the
 * stored snapshot with a value nested past the depth SQLite parses, and
 * asserts that this runtime's SQLite cannot parse it, so a SQLite with a
 * higher depth limit fails the test instead of turning it into a readable-row
 * test.
 */
async function storeAs(form: (typeof STORED_FORMS)[number]): Promise<void> {
  if (form === 'readable') return;
  const [row] = await rows();
  await db()
    .prepare('UPDATE mastra_workflow_snapshot SET snapshot = ?1')
    .bind(JSON.stringify(withDeepValue(JSON.parse(String(row?.snapshot)))))
    .run();
  const stored = await db()
    .prepare(
      'SELECT json_valid(snapshot) AS valid FROM mastra_workflow_snapshot',
    )
    .all<{ valid: number }>();
  expect(stored.results).toEqual([{ valid: 0 }]);
}

describe('FencedWorkflowsStorageD1 on workerd D1', () => {
  beforeEach(reset);

  it.each(
    STORED_FORMS,
  )('refuses a stale write over a terminated row (stored %s) and admits its successor', async (form) => {
    // #given a run row settled as cancelled
    const { domain } = await fencedDomain();
    await persist(domain, snapshot('failed', TERMINATED));
    await storeAs(form);
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
  )('reports a terminated row settled and an unsettled row live to the liveness touch (stored %s)', async (form) => {
    const touchedAt = '2030-01-02T03:04:05.006Z';
    const touchedMs = Date.parse(touchedAt);

    // #given an unsettled row
    const { domain, capability } = await fencedDomain();
    await persist(domain, snapshot('running'));
    await storeAs(form);
    const [unsettled] = await rows();

    // #when a leg touches it
    const live = await capability.touchRun?.(ADDRESS, touchedMs);

    // #then the row is live and only its updatedAt moves
    expect(live).toBe('live');
    expect(await rows()).toEqual([{ ...unsettled, updatedAt: touchedAt }]);

    // #given the row is then settled as cancelled
    await persist(domain, snapshot('failed', TERMINATED));
    await storeAs(form);
    const settled = await rows();

    // #when a later touch arrives
    const outgoing = await capability.touchRun?.(ADDRESS, touchedMs + 1_000);

    // #then it reports the row settled and leaves it as it is
    expect(outgoing).toBe('settled');
    expect(await rows()).toEqual(settled);
  });

  it.each(
    STORED_FORMS,
  )('keeps a recorded intent under a stale write, and carries it into a write of the same revision (stored %s)', async (form) => {
    // #given an unsettled row whose lifecycle records a cancellation intent
    const intent = {
      status: 'cancelled',
      requestedAt: 50,
      replayPrincipals: [{ kind: 'human', id: 'Alice' }],
    };
    const recorded = { version: 1, revision: 3, transitionIntent: intent };
    const { domain } = await fencedDomain();
    await persist(domain, snapshot('running', recorded));
    await storeAs(form);

    // #when a leg's write serialized before the intent lands
    await persist(domain, {
      ...snapshot('running', { version: 1, revision: 2 }),
      result: { progress: 1 },
    });

    // #then the write's state is stored and the intent stays
    const [row] = await rows();
    expect(JSON.parse(String(row?.snapshot))).toMatchObject({
      result: { progress: 1 },
    });
    expect(await storedLifecycle()).toEqual(recorded);

    // #when the row is left in the same form again, and a write whose own
    // lifecycle reached revision 3 without the intent lands
    await storeAs(form);
    await persist(
      domain,
      snapshot('running', { version: 1, revision: 3, deadlineAt: 9 }),
    );

    // #then its lifecycle is stored with the intent added
    expect(await storedLifecycle()).toEqual({
      version: 1,
      revision: 3,
      deadlineAt: 9,
      transitionIntent: intent,
    });
  });

  it('refuses a snapshot nested past the depth SQLite parses, over a readable row and for a run with no row', async () => {
    // #given a run with no row, and a write nested past the depth SQLite parses
    const { domain } = await fencedDomain();
    const deep = withDeepValue(snapshot('running'));

    // #when the write persists for the run with no row
    const insert = persist(domain, deep);

    // #then it is refused as state that cannot be stored and no row is inserted
    await expect(insert).rejects.toMatchObject({
      name: 'RunStateNotStorableError',
      status: 422,
    });
    expect(await rows()).toEqual([]);

    // #given the run now has a readable row
    await persist(domain, snapshot('running'));
    const stored = await rows();
    expect(stored).toHaveLength(1);

    // #when the same write persists over it
    const update = persist(domain, deep);

    // #then it is refused and the row keeps its bytes
    await expect(update).rejects.toMatchObject({
      name: 'RunStateNotStorableError',
      status: 422,
    });
    expect(await rows()).toEqual(stored);
  });

  it('admits a successor nested past the depth over a row already stored too deep to parse', async () => {
    // #given a run row settled as cancelled and stored nested past the depth
    // SQLite parses
    const { domain } = await fencedDomain();
    await persist(domain, snapshot('failed', TERMINATED));
    await storeAs('nested past the depth SQLite parses');

    // #when the settlement's successor, itself nested past the depth, advances
    // the revision with the settlement intact
    const successor = withDeepValue(
      snapshot('failed', {
        ...TERMINATED,
        revision: 4,
        terminal: { ...TERMINATED.terminal, cleanupCompletedAt: 200 },
      }),
    );
    await persist(domain, successor);

    // #then it is stored as written
    const [row] = await rows();
    expect(row?.snapshot).toBe(JSON.stringify(successor));
  });
});
