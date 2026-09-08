// SPDX-License-Identifier: Apache-2.0
import { afterEach, describe, expect, it, vi } from 'vitest';

import type {
  ApprovalActor,
  SelfDecisionPolicy,
} from '../approval-api/index.js';
import {
  ApprovalAuthzError,
  InMemoryApprovalStoreFactory,
} from '../approval-api/index.js';
import type { ExecutionFenceStore } from '../do-runner/execution-fence.js';
import {
  buildHostApprovalService,
  maintenancePrincipal,
  runApprovalRetentionPurge,
  runSlaSweepMaintenance,
} from './host-approval-service.js';

const OPERATOR: ApprovalActor = {
  id: 'opal',
  role: 'operator',
};
const ADMIN: ApprovalActor = { id: 'ada', role: 'admin' };

describe('runApprovalRetentionPurge', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it.each([
    ['null-prototype rejection', Object.create(null), 'unreadable error'],
    [
      'throwing coercion',
      {
        [Symbol.toPrimitive]() {
          throw new Error('conversion failed');
        },
      },
      'unreadable error',
    ],
    ['large diagnostic', 'x'.repeat(300), 'x'.repeat(256)],
  ])('contains a %s with a bounded failure outcome', async (_name, failure, expected) => {
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {});
    const store = new InMemoryApprovalStoreFactory().store();
    vi.spyOn(store, 'purgeExpired').mockRejectedValue(failure);

    await expect(
      runApprovalRetentionPurge({
        store,
        retentionDays: undefined,
        trigger: 'purge',
      }),
    ).resolves.toEqual({
      ok: false,
      error: expected,
    });
    expect(logged).toHaveBeenCalledOnce();
    expect(JSON.parse(logged.mock.calls[0]?.[0] as string)).toEqual({
      type: 'maintenance-error',
      surface: 'approval-retention-purge',
      trigger: 'purge',
      error: expected,
    });
  });

  it('reports retention-duration overflow as a failed purge', async () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    const store = new InMemoryApprovalStoreFactory().store();

    const outcome = await runApprovalRetentionPurge({
      store,
      retentionDays: '1e303',
      trigger: 'purge',
    });

    expect(outcome).toEqual({
      ok: false,
      error: expect.stringContaining('TypeError'),
    });
    const logged = errorSpy.mock.calls
      .map(([line]) => String(line))
      .filter((line) => line.includes('maintenance-error'));
    expect(logged).toHaveLength(1);
    expect(JSON.parse(logged[0] ?? '{}')).toMatchObject({
      type: 'maintenance-error',
      surface: 'approval-retention-purge',
      trigger: 'purge',
      error: expect.stringContaining('TypeError'),
    });
  });

  it('purges through the real store on a sane retentionDays value', async () => {
    const factory = new InMemoryApprovalStoreFactory();
    const store = factory.store();
    await store.create({
      id: 'apr-retention-1',
      workflowId: 'wf',
      runId: 'acme_run-1',
      title: 'old decided approval',
      connectors: [],
      priority: 'normal',
      status: 'approved',
      createdAt: new Date(0).toISOString(),
      decidedAt: new Date(0).toISOString(),
      updatedAt: new Date(0).toISOString(),
    });

    const outcome = await runApprovalRetentionPurge({
      store: factory.store(),
      retentionDays: '0',
      trigger: 'purge',
    });

    expect(outcome).toEqual({ ok: true, value: 1 });
  });
});

describe('runSlaSweepMaintenance', () => {
  afterEach(() => vi.restoreAllMocks());

  async function overdueApprovals() {
    const store = new InMemoryApprovalStoreFactory().store();
    const past = new Date(0).toISOString();
    for (const id of ['apr-first', 'apr-second']) {
      await store.create({
        id,
        workflowId: 'wf',
        runId: id,
        title: 'overdue approval',
        connectors: [],
        priority: 'normal',
        status: 'pending',
        createdAt: past,
        updatedAt: past,
        slaDeadlineAt: past,
      });
    }
    return store;
  }

  describe.each([
    'synchronous throw',
    'asynchronous rejection',
  ])('%s', (mode) => {
    it.each([
      {
        name: 'ordinary Error',
        failure: () => new Error('hub unavailable'),
        diagnostic: 'hub unavailable',
        outcome: 'Error: hub unavailable',
      },
      {
        name: 'throwing message getter',
        failure: () =>
          Object.defineProperty(new Error(), 'message', {
            get() {
              throw new Error('message getter failed');
            },
          }),
        diagnostic: 'unreadable error',
        outcome: 'unreadable error',
      },
      {
        name: 'BigInt message',
        failure: () =>
          Object.defineProperty(new Error(), 'message', { value: 1n }),
        diagnostic: '1',
        outcome: 'Error: 1',
      },
      {
        name: 'null-prototype rejection',
        failure: () => Object.create(null),
        diagnostic: 'unreadable error',
        outcome: 'unreadable error',
      },
    ])('contains a stream failure with $name after durable escalation', async ({
      failure,
      diagnostic,
      outcome,
    }) => {
      const logged = vi.spyOn(console, 'error').mockImplementation(() => {});
      vi.spyOn(console, 'log').mockImplementation(() => {});
      const store = await overdueApprovals();
      const stream = vi.fn(() => {
        if (mode === 'synchronous throw') throw failure();
        return Promise.reject(failure());
      });

      await expect(
        runSlaSweepMaintenance({
          store,
          systemPrincipal: maintenancePrincipal('maintenance'),
          trigger: 'sweep',
          stream,
        }),
      ).resolves.toEqual({
        ok: false,
        error: `stream-publish: ${outcome}; stream-publish: ${outcome}`,
      });

      expect(stream).toHaveBeenCalledTimes(2);
      expect((await store.get('apr-first'))?.status).toBe('escalated');
      expect((await store.get('apr-second'))?.status).toBe('escalated');
      expect(
        logged.mock.calls.map(([line]) => JSON.parse(line as string)),
      ).toEqual([
        { type: 'stream-publish-error', reason: diagnostic },
        { type: 'stream-publish-error', reason: diagnostic },
      ]);
    });
  });

  it.each([
    'absent',
    'synchronous',
    'asynchronous',
  ])('reports successful maintenance for %s stream handling', async (mode) => {
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.spyOn(console, 'log').mockImplementation(() => {});
    const store = await overdueApprovals();
    const stream = vi.fn(() =>
      mode === 'asynchronous' ? Promise.resolve() : undefined,
    );

    await expect(
      runSlaSweepMaintenance({
        store,
        systemPrincipal: maintenancePrincipal('maintenance'),
        trigger: 'sweep',
        ...(mode === 'absent' ? {} : { stream }),
      }),
    ).resolves.toEqual({ ok: true, value: undefined });

    expect(stream).toHaveBeenCalledTimes(mode === 'absent' ? 0 : 2);
    expect((await store.get('apr-first'))?.status).toBe('escalated');
    expect((await store.get('apr-second'))?.status).toBe('escalated');
    expect(logged).not.toHaveBeenCalled();
  });

  it('contains an unprintable SLA-store failure as a maintenance outcome', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.spyOn(console, 'log').mockImplementation(() => {});
    const store = new InMemoryApprovalStoreFactory().store();
    vi.spyOn(store, 'list').mockRejectedValue(Object.create(null));
    await expect(
      runSlaSweepMaintenance({
        store,
        systemPrincipal: maintenancePrincipal('maintenance'),
        trigger: 'sweep',
      }),
    ).resolves.toEqual({ ok: false, error: 'unreadable error' });
  });
});

describe('buildHostApprovalService allowSelfDecision passthrough', () => {
  function buildService(allowSelfDecision?: SelfDecisionPolicy) {
    const store = new InMemoryApprovalStoreFactory().store();
    return buildHostApprovalService(store, {
      systemPrincipalId: 'flowsafe-system',
      // A benign resume topology: decide() calls #resume on approve, and a
      // non-'suspended' summary means resumeRunWithRequeue queues nothing.
      resumeRun: async (record) => ({ runId: record.runId, status: 'success' }),
      allowSelfDecision,
      // In-memory approval store — no database, nothing to fence.
      executionFence: 'none',
    });
  }

  async function adminRequestedRecordId(
    service: ReturnType<typeof buildService>,
  ): Promise<string> {
    const { record } = await service.create(
      {
        workflowId: 'wf',
        runId: 'acme_run-1',
        title: 'self-request',
        requestedBy: ADMIN.id,
        requestedByKind: 'human',
      },
      OPERATOR,
    );
    return record.id;
  }

  it('forwards a role-scoped exemption so admin can self-decide', async () => {
    // #given
    const service = buildService({ roles: ['admin'] });
    const id = await adminRequestedRecordId(service);

    // #when
    const result = await service.decide(id, { decision: 'approve' }, ADMIN);

    // #then
    expect(result.record.status).toBe('approved');
  });

  it('defaults to SoD ON when allowSelfDecision is unset', async () => {
    // #given
    const service = buildService();
    const id = await adminRequestedRecordId(service);

    // #when / #then — the requester (admin) is refused their own request
    await expect(
      service.decide(id, { decision: 'approve' }, ADMIN),
    ).rejects.toBeInstanceOf(ApprovalAuthzError);
  });
});

describe('FS8 D3 proof activation host approval namespace', () => {
  it('forwards the explicit namespace to the deciding service without inferring an omitted prefix', async () => {
    const execution = {
      tablePrefix: 'proof_',
      workflowId: 'wf',
      runId: 'run',
      startToken: 'generation',
    };
    const readCurrentRunExecution = vi.fn(async () => execution);
    const fence = {
      read: async () => ({
        state: 'proof-only',
        proofKey: 'key',
        proofRunId: 'run',
        proofExecution: execution,
      }),
      readCurrentRunExecution,
    } as unknown as ExecutionFenceStore;
    for (const workflowTablePrefix of [undefined, 'PROOF_']) {
      const store = new InMemoryApprovalStoreFactory().store();
      const service = buildHostApprovalService(store, {
        systemPrincipalId: 'system',
        executionFence: fence,
        workflowTablePrefix,
        resumeRun: async () => ({ runId: 'run', status: 'success' }),
      });
      const { record } = await service.create(
        { workflowId: 'wf', runId: 'run', title: 'proof' },
        OPERATOR,
      );
      const result = await service
        .decide(record.id, { decision: 'approve' }, ADMIN)
        .catch((error) => error);
      expect((await store.get(record.id))?.status).toBe(
        workflowTablePrefix === undefined ? 'pending' : 'approved',
      );
      if (workflowTablePrefix !== undefined) {
        expect(result).toMatchObject({ record: { status: 'approved' } });
        expect(readCurrentRunExecution).toHaveBeenCalledWith({
          tablePrefix: 'proof_',
          workflowId: 'wf',
          runId: 'run',
        });
      }
    }
  });
});
