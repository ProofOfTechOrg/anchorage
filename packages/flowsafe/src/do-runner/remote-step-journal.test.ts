// SPDX-License-Identifier: Apache-2.0

import { afterEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod';
import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';
import { createD1Storage } from './d1-storage.js';
import { ExecutionFenceStore } from './execution-fence.js';
import { init } from './init.js';
import { DeploymentInventory } from './inventory.js';
import {
  REMOTE_STEP_MARKER,
  REMOTE_STEP_TABLE,
  RemoteStepJournal,
} from './remote-step-journal.js';
import type { RunnerRuntime, RunSummary } from './runtime.js';

const ADDRESS = {
  deploymentId: 'tenant-1',
  tablePrefix: '',
  workflowId: 'durable-agentic-loop',
  agentId: 'writer',
  threadId: 'thread-1',
  resourceId: 'thread-1',
  runId: 'run-1',
};
const PRINCIPAL = {
  kind: 'human',
  id: 'operator-1',
  role: 'operator',
} as const;

describe('remote step journal dispatch admission', () => {
  const databases: Array<{ close(): void }> = [];

  afterEach(() => {
    for (const database of databases.splice(0)) database.close();
  });

  async function fixture() {
    const sqlite = openSqlite() as ReturnType<typeof openSqlite> & {
      close(): void;
    };
    databases.push(sqlite);
    const db = sqliteUnitDatabase(sqlite);
    sqlite.exec(`CREATE TABLE mastra_workflow_snapshot (
      workflow_name TEXT NOT NULL, run_id TEXT NOT NULL,
      snapshot TEXT NOT NULL, PRIMARY KEY (workflow_name, run_id))`);
    sqlite
      .prepare(`INSERT INTO mastra_workflow_snapshot VALUES (?, ?, ?)`)
      .run(ADDRESS.workflowId, ADDRESS.runId, '{"status":"suspended"}');
    const fence = new ExecutionFenceStore(db);
    await fence.seed('open');
    let now = 1_000;
    let summary: RunSummary = {
      runId: ADDRESS.runId,
      status: 'suspended',
      suspended: [['durable-agentic-execution']],
      suspendedAt: { 'durable-agentic-execution': 500 },
      suspendPayload: {
        'durable-agentic-execution': {
          type: 'suspension',
          toolCallId: 'call-1',
          toolName: 'remote_lookup',
          toolCallSuspended: {
            remoteStep: REMOTE_STEP_MARKER,
            request: { account: 'acme' },
          },
        },
      },
    };
    const runtime = {
      authoritativeStatus: async () => summary,
      authoritativeStartState: async () => ({
        storage: 'd1',
        snapshot: JSON.parse(
          (
            sqlite
              .prepare('SELECT snapshot FROM mastra_workflow_snapshot')
              .get() as { snapshot: string }
          ).snapshot,
        ),
        execution: {
          tablePrefix: '',
          workflowId: ADDRESS.workflowId,
          runId: ADDRESS.runId,
          startToken: 'start-token',
        },
        provenance: {
          startIdentity: {
            owner: { kind: PRINCIPAL.kind, id: PRINCIPAL.id },
            target: {
              kind: 'agent',
              id: ADDRESS.agentId,
              threadId: ADDRESS.threadId,
            },
          },
        },
      }),
    } as unknown as Pick<
      RunnerRuntime,
      'authoritativeStatus' | 'authoritativeStartState'
    >;
    const verifyCompletion = vi.fn(async () => undefined);
    const journal = new RemoteStepJournal({
      db,
      fence,
      runtime,
      deploymentId: ADDRESS.deploymentId,
      tablePrefix: ADDRESS.tablePrefix,
      allowedToolIds: ['remote_lookup'],
      verifyCompletion,
      now: () => now,
    });
    await journal.initialize();
    return {
      db,
      fence,
      journal,
      verifyCompletion,
      runtime,
      reopen: () =>
        new RemoteStepJournal({
          db,
          fence,
          runtime,
          deploymentId: ADDRESS.deploymentId,
          tablePrefix: ADDRESS.tablePrefix,
          allowedToolIds: ['remote_lookup'],
          verifyCompletion,
          now: () => now,
        }),
      sqlite,
      setNow: (value: number) => {
        now = value;
      },
      setSummary: (value: RunSummary) => {
        summary = value;
      },
      setRequest: (account: string) => {
        summary = {
          ...summary,
          suspendPayload: {
            'durable-agentic-execution': {
              type: 'suspension',
              toolCallId: 'call-1',
              toolName: 'remote_lookup',
              toolCallSuspended: {
                remoteStep: REMOTE_STEP_MARKER,
                request: { account },
              },
            },
          },
        };
        sqlite
          .prepare('UPDATE mastra_workflow_snapshot SET snapshot = ?')
          .run(JSON.stringify({ status: 'suspended', request: { account } }));
      },
    };
  }

  it('converges repeated observation on one invocation without a permit', async () => {
    const h = await fixture();
    const [first, replay] = await Promise.all([
      h.journal.register(ADDRESS, 10_000, PRINCIPAL),
      h.journal.register(ADDRESS, 10_000, PRINCIPAL),
    ]);
    expect(replay.invocationId).toBe(first.invocationId);
    expect(first).toMatchObject({
      status: 'pending',
      permitGeneration: 0,
      request: { account: 'acme' },
      stepPath: ['durable-agentic-execution'],
      suspendedAt: 500,
    });
    expect(
      h.sqlite
        .prepare(`SELECT count(*) AS count FROM ${REMOTE_STEP_TABLE}`)
        .get(),
    ).toMatchObject({ count: 1 });
    expect(
      (await new DeploymentInventory(h.db).read('remote-steps')).entries,
    ).toEqual([
      {
        key: [first.invocationId],
        detail: expect.objectContaining({
          status: 'pending',
          run_id: ADDRESS.runId,
        }),
      },
    ]);
  });

  it('binds registration to the original execution principal', async () => {
    const h = await fixture();
    const invocation = await h.journal.register(ADDRESS, 10_000, PRINCIPAL);
    expect(invocation.principal).toEqual(PRINCIPAL);
    await expect(
      h.journal.register(ADDRESS, 10_000, {
        kind: 'human',
        id: 'other',
        role: 'admin',
      }),
    ).rejects.toMatchObject({ status: 409 });
  });

  it('returns raw effect authority only to the winning consume call', async () => {
    const h = await fixture();
    const invocation = await h.journal.register(ADDRESS, 10_000, PRINCIPAL);
    const permit = await h.journal.issuePermit(invocation.invocationId, 1_000);
    expect(permit).toMatchObject({ generation: 1, expiresAt: 2_000 });
    const winner = await h.journal.consumePermit(permit);
    expect(winner).toMatchObject({
      kind: 'accepted',
      rawPermit: expect.stringMatching(/^[A-Za-z0-9_-]{43}$/),
    });
    expect(await h.journal.consumePermit(permit)).toEqual({
      kind: 'already-consumed',
    });
    const stored = h.sqlite
      .prepare(`SELECT permit_digest FROM ${REMOTE_STEP_TABLE}`)
      .get() as { permit_digest: string };
    expect(stored.permit_digest).toMatch(/^[a-f0-9]{64}$/);
    expect(JSON.stringify(stored)).not.toContain(
      (winner as { rawPermit: string }).rawPermit,
    );
    await expect(
      h.journal.issuePermit(invocation.invocationId, 1_000),
    ).rejects.toMatchObject({ status: 409 });
    expect(
      (await h.journal.get(invocation.invocationId))?.permitConsumedAt,
    ).toBeDefined();
  });

  it('recovers one permit after a journal instance is replaced', async () => {
    const h = await fixture();
    const invocation = await h.journal.register(ADDRESS, 10_000, PRINCIPAL);
    const permit = await h.journal.issuePermit(invocation.invocationId, 1_000);
    const recovered = h.reopen();
    expect(await recovered.issuePermit(invocation.invocationId, 1_000)).toEqual(
      permit,
    );
    expect(await recovered.consumePermit(permit)).toMatchObject({
      kind: 'accepted',
      rawPermit: expect.any(String),
    });
    expect(await h.reopen().consumePermit(permit)).toEqual({
      kind: 'already-consumed',
    });
  });

  it('stores one verified JSON result and replays only the same bytes', async () => {
    const h = await fixture();
    const invocation = await h.journal.register(ADDRESS, 10_000, PRINCIPAL);
    const permit = await h.journal.issuePermit(invocation.invocationId, 1_000);
    const consumed = await h.journal.consumePermit(permit);
    if (consumed.kind !== 'accepted') throw new Error('permit was not won');
    const bytes = '{ "value": "done" }';
    const evidence = '{"receipt":"verified-completion"}';
    expect(
      await h.journal.recordResult(
        invocation.invocationId,
        consumed.rawPermit,
        bytes,
        evidence,
      ),
    ).toMatchObject({ kind: 'recorded', resultJson: bytes });
    expect(h.verifyCompletion).toHaveBeenCalledOnce();
    expect(h.verifyCompletion).toHaveBeenCalledWith(
      expect.objectContaining({
        invocation: expect.objectContaining({
          invocationId: invocation.invocationId,
        }),
        rawPermit: consumed.rawPermit,
        resultJson: bytes,
        evidenceJson: evidence,
      }),
    );
    expect(
      await h
        .reopen()
        .recordResult(
          invocation.invocationId,
          consumed.rawPermit,
          bytes,
          evidence,
        ),
    ).toMatchObject({ kind: 'replay', resultJson: bytes });
    expect(h.verifyCompletion).toHaveBeenCalledOnce();
    await expect(
      h.journal.recordResult(
        invocation.invocationId,
        consumed.rawPermit,
        '{"value":"done"}',
        evidence,
      ),
    ).rejects.toMatchObject({ status: 409 });
    await expect(
      h.journal.recordResult(
        invocation.invocationId,
        consumed.rawPermit,
        bytes,
        '{"receipt":"different"}',
      ),
    ).rejects.toMatchObject({ status: 409 });
    const stored = h.sqlite
      .prepare(
        `SELECT result_json, result_digest, executor_quiescent_at
         FROM ${REMOTE_STEP_TABLE}`,
      )
      .get() as Record<string, unknown>;
    expect(stored).toMatchObject({
      result_json: bytes,
      result_digest: expect.stringMatching(/^[a-f0-9]{64}$/),
      executor_quiescent_at: expect.any(Number),
    });
    expect(
      await h.journal.readRecordedResult(invocation.invocationId),
    ).toMatchObject({
      resultJson: bytes,
      resultDigest: stored.result_digest,
    });
  });

  it('settles a verified result only after the physical run advances', async () => {
    const h = await fixture();
    const invocation = await h.journal.register(ADDRESS, 10_000, PRINCIPAL);
    const permit = await h.journal.issuePermit(invocation.invocationId, 1_000);
    const consumed = await h.journal.consumePermit(permit);
    if (consumed.kind !== 'accepted') throw new Error('permit was not won');
    await h.journal.recordResult(
      invocation.invocationId,
      consumed.rawPermit,
      '{"value":"done"}',
      '{"receipt":"verified"}',
    );
    await expect(
      h.journal.settleAfterResume(invocation.invocationId),
    ).rejects.toMatchObject({ status: 409 });
    h.setSummary({ runId: ADDRESS.runId, status: 'success' });
    h.sqlite
      .prepare('UPDATE mastra_workflow_snapshot SET snapshot = ?')
      .run('{"status":"success"}');
    expect(
      await h.journal.settleAfterResume(invocation.invocationId),
    ).toMatchObject({
      status: 'settled',
    });
    expect(
      (await h.reopen().settleAfterResume(invocation.invocationId)).status,
    ).toBe('settled');
    expect(
      (await new DeploymentInventory(h.db).read('remote-steps')).entries,
    ).toEqual([]);
  });

  it('records verified completion from a prior epoch without resuming a locked run', async () => {
    const h = await fixture();
    const invocation = await h.journal.register(ADDRESS, 10_000, PRINCIPAL);
    const permit = await h.journal.issuePermit(invocation.invocationId, 1_000);
    const consumed = await h.journal.consumePermit(permit);
    if (consumed.kind !== 'accepted') throw new Error('permit was not won');
    const before = await h.fence.read();
    await h.fence.transition({
      expected: 'open',
      next: 'migration-locked',
      expectedMutationEpoch: before.mutationEpoch,
      expectedRevision: before.transitionRevision,
      advanceMutationEpoch: true,
    });
    expect(
      await h.journal.recordResult(
        invocation.invocationId,
        consumed.rawPermit,
        '{"value":"done"}',
        '{"receipt":"verified"}',
      ),
    ).toMatchObject({ kind: 'recorded' });
    await expect(
      h.journal.settleAfterResume(invocation.invocationId),
    ).rejects.toMatchObject({ status: 409 });
    expect(
      (await new DeploymentInventory(h.db).read('remote-steps')).count,
    ).toBe(1);
  });

  it('keeps a consumed claim active if completion verification fails', async () => {
    const h = await fixture();
    const invocation = await h.journal.register(ADDRESS, 10_000, PRINCIPAL);
    const permit = await h.journal.issuePermit(invocation.invocationId, 1_000);
    const consumed = await h.journal.consumePermit(permit);
    if (consumed.kind !== 'accepted') throw new Error('permit was not won');
    h.verifyCompletion.mockRejectedValueOnce(new Error('unverified executor'));
    await expect(
      h.journal.recordResult(
        invocation.invocationId,
        consumed.rawPermit,
        '{"value":"done"}',
        '{"receipt":"unverified"}',
      ),
    ).rejects.toThrow('unverified executor');
    expect((await h.journal.get(invocation.invocationId))?.status).toBe(
      'dispatched',
    );
    expect(
      (await new DeploymentInventory(h.db).read('remote-steps')).count,
    ).toBe(1);
  });

  it('refuses a wrong raw permit before calling the completion verifier', async () => {
    const h = await fixture();
    const invocation = await h.journal.register(ADDRESS, 10_000, PRINCIPAL);
    const permit = await h.journal.issuePermit(invocation.invocationId, 1_000);
    const consumed = await h.journal.consumePermit(permit);
    if (consumed.kind !== 'accepted') throw new Error('permit was not won');
    const wrong =
      consumed.rawPermit === 'A'.repeat(43) ? 'B'.repeat(43) : 'A'.repeat(43);
    await expect(
      h.journal.recordResult(
        invocation.invocationId,
        wrong,
        '{"value":"done"}',
        '{"receipt":"verified"}',
      ),
    ).rejects.toMatchObject({ status: 403 });
    expect(h.verifyCompletion).not.toHaveBeenCalled();
    expect((await h.journal.get(invocation.invocationId))?.status).toBe(
      'dispatched',
    );
  });

  it.each([
    'issue',
    'consume',
  ] as const)('refuses %s when the stored suspension request changes', async (operation) => {
    const h = await fixture();
    const invocation = await h.journal.register(ADDRESS, 10_000, PRINCIPAL);
    const permit =
      operation === 'consume'
        ? await h.journal.issuePermit(invocation.invocationId, 1_000)
        : undefined;
    h.setRequest('other');
    await expect(
      permit
        ? h.journal.consumePermit(permit)
        : h.journal.issuePermit(invocation.invocationId, 1_000),
    ).rejects.toMatchObject({ status: 409 });
  });

  it('tightens a repeated registration to the earlier host deadline', async () => {
    const h = await fixture();
    const first = await h.journal.register(ADDRESS, 10_000, PRINCIPAL);
    const repeated = await h.journal.register(ADDRESS, 1_500, PRINCIPAL);
    expect(repeated.invocationId).toBe(first.invocationId);
    expect(repeated.deadlineAt).toBe(1_500);
    h.setNow(1_501);
    await expect(
      h.journal.issuePermit(first.invocationId, 1_000),
    ).rejects.toMatchObject({ status: 409 });
  });

  it('caps a permit when the host deadline tightens after preflight', async () => {
    const h = await fixture();
    const invocation = await h.journal.register(ADDRESS, 10_000, PRINCIPAL);
    const prepare = h.db.prepare.bind(h.db);
    h.db.prepare = (sql) => {
      const statement = prepare(sql);
      if (!sql.includes("status = 'dispatched', permit_generation"))
        return statement;
      return {
        ...statement,
        bind: (...values) => {
          const bound = statement.bind(...values);
          return {
            ...bound,
            all: async <T = unknown>() => {
              await h.journal.register(ADDRESS, 1_500, PRINCIPAL);
              return bound.all<T>();
            },
          };
        },
      };
    };
    const permit = await h.journal.issuePermit(invocation.invocationId, 1_000);
    expect(permit.expiresAt).toBe(1_500);
    h.setNow(1_501);
    await expect(h.journal.consumePermit(permit)).rejects.toMatchObject({
      status: 409,
    });
  });

  it.each([
    'issue',
    'consume',
  ] as const)('refuses %s while a cancellation intent is persisted', async (operation) => {
    const h = await fixture();
    const invocation = await h.journal.register(ADDRESS, 10_000, PRINCIPAL);
    const permit =
      operation === 'consume'
        ? await h.journal.issuePermit(invocation.invocationId, 1_000)
        : undefined;
    h.sqlite.prepare('UPDATE mastra_workflow_snapshot SET snapshot = ?').run(
      JSON.stringify({
        status: 'suspended',
        requestContext: {
          'flowsafe.runLifecycle': {
            version: 1,
            revision: 2,
            transitionIntent: {
              status: 'cancelled',
              requestedAt: 1_000,
              replayPrincipals: [{ kind: 'human', id: 'operator-1' }],
            },
          },
        },
      }),
    );
    await expect(
      permit
        ? h.journal.consumePermit(permit)
        : h.journal.issuePermit(invocation.invocationId, 1_000),
    ).rejects.toMatchObject({ status: 409 });
  });

  it('rejects a remote request larger than its stored payload limit', async () => {
    const h = await fixture();
    h.setSummary({
      runId: ADDRESS.runId,
      status: 'suspended',
      suspended: [['durable-agentic-execution']],
      suspendedAt: { 'durable-agentic-execution': 500 },
      suspendPayload: {
        'durable-agentic-execution': {
          type: 'suspension',
          toolCallId: 'call-1',
          toolName: 'remote_lookup',
          toolCallSuspended: {
            remoteStep: REMOTE_STEP_MARKER,
            request: 'x'.repeat(16_384),
          },
        },
      },
    });
    await expect(
      h.journal.register(ADDRESS, 10_000, PRINCIPAL),
    ).rejects.toMatchObject({
      status: 400,
    });
  });

  it('refuses a marker from a tool the host did not designate', async () => {
    const h = await fixture();
    const restricted = new RemoteStepJournal({
      db: h.db,
      fence: h.fence,
      runtime: h.runtime,
      deploymentId: ADDRESS.deploymentId,
      tablePrefix: ADDRESS.tablePrefix,
      allowedToolIds: ['other_tool'],
      verifyCompletion: h.verifyCompletion,
    });
    await expect(
      restricted.register(ADDRESS, 10_000, PRINCIPAL),
    ).rejects.toMatchObject({
      status: 503,
    });
  });

  it('refuses a corrupted persisted table identity before building a query', async () => {
    const h = await fixture();
    const invocation = await h.journal.register(ADDRESS, 10_000, PRINCIPAL);
    h.sqlite
      .prepare(`UPDATE ${REMOTE_STEP_TABLE} SET table_prefix = ?`)
      .run('bad"_prefix');
    await expect(
      h.journal.issuePermit(invocation.invocationId, 1_000),
    ).rejects.toMatchObject({
      status: 503,
    });
  });

  it('refuses an unconsumed permit after the fence moves to migration lock', async () => {
    const h = await fixture();
    const invocation = await h.journal.register(ADDRESS, 10_000, PRINCIPAL);
    const permit = await h.journal.issuePermit(invocation.invocationId, 1_000);
    const before = await h.fence.read();
    await h.fence.transition({
      expected: 'open',
      next: 'migration-locked',
      expectedMutationEpoch: before.mutationEpoch,
      expectedRevision: before.transitionRevision,
      advanceMutationEpoch: true,
    });
    await expect(h.journal.consumePermit(permit)).rejects.toMatchObject({
      status: 503,
    });
    expect(
      (await h.journal.get(invocation.invocationId))?.permitConsumedAt,
    ).toBeUndefined();
  });

  it('refuses a permit when the fence moves after preflight but before the write', async () => {
    const h = await fixture();
    const invocation = await h.journal.register(ADDRESS, 10_000, PRINCIPAL);
    const permit = await h.journal.issuePermit(invocation.invocationId, 1_000);
    const prepare = h.db.prepare.bind(h.db);
    h.db.prepare = (sql) => {
      const statement = prepare(sql);
      if (!sql.includes('SET permit_consumed_at = ?')) return statement;
      return {
        ...statement,
        bind: (...values) => {
          const bound = statement.bind(...values);
          return {
            ...bound,
            all: async <T = unknown>() => {
              const before = await h.fence.read();
              await h.fence.transition({
                expected: 'open',
                next: 'migration-locked',
                expectedMutationEpoch: before.mutationEpoch,
                expectedRevision: before.transitionRevision,
                advanceMutationEpoch: true,
              });
              return bound.all<T>();
            },
          };
        },
      };
    };
    await expect(h.journal.consumePermit(permit)).rejects.toMatchObject({
      status: 409,
    });
    expect(
      (await h.journal.get(invocation.invocationId))?.permitConsumedAt,
    ).toBeUndefined();
  });

  it('refuses dispatch after cancellation or deadline', async () => {
    const h = await fixture();
    const invocation = await h.journal.register(ADDRESS, 10_000, PRINCIPAL);
    const permit = await h.journal.issuePermit(invocation.invocationId, 1_000);
    h.setSummary({ runId: ADDRESS.runId, status: 'cancelled' });
    h.sqlite
      .prepare(`UPDATE mastra_workflow_snapshot SET snapshot = ?`)
      .run('{"status":"cancelled"}');
    await expect(h.journal.consumePermit(permit)).rejects.toMatchObject({
      status: 409,
    });
    h.setNow(10_001);
    await expect(
      h.journal.issuePermit(invocation.invocationId, 1_000),
    ).rejects.toMatchObject({
      status: 409,
    });
  });

  it('refuses a real suspended snapshot after runtime persists cancellation intent', async () => {
    const sqlite = openSqlite() as ReturnType<typeof openSqlite> & {
      close(): void;
    };
    databases.push(sqlite);
    const db = sqliteUnitDatabase(sqlite);
    const storage = createD1Storage({ binding: db });
    await storage.init();
    const fence = new ExecutionFenceStore(db);
    await fence.seed('open');
    const { createWorkflow, createStep, runtime } = init(
      { storage },
      { executionFence: fence, startIdempotency: 'none' },
    );
    const gate = createStep({
      id: 'durable-agentic-execution',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      suspendSchema: z.object({
        type: z.literal('suspension'),
        toolCallId: z.string(),
        toolName: z.string(),
        toolCallSuspended: z.object({
          remoteStep: z.literal(REMOTE_STEP_MARKER),
          request: z.object({ account: z.string() }),
        }),
      }),
      execute: async ({ suspend }) =>
        suspend({
          type: 'suspension',
          toolCallId: 'call-1',
          toolName: 'remote_lookup',
          toolCallSuspended: {
            remoteStep: REMOTE_STEP_MARKER,
            request: { account: 'acme' },
          },
        }),
    });
    createWorkflow({
      id: ADDRESS.workflowId,
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(gate)
      .commit();
    const started = await runtime.start(ADDRESS.workflowId, {
      runId: ADDRESS.runId,
      inputData: {},
      requestedBy: 'operator-1',
      requestedByKind: 'human',
      startIdentity: {
        owner: { kind: 'human', id: 'operator-1' },
        target: {
          kind: 'agent',
          id: ADDRESS.agentId,
          threadId: ADDRESS.threadId,
        },
      },
      agentStart: { threaded: false },
    });
    expect(started.status).toBe('suspended');
    const journal = new RemoteStepJournal({
      db,
      fence,
      runtime,
      deploymentId: ADDRESS.deploymentId,
      tablePrefix: ADDRESS.tablePrefix,
      allowedToolIds: ['remote_lookup'],
      verifyCompletion: async () => undefined,
    });
    await journal.initialize();
    const invocation = await journal.register(
      ADDRESS,
      Date.now() + 60_000,
      PRINCIPAL,
    );
    const permit = await journal.issuePermit(invocation.invocationId, 30_000);
    expect(permit.generation).toBe(1);
    expect(
      await runtime.cancelActiveExecution(
        ADDRESS.workflowId,
        ADDRESS.runId,
        'cancelled',
        [{ kind: 'human', id: 'operator-1' }],
      ),
    ).toBe(false);
    expect(
      await runtime.authoritativeStatus(ADDRESS.workflowId, ADDRESS.runId),
    ).toMatchObject({ status: 'suspended' });
    const stored = sqlite
      .prepare(
        `SELECT json_type(snapshot,
          '$.requestContext."flowsafe.runLifecycle".transitionIntent') AS intent
         FROM mastra_workflow_snapshot WHERE workflow_name = ? AND run_id = ?`,
      )
      .get(ADDRESS.workflowId, ADDRESS.runId) as { intent: string };
    expect(stored.intent).toBe('object');
    await expect(journal.consumePermit(permit)).rejects.toMatchObject({
      status: 409,
    });
  });
});
