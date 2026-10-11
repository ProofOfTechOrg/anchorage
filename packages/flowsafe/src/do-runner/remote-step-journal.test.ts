// SPDX-License-Identifier: Apache-2.0

import { afterEach, describe, expect, it } from 'vitest';
import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';
import { ExecutionFenceStore } from './execution-fence.js';
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
        execution: {
          tablePrefix: '',
          workflowId: ADDRESS.workflowId,
          runId: ADDRESS.runId,
          startToken: 'start-token',
        },
        provenance: {
          startIdentity: {
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
    const journal = new RemoteStepJournal({
      db,
      fence,
      runtime,
      deploymentId: ADDRESS.deploymentId,
      tablePrefix: ADDRESS.tablePrefix,
      allowedToolIds: ['remote_lookup'],
      now: () => now,
    });
    await journal.initialize();
    return {
      db,
      fence,
      journal,
      runtime,
      reopen: () =>
        new RemoteStepJournal({
          db,
          fence,
          runtime,
          deploymentId: ADDRESS.deploymentId,
          tablePrefix: ADDRESS.tablePrefix,
          allowedToolIds: ['remote_lookup'],
          now: () => now,
        }),
      sqlite,
      setNow: (value: number) => {
        now = value;
      },
      setSummary: (value: RunSummary) => {
        summary = value;
      },
    };
  }

  it('converges repeated observation on one invocation without a permit', async () => {
    const h = await fixture();
    const [first, replay] = await Promise.all([
      h.journal.register(ADDRESS, 10_000),
      h.journal.register(ADDRESS, 10_000),
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

  it('accepts one permit consumption and refuses replay as effect authority', async () => {
    const h = await fixture();
    const invocation = await h.journal.register(ADDRESS, 10_000);
    const permit = await h.journal.issuePermit(invocation.invocationId, 1_000);
    expect(permit).toMatchObject({ generation: 1, expiresAt: 2_000 });
    expect(await h.journal.consumePermit(permit)).toBe('accepted');
    expect(await h.journal.consumePermit(permit)).toBe('already-consumed');
    await expect(
      h.journal.issuePermit(invocation.invocationId, 1_000),
    ).rejects.toMatchObject({
      status: 409,
    });
    await h.journal.closePermit(permit);
    const renewed = await h.journal.issuePermit(invocation.invocationId, 1_000);
    expect(renewed.generation).toBe(2);
    await expect(h.journal.consumePermit(permit)).rejects.toMatchObject({
      status: 409,
    });
    expect(await h.journal.consumePermit(renewed)).toBe('accepted');
  });

  it('recovers one permit after a journal instance is replaced', async () => {
    const h = await fixture();
    const invocation = await h.journal.register(ADDRESS, 10_000);
    const permit = await h.journal.issuePermit(invocation.invocationId, 1_000);
    const recovered = h.reopen();
    expect(await recovered.issuePermit(invocation.invocationId, 1_000)).toEqual(
      permit,
    );
    expect(await recovered.consumePermit(permit)).toBe('accepted');
    expect(await h.reopen().consumePermit(permit)).toBe('already-consumed');
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
    await expect(h.journal.register(ADDRESS, 10_000)).rejects.toMatchObject({
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
    });
    await expect(restricted.register(ADDRESS, 10_000)).rejects.toMatchObject({
      status: 503,
    });
  });

  it('refuses a corrupted persisted table identity before building a query', async () => {
    const h = await fixture();
    const invocation = await h.journal.register(ADDRESS, 10_000);
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
    const invocation = await h.journal.register(ADDRESS, 10_000);
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
    const invocation = await h.journal.register(ADDRESS, 10_000);
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
    const invocation = await h.journal.register(ADDRESS, 10_000);
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
});
