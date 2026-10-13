// SPDX-License-Identifier: Apache-2.0

import { afterEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod';
import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';
import { createAgentThreadTopology } from '../agent-host/thread-topology.js';
import type { FlowsafeWorkerEnv } from '../host-kit/flowsafe-worker.js';
import { createFlowsafeWorker } from '../host-kit/flowsafe-worker.js';
import { approvalStoreFactoryFor } from '../host-kit/host-approval-service.js';
import { staticTokenVerifier } from '../host-kit/verifier.js';
import { createD1Storage } from './d1-storage.js';
import { credentialsMatch } from './deployment-identity.js';
import { ExecutionFenceStore } from './execution-fence.js';
import { init } from './init.js';
import { DeploymentInventory } from './inventory.js';
import {
  REMOTE_STEP_MARKER,
  REMOTE_STEP_TABLE,
  RemoteStepJournal,
  WORKFLOW_REMOTE_STEP_MARKER,
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
              resourceId: ADDRESS.resourceId,
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
      retainResult: async () => {
        const invocation = await journal.register(ADDRESS, 10_000, PRINCIPAL);
        const permit = await journal.issuePermit(
          invocation.invocationId,
          1_000,
        );
        const consumed = await journal.consumePermit(permit);
        if (consumed.kind !== 'accepted') throw new Error('permit was not won');
        await journal.recordResult(
          invocation.invocationId,
          consumed.rawPermit,
          '{"value":"done"}',
          '{"receipt":"verified"}',
        );
        return { invocation, rawPermit: consumed.rawPermit };
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

  function restoreV3Schema(h: Awaited<ReturnType<typeof fixture>>) {
    const columns = (
      h.sqlite
        .prepare(`PRAGMA table_xinfo(${REMOTE_STEP_TABLE})`)
        .all() as Array<{ name: string }>
    )
      .slice(0, -4)
      .map(({ name }) => name)
      .join(', ');
    h.sqlite.exec(`CREATE TABLE ${REMOTE_STEP_TABLE}_v3 (
      invocation_id TEXT PRIMARY KEY, principal_json TEXT NOT NULL,
      start_token TEXT NOT NULL, deployment_id TEXT NOT NULL,
      table_prefix TEXT NOT NULL, workflow_id TEXT NOT NULL,
      agent_id TEXT NOT NULL, thread_id TEXT NOT NULL,
      resource_id TEXT NOT NULL, run_id TEXT NOT NULL,
      step_path TEXT NOT NULL, suspended_at INTEGER NOT NULL,
      resume_count INTEGER NOT NULL, request_json TEXT NOT NULL,
      deadline_at INTEGER NOT NULL, status TEXT NOT NULL,
      permit_generation INTEGER NOT NULL DEFAULT 0, permit_epoch INTEGER,
      permit_revision INTEGER, permit_expires_at INTEGER,
      permit_consumed_at INTEGER, permit_digest TEXT, result_json TEXT,
      result_digest TEXT, quiescence_digest TEXT, executor_quiescent_at INTEGER,
      resume_lease_owner TEXT, resume_lease_expires_at INTEGER,
      settled_at INTEGER, settlement_outcome TEXT,
      UNIQUE (deployment_id, table_prefix, workflow_id, agent_id,
        thread_id, resource_id, run_id, step_path, suspended_at, resume_count)
    )`);
    h.sqlite.exec(
      `INSERT INTO ${REMOTE_STEP_TABLE}_v3 (${columns}) SELECT ${columns} FROM ${REMOTE_STEP_TABLE}`,
    );
    h.sqlite.exec(`DROP TABLE ${REMOTE_STEP_TABLE}`);
    h.sqlite.exec(
      `ALTER TABLE ${REMOTE_STEP_TABLE}_v3 RENAME TO ${REMOTE_STEP_TABLE}`,
    );
    h.sqlite.exec(
      `CREATE INDEX flowsafe_remote_step_recovery ON ${REMOTE_STEP_TABLE} (invocation_id) WHERE status IN ('result-retained', 'resuming')`,
    );
    h.sqlite.exec('UPDATE flowsafe_remote_step_schema SET version = 3');
  }

  async function workflowFixture() {
    const h = await fixture();
    const original = h.runtime.authoritativeStartState.bind(h.runtime);
    h.runtime.authoritativeStartState = (async (
      workflowId: string,
      runId: string,
    ) => {
      const state = await original(workflowId, runId);
      if (!state) return state;
      return {
        ...state,
        provenance: {
          startIdentity: {
            owner: { kind: PRINCIPAL.kind, id: PRINCIPAL.id },
            target: { kind: 'workflow', id: ADDRESS.workflowId },
          },
        },
      } as typeof state;
    }) as typeof h.runtime.authoritativeStartState;
    const request = { version: 2, input: { query: 'x' } };
    const summary = (value: unknown): RunSummary => ({
      runId: ADDRESS.runId,
      status: 'suspended',
      suspended: [['execute']],
      suspendedAt: { execute: 500 },
      suspendPayload: {
        execute: {
          type: 'suspension',
          remoteStep: {
            marker: WORKFLOW_REMOTE_STEP_MARKER,
            request: value,
          },
        },
      },
    });
    h.setSummary(summary(request));
    const verifyWorkflowCompletion = vi.fn(async () => undefined);
    const journal = () =>
      new RemoteStepJournal({
        db: h.db,
        fence: h.fence,
        runtime: h.runtime,
        deploymentId: ADDRESS.deploymentId,
        tablePrefix: ADDRESS.tablePrefix,
        allowedToolIds: ['remote_lookup'],
        verifyCompletion: h.verifyCompletion,
        verifyWorkflowCompletion,
        now: () => 1_000,
      });
    const address = {
      targetKind: 'workflow',
      deploymentId: ADDRESS.deploymentId,
      tablePrefix: ADDRESS.tablePrefix,
      workflowId: ADDRESS.workflowId,
      runId: ADDRESS.runId,
    } as const;
    return { ...h, address, journal, summary, verifyWorkflowCompletion };
  }

  it('converges repeated observation on one invocation without a permit', async () => {
    const h = await fixture();
    const [first, replay] = await Promise.all([
      h.journal.register(ADDRESS, 10_000, PRINCIPAL),
      h.journal.register(ADDRESS, 10_000, PRINCIPAL),
    ]);
    expect(replay.invocationId).toBe(first.invocationId);
    expect(await h.journal.readDispatchRequestJson(first.invocationId)).toBe(
      '{"account":"acme"}',
    );
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

  it('refuses unauthorized dispatch and replay before the trusted effect', async () => {
    const h = await fixture();
    const invocation = await h.journal.register(ADDRESS, 10_000, PRINCIPAL);
    const owner = { kind: PRINCIPAL.kind, id: PRINCIPAL.id };
    const resources = approvalStoreFactoryFor(h.db).resources();
    for (const kind of ['thread', 'resource', 'run'] as const) {
      const id = kind === 'run' ? ADDRESS.runId : ADDRESS.threadId;
      expect(await resources.claim(kind, id, owner)).toBe(true);
    }
    const topology = createAgentThreadTopology(
      {
        idFromName: (name) => name,
        get: () => ({
          fetch: async () => {
            throw new Error('dispatch admission must not address a thread');
          },
        }),
      },
      'test-deployment-identity-secret-0001',
      {
        startIdempotency: 'none',
        executionFence: 'none',
        remoteStepDispatch: h.journal,
      },
    );
    type Env = FlowsafeWorkerEnv & { DISPATCH_FIXTURE_SECRET: string };
    const effect = vi.fn(async (_rawPermit: string) => undefined);
    const worker = createFlowsafeWorker<Env>({
      workflows: [],
      systemPrincipalId: 'test-system',
      buildVerifier: () => staticTokenVerifier(new Map()),
      maintenance: { sweepIntervalMs: 60_000, purgeIntervalMs: 60_000 },
      preRoutes: async (request, env, _ctx, kit) => {
        if (new URL(request.url).pathname !== '/__fixture/remote-dispatch')
          return null;
        const credential = request.headers.get('x-dispatch-fixture-secret');
        if (
          !credential ||
          !(await credentialsMatch(credential, env.DISPATCH_FIXTURE_SECRET))
        )
          return new Response(null, { status: 401 });
        const input = (await request.json()) as Record<string, unknown>;
        const keys = Object.keys(input).sort().join(',');
        const invocationId =
          input.action === 'issue' && keys === 'action,invocationId'
            ? input.invocationId
            : input.action === 'consume' && keys === 'action,permit'
              ? (input.permit as { invocationId?: unknown } | null)
                  ?.invocationId
              : undefined;
        if (typeof invocationId !== 'string')
          return new Response(null, { status: 400 });
        const context = await kit.contextForStoredInvocation(
          h.journal,
          invocationId,
        );
        if (!context) return new Response(null, { status: 404 });
        if (input.action === 'issue') {
          const ticket = await topology.issueRemoteStepDispatch(
            context,
            invocationId,
            1_000,
          );
          return Response.json(ticket);
        }
        const result = await topology.consumeRemoteStepDispatch(
          context,
          input.permit as Parameters<
            typeof topology.consumeRemoteStepDispatch
          >[1],
        );
        if (result.kind === 'already-consumed')
          return new Response(null, { status: 409 });
        await effect(result.rawPermit);
        return Response.json({ kind: 'accepted' });
      },
    });
    h.sqlite.exec(`CREATE TABLE flowsafe_deployment (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      tenant_tag TEXT NOT NULL,
      provisioned_at TEXT NOT NULL
    )`);
    h.sqlite
      .prepare(
        `INSERT INTO flowsafe_deployment VALUES (1, 'acme', '2026-08-10T00:00:00.000Z')`,
      )
      .run();
    const fixtureEnv: Env = {
      DB: h.db,
      DEPLOYMENT_TENANT: 'acme',
      DEPLOYMENT_IDENTITY_SECRET: 'test-deployment-identity-secret-0001',
      DISPATCH_FIXTURE_SECRET: 'separate-test-dispatch-secret-0001',
      RUNNER: {
        idFromName: (name) => name,
        get: () => ({ fetch: async () => new Response(null, { status: 503 }) }),
      },
      MAINTENANCE: {
        idFromName: (name) => name,
        get: () => ({ fetch: async () => new Response(null, { status: 503 }) }),
      },
    };
    const ctx = { waitUntil: () => undefined };
    const send = (body: unknown, secret?: string) =>
      worker.fetch(
        new Request('https://host/__fixture/remote-dispatch', {
          method: 'POST',
          headers: {
            'content-type': 'application/json',
            ...(secret ? { 'x-dispatch-fixture-secret': secret } : {}),
          },
          body: JSON.stringify(body),
        }),
        fixtureEnv,
        ctx,
      );

    expect(
      (await send({ action: 'issue', invocationId: invocation.invocationId }))
        .status,
    ).toBe(401);
    expect(
      (
        await send(
          { action: 'issue', invocationId: invocation.invocationId },
          'wrong-secret',
        )
      ).status,
    ).toBe(401);
    expect((await h.journal.get(invocation.invocationId))?.status).toBe(
      'pending',
    );
    expect(effect).not.toHaveBeenCalled();
    expect(
      (
        await send(
          {
            action: 'issue',
            invocationId: invocation.invocationId,
            lifetimeMs: 60_000,
          },
          fixtureEnv.DISPATCH_FIXTURE_SECRET,
        )
      ).status,
    ).toBe(400);
    expect(
      (
        await send(
          {
            action: 'issue',
            invocationId: invocation.invocationId,
            principal: PRINCIPAL,
          },
          fixtureEnv.DISPATCH_FIXTURE_SECRET,
        )
      ).status,
    ).toBe(400);
    expect((await h.journal.get(invocation.invocationId))?.status).toBe(
      'pending',
    );

    const issued = await send(
      { action: 'issue', invocationId: invocation.invocationId },
      fixtureEnv.DISPATCH_FIXTURE_SECRET,
    );
    expect(issued.status).toBe(200);
    const ticket = (await issued.json()) as {
      permit: Parameters<typeof topology.consumeRemoteStepDispatch>[1];
    };
    expect(ticket).not.toHaveProperty('rawPermit');
    const consume = { action: 'consume', permit: ticket.permit };
    expect((await send(consume)).status).toBe(401);
    expect((await send(consume, 'wrong-secret')).status).toBe(401);
    expect(effect).not.toHaveBeenCalled();
    const accepted = await send(consume, fixtureEnv.DISPATCH_FIXTURE_SECRET);
    expect(accepted.status).toBe(200);
    expect(await accepted.json()).toEqual({ kind: 'accepted' });
    expect(effect).toHaveBeenCalledTimes(1);
    expect(effect).toHaveBeenCalledWith(expect.stringMatching(/^[\w-]{43}$/));
    expect(
      (await send(consume, fixtureEnv.DISPATCH_FIXTURE_SECRET)).status,
    ).toBe(409);
    expect(effect).toHaveBeenCalledTimes(1);
  });

  it('registers the resource recorded in start provenance independently of the thread id', async () => {
    const h = await fixture();
    const original = h.runtime.authoritativeStartState.bind(h.runtime);
    h.runtime.authoritativeStartState = (async (
      workflowId: string,
      runId: string,
    ) => {
      const state = await original(workflowId, runId);
      if (!state) return state;
      return {
        ...state,
        provenance: {
          ...state.provenance,
          startIdentity: {
            ...state.provenance.startIdentity,
            target: {
              kind: 'agent',
              id: ADDRESS.agentId,
              threadId: ADDRESS.threadId,
              resourceId: 'account-7',
            },
          },
        },
      } as typeof state;
    }) as typeof h.runtime.authoritativeStartState;

    const invocation = await h.journal.register(
      { ...ADDRESS, resourceId: 'account-7' },
      10_000,
      PRINCIPAL,
    );
    expect(invocation.resourceId).toBe('account-7');
  });

  it('refuses remote dispatch when legacy start provenance lacks a resource', async () => {
    const h = await fixture();
    const original = h.runtime.authoritativeStartState.bind(h.runtime);
    h.runtime.authoritativeStartState = (async (
      workflowId: string,
      runId: string,
    ) => {
      const state = await original(workflowId, runId);
      if (!state) return state;
      return {
        ...state,
        provenance: {
          ...state.provenance,
          startIdentity: {
            ...state.provenance.startIdentity,
            target: {
              kind: 'agent',
              id: ADDRESS.agentId,
              threadId: ADDRESS.threadId,
            },
          },
        },
      } as typeof state;
    }) as typeof h.runtime.authoritativeStartState;

    await expect(
      h.journal.register(ADDRESS, 10_000, PRINCIPAL),
    ).rejects.toMatchObject({
      status: 503,
    });
    expect(
      h.sqlite
        .prepare(`SELECT count(*) AS count FROM ${REMOTE_STEP_TABLE}`)
        .get(),
    ).toEqual({ count: 0 });
  });

  it('upgrades a prior journal while preserving an existing invocation', async () => {
    const h = await fixture();
    const invocation = await h.journal.register(ADDRESS, 10_000, PRINCIPAL);
    restoreV3Schema(h);
    h.sqlite.exec('DROP INDEX flowsafe_remote_step_recovery');
    for (const column of [
      'settlement_outcome',
      'settled_at',
      'resume_lease_expires_at',
      'resume_lease_owner',
      'executor_quiescent_at',
      'quiescence_digest',
      'result_digest',
      'result_json',
    ])
      h.sqlite.exec(`ALTER TABLE ${REMOTE_STEP_TABLE} DROP COLUMN ${column}`);
    h.sqlite.exec('DROP TABLE flowsafe_remote_step_schema');

    await h.reopen().initialize();

    expect(await h.reopen().get(invocation.invocationId)).toMatchObject({
      invocationId: invocation.invocationId,
      status: 'pending',
    });
    const columns = h.sqlite
      .prepare(`PRAGMA table_xinfo(${REMOTE_STEP_TABLE})`)
      .all() as Array<{ name: string }>;
    expect(columns.map((column) => column.name)).toContain(
      'executor_quiescent_at',
    );
  });

  it('upgrades a live v2 result without losing its retained bytes', async () => {
    const h = await fixture();
    const invocation = await h.journal.register(ADDRESS, 10_000, PRINCIPAL);
    const permit = await h.journal.issuePermit(invocation.invocationId, 1_000);
    const consumed = await h.journal.consumePermit(permit);
    if (consumed.kind !== 'accepted') throw new Error('permit was not won');
    await h.journal.recordResult(
      invocation.invocationId,
      consumed.rawPermit,
      '{"value":"kept"}',
      '{"receipt":"verified"}',
    );
    restoreV3Schema(h);
    h.sqlite.exec('DROP INDEX flowsafe_remote_step_recovery');
    for (const column of [
      'settlement_outcome',
      'settled_at',
      'resume_lease_expires_at',
      'resume_lease_owner',
    ])
      h.sqlite.exec(`ALTER TABLE ${REMOTE_STEP_TABLE} DROP COLUMN ${column}`);
    h.sqlite.exec('UPDATE flowsafe_remote_step_schema SET version = 2');

    await h.reopen().initialize();

    expect(
      await h.reopen().readRetainedResult(invocation.invocationId),
    ).toMatchObject({
      resultJson: '{"value":"kept"}',
    });
    expect(
      h.sqlite.prepare('SELECT version FROM flowsafe_remote_step_schema').get(),
    ).toEqual({ version: 4 });
    expect(await h.reopen().recoverableResults(1)).toEqual({
      invocationIds: [invocation.invocationId],
      nextCursor: invocation.invocationId,
    });
  });

  it('preserves a live v3 resume lease while upgrading the journal', async () => {
    const h = await fixture();
    const { invocation } = await h.retainResult();
    const owner = await h.journal.claimResume(invocation.invocationId, 1_000);
    restoreV3Schema(h);
    const reopened = h.reopen();
    await reopened.initialize();
    expect(await reopened.get(invocation.invocationId)).toMatchObject({
      status: 'resuming',
      resumeLeaseExpiresAt: 2_000,
    });
    expect(
      await reopened.readRetainedResult(invocation.invocationId),
    ).toMatchObject({
      resultJson: '{"value":"done"}',
    });
    expect(
      await reopened.settleAfterResume(invocation.invocationId, owner),
    ).toBe('retry');
  });

  it('refuses an unknown journal schema version before dispatch', async () => {
    const h = await fixture();
    h.sqlite.exec('UPDATE flowsafe_remote_step_schema SET version = 99');

    await expect(h.reopen().initialize()).rejects.toMatchObject({
      status: 503,
    });
  });

  it('refuses a partial journal schema without changing its rows', async () => {
    const h = await fixture();
    const invocation = await h.journal.register(ADDRESS, 10_000, PRINCIPAL);
    h.sqlite.exec('DROP TABLE flowsafe_remote_step_schema');
    h.sqlite.exec(`ALTER TABLE ${REMOTE_STEP_TABLE} DROP COLUMN result_digest`);

    await expect(h.reopen().initialize()).rejects.toMatchObject({
      status: 503,
    });
    expect(
      h.sqlite.prepare(`SELECT invocation_id FROM ${REMOTE_STEP_TABLE}`).get(),
    ).toMatchObject({ invocation_id: invocation.invocationId });
  });

  it('refuses an unversioned journal without its suspension uniqueness constraint', async () => {
    const h = await fixture();
    restoreV3Schema(h);
    const ddl = (
      h.sqlite
        .prepare(
          `SELECT sql FROM sqlite_schema WHERE name = '${REMOTE_STEP_TABLE}'`,
        )
        .get() as { sql: string }
    ).sql;
    h.sqlite.exec(`DROP TABLE ${REMOTE_STEP_TABLE}`);
    h.sqlite.exec('DROP TABLE flowsafe_remote_step_schema');
    h.sqlite.exec(ddl.replace(/,\s*UNIQUE \(deployment_id,[^)]*\)/, ''));

    await expect(h.reopen().initialize()).rejects.toMatchObject({
      status: 503,
    });
  });

  it('refuses an unversioned journal without the permit-generation default', async () => {
    const h = await fixture();
    const ddl = (
      h.sqlite
        .prepare(
          `SELECT sql FROM sqlite_schema WHERE name = '${REMOTE_STEP_TABLE}'`,
        )
        .get() as { sql: string }
    ).sql;
    h.sqlite.exec(`DROP TABLE ${REMOTE_STEP_TABLE}`);
    h.sqlite.exec('DROP TABLE flowsafe_remote_step_schema');
    h.sqlite.exec(
      ddl.replace(
        'permit_generation INTEGER NOT NULL DEFAULT 0',
        'permit_generation INTEGER NOT NULL',
      ),
    );

    await expect(h.reopen().initialize()).rejects.toMatchObject({
      status: 503,
    });
  });

  it('refuses an unversioned workflow journal with a changed target constraint', async () => {
    const h = await fixture();
    const ddl = (
      h.sqlite
        .prepare(
          `SELECT sql FROM sqlite_schema WHERE name = '${REMOTE_STEP_TABLE}'`,
        )
        .get() as { sql: string }
    ).sql;
    h.sqlite.exec(`DROP TABLE ${REMOTE_STEP_TABLE}`);
    h.sqlite.exec('DROP TABLE flowsafe_remote_step_schema');
    h.sqlite.exec(
      ddl.replace(
        "CHECK ((target_kind = 'agent' AND agent_id IS NOT NULL",
        "CHECK ((target_kind = 'agent' AND agent_id IS NULL",
      ),
    );
    await expect(h.reopen().initialize()).rejects.toMatchObject({
      status: 503,
    });
  });

  it('converges when another initializer wins the same journal upgrade', async () => {
    const h = await fixture();
    const invocation = await h.journal.register(ADDRESS, 10_000, PRINCIPAL);
    restoreV3Schema(h);
    h.sqlite.exec('DROP INDEX flowsafe_remote_step_recovery');
    for (const column of [
      'settlement_outcome',
      'settled_at',
      'resume_lease_expires_at',
      'resume_lease_owner',
      'executor_quiescent_at',
      'quiescence_digest',
      'result_digest',
      'result_json',
    ])
      h.sqlite.exec(`ALTER TABLE ${REMOTE_STEP_TABLE} DROP COLUMN ${column}`);
    h.sqlite.exec('DROP TABLE flowsafe_remote_step_schema');
    const originalBatch = h.db.batch.bind(h.db);
    h.db.batch = async (statements) => {
      h.db.batch = originalBatch;
      await h.reopen().initialize();
      return originalBatch(statements);
    };

    await h.reopen().initialize();

    expect(await h.reopen().get(invocation.invocationId)).toMatchObject({
      invocationId: invocation.invocationId,
      status: 'pending',
    });
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

  it('grants raw authority to one concurrent D1 consume winner', async () => {
    const h = await fixture();
    const invocation = await h.journal.register(ADDRESS, 10_000, PRINCIPAL);
    const permit = await h.journal.issuePermit(invocation.invocationId, 1_000);
    const outcomes = await Promise.all([
      h.journal.consumePermit(permit),
      h.reopen().consumePermit(permit),
    ]);
    expect(outcomes.filter((item) => item.kind === 'accepted')).toHaveLength(1);
    expect(outcomes.filter((item) => item.kind === 'already-consumed')).toEqual(
      [{ kind: 'already-consumed' }],
    );
    const winner = outcomes.find((item) => item.kind === 'accepted');
    expect(winner).toMatchObject({ rawPermit: expect.any(String) });
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
      await h.journal.readRetainedResult(invocation.invocationId),
    ).toMatchObject({
      resultJson: bytes,
      resultDigest: stored.result_digest,
    });
  });

  it('retains drain work after an unproven run advancement', async () => {
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
    expect((await h.journal.get(invocation.invocationId))?.status).toBe(
      'result-retained',
    );
    h.setSummary({ runId: ADDRESS.runId, status: 'success' });
    h.sqlite
      .prepare('UPDATE mastra_workflow_snapshot SET snapshot = ?')
      .run('{"status":"success"}');
    expect((await h.reopen().get(invocation.invocationId))?.status).toBe(
      'result-retained',
    );
    expect(
      (await new DeploymentInventory(h.db).read('remote-steps')).count,
    ).toBe(1);
  });

  it('settles a witnessed new suspension and keeps callback replay idempotent', async () => {
    const h = await fixture();
    const { invocation, rawPermit } = await h.retainResult();
    const owner = await h.journal.claimResume(invocation.invocationId, 1_000);
    await expect(
      h.reopen().claimResume(invocation.invocationId, 1_000),
    ).rejects.toMatchObject({ status: 409 });
    const next: RunSummary = {
      runId: ADDRESS.runId,
      status: 'suspended',
      suspended: [['next']],
      suspendedAt: { next: 1_100 },
      suspendPayload: { next: { type: 'suspension' } },
    };
    expect(
      await h.reopen().settleAfterResume(invocation.invocationId, owner, next),
    ).toBe('retry');
    const nextOwner = await h.journal.claimResume(
      invocation.invocationId,
      1_000,
    );
    h.setSummary(next);
    expect(
      await h
        .reopen()
        .settleAfterResume(invocation.invocationId, nextOwner, next),
    ).toBe('applied');
    expect(await h.journal.get(invocation.invocationId)).toMatchObject({
      status: 'settled',
      settlementOutcome: 'applied',
    });
    expect(
      (await new DeploymentInventory(h.db).read('remote-steps')).count,
    ).toBe(0);
    expect(await h.journal.recoverableResults(1)).toEqual({
      invocationIds: [],
    });
    expect(
      await h.journal.recoverableResults(1, invocation.invocationId),
    ).toEqual({ invocationIds: [] });
    expect(
      await h.journal.recordResult(
        invocation.invocationId,
        rawPermit,
        '{"value":"done"}',
        '{"receipt":"verified"}',
      ),
    ).toMatchObject({ kind: 'replay' });
    expect(
      await h.journal.readRetainedResult(invocation.invocationId),
    ).toBeUndefined();
    h.sqlite
      .prepare(
        `UPDATE ${REMOTE_STEP_TABLE} SET result_digest = NULL, write_revision = write_revision + 1`,
      )
      .run();
    expect(
      (await new DeploymentInventory(h.db).read('remote-steps')).count,
    ).toBe(1);
  });

  it('retains an expired claim across another journal instance until its owner settles', async () => {
    const h = await fixture();
    const { invocation } = await h.retainResult();
    const owner = await h.journal.claimResume(invocation.invocationId, 100);
    expect(await h.reopen().recoverableResults(10)).toEqual({
      invocationIds: [],
    });
    h.setNow(1_101);
    expect(await h.reopen().recoverableResults(10)).toEqual({
      invocationIds: [invocation.invocationId],
    });
    await expect(
      h.journal.claimResume(invocation.invocationId, 1_000),
    ).rejects.toMatchObject({ status: 409 });
    expect(await h.reopen().reconcileResult(invocation.invocationId)).toBe(
      'pending-quiescence',
    );
    expect(await h.journal.get(invocation.invocationId)).toMatchObject({
      status: 'resuming',
    });
    expect(
      h.sqlite
        .prepare(`SELECT resume_lease_owner FROM ${REMOTE_STEP_TABLE}`)
        .get(),
    ).toEqual({ resume_lease_owner: owner });
    expect(
      (await new DeploymentInventory(h.db).read('remote-steps')).count,
    ).toBe(1);
    h.setSummary({ runId: ADDRESS.runId, status: 'success' });
    expect(
      await h.journal.settleAfterResume(invocation.invocationId, owner, {
        runId: ADDRESS.runId,
        status: 'success',
      }),
    ).toBe('applied');
    expect(await h.journal.get(invocation.invocationId)).toMatchObject({
      status: 'settled',
      settlementOutcome: 'applied',
    });
    expect(
      (await new DeploymentInventory(h.db).read('remote-steps')).count,
    ).toBe(0);
  });

  it('retains uncertain advancement when no resume lease was claimed', async () => {
    const h = await fixture();
    const { invocation } = await h.retainResult();
    h.setSummary({ runId: ADDRESS.runId, status: 'success' });
    expect(await h.reopen().reconcileResult(invocation.invocationId)).toBe(
      'advanced-without-proof',
    );
    expect(await h.journal.get(invocation.invocationId)).toMatchObject({
      status: 'failed',
      settlementOutcome: 'advanced-without-proof',
    });
    expect(
      (await new DeploymentInventory(h.db).read('remote-steps')).count,
    ).toBe(1);
    await expect(
      h.journal.claimResume(invocation.invocationId, 1_000),
    ).rejects.toMatchObject({ status: 409 });
  });

  it('records cancellation separately from a successful result application', async () => {
    const h = await fixture();
    const { invocation } = await h.retainResult();
    const owner = await h.journal.claimResume(invocation.invocationId, 1_000);
    h.setSummary({ runId: ADDRESS.runId, status: 'cancelled' });
    expect(
      await h.journal.settleAfterResume(invocation.invocationId, owner),
    ).toBe('terminal-not-success');
    expect(await h.journal.get(invocation.invocationId)).toMatchObject({
      status: 'failed',
      settlementOutcome: 'terminal-not-success',
    });
    expect(
      (await new DeploymentInventory(h.db).read('remote-steps')).count,
    ).toBe(0);
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
    expect((await h.journal.get(invocation.invocationId))?.status).toBe(
      'result-retained',
    );
    expect(
      (await new DeploymentInventory(h.db).read('remote-steps')).count,
    ).toBe(1);
  });

  it('retains a verified late result as drain work after cancellation', async () => {
    const h = await fixture();
    const invocation = await h.journal.register(ADDRESS, 10_000, PRINCIPAL);
    const permit = await h.journal.issuePermit(invocation.invocationId, 1_000);
    const consumed = await h.journal.consumePermit(permit);
    if (consumed.kind !== 'accepted') throw new Error('permit was not won');
    h.setSummary({ runId: ADDRESS.runId, status: 'cancelled' });
    h.sqlite
      .prepare('UPDATE mastra_workflow_snapshot SET snapshot = ?')
      .run('{"status":"cancelled"}');
    await h.journal.recordResult(
      invocation.invocationId,
      consumed.rawPermit,
      '{"value":"done"}',
      '{"receipt":"verified"}',
    );
    expect((await h.journal.get(invocation.invocationId))?.status).toBe(
      'result-retained',
    );
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
      .prepare(
        `UPDATE ${REMOTE_STEP_TABLE} SET table_prefix = ?, write_revision = write_revision + 1`,
      )
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

  it('retains a workflow result across journal reopen and rejects permit replay', async () => {
    const h = await workflowFixture();
    const journal = h.journal();
    const first = await journal.registerWorkflow(h.address, 10_000, PRINCIPAL);
    const replay = await journal.registerWorkflow(h.address, 10_000, PRINCIPAL);
    expect(replay.invocationId).toBe(first.invocationId);
    expect(first.requestDigest).toBe(
      'f908a6c18fa0b52b0f1295fc8f1cb5db083020cb18dab24ebf26637f55a9af49',
    );
    expect(await journal.get(first.invocationId)).toBeUndefined();
    expect(
      await journal.readWorkflowDispatchRequestJson(first.invocationId),
    ).toBe('{"version":2,"input":{"query":"x"}}');
    await expect(
      journal.readDispatchRequestJson(first.invocationId),
    ).rejects.toMatchObject({ status: 404 });
    expect(await journal.getWorkflow(first.invocationId)).toMatchObject({
      targetKind: 'workflow',
      status: 'pending',
      stepPath: ['execute'],
    });
    await expect(
      h.reopen().issuePermit(first.invocationId, 1_000),
    ).rejects.toMatchObject({
      status: 503,
    });
    const permit = await journal.issuePermit(first.invocationId, 1_000);
    const winner = await journal.consumePermit(permit);
    if (winner.kind !== 'accepted') throw new Error('permit was not won');
    expect(await h.journal().consumePermit(permit)).toEqual({
      kind: 'already-consumed',
    });
    await expect(
      journal.recordResult(
        first.invocationId,
        'A'.repeat(43),
        '{"value":"done"}',
        '{"receipt":"verified"}',
      ),
    ).rejects.toMatchObject({ status: 403 });
    expect(h.verifyWorkflowCompletion).not.toHaveBeenCalled();
    expect(
      await journal.recordResult(
        first.invocationId,
        winner.rawPermit,
        '{"value":"done"}',
        '{"receipt":"verified"}',
      ),
    ).toMatchObject({ kind: 'recorded' });
    expect(h.verifyWorkflowCompletion).toHaveBeenCalledExactlyOnceWith({
      invocation: expect.objectContaining({
        invocationId: first.invocationId,
        targetKind: 'workflow',
        requestDigest: first.requestDigest,
        status: 'dispatched',
      }),
      rawPermit: winner.rawPermit,
      resultJson: '{"value":"done"}',
      evidenceJson: '{"receipt":"verified"}',
    });
    const reopened = h.journal();
    await reopened.initialize();
    expect(await reopened.readRetainedResult(first.invocationId)).toMatchObject(
      {
        resultJson: '{"value":"done"}',
      },
    );
    expect(await reopened.recoverableResults(10)).toEqual({
      invocationIds: [],
    });
    expect(
      (await new DeploymentInventory(h.db).read('remote-steps')).entries,
    ).toEqual([expect.objectContaining({ key: [first.invocationId] })]);
    expect(
      await reopened.recordResult(
        first.invocationId,
        winner.rawPermit,
        '{"value":"done"}',
        '{"receipt":"verified"}',
      ),
    ).toMatchObject({ kind: 'replay' });
    await expect(
      reopened.recordResult(
        first.invocationId,
        winner.rawPermit,
        '{"value":"changed"}',
        '{"receipt":"verified"}',
      ),
    ).rejects.toMatchObject({ status: 409 });
    expect(h.verifyWorkflowCompletion).toHaveBeenCalledTimes(1);
    await expect(
      reopened.claimResume(first.invocationId, 1_000),
    ).rejects.toMatchObject({
      status: 404,
    });
  });

  it('refuses workflow dispatch when trusted provenance or suspension changes', async () => {
    const h = await workflowFixture();
    const journal = h.journal();
    await expect(
      journal.registerWorkflow(h.address, 10_000, {
        ...PRINCIPAL,
        id: 'another-operator',
      }),
    ).rejects.toMatchObject({ status: 409 });
    const first = await journal.registerWorkflow(h.address, 10_000, PRINCIPAL);
    h.setSummary(h.summary({ version: 2, input: { query: 'changed' } }));
    await expect(
      journal.issuePermit(first.invocationId, 1_000),
    ).rejects.toMatchObject({
      status: 409,
    });
    h.setSummary(h.summary({ version: 2, input: { query: 'x' } }));
    const permit = await journal.issuePermit(first.invocationId, 1_000);
    const before = await h.fence.read();
    await h.fence.transition({
      expected: 'open',
      next: 'migration-locked',
      expectedMutationEpoch: before.mutationEpoch,
      expectedRevision: before.transitionRevision,
      advanceMutationEpoch: true,
    });
    await expect(journal.consumePermit(permit)).rejects.toMatchObject({
      status: 503,
    });
  });

  it('rejects stale v3-style journal writes after v4 migration', async () => {
    const h = await fixture();
    const invocation = await h.journal.register(ADDRESS, 10_000, PRINCIPAL);
    restoreV3Schema(h);
    await h.reopen().initialize();
    expect(() =>
      h.sqlite
        .prepare(
          `UPDATE ${REMOTE_STEP_TABLE} SET deadline_at = ? WHERE invocation_id = ?`,
        )
        .run(9_000, invocation.invocationId),
    ).toThrow('remote step writer version changed');
    expect(() =>
      h.sqlite
        .prepare(`INSERT INTO ${REMOTE_STEP_TABLE} (
        invocation_id, principal_json, start_token, deployment_id,
        table_prefix, workflow_id, agent_id, thread_id, resource_id, run_id,
        step_path, suspended_at, resume_count, request_json, deadline_at, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')`)
        .run(
          'old-writer',
          JSON.stringify(PRINCIPAL),
          'start-token',
          ADDRESS.deploymentId,
          ADDRESS.tablePrefix,
          ADDRESS.workflowId,
          ADDRESS.agentId,
          ADDRESS.threadId,
          ADDRESS.resourceId,
          ADDRESS.runId,
          '["execute"]',
          500,
          -1,
          '{}',
          10_000,
        ),
    ).toThrow();
    expect(await h.reopen().get(invocation.invocationId)).toMatchObject({
      deadlineAt: 10_000,
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
          resourceId: ADDRESS.resourceId,
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
