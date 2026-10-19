// SPDX-License-Identifier: Apache-2.0
/// <reference types="@cloudflare/vitest-pool-workers/types" />
import { reset } from 'cloudflare:test';
import { env } from 'cloudflare:workers';
import { beforeEach, describe, expect, it } from 'vitest';

import {
  D1ResourceOwnershipStore,
  type ResourceOwner,
} from './approval-api/resource-ownership.js';
import {
  assertDeploymentIdentity,
  readDeploymentIdentity,
  seedDeploymentIdentity,
} from './do-runner/deployment-identity.js';
import type {
  ExecutionFenceDatabase,
  ExecutionFenceStatement,
} from './do-runner/execution-fence.js';
import { ExecutionFenceStore } from './do-runner/execution-fence.js';
import {
  type RemoteStepInvocation,
  RemoteStepJournal,
} from './do-runner/remote-step-journal.js';
import type { ThreadScope } from './do-runner/thread-do.js';
import { D1SchedulesStorage } from './schedules/schedules-d1.js';
import { D1SubscriptionStoreFactory } from './signal-providers/subscription-d1.js';
import type { SignalDatabase, SignalStatement } from './signals/d1-shared.js';
import {
  D1NotificationsStorage,
  type NotificationOperationInput,
  projectNotificationOperation,
} from './signals/notifications-d1.js';
import { createThreadSignalRoutes } from './signals/thread-do-routes.js';

interface TestBindings {
  DB: D1Database;
  SIGNAL_PROVIDER_HOST: DurableObjectNamespace;
}

const db = (): D1Database => (env as unknown as TestBindings).DB;
const notificationRequestDigests = {
  'operation-1':
    '69d0b9cae1fecec25e3fdad599bc1d4b441761843e4c399563c43b4f8e27699c',
  'operation-2':
    '96bdec0f9c3160a3bdd190c24543168f4a4bcc5b7041507c996b042fc593f1dc',
} as const;

function receiptActor(name: string) {
  const namespace = (env as unknown as TestBindings).SIGNAL_PROVIDER_HOST;
  return namespace.get(namespace.idFromName(name));
}

function createReceipt(
  actor: ReturnType<typeof receiptActor>,
  operationKey: string,
  notificationId: string,
  receiptLimit: number,
  barrier?: {
    id: string;
    participant: string;
    phase?: 'receipt-read' | 'scope-census';
  },
  tablePrefix = '',
) {
  return actor.fetch(
    new Request('http://localhost/receipt', {
      method: 'POST',
      body: JSON.stringify({
        operationKey,
        notificationId,
        receiptLimit,
        tablePrefix,
        barrierId: barrier?.id,
        participant: barrier?.participant,
        barrierPhase: barrier?.phase,
      }),
    }),
  );
}

describe('FlowSafe Cloudflare fidelity', () => {
  beforeEach(reset);

  it('provisions one deployment identity concurrently and refuses re-homing', async () => {
    expect(await readDeploymentIdentity(db())).toBeUndefined();

    await Promise.all([
      seedDeploymentIdentity(db(), 'acme', 'open'),
      seedDeploymentIdentity(db(), 'acme', 'open'),
    ]);

    await expect(assertDeploymentIdentity(db(), 'acme')).resolves.toBe(
      undefined,
    );
    await expect(
      seedDeploymentIdentity(db(), 'globex', 'open'),
    ).rejects.toThrow(/already belongs to deployment 'acme'/);
    expect(await readDeploymentIdentity(db())).toBe('acme');
  });

  it('isolates D1 per test and resolves concurrent resource claims to one owner', async () => {
    expect(await readDeploymentIdentity(db())).toBeUndefined();
    await seedDeploymentIdentity(db(), 'acme', 'open');

    const store = new D1ResourceOwnershipStore(db());
    const contenders: readonly ResourceOwner[] = [
      { kind: 'human', id: 'alice' },
      { kind: 'human', id: 'bob' },
    ];
    const outcomes = await Promise.all(
      contenders.map(async (owner) => ({
        claimed: await store.claim('run', 'run-cas', owner),
        owner,
      })),
    );

    const winners = outcomes.filter(({ claimed }) => claimed);
    expect(winners).toHaveLength(1);
    expect(await store.owner('run', 'run-cas')).toEqual(winners[0]?.owner);
  });

  it('commits one notification receipt and effect when distinct durable objects submit the same operation', async () => {
    await new D1NotificationsStorage(db()).init();
    await db()
      .prepare(`CREATE TABLE notification_test_barrier (
        barrier_id TEXT NOT NULL,
        participant TEXT NOT NULL,
        PRIMARY KEY (barrier_id, participant)
      )`)
      .run();
    const left = receiptActor('receipt-left');
    const right = receiptActor('receipt-right');
    const responses = await Promise.all([
      createReceipt(left, 'operation-1', 'notification-1', 1, {
        id: 'same-key',
        participant: 'left',
      }),
      createReceipt(right, 'operation-1', 'notification-1', 1, {
        id: 'same-key',
        participant: 'right',
      }),
    ]);

    const ready = await db()
      .prepare(
        "SELECT participant FROM notification_test_barrier WHERE barrier_id = 'same-key' ORDER BY participant",
      )
      .all<{ participant: string }>();
    expect(ready.results).toEqual([
      { participant: 'left' },
      { participant: 'right' },
    ]);

    expect(responses.map((response) => response.status)).toEqual([200, 200]);
    expect(
      await Promise.all(responses.map((response) => response.json())),
    ).toEqual([
      {
        operationKey: 'operation-1',
        notificationId: 'notification-1',
        threadId: 'thread',
        requestDigest: notificationRequestDigests['operation-1'],
      },
      {
        operationKey: 'operation-1',
        notificationId: 'notification-1',
        threadId: 'thread',
        requestDigest: notificationRequestDigests['operation-1'],
      },
    ]);
    const receipts = await db()
      .prepare(
        'SELECT operation_key, notification_id FROM flowsafe_notification_operation_receipts',
      )
      .all<{ operation_key: string; notification_id: string }>();
    expect(receipts.results).toEqual([
      { operation_key: 'operation-1', notification_id: 'notification-1' },
    ]);
    const inbox = await db()
      .prepare('SELECT id, thread_id FROM mastra_notifications')
      .all<{ id: string; thread_id: string }>();
    expect(inbox.results).toEqual([
      { id: 'notification-1', thread_id: 'thread' },
    ]);
  });

  it('admits one distinct durable object operation at the last receipt slot', async () => {
    await new D1NotificationsStorage(db()).init();
    await db()
      .prepare(`CREATE TABLE notification_test_barrier (
        barrier_id TEXT NOT NULL,
        participant TEXT NOT NULL,
        PRIMARY KEY (barrier_id, participant)
      )`)
      .run();
    await db()
      .prepare(`WITH RECURSIVE sequence(n) AS (
        VALUES (1) UNION ALL SELECT n + 1 FROM sequence WHERE n < 99999
      )
      INSERT INTO flowsafe_notification_operation_receipts
        (operation_key, request_digest, thread_id, notification_id)
      SELECT 'seed-' || n, 'seed', 'seed-thread', 'seed-' || n FROM sequence`)
      .run();
    const left = receiptActor('slot-left');
    const right = receiptActor('slot-right');
    const responses = await Promise.all([
      createReceipt(left, 'operation-1', 'notification-1', 100_000, {
        id: 'last-slot',
        participant: 'left',
      }),
      createReceipt(right, 'operation-2', 'notification-2', 100_000, {
        id: 'last-slot',
        participant: 'right',
      }),
    ]);

    const ready = await db()
      .prepare(
        "SELECT participant FROM notification_test_barrier WHERE barrier_id = 'last-slot' ORDER BY participant",
      )
      .all<{ participant: string }>();
    expect(ready.results).toEqual([
      { participant: 'left' },
      { participant: 'right' },
    ]);

    expect(responses.map((response) => response.status).sort()).toEqual([
      200, 409,
    ]);
    const receipts = await db()
      .prepare(`SELECT operation_key, notification_id, request_digest
        FROM flowsafe_notification_operation_receipts
        WHERE operation_key IN ('operation-1', 'operation-2')`)
      .all<{
        operation_key: string;
        notification_id: string;
        request_digest: string;
      }>();
    const inbox = await db()
      .prepare('SELECT id FROM mastra_notifications')
      .all<{ id: string }>();
    expect([
      [
        {
          operation_key: 'operation-1',
          notification_id: 'notification-1',
          request_digest: notificationRequestDigests['operation-1'],
        },
      ],
      [
        {
          operation_key: 'operation-2',
          notification_id: 'notification-2',
          request_digest: notificationRequestDigests['operation-2'],
        },
      ],
    ]).toContainEqual(receipts.results);
    const winner = receipts.results[0];
    if (!winner) throw new Error('receipt is missing');
    expect(inbox.results).toEqual([{ id: winner.notification_id }]);
    const count = await db()
      .prepare(
        'SELECT COUNT(*) AS total FROM flowsafe_notification_operation_receipts',
      )
      .first<{ total: number }>();
    expect(count?.total).toBe(100_000);
    const loserResponse = responses.find((response) => response.status === 409);
    expect(await loserResponse?.json()).toEqual({
      error: 'notification operation receipt cap reached',
    });
    const replay = await createReceipt(
      winner.operation_key === 'operation-1' ? right : left,
      winner.operation_key,
      winner.notification_id,
      100_000,
    );
    expect(replay.status).toBe(200);
    expect(await replay.json()).toEqual({
      operationKey: winner.operation_key,
      notificationId: winner.notification_id,
      threadId: 'thread',
      requestDigest: winner.request_digest,
    });
  }, 30_000);

  it('admits one receipt prefix across distinct durable objects', async () => {
    await Promise.all([
      new D1NotificationsStorage(db(), 'left_').init(),
      new D1NotificationsStorage(db(), 'right_').init(),
    ]);
    await db()
      .prepare(`CREATE TABLE notification_test_barrier (
        barrier_id TEXT NOT NULL,
        participant TEXT NOT NULL,
        PRIMARY KEY (barrier_id, participant)
      )`)
      .run();
    const responses = await Promise.all([
      createReceipt(
        receiptActor('prefix-left'),
        'operation-1',
        'notification-1',
        1,
        { id: 'prefix-claim', participant: 'left', phase: 'scope-census' },
        'left_',
      ),
      createReceipt(
        receiptActor('prefix-right'),
        'operation-2',
        'notification-2',
        1,
        { id: 'prefix-claim', participant: 'right', phase: 'scope-census' },
        'right_',
      ),
    ]);
    const ready = await db()
      .prepare(`SELECT participant FROM notification_test_barrier
        WHERE barrier_id = 'prefix-claim' ORDER BY participant`)
      .all<{ participant: string }>();
    expect(ready.results).toEqual([
      { participant: 'left' },
      { participant: 'right' },
    ]);
    expect(responses.map((response) => response.status).sort()).toEqual([
      200, 409,
    ]);
    const loser = responses.find((response) => response.status === 409);
    expect(await loser?.json()).toEqual({
      error: 'notification receipt scope differs from this deployment',
    });
    const [leftReceipts, rightReceipts, leftInbox, rightInbox] =
      await Promise.all([
        db()
          .prepare(
            'SELECT operation_key FROM left_flowsafe_notification_operation_receipts',
          )
          .all<{ operation_key: string }>(),
        db()
          .prepare(
            'SELECT operation_key FROM right_flowsafe_notification_operation_receipts',
          )
          .all<{ operation_key: string }>(),
        db()
          .prepare('SELECT id FROM left_mastra_notifications')
          .all<{ id: string }>(),
        db()
          .prepare('SELECT id FROM right_mastra_notifications')
          .all<{ id: string }>(),
      ]);
    const expected =
      responses[0]?.status === 200
        ? {
            receipts: [[{ operation_key: 'operation-1' }], []],
            inbox: [[{ id: 'notification-1' }], []],
            prefix: 'left_',
          }
        : {
            receipts: [[], [{ operation_key: 'operation-2' }]],
            inbox: [[], [{ id: 'notification-2' }]],
            prefix: 'right_',
          };
    expect([leftReceipts.results, rightReceipts.results]).toEqual(
      expected.receipts,
    );
    expect([leftInbox.results, rightInbox.results]).toEqual(expected.inbox);
    const scope = await db()
      .prepare(
        'SELECT table_prefix, receipt_limit FROM flowsafe_notification_receipt_scope',
      )
      .first<{ table_prefix: string; receipt_limit: number }>();
    expect(scope).toEqual({ table_prefix: expected.prefix, receipt_limit: 1 });
  });
});

describe('producer mutation admission on D1', () => {
  beforeEach(reset);
  it.each([
    'notification',
    'coalescing',
    'operation',
    'subscribe',
    'unsubscribe',
  ] as const)('refuses %s when the epoch advances immediately before its authoritative write', async (operation) => {
    const binding = db();
    const fence = new ExecutionFenceStore(binding);
    await fence.seed('open');
    let armed = false;
    const advance = async () => {
      if (!armed) return;
      armed = false;
      await fence.transition({
        expected: 'open',
        next: 'draining',
        expectedMutationEpoch: 0,
        expectedRevision: 0,
        advanceMutationEpoch: true,
      });
    };
    const originals = new WeakMap<SignalStatement, D1PreparedStatement>();
    const wrapped = (
      query: string,
      statement: SignalStatement,
    ): SignalStatement => {
      const result: SignalStatement = {
        bind: (...values) => wrapped(query, statement.bind(...values)),
        async first<T>() {
          if (
            query.includes('INSERT INTO flowsafe_signal_subscriptions') ||
            query.includes('UPDATE mastra_notifications')
          )
            await advance();
          return statement.first<T>();
        },
        all: <T>() => statement.all<T>(),
        async run() {
          if (
            query.includes('INSERT INTO mastra_notifications') ||
            query.includes('DELETE FROM flowsafe_signal_subscriptions')
          )
            await advance();
          return statement.run();
        },
      };
      originals.set(result, statement as D1PreparedStatement);
      return result;
    };
    const adapter: SignalDatabase = {
      prepare: (query) => wrapped(query, binding.prepare(query)),
      async batch(statements) {
        await advance();
        return binding.batch(
          statements.map((statement) => {
            const original = originals.get(statement);
            if (!original) throw new Error('unrecognized prepared statement');
            return original;
          }),
        );
      },
    };
    // Batch execution needs native prepared statements; the operation path's
    // wrapper is applied at the transaction boundary instead of each statement.
    const operationAdapter: SignalDatabase = {
      prepare: (query) => binding.prepare(query),
      async batch(statements) {
        await advance();
        return binding.batch(statements as D1PreparedStatement[]);
      },
    };
    const notifications = new D1NotificationsStorage(
      operation === 'operation' ? operationAdapter : adapter,
    );
    const subscriptions = new D1SubscriptionStoreFactory(adapter).store();
    await notifications.init();
    const notification = {
      id: 'original',
      threadId: 'thread',
      source: 'provider',
      kind: 'update',
      summary: 'original',
      coalesceKey: 'same',
    };
    if (operation === 'coalescing')
      await notifications.createNotification(notification);
    const subscription = {
      providerId: 'github',
      externalResourceId: 'repo',
      threadId: 'thread',
      resourceId: 'owner',
    };
    await subscriptions.subscribe(subscription);
    armed = true;
    const pending =
      operation === 'operation'
        ? notifications.createNotificationOnce(
            'new-operation',
            { ...notification, createdAt: new Date('2026-10-19T00:00:00Z') },
            { mutationEpoch: 0 },
          )
        : operation === 'subscribe'
          ? subscriptions.subscribe(
              { ...subscription, resourceId: 'different' },
              { mutationEpoch: 0 },
            )
          : operation === 'unsubscribe'
            ? subscriptions.unsubscribe('github', 'repo', 'thread', {
                mutationEpoch: 0,
              })
            : notifications.createNotification(
                {
                  ...notification,
                  id: 'late',
                  summary: 'late',
                  ...(operation === 'notification'
                    ? { coalesceKey: undefined }
                    : {}),
                },
                { mutationEpoch: 0 },
              );
    await expect(pending).rejects.toMatchObject({
      reason: { code: 'MUTATION_EPOCH_MISMATCH' },
    });
    expect(await subscriptions.listForThread('thread')).toMatchObject([
      { resourceId: 'owner' },
    ]);
    const inbox = await notifications.listNotifications({ threadId: 'thread' });
    expect(
      inbox.map(({ id, summary, coalescedCount }) => ({
        id,
        summary,
        coalescedCount,
      })),
    ).toEqual(
      operation === 'coalescing'
        ? [{ id: 'original', summary: 'original', coalescedCount: 1 }]
        : [],
    );
    expect(
      await notifications.readNotificationOperationReceipt('new-operation'),
    ).toBeUndefined();
  });
});

describe('schedule producer mutation admission on D1', () => {
  beforeEach(reset);
  it('keeps the due fire and trigger inventory unchanged when cutover wins the batch', async () => {
    const binding = db();
    const fence = new ExecutionFenceStore(binding);
    let armed = false;
    const storage = new D1SchedulesStorage({
      prepare: (query) => binding.prepare(query),
      async batch(statements) {
        if (armed) {
          armed = false;
          await fence.transition({
            expected: 'open',
            next: 'draining',
            expectedMutationEpoch: 1,
            expectedRevision: 2,
            advanceMutationEpoch: true,
          });
          await fence.transition({
            expected: 'draining',
            next: 'open',
            expectedMutationEpoch: 2,
            expectedRevision: 3,
          });
        }
        return binding.batch(statements as D1PreparedStatement[]);
      },
    });
    await storage.createOwnedSchedule(
      {
        id: 'schedule_a',
        target: { type: 'workflow', workflowId: 'wf' },
        cron: '* * * * *',
        status: 'active',
        nextFireAt: 100,
        createdAt: 100,
        updatedAt: 100,
        metadata: {},
        creatorRole: 'operator',
      },
      { kind: 'human', id: 'opal' },
      100,
    );
    await fence.transition({
      expected: 'open',
      next: 'draining',
      expectedMutationEpoch: 0,
      expectedRevision: 0,
      advanceMutationEpoch: true,
    });
    await fence.transition({
      expected: 'draining',
      next: 'open',
      expectedMutationEpoch: 1,
      expectedRevision: 1,
    });
    armed = true;
    await expect(
      storage.claimScheduleFire({
        mutationEpoch: 1,
        scheduleId: 'schedule_a',
        expectedNextFireAt: 100,
        newNextFireAt: 200,
        actualFireAt: 100,
        runId: 'run1',
        trigger: {
          id: 'trigger1',
          scheduleId: 'schedule_a',
          runId: 'run1',
          scheduledFireAt: 100,
          actualFireAt: 100,
          outcome: 'deferred',
        },
      }),
    ).rejects.toMatchObject({ reason: { code: 'MUTATION_EPOCH_MISMATCH' } });
    expect(await storage.getSchedule('schedule_a')).toMatchObject({
      nextFireAt: 100,
    });
    expect(await storage.listTriggers('schedule_a')).toEqual([]);
  });
});

describe('notification dispatch required-epoch refusal on D1', () => {
  beforeEach(reset);
  it('retains the inbox and receipt when activation wins a paused fence lookup', async () => {
    const binding = db();
    const fence = new ExecutionFenceStore(binding);
    const storage = new D1NotificationsStorage(binding);
    await fence.seed('open');
    const input = {
      id: 'notification',
      threadId: 'thread',
      source: 'provider',
      kind: 'update',
      summary: 'pending',
      createdAt: new Date('2026-10-19T00:00:00Z'),
    };
    const receipt = await storage.createNotificationOnce('accepted', input);
    let entered!: () => void;
    let release!: () => void;
    const paused = new Promise<void>((resolve) => {
      entered = resolve;
    });
    const resumed = new Promise<void>((resolve) => {
      release = resolve;
    });
    let armed = true;
    const wrap = (
      query: string,
      statement: ExecutionFenceStatement,
    ): ExecutionFenceStatement => ({
      bind: (...values) => wrap(query, statement.bind(...values)),
      run: () => statement.run(),
      async all<T>() {
        if (armed && query.includes('FROM flowsafe_execution_fence')) {
          armed = false;
          entered();
          await resumed;
        }
        return statement.all<T>();
      },
    });
    const gated: ExecutionFenceDatabase = {
      prepare: (query) => wrap(query, binding.prepare(query)),
    };
    let targetCalls = 0;
    const routes = createThreadSignalRoutes({
      resolveAgent() {
        targetCalls += 1;
        throw new Error('dispatch must not resolve an agent');
      },
    });
    const pending = routes(
      new Request('https://thread/signal/notifications/dispatch', {
        method: 'POST',
        body: JSON.stringify({
          agentId: 'agent',
          notificationIds: ['notification'],
          resourceId: 'resource',
        }),
      }),
      {
        threadId: 'thread',
        mutationEpoch: 0,
        principal: { kind: 'service', id: 'provider', role: 'operator' },
        init: { executionFence: new ExecutionFenceStore(gated) },
      } as unknown as ThreadScope,
    );
    await paused;
    await fence.transition({
      expected: 'open',
      next: 'draining',
      expectedMutationEpoch: 0,
      expectedRevision: 0,
      advanceMutationEpoch: true,
    });
    await fence.transition({
      expected: 'draining',
      next: 'open',
      expectedMutationEpoch: 1,
      expectedRevision: 1,
    });
    release();
    const response = await pending;
    expect(response?.status).toBe(503);
    expect(targetCalls).toBe(0);
    expect(
      await storage.getNotification({ threadId: 'thread', id: 'notification' }),
    ).toMatchObject({
      status: 'pending',
      summary: 'pending',
      deliveryAttempts: 0,
    });
    expect(
      await storage.createNotificationOnce('accepted', input, {
        mutationEpoch: 0,
      }),
    ).toEqual(receipt);
  });
});

describe('legacy draining notification admission on D1', () => {
  beforeEach(reset);
  it('records legacy inbox data but refuses a required-epoch activation before insertion', async () => {
    const binding = db();
    const fence = new ExecutionFenceStore(binding);
    await fence.seed('open');
    await fence.transition({ expected: 'open', next: 'draining' });
    const storage = new D1NotificationsStorage(binding);
    const input = {
      id: 'accepted',
      threadId: 'thread',
      source: 'provider',
      kind: 'update',
      summary: 'pending',
      createdAt: new Date('2026-10-19T00:00:00Z'),
    };
    await storage.createNotification(input);
    const receipt = await storage.createNotificationOnce('accepted-operation', {
      ...input,
      id: 'receipt',
    });
    let armed = true;
    const wrap = (
      query: string,
      statement: SignalStatement,
    ): SignalStatement => ({
      bind: (...values) => wrap(query, statement.bind(...values)),
      first: <T>() => statement.first<T>(),
      all: <T>() => statement.all<T>(),
      async run() {
        if (armed && query.includes('INSERT INTO mastra_notifications')) {
          armed = false;
          await fence.transition({
            expected: 'draining',
            next: 'open',
            expectedMutationEpoch: 0,
            expectedRevision: 1,
          });
          await fence.transition({
            expected: 'open',
            next: 'draining',
            expectedMutationEpoch: 0,
            expectedRevision: 2,
            advanceMutationEpoch: true,
          });
        }
        return statement.run();
      },
    });
    const originals = new WeakMap<SignalStatement, D1PreparedStatement>();
    const adapter: SignalDatabase = {
      prepare(query) {
        const statement = binding.prepare(query);
        const wrapped = wrap(query, statement);
        // Notification initialization batches unbound schema statements.
        originals.set(wrapped, statement);
        return wrapped;
      },
      batch: (statements) =>
        binding.batch(
          statements.map((statement) => {
            const original = originals.get(statement);
            if (!original) throw new Error('unrecognized schema statement');
            return original;
          }),
        ),
    };
    const racing = new D1NotificationsStorage(adapter);
    await expect(
      racing.createNotification({ ...input, id: 'late' }, { mutationEpoch: 0 }),
    ).rejects.toMatchObject({ reason: { code: 'MUTATION_EPOCH_MISMATCH' } });
    expect(
      (await storage.listNotifications({ threadId: 'thread' })).map(
        ({ id }) => id,
      ),
    ).toEqual(['accepted', 'receipt']);
    expect(
      await storage.createNotificationOnce(
        'accepted-operation',
        { ...input, id: 'receipt' },
        { mutationEpoch: 0 },
      ),
    ).toEqual(receipt);
    await expect(
      storage.createNotification(
        { ...input, id: 'positive' },
        { mutationEpoch: 1 },
      ),
    ).rejects.toMatchObject({ reason: { code: 'EXECUTION_FENCED' } });
  });
});

describe('admitted notification effect receipt on D1', () => {
  let fence: ExecutionFenceStore;
  let storage: D1NotificationsStorage;
  let invocation: RemoteStepInvocation;
  let input: NotificationOperationInput;
  beforeEach(async () => {
    await reset();
    const binding = db();
    fence = new ExecutionFenceStore(binding);
    await fence.seed('open');
    const journal = new RemoteStepJournal({
      db: binding,
      fence,
      runtime: {
        authoritativeStatus: async () => null,
        authoritativeStartState: async () => null,
      },
      deploymentId: 'deployment',
      tablePrefix: '',
      allowedToolIds: ['remote'],
      verifyCompletion: async () => undefined,
    });
    await journal.initialize();
    storage = journal.notificationStorage();
    await storage.init();
    invocation = {
      invocationId: 'invocation',
      deploymentId: 'deployment',
      tablePrefix: '',
      workflowId: 'workflow',
      agentId: 'agent',
      threadId: 'thread',
      resourceId: 'resource',
      runId: 'run',
      principal: { kind: 'human', id: 'owner', role: 'operator' },
      startToken: 'start',
      stepPath: ['execute'],
      suspendedAt: 100,
      resumeCount: 1,
      request: { notificationOperationId: 'enqueue' },
      deadlineAt: 10000,
      status: 'result-retained',
      permitGeneration: 1,
      permitEpoch: 0,
    };
    input = {
      id: 'notification',
      threadId: 'thread',
      resourceId: 'resource',
      agentId: 'agent',
      source: 'provider',
      kind: 'update',
      summary: 'accepted',
      sourceId: 'invocation',
      createdAt: new Date('2026-10-19T00:00:00Z'),
    };
    const projected = await projectNotificationOperation(input);
    const row: Record<string, unknown> = {
      invocation_id: invocation.invocationId,
      principal_json: JSON.stringify(invocation.principal),
      start_token: invocation.startToken,
      deployment_id: invocation.deploymentId,
      table_prefix: invocation.tablePrefix,
      workflow_id: invocation.workflowId,
      agent_id: invocation.agentId,
      thread_id: invocation.threadId,
      resource_id: invocation.resourceId,
      run_id: invocation.runId,
      step_path: JSON.stringify(invocation.stepPath),
      suspended_at: invocation.suspendedAt,
      resume_count: invocation.resumeCount,
      request_json: JSON.stringify(invocation.request),
      deadline_at: invocation.deadlineAt,
      status: 'result-retained',
      permit_generation: 1,
      permit_epoch: 0,
      target_kind: 'agent',
      suspension_marker: 'flowsafe.remote-step/v1',
      write_revision: 1,
      write_generation: 1,
      operation_key: 'operation',
      operation_id: 'enqueue',
      notification_id: input.id,
      notification_request_json: JSON.stringify(input),
      notification_projection_version: projected.version,
      notification_projection_json: projected.requestJson,
      notification_request_digest: projected.requestDigest,
      operation_state: 'effect-admitted',
      effect_admitted_at: 100,
      result_json: '{}',
      result_digest: 'a'.repeat(64),
      quiescence_digest: 'b'.repeat(64),
      executor_quiescent_at: 100,
    };
    await binding
      .prepare(
        `INSERT INTO flowsafe_remote_step_invocations (${Object.keys(row).join(', ')}) VALUES (${Object.keys(
          row,
        )
          .map(() => '?')
          .join(', ')})`,
      )
      .bind(...Object.values(row))
      .run();
  });

  it.each([
    'migration-locked',
    'proof-only',
  ] as const)('completes the admitted receipt in %s without new generic enqueue authority', async (state) => {
    await fence.transition({
      expected: 'open',
      next: 'migration-locked',
      expectedMutationEpoch: 0,
      expectedRevision: 0,
      advanceMutationEpoch: true,
    });
    if (state === 'proof-only')
      await fence.transition({
        expected: 'migration-locked',
        next: 'proof-only',
        proofKey: 'proof',
        expectedMutationEpoch: 1,
        expectedRevision: 1,
      });
    await expect(
      storage.createNotificationOnce('operation', input),
    ).rejects.toMatchObject({ reason: { code: 'MUTATION_EPOCH_MISMATCH' } });
    const receipt = await storage.createNotificationOnce('operation', input, {
      admittedInvocation: invocation,
    });
    expect(receipt).toMatchObject({
      operationKey: 'operation',
      notificationId: 'notification',
      threadId: 'thread',
    });
    expect(
      await storage.getNotification({ threadId: 'thread', id: 'notification' }),
    ).toMatchObject({ summary: 'accepted', status: 'pending' });
  });

  it('refuses a missing or mismatched held authority even while the deployment is open', async () => {
    for (const changed of [
      { invocationId: 'missing' },
      { startToken: 'different' },
      { permitGeneration: 2 },
      {
        principal: {
          kind: 'human' as const,
          id: 'other',
          role: 'operator' as const,
        },
      },
      { request: { notificationOperationId: 'other' } },
    ]) {
      await expect(
        storage.createNotificationOnce('operation', input, {
          admittedInvocation: { ...invocation, ...changed },
        }),
      ).rejects.toThrow('notification effect admission changed');
    }
    await expect(
      storage.createNotificationOnce('different-operation', input, {
        admittedInvocation: invocation,
      }),
    ).rejects.toThrow('notification effect admission changed');
    await expect(
      storage.createNotificationOnce(
        'operation',
        { ...input, summary: 'changed' },
        { admittedInvocation: invocation },
      ),
    ).rejects.toThrow('notification effect admission changed');
    expect(await storage.listNotifications({ threadId: 'thread' })).toEqual([]);
    expect(
      await storage.readNotificationOperationReceipt('operation'),
    ).toBeUndefined();
  });

  it('refuses a generation change between held-authority observation and the receipt transaction', async () => {
    const binding = db();
    let armed = true;
    const originals = new WeakMap<SignalStatement, D1PreparedStatement>();
    const wrap = (
      query: string,
      statement: D1PreparedStatement,
    ): SignalStatement => {
      const wrapped: SignalStatement = {
        bind: (...values) => wrap(query, statement.bind(...values)),
        all: <T>() => statement.all<T>(),
        run: () => statement.run(),
        async first<T>() {
          const row = await statement.first<T>();
          if (armed && query.startsWith('SELECT write_revision')) {
            armed = false;
            await binding
              .prepare(
                "UPDATE flowsafe_remote_step_invocations SET write_revision = write_revision + 1, write_generation = write_generation + 1 WHERE invocation_id = 'invocation'",
              )
              .run();
          }
          return row;
        },
      };
      originals.set(wrapped, statement);
      return wrapped;
    };
    const adapter: SignalDatabase = {
      prepare: (query) => wrap(query, binding.prepare(query)),
      batch: (statements) =>
        binding.batch(
          statements.map((statement) => {
            const original = originals.get(statement);
            if (!original) throw new Error('unrecognized prepared statement');
            return original;
          }),
        ),
    };
    const racing = new D1NotificationsStorage(adapter);
    await expect(
      racing.createNotificationOnce('operation', input, {
        admittedInvocation: invocation,
      }),
    ).rejects.toThrow('notification effect admission changed');
    expect(
      await storage.readNotificationOperationReceipt('operation'),
    ).toBeUndefined();
    expect(await storage.listNotifications({ threadId: 'thread' })).toEqual([]);
    await expect(
      storage.createNotificationOnce('operation', input, {
        admittedInvocation: invocation,
      }),
    ).resolves.toMatchObject({ notificationId: 'notification' });
  });
});
