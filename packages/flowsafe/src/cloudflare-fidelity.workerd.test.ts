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
import { D1NotificationsStorage } from './signals/notifications-d1.js';

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
  barrier?: { id: string; participant: string },
) {
  return actor.fetch(
    new Request('http://localhost/receipt', {
      method: 'POST',
      body: JSON.stringify({
        operationKey,
        notificationId,
        receiptLimit,
        barrierId: barrier?.id,
        participant: barrier?.participant,
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
});
