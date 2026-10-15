// SPDX-License-Identifier: Apache-2.0

import type {
  SignalDatabase,
  SignalStatement,
} from '../src/signals/d1-shared.js';
import { D1NotificationsStorage } from '../src/signals/notifications-d1.js';

async function waitForReceiptReaders(
  db: D1Database,
  hub: DurableObjectNamespace,
  barrierId: string,
  participant: string,
): Promise<void> {
  await db
    .prepare(
      'INSERT INTO notification_test_barrier (barrier_id, participant) VALUES (?, ?)',
    )
    .bind(barrierId, participant)
    .run();
  const response = await hub.get(hub.idFromName(barrierId)).fetch(
    new Request('http://localhost/barrier', {
      method: 'POST',
      body: JSON.stringify({ barrierId }),
    }),
  );
  if (!response.ok) throw new Error('notification receipt barrier timed out');
}

function receiptBarrierDatabase(
  db: D1Database,
  hub: DurableObjectNamespace,
  barrierId: string,
  participant: string,
  phase: 'receipt-read' | 'scope-census',
): SignalDatabase {
  let barrierEntered = false;
  function intercept(statement: SignalStatement): SignalStatement {
    return {
      bind(...values) {
        return intercept(statement.bind(...values));
      },
      async first<T>() {
        const row = await statement.first<T>();
        if (phase === 'receipt-read' && row === null && !barrierEntered) {
          barrierEntered = true;
          await waitForReceiptReaders(db, hub, barrierId, participant);
        }
        return row;
      },
      async all<T>() {
        const rows = await statement.all<T>();
        if (phase === 'scope-census' && !barrierEntered) {
          barrierEntered = true;
          await waitForReceiptReaders(db, hub, barrierId, participant);
        }
        return rows;
      },
      run() {
        return statement.run();
      },
    };
  }
  return {
    prepare(query) {
      const statement = db.prepare(query);
      return (
        phase === 'scope-census'
          ? query.includes('SELECT name FROM sqlite_master')
          : query.includes('SELECT request_digest, thread_id, notification_id')
      )
        ? intercept(statement)
        : statement;
    },
    batch: db.batch.bind(db) as SignalDatabase['batch'],
  };
}

class TestDurableObject {
  fetch(_request: Request): Response | Promise<Response> {
    return new Response(null, { status: 204 });
  }
}

export class DemoRunner extends TestDurableObject {}
export class DemoHub extends TestDurableObject {
  #waiting?: {
    resolve: (response: Response) => void;
    timeout: ReturnType<typeof setTimeout>;
  };

  constructor(
    _state: DurableObjectState,
    private readonly bindings: { DB: D1Database },
  ) {
    super();
  }

  async fetch(request: Request): Promise<Response> {
    const { barrierId } = (await request.json()) as { barrierId: string };
    const ready = await this.bindings.DB.prepare(
      'SELECT COUNT(*) AS count FROM notification_test_barrier WHERE barrier_id = ?',
    )
      .bind(barrierId)
      .first<{ count: number }>();
    if (this.#waiting) {
      const waiting = this.#waiting;
      this.#waiting = undefined;
      clearTimeout(waiting.timeout);
      const status = ready?.count === 2 ? 200 : 409;
      waiting.resolve(Response.json({ ready: ready?.count }, { status }));
      return Response.json({ ready: ready?.count }, { status });
    }
    return new Promise<Response>((resolve) => {
      const timeout = setTimeout(() => {
        this.#waiting = undefined;
        resolve(new Response(null, { status: 504 }));
      }, 10_000);
      this.#waiting = { resolve, timeout };
    });
  }
}
export class DemoThread extends TestDurableObject {}
export class DemoBackgroundTasks extends TestDurableObject {}
export class DemoSignalProviderHost extends TestDurableObject {
  constructor(
    _state: DurableObjectState,
    private readonly bindings: {
      DB: D1Database;
      HUB: DurableObjectNamespace;
    },
  ) {
    super();
  }

  async fetch(request: Request): Promise<Response> {
    const input = (await request.json()) as {
      operationKey: string;
      notificationId: string;
      receiptLimit: number;
      tablePrefix?: string;
      barrierId?: string;
      participant?: string;
      barrierPhase?: 'receipt-read' | 'scope-census';
    };
    const database =
      input.barrierId && input.participant
        ? receiptBarrierDatabase(
            this.bindings.DB,
            this.bindings.HUB,
            input.barrierId,
            input.participant,
            input.barrierPhase ?? 'receipt-read',
          )
        : this.bindings.DB;
    const notifications = new D1NotificationsStorage(
      database,
      input.tablePrefix ?? '',
      input.receiptLimit,
    );
    try {
      const receipt = await notifications.createNotificationOnce(
        input.operationKey,
        {
          id: input.notificationId,
          threadId: 'thread',
          source: 'remote-step',
          kind: 'result',
          summary: 'Completed',
          createdAt: new Date('2026-10-13T00:00:00.000Z'),
        },
      );
      return Response.json(receipt);
    } catch (error) {
      return Response.json(
        { error: error instanceof Error ? error.message : String(error) },
        { status: 409 },
      );
    }
  }
}

export default {
  fetch(): Response {
    return new Response(null, { status: 204 });
  },
};
