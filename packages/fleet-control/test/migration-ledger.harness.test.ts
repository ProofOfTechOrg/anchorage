// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  createTestHarness,
  type TestHarness,
  type WorkerHandle,
} from 'wrangler';

const ROOT = new URL('..', import.meta.url).pathname;
const PROBE = new URL(
  './fixtures/migration-ledger-harness-probe.ts',
  import.meta.url,
).pathname;

interface ProbeError {
  readonly name: string;
  readonly message: string;
}

function harnessOptions() {
  return {
    root: ROOT,
    workers: [
      {
        config: {
          name: 'migration-ledger-harness-probe',
          main: PROBE,
          compatibility_date: '2026-08-06',
          compatibility_flags: ['nodejs_compat'],
          d1_databases: [
            {
              binding: 'DB',
              database_name: 'migration-ledger-harness',
              database_id: '00000000-0000-0000-0000-000000000000',
            },
          ],
        },
      },
    ],
  } satisfies Parameters<typeof createTestHarness>[0];
}

describe.sequential('migration ledger real-D1 fidelity', {
  timeout: 30_000,
}, () => {
  let server: TestHarness;
  let worker: WorkerHandle;

  beforeAll(async () => {
    server = createTestHarness(harnessOptions());
    await server.listen();
    worker = server.getWorker();
  }, 30_000);

  afterAll(async () => {
    await server.close();
  }, 30_000);

  async function probe<T>(action: string): Promise<T> {
    const response = await worker.fetch('/migration-ledger', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ action }),
    });
    const body = (await response.json()) as T | { error: ProbeError };
    if (!response.ok) {
      throw new Error(`probe failed: ${JSON.stringify(body)}`);
    }
    return body as T;
  }

  it('atomically rolls back migration SQL when the ledger write fails', async () => {
    await expect(
      probe<{
        failure: ProbeError;
        values: number;
        ledger: number;
      }>('atomic-rollback'),
    ).resolves.toEqual({
      failure: {
        name: 'Error',
        message: 'failed to apply D1 migration 1',
      },
      values: 0,
      ledger: 0,
    });
  });

  it('converges concurrent applications of one version to one commit', async () => {
    await expect(
      probe<{
        fulfilled: number;
        rejected: ProbeError[];
        values: number;
        ledger: number;
      }>('concurrent-application'),
    ).resolves.toEqual({
      fulfilled: 12,
      rejected: [],
      values: 1,
      ledger: 1,
    });
  });

  it('converges concurrent first applications on a cold ledger', async () => {
    await expect(
      probe<{
        coldBefore: number;
        settlements: Array<
          { status: 'fulfilled' } | { status: 'rejected'; message: string }
        >;
        values: number;
        ledger: number;
      }>('cold-application'),
    ).resolves.toEqual({
      coldBefore: 0,
      settlements: [{ status: 'fulfilled' }, { status: 'fulfilled' }],
      values: 1,
      ledger: 1,
    });
  });

  it('rejects changed SQL for an already committed historical version', async () => {
    await expect(
      probe<{ failure: ProbeError; values: string[] }>('changed-history'),
    ).resolves.toEqual({
      failure: {
        name: 'Error',
        message: 'D1 migration 1 was already applied with different SQL',
      },
      values: ['original'],
    });
  });

  it('preserves the committed schema, data, and ledger across fetch boundaries', async () => {
    await expect(probe('commit-boundary')).resolves.toEqual({
      committed: true,
    });
    await expect(
      probe<{ table: number; values: string[]; ledger: number }>(
        'read-boundary',
      ),
    ).resolves.toEqual({ table: 1, values: ['durable'], ledger: 1 });
  });
});
