// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  createTestHarness,
  type TestHarness,
  type WorkerHandle,
} from 'wrangler';

const ROOT = new URL('..', import.meta.url).pathname;
const PROBE = new URL('./fixtures/r2-export-harness-probe.ts', import.meta.url)
  .pathname;

function harnessOptions() {
  return {
    root: ROOT,
    workers: [
      {
        config: {
          name: 'r2-export-harness-probe',
          main: PROBE,
          compatibility_date: '2026-08-06',
          r2_buckets: [
            {
              binding: 'EXPORTS',
              bucket_name: 'fleet-r2-export-harness',
            },
          ],
        },
      },
    ],
  } satisfies Parameters<typeof createTestHarness>[0];
}

function field(value: unknown, name: string): unknown {
  if (typeof value !== 'object' || value === null) {
    throw new Error('probe returned a non-object response');
  }
  return Reflect.get(value, name);
}

describe.sequential('R2DatabaseExportStore Wrangler harness', {
  // The hooks below repeat this value because hooks take vitest's
  // hookTimeout, not this option; every title inherits it.
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

  async function probe(action: string): Promise<unknown> {
    const response = await worker.fetch('/r2-export', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ action }),
    });
    const body: unknown = await response.json();
    if (!response.ok) {
      throw new Error(`probe failed: ${JSON.stringify(body)}`);
    }
    return body;
  }

  it('streams and independently verifies a deterministic large export', async () => {
    const result = await probe('success');
    expect(result).toMatchObject({
      size: 1_048_576,
      bytesEqual: true,
      objectSize: 1_048_576,
      cleaned: true,
    });
    expect(field(result, 'location')).toMatch(
      /^r2:\/\/exports\/exports\/success-db\/.+-export\.sqlite3$/,
    );
    expect(field(result, 'sha256')).toMatch(/^[0-9a-f]{64}$/);
    expect(field(result, 'readbackSha256')).toBe(field(result, 'sha256'));
  });

  it('refuses an empty export without creating an object', async () => {
    await expect(probe('empty')).resolves.toEqual({
      message: 'R2 export refuses an empty body',
      objectCount: 0,
    });
  });

  it('rejects a short body without leaving an object', async () => {
    await expect(probe('short')).resolves.toEqual({
      message: 'R2 export upload failed',
      objectCount: 0,
    });
  });

  it('rejects a mid-transfer source error without leaving an object', async () => {
    await expect(probe('mid-transfer-error')).resolves.toEqual({
      message: 'R2 export upload failed',
      objectCount: 0,
      pullCount: 2,
      bytesEnqueued: 4096,
    });
  });

  it('preserves the winner of a conditional collision', async () => {
    const result = await probe('collision');
    expect(result).toMatchObject({
      fulfilled: 1,
      rejected: 1,
      message: 'R2 export key already exists',
      objectSurvived: true,
      cleaned: true,
    });
    expect(['first', 'second']).toContain(field(result, 'winner'));
  });

  it('replays one stable receipt without creating another R2 object', async () => {
    const result = await probe('receipt-replay');
    expect(result).toMatchObject({
      sameResult: true,
      objectCount: 1,
      bytesEqual: true,
      cleaned: true,
      customMetadata: {
        anchorageReceiptVersion: '1',
        anchorageReceiptAuthority: 'r2://exports/exports/receipts/v1',
        anchorageDatabaseId: '11111111-1111-1111-1111-111111111111',
        anchorageOperationId: '22222222-2222-4222-8222-222222222222',
      },
    });
    expect(field(result, 'location')).toBe(
      'r2://exports/exports/receipts/v1/11111111-1111-1111-1111-111111111111/22222222-2222-4222-8222-222222222222.sql',
    );
  });

  it('converges matching concurrent receipt attempts to one winner', async () => {
    const result = await probe('receipt-concurrent');
    expect(result).toMatchObject({
      sameResult: true,
      objectCount: 1,
      bytesEqual: true,
      cleaned: true,
    });
    expect(field(result, 'location')).toBe(
      'r2://exports/exports/receipts/v1/11111111-1111-1111-1111-111111111111/33333333-3333-4333-8333-333333333333.sql',
    );
  });

  it('preserves and refuses a same-operation receipt with different bytes', async () => {
    const result = await probe('receipt-mismatch');
    expect(result).toMatchObject({
      message:
        'database export receipt collision differs from the committed export',
      objectCount: 1,
      winnerPreserved: true,
      challengerRejected: true,
      cleaned: true,
    });
    expect(field(result, 'location')).toBe(
      'r2://exports/exports/receipts/v1/11111111-1111-1111-1111-111111111111/44444444-4444-4444-8444-444444444444.sql',
    );
  });
});
