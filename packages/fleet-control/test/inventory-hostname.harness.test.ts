// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  createTestHarness,
  type TestHarness,
  type WorkerHandle,
} from 'wrangler';

const ROOT = new URL('..', import.meta.url).pathname;
const PROBE = new URL(
  './fixtures/inventory-hostname-harness-probe.ts',
  import.meta.url,
).pathname;

describe.sequential('inventory hostnames in workerd', {
  timeout: 30_000,
}, () => {
  let server: TestHarness;
  let worker: WorkerHandle;

  beforeAll(async () => {
    server = createTestHarness({
      root: ROOT,
      workers: [
        {
          config: {
            name: 'inventory-hostname-probe',
            main: PROBE,
            compatibility_date: '2026-08-06',
          },
        },
      ],
    });
    await server.listen();
    worker = server.getWorker();
  }, 30_000);

  afterAll(async () => {
    await server.close();
  }, 30_000);

  it('creates distinct base64url reservation nonces through the allowed crypto API', async () => {
    const response = await worker.fetch('/reservation');
    expect(response.status).toBe(200);
    const body = (await response.json()) as {
      first: Array<{ reservationNonce: string; bucketName: string }>;
      second: Array<{ reservationNonce: string; bucketName: string }>;
      secrets: { deploymentIdentity: string; maintenanceAdmin: string };
    };
    for (const value of [
      body.secrets.deploymentIdentity,
      body.secrets.maintenanceAdmin,
    ]) {
      expect(value).toMatch(/^[A-Za-z0-9_-]{43}$/);
      expect(Buffer.from(value, 'base64url')).toHaveLength(32);
    }
    expect(body.secrets.deploymentIdentity).not.toBe(
      body.secrets.maintenanceAdmin,
    );
    expect(body.first).toHaveLength(1);
    expect(body.second).toHaveLength(1);
    for (const resource of [...body.first, ...body.second]) {
      expect(resource.reservationNonce).toMatch(/^[A-Za-z0-9_-]{32}$/);
      expect(Buffer.from(resource.reservationNonce, 'base64url')).toHaveLength(
        24,
      );
      expect(resource.bucketName).toMatch(/^fleet-tenanta-prod-[0-9a-f]{20}$/);
    }
    expect(body.first[0]?.reservationNonce).not.toBe(
      body.second[0]?.reservationNonce,
    );
    expect(body.first[0]?.bucketName).not.toBe(body.second[0]?.bucketName);
  });

  it.each([
    'bücher.example',
    '例子.example',
    'ｘ',
    'ｙ',
    'ｌｏｃａｌｈｏｓｔ',
    '\u200b.example',
    'ASCII.example',
  ])('preserves the recorded hostname %j', async (hostname) => {
    const response = await worker.fetch(
      `/?hostname=${encodeURIComponent(hostname)}`,
    );
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({
      rows: [
        { kind: 'route', payload: { hostname } },
        {
          kind: 'finding',
          payload: {
            kind: 'stale-route',
            detail: `custom domain '${hostname}' points to a missing or incomplete plain Worker 'anchorage-missing'`,
          },
        },
      ],
      providerRequests: 3,
    });
  });

  it.each([
    'user@é.example\t',
    'é.example:80\t',
    'bad\u0000é.example',
  ])('retains finding refusal for %j', async (hostname) => {
    const response = await worker.fetch(
      `/?hostname=${encodeURIComponent(hostname)}`,
    );
    expect(response.status).toBe(400);
    expect(await response.json()).toEqual({
      error: 'FleetInventoryFindingValueError',
    });
  });
});
