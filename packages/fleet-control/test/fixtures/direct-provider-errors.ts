// SPDX-License-Identifier: Apache-2.0

import { APIConnectionTimeoutError } from 'cloudflare';
import { expect } from 'vitest';
import { DirectReferenceTransport } from '../../scripts/direct-reference-transport.js';
import {
  advanceCloudflareWorkerAttachmentScan,
  CloudflareProviderRequestNotDispatchedError,
  CloudflareProvisioningClient,
  withProviderDispatchTracking,
} from '../../src/cloudflare-client.js';
import {
  CloudflareAttachmentScanDriftError,
  CloudflareAttachmentScanProgressError,
  initialWorkerAttachmentScan,
  parseWorkerAttachmentScanProgress,
} from '../../src/cloudflare-worker-attachment-scan.js';
import {
  pageArray,
  recordingFetch,
  single,
  testRateCoordinator,
} from './cloudflare-fetch-fixture.js';

async function rejection(
  operation: () => unknown | Promise<unknown>,
): Promise<Error> {
  try {
    await operation();
  } catch (error) {
    if (!(error instanceof Error)) throw error;
    return error;
  }
  throw new Error('provider fixture did not reject');
}

function client(fetch: typeof globalThis.fetch) {
  return new CloudflareProvisioningClient({
    accountId: 'account',
    apiToken: 'inert-token',
    plane: 'plain-worker',
    rateCoordinator: testRateCoordinator(),
    fetch,
    requestTimeoutMs: 1000,
  });
}

async function metadata(incomplete: boolean) {
  const fixture = recordingFetch(() =>
    single({
      name: incomplete ? 'other-bucket' : 'fixture-bucket',
      jurisdiction: 'default',
      creation_date: 'invalid',
    }),
  );
  return rejection(() =>
    client(fixture.fetch).getR2Bucket('fixture-bucket', 'default'),
  );
}

async function drift() {
  let inventories = 0;
  const fixture = recordingFetch(({ url }) => {
    const path = new URL(url).pathname;
    if (path.endsWith('/workers/scripts'))
      return pageArray([{ id: ++inventories === 1 ? 'ordinary' : 'changed' }]);
    if (path.endsWith('/deployments'))
      return single({
        deployments: [
          {
            versions: [
              { version_id: 'v1', percentage: 50 },
              { version_id: 'v2', percentage: 50 },
            ],
          },
        ],
      });
    if (path.includes('/versions/'))
      return single({ resources: { bindings: [] } });
    throw new Error(`unexpected fixture request ${path}`);
  });
  const subject = client(fixture.fetch);
  const target = { kind: 'd1', databaseId: 'target-db' } as const;
  const first = await advanceCloudflareWorkerAttachmentScan(subject, {
    target,
    progress: initialWorkerAttachmentScan(target),
    maxProviderRequests: 9,
  });
  expect(first.status).toBe('pending');
  if (first.status !== 'pending')
    throw new Error('scan did not retain progress');
  const error = await rejection(() =>
    advanceCloudflareWorkerAttachmentScan(subject, {
      target,
      progress: first.progress,
      maxProviderRequests: 9,
    }),
  );
  expect(error).toBeInstanceOf(CloudflareAttachmentScanDriftError);
  return error;
}

async function progress() {
  const error = await rejection(() =>
    parseWorkerAttachmentScanProgress(
      { version: 2 },
      { kind: 'd1', databaseId: 'target-db' },
    ),
  );
  expect(error).toBeInstanceOf(CloudflareAttachmentScanProgressError);
  return error;
}

export const undispatchedCause = new Error('private SQL/fence cause', {
  cause: 'private-token',
});
async function notDispatched() {
  const fixture = recordingFetch(() => single({}));
  const error = await rejection(() =>
    withProviderDispatchTracking(client(fixture.fetch), async () => {
      throw undispatchedCause;
    }),
  );
  expect(error).toBeInstanceOf(CloudflareProviderRequestNotDispatchedError);
  expect(error.cause).toBe(undispatchedCause);
  expect(fixture.requests).toEqual([]);
  return error;
}

async function timeout(sdk: boolean) {
  const transport = new DirectReferenceTransport({
    runtime: {
      requestTimeoutMs: 5,
      invocationTimeoutMs: 60_000,
      maxProviderRequests: 100,
    },
    startedAt: performance.now(),
    signal: new AbortController().signal,
    fetch: async (input, init) => {
      const request = new Request(input, init);
      return new Promise<Response>((_resolve, reject) => {
        const abort = () => reject(request.signal.reason);
        if (request.signal.aborted) abort();
        else request.signal.addEventListener('abort', abort, { once: true });
      });
    },
  });
  const error = await rejection(() =>
    sdk
      ? client(transport.providerFetch).getR2Bucket('fixture-bucket', 'default')
      : transport.providerFetch('https://provider.fixture.test/timeout'),
  );
  expect(transport.snapshot().failure).toBeNull();
  expect(transport.snapshot().providerAttempts).toBeGreaterThan(0);
  expect(error).toBeInstanceOf(sdk ? APIConnectionTimeoutError : DOMException);
  return error;
}

export const providerRefusalCases = [
  { name: 'incomplete metadata', produce: () => metadata(true) },
  { name: 'invalid creation date', produce: () => metadata(false) },
  { name: 'attachment drift', produce: drift },
  { name: 'attachment progress', produce: progress },
  { name: 'not dispatched', produce: notDispatched },
  { name: 'native request timeout', produce: () => timeout(false) },
  { name: 'SDK request timeout', produce: () => timeout(true) },
] as const;
