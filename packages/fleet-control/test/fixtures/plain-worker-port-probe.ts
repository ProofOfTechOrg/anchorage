// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { vi } from 'vitest';
import type { DurableDatabaseExportStore } from '../../src/cloudflare-client.js';
import type {
  ExternalMutationFence,
  PlainWorkerRouteApi,
} from '../../src/types.js';

export function routeApi(
  overrides: Partial<PlainWorkerRouteApi> = {},
): PlainWorkerRouteApi {
  return {
    async withMutationFence(fence, operation) {
      // Models the tolerated legacy entry-asserting shape from
      // test/wrangler-loop-backend.test.ts:201-206. Real adapters are required
      // only to assert each mutating request; B2's conformance fixture should
      // default to the non-asserting production shape.
      await fence.assertOwned();
      return operation();
    },
    async queryDatabase() {
      return [];
    },
    async batchDatabase() {},
    async listWorkerDatabaseAttachments() {
      return [];
    },
    async inspectActiveWorkerRoute() {
      return undefined;
    },
    async listCustomDomains() {
      return [];
    },
    async inspectOrdinaryWorkerFootprint() {
      return { scriptPresent: false, customDomains: [], zoneRoutes: [] };
    },
    async listDurableObjectNamespaces() {
      return [];
    },
    async listOrdinaryWorkerSecretNames() {
      return [];
    },
    async deleteControlSecrets() {},
    async attachCustomDomain() {},
    async detachCustomDomain() {},
    async disableOrdinaryWorkerPublicAccess() {},
    ...overrides,
  };
}

export function mutationFence(
  assertOwned = vi.fn(async () => {}),
): ExternalMutationFence {
  return { mutationLeaseTtlMs: 15 * 60_000, assertOwned };
}

export async function drain(body: ReadableStream<Uint8Array>): Promise<{
  readonly size: number;
  readonly sha256: string;
}> {
  const chunks: Uint8Array[] = [];
  const reader = body.getReader();
  for (;;) {
    const chunk = await reader.read();
    if (chunk.done) break;
    chunks.push(chunk.value);
  }
  const bytes = Buffer.concat(chunks);
  return {
    size: bytes.byteLength,
    sha256: createHash('sha256').update(bytes).digest('hex'),
  };
}

export function memoryStore(
  location = 'memory://export',
): DurableDatabaseExportStore {
  return {
    async write(input) {
      const committed = await drain(input.body);
      return { location, size: committed.size, sha256: committed.sha256 };
    },
  };
}
