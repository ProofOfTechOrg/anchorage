// SPDX-License-Identifier: Apache-2.0

import { expect, it, vi } from 'vitest';
import {
  createDirectReferenceContext,
  type DirectReferenceContext,
  type DirectReferenceEnvironment,
} from '../scripts/direct-reference-context.js';
import { DIRECT_REFERENCE_PATH } from '../scripts/direct-reference-contract.mjs';
import { DirectReferenceExecutionError } from '../scripts/direct-reference-http.js';
import { DirectReferenceTransport } from '../scripts/direct-reference-transport.js';
import { createDirectReferenceWorker } from '../scripts/direct-reference-worker.js';
import { directFixtureManifest } from './fixtures/direct-credentialed-config.js';

vi.mock('../scripts/direct-reference-context.js', () => ({
  createDirectReferenceContext: vi.fn(),
}));

it('classifies a raw Promise.all dispatch rejection after attempts exhaust', async () => {
  const manifest = directFixtureManifest({ maxProviderRequests: 9 });
  const nativeFetch = vi.fn<typeof fetch>(
    async () => new Response(null, { status: 204 }),
  );
  const deployments = vi.fn();
  const sentinel = new Error('raw-journal-sentinel');
  let transport: DirectReferenceTransport | undefined;
  let overflow: unknown;
  let rawRejection: unknown;
  vi.mocked(createDirectReferenceContext).mockImplementation(
    async (_manifest, _environment, invocation) => {
      const current = new DirectReferenceTransport({
        runtime: manifest.referenceRuntime,
        startedAt: invocation.startedAt,
        signal: invocation.signal,
        fetch: nativeFetch,
      });
      transport = current;
      const peers: Array<Promise<undefined>> = [];
      let selected = false;
      return {
        transport: current,
        control: { getDeployment: deployments },
        journal: {
          readOperation() {
            const peer = Promise.resolve(undefined);
            peers.push(peer);
            if (selected) return peer;
            selected = true;
            return (async () => {
              await Promise.resolve();
              await Promise.all(peers);
              for (
                let ordinal = 0;
                ordinal < manifest.referenceRuntime.maxProviderRequests;
                ordinal++
              ) {
                await current.providerFetch(
                  'https://worker-budget.fixture.test/fill',
                );
              }
              try {
                await current.providerFetch(
                  'https://worker-budget.fixture.test/overflow',
                );
              } catch (error) {
                overflow = error;
              }
              rawRejection = sentinel;
              throw sentinel;
            })();
          },
        },
      } as unknown as DirectReferenceContext;
    },
  );
  const response = await createDirectReferenceWorker(manifest).fetch(
    new Request(`https://reference.test${DIRECT_REFERENCE_PATH}`, {
      method: 'POST',
      headers: { authorization: 'Bearer test-invoke' },
      body: JSON.stringify({
        contractVersion: 1,
        configSha256: manifest.configSha256,
        action: { kind: 'control-read' },
      }),
    }),
    {
      FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET: 'test-invoke',
    } as DirectReferenceEnvironment,
  );
  expect(rawRejection).toBe(sentinel);
  expect(overflow).toBeInstanceOf(DirectReferenceExecutionError);
  expect(overflow).toMatchObject({ code: 'budget-exhausted' });
  expect(transport?.snapshot()).toMatchObject({
    failure: 'attempts',
    providerAttempts: 9,
  });
  expect(nativeFetch).toHaveBeenCalledTimes(9);
  expect(response.status).toBe(503);
  expect(await response.json()).toMatchObject({
    ok: false,
    error: { code: 'budget-exhausted' },
  });
  expect(response.headers.get('X-Direct-Provider-Attempts')).toBe('9');
  expect(response.headers.get('X-Direct-Maintenance-Attempts')).toBe('0');
  expect(response.headers.get('X-Direct-Application-Attempts')).toBe('0');
  expect(response.headers.get('Cache-Control')).toBe('no-store');
  expect(response.headers.get('Content-Type')).toContain('application/json');
  expect(deployments).not.toHaveBeenCalled();
});
