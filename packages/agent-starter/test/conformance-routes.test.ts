// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';

import { CONFORMANCE_CONTRACT } from '../src/conformance/contract.js';
import type { ConformanceCandidateEnv } from '../src/conformance/env.js';
import { mountConformanceRoutes } from '../src/conformance/routes.js';

/**
 * The refusal paths. `scripts/conformance-verify.mjs` drives every accepted
 * action against real workerd; these are the cases it never sends, where the
 * only evidence of correctness is that the candidate answers in contract shape
 * instead of leaking a stack or a bare 500.
 */

const ACTIONS_URL = `https://tenanta.example${CONFORMANCE_CONTRACT.httpPath}`;
const SOCKET_URL = `https://tenanta.example${CONFORMANCE_CONTRACT.webSocketPath}`;

function env(overrides: Partial<ConformanceCandidateEnv> = {}) {
  return {
    DEPLOYMENT_TENANT: 'tenanta',
    DEPLOYMENT_IDENTITY_SECRET: 'conformance-test-deployment-identity-secret',
    APPLICATION_MODE: CONFORMANCE_CONTRACT.applicationVariableValue,
    APPLICATION_CONFORMANCE_SECRET: 'conformance-test-application-secret',
    ...overrides,
  } as unknown as ConformanceCandidateEnv;
}

function post(body: unknown): Request {
  return new Request(ACTIONS_URL, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
}

describe('conformance candidate routes', () => {
  it('ignores every path outside the contract', async () => {
    const response = await mountConformanceRoutes(
      new Request('https://tenanta.example/api/anything'),
      env(),
    );
    expect(response).toBeNull();
  });

  it.each([
    ['GET', 405],
    ['PUT', 405],
  ])('refuses %s on the action endpoint', async (method, status) => {
    const response = await mountConformanceRoutes(
      new Request(ACTIONS_URL, { method }),
      env(),
    );
    expect(response?.status).toBe(status);
  });

  it.each([
    [{ contractVersion: 2, action: 'cpu-control' }],
    [{ action: 'cpu-control' }],
  ])('refuses another contract version (%o)', async (body) => {
    const response = await mountConformanceRoutes(post(body), env());
    expect(response?.status).toBe(400);
  });

  it('refuses a body that is not an action request', async () => {
    const malformed = new Request(ACTIONS_URL, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: '{',
    });
    expect((await mountConformanceRoutes(malformed, env()))?.status).toBe(400);
    expect(
      (await mountConformanceRoutes(post({ contractVersion: 1 }), env()))
        ?.status,
    ).toBe(400);
  });

  it('answers an unknown action in contract shape rather than a bare failure', async () => {
    const response = await mountConformanceRoutes(
      post({ contractVersion: 1, action: 'not-an-action' }),
      env(),
    );
    expect(response?.status).toBe(500);
    await expect(response?.json()).resolves.toEqual({
      contractVersion: 1,
      action: 'not-an-action',
      failed: true,
    });
  });

  it('answers state-new-class in contract shape before the v2 binding exists', async () => {
    // Release one has no CONFORMANCE_V2 binding. The gate never asks this early,
    // so the value of answering in shape is that a premature ask names the
    // action instead of surfacing an undefined-namespace crash.
    const response = await mountConformanceRoutes(
      post({ contractVersion: 1, action: 'state-new-class', nonce: 'probe' }),
      env(),
    );
    expect(response?.status).toBe(409);
    await expect(response?.json()).resolves.toEqual({
      contractVersion: 1,
      action: 'state-new-class',
      nonce: 'probe',
      stored: false,
    });
  });

  it('requires a WebSocket upgrade on the socket path', async () => {
    const response = await mountConformanceRoutes(
      new Request(SOCKET_URL),
      env(),
    );
    expect(response?.status).toBe(426);
  });
});
