// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import { deploymentIdentityHeaders } from '../do-runner/index.js';
import {
  AUDIT_PROXY_INSTANCE_NAME,
  createAuditProxyDurableObjectBinding,
  createAuditProxyHandler,
  createAuditProxyQueue,
  createAuditProxyServiceBinding,
  FlowsafeFleetAuditProxy,
} from './audit-proxy.js';

const SECRET = 'audit-proxy-deployment-secret-0000001';
const OTHER_SECRET = 'other-audit-proxy-deployment-secret-01';

function event() {
  return {
    actor: null,
    action: 'approval.decide',
    resource: 'approval:one',
    decision: 'allowed',
    detail: { deploymentTag: 'victim' },
    tenantTag: 'victim',
    environment: 'victim-environment',
    scriptName: 'victim-script',
    fleetAttribution: { tenantTag: 'victim' },
  } as const;
}

function proxy(send = vi.fn(async () => {})) {
  return {
    send,
    handler: createAuditProxyHandler({
      queue: { send },
      deploymentIdentitySecret: SECRET,
      attribution: {
        tenantTag: 'acme',
        environment: 'production',
        scriptName: 'acme-prod',
      },
    }),
  };
}

describe('trusted audit proxy', () => {
  it('overwrites candidate-supplied attribution before shared enqueue', async () => {
    const { handler, send } = proxy();
    const response = await handler(
      new Request('http://audit-proxy/internal/audit', {
        method: 'POST',
        headers: deploymentIdentityHeaders(SECRET),
        body: JSON.stringify(event()),
      }),
    );

    expect(response.status).toBe(204);
    expect(send).toHaveBeenCalledWith({
      fleetAttribution: {
        source: 'external-candidate-via-trusted-proxy',
        eventTrust: 'untrusted',
        tenantTag: 'acme',
        environment: 'production',
        scriptName: 'acme-prod',
      },
      event: event(),
    });
  });

  it('rejects cross-deployment, malformed, and oversized events', async () => {
    const { handler, send } = proxy();
    const request = (body: string, secret = SECRET) =>
      new Request('http://audit-proxy/internal/audit', {
        method: 'POST',
        headers: deploymentIdentityHeaders(secret),
        body,
      });

    expect(
      (await handler(request(JSON.stringify(event()), OTHER_SECRET))).status,
    ).toBe(401);
    expect((await handler(request('{'))).status).toBe(400);
    expect(
      (await handler(request(JSON.stringify({ action: 'missing-fields' }))))
        .status,
    ).toBe(400);
    expect((await handler(request('x'.repeat(120 * 1_024 + 1)))).status).toBe(
      413,
    );
    expect(send).not.toHaveBeenCalled();
  });

  it('rejects malformed static attribution before accepting requests', () => {
    expect(() =>
      createAuditProxyHandler({
        queue: { send: vi.fn(async () => {}) },
        deploymentIdentitySecret: SECRET,
        attribution: {
          tenantTag: 'acme',
          environment: 'Production',
          scriptName: 'acme-prod',
        },
      }),
    ).toThrow(/attribution is invalid/);
  });

  it('fixes a remote namespace to the one audit object', async () => {
    const acceptedFetch = vi.fn(
      async (_request: Request) => new Response(null, { status: 204 }),
    );
    const idFromName = vi.fn((name: string) => `id:${name}`);
    const get = vi.fn(() => ({ fetch: acceptedFetch }));
    const queue = createAuditProxyQueue(
      createAuditProxyDurableObjectBinding({ idFromName, get }),
      SECRET,
    );
    await expect(queue.send(event())).resolves.toBeUndefined();
    expect(idFromName).toHaveBeenCalledWith(AUDIT_PROXY_INSTANCE_NAME);
    expect(get).toHaveBeenCalledWith(`id:${AUDIT_PROXY_INSTANCE_NAME}`);
    const request = acceptedFetch.mock.calls[0]?.[0];
    expect(request?.headers.get('x-flowsafe-deployment-identity')).toBe(SECRET);
    expect(await request?.json()).toEqual(event());
  });

  it('supports getByName without exposing an object selector', async () => {
    const acceptedFetch = vi.fn(
      async (_request: Request) => new Response(null, { status: 204 }),
    );
    const getByName = vi.fn(() => ({ fetch: acceptedFetch }));
    const binding = createAuditProxyDurableObjectBinding({ getByName });

    await binding.fetch(new Request('http://audit-proxy/internal/audit'));

    expect(getByName).toHaveBeenCalledWith(AUDIT_PROXY_INSTANCE_NAME);
    expect(Object.keys(binding)).toEqual(['fetch']);
  });

  it('keeps ordinary service compatibility explicit and surfaces rejection', async () => {
    const rejectedService = createAuditProxyServiceBinding({
      fetch: async () => new Response(null, { status: 401 }),
    });

    const rejected = createAuditProxyQueue(rejectedService, SECRET);
    await expect(rejected.send(event())).rejects.toThrow(
      /rejected event with 401/,
    );
  });

  it('exports the trusted fixed-singleton Durable Object host', async () => {
    const send = vi.fn(async () => {});
    const instance = new FlowsafeFleetAuditProxy(
      { id: { name: AUDIT_PROXY_INSTANCE_NAME } },
      {
        AUDIT_QUEUE: { send },
        DEPLOYMENT_IDENTITY_SECRET: SECRET,
        DEPLOYMENT_TENANT: 'acme',
        FLEET_ENVIRONMENT: 'production',
        FLEET_DEPLOYMENT_SCRIPT: 'acme-prod',
      },
    );
    const response = await instance.fetch(
      new Request('http://audit-proxy/internal/audit', {
        method: 'POST',
        headers: deploymentIdentityHeaders(SECRET),
        body: JSON.stringify(event()),
      }),
    );

    expect(response.status).toBe(204);
    expect(send).toHaveBeenCalledWith(
      expect.objectContaining({
        fleetAttribution: expect.objectContaining({ tenantTag: 'acme' }),
      }),
    );
    const wrongInstance = new FlowsafeFleetAuditProxy(
      { id: { name: 'candidate-selected-object' } },
      {
        AUDIT_QUEUE: { send },
        DEPLOYMENT_IDENTITY_SECRET: SECRET,
        DEPLOYMENT_TENANT: 'acme',
        FLEET_ENVIRONMENT: 'production',
        FLEET_DEPLOYMENT_SCRIPT: 'acme-prod',
      },
    );
    await expect(
      wrongInstance.fetch(new Request('http://audit-proxy/internal/audit')),
    ).rejects.toThrow(/must be addressed/);
  });
});
