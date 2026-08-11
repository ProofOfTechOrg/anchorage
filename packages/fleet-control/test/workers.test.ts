// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import {
  type MaintenanceCapabilityJwk,
  mintAsymmetricMaintenanceCapability,
  STATE_EGRESS_HEADERS,
} from '@proofoftech/flowsafe/host-kit';
import { afterEach, describe, expect, it, vi } from 'vitest';
import auditConsumer from '../src/workers/audit-consumer.js';
import dispatchWorker from '../src/workers/dispatch.js';
import outboundWorker, {
  createEgressProxyFetch,
  StateEgress,
} from '../src/workers/outbound.js';

const MAINTENANCE_PRIVATE_KEY = {
  kty: 'OKP',
  crv: 'Ed25519',
  alg: 'EdDSA',
  kid: 'fleet-maintenance-v1',
  x: 'Lhp1XFeTJJx8FLOCKpn4nkO-tWuZZxXX8ziw0LEvUZo',
  d: 'gkXf8_b8kcCJxZ33fUYUac7yCsxZAxQXgsgPbwDpnlM',
} satisfies MaintenanceCapabilityJwk;
const MAINTENANCE_PUBLIC_KEY = JSON.stringify({
  kty: 'OKP',
  crv: 'Ed25519',
  alg: 'EdDSA',
  kid: MAINTENANCE_PRIVATE_KEY.kid,
  x: MAINTENANCE_PRIVATE_KEY.x,
});
const MAINTENANCE_SPEC_DIGEST = 'a'.repeat(64);

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

function policyContext(options?: {
  readonly allowedHosts?: readonly string[];
  readonly environment?: string;
  readonly policyId?: string;
  readonly scriptName?: string;
  readonly tenantTag?: string;
}) {
  const allowedHosts = [
    ...(options?.allowedHosts ?? ['api.example.com']),
  ].sort();
  const context = {
    policyId: options?.policyId ?? 'policy-acme-production',
    tenantTag: options?.tenantTag ?? 'acme',
    environment: options?.environment ?? 'production',
    allowedHosts,
  };
  return {
    scriptName: options?.scriptName ?? 'acme-prod',
    tenantTag: context.tenantTag,
    environment: context.environment,
    policyId: context.policyId,
    policyDigest: createHash('sha256')
      .update(JSON.stringify(context))
      .digest('hex'),
    policyHosts: JSON.stringify(allowedHosts),
  };
}

function routingTarget(options?: Parameters<typeof policyContext>[0]) {
  const context = policyContext(options);
  return {
    ...context,
    policyHosts: JSON.parse(context.policyHosts) as string[],
  };
}

function deploymentProxyContext(
  target = routingTarget(),
): Parameters<typeof outboundWorker.fetch>[1] {
  return {
    HOSTS: { get: vi.fn(async () => JSON.stringify(target)) },
    routeHostname: 'dispatch.example.test',
    scriptName: 'acme-production-state-a1b2c3d4',
    tenantTag: 'acme',
    environment: 'production',
    policyId: 'policy-acme-production',
    resourceRole: 'platform-state',
  };
}

describe('platform Workers', () => {
  it('fails closed for unknown dispatch hosts', async () => {
    const response = await dispatchWorker.fetch(
      new Request('https://unknown.example.test/runs'),
      {
        HOSTS: { get: vi.fn(async () => null) },
        DISPATCH: { get: vi.fn() },
        TENANT_CPU_LIMIT_MS: '20',
        TENANT_SUBREQUEST_LIMIT: '50',
      },
    );
    expect(response.status).toBe(404);
  });

  it('fails closed before dispatch when route policy metadata is missing', async () => {
    const get = vi.fn();
    const response = await dispatchWorker.fetch(
      new Request('https://acme.example.test/runs'),
      {
        HOSTS: {
          get: vi.fn(async () =>
            JSON.stringify({ scriptName: 'acme-prod', tenantTag: 'acme' }),
          ),
        },
        DISPATCH: { get },
        TENANT_CPU_LIMIT_MS: '20',
        TENANT_SUBREQUEST_LIMIT: '50',
      },
    );

    expect(response.status).toBe(503);
    expect(get).not.toHaveBeenCalled();
  });

  it('dispatches with tenant attribution and hard resource limits', async () => {
    const fetch = vi.fn(async () => new Response('ok'));
    const get = vi.fn(() => ({ fetch }));
    const request = new Request('https://acme.example.test/runs');
    const response = await dispatchWorker.fetch(request, {
      HOSTS: {
        get: vi.fn(async () => JSON.stringify(routingTarget())),
      },
      DISPATCH: { get },
      TENANT_CPU_LIMIT_MS: '20',
      TENANT_SUBREQUEST_LIMIT: '50',
    });

    expect(await response.text()).toBe('ok');
    expect(get).toHaveBeenCalledWith(
      'acme-prod',
      {},
      {
        limits: { cpuMs: 20, subRequests: 50 },
        outbound: policyContext(),
      },
    );
    expect(fetch).toHaveBeenCalledWith(request);
  });

  it('fails closed before dispatch when the route policy digest is forged', async () => {
    const get = vi.fn();
    const response = await dispatchWorker.fetch(
      new Request('https://acme.example.test/runs'),
      {
        HOSTS: {
          get: vi.fn(async () =>
            JSON.stringify({
              ...routingTarget(),
              policyDigest: 'f'.repeat(64),
            }),
          ),
        },
        DISPATCH: { get },
        TENANT_CPU_LIMIT_MS: '20',
        TENANT_SUBREQUEST_LIMIT: '50',
      },
    );

    expect(response.status).toBe(503);
    expect(get).not.toHaveBeenCalled();
  });

  it('addresses an unpublished candidate only through its authenticated maintenance routes', async () => {
    const fetch = vi.fn(async (_request: Request) => new Response('armed'));
    const get = vi.fn(() => ({ fetch }));
    const capability = await mintAsymmetricMaintenanceCapability({
      privateKey: MAINTENANCE_PRIVATE_KEY,
      operation: 'ensure-maintenance',
      tenantTag: 'acme',
      environment: 'production',
      scriptName: 'acme-release-a1b2',
      specDigest: MAINTENANCE_SPEC_DIGEST,
    });
    const response = await dispatchWorker.fetch(
      new Request(
        `https://control.example.test/.well-known/anchorage/maintenance/acme/production/acme-release-a1b2/${MAINTENANCE_SPEC_DIGEST}/ensure-maintenance`,
        {
          method: 'POST',
          headers: { authorization: `Bearer ${capability.token}` },
        },
      ),
      {
        HOSTS: { get: vi.fn(async () => null) },
        DISPATCH: { get },
        TENANT_CPU_LIMIT_MS: '20',
        TENANT_SUBREQUEST_LIMIT: '50',
        FLEET_MAINTENANCE_CAPABILITY_PUBLIC_KEY: MAINTENANCE_PUBLIC_KEY,
      },
    );

    expect(await response.text()).toBe('armed');
    expect(get).toHaveBeenCalledWith(
      'acme-release-a1b2',
      {},
      {
        limits: { cpuMs: 20, subRequests: 50 },
        outbound: {
          scriptName: 'acme-release-a1b2',
          tenantTag: 'acme',
          environment: 'production',
          policyId: 'maintenance-deny-all',
          policyDigest:
            '0000000000000000000000000000000000000000000000000000000000000000',
          policyHosts: '[]',
        },
      },
    );
    const forwarded = fetch.mock.calls[0]?.[0];
    expect(forwarded).toBeInstanceOf(Request);
    expect(forwarded?.url).toBe(
      'https://control.example.test/admin/ensure-maintenance',
    );
    expect(forwarded?.method).toBe('POST');
    expect(forwarded?.headers.get('authorization')).toBe(
      `Bearer ${capability.token}`,
    );
  });

  it('authenticates maintenance capability claims before invoking customer code', async () => {
    const get = vi.fn();
    const valid = await mintAsymmetricMaintenanceCapability({
      privateKey: MAINTENANCE_PRIVATE_KEY,
      operation: 'ensure-maintenance',
      tenantTag: 'acme',
      environment: 'production',
      scriptName: 'acme-release-a1b2',
      specDigest: MAINTENANCE_SPEC_DIGEST,
    });
    const cases = [
      undefined,
      'Bearer invalid',
      `Bearer ${valid.token}`,
      `Bearer ${valid.token}`,
      `Bearer ${valid.token}`,
      `Bearer ${valid.token}`,
    ];
    const urls = [
      `https://control.example.test/.well-known/anchorage/maintenance/acme/production/acme-release-a1b2/${MAINTENANCE_SPEC_DIGEST}/ensure-maintenance`,
      `https://control.example.test/.well-known/anchorage/maintenance/acme/production/acme-release-a1b2/${MAINTENANCE_SPEC_DIGEST}/ensure-maintenance`,
      `https://control.example.test/.well-known/anchorage/maintenance/acme/production/acme-release-a1b2/${'b'.repeat(64)}/ensure-maintenance`,
      `https://control.example.test/.well-known/anchorage/maintenance/victim/production/acme-release-a1b2/${MAINTENANCE_SPEC_DIGEST}/ensure-maintenance`,
      `https://control.example.test/.well-known/anchorage/maintenance/acme/production/victim-release/${MAINTENANCE_SPEC_DIGEST}/ensure-maintenance`,
      `https://control.example.test/.well-known/anchorage/maintenance/acme/staging/acme-release-a1b2/${MAINTENANCE_SPEC_DIGEST}/ensure-maintenance`,
    ];

    for (const [index, url] of urls.entries()) {
      const authorization = cases[index];
      const response = await dispatchWorker.fetch(
        new Request(url, {
          method: 'POST',
          ...(authorization ? { headers: { authorization } } : {}),
        }),
        {
          HOSTS: { get: vi.fn(async () => null) },
          DISPATCH: { get },
          TENANT_CPU_LIMIT_MS: '20',
          TENANT_SUBREQUEST_LIMIT: '50',
          FLEET_MAINTENANCE_CAPABILITY_PUBLIC_KEY: MAINTENANCE_PUBLIC_KEY,
        },
      );
      expect(response.status).toBe(401);
    }
    expect(get).not.toHaveBeenCalled();
  });

  it('does not expose arbitrary candidate application routes', async () => {
    const get = vi.fn();
    const response = await dispatchWorker.fetch(
      new Request(
        `https://control.example.test/.well-known/anchorage/maintenance/acme/production/acme-release-a1b2/${MAINTENANCE_SPEC_DIGEST}/runs`,
      ),
      {
        HOSTS: { get: vi.fn(async () => null) },
        DISPATCH: { get },
        TENANT_CPU_LIMIT_MS: '20',
        TENANT_SUBREQUEST_LIMIT: '50',
      },
    );

    expect(response.status).toBe(404);
    expect(get).not.toHaveBeenCalled();
  });

  it('blocks outbound hosts outside the organization allowlist', async () => {
    const upstream = vi.fn(async () => new Response('upstream'));
    vi.stubGlobal('fetch', upstream);
    const env = policyContext();
    const denied = await outboundWorker.fetch(
      new Request('https://evil.example.net/'),
      env,
    );
    expect(denied.status).toBe(403);
    expect(upstream).not.toHaveBeenCalled();

    const allowedRequest = new Request('https://api.example.com/v1');
    const allowed = await outboundWorker.fetch(allowedRequest, env);
    expect(await allowed.text()).toBe('upstream');
    expect(upstream).toHaveBeenCalledWith(allowedRequest, {
      redirect: 'manual',
    });
  });

  it('blocks redirects so an allowed host cannot escape the allowlist', async () => {
    const upstream = vi.fn(
      async () =>
        new Response(null, {
          status: 302,
          headers: { location: 'https://evil.example.net/collect' },
        }),
    );
    vi.stubGlobal('fetch', upstream);

    const response = await outboundWorker.fetch(
      new Request('https://api.example.com/redirect'),
      {
        ...policyContext(),
      },
    );

    expect(response.status).toBe(502);
    expect(upstream).toHaveBeenCalledOnce();
  });

  it('authenticates named state egress against canonical route context and strips reserved headers', async () => {
    const credential = 'state-credential-acme-production-0123456789';
    const target = {
      ...routingTarget(),
      stateEgress: {
        resourceGroupId: 'a'.repeat(20),
        stateScriptName: 'acme-production-state',
        credentialDigest: createHash('sha256').update(credential).digest('hex'),
      },
    };
    const upstream = vi.fn(async (request: Request) => {
      for (const name of Object.values(STATE_EGRESS_HEADERS)) {
        expect(request.headers.has(name)).toBe(false);
      }
      return new Response('upstream');
    });
    vi.stubGlobal('fetch', upstream);
    const headers = new Headers({
      [STATE_EGRESS_HEADERS.credential]: credential,
      [STATE_EGRESS_HEADERS.tenantTag]: 'acme',
      [STATE_EGRESS_HEADERS.environment]: 'production',
      [STATE_EGRESS_HEADERS.resourceGroupId]: 'a'.repeat(20),
      [STATE_EGRESS_HEADERS.stateScriptName]: 'acme-production-state',
      [STATE_EGRESS_HEADERS.routeHostname]: 'dispatch.example.test',
      [STATE_EGRESS_HEADERS.policyId]: 'policy-acme-production',
    });
    const entrypoint = new StateEgress(
      {},
      {
        HOSTS: { get: vi.fn(async () => JSON.stringify(target)) },
        scriptName: '',
        tenantTag: '',
        environment: '',
        policyId: '',
      },
    );

    const allowed = await entrypoint.fetch(
      new Request('https://api.example.com/data', { headers }),
    );
    const denied = await entrypoint.fetch(
      new Request('https://evil.example.net/data', { headers }),
    );

    expect(await allowed.text()).toBe('upstream');
    expect(denied.status).toBe(403);
    expect(upstream).toHaveBeenCalledOnce();
  });

  it('rejects a valid state credential combined with another deployment context', async () => {
    const credential = 'state-credential-acme-production-0123456789';
    const target = {
      ...routingTarget(),
      stateEgress: {
        resourceGroupId: 'a'.repeat(20),
        stateScriptName: 'acme-production-state',
        credentialDigest: createHash('sha256').update(credential).digest('hex'),
      },
    };
    const entrypoint = new StateEgress(
      {},
      {
        HOSTS: { get: vi.fn(async () => JSON.stringify(target)) },
        scriptName: '',
        tenantTag: '',
        environment: '',
        policyId: '',
      },
    );
    const response = await entrypoint.fetch(
      new Request('https://api.example.com/data', {
        headers: {
          [STATE_EGRESS_HEADERS.credential]: credential,
          [STATE_EGRESS_HEADERS.tenantTag]: 'beta',
          [STATE_EGRESS_HEADERS.environment]: 'production',
          [STATE_EGRESS_HEADERS.resourceGroupId]: 'b'.repeat(20),
          [STATE_EGRESS_HEADERS.stateScriptName]: 'beta-production-state',
          [STATE_EGRESS_HEADERS.routeHostname]: 'dispatch.example.test',
          [STATE_EGRESS_HEADERS.policyId]: 'policy-acme-production',
        },
      }),
    );

    expect(response.status).toBe(403);
  });

  it('does not let reserved state headers elevate normal outbound traffic', async () => {
    const response = await outboundWorker.fetch(
      new Request('https://evil.example.net/data', {
        headers: Object.fromEntries(
          Object.values(STATE_EGRESS_HEADERS).map((name) => [name, 'spoofed']),
        ),
      }),
      policyContext(),
    );

    expect(response.status).toBe(403);
  });

  it('adapts a service binding for Durable Object connector fetches', async () => {
    const binding = { fetch: vi.fn(async () => new Response('proxied')) };
    const proxiedFetch = createEgressProxyFetch(binding);

    const response = await proxiedFetch('https://api.example.com/data', {
      method: 'POST',
    });

    expect(await response.text()).toBe('proxied');
    expect(binding.fetch).toHaveBeenCalledWith(
      expect.objectContaining({ method: 'POST' }),
    );
  });

  it('attributes deployment proxy egress while resolving policy from the shared host route', async () => {
    const info = vi.spyOn(console, 'info').mockImplementation(() => {});
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => new Response('ok')),
    );

    await outboundWorker.fetch(
      new Request('https://api.example.com/data'),
      deploymentProxyContext(),
    );

    expect(JSON.parse(String(info.mock.calls[0]?.[0]))).toMatchObject({
      type: 'fleet-egress',
      scriptName: 'acme-production-state-a1b2c3d4',
      tenantTag: 'acme',
      environment: 'production',
      resourceRole: 'platform-state',
      policyId: 'policy-acme-production',
    });
  });

  it('fails closed when a deployment proxy route is absent or owned by another deployment', async () => {
    const upstream = vi.fn(async () => new Response('upstream'));
    vi.stubGlobal('fetch', upstream);
    const absent = {
      ...deploymentProxyContext(),
      HOSTS: { get: vi.fn(async () => null) },
    };
    const mismatched = deploymentProxyContext(
      routingTarget({ tenantTag: 'beta' }),
    );

    const absentResponse = await outboundWorker.fetch(
      new Request('https://api.example.com/data'),
      absent,
    );
    const mismatchedResponse = await outboundWorker.fetch(
      new Request('https://api.example.com/data'),
      mismatched,
    );

    expect(absentResponse.status).toBe(403);
    expect(mismatchedResponse.status).toBe(403);
    expect(upstream).not.toHaveBeenCalled();
  });

  it('denies missing or mismatched deployment policy context', async () => {
    const upstream = vi.fn(async () => new Response('upstream'));
    vi.stubGlobal('fetch', upstream);
    const valid = policyContext();

    const missing = await outboundWorker.fetch(
      new Request('https://api.example.com/data'),
      { ...valid, policyId: '' },
    );
    const mismatched = await outboundWorker.fetch(
      new Request('https://api.example.com/data'),
      { ...valid, environment: 'staging' },
    );

    expect(missing.status).toBe(403);
    expect(mismatched.status).toBe(403);
    expect(upstream).not.toHaveBeenCalled();
  });

  it('keeps the shared audit consumer queue-only and retries missing config', async () => {
    const retryAll = vi.fn();
    const ackAll = vi.fn();
    await auditConsumer.queue(
      {
        messages: [
          { body: { type: 'approval' }, ack: vi.fn(), retry: vi.fn() },
        ],
        retryAll,
        ackAll,
      },
      {},
    );
    expect(retryAll).toHaveBeenCalledOnce();
    expect(ackAll).not.toHaveBeenCalled();
    expect(Object.keys(auditConsumer)).toEqual(['queue']);
  });
});
