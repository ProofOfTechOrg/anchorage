// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import {
  createStateEgressFetch,
  STATE_EGRESS_CREDENTIAL_HEADER,
  STATE_EGRESS_ENVIRONMENT_HEADER,
  STATE_EGRESS_POLICY_ID_HEADER,
  STATE_EGRESS_RESOURCE_GROUP_HEADER,
  STATE_EGRESS_ROUTE_HOSTNAME_HEADER,
  STATE_EGRESS_SCRIPT_HEADER,
  STATE_EGRESS_TENANT_HEADER,
  type StateEgressEnv,
  stateEgressCredentialMatches,
  validateStateEgressEnv,
} from './state-egress.js';

const OUTBOUND_CREDENTIAL = 'outbound-state-credential-0000000001';
const DEPLOYMENT_IDENTITY = 'deployment-identity-credential-00001';
const PRIVATE_VERSION_HEADER = 'x-anchorage-state-activation-version';
const PRIVATE_KIND_HEADER = 'x-anchorage-state-activation-kind';
const PRIVATE_KEY_HEADER = 'x-anchorage-state-dispatch-key';

function stateEnv(
  fetch = vi.fn(async (_request: Request) => new Response('proxied')),
): StateEgressEnv {
  return {
    OUTBOUND_PROXY: { fetch },
    OUTBOUND_PROXY_CREDENTIAL: OUTBOUND_CREDENTIAL,
    OUTBOUND_TENANT_ID: 'acme',
    OUTBOUND_ENVIRONMENT: 'production',
    OUTBOUND_RESOURCE_GROUP_ID: '0123456789abcdefabcd',
    OUTBOUND_STATE_SCRIPT_NAME: 'acme-production-state-0123456789abcdefabcd',
    OUTBOUND_ROUTE_HOSTNAME: 'acme.example.test',
    OUTBOUND_POLICY_ID: '0123456789abcdefabcd',
    DEPLOYMENT_IDENTITY_SECRET: DEPLOYMENT_IDENTITY,
  };
}

describe('trusted state egress adapter', () => {
  it('uses a private dispatch key without a host route and removes the spoofed host header', async () => {
    const bindingFetch = vi.fn(
      async (_request: Request) => new Response('private'),
    );
    const { OUTBOUND_ROUTE_HOSTNAME: _route, ...legacy } =
      stateEnv(bindingFetch);
    const env = {
      ...legacy,
      OUTBOUND_ACTIVATION_VERSION: '2',
      OUTBOUND_ACTIVATION_KIND: 'internal-dispatch',
      OUTBOUND_DISPATCH_KEY: 'acme:production',
    } as unknown as StateEgressEnv;

    await expect(validateStateEgressEnv(env)).resolves.toMatchObject({
      activationVersion: '2',
      activationKind: 'internal-dispatch',
      dispatchKey: 'acme:production',
    });
    const response = await createStateEgressFetch(env)(
      new Request('https://api.vendor.test/write', {
        headers: {
          [STATE_EGRESS_ROUTE_HOSTNAME_HEADER]: 'victim.example.test',
          [PRIVATE_KEY_HEADER]: 'victim:production',
        },
      }),
    );
    expect(await response.text()).toBe('private');
    const forwarded = bindingFetch.mock.calls[0]?.[0];
    expect(
      forwarded?.headers.get(STATE_EGRESS_ROUTE_HOSTNAME_HEADER),
    ).toBeNull();
    expect(forwarded?.headers.get(PRIVATE_VERSION_HEADER)).toBe('2');
    expect(forwarded?.headers.get(PRIVATE_KIND_HEADER)).toBe(
      'internal-dispatch',
    );
    expect(forwarded?.headers.get(PRIVATE_KEY_HEADER)).toBe('acme:production');
  });

  it('rejects mixed host and private activation bindings', async () => {
    const env = {
      ...stateEnv(),
      OUTBOUND_ACTIVATION_VERSION: '2',
      OUTBOUND_ACTIVATION_KIND: 'internal-dispatch',
      OUTBOUND_DISPATCH_KEY: 'acme:production',
    } as StateEgressEnv;
    await expect(validateStateEgressEnv(env)).rejects.toThrow();
  });

  it.each([
    ['unknown version', { OUTBOUND_ACTIVATION_VERSION: '3' }],
    ['unknown kind', { OUTBOUND_ACTIVATION_KIND: 'host' }],
    ['foreign key', { OUTBOUND_DISPATCH_KEY: 'other:production' }],
    ['host value', { OUTBOUND_ROUTE_HOSTNAME: 'acme.example.test' }],
  ])('rejects a private activation with %s', async (_case, patch) => {
    const { OUTBOUND_ROUTE_HOSTNAME: _route, ...legacy } = stateEnv();
    const env = {
      ...legacy,
      OUTBOUND_ACTIVATION_VERSION: '2',
      OUTBOUND_ACTIVATION_KIND: 'internal-dispatch',
      OUTBOUND_DISPATCH_KEY: 'acme:production',
      ...patch,
    } as unknown as StateEgressEnv;
    await expect(validateStateEgressEnv(env)).rejects.toThrow();
  });

  it('strips private authority headers from a legacy host request', async () => {
    const bindingFetch = vi.fn(async (_request: Request) => new Response());
    await createStateEgressFetch(stateEnv(bindingFetch))(
      new Request('https://api.vendor.test/write', {
        headers: {
          [PRIVATE_VERSION_HEADER]: '2',
          [PRIVATE_KIND_HEADER]: 'internal-dispatch',
          [PRIVATE_KEY_HEADER]: 'victim:production',
        },
      }),
    );
    const forwarded = bindingFetch.mock.calls[0]?.[0];
    expect(forwarded?.headers.get(PRIVATE_VERSION_HEADER)).toBeNull();
    expect(forwarded?.headers.get(PRIVATE_KIND_HEADER)).toBeNull();
    expect(forwarded?.headers.get(PRIVATE_KEY_HEADER)).toBeNull();
  });
  it('matches a presented credential against its canonical SHA-256 digest', async () => {
    const digest = await crypto.subtle.digest(
      'SHA-256',
      new TextEncoder().encode(OUTBOUND_CREDENTIAL),
    );
    const expected = Array.from(new Uint8Array(digest), (byte) =>
      byte.toString(16).padStart(2, '0'),
    ).join('');

    await expect(
      stateEgressCredentialMatches(OUTBOUND_CREDENTIAL, expected),
    ).resolves.toBe(true);
    await expect(
      stateEgressCredentialMatches(`${OUTBOUND_CREDENTIAL}x`, expected),
    ).resolves.toBe(false);
    await expect(
      stateEgressCredentialMatches(OUTBOUND_CREDENTIAL, 'not-a-digest'),
    ).resolves.toBe(false);
  });

  it('overwrites every reserved header on the cloned service request', async () => {
    const bindingFetch = vi.fn(
      async (_request: Request) => new Response('proxied'),
    );
    const env = stateEnv(bindingFetch);
    const spoofed = new Headers({
      [STATE_EGRESS_CREDENTIAL_HEADER]: 'candidate-spoof',
      [STATE_EGRESS_TENANT_HEADER]: 'victim',
      [STATE_EGRESS_ENVIRONMENT_HEADER]: 'victim',
      [STATE_EGRESS_RESOURCE_GROUP_HEADER]: 'f'.repeat(20),
      [STATE_EGRESS_SCRIPT_HEADER]: 'victim-state',
      [STATE_EGRESS_ROUTE_HOSTNAME_HEADER]: 'victim.example.test',
      [STATE_EGRESS_POLICY_ID_HEADER]: 'victim-policy',
      'x-application-header': 'preserved',
    });

    const response = await createStateEgressFetch(env)(
      new Request('https://api.vendor.test/write', {
        method: 'POST',
        headers: spoofed,
        body: 'payload',
      }),
    );

    expect(await response.text()).toBe('proxied');
    expect(bindingFetch).toHaveBeenCalledTimes(1);
    const forwarded = bindingFetch.mock.calls[0]?.[0];
    expect(forwarded?.headers.get(STATE_EGRESS_CREDENTIAL_HEADER)).toBe(
      OUTBOUND_CREDENTIAL,
    );
    expect(forwarded?.headers.get(STATE_EGRESS_TENANT_HEADER)).toBe('acme');
    expect(forwarded?.headers.get(STATE_EGRESS_ENVIRONMENT_HEADER)).toBe(
      'production',
    );
    expect(forwarded?.headers.get(STATE_EGRESS_RESOURCE_GROUP_HEADER)).toBe(
      '0123456789abcdefabcd',
    );
    expect(forwarded?.headers.get(STATE_EGRESS_SCRIPT_HEADER)).toBe(
      'acme-production-state-0123456789abcdefabcd',
    );
    expect(forwarded?.headers.get(STATE_EGRESS_ROUTE_HOSTNAME_HEADER)).toBe(
      'acme.example.test',
    );
    expect(forwarded?.headers.get(STATE_EGRESS_POLICY_ID_HEADER)).toBe(
      '0123456789abcdefabcd',
    );
    expect(forwarded?.headers.get('x-application-header')).toBe('preserved');
    expect(await forwarded?.text()).toBe('payload');
  });

  it.each([
    ['credential', { OUTBOUND_PROXY_CREDENTIAL: 'short' }],
    ['tenant', { OUTBOUND_TENANT_ID: 'Acme' }],
    ['environment', { OUTBOUND_ENVIRONMENT: 'Production' }],
    ['resource group', { OUTBOUND_RESOURCE_GROUP_ID: 'wrong' }],
    ['state script', { OUTBOUND_STATE_SCRIPT_NAME: 'INVALID' }],
    ['route hostname', { OUTBOUND_ROUTE_HOSTNAME: 'Acme.example.test' }],
    ['policy', { OUTBOUND_POLICY_ID: 'INVALID' }],
  ])('rejects an invalid %s before calling the service', async (_field, patch) => {
    const bindingFetch = vi.fn(async (_request: Request) => new Response(null));
    const env = { ...stateEnv(bindingFetch), ...patch } as StateEgressEnv;

    await expect(validateStateEgressEnv(env)).rejects.toThrow();
    expect(bindingFetch).not.toHaveBeenCalled();
  });

  it('rejects credential reuse before calling the service', async () => {
    const bindingFetch = vi.fn(async (_request: Request) => new Response(null));
    const env = {
      ...stateEnv(bindingFetch),
      OUTBOUND_PROXY_CREDENTIAL: DEPLOYMENT_IDENTITY,
    };
    const fetch = createStateEgressFetch(env);

    await expect(fetch('https://api.vendor.test')).rejects.toThrow(
      /must differ/,
    );
    expect(bindingFetch).not.toHaveBeenCalled();
  });
});
