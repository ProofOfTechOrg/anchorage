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
