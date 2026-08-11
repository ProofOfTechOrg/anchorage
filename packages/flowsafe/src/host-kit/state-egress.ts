// SPDX-License-Identifier: Apache-2.0

import {
  DEPLOYMENT_TAG_PATTERN,
  isDeploymentEnvironment,
} from '../deployment-identity-protocol.js';
import { credentialsMatch } from '../do-runner/deployment-identity.js';

export const STATE_EGRESS_CREDENTIAL_HEADER =
  'x-anchorage-state-credential' as const;
export const STATE_EGRESS_TENANT_HEADER = 'x-anchorage-state-tenant' as const;
export const STATE_EGRESS_ENVIRONMENT_HEADER =
  'x-anchorage-state-environment' as const;
export const STATE_EGRESS_RESOURCE_GROUP_HEADER =
  'x-anchorage-state-resource-group' as const;
export const STATE_EGRESS_SCRIPT_HEADER = 'x-anchorage-state-script' as const;
export const STATE_EGRESS_ROUTE_HOSTNAME_HEADER =
  'x-anchorage-state-route-hostname' as const;
export const STATE_EGRESS_POLICY_ID_HEADER =
  'x-anchorage-state-policy-id' as const;

export const STATE_EGRESS_HEADERS = {
  credential: STATE_EGRESS_CREDENTIAL_HEADER,
  tenantTag: STATE_EGRESS_TENANT_HEADER,
  environment: STATE_EGRESS_ENVIRONMENT_HEADER,
  resourceGroupId: STATE_EGRESS_RESOURCE_GROUP_HEADER,
  stateScriptName: STATE_EGRESS_SCRIPT_HEADER,
  routeHostname: STATE_EGRESS_ROUTE_HOSTNAME_HEADER,
  policyId: STATE_EGRESS_POLICY_ID_HEADER,
} as const;

export const STATE_EGRESS_RESERVED_HEADERS =
  Object.values(STATE_EGRESS_HEADERS);

export interface StateEgressBinding {
  fetch(request: Request): Promise<Response>;
}

export interface StateEgressContext {
  readonly credential: string;
  readonly tenantTag: string;
  readonly environment: string;
  readonly resourceGroupId: string;
  readonly stateScriptName: string;
  readonly routeHostname: string;
  readonly policyId: string;
}

/** Exact trusted-state bindings consumed by createStateEgressFetch. */
export interface StateEgressEnv {
  readonly OUTBOUND_PROXY: StateEgressBinding;
  readonly OUTBOUND_PROXY_CREDENTIAL: string;
  readonly OUTBOUND_TENANT_ID: string;
  readonly OUTBOUND_ENVIRONMENT: string;
  readonly OUTBOUND_RESOURCE_GROUP_ID: string;
  readonly OUTBOUND_STATE_SCRIPT_NAME: string;
  readonly OUTBOUND_ROUTE_HOSTNAME: string;
  readonly OUTBOUND_POLICY_ID: string;
  readonly DEPLOYMENT_IDENTITY_SECRET: string;
}

const CREDENTIAL_PATTERN = /^[\x21-\x7e]{32,256}$/;
const RESOURCE_GROUP_PATTERN = /^[a-f0-9]{20}$/;
const SCRIPT_NAME_PATTERN = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/;
const POLICY_ID_PATTERN = /^[a-z0-9][a-z0-9:_-]{0,127}$/;
const SHA256_PATTERN = /^[a-f0-9]{64}$/;

/** Compare a presented state credential with the canonical stored digest. */
export async function stateEgressCredentialMatches(
  actual: string,
  expectedSha256: string,
): Promise<boolean> {
  if (
    !CREDENTIAL_PATTERN.test(actual) ||
    !SHA256_PATTERN.test(expectedSha256)
  ) {
    return false;
  }
  const digest = await crypto.subtle.digest(
    'SHA-256',
    new TextEncoder().encode(actual),
  );
  const actualSha256 = Array.from(new Uint8Array(digest), (byte) =>
    byte.toString(16).padStart(2, '0'),
  ).join('');
  return credentialsMatch(actualSha256, expectedSha256);
}

function canonicalRouteHostname(value: string): string | undefined {
  if (value !== value.toLowerCase()) return undefined;
  try {
    const parsed = new URL(`https://${value}`);
    return parsed.hostname === value && !parsed.port && parsed.pathname === '/'
      ? value
      : undefined;
  } catch {
    return undefined;
  }
}

function stateEgressContext(env: StateEgressEnv): StateEgressContext {
  if (!env.OUTBOUND_PROXY || typeof env.OUTBOUND_PROXY.fetch !== 'function') {
    throw new Error('trusted state has no outbound proxy service binding');
  }
  if (!CREDENTIAL_PATTERN.test(env.OUTBOUND_PROXY_CREDENTIAL)) {
    throw new Error(
      'trusted state outbound credential must contain 32-256 visible ASCII characters',
    );
  }
  if (!CREDENTIAL_PATTERN.test(env.DEPLOYMENT_IDENTITY_SECRET)) {
    throw new Error('trusted state deployment identity is invalid');
  }
  if (!DEPLOYMENT_TAG_PATTERN.test(env.OUTBOUND_TENANT_ID)) {
    throw new Error('trusted state outbound tenant is invalid');
  }
  if (!isDeploymentEnvironment(env.OUTBOUND_ENVIRONMENT)) {
    throw new Error('trusted state outbound environment is invalid');
  }
  if (!RESOURCE_GROUP_PATTERN.test(env.OUTBOUND_RESOURCE_GROUP_ID)) {
    throw new Error('trusted state outbound resource group is invalid');
  }
  if (!SCRIPT_NAME_PATTERN.test(env.OUTBOUND_STATE_SCRIPT_NAME)) {
    throw new Error('trusted state outbound script is invalid');
  }
  const routeHostname = canonicalRouteHostname(env.OUTBOUND_ROUTE_HOSTNAME);
  if (!routeHostname) {
    throw new Error('trusted state outbound route hostname is invalid');
  }
  if (!POLICY_ID_PATTERN.test(env.OUTBOUND_POLICY_ID)) {
    throw new Error('trusted state outbound policy is invalid');
  }
  return {
    credential: env.OUTBOUND_PROXY_CREDENTIAL,
    tenantTag: env.OUTBOUND_TENANT_ID,
    environment: env.OUTBOUND_ENVIRONMENT,
    resourceGroupId: env.OUTBOUND_RESOURCE_GROUP_ID,
    stateScriptName: env.OUTBOUND_STATE_SCRIPT_NAME,
    routeHostname,
    policyId: env.OUTBOUND_POLICY_ID,
  };
}

/** Validate the complete trusted-state egress binding group. */
export async function validateStateEgressEnv(
  env: StateEgressEnv,
): Promise<StateEgressContext> {
  const context = stateEgressContext(env);
  if (
    await credentialsMatch(context.credential, env.DEPLOYMENT_IDENTITY_SECRET)
  ) {
    throw new Error(
      'trusted state outbound credential must differ from deployment identity',
    );
  }
  return context;
}

/**
 * Adapt trusted-state fetches to the shared StateEgress entrypoint. The
 * platform entrypoint, not this adapter, resolves and enforces the allowlist.
 */
export function createStateEgressFetch(env: StateEgressEnv): typeof fetch {
  const context = stateEgressContext(env);
  const service = env.OUTBOUND_PROXY;
  const deploymentIdentitySecret = env.DEPLOYMENT_IDENTITY_SECRET;
  return async (input, init) => {
    if (await credentialsMatch(context.credential, deploymentIdentitySecret)) {
      throw new Error(
        'trusted state outbound credential must differ from deployment identity',
      );
    }
    const request = new Request(input, init);
    const headers = new Headers(request.headers);
    headers.set(STATE_EGRESS_CREDENTIAL_HEADER, context.credential);
    headers.set(STATE_EGRESS_TENANT_HEADER, context.tenantTag);
    headers.set(STATE_EGRESS_ENVIRONMENT_HEADER, context.environment);
    headers.set(STATE_EGRESS_RESOURCE_GROUP_HEADER, context.resourceGroupId);
    headers.set(STATE_EGRESS_SCRIPT_HEADER, context.stateScriptName);
    headers.set(STATE_EGRESS_ROUTE_HOSTNAME_HEADER, context.routeHostname);
    headers.set(STATE_EGRESS_POLICY_ID_HEADER, context.policyId);
    return service.fetch(new Request(request, { headers }));
  };
}
