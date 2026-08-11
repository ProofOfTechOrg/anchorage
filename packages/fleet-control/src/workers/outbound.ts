// SPDX-License-Identifier: Apache-2.0

import {
  STATE_EGRESS_HEADERS,
  stateEgressCredentialMatches,
} from '@proofoftech/flowsafe/host-kit';

import {
  isDeploymentEnvironment,
  isDeploymentPolicyId,
  isDeploymentScriptName,
  isDeploymentTenantTag,
} from '../deployment-context.js';
import { parseHostRoutingTarget } from './host-routing.js';

export interface FleetOutboundEnv {
  readonly scriptName: string;
  readonly tenantTag: string;
  readonly environment: string;
  readonly policyId: string;
  readonly policyDigest?: string;
  readonly policyHosts?: string;
  readonly resourceRole?: string;
  readonly routeHostname?: string;
  readonly HOSTS?: { get(key: string): Promise<string | null> };
}

export interface EgressProxyBinding {
  fetch(request: Request): Promise<Response>;
}

export function createEgressProxyFetch(
  binding: EgressProxyBinding,
): typeof fetch {
  return async (input, init) => binding.fetch(new Request(input, init));
}

interface ResolvedPolicy {
  readonly allowedHosts: ReadonlySet<string>;
  readonly digest: string;
}

function stripStateEgressHeaders(request: Request): Request {
  const headers = new Headers(request.headers);
  for (const name of Object.values(STATE_EGRESS_HEADERS)) headers.delete(name);
  return new Request(request, { headers });
}

async function resolveStatePolicy(
  request: Request,
  env: FleetOutboundEnv,
): Promise<ResolvedPolicy> {
  if (!env.HOSTS) throw new Error('state egress has no routing authority');
  const context = {
    credential: request.headers.get(STATE_EGRESS_HEADERS.credential) ?? '',
    tenantTag: request.headers.get(STATE_EGRESS_HEADERS.tenantTag) ?? '',
    environment: request.headers.get(STATE_EGRESS_HEADERS.environment) ?? '',
    resourceGroupId:
      request.headers.get(STATE_EGRESS_HEADERS.resourceGroupId) ?? '',
    stateScriptName:
      request.headers.get(STATE_EGRESS_HEADERS.stateScriptName) ?? '',
    routeHostname:
      request.headers.get(STATE_EGRESS_HEADERS.routeHostname) ?? '',
    policyId: request.headers.get(STATE_EGRESS_HEADERS.policyId) ?? '',
  };
  if (
    !isDeploymentTenantTag(context.tenantTag) ||
    !isDeploymentEnvironment(context.environment) ||
    !isDeploymentPolicyId(context.resourceGroupId) ||
    !isDeploymentScriptName(context.stateScriptName) ||
    !isDeploymentPolicyId(context.policyId) ||
    context.routeHostname !== context.routeHostname.toLowerCase() ||
    context.credential.length < 32
  ) {
    throw new Error('state egress context is missing or invalid');
  }
  const routeUrl = new URL(`https://${context.routeHostname}`);
  if (
    routeUrl.hostname !== context.routeHostname ||
    routeUrl.port !== '' ||
    routeUrl.pathname !== '/'
  ) {
    throw new Error('state egress route hostname is invalid');
  }
  const serialized = await env.HOSTS.get(context.routeHostname);
  if (!serialized) throw new Error('state egress host route is absent');
  const target = await parseHostRoutingTarget(serialized);
  const stateEgress = target.stateEgress;
  if (!stateEgress) throw new Error('state egress host route is not armed');
  if (
    target.tenantTag !== context.tenantTag ||
    target.environment !== context.environment ||
    target.policyId !== context.policyId ||
    stateEgress.resourceGroupId !== context.resourceGroupId ||
    stateEgress.stateScriptName !== context.stateScriptName
  ) {
    throw new Error('state egress host route has another owner');
  }
  if (
    !(await stateEgressCredentialMatches(
      context.credential,
      stateEgress.credentialDigest,
    ))
  ) {
    throw new Error('state egress authentication failed');
  }
  return {
    allowedHosts: new Set(target.policyHosts),
    digest: target.policyDigest,
  };
}

async function resolvePolicy(env: FleetOutboundEnv): Promise<ResolvedPolicy> {
  if (
    !isDeploymentScriptName(env.scriptName) ||
    !isDeploymentTenantTag(env.tenantTag) ||
    !isDeploymentEnvironment(env.environment) ||
    !isDeploymentPolicyId(env.policyId)
  ) {
    throw new Error('outbound policy context is missing or invalid');
  }
  if (env.resourceRole === 'platform-state') {
    if (!env.HOSTS || !env.routeHostname) {
      throw new Error('deployment egress proxy has no host-routing authority');
    }
    const routeHostname = env.routeHostname.toLowerCase();
    const parsedHostname = new URL(`https://${routeHostname}`);
    if (
      parsedHostname.hostname !== routeHostname ||
      parsedHostname.port !== '' ||
      parsedHostname.pathname !== '/'
    ) {
      throw new Error('deployment egress proxy route hostname is invalid');
    }
    const serialized = await env.HOSTS.get(routeHostname);
    if (!serialized) {
      throw new Error('deployment egress proxy host route is absent');
    }
    const target = await parseHostRoutingTarget(serialized);
    if (
      target.tenantTag !== env.tenantTag ||
      target.environment !== env.environment ||
      target.policyId !== env.policyId
    ) {
      throw new Error('deployment egress proxy host route has another owner');
    }
    return {
      allowedHosts: new Set(target.policyHosts),
      digest: target.policyDigest,
    };
  }
  if (!env.policyDigest || !env.policyHosts) {
    throw new Error('shared outbound policy context is missing');
  }
  const target = await parseHostRoutingTarget(
    JSON.stringify({
      scriptName: env.scriptName,
      tenantTag: env.tenantTag,
      environment: env.environment,
      policyId: env.policyId,
      policyDigest: env.policyDigest,
      policyHosts: JSON.parse(env.policyHosts) as unknown,
    }),
  );
  return {
    allowedHosts: new Set(target.policyHosts),
    digest: target.policyDigest,
  };
}

export default {
  async fetch(request: Request, env: FleetOutboundEnv): Promise<Response> {
    const url = new URL(request.url);
    let policy: ResolvedPolicy;
    try {
      policy = await resolvePolicy(env);
    } catch (error) {
      console.warn(
        JSON.stringify({
          type: 'fleet-egress-policy-denied',
          host: url.hostname,
          scriptName: env.scriptName,
          tenantTag: env.tenantTag,
          environment: env.environment,
          policyId: env.policyId,
          reason: String(error),
        }),
      );
      return new Response('egress denied', { status: 403 });
    }
    if (
      (url.protocol !== 'https:' && url.protocol !== 'http:') ||
      !policy.allowedHosts.has(url.hostname.toLowerCase())
    ) {
      console.warn(
        JSON.stringify({
          type: 'fleet-egress-denied',
          host: url.hostname,
          scriptName: env.scriptName,
          tenantTag: env.tenantTag,
          environment: env.environment,
          policyId: env.policyId,
          policyDigest: policy.digest,
          ...(env.resourceRole ? { resourceRole: env.resourceRole } : {}),
        }),
      );
      return new Response('egress denied', { status: 403 });
    }
    console.info(
      JSON.stringify({
        type: 'fleet-egress',
        host: url.hostname,
        scriptName: env.scriptName,
        tenantTag: env.tenantTag,
        environment: env.environment,
        policyId: env.policyId,
        policyDigest: policy.digest,
        ...(env.resourceRole ? { resourceRole: env.resourceRole } : {}),
      }),
    );
    const response = await fetch(request, { redirect: 'manual' });
    if ([301, 302, 303, 307, 308].includes(response.status)) {
      console.warn(
        JSON.stringify({
          type: 'fleet-egress-redirect-denied',
          host: url.hostname,
          scriptName: env.scriptName,
          tenantTag: env.tenantTag,
          environment: env.environment,
          policyId: env.policyId,
          policyDigest: policy.digest,
          ...(env.resourceRole ? { resourceRole: env.resourceRole } : {}),
        }),
      );
      return new Response('egress redirect denied', { status: 502 });
    }
    return response;
  },
};

export class StateEgress {
  readonly #env: FleetOutboundEnv;

  constructor(_context: unknown, env: FleetOutboundEnv) {
    this.#env = env;
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    let policy: ResolvedPolicy;
    try {
      policy = await resolveStatePolicy(request, this.#env);
    } catch (error) {
      console.warn(
        JSON.stringify({
          type: 'fleet-state-egress-policy-denied',
          host: url.hostname,
          reason: String(error),
        }),
      );
      return new Response('egress denied', { status: 403 });
    }
    if (
      (url.protocol !== 'https:' && url.protocol !== 'http:') ||
      !policy.allowedHosts.has(url.hostname.toLowerCase())
    ) {
      return new Response('egress denied', { status: 403 });
    }
    const response = await fetch(stripStateEgressHeaders(request), {
      redirect: 'manual',
    });
    if ([301, 302, 303, 307, 308].includes(response.status)) {
      return new Response('egress redirect denied', { status: 502 });
    }
    return response;
  }
}
