// SPDX-License-Identifier: Apache-2.0

import {
  type MaintenanceCapabilityJwk,
  verifyAsymmetricMaintenanceCapability,
} from '@proofoftech/flowsafe/host-kit';
import {
  isDeploymentEnvironment,
  isDeploymentScriptName,
  isDeploymentTenantTag,
} from '../deployment-context.js';
import {
  type HostRoutingTarget,
  parseHostRoutingTarget,
} from './host-routing.js';

const MAINTENANCE_PATH_PREFIX = [
  '.well-known',
  'anchorage',
  'maintenance',
] as const;
const MAINTENANCE_OPERATIONS = new Set([
  'ensure-maintenance',
  'maintenance-status',
]);

export interface DispatchNamespaceLike {
  get(
    scriptName: string,
    arguments_?: Record<string, unknown>,
    options?: {
      readonly limits?: {
        readonly cpuMs?: number;
        readonly subRequests?: number;
      };
      readonly outbound?: Record<string, unknown>;
    },
  ): { fetch(request: Request): Promise<Response> };
}

export interface FleetDispatchEnv {
  readonly DISPATCH: DispatchNamespaceLike;
  readonly HOSTS: { get(key: string): Promise<string | null> };
  readonly TENANT_CPU_LIMIT_MS: string;
  readonly TENANT_SUBREQUEST_LIMIT: string;
  readonly FLEET_MAINTENANCE_CAPABILITY_PUBLIC_KEY?: string;
}

function positiveLimit(value: string, name: string): number {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 1) {
    throw new Error(`${name} must be a positive integer`);
  }
  return parsed;
}

function maintenanceTarget(url: URL):
  | {
      readonly operation: string;
      readonly scriptName: string;
      readonly specDigest: string;
      readonly tenantTag: string;
      readonly environment: string;
      readonly policyId: string;
      readonly policyDigest: string;
      readonly policyHosts: readonly string[];
    }
  | undefined {
  const segments = url.pathname.split('/').filter(Boolean);
  if (
    segments.length !== 8 ||
    !MAINTENANCE_PATH_PREFIX.every(
      (segment, index) => segments[index] === segment,
    )
  ) {
    return undefined;
  }
  const tenantTag = segments[3];
  const environment = segments[4];
  const scriptName = segments[5];
  const specDigest = segments[6];
  const operation = segments[7];
  if (
    !tenantTag ||
    !environment ||
    !scriptName ||
    !operation ||
    !isDeploymentTenantTag(tenantTag) ||
    !isDeploymentEnvironment(environment) ||
    !isDeploymentScriptName(scriptName) ||
    !specDigest ||
    !/^[a-f0-9]{64}$/u.test(specDigest) ||
    !MAINTENANCE_OPERATIONS.has(operation)
  ) {
    return undefined;
  }
  return {
    operation,
    scriptName,
    specDigest,
    tenantTag,
    environment,
    policyId: 'maintenance-deny-all',
    policyDigest:
      '0000000000000000000000000000000000000000000000000000000000000000',
    policyHosts: [],
  };
}

async function authorizedMaintenanceRequest(
  request: Request,
  env: FleetDispatchEnv,
  target: NonNullable<ReturnType<typeof maintenanceTarget>>,
): Promise<boolean> {
  const token = request.headers
    .get('authorization')
    ?.match(/^Bearer\s+(.+)$/i)?.[1];
  if (!token || token.length > 2_048) return false;
  let publicKey: MaintenanceCapabilityJwk;
  try {
    if (!env.FLEET_MAINTENANCE_CAPABILITY_PUBLIC_KEY) return false;
    const parsed = JSON.parse(
      env.FLEET_MAINTENANCE_CAPABILITY_PUBLIC_KEY,
    ) as Readonly<Record<string, unknown>>;
    if (
      !parsed ||
      typeof parsed !== 'object' ||
      Array.isArray(parsed) ||
      parsed.kty !== 'OKP' ||
      parsed.crv !== 'Ed25519' ||
      parsed.alg !== 'EdDSA' ||
      typeof parsed.kid !== 'string' ||
      !parsed.kid ||
      typeof parsed.x !== 'string' ||
      parsed.d !== undefined
    ) {
      return false;
    }
    publicKey = {
      kty: parsed.kty,
      crv: parsed.crv,
      alg: parsed.alg,
      kid: parsed.kid,
      x: parsed.x,
    };
  } catch {
    return false;
  }
  const capability = await verifyAsymmetricMaintenanceCapability({
    publicKey,
    token,
    operation: target.operation as 'ensure-maintenance' | 'maintenance-status',
    tenantTag: target.tenantTag,
    environment: target.environment,
  });
  return (
    capability?.scriptName === target.scriptName &&
    capability.specDigest === target.specDigest
  );
}

async function dispatch(
  request: Request,
  env: FleetDispatchEnv,
  target: HostRoutingTarget,
): Promise<Response> {
  const cpuMs = positiveLimit(env.TENANT_CPU_LIMIT_MS, 'TENANT_CPU_LIMIT_MS');
  const subrequests = positiveLimit(
    env.TENANT_SUBREQUEST_LIMIT,
    'TENANT_SUBREQUEST_LIMIT',
  );
  try {
    return await env.DISPATCH.get(
      target.scriptName,
      {},
      {
        limits: { cpuMs, subRequests: subrequests },
        outbound: {
          scriptName: target.scriptName,
          tenantTag: target.tenantTag,
          environment: target.environment,
          policyId: target.policyId,
          policyDigest: target.policyDigest,
          policyHosts: JSON.stringify(target.policyHosts),
        },
      },
    ).fetch(request);
  } catch (error) {
    console.error(
      JSON.stringify({
        type: 'fleet-dispatch-error',
        host: new URL(request.url).hostname.toLowerCase(),
        scriptName: target.scriptName,
        tenantTag: target.tenantTag,
        reason: String(error),
      }),
    );
    return new Response('deployment unavailable', { status: 503 });
  }
}

export default {
  async fetch(request: Request, env: FleetDispatchEnv): Promise<Response> {
    const requestUrl = new URL(request.url);
    const maintenance = maintenanceTarget(requestUrl);
    if (maintenance) {
      if (!(await authorizedMaintenanceRequest(request, env, maintenance))) {
        return new Response('authentication required', { status: 401 });
      }
      const forwardedUrl = new URL(request.url);
      forwardedUrl.pathname = `/admin/${maintenance.operation}`;
      forwardedUrl.search = '';
      forwardedUrl.hash = '';
      return dispatch(new Request(forwardedUrl, request), env, maintenance);
    }

    const host = requestUrl.hostname.toLowerCase();
    const serialized = await env.HOSTS.get(host);
    if (!serialized) return new Response('unknown deployment', { status: 404 });
    let target: HostRoutingTarget;
    try {
      target = await parseHostRoutingTarget(serialized);
    } catch (error) {
      console.error(
        JSON.stringify({
          type: 'fleet-dispatch-config-error',
          host,
          reason: String(error),
        }),
      );
      return new Response('deployment mapping unavailable', { status: 503 });
    }
    return dispatch(request, env, target);
  },
};
