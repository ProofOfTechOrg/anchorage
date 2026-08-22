// SPDX-License-Identifier: Apache-2.0

import {
  canonicalEgressHosts,
  isDeploymentEnvironment,
  isDeploymentPolicyId,
  isDeploymentScriptName,
  isDeploymentTenantTag,
  isSha256,
} from './deployment-context.js';

export interface HostRoutingTarget {
  readonly scriptName: string;
  readonly tenantTag: string;
  readonly environment: string;
  readonly policyId: string;
  readonly policyDigest: string;
  readonly policyHosts: readonly string[];
  readonly stateEgress?: Readonly<{
    resourceGroupId: string;
    stateScriptName: string;
    credentialDigest: string;
  }>;
}

async function sha256(value: string): Promise<string> {
  const digest = await crypto.subtle.digest(
    'SHA-256',
    new TextEncoder().encode(value),
  );
  return [...new Uint8Array(digest)]
    .map((byte) => byte.toString(16).padStart(2, '0'))
    .join('');
}

export async function parseHostRoutingTarget(
  serialized: string,
): Promise<HostRoutingTarget> {
  const value: unknown = JSON.parse(serialized);
  if (!value || typeof value !== 'object') {
    throw new Error('host routing target must be an object');
  }
  const target = value as Record<string, unknown>;
  if (
    typeof target.scriptName !== 'string' ||
    typeof target.tenantTag !== 'string' ||
    typeof target.environment !== 'string' ||
    typeof target.policyId !== 'string' ||
    typeof target.policyDigest !== 'string' ||
    !Array.isArray(target.policyHosts) ||
    !target.policyHosts.every((host) => typeof host === 'string')
  ) {
    throw new Error('host routing target is missing outbound policy context');
  }
  if (
    !isDeploymentScriptName(target.scriptName) ||
    !isDeploymentTenantTag(target.tenantTag) ||
    !isDeploymentEnvironment(target.environment) ||
    !isDeploymentPolicyId(target.policyId) ||
    !isSha256(target.policyDigest)
  ) {
    throw new Error('host routing target has invalid outbound policy context');
  }
  let policyHosts: readonly string[];
  try {
    policyHosts = canonicalEgressHosts(target.policyHosts as string[]);
  } catch {
    throw new Error('host routing target has invalid outbound policy hosts');
  }
  if (JSON.stringify(target.policyHosts) !== JSON.stringify(policyHosts)) {
    throw new Error(
      'host routing target has non-canonical outbound policy hosts',
    );
  }
  const policyDigest = await sha256(
    JSON.stringify({
      policyId: target.policyId,
      tenantTag: target.tenantTag,
      environment: target.environment,
      allowedHosts: policyHosts,
    }),
  );
  if (policyDigest !== target.policyDigest) {
    throw new Error(
      'host routing target has a mismatched outbound policy digest',
    );
  }
  let stateEgress: HostRoutingTarget['stateEgress'];
  if (target.stateEgress !== undefined) {
    if (
      !target.stateEgress ||
      typeof target.stateEgress !== 'object' ||
      Array.isArray(target.stateEgress)
    ) {
      throw new Error('host routing target has invalid state egress context');
    }
    const candidate = target.stateEgress as Record<string, unknown>;
    if (
      typeof candidate.resourceGroupId !== 'string' ||
      !isDeploymentPolicyId(candidate.resourceGroupId) ||
      typeof candidate.stateScriptName !== 'string' ||
      !isDeploymentScriptName(candidate.stateScriptName) ||
      typeof candidate.credentialDigest !== 'string' ||
      !isSha256(candidate.credentialDigest)
    ) {
      throw new Error('host routing target has invalid state egress context');
    }
    stateEgress = {
      resourceGroupId: candidate.resourceGroupId,
      stateScriptName: candidate.stateScriptName,
      credentialDigest: candidate.credentialDigest,
    };
  }
  return {
    scriptName: target.scriptName,
    tenantTag: target.tenantTag,
    environment: target.environment,
    policyId: target.policyId,
    policyDigest,
    policyHosts,
    ...(stateEgress ? { stateEgress } : {}),
  };
}
