// SPDX-License-Identifier: Apache-2.0

import {
  DEPLOYMENT_TAG_PATTERN,
  isDeploymentEnvironment as isProtocolEnvironment,
} from '@proofoftech/flowsafe/deployment-identity-protocol';

const SCRIPT_NAME_PATTERN = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/u;
const POLICY_ID_PATTERN = /^[a-z0-9][a-z0-9:_-]{0,127}$/u;
const SHA256_PATTERN = /^[a-f0-9]{64}$/u;

export function isDeploymentTenantTag(value: string): boolean {
  return DEPLOYMENT_TAG_PATTERN.test(value);
}

export function isDeploymentEnvironment(value: string): boolean {
  return isProtocolEnvironment(value);
}

export function isDeploymentScriptName(value: string): boolean {
  return SCRIPT_NAME_PATTERN.test(value);
}

export function isDeploymentPolicyId(value: string): boolean {
  return POLICY_ID_PATTERN.test(value);
}

export function isSha256(value: string): boolean {
  return SHA256_PATTERN.test(value);
}

export function canonicalEgressHosts(
  hosts: readonly string[],
): readonly string[] {
  const canonical = hosts.map((host) => {
    if (host !== host.toLowerCase()) {
      throw new Error(`egress host '${host}' is not canonical`);
    }
    let parsed: URL;
    try {
      parsed = new URL(`https://${host}`);
    } catch {
      throw new Error(`egress host '${host}' is invalid`);
    }
    if (parsed.hostname !== host || parsed.port || parsed.pathname !== '/') {
      throw new Error(`egress host '${host}' is invalid`);
    }
    return host;
  });
  canonical.sort();
  if (new Set(canonical).size !== canonical.length) {
    throw new Error('egress hosts must be unique');
  }
  return canonical;
}
