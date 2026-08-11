// SPDX-License-Identifier: Apache-2.0

import { isDeploymentEnvironment as isProtocolEnvironment } from '@proofoftech/flowsafe/deployment-identity-protocol';
import {
  mintMaintenanceCapability,
  verifyMaintenanceCapability,
} from '@proofoftech/flowsafe/host-kit';
import { describe, expect, it } from 'vitest';
import {
  canonicalEgressHosts,
  isDeploymentEnvironment,
  isDeploymentPolicyId,
  isDeploymentScriptName,
  isDeploymentTenantTag,
  isSha256,
} from '../src/deployment-context.js';

const NOW = Date.parse('2026-08-11T12:00:00.000Z');
const SECRET = 'maintenance-capability-secret-000000001';
const FLEET_ENVIRONMENTS = [
  'a',
  '0',
  'production',
  'preview-1',
  `${'a'.repeat(30)}-b`,
] as const;

describe('deployment context validation', () => {
  it('keeps tenant, environment, and script-name contracts distinct', () => {
    expect(isDeploymentTenantTag('acme')).toBe(true);
    expect(isDeploymentEnvironment('a'.repeat(32))).toBe(true);
    expect(isDeploymentEnvironment('a'.repeat(33))).toBe(false);
    expect(isDeploymentEnvironment('preview-')).toBe(false);
    expect(isDeploymentScriptName('a'.repeat(63))).toBe(true);
    expect(isDeploymentScriptName('a'.repeat(64))).toBe(false);
    expect(isDeploymentPolicyId('policy:acme-production')).toBe(true);
    expect(isSha256('a'.repeat(64))).toBe(true);
  });

  it.each([
    ...FLEET_ENVIRONMENTS,
    '',
    'Production',
    'preview-',
    'a'.repeat(33),
  ])('delegates fleet environment %s to the shared protocol authority', (value) => {
    expect(isDeploymentEnvironment(value)).toBe(isProtocolEnvironment(value));
  });

  it.each(
    FLEET_ENVIRONMENTS,
  )('mints and verifies maintenance authority for fleet environment %s', async (environment) => {
    expect(isDeploymentEnvironment(environment)).toBe(true);
    const minted = await mintMaintenanceCapability({
      secret: SECRET,
      operation: 'ensure-maintenance',
      tenantTag: 'acme',
      environment,
      scriptName: 'acme-release-a1b2',
      specDigest: 'a'.repeat(64),
      now: () => NOW,
      nonce: 'AAAAAAAAAAAAAAAAAAAAAA',
    });

    await expect(
      verifyMaintenanceCapability({
        secret: SECRET,
        token: minted.token,
        operation: 'ensure-maintenance',
        tenantTag: 'acme',
        environment,
        now: () => NOW,
      }),
    ).resolves.toEqual(minted.claims);
  });

  it('canonicalizes order while rejecting ambiguous hostnames', () => {
    expect(canonicalEgressHosts(['b.example.com', 'a.example.com'])).toEqual([
      'a.example.com',
      'b.example.com',
    ]);
    expect(() => canonicalEgressHosts(['API.example.com'])).toThrow(
      /not canonical/,
    );
    expect(() =>
      canonicalEgressHosts(['a.example.com', 'a.example.com']),
    ).toThrow(/unique/);
  });
});
