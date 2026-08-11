// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  type MaintenanceCapabilityJwk,
  mintAsymmetricMaintenanceCapability,
  mintMaintenanceCapability,
  mintMaintenanceReceipt,
  verifyAsymmetricMaintenanceCapability,
  verifyMaintenanceCapability,
  verifyMaintenanceReceipt,
} from './maintenance-capability.js';

const NOW = Date.parse('2026-08-11T12:00:00.000Z');
const SECRET = 'maintenance-capability-secret-000000001';
const OTHER_SECRET = 'other-maintenance-capability-secret-0001';
const NONCE = 'AAAAAAAAAAAAAAAAAAAAAA';
const PRIVATE_KEY = {
  kty: 'OKP',
  crv: 'Ed25519',
  alg: 'EdDSA',
  kid: 'fleet-maintenance-v1',
  x: 'Lhp1XFeTJJx8FLOCKpn4nkO-tWuZZxXX8ziw0LEvUZo',
  d: 'gkXf8_b8kcCJxZ33fUYUac7yCsxZAxQXgsgPbwDpnlM',
} satisfies MaintenanceCapabilityJwk;
const PUBLIC_KEY = {
  kty: 'OKP',
  crv: 'Ed25519',
  alg: 'EdDSA',
  kid: PRIVATE_KEY.kid,
  x: PRIVATE_KEY.x,
} satisfies MaintenanceCapabilityJwk;

async function capability() {
  return mintMaintenanceCapability({
    secret: SECRET,
    operation: 'ensure-maintenance',
    tenantTag: 'acme',
    environment: 'production',
    scriptName: 'acme-release-a1b2',
    specDigest: 'a'.repeat(64),
    ttlSeconds: 30,
    now: () => NOW,
    nonce: NONCE,
  });
}

describe('maintenance capabilities', () => {
  it('supports a public verifier without exposing signing material', async () => {
    const minted = await mintAsymmetricMaintenanceCapability({
      privateKey: PRIVATE_KEY,
      operation: 'ensure-maintenance',
      tenantTag: 'acme',
      environment: 'production',
      scriptName: 'acme-release-a1b2',
      specDigest: 'a'.repeat(64),
      now: () => NOW,
      nonce: NONCE,
    });

    await expect(
      verifyAsymmetricMaintenanceCapability({
        publicKey: PUBLIC_KEY,
        token: minted.token,
        operation: 'ensure-maintenance',
        tenantTag: 'acme',
        environment: 'production',
        now: () => NOW,
      }),
    ).resolves.toEqual(minted.claims);
    await expect(
      verifyAsymmetricMaintenanceCapability({
        publicKey: { ...PUBLIC_KEY, kid: 'retired-key' },
        token: minted.token,
        operation: 'ensure-maintenance',
        tenantTag: 'acme',
        environment: 'production',
        now: () => NOW,
      }),
    ).resolves.toBeUndefined();
  });
  it('binds operation, tenant, script, digest, expiry, and nonce', async () => {
    const minted = await capability();

    await expect(
      verifyMaintenanceCapability({
        secret: SECRET,
        token: minted.token,
        operation: 'ensure-maintenance',
        tenantTag: 'acme',
        environment: 'production',
        now: () => NOW,
      }),
    ).resolves.toEqual(minted.claims);
  });

  it('rejects forged, expired, cross-operation, and cross-tenant tokens', async () => {
    const minted = await capability();
    const common = { token: minted.token, now: () => NOW } as const;

    await expect(
      verifyMaintenanceCapability({
        ...common,
        secret: OTHER_SECRET,
        operation: 'ensure-maintenance',
        tenantTag: 'acme',
        environment: 'production',
      }),
    ).resolves.toBeUndefined();
    await expect(
      verifyMaintenanceCapability({
        ...common,
        secret: SECRET,
        operation: 'maintenance-status',
        tenantTag: 'acme',
        environment: 'production',
      }),
    ).resolves.toBeUndefined();
    await expect(
      verifyMaintenanceCapability({
        ...common,
        secret: SECRET,
        operation: 'ensure-maintenance',
        tenantTag: 'other',
        environment: 'production',
      }),
    ).resolves.toBeUndefined();
    await expect(
      verifyMaintenanceCapability({
        ...common,
        secret: SECRET,
        operation: 'ensure-maintenance',
        tenantTag: 'acme',
        environment: 'production',
        now: () => NOW + 31_000,
      }),
    ).resolves.toBeUndefined();
  });

  it('attests a result to the exact request capability', async () => {
    const minted = await capability();
    const result = { alarmAt: NOW, nextSweepAt: NOW };
    const receipt = await mintMaintenanceReceipt(SECRET, minted.claims, result);

    await expect(
      verifyMaintenanceReceipt({
        secret: SECRET,
        token: receipt,
        capability: minted.claims,
        now: () => NOW,
      }),
    ).resolves.toEqual(result);
    await expect(
      verifyMaintenanceReceipt({
        secret: SECRET,
        token: receipt,
        capability: { ...minted.claims, nonce: 'BBBBBBBBBBBBBBBBBBBBBB' },
        now: () => NOW,
      }),
    ).resolves.toBeUndefined();
  });
});
