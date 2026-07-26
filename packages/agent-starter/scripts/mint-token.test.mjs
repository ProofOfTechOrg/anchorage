// SPDX-License-Identifier: Apache-2.0

import { hmacVerifier } from '@proofoftech/flowsafe/host-kit';
import { describe, expect, it } from 'vitest';

import { mintStarterToken } from './mint-token.mjs';

const OPTIONS = {
  tenantId: 'acme',
  role: 'operator',
  actorId: 'local-operator',
  secret: 'synthetic-test-secret',
  issuer: 'anchorage-agent-starter',
  audience: 'anchorage-agent-starter-api',
};

describe('agent-starter token minting', () => {
  it('mints a token the HMAC verifier accepts', async () => {
    const token = await mintStarterToken(OPTIONS);
    const verifier = hmacVerifier({
      keys: new Map([['primary', OPTIONS.secret]]),
      issuer: OPTIONS.issuer,
      audience: OPTIONS.audience,
    });

    await expect(verifier.verify(token)).resolves.toEqual({
      id: OPTIONS.actorId,
      role: OPTIONS.role,
      tenantId: OPTIONS.tenantId,
    });
  });

  it.each([
    ['empty actor id', { actorId: '' }],
    ['malformed tenant id', { tenantId: 'Bad_Tenant' }],
    ['reserved tenant id', { tenantId: 'system' }],
  ])('rejects an actor with %s', async (_label, override) => {
    await expect(mintStarterToken({ ...OPTIONS, ...override })).rejects.toThrow(
      'actorId must be non-empty and tenantId must be a valid, non-reserved tenant ID',
    );
  });

  it('retains the role-specific validation error', async () => {
    await expect(
      mintStarterToken({ ...OPTIONS, role: 'root' }),
    ).rejects.toThrow('role must be one of:');
  });
});
