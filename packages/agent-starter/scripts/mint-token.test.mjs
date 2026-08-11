// SPDX-License-Identifier: Apache-2.0

import { hmacVerifier } from '@proofoftech/flowsafe/host-kit';
import { describe, expect, it } from 'vitest';

import { mintStarterToken } from './mint-token.mjs';

const OPTIONS = {
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
    });
  });

  it('rejects an empty actor id', async () => {
    await expect(mintStarterToken({ ...OPTIONS, actorId: '' })).rejects.toThrow(
      'actorId must be non-empty',
    );
  });

  it('retains the role-specific validation error', async () => {
    await expect(
      mintStarterToken({ ...OPTIONS, role: 'root' }),
    ).rejects.toThrow('role must be one of:');
  });
});
