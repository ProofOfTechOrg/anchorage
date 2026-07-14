// SPDX-License-Identifier: Apache-2.0
// The stream ticket is trusted-computing-base ADDRESSING: every failure path
// must verify to `undefined` (fail closed), never to a usable ticket. Exercised
// against real crypto.subtle round-trips via mintStreamTicket — no mocked
// signatures. A "forge" helper signs arbitrary claims with the real secret to
// prove that a validly-SIGNED-but-malformed ticket is still refused (the
// claim-level validation, not just the signature).

import { describe, expect, it } from 'vitest';

import type { ApprovalActor } from '../approval-api/index.js';
import { mintStreamTicket, verifyStreamTicket } from './stream-ticket.js';
import { base64UrlEncode, hmacSign } from './verifier.js';

const SECRET = 'stream-ticket-secret';
const ACTOR: ApprovalActor = { id: 'ray', role: 'reviewer', tenantId: 'acme' };
const RUN_ID = 'acme_run-1';

const encoder = new TextEncoder();

/** Sign arbitrary claims with `secret` — the payload the verifier will parse. */
async function forge(
  claims: Record<string, unknown>,
  secret = SECRET,
): Promise<string> {
  const payload = base64UrlEncode(encoder.encode(JSON.stringify(claims)));
  return `${payload}.${await hmacSign(secret, payload)}`;
}

function validClaims(
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
    tenantId: 'acme',
    channel: 'hub',
    actorId: 'ray',
    role: 'reviewer',
    exp: Math.floor(Date.now() / 1000) + 60,
    ...overrides,
  };
}

describe('mintStreamTicket / verifyStreamTicket round-trip', () => {
  it('round-trips a hub ticket to its claims (no runId)', async () => {
    // #given
    const token = await mintStreamTicket({
      secret: SECRET,
      tenantId: 'acme',
      channel: 'hub',
      actor: ACTOR,
    });

    // #when
    const claims = await verifyStreamTicket({ secret: SECRET, token });

    // #then
    expect(claims).toMatchObject({
      tenantId: 'acme',
      channel: 'hub',
      actorId: 'ray',
      role: 'reviewer',
    });
    expect(claims?.runId).toBeUndefined();
    expect(typeof claims?.exp).toBe('number');
  });

  it('round-trips a run ticket carrying its runId', async () => {
    // #given
    const token = await mintStreamTicket({
      secret: SECRET,
      tenantId: 'acme',
      channel: 'run',
      runId: RUN_ID,
      actor: ACTOR,
    });

    // #when
    const claims = await verifyStreamTicket({ secret: SECRET, token });

    // #then
    expect(claims).toMatchObject({ channel: 'run', runId: RUN_ID });
  });

  it("throws when a 'run' ticket is minted without a runId", async () => {
    // #when / #then — a run ticket with no run to address is a programmer error
    await expect(
      mintStreamTicket({
        secret: SECRET,
        tenantId: 'acme',
        channel: 'run',
        actor: ACTOR,
      }),
    ).rejects.toThrow(/runId/);
  });

  it('honors an injected clock (unexpired verifies, expired does not)', async () => {
    // #given — minted at t0 with a 60s ttl
    const t0 = 1_751_000_000_000;
    const token = await mintStreamTicket({
      secret: SECRET,
      tenantId: 'acme',
      channel: 'hub',
      actor: ACTOR,
      ttlSeconds: 60,
      now: () => t0,
    });

    // #when / #then
    expect(
      await verifyStreamTicket({
        secret: SECRET,
        token,
        now: () => t0 + 30_000,
      }),
    ).toMatchObject({ tenantId: 'acme' });
    expect(
      await verifyStreamTicket({
        secret: SECRET,
        token,
        now: () => t0 + 61_000,
      }),
    ).toBeUndefined();
  });
});

describe('verifyStreamTicket fail-closed', () => {
  it('rejects an expired ticket', async () => {
    // #given — exp already in the past
    const token = await forge(
      validClaims({ exp: Math.floor(Date.now() / 1000) - 1 }),
    );

    // #when / #then
    expect(await verifyStreamTicket({ secret: SECRET, token })).toBeUndefined();
  });

  it('rejects a ticket signed with a DIFFERENT secret (forged)', async () => {
    // #given — validly-shaped claims, wrong signing key
    const token = await forge(validClaims(), 'not-the-secret');

    // #when / #then
    expect(await verifyStreamTicket({ secret: SECRET, token })).toBeUndefined();
  });

  it('rejects a tampered signature of the SAME length (constant-time content branch)', async () => {
    // #given — a genuine ticket whose last signature char is flipped in place
    const token = await mintStreamTicket({
      secret: SECRET,
      tenantId: 'acme',
      channel: 'hub',
      actor: ACTOR,
    });
    const [payload = '', signature = ''] = token.split('.');
    const flipped =
      signature.slice(0, -1) + (signature.at(-1) === 'A' ? 'B' : 'A');
    expect(flipped).toHaveLength(signature.length);
    expect(flipped).not.toBe(signature);

    // #when / #then
    expect(
      await verifyStreamTicket({
        secret: SECRET,
        token: `${payload}.${flipped}`,
      }),
    ).toBeUndefined();
  });

  it('rejects a signature of the WRONG length (constant-time length branch)', async () => {
    // #given
    const token = await mintStreamTicket({
      secret: SECRET,
      tenantId: 'acme',
      channel: 'hub',
      actor: ACTOR,
    });
    const [payload = '', signature = ''] = token.split('.');

    // #when / #then — truncating the signature must not verify
    expect(
      await verifyStreamTicket({
        secret: SECRET,
        token: `${payload}.${signature.slice(0, -1)}`,
      }),
    ).toBeUndefined();
  });

  it('rejects a tampered payload (signature no longer matches)', async () => {
    // #given — a genuine ticket whose payload is swapped for a different tenant
    const token = await mintStreamTicket({
      secret: SECRET,
      tenantId: 'acme',
      channel: 'hub',
      actor: ACTOR,
    });
    const [, signature = ''] = token.split('.');
    const forgedPayload = base64UrlEncode(
      encoder.encode(JSON.stringify(validClaims({ tenantId: 'evil' }))),
    );

    // #when / #then
    expect(
      await verifyStreamTicket({
        secret: SECRET,
        token: `${forgedPayload}.${signature}`,
      }),
    ).toBeUndefined();
  });

  it('returns undefined (never throws) on a validly-signed JSON null payload (F5)', async () => {
    // #given — a null payload signed with the real secret. `typeof null` is
    // 'object', so a bare object check would slip it past and throw on claims.exp.
    const payload = base64UrlEncode(encoder.encode(JSON.stringify(null)));
    const token = `${payload}.${await hmacSign(SECRET, payload)}`;

    // #when / #then — fail closed to undefined, not a throw
    await expect(
      verifyStreamTicket({ secret: SECRET, token }),
    ).resolves.toBeUndefined();
  });

  it.each([
    ['wrong channel value', validClaims({ channel: 'admin' })],
    ['non-INV-3 tenantId', validClaims({ tenantId: 'ACME' })],
    ['underscore tenantId', validClaims({ tenantId: 'a_b' })],
    ["reserved tenantId ('system')", validClaims({ tenantId: 'system' })],
    ['missing actorId', validClaims({ actorId: undefined })],
    ['empty actorId', validClaims({ actorId: '' })],
    ['missing role', validClaims({ role: undefined })],
    ['unknown role', validClaims({ role: 'root' })],
    ['missing exp', validClaims({ exp: undefined })],
    ['non-numeric exp', validClaims({ exp: 'soon' })],
    ['runId present on a hub ticket', validClaims({ runId: 'acme_run-1' })],
    [
      'run ticket missing its runId',
      {
        tenantId: 'acme',
        channel: 'run',
        actorId: 'ray',
        role: 'reviewer',
        exp: Math.floor(Date.now() / 1000) + 60,
      },
    ],
    [
      'run ticket with a non-PATH_SAFE runId',
      validClaims({ channel: 'run', runId: 'acme_../etc' }),
    ],
  ])('rejects a validly-signed ticket with a %s', async (_label, claims) => {
    // #given — the signature is genuine; only a claim is malformed
    const token = await forge(claims);

    // #when / #then — fail closed at the claim layer
    expect(await verifyStreamTicket({ secret: SECRET, token })).toBeUndefined();
  });

  it("rejects a 'run' ticket for another tenant's runId (tenantOwnsSaltedId fails)", async () => {
    // #given — the ticket's tenant is 'acme', but the runId belongs to 'bravo'
    const token = await forge(
      validClaims({ channel: 'run', tenantId: 'acme', runId: 'bravo_run-1' }),
    );

    // #when / #then
    expect(await verifyStreamTicket({ secret: SECRET, token })).toBeUndefined();
  });

  it('rejects structurally malformed tokens without throwing', async () => {
    // #when / #then
    for (const garbage of ['', 'nodot', 'a.b.c', '!!!.@@@', '.', 'x.']) {
      expect(
        await verifyStreamTicket({ secret: SECRET, token: garbage }),
      ).toBeUndefined();
    }
  });
});
