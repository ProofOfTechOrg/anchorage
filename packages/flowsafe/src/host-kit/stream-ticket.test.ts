// SPDX-License-Identifier: Apache-2.0
// The stream ticket is trusted-computing-base ADDRESSING: every failure path
// must verify to `undefined` (fail closed), never to a usable ticket. Exercised
// against real crypto.subtle round-trips via mintStreamTicket — no mocked
// signatures. A "forge" helper signs arbitrary claims with the real secret to
// prove that a validly-SIGNED-but-malformed ticket is still refused (the
// claim-level validation, not just the signature).

import { CompactSign, SignJWT } from 'jose';
import { describe, expect, it } from 'vitest';

import type { ApprovalActor } from '../approval-api/index.js';
import { mintStreamTicket, verifyStreamTicket } from './stream-ticket.js';
import { mintHmacToken } from './verifier.js';

const SECRET = 'stream-ticket-secret';
const ACTOR: ApprovalActor = { id: 'ray', role: 'reviewer' };
const RUN_ID = 'acme_run-1';
const WORKFLOW_ID = 'workflow-1';
const AUDIENCE = 'flowsafe-stream';
const TYPE = 'flowsafe-stream-ticket+jwt';

const encoder = new TextEncoder();

/** Sign arbitrary claims with `secret` — the payload the verifier will parse. */
async function forge(
  claims: Record<string, unknown>,
  secret = SECRET,
): Promise<string> {
  return new SignJWT({ ...claims, aud: AUDIENCE })
    .setProtectedHeader({ alg: 'HS256', typ: TYPE })
    .sign(encoder.encode(secret));
}

function validClaims(
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
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
      channel: 'hub',
      actor: ACTOR,
    });

    // #when
    const claims = await verifyStreamTicket({ secret: SECRET, token });

    // #then
    expect(claims).toMatchObject({
      channel: 'hub',
      actorId: 'ray',
      role: 'reviewer',
    });
    expect(claims?.runId).toBeUndefined();
    expect(claims?.workflowId).toBeUndefined();
    expect(typeof claims?.exp).toBe('number');
  });

  it('round-trips a run ticket carrying its runId', async () => {
    // #given
    const token = await mintStreamTicket({
      secret: SECRET,
      channel: 'run',
      workflowId: WORKFLOW_ID,
      runId: RUN_ID,
      actor: ACTOR,
    });

    // #when
    const claims = await verifyStreamTicket({ secret: SECRET, token });

    // #then
    expect(claims).toMatchObject({
      channel: 'run',
      workflowId: WORKFLOW_ID,
      runId: RUN_ID,
    });
  });

  it("throws when a 'run' ticket is minted without its workflow/run address", async () => {
    // #when / #then — a run ticket with no run to address is a programmer error
    await expect(
      mintStreamTicket({
        secret: SECRET,
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
    ).toMatchObject({ channel: 'hub' });
    expect(
      await verifyStreamTicket({
        secret: SECRET,
        token,
        now: () => t0 + 60_000,
      }),
    ).toBeUndefined();
  });

  it('rejects an actor JWT even when the signing secret is reused', async () => {
    const token = await mintHmacToken({
      secret: SECRET,
      kid: 'stream-key',
      issuer: 'https://issuer.test',
      audience: AUDIENCE,
      actor: ACTOR,
      ttlSeconds: 60,
    });

    await expect(
      verifyStreamTicket({ secret: SECRET, token }),
    ).resolves.toBeUndefined();
  });
});

describe('verifyStreamTicket fail-closed', () => {
  it.each([
    ['wrong audience', 'other-audience', TYPE],
    ['wrong type', AUDIENCE, 'JWT'],
  ])('rejects a token with %s', async (_label, audience, typ) => {
    const token = await new SignJWT(validClaims())
      .setProtectedHeader({ alg: 'HS256', typ })
      .setAudience(audience)
      .sign(encoder.encode(SECRET));

    await expect(
      verifyStreamTicket({ secret: SECRET, token }),
    ).resolves.toBeUndefined();
  });

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

  it('rejects a tampered signature of the SAME length', async () => {
    // #given — mutate a character that always carries signature bits; the
    // final base64url character also contains padding bits for a 32-byte MAC.
    const token = await mintStreamTicket({
      secret: SECRET,
      channel: 'hub',
      actor: ACTOR,
    });
    const [header = '', payload = '', signature = ''] = token.split('.');
    const flipped = (signature.at(0) === 'A' ? 'B' : 'A') + signature.slice(1);
    expect(flipped).toHaveLength(signature.length);
    expect(flipped).not.toBe(signature);

    // #when / #then
    expect(
      await verifyStreamTicket({
        secret: SECRET,
        token: `${header}.${payload}.${flipped}`,
      }),
    ).toBeUndefined();
  });

  it('rejects a signature of the WRONG length', async () => {
    // #given
    const token = await mintStreamTicket({
      secret: SECRET,
      channel: 'hub',
      actor: ACTOR,
    });
    const [header = '', payload = '', signature = ''] = token.split('.');

    // #when / #then — truncating the signature must not verify
    expect(
      await verifyStreamTicket({
        secret: SECRET,
        token: `${header}.${payload}.${signature.slice(0, -1)}`,
      }),
    ).toBeUndefined();
  });

  it('rejects a tampered payload (signature no longer matches)', async () => {
    // #given — a genuine ticket whose payload is swapped for different claims
    const token = await mintStreamTicket({
      secret: SECRET,
      channel: 'hub',
      actor: ACTOR,
    });
    const [header = '', , signature = ''] = token.split('.');
    const forged = await forge(validClaims({ actorId: 'mallory' }));
    const [, forgedPayload = ''] = forged.split('.');

    // #when / #then
    expect(
      await verifyStreamTicket({
        secret: SECRET,
        token: `${header}.${forgedPayload}.${signature}`,
      }),
    ).toBeUndefined();
  });

  it('returns undefined (never throws) on a validly-signed JSON null payload (F5)', async () => {
    // #given — a null payload signed with the real secret. `typeof null` is
    // 'object', so a bare object check would slip it past and throw on claims.exp.
    const token = await new CompactSign(encoder.encode(JSON.stringify(null)))
      .setProtectedHeader({ alg: 'HS256', typ: TYPE })
      .sign(encoder.encode(SECRET));

    // #when / #then — fail closed to undefined, not a throw
    await expect(
      verifyStreamTicket({ secret: SECRET, token }),
    ).resolves.toBeUndefined();
  });

  it.each([
    ['wrong channel value', validClaims({ channel: 'admin' })],
    ['missing actorId', validClaims({ actorId: undefined })],
    ['empty actorId', validClaims({ actorId: '' })],
    ['missing role', validClaims({ role: undefined })],
    ['unknown role', validClaims({ role: 'root' })],
    ['missing exp', validClaims({ exp: undefined })],
    ['non-numeric exp', validClaims({ exp: 'soon' })],
    ['runId present on a hub ticket', validClaims({ runId: 'acme_run-1' })],
    [
      'workflowId present on a hub ticket',
      validClaims({ workflowId: WORKFLOW_ID }),
    ],
    [
      'run ticket missing its runId',
      {
        channel: 'run',
        workflowId: WORKFLOW_ID,
        actorId: 'ray',
        role: 'reviewer',
        exp: Math.floor(Date.now() / 1000) + 60,
      },
    ],
    [
      'run ticket with a non-PATH_SAFE runId',
      validClaims({
        channel: 'run',
        workflowId: WORKFLOW_ID,
        runId: 'acme_../etc',
      }),
    ],
    [
      'run ticket with a non-PATH_SAFE workflowId',
      validClaims({
        channel: 'run',
        workflowId: '../workflow',
        runId: RUN_ID,
      }),
    ],
  ])('rejects a validly-signed ticket with a %s', async (_label, claims) => {
    // #given — the signature is genuine; only a claim is malformed
    const token = await forge(claims);

    // #when / #then — fail closed at the claim layer
    expect(await verifyStreamTicket({ secret: SECRET, token })).toBeUndefined();
  });

  it('accepts any path-safe host-minted runId', async () => {
    const token = await forge(
      validClaims({
        channel: 'run',
        workflowId: WORKFLOW_ID,
        runId: 'run_01JTEST',
      }),
    );

    await expect(
      verifyStreamTicket({ secret: SECRET, token }),
    ).resolves.toMatchObject({
      channel: 'run',
      workflowId: WORKFLOW_ID,
      runId: 'run_01JTEST',
    });
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
