// SPDX-License-Identifier: Apache-2.0
// The token-verification seam is trusted-computing-base code: everything
// here must fail CLOSED (verify to undefined), never to a default actor.
// hmacVerifier is exercised against real crypto.subtle round-trips via
// mintHmacToken — no mocked signatures.

import { SignJWT } from 'jose';
import { describe, expect, it, vi } from 'vitest';

import type { ApprovalActor } from '../approval-api/index.js';
import { parseActorTokens } from './bearer-auth.js';
import { mintStreamTicket } from './stream-ticket.js';
import {
  base64UrlEncode,
  hmacVerifier,
  mintHmacToken,
  staticTokenVerifier,
  toApprovalActor,
} from './verifier.js';

const ACTOR: ApprovalActor = { id: 'ray', role: 'reviewer' };

const MINT = {
  secret: 'test-secret-key',
  kid: 'k1',
  issuer: 'https://issuer.test',
  audience: 'anchorage-api',
  actor: ACTOR,
  ttlSeconds: 3600,
} as const;

function verifier(now?: () => number) {
  return hmacVerifier({
    keys: new Map([['k1', 'test-secret-key']]),
    issuer: 'https://issuer.test',
    audience: 'anchorage-api',
    ...(now ? { now } : {}),
  });
}

const encoder = new TextEncoder();

function encodeSegment(value: unknown): string {
  return base64UrlEncode(encoder.encode(JSON.stringify(value)));
}

describe('toApprovalActor', () => {
  it('accepts a complete candidate', () => {
    // #given / #when / #then
    expect(toApprovalActor({ id: 'a', role: 'admin' })).toEqual({
      id: 'a',
      role: 'admin',
    });
  });

  it.each([
    ['unknown role', { id: 'a', role: 'root' }],
    ['empty id', { id: '', role: 'admin' }],
    ['overlong id', { id: 'a'.repeat(201), role: 'admin' }],
    ['control-bearing id', { id: 'a\nb', role: 'admin' }],
    ['non-object', 'admin'],
    ['null', null],
  ])('rejects %s', (_label, candidate) => {
    // #when / #then — fail closed, no default actor
    expect(toApprovalActor(candidate)).toBeUndefined();
  });
});

describe('parseActorTokens', () => {
  it('keeps valid actors and drops invalid entries', () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    const parsed = parseActorTokens(
      JSON.stringify({
        good: { id: 'a', role: 'admin' },
        reviewer: { id: 'b', role: 'reviewer' },
        bad: { id: 'c', role: 'root' },
      }),
    );

    expect(parsed.size).toBe(2);
    expect(parsed.get('good')).toEqual({
      id: 'a',
      role: 'admin',
    });
    expect(parsed.get('reviewer')).toEqual({ id: 'b', role: 'reviewer' });
    expect(parsed.get('bad')).toBeUndefined();
    expect(errorSpy).toHaveBeenCalledWith(
      expect.stringContaining('config-error'),
    );
    errorSpy.mockRestore();
  });
});

describe('staticTokenVerifier', () => {
  it('resolves a known token and rejects an unknown one', async () => {
    // #given
    const verify = staticTokenVerifier(new Map([['tok', ACTOR]]));

    // #when / #then
    expect(await verify.verify('tok')).toEqual(ACTOR);
    expect(await verify.verify('other')).toBeUndefined();
  });

  it('snapshots configured actors instead of retaining mutable map values', async () => {
    const source: ApprovalActor = { id: 'ray', role: 'reviewer' };
    const mutableSource = source as {
      id: string;
      role: ApprovalActor['role'];
    };
    const verify = staticTokenVerifier(new Map([['tok', source]]));

    mutableSource.id = 'mutated';
    mutableSource.role = 'admin';

    expect(await verify.verify('tok')).toEqual({
      id: 'ray',
      role: 'reviewer',
    });
  });
});

describe('hmacVerifier', () => {
  it('round-trips a minted token to the actor', async () => {
    // #given
    const token = await mintHmacToken(MINT);

    // #when / #then
    expect(await verifier().verify(token)).toEqual(ACTOR);
  });

  it('rejects a tampered payload (signature no longer matches)', async () => {
    // #given — swap the role claim without re-signing
    const token = await mintHmacToken(MINT);
    const [header = '', , signature = ''] = token.split('.');
    const forgedClaims = encodeSegment({
      iss: MINT.issuer,
      aud: MINT.audience,
      sub: 'ray',
      role: 'admin',
      exp: Math.floor(Date.now() / 1000) + 3600,
    });

    // #when / #then
    expect(
      await verifier().verify(`${header}.${forgedClaims}.${signature}`),
    ).toBeUndefined();
  });

  it("rejects alg 'none' — a token cannot choose its own verification", async () => {
    // #given — a well-formed unsigned token claiming alg none
    const header = encodeSegment({ alg: 'none', kid: 'k1' });
    const claims = encodeSegment({
      iss: MINT.issuer,
      aud: MINT.audience,
      sub: 'ray',
      role: 'admin',
      exp: Math.floor(Date.now() / 1000) + 3600,
    });

    // #when / #then
    expect(await verifier().verify(`${header}.${claims}.`)).toBeUndefined();
  });

  it('rejects any negotiated algorithm other than HS256', async () => {
    // #given — same signed token, header re-labeled HS512: the pinned alg
    // check must fire before any cryptography
    const token = await mintHmacToken(MINT);
    const [, claims = '', signature = ''] = token.split('.');
    const header = encodeSegment({ alg: 'HS512', typ: 'JWT', kid: 'k1' });

    // #when / #then
    expect(
      await verifier().verify(`${header}.${claims}.${signature}`),
    ).toBeUndefined();
  });

  it.each([
    ['missing', undefined],
    ['wrong', 'flowsafe-stream-ticket+jwt'],
  ])('rejects a %s protected typ', async (_label, typ) => {
    const nowSeconds = Math.floor(Date.now() / 1000);
    const token = await new SignJWT({ role: ACTOR.role })
      .setProtectedHeader({
        alg: 'HS256',
        kid: MINT.kid,
        ...(typ ? { typ } : {}),
      })
      .setIssuer(MINT.issuer)
      .setAudience(MINT.audience)
      .setSubject(ACTOR.id)
      .setExpirationTime(nowSeconds + MINT.ttlSeconds)
      .sign(encoder.encode(MINT.secret));

    expect(await verifier().verify(token)).toBeUndefined();
  });

  it('rejects an unknown kid (plain Map lookup, no interpretation)', async () => {
    // #given
    const token = await mintHmacToken({ ...MINT, kid: '../etc/passwd' });

    // #when / #then
    expect(await verifier().verify(token)).toBeUndefined();
  });

  it('falls back to the sole key only when exactly one key is configured', async () => {
    // #given — a token with NO kid
    const nowSeconds = Math.floor(Date.now() / 1000);
    const header = encodeSegment({ alg: 'HS256', typ: 'JWT' });
    const claims = encodeSegment({
      iss: MINT.issuer,
      aud: MINT.audience,
      sub: 'ray',
      role: 'reviewer',
      exp: nowSeconds + 3600,
    });
    const key = await crypto.subtle.importKey(
      'raw',
      encoder.encode('test-secret-key'),
      { name: 'HMAC', hash: 'SHA-256' },
      false,
      ['sign'],
    );
    const signature = base64UrlEncode(
      new Uint8Array(
        await crypto.subtle.sign(
          'HMAC',
          key,
          encoder.encode(`${header}.${claims}`),
        ),
      ),
    );
    const token = `${header}.${claims}.${signature}`;

    // #when / #then — single key: accepted; two keys: ambiguous, rejected
    expect(await verifier().verify(token)).toEqual(ACTOR);
    const twoKeys = hmacVerifier({
      keys: new Map([
        ['k1', 'test-secret-key'],
        ['k2', 'other'],
      ]),
      issuer: MINT.issuer,
      audience: MINT.audience,
    });
    expect(await twoKeys.verify(token)).toBeUndefined();
  });

  it('rejects at the exact expiry boundary', async () => {
    // #given — minted at T, 60s ttl
    const t0 = 1_751_000_000_000;
    const token = await mintHmacToken({
      ...MINT,
      ttlSeconds: 60,
      now: () => t0,
    });

    // #when / #then
    expect(await verifier(() => t0 + 30_000).verify(token)).toEqual(ACTOR);
    expect(await verifier(() => t0 + 60_000).verify(token)).toBeUndefined();
  });

  it('rejects before nbf and accepts at its exact boundary', async () => {
    const t0 = 1_751_000_000_000;
    const nowSeconds = Math.floor(t0 / 1000);
    const token = await new SignJWT({ role: ACTOR.role })
      .setProtectedHeader({ alg: 'HS256', typ: 'JWT', kid: MINT.kid })
      .setIssuer(MINT.issuer)
      .setAudience(MINT.audience)
      .setSubject(ACTOR.id)
      .setNotBefore(nowSeconds + 60)
      .setExpirationTime(nowSeconds + 120)
      .sign(encoder.encode(MINT.secret));

    expect(await verifier(() => t0 + 59_000).verify(token)).toBeUndefined();
    expect(await verifier(() => t0 + 60_000).verify(token)).toEqual(ACTOR);
  });

  it('rejects a stream ticket even when the signing secret is reused', async () => {
    const token = await mintStreamTicket({
      secret: MINT.secret,
      channel: 'hub',
      actor: ACTOR,
    });

    expect(await verifier().verify(token)).toBeUndefined();
  });

  it('rejects wrong issuer and wrong audience', async () => {
    // #given
    const token = await mintHmacToken(MINT);

    // #when / #then
    const wrongIssuer = hmacVerifier({
      keys: new Map([['k1', 'test-secret-key']]),
      issuer: 'https://other.test',
      audience: MINT.audience,
    });
    const wrongAudience = hmacVerifier({
      keys: new Map([['k1', 'test-secret-key']]),
      issuer: MINT.issuer,
      audience: 'other-api',
    });
    expect(await wrongIssuer.verify(token)).toBeUndefined();
    expect(await wrongAudience.verify(token)).toBeUndefined();
  });

  it('rejects a signed token with an unknown role', async () => {
    // #given
    const token = await mintHmacToken({
      ...MINT,
      actor: { ...ACTOR, role: 'root' as ApprovalActor['role'] },
    });

    // #when / #then
    expect(await verifier().verify(token)).toBeUndefined();
  });

  it('rejects structural garbage without throwing', async () => {
    // #when / #then
    for (const garbage of ['', 'a.b', 'a.b.c.d', '!!!.@@@.###', 'a.b.c']) {
      expect(await verifier().verify(garbage)).toBeUndefined();
    }
  });
});
