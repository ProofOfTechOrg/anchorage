// The token-verification seam is trusted-computing-base code: everything
// here must fail CLOSED (verify to undefined), never to a default actor.
// hmacVerifier is exercised against real crypto.subtle round-trips via
// mintHmacToken — no mocked signatures.

import { describe, expect, it, vi } from 'vitest';

import type { ApprovalActor } from '../approval-api/index.js';
import { parseActorTokens } from './bearer-auth.js';
import {
  base64UrlEncode,
  hmacVerifier,
  mintHmacToken,
  staticTokenVerifier,
  toApprovalActor,
} from './verifier.js';

const ACTOR: ApprovalActor = { id: 'ray', role: 'reviewer', tenantId: 'acme' };

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
    expect(
      toApprovalActor({ id: 'a', role: 'admin', tenantId: 'acme' }),
    ).toEqual({ id: 'a', role: 'admin', tenantId: 'acme' });
  });

  it.each([
    ['missing tenantId', { id: 'a', role: 'admin' }],
    ['uppercase tenantId', { id: 'a', role: 'admin', tenantId: 'Acme' }],
    ['short tenantId', { id: 'a', role: 'admin', tenantId: 'ab' }],
    ['underscore tenantId', { id: 'a', role: 'admin', tenantId: 'a_b' }],
    [
      "reserved identity tenantId ('system')",
      { id: 'a', role: 'admin', tenantId: 'system' },
    ],
    ['unknown role', { id: 'a', role: 'root', tenantId: 'acme' }],
    ['empty id', { id: '', role: 'admin', tenantId: 'acme' }],
    ['non-object', 'admin'],
    ['null', null],
  ])('rejects %s', (_label, candidate) => {
    // #when / #then — fail closed, no default actor
    expect(toApprovalActor(candidate)).toBeUndefined();
  });

  it.each(['docs', 'api', 'default'])(
    "accepts tenantId '%s' — allocation/routing reservations never bite at authentication",
    (tenantId) => {
      // #given a single-tenant host named after an allocation-reserved slug
      // (no subdomains, so the routing collision cannot occur)
      // #when / #then — only RESERVED_TENANT_IDS ('system') 401s at the
      // token layer; re-conflating the two lists would 401 this host
      expect(toApprovalActor({ id: 'a', role: 'admin', tenantId })).toEqual({
        id: 'a',
        role: 'admin',
        tenantId,
      });
    },
  );
});

describe('parseActorTokens (tenant required)', () => {
  it('drops entries lacking an INV-3 tenantId; their tokens 401', () => {
    // #given — one valid entry, one legacy tenant-less entry, one bad tenant
    const parsed = parseActorTokens(
      JSON.stringify({
        good: { id: 'a', role: 'admin', tenantId: 'acme' },
        legacy: { id: 'b', role: 'admin' },
        bad: { id: 'c', role: 'admin', tenantId: 'NOPE' },
      }),
    );

    // #then
    expect(parsed.size).toBe(1);
    expect(parsed.get('good')).toEqual({
      id: 'a',
      role: 'admin',
      tenantId: 'acme',
    });
    expect(parsed.get('legacy')).toBeUndefined();
    expect(parsed.get('bad')).toBeUndefined();
  });

  it("drops a 'system' entry (the TCB's own audit identity) and logs config-error", () => {
    // #given
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    try {
      // #when
      const parsed = parseActorTokens(
        JSON.stringify({
          tok: { id: 'x', role: 'admin', tenantId: 'system' },
        }),
      );

      // #then — empty map (the token 401s), and the drop is loud: a silent
      // drop is how a broken token map hides until every route 401s
      expect(parsed.size).toBe(0);
      expect(errorSpy).toHaveBeenCalledWith(
        expect.stringContaining('config-error'),
      );
    } finally {
      errorSpy.mockRestore();
    }
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
      tenantId: 'acme',
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
      tenantId: 'acme',
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
      tenantId: 'acme',
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

  it('rejects an expired token and one not yet valid', async () => {
    // #given — minted at T, 60s ttl
    const t0 = 1_751_000_000_000;
    const token = await mintHmacToken({
      ...MINT,
      ttlSeconds: 60,
      now: () => t0,
    });

    // #when / #then
    expect(await verifier(() => t0 + 30_000).verify(token)).toEqual(ACTOR);
    expect(await verifier(() => t0 + 61_000).verify(token)).toBeUndefined();
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

  it('rejects a signed token whose tenant claim violates INV-3', async () => {
    // #given — correctly signed, but the tenant crossed the JWT boundary
    // malformed; the verifier is the chokepoint that must catch it
    const token = await mintHmacToken({
      ...MINT,
      actor: { ...ACTOR, tenantId: 'Bad_Tenant' },
    });

    // #when / #then
    expect(await verifier().verify(token)).toBeUndefined();
  });

  it("rejects a signed token claiming the reserved identity 'system'", async () => {
    // #given — correctly signed; the verifier chokepoint must still refuse
    // the TCB's own audit identity (cron maintenance attribution)
    const token = await mintHmacToken({
      ...MINT,
      actor: { ...ACTOR, tenantId: 'system' },
    });

    // #when / #then
    expect(await verifier().verify(token)).toBeUndefined();
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
