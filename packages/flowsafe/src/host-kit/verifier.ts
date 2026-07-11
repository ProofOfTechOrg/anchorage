// SPDX-License-Identifier: Apache-2.0
// The token-verification seam identity flows through. Both routers accept an
// `authenticate(request)`; bearerActorAuthenticator (bearer-auth.ts) extracts
// the bearer token and delegates to a TokenVerifier:
//
//   staticTokenVerifier — the parsed APPROVAL_ACTOR_TOKENS map (dev/test and
//     small deployments).
//   hmacVerifier        — HS256 JWTs signed with a Worker secret (the demo
//     worker mints these; a commercial IdP later drops in as another
//     TokenVerifier implementation without touching the routers).
//
// TRUSTED-COMPUTING-BASE code: a verifier ASSERTS identity, including
// `tenantId` — the axis every isolation predicate keys on. Every path out of
// this module therefore validates the tenant against TENANT_ID_PATTERN
// (INV-3) and the role against APPROVAL_ROLES before an ApprovalActor exists.
// Fail closed: anything malformed verifies to `undefined` (a 401), never to a
// default actor.

import { APPROVAL_ROLES, type ApprovalActor } from '../approval-api/index.js';
import {
  RESERVED_TENANT_IDS,
  TENANT_ID_PATTERN,
} from '../do-runner/path-safe-id.js';

export interface TokenVerifier {
  verify(token: string): Promise<ApprovalActor | undefined>;
}

/**
 * Validate an untyped candidate into an ApprovalActor — the ONE place a
 * decoded token/map entry becomes an identity. No `as`-casting at the JSON
 * boundary: id non-empty, role recognized, tenantId INV-3-valid and not a
 * reserved identity.
 */
export function toApprovalActor(candidate: unknown): ApprovalActor | undefined {
  if (candidate === null || typeof candidate !== 'object') return undefined;
  const { id, role, tenantId } = candidate as {
    id?: unknown;
    role?: unknown;
    tenantId?: unknown;
  };
  if (typeof id !== 'string' || id.length === 0) return undefined;
  if (
    typeof role !== 'string' ||
    !(APPROVAL_ROLES as readonly string[]).includes(role)
  ) {
    return undefined;
  }
  if (typeof tenantId !== 'string' || !TENANT_ID_PATTERN.test(tenantId)) {
    return undefined;
  }
  // 'system' is the TCB's own audit identity (cron maintenance attribution);
  // a client token claiming it would launder into the operator's maintenance
  // log stream. Only RESERVED_TENANT_IDS bites here: rejecting the routing
  // slugs (api/docs/...) too would 401 a single-tenant host named 'api' over
  // a subdomain collision that cannot occur on a host with no subdomains —
  // those stay allocation/routing concerns (RESERVED_FOR_ALLOCATION).
  if (RESERVED_TENANT_IDS.includes(tenantId)) return undefined;
  return { id, role: role as ApprovalActor['role'], tenantId };
}

/** The parsed bearer map as a TokenVerifier (entries are pre-validated). */
export function staticTokenVerifier(
  actors: ReadonlyMap<string, ApprovalActor>,
): TokenVerifier {
  return {
    async verify(token) {
      return actors.get(token);
    },
  };
}

export interface HmacVerifierOptions {
  /**
   * kid -> HMAC secret. A plain Map lookup by design: a kid must never be
   * interpreted as a path or URL (kid-injection). With exactly one key, a
   * token without a kid falls back to it; with several, kid is required.
   */
  keys: ReadonlyMap<string, string>;
  issuer: string;
  audience: string;
  /** Epoch-ms clock, injectable for tests. Default Date.now. */
  now?: () => number;
}

interface JwtHeader {
  alg?: unknown;
  kid?: unknown;
}

interface JwtClaims {
  iss?: unknown;
  aud?: unknown;
  exp?: unknown;
  nbf?: unknown;
  sub?: unknown;
  role?: unknown;
  tenantId?: unknown;
}

const encoder = new TextEncoder();

function base64UrlDecodeBytes(segment: string): Uint8Array | undefined {
  const normalized = segment.replace(/-/g, '+').replace(/_/g, '/');
  const padded = normalized + '='.repeat((4 - (normalized.length % 4)) % 4);
  try {
    const binary = atob(padded);
    const bytes = new Uint8Array(binary.length);
    for (let index = 0; index < binary.length; index += 1) {
      bytes[index] = binary.charCodeAt(index);
    }
    return bytes;
  } catch {
    return undefined;
  }
}

function base64UrlDecodeJson(segment: string): unknown {
  const bytes = base64UrlDecodeBytes(segment);
  if (!bytes) return undefined;
  try {
    return JSON.parse(new TextDecoder().decode(bytes));
  } catch {
    return undefined;
  }
}

export function base64UrlEncode(bytes: Uint8Array): string {
  let binary = '';
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary)
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/, '');
}

/**
 * HMAC-SHA256 a payload with `secret`, base64url-encoded. The canonical
 * signer — the demo's OAuth state signing reuses it rather than re-rolling
 * importKey + subtle.sign (two HMAC paths drift; the base64url pair already
 * did).
 */
export async function hmacSign(
  secret: string,
  payload: string,
): Promise<string> {
  const key = await importHmacKey(secret, 'sign');
  const signature = await crypto.subtle.sign(
    'HMAC',
    key,
    encoder.encode(payload),
  );
  return base64UrlEncode(new Uint8Array(signature));
}

async function importHmacKey(
  secret: string,
  usage: 'sign' | 'verify',
): Promise<CryptoKey> {
  return crypto.subtle.importKey(
    'raw',
    encoder.encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    [usage],
  );
}

/**
 * HS256 JWT verifier over Worker-secret keys.
 *
 * The algorithm is PINNED: only `alg: "HS256"` verifies — `none` and every
 * negotiated value are rejected before any cryptography runs, so a token
 * cannot choose its own verification scheme. Signature comparison goes
 * through crypto.subtle.verify (constant-time), never a hand-rolled string
 * compare. Claims checked: exp (required), nbf (when present), iss, aud
 * (string or array), then sub/role/tenantId through toApprovalActor.
 */
export function hmacVerifier(options: HmacVerifierOptions): TokenVerifier {
  const now = options.now ?? Date.now;
  return {
    async verify(token) {
      const parts = token.split('.');
      if (parts.length !== 3) return undefined;
      const [headerPart, claimsPart, signaturePart] = parts as [
        string,
        string,
        string,
      ];

      const header = base64UrlDecodeJson(headerPart) as JwtHeader | undefined;
      if (header?.alg !== 'HS256') return undefined;

      let secret: string | undefined;
      if (typeof header.kid === 'string') {
        secret = options.keys.get(header.kid);
      } else if (header.kid === undefined && options.keys.size === 1) {
        secret = [...options.keys.values()][0];
      }
      if (secret === undefined) return undefined;

      const signature = base64UrlDecodeBytes(signaturePart);
      if (!signature) return undefined;
      const key = await importHmacKey(secret, 'verify');
      const valid = await crypto.subtle.verify(
        'HMAC',
        key,
        signature,
        encoder.encode(`${headerPart}.${claimsPart}`),
      );
      if (!valid) return undefined;

      const claims = base64UrlDecodeJson(claimsPart) as JwtClaims | undefined;
      if (!claims) return undefined;
      const nowSeconds = now() / 1000;
      if (typeof claims.exp !== 'number' || nowSeconds >= claims.exp) {
        return undefined;
      }
      if (claims.nbf !== undefined) {
        if (typeof claims.nbf !== 'number' || nowSeconds < claims.nbf) {
          return undefined;
        }
      }
      if (claims.iss !== options.issuer) return undefined;
      const audiences = Array.isArray(claims.aud) ? claims.aud : [claims.aud];
      if (!audiences.includes(options.audience)) return undefined;

      return toApprovalActor({
        id: claims.sub,
        role: claims.role,
        tenantId: claims.tenantId,
      });
    },
  };
}

export interface MintHmacTokenOptions {
  /** The signing secret; `kid` names it for the verifier's key map. */
  secret: string;
  kid: string;
  issuer: string;
  audience: string;
  actor: ApprovalActor;
  /** Token lifetime in seconds (exp = now + ttl). */
  ttlSeconds: number;
  /** Epoch-ms clock, injectable for tests. Default Date.now. */
  now?: () => number;
}

/**
 * Mint an HS256 JWT hmacVerifier accepts — the demo worker's token factory
 * and the test suite's fixture builder. Server-side only: the secret must
 * never reach a client.
 */
export async function mintHmacToken(
  options: MintHmacTokenOptions,
): Promise<string> {
  const nowSeconds = Math.floor((options.now ?? Date.now)() / 1000);
  const header = { alg: 'HS256', typ: 'JWT', kid: options.kid };
  const claims = {
    iss: options.issuer,
    aud: options.audience,
    sub: options.actor.id,
    role: options.actor.role,
    tenantId: options.actor.tenantId,
    iat: nowSeconds,
    exp: nowSeconds + options.ttlSeconds,
  };
  const headerPart = base64UrlEncode(encoder.encode(JSON.stringify(header)));
  const claimsPart = base64UrlEncode(encoder.encode(JSON.stringify(claims)));
  const key = await importHmacKey(options.secret, 'sign');
  const signature = await crypto.subtle.sign(
    'HMAC',
    key,
    encoder.encode(`${headerPart}.${claimsPart}`),
  );
  return `${headerPart}.${claimsPart}.${base64UrlEncode(new Uint8Array(signature))}`;
}
