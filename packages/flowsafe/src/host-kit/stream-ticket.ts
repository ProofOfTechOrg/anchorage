// SPDX-License-Identifier: Apache-2.0
// Short-lived HMAC "stream ticket" auth for the WebSocket handshake (DL-010).
//
// A browser cannot set an Authorization header on `new WebSocket()`, so the
// bearer identity cannot ride the upgrade and a long-lived token in the URL
// would leak through proxy/referrer logs. Instead a client mints a ~60s ticket
// over the AUTHENTICATED REST surface (stream-router.ts) and presents it in the
// WS URL query. The ticket is signed with the SAME HS256 primitives as the
// session verifier (verifier.ts hmacSign/base64UrlEncode) but under a DEDICATED
// STREAM_TICKET_SECRET (DL-019), so a ticket and a session JWT can never be
// confused under one signing key. A leaked ticket URL is limited to a brief
// replay on exactly ONE deployment channel.
//
// The ticket carries ONLY ADDRESSING — channel, workflowId/runId, actor, exp —
// never a grant. Connector capability comes solely from the server-derived
// requestContext grant the runtime mints per leg (approval-api/grants.ts); no
// capability ever travels on the WebSocket. The Worker is the SOLE verification
// authority: it verifies the ticket once and routes by channel/runId to
// idFromName, so the hub/runner DOs re-bind the connection to the deployment or
// run by their own identity and hold no secret.
//
// Fail-closed: an expired, forged, cross-channel, or otherwise
// malformed ticket verifies to `undefined`, never to a default identity.

import {
  APPROVAL_ROLES,
  type ApprovalActor,
  type ApprovalRole,
} from '../approval-api/index.js';
import { isPathSafeId } from '../do-runner/path-safe-id.js';
import { base64UrlEncode, hmacSign } from './verifier.js';

/** The two live channels: the deployment hub or a per-run WebSocket. */
export type StreamChannel = 'hub' | 'run';

/**
 * The addressing a stream ticket carries. `runId` is present IFF `channel` is
 * `'run'`. `exp` is epoch SECONDS (matching the JWT `exp` convention). No
 * capability is ever encoded here — the ticket addresses a channel, nothing
 * more.
 */
export interface StreamTicketClaims {
  channel: StreamChannel;
  workflowId?: string;
  runId?: string;
  actorId: string;
  role: ApprovalRole;
  /** Epoch SECONDS. */
  exp: number;
}

export interface MintStreamTicketOptions {
  /** The dedicated stream-ticket signing secret (STREAM_TICKET_SECRET). */
  secret: string;
  channel: StreamChannel;
  /** REQUIRED for the `'run'` channel; omitted for `'hub'`. */
  workflowId?: string;
  /** REQUIRED for the `'run'` channel; omitted for `'hub'`. */
  runId?: string;
  /** The authenticated actor — only `id` and `role` are carried. */
  actor: ApprovalActor;
  /** Ticket lifetime in seconds (exp = now + ttl). Default 60. */
  ttlSeconds?: number;
  /** Epoch-ms clock, injectable for tests. Default Date.now. */
  now?: () => number;
}

export interface VerifyStreamTicketOptions {
  secret: string;
  token: string;
  /** Epoch-ms clock, injectable for tests. Default Date.now. */
  now?: () => number;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/**
 * Mint a compact `base64url(claims).signature` ticket. For the `'run'` channel
 * a runId is REQUIRED — a run ticket with no run to address is a programmer
 * error (throws a generic Error; a client never reaches this mint directly).
 */
export async function mintStreamTicket(
  options: MintStreamTicketOptions,
): Promise<string> {
  const { secret, channel, workflowId, runId, actor } = options;
  if (channel === 'run' && (workflowId === undefined || runId === undefined)) {
    throw new Error(
      "mintStreamTicket: a 'run' channel ticket requires workflowId and runId",
    );
  }
  const nowSeconds = Math.floor((options.now ?? Date.now)() / 1000);
  const claims: StreamTicketClaims = {
    channel,
    ...(workflowId !== undefined ? { workflowId } : {}),
    // Omit runId entirely off the run channel so the "present IFF run" verify
    // check is exact (an undefined field serializes away under JSON.stringify).
    ...(runId !== undefined ? { runId } : {}),
    actorId: actor.id,
    role: actor.role,
    exp: nowSeconds + (options.ttlSeconds ?? 60),
  };
  const payload = base64UrlEncode(encoder.encode(JSON.stringify(claims)));
  const signature = await hmacSign(secret, payload);
  return `${payload}.${signature}`;
}

/**
 * Verify a stream ticket, returning its claims or `undefined` (fail closed).
 * The signature is recomputed over the presented payload segment and compared
 * CONSTANT-TIME; then every claim is validated: `exp` unexpired, `channel`
 * known, `runId` present if and only if the run channel (and when present
 * path-safe), `actorId` non-empty, and `role` a recognized APPROVAL_ROLE. Any
 * failure returns `undefined`.
 */
export async function verifyStreamTicket(
  options: VerifyStreamTicketOptions,
): Promise<StreamTicketClaims | undefined> {
  const { secret, token } = options;
  const parts = token.split('.');
  if (parts.length !== 2) return undefined;
  const [payload, signature] = parts as [string, string];

  // Recompute the HMAC over the presented payload segment and compare in
  // constant time. hmacSign is the one signer both mint and verify share, so a
  // tampered payload OR signature yields a different recomputed sig and fails.
  const expected = await hmacSign(secret, payload);
  if (!constantTimeEqual(signature, expected)) return undefined;

  const claims = base64UrlDecodeJson(payload) as
    | Partial<StreamTicketClaims>
    | undefined;
  // `typeof null === 'object'`, so a JSON `null` payload would slip past a bare
  // object check and throw on the first `claims.exp` read — breaking this
  // function's "malformed → undefined, never throws" contract (F5). Guard it.
  if (claims === undefined || claims === null || typeof claims !== 'object') {
    return undefined;
  }

  const nowSeconds = (options.now ?? Date.now)() / 1000;
  if (typeof claims.exp !== 'number' || nowSeconds >= claims.exp) {
    return undefined;
  }
  if (claims.channel !== 'hub' && claims.channel !== 'run') return undefined;

  // workflowId/runId are present IFF the run channel and path-safe.
  const hasWorkflowId = claims.workflowId !== undefined;
  const hasRunId = claims.runId !== undefined;
  if (
    (claims.channel === 'run') !== hasWorkflowId ||
    (claims.channel === 'run') !== hasRunId
  ) {
    return undefined;
  }
  if (claims.channel === 'run') {
    if (
      typeof claims.workflowId !== 'string' ||
      !isPathSafeId(claims.workflowId) ||
      typeof claims.runId !== 'string' ||
      !isPathSafeId(claims.runId)
    ) {
      return undefined;
    }
  }

  if (typeof claims.actorId !== 'string' || claims.actorId.length === 0) {
    return undefined;
  }
  if (
    typeof claims.role !== 'string' ||
    !(APPROVAL_ROLES as readonly string[]).includes(claims.role)
  ) {
    return undefined;
  }

  return {
    channel: claims.channel,
    ...(claims.channel === 'run'
      ? { workflowId: claims.workflowId, runId: claims.runId }
      : {}),
    actorId: claims.actorId,
    role: claims.role,
    exp: claims.exp,
  };
}

/**
 * Constant-time string equality for the recomputed base64url signatures. The
 * length branch leaks nothing: an HMAC-SHA256 base64url signature is a fixed 43
 * chars, so the length is not secret; only the content comparison must be
 * timing-safe, and it is (a full XOR-accumulate over every char).
 */
function constantTimeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let mismatch = 0;
  for (let index = 0; index < a.length; index += 1) {
    mismatch |= a.charCodeAt(index) ^ b.charCodeAt(index);
  }
  return mismatch === 0;
}

/**
 * Compact base64url -> JSON decoder for the claims segment (verifier.ts keeps
 * its own copy module-private; this is the "add a compact one" the ticket needs
 * to read claims). Any malformed input returns `undefined` rather than throwing.
 */
function base64UrlDecodeJson(segment: string): unknown {
  const normalized = segment.replace(/-/g, '+').replace(/_/g, '/');
  const padded = normalized + '='.repeat((4 - (normalized.length % 4)) % 4);
  try {
    const binary = atob(padded);
    const bytes = new Uint8Array(binary.length);
    for (let index = 0; index < binary.length; index += 1) {
      bytes[index] = binary.charCodeAt(index);
    }
    return JSON.parse(decoder.decode(bytes));
  } catch {
    return undefined;
  }
}
