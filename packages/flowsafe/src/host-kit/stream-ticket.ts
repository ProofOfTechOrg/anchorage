// SPDX-License-Identifier: Apache-2.0
// Short-lived HMAC "stream ticket" auth for the WebSocket handshake (DL-010).
//
// A browser cannot set an Authorization header on `new WebSocket()`, so the
// bearer identity cannot ride the upgrade and a long-lived token in the URL
// would leak through proxy/referrer logs. Instead a client mints a ~60s ticket
// over the AUTHENTICATED REST surface (stream-router.ts) and presents it in the
// WS URL query. The ticket is a standard HS256 JWT under a DEDICATED
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

import { jwtVerify, SignJWT } from 'jose';
import {
  APPROVAL_ROLES,
  type ApprovalActor,
  type ApprovalRole,
} from '../approval-api/index.js';
import { isPathSafeId } from '../do-runner/path-safe-id.js';

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
const STREAM_TICKET_AUDIENCE = 'flowsafe-stream';
const STREAM_TICKET_TYPE = 'flowsafe-stream-ticket+jwt';

/**
 * Mint a compact signed JWT ticket. For the `'run'` channel
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
  return new SignJWT({
    channel: claims.channel,
    ...(claims.workflowId !== undefined
      ? { workflowId: claims.workflowId }
      : {}),
    ...(claims.runId !== undefined ? { runId: claims.runId } : {}),
    actorId: claims.actorId,
    role: claims.role,
    exp: claims.exp,
  })
    .setProtectedHeader({ alg: 'HS256', typ: STREAM_TICKET_TYPE })
    .setAudience(STREAM_TICKET_AUDIENCE)
    .setExpirationTime(claims.exp)
    .sign(encoder.encode(secret));
}

/**
 * Verify a stream ticket, returning its claims or `undefined` (fail closed).
 * JOSE verifies the signature and registered claims; then every domain claim
 * is validated: `channel`
 * known, `runId` present if and only if the run channel (and when present
 * path-safe), `actorId` non-empty, and `role` a recognized APPROVAL_ROLE. Any
 * failure returns `undefined`.
 */
export async function verifyStreamTicket(
  options: VerifyStreamTicketOptions,
): Promise<StreamTicketClaims | undefined> {
  const { secret, token } = options;
  let claims: Partial<StreamTicketClaims>;
  try {
    const verified = await jwtVerify(token, encoder.encode(secret), {
      algorithms: ['HS256'],
      audience: STREAM_TICKET_AUDIENCE,
      typ: STREAM_TICKET_TYPE,
      currentDate: new Date((options.now ?? Date.now)()),
      clockTolerance: 0,
      requiredClaims: ['exp'],
    });
    claims = verified.payload;
  } catch {
    return undefined;
  }
  if (typeof claims.exp !== 'number') return undefined;
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
