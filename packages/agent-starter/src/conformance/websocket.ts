// SPDX-License-Identifier: Apache-2.0

import { throughState } from './actions.js';
import { CONFORMANCE_CONTRACT_VERSION, requiredText } from './contract.js';
import type { ConformanceCandidateEnv } from './env.js';

/**
 * The WebSocket half of the contract. The gate sends its request envelope as
 * the FIRST client frame and reads the FIRST server frame, so this must not
 * greet, and each reply carries only the documented fields plus the echoed
 * nonce.
 */

interface WebSocketEnvelope {
  readonly contractVersion?: unknown;
  readonly action?: unknown;
  readonly nonce?: unknown;
  readonly runId?: unknown;
  readonly approvalId?: unknown;
  readonly revision?: unknown;
}

async function replyFor(
  env: ConformanceCandidateEnv,
  envelope: WebSocketEnvelope,
): Promise<Record<string, unknown>> {
  const nonce = requiredText(envelope.nonce, 'nonce');
  if (envelope.contractVersion !== CONFORMANCE_CONTRACT_VERSION) {
    throw new Error('unsupported contract version');
  }
  if (envelope.action === 'nonce-echo') {
    return {
      contractVersion: CONFORMANCE_CONTRACT_VERSION,
      action: 'nonce-echo',
      nonce,
    };
  }
  if (envelope.action === 'flowsafe-approval-update') {
    const response = await throughState(env, 'flowsafe-approval-update', {
      runId: requiredText(envelope.runId, 'runId'),
      approvalId: requiredText(envelope.approvalId, 'approvalId'),
      revision: Number(envelope.revision),
    });
    // The state response already carries contractVersion and action from
    // contractResponse; the nonce is the only field this surface adds.
    return { ...((await response.json()) as Record<string, unknown>), nonce };
  }
  throw new Error(`unknown WebSocket action '${String(envelope.action)}'`);
}

/**
 * Answer one frame, and never throw.
 *
 * A failure ANSWERS rather than closing, because the gate's WebSocket client
 * registers no `close` listener (`credentialed-conformance.mjs`,
 * `contractWebSocketFrame`): a close-only failure stalls it for the full 15
 * seconds and then reports a bare timeout. A reply echoing the action and
 * nonce passes its envelope check and fails its exact-key check immediately,
 * naming the action that broke.
 *
 * The underlying reason never travels. It can carry approval identifiers and
 * trusted-state detail, and this endpoint is publicly routed, so it goes to the
 * Worker log instead — the same containment the HTTP path applies.
 */
export async function conformanceWebSocketReply(
  env: ConformanceCandidateEnv,
  frame: string,
): Promise<Record<string, unknown>> {
  let envelope: WebSocketEnvelope | undefined;
  try {
    envelope = JSON.parse(frame) as WebSocketEnvelope;
    return await replyFor(env, envelope);
  } catch (error) {
    console.error(
      JSON.stringify({
        type: 'conformance-websocket-error',
        action:
          typeof envelope?.action === 'string' ? envelope.action : 'unknown',
        error: error instanceof Error ? error.message : String(error),
      }),
    );
    return {
      contractVersion: CONFORMANCE_CONTRACT_VERSION,
      action:
        typeof envelope?.action === 'string' ? envelope.action : 'unknown',
      nonce: typeof envelope?.nonce === 'string' ? envelope.nonce : 'unknown',
      failed: true,
    };
  }
}

export function handleConformanceWebSocket(
  env: ConformanceCandidateEnv,
): Response {
  const pair = new WebSocketPair();
  const client = pair[0];
  const server = pair[1];
  server.accept();
  server.addEventListener('message', (event) => {
    const frame = typeof event.data === 'string' ? event.data : '';
    void conformanceWebSocketReply(env, frame)
      .then((reply) => {
        server.send(JSON.stringify(reply));
      })
      // The reply cannot reject, but send() can if the peer closed between the
      // frame and the answer. Contained rather than left unhandled.
      .catch((error) => {
        console.error(
          JSON.stringify({
            type: 'conformance-websocket-send-error',
            error: error instanceof Error ? error.message : String(error),
          }),
        );
      });
  });
  return new Response(null, { status: 101, webSocket: client });
}
