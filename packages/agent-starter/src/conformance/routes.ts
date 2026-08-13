// SPDX-License-Identifier: Apache-2.0

import {
  type ConformanceRequest,
  contractResponse,
  handleConformanceAction,
} from './actions.js';
import {
  CONFORMANCE_CONTRACT,
  CONFORMANCE_CONTRACT_VERSION,
} from './contract.js';
import type { ConformanceCandidateEnv } from './env.js';
import { handleConformanceWebSocket } from './websocket.js';

/**
 * The single seam the candidate mounts. No non-test module outside
 * `src/conformance/` imports it, so removing that directory and the
 * `conformance/` configuration directory removes the whole surface — the
 * README's "Delete it" carries the complete list.
 */
export async function mountConformanceRoutes(
  request: Request,
  env: ConformanceCandidateEnv,
): Promise<Response | null> {
  const url = new URL(request.url);
  if (url.pathname === CONFORMANCE_CONTRACT.webSocketPath) {
    if (request.headers.get('upgrade')?.toLowerCase() !== 'websocket') {
      return new Response('expected a websocket upgrade', { status: 426 });
    }
    return handleConformanceWebSocket(env);
  }
  if (url.pathname !== CONFORMANCE_CONTRACT.httpPath) return null;
  if (request.method !== 'POST') {
    return new Response('method not allowed', { status: 405 });
  }

  let body: ConformanceRequest;
  try {
    body = (await request.json()) as ConformanceRequest;
  } catch {
    return new Response('invalid request body', { status: 400 });
  }
  if (body?.contractVersion !== CONFORMANCE_CONTRACT_VERSION) {
    return new Response('unsupported contract version', { status: 400 });
  }
  if (typeof body.action !== 'string') {
    return new Response('action is required', { status: 400 });
  }
  try {
    return await handleConformanceAction(env, body);
  } catch (error) {
    // A failure still answers in contract shape so the gate reports the action
    // rather than a bare 500, but it carries no field the contract defines —
    // `assertExactKeys` then names the action that broke.
    console.error(
      JSON.stringify({
        type: 'conformance-action-error',
        action: body.action,
        error: error instanceof Error ? error.message : String(error),
      }),
    );
    return contractResponse(body.action, { failed: true }, 500);
  }
}
