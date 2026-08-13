// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';

import type { ConformanceCandidateEnv } from '../src/conformance/env.js';
import { conformanceWebSocketReply } from '../src/conformance/websocket.js';

/**
 * The WebSocket failure path, which the workerd harness never reaches because
 * it only sends well-formed frames. Two properties matter and neither is
 * observable from a passing run: the gate's client registers no `close`
 * listener, so a close-only failure costs it a 15-second timeout and a
 * useless message; and the trusted state object's own error text must not
 * travel to a publicly routed endpoint.
 */

const INTERNAL_DETAIL = "approval 'abc' moved past revision 7";

function env(): ConformanceCandidateEnv {
  return {
    DEPLOYMENT_IDENTITY_SECRET: 'conformance-test-deployment-identity-secret',
    CONFORMANCE_STATE: {
      idFromName: () => 'id',
      get: () => ({
        fetch: async () =>
          new Response(JSON.stringify({ error: INTERNAL_DETAIL }), {
            status: 500,
          }),
      }),
    },
  } as unknown as ConformanceCandidateEnv;
}

describe('conformance WebSocket replies', () => {
  it('echoes a nonce without reaching the trusted state object', async () => {
    await expect(
      conformanceWebSocketReply(
        env(),
        JSON.stringify({
          contractVersion: 1,
          action: 'nonce-echo',
          nonce: 'probe',
        }),
      ),
    ).resolves.toEqual({
      contractVersion: 1,
      action: 'nonce-echo',
      nonce: 'probe',
    });
  });

  it.each([
    ['a malformed frame', 'not json {{{', 'unknown', 'unknown'],
    [
      'another contract version',
      JSON.stringify({ contractVersion: 2, action: 'nonce-echo', nonce: 'p' }),
      'nonce-echo',
      'p',
    ],
    [
      'an unknown action',
      JSON.stringify({ contractVersion: 1, action: 'nope', nonce: 'p' }),
      'nope',
      'p',
    ],
    [
      'a missing nonce',
      JSON.stringify({ contractVersion: 1, action: 'nonce-echo' }),
      'nonce-echo',
      'unknown',
    ],
  ])('answers %s with an envelope the gate can diagnose', async (_label, frame, action, nonce) => {
    // Echoing action and nonce is what makes the gate's exact-key assertion
    // fire on the first frame instead of timing out.
    await expect(conformanceWebSocketReply(env(), frame)).resolves.toEqual({
      contractVersion: 1,
      action,
      nonce,
      failed: true,
    });
  });

  it('never returns the trusted state object error text', async () => {
    const reply = await conformanceWebSocketReply(
      env(),
      JSON.stringify({
        contractVersion: 1,
        action: 'flowsafe-approval-update',
        nonce: 'probe',
        runId: 'run',
        approvalId: 'abc',
        revision: 1,
      }),
    );
    expect(reply).toEqual({
      contractVersion: 1,
      action: 'flowsafe-approval-update',
      nonce: 'probe',
      failed: true,
    });
    expect(JSON.stringify(reply)).not.toContain('moved past');
  });
});
