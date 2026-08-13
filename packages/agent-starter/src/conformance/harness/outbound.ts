// SPDX-License-Identifier: Apache-2.0

import { WorkerEntrypoint } from 'cloudflare:workers';

import { STATE_EGRESS_HEADERS } from '@proofoftech/flowsafe/host-kit';

/**
 * LOCAL HARNESS ONLY — never deployed, never uploaded by the gate.
 *
 * A stand-in for the fleet's one shared outbound Worker
 * (`packages/fleet-control/src/workers/outbound.ts`), reduced to the two
 * behaviours `scripts/conformance-verify.mjs` needs: prove that
 * `createStateEgressFetch(env)` stamped every reserved attribution header, and
 * apply a hostname allowlist so the denied probe gets a real upstream status.
 *
 * The real entrypoint additionally resolves the canonical `HOSTS` record,
 * compares the credential digest in constant time, and attributes the denial.
 * Only the paid gate exercises that.
 */

export interface HarnessOutboundEnv {
  /** Comma-separated hostnames this deployment may reach. */
  readonly ALLOWED_HOSTS: string;
  readonly DENIED_STATUS: string;
}

const REQUIRED_HEADERS = Object.values(STATE_EGRESS_HEADERS);

/**
 * A named entrypoint must extend `WorkerEntrypoint`; workerd otherwise reports
 * "worker is not an actor but class name was requested" when the binding is
 * resolved, which surfaces as an opaque failure inside the caller.
 */
export class StateEgress extends WorkerEntrypoint<HarnessOutboundEnv> {
  async fetch(request: Request): Promise<Response> {
    const missing = REQUIRED_HEADERS.filter(
      (name) => !request.headers.get(name),
    );
    if (missing.length > 0) {
      return new Response(`state egress is missing ${missing.join(', ')}`, {
        status: 400,
      });
    }
    const allowed = new Set(
      this.env.ALLOWED_HOSTS.split(',')
        .map((host) => host.trim())
        .filter(Boolean),
    );
    const url = new URL(request.url);
    if (!allowed.has(url.hostname)) {
      return new Response('egress denied', {
        status: Number(this.env.DENIED_STATUS),
      });
    }
    // The real entrypoint strips these before the origin request; do the same
    // so the harness cannot pass on a behaviour the platform would not.
    const headers = new Headers(request.headers);
    for (const name of REQUIRED_HEADERS) headers.delete(name);
    return fetch(new Request(request, { headers, redirect: 'manual' }));
  }
}

export default {
  fetch: () => new Response('not found', { status: 404 }),
};
