// SPDX-License-Identifier: Apache-2.0
// The per-thread DO topology seam — and the minter for the execution-principal
// header ThreadDurableObject verifies (DL-002). Mint and verify ship together,
// because a verifier whose input the caller can write is not a check.
//
// THE TRAP THIS EXISTS TO CLOSE. The house idiom for forwarding into a DO is
// hub-topology.ts's `forwardSubscribe(request) => stub().fetch(request)` — the
// CLIENT's Request, headers and all. That is safe for the hub, which trusts the
// forwarded request for nothing. A thread
// route copying that shape would hand the client the very header the thread DO
// authenticates on. This module, not the caller, decides the principal header's
// value on every path, including forwarded upgrades, where it overwrites.
//
// Reach a thread DO through here. A route that addresses the namespace itself
// re-opens the hole, so `send`/`forward` are the sanctioned surface and both
// validate the threadId before the DO is addressed.
//
// Structural namespace/stub types (method syntax, so a real
// DurableObjectNamespace satisfies them under TS method-parameter bivariance),
// keeping host-kit free of @cloudflare/workers-types — same convention as
// RunnerNamespaceLike / HubNamespaceLike.

import {
  type ActorContext,
  assertExecutionPrincipal,
  encodeExecutionPrincipal,
} from '../approval-api/index.js';
import {
  deploymentIdentityHeaders,
  EXECUTION_PRINCIPAL_HEADER,
  stampDeploymentIdentityRequest,
} from '../do-runner/index.js';
import { requireMemoryId } from './memory-boundary.js';

/**
 * The subset of a DurableObjectStub the thread topology uses. The raw-`Request`
 * overload is what lets a WebSocket UPGRADE forward through the stub unchanged;
 * the string/init overload carries ordinary JSON routes.
 */
export interface ThreadStubLike {
  fetch(request: Request): Promise<Response>;
  fetch(
    url: string,
    init?: {
      method?: string;
      headers?: Record<string, string>;
      body?: string;
    },
  ): Promise<Response>;
}

/** The subset of a DurableObjectNamespace the thread topology uses. */
export interface ThreadNamespaceLike<Id = unknown> {
  idFromName(name: string): Id;
  get(id: Id): ThreadStubLike;
}

/** What `send` carries beyond the sanctioned header — a route's own payload. */
export interface ThreadRequestInit {
  method?: string;
  headers?: Record<string, string>;
  body?: string;
}

export type ThreadPrincipalContext = Pick<ActorContext, 'principal'>;

/** A standing-memory target that must resolve to a durable thread binding. */
export interface BoundThreadTarget {
  threadId: string;
  resourceId?: string;
  agentId?: string;
}

/** Target-side proof that a thread is durable memory, not an ephemeral run id. */
export type BoundThreadTargetValidator = (
  context: ActorContext,
  target: BoundThreadTarget,
) => Promise<void>;

export interface ThreadTopology {
  /**
   * Send an authenticated request to a thread DO. `path` is the DO-side route
   * (e.g. `/messages`). Throws RunRouteError(404) when `threadId` is malformed,
   * before the DO is addressed.
   */
  send(
    context: ThreadPrincipalContext,
    threadId: string,
    path: string,
    init?: ThreadRequestInit,
  ): Promise<Response>;
  /**
   * Forward a client Request (e.g. a verified WebSocket upgrade) to a thread DO,
   * with the principal header overwritten from the authenticated context. Same
   * validation 404 as `send`. Use this instead of `stub.fetch(request)`.
   */
  forward(
    context: ThreadPrincipalContext,
    threadId: string,
    request: Request,
  ): Promise<Response>;
}

/**
 * The principal is the only identity on the wire; no parallel actor header can
 * disagree with what executes.
 */
function stampedPrincipal(context: ThreadPrincipalContext) {
  return assertExecutionPrincipal(
    context.principal,
    'thread topology principal',
  );
}

export function createThreadTopology<Id>(
  namespace: ThreadNamespaceLike<Id>,
  deploymentIdentitySecret: string,
): ThreadTopology {
  // One DO per thread: id.name is the host-minted threadId (DL-002).
  const stub = (threadId: string): ThreadStubLike =>
    namespace.get(namespace.idFromName(threadId));

  const addressed = (threadId: string): string =>
    requireMemoryId(threadId, 'threadId');

  // Both surfaces are `async` so the ownership refusal REJECTS rather than
  // throwing synchronously out of a Promise-typed call: a caller writing
  // `topology.send(...).catch(handle)` would never see a sync throw, and the
  // 404 would escape past the very handler meant to map it.
  return {
    send: async (context, threadId, path, init = {}) => {
      // Merge through Headers so the stamp wins by case-insensitive name. A
      // plain-object spread can preserve duplicate case variants instead.
      const merged = new Headers(init.headers);
      const principal = stampedPrincipal(context);
      merged.set(
        EXECUTION_PRINCIPAL_HEADER,
        encodeExecutionPrincipal(principal),
      );
      // Retired identity headers: nothing reads them, but a caller's value must
      // not ride into the DO as if the topology had stamped it.
      merged.delete('x-flowsafe-actor');
      merged.delete('x-flowsafe-role');
      merged.delete('x-flowsafe-tenant');
      const headers = deploymentIdentityHeaders(
        deploymentIdentitySecret,
        merged,
      );
      return stub(addressed(threadId)).fetch(`http://thread${path}`, {
        ...init,
        headers,
      });
    },
    forward: async (context, threadId, request) => {
      const threadName = addressed(threadId);
      // A cloned Request has MUTABLE headers where an inbound one does not, so
      // this is what lets the overwrite happen at all — and `set` (not `append`)
      // is what makes a forged client value vanish rather than ride along as a
      // second value the DO might read.
      const forwarded = stampDeploymentIdentityRequest(
        request,
        deploymentIdentitySecret,
      );
      const principal = stampedPrincipal(context);
      // Retired identity headers: nothing reads them, but a client's forged
      // value must not ride into the DO as if the topology had stamped it.
      forwarded.headers.delete('x-flowsafe-actor');
      forwarded.headers.delete('x-flowsafe-role');
      forwarded.headers.delete('x-flowsafe-tenant');
      forwarded.headers.set(
        EXECUTION_PRINCIPAL_HEADER,
        encodeExecutionPrincipal(principal),
      );
      return stub(threadName).fetch(forwarded);
    },
  };
}
