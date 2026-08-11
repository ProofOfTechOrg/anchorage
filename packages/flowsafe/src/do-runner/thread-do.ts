// SPDX-License-Identifier: Apache-2.0
// Per-thread Durable Object — the host for a thread's agent loop (DL-002/DL-020).
//
// WHY a DO per thread: core drains signals through an in-process registry
// closure and documents that "signals sent to a restarted worker will not be
// drained" by a cross-process engine; Mastra's own cross-instance answer is
// Redis distributed leasing. On Cloudflare a DO instance keyed by name IS that
// lease — idFromName(threadId) serializes every send/subscribe for a thread onto
// one isolate — so the platform supplies the affinity for free. Thread-affine
// from day one (DL-020): a threadless agent run mints a throwaway thread id
// rather than starting per-run and paying a migration when signals land.
// Workflow runs stay on the per-run DurableObjectRunner; runIds stay
// server-minted whichever DO hosts them.
//
// Identity at this boundary is the EXECUTION PRINCIPAL: every request must
// carry the trusted Worker-stamped EXECUTION_PRINCIPAL_HEADER, the sole channel
// for WHO is executing. A request without one did not come through
// `createThreadTopology` (the only sanctioned way to reach a thread DO) and is
// refused — defaulting to a human would let a dropped header turn automation
// into a person. The deployment-identity check (env tag vs D1 sentinel) runs
// before any route, so a mis-provisioned namespace refuses rather than serves.
//
// Classic constructor(state, env) + fetch contract — deliberately NOT `extends
// DurableObject` from 'cloudflare:workers' — so this module and its graph load
// in node/vitest, the same posture as DurableObjectRunner and HubDurableObject.

import {
  decodeExecutionPrincipal,
  type ExecutionPrincipal,
} from '../approval-api/principal.js';
import type { DurableObjectRunnerState } from './cf-types.js';
import {
  verifyDurableObjectDeploymentIdentity,
  verifyDurableObjectDeploymentRequest,
} from './deployment-identity.js';
import { DoStatusError, doErrorResponse } from './do-error-response.js';
import { EXECUTION_PRINCIPAL_HEADER } from './execution-principal-header.js';
import type { InitResult } from './init.js';
import { isPathSafeId } from './path-safe-id.js';

const THREAD_ALARM_RECOVERY_DELAY_MS = 60_000;

/**
 * A request refused at the thread DO's identity boundary: the DO's name is
 * unresolvable, or the request carries no valid execution principal. Surfaced
 * as 403 — this boundary is internal (a DO namespace is not client-reachable
 * and the Worker validates the threadId before forwarding), so there is no
 * existence oracle to protect here and a distinct status keeps a routing bug
 * from reading as a generic 500.
 *
 * Extends DoStatusError because that is how a status opts in: this class is what
 * doErrorResponse recognizes, not the shape of its fields.
 */
export class ThreadIdentityError extends DoStatusError {
  readonly status = 403;

  constructor(message: string) {
    super(message);
    this.name = 'ThreadIdentityError';
  }
}

/** The identity + wiring every thread route receives, already asserted. */
export interface ThreadScope {
  /** This instance's thread — its own idFromName identity, never the request's. */
  readonly threadId: string;
  /** Verified infrastructure tag for audit attribution (present under workerd). */
  readonly deploymentTag?: string;
  /**
   * WHO is executing — the authority the agent host gates on. Automated
   * principals must have been declared by the target agent.
   */
  readonly principal: ExecutionPrincipal;
  /** This DO's storage/runtime/pubsub wiring, built once per instance. */
  readonly init: InitResult;
}

/**
 * Per-thread Durable Object base. Hosts subclass it, supply `build()` (their
 * init() wiring) and `route()` (their thread routes), and bind the subclass
 * under a wrangler namespace addressed `idFromName(threadId)`.
 *
 * `route()` runs only AFTER the identity assertion, so a track adding a route
 * cannot forget it — the reason the dispatch is a template method rather than a
 * fetch() each subclass writes.
 */
export abstract class ThreadDurableObject<TEnv = unknown> {
  protected readonly env: TEnv;
  /** Absent in node tests; present under workerd. */
  protected readonly state?: DurableObjectRunnerState;
  #init?: InitResult;

  constructor(state: DurableObjectRunnerState | undefined, env: TEnv) {
    this.state = state;
    this.env = env;
  }

  /**
   * Build this DO's storage + runtime + pubsub identity via init(). Called once,
   * lazily — one init() per instance is what makes its pubsub the isolate's ONE
   * identity (pubsub.ts), so never call init() again from a route.
   */
  protected abstract build(env: TEnv): InitResult;

  /**
   * Serve a request whose identity has already been asserted against this
   * instance. Abstract because the skeleton hosts no routes of its own: the
   * durable-agent host drives the loop through `scope.init.runtime`, and the
   * signals package mounts signal routes on the same asserted scope.
   */
  protected abstract route(
    request: Request,
    scope: ThreadScope,
  ): Promise<Response>;

  /** Optional Durable Object alarm work after deployment identity is verified. */
  protected async onAlarm(
    _env: TEnv,
    _threadId: string,
    _init: InitResult,
  ): Promise<void> {}

  /**
   * The thread this instance serves, recovered from its OWN idFromName identity
   * — unforgeable at this boundary (`id.name` is populated only for
   * idFromName-created ids). Throws rather than defaulting: a thread DO that
   * cannot name itself cannot scope anything it stores.
   */
  protected get threadId(): string {
    const name = this.state?.id?.name;
    if (!isPathSafeId(name)) {
      throw new ThreadIdentityError(
        'ThreadDurableObject.threadId: the DO has no path-safe id.name — thread unresolvable, refusing to serve',
      );
    }
    return name;
  }

  async fetch(request: Request): Promise<Response> {
    try {
      // Deployment identity BEFORE request identity: a mis-provisioned
      // namespace (env tag vs D1 sentinel) refuses every request outright.
      // No-op off workerd (state undefined), memoized after first success.
      const deploymentTag = await verifyDurableObjectDeploymentRequest(
        request,
        this.state,
        this.env,
      );
      // Assert BEFORE building: a refused caller never reaches storage, and the
      // ordering is visible here rather than buried inside the assertion.
      const identity = this.#assertIdentity(request);
      return await this.route(request, {
        ...identity,
        ...(deploymentTag !== undefined ? { deploymentTag } : {}),
        init: this.#ensureInit(),
      });
    } catch (error) {
      // The SAME taxonomy DurableObjectRunner answers with: route() drives runs
      // through scope.init.runtime, so its typed errors (unknown run -> 404, not
      // suspended -> 409, malformed -> 400) must survive to the caller rather
      // than collapse into a 500 that reads as "this DO is broken".
      // ThreadIdentityError carries its own 403.
      return doErrorResponse(error);
    }
  }

  async alarm(): Promise<void> {
    const threadId = this.threadId;
    await this.state?.storage.setAlarm?.(
      Date.now() + THREAD_ALARM_RECOVERY_DELAY_MS,
    );
    try {
      await verifyDurableObjectDeploymentIdentity(this.state, this.env);
      await this.onAlarm(this.env, threadId, this.#ensureInit());
    } catch (error) {
      await this.state?.storage.setAlarm?.(
        Date.now() + THREAD_ALARM_RECOVERY_DELAY_MS,
      );
      throw error;
    }
  }

  /**
   * The chokepoint: every request must carry the trusted execution principal.
   * `route()` receives the asserted scope, never the raw request identity.
   */
  #assertIdentity(request: Request): Omit<ThreadScope, 'init'> {
    const threadId = this.threadId;
    const principal = this.#principalFrom(request);
    return {
      threadId,
      principal,
    };
  }

  /**
   * Reconstruct the execution principal from the trusted header.
   *
   * ABSENT IS A REFUSAL, not a human default. `createThreadTopology` stamps
   * this on every send and forward — the only sanctioned way to reach a thread
   * DO — so a request without it did not come through the topology. Defaulting
   * to a human here would let a dropped header turn automation into a person.
   *
   * This is the SOLE identity channel: every route consumes `scope.principal`,
   * so no parallel actor/requester representation can disagree with it.
   */
  #principalFrom(request: Request): ExecutionPrincipal {
    const header = request.headers.get(EXECUTION_PRINCIPAL_HEADER);
    if (header === null) {
      throw new ThreadIdentityError(
        `thread identity mismatch: request for '${this.threadId}' carries no trusted execution principal`,
      );
    }
    const principal = decodeExecutionPrincipal(header);
    if (!principal) {
      throw new ThreadIdentityError(
        `thread identity mismatch: request for '${this.threadId}' carries an invalid execution principal`,
      );
    }
    return principal;
  }

  #ensureInit(): InitResult {
    if (!this.#init) {
      this.#init = this.build(this.env);
    }
    return this.#init;
  }
}
