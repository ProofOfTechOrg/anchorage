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
// Workflow runs stay on the per-run DurableObjectRunner; runIds stay INV-1
// whichever DO hosts them.
//
// The name IS the tenant carrier, exactly like a runId: threadIds are minted
// `${tenantId}_${uuid}` by memory-id.ts, so `id.name` decodes to the owning
// tenant (docs/agent-memory-tenancy.md). Two assertions, both fail-closed:
// `tenantId` refuses a name carrying no INV-3 prefix (an unscoped thread is a
// cross-tenant read), and every request must carry the AUTHENTICATED tenant
// (THREAD_TENANT_HEADER) matching it.
//
// The header is load-bearing, not ceremony: a name-vs-path check alone
// (DurableObjectRunner's #assertRunIdentity) cannot catch the actual attack —
// tenant B presenting a valid token for tenant A's threadId routes to A's
// instance, where name and path agree. Only comparing the name's tenant against
// the tenant the Worker AUTHENTICATED closes it, which is why this DO refuses
// to serve any request that fails to state one.
//
// Classic constructor(state, env) + fetch contract — deliberately NOT `extends
// DurableObject` from 'cloudflare:workers' — so this module and its graph load
// in node/vitest, the same posture as DurableObjectRunner and HubDurableObject.

import type { ApprovalActor, ApprovalRole } from '../approval-api/contract.js';
import {
  type ExecutionPrincipal,
  isExecutionPrincipal,
  principalActor,
} from '../approval-api/principal.js';
import type { DurableObjectRunnerState } from './cf-types.js';
import { DoStatusError, doErrorResponse } from './do-error-response.js';
import type { InitResult } from './init.js';
import { tenantOfMemoryId } from './memory-id.js';
import { DurableStorageResumeLedger } from './resume-ledger.js';
import {
  THREAD_ACTOR_HEADER,
  THREAD_ACTOR_ROLE_HEADER,
  THREAD_PRINCIPAL_HEADER,
  THREAD_TENANT_HEADER,
} from './thread-header.js';

const THREAD_ACTOR_ROLES: readonly ApprovalRole[] = [
  'admin',
  'builder',
  'operator',
  'reviewer',
  'viewer',
];

/**
 * A request refused at the thread DO's identity boundary: the DO's name carries
 * no tenant, or the request's authenticated tenant is not the one the name
 * carries. Surfaced as 403 — this boundary is internal (a DO namespace is not
 * client-reachable and the Worker 404s a foreign threadId before forwarding),
 * so there is no existence oracle to protect here and a distinct status keeps a
 * routing bug from reading as a generic 500.
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
  /** The tenant the threadId carries, equal to the request's authenticated one. */
  readonly tenantId: string;
  /**
   * Complete server-stamped requester identity, in human shape. Kept for the
   * approval service and every existing thread route; for automated principals
   * it is the least-privileged projection of `principal`.
   */
  readonly actor: ApprovalActor;
  /**
   * WHO is executing — the authority the agent host gates on. A human here is
   * the same identity as `actor`; anything else is automation that must have
   * been declared by the target agent.
   */
  readonly principal: ExecutionPrincipal;
  /** Compatibility alias for actor.id. */
  readonly requestedBy: string;
  /** This DO's storage/runtime/pubsub wiring, built once per instance. */
  readonly init: InitResult;
}

/**
 * Per-thread Durable Object base. Hosts subclass it, supply `build()` (their
 * init() wiring) and `route()` (their thread routes), and bind the subclass
 * under a wrangler namespace addressed `idFromName(threadId)`.
 *
 * `route()` runs only AFTER the tenant assertion, so a track adding a route
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
   * Serve a request whose tenant has already been asserted against this
   * instance's identity. Abstract because the skeleton hosts no routes of its
   * own: the durable-agent host drives the loop through `scope.init.runtime`,
   * and the signals package mounts signal routes on the same asserted scope.
   */
  protected abstract route(
    request: Request,
    scope: ThreadScope,
  ): Promise<Response>;

  /**
   * The thread this instance serves, recovered from its OWN idFromName identity
   * — unforgeable at this boundary (`id.name` is populated only for
   * idFromName-created ids). Throws rather than defaulting: a thread DO that
   * cannot name itself cannot scope anything it stores.
   */
  protected get threadId(): string {
    const name = this.state?.id?.name;
    if (!name) {
      throw new ThreadIdentityError(
        'ThreadDurableObject.threadId: the DO has no id.name (not created via idFromName, or running without state) — thread unresolvable, refusing to serve',
      );
    }
    return name;
  }

  /**
   * The tenant this instance serves, decoded from its own thread id — the same
   * salted-prefix decode used for runIds (one decode, tenantOfMemoryId to
   * tenantOfRunId). Throws on a name carrying no valid tenant prefix: a hand-built or
   * client-chosen threadId never reaches a tenant-scoped store.
   */
  protected get tenantId(): string {
    const threadId = this.threadId;
    const tenantId = tenantOfMemoryId(threadId);
    if (tenantId === undefined) {
      throw new ThreadIdentityError(
        `ThreadDurableObject.tenantId: threadId '${threadId}' carries no INV-3 tenant segment (thread ids are minted \`\${tenantId}_\${uuid}\` — see docs/agent-memory-tenancy.md)`,
      );
    }
    return tenantId;
  }

  async fetch(request: Request): Promise<Response> {
    try {
      // Assert BEFORE building: a refused caller never reaches storage, and the
      // ordering is visible here rather than buried inside the assertion.
      const identity = this.#assertTenantIdentity(request);
      return await this.route(request, {
        ...identity,
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

  /**
   * The chokepoint: the request's authenticated tenant MUST be the one this
   * instance's name carries. An absent header fails it too — the trusted Worker
   * always stamps one, so its absence means the request did not come through
   * authentication, and defaulting to "trust the name" would forfeit exactly the
   * cross-tenant case (a valid token for another tenant's threadId) this check
   * exists for.
   */
  #assertTenantIdentity(request: Request): Omit<ThreadScope, 'init'> {
    const threadId = this.threadId;
    const tenantId = this.tenantId;
    const claimed = request.headers.get(THREAD_TENANT_HEADER);
    if (claimed !== tenantId) {
      throw new ThreadIdentityError(
        `thread identity mismatch: instance '${threadId}' belongs to tenant '${tenantId}' but the request authenticates as '${claimed ?? '<none>'}' — refusing`,
      );
    }
    const actorId = request.headers.get(THREAD_ACTOR_HEADER);
    if (!actorId) {
      throw new ThreadIdentityError(
        `thread identity mismatch: request for '${threadId}' carries no trusted actor`,
      );
    }
    const role = request.headers.get(THREAD_ACTOR_ROLE_HEADER);
    if (
      role === null ||
      !(THREAD_ACTOR_ROLES as readonly string[]).includes(role)
    ) {
      throw new ThreadIdentityError(
        `thread identity mismatch: request for '${threadId}' carries no valid trusted actor role`,
      );
    }
    const principal = this.#principalFrom(
      request,
      actorId,
      role as ApprovalRole,
      tenantId,
    );
    return {
      threadId,
      tenantId,
      actor: principalActor(principal),
      principal,
      requestedBy: principal.id,
    };
  }

  /**
   * Reconstruct the execution principal from the trusted headers.
   *
   * An absent principal header means a human — see THREAD_PRINCIPAL_HEADER for
   * why that default cannot grant authority. A header that IS present but
   * malformed, or that names an automated kind without a purpose, is refused
   * rather than downgraded to human: a caller that tried to assert automation
   * and got it wrong must not silently become a person.
   */
  #principalFrom(
    request: Request,
    actorId: string,
    role: ApprovalRole,
    tenantId: string,
  ): ExecutionPrincipal {
    const header = request.headers.get(THREAD_PRINCIPAL_HEADER);
    if (header === null) {
      return { kind: 'human', id: actorId, tenantId, role };
    }
    let parsed: unknown;
    try {
      parsed = JSON.parse(header);
    } catch {
      throw new ThreadIdentityError(
        `thread identity mismatch: request for '${this.threadId}' carries an unparseable principal`,
      );
    }
    const fields =
      parsed !== null && typeof parsed === 'object'
        ? (parsed as Record<string, unknown>)
        : undefined;
    const candidate =
      fields?.kind === 'human'
        ? { kind: 'human', id: actorId, tenantId, role }
        : { ...fields, id: actorId, tenantId };
    if (!isExecutionPrincipal(candidate)) {
      throw new ThreadIdentityError(
        `thread identity mismatch: request for '${this.threadId}' carries an invalid principal`,
      );
    }
    return candidate;
  }

  #ensureInit(): InitResult {
    if (!this.#init) {
      this.#init = this.build(this.env);
      // Durable resume ledger, adopted HERE for the same reason
      // DurableObjectRunner adopts it rather than taking it as a parameter: an
      // agent loop suspends and resumes like any other run, and ctx.storage is
      // what survives eviction/hibernation/deploys where in-memory class state
      // does not. Absent under node/vitest (state is undefined), where the
      // runtime keeps its in-memory default.
      const storage = this.state?.storage;
      if (storage) {
        this.#init.runtime.adoptDefaultResumeLedger(
          new DurableStorageResumeLedger(storage),
        );
      }
    }
    return this.#init;
  }
}
