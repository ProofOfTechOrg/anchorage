// SPDX-License-Identifier: Apache-2.0

import type { D1Database, R2Bucket } from '@cloudflare/workers-types';
import type { AuditQueue } from '@proofoftech/flowsafe/audit-export';
import type {
  FlowsafeWorkerEnv,
  StateEgressEnv,
} from '@proofoftech/flowsafe/host-kit';

/**
 * The fleet roles are separate Workers, so they get separate `Env` types rather
 * than optional fields on the standalone starter's global `Env`. Fleet control
 * decides both binding sets — see
 * `packages/fleet-control/src/cloudflare-client.ts` `uploadDispatchWorker`
 * (candidate) and `uploadNamespacedStateWorker` (trusted state) — so a field
 * here that fleet control does not supply is a bug, not a configuration knob.
 */

/**
 * The subset of a Durable Object namespace these artifacts address by name.
 * Generic over the id so `get` can only receive what `idFromName` produced,
 * matching `RunnerNamespaceLike` in flowsafe's host-kit.
 */
export interface NamespaceLike<Id = unknown> {
  idFromName(name: string): Id;
  get(id: Id): { fetch(request: Request): Promise<Response> };
}

/** Bindings fleet control gives an external candidate. */
export interface ConformanceCandidateEnv {
  // DB, DEPLOYMENT_TENANT, FLEET_ENVIRONMENT and FLEET_SPEC_DIGEST are declared
  // to pin what fleet control supplies, not because this role reads them: the
  // candidate proxies every durable action to trusted state, which owns D1.
  readonly DB: D1Database;
  readonly DEPLOYMENT_TENANT: string;
  readonly DEPLOYMENT_IDENTITY_SECRET: string;

  /** Application bindings this deployment declares. */
  readonly APPLICATION_MODE: string;
  readonly APPLICATION_CONFORMANCE_SECRET: string;
  readonly APPLICATION_FILES: R2Bucket;

  /** Remote Durable Object bindings into the stable trusted state script. */
  readonly CONFORMANCE_STATE: NamespaceLike;
  readonly AUDIT_PROXY: NamespaceLike;
  /** Present only from the release whose migration added the class. */
  readonly CONFORMANCE_V2?: NamespaceLike;

  /** Static fleet attribution. Plain text, never confidential. */
  readonly FLEET_ENVIRONMENT?: string;
  readonly FLEET_SPEC_DIGEST?: string;
}

/**
 * Bindings fleet control gives the trusted state script. An intersection rather
 * than an `extends` clause: the two library env contracts declare
 * `DEPLOYMENT_IDENTITY_SECRET` with different readonly modifiers, and an
 * interface cannot extend both.
 */
export type ConformanceStateEnv = FlowsafeWorkerEnv &
  StateEgressEnv & {
    readonly DB: D1Database & FlowsafeWorkerEnv['DB'];
    readonly MAINTENANCE_ADMIN_SECRET: string;
    readonly CONFORMANCE_STATE: NamespaceLike;
    readonly CONFORMANCE_V2?: NamespaceLike;
    /** The trusted audit object's own local namespace and queue producer. */
    readonly FLEET_AUDIT_PROXY_OBJECT?: NamespaceLike;
    readonly AUDIT_QUEUE?: AuditQueue<unknown>;
  };
