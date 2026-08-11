// SPDX-License-Identifier: Apache-2.0
// Request-scoped authenticated actor context. The resolver authenticates and
// validates the human before exposing services or server-owned identifiers.

import { DEPLOYMENT_IDENTITY_HEADER } from '../do-runner/deployment-identity.js';
import { EXECUTION_PRINCIPAL_HEADER } from '../do-runner/execution-principal-header.js';
import { mintThreadId, resourceIdFromKey } from '../do-runner/memory-id.js';
import { isPathSafeId } from '../do-runner/path-safe-id.js';
import {
  type ApprovalActor,
  type ApprovalRole,
  DECIDER_ROLES,
} from './contract.js';
import {
  assertExecutionPrincipal,
  canonicalApprovalActor,
  type ExecutionPrincipal,
  humanPrincipal,
  principalActor,
} from './principal.js';
import {
  principalMayAccess,
  principalOwner,
  type ResourceAccess,
  type ResourceClaim,
  type ResourceKind,
  type ResourceOwner,
  ResourceOwnershipError,
  type ResourceOwnershipStore,
  requireCommonResourceOwner,
} from './resource-ownership.js';
import {
  type ApprovalService,
  type SelfDecisionPolicy,
  selfDecisionExempts,
} from './service.js';
import type { ApprovalStore } from './store.js';
import type { ApprovalStoreFactory } from './store-factory.js';

export interface ActorContext {
  /** Already authenticated and validated. */
  readonly actor: ApprovalActor;
  /** Infrastructure-verified deployment tag for audit attribution. */
  readonly deploymentTag?: string;
  /** WHO is executing; authenticated HTTP requests always resolve to human. */
  readonly principal: ExecutionPrincipal;
  /** Immutable owner assigned to resources created through this context. */
  readonly resourceOwner: ResourceOwner;
  /** The approval service over this deployment's store. */
  service(): ApprovalService;
  /** Mint a server-owned path-safe run id. */
  newRunId(): string;
  /** Mint a server-owned path-safe memory thread id. */
  newThreadId(): string;
  /** Validate a host-owned business key as a memory resource id. */
  resourceIdFromKey(resourceKey: string): string;
  /** Atomically bind a server-selected id to this principal. */
  claimResource(kind: ResourceKind, resourceId: string): Promise<void>;
  /** Release a claim after the authoritative domain record is gone. */
  releaseResource(kind: ResourceKind, resourceId: string): Promise<void>;
  /** Return the registered owner for trusted host-side policy composition. */
  resourceOwnerFor(
    kind: ResourceKind,
    resourceId: string,
  ): Promise<ResourceOwner | undefined>;
  /** Resolve deployment-local ownership without exposing existence. */
  canAccessResource(
    kind: ResourceKind,
    resourceId: string,
    access: ResourceAccess,
  ): Promise<boolean>;
  /** Display hint only; ApprovalService remains authoritative. */
  canSelfDecide(role: ApprovalRole): boolean;
}

export type ActorResolver = (
  request: Request,
) => Promise<ActorContext | undefined>;

export class ActorResolutionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ActorResolutionError';
  }
}

export interface CreateActorResolverOptions {
  /** The host's authentication seam. */
  authenticate: (
    request: Request,
  ) => Promise<ApprovalActor | undefined> | ApprovalActor | undefined;
  /** The deployment approval-store factory. */
  storeFactory: ApprovalStoreFactory;
  /** Infrastructure-configured deployment tag, never derived from claims. */
  deploymentTag?: string;
  /** Host-specific service assembly, called lazily at most once per request. */
  buildService: (store: ApprovalStore, actor: ApprovalActor) => ApprovalService;
  /** Run-id generator. Default: crypto.randomUUID. */
  newRunId?: () => string;
  /** The same self-decision policy supplied to ApprovalService. */
  allowSelfDecision?: SelfDecisionPolicy;
}

export interface CreatePrincipalActorContextOptions {
  /** Trusted execution identity; canonicalized once before any async work. */
  principal: ExecutionPrincipal;
  storeFactory: ApprovalStoreFactory;
  deploymentTag?: string;
  buildService: (store: ApprovalStore, actor: ApprovalActor) => ApprovalService;
  newRunId?: () => string;
  canSelfDecide?: (role: ApprovalRole) => boolean;
}

/** Build one authority-bearing context from a canonical execution principal. */
export function createPrincipalActorContext(
  options: CreatePrincipalActorContextOptions,
): ActorContext {
  const principal = assertExecutionPrincipal(
    options.principal,
    'actor context principal',
  );
  const actor = principalActor(principal);
  const owner = principalOwner(principal);
  const resources = options.storeFactory.resources();
  const mintUuid = options.newRunId ?? (() => crypto.randomUUID());
  let service: ApprovalService | undefined;
  return {
    actor,
    ...(options.deploymentTag !== undefined
      ? { deploymentTag: options.deploymentTag }
      : {}),
    principal,
    resourceOwner: owner,
    service: () => {
      service ??= options.buildService(options.storeFactory.store(), actor);
      return service;
    },
    newRunId: () => {
      const runId = mintUuid();
      if (!isPathSafeId(runId)) {
        throw new Error(
          `newRunId: generated runId '${runId}' must match PATH_SAFE_ID_PATTERN`,
        );
      }
      return runId;
    },
    newThreadId: () => mintThreadId(mintUuid),
    resourceIdFromKey: resourceIdFromKey,
    claimResource: async (kind, resourceId) => {
      if (!(await resources.claim(kind, resourceId, owner))) {
        throw new ResourceOwnershipError(kind, resourceId);
      }
    },
    releaseResource: async (kind, resourceId) => {
      const stored = await resources.owner(kind, resourceId);
      if (!stored) return;
      if (!principalMayAccess(principal, stored, 'write')) {
        throw new ResourceOwnershipError(kind, resourceId);
      }
      await resources.release(kind, resourceId, stored);
    },
    resourceOwnerFor: (kind, resourceId) => resources.owner(kind, resourceId),
    canAccessResource: async (kind, resourceId, access) => {
      const stored = await resources.owner(kind, resourceId);
      return (
        stored !== undefined && principalMayAccess(principal, stored, access)
      );
    },
    canSelfDecide: options.canSelfDecide ?? (() => false),
  };
}

/** Rebind mutations and reads to the common registered owner of trusted ids. */
export async function withRegisteredResourceOwner(
  context: ActorContext,
  resources: ResourceOwnershipStore,
  claims: readonly ResourceClaim[],
): Promise<ActorContext> {
  const owner = await requireCommonResourceOwner(resources, claims);
  return {
    ...context,
    resourceOwner: owner,
    claimResource: async (kind, resourceId) => {
      if (!(await resources.claim(kind, resourceId, owner))) {
        throw new ResourceOwnershipError(kind, resourceId);
      }
    },
    releaseResource: async (kind, resourceId) => {
      await resources.release(kind, resourceId, owner);
    },
    resourceOwnerFor: (kind, resourceId) => resources.owner(kind, resourceId),
    canAccessResource: async (kind, resourceId) => {
      const stored = await resources.owner(kind, resourceId);
      return stored?.kind === owner.kind && stored.id === owner.id;
    },
  };
}

export function createActorResolver(
  options: CreateActorResolverOptions,
): ActorResolver {
  const mintUuid = options.newRunId ?? (() => crypto.randomUUID());
  return async (request) => {
    if (
      request.headers.has(EXECUTION_PRINCIPAL_HEADER) ||
      request.headers.has(DEPLOYMENT_IDENTITY_HEADER) ||
      request.headers.has('x-flowsafe-actor') ||
      request.headers.has('x-flowsafe-role') ||
      request.headers.has('x-flowsafe-tenant')
    ) {
      throw new ActorResolutionError(
        'inbound request carries an internal server-stamped identity header — refusing to scope it',
      );
    }
    const authenticated = await options.authenticate(request);
    if (!authenticated) return undefined;
    const actor = canonicalApprovalActor(authenticated);
    if (!actor) {
      throw new ActorResolutionError(
        'authenticated claims carry an invalid actor id or role — fix the verifier; refusing to scope the request',
      );
    }
    const principal = humanPrincipal(actor);
    return createPrincipalActorContext({
      principal,
      storeFactory: options.storeFactory,
      ...(options.deploymentTag !== undefined
        ? { deploymentTag: options.deploymentTag }
        : {}),
      buildService: options.buildService,
      newRunId: mintUuid,
      canSelfDecide: (role: ApprovalRole) =>
        DECIDER_ROLES.includes(role) &&
        selfDecisionExempts(options.allowSelfDecision, role),
    });
  };
}
