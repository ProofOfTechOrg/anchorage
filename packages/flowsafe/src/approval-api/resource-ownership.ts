// SPDX-License-Identifier: Apache-2.0
// Deployment-local resource ownership. Physical deployment isolation removes
// cross-organization predicates; this registry retains the independent
// user-to-user boundary for opaque run/thread/schedule ids and validated
// host-owned resource keys.

import { isPathSafeId } from '../do-runner/path-safe-id.js';
import type {
  ExecutionPrincipal,
  ExecutionPrincipalKind,
} from './principal.js';
import {
  assertExecutionPrincipal,
  isExecutionPrincipalId,
  isExecutionPrincipalKind,
} from './principal.js';

export const RESOURCE_KINDS = [
  'run',
  'thread',
  'resource',
  'schedule',
] as const;

export type ResourceKind = (typeof RESOURCE_KINDS)[number];
export type ResourceAccess = 'read' | 'write';

export interface ResourceOwner {
  readonly kind: ExecutionPrincipalKind;
  readonly id: string;
}

export interface ResourceClaim {
  readonly kind: ResourceKind;
  readonly resourceId: string;
}

export interface ResourceOwnershipStore {
  /** Claim an available id; returns false for pending or foreign-owned ids. */
  claim(
    kind: ResourceKind,
    resourceId: string,
    owner: ResourceOwner,
  ): Promise<boolean>;
  owner(
    kind: ResourceKind,
    resourceId: string,
  ): Promise<ResourceOwner | undefined>;
  /** Release a committed id only when it is still owned by the expected principal. */
  release(
    kind: ResourceKind,
    resourceId: string,
    owner: ResourceOwner,
  ): Promise<boolean>;
}

export interface RecoverableResourceOwnershipStore
  extends ResourceOwnershipStore {
  /** Reserve every id under one recoverable target-side attempt token. */
  reserveAll(
    claims: readonly ResourceClaim[],
    owner: ResourceOwner,
    token: string,
  ): Promise<boolean>;
  /** Delete selected token-created claims and commit every retained claim. */
  settleReservation(
    token: string,
    release: readonly ResourceClaim[],
  ): Promise<void>;
}

export interface ResourceOwnershipDatabase {
  prepare(query: string): ResourceOwnershipStatement;
  batch?(statements: ResourceOwnershipStatement[]): Promise<unknown[]>;
}

export interface ResourceOwnershipStatement {
  bind(...values: unknown[]): ResourceOwnershipStatement;
  first<T = unknown>(): Promise<T | null>;
  run(): Promise<unknown>;
}

export class ResourceOwnershipError extends Error {
  constructor(kind: ResourceKind, resourceId: string) {
    super(
      `${kind} '${resourceId}' is unavailable or owned by another principal`,
    );
    this.name = 'ResourceOwnershipError';
  }
}

function canonicalResourceKind(kind: unknown): ResourceKind {
  if (
    typeof kind !== 'string' ||
    !RESOURCE_KINDS.includes(kind as ResourceKind)
  ) {
    throw new Error('resource kind is invalid');
  }
  return kind as ResourceKind;
}

function canonicalResourceId(resourceId: unknown): string {
  if (!isPathSafeId(resourceId)) {
    throw new Error('resource id must be path-safe');
  }
  return resourceId;
}

function ownDataField(value: object, key: PropertyKey): unknown {
  const descriptor = Object.getOwnPropertyDescriptor(value, key);
  return descriptor !== undefined && 'value' in descriptor
    ? descriptor.value
    : undefined;
}

export function canonicalResourceOwner(owner: unknown): ResourceOwner {
  if (owner === null || typeof owner !== 'object') {
    throw new Error('resource owner must be a valid execution principal');
  }
  const kind = ownDataField(owner, 'kind');
  const id = ownDataField(owner, 'id');
  if (!isExecutionPrincipalKind(kind) || !isExecutionPrincipalId(id)) {
    throw new Error('resource owner must be a valid execution principal');
  }
  return Object.freeze({ kind, id });
}

function canonicalClaims(claims: readonly ResourceClaim[]): ResourceClaim[] {
  if (!Array.isArray(claims))
    throw new Error('resource claims must be an array');
  const unique = new Set<string>();
  return Array.from(claims, (claim: unknown) => {
    if (claim === null || typeof claim !== 'object') {
      throw new Error('resource claim is invalid');
    }
    const kind = canonicalResourceKind(ownDataField(claim, 'kind'));
    const resourceId = canonicalResourceId(ownDataField(claim, 'resourceId'));
    const key = `${kind}:${resourceId}`;
    if (unique.has(key)) throw new Error(`duplicate resource claim '${key}'`);
    unique.add(key);
    return Object.freeze({ kind, resourceId });
  });
}

function sameOwner(left: ResourceOwner, right: ResourceOwner): boolean {
  return left.kind === right.kind && left.id === right.id;
}

export const RESOURCE_OWNERSHIP_TABLE = 'flowsafe_resource_owners';
const SCHEMA = `CREATE TABLE IF NOT EXISTS ${RESOURCE_OWNERSHIP_TABLE} (
    resource_kind TEXT NOT NULL CHECK (resource_kind IN ('run', 'thread', 'resource', 'schedule')),
    resource_id TEXT NOT NULL,
    owner_kind TEXT NOT NULL CHECK (owner_kind IN ('human', 'service', 'agent', 'system')),
    owner_id TEXT NOT NULL,
    reservation_token TEXT,
    PRIMARY KEY (resource_kind, resource_id)
  )`;

export async function createResourceOwnershipSchema(
  db: Pick<ResourceOwnershipDatabase, 'prepare'>,
): Promise<void> {
  await db.prepare(SCHEMA).run();
}

interface ResourceOwnerRow {
  owner_kind: unknown;
  owner_id: unknown;
  reservation_token?: unknown;
}

function ownerFromRow(row: ResourceOwnerRow | null): ResourceOwner | undefined {
  if (row === null) return undefined;
  try {
    return canonicalResourceOwner({
      kind: row.owner_kind,
      id: row.owner_id,
    });
  } catch {
    return undefined;
  }
}

export class D1ResourceOwnershipStore
  implements RecoverableResourceOwnershipStore
{
  readonly #db: ResourceOwnershipDatabase;
  readonly #ready: () => Promise<void>;

  constructor(
    db: ResourceOwnershipDatabase,
    options: { ready?: () => Promise<void> } = {},
  ) {
    this.#db = db;
    if (options.ready) {
      this.#ready = options.ready;
    } else {
      let ready: Promise<void> | undefined;
      this.#ready = () => {
        ready ??= createResourceOwnershipSchema(db).catch((error: unknown) => {
          ready = undefined;
          throw error;
        });
        return ready;
      };
    }
  }

  async claim(
    kind: ResourceKind,
    resourceId: string,
    owner: ResourceOwner,
  ): Promise<boolean> {
    const safeKind = canonicalResourceKind(kind);
    const safeResourceId = canonicalResourceId(resourceId);
    const safeOwner = canonicalResourceOwner(owner);
    await this.#ready();
    await this.#db
      .prepare(
        `INSERT OR IGNORE INTO ${RESOURCE_OWNERSHIP_TABLE}
          (resource_kind, resource_id, owner_kind, owner_id)
         VALUES (?, ?, ?, ?)`,
      )
      .bind(safeKind, safeResourceId, safeOwner.kind, safeOwner.id)
      .run();
    const stored = await this.owner(safeKind, safeResourceId);
    return stored !== undefined && sameOwner(stored, safeOwner);
  }

  async reserveAll(
    claims: readonly ResourceClaim[],
    owner: ResourceOwner,
    token: string,
  ): Promise<boolean> {
    const safeOwner = canonicalResourceOwner(owner);
    const safeToken = canonicalResourceId(token);
    const safeClaims = canonicalClaims(claims);
    if (safeClaims.length === 0) return true;
    await this.#ready();
    const requested = safeClaims.map(() => '(?, ?)').join(', ');
    await this.#db
      .prepare(
        `WITH requested(resource_kind, resource_id) AS (VALUES ${requested})
         INSERT OR IGNORE INTO ${RESOURCE_OWNERSHIP_TABLE}
           (resource_kind, resource_id, owner_kind, owner_id, reservation_token)
         SELECT resource_kind, resource_id, ?, ?, ? FROM requested
         WHERE NOT EXISTS (
           SELECT 1 FROM requested AS candidate
           JOIN ${RESOURCE_OWNERSHIP_TABLE} AS existing
             ON existing.resource_kind = candidate.resource_kind
            AND existing.resource_id = candidate.resource_id
           WHERE existing.owner_kind <> ? OR existing.owner_id <> ?
              OR (existing.reservation_token IS NOT NULL
                  AND existing.reservation_token <> ?)
         )`,
      )
      .bind(
        ...safeClaims.flatMap((claim) => [claim.kind, claim.resourceId]),
        safeOwner.kind,
        safeOwner.id,
        safeToken,
        safeOwner.kind,
        safeOwner.id,
        safeToken,
      )
      .run();
    for (const claim of safeClaims) {
      const row = await this.#db
        .prepare(
          `SELECT owner_kind, owner_id, reservation_token
           FROM ${RESOURCE_OWNERSHIP_TABLE}
           WHERE resource_kind = ? AND resource_id = ?`,
        )
        .bind(claim.kind, claim.resourceId)
        .first<ResourceOwnerRow>();
      const stored = ownerFromRow(row);
      if (
        !stored ||
        !sameOwner(stored, safeOwner) ||
        (row?.reservation_token != null && row.reservation_token !== safeToken)
      ) {
        return false;
      }
    }
    return true;
  }

  async settleReservation(
    token: string,
    release: readonly ResourceClaim[],
  ): Promise<void> {
    const safeToken = canonicalResourceId(token);
    const safeRelease = canonicalClaims(release);
    await this.#ready();
    const batch = this.#db.batch?.bind(this.#db);
    if (!batch) {
      throw new Error(
        'settleReservation requires database.batch() for atomic recovery',
      );
    }
    const statements: ResourceOwnershipStatement[] = [];
    for (const claim of safeRelease) {
      statements.push(
        this.#db
          .prepare(
            `DELETE FROM ${RESOURCE_OWNERSHIP_TABLE}
             WHERE resource_kind = ? AND resource_id = ?
               AND reservation_token = ?`,
          )
          .bind(claim.kind, claim.resourceId, safeToken),
      );
    }
    statements.push(
      this.#db
        .prepare(
          `UPDATE ${RESOURCE_OWNERSHIP_TABLE}
         SET reservation_token = NULL
         WHERE reservation_token = ?`,
        )
        .bind(safeToken),
    );
    await batch(statements);
  }

  async owner(
    kind: ResourceKind,
    resourceId: string,
  ): Promise<ResourceOwner | undefined> {
    const safeKind = canonicalResourceKind(kind);
    const safeResourceId = canonicalResourceId(resourceId);
    await this.#ready();
    const row = await this.#db
      .prepare(
        `SELECT owner_kind, owner_id FROM ${RESOURCE_OWNERSHIP_TABLE}
         WHERE resource_kind = ? AND resource_id = ?
           AND reservation_token IS NULL`,
      )
      .bind(safeKind, safeResourceId)
      .first<ResourceOwnerRow>();
    return ownerFromRow(row);
  }

  async release(
    kind: ResourceKind,
    resourceId: string,
    owner: ResourceOwner,
  ): Promise<boolean> {
    const safeKind = canonicalResourceKind(kind);
    const safeResourceId = canonicalResourceId(resourceId);
    const safeOwner = canonicalResourceOwner(owner);
    await this.#ready();
    const result = (await this.#db
      .prepare(
        `DELETE FROM ${RESOURCE_OWNERSHIP_TABLE}
         WHERE resource_kind = ? AND resource_id = ?
           AND owner_kind = ? AND owner_id = ?
           AND reservation_token IS NULL`,
      )
      .bind(safeKind, safeResourceId, safeOwner.kind, safeOwner.id)
      .run()) as { meta?: { changes?: number } };
    return Number(result.meta?.changes ?? 0) > 0;
  }
}

export class InMemoryResourceOwnershipStore
  implements RecoverableResourceOwnershipStore
{
  readonly #owners = new Map<string, ResourceOwner>();
  readonly #reservations = new Map<string, string>();

  async claim(
    kind: ResourceKind,
    resourceId: string,
    owner: ResourceOwner,
  ): Promise<boolean> {
    const safeKind = canonicalResourceKind(kind);
    const safeResourceId = canonicalResourceId(resourceId);
    const safeOwner = canonicalResourceOwner(owner);
    const key = `${safeKind}:${safeResourceId}`;
    const stored = this.#owners.get(key);
    if (stored) {
      return !this.#reservations.has(key) && sameOwner(stored, safeOwner);
    }
    this.#owners.set(key, safeOwner);
    return true;
  }

  async reserveAll(
    claims: readonly ResourceClaim[],
    owner: ResourceOwner,
    token: string,
  ): Promise<boolean> {
    const safeOwner = canonicalResourceOwner(owner);
    const safeToken = canonicalResourceId(token);
    const safeClaims = canonicalClaims(claims);
    for (const claim of safeClaims) {
      const key = `${claim.kind}:${claim.resourceId}`;
      const stored = this.#owners.get(key);
      if (stored && !sameOwner(stored, safeOwner)) return false;
      const reservedBy = this.#reservations.get(key);
      if (reservedBy !== undefined && reservedBy !== safeToken) return false;
    }
    for (const claim of safeClaims) {
      const key = `${claim.kind}:${claim.resourceId}`;
      if (!this.#owners.has(key)) {
        this.#owners.set(key, safeOwner);
        this.#reservations.set(key, safeToken);
      }
    }
    return true;
  }

  async settleReservation(
    token: string,
    release: readonly ResourceClaim[],
  ): Promise<void> {
    const safeToken = canonicalResourceId(token);
    const safeRelease = canonicalClaims(release);
    const released = new Set(
      safeRelease.map((claim) => `${claim.kind}:${claim.resourceId}`),
    );
    for (const [key, reservedBy] of this.#reservations) {
      if (reservedBy !== safeToken) continue;
      if (released.has(key)) this.#owners.delete(key);
      this.#reservations.delete(key);
    }
  }

  async owner(
    kind: ResourceKind,
    resourceId: string,
  ): Promise<ResourceOwner | undefined> {
    const safeKind = canonicalResourceKind(kind);
    const safeResourceId = canonicalResourceId(resourceId);
    const key = `${safeKind}:${safeResourceId}`;
    if (this.#reservations.has(key)) return undefined;
    const stored = this.#owners.get(key);
    return stored;
  }

  async release(
    kind: ResourceKind,
    resourceId: string,
    owner: ResourceOwner,
  ): Promise<boolean> {
    const safeKind = canonicalResourceKind(kind);
    const safeResourceId = canonicalResourceId(resourceId);
    const safeOwner = canonicalResourceOwner(owner);
    const key = `${safeKind}:${safeResourceId}`;
    if (this.#reservations.has(key)) return false;
    const stored = this.#owners.get(key);
    if (!stored || !sameOwner(stored, safeOwner)) return false;
    return this.#owners.delete(key);
  }
}

export async function requireResourceOwner(
  resources: Pick<ResourceOwnershipStore, 'owner'>,
  kind: ResourceKind,
  resourceId: string,
): Promise<ResourceOwner> {
  const owner = await resources.owner(kind, resourceId);
  if (!owner)
    throw new Error(`${kind} '${resourceId}' has no registered owner`);
  return owner;
}

export async function requireCommonResourceOwner(
  resources: Pick<ResourceOwnershipStore, 'owner'>,
  claims: readonly ResourceClaim[],
): Promise<ResourceOwner> {
  const safeClaims = canonicalClaims(claims);
  if (safeClaims.length === 0) {
    throw new Error('at least one registered resource is required');
  }
  const first = safeClaims[0] as ResourceClaim;
  const owner = await requireResourceOwner(
    resources,
    first.kind,
    first.resourceId,
  );
  for (const claim of safeClaims.slice(1)) {
    const candidate = await requireResourceOwner(
      resources,
      claim.kind,
      claim.resourceId,
    );
    if (!sameOwner(owner, candidate)) {
      throw new Error('registered resources do not share one owner');
    }
  }
  return owner;
}

export function principalOwner(principal: ExecutionPrincipal): ResourceOwner {
  const safePrincipal = assertExecutionPrincipal(
    principal,
    'resource owner principal',
  );
  return Object.freeze({ kind: safePrincipal.kind, id: safePrincipal.id });
}

export function principalMayAccess(
  principal: ExecutionPrincipal,
  owner: ResourceOwner,
  access: ResourceAccess,
): boolean {
  const safePrincipal = assertExecutionPrincipal(
    principal,
    'resource access principal',
  );
  const safeOwner = canonicalResourceOwner(owner);
  if (access !== 'read' && access !== 'write') {
    throw new Error('resource access mode is invalid');
  }
  if (sameOwner(safePrincipal, safeOwner)) return true;
  if (safePrincipal.kind !== 'human') return false;
  if (safePrincipal.role === 'admin') return true;
  return (
    access === 'read' &&
    (safePrincipal.role === 'reviewer' || safePrincipal.role === 'viewer')
  );
}
