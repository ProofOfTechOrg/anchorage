// SPDX-License-Identifier: Apache-2.0

import { databaseExportReceiptIdentityFromUnknown } from './database-export-store.js';
import { cloneBoundedPlainData } from './strict-plain-data.js';
import type {
  DatabaseExport,
  DatabaseExportReceiptIdentity,
  DatabaseReference,
  ExternalMutationFence,
  FleetRecord,
} from './types.js';

const DATABASE_EXPORT_RESULT_ERROR =
  'bounded decommission database export result is malformed';
const DATABASE_REFERENCE_ERROR = 'persisted database reference is malformed';
const DATABASE_OWNER_ERROR = 'persisted database owner is malformed';
const STRING_BYTE_BOUND = 4_096;
const RESULT_PLAIN_DATA_DEPTH_BOUND = 64;
const RESULT_PLAIN_DATA_NODE_BOUND = 8_192;
const RESULT_PLAIN_DATA_BYTE_BOUND = 96 * 1_024;
const DATABASE_REFERENCE_NODE_BOUND = 8;
const DATABASE_REFERENCE_BYTE_BOUND = 16_384;
const SHA256 = /^[0-9a-f]{64}$/u;
const STRUCTURED_CLONE = structuredClone;

type Settlement<Value> =
  | Readonly<{ status: 'fulfilled'; value: Value }>
  | Readonly<{ status: 'rejected'; reason: unknown }>;

async function settleOperation<Value>(
  operation: () => Promise<Value>,
): Promise<Settlement<Value>> {
  try {
    return { status: 'fulfilled', value: await operation() };
  } catch (reason) {
    return { status: 'rejected', reason };
  }
}

function boundedString(value: unknown): value is string {
  return (
    typeof value === 'string' &&
    value.length > 0 &&
    value.length <= STRING_BYTE_BOUND &&
    new TextEncoder().encode(value).byteLength <= STRING_BYTE_BOUND
  );
}

export interface ReconcilePersistedDatabaseFromCallbacksOptions {
  readonly getDatabase: (databaseId: string) => Promise<unknown>;
  readonly readOwner?: (
    database: DatabaseReference,
    fence: ExternalMutationFence,
  ) => Promise<unknown>;
  readonly record: Pick<
    FleetRecord,
    'databaseId' | 'databaseName' | 'tenantTag'
  >;
  readonly allowAbsent: boolean;
  readonly requireOwner: boolean;
  readonly fence: ExternalMutationFence;
}

export async function reconcilePersistedDatabaseFromCallbacks(
  options: ReconcilePersistedDatabaseFromCallbacksOptions,
): Promise<(DatabaseReference & { readonly created: false }) | undefined> {
  const rawDatabase = await options.getDatabase(options.record.databaseId);
  if (rawDatabase === undefined) {
    if (options.allowAbsent) return undefined;
    throw new Error(
      `persisted database '${options.record.databaseId}' is absent`,
    );
  }
  let plainDatabase: unknown;
  try {
    plainDatabase = cloneBoundedPlainData(rawDatabase, {
      maxDepth: 1,
      maxNodes: DATABASE_REFERENCE_NODE_BOUND,
      maxScalarBytes: DATABASE_REFERENCE_BYTE_BOUND,
      maxSerializedBytes: DATABASE_REFERENCE_BYTE_BOUND,
      error: () => new Error(DATABASE_REFERENCE_ERROR),
    });
    Reflect.apply(STRUCTURED_CLONE, undefined, [rawDatabase]);
  } catch {
    throw new Error(DATABASE_REFERENCE_ERROR);
  }
  if (
    !plainDatabase ||
    typeof plainDatabase !== 'object' ||
    Array.isArray(plainDatabase)
  ) {
    throw new Error(DATABASE_REFERENCE_ERROR);
  }
  const candidate = plainDatabase as Record<string, unknown>;
  if (
    !boundedString(candidate.id) ||
    !boundedString(candidate.name) ||
    candidate.created !== false
  ) {
    throw new Error(DATABASE_REFERENCE_ERROR);
  }
  const database = {
    id: candidate.id,
    name: candidate.name,
    created: false as const,
  };
  if (
    database.id !== options.record.databaseId ||
    database.name !== options.record.databaseName
  ) {
    throw new Error(
      `persisted database '${options.record.databaseId}' resolved with unexpected identity '${database.id}:${database.name}'`,
    );
  }
  if (options.requireOwner) {
    if (!options.readOwner) throw new Error(DATABASE_OWNER_ERROR);
    const owner = await options.readOwner(database, options.fence);
    if (owner !== undefined && !boundedString(owner)) {
      throw new Error(DATABASE_OWNER_ERROR);
    }
    if (owner !== options.record.tenantTag) {
      throw new Error(
        `refusing database operation for '${database.id}' owned by '${owner ?? 'no deployment'}'`,
      );
    }
  }
  return database;
}

export function databaseExportFromUnknown(
  value: unknown,
  databaseId: string,
): DatabaseExport {
  let plain: unknown;
  try {
    plain = cloneBoundedPlainData(value, {
      maxDepth: RESULT_PLAIN_DATA_DEPTH_BOUND,
      maxNodes: RESULT_PLAIN_DATA_NODE_BOUND,
      maxScalarBytes: RESULT_PLAIN_DATA_BYTE_BOUND,
      maxSerializedBytes: RESULT_PLAIN_DATA_BYTE_BOUND,
      error: () => new Error(DATABASE_EXPORT_RESULT_ERROR),
    });
    Reflect.apply(STRUCTURED_CLONE, undefined, [value]);
  } catch {
    throw new Error(DATABASE_EXPORT_RESULT_ERROR);
  }
  if (!plain || typeof plain !== 'object' || Array.isArray(plain)) {
    throw new Error(DATABASE_EXPORT_RESULT_ERROR);
  }
  const candidate = plain as Record<string, unknown>;
  if (
    candidate.databaseId !== databaseId ||
    !boundedString(candidate.location) ||
    !Number.isSafeInteger(candidate.size) ||
    Number(candidate.size) < 1 ||
    typeof candidate.sha256 !== 'string' ||
    !SHA256.test(candidate.sha256)
  ) {
    throw new Error(DATABASE_EXPORT_RESULT_ERROR);
  }
  return {
    databaseId,
    location: candidate.location,
    size: Number(candidate.size),
    sha256: candidate.sha256,
  };
}

export function databaseExportReceiptIdentity(
  record: Pick<FleetRecord, 'databaseId'>,
  operationId: string,
  authority: string,
  expectedAuthority: string,
): DatabaseExportReceiptIdentity {
  return databaseExportReceiptIdentityFromUnknown(
    {
      version: 1,
      authority,
      databaseId: record.databaseId,
      operationId,
    },
    expectedAuthority,
  );
}

export async function settleDatabaseDeletionUnderBarrier<Barrier>(options: {
  readonly lease: Pick<ExternalMutationFence, 'assertOwned'>;
  readonly databaseId: string;
  readonly barrier: Barrier;
  readonly deleteDatabase: () => Promise<void>;
  readonly readDatabase: () => Promise<unknown>;
}): Promise<Barrier> {
  await options.lease.assertOwned();
  const deletion = await settleOperation(options.deleteDatabase);
  await options.lease.assertOwned();
  const readback = await settleOperation(options.readDatabase);
  await options.lease.assertOwned();
  if (readback.status === 'rejected') throw readback.reason;
  if (readback.value === undefined) return options.barrier;
  if (deletion.status === 'rejected') throw deletion.reason;
  throw new Error(`database '${options.databaseId}' remains after deletion`);
}
