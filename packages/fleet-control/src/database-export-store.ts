// SPDX-License-Identifier: Apache-2.0

import { cloneBoundedPlainData } from './strict-plain-data.js';
import type {
  DatabaseExportIntegrity,
  DatabaseExportReceiptIdentity,
} from './types.js';

const DATABASE_ID =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u;
const OPERATION_ID =
  /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;
const SHA256 = /^[0-9a-f]{64}$/u;
const MAX_AUTHORITY_UTF8_BYTES = 4_096;
const MAX_ESCAPED_IDENTITY_BYTES = MAX_AUTHORITY_UTF8_BYTES * 6 + 512;
const FUNCTION_BIND = Function.prototype.bind;
const PROMISE_THEN = Promise.prototype.then;
const STRUCTURED_CLONE = structuredClone;
const TEXT_ENCODER_ENCODE = TextEncoder.prototype.encode;
const textEncoder = new TextEncoder();
const receiptErrors = new WeakSet<Error>();

type DatabaseExportReceiptErrorKind =
  | 'authority-mismatch'
  | 'capability-malformed'
  | 'collision'
  | 'identity-malformed'
  | 'integrity-malformed'
  | 'key-too-long'
  | 'readback'
  | 'source-mismatch';

const RECEIPT_ERROR_MESSAGES = {
  'authority-mismatch':
    'database export receipt authority differs from configured authority',
  'capability-malformed': 'database export receipt capability is malformed',
  collision:
    'database export receipt collision differs from the committed export',
  'identity-malformed': 'database export receipt identity is malformed',
  'integrity-malformed': 'database export receipt integrity is malformed',
  'key-too-long': 'database export receipt key exceeds 1024 UTF-8 bytes',
  readback: 'database export receipt readback failed',
  'source-mismatch':
    'database export receipt source integrity differs from the streamed export',
} as const satisfies Record<DatabaseExportReceiptErrorKind, string>;

export interface CapturedDatabaseExportReceiptCapability {
  readonly authority: string;
  readonly method: (...input: never[]) => unknown;
}

export function databaseExportReceiptError(
  kind: DatabaseExportReceiptErrorKind,
): Error {
  const error = new Error(RECEIPT_ERROR_MESSAGES[kind]);
  receiptErrors.add(error);
  return error;
}

export function isDatabaseExportReceiptError(error: unknown): error is Error {
  return (
    typeof error === 'object' &&
    error !== null &&
    receiptErrors.has(error as Error)
  );
}

function utf8Length(value: string): number {
  return Reflect.apply(TEXT_ENCODER_ENCODE, textEncoder, [value]).byteLength;
}

export function databaseExportReceiptAuthorityFromUnknown(
  value: unknown,
): string {
  if (
    typeof value !== 'string' ||
    value.length === 0 ||
    utf8Length(value) > MAX_AUTHORITY_UTF8_BYTES
  ) {
    throw databaseExportReceiptError('capability-malformed');
  }
  return value;
}

export function captureDatabaseExportReceiptCapability(
  receiver: object,
  readPair: () => readonly [unknown, unknown],
): CapturedDatabaseExportReceiptCapability | undefined {
  let authorityValue: unknown;
  let methodValue: unknown;
  try {
    const pair = readPair();
    authorityValue = pair[0];
    methodValue = pair[1];
  } catch {
    throw databaseExportReceiptError('capability-malformed');
  }
  if (authorityValue === undefined && methodValue === undefined) {
    return undefined;
  }
  const authority = databaseExportReceiptAuthorityFromUnknown(authorityValue);
  if (typeof methodValue !== 'function') {
    throw databaseExportReceiptError('capability-malformed');
  }
  try {
    return {
      authority,
      method: Reflect.apply(FUNCTION_BIND, methodValue, [receiver]) as (
        ...input: never[]
      ) => unknown,
    };
  } catch {
    throw databaseExportReceiptError('capability-malformed');
  }
}

function strictReceiptObject(
  value: unknown,
  errorKind: 'identity-malformed' | 'integrity-malformed',
  maximumScalarBytes: number,
): Record<string, unknown> {
  const error = () => databaseExportReceiptError(errorKind);
  const cloned = cloneBoundedPlainData(value, {
    maxDepth: 1,
    maxNodes: 5,
    maxScalarBytes: maximumScalarBytes,
    maxSerializedBytes: maximumScalarBytes + 256,
    error,
  });
  if (typeof cloned !== 'object' || cloned === null || Array.isArray(cloned)) {
    throw error();
  }
  try {
    Reflect.apply(STRUCTURED_CLONE, undefined, [value]);
  } catch {
    throw error();
  }
  return cloned as Record<string, unknown>;
}

export function databaseExportReceiptIdentityFromUnknown(
  value: unknown,
  expectedAuthority: string,
): DatabaseExportReceiptIdentity {
  const authority =
    databaseExportReceiptAuthorityFromUnknown(expectedAuthority);
  const cloned = strictReceiptObject(
    value,
    'identity-malformed',
    MAX_ESCAPED_IDENTITY_BYTES,
  );
  const keys = Object.keys(cloned);
  if (
    keys.length !== 4 ||
    !keys.includes('version') ||
    !keys.includes('authority') ||
    !keys.includes('databaseId') ||
    !keys.includes('operationId') ||
    cloned.version !== 1 ||
    typeof cloned.authority !== 'string' ||
    cloned.authority.length === 0 ||
    utf8Length(cloned.authority) > MAX_AUTHORITY_UTF8_BYTES ||
    typeof cloned.databaseId !== 'string' ||
    !DATABASE_ID.test(cloned.databaseId) ||
    typeof cloned.operationId !== 'string' ||
    !OPERATION_ID.test(cloned.operationId)
  ) {
    throw databaseExportReceiptError('identity-malformed');
  }
  if (cloned.authority !== authority) {
    throw databaseExportReceiptError('authority-mismatch');
  }
  return Object.freeze({
    version: 1,
    authority,
    databaseId: cloned.databaseId,
    operationId: cloned.operationId,
  });
}

export function databaseExportIntegrityFromUnknown(
  value: unknown,
): DatabaseExportIntegrity {
  const cloned = strictReceiptObject(value, 'integrity-malformed', 512);
  const keys = Object.keys(cloned);
  if (
    keys.length !== 2 ||
    !keys.includes('size') ||
    !keys.includes('sha256') ||
    typeof cloned.size !== 'number' ||
    !Number.isSafeInteger(cloned.size) ||
    cloned.size < 1 ||
    typeof cloned.sha256 !== 'string' ||
    !SHA256.test(cloned.sha256)
  ) {
    throw databaseExportReceiptError('integrity-malformed');
  }
  return Object.freeze({ size: cloned.size, sha256: cloned.sha256 });
}

export function captureDatabaseExportIntegrityPromise(
  value: unknown,
): Promise<DatabaseExportIntegrity> {
  let fulfill!: (box: Readonly<{ value: unknown }>) => void;
  let reject!: (reason: unknown) => void;
  const owned = new Promise<Readonly<{ value: unknown }>>(
    (resolve, rejectPromise) => {
      fulfill = resolve;
      reject = rejectPromise;
    },
  );
  try {
    Reflect.apply(PROMISE_THEN, value, [
      (resolved: unknown) => fulfill({ value: resolved }),
      () => reject(databaseExportReceiptError('integrity-malformed')),
    ]);
  } catch {
    throw databaseExportReceiptError('integrity-malformed');
  }
  const normalized = Reflect.apply(PROMISE_THEN, owned, [
    (box: Readonly<{ value: unknown }>) =>
      databaseExportIntegrityFromUnknown(box.value),
    () => {
      throw databaseExportReceiptError('integrity-malformed');
    },
  ]) as Promise<DatabaseExportIntegrity>;
  void Reflect.apply(PROMISE_THEN, normalized, [undefined, () => undefined]);
  return normalized;
}

export function cancelBodyWithoutAwait(body: unknown, reason: unknown): void {
  try {
    if (
      (typeof body !== 'object' || body === null) &&
      typeof body !== 'function'
    ) {
      return;
    }
    const cancel = Reflect.get(body, 'cancel');
    if (typeof cancel !== 'function') return;
    const cancellation = Reflect.apply(cancel, body, [reason]);
    try {
      void Reflect.apply(PROMISE_THEN, cancellation, [
        undefined,
        () => undefined,
      ]);
    } catch {}
  } catch {}
}

export interface DurableDatabaseExportStore {
  /**
   * Canonical immutable storage authority. Present together with
   * `writeReceipt`, absent together when operation receipts are unsupported.
   */
  readonly receiptAuthority?: string;
  write(input: {
    readonly databaseId: string;
    readonly fileName: string;
    readonly body: ReadableStream<Uint8Array>;
    readonly contentLength?: number;
  }): Promise<{
    readonly location: string;
    readonly size: number;
    readonly sha256: string;
  }>;
  /**
   * Streams one operation-scoped receipt while the eager source-integrity
   * promise settles. Reusing the exact identity converges only on exact bytes;
   * a differing identity or committed export is preserved and refused.
   */
  writeReceipt?(input: {
    readonly identity: DatabaseExportReceiptIdentity;
    readonly body: ReadableStream<Uint8Array>;
    readonly contentLength?: number;
    readonly expectedIntegrity: Promise<DatabaseExportIntegrity>;
  }): Promise<DatabaseExportIntegrity & { readonly location: string }>;
}
