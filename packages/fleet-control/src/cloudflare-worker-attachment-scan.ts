// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import {
  CLOUDFLARE_INVENTORY_BOUND,
  CLOUDFLARE_SDK_MAX_ATTEMPTS,
} from './cloudflare-client-config.js';
import type { CloudflareSdk } from './cloudflare-ordinary-worker-operations.js';
import { isNotFound } from './cloudflare-provider-errors.js';

const DISPATCH_PAGE_SIZE = 100;
const DISPATCH_PAGE_BOUND = 100;
const PROGRESS_BYTE_BOUND = 65_536;
const CURSOR_BYTE_BOUND = 4_096;
const EVIDENCE_BOUND = 1_000_000;
const PLAIN_DATA_DEPTH_BOUND = 64;
const PLAIN_DATA_NODE_BOUND = 8_192;
const SHA256_PATTERN = /^[0-9a-f]{64}$/;
const EMPTY_MULTISET_SUM256 = '0'.repeat(64);

export type WorkerAttachmentScanTarget =
  | Readonly<{ kind: 'd1'; databaseId: string }>
  | Readonly<{ kind: 'r2'; bucketName: string }>;

export interface WorkerAttachment {
  readonly scriptName: string;
  readonly plane: 'ordinary' | 'dispatch';
  readonly dispatchNamespace?: string;
}

interface ScanCommon {
  readonly version: 1;
  readonly target: WorkerAttachmentScanTarget;
  readonly evidenceSha256: string;
  readonly evidenceCount: number;
}

export type WorkerAttachmentScanProgress =
  | (ScanCommon &
      Readonly<{
        stage: 'ordinary-script-inventory';
        ordinaryInventorySha256?: string;
        scriptIndex: number;
      }>)
  | (ScanCommon &
      Readonly<{
        stage: 'ordinary-deployment';
        ordinaryInventorySha256: string;
        scriptIndex: number;
        scriptName: string;
      }>)
  | (ScanCommon &
      Readonly<{
        stage: 'ordinary-version';
        ordinaryInventorySha256: string;
        scriptIndex: number;
        scriptName: string;
        deploymentSha256: string;
        versionIndex: number;
      }>)
  | (ScanCommon &
      Readonly<{
        stage: 'dispatch-namespace-inventory';
        ordinaryInventorySha256: string;
        namespaceInventorySha256?: string;
        namespaceIndex: number;
      }>)
  | (ScanCommon &
      Readonly<{
        stage: 'dispatch-script-page';
        ordinaryInventorySha256: string;
        namespaceInventorySha256: string;
        namespaceIndex: number;
        namespaceName: string;
        pageStartCursor?: string;
        pageNumber: number;
        seenCursorSha256: readonly string[];
        totalDispatchItems: number;
        dispatchEvidenceSum256: string;
        dispatchEvidenceCount: number;
      }>)
  | (ScanCommon &
      Readonly<{
        stage: 'dispatch-script-settings';
        ordinaryInventorySha256: string;
        namespaceInventorySha256: string;
        namespaceIndex: number;
        namespaceName: string;
        pageStartCursor?: string;
        nextCursor?: string;
        pageSha256: string;
        pageItemCount: number;
        itemOffset: number;
        pageNumber: number;
        seenCursorSha256: readonly string[];
        totalDispatchItems: number;
        dispatchEvidenceSum256: string;
        dispatchEvidenceCount: number;
      }>);

export type WorkerAttachmentScanChunk =
  | Readonly<{
      status: 'pending';
      progress: WorkerAttachmentScanProgress;
      attachments: readonly WorkerAttachment[];
      providerFetchAttemptsReserved: number;
    }>
  | Readonly<{
      status: 'attached';
      attachment: WorkerAttachment;
      providerFetchAttemptsReserved: number;
    }>
  | Readonly<{
      status: 'complete';
      evidenceSha256: string;
      evidenceCount: number;
      attachments: readonly WorkerAttachment[];
      providerFetchAttemptsReserved: number;
    }>;

export interface DispatchScriptPageInput {
  readonly namespace: string;
  readonly cursor?: string;
  readonly perPage: number;
  readonly signal?: AbortSignal;
}

export interface WorkerAttachmentScanInput {
  readonly target: WorkerAttachmentScanTarget;
  readonly progress: WorkerAttachmentScanProgress;
  readonly maxProviderRequests: number;
  readonly signal?: AbortSignal;
  readonly stopOnFirstAttachment?: boolean;
}

export interface CloudflareWorkerAttachmentScanContext {
  readonly accountId: string;
  readonly client: CloudflareSdk;
  readonly dispatchNamespace?: string;
  requestDispatchScriptPage(input: DispatchScriptPageInput): Promise<Response>;
}

interface NormalizedNamespace {
  readonly name: string;
  readonly id: string | null;
  readonly scriptCount: number | null;
}

interface NormalizedDispatchScript {
  readonly id: string;
  readonly tags: readonly string[];
}

interface DispatchScriptPage {
  readonly scripts: readonly NormalizedDispatchScript[];
  readonly nextCursor?: string;
}

interface ActiveVersion {
  readonly versionId: string;
  readonly percentage: number | undefined;
}

class ProviderFetchBudget {
  readonly #maximum: number;
  #reserved = 0;

  constructor(maximum: number) {
    if (!Number.isSafeInteger(maximum) || maximum < 9 || maximum > 1_000) {
      throw new Error('maxProviderRequests must be an integer from 9 to 1000');
    }
    this.#maximum = maximum;
  }

  reserve(): boolean {
    if (this.#reserved + CLOUDFLARE_SDK_MAX_ATTEMPTS > this.#maximum) {
      return false;
    }
    this.#reserved += CLOUDFLARE_SDK_MAX_ATTEMPTS;
    return true;
  }

  get reserved(): number {
    return this.#reserved;
  }
}

export class CloudflareAttachmentScanProgressError extends Error {
  constructor() {
    super('Cloudflare attachment scan progress is malformed');
    this.name = 'CloudflareAttachmentScanProgressError';
  }
}

export class CloudflareAttachmentScanDriftError extends Error {
  constructor() {
    super('Cloudflare attachment inventory changed during a resumable scan');
    this.name = 'CloudflareAttachmentScanDriftError';
  }
}

function sha256(value: string): string {
  return createHash('sha256').update(value).digest('hex');
}

function targetValue(target: WorkerAttachmentScanTarget): string {
  return target.kind === 'd1' ? target.databaseId : target.bucketName;
}

function initialEvidence(target: WorkerAttachmentScanTarget): string {
  return sha256(
    JSON.stringify(['attachment-scan-v1', target.kind, targetValue(target)]),
  );
}

function addEvidence(
  progress: WorkerAttachmentScanProgress,
  leaf: readonly unknown[],
  observationCount = 1,
): Pick<ScanCommon, 'evidenceSha256' | 'evidenceCount'> {
  if (
    !safeInteger(observationCount, EVIDENCE_BOUND) ||
    progress.evidenceCount + observationCount > EVIDENCE_BOUND
  ) {
    throw new Error(
      `Cloudflare attachment evidence exceeded ${EVIDENCE_BOUND} leaves`,
    );
  }
  return {
    evidenceSha256: sha256(JSON.stringify([progress.evidenceSha256, leaf])),
    evidenceCount: progress.evidenceCount + observationCount,
  };
}

function addMultisetEvidence(sum256: string, leaf: readonly unknown[]): string {
  const modulus = 1n << 256n;
  const sum = BigInt(`0x${sum256}`);
  const digest = BigInt(`0x${sha256(JSON.stringify(leaf))}`);
  return ((sum + digest) % modulus).toString(16).padStart(64, '0');
}

function utf8Length(value: string): number {
  return new TextEncoder().encode(value).byteLength;
}

function plainRecord(value: unknown): Record<string, unknown> | undefined {
  try {
    if (typeof value !== 'object' || value === null || Array.isArray(value)) {
      return undefined;
    }
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) return undefined;
    const keys = Reflect.ownKeys(value);
    const entries: Array<readonly [string, unknown]> = [];
    for (const key of keys) {
      if (typeof key !== 'string') return undefined;
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (!descriptor || !('value' in descriptor) || !descriptor.enumerable) {
        return undefined;
      }
      entries.push([key, descriptor.value]);
    }
    return Object.fromEntries(entries);
  } catch {
    return undefined;
  }
}

function plainArray(value: unknown): readonly unknown[] | undefined {
  try {
    if (!Array.isArray(value)) return undefined;
    if (Object.getPrototypeOf(value) !== Array.prototype) return undefined;
    const keys = Reflect.ownKeys(value);
    const lengthDescriptor = Object.getOwnPropertyDescriptor(value, 'length');
    const length =
      lengthDescriptor && 'value' in lengthDescriptor
        ? lengthDescriptor.value
        : undefined;
    if (
      !Number.isSafeInteger(length) ||
      Number(length) < 0 ||
      lengthDescriptor?.enumerable !== false ||
      keys.length !== Number(length) + 1 ||
      !keys.includes('length') ||
      keys.some((key) => typeof key !== 'string')
    ) {
      return undefined;
    }
    const array: unknown[] = [];
    for (let index = 0; index < Number(length); index += 1) {
      const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
      if (!descriptor || !('value' in descriptor) || !descriptor.enumerable) {
        return undefined;
      }
      array.push(descriptor.value);
    }
    return array;
  } catch {
    return undefined;
  }
}

type PlainData =
  | null
  | boolean
  | number
  | string
  | PlainDataArray
  | PlainDataMap;

interface PlainDataArray extends ReadonlyArray<PlainData> {}

interface PlainDataMap {
  readonly [key: string]: PlainData;
}

interface PlainDataBudget {
  nodes: number;
  scalarUtf8Bytes: number;
}

type PlainDataResult =
  | Readonly<{ valid: true; value: PlainData }>
  | Readonly<{ valid: false }>;

function chargePlainDataScalar(
  budget: PlainDataBudget,
  value: null | boolean | number | string,
): boolean {
  if (
    typeof value === 'string' &&
    value.length > PROGRESS_BYTE_BOUND - budget.scalarUtf8Bytes
  ) {
    return false;
  }
  const serialized = JSON.stringify(value);
  budget.scalarUtf8Bytes += utf8Length(serialized);
  return budget.scalarUtf8Bytes <= PROGRESS_BYTE_BOUND;
}

function clonePlainData(
  value: unknown,
  ancestors = new Set<object>(),
  depth = 0,
  budget: PlainDataBudget = { nodes: 0, scalarUtf8Bytes: 0 },
): PlainDataResult {
  budget.nodes += 1;
  if (depth > PLAIN_DATA_DEPTH_BOUND || budget.nodes > PLAIN_DATA_NODE_BOUND) {
    return { valid: false };
  }
  if (
    value === null ||
    typeof value === 'string' ||
    typeof value === 'boolean' ||
    (typeof value === 'number' && Number.isFinite(value))
  ) {
    return chargePlainDataScalar(budget, value)
      ? ({ valid: true, value } as PlainDataResult)
      : { valid: false };
  }
  if (typeof value !== 'object') return { valid: false };
  if (ancestors.has(value)) return { valid: false };
  ancestors.add(value);
  try {
    if (Array.isArray(value)) {
      if (Object.getPrototypeOf(value) !== Array.prototype) {
        return { valid: false };
      }
      const lengthDescriptor = Object.getOwnPropertyDescriptor(value, 'length');
      const length =
        lengthDescriptor && 'value' in lengthDescriptor
          ? lengthDescriptor.value
          : undefined;
      if (
        !Number.isSafeInteger(length) ||
        Number(length) < 0 ||
        lengthDescriptor?.enumerable !== false ||
        Number(length) > PLAIN_DATA_NODE_BOUND - budget.nodes
      ) {
        return { valid: false };
      }
      const keys = Reflect.ownKeys(value);
      if (
        keys.length !== Number(length) + 1 ||
        !keys.includes('length') ||
        keys.some((key) => typeof key !== 'string')
      ) {
        return { valid: false };
      }
      const cloned: PlainData[] = [];
      for (let index = 0; index < Number(length); index += 1) {
        const descriptor = Object.getOwnPropertyDescriptor(
          value,
          String(index),
        );
        if (!descriptor || !('value' in descriptor) || !descriptor.enumerable) {
          return { valid: false };
        }
        const result = clonePlainData(
          descriptor.value,
          ancestors,
          depth + 1,
          budget,
        );
        if (!result.valid) return result;
        cloned.push(result.value);
      }
      return { valid: true, value: cloned };
    }
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      return { valid: false };
    }
    const keys = Reflect.ownKeys(value);
    if (keys.length > PLAIN_DATA_NODE_BOUND - budget.nodes) {
      return { valid: false };
    }
    const cloned = Object.create(null) as Record<string, PlainData>;
    for (const key of keys) {
      if (typeof key !== 'string' || !chargePlainDataScalar(budget, key)) {
        return { valid: false };
      }
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (!descriptor || !('value' in descriptor) || !descriptor.enumerable) {
        return { valid: false };
      }
      const result = clonePlainData(
        descriptor.value,
        ancestors,
        depth + 1,
        budget,
      );
      if (!result.valid) return result;
      cloned[key] = result.value;
    }
    return { valid: true, value: cloned };
  } finally {
    ancestors.delete(value);
  }
}

function exactKeys(
  value: Record<string, unknown>,
  required: readonly string[],
  optional: readonly string[] = [],
): boolean {
  const keys = Object.keys(value).sort();
  const allowed = [...required, ...optional].sort();
  return (
    required.every((key) => Object.hasOwn(value, key)) &&
    keys.length ===
      allowed.length -
        optional.filter((key) => !Object.hasOwn(value, key)).length &&
    keys.every((key) => allowed.includes(key))
  );
}

function safeInteger(
  value: unknown,
  maximum = Number.MAX_SAFE_INTEGER,
): value is number {
  return (
    Number.isSafeInteger(value) &&
    Number(value) >= 0 &&
    Number(value) <= maximum
  );
}

function hash(value: unknown): value is string {
  return typeof value === 'string' && SHA256_PATTERN.test(value);
}

function boundedString(
  value: unknown,
  options: { readonly allowEmpty?: boolean } = {},
): value is string {
  return (
    typeof value === 'string' &&
    (options.allowEmpty || value.length > 0) &&
    utf8Length(value) <= CURSOR_BYTE_BOUND
  );
}

function parseTarget(value: unknown): WorkerAttachmentScanTarget | undefined {
  const record = plainRecord(value);
  if (!record || typeof record.kind !== 'string') return undefined;
  if (
    record.kind === 'd1' &&
    exactKeys(record, ['kind', 'databaseId']) &&
    boundedString(record.databaseId)
  ) {
    return { kind: 'd1', databaseId: record.databaseId };
  }
  if (
    record.kind === 'r2' &&
    exactKeys(record, ['kind', 'bucketName']) &&
    boundedString(record.bucketName)
  ) {
    return { kind: 'r2', bucketName: record.bucketName };
  }
  return undefined;
}

function sameTarget(
  left: WorkerAttachmentScanTarget,
  right: WorkerAttachmentScanTarget,
): boolean {
  return left.kind === right.kind && targetValue(left) === targetValue(right);
}

const COMMON_KEYS = [
  'version',
  'target',
  'stage',
  'evidenceSha256',
  'evidenceCount',
] as const;

function parseCommon(
  record: Record<string, unknown>,
  target: WorkerAttachmentScanTarget,
): ScanCommon | undefined {
  const parsedTarget = parseTarget(record.target);
  if (
    record.version !== 1 ||
    !parsedTarget ||
    !sameTarget(parsedTarget, target) ||
    !hash(record.evidenceSha256) ||
    !safeInteger(record.evidenceCount, EVIDENCE_BOUND)
  ) {
    return undefined;
  }
  return {
    version: 1,
    target: parsedTarget,
    evidenceSha256: record.evidenceSha256,
    evidenceCount: record.evidenceCount,
  };
}

function cursorHashes(value: unknown): readonly string[] | undefined {
  const array = plainArray(value);
  if (
    !array ||
    array.length > DISPATCH_PAGE_BOUND ||
    !array.every(hash) ||
    new Set(array).size !== array.length
  ) {
    return undefined;
  }
  return [...array] as string[];
}

function codeUnitCompare(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

function validPageStart(
  pageNumber: number,
  pageStartCursor: unknown,
  seenCursorSha256: readonly string[],
): boolean {
  if (pageNumber === 0) {
    return pageStartCursor === undefined;
  }
  return (
    typeof pageStartCursor === 'string' &&
    seenCursorSha256[pageNumber - 1] === sha256(pageStartCursor)
  );
}

export function parseWorkerAttachmentScanProgress(
  value: unknown,
  target: WorkerAttachmentScanTarget,
): WorkerAttachmentScanProgress {
  let plain: PlainDataResult;
  try {
    plain = clonePlainData(value);
    if (
      !plain.valid ||
      utf8Length(JSON.stringify(plain.value)) > PROGRESS_BYTE_BOUND
    ) {
      throw new CloudflareAttachmentScanProgressError();
    }
  } catch (error) {
    if (error instanceof CloudflareAttachmentScanProgressError) throw error;
    throw new CloudflareAttachmentScanProgressError();
  }
  const record = plainRecord(plain.value);
  if (!record || typeof record.stage !== 'string') {
    throw new CloudflareAttachmentScanProgressError();
  }
  const common = parseCommon(record, target);
  if (!common) throw new CloudflareAttachmentScanProgressError();

  const malformed = (): never => {
    throw new CloudflareAttachmentScanProgressError();
  };
  switch (record.stage) {
    case 'ordinary-script-inventory': {
      if (
        !exactKeys(
          record,
          [...COMMON_KEYS, 'scriptIndex'],
          ['ordinaryInventorySha256'],
        ) ||
        !safeInteger(record.scriptIndex, CLOUDFLARE_INVENTORY_BOUND) ||
        record.ordinaryInventorySha256 !== undefined ||
        record.scriptIndex !== 0 ||
        common.evidenceCount !== 0 ||
        common.evidenceSha256 !== initialEvidence(common.target)
      ) {
        return malformed();
      }
      return {
        ...common,
        stage: record.stage,
        scriptIndex: record.scriptIndex,
        ...(typeof record.ordinaryInventorySha256 === 'string'
          ? { ordinaryInventorySha256: record.ordinaryInventorySha256 }
          : {}),
      };
    }
    case 'ordinary-deployment': {
      if (
        !exactKeys(record, [
          ...COMMON_KEYS,
          'ordinaryInventorySha256',
          'scriptIndex',
          'scriptName',
        ]) ||
        !hash(record.ordinaryInventorySha256) ||
        !safeInteger(record.scriptIndex, CLOUDFLARE_INVENTORY_BOUND) ||
        !boundedString(record.scriptName) ||
        common.evidenceCount < record.scriptIndex + 1
      ) {
        return malformed();
      }
      return {
        ...common,
        stage: record.stage,
        ordinaryInventorySha256: record.ordinaryInventorySha256,
        scriptIndex: record.scriptIndex,
        scriptName: record.scriptName,
      };
    }
    case 'ordinary-version': {
      if (
        !exactKeys(record, [
          ...COMMON_KEYS,
          'ordinaryInventorySha256',
          'scriptIndex',
          'scriptName',
          'deploymentSha256',
          'versionIndex',
        ]) ||
        !hash(record.ordinaryInventorySha256) ||
        !safeInteger(record.scriptIndex, CLOUDFLARE_INVENTORY_BOUND) ||
        !boundedString(record.scriptName) ||
        !hash(record.deploymentSha256) ||
        !safeInteger(record.versionIndex, CLOUDFLARE_INVENTORY_BOUND) ||
        common.evidenceCount < record.scriptIndex + record.versionIndex + 2
      ) {
        return malformed();
      }
      return {
        ...common,
        stage: record.stage,
        ordinaryInventorySha256: record.ordinaryInventorySha256,
        scriptIndex: record.scriptIndex,
        scriptName: record.scriptName,
        deploymentSha256: record.deploymentSha256,
        versionIndex: record.versionIndex,
      };
    }
    case 'dispatch-namespace-inventory': {
      if (
        !exactKeys(
          record,
          [...COMMON_KEYS, 'ordinaryInventorySha256', 'namespaceIndex'],
          ['namespaceInventorySha256'],
        ) ||
        !hash(record.ordinaryInventorySha256) ||
        !safeInteger(record.namespaceIndex, CLOUDFLARE_INVENTORY_BOUND) ||
        record.namespaceInventorySha256 !== undefined ||
        record.namespaceIndex !== 0 ||
        common.evidenceCount < 1
      ) {
        return malformed();
      }
      return {
        ...common,
        stage: record.stage,
        ordinaryInventorySha256: record.ordinaryInventorySha256,
        namespaceIndex: record.namespaceIndex,
        ...(typeof record.namespaceInventorySha256 === 'string'
          ? { namespaceInventorySha256: record.namespaceInventorySha256 }
          : {}),
      };
    }
    case 'dispatch-script-page': {
      const seen = cursorHashes(record.seenCursorSha256);
      if (
        !exactKeys(
          record,
          [
            ...COMMON_KEYS,
            'ordinaryInventorySha256',
            'namespaceInventorySha256',
            'namespaceIndex',
            'namespaceName',
            'pageNumber',
            'seenCursorSha256',
            'totalDispatchItems',
            'dispatchEvidenceSum256',
            'dispatchEvidenceCount',
          ],
          ['pageStartCursor'],
        ) ||
        !hash(record.ordinaryInventorySha256) ||
        !hash(record.namespaceInventorySha256) ||
        !safeInteger(record.namespaceIndex, CLOUDFLARE_INVENTORY_BOUND) ||
        !boundedString(record.namespaceName) ||
        (record.pageStartCursor !== undefined &&
          !boundedString(record.pageStartCursor)) ||
        !safeInteger(record.pageNumber, DISPATCH_PAGE_BOUND - 1) ||
        !seen ||
        !safeInteger(record.totalDispatchItems, CLOUDFLARE_INVENTORY_BOUND) ||
        !hash(record.dispatchEvidenceSum256) ||
        !safeInteger(
          record.dispatchEvidenceCount,
          CLOUDFLARE_INVENTORY_BOUND,
        ) ||
        record.dispatchEvidenceCount !== record.totalDispatchItems ||
        (record.dispatchEvidenceCount === 0 &&
          record.dispatchEvidenceSum256 !== EMPTY_MULTISET_SUM256) ||
        seen.length !== record.pageNumber ||
        !validPageStart(record.pageNumber, record.pageStartCursor, seen) ||
        common.evidenceCount < record.namespaceIndex + 2 ||
        (record.pageNumber === 0 && record.totalDispatchItems !== 0)
      ) {
        return malformed();
      }
      return {
        ...common,
        stage: record.stage,
        ordinaryInventorySha256: record.ordinaryInventorySha256,
        namespaceInventorySha256: record.namespaceInventorySha256,
        namespaceIndex: record.namespaceIndex,
        namespaceName: record.namespaceName,
        ...(typeof record.pageStartCursor === 'string'
          ? { pageStartCursor: record.pageStartCursor }
          : {}),
        pageNumber: record.pageNumber,
        seenCursorSha256: seen,
        totalDispatchItems: record.totalDispatchItems,
        dispatchEvidenceSum256: record.dispatchEvidenceSum256,
        dispatchEvidenceCount: record.dispatchEvidenceCount,
      };
    }
    case 'dispatch-script-settings': {
      const seen = cursorHashes(record.seenCursorSha256);
      if (
        !exactKeys(
          record,
          [
            ...COMMON_KEYS,
            'ordinaryInventorySha256',
            'namespaceInventorySha256',
            'namespaceIndex',
            'namespaceName',
            'pageSha256',
            'pageItemCount',
            'itemOffset',
            'pageNumber',
            'seenCursorSha256',
            'totalDispatchItems',
            'dispatchEvidenceSum256',
            'dispatchEvidenceCount',
          ],
          ['pageStartCursor', 'nextCursor'],
        ) ||
        !hash(record.ordinaryInventorySha256) ||
        !hash(record.namespaceInventorySha256) ||
        !safeInteger(record.namespaceIndex, CLOUDFLARE_INVENTORY_BOUND) ||
        !boundedString(record.namespaceName) ||
        (record.pageStartCursor !== undefined &&
          !boundedString(record.pageStartCursor)) ||
        (record.nextCursor !== undefined &&
          !boundedString(record.nextCursor)) ||
        !hash(record.pageSha256) ||
        !safeInteger(record.pageItemCount, DISPATCH_PAGE_SIZE) ||
        record.pageItemCount === 0 ||
        !safeInteger(record.itemOffset, record.pageItemCount as number) ||
        !safeInteger(record.pageNumber, DISPATCH_PAGE_BOUND - 1) ||
        !seen ||
        !safeInteger(record.totalDispatchItems, CLOUDFLARE_INVENTORY_BOUND) ||
        !hash(record.dispatchEvidenceSum256) ||
        !safeInteger(
          record.dispatchEvidenceCount,
          CLOUDFLARE_INVENTORY_BOUND,
        ) ||
        (record.dispatchEvidenceCount === 0 &&
          record.dispatchEvidenceSum256 !== EMPTY_MULTISET_SUM256) ||
        record.totalDispatchItems < record.pageItemCount ||
        record.dispatchEvidenceCount !==
          record.totalDispatchItems -
            record.pageItemCount +
            (record.itemOffset as number) ||
        seen.length !==
          record.pageNumber + (record.nextCursor === undefined ? 0 : 1) ||
        !validPageStart(record.pageNumber, record.pageStartCursor, seen) ||
        common.evidenceCount < record.namespaceIndex + 2 ||
        (record.pageNumber === 0 &&
          record.totalDispatchItems !== record.pageItemCount) ||
        (record.pageNumber === DISPATCH_PAGE_BOUND - 1 &&
          record.nextCursor !== undefined) ||
        (typeof record.nextCursor === 'string' &&
          seen.at(-1) !== sha256(record.nextCursor))
      ) {
        return malformed();
      }
      return {
        ...common,
        stage: record.stage,
        ordinaryInventorySha256: record.ordinaryInventorySha256,
        namespaceInventorySha256: record.namespaceInventorySha256,
        namespaceIndex: record.namespaceIndex,
        namespaceName: record.namespaceName,
        ...(typeof record.pageStartCursor === 'string'
          ? { pageStartCursor: record.pageStartCursor }
          : {}),
        ...(typeof record.nextCursor === 'string'
          ? { nextCursor: record.nextCursor }
          : {}),
        pageSha256: record.pageSha256,
        pageItemCount: record.pageItemCount,
        itemOffset: record.itemOffset,
        pageNumber: record.pageNumber,
        seenCursorSha256: seen,
        totalDispatchItems: record.totalDispatchItems,
        dispatchEvidenceSum256: record.dispatchEvidenceSum256,
        dispatchEvidenceCount: record.dispatchEvidenceCount,
      };
    }
    default:
      return malformed();
  }
}

export function initialWorkerAttachmentScan(
  target: WorkerAttachmentScanTarget,
): WorkerAttachmentScanProgress {
  const parsedTarget = parseTarget(target);
  if (!parsedTarget) throw new CloudflareAttachmentScanProgressError();
  return {
    version: 1,
    target: parsedTarget,
    stage: 'ordinary-script-inventory',
    evidenceSha256: initialEvidence(parsedTarget),
    evidenceCount: 0,
    scriptIndex: 0,
  };
}

function inventoryBoundExceeded(label: string, max: number): Error {
  return new Error(
    `${label} exceeded the supported inventory bound of ${max} items`,
  );
}

function drift(): never {
  throw new CloudflareAttachmentScanDriftError();
}

function checkSignal(signal: AbortSignal | undefined): void {
  signal?.throwIfAborted();
}

function pending(
  progress: WorkerAttachmentScanProgress,
  attachments: readonly WorkerAttachment[],
  budget: ProviderFetchBudget,
): WorkerAttachmentScanChunk {
  return {
    status: 'pending',
    progress,
    attachments,
    providerFetchAttemptsReserved: budget.reserved,
  };
}

function attached(
  attachment: WorkerAttachment,
  budget: ProviderFetchBudget,
): WorkerAttachmentScanChunk {
  return {
    status: 'attached',
    attachment,
    providerFetchAttemptsReserved: budget.reserved,
  };
}

function complete(
  progress: WorkerAttachmentScanProgress,
  attachments: readonly WorkerAttachment[],
  budget: ProviderFetchBudget,
): WorkerAttachmentScanChunk {
  return {
    status: 'complete',
    evidenceSha256: progress.evidenceSha256,
    evidenceCount: progress.evidenceCount,
    attachments,
    providerFetchAttemptsReserved: budget.reserved,
  };
}

function targetMatches(
  target: WorkerAttachmentScanTarget,
  bindings: readonly Readonly<Record<string, unknown>>[],
): boolean {
  return bindings.some((binding) =>
    target.kind === 'd1'
      ? binding.type === 'd1' && binding.database_id === target.databaseId
      : binding.type === 'r2_bucket' &&
        binding.bucket_name === target.bucketName,
  );
}

function bindingsFrom(
  value: readonly unknown[],
): readonly Readonly<Record<string, unknown>>[] {
  return value.map((binding) => {
    const record = plainRecord(binding);
    if (!record) {
      throw new Error('Cloudflare Worker binding inventory was malformed');
    }
    return record;
  });
}

function versionBindings(
  value: unknown,
): readonly Readonly<Record<string, unknown>>[] {
  const record = plainRecord(value);
  const resources = plainRecord(record?.resources);
  if (!resources || !Array.isArray(resources.bindings)) {
    throw new Error(
      'Cloudflare ordinary Worker version binding inventory was malformed',
    );
  }
  return bindingsFrom(resources?.bindings);
}

function settingsBindings(
  value: unknown,
): readonly Readonly<Record<string, unknown>>[] {
  const record = plainRecord(value);
  if (!record || !Array.isArray(record.bindings)) {
    throw new Error(
      'Cloudflare dispatch Worker binding inventory was malformed',
    );
  }
  return bindingsFrom(record.bindings);
}

function normalizedPageDigest(
  page: DispatchScriptPage,
  pageStartCursor: string | undefined,
): string {
  const scripts = canonicalDispatchScripts(page.scripts);
  return sha256(
    JSON.stringify([
      pageStartCursor ?? null,
      scripts.map((script) => [script.id, script.tags]),
      page.nextCursor ?? null,
    ]),
  );
}

function canonicalDispatchScripts(
  scripts: readonly NormalizedDispatchScript[],
): readonly NormalizedDispatchScript[] {
  return scripts
    .map((script) => ({
      id: script.id,
      tags: [...script.tags].sort(codeUnitCompare),
    }))
    .sort((left, right) => codeUnitCompare(left.id, right.id));
}

function cursorDigest(cursor: string): string {
  return sha256(cursor);
}

function normalizeProviderCursor(value: unknown): string | undefined {
  if (value === undefined || value === null || value === '') return undefined;
  if (typeof value !== 'string' || utf8Length(value) > CURSOR_BYTE_BOUND) {
    throw new Error(
      'Cloudflare dispatch script listing returned a malformed cursor',
    );
  }
  return value;
}

function nextCursorFrom(payload: Record<string, unknown>): string | undefined {
  if (!Object.hasOwn(payload, 'result_info')) return undefined;
  const resultInfo = plainRecord(payload.result_info);
  if (!resultInfo) {
    throw new Error(
      'Cloudflare dispatch script listing returned a malformed cursor',
    );
  }
  let cursors: Record<string, unknown> | undefined;
  if (Object.hasOwn(resultInfo, 'cursors')) {
    cursors = plainRecord(resultInfo.cursors);
    if (!cursors) {
      throw new Error(
        'Cloudflare dispatch script listing returned a malformed cursor',
      );
    }
  }
  return normalizeProviderCursor(
    Object.hasOwn(resultInfo, 'cursor') ? resultInfo.cursor : cursors?.after,
  );
}

export async function listDispatchScriptPage(
  context: CloudflareWorkerAttachmentScanContext,
  input: DispatchScriptPageInput,
): Promise<DispatchScriptPage> {
  let response: Response | undefined;
  for (let attempt = 0; attempt < CLOUDFLARE_SDK_MAX_ATTEMPTS; attempt += 1) {
    checkSignal(input.signal);
    response = await context.requestDispatchScriptPage(input);
    if (response.status !== 429 && response.status < 500) break;
  }
  if (!response?.ok) {
    throw new Error(
      `Cloudflare dispatch script listing failed with status ${response?.status ?? 'unknown'}`,
    );
  }
  const payload: unknown = await response.json();
  const record = plainRecord(payload);
  if (!record || !Object.hasOwn(record, 'result')) {
    throw new Error('Cloudflare dispatch script listing was malformed');
  }
  if (!Array.isArray(record.result)) {
    throw new Error('Cloudflare dispatch script listing had no result array');
  }
  const scripts: NormalizedDispatchScript[] = [];
  for (const item of record.result) {
    const candidate = plainRecord(item);
    if (!candidate || !Object.hasOwn(candidate, 'id')) {
      throw new Error(
        'Cloudflare dispatch script listing contained an invalid item',
      );
    }
    const id = candidate.id;
    const tags = candidate.tags;
    if (
      !boundedString(id) ||
      (tags !== undefined &&
        (!Array.isArray(tags) || !tags.every((tag) => typeof tag === 'string')))
    ) {
      throw new Error(
        'Cloudflare dispatch script listing contained malformed script metadata',
      );
    }
    scripts.push({
      id,
      tags: (tags as string[] | undefined) ?? [],
    });
  }
  const nextCursor = nextCursorFrom(record);
  return {
    scripts,
    ...(nextCursor === undefined ? {} : { nextCursor }),
  };
}

export async function listAllDispatchScripts(
  context: CloudflareWorkerAttachmentScanContext,
  namespace: string,
  signal?: AbortSignal,
): Promise<readonly NormalizedDispatchScript[]> {
  const scripts: NormalizedDispatchScript[] = [];
  let cursor: string | undefined;
  let pageNumber = 0;
  const seenCursorSha256 = new Set<string>();
  do {
    const page = await listDispatchScriptPage(context, {
      namespace,
      cursor,
      perPage: 1_000,
      signal,
    });
    scripts.push(...page.scripts);
    if (scripts.length > CLOUDFLARE_INVENTORY_BOUND) {
      throw inventoryBoundExceeded(
        'dispatch script inventory',
        CLOUDFLARE_INVENTORY_BOUND,
      );
    }
    cursor = page.nextCursor;
    if (cursor) {
      pageNumber += 1;
      if (pageNumber >= DISPATCH_PAGE_BOUND) {
        throw new Error(
          'Cloudflare dispatch script listing exceeded 100 pages',
        );
      }
      const digest = cursorDigest(cursor);
      if (seenCursorSha256.has(digest)) {
        throw new Error('Cloudflare dispatch script listing repeated a cursor');
      }
      seenCursorSha256.add(digest);
    }
  } while (cursor);
  return scripts;
}

async function listOrdinaryScripts(
  context: CloudflareWorkerAttachmentScanContext,
  target: WorkerAttachmentScanTarget,
  signal: AbortSignal | undefined,
): Promise<readonly string[]> {
  checkSignal(signal);
  const scripts: string[] = [];
  const listed = context.client.workers.scripts.list(
    { account_id: context.accountId },
    { signal },
  );
  for await (const script of listed) {
    const id = script.id;
    if (!boundedString(id)) {
      throw new Error(
        target.kind === 'd1'
          ? 'Cloudflare ordinary Worker listing contained a script without an id'
          : 'ordinary Worker has no id',
      );
    }
    scripts.push(id);
    if (scripts.length > CLOUDFLARE_INVENTORY_BOUND) {
      throw inventoryBoundExceeded(
        'ordinary Worker script inventory',
        CLOUDFLARE_INVENTORY_BOUND,
      );
    }
  }
  return scripts.sort(codeUnitCompare);
}

async function listDispatchNamespaces(
  context: CloudflareWorkerAttachmentScanContext,
  target: WorkerAttachmentScanTarget,
  signal: AbortSignal | undefined,
): Promise<readonly NormalizedNamespace[]> {
  checkSignal(signal);
  const namespaces: NormalizedNamespace[] = [];
  let yielded = false;
  try {
    const listed = context.client.workersForPlatforms.dispatch.namespaces.list(
      { account_id: context.accountId },
      { signal },
    );
    for await (const namespace of listed) {
      yielded = true;
      if (!boundedString(namespace.namespace_name)) {
        throw new Error(
          target.kind === 'd1'
            ? 'Cloudflare dispatch namespace listing contained an unidentified namespace'
            : 'dispatch namespace has no name',
        );
      }
      if (
        namespace.namespace_id !== undefined &&
        !boundedString(namespace.namespace_id)
      ) {
        throw new Error(
          `Cloudflare dispatch namespace '${namespace.namespace_name}' had malformed identity metadata`,
        );
      }
      namespaces.push({
        name: namespace.namespace_name,
        id: namespace.namespace_id ?? null,
        scriptCount:
          typeof namespace.script_count === 'number' &&
          Number.isSafeInteger(namespace.script_count) &&
          namespace.script_count >= 0
            ? namespace.script_count
            : null,
      });
      if (namespaces.length > CLOUDFLARE_INVENTORY_BOUND) {
        throw inventoryBoundExceeded(
          'dispatch namespace inventory',
          CLOUDFLARE_INVENTORY_BOUND,
        );
      }
    }
  } catch (error) {
    if (
      context.dispatchNamespace !== undefined ||
      yielded ||
      !isNotFound(error)
    ) {
      throw error;
    }
  }
  return namespaces.sort((left, right) =>
    codeUnitCompare(left.name, right.name),
  );
}

function deploymentVersions(
  value: unknown,
  target: WorkerAttachmentScanTarget,
  scriptName: string,
): readonly ActiveVersion[] | undefined {
  const response = plainRecord(value);
  const deployments = response?.deployments;
  if (!Array.isArray(deployments)) {
    throw new Error(
      `Cloudflare deployment inventory for ordinary Worker '${scriptName}' was malformed`,
    );
  }
  if (deployments.length === 0) return undefined;
  const active = plainRecord(deployments[0]);
  const rawVersions = active?.versions;
  if (
    target.kind === 'd1' &&
    (!Array.isArray(rawVersions) || rawVersions.length === 0)
  ) {
    throw new Error(
      `Cloudflare current deployment for ordinary Worker '${scriptName}' had no versions`,
    );
  }
  if (!Array.isArray(rawVersions)) return undefined;
  const versions: ActiveVersion[] = [];
  let hasLiveVersion = false;
  for (const raw of rawVersions) {
    const version = plainRecord(raw);
    const versionId = version?.version_id;
    if (typeof versionId !== 'string' || versionId.length === 0) {
      throw new Error(
        target.kind === 'd1'
          ? `Cloudflare current deployment for ordinary Worker '${scriptName}' had malformed version metadata`
          : `ordinary Worker '${scriptName}' has a malformed version`,
      );
    }
    const percentage = version?.percentage;
    if (target.kind === 'd1') {
      if (
        typeof percentage !== 'number' ||
        !Number.isFinite(percentage) ||
        percentage < 0 ||
        percentage > 100
      ) {
        throw new Error(
          `Cloudflare current deployment for ordinary Worker '${scriptName}' had malformed version metadata`,
        );
      }
      if (percentage > 0) hasLiveVersion = true;
    }
    versions.push({
      versionId,
      percentage:
        typeof percentage === 'number' && Number.isFinite(percentage)
          ? percentage
          : undefined,
    });
    if (versions.length > CLOUDFLARE_INVENTORY_BOUND) {
      throw inventoryBoundExceeded(
        `ordinary Worker '${scriptName}' deployment version inventory`,
        CLOUDFLARE_INVENTORY_BOUND,
      );
    }
  }
  if (target.kind === 'd1' && !hasLiveVersion) {
    throw new Error(
      `Cloudflare current deployment for ordinary Worker '${scriptName}' had no live versions`,
    );
  }
  return versions;
}

function deploymentDigest(versions: readonly ActiveVersion[]): string {
  return sha256(
    JSON.stringify(
      versions.map((version) => [version.versionId, version.percentage]),
    ),
  );
}

function namespaceDigest(namespaces: readonly NormalizedNamespace[]): string {
  return sha256(
    JSON.stringify(
      namespaces.map((namespace) => [
        namespace.name,
        namespace.id,
        namespace.scriptCount,
      ]),
    ),
  );
}

function ordinaryDigest(scripts: readonly string[]): string {
  return sha256(JSON.stringify(scripts));
}

function nextCommon(
  progress: WorkerAttachmentScanProgress,
  evidence?: Pick<ScanCommon, 'evidenceSha256' | 'evidenceCount'>,
): ScanCommon {
  return {
    version: 1,
    target: progress.target,
    evidenceSha256: evidence?.evidenceSha256 ?? progress.evidenceSha256,
    evidenceCount: evidence?.evidenceCount ?? progress.evidenceCount,
  };
}

type DispatchScanProgress = Extract<
  WorkerAttachmentScanProgress,
  { stage: 'dispatch-script-page' | 'dispatch-script-settings' }
>;

function completeDispatchNamespaceEvidence(
  progress: DispatchScanProgress,
): Pick<ScanCommon, 'evidenceSha256' | 'evidenceCount'> {
  return addEvidence(
    progress,
    [
      'dispatch-namespace',
      progress.namespaceName,
      progress.dispatchEvidenceCount,
      progress.dispatchEvidenceSum256,
    ],
    progress.dispatchEvidenceCount + 1,
  );
}

export async function advanceWorkerAttachmentScan(
  context: CloudflareWorkerAttachmentScanContext,
  input: WorkerAttachmentScanInput,
): Promise<WorkerAttachmentScanChunk> {
  let progress = parseWorkerAttachmentScanProgress(
    input.progress,
    input.target,
  );
  const budget = new ProviderFetchBudget(input.maxProviderRequests);
  const attachments: WorkerAttachment[] = [];
  let ordinaryScripts: readonly string[] | undefined;
  let activeDeployment:
    | Readonly<{
        scriptName: string;
        digest: string;
        versions: readonly ActiveVersion[];
      }>
    | undefined;
  let namespaces: readonly NormalizedNamespace[] | undefined;

  const ensureOrdinaryScripts = async (): Promise<
    readonly string[] | undefined
  > => {
    if (ordinaryScripts) return ordinaryScripts;
    if (!budget.reserve()) return undefined;
    ordinaryScripts = await listOrdinaryScripts(
      context,
      input.target,
      input.signal,
    );
    return ordinaryScripts;
  };
  const ensureNamespaces = async (): Promise<
    readonly NormalizedNamespace[] | undefined
  > => {
    if (namespaces) return namespaces;
    if (!budget.reserve()) return undefined;
    namespaces = await listDispatchNamespaces(
      context,
      input.target,
      input.signal,
    );
    return namespaces;
  };

  for (;;) {
    checkSignal(input.signal);
    switch (progress.stage) {
      case 'ordinary-script-inventory': {
        const scripts = await ensureOrdinaryScripts();
        if (!scripts) return pending(progress, attachments, budget);
        const digest = ordinaryDigest(scripts);
        if (
          progress.ordinaryInventorySha256 !== undefined &&
          progress.ordinaryInventorySha256 !== digest
        ) {
          return drift();
        }
        const evidence =
          progress.ordinaryInventorySha256 === undefined
            ? addEvidence(progress, ['ordinary-inventory', scripts])
            : {
                evidenceSha256: progress.evidenceSha256,
                evidenceCount: progress.evidenceCount,
              };
        if (progress.scriptIndex > scripts.length) return drift();
        if (progress.scriptIndex === scripts.length) {
          progress = {
            ...nextCommon(progress, evidence),
            stage: 'dispatch-namespace-inventory',
            ordinaryInventorySha256: digest,
            namespaceIndex: 0,
          };
          continue;
        }
        progress = {
          ...nextCommon(progress, evidence),
          stage: 'ordinary-deployment',
          ordinaryInventorySha256: digest,
          scriptIndex: progress.scriptIndex,
          scriptName: scripts[progress.scriptIndex] as string,
        };
        continue;
      }

      case 'ordinary-deployment': {
        const scripts = await ensureOrdinaryScripts();
        if (!scripts) return pending(progress, attachments, budget);
        if (
          ordinaryDigest(scripts) !== progress.ordinaryInventorySha256 ||
          scripts[progress.scriptIndex] !== progress.scriptName
        ) {
          return drift();
        }
        if (!budget.reserve()) return pending(progress, attachments, budget);
        checkSignal(input.signal);
        const listed = await context.client.workers.scripts.deployments.list(
          progress.scriptName,
          { account_id: context.accountId },
          { signal: input.signal },
        );
        const versions = deploymentVersions(
          listed,
          input.target,
          progress.scriptName,
        );
        const normalized = versions ?? [];
        const evidence = addEvidence(progress, [
          'ordinary-deployment',
          progress.scriptName,
          normalized.map((version) => [version.versionId, version.percentage]),
        ]);
        if (normalized.length === 0) {
          progress = {
            ...nextCommon(progress, evidence),
            stage: 'ordinary-script-inventory',
            ordinaryInventorySha256: progress.ordinaryInventorySha256,
            scriptIndex: progress.scriptIndex + 1,
          };
          continue;
        }
        progress = {
          ...nextCommon(progress, evidence),
          stage: 'ordinary-version',
          ordinaryInventorySha256: progress.ordinaryInventorySha256,
          scriptIndex: progress.scriptIndex,
          scriptName: progress.scriptName,
          deploymentSha256: deploymentDigest(normalized),
          versionIndex: 0,
        };
        activeDeployment = {
          scriptName: progress.scriptName,
          digest: progress.deploymentSha256,
          versions: normalized,
        };
        continue;
      }

      case 'ordinary-version': {
        const scripts = await ensureOrdinaryScripts();
        if (!scripts) return pending(progress, attachments, budget);
        if (
          ordinaryDigest(scripts) !== progress.ordinaryInventorySha256 ||
          scripts[progress.scriptIndex] !== progress.scriptName
        ) {
          return drift();
        }
        let versions =
          activeDeployment?.scriptName === progress.scriptName &&
          activeDeployment.digest === progress.deploymentSha256
            ? activeDeployment.versions
            : undefined;
        if (!versions) {
          if (!budget.reserve()) return pending(progress, attachments, budget);
          checkSignal(input.signal);
          const listed = await context.client.workers.scripts.deployments.list(
            progress.scriptName,
            { account_id: context.accountId },
            { signal: input.signal },
          );
          versions = deploymentVersions(
            listed,
            input.target,
            progress.scriptName,
          );
        }
        if (
          !versions ||
          deploymentDigest(versions) !== progress.deploymentSha256 ||
          progress.versionIndex > versions.length
        ) {
          return drift();
        }
        if (progress.versionIndex === versions.length) {
          progress = {
            ...nextCommon(progress),
            stage: 'ordinary-script-inventory',
            ordinaryInventorySha256: progress.ordinaryInventorySha256,
            scriptIndex: progress.scriptIndex + 1,
          };
          continue;
        }
        if (!budget.reserve()) return pending(progress, attachments, budget);
        const version = versions[progress.versionIndex] as ActiveVersion;
        checkSignal(input.signal);
        const detail = await context.client.workers.scripts.versions.get(
          version.versionId,
          {
            account_id: context.accountId,
            script_name: progress.scriptName,
          },
          { signal: input.signal },
        );
        const matched = targetMatches(input.target, versionBindings(detail));
        const evidence = addEvidence(progress, [
          'ordinary-version',
          progress.scriptName,
          version.versionId,
          matched,
        ]);
        if (matched) {
          const attachment: WorkerAttachment = {
            scriptName: progress.scriptName,
            plane: 'ordinary',
          };
          if (input.stopOnFirstAttachment) {
            return attached(attachment, budget);
          }
          attachments.push(attachment);
          progress = {
            ...nextCommon(progress, evidence),
            stage: 'ordinary-script-inventory',
            ordinaryInventorySha256: progress.ordinaryInventorySha256,
            scriptIndex: progress.scriptIndex + 1,
          };
          continue;
        }
        progress = {
          ...nextCommon(progress, evidence),
          stage: 'ordinary-version',
          ordinaryInventorySha256: progress.ordinaryInventorySha256,
          scriptIndex: progress.scriptIndex,
          scriptName: progress.scriptName,
          deploymentSha256: progress.deploymentSha256,
          versionIndex: progress.versionIndex + 1,
        };
        continue;
      }

      case 'dispatch-namespace-inventory': {
        const listed = await ensureNamespaces();
        if (!listed) return pending(progress, attachments, budget);
        const digest = namespaceDigest(listed);
        if (
          progress.namespaceInventorySha256 !== undefined &&
          progress.namespaceInventorySha256 !== digest
        ) {
          return drift();
        }
        const evidence =
          progress.namespaceInventorySha256 === undefined
            ? addEvidence(progress, [
                'dispatch-namespaces',
                listed.map((namespace) => [
                  namespace.name,
                  namespace.id,
                  namespace.scriptCount,
                ]),
              ])
            : {
                evidenceSha256: progress.evidenceSha256,
                evidenceCount: progress.evidenceCount,
              };
        if (progress.namespaceIndex > listed.length) return drift();
        if (progress.namespaceIndex === listed.length) {
          progress = {
            ...nextCommon(progress, evidence),
            stage: 'dispatch-namespace-inventory',
            ordinaryInventorySha256: progress.ordinaryInventorySha256,
            namespaceInventorySha256: digest,
            namespaceIndex: progress.namespaceIndex,
          };
          return complete(progress, attachments, budget);
        }
        progress = {
          ...nextCommon(progress, evidence),
          stage: 'dispatch-script-page',
          ordinaryInventorySha256: progress.ordinaryInventorySha256,
          namespaceInventorySha256: digest,
          namespaceIndex: progress.namespaceIndex,
          namespaceName: (
            listed[progress.namespaceIndex] as NormalizedNamespace
          ).name,
          pageNumber: 0,
          seenCursorSha256: [],
          totalDispatchItems: 0,
          dispatchEvidenceSum256: EMPTY_MULTISET_SUM256,
          dispatchEvidenceCount: 0,
        };
        continue;
      }

      case 'dispatch-script-page': {
        const listed = await ensureNamespaces();
        if (!listed) return pending(progress, attachments, budget);
        if (
          namespaceDigest(listed) !== progress.namespaceInventorySha256 ||
          listed[progress.namespaceIndex]?.name !== progress.namespaceName
        ) {
          return drift();
        }
        if (!budget.reserve()) return pending(progress, attachments, budget);
        const page = await listDispatchScriptPage(context, {
          namespace: progress.namespaceName,
          cursor: progress.pageStartCursor,
          perPage: DISPATCH_PAGE_SIZE,
          signal: input.signal,
        });
        if (page.scripts.length > DISPATCH_PAGE_SIZE) {
          throw new Error(
            `Cloudflare dispatch script listing returned more than ${DISPATCH_PAGE_SIZE} items in one page`,
          );
        }
        const pageScripts = canonicalDispatchScripts(page.scripts);
        const totalDispatchItems =
          progress.totalDispatchItems + pageScripts.length;
        if (totalDispatchItems > CLOUDFLARE_INVENTORY_BOUND) {
          throw inventoryBoundExceeded(
            'dispatch script inventory',
            CLOUDFLARE_INVENTORY_BOUND,
          );
        }
        let seenCursorSha256 = progress.seenCursorSha256;
        if (page.nextCursor) {
          if (progress.pageNumber + 1 >= DISPATCH_PAGE_BOUND) {
            throw new Error(
              'Cloudflare dispatch script listing exceeded 100 pages',
            );
          }
          const nextHash = cursorDigest(page.nextCursor);
          if (seenCursorSha256.includes(nextHash)) {
            throw new Error(
              'Cloudflare dispatch script listing repeated a cursor',
            );
          }
          seenCursorSha256 = [...seenCursorSha256, nextHash];
        }
        if (pageScripts.length > 0) {
          progress = {
            ...nextCommon(progress),
            stage: 'dispatch-script-settings',
            ordinaryInventorySha256: progress.ordinaryInventorySha256,
            namespaceInventorySha256: progress.namespaceInventorySha256,
            namespaceIndex: progress.namespaceIndex,
            namespaceName: progress.namespaceName,
            ...(progress.pageStartCursor
              ? { pageStartCursor: progress.pageStartCursor }
              : {}),
            ...(page.nextCursor ? { nextCursor: page.nextCursor } : {}),
            pageSha256: normalizedPageDigest(page, progress.pageStartCursor),
            pageItemCount: pageScripts.length,
            itemOffset: 0,
            pageNumber: progress.pageNumber,
            seenCursorSha256,
            totalDispatchItems,
            dispatchEvidenceSum256: progress.dispatchEvidenceSum256,
            dispatchEvidenceCount: progress.dispatchEvidenceCount,
          };
          continue;
        }
        if (page.nextCursor) {
          progress = {
            ...nextCommon(progress),
            stage: 'dispatch-script-page',
            ordinaryInventorySha256: progress.ordinaryInventorySha256,
            namespaceInventorySha256: progress.namespaceInventorySha256,
            namespaceIndex: progress.namespaceIndex,
            namespaceName: progress.namespaceName,
            pageStartCursor: page.nextCursor,
            pageNumber: progress.pageNumber + 1,
            seenCursorSha256,
            totalDispatchItems,
            dispatchEvidenceSum256: progress.dispatchEvidenceSum256,
            dispatchEvidenceCount: progress.dispatchEvidenceCount,
          };
          continue;
        }
        const evidence = completeDispatchNamespaceEvidence(progress);
        progress = {
          ...nextCommon(progress, evidence),
          stage: 'dispatch-namespace-inventory',
          ordinaryInventorySha256: progress.ordinaryInventorySha256,
          namespaceInventorySha256: progress.namespaceInventorySha256,
          namespaceIndex: progress.namespaceIndex + 1,
        };
        continue;
      }

      case 'dispatch-script-settings': {
        const listed = await ensureNamespaces();
        if (!listed) return pending(progress, attachments, budget);
        if (
          namespaceDigest(listed) !== progress.namespaceInventorySha256 ||
          listed[progress.namespaceIndex]?.name !== progress.namespaceName
        ) {
          return drift();
        }
        if (!budget.reserve()) return pending(progress, attachments, budget);
        const page = await listDispatchScriptPage(context, {
          namespace: progress.namespaceName,
          cursor: progress.pageStartCursor,
          perPage: DISPATCH_PAGE_SIZE,
          signal: input.signal,
        });
        if (page.scripts.length > DISPATCH_PAGE_SIZE) {
          throw new Error(
            `Cloudflare dispatch script listing returned more than ${DISPATCH_PAGE_SIZE} items in one page`,
          );
        }
        const pageScripts = canonicalDispatchScripts(page.scripts);
        if (
          normalizedPageDigest(page, progress.pageStartCursor) !==
            progress.pageSha256 ||
          pageScripts.length !== progress.pageItemCount ||
          page.nextCursor !== progress.nextCursor ||
          progress.itemOffset > pageScripts.length
        ) {
          return drift();
        }
        if (progress.itemOffset === pageScripts.length) {
          if (page.nextCursor) {
            progress = {
              ...nextCommon(progress),
              stage: 'dispatch-script-page',
              ordinaryInventorySha256: progress.ordinaryInventorySha256,
              namespaceInventorySha256: progress.namespaceInventorySha256,
              namespaceIndex: progress.namespaceIndex,
              namespaceName: progress.namespaceName,
              pageStartCursor: page.nextCursor,
              pageNumber: progress.pageNumber + 1,
              seenCursorSha256: progress.seenCursorSha256,
              totalDispatchItems: progress.totalDispatchItems,
              dispatchEvidenceSum256: progress.dispatchEvidenceSum256,
              dispatchEvidenceCount: progress.dispatchEvidenceCount,
            };
            continue;
          }
          const evidence = completeDispatchNamespaceEvidence(progress);
          progress = {
            ...nextCommon(progress, evidence),
            stage: 'dispatch-namespace-inventory',
            ordinaryInventorySha256: progress.ordinaryInventorySha256,
            namespaceInventorySha256: progress.namespaceInventorySha256,
            namespaceIndex: progress.namespaceIndex + 1,
          };
          continue;
        }
        if (!budget.reserve()) return pending(progress, attachments, budget);
        const script = pageScripts[
          progress.itemOffset
        ] as NormalizedDispatchScript;
        checkSignal(input.signal);
        const settings =
          await context.client.workersForPlatforms.dispatch.namespaces.scripts.settings.get(
            script.id,
            {
              account_id: context.accountId,
              dispatch_namespace: progress.namespaceName,
            },
            { signal: input.signal },
          );
        const matched = targetMatches(input.target, settingsBindings(settings));
        const dispatchEvidenceSum256 = addMultisetEvidence(
          progress.dispatchEvidenceSum256,
          [
            'dispatch-settings',
            progress.namespaceName,
            script.id,
            script.tags,
            matched,
          ],
        );
        const dispatchEvidenceCount = progress.dispatchEvidenceCount + 1;
        if (matched) {
          const attachment: WorkerAttachment = {
            scriptName: script.id,
            plane: 'dispatch',
            dispatchNamespace: progress.namespaceName,
          };
          if (input.stopOnFirstAttachment) {
            return attached(attachment, budget);
          }
          attachments.push(attachment);
        }
        progress = {
          ...nextCommon(progress),
          stage: 'dispatch-script-settings',
          ordinaryInventorySha256: progress.ordinaryInventorySha256,
          namespaceInventorySha256: progress.namespaceInventorySha256,
          namespaceIndex: progress.namespaceIndex,
          namespaceName: progress.namespaceName,
          ...(progress.pageStartCursor
            ? { pageStartCursor: progress.pageStartCursor }
            : {}),
          ...(progress.nextCursor ? { nextCursor: progress.nextCursor } : {}),
          pageSha256: progress.pageSha256,
          pageItemCount: progress.pageItemCount,
          itemOffset: progress.itemOffset + 1,
          pageNumber: progress.pageNumber,
          seenCursorSha256: progress.seenCursorSha256,
          totalDispatchItems: progress.totalDispatchItems,
          dispatchEvidenceSum256,
          dispatchEvidenceCount,
        };
        continue;
      }
    }
  }
}

export async function listAllWorkerAttachments(
  context: CloudflareWorkerAttachmentScanContext,
  target: WorkerAttachmentScanTarget,
  signal?: AbortSignal,
): Promise<readonly WorkerAttachment[]> {
  let progress = initialWorkerAttachmentScan(target);
  const attachments: WorkerAttachment[] = [];
  for (;;) {
    const chunk = await advanceWorkerAttachmentScan(context, {
      target,
      progress,
      maxProviderRequests: 1_000,
      signal,
    });
    if (chunk.status === 'attached') {
      throw new Error('complete attachment scan returned an early match');
    }
    attachments.push(...chunk.attachments);
    if (chunk.status === 'complete') {
      const sorted = attachments.sort((left, right) =>
        `${left.plane}:${left.dispatchNamespace ?? ''}:${left.scriptName}`.localeCompare(
          `${right.plane}:${right.dispatchNamespace ?? ''}:${right.scriptName}`,
        ),
      );
      if (target.kind === 'r2') return sorted;
      const seen = new Set<string>();
      return sorted.filter((attachment) => {
        const key = `${attachment.plane}:${attachment.dispatchNamespace ?? ''}:${attachment.scriptName}`;
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });
    }
    progress = chunk.progress;
  }
}
