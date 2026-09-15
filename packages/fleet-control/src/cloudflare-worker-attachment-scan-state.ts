// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { CLOUDFLARE_INVENTORY_BOUND } from './cloudflare-client-config.js';
import { cloneBoundedPlainData } from './strict-plain-data.js';

export const WORKER_ATTACHMENT_DISPATCH_PAGE_SIZE = 100;
export const WORKER_ATTACHMENT_DISPATCH_PAGE_BOUND = 100;
const PROGRESS_BYTE_BOUND = 65_536;
export const WORKER_ATTACHMENT_CURSOR_BYTE_BOUND = 4_096;
export const WORKER_ATTACHMENT_EVIDENCE_BOUND = 1_000_000;
const PLAIN_DATA_DEPTH_BOUND = 64;
const PLAIN_DATA_NODE_BOUND = 8_192;
const SHA256_PATTERN = /^[0-9a-f]{64}$/;
export const WORKER_ATTACHMENT_EMPTY_MULTISET_SUM256 = '0'.repeat(64);

/** @internal Package-private request-budget validation shared by the scanner and lifecycle. */
export function assertWorkerAttachmentProviderRequestBudget(
  value: number,
): void {
  if (!Number.isSafeInteger(value) || value < 9 || value > 1_000) {
    throw new Error('maxProviderRequests must be an integer from 9 to 1000');
  }
}

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

export interface WorkerAttachmentScanInput {
  readonly target: WorkerAttachmentScanTarget;
  readonly progress: WorkerAttachmentScanProgress;
  readonly maxProviderRequests: number;
  readonly signal?: AbortSignal;
  readonly stopOnFirstAttachment?: boolean;
}

export class CloudflareAttachmentScanProgressError extends Error {
  constructor() {
    super('Cloudflare attachment scan progress is malformed');
    this.name = 'CloudflareAttachmentScanProgressError';
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

function utf8Length(value: string): number {
  return new TextEncoder().encode(value).byteLength;
}

function normalized(value: unknown): unknown {
  return cloneBoundedPlainData(value, {
    maxDepth: PLAIN_DATA_DEPTH_BOUND,
    maxNodes: PLAIN_DATA_NODE_BOUND,
    maxScalarBytes: PROGRESS_BYTE_BOUND,
    maxSerializedBytes: PROGRESS_BYTE_BOUND,
    error: () => new CloudflareAttachmentScanProgressError(),
  });
}

function plainRecord(value: unknown): Record<string, unknown> | undefined {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
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
    utf8Length(value) <= WORKER_ATTACHMENT_CURSOR_BYTE_BOUND
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
    !safeInteger(record.evidenceCount, WORKER_ATTACHMENT_EVIDENCE_BOUND)
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
  if (
    !Array.isArray(value) ||
    value.length > WORKER_ATTACHMENT_DISPATCH_PAGE_BOUND ||
    !value.every(hash) ||
    new Set(value).size !== value.length
  ) {
    return undefined;
  }
  return [...value] as string[];
}

function validPageStart(
  pageNumber: number,
  pageStartCursor: unknown,
  seenCursorSha256: readonly string[],
): boolean {
  if (pageNumber === 0) return pageStartCursor === undefined;
  return (
    typeof pageStartCursor === 'string' &&
    seenCursorSha256[pageNumber - 1] === sha256(pageStartCursor)
  );
}

export function parseWorkerAttachmentScanProgress(
  value: unknown,
  target: WorkerAttachmentScanTarget,
): WorkerAttachmentScanProgress {
  const plain = normalized(value);
  const record = plainRecord(plain);
  if (!record || typeof record.stage !== 'string') {
    throw new CloudflareAttachmentScanProgressError();
  }
  const parsedExpectedTarget = parseTarget(normalized(target));
  if (!parsedExpectedTarget) throw new CloudflareAttachmentScanProgressError();
  const common = parseCommon(record, parsedExpectedTarget);
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
        !safeInteger(
          record.pageNumber,
          WORKER_ATTACHMENT_DISPATCH_PAGE_BOUND - 1,
        ) ||
        !seen ||
        !safeInteger(record.totalDispatchItems, CLOUDFLARE_INVENTORY_BOUND) ||
        !hash(record.dispatchEvidenceSum256) ||
        !safeInteger(
          record.dispatchEvidenceCount,
          CLOUDFLARE_INVENTORY_BOUND,
        ) ||
        record.dispatchEvidenceCount !== record.totalDispatchItems ||
        (record.dispatchEvidenceCount === 0 &&
          record.dispatchEvidenceSum256 !==
            WORKER_ATTACHMENT_EMPTY_MULTISET_SUM256) ||
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
        !safeInteger(
          record.pageItemCount,
          WORKER_ATTACHMENT_DISPATCH_PAGE_SIZE,
        ) ||
        record.pageItemCount === 0 ||
        !safeInteger(record.itemOffset, record.pageItemCount as number) ||
        !safeInteger(
          record.pageNumber,
          WORKER_ATTACHMENT_DISPATCH_PAGE_BOUND - 1,
        ) ||
        !seen ||
        !safeInteger(record.totalDispatchItems, CLOUDFLARE_INVENTORY_BOUND) ||
        !hash(record.dispatchEvidenceSum256) ||
        !safeInteger(
          record.dispatchEvidenceCount,
          CLOUDFLARE_INVENTORY_BOUND,
        ) ||
        (record.dispatchEvidenceCount === 0 &&
          record.dispatchEvidenceSum256 !==
            WORKER_ATTACHMENT_EMPTY_MULTISET_SUM256) ||
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
        (record.pageNumber === WORKER_ATTACHMENT_DISPATCH_PAGE_BOUND - 1 &&
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
  const parsedTarget = parseTarget(normalized(target));
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
