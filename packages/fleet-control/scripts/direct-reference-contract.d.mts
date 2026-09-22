// SPDX-License-Identifier: Apache-2.0

import type { DirectFixtureRole } from './direct-credentialed-spec.js';

export const DIRECT_REFERENCE_PATH: '/.well-known/anchorage/direct-conformance/v1/actions';
export const DIRECT_REFERENCE_BODY_LIMIT: number;
export type DirectReferenceErrorCode =
  | 'invalid-request'
  | 'payload-too-large'
  | 'invalid-utf8'
  | 'run-binding-mismatch'
  | 'request-hash-mismatch';

export class DirectReferenceRequestError extends Error {
  readonly code: DirectReferenceErrorCode;
  constructor(code?: DirectReferenceErrorCode);
}

export type DirectInventorySlot = 'inventory-before' | 'inventory-after';
export type DirectAuditSlot = 'audit-before' | 'audit-after';
export type DirectReferenceAction =
  | Readonly<{
      kind: 'reconcile-invocation';
      ordinal: number;
      requestSha256: string;
    }>
  | Readonly<{ kind: 'force-terminal'; role: 'a' }>
  | Readonly<{
      kind: 'tenant-fence';
      role: 'a' | 'b';
      operation: 'drain' | 'reopen' | 'lock' | 'unlock';
      expectedMutationEpoch: number;
      expectedRevision: number;
    }>
  | Readonly<{
      kind: 'tenant-fence';
      role: 'a' | 'b';
      operation:
        | 'read'
        | 'inventory'
        | 'mutate-current'
        | 'probe-missing'
        | 'probe-stale'
        | 'probe-future';
    }>
  | Readonly<{
      kind: 'tenant-probe';
      role: DirectFixtureRole;
      operation: 'health' | 'object-put' | 'object-read' | 'object-delete';
    }>
  | Readonly<{
      kind: 'tenant-continuation';
      operation: 'start';
      challenge: string;
    }>
  | Readonly<{
      kind: 'tenant-continuation';
      operation: 'status' | 'resume-locked';
      runId: string;
    }>
  | Readonly<{
      kind: 'tenant-continuation';
      operation: 'resume';
      runId: string;
      approvalId: string;
    }>
  | Readonly<{
      kind:
        | 'control-read'
        | 'migration-start'
        | 'migration-reprovision-a'
        | 'migration-abandon'
        | 'force-recovery'
        | 'force-observe'
        | 'recover-force-residual';
    }>
  | Readonly<{
      kind: 'provision';
      role: DirectFixtureRole;
      release: 'initial';
      cycle?: 'reprovision';
    }>
  | Readonly<{
      kind: 'provision';
      role: 'recovery';
      release: 'failed-recovery';
    }>
  | Readonly<{
      kind: 'inventory-start' | 'inventory-read';
      slot: DirectInventorySlot;
    }>
  | Readonly<{
      kind: 'inventory-continue';
      slot: DirectInventorySlot;
      token?: unknown;
    }>
  | Readonly<{ kind: 'audit-start' | 'audit-abandon'; slot: DirectAuditSlot }>
  | Readonly<{ kind: 'audit-continue'; slot: DirectAuditSlot; token?: unknown }>
  | Readonly<{
      kind: 'audit-page';
      slot: DirectAuditSlot;
      afterOrdinal?: number;
      limit: number;
    }>
  | Readonly<{ kind: 'migration-page'; afterOrdinal?: number; limit: number }>
  | Readonly<{ kind: 'migration-continue'; token?: unknown }>
  | Readonly<{
      kind:
        | 'cleanup-start'
        | 'cleanup-receipt'
        | 'decommission-start'
        | 'decommission-export';
      role: DirectFixtureRole;
      cycle?: 'reprovision';
    }>
  | Readonly<{
      kind: 'cleanup-continue' | 'decommission-continue';
      role: DirectFixtureRole;
      token?: unknown;
      cycle?: 'reprovision';
    }>
  | Readonly<{
      kind: 'cleanup-restart-blocked' | 'decommission-restart-blocked';
      role: DirectFixtureRole;
      token: unknown;
      cycle?: 'reprovision';
    }>;

export interface DirectReferenceRequest {
  readonly contractVersion: 2;
  readonly configSha256: string;
  readonly action: DirectReferenceAction;
  readonly reservation: Readonly<{
    ordinal: number;
    requestSha256: string;
  }> | null;
}

export function serializeDirectReferenceCore(value: unknown): string;
export function directReferenceRequestSha256(value: unknown): string;
export function isDirectReferenceReadOnlyAction(
  action: DirectReferenceAction,
): boolean;

export function readDirectReferenceRequest(
  request: Request,
  expectedConfigSha256: string,
): Promise<DirectReferenceRequest>;
