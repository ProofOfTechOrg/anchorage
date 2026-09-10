// SPDX-License-Identifier: Apache-2.0

import type { DirectFixtureRole } from './direct-credentialed-spec.js';

export const DIRECT_REFERENCE_PATH: '/.well-known/anchorage/direct-conformance/v1/actions';
export const DIRECT_REFERENCE_BODY_LIMIT: number;
export type DirectReferenceErrorCode =
  | 'invalid-request'
  | 'payload-too-large'
  | 'invalid-utf8'
  | 'run-binding-mismatch';

export class DirectReferenceRequestError extends Error {
  readonly code: DirectReferenceErrorCode;
  constructor(code?: DirectReferenceErrorCode);
}

export type DirectInventorySlot = 'inventory-before' | 'inventory-after';
export type DirectAuditSlot = 'audit-before' | 'audit-after';
export type DirectReferenceAction =
  | Readonly<{
      kind: 'tenant-probe';
      role: DirectFixtureRole;
      operation: 'health' | 'object-put' | 'object-read' | 'object-delete';
    }>
  | Readonly<{
      kind:
        | 'control-read'
        | 'migration-start'
        | 'migration-abandon'
        | 'force-recovery'
        | 'force-observe';
    }>
  | Readonly<{ kind: 'provision'; role: DirectFixtureRole; release: 'initial' }>
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
      kind: 'cleanup-start' | 'cleanup-receipt' | 'decommission-start';
      role: DirectFixtureRole;
    }>
  | Readonly<{
      kind: 'cleanup-continue' | 'decommission-continue';
      role: DirectFixtureRole;
      token?: unknown;
    }>
  | Readonly<{
      kind: 'cleanup-restart-blocked' | 'decommission-restart-blocked';
      role: DirectFixtureRole;
      token: unknown;
    }>;

export interface DirectReferenceRequest {
  readonly contractVersion: 1;
  readonly configSha256: string;
  readonly action: DirectReferenceAction;
}

export function readDirectReferenceRequest(
  request: Request,
  expectedConfigSha256: string,
): Promise<DirectReferenceRequest>;
