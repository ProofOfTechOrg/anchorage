// SPDX-License-Identifier: Apache-2.0

import type { PreparedDirectConformance } from './direct-credentialed-conformance-preflight.mjs';
import type { DirectRunJournal } from './direct-credentialed-run-state.mjs';
import type { DirectReferenceAction } from './direct-reference-contract.mjs';

export type DirectInvocationErrorCode =
  | 'invalid-input'
  | 'invocation-busy'
  | 'invocation-budget-exhausted'
  | 'outcome-unknown'
  | 'injected-response-loss'
  | 'reference-refused';

export type DirectReferenceRefusalCode =
  | 'run-binding-mismatch'
  | 'operation-mismatch'
  | 'prerequisite-unavailable'
  | 'missing-start'
  | 'operation-refused'
  | 'wrong-operation'
  | 'missing-continuation'
  | 'duplicate-ordinal'
  | 'budget-exhausted';

export const DIRECT_INVOCATION_FAILURE_DETAILS: readonly [
  'platform-page',
  'transport-failure',
  'non-contract-answer',
  'delivery-window-expired',
];
export type DirectInvocationFailureDetail =
  (typeof DIRECT_INVOCATION_FAILURE_DETAILS)[number];

export class DirectInvocationError extends Error {
  readonly code: DirectInvocationErrorCode;
  readonly attempts: DirectInvocationAttempts | undefined;
  readonly detail: DirectInvocationFailureDetail | undefined;
  readonly referenceCode: DirectReferenceRefusalCode | undefined;
  constructor(
    code?: DirectInvocationErrorCode,
    attempts?: DirectInvocationAttempts,
    referenceCode?: DirectReferenceRefusalCode,
    detail?: DirectInvocationFailureDetail,
  );
}

export interface DirectInvocationAttempts {
  readonly provider: number;
  readonly maintenance: number;
  readonly application: number;
}

export interface DirectInvocationResult {
  readonly result: unknown;
  readonly attempts: DirectInvocationAttempts;
}

export interface DirectInvocationClient {
  invoke(action: DirectReferenceAction): Promise<DirectInvocationResult>;
}

export type DirectInvocationReconciliation =
  | 'received'
  | 'executed'
  | 'failed'
  | 'cancelled'
  | 'unreachable';

export const DIRECT_RECONCILIATION_MARGIN_MS: number;
export const DIRECT_RECONCILIATION_INTERVAL_MS: number;
export const DIRECT_RECONCILIATION_MAX_INTERVAL_MS: number;
export const DIRECT_RECONCILIATION_MAX_REQUESTS: number;

export function resolveDirectReferenceEndpoint(
  prepared: PreparedDirectConformance,
  accountWorkersDevSubdomain: string,
): Readonly<{ config: PreparedDirectConformance['config']; endpoint: string }>;

export function reconcileDirectInvocation(
  input: Readonly<{
    endpoint: string | URL;
    secret: string;
    configSha256: string;
    ordinal: number;
    requestSha256: string;
    workerDeadlineMs: number;
    deadlineMs?: number;
    intervalMs?: number;
    fetch?: typeof fetch;
    sleep?: (ms: number) => Promise<void>;
  }>,
): Promise<DirectInvocationReconciliation>;

export function createDirectInvocationClient(
  input: Readonly<{
    prepared: PreparedDirectConformance;
    journal: DirectRunJournal;
    accountWorkersDevSubdomain: string;
    invokeSecret: string;
    fetch?: typeof fetch;
  }>,
): DirectInvocationClient;

export function awaitReferenceIngress(
  input: Readonly<{
    prepared: PreparedDirectConformance;
    accountWorkersDevSubdomain: string;
    fetch?: typeof fetch;
    deadlineMs?: number;
    intervalMs?: number;
    sleep?: (ms: number) => Promise<void>;
  }>,
): Promise<boolean>;
