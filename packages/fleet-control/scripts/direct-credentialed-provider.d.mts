// SPDX-License-Identifier: Apache-2.0

import type Cloudflare from 'cloudflare';

export type DirectProviderErrorCode =
  | 'invalid-input'
  | 'provider-unavailable'
  | 'observation-mismatch'
  | 'budget-exhausted'
  | 'forbidden';

export class DirectProviderError extends Error {
  readonly code: DirectProviderErrorCode;
  constructor(code?: DirectProviderErrorCode);
}

export const DIRECT_PROVIDER_MAX_REQUESTS: 512;

export interface DirectProviderTransport {
  assertBudget(): void;
  failure(): DirectProviderErrorCode | undefined;
  close(): void;
  fetch(
    input: RequestInfo | URL,
    init?: RequestInit,
    rawByteLimit?: number,
  ): Promise<Response>;
}

export interface DirectProviderSession {
  readonly sdk: Cloudflare;
  readonly numbered: Cloudflare;
  readonly single: Cloudflare;
  readonly status: Cloudflare;
  readonly settled: Cloudflare;
  readonly APIError: typeof import('cloudflare').APIError;
  readonly bound: number;
  readonly transport: DirectProviderTransport;
  exportReader(expectedSize: number): Cloudflare;
}

export interface DirectProviderPage<Row> {
  readonly result: readonly Row[];
  readonly result_info?: Readonly<{
    total_count?: number;
    total_pages?: number;
  }>;
}

export interface DirectProviderPages<Row> {
  iterPages(): AsyncIterable<DirectProviderPage<Row>>;
}

export type DirectDispatchClassification = Readonly<{
  kind: 'first-page-404' | 'empty' | 'enumerated';
  count: number;
  names: readonly string[];
}>;

export function validateProviderAuth(value: unknown): void;

export function identifier(value: unknown, max?: number): string;

export function providerErrorFrom(error: unknown): DirectProviderError | null;

export function inventory<Row>(
  pages: PromiseLike<DirectProviderPages<Row>>,
  identity: (row: Row) => readonly string[],
  bound: number,
): Promise<Row[]>;

export function resolveDirectZone(
  input: Readonly<{
    sdk: Cloudflare;
    numbered: Cloudflare;
    accountId: string;
    ownedHostname: string;
    bound: number;
  }>,
): Promise<Readonly<{ id: string; name: string; type: string }>>;

export function classifyDispatchNamespaces(
  single: Cloudflare,
  selectors: Readonly<{ account_id: string }>,
  bound: number,
): Promise<DirectDispatchClassification>;

export function singlePage<Row>(
  promise: PromiseLike<{ readonly result: readonly Row[] }>,
): Promise<Readonly<{ rows: readonly Row[]; exhaustive: boolean }>>;

export function bucketPages(
  input: Readonly<{
    sdk: Cloudflare;
    selectors: Readonly<{ account_id: string }>;
    jurisdiction: 'default' | 'eu' | 'fedramp';
    bound: number;
  }>,
): Promise<Readonly<Record<string, unknown>>[]>;

export function probeAbsent(
  promise: PromiseLike<unknown>,
): Promise<'absent' | 'present'>;

export function openDirectProviderSession(
  input: Readonly<{
    apiToken: string;
    fetchRequest: typeof fetch;
    timeoutMs: number;
  }>,
): Promise<DirectProviderSession>;
