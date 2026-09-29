// SPDX-License-Identifier: Apache-2.0

import type Cloudflare from 'cloudflare';

export interface DirectResidualRows<Row = Readonly<Record<string, unknown>>> {
  readonly rows: readonly Row[];
  readonly exhaustive: boolean;
}

export interface DirectResidualListing {
  readonly databases: DirectResidualRows;
  readonly allDatabases: DirectResidualRows;
  readonly namespaces: DirectResidualRows;
  readonly scripts: DirectResidualRows;
  readonly buckets: DirectResidualRows;
  readonly domains: DirectResidualRows;
  readonly routes: DirectResidualRows;
  readonly queues: DirectResidualRows;
  readonly dispatch: Readonly<{
    kind: 'first-page-404' | 'empty' | 'enumerated' | 'fail-closed';
    count: number;
    names: readonly string[];
    exhaustive: boolean;
    status: number | null;
  }>;
}

export function listDirectCredentialedResiduals(
  input: Readonly<{
    sdk: Cloudflare;
    numbered: Cloudflare;
    single: Cloudflare;
    APIError: typeof import('cloudflare').APIError;
    selectors: Readonly<{ account_id: string }>;
    zoneId: string;
    prefix: string;
    bound: number;
    disposable: boolean;
  }>,
): Promise<DirectResidualListing>;
