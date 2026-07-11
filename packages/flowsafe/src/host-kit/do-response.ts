// SPDX-License-Identifier: Apache-2.0
// Reading a Durable Object's answer back as a RunSummary.
//
// Hosts that reach their runs through a DO stub (the showcase, the deploy
// template) all need this: the DO already mapped the runtime's typed errors to
// 404/409/400, so a non-ok answer must carry that status through the run
// router's error mapping rather than collapse into a generic 500.

import type { RunSummary } from '../do-runner/index.js';
import { RunRouteError } from './run-route-error.js';

/** The subset of a DO fetch Response this reader touches. */
export interface DoResponseLike {
  ok: boolean;
  status: number;
  json(): Promise<unknown>;
}

/**
 * Parse a DO response as a RunSummary, translating a non-ok answer into a
 * RunRouteError carrying the DO's own status and message.
 */
export async function doSummary(response: DoResponseLike): Promise<RunSummary> {
  const payload = await response.json();
  if (!response.ok) {
    const message =
      payload !== null &&
      typeof payload === 'object' &&
      typeof (payload as { error?: unknown }).error === 'string'
        ? (payload as { error: string }).error
        : `run request failed with status ${response.status}`;
    throw new RunRouteError(response.status, message);
  }
  return payload as RunSummary;
}
