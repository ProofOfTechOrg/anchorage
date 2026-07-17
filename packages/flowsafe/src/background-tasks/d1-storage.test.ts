// SPDX-License-Identifier: Apache-2.0
// The `backgroundTasksStore` fail-closed accessor (LOW 4) + the R-B3
// execution-unblock enforcement guard (HIGH). The accessor tests pin the two
// clear-message throws a host hits when its Mastra has no storage / no
// `backgroundTasks` domain. The R-B3 guard COUPLES "@mastra/cloudflare-d1 can
// execute background bodies" to "the internal-run tenant-isolation leak is
// closed": while the D1 workflows store cannot do concurrent updates, execution
// is R-B1-blocked and R-B3 is inert; the instant a future adapter/bump flips
// `supportsConcurrentUpdates()` to true, this test FAILS, so CI cannot go green
// on an execution-capable adapter until R-B3 is closed.

import type { Mastra } from '@mastra/core/mastra';
import { describe, expect, it } from 'vitest';

import { d1DatabaseLike, openSqlite } from '../../test-support/sqlite.js';
import { createD1Storage } from '../do-runner/index.js';
import { backgroundTasksStore } from './d1-storage.js';

describe('backgroundTasksStore — fail-closed accessor', () => {
  it('throws a clear message when the hosting Mastra has no storage', async () => {
    const mastra = { getStorage: () => undefined } as unknown as Mastra;
    await expect(backgroundTasksStore(mastra)).rejects.toThrow(
      /has no storage configured/,
    );
  });

  it("throws when the storage adapter does not implement the 'backgroundTasks' domain", async () => {
    // A composite store whose getStore('backgroundTasks') resolves null — a
    // storage adapter that does not ship the domain the manager reads through.
    const mastra = {
      getStorage: () => ({ getStore: async () => null }),
    } as unknown as Mastra;
    await expect(backgroundTasksStore(mastra)).rejects.toThrow(
      /does not implement the 'backgroundTasks' domain/,
    );
  });
});

describe('R-B3 execution-unblock enforcement guard', () => {
  it('FAILS the instant @mastra/cloudflare-d1 can execute background bodies, forcing the R-B3 internal-run tenant leak closed in the same change', async () => {
    // Read the @mastra/cloudflare-d1 WorkflowsStorage the hosts actually run on,
    // via the exact path core's evented createRun uses
    // (getStorage().getStore('workflows').supportsConcurrentUpdates()).
    const storage = createD1Storage({
      binding: d1DatabaseLike(openSqlite()) as never,
    });
    const workflows = await storage.getStore('workflows');
    const supportsConcurrentUpdates =
      workflows?.supportsConcurrentUpdates?.() ?? false;

    // COUPLING: while this is false, background-task EXECUTION is R-B1-blocked
    // (core's evented createRun throws ATOMIC_STORAGE_OPERATIONS_NOT_SUPPORTED),
    // so no internal `__background-task` snapshot row is ever written and R-B3 is
    // inert. The moment it returns true, execution goes live AND silent — the
    // failure message is the forcing function to close R-B3 first.
    expect(
      supportsConcurrentUpdates,
      "R-B3 ENFORCEMENT: @mastra/cloudflare-d1 WorkflowsStorage.supportsConcurrentUpdates() is now TRUE, so background-task bodies can EXECUTE on D1. Before enabling execution you MUST close R-B3: core keys the internal __background-task run by the UNSALTED taskId (createRun({ runId: taskId })), so its mastra_workflow_snapshot row escapes tenant offboarding (purgeTenant's salted [tid_, tid`) run_id range) and, while suspended, purgeExpiredWorkflowRuns — an offboarded tenant's background-task engine runs would leak. Salt the internal __background-task runId with the parent tenant, or reap the snapshot by the task's salted originating run_id, THEN update this guard. See background-tasks/host.ts R-B3.",
    ).toBe(false);
  });
});
