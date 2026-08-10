// SPDX-License-Identifier: Apache-2.0
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import {
  createTestHarness,
  type TestHarness,
  type WorkerHandle,
} from 'wrangler';

const REPO_ROOT = new URL('..', import.meta.url).pathname;
const DATABASE = {
  binding: 'DB',
  database_name: 'flowsafe-demo',
  database_id: '00000000-0000-0000-0000-000000000000',
};

function harnessOptions() {
  return {
    root: REPO_ROOT,
    workers: [
      { configPath: 'packages/flowsafe/spike/wrangler.jsonc' },
      {
        config: {
          name: 'flowsafe-harness-probe',
          main: `${REPO_ROOT}/packages/flowsafe/test-support/harness-probe.ts`,
          compatibility_date: '2025-06-01',
          compatibility_flags: ['nodejs_compat'],
          d1_databases: [DATABASE],
        },
      },
    ],
  } satisfies Parameters<typeof createTestHarness>[0];
}

async function result<T>(worker: WorkerHandle, path: string): Promise<T> {
  const response = await worker.fetch(path, { method: 'POST' });
  const body = await response.text();
  expect(response.status, body).toBe(200);
  return JSON.parse(body) as T;
}

describe.sequential('FlowSafe Wrangler test harness', () => {
  let server: TestHarness;
  let spike: WorkerHandle;
  let probe: WorkerHandle;

  beforeAll(async () => {
    server = createTestHarness(harnessOptions());
    await server.listen();
  });

  beforeEach(async () => {
    await server.reset();
    spike = server.getWorker('flowsafe-do-runner-demo');
    probe = server.getWorker('flowsafe-harness-probe');
    await result(probe, '/seed');
  });

  afterAll(async () => {
    await server.close();
  });

  it('boots the full spike and initializes D1ApprovalStore', async () => {
    const catalog = await spike.fetch('/workflows', {
      headers: { authorization: 'Bearer spike-operator' },
    });
    expect(catalog.status).toBe(200);
    const approvals = await spike.fetch('/api/approvals', {
      headers: { authorization: 'Bearer spike-viewer' },
    });
    expect(approvals.status).toBe(200);
    expect(await approvals.json()).toEqual([]);
  });

  it('resolves approval open-create and transition races to one winner', async () => {
    const outcome = await result<{
      created: number;
      openIds: string[];
      transitionWinners: string[];
      stored: { status: string; claimedBy: string };
    }>(probe, '/approval');
    expect(outcome.created).toBe(1);
    expect(outcome.openIds).toHaveLength(1);
    expect(outcome.transitionWinners).toHaveLength(1);
    expect(outcome.stored).toMatchObject({
      status: 'claimed',
      claimedBy: outcome.transitionWinners[0],
    });
  });

  it('enforces schedule ownership, cap, claim, and delete rollback atomically', async () => {
    const outcome = await result<{
      capWinners: number;
      winnerId: string;
      loserId?: string;
      storedSchedules: string[];
      winnerOwner: { kind: string; id: string };
      loserOwner?: unknown;
      claimWinners: number;
      claimTriggers: unknown[];
      rollbackError: string;
      rollbackSchedule: { id: string };
      rollbackTriggers: unknown[];
      rollbackOwner: { kind: string; id: string };
      deleteResult: string;
      deletedSchedule: null;
      deletedTriggers: unknown[];
      deletedOwner?: unknown;
      ownerInsertError: string;
      ownerFailureSchedule: null;
      ownerFailureOwner?: unknown;
    }>(probe, '/schedule');
    expect(outcome.capWinners).toBe(1);
    expect(outcome.storedSchedules).toContain(outcome.winnerId);
    expect(outcome.storedSchedules).not.toContain(outcome.loserId);
    expect(outcome.winnerOwner).toEqual({ kind: 'human', id: 'opal' });
    expect(outcome.loserOwner).toBeUndefined();
    expect(outcome.claimWinners).toBe(1);
    expect(outcome.claimTriggers).toHaveLength(1);
    expect(outcome.rollbackError).toMatch(/injected owner delete failure/);
    expect(outcome.rollbackSchedule.id).toBe('schedule-rollback');
    expect(outcome.rollbackTriggers).toHaveLength(1);
    expect(outcome.rollbackOwner).toEqual({ kind: 'human', id: 'opal' });
    expect(outcome.deleteResult).toBe('deleted');
    expect(outcome.deletedSchedule).toBeNull();
    expect(outcome.deletedTriggers).toEqual([]);
    expect(outcome.deletedOwner).toBeUndefined();
    expect(outcome.ownerInsertError).toMatch(/injected owner insert failure/);
    expect(outcome.ownerFailureSchedule).toBeNull();
    expect(outcome.ownerFailureOwner).toBeUndefined();
  });

  it('coalesces notification maps through concurrent writers and rolls back a failed batch', async () => {
    const outcome = await result<{
      migrated: Array<{ id: string; insertionOrdinal: number }>;
      coalescedIds: string[];
      record: {
        coalescedCount: number;
        attributes: Record<string, boolean>;
        metadata: Record<string, boolean>;
      };
      rollbackError: string;
      rollbackSummary: string;
      ordinalColumns: number;
    }>(probe, '/notification');
    expect(outcome.migrated).toEqual([
      { id: 'physical-first', insertionOrdinal: 1 },
      { id: 'physical-second', insertionOrdinal: 2 },
    ]);
    expect(outcome.coalescedIds).toEqual(['notification-base']);
    expect(outcome.record).toMatchObject({
      coalescedCount: 3,
      attributes: { base: true, left: true, right: true },
      metadata: { base: true, left: true, right: true },
    });
    expect(outcome.ordinalColumns).toBe(1);
    expect(outcome.rollbackError).toMatch(/missing_notification_table/);
    expect(outcome.rollbackSummary).not.toBe('should-rollback');
  });

  it('preserves concurrent background workflow state and results in Mastra D1', async () => {
    const outcome = await result<{
      supportsConcurrentUpdates: boolean;
      stored: {
        status: string;
        context: { execute: { status: string; output: { ok: boolean } } };
        requestContext: { trace: string };
      };
    }>(probe, '/background');
    expect(outcome.supportsConcurrentUpdates).toBe(true);
    expect(outcome.stored).toMatchObject({
      status: 'running',
      context: { execute: { status: 'success', output: { ok: true } } },
      requestContext: { trace: 'yes' },
    });
  });

  it('keeps purge/write races safe and rolls back run-owner deletion together', async () => {
    const outcome = await result<{
      purged: number;
      updateChanges: number;
      racedRow: { updatedAt: string } | null;
      rollbackError: string;
      rollbackRow: { run_id: string };
      rollbackOwner: { kind: string; id: string };
      threadRetention: {
        purged: { threads: number; messages: number };
        threads: Array<{ id: string }>;
        messages: Array<{ id: string; thread_id: string }>;
        orphans: Array<{ id: string }>;
      };
    }>(probe, '/retention');
    if (outcome.updateChanges === 1) {
      expect(outcome.purged).toBe(0);
      expect(outcome.racedRow?.updatedAt).toBe('2026-08-10T12:00:00.000Z');
    } else {
      expect(outcome.purged).toBe(1);
      expect(outcome.racedRow).toBeNull();
    }
    expect(outcome.rollbackError).toMatch(/injected snapshot delete failure/);
    expect(outcome.rollbackRow).toEqual({ run_id: 'run-rollback' });
    expect(outcome.rollbackOwner).toEqual({
      kind: 'human',
      id: 'owner-retention',
    });
    expect(outcome.threadRetention).toEqual({
      purged: { threads: 0, messages: 1 },
      threads: [{ id: 'thread-resurrected' }, { id: 'thread-torn' }],
      messages: [
        { id: 'message-history', thread_id: 'thread-resurrected' },
        { id: 'message-just-sent', thread_id: 'thread-torn' },
        { id: 'message-resurrection', thread_id: 'thread-resurrected' },
      ],
      orphans: [],
    });
  });
});
