// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { CleanupAdvanceResult } from '../src/cleanup-advance.js';
import type { DecommissionAdvanceResult } from '../src/decommission-advance.js';
import {
  createDirectReferenceHarness,
  type DirectReferenceHarness,
} from './fixtures/direct-reference-harness.js';

describe.sequential('direct lifecycle through native control state', {
  timeout: 180_000,
}, () => {
  let fixture: DirectReferenceHarness;
  beforeAll(async () => {
    fixture = await createDirectReferenceHarness();
  }, 60_000);
  afterAll(async () => {
    await fixture?.close();
  }, 30_000);
  async function finishCleanup(
    token: unknown,
  ): Promise<Extract<CleanupAdvanceResult, { status: 'complete' }>> {
    for (let calls = 0; calls < 150; calls++) {
      const result = await fixture.success<CleanupAdvanceResult>({
        kind: 'cleanup-continue',
        role: 'recovery',
        token,
      });
      if (result.status === 'complete') return result;
      expect(result.status).toBe('pending');
      token = result.token;
    }
    throw new Error('fixture cleanup exhausted its invocation bound');
  }

  it('uses real provision and normal teardown with native export integrity', async () => {
    const result = await fixture.success<{ status: string }>({
      kind: 'provision',
      role: 'a',
      release: 'initial',
    });
    expect(result.status).toBe('ready');
    expect(fixture.sqlFailures).toEqual([]);
    const before = fixture.world.databases.length;
    expect(
      (
        await fixture.success<{ status: string }>({
          kind: 'provision',
          role: 'a',
          release: 'initial',
        })
      ).status,
    ).toBe('ready');
    expect(fixture.world.databases).toHaveLength(before);
    let advance = await fixture.success<DecommissionAdvanceResult>({
      kind: 'decommission-start',
      role: 'a',
    });
    for (let calls = 0; advance.status !== 'complete' && calls < 150; calls++) {
      expect(advance.status).toBe('pending');
      advance = await fixture.success<DecommissionAdvanceResult>({
        kind: 'decommission-continue',
        role: 'a',
        token: advance.token,
      });
    }
    expect(advance.status).toBe('complete');
    expect(fixture.world.databases).toHaveLength(0);
    expect(fixture.buckets.size).toBe(0);
    const stored = await fixture.exportBytes.list();
    expect(stored.objects.length).toBeGreaterThan(0);
    const expectedSql = [...fixture.world.exports.values()][0];
    if (!expectedSql) throw new Error('fixture export bytes are missing');
    if (advance.status !== 'complete')
      throw new Error('decommission did not complete');
    expect(advance.result.databaseExport.size).toBe(expectedSql.byteLength);
    expect(advance.result.databaseExport.sha256).toBe(
      createHash('sha256').update(expectedSql).digest('hex'),
    );
    const objects = await Promise.all(
      stored.objects.map(async (object) => {
        const value = await fixture.exportBytes.get(object.key);
        if (!value) throw new Error('stored export object is missing');
        return new Uint8Array(await value.arrayBuffer());
      }),
    );
    expect(
      objects.some((bytes) =>
        Buffer.from(bytes).equals(Buffer.from(expectedSql)),
      ),
    ).toBe(true);
    const replay = await fixture.success<DecommissionAdvanceResult>({
      kind: 'decommission-start',
      role: 'a',
    });
    expect(replay.status).toBe('complete');
    const control = await fixture.success<{
      records: { role: string; phase: string }[];
    }>({ kind: 'control-read' });
    expect(control.records.find((record) => record.role === 'a')?.phase).toBe(
      'decommissioned',
    );
  });

  it('preserves failed and fresh recovery rollback histories and opaque replay', async () => {
    const failed = await fixture.success<{
      status: string;
      slot: string;
      cleanup: CleanupAdvanceResult;
    }>({ kind: 'provision', role: 'recovery', release: 'failed-recovery' });
    expect(failed).toMatchObject({
      status: 'failed-provision',
      slot: 'cleanup-recovery',
      cleanup: { status: 'pending' },
    });
    expect(fixture.sqlFailures).toEqual([
      'no such table: direct_conformance_missing_table',
    ]);
    const historical = await finishCleanup(failed.cleanup.token);
    fixture.world.failNext('uploadCandidate', { dispatched: false });
    const fresh = await fixture.success<{
      status: string;
      slot: string;
      cleanup: CleanupAdvanceResult;
    }>({ kind: 'provision', role: 'recovery', release: 'initial' });
    expect(fresh).toMatchObject({
      status: 'failed-provision',
      slot: 'cleanup-recovery-initial',
      cleanup: { status: 'pending' },
    });
    expect(fresh.cleanup.token.operationId).not.toBe(
      historical.token.operationId,
    );
    const historicalReplay = await fixture.success<CleanupAdvanceResult>({
      kind: 'cleanup-continue',
      role: 'recovery',
      token: historical.token,
    });
    expect(historicalReplay).toMatchObject({
      status: 'complete',
      receipt: historical.receipt,
    });
    const completed = await finishCleanup(fresh.cleanup.token);
    expect(completed.receipt.operationId).toBe(fresh.cleanup.token.operationId);
    expect(
      (await fixture.journal().readOperation('cleanup-recovery'))?.operationId,
    ).toBe(historical.receipt.operationId);
    expect(
      (await fixture.journal().readOperation('cleanup-recovery-initial'))
        ?.operationId,
    ).toBe(completed.receipt.operationId);
    const selected = await fixture.success<{ slot: string; receipt: unknown }>({
      kind: 'cleanup-receipt',
      role: 'recovery',
    });
    expect(selected).toEqual({
      slot: 'cleanup-recovery-initial',
      receipt: completed.receipt,
    });
    const attempts = fixture.projection.requests.length;
    const refused = await fixture.call({
      kind: 'provision',
      role: 'recovery',
      release: 'initial',
    });
    expect(refused.value.ok).toBe(false);
    expect(fixture.projection.requests).toHaveLength(attempts);
    for (const token of [null, {}, { operationId: 'foreign', revision: 1 }]) {
      expect(
        (
          await fixture.call({
            kind: 'cleanup-continue',
            role: 'recovery',
            token,
          })
        ).value.ok,
      ).toBe(false);
    }
  });

  it('does not provision again from prepared history with no observable result', async () => {
    const spec = fixture.specs[1];
    if (!spec) throw new Error('fixture b specification is missing');
    const { deploymentSpecDigest } = await import('../src/spec-digest.js');
    await fixture.journal().freezeStart('cleanup-b', async () => ({
      operationId: null,
      inputJson: JSON.stringify({
        version: 1,
        role: 'b',
        release: 'initial',
        specDigest: deploymentSpecDigest(spec),
      }),
    }));
    const attempts = fixture.projection.requests.length;
    const response = await fixture.call({
      kind: 'provision',
      role: 'b',
      release: 'initial',
    });
    expect(response.value.ok).toBe(false);
    expect(fixture.projection.requests).toHaveLength(attempts);
    expect(
      fixture.world.databases.some(
        (database) => database.name === spec.databaseName,
      ),
    ).toBe(false);
    expect(fixture.bridgeErrors).toEqual([]);
  });
});
