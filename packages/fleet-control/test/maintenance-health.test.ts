// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { readMaintenanceHealth } from '../src/maintenance-health.js';

describe('maintenance health adapter', () => {
  const specDigest = 'a'.repeat(64);

  it('maps the FlowSafe singleton response without inventing fields', async () => {
    await expect(
      readMaintenanceHealth(
        Response.json({
          nextSweepAt: 2_000,
          nextPurgeAt: 3_000,
          alarmAt: 2_000,
          lastSweepAt: 1_000,
          lastSweepAttemptAt: 1_500,
          lastSweepError: 'sweep failed',
          nextTickAt: 2_500,
          lastTickAttemptAt: 1_750,
          lastTickError: 'tick failed',
          deploymentSpecDigest: specDigest,
        }),
      ),
    ).resolves.toEqual({
      armed: true,
      nextAlarmAt: 2_000,
      lastSweepAt: 1_000,
      lastPurgeAt: null,
      lastSweepAttemptAt: 1_500,
      lastSweepError: 'sweep failed',
      lastTickAt: null,
      lastTickAttemptAt: 1_750,
      lastTickError: 'tick failed',
      deploymentSpecDigest: specDigest,
    });
  });

  it('reports an unarmed status when no alarm exists', async () => {
    await expect(
      readMaintenanceHealth(Response.json({ alarmAt: null })),
    ).resolves.toEqual({
      armed: false,
      nextAlarmAt: null,
      lastSweepAt: null,
      lastPurgeAt: null,
    });
  });

  it('rejects malformed persisted failure health', async () => {
    await expect(
      readMaintenanceHealth(
        Response.json({ alarmAt: 1_000, lastPurgeError: '' }),
      ),
    ).rejects.toThrow(/lastPurgeError/);
  });

  it('rejects a malformed deployment specification digest', async () => {
    await expect(
      readMaintenanceHealth(
        Response.json({ alarmAt: 1_000, deploymentSpecDigest: 'not-a-digest' }),
      ),
    ).rejects.toThrow(/deploymentSpecDigest/);
  });
});
