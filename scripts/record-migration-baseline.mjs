// SPDX-License-Identifier: Apache-2.0

// Usage:
//   node scripts/record-migration-baseline.mjs --check
//   node scripts/record-migration-baseline.mjs --write

import { runBaselineRecorder } from './baseline-recorder.mjs';

await runBaselineRecorder({
  scriptUrl: import.meta.url,
  noun: 'migration',
  worldModule: 'packages/fleet-control/test/fixtures/fleet-migration-worlds.ts',
  baselineFile:
    'packages/fleet-control/test/fixtures/fleet-migration-baseline.ts',
  run: async (world) => {
    const success = await world.runFleetMigrationSuccessBaseline();
    const stop = await world.runFleetMigrationStopBaseline();
    return {
      successResult: success.result,
      successOps: success.ops,
      stopError: stop.error,
      stopOps: stop.ops,
    };
  },
  imports: `import type { FleetRecord } from '../../src/types.js';
import type { MigrationOpLogEntry } from './fleet-migration-worlds.js';`,
  exports: [
    {
      name: 'MIGRATION_SUCCESS_BASELINE_RESULT',
      key: 'successResult',
      jsDoc: `/**
 * An absent \`durableObjectTag\` key differs from a present key whose value
 * is \`undefined\`.
 */`,
      satisfies: 'readonly FleetRecord[]',
    },
    {
      name: 'MIGRATION_SUCCESS_BASELINE_OPS',
      key: 'successOps',
      jsDoc: `/**
 * \`applyMigrations:verify\` depends on the spec array's reference identity;
 * a per-version slice can contain the same migrations.
 */`,
      satisfies: 'readonly MigrationOpLogEntry[]',
    },
    {
      name: 'MIGRATION_STOP_BASELINE_ERROR',
      key: 'stopError',
      satisfies: 'string',
    },
    {
      name: 'MIGRATION_STOP_BASELINE_OPS',
      key: 'stopOps',
      satisfies: 'readonly MigrationOpLogEntry[]',
    },
  ],
  summary: (baseline) =>
    `${baseline.successResult.length} migrated records, ` +
    `${baseline.successOps.length} success ops, ` +
    `${baseline.stopOps.length} stop ops`,
});
