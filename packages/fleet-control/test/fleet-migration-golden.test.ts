// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  MIGRATION_STOP_BASELINE_ERROR,
  MIGRATION_STOP_BASELINE_OPS,
  MIGRATION_SUCCESS_BASELINE_OPS,
  MIGRATION_SUCCESS_BASELINE_RESULT,
} from './fixtures/fleet-migration-baseline.js';
import {
  runFleetMigrationStopBaseline,
  runFleetMigrationSuccessBaseline,
} from './fixtures/fleet-migration-worlds.js';

describe('fleet migration golden baselines', () => {
  it('migrates the recorded success world into the frozen golden records and op log', async () => {
    const { result, ops } = await runFleetMigrationSuccessBaseline();

    expect(result).toStrictEqual(MIGRATION_SUCCESS_BASELINE_RESULT);
    expect(ops).toStrictEqual(MIGRATION_SUCCESS_BASELINE_OPS);
  });

  it('stops the recorded stop world on the frozen golden refusal and op log', async () => {
    const { error, ops } = await runFleetMigrationStopBaseline();

    expect(error).toStrictEqual(MIGRATION_STOP_BASELINE_ERROR);
    expect(ops).toStrictEqual(MIGRATION_STOP_BASELINE_OPS);
  });
});
