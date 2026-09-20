// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import * as fleet from '../src/fleet.js';
import { deploymentSpecDigest } from '../src/spec-digest.js';
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

  it('refuses an unknown database before recording an apply token', async () => {
    const migrate = vi
      .spyOn(fleet, 'migrateFleet')
      .mockImplementationOnce(async (options) => {
        const record = options.records.find(
          ({ tenantTag }) => tenantTag === 'plainmulti',
        );
        if (!record) throw new Error('missing plainmulti fixture');
        const backend = options.backendFor(record);
        const spec = options.specFor(record);
        await expect
          .soft(
            backend.applyMigrations(
              { id: 'db-unknown', name: 'database-unknown', created: false },
              spec.migrations,
              { mutationLeaseTtlMs: 900_000, assertOwned: async () => {} },
            ),
          )
          .rejects.toThrowError(
            new Error("no spec fixture for database 'db-unknown'"),
          );
        return options.records;
      });

    try {
      const { ops } = await runFleetMigrationSuccessBaseline();
      expect(
        ops.filter((op) => op.startsWith('applyMigrations:')),
      ).toStrictEqual([]);
    } finally {
      migrate.mockRestore();
    }
  });

  it.each([
    ['extfull', 'deploymentIdentity', 'deployment identity'],
    ['extfull', 'maintenanceAdmin', 'maintenance'],
    ['plainmulti', 'deploymentIdentity', 'deployment identity'],
    ['plainmulti', 'maintenanceAdmin', 'maintenance'],
  ] as const)('refuses %s upload with a foreign %s before changing live state', async (tenantTag, credential, label) => {
    const message = `${label} credential for '${tenantTag}:production' reached a call for another deployment`;
    const migrate = vi
      .spyOn(fleet, 'migrateFleet')
      .mockImplementationOnce(async (options) => {
        const record = options.records.find(
          (candidateRecord) => candidateRecord.tenantTag === tenantTag,
        );
        const foreignRecord = options.records.find(
          (candidateRecord) => candidateRecord.tenantTag !== tenantTag,
        );
        if (!record || !foreignRecord)
          throw new Error('missing credential fixtures');
        const backend = options.backendFor(record);
        const spec = options.specFor(record);
        const secrets = options.secretsFor(record);
        const foreignSecrets = options.secretsFor(foreignRecord);
        const before = structuredClone(
          await backend.inspect(spec, secrets.maintenanceAdmin, undefined),
        );
        if (tenantTag === 'extfull') {
          expect(before).toBeUndefined();
        } else {
          expect(before).toBeDefined();
          expect(before?.desiredSpecDigest).not.toBe(
            deploymentSpecDigest(spec),
          );
        }

        await expect
          .soft(
            backend.deployWorker(
              spec,
              {
                id: record.databaseId,
                name: record.databaseName,
                created: false,
              },
              { ...secrets, [credential]: foreignSecrets[credential] },
              record.platformResources,
              { mutationLeaseTtlMs: 900_000, assertOwned: async () => {} },
              undefined,
              record.applicationBindings,
            ),
          )
          .rejects.toThrowError(new Error(message));
        expect(
          await backend.inspect(spec, secrets.maintenanceAdmin, undefined),
        ).toStrictEqual(before);
        return options.records;
      });

    try {
      await expect(runFleetMigrationSuccessBaseline()).rejects.toThrowError(
        new Error(`fence violated: ${message}`),
      );
    } finally {
      migrate.mockRestore();
    }
  });
});
