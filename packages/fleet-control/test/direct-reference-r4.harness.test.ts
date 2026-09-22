// SPDX-License-Identifier: Apache-2.0

import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
} from 'vitest';
import { directDeploymentSpec } from '../scripts/direct-credentialed-spec.js';
import type {
  DirectAuditSlot,
  DirectInventorySlot,
} from '../scripts/direct-reference-contract.mjs';
import type { FleetAuditAdvanceResult } from '../src/fleet-audit-advance.js';
import type { FleetInventoryAdvanceResult } from '../src/fleet-inventory-advance.js';
import type { FleetMigrationAdvanceResult } from '../src/fleet-migration-advance.js';
import type { FleetMigrationItem } from '../src/fleet-migration-state.js';
import { fleetSettlementKey } from '../src/settlement.js';
import { deploymentSpecDigest } from '../src/spec-digest.js';
import {
  createDirectReferenceHarness,
  type DirectReferenceHarness,
} from './fixtures/direct-reference-harness.js';

async function inventory(
  fixture: DirectReferenceHarness,
  slot: DirectInventorySlot,
) {
  let result = await fixture.success<FleetInventoryAdvanceResult>({
    kind: 'inventory-start',
    slot,
  });
  for (let count = 0; result.status === 'pending' && count < 100; count++)
    result = await fixture.success<FleetInventoryAdvanceResult>({
      kind: 'inventory-continue',
      slot,
      token: result.token,
    });
  if (result.status !== 'complete')
    throw new Error('inventory exceeded the fixture invocation bound');
  return result;
}

async function retrySettlementAfterWriteFailure(
  fixture: DirectReferenceHarness,
  token: FleetMigrationAdvanceResult['token'],
) {
  const stored = await fixture.journal().readOperation('migration-next');
  await fixture.db.exec(
    'CREATE TABLE fixture_settlement_attempts (id INTEGER PRIMARY KEY)',
  );
  await fixture.db.exec(
    "CREATE TRIGGER observe_settlement BEFORE INSERT ON direct_reference_observations WHEN NEW.observation_kind='settlement' BEGIN INSERT INTO fixture_settlement_attempts(id) VALUES(NULL); END",
  );
  await fixture.db.exec(
    "CREATE TRIGGER refuse_settled_record BEFORE UPDATE ON anchorage_fleet_deployments WHEN NEW.settled_settlement_key IS NOT NULL AND NEW.schema_version=2 BEGIN SELECT RAISE(ABORT,'fixture settling write failure'); END",
  );
  await fixture.db.exec(
    "CREATE TRIGGER refuse_failure_record BEFORE UPDATE ON anchorage_fleet_operations WHEN NEW.operation_kind='migration' AND json_extract(NEW.op_record,'$.state')='failed' BEGIN SELECT RAISE(ABORT,'fixture operation failure write'); END",
  );
  try {
    const failed = await fixture.call({ kind: 'migration-continue', token });
    expect(failed.response.status).toBe(500);
    expect(await fixture.journal().readOperation('migration-next')).toEqual(
      stored,
    );
  } finally {
    await fixture.db.exec('DROP TRIGGER refuse_settled_record');
    await fixture.db.exec('DROP TRIGGER refuse_failure_record');
  }
  try {
    const row = await fixture.db
      .prepare(
        "SELECT observation_key FROM direct_reference_observations WHERE observation_kind='settlement'",
      )
      .first<{ observation_key: string }>();
    if (!row)
      throw new Error('settlement effect did not precede the failed write');
    const first = await fixture.journal().readSettlement(row.observation_key);
    expect(first).toBeDefined();
    const result = await fixture.success<FleetMigrationAdvanceResult>({
      kind: 'migration-continue',
      token,
    });
    expect(result.token.revision).toBeGreaterThan(token.revision);
    expect(await fixture.journal().readSettlement(row.observation_key)).toEqual(
      first,
    );
    expect(
      (
        await fixture.db
          .prepare('SELECT id FROM fixture_settlement_attempts')
          .all()
      ).results,
    ).toHaveLength(2);
    return result;
  } finally {
    await fixture.db.exec('DROP TRIGGER observe_settlement');
    await fixture.db.exec('DROP TABLE fixture_settlement_attempts');
  }
}

describe.sequential('reference audit and migration in native control state', {
  timeout: 180_000,
}, () => {
  let fixture: DirectReferenceHarness;
  beforeAll(async () => {
    fixture = await createDirectReferenceHarness({ maintenanceNow: Date.now });
  }, 60_000);
  afterAll(async () => {
    await fixture?.close();
  }, 30_000);

  async function audit(slot: DirectAuditSlot) {
    let result = await fixture.success<FleetAuditAdvanceResult>({
      kind: 'audit-start',
      slot,
    });
    for (let count = 0; result.status === 'pending' && count < 150; count++)
      result = await fixture.success<FleetAuditAdvanceResult>({
        kind: 'audit-continue',
        slot,
        token: result.token,
      });
    expect(result.status).toBe('complete');
    const page = await fixture.success<{ findings: unknown[]; done: boolean }>({
      kind: 'audit-page',
      slot,
      limit: 1,
    });
    expect(page).toEqual({ findings: [], done: true });
    return result;
  }

  async function items() {
    return fixture.success<{ items: FleetMigrationItem[]; done: boolean }>({
      kind: 'migration-page',
      limit: 2,
    });
  }

  it('retains audits and replays the original claim after one actual migration admission', async () => {
    const premature = await fixture.call({
      kind: 'audit-start',
      slot: 'audit-before',
    });
    expect(premature.response.status).toBe(409);
    expect(
      await fixture.journal().readOperation('audit-before'),
    ).toBeUndefined();
    for (const role of ['a', 'b'] as const)
      expect(
        await fixture.success({ kind: 'provision', role, release: 'initial' }),
      ).toMatchObject({ status: 'ready' });
    const before = await inventory(fixture, 'inventory-before');
    const beforeAudit = await audit('audit-before');
    const frozen = await fixture.journal().readOperation('audit-before');
    expect(
      JSON.parse(frozen?.inputJson ?? '{}').records.map(
        (record: { tenantTag: string }) => record.tenantTag,
      ),
    ).toEqual([
      fixture.manifest.names.roles.a.tenantTag,
      fixture.manifest.names.roles.b.tenantTag,
    ]);
    expect(JSON.parse(frozen?.inputJson ?? '{}').generation).toBe(
      before.generation.generation,
    );
    const started = await fixture.success<FleetMigrationAdvanceResult>({
      kind: 'migration-start',
    });
    expect(started).toMatchObject({
      status: 'pending',
      token: { revision: 1 },
      itemOrdinal: 0,
    });
    expect(
      (await items()).items.map((item) => [
        item.ordinal,
        item.status,
        item.canaryRank,
      ]),
    ).toEqual([
      [0, 'pending', 0],
      [1, 'pending', undefined],
    ]);
    const uploadCounts = [...fixture.world.scripts.values()].map(
      (script) => script.versions.length,
    );
    const loss = await fixture.call({
      kind: 'migration-continue',
      token: started.token,
    });
    expect(loss.response.status).toBe(503);
    expect(loss.value).toMatchObject({
      ok: false,
      error: { code: 'injected-response-loss' },
    });
    const originalInstance = loss.response.headers.get('X-Fixture-Instance');
    expect(
      [...fixture.world.scripts.values()].map(
        (script) => script.versions.length,
      ),
    ).toEqual(uploadCounts);
    const admitted = (await items()).items;
    expect(admitted[0]).toMatchObject({
      status: 'active',
      planCursor: 0,
      ordinal: 0,
    });
    expect(admitted[1]).toMatchObject({ status: 'pending' });
    for (const database of fixture.world.databases)
      expect(
        database.d1.queryDatabase(
          'SELECT marker FROM direct_conformance_fixture WHERE id=1',
        ),
      ).toEqual([{ marker: 'initial' }]);
    const control = await fixture.success<{ interruption: string }>({
      kind: 'control-read',
    });
    const witness = JSON.parse(control.interruption) as {
      claimJson: string;
      returnedTokenJson: string;
      item: unknown;
    };
    expect(JSON.parse(witness.claimJson)).toEqual(started.token);
    expect(witness.item).toMatchObject({
      ordinal: 0,
      beforeStatus: 'pending',
      afterStatus: 'active',
      planCursor: 0,
    });
    await fixture.reload();
    const reloaded = await fixture.call({ kind: 'control-read' });
    expect(reloaded.response.headers.get('X-Fixture-Instance')).not.toBe(
      originalInstance,
    );
    expect(reloaded.value.result).toMatchObject({
      interruption: control.interruption,
    });
    const replay = await fixture.call({
      kind: 'migration-continue',
      token: JSON.parse(witness.claimJson),
    });
    expect(replay.response.status).toBe(200);
    expect(replay.response.headers.get('X-Direct-Provider-Attempts')).toBe('0');
    expect(replay.response.headers.get('X-Direct-Maintenance-Attempts')).toBe(
      '0',
    );
    expect(replay.value.result).toMatchObject({
      status: 'pending',
      token: JSON.parse(witness.returnedTokenJson),
      itemOrdinal: 0,
      planCursor: 0,
    });
    expect((await items()).items).toEqual(admitted);
    let progress = replay.value.result as FleetMigrationAdvanceResult;
    let settlementRetried = false;
    for (let count = 0; progress.status === 'pending' && count < 100; count++) {
      const first = (await items()).items[0];
      if (
        !settlementRetried &&
        first?.planCursor !== undefined &&
        first.plan?.[first.planCursor]?.step === 'settle-ready'
      ) {
        progress = await retrySettlementAfterWriteFailure(
          fixture,
          progress.token,
        );
        settlementRetried = true;
      } else {
        progress = await fixture.success<FleetMigrationAdvanceResult>({
          kind: 'migration-continue',
          token: progress.token,
        });
      }
      const current = (await items()).items;
      if (current[1]?.status !== 'pending')
        expect(current[0]?.status).toBe('complete');
    }
    expect(progress.status).toBe('complete');
    expect(settlementRetried).toBe(true);
    expect(
      (
        await fixture.success<{ interruption: string }>({
          kind: 'control-read',
        })
      ).interruption,
    ).toBe(control.interruption);
    for (const role of ['a', 'b'] as const) {
      const record = await fixture.fleetStore.get(
        fixture.manifest.names.roles[role].tenantTag,
        fixture.manifest.environment,
      );
      const target = directDeploymentSpec(
        fixture.manifest,
        role,
        'next',
        fixture.secrets[role],
        fixture.binding,
      );
      expect(record).toMatchObject({
        phase: 'ready',
        schemaVersion: 2,
        desiredSpecDigest: deploymentSpecDigest(target),
      });
      if (!record) throw new Error('migrated fixture record is missing');
      const key = fleetSettlementKey({
        tenantTag: record.tenantTag,
        environment: record.environment,
        specDigest: record.desiredSpecDigest,
        artifactVersion: record.artifactVersion,
      });
      expect(record.settledSettlementKey).toBe(key);
      expect(await fixture.journal().readSettlement(key)).toBeDefined();
      const database = fixture.world.databases.find(
        (candidateDatabase) =>
          candidateDatabase.databaseId === record.databaseId,
      );
      expect(
        database?.d1.queryDatabase(
          'SELECT marker,release FROM direct_conformance_fixture WHERE id=1',
        ),
      ).toEqual([{ marker: 'next', release: 'next' }]);
    }
    const effects = await fixture.db
      .prepare(
        "SELECT observation_key FROM direct_reference_observations WHERE run_key=? AND observation_kind='settlement'",
      )
      .bind(fixture.manifest.resourcePrefix)
      .all();
    expect(effects.results).toHaveLength(2);
    const after = await inventory(fixture, 'inventory-after');
    expect(after.generation.generation).toBeGreaterThan(
      before.generation.generation,
    );
    await audit('audit-after');
    expect(
      await fixture.success({ kind: 'audit-start', slot: 'audit-before' }),
    ).toEqual(beforeAudit);
    expect(await fixture.journal().readOperation('audit-before')).toEqual(
      frozen,
    );
    expect(
      await fixture.success({
        kind: 'inventory-read',
        slot: 'inventory-before',
      }),
    ).toMatchObject({
      operationId: before.generation.operationId,
      generation: before.generation.generation,
    });
    const firstPage = await fixture.success<{
      items: FleetMigrationItem[];
      done: boolean;
    }>({ kind: 'migration-page', limit: 1 });
    expect(firstPage.items.map((item) => item.ordinal)).toEqual([0]);
    expect(firstPage.done).toBe(false);
    const secondPage = await fixture.success<{
      items: FleetMigrationItem[];
      done: boolean;
    }>({ kind: 'migration-page', limit: 1, afterOrdinal: 0 });
    expect(secondPage.items.map((item) => item.ordinal)).toEqual([1]);
    expect(secondPage.done).toBe(true);
    expect(
      await fixture.success({
        kind: 'migration-page',
        limit: 1,
        afterOrdinal: 1,
      }),
    ).toEqual({ items: [], done: true });
  });
});

describe.sequential('reference operation failure recovery', {
  timeout: 180_000,
}, () => {
  let fixture: DirectReferenceHarness;
  beforeEach(async () => {
    fixture = await createDirectReferenceHarness({ maintenanceNow: Date.now });
    for (const role of ['a', 'b'] as const)
      await fixture.success({ kind: 'provision', role, release: 'initial' });
  }, 60_000);
  afterEach(async () => {
    await fixture?.close();
  }, 30_000);

  it('retains the returned admission token when the interruption write fails and abandons running operations', async () => {
    const selected = await inventory(fixture, 'inventory-before');
    const audit = await fixture.success<FleetAuditAdvanceResult>({
      kind: 'audit-start',
      slot: 'audit-before',
    });
    expect(audit.status).toBe('pending');
    const auditStart = await fixture.journal().readOperation('audit-before');
    expect(
      await fixture.success({ kind: 'audit-abandon', slot: 'audit-before' }),
    ).toEqual({ operationId: audit.token.operationId });
    expect(await fixture.journal().readOperation('audit-before')).toEqual(
      auditStart,
    );
    const abandonedAudit = await fixture.success<FleetAuditAdvanceResult>({
      kind: 'audit-start',
      slot: 'audit-before',
    });
    expect(abandonedAudit).toMatchObject({
      status: 'failed',
      failure: { reason: 'operator-abandoned' },
    });
    expect(abandonedAudit.token.operationId).toBe(audit.token.operationId);
    expect(
      await fixture.success({
        kind: 'inventory-read',
        slot: 'inventory-before',
      }),
    ).toMatchObject({ generation: selected.generation.generation });

    const started = await fixture.success<FleetMigrationAdvanceResult>({
      kind: 'migration-start',
    });
    await fixture.db.exec(
      "CREATE TRIGGER refuse_interruption BEFORE UPDATE OF interruption_json ON direct_reference_run WHEN NEW.interruption_json IS NOT NULL BEGIN SELECT RAISE(ABORT,'fixture interruption write'); END",
    );
    try {
      const failed = await fixture.call({
        kind: 'migration-continue',
        token: started.token,
      });
      expect(failed.response.status).toBe(500);
    } finally {
      await fixture.db.exec('DROP TRIGGER refuse_interruption');
    }
    const retained = await fixture.journal().readOperation('migration-next');
    expect(retained?.tokenRevision).toBeGreaterThan(started.token.revision);
    expect(await fixture.journal().readInterruption()).toBeNull();
    const replay = await fixture.call({
      kind: 'migration-continue',
      token: started.token,
    });
    expect(replay.response.status).toBe(200);
    expect(replay.response.headers.get('X-Direct-Provider-Attempts')).toBe('0');
    expect(replay.response.headers.get('X-Direct-Maintenance-Attempts')).toBe(
      '0',
    );
    expect(replay.value.result).toMatchObject({
      status: 'pending',
      planCursor: 0,
      token: JSON.parse(retained?.tokenJson ?? '{}'),
    });
    expect(await fixture.journal().readInterruption()).toBeNull();
    expect(await fixture.success({ kind: 'migration-abandon' })).toEqual({
      operationId: started.token.operationId,
    });
    expect(await fixture.journal().readOperation('migration-next')).toEqual(
      retained,
    );
    const abandoned = await fixture.success<FleetMigrationAdvanceResult>({
      kind: 'migration-start',
    });
    expect(abandoned).toMatchObject({
      status: 'failed',
      failure: { reason: 'operator-abandoned' },
    });
    expect(abandoned.token.operationId).toBe(started.token.operationId);
    expect(
      (
        await fixture.success<{ items: FleetMigrationItem[] }>({
          kind: 'migration-page',
          limit: 2,
        })
      ).items.map(({ status }) => status),
    ).toEqual(['failed', 'pending']);
  });

  it('recovers the actual failed operation after a migration throws without a returned token', async () => {
    const started = await fixture.success<FleetMigrationAdvanceResult>({
      kind: 'migration-start',
    });
    const admission = await fixture.call({
      kind: 'migration-continue',
      token: started.token,
    });
    expect(admission.response.status).toBe(503);
    const row = await fixture.fleetStore.get(
      fixture.manifest.names.roles.a.tenantTag,
      fixture.manifest.environment,
    );
    const database = fixture.world.databases.find(
      ({ databaseId }) => databaseId === row?.databaseId,
    );
    if (!database) throw new Error('canary database is missing');
    database.d1.queryDatabase('DROP TABLE direct_conformance_fixture');
    let hint = await fixture.journal().readOperation('migration-next');
    let failed = false;
    for (let count = 0; count < 100; count++) {
      const response = await fixture.call({ kind: 'migration-continue' });
      if (response.response.status === 500) {
        expect(await fixture.journal().readOperation('migration-next')).toEqual(
          hint,
        );
        failed = true;
        break;
      }
      expect(response.response.status).toBe(200);
      expect(response.value.result).toMatchObject({ status: 'pending' });
      hint = await fixture.journal().readOperation('migration-next');
    }
    expect(failed).toBe(true);
    expect(fixture.sqlFailures).toContain(
      'no such table: direct_conformance_fixture',
    );
    const recovered = await fixture.call({ kind: 'migration-start' });
    expect(recovered.response.status).toBe(200);
    expect(recovered.response.headers.get('X-Direct-Provider-Attempts')).toBe(
      '0',
    );
    expect(
      recovered.response.headers.get('X-Direct-Maintenance-Attempts'),
    ).toBe('0');
    expect(recovered.value.result).toMatchObject({
      status: 'failed',
      failure: { reason: 'item-failed' },
      token: { operationId: started.token.operationId },
    });
    const stored = await fixture.journal().readOperation('migration-next');
    expect(stored?.inputJson).toBe(hint?.inputJson);
    expect(stored?.tokenRevision).toBeGreaterThan(hint?.tokenRevision ?? 0);
    expect(
      (
        await fixture.success<{ items: FleetMigrationItem[] }>({
          kind: 'migration-page',
          limit: 2,
        })
      ).items.map(({ status }) => status),
    ).toEqual(['failed', 'pending']);
  });
});
