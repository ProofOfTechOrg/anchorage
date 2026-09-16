// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { directDeploymentSpec } from '../scripts/direct-credentialed-spec.js';
import type { DirectDecommissionExportMetadata } from '../scripts/direct-reference-lifecycle.js';
import type { CleanupAdvanceResult } from '../src/cleanup-advance.js';
import type { DecommissionAdvanceResult } from '../src/decommission-advance.js';
import { deploymentSpecDigest } from '../src/spec-digest.js';
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
    const metadataAction = { kind: 'decommission-export', role: 'a' } as const;
    expect(await fixture.success(metadataAction)).toEqual({
      available: false,
      role: 'a',
      lifecyclePhase: 'not-started',
    });
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
    expect(await fixture.success(metadataAction)).toMatchObject({
      available: false,
      role: 'a',
    });
    let verifiedBeforeDelete = false;
    for (let calls = 0; advance.status !== 'complete' && calls < 150; calls++) {
      expect(advance.status).toBe('pending');
      advance = await fixture.success<DecommissionAdvanceResult>({
        kind: 'decommission-continue',
        role: 'a',
        token: advance.token,
      });
      const current = await fixture.fleetStore.get(
        fixture.manifest.names.roles.a.tenantTag,
        fixture.manifest.environment,
      );
      if (
        !verifiedBeforeDelete &&
        current?.decommissionIntent?.state === 'transitioning' &&
        current.decommissionIntent.lifecyclePhase === 'database-exported'
      ) {
        const selected = await fixture
          .journal()
          .readOperation('decommission-a');
        const providerRequests = fixture.projection.requests.length;
        const journalRow = await fixture.db
          .prepare(
            'SELECT operation_id,start_json,start_sha256,token_json,token_sha256,token_revision FROM direct_reference_operations WHERE run_key=? AND slot=?',
          )
          .bind(fixture.manifest.resourcePrefix, 'decommission-a')
          .first<{
            operation_id: string;
            start_json: string;
            start_sha256: string;
            token_json: string;
            token_sha256: string;
            token_revision: number;
          }>();
        if (!journalRow?.token_json)
          throw new Error('selected journal token is missing');
        const intent = current.decommissionIntent;
        expect(
          fixture.world.databases.some(
            (database) => database.databaseId === current.databaseId,
          ),
        ).toBe(true);
        const metadata =
          await fixture.success<DirectDecommissionExportMetadata>(
            metadataAction,
          );
        if (!metadata.available)
          throw new Error('saved export metadata is unavailable');
        const operationId = current.decommissionIntent.operationId;
        const key = `${fixture.manifest.resourcePrefix}/receipts/v1/${current.databaseId}/${operationId}.sql`;
        expect(metadata).toEqual({
          available: true,
          role: 'a',
          receipt: {
            version: 1,
            authority: `r2://${fixture.binding.exportBucketName}/${fixture.manifest.resourcePrefix}/receipts/v1`,
            databaseId: current.databaseId,
            operationId,
          },
          location: `r2://${fixture.binding.exportBucketName}/${key}`,
          size: current.databaseExportSize,
          sha256: current.databaseExportSha256,
          lifecyclePhase: 'database-exported',
          intentState: 'transitioning',
          revision: current.decommissionIntent.revision,
          generation: current.decommissionIntent.generation,
        });
        const object = await fixture.exportBytes.get(key);
        if (!object?.customMetadata)
          throw new Error('receipt object metadata is missing');
        const originalMetadata = { ...object.customMetadata };
        const bytes = new Uint8Array(await object.arrayBuffer());
        expect(bytes.byteLength).toBe(metadata.size);
        expect(createHash('sha256').update(bytes).digest('hex')).toBe(
          metadata.sha256,
        );
        const failures = await Promise.allSettled([
          (async () => {
            for (const changed of [
              {
                ...current,
                decommissionIntent: {
                  ...intent,
                  operationId: '00000000-0000-4000-8000-000000000000',
                },
              },
              {
                ...current,
                databaseExportLocation: 'r2://different/receipts/v1/other.sql',
              },
              { ...current, databaseExportSize: metadata.size + 1 },
              { ...current, databaseExportSha256: 'invalid' },
              {
                ...current,
                decommissionIntent: {
                  ...intent,
                  databaseExportReceiptAuthority: 'r2://different/receipts/v1',
                },
              },
            ]) {
              await fixture.fleetStore.withDeploymentLease(
                current.tenantTag,
                current.environment,
                (lease) => lease.put(changed),
              );
              expect(
                await fixture.fleetStore.get(
                  current.tenantTag,
                  current.environment,
                ),
              ).toEqual(changed);
              expect((await fixture.call(metadataAction)).value.ok).toBe(false);
              await fixture.fleetStore.withDeploymentLease(
                current.tenantTag,
                current.environment,
                (lease) => lease.put(current),
              );
            }
            const otherOperationId = '00000000-0000-4000-8000-000000000000';
            const tokenJson = JSON.stringify({
              ...JSON.parse(journalRow.token_json),
              operationId: otherOperationId,
            });
            const tokenHash = createHash('sha256')
              .update(
                JSON.stringify([
                  'token',
                  fixture.manifest.resourcePrefix,
                  'decommission-a',
                  tokenJson,
                ]),
              )
              .digest('hex');
            await fixture.db
              .prepare(
                'UPDATE direct_reference_operations SET operation_id=?,token_json=?,token_sha256=? WHERE run_key=? AND slot=?',
              )
              .bind(
                otherOperationId,
                tokenJson,
                tokenHash,
                fixture.manifest.resourcePrefix,
                'decommission-a',
              )
              .run();
            expect(
              (await fixture.journal().readOperation('decommission-a'))
                ?.operationId,
            ).toBe(otherOperationId);
            expect((await fixture.call(metadataAction)).value.ok).toBe(false);
            await fixture.db
              .prepare(
                'UPDATE direct_reference_operations SET operation_id=?,token_json=?,token_sha256=? WHERE run_key=? AND slot=?',
              )
              .bind(
                journalRow.operation_id,
                journalRow.token_json,
                journalRow.token_sha256,
                fixture.manifest.resourcePrefix,
                'decommission-a',
              )
              .run();
            const nextSpec = directDeploymentSpec(
              fixture.manifest,
              'a',
              'next',
              fixture.secrets.a,
              fixture.binding,
            );
            const startJson = JSON.stringify({
              version: 1,
              role: 'a',
              release: 'next',
              specDigest: deploymentSpecDigest(nextSpec),
            });
            const startHash = createHash('sha256')
              .update(
                JSON.stringify([
                  'start',
                  fixture.manifest.resourcePrefix,
                  'decommission-a',
                  'decommission',
                  null,
                  startJson,
                ]),
              )
              .digest('hex');
            await fixture.db
              .prepare(
                'UPDATE direct_reference_operations SET start_json=?,start_sha256=? WHERE run_key=? AND slot=?',
              )
              .bind(
                startJson,
                startHash,
                fixture.manifest.resourcePrefix,
                'decommission-a',
              )
              .run();
            expect(
              (await fixture.journal().readOperation('decommission-a'))
                ?.inputJson,
            ).toBe(startJson);
            expect((await fixture.call(metadataAction)).value.ok).toBe(false);
            await fixture.db
              .prepare(
                'UPDATE direct_reference_operations SET start_json=?,start_sha256=? WHERE run_key=? AND slot=?',
              )
              .bind(
                journalRow.start_json,
                journalRow.start_sha256,
                fixture.manifest.resourcePrefix,
                'decommission-a',
              )
              .run();
            for (const customMetadata of [
              {},
              {
                ...originalMetadata,
                anchorageOperationId: '00000000-0000-4000-8000-000000000000',
              },
              {
                ...originalMetadata,
                anchorageReceiptAuthority: 'r2://foreign/receipts/v1',
              },
              { ...originalMetadata, extra: 'unexpected' },
            ]) {
              await fixture.exportBytes.put(key, bytes, { customMetadata });
              expect((await fixture.call(metadataAction)).value).toEqual({
                contractVersion: 1,
                ok: false,
                error: { code: 'operation-refused' },
              });
            }
            await fixture.exportBytes.put(
              key,
              bytes.slice(0, bytes.byteLength - 1),
              { customMetadata: originalMetadata },
            );
            expect((await fixture.call(metadataAction)).value.ok).toBe(false);
            await fixture.exportBytes.delete(key);
            expect((await fixture.call(metadataAction)).value.ok).toBe(false);
          })(),
        ]);
        const restored = await Promise.allSettled([
          fixture.fleetStore.withDeploymentLease(
            current.tenantTag,
            current.environment,
            (lease) => lease.put(current),
          ),
          fixture.db
            .prepare(
              'UPDATE direct_reference_operations SET operation_id=?,start_json=?,start_sha256=?,token_json=?,token_sha256=?,token_revision=? WHERE run_key=? AND slot=?',
            )
            .bind(
              journalRow.operation_id,
              journalRow.start_json,
              journalRow.start_sha256,
              journalRow.token_json,
              journalRow.token_sha256,
              journalRow.token_revision,
              fixture.manifest.resourcePrefix,
              'decommission-a',
            )
            .run(),
          fixture.exportBytes.put(key, bytes, {
            customMetadata: originalMetadata,
          }),
        ]);
        const errors = [...failures, ...restored].flatMap((result) =>
          result.status === 'rejected' ? [result.reason] : [],
        );
        if (errors.length)
          throw new AggregateError(
            errors,
            'export metadata controls or restoration failed',
          );
        expect(await fixture.success(metadataAction)).toEqual(metadata);
        expect(
          await fixture.fleetStore.get(current.tenantTag, current.environment),
        ).toEqual(current);
        expect(await fixture.journal().readOperation('decommission-a')).toEqual(
          selected,
        );
        expect(fixture.projection.requests).toHaveLength(providerRequests);
        expect(
          fixture.world.databases.some(
            (database) => database.databaseId === current.databaseId,
          ),
        ).toBe(true);
        verifiedBeforeDelete = true;
      }
    }
    expect(verifiedBeforeDelete).toBe(true);
    expect(advance.status).toBe('complete');
    expect(fixture.world.databases).toHaveLength(0);
    expect(fixture.buckets.size).toBe(0);
    expect(await fixture.success(metadataAction)).toMatchObject({
      available: true,
      role: 'a',
      lifecyclePhase: 'decommissioned',
      intentState: 'complete',
    });
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

describe.sequential('private force through native control state', {
  timeout: 180_000,
}, () => {
  async function readyRecovery(fixture: DirectReferenceHarness) {
    const names = fixture.manifest.names.roles.recovery;
    const environment = fixture.manifest.environment;
    const failed = await fixture.success<{ cleanup: CleanupAdvanceResult }>({
      kind: 'provision',
      role: 'recovery',
      release: 'failed-recovery',
    });
    let cleanup = failed.cleanup;
    for (let count = 0; cleanup.status !== 'complete' && count < 150; count++) {
      expect(cleanup.status).toBe('pending');
      cleanup = await fixture.success<CleanupAdvanceResult>({
        kind: 'cleanup-continue',
        role: 'recovery',
        token: cleanup.token,
      });
    }
    if (cleanup.status !== 'complete')
      throw new Error('force fixture cleanup did not complete');
    const receipt = cleanup.receipt;
    await fixture.success({
      kind: 'provision',
      role: 'recovery',
      release: 'initial',
    });
    const ready = await fixture.fleetStore.get(names.tenantTag, environment);
    if (!ready) throw new Error('fresh recovery record is missing');
    expect(ready.phase).toBe('ready');
    expect(
      fixture.world.databases.some(
        (database) => database.databaseId === ready.databaseId,
      ),
    ).toBe(true);
    return { ready, receipt };
  }

  it('force terminal guards the leased identity and clears a terminal row without provider requests', async () => {
    const fixture = await createDirectReferenceHarness();
    try {
      const names = fixture.manifest.names.roles.a;
      const environment = fixture.manifest.environment;
      const action = { kind: 'force-terminal', role: 'a' } as const;
      await fixture.success({
        kind: 'provision',
        role: 'a',
        release: 'initial',
      });
      const ready = await fixture.fleetStore.get(names.tenantTag, environment);
      if (!ready) throw new Error('ready row is missing');
      const refused = await fixture.call(action);
      expect(refused.response.status).toBe(409);
      expect(refused.response.headers.get('X-Direct-Provider-Attempts')).toBe(
        '0',
      );
      await fixture.success({ kind: 'decommission-start', role: 'a' });
      const active = await fixture.fleetStore.get(names.tenantTag, environment);
      if (!active?.decommissionIntent)
        throw new Error('decommission intent is missing');
      // The store rejects this phase/intent pair on write, so corrupt the row
      // through SQL to exercise refusal at the native read boundary.
      await fixture.db
        .prepare(
          "UPDATE anchorage_fleet_deployments SET phase = 'decommissioned' WHERE tenant_tag = ? AND environment = ?",
        )
        .bind(names.tenantTag, environment)
        .run();
      expect((await fixture.call(action)).response.status).toBe(500);
      await fixture.db
        .prepare(
          'UPDATE anchorage_fleet_deployments SET phase = ? WHERE tenant_tag = ? AND environment = ?',
        )
        .bind(active.phase, names.tenantTag, environment)
        .run();
      await fixture.fleetStore.withDeploymentLease(
        names.tenantTag,
        environment,
        (lease) =>
          lease.put({
            ...ready,
            phase: 'decommissioned',
            scriptName: fixture.manifest.names.roles.b.scriptName,
          }),
      );
      expect((await fixture.call(action)).response.status).toBe(409);
      await fixture.fleetStore.withDeploymentLease(
        names.tenantTag,
        environment,
        (lease) => lease.put({ ...ready, phase: 'decommissioned' }),
      );
      const requests = fixture.projection.requests.length;
      const cleared = await fixture.call(action);
      expect(cleared.response.status).toBe(200);
      expect(cleared.response.headers.get('X-Direct-Provider-Attempts')).toBe(
        '0',
      );
      expect(cleared.value.result).toEqual({
        returned: true,
        before: { databaseId: ready.databaseId, scriptName: ready.scriptName },
        after: { present: false },
      });
      expect(
        await fixture.fleetStore.get(names.tenantTag, environment),
      ).toBeUndefined();
      const repeated = await fixture.call(action);
      expect(repeated.response.status).toBe(200);
      expect(repeated.response.headers.get('X-Direct-Provider-Attempts')).toBe(
        '0',
      );
      expect(repeated.value.result).toEqual({
        returned: true,
        before: null,
        after: { present: false },
      });
      expect(fixture.projection.requests).toHaveLength(requests);
    } finally {
      await fixture.close();
    }
  });

  it('preserves force witnesses and resumes settled residual cleanup after exhausting the provider budget', async () => {
    const fixture = await createDirectReferenceHarness();
    try {
      const names = fixture.manifest.names.roles.recovery;
      const environment = fixture.manifest.environment;
      const absent = await fixture.call({ kind: 'force-recovery' });
      expect(absent.response.status).toBe(409);
      expect(fixture.projection.requests).toEqual([]);
      const { ready, receipt } = await readyRecovery(fixture);
      const mutations = [...fixture.world.mutationLog];
      await fixture.db.exec(
        "CREATE TRIGGER refuse_force_witness BEFORE INSERT ON direct_reference_observations WHEN NEW.observation_kind='force-before' BEGIN SELECT RAISE(ABORT,'fixture force witness unavailable'); END",
      );
      try {
        const refused = await fixture.call({ kind: 'force-recovery' });
        expect(refused.response.status).toBe(500);
        expect(
          (await fixture.fleetStore.get(names.tenantTag, environment))?.phase,
        ).toBe('ready');
        expect(fixture.world.mutationLog).toEqual(mutations);
        expect(await fixture.journal().readForceBefore()).toBeUndefined();
      } finally {
        await fixture.db.exec('DROP TRIGGER refuse_force_witness');
      }
      await fixture.db.exec(
        "CREATE TRIGGER require_force_witness BEFORE UPDATE ON anchorage_fleet_deployments WHEN NEW.phase='decommissioning' AND NOT EXISTS(SELECT 1 FROM direct_reference_observations WHERE observation_kind='force-before') BEGIN SELECT RAISE(ABORT,'force phase precedes its witness'); END",
      );
      fixture.world.failNext('deleteDatabase', { dispatched: false });
      const interrupted = await fixture.call({ kind: 'force-recovery' });
      expect(interrupted.response.status).toBe(500);
      expect(fixture.world.peekFailure('deleteDatabase')).toBeUndefined();
      expect(
        (await fixture.fleetStore.get(names.tenantTag, environment))?.phase,
      ).toBe('database-deleting');
      const before = await fixture.journal().readForceBefore();
      if (!before) throw new Error('force before witness is missing');
      expect(JSON.parse(before.provenanceJson)).toMatchObject({
        phase: 'ready',
        recordUpdatedAt: ready.updatedAt,
      });
      expect(JSON.parse(before.identityJson).priorCleanup.operationId).toBe(
        receipt.operationId,
      );
      const exportCount = fixture.world.exports.size;
      await fixture.reload();
      expect(await fixture.success({ kind: 'force-recovery' })).toMatchObject({
        returned: true,
      });
      expect(
        await fixture.fleetStore.get(names.tenantTag, environment),
      ).toBeUndefined();
      expect(
        fixture.world.databases.some(
          (database) => database.databaseId === ready.databaseId,
        ),
      ).toBe(false);
      expect(fixture.world.scripts.has(ready.scriptName)).toBe(true);
      for (const resource of ready.applicationResources ?? [])
        expect(
          fixture.buckets.has(
            `${resource.jurisdiction}:${resource.bucketName}`,
          ),
        ).toBe(true);
      expect(
        await fixture.fleetStore.readCleanupReceipt(receipt.operationId),
      ).toEqual(receipt);
      expect(await fixture.journal().readForceBefore()).toEqual(before);
      expect(fixture.world.exports.size).toBe(exportCount);
      const after = [...fixture.world.mutationLog];
      await fixture.fleetStore.withDeploymentLease(
        names.tenantTag,
        environment,
        (lease) =>
          lease.put({
            ...ready,
            databaseId: '00000000-0000-4000-8000-000000000099',
          }),
      );
      const beforeForeignReplay = fixture.projection.requests.length;
      expect(
        (await fixture.call({ kind: 'force-recovery' })).response.status,
      ).toBe(409);
      expect(fixture.projection.requests).toHaveLength(beforeForeignReplay);
      expect(fixture.world.mutationLog).toEqual(after);
      await fixture.fleetStore.withDeploymentLease(
        names.tenantTag,
        environment,
        (lease) => {
          if (!lease.deleteReleasingClaims)
            throw new Error('fixture claim release is unavailable');
          return lease.deleteReleasingClaims();
        },
      );
      fixture.world.durableObjectNamespaces.push({
        id: 'unrelated-namespace',
        script: 'unrelated-script',
        className: 'Other',
      });
      const footprint = await fixture.success<{
        observation: Record<string, unknown>;
        provenance: Record<string, unknown>;
      }>({ kind: 'force-observe' });
      const retainedScript = fixture.world.scripts.get(ready.scriptName);
      if (!retainedScript) throw new Error('force removed the retained script');
      expect(footprint.observation).toMatchObject({
        version: 1,
        role: 'recovery',
        beforeIdentitySha256: JSON.parse(before.identityJson)
          .beforeIdentitySha256,
        fleetRecordPresent: false,
        deploymentClaimsPresent: false,
        database: {
          id: ready.databaseId,
          expectedName: ready.databaseName,
          observedName: null,
        },
        worker: {
          scriptName: ready.scriptName,
          scriptPresent: true,
          workersDevEnabled: false,
          previewUrlsEnabled: false,
          customDomains: [],
          zoneRoutes: [],
          currentSecretNames: [],
          currentVersionIds: retainedScript.versions
            .map((version) => version.versionId)
            .sort(),
          currentNamespaceIds: ready.durableObjectBindings
            .map((binding) => binding.namespaceId)
            .sort(),
          survivingRecordedNamespaceIds: ready.durableObjectBindings
            .map((binding) => binding.namespaceId)
            .sort(),
        },
        priorCleanup: { operationId: receipt.operationId, matchesBefore: true },
      });
      expect(footprint.observation.buckets).toEqual(
        (ready.applicationResources ?? [])
          .map((resource) => ({
            bindingName: resource.name,
            bucketName: resource.bucketName,
            jurisdiction: resource.jurisdiction,
            expectedCreationDate: resource.creationDate,
            observedCreationDate: resource.creationDate,
          }))
          .sort((a, b) => a.bindingName.localeCompare(b.bindingName)),
      );
      expect(fixture.world.mutationLog).toEqual(after);
      const reads = fixture.projection.requests.length;
      await fixture.reload();
      expect(await fixture.success({ kind: 'force-observe' })).toEqual(
        footprint,
      );
      expect(fixture.projection.requests).toHaveLength(reads);
      expect(await fixture.success({ kind: 'force-recovery' })).toMatchObject({
        returned: true,
      });
      expect(fixture.world.mutationLog).toEqual(after);
      expect(
        (
          await fixture.call({
            kind: 'provision',
            role: 'recovery',
            release: 'initial',
          })
        ).response.status,
      ).toBe(409);
      expect(fixture.world.mutationLog).toEqual(after);
      const archived = await fixture.journal().readForceAfter();
      const operations = await fixture.db
        .prepare('SELECT * FROM direct_reference_operations ORDER BY slot')
        .all();
      const exports = await fixture.exportBytes.list();
      const exhausted = await fixture.call({ kind: 'recover-force-residual' });
      expect(exhausted.response.status).toBe(500);
      expect(exhausted.value).toMatchObject({
        ok: false,
        error: { code: 'operation-refused' },
      });
      expect(exhausted.response.headers.get('X-Direct-Provider-Attempts')).toBe(
        '100',
      );
      expect(retainedScript.present).toBe(false);
      expect(retainedScript.versions).toEqual([]);
      for (const resource of ready.applicationResources ?? [])
        expect(
          fixture.buckets.has(
            `${resource.jurisdiction}:${resource.bucketName}`,
          ),
        ).toBe(false);
      const exhaustedDeletes = fixture.projection.requests.filter(
        (request) => request.method === 'DELETE',
      );
      expect(await fixture.journal().readForceAfter()).toEqual(archived);
      expect(
        await fixture.fleetStore.readCleanupReceipt(receipt.operationId),
      ).toEqual(receipt);
      expect(
        (
          await fixture.db
            .prepare('SELECT * FROM direct_reference_operations ORDER BY slot')
            .all()
        ).results,
      ).toEqual(operations.results);
      await fixture.reload();
      const recovered = await fixture.success<{
        returned: true;
        observation: Record<string, unknown>;
      }>({ kind: 'recover-force-residual' });
      expect(
        fixture.projection.requests.filter(
          (request) => request.method === 'DELETE',
        ),
      ).toEqual(exhaustedDeletes);
      expect(recovered).toMatchObject({
        returned: true,
        observation: {
          beforeIdentitySha256: footprint.observation.beforeIdentitySha256,
          fleetRecordPresent: false,
          deploymentClaimsPresent: false,
          database: { id: ready.databaseId, observedName: null },
          worker: {
            scriptPresent: false,
            currentVersionIds: null,
            currentNamespaceIds: [],
            survivingRecordedNamespaceIds: [],
            customDomains: [],
            zoneRoutes: [],
            currentSecretNames: [],
          },
          buckets: (ready.applicationResources ?? []).map((resource) => ({
            bucketName: resource.bucketName,
            observedCreationDate: null,
          })),
          priorCleanup: {
            operationId: receipt.operationId,
            matchesBefore: true,
          },
        },
      });
      expect(retainedScript.present).toBe(false);
      expect(retainedScript.versions).toEqual([]);
      expect(fixture.world.durableObjectNamespaces).toEqual([
        {
          id: 'unrelated-namespace',
          script: 'unrelated-script',
          className: 'Other',
        },
      ]);
      for (const resource of ready.applicationResources ?? [])
        expect(
          fixture.buckets.has(
            `${resource.jurisdiction}:${resource.bucketName}`,
          ),
        ).toBe(false);
      const deletes = fixture.projection.requests.filter(
        (request) => request.method === 'DELETE',
      );
      await fixture.reload();
      expect(
        await fixture.success({ kind: 'recover-force-residual' }),
      ).toMatchObject({
        returned: true,
        observation: recovered.observation,
      });
      expect(
        fixture.projection.requests.filter(
          (request) => request.method === 'DELETE',
        ),
      ).toEqual(deletes);
      expect(await fixture.success({ kind: 'force-observe' })).toEqual(
        footprint,
      );
      expect(await fixture.journal().readForceAfter()).toEqual(archived);
      expect(await fixture.journal().readForceBefore()).toEqual(before);
      expect(
        (
          await fixture.db
            .prepare('SELECT * FROM direct_reference_operations ORDER BY slot')
            .all()
        ).results,
      ).toEqual(operations.results);
      expect(
        await fixture.fleetStore.readCleanupReceipt(receipt.operationId),
      ).toEqual(receipt);
      expect(
        await fixture.fleetStore.get(names.tenantTag, environment),
      ).toBeUndefined();
      expect((await fixture.exportBytes.list()).objects).toEqual(
        exports.objects,
      );
      expect(fixture.world.exports.size).toBe(exportCount);
    } finally {
      await fixture.close();
    }
  });

  it('refuses residual deletion when archived evidence or current ownership and resource facts change', async () => {
    let hideAttachmentIdentity = false;
    const fixture = await createDirectReferenceHarness({
      async providerResponse(request, response) {
        const path = new URL(request.url).pathname;
        if (
          !hideAttachmentIdentity ||
          request.method !== 'GET' ||
          !path.includes('/scripts/foreign-attachment/') ||
          !(
            path.endsWith('/versions/foreign-version') ||
            path.endsWith('/settings')
          )
        )
          return response;
        const body = (await response.json()) as {
          result: {
            resources?: { bindings: Record<string, unknown>[] };
            bindings?: Record<string, unknown>[];
          };
        };
        const bindings =
          body.result.resources?.bindings ?? body.result.bindings;
        if (!bindings)
          throw new Error('fixture attachment response is missing bindings');
        for (const binding of bindings) {
          if (binding.type === 'd1') delete binding.database_id;
          if (binding.type === 'r2_bucket') delete binding.bucket_name;
        }
        return Response.json(body, {
          status: response.status,
          headers: response.headers,
        });
      },
    });
    try {
      const { ready, receipt } = await readyRecovery(fixture);
      const database = fixture.world.databases.find(
        (entry) => entry.databaseId === ready.databaseId,
      );
      if (!database) throw new Error('recovery database is missing');
      await fixture.success({ kind: 'force-recovery' });
      const mutations = [...fixture.world.mutationLog];
      const deletes = fixture.projection.requests.filter(
        (request) => request.method === 'DELETE',
      );
      async function refused(reason: string) {
        const result = await fixture.call({ kind: 'recover-force-residual' });
        expect(result.value.ok, reason).toBe(false);
        expect(fixture.world.mutationLog, reason).toEqual(mutations);
        expect(
          fixture.projection.requests.filter(
            (request) => request.method === 'DELETE',
          ),
          reason,
        ).toEqual(deletes);
      }
      await refused('missing force-after');
      await fixture.success({ kind: 'force-observe' });
      const archived = await fixture.journal().readForceAfter();
      if (!archived) throw new Error('force-after is missing');
      const original = JSON.parse(archived.identityJson);
      async function storeAfter(
        identityJson: string,
        provenanceJson = archived?.provenanceJson,
      ) {
        if (!provenanceJson) throw new Error('force provenance is missing');
        const hash = (field: string, value: string) =>
          createHash('sha256')
            .update(
              JSON.stringify([
                'observation',
                fixture.manifest.resourcePrefix,
                'force-after',
                'recovery',
                field,
                value,
              ]),
            )
            .digest('hex');
        await fixture.db
          .prepare(
            "UPDATE direct_reference_observations SET identity_json=?,identity_sha256=?,provenance_json=?,provenance_sha256=? WHERE observation_kind='force-after'",
          )
          .bind(
            identityJson,
            hash('identity', identityJson),
            provenanceJson,
            hash('provenance', provenanceJson),
          )
          .run();
      }
      const malformed = [
        {
          ...original,
          worker: { ...original.worker, scriptPresent: undefined },
        },
        { ...original, extra: true },
        {
          ...original,
          worker: {
            ...original.worker,
            currentVersionIds: [
              ...original.worker.currentVersionIds,
              original.worker.currentVersionIds[0],
            ],
          },
        },
        {
          ...original,
          database: { ...original.database, id: 'foreign-database' },
        },
        { ...original, buckets: [] },
      ];
      for (const [index, observation] of malformed.entries()) {
        await storeAfter(JSON.stringify(observation));
        expect(await fixture.journal().readForceAfter()).toBeDefined();
        await refused(`valid-hash malformed force-after ${index}`);
        expect((await fixture.call({ kind: 'force-observe' })).value.ok).toBe(
          false,
        );
      }
      for (const observation of [
        { ...original, fleetRecordPresent: true },
        { ...original, deploymentClaimsPresent: true },
        {
          ...original,
          database: { ...original.database, observedName: ready.databaseName },
        },
        {
          ...original,
          worker: { ...original.worker, workersDevEnabled: null },
        },
        { ...original, worker: { ...original.worker, scriptPresent: false } },
        {
          ...original,
          worker: { ...original.worker, currentVersionIds: null },
        },
        {
          ...original,
          buckets: original.buckets.map((bucket: Record<string, unknown>) => ({
            ...bucket,
            observedCreationDate: null,
          })),
        },
        {
          ...original,
          priorCleanup: { ...original.priorCleanup, matchesBefore: false },
        },
      ]) {
        await storeAfter(JSON.stringify(observation));
        await refused('complete but unsuccessful initial footprint');
      }
      await storeAfter(
        archived.identityJson,
        JSON.stringify({ startedAtMs: 0, completedAtMs: -1 }),
      );
      await refused('valid-hash invalid provenance');
      await storeAfter(archived.identityJson);
      await fixture.fleetStore.withDeploymentLease(
        ready.tenantTag,
        ready.environment,
        (lease) => lease.put(ready),
      );
      await refused('replacement fleet record');
      await fixture.fleetStore.withDeploymentLease(
        ready.tenantTag,
        ready.environment,
        (lease) => {
          if (!lease.deleteReleasingClaims)
            throw new Error('claim release is unavailable');
          return lease.deleteReleasingClaims();
        },
      );
      const resource = ready.applicationResources?.[0];
      if (!resource) throw new Error('recovery bucket witness is missing');
      for (const [type, name, set] of [
        [
          'worker-script',
          'unrelated-worker',
          `deployment:${ready.tenantTag}:${ready.environment}`,
        ],
        ['worker-script', ready.scriptName, 'foreign-set'],
        ['r2-bucket', resource.bucketName, 'foreign-set'],
      ]) {
        await fixture.db
          .prepare(
            'INSERT INTO anchorage_platform_plane_claims (account_id,resource_type,resource_name,resource_role,resource_set_key,platform_plane_identity) VALUES (?,?,?,?,?,?)',
          )
          .bind(
            fixture.binding.accountId,
            type,
            name,
            type === 'r2-bucket' ? 'deployment-r2' : 'deployment-worker',
            set,
            'fixture-residual-foreign',
          )
          .run();
        await refused(`visible ${type} claim ${name}`);
        await fixture.db
          .prepare(
            "DELETE FROM anchorage_platform_plane_claims WHERE platform_plane_identity='fixture-residual-foreign'",
          )
          .run();
      }
      fixture.world.databases.push(database);
      await refused('D1 reappeared');
      fixture.world.databases.splice(
        fixture.world.databases.indexOf(database),
        1,
      );
      const script = fixture.world.scripts.get(ready.scriptName);
      if (!script) throw new Error('retained Worker is missing');
      script.subdomain.enabled = true;
      await refused('workers.dev enabled');
      script.subdomain.enabled = false;
      script.subdomain.previewsEnabled = true;
      await refused('preview URLs enabled');
      script.subdomain.previewsEnabled = false;
      fixture.world.customDomains.push({
        id: 'residual-domain',
        hostname: 'foreign.example.test',
        service: ready.scriptName,
      });
      await refused('custom domain returned');
      fixture.world.customDomains.pop();
      fixture.world.zones.push({ id: 'residual-zone' });
      fixture.world.routes.push({
        zoneId: 'residual-zone',
        id: 'residual-route',
        pattern: 'foreign.example.test/*',
        script: ready.scriptName,
      });
      await refused('zone route returned');
      fixture.world.routes.pop();
      fixture.world.zones.pop();
      script.secretNames.add('FOREIGN_SECRET');
      await refused('secret returned');
      script.secretNames.delete('FOREIGN_SECRET');
      const versions = [...script.versions];
      const first = versions[0];
      if (!first) throw new Error('retained version is missing');
      script.versions.push({ ...first, versionId: 'foreign-version' });
      await refused('unrecorded version');
      script.versions = versions.filter(
        (version) => version.versionId !== ready.artifactVersion,
      );
      await refused('original version anchor missing');
      script.versions = [];
      await refused('retained version inventory empty');
      script.versions = versions;
      const namespaces = [...fixture.world.durableObjectNamespaces];
      fixture.world.durableObjectNamespaces.push({
        id: 'foreign-namespace',
        script: ready.scriptName,
        className: 'Foreign',
      });
      await refused('unrecorded namespace');
      fixture.world.durableObjectNamespaces.splice(
        0,
        fixture.world.durableObjectNamespaces.length,
        ...namespaces.slice(1),
      );
      await refused('recorded namespace missing');
      fixture.world.durableObjectNamespaces.splice(
        0,
        fixture.world.durableObjectNamespaces.length,
        ...namespaces,
      );
      const bucketKey = `${resource.jurisdiction}:${resource.bucketName}`;
      const bucket = fixture.buckets.get(bucketKey);
      if (!bucket) throw new Error('retained bucket is missing');
      const creationDate = bucket.creation_date;
      bucket.creation_date = new Date(
        Date.parse(creationDate) + 1000,
      ).toISOString();
      await refused('bucket incarnation changed');
      bucket.creation_date = creationDate;
      fixture.buckets.delete(bucketKey);
      await refused('bucket absent while Worker remains');
      fixture.buckets.set(bucketKey, bucket);
      const objectKey = `${bucketKey}/foreign-object`;
      await fixture.applicationBytes.put(objectKey, 'must survive');
      await refused('nonempty bucket prevents Worker deletion');
      expect(
        await (await fixture.applicationBytes.get(objectKey))?.text(),
      ).toBe('must survive');
      await fixture.applicationBytes.delete(objectKey);
      for (const binding of [
        { type: 'd1', name: 'FOREIGN_DATABASE', database_id: ready.databaseId },
        {
          type: 'r2_bucket',
          name: 'FOREIGN_BUCKET',
          bucket_name: resource.bucketName,
        },
      ]) {
        fixture.world.seedScript('foreign-attachment', {
          versions: [
            { ...first, versionId: 'foreign-version', bindings: [binding] },
          ],
          deployment: [{ versionId: 'foreign-version', percentage: 100 }],
          subdomain: { enabled: false, previewsEnabled: false },
        });
        await refused(`foreign ordinary ${binding.type} attachment`);
        hideAttachmentIdentity = true;
        await refused(`incomplete foreign ordinary ${binding.type} attachment`);
        hideAttachmentIdentity = false;
        expect(
          fixture.world.scripts.get('foreign-attachment')?.versions[0]
            ?.bindings,
        ).toEqual([binding]);
        fixture.world.scripts.delete('foreign-attachment');
        fixture.world.dispatchNamespaces.push({
          name: 'foreign-dispatch',
          scripts: [{ name: 'foreign-attachment', bindings: [binding] }],
        });
        await refused(`foreign dispatch ${binding.type} attachment`);
        hideAttachmentIdentity = true;
        await refused(`incomplete foreign dispatch ${binding.type} attachment`);
        hideAttachmentIdentity = false;
        expect(
          fixture.world.dispatchNamespaces.at(-1)?.scripts[0]?.bindings,
        ).toEqual([binding]);
        fixture.world.dispatchNamespaces.pop();
      }
      const completedAtMs = receipt.completedAtMs;
      if (completedAtMs === undefined)
        throw new Error('cleanup completion time is missing');
      await fixture.db
        .prepare(
          'UPDATE anchorage_fleet_cleanup_receipts SET completed_at_ms=? WHERE operation_id=?',
        )
        .bind(completedAtMs + 1, receipt.operationId)
        .run();
      await refused('historical receipt changed');
      await fixture.db
        .prepare(
          'UPDATE anchorage_fleet_cleanup_receipts SET completed_at_ms=? WHERE operation_id=?',
        )
        .bind(completedAtMs, receipt.operationId)
        .run();
      for (const fault of ['claim', 'lease'] as const) {
        let applied = false;
        fixture.world.afterNext('listCustomDomains', async () => {
          if (fault === 'claim') {
            await fixture.db
              .prepare(
                'INSERT INTO anchorage_platform_plane_claims (account_id,resource_type,resource_name,resource_role,resource_set_key,platform_plane_identity) VALUES (?,?,?,?,?,?)',
              )
              .bind(
                fixture.binding.accountId,
                'worker-script',
                ready.scriptName,
                'deployment-worker',
                'foreign-set',
                'fixture-residual-foreign',
              )
              .run();
          } else {
            await fixture.db
              .prepare(
                'UPDATE anchorage_fleet_leases SET owner_token=? WHERE tenant_tag=? AND environment=?',
              )
              .bind('foreign-lease-owner', ready.tenantTag, ready.environment)
              .run();
          }
          applied = true;
        });
        await refused(`${fault} changed during provider reads`);
        expect(applied).toBe(true);
        await fixture.db
          .prepare(
            "DELETE FROM anchorage_platform_plane_claims WHERE platform_plane_identity='fixture-residual-foreign'",
          )
          .run();
        await fixture.db
          .prepare(
            "DELETE FROM anchorage_fleet_leases WHERE owner_token='foreign-lease-owner'",
          )
          .run();
      }
      expect(await fixture.journal().readForceAfter()).toEqual(archived);
      expect(
        await fixture.fleetStore.readCleanupReceipt(receipt.operationId),
      ).toEqual(receipt);
      expect(
        await fixture.success({ kind: 'recover-force-residual' }),
      ).toMatchObject({ returned: true });
      expect(fixture.bridgeErrors).toEqual([]);
    } finally {
      await fixture.close();
    }
  });

  it('retries settled residual deletions without duplicating committed Worker or bucket deletes', async () => {
    const fixture = await createDirectReferenceHarness();
    try {
      const { ready, receipt } = await readyRecovery(fixture);
      await fixture.success({ kind: 'force-recovery' });
      const observed = await fixture.success({ kind: 'force-observe' });
      const resource = ready.applicationResources?.[0];
      const script = fixture.world.scripts.get(ready.scriptName);
      if (!resource || !script) throw new Error('residual fixture is missing');
      const bucketKey = `${resource.jurisdiction}:${resource.bucketName}`;
      const originalDeletes = fixture.projection.requests.filter(
        (request) => request.method === 'DELETE',
      ).length;
      fixture.world.failNext('deleteWorkerScript', { dispatched: false });
      expect(
        (await fixture.call({ kind: 'recover-force-residual' })).value.ok,
      ).toBe(false);
      expect(fixture.world.peekFailure('deleteWorkerScript')).toBeUndefined();
      expect(script.present).toBe(true);
      expect(fixture.buckets.has(bucketKey)).toBe(true);
      const namespaces = [...fixture.world.durableObjectNamespaces];
      fixture.world.failNext('deleteWorkerScript', { dispatched: true });
      fixture.world.afterNext('deleteWorkerScript', () => {
        fixture.world.durableObjectNamespaces.push(
          ...namespaces.map((namespace) => ({
            ...namespace,
            script: 'provider-lag',
          })),
        );
      });
      expect(
        (await fixture.call({ kind: 'recover-force-residual' })).value.ok,
      ).toBe(false);
      expect(script.present).toBe(false);
      expect(fixture.buckets.has(bucketKey)).toBe(true);
      expect(
        fixture.world.mutationLog.filter(
          (entry) => entry === `delete-script:${ready.scriptName}`,
        ),
      ).toHaveLength(1);
      const afterWorker = fixture.projection.requests.filter(
        (request) => request.method === 'DELETE',
      );
      expect(afterWorker).toHaveLength(originalDeletes + 2);
      await fixture.reload();
      expect(
        (await fixture.call({ kind: 'recover-force-residual' })).value.ok,
      ).toBe(false);
      expect(
        fixture.projection.requests.filter(
          (request) => request.method === 'DELETE',
        ),
      ).toEqual(afterWorker);
      expect(fixture.buckets.has(bucketKey)).toBe(true);
      fixture.world.durableObjectNamespaces.length = 0;
      fixture.world.failNext('deleteApplicationR2Bucket', {
        dispatched: false,
      });
      expect(
        (await fixture.call({ kind: 'recover-force-residual' })).value.ok,
      ).toBe(false);
      expect(
        fixture.world.peekFailure('deleteApplicationR2Bucket'),
      ).toBeUndefined();
      expect(fixture.buckets.has(bucketKey)).toBe(true);
      fixture.world.failNext('deleteApplicationR2Bucket', { dispatched: true });
      fixture.world.afterNext('deleteApplicationR2Bucket', () => {
        fixture.world.failNext('getApplicationR2Bucket', { dispatched: false });
      });
      expect(
        (await fixture.call({ kind: 'recover-force-residual' })).value.ok,
      ).toBe(false);
      expect(
        fixture.world.peekFailure('deleteApplicationR2Bucket'),
      ).toBeUndefined();
      expect(fixture.buckets.has(bucketKey)).toBe(false);
      const afterBucket = fixture.projection.requests.filter(
        (request) => request.method === 'DELETE',
      );
      expect(afterBucket).toHaveLength(originalDeletes + 4);
      await fixture.db
        .prepare(
          'INSERT INTO anchorage_platform_plane_claims (account_id,resource_type,resource_name,resource_role,resource_set_key,platform_plane_identity) VALUES (?,?,?,?,?,?)',
        )
        .bind(
          fixture.binding.accountId,
          'r2-bucket',
          resource.bucketName,
          'deployment-r2',
          'foreign-set',
          'fixture-residual-foreign',
        )
        .run();
      expect(
        (await fixture.call({ kind: 'recover-force-residual' })).value.ok,
      ).toBe(false);
      expect(
        fixture.projection.requests.filter(
          (request) => request.method === 'DELETE',
        ),
      ).toEqual(afterBucket);
      await fixture.db
        .prepare(
          "DELETE FROM anchorage_platform_plane_claims WHERE platform_plane_identity='fixture-residual-foreign'",
        )
        .run();
      await fixture.reload();
      expect(
        (await fixture.call({ kind: 'recover-force-residual' })).value.ok,
      ).toBe(false);
      expect(
        fixture.world.peekFailure('getApplicationR2Bucket'),
      ).toBeUndefined();
      expect(
        fixture.projection.requests.filter(
          (request) => request.method === 'DELETE',
        ),
      ).toEqual(afterBucket);
      expect(
        await fixture.success({ kind: 'recover-force-residual' }),
      ).toMatchObject({ returned: true });
      expect(
        fixture.projection.requests.filter(
          (request) => request.method === 'DELETE',
        ),
      ).toEqual(afterBucket);
      expect(await fixture.success({ kind: 'force-observe' })).toEqual(
        observed,
      );
      expect(
        await fixture.fleetStore.readCleanupReceipt(receipt.operationId),
      ).toEqual(receipt);
      expect(fixture.bridgeErrors).toEqual([]);
    } finally {
      await fixture.close();
    }
  });

  it('leaves incomplete reads unrecorded and reports retained claims and changed resources independently', async () => {
    const fixture = await createDirectReferenceHarness();
    try {
      const { ready, receipt } = await readyRecovery(fixture);
      fixture.world.failNext('deleteDatabase', { dispatched: false });
      expect(
        (await fixture.call({ kind: 'force-recovery' })).response.status,
      ).toBe(500);
      expect(fixture.world.peekFailure('deleteDatabase')).toBeUndefined();
      expect(await fixture.journal().readForceBefore()).toBeDefined();
      const script = fixture.world.scripts.get(ready.scriptName);
      const version = script?.versions[0];
      if (!version)
        throw new Error('force fixture has no recorded Worker version');
      const descriptor = Object.getOwnPropertyDescriptor(version, 'versionId');
      if (!descriptor)
        throw new Error('force fixture version has no identifier');
      Object.defineProperty(version, 'versionId', { value: undefined });
      const mutations = [...fixture.world.mutationLog];
      try {
        expect(
          (await fixture.call({ kind: 'force-observe' })).response.status,
        ).toBe(500);
        expect(await fixture.journal().readForceAfter()).toBeUndefined();
        expect(fixture.world.mutationLog).toEqual(mutations);
      } finally {
        Object.defineProperty(version, 'versionId', descriptor);
      }
      await fixture.fleetStore.withDeploymentLease(
        ready.tenantTag,
        ready.environment,
        (lease) => lease.delete(),
      );
      expect(
        await fixture.fleetStore.get(ready.tenantTag, ready.environment),
      ).toBeUndefined();
      await fixture.fleetStore.pruneCleanupReceipts({
        completedBeforeMs: Number.MAX_SAFE_INTEGER,
        limit: 100,
      });
      expect(
        await fixture.fleetStore.readCleanupReceipt(receipt.operationId),
      ).toBeUndefined();
      const resource = ready.applicationResources?.[0];
      if (!resource) throw new Error('force fixture has no application bucket');
      const bucket = fixture.buckets.get(
        `${resource.jurisdiction}:${resource.bucketName}`,
      );
      if (!bucket) throw new Error('force fixture bucket is missing');
      const originalCreationDate = bucket.creation_date;
      bucket.creation_date = new Date(
        Date.parse(originalCreationDate) + 1000,
      ).toISOString();
      const observed = await fixture.success<{
        observation: Record<string, unknown>;
      }>({ kind: 'force-observe' });
      expect(observed.observation).toMatchObject({
        fleetRecordPresent: false,
        deploymentClaimsPresent: true,
        database: { id: ready.databaseId, observedName: ready.databaseName },
        buckets: [
          {
            bindingName: resource.name,
            expectedCreationDate: originalCreationDate,
            observedCreationDate: bucket.creation_date,
          },
        ],
        priorCleanup: {
          operationId: receipt.operationId,
          observedReceiptSha256: null,
          matchesBefore: false,
        },
      });
      expect(fixture.world.mutationLog).toEqual(mutations);
      bucket.creation_date = originalCreationDate;
      const requests = fixture.projection.requests.length;
      await fixture.reload();
      expect(await fixture.success({ kind: 'force-observe' })).toEqual(
        observed,
      );
      expect(fixture.projection.requests).toHaveLength(requests);
    } finally {
      await fixture.close();
    }
  });
});
