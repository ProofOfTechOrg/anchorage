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

  it('captures before the force phase write and retains the original witness across deletion failure and replay', async () => {
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
