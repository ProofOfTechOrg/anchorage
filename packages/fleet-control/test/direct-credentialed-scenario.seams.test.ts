// SPDX-License-Identifier: Apache-2.0

import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { runDirectConformance } from '../scripts/direct-credentialed-conformance-runtime.mjs';
import { createDirectInvocationClient } from '../scripts/direct-credentialed-invocation.mjs';
import {
  type DirectRunJournal,
  openDirectRunState,
} from '../scripts/direct-credentialed-run-state.mjs';
import {
  type DirectScenarioState,
  runDirectCredentialedScenario,
} from '../scripts/direct-credentialed-scenario.mjs';
import { DIRECT_SCENARIO_INVOCATION_BUDGET } from '../scripts/direct-credentialed-scenario-budget.mjs';
import { hash } from '../scripts/direct-credentialed-scenario-checks.mjs';
import { DIRECT_TENANT_OBJECT_BODY } from '../scripts/direct-credentialed-tenant-object.mjs';
import { present } from './fixtures/direct-run-state-builder.js';
import {
  childResumeDirectScenario as childResume,
  childResumeDirectRuntimeSweepFault,
  closeDirectScenarioFixtures,
  createDirectScenarioFixture as fixture,
  resumeDirectScenarioJournal as resume,
  seedDirectScenarioReference,
} from './fixtures/direct-scenario-harness.js';

afterEach(closeDirectScenarioFixtures);

const exportVerificationSha256 = (proof: {
  receipt: unknown;
  location: string;
  size: number;
  sha256: string;
}) =>
  hash(
    JSON.stringify({
      receipt: proof.receipt,
      location: proof.location,
      size: proof.size,
      sha256: proof.sha256,
    }),
  );

describe.sequential('fixed Node scenario through native reference dispatch', {
  timeout: 660_000,
}, () => {
  it('process loss after the continuation start witness persist reconstructs without a pre-witness inventory sweep', async () => {
    const f = await fixture({
      maxProviderRequests: 400,
      nativeArtifacts: true,
      nativeTenant: true,
      nativeTenantGeneration: 'reprovision',
    });
    try {
      expect(await runDirectCredentialedScenario(f.input())).toEqual({
        status: 'restart-required',
      });
      await f.native.reload();
      const faulted = await childResume(
        f,
        'continuation-start-witness-persist',
      );
      expect(faulted.result).toBeNull();
      expect(faulted.stdout).toContain(
        'SCENARIO_FAULT continuation-start-witness-persist',
      );
      const saved = JSON.parse(
        await readFile(join(f.local.journal.directory, 'journal.json'), 'utf8'),
      ) as { scenario: DirectScenarioState };
      const witness = saved.scenario.mutation?.witness as
        | { emptyBeforeOrdinal?: number }
        | undefined;
      expect(saved.scenario).toMatchObject({
        phase: 'continuation-start',
        mutation: {
          outcome: 'returned',
          action: { kind: 'tenant-continuation', operation: 'start' },
          witness: { kind: 'start', emptyBeforeOrdinal: expect.any(Number) },
        },
        proofs: { continuation: { started: null } },
      });
      const hostname = f.local.prepared.names.roles.a.routeHostname;
      const inventoryCount = () =>
        f.native.projection.requests.filter((request) => {
          const url = new URL(request.url);
          return (
            url.hostname === hostname && url.pathname === '/admin/inventory'
          );
        }).length;
      const beforeInventory = inventoryCount();
      const resumed = await resume(f);
      let inventoryAtReconstruct: number | undefined;
      const reconstructing: DirectRunJournal = {
        ...resumed,
        recordScenario: async (scenarioState) => {
          await resumed.recordScenario(scenarioState);
          if (
            inventoryAtReconstruct === undefined &&
            scenarioState.proofs.continuation.started !== null
          )
            inventoryAtReconstruct = inventoryCount();
        },
      };
      expect(
        await runDirectCredentialedScenario(f.input(reconstructing)),
      ).toMatchObject({ status: 'complete' });
      const started = present(
        resumed.snapshot().scenario?.proofs.continuation.started,
      ) as { emptyBeforeOrdinal: number; sourceInvocationOrdinal: number };
      expect(started.emptyBeforeOrdinal).toBe(witness?.emptyBeforeOrdinal);
      expect(started.emptyBeforeOrdinal).toBeLessThan(
        started.sourceInvocationOrdinal,
      );
      expect(inventoryAtReconstruct).toBe(beforeInventory);
    } finally {
      await closeDirectScenarioFixtures();
    }
  }, 2_100_000);

  it('reconciles process loss at scenario and sweep reservation windows before native teardown', async () => {
    const f = await fixture({
      maxProviderRequests: 400,
      nativeArtifacts: true,
      nativeTenant: true,
      nativeTenantGeneration: 'reprovision',
    });
    try {
      expect(await runDirectCredentialedScenario(f.input())).toEqual({
        status: 'restart-required',
      });
      await f.native.reload();
      const scenarioFault = await childResume(f, 'invocation-after-dispatch');
      expect(scenarioFault.result).toBeNull();
      expect(scenarioFault.stdout).toContain(
        'SCENARIO_FAULT invocation-after-dispatch',
      );
      const journalPath = join(f.local.journal.directory, 'journal.json');
      const scenarioPending = JSON.parse(await readFile(journalPath, 'utf8'));
      expect(scenarioPending).toMatchObject({
        lastInvocation: {
          state: 'pending',
          action: { kind: 'tenant-continuation', operation: 'start' },
        },
        scenario: {
          phase: 'continuation-start',
          lastCall: { outcome: 'prepared' },
          failure: null,
        },
      });
      expect(
        await f.native.fleetStore.get(
          f.local.prepared.names.roles.a.tenantTag,
          f.local.prepared.config.environment,
        ),
      ).toMatchObject({ phase: 'ready' });
      expect(
        await f.native.fleetStore.get(
          f.local.prepared.names.roles.recovery.tenantTag,
          f.local.prepared.config.environment,
        ),
      ).toMatchObject({ phase: 'ready' });
      expect(f.tenant?.generation).toBeDefined();

      seedDirectScenarioReference(f);
      await f.native.reload();
      await childResumeDirectRuntimeSweepFault(f);
      const sweepPending = JSON.parse(await readFile(journalPath, 'utf8'));
      expect(sweepPending).toMatchObject({
        lastInvocation: {
          state: 'pending',
          action: { kind: 'control-read' },
        },
        reconciliations: [
          {
            ordinal: scenarioPending.lastInvocation.ordinal,
            state: 'executed',
          },
        ],
        scenario: {
          failure: {
            code: 'proof-unavailable',
            detail: 'lost-run-id-abandoned',
          },
        },
        sweep: {
          phase: 'sweeping',
          lastCall: { action: 'control-read', outcome: 'prepared' },
        },
      });

      await f.native.reload();
      const result = await runDirectConformance({
        mode: 'resume',
        configPath: f.local.configPath,
        env: {
          CLOUDFLARE_ACCOUNT_ID: 'account',
          CLOUDFLARE_API_TOKEN: 'inert-provider-token',
          FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET: 'inert-invoke',
        },
        fetch: f.native.fetch,
        modules: {
          distPresent: () => true,
          bootstrap: async ({ prepared, journal }) =>
            createDirectInvocationClient({
              prepared,
              journal,
              accountWorkersDevSubdomain: 'attested-account',
              invokeSecret: 'inert-invoke',
              fetch: f.native.fetch,
            }),
        },
        now: () => Date.parse('2026-09-14T00:00:00.000Z'),
        git: () => 'a'.repeat(40),
      });
      const settled = await openDirectRunState({
        configPath: f.local.configPath,
        prepared: f.local.prepared,
        accountId: 'account',
        mode: 'resume',
      });
      const snapshot = settled.snapshot();
      await settled.close();
      process.stdout.write(
        `FIX3B_NATIVE_RECONCILIATION ${JSON.stringify({
          scenarioLossOrdinal: scenarioPending.lastInvocation.ordinal,
          sweepLossOrdinal: sweepPending.lastInvocation.ordinal,
          finalInvocationCount: snapshot.invocationCount,
          sweepInvocations:
            snapshot.invocationCount - sweepPending.lastInvocation.ordinal,
        })}\n`,
      );
      expect(
        result,
        JSON.stringify({
          result: result.summary,
          sweep: snapshot.sweep,
          teardown: snapshot.teardown,
        }),
      ).toMatchObject({
        exitCode: 1,
        summary: {
          status: 'failed',
          teardownCall: { status: 'cleaned', failure: null },
        },
      });
      expect(
        snapshot.reconciliations.slice(-2).map(({ state }) => state),
      ).toEqual(['executed', 'executed']);
      expect(snapshot).toMatchObject({
        sweep: {
          phase: 'complete',
          roles: {
            a: { kind: 'completed', after: 'decommissioned' },
            recovery: { kind: 'completed', after: 'absent' },
          },
        },
        teardown: { phase: 'complete', failure: null },
      });
      const prefix = f.local.prepared.config.resourcePrefix;
      expect(
        [...f.native.world.scripts.entries()].filter(
          ([name, script]) => script.present && name.startsWith(prefix),
        ),
      ).toEqual([]);
      expect(
        f.native.world.databases.filter(({ name }) => name.startsWith(prefix)),
      ).toEqual([]);
      expect(
        f.native.world.durableObjectNamespaces.filter(({ script }) =>
          script.startsWith(prefix),
        ),
      ).toEqual([]);
      expect(
        f.native.world.customDomains.filter(({ service }) =>
          service.startsWith(prefix),
        ),
      ).toEqual([]);
      expect(
        f.native.world.routes.filter(({ script }) => script.startsWith(prefix)),
      ).toEqual([]);
      expect(
        [...f.native.buckets.keys()].filter((name) => name.includes(prefix)),
      ).toEqual([]);
      expect(f.tenant?.generation).toBeUndefined();
      expect(f.native.bridgeErrors).toEqual([]);
    } finally {
      await closeDirectScenarioFixtures();
    }
  }, 2_100_000);

  it('crash after decommission export persist before continue preserves one reference and advances the proof ordinal', async () => {
    const f = await fixture({
      maxProviderRequests: 400,
      nativeArtifacts: true,
      nativeTenant: true,
      nativeTenantGeneration: 'reprovision',
    });
    try {
      console.log('LV2_FULL_STAGE fixture-ready');
      const first = await runDirectCredentialedScenario(f.input());
      console.log(
        'LV2_FULL_STAGE first-finished',
        JSON.stringify({
          first,
          phase: f.local.journal.snapshot().scenario?.phase,
        }),
      );
      expect({
        first,
        bridgeErrors: f.native.bridgeErrors,
        state: f.local.journal.snapshot().scenario,
      }).toMatchObject({
        first: { status: 'restart-required' },
        bridgeErrors: [],
      });
      const initial = f.local.journal.snapshot();
      const count = initial.invocationCount;
      expect(await runDirectCredentialedScenario(f.input())).toEqual({
        status: 'restart-required',
      });
      expect(f.local.journal.snapshot().invocationCount).toBe(count);
      const witness = await f.native.journal().readInterruption();
      expect(witness).not.toBeNull();
      const stale = JSON.parse(JSON.parse(witness as string).claimJson);
      const wrong = await f.native.call({
        kind: 'migration-continue',
        token: { ...stale, operationId: 'foreign-operation' },
      });
      expect(wrong.response.status).toBe(409);
      expect(wrong.response.headers.get('X-Direct-Provider-Attempts')).toBe(
        '0',
      );
      await f.native.reload();
      console.log('LV2_FULL_STAGE child-start');
      const faulted = await childResume(
        f,
        'decommission-reprovision-export-persist',
      );
      expect(faulted.result).toBeNull();
      expect(faulted.stdout).toContain(
        'SCENARIO_FAULT decommission-reprovision-export-persist',
      );
      const persistedAtSeam = JSON.parse(
        await readFile(join(f.local.journal.directory, 'journal.json'), 'utf8'),
      );
      expect(
        persistedAtSeam.scenario.proofs.exportVerifications.filter(
          (entry: { role: string; cycle: string | null }) =>
            entry.role === 'a' && entry.cycle === 'reprovision',
        ),
      ).toHaveLength(1);
      const persistedReference = present(
        persistedAtSeam.scenario.proofs.exportVerifications.find(
          (entry: { role: string; cycle: string | null }) =>
            entry.role === 'a' && entry.cycle === 'reprovision',
        ),
      );
      const persistedProofOrdinal =
        persistedAtSeam.scenario.proofs.reprovisionExports.a
          .sourceInvocationOrdinal;
      const child = await childResume(f);
      console.log(
        'LV2_FULL_STAGE child-finished',
        JSON.stringify({ status: child.result?.status ?? null }),
      );
      expect({
        result: child.result,
        bridgeErrors: f.native.bridgeErrors,
        journal: await readFile(
          join(f.local.journal.directory, 'journal.json'),
          'utf8',
        ),
      }).toMatchObject({ result: { status: 'complete' }, bridgeErrors: [] });
      if (child.result?.status !== 'complete')
        throw new Error('scenario did not complete');
      const proofs = child.result.facts;
      expect(proofs.restart?.process.pid).toBe(process.pid);
      expect(proofs.restart?.resumedProcess?.pid).not.toBe(process.pid);
      for (const role of ['a', 'b'] as const) {
        const fence = proofs.fence;
        expect(fence.drain[role]).toMatchObject({
          before: {
            state: 'open',
            mutationEpoch: 0,
            requireMutationEpoch: false,
          },
          after: {
            state: 'draining',
            mutationEpoch: 1,
            requireMutationEpoch: true,
          },
          ordinal: expect.any(Number),
        });
        expect(fence.reopen[role]).toMatchObject({
          before: {
            state: 'draining',
            mutationEpoch: 1,
            requireMutationEpoch: true,
          },
          after: {
            state: 'open',
            mutationEpoch: 1,
            requireMutationEpoch: true,
          },
          ordinal: expect.any(Number),
        });
        expect(fence.sweeps[role]).toMatchObject({
          first: { fence: { state: 'draining' }, ordinal: expect.any(Number) },
          second: { fence: { state: 'draining' }, ordinal: expect.any(Number) },
          intervalMs: expect.any(Number),
        });
        for (const sweep of [
          fence.sweeps[role]?.first,
          fence.sweeps[role]?.second,
        ]) {
          expect(sweep).not.toBeNull();
          expect(
            sweep?.categories.every(
              (entry) => entry.class !== 'work' || entry.empty,
            ),
          ).toBe(true);
        }
        expect(fence.probes[role]).toEqual({
          current: 'accepted',
          missing: 'missing',
          stale: 'stale',
          future: 'future',
          mutationEpoch: 1,
          ordinal: expect.any(Number),
        });
        expect(proofs.initial[role]?.trafficPercentage).toBe(100);
        expect(proofs.candidate[role]?.trafficPercentage).toBe(0);
        expect(proofs.final[role]?.trafficPercentage).toBe(100);
        expect(proofs.exports[role]?.verified).toBe(true);
        expect(proofs.decommission[role]?.phase).toBe('decommissioned');
        const history = proofs.exportVerifications.filter(
          (entry) => entry.role === role && entry.cycle === null,
        );
        expect(history.length).toBeGreaterThan(0);
        expect(history.at(-1)?.exportSha256).toBe(
          exportVerificationSha256(present(proofs.exports[role])),
        );
      }
      expect(proofs.terminalForce.a).toEqual({
        databaseId: proofs.decommission.a?.databaseId,
        scriptName: proofs.decommission.a?.scriptName,
        ordinal: expect.any(Number),
        attempts: { provider: 0, maintenance: 0, application: 0 },
      });
      for (const role of ['a', 'b'] as const) {
        const identity = present(proofs.identities[role]);
        expect(identity).toMatchObject({
          before: 'initial',
          after: 'final',
          routeHostname: f.local.prepared.names.roles[role].routeHostname,
          evidenceSha256: expect.stringMatching(/^[a-f0-9]{64}$/u),
        });
      }
      const replacementA = present(proofs.reprovision.a);
      const replacementB = present(proofs.reprovisionFinal.a);
      expect(replacementA.versionId).not.toBe(replacementB.versionId);
      expect(replacementA).toMatchObject({
        databaseId: replacementB.databaseId,
        scriptName: replacementB.scriptName,
        routeHostname: replacementB.routeHostname,
        namespaces: replacementB.namespaces,
        applicationRelease: '1',
        schemaVersion: 1,
        trafficPercentage: 100,
      });
      expect(replacementB).toMatchObject({
        applicationRelease: '2',
        schemaVersion: 2,
        trafficPercentage: 100,
      });
      const continuation = proofs.continuation;
      expect(continuation.started?.runId).toBe(continuation.refused?.runId);
      expect(continuation.started?.runId).toBe(continuation.finished?.runId);
      expect(continuation.started?.challengeSha256).toBe(
        continuation.finished?.challengeSha256,
      );
      expect(continuation.locked).toMatchObject({
        before: { state: 'open' },
        after: { state: 'migration-locked' },
      });
      expect(continuation.refused).toMatchObject({
        status: 503,
        code: 'EXECUTION_FENCED',
        state: 'migration-locked',
      });
      expect(continuation.reopened).toMatchObject({
        before: { state: 'migration-locked' },
        after: { state: 'open' },
      });
      expect(continuation.finished).toMatchObject({
        status: 'success',
        release: '2',
      });
      expect(proofs.reprovisionSettlement.a).toMatchObject({
        databaseId: replacementB.databaseId,
        scriptName: replacementB.scriptName,
        versionId: replacementB.versionId,
        specDigest: replacementB.specDigest,
        settlementKey: expect.stringMatching(/^[a-f0-9]{64}$/u),
      });
      expect(proofs.reprovisionExports.a?.verified).toBe(true);
      expect(proofs.redecommission.a?.phase).toBe('decommissioned');
      const replacementHistory = proofs.exportVerifications.filter(
        ({ role, cycle }) => role === 'a' && cycle === 'reprovision',
      );
      expect(replacementHistory).toEqual([persistedReference]);
      expect(
        present(proofs.reprovisionExports.a).sourceInvocationOrdinal,
      ).toBeGreaterThan(persistedProofOrdinal);
      expect(replacementHistory[0]?.exportSha256).toBe(
        exportVerificationSha256(present(proofs.reprovisionExports.a)),
      );
      expect(proofs.inventories.after?.routeHostnames).toEqual(
        proofs.inventories.before?.routeHostnames,
      );
      expect(proofs.inventories.after?.routeHostnames).toEqual(
        (['a', 'b'] as const)
          .map((role) => f.local.prepared.names.roles[role].routeHostname)
          .sort(),
      );
      expect(proofs.effects).toHaveLength(2);
      expect(proofs.inventories.before?.calls).toBeGreaterThan(1);
      expect(proofs.inventories.after?.generation).toBeGreaterThan(
        proofs.inventories.before?.generation ?? 0,
      );
      expect(proofs.audits.before?.findings).toEqual([]);
      expect(proofs.audits.after?.findings).toEqual([]);
      expect(proofs.force?.worker.scriptPresent).toBe(true);
      expect(proofs.residual?.worker.scriptPresent).toBe(false);
      expect(proofs.force?.priorCleanup.operationId).toBe(
        proofs.cleanup?.operationId,
      );
      expect(proofs.residual?.priorCleanup).toEqual(proofs.force?.priorCleanup);
      expect(f.native.world.databases).toEqual([]);
      expect(f.native.buckets.size).toBe(0);
      expect(
        (await f.native.exportBytes.list()).objects.length,
      ).toBeGreaterThan(0);
      const serialized = await readFile(
        join(f.local.journal.directory, 'journal.json'),
        'utf8',
      );
      for (const sentinel of [
        'APP_PROBE_TOKEN',
        'inert-provider-token',
        'inert-invoke',
        'claimJson',
        'tokenJson',
        'SELECT ',
        'CREATE TABLE ',
        'INSERT INTO ',
        DIRECT_TENANT_OBJECT_BODY,
      ])
        expect(serialized).not.toContain(sentinel);
      const budget: Record<string, { ceiling: number }> =
        DIRECT_SCENARIO_INVOCATION_BUDGET;
      const phaseCalls: Record<string, number> =
        JSON.parse(serialized).scenario.phaseCalls;
      for (const phase of ['fence-drain', 'fence-reopen', 'fence-proofs'])
        expect(phaseCalls[phase]).toBe(9);
      expect(phaseCalls).toMatchObject({
        'reprovision-a': 6,
        'continuation-start': 4,
        'continuation-lock': 4,
        'continuation-migrate': 4,
        'continuation-refuse': 5,
        'continuation-finish': 7,
      });
      expect(phaseCalls['decommission-reprovisioned-a']).toBeGreaterThan(70);
      console.log(
        'LV2_PHASE_CALLS',
        JSON.stringify(
          Object.fromEntries(
            Object.entries(phaseCalls).filter(
              ([phase]) =>
                phase.includes('continuation') || phase.includes('reprovision'),
            ),
          ),
        ),
      );
      expect(
        Object.entries(phaseCalls).filter(
          ([phase, calls]) => calls > (budget[phase]?.ceiling ?? 0),
        ),
      ).toEqual([]);
      expect(serialized).not.toContain(JSON.parse(witness as string).claimJson);
      const resumed = await resume(f);
      expect(
        (await runDirectCredentialedScenario(f.input(resumed))).status,
      ).toBe('complete');
    } finally {
      await closeDirectScenarioFixtures();
    }
  }, 2_100_000);
});
