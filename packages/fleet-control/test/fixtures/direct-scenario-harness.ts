// SPDX-License-Identifier: Apache-2.0

import { writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { expect } from 'vitest';
import { createDirectInvocationClient } from '../../scripts/direct-credentialed-invocation.mjs';
import {
  type DirectRunJournal,
  openDirectRunState,
} from '../../scripts/direct-credentialed-run-state.mjs';
import type { DirectScenarioOutcome } from '../../scripts/direct-credentialed-scenario.mjs';
import {
  directBridgePreamble,
  directModuleUrl,
  spawnDirectChild,
} from './direct-cli-child.js';
import { directObservationFixture } from './direct-observations.js';
import { createDirectReferenceHarness } from './direct-reference-harness.js';
import { cleanupDirectRunState } from './direct-run-state-builder.js';

export type DirectScenarioNodeResponse = NonNullable<
  NonNullable<
    Parameters<typeof createDirectReferenceHarness>[0]
  >['nodeResponse']
>;

/**
 * Closers the scenario suites hand back, newest first. The suites push their
 * own observation fixtures onto it, so it lives beside the helpers that
 * create them rather than in each suite.
 */
export const directScenarioCleanup: (() => Promise<void>)[] = [];

export async function closeDirectScenarioFixtures() {
  const results = await Promise.allSettled(
    directScenarioCleanup
      .splice(0)
      .reverse()
      .map((close) => close()),
  );
  await cleanupDirectRunState();
  const failed = results.filter((result) => result.status === 'rejected');
  expect(failed).toEqual([]);
}

export async function createDirectScenarioFixture(
  options: { nodeResponse?: DirectScenarioNodeResponse } = {},
) {
  const local = await directObservationFixture(30_000, 'confirmed', {
    invocationTimeoutMs: 600_000,
    maxProviderRequests: 1000,
  });
  directScenarioCleanup.push(() => local.close());
  const native = await createDirectReferenceHarness({
    manifest: local.prepared.manifest,
    binding: {
      version: 1,
      accountId: 'account',
      fleetDatabaseId: 'fleet-id',
      quotaDatabaseId: 'quota-id',
      exportBucketName: local.prepared.names.exportBucket,
      referenceModuleSetSha256: local.prepared.referenceModuleSetSha256,
      accountWorkersDevSubdomain: 'attested-account',
    },
    maintenanceNow: Date.now,
    applicationProbes: true,
    nodeProviderRest: true,
    ...(options.nodeResponse ? { nodeResponse: options.nodeResponse } : {}),
  });
  directScenarioCleanup.push(() => native.close());
  const input = (journal: DirectRunJournal = local.journal) => ({
    prepared: local.prepared,
    journal,
    apiToken: 'inert-provider-token',
    fetch: native.fetch,
    invocation: createDirectInvocationClient({
      prepared: local.prepared,
      journal,
      accountWorkersDevSubdomain: native.binding.accountWorkersDevSubdomain,
      invokeSecret: 'inert-invoke',
      fetch: native.fetch,
    }),
  });
  return { local, native, input };
}

export type DirectScenarioFixture = Awaited<
  ReturnType<typeof createDirectScenarioFixture>
>;

export async function resumeDirectScenarioJournal(f: DirectScenarioFixture) {
  const resumed = await openDirectRunState({
    configPath: f.local.configPath,
    prepared: f.local.prepared,
    accountId: 'account',
    mode: 'resume',
  });
  directScenarioCleanup.push(() => resumed.close());
  return resumed;
}

/**
 * The journal wrapper a faulted child installs: it records the scenario, then
 * stops the process at the first state matching `predicate`, so the suite can
 * re-enter from a boundary the parent process cannot reach.
 */
const childFault = (
  name: string,
  predicate: string,
) => `const faulted = Object.freeze({
  ...journal,
  recordScenario: async (...args) => {
    const result = await journal.recordScenario(...args);
    const [scenario] = args;
    if (${predicate}) {
      console.log('SCENARIO_FAULT ${name}');
      process.exit(0);
    }
    return result;
  },
});`;

export async function childResumeDirectScenario(
  f: DirectScenarioFixture,
  fault?:
    | 'export-fsync'
    | 'fence-reopen-after-settle'
    | 'fence-drain-role-split',
) {
  await f.local.journal.close();
  const script = join(f.local.directory, 'resume.mjs');
  await writeFile(
    script,
    `
import {preflightDirectConformance} from ${JSON.stringify(directModuleUrl('direct-credentialed-conformance-preflight'))};
import {openDirectRunState} from ${JSON.stringify(directModuleUrl('direct-credentialed-run-state'))};
import {createDirectInvocationClient} from ${JSON.stringify(directModuleUrl('direct-credentialed-invocation'))};
import {runDirectCredentialedScenario} from ${JSON.stringify(directModuleUrl('direct-credentialed-scenario'))};
import fs from 'node:fs/promises';
import {syncBuiltinESMExports} from 'node:module';
${fault === 'export-fsync' ? `const realOpen=fs.open;let injected=false;fs.open=async(...args)=>{const handle=await realOpen(...args);if(String(args[0]).includes('/.journal-')){const realWrite=handle.writeFile.bind(handle),realSync=handle.sync.bind(handle);let proof=false;handle.writeFile=async(value,...rest)=>{const state=JSON.parse(String(value));proof=Boolean(state.scenario?.proofs.exports.a);return realWrite(value,...rest);};handle.sync=async()=>{if(proof&&!injected){injected=true;console.log('SCENARIO_FAULT export-fsync');throw new Error('fixture proof fsync failure');}return realSync();};}return handle;};syncBuiltinESMExports();` : ''}
const prepared=await preflightDirectConformance({configPath:${JSON.stringify(f.local.configPath)}});
const journal=await openDirectRunState({configPath:${JSON.stringify(f.local.configPath)},prepared,accountId:'account',mode:'resume'});
${
  fault === 'fence-reopen-after-settle'
    ? childFault(
        fault,
        `scenario.phase === 'fence-reopen' &&
        scenario.mutation?.outcome === 'returned' &&
        scenario.mutation?.action?.kind === 'tenant-fence' &&
        scenario.mutation?.action?.operation === 'reopen' &&
        scenario.mutation?.action?.role === 'a' &&
        scenario.proofs.fence.reopen.a?.after === null`,
      )
    : fault === 'fence-drain-role-split'
      ? childFault(
          fault,
          `scenario.phase === 'fence-drain' &&
        scenario.proofs.fence.drain.a?.after != null &&
        scenario.proofs.fence.sweeps.a !== null &&
        scenario.proofs.fence.drain.b?.after === null &&
        scenario.proofs.fence.drain.b?.ordinal === null`,
        )
      : 'const faulted = journal;'
}
${directBridgePreamble({
  bridgeUrl: f.native.bridgeUrl,
  workerOrigin: `https://${f.local.prepared.names.referenceWorker}.attested-account.workers.dev`,
})}try {const invocation=createDirectInvocationClient({prepared,journal: faulted,accountWorkersDevSubdomain:'attested-account',invokeSecret:'inert-invoke',fetch});const result=await runDirectCredentialedScenario({prepared,journal: faulted,invocation,apiToken:'inert-provider-token',fetch});console.log('SCENARIO_RESULT '+JSON.stringify(result));}finally{await faulted.close();}
`,
  );
  const { status, stdout, stderr } = await spawnDirectChild([script], {
    timeoutMs: 540_000,
  });
  expect({ status, stderr }).toEqual({ status: 0, stderr: '' });
  const line = stdout
    .split('\n')
    .find((line) => line.startsWith('SCENARIO_RESULT '));
  if (!line) {
    if (
      fault !== 'fence-reopen-after-settle' &&
      fault !== 'fence-drain-role-split'
    )
      throw new Error(`child returned no result: ${stdout}`);
    return { result: null, stdout, stderr };
  }
  return {
    result: JSON.parse(
      line.slice('SCENARIO_RESULT '.length),
    ) as DirectScenarioOutcome,
    stdout,
    stderr,
  };
}
