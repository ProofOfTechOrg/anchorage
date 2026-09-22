// SPDX-License-Identifier: Apache-2.0

import { writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { expect } from 'vitest';
import { createDirectInvocationClient } from '../../scripts/direct-credentialed-invocation.mjs';
import { REFERENCE_SECRET_NAMES } from '../../scripts/direct-credentialed-reference-vocabulary.mjs';
import {
  type DirectRunJournal,
  openDirectRunState,
} from '../../scripts/direct-credentialed-run-state.mjs';
import type { DirectScenarioOutcome } from '../../scripts/direct-credentialed-scenario.mjs';
import { directDeploymentSpec } from '../../scripts/direct-credentialed-spec.js';
import {
  directBridgePreamble,
  directModuleUrl,
  spawnDirectChild,
} from './direct-cli-child.js';
import {
  createDirectNativeTenant,
  type DirectNativeTenant,
} from './direct-native-tenant.js';
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
  options: {
    nodeResponse?: DirectScenarioNodeResponse;
    maxProviderRequests?: number;
    nativeArtifacts?: boolean;
    nativeTenant?: boolean;
    nativeTenantGeneration?: 'first' | 'reprovision';
  } = {},
) {
  const local = await directObservationFixture(30_000, 'confirmed', {
    invocationTimeoutMs: 600_000,
    maxProviderRequests: options.maxProviderRequests ?? 1000,
    nativeArtifacts: options.nativeArtifacts ?? false,
  });
  directScenarioCleanup.push(() => local.close());
  let tenant: DirectNativeTenant | undefined;
  if (options.nativeTenant) {
    tenant = await createDirectNativeTenant(local.prepared.manifest);
    directScenarioCleanup.push(() => tenant?.close() ?? Promise.resolve());
  }
  let nativeReference:
    | Awaited<ReturnType<typeof createDirectReferenceHarness>>
    | undefined;
  let firstRoleADatabaseId: string | undefined;
  const nativeGeneration = options.nativeTenantGeneration ?? 'first';
  const nativeDatabase = (selectedDatabaseId: string) => {
    const database = nativeReference?.world.databases.find(
      ({ databaseId }) => databaseId === selectedDatabaseId,
    );
    if (database?.name !== local.prepared.names.roles.a.databaseName)
      return false;
    firstRoleADatabaseId ??= selectedDatabaseId;
    return (
      nativeGeneration === 'first' ||
      selectedDatabaseId !== firstRoleADatabaseId
    );
  };
  const bindingText = (bindings: readonly unknown[], name: string) => {
    const binding = bindings.find(
      (candidate) =>
        candidate &&
        typeof candidate === 'object' &&
        Reflect.get(candidate, 'name') === name,
    );
    const value =
      binding && typeof binding === 'object'
        ? (Reflect.get(binding, 'text') ??
          Reflect.get(binding, 'database_id') ??
          Reflect.get(binding, 'namespace_id') ??
          Reflect.get(binding, 'bucket_name'))
        : undefined;
    if (typeof value !== 'string' || !value)
      throw new Error(`native tenant binding '${name}' is missing`);
    return value;
  };
  const syncNativeTenant = async () => {
    if (!tenant || !nativeReference) return;
    const spec = nativeReference.specs.find(
      (candidate) =>
        candidate.tenantTag === local.prepared.names.roles.a.tenantTag,
    );
    if (!spec) throw new Error('native tenant role-a spec is missing');
    const script = nativeReference.world.scripts.get(spec.scriptName);
    const activeId = script?.deployment?.find(
      ({ percentage }) => percentage === 100,
    )?.versionId;
    const version = script?.versions.find(
      ({ versionId }) => versionId === activeId,
    );
    if (!script?.present || !version) {
      tenant.deactivate();
      return;
    }
    const versionDatabaseId = bindingText(version.bindings, 'DB');
    if (!nativeDatabase(versionDatabaseId)) return;
    if (tenant.generation?.versionId === version.versionId) return;
    const release = bindingText(version.bindings, 'APPLICATION_RELEASE');
    if (release !== '1' && release !== '2')
      throw new Error('native tenant release is invalid');
    const selectedSpec = nativeReference.specs.find(
      (candidate) => candidate.tenantTag === spec.tenantTag,
    );
    const nextSpec =
      release === '1'
        ? selectedSpec
        : directDeploymentSpec(
            local.prepared.manifest,
            'a',
            'next',
            nativeReference.secrets.a,
            nativeReference.binding,
          );
    if (!nextSpec) throw new Error('native tenant deployment spec is missing');
    await tenant.activate({
      spec: nextSpec,
      versionId: version.versionId,
      databaseId: versionDatabaseId,
      maintenanceNamespaceId: bindingText(version.bindings, 'MAINTENANCE'),
      runnerNamespaceId: bindingText(version.bindings, 'RUNNER'),
      deploymentIdentitySecret: nativeReference.secrets.a.deploymentIdentity,
      maintenanceAdminSecret: nativeReference.secrets.a.maintenanceAdmin,
      applicationToken:
        nativeReference.secrets.a.application?.APP_PROBE_TOKEN ?? '',
      release,
      bucketName: bindingText(version.bindings, 'PROBE_BUCKET'),
    });
  };
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
    ...(tenant
      ? {
          applicationFetch: tenant.fetch,
          applicationFetchRole: 'a',
          applicationFetchActive: () => tenant?.generation !== undefined,
          providerRequest: async (request) => {
            const match = new URL(request.url).pathname.match(
              /\/d1\/database\/([^/]+)\/query$/u,
            );
            if (match?.[1] === 'fleet-id' || match?.[1] === 'quota-id')
              return undefined;
            if (match?.[1] && !nativeDatabase(match[1])) return undefined;
            return tenant?.providerRequest(request);
          },
          providerResponse: async (_request, response) => {
            if (response.ok) await syncNativeTenant();
            return response;
          },
        }
      : {}),
    ...(options.nodeResponse ? { nodeResponse: options.nodeResponse } : {}),
  });
  nativeReference = native;
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
  return { local, native, tenant, input };
}

export type DirectScenarioFixture = Awaited<
  ReturnType<typeof createDirectScenarioFixture>
>;

export function seedDirectScenarioReference(f: DirectScenarioFixture) {
  const bootstrap = f.local.journal.snapshot().bootstrap;
  if (
    !bootstrap?.fleet ||
    !bootstrap.quota ||
    !bootstrap.exports ||
    !bootstrap.active
  )
    throw new Error('confirmed reference bootstrap is absent');
  if (!f.native.world.zones.some(({ id }) => id === bootstrap.context.zoneId))
    f.native.world.zones.push({
      id: bootstrap.context.zoneId,
      name: bootstrap.context.zoneName,
    });
  for (const receipt of [bootstrap.fleet, bootstrap.quota])
    if (
      !f.native.world.databases.some(
        ({ databaseId }) => databaseId === receipt.uuid,
      )
    )
      f.native.world.seedDatabase(receipt.name, { databaseId: receipt.uuid });
  f.native.buckets.set(
    `${bootstrap.exports.jurisdiction}:${bootstrap.exports.name}`,
    {
      name: bootstrap.exports.name,
      jurisdiction: bootstrap.exports.jurisdiction,
      creation_date: bootstrap.exports.creationDate,
    },
  );
  const bindings = [
    { name: 'FLEET_DB', type: 'd1', database_id: bootstrap.fleet.uuid },
    { name: 'QUOTA_DB', type: 'd1', database_id: bootstrap.quota.uuid },
    {
      name: 'EXPORTS',
      type: 'r2_bucket',
      bucket_name: bootstrap.exports.name,
    },
    {
      name: 'DIRECT_RUN_BINDING',
      type: 'plain_text',
      text: JSON.stringify(f.native.binding),
    },
    ...REFERENCE_SECRET_NAMES.map((name) => ({ name, type: 'secret_text' })),
  ];
  f.native.world.seedScript(f.local.prepared.names.referenceWorker, {
    versions: [
      {
        versionId: bootstrap.active.versionId,
        tag: undefined,
        bindings,
        mainModule: 'worker.js',
        modules: [],
      },
    ],
    deployment: [{ versionId: bootstrap.active.versionId, percentage: 100 }],
    deploymentId: bootstrap.active.deploymentId,
    subdomain: { enabled: true, previewsEnabled: false },
  });
  const runtime = f.local.prepared.config.referenceWorker;
  f.native.versionRuntime.set(bootstrap.active.versionId, {
    compatibility_date: runtime.compatibilityDate,
    compatibility_flags: runtime.compatibilityFlags,
    limits: {
      cpu_ms: runtime.cpuLimitMs,
      subrequests: runtime.subrequestLimit,
    },
  });
}

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
    | 'fence-drain-role-split'
    | 'decommission-reprovision-export-persist'
    | 'continuation-start-witness-persist'
    | 'invocation-after-dispatch',
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
      : fault === 'decommission-reprovision-export-persist'
        ? childFault(
            fault,
            `scenario.phase === 'decommission-reprovisioned-a' &&
        scenario.lastCall?.action?.kind === 'decommission-export' &&
        scenario.proofs.reprovisionExports.a !== null &&
        scenario.proofs.redecommission.a === null &&
        scenario.proofs.exportVerifications.filter((entry) =>
          entry.role === 'a' && entry.cycle === 'reprovision'
        ).length === 1`,
          )
        : fault === 'continuation-start-witness-persist'
          ? childFault(
              fault,
              `scenario.phase === 'continuation-start' &&
        scenario.mutation?.action?.kind === 'tenant-continuation' &&
        scenario.mutation?.action?.operation === 'start' &&
        scenario.mutation?.outcome === 'returned' &&
        scenario.proofs.continuation.started === null`,
            )
          : `let tracedPhase,tracedCalls=-1;const faulted=Object.freeze({...journal,recordScenario:async(...args)=>{const result=await journal.recordScenario(...args);const [scenario]=args;if(scenario.phase!==tracedPhase||scenario.callCount-tracedCalls>=25){tracedPhase=scenario.phase;tracedCalls=scenario.callCount;console.log('SCENARIO_PHASE '+scenario.phase+' calls='+String(scenario.callCount));}return result;}});`
}

${directBridgePreamble({
  bridgeUrl: f.native.bridgeUrl,
  workerOrigin: `https://${f.local.prepared.names.referenceWorker}.attested-account.workers.dev`,
})}${fault === 'invocation-after-dispatch' ? `const invocationFetch=async(input,init)=>{const request=new Request(input,init),copy=request.clone();const response=await fetch(request);let action;try{action=(await copy.json()).action;}catch{}if(action?.kind==='tenant-continuation'&&action?.operation==='start'){console.log('SCENARIO_FAULT invocation-after-dispatch');process.exit(0);}return response;};` : 'const invocationFetch=fetch;'}try {const invocation=createDirectInvocationClient({prepared,journal: faulted,accountWorkersDevSubdomain:'attested-account',invokeSecret:'inert-invoke',fetch:invocationFetch});const result=await runDirectCredentialedScenario({prepared,journal: faulted,invocation,apiToken:'inert-provider-token',fetch:invocationFetch});console.log('SCENARIO_RESULT '+JSON.stringify(result));}finally{await faulted.close();}
`,
  );
  const { status, stdout, stderr } = await spawnDirectChild([script], {
    timeoutMs: 1_500_000,
  });
  if (status !== 0 || stderr !== '') {
    const phase = stdout
      .split('\n')
      .filter((phaseLine) => phaseLine.startsWith('SCENARIO_PHASE '))
      .at(-1);
    throw new Error(
      `child scenario failed with status ${String(status)} at ${phase ?? 'unknown phase'}: ${stderr}`,
    );
  }
  const line = stdout
    .split('\n')
    .find((outputLine) => outputLine.startsWith('SCENARIO_RESULT '));
  if (!line) {
    if (
      fault !== 'fence-reopen-after-settle' &&
      fault !== 'fence-drain-role-split' &&
      fault !== 'decommission-reprovision-export-persist' &&
      fault !== 'continuation-start-witness-persist' &&
      fault !== 'invocation-after-dispatch'
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

export async function childResumeDirectRuntimeSweepFault(
  f: DirectScenarioFixture,
) {
  const script = join(f.local.directory, 'resume-runtime.mjs');
  await writeFile(
    script,
    `
import {runDirectConformance} from ${JSON.stringify(directModuleUrl('direct-credentialed-conformance-runtime'))};
import {createDirectInvocationClient} from ${JSON.stringify(directModuleUrl('direct-credentialed-invocation'))};
${directBridgePreamble({
  bridgeUrl: f.native.bridgeUrl,
  workerOrigin: `https://${f.local.prepared.names.referenceWorker}.attested-account.workers.dev`,
})}
const faultFetch=async(input,init)=>{const request=new Request(input,init),copy=request.clone();const response=await fetch(request);let action;try{action=(await copy.json()).action;}catch{}if(action?.kind==='control-read'){console.log('SCENARIO_FAULT sweep-invocation-after-dispatch');process.exit(0);}return response;};
const result=await runDirectConformance({mode:'resume',configPath:${JSON.stringify(f.local.configPath)},env:{CLOUDFLARE_ACCOUNT_ID:'account',CLOUDFLARE_API_TOKEN:'inert-provider-token',FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET:'inert-invoke'},fetch:faultFetch,modules:{distPresent:()=>true,bootstrap:async({prepared,journal})=>createDirectInvocationClient({prepared,journal,accountWorkersDevSubdomain:'attested-account',invokeSecret:'inert-invoke',fetch:faultFetch})}});
console.log('SCENARIO_RESULT '+JSON.stringify(result));
process.exitCode=result.exitCode;
`,
  );
  const child = await spawnDirectChild([script], { timeoutMs: 1_500_000 });
  if (
    child.status !== 0 ||
    child.stderr !== '' ||
    !child.stdout.includes('SCENARIO_FAULT sweep-invocation-after-dispatch')
  )
    throw new Error(
      `child runtime sweep fault failed with status ${String(child.status)}: ${child.stderr}\n${child.stdout}`,
    );
  return child;
}
