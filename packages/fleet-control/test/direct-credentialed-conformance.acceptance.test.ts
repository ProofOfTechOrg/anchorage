// SPDX-License-Identifier: Apache-2.0

import { existsSync } from 'node:fs';
import { readdir, readFile, stat, unlink, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterEach, beforeAll, describe, expect, it } from 'vitest';
import {
  DIRECT_CONFORMANCE_EXIT_CODES,
  type runDirectConformance,
} from '../scripts/direct-credentialed-conformance-runtime.mjs';
import {
  DIRECT_EVIDENCE_LITERALS,
  inspectDirectEvidence,
} from '../scripts/direct-credentialed-evidence.mjs';
import {
  DIRECT_RESIDUAL_SURFACES,
  REFERENCE_SECRET_NAMES,
} from '../scripts/direct-credentialed-reference-vocabulary.mjs';
import { openDirectRunState } from '../scripts/direct-credentialed-run-state.mjs';
import { expectBuiltDist } from './fixtures/built-dist.js';
import {
  directBridgePreamble,
  directModuleUrl,
  spawnDirectChild,
} from './fixtures/direct-cli-child.js';
import { directObservationFixture } from './fixtures/direct-observations.js';
import { createDirectReferenceHarness } from './fixtures/direct-reference-harness.js';
import {
  cleanupDirectRunState,
  fixture as directRunStateFixture,
} from './fixtures/direct-run-state-builder.js';

const cleanup: Array<() => Promise<void>> = [];
const apiToken = 'inert-provider-token';
const invokeSecret = 'inert-invoke';
const SUITE_TIMEOUT_MS = 900_000;
// The child is killed a minute before the suite times out, so a hung run is
// reported with the child's own output rather than as a suite timeout.
const CHILD_TIMEOUT_MS = SUITE_TIMEOUT_MS - 60_000;
const REFERENCE_REQUEST_TIMEOUT_MS = 30_000;
const INVOCATION_TIMEOUT_MS = 600_000;
const PROVIDER_REQUEST_BUDGET = 1000;

afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close();
  await cleanupDirectRunState();
});

const childProcess = (args: string[], env: NodeJS.ProcessEnv = {}) =>
  spawnDirectChild(args, { env, timeoutMs: CHILD_TIMEOUT_MS });

async function fixture() {
  const local = await directObservationFixture(
    REFERENCE_REQUEST_TIMEOUT_MS,
    'confirmed',
    {
      invocationTimeoutMs: INVOCATION_TIMEOUT_MS,
      maxProviderRequests: PROVIDER_REQUEST_BUDGET,
    },
  );
  cleanup.push(() => local.close());
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
  });
  cleanup.push(() => native.close());
  const bootstrap = local.journal.snapshot().bootstrap;
  if (
    !bootstrap?.fleet ||
    !bootstrap.quota ||
    !bootstrap.exports ||
    !bootstrap.active
  )
    throw new Error('confirmed fixture receipts absent');
  native.world.zones.push({
    id: 'zone',
    name: local.prepared.config.ownedHostname,
  });
  const exportedDatabases = [bootstrap.fleet, bootstrap.quota];
  for (const receipt of exportedDatabases)
    native.world.seedDatabase(receipt.name, { databaseId: receipt.uuid });
  native.buckets.set(
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
    { name: 'EXPORTS', type: 'r2_bucket', bucket_name: bootstrap.exports.name },
    {
      name: 'DIRECT_RUN_BINDING',
      type: 'plain_text',
      text: JSON.stringify(native.binding),
    },
    ...REFERENCE_SECRET_NAMES.map((name) => ({ name, type: 'secret_text' })),
  ];
  native.world.seedScript(local.prepared.names.referenceWorker, {
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
  const runtime = local.prepared.config.referenceWorker;
  native.versionRuntime.set(bootstrap.active.versionId, {
    compatibility_date: runtime.compatibilityDate,
    compatibility_flags: runtime.compatibilityFlags,
    limits: {
      cpu_ms: runtime.cpuLimitMs,
      subrequests: runtime.subrequestLimit,
    },
  });
  await local.journal.close();
  return {
    local,
    native,
    exportedDatabaseCount: exportedDatabases.length,
    seededSecretCount: bindings.filter(
      (binding) => binding.type === 'secret_text',
    ).length,
  };
}

async function drive(
  f: Awaited<ReturnType<typeof fixture>>,
  mode: 'run' | 'resume',
) {
  const script = join(f.local.directory, 'runtime.mjs');
  await writeFile(
    script,
    `
import { runDirectConformance } from ${JSON.stringify(directModuleUrl('direct-credentialed-conformance-runtime'))};
${directBridgePreamble({
  bridgeUrl: f.native.bridgeUrl,
  workerOrigin: `https://${f.local.prepared.names.referenceWorker}.attested-account.workers.dev`,
  countRequests: true,
})}const refusals = {
  origin: await fetch('https://forbidden.test/probe').then(
    () => null,
    (error) => error.message,
  ),
  closed: await globalThis.fetch('https://api.cloudflare.com/probe').then(
    () => null,
    (error) => error.message,
  ),
};
const result = await runDirectConformance({
  mode: ${JSON.stringify(mode)}, configPath: ${JSON.stringify(f.local.configPath)},
  env: { CLOUDFLARE_ACCOUNT_ID: 'account', CLOUDFLARE_API_TOKEN: ${JSON.stringify(apiToken)}, FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET: ${JSON.stringify(invokeSecret)} }, fetch,
});
console.log('CLI_RESULT ' + JSON.stringify({ result, requests, refusals }));
process.exitCode = result.exitCode;
`,
  );
  const child = await childProcess([script]);
  expect(child.stderr).toBe('');
  const lines = child.stdout
    .split('\n')
    .filter((line) => line.startsWith('CLI_RESULT '));
  expect(lines, child.stdout).toHaveLength(1);
  const parsed = JSON.parse(lines[0]?.slice('CLI_RESULT '.length) ?? '') as {
    result: Awaited<ReturnType<typeof runDirectConformance>>;
    requests: number;
    refusals: Record<string, string | null>;
  };
  expect(parsed.refusals).toEqual({
    origin: 'unexpected child origin',
    closed: 'unexpected child network',
  });
  expect(Object.values(DIRECT_CONFORMANCE_EXIT_CODES)).toContain(child.status);
  return { ...parsed, status: child.status };
}

describe.sequential('direct credentialed CLI native offline acceptance', {
  timeout: SUITE_TIMEOUT_MS,
}, () => {
  beforeAll(() => {
    expectBuiltDist(
      '../dist/index.js',
      import.meta.url,
      'run pnpm --filter @proofoftech/fleet-control build before acceptance',
    );
  });

  it('revalidates, restarts, cleans, rereads evidence, and refuses existing runs and concurrent resumes', async () => {
    const f = await fixture();
    const diagnostic = (step: Awaited<ReturnType<typeof drive>>) =>
      JSON.stringify({
        bridgeErrors: f.native.bridgeErrors,
        requests: f.native.projection.requests
          .slice(-12)
          .map(({ method, url }) => ({ method, url })),
        result: step.result.summary,
      });
    const sentinels = {
      secrets: [apiToken, invokeSecret],
      literals: DIRECT_EVIDENCE_LITERALS,
    };
    const first = await drive(f, 'resume');
    expect(first.result, diagnostic(first)).toMatchObject({ exitCode: 3 });
    // The run reports where it published; the layout is the runtime's, not a
    // second derivation here.
    const evidencePath = first.result.evidencePath as string;
    const directory = dirname(evidencePath);
    const evidence = async () =>
      JSON.parse(await readFile(evidencePath, 'utf8'));
    const interrupted = await evidence();
    expect(interrupted).toMatchObject({
      status: 'restart-required',
      resumeCount: 1,
    });
    const second = await drive(f, 'resume');
    expect(second.result, diagnostic(second)).toMatchObject({ exitCode: 0 });
    expect(second.result.evidencePath).toBe(evidencePath);
    const cleaned = await evidence();
    expect(cleaned).toMatchObject({
      status: 'cleaned',
      resumeCount: 2,
      teardownCall: { status: 'cleaned' },
      teardown: { phase: 'complete', failure: null },
    });
    for (const key of ['ingress', 'fleet', 'quota', 'exports'])
      expect(cleaned.teardown.receipts[key]).toMatchObject({
        settledByReread: false,
      });
    expect(cleaned.teardown.receipts.worker).toMatchObject({
      settledByReread: false,
      secretNameCount: f.seededSecretCount,
    });
    expect(cleaned.teardown.receipts.exportObjects).toMatchObject({
      count: f.exportedDatabaseCount,
      settledByReread: 0,
    });
    const residual = cleaned.teardown.residual;
    expect(residual).toMatchObject({
      bucketJurisdictions: ['default'],
      settleAttempts: 1,
    });
    expect(Object.keys(residual.surfaces).sort()).toEqual(
      [...DIRECT_RESIDUAL_SURFACES].sort(),
    );
    for (const surface of Object.values(residual.surfaces) as Array<{
      prefixCount: number;
      globalCount: number;
    }>) {
      expect(surface.prefixCount).toBe(0);
      expect(surface.globalCount).toBe(0);
    }
    expect(residual.dispatch.prefixCount).toBe(0);
    expect(residual.versionsGone).toBe(true);
    expect(residual.dispatch.kind).toBe('empty');
    expect(residual.dispatch.count).toBe(0);
    expect(cleaned.retainedIdentities).toEqual({
      fleetUuid: null,
      quotaUuid: null,
      exportBucket: null,
      scriptName: null,
      activeVersionId: null,
    });
    expect((await stat(evidencePath)).mode & 0o777).toBe(0o600);
    expect(await readdir(directory)).not.toEqual(
      expect.arrayContaining([expect.stringMatching(/\.tmp$/u)]),
    );
    for (const document of [interrupted, cleaned])
      expect(inspectDirectEvidence(document, sentinels).hit).toBeNull();
    const third = await drive(f, 'resume');
    expect(third).toMatchObject({ status: 0, requests: 0 });
    expect(third.result.evidencePath).toBe(evidencePath);
    const reread = await evidence();
    expect(reread).toMatchObject({ resumeCount: 3, teardownCall: null });
    const mask = new Set([
      'finishedAt',
      'mode',
      'exitCode',
      'status',
      'resumeCount',
      'teardownCall',
    ]);
    const durable = (value: object) =>
      Object.fromEntries(
        Object.entries(value).filter(([key]) => !mask.has(key)),
      );
    for (const document of [cleaned, reread]) {
      expect(document).toMatchObject({
        mode: 'resume',
        exitCode: 0,
        status: 'cleaned',
      });
      expect(inspectDirectEvidence(document, sentinels).hit).toBeNull();
    }
    expect(durable(reread)).toEqual(durable(cleaned));
    await unlink(evidencePath);
    const journalPath = join(directory, 'journal.json');
    const before = await readFile(journalPath, 'utf8');
    const siblings = await readdir(
      join(f.local.directory, '.direct-conformance'),
    );
    const fourth = await drive(f, 'run');
    expect(fourth).toMatchObject({
      status: 1,
      requests: 0,
      result: { evidencePath: null, summary: { code: 'run-exists' } },
    });
    expect(existsSync(evidencePath)).toBe(false);
    const holder = await openDirectRunState({
      configPath: f.local.configPath,
      prepared: f.local.prepared,
      accountId: 'account',
      mode: 'resume',
    });
    let fifth: Awaited<ReturnType<typeof drive>>;
    try {
      fifth = await drive(f, 'resume');
      expect(fifth).toMatchObject({
        status: 1,
        requests: 0,
        result: { evidencePath: null, summary: { code: 'lock-unavailable' } },
      });
      expect(existsSync(evidencePath)).toBe(false);
      expect(await readFile(journalPath, 'utf8')).toBe(before);
      expect(
        await readdir(join(f.local.directory, '.direct-conformance')),
      ).toEqual(siblings);
    } finally {
      await holder.close();
    }
    expect(f.native.bridgeErrors).toEqual([]);
    process.stdout.write(
      `CLI_ACCEPTANCE ${JSON.stringify({ exits: [first.status, second.status, third.status, fourth.status, fifth.status], statuses: [interrupted.status, cleaned.status, reread.status], resumeCounts: [interrupted.resumeCount, cleaned.resumeCount, reread.resumeCount], residual, retainedIdentities: cleaned.retainedIdentities, teardownCall: reread.teardownCall })}\n`,
    );
  });

  it('runs the real entry for preflight and help and pins both workspace scripts', async () => {
    const local = await directRunStateFixture();
    const entry = fileURLToPath(
      new URL(
        '../scripts/direct-credentialed-conformance.mjs',
        import.meta.url,
      ),
    );
    const attemptsPath = join(local.directory, 'network-attempts.json');
    const guard = join(local.directory, 'network-guard.mjs');
    await writeFile(
      guard,
      `import { writeFileSync } from 'node:fs';
const attempts = [];
globalThis.fetch = async (input) => {
  attempts.push(new Request(input).url);
  throw new Error('unexpected entry network');
};
process.on('exit', () => writeFileSync(${JSON.stringify(attemptsPath)}, JSON.stringify(attempts)));
`,
    );
    const attempts = async () =>
      JSON.parse(await readFile(attemptsPath, 'utf8')) as string[];
    const preflight = await childProcess(
      ['--import', guard, entry, '--preflight'],
      {
        FLEET_DIRECT_CONFORMANCE_CONFIG: local.configPath,
      },
    );
    expect(preflight.status).toBe(0);
    expect(preflight.stderr).toBe('');
    expect(preflight.stdout.trim().split('\n')).toHaveLength(1);
    expect(preflight.stdout).toMatch(/^DIRECT_CONFORMANCE /u);
    expect(await attempts()).toEqual([]);
    const help = await childProcess(['--import', guard, entry, '--help']);
    expect(help.status).toBe(0);
    expect(help.stderr).toBe('');
    expect(help.stdout).toContain('--resume');
    expect(await attempts()).toEqual([]);
    const manifest = JSON.parse(
      await readFile(new URL('../package.json', import.meta.url), 'utf8'),
    );
    const root = JSON.parse(
      await readFile(new URL('../../../package.json', import.meta.url), 'utf8'),
    );
    expect(manifest.scripts['test:credentialed:direct']).toBe(
      'pnpm build && node scripts/direct-credentialed-conformance.mjs',
    );
    expect(root.scripts['fleet-control:credentialed:direct']).toBe(
      'pnpm --filter @proofoftech/fleet-control test:credentialed:direct',
    );
  });
});
