// SPDX-License-Identifier: Apache-2.0

import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { readdir, readFile, stat, unlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterEach, beforeAll, describe, expect, it } from 'vitest';
import type { runDirectConformance } from '../scripts/direct-credentialed-conformance-runtime.mjs';
import {
  DIRECT_EVIDENCE_LITERALS,
  scanDirectEvidence,
} from '../scripts/direct-credentialed-evidence.mjs';
import {
  DIRECT_RESIDUAL_SURFACES,
  openDirectRunState,
} from '../scripts/direct-credentialed-run-state.mjs';
import { directObservationFixture } from './fixtures/direct-observations.js';
import { createDirectReferenceHarness } from './fixtures/direct-reference-harness.js';

const cleanup: Array<() => Promise<void>> = [];
const apiToken = 'inert-provider-token';
const invokeSecret = 'inert-invoke';
const moduleUrl = (name: string) =>
  new URL(`../scripts/${name}.mjs`, import.meta.url).href;

afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close();
});

async function childProcess(args: string[], env: NodeJS.ProcessEnv = {}) {
  const child = spawn(process.execPath, args, {
    env: { PATH: process.env.PATH, ...env },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stdout = '';
  let stderr = '';
  child.stdout.on('data', (chunk) => {
    stdout += String(chunk);
  });
  child.stderr.on('data', (chunk) => {
    stderr += String(chunk);
  });
  const status = await new Promise<number | null>((resolve, reject) => {
    const timer = setTimeout(() => {
      child.kill('SIGKILL');
    }, 840_000);
    child.once('error', (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.once('close', (code) => {
      clearTimeout(timer);
      resolve(code);
    });
  });
  return { status, stdout, stderr };
}

async function fixture() {
  const local = await directObservationFixture(30_000, 'confirmed', {
    invocationTimeoutMs: 600_000,
    maxProviderRequests: 1000,
  });
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
  for (const receipt of [bootstrap.fleet, bootstrap.quota])
    native.world.seedDatabase(receipt.name, { databaseId: receipt.uuid });
  native.buckets.set(`default:${bootstrap.exports.name}`, {
    name: bootstrap.exports.name,
    jurisdiction: bootstrap.exports.jurisdiction,
    creation_date: bootstrap.exports.creationDate,
  });
  const bindings = [
    { name: 'FLEET_DB', type: 'd1', database_id: bootstrap.fleet.uuid },
    { name: 'QUOTA_DB', type: 'd1', database_id: bootstrap.quota.uuid },
    { name: 'EXPORTS', type: 'r2_bucket', bucket_name: bootstrap.exports.name },
    {
      name: 'DIRECT_RUN_BINDING',
      type: 'plain_text',
      text: JSON.stringify(native.binding),
    },
    ...[
      'CLOUDFLARE_API_TOKEN',
      'FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET',
      'DIRECT_DEPLOYMENT_SECRETS',
    ].map((name) => ({ name, type: 'secret_text' })),
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
  return { local, native };
}

async function drive(
  f: Awaited<ReturnType<typeof fixture>>,
  mode: 'run' | 'resume',
) {
  const script = join(f.local.directory, 'runtime.mjs');
  await writeFile(
    script,
    `
import { runDirectConformance } from ${JSON.stringify(moduleUrl('direct-credentialed-conformance-runtime'))};
const originalFetch = globalThis.fetch;
let requests = 0;
const fetch = async (input, init) => {
  const request = new Request(input, init);
  const url = new URL(request.url);
  if (url.origin !== 'https://api.cloudflare.com' && url.origin !== ${JSON.stringify(`https://${f.local.prepared.names.referenceWorker}.attested-account.workers.dev`)})
    throw new Error('unexpected child origin');
  requests += 1;
  const headers = new Headers(request.headers);
  headers.set('X-Direct-Fixture-Url', request.url);
  return originalFetch(${JSON.stringify(f.native.bridgeUrl)}, { method: request.method, headers, body: request.body, signal: request.signal, redirect: 'manual', duplex: 'half' });
};
globalThis.fetch = async () => { throw new Error('unexpected child network'); };
const result = await runDirectConformance({
  mode: ${JSON.stringify(mode)}, configPath: ${JSON.stringify(f.local.configPath)},
  env: { CLOUDFLARE_ACCOUNT_ID: 'account', CLOUDFLARE_API_TOKEN: ${JSON.stringify(apiToken)}, FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET: ${JSON.stringify(invokeSecret)} }, fetch,
});
console.log('CLI_RESULT ' + JSON.stringify({ result, requests }));
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
  };
  expect(child.status).toBe(parsed.result.exitCode);
  return { ...parsed, status: child.status };
}

describe.sequential('direct credentialed CLI native offline acceptance', {
  timeout: 900_000,
}, () => {
  beforeAll(() => {
    expect(
      existsSync(new URL('../dist/index.js', import.meta.url)),
      'run pnpm --filter @proofoftech/fleet-control build before acceptance',
    ).toBe(true);
  });

  it('revalidates, restarts, cleans, rereads evidence, and refuses existing runs and concurrent resumes', async () => {
    const f = await fixture();
    const directory = join(
      f.local.directory,
      '.direct-conformance',
      f.local.prepared.config.resourcePrefix,
    );
    const evidencePath = join(directory, 'evidence.json');
    const evidence = async () =>
      JSON.parse(await readFile(evidencePath, 'utf8'));
    const first = await drive(f, 'resume');
    expect(
      first.result,
      JSON.stringify({
        bridgeErrors: f.native.bridgeErrors,
        requests: f.native.projection.requests
          .slice(-12)
          .map(({ method, url }) => ({ method, url })),
        result: first.result.summary,
      }),
    ).toMatchObject({ exitCode: 3 });
    const interrupted = await evidence();
    expect(interrupted).toMatchObject({
      status: 'restart-required',
      resumeCount: 1,
    });
    const second = await drive(f, 'resume');
    expect(
      second.result,
      JSON.stringify({
        bridgeErrors: f.native.bridgeErrors,
        requests: f.native.projection.requests
          .slice(-12)
          .map(({ method, url }) => ({ method, url })),
        result: second.result.summary,
      }),
    ).toMatchObject({ exitCode: 0 });
    const cleaned = await evidence();
    expect(cleaned).toMatchObject({
      status: 'cleaned',
      resumeCount: 2,
      teardownCall: { status: 'cleaned' },
      teardown: { phase: 'complete', failure: null },
    });
    expect(Object.keys(cleaned.teardown.receipts).sort()).toEqual(
      [
        'ingress',
        'worker',
        'fleet',
        'quota',
        'exports',
        'exportObjects',
      ].sort(),
    );
    for (const key of ['ingress', 'fleet', 'quota', 'exports'])
      expect(cleaned.teardown.receipts[key]).toMatchObject({
        settledByReread: false,
      });
    expect(cleaned.teardown.receipts.worker).toMatchObject({
      settledByReread: false,
      secretNameCount: 3,
    });
    expect(cleaned.teardown.receipts.exportObjects).toMatchObject({
      count: 2,
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
    expect(
      scanDirectEvidence(cleaned, {
        secrets: [apiToken, invokeSecret],
        literals: DIRECT_EVIDENCE_LITERALS,
      }),
    ).toBeNull();
    const third = await drive(f, 'resume');
    expect(third).toMatchObject({ status: 0, requests: 0 });
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
    for (const document of [cleaned, reread])
      expect(document).toMatchObject({
        mode: 'resume',
        exitCode: 0,
        status: 'cleaned',
      });
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
      `CLI_ACCEPTANCE ${JSON.stringify({ exits: [first.status, second.status, third.status, fourth.status, fifth.status], statuses: [interrupted.status, cleaned.status, reread.status, null, null], resumeCounts: [interrupted.resumeCount, cleaned.resumeCount, reread.resumeCount], residual, retainedIdentities: cleaned.retainedIdentities, evidenceOnlyEqual: true, teardownCall: reread.teardownCall, refusalEvidenceAbsent: true })}\n`,
    );
  });

  it('runs the real entry for preflight and help and pins both workspace scripts', async () => {
    const local = await directObservationFixture();
    cleanup.push(() => local.close());
    const entry = fileURLToPath(
      new URL(
        '../scripts/direct-credentialed-conformance.mjs',
        import.meta.url,
      ),
    );
    const preflight = await childProcess([entry, '--preflight'], {
      FLEET_DIRECT_CONFORMANCE_CONFIG: local.configPath,
    });
    expect(preflight.status).toBe(0);
    expect(preflight.stderr).toBe('');
    expect(preflight.stdout.trim().split('\n')).toHaveLength(1);
    expect(preflight.stdout).toMatch(/^DIRECT_CONFORMANCE /u);
    const help = await childProcess([entry, '--help']);
    expect(help.status).toBe(0);
    expect(help.stderr).toBe('');
    expect(help.stdout).toContain('--resume');
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
