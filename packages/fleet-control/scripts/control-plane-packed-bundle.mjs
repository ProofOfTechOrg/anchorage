// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdir, readFile, realpath, writeFile } from 'node:fs/promises';
import { createRequire, isBuiltin } from 'node:module';
import { join, resolve } from 'node:path';
import { gzipSync } from 'node:zlib';

const variants = [
  { name: 'supported', date: '2026-08-06', flags: [] },
  {
    name: 'historical-v1',
    date: '2026-08-03',
    flags: ['nodejs_compat', 'no_nodejs_compat_v2'],
  },
  {
    name: 'historical-v2',
    date: '2026-08-03',
    flags: ['nodejs_compat'],
  },
];

const hostModule =
  /(?:^|\/)fleet-control\/dist\/(?:export-store|wrangler-loop-backend|wrangler-plain-worker-provisioning-api|wrangler-runner)\.js$/;

const bundleBudget = { rawBytes: 4_128_768, gzipBytes: 589_824 };

export function assertControlPlaneBundleGraph(metadata) {
  for (const [input, details] of Object.entries(metadata.inputs)) {
    assert.ok(!hostModule.test(input), `Worker reaches host module: ${input}`);
    for (const imported of details.imports) {
      assertAllowedCore(imported.path);
    }
  }
  for (const details of Object.values(metadata.outputs)) {
    for (const imported of details.imports) {
      assertAllowedCore(imported.path);
    }
  }
}

function assertAllowedCore(path) {
  if (!path.startsWith('node:') && !isBuiltin(path)) return;
  assert.ok(
    ['crypto', 'async_hooks'].includes(path.replace(/^node:/, '')),
    `Worker reaches forbidden core module: ${path}`,
  );
}

export async function verifyControlPlanePackedBundle({
  consumerDirectory,
  packageRoot,
}) {
  const directory = join(consumerDirectory, 'control-plane-bundle');
  const consumerRequire = createRequire(
    join(consumerDirectory, 'package.json'),
  );
  const installedEntry = await realpath(
    consumerRequire.resolve(
      '@proofoftech/fleet-control/cloudflare-control-plane',
    ),
  );
  await mkdir(directory);
  await writeFile(
    join(directory, 'worker.ts'),
    `import * as controlPlane from '@proofoftech/fleet-control/cloudflare-control-plane';
export default {
  fetch() {
    return Response.json(Object.fromEntries(
      Object.entries(controlPlane).map(([name, value]) => [name, typeof value]),
    ));
  },
};
`,
  );

  const measurements = [];
  for (const variant of variants) {
    const outputDirectory = join(directory, variant.name);
    await mkdir(outputDirectory);
    const config = join(outputDirectory, 'wrangler.json');
    const metafile = join(outputDirectory, 'metafile.json');
    const bundleDirectory = join(outputDirectory, 'bundle');
    const workerBundle = join(outputDirectory, 'worker.bundle');
    await writeFile(
      config,
      `${JSON.stringify(
        {
          name: 'fleet-control-packed-bundle',
          main: '../worker.ts',
          compatibility_date: variant.date,
          compatibility_flags: variant.flags,
        },
        null,
        2,
      )}\n`,
    );
    const started = performance.now();
    const log = execFileSync(
      join(packageRoot, 'node_modules/.bin/wrangler'),
      [
        'deploy',
        '--config',
        config,
        '--dry-run',
        '--outdir',
        bundleDirectory,
        '--metafile',
        metafile,
        '--outfile',
        workerBundle,
      ],
      { cwd: consumerDirectory, encoding: 'utf8', stdio: 'pipe' },
    );
    await writeFile(join(outputDirectory, 'dry-run.log'), log);
    const metadata = JSON.parse(await readFile(metafile, 'utf8'));
    assertControlPlaneBundleGraph(metadata);
    const entries = Object.keys(metadata.inputs).filter((input) =>
      input.endsWith('/fleet-control/dist/cloudflare-control-plane.js'),
    );
    assert.equal(
      entries.length,
      1,
      'bundle must reach the curated installed entry',
    );
    assert.equal(
      await realpath(resolve(outputDirectory, entries[0])),
      installedEntry,
      'bundle must reach the same installed Fleet package as the consumer',
    );
    const bytes = await readFile(join(bundleDirectory, 'worker.js'));
    const gzipBytes = gzipSync(bytes).length;
    assert.ok(
      bytes.length <= bundleBudget.rawBytes,
      `${variant.name} raw bundle exceeds the regression budget`,
    );
    assert.ok(
      gzipBytes <= bundleBudget.gzipBytes,
      `${variant.name} gzip bundle exceeds the regression budget`,
    );
    const profilePath = join(outputDirectory, 'startup.cpuprofile');
    const startupLog = execFileSync(
      join(packageRoot, 'node_modules/.bin/wrangler'),
      ['check', 'startup', '--worker', workerBundle, '--outfile', profilePath],
      { cwd: consumerDirectory, encoding: 'utf8', stdio: 'pipe' },
    );
    await writeFile(join(outputDirectory, 'startup.log'), startupLog);
    const profile = JSON.parse(await readFile(profilePath, 'utf8'));
    const nodes = new Map(
      profile.nodes.map((node) => [node.id, node.callFrame.functionName]),
    );
    const activeMicroseconds = profile.timeDeltas.reduce(
      (sum, delta, index) =>
        sum + (nodes.get(profile.samples[index]) === '(idle)' ? 0 : delta),
      0,
    );
    measurements.push({
      ...variant,
      rawBytes: bytes.length,
      gzipBytes,
      sha256: createHash('sha256').update(bytes).digest('hex'),
      buildAndLocalProfileWallMs: performance.now() - started,
      localStartupProfileWindowMs:
        (profile.endTime - profile.startTime) / 1_000,
      localStartupSampledActiveMs: activeMicroseconds / 1_000,
    });
  }

  const baseline = measurements[0];
  for (const measurement of measurements) {
    measurement.rawDeltaBytes = measurement.rawBytes - baseline.rawBytes;
    measurement.gzipDeltaBytes = measurement.gzipBytes - baseline.gzipBytes;
    measurement.rawDeltaPercent =
      (100 * measurement.rawDeltaBytes) / baseline.rawBytes;
    measurement.gzipDeltaPercent =
      (100 * measurement.gzipDeltaBytes) / baseline.gzipBytes;
  }
  const report = {
    measurement:
      'Unminified namespace-import Worker; gzip uses Node defaults. Startup CPU samples come from Wrangler local profiling, not Cloudflare deployment acceptance.',
    bundleBudget,
    measurements,
  };
  await writeFile(
    join(directory, 'measurements.json'),
    `${JSON.stringify(report, null, 2)}\n`,
  );
  process.stdout.write(
    `fleet-control packed bundle: ${JSON.stringify(report)}\n`,
  );
  return report;
}
