// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import {
  cp,
  mkdir,
  mkdtemp,
  readFile,
  realpath,
  rm,
  symlink,
  writeFile,
} from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import test, { after, before } from 'node:test';
import { fileURLToPath } from 'node:url';
import { verifyControlPlanePackedSurface } from './control-plane-packed-surface.mjs';

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
let baselineDirectory;

before(async () => {
  baselineDirectory = await mkdtemp(join(tmpdir(), 'fleet-surface-baseline-'));
  await cp(join(packageRoot, 'dist'), join(baselineDirectory, 'dist'), {
    recursive: true,
  });
  await cp(
    join(packageRoot, 'package.json'),
    join(baselineDirectory, 'package.json'),
  );
});

after(async () => {
  if (baselineDirectory)
    await rm(baselineDirectory, { recursive: true, force: true });
});

async function fixture(context) {
  const consumerDirectory = await mkdtemp(
    join(tmpdir(), 'fleet-surface-mutation-'),
  );
  context.after(() => rm(consumerDirectory, { recursive: true, force: true }));
  const installedRoot = join(
    consumerDirectory,
    'node_modules/@proofoftech/fleet-control',
  );
  await mkdir(installedRoot, { recursive: true });
  // Copies isolate mutation tests; the publication gate consumes an installed tarball.
  await cp(join(baselineDirectory, 'dist'), join(installedRoot, 'dist'), {
    recursive: true,
  });
  const manifest = JSON.parse(
    await readFile(join(baselineDirectory, 'package.json'), 'utf8'),
  );
  manifest.exports['./cloudflare-control-plane'] = {
    types: './dist/cloudflare-control-plane.d.ts',
    default: './dist/cloudflare-control-plane.js',
  };
  await writeFile(
    join(installedRoot, 'package.json'),
    JSON.stringify(manifest),
  );
  await writeFile(
    join(consumerDirectory, 'package.json'),
    JSON.stringify({ private: true, type: 'module' }),
  );
  await mkdir(join(consumerDirectory, 'node_modules/@cloudflare'), {
    recursive: true,
  });
  for (const [name, target] of [
    [
      '@cloudflare/workers-types',
      join(packageRoot, 'node_modules/@cloudflare/workers-types'),
    ],
    ['@proofoftech/flowsafe', resolve(packageRoot, '../flowsafe')],
    ['cloudflare', join(packageRoot, 'node_modules/cloudflare')],
    ['p-queue', join(packageRoot, 'node_modules/p-queue')],
  ]) {
    await symlink(
      await realpath(target),
      join(consumerDirectory, 'node_modules', name),
      'dir',
    );
  }
  return {
    consumerDirectory,
    installedRoot,
    verify: () =>
      verifyControlPlanePackedSurface({ consumerDirectory, packageRoot }),
    async replace(file, before, after) {
      const path = join(installedRoot, 'dist', file);
      const source = await readFile(path, 'utf8');
      assert.ok(source.includes(before), `mutation anchor missing in ${file}`);
      await writeFile(path, source.replace(before, after));
    },
  };
}

test('accepts the built public surface through isolated package resolution', async (context) => {
  const consumer = await fixture(context);
  const result = await consumer.verify();
  assert.equal(result.identity.packageRoot, consumer.installedRoot);
  const report = JSON.parse(await readFile(result.artifacts.report, 'utf8'));
  assert.equal(report.status, 'passed');
  assert.deepEqual(report.diagnostics, []);
  const config = JSON.parse(await readFile(result.artifacts.config, 'utf8'));
  assert.equal(config.compilerOptions.skipLibCheck, false);
  assert.deepEqual(config.compilerOptions.types, ['@cloudflare/workers-types']);
});

test('rejects a missing named type import with its compiler diagnostic', async (context) => {
  const consumer = await fixture(context);
  await consumer.replace(
    'cloudflare-control-plane.d.ts',
    'export interface CloudflareFleetInventoryOptions',
    'interface CloudflareFleetInventoryOptions',
  );
  await assert.rejects(
    consumer.verify(),
    /has no exported member named 'CloudflareFleetInventoryOptions'/,
  );
});

test('rejects an extra declaration export without relying on compiler errors', async (context) => {
  const consumer = await fixture(context);
  await consumer.replace(
    'cloudflare-control-plane.d.ts',
    'export interface CloudflareControlPlaneOptions',
    'export interface UnexpectedPublicType {}\nexport interface CloudflareControlPlaneOptions',
  );
  await assert.rejects(
    consumer.verify(),
    /curated declaration export table differs/,
  );
});

test('rejects a referenced local data type missing from the curated exports', async (context) => {
  const consumer = await fixture(context);
  await consumer.replace(
    'cloudflare-control-plane.d.ts',
    'export interface CloudflareControlPlaneOptions {',
    'interface HiddenPackedData { marker: string }\nexport interface CloudflareControlPlaneOptions { readonly hidden?: HiddenPackedData;',
  );
  await assert.rejects(
    consumer.verify(),
    /missing public type export HiddenPackedData/,
  );
});

test('rejects a nominal provider client exposed through an adapter constructor', async (context) => {
  const consumer = await fixture(context);
  await consumer.replace(
    'd1-fleet-state-database.d.ts',
    'constructor(binding: D1Database)',
    "constructor(binding: D1Database, client?: import('./cloudflare-client.js').CloudflareProvisioningClient)",
  );
  await assert.rejects(
    consumer.verify(),
    /forbidden public capability CloudflareProvisioningClient/,
  );
});

test('rejects a provider capability exposed through a callback result', async (context) => {
  const consumer = await fixture(context);
  await consumer.replace(
    'cloudflare-control-plane.d.ts',
    'export interface CloudflareControlPlaneOptions {',
    "export interface CloudflareControlPlaneOptions { readonly escaped?: () => import('./types.js').ProvisioningBackend;",
  );
  await assert.rejects(
    consumer.verify(),
    /forbidden public capability ProvisioningBackend/,
  );
});

test('excludes private and protected fields from callable public closure', async (context) => {
  const consumer = await fixture(context);
  await consumer.replace(
    'd1-fleet-state-database.d.ts',
    '#private;',
    "#private;\nprivate client: import('./cloudflare-client.js').CloudflareProvisioningClient;\nprotected backend: import('./types.js').ProvisioningBackend;",
  );
  await consumer.verify();
});

test('rejects a runtime export absent from the declaration table', async (context) => {
  const consumer = await fixture(context);
  await consumer.replace(
    'cloudflare-control-plane.js',
    'export { ProvisioningError }',
    'export const unexpectedRuntimeCapability = () => {};\nexport { ProvisioningError }',
  );
  await assert.rejects(consumer.verify(), /unexpectedRuntimeCapability/);
});

test('rejects a missing runtime export despite intact declarations', async (context) => {
  const consumer = await fixture(context);
  await consumer.replace(
    'cloudflare-control-plane.js',
    "export { ProvisioningError } from './provision.js';",
    '',
  );
  await assert.rejects(consumer.verify(), /ProvisioningError/);
});

test('rejects an extra callable capability on the factory return object', async (context) => {
  const consumer = await fixture(context);
  await consumer.replace(
    'cloudflare-control-plane.js',
    'return Object.freeze({',
    'return Object.freeze({ unexpectedFactoryCapability() {},',
  );
  await assert.rejects(consumer.verify(), /unexpectedFactoryCapability/);
});
