// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import {
  copyFile,
  mkdir,
  readdir,
  readFile,
  realpath,
  symlink,
  writeFile,
} from 'node:fs/promises';
import { createRequire } from 'node:module';
import { dirname, join, resolve, sep } from 'node:path';
import { pathToFileURL } from 'node:url';

const hostModule =
  /(?:^|\/)fleet-control\/dist\/(?:export-store|wrangler-loop-backend|wrangler-plain-worker-provisioning-api|wrangler-runner)\.js$/u;

export function assertDirectArtifactSourceInputs(directory, inputPaths) {
  const sourceRoot = join(directory, 'src');
  const sourceInputs = inputPaths.filter((path) =>
    path.startsWith(`${sourceRoot}${sep}`),
  );
  assert.deepEqual(
    [...new Set(sourceInputs)].sort(),
    [join(sourceRoot, 'export-file-name.ts')],
    'reference Worker source inputs must be exactly the shared receipt-key leaf',
  );
}

export function assertDirectArtifactSdkGraph(
  metadata,
  canonicalInputs,
  expected,
) {
  const sdkInputs = Object.keys(metadata.inputs).filter(
    (path) =>
      /(?:^|\/)cloudflare\//u.test(path) ||
      /(?:^|\/)cloudflare\//u.test(canonicalInputs[path]) ||
      canonicalInputs[path]?.startsWith(`${expected.root}${sep}`),
  );
  const classifierInputs = sdkInputs.filter((path) =>
    canonicalInputs[path]?.startsWith(`${expected.root}${sep}`),
  );
  assert.equal(
    new Set(classifierInputs.map((path) => canonicalInputs[path])).size,
    classifierInputs.length,
    'reference SDK modules must not have duplicate raw aliases',
  );
  for (const path of sdkInputs) {
    assert.ok(
      [expected.root, expected.mastra.root].some((root) =>
        canonicalInputs[path]?.startsWith(`${root}${sep}`),
      ),
      `reference SDK input must use its resolved dependency root: ${path}`,
    );
  }
  for (const path of classifierInputs) {
    assert.ok(
      !/\/(?:index|core\/error)\.js$/u.test(canonicalInputs[path]),
      'reference SDK must use ESM errors',
    );
  }
  for (const [label, target] of [
    ['entry', expected.entry],
    ['error', expected.error],
  ]) {
    const instances = classifierInputs.filter(
      (path) => canonicalInputs[path] === target,
    );
    assert.equal(
      instances.length,
      1,
      `reference SDK ${label} must have one raw input instance`,
    );
  }
  for (const [importer, entry] of [
    [expected.http, expected.entry],
    [expected.client, expected.entry],
    [expected.mastra.owner, expected.mastra.entry],
  ]) {
    const paths = Object.keys(metadata.inputs).filter(
      (path) => canonicalInputs[path] === importer,
    );
    assert.equal(
      paths.length,
      1,
      'reference SDK importer must have one input instance',
    );
    const imports = metadata.inputs[paths[0]].imports.filter(
      (edge) => !edge.external && canonicalInputs[edge.path] === entry,
    );
    assert.equal(
      imports.length,
      1,
      'reference SDK importer must reach its expected ESM entry',
    );
  }
}

export async function assertDirectArtifactSdkResolution(
  httpPath,
  sdkEntry,
  sdkError,
) {
  const copiedRequire = createRequire(httpPath);
  assert.equal(
    await realpath(copiedRequire.resolve('cloudflare/index.mjs')),
    sdkEntry,
    'copied HTTP must resolve the installed SDK entry',
  );
  assert.equal(
    await realpath(copiedRequire.resolve('cloudflare/core/error.mjs')),
    sdkError,
    'copied HTTP must resolve the installed SDK error module',
  );
}

export async function verifyDirectArtifactsPacked({
  consumerDirectory,
  packageRoot,
}) {
  const directory = join(consumerDirectory, 'direct-artifact-fixture');
  const scripts = join(directory, 'scripts');
  await mkdir(directory);
  await mkdir(scripts);
  await mkdir(join(directory, 'src'));
  await mkdir(join(directory, 'node_modules'));
  for (const name of await readdir(join(packageRoot, 'scripts'))) {
    if (name.startsWith('direct-') && /\.(?:ts|mts|mjs|json)$/u.test(name)) {
      await copyFile(join(packageRoot, 'scripts', name), join(scripts, name));
    }
  }
  for (const name of ['export-file-name.ts', 'strict-plain-data.ts']) {
    await copyFile(
      join(packageRoot, 'src', name),
      join(directory, 'src', name),
    );
  }
  for (const tool of ['wrangler', 'typescript', 'zod']) {
    await symlink(
      await realpath(join(packageRoot, 'node_modules', tool)),
      join(directory, 'node_modules', tool),
      'dir',
    );
  }
  const consumerRequire = createRequire(
    join(consumerDirectory, 'package.json'),
  );
  const entries = await Promise.all(
    [
      '@proofoftech/fleet-control',
      '@proofoftech/fleet-control/cloudflare-control-plane',
    ].map(async (entry) => [
      entry,
      await realpath(consumerRequire.resolve(entry)),
    ]),
  );
  const fleetEntry = entries[0][1];
  const fleetRequire = createRequire(fleetEntry);
  const sdkEntry = await realpath(fleetRequire.resolve('cloudflare/index.mjs'));
  const sdkRoot = dirname(sdkEntry);
  const sdkError = await realpath(
    fleetRequire.resolve('cloudflare/core/error.mjs'),
  );
  const flowsafeRequire = createRequire(
    await realpath(fleetRequire.resolve('@proofoftech/flowsafe')),
  );
  const mastraManifestPath = await realpath(
    flowsafeRequire.resolve('@mastra/cloudflare-d1/package.json'),
  );
  const mastraManifest = JSON.parse(await readFile(mastraManifestPath, 'utf8'));
  assert.equal(mastraManifest.name, '@mastra/cloudflare-d1');
  const mastraEntry = await realpath(
    join(
      dirname(mastraManifestPath),
      mastraManifest.exports['.'].import.default,
    ),
  );
  const mastraRequire = createRequire(mastraEntry);
  const mastraSdkEntry = await realpath(
    mastraRequire.resolve('cloudflare/index.mjs'),
  );
  const mastraSdkRoot = dirname(mastraSdkEntry);
  const mastraSdkManifest = JSON.parse(
    await readFile(join(mastraSdkRoot, 'package.json'), 'utf8'),
  );
  assert.equal(mastraSdkManifest.name, 'cloudflare');
  const mastraSdk = {
    owner: mastraEntry,
    ownerVersion: mastraManifest.version,
    declaredDependency: mastraManifest.dependencies.cloudflare,
    root: mastraSdkRoot,
    entry: mastraSdkEntry,
    error: await realpath(mastraRequire.resolve('cloudflare/error.mjs')),
    version: mastraSdkManifest.version,
  };
  const sdkManifest = JSON.parse(
    await readFile(join(sdkRoot, 'package.json'), 'utf8'),
  );
  const fleetManifest = JSON.parse(
    await readFile(join(dirname(fleetEntry), '..', 'package.json'), 'utf8'),
  );
  assert.equal(sdkManifest.name, 'cloudflare');
  assert.equal(sdkManifest.version, fleetManifest.dependencies.cloudflare);
  assert.equal(sdkManifest.exports['.'].default, './index.mjs');
  assert.equal(sdkManifest.exports['./index.mjs'].default, './index.mjs');
  assert.equal(sdkManifest.exports['./core/*.mjs'].default, './core/*.mjs');
  await symlink(sdkRoot, join(directory, 'node_modules', 'cloudflare'), 'dir');
  await assertDirectArtifactSdkResolution(
    join(scripts, 'direct-reference-http.ts'),
    sdkEntry,
    sdkError,
  );
  const { buildDirectConformanceArtifacts } = await import(
    pathToFileURL(join(scripts, 'direct-credentialed-artifacts.mjs'))
  );
  const built = await buildDirectConformanceArtifacts({
    configPath: join(scripts, 'direct-credentialed-conformance.example.json'),
    outputDirectory: join(directory, 'built'),
  });
  const metadataBytes = await readFile(built.builds.reference.metafilePath);
  const metadata = JSON.parse(metadataBytes.toString('utf8'));
  const metadataDirectory = dirname(built.builds.reference.metafilePath);
  const inputPaths = Object.keys(metadata.inputs).map((path) =>
    resolve(metadataDirectory, path),
  );
  const canonicalInputs = Object.fromEntries(
    await Promise.all(
      Object.keys(metadata.inputs).map(async (path) => [
        path,
        path.startsWith('node-built-in-modules:')
          ? path
          : await realpath(resolve(metadataDirectory, path)),
      ]),
    ),
  );
  const sdk = {
    root: sdkRoot,
    entry: sdkEntry,
    error: sdkError,
    http: await realpath(join(scripts, 'direct-reference-http.ts')),
    client: await realpath(join(dirname(fleetEntry), 'cloudflare-client.js')),
    mastra: mastraSdk,
  };
  assertDirectArtifactSdkGraph(metadata, canonicalInputs, sdk);
  for (const [entry, expected] of entries) {
    const candidates = inputPaths.filter((path) =>
      path.endsWith(
        `/fleet-control/dist/${entry.endsWith('/cloudflare-control-plane') ? 'cloudflare-control-plane' : 'index'}.js`,
      ),
    );
    assert.equal(
      candidates.length,
      1,
      `default reference must reach one installed ${entry} entry`,
    );
    assert.equal(
      await realpath(candidates[0]),
      expected,
      `default reference must resolve the consumer's ${entry}`,
    );
  }
  assertDirectArtifactSourceInputs(directory, inputPaths);
  const mainPath = join(metadataDirectory, 'out', 'reference.js');
  const output = Object.entries(metadata.outputs).find(
    ([path]) => resolve(metadataDirectory, path) === mainPath,
  )?.[1];
  assert.ok(output, 'default reference main output must be present');
  assert.equal(output.bytes, built.builds.reference.rawBytes);
  const contributions = Object.entries(output.inputs)
    .map(([path, entry]) => ({ path, bytes: entry.bytesInOutput }))
    .sort(
      (left, right) =>
        right.bytes - left.bytes || left.path.localeCompare(right.path),
    );
  for (const entry of contributions) {
    assert.ok(Number.isSafeInteger(entry.bytes) && entry.bytes >= 0);
    assert.ok(
      entry.bytes === 0 || !hostModule.test(entry.path),
      `default Worker contains host implementation: ${entry.path}`,
    );
  }
  const attributedBytes = contributions.reduce(
    (sum, entry) => sum + entry.bytes,
    0,
  );
  assert.ok(attributedBytes <= output.bytes);
  const graph = {
    entries: Object.fromEntries(entries),
    sdk,
    canonicalInputs,
    inputs: Object.keys(metadata.inputs).sort(),
    runtimeImports: output.imports,
    contributions,
    attributedBytes,
    outputBytes: output.bytes,
    metafileSha256: createHash('sha256').update(metadataBytes).digest('hex'),
  };
  await writeFile(
    join(directory, 'default-reference-graph.json'),
    `${JSON.stringify(graph, null, 2)}\n`,
  );
  process.stdout.write(
    `fleet-control default direct artifacts: ${JSON.stringify({ builds: built.builds, referenceUploadBytes: built.prepared.referenceUploadBytes, referenceModuleSetSha256: built.prepared.referenceModuleSetSha256, entries: graph.entries, sdk: graph.sdk, runtimeImports: graph.runtimeImports, attributedBytes, graphSha256: graph.metafileSha256, hostContributions: contributions.filter((entry) => hostModule.test(entry.path)) })}\n`,
  );
  return built;
}
