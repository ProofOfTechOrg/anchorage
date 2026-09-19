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
  for (const tool of ['wrangler', 'typescript']) {
    await symlink(
      await realpath(join(packageRoot, 'node_modules', tool)),
      join(directory, 'node_modules', tool),
      'dir',
    );
  }
  const { buildDirectConformanceArtifacts } = await import(
    pathToFileURL(join(scripts, 'direct-credentialed-artifacts.mjs'))
  );
  const built = await buildDirectConformanceArtifacts({
    configPath: join(scripts, 'direct-credentialed-conformance.example.json'),
    outputDirectory: join(directory, 'built'),
  });
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
  const metadataBytes = await readFile(built.builds.reference.metafilePath);
  const metadata = JSON.parse(metadataBytes.toString('utf8'));
  const metadataDirectory = dirname(built.builds.reference.metafilePath);
  const inputPaths = Object.keys(metadata.inputs).map((path) =>
    resolve(metadataDirectory, path),
  );
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
    `fleet-control default direct artifacts: ${JSON.stringify({ builds: built.builds, referenceUploadBytes: built.prepared.referenceUploadBytes, referenceModuleSetSha256: built.prepared.referenceModuleSetSha256, entries: graph.entries, runtimeImports: graph.runtimeImports, attributedBytes, graphSha256: graph.metafileSha256, hostContributions: contributions.filter((entry) => hostModule.test(entry.path)) })}\n`,
  );
  return built;
}
