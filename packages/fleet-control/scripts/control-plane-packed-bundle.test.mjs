// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import { mkdir, mkdtemp, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { test } from 'node:test';
import { assertControlPlaneBundleGraph } from './control-plane-packed-bundle.mjs';
import {
  assertDirectArtifactSdkGraph,
  assertDirectArtifactSdkResolution,
  assertDirectArtifactSourceInputs,
} from './direct-artifacts-packed.mjs';

test('requires the copied HTTP SDK link to select the installed ESM identity', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'direct-sdk-resolution-'));
  try {
    const importer = join(
      directory,
      'fixture',
      'scripts',
      'direct-reference-http.ts',
    );
    const modules = join(directory, 'fixture', 'node_modules');
    await mkdir(modules, { recursive: true });
    for (const name of ['installed', 'foreign']) {
      const root = join(directory, name);
      await mkdir(join(root, 'core'), { recursive: true });
      await writeFile(
        join(root, 'package.json'),
        JSON.stringify({
          name: 'cloudflare',
          type: 'module',
          exports: {
            './index.mjs': './index.mjs',
            './core/error.mjs': './core/error.mjs',
          },
        }),
      );
      await writeFile(join(root, 'index.mjs'), 'export {};');
      await writeFile(join(root, 'core/error.mjs'), 'export {};');
    }
    const entry = join(directory, 'installed', 'index.mjs'),
      error = join(directory, 'installed', 'core', 'error.mjs');
    await assert.rejects(
      assertDirectArtifactSdkResolution(importer, entry, error),
      { code: 'MODULE_NOT_FOUND' },
    );
    await symlink(
      join(directory, 'foreign'),
      join(modules, 'cloudflare'),
      'dir',
    );
    await assert.rejects(
      assertDirectArtifactSdkResolution(importer, entry, error),
      /copied HTTP must resolve the installed SDK entry/u,
    );
    const admitted = join(directory, 'admitted');
    await mkdir(join(admitted, 'node_modules'), { recursive: true });
    await symlink(
      join(directory, 'installed'),
      join(admitted, 'node_modules', 'cloudflare'),
      'dir',
    );
    await assertDirectArtifactSdkResolution(
      join(admitted, 'direct-reference-http.ts'),
      entry,
      error,
    );
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

function sdkGraph() {
  const root = resolve('consumer/node_modules/cloudflare');
  const expected = {
    root,
    entry: join(root, 'index.mjs'),
    error: join(root, 'core/error.mjs'),
    http: resolve('fixture/scripts/direct-reference-http.ts'),
    client: resolve('consumer/fleet-control/dist/cloudflare-client.js'),
    mastra: {
      root: resolve('flowsafe/node_modules/cloudflare'),
      entry: resolve('flowsafe/node_modules/cloudflare/index.mjs'),
      error: resolve('flowsafe/node_modules/cloudflare/error.mjs'),
      owner: resolve(
        'flowsafe/node_modules/@mastra/cloudflare-d1/dist/index.js',
      ),
    },
  };
  const canonicalInputs = {
    'fixture/direct-reference-http.ts': expected.http,
    'fleet-control/dist/cloudflare-client.js': expected.client,
    'node_modules/cloudflare/index.mjs': expected.entry,
    'node_modules/cloudflare/core/error.mjs': expected.error,
    'flowsafe/node_modules/cloudflare/index.mjs': expected.mastra.entry,
    'flowsafe/node_modules/cloudflare/error.mjs': expected.mastra.error,
    'flowsafe/mastra/index.js': expected.mastra.owner,
  };
  const inputs = Object.fromEntries(
    Object.keys(canonicalInputs).map((path) => [
      path,
      {
        imports:
          path.endsWith('.ts') || path.endsWith('cloudflare-client.js')
            ? [{ path: 'node_modules/cloudflare/index.mjs' }]
            : [],
      },
    ]),
  );
  inputs['flowsafe/mastra/index.js'].imports = [
    { path: 'flowsafe/node_modules/cloudflare/index.mjs' },
  ];
  return { graph: { inputs }, canonicalInputs, expected };
}

test('admits the shared installed SDK ESM graph alongside the Mastra SDK', () => {
  const { graph, canonicalInputs, expected } = sdkGraph();
  assertDirectArtifactSdkGraph(graph, canonicalInputs, expected);
});

for (const kind of [
  'wrong root',
  'duplicate alias',
  'CJS error',
  'CJS entry',
  'missing entry',
  'foreign HTTP edge',
  'foreign client edge',
  'HTTP SDK5 edge',
  'client SDK5 edge',
  'SDK5 error substitution',
]) {
  test(`rejects SDK ${kind}`, () => {
    const { graph, canonicalInputs, expected } = sdkGraph();
    if (kind === 'wrong root')
      canonicalInputs['node_modules/cloudflare/core/error.mjs'] = resolve(
        'workspace/node_modules/cloudflare/core/error.mjs',
      );
    else if (kind === 'duplicate alias') {
      graph.inputs['alias-error.mjs'] = { imports: [] };
      canonicalInputs['alias-error.mjs'] = expected.error;
    } else if (kind === 'CJS error' || kind === 'CJS entry') {
      const path =
        kind === 'CJS error'
          ? 'node_modules/cloudflare/core/error.js'
          : 'node_modules/cloudflare/index.js';
      graph.inputs[path] = { imports: [] };
      canonicalInputs[path] = join(
        expected.root,
        kind === 'CJS error' ? 'core/error.js' : 'index.js',
      );
    } else if (kind === 'SDK5 error substitution')
      canonicalInputs['node_modules/cloudflare/core/error.mjs'] =
        expected.mastra.error;
    else if (kind === 'HTTP SDK5 edge' || kind === 'client SDK5 edge')
      graph.inputs[
        kind === 'HTTP SDK5 edge'
          ? 'fixture/direct-reference-http.ts'
          : 'fleet-control/dist/cloudflare-client.js'
      ].imports = [{ path: 'flowsafe/node_modules/cloudflare/index.mjs' }];
    else if (kind === 'missing entry')
      delete graph.inputs['node_modules/cloudflare/index.mjs'];
    else
      graph.inputs[
        kind === 'foreign HTTP edge'
          ? 'fixture/direct-reference-http.ts'
          : 'fleet-control/dist/cloudflare-client.js'
      ].imports = [{ path: 'foreign/index.mjs' }];
    assert.throws(
      () => assertDirectArtifactSdkGraph(graph, canonicalInputs, expected),
      /reference (?:SDK|HTTP)/u,
    );
  });
}

function metadata(path, imports = []) {
  return {
    inputs: { [path]: { imports } },
    outputs: { 'worker.js': { imports: [] } },
  };
}

const directFixture = resolve('packed-direct-artifact-fixture');
const sharedReceiptLeaf = join(directFixture, 'src', 'export-file-name.ts');

test('admits the exact shared receipt-key source leaf alongside installed inputs', () => {
  assertDirectArtifactSourceInputs(directFixture, [
    sharedReceiptLeaf,
    sharedReceiptLeaf,
    join(
      directFixture,
      'node_modules',
      '@proofoftech',
      'fleet-control',
      'dist',
      'index.js',
    ),
  ]);
});

for (const [name, sourceInputs] of [
  ['an empty source set', []],
  [
    'the private config helper',
    [join(directFixture, 'src', 'strict-plain-data.ts')],
  ],
  ['an arbitrary sibling', [join(directFixture, 'src', 'other-helper.ts')]],
  [
    'a suffixed lookalike',
    [join(directFixture, 'src', 'export-file-name.ts.extra')],
  ],
  [
    'a different extension',
    [join(directFixture, 'src', 'export-file-name.mts')],
  ],
  [
    'a nested lookalike',
    [join(directFixture, 'src', 'nested', 'export-file-name.ts')],
  ],
  [
    'a directory-prefix lookalike',
    [join(directFixture, 'src-extra', 'export-file-name.ts')],
  ],
  [
    'the leaf plus the private helper',
    [sharedReceiptLeaf, join(directFixture, 'src', 'strict-plain-data.ts')],
  ],
  [
    'the leaf plus an arbitrary sibling',
    [sharedReceiptLeaf, join(directFixture, 'src', 'other-helper.ts')],
  ],
  [
    'the leaf plus a suffixed lookalike',
    [
      sharedReceiptLeaf,
      join(directFixture, 'src', 'export-file-name.ts.extra'),
    ],
  ],
  [
    'the leaf plus a nested lookalike',
    [
      sharedReceiptLeaf,
      join(directFixture, 'src', 'nested', 'export-file-name.ts'),
    ],
  ],
]) {
  test(`rejects ${name} from the copied source-input set`, () => {
    assert.throws(
      () => assertDirectArtifactSourceInputs(directFixture, sourceInputs),
      /reference Worker source inputs must be exactly the shared receipt-key leaf/u,
    );
  });
}

test('rejects a forbidden import even when esbuild omits it from the emitted Worker', () => {
  assert.throws(
    () =>
      assertControlPlaneBundleGraph(
        metadata('dependency.js', [
          {
            path: 'node:fs/promises',
            kind: 'import-statement',
            external: true,
          },
        ]),
      ),
    /forbidden core module: node:fs\/promises/,
  );
});

test('rejects prefix-only builtins and unrecognized node names in emitted imports', () => {
  for (const path of [
    'node:test',
    'node:test/reporters',
    'node:sea',
    'node:sqlite',
    'node:future',
  ]) {
    const graph = metadata('dependency.js');
    graph.outputs['worker.js'].imports.push({ path, external: true });
    assert.throws(
      () => assertControlPlaneBundleGraph(graph),
      /forbidden core module/,
    );
  }
});

test('rejects host adapters independent of their remaining emitted imports', () => {
  for (const name of [
    'export-store',
    'wrangler-loop-backend',
    'wrangler-plain-worker-provisioning-api',
    'wrangler-runner',
  ]) {
    assert.throws(
      () =>
        assertControlPlaneBundleGraph(
          metadata(
            `../../node_modules/.pnpm/fleet/node_modules/@proofoftech/fleet-control/dist/${name}.js`,
          ),
        ),
      /Worker reaches host module/,
    );
  }
});

test('admits supported core imports without confusing SDK paths for Node modules', () => {
  assertControlPlaneBundleGraph(
    metadata(
      '../node_modules/cloudflare/internal/utils/path.mjs',
      [
        'crypto',
        'node:crypto',
        'async_hooks',
        'node:async_hooks',
        '<runtime>',
      ].map((path) => ({ path, external: true })),
    ),
  );
  assertControlPlaneBundleGraph(
    metadata(
      '../node_modules/@proofoftech/fleet-control/dist/r2-export-store.js',
    ),
  );
});
