// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import { join, resolve } from 'node:path';
import { test } from 'node:test';
import { assertControlPlaneBundleGraph } from './control-plane-packed-bundle.mjs';
import { assertDirectArtifactSourceInputs } from './direct-artifacts-packed.mjs';

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
