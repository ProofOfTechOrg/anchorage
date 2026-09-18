// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import { test } from 'node:test';
import { assertControlPlaneBundleGraph } from './control-plane-packed-bundle.mjs';

function metadata(path, imports = []) {
  return {
    inputs: { [path]: { imports } },
    outputs: { 'worker.js': { imports: [] } },
  };
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
