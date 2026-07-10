import { defineConfig } from 'vitest/config';

const src = (path: string) => new URL(path, import.meta.url).pathname;

export default defineConfig({
  resolve: {
    alias: [
      // Package-internal aliases (mirror tsconfig paths). #worker/* needs no
      // entry — Vite resolves the package.json "imports" field natively.
      { find: /^@\/(.*)$/, replacement: src('./src/$1') },
      // Deep DOM-free flowsafe source imports used by the SPA.
      { find: /^@flowsafe\/(.*)$/, replacement: src('../flowsafe/src/$1') },
      // Cross-package test fixtures (tsconfig.paths.json mirrors this).
      {
        find: /^@flowsafe-test\/(.*)$/,
        replacement: src('../flowsafe/test-support/$1'),
      },
      // Cross-package specifiers resolve to SOURCE (exact matches, mirroring
      // tsconfig.worker.json) so tests never depend on a built dist.
      {
        find: /^@proofoftech\/breakwater$/,
        replacement: src('../breakwater/src/index.ts'),
      },
      {
        find: /^@proofoftech\/flowsafe\/host-kit\/module$/,
        replacement: src('../flowsafe/src/host-kit/module.ts'),
      },
      ...[
        'approval-api',
        'artifacts',
        'audit-export',
        'do-runner',
        'host-kit',
      ].map((subpath) => ({
        find: new RegExp(`^@proofoftech/flowsafe/${subpath}$`),
        replacement: src(`../flowsafe/src/${subpath}/index.ts`),
      })),
    ],
  },
  test: {
    include: ['src/**/*.test.{ts,tsx}', 'worker/**/*.test.ts'],
    // Node env by default (worker + logic tests); component tests opt into
    // jsdom per-file via `// @vitest-environment jsdom`.
    setupFiles: ['./src/test/setup.ts'],
  },
});
